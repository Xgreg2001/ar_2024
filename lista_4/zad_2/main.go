package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type globalCounter struct {
	mu    sync.Mutex
	count uint64
}

var counter globalCounter

type UID struct {
	High uint64
	Low  uint64
}

func (u UID) String() string {
	return fmt.Sprintf("%016x%016x", u.High, u.Low)
}

type UIDGenerator struct {
	workerID      uint16
	threadID      uint16
	datacenterID  uint16
	sequence      uint32
	lastTimestamp uint64
}

const (
	timestampBits    = 48
	workerIDBits     = 16
	threadIDBits     = 16
	datacenterIDBits = 16
	sequenceBits     = 32
)

const maxTimestamp = (uint64(1) << timestampBits) - 1

func NewUIDGenerator(workerID, threadID, datacenterID uint16) *UIDGenerator {
	return &UIDGenerator{
		workerID:     workerID,
		threadID:     threadID,
		datacenterID: datacenterID,
	}
}

func (g *UIDGenerator) currentTimestamp() uint64 {
	return uint64(time.Now().UnixMilli())
}

func (g *UIDGenerator) NextUID() UID {
	timestamp := g.currentTimestamp()
	if timestamp > maxTimestamp {
		timestamp &= maxTimestamp
	}

	if timestamp == g.lastTimestamp {
		g.sequence++
		if g.sequence == 0 {
			for timestamp <= g.lastTimestamp {
				time.Sleep(time.Millisecond)
				timestamp = g.currentTimestamp()
			}
			g.lastTimestamp = timestamp
			g.sequence = 0
		}
	} else {
		g.sequence = 0
		g.lastTimestamp = timestamp
	}

	high := ((timestamp & maxTimestamp) << workerIDBits) | uint64(g.workerID)
	low := (uint64(g.threadID) << (datacenterIDBits + sequenceBits)) |
		(uint64(g.datacenterID) << sequenceBits) |
		uint64(g.sequence)

	return UID{High: high, Low: low}
}

type Heartbeat struct {
	WorkerID  uint16
	Timestamp time.Time
}

type Worker struct {
	id          uint16
	generator   *UIDGenerator
	stopCh      chan struct{}
	heartbeatCh chan<- Heartbeat
	UIDcount    uint64
}

func NewWorker(id uint16, heartbeatCh chan<- Heartbeat) (*Worker, error) {
	generator := NewUIDGenerator(id, 1, 1)
	return &Worker{
		id:          id,
		generator:   generator,
		stopCh:      make(chan struct{}),
		heartbeatCh: heartbeatCh,
		UIDcount:    0,
	}, nil
}

func (w *Worker) Start() {
	go w.generateUIDs()
	go w.sendHeartbeats()
}

func (w *Worker) generateUIDs() {
	for {
		select {
		case <-w.stopCh:
			fmt.Printf("Worker %d: Zatrzymanie generowania UIDów\n", w.id)
			return
		default:
			_ = w.generator.NextUID()
			w.UIDcount++
		}
	}
}

func (w *Worker) sendHeartbeats() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			fmt.Printf("Worker %d: Zatrzymanie wysyłania heartbeatów\n", w.id)
			return
		case <-ticker.C:
			if rand.Float64() < 0.01 {
				fmt.Printf("Worker %d: Symulacja awarii – przerywam heartbeat\n", w.id)
				w.Stop()
				return
			}
			hb := Heartbeat{
				WorkerID:  w.id,
				Timestamp: time.Now(),
			}
			w.heartbeatCh <- hb
		}
	}
}

func (w *Worker) Stop() {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)

		fmt.Printf("Worker %d: Wygenerowano %d UID-ów\n", w.id, w.UIDcount)

		counter.mu.Lock()
		counter.count += w.UIDcount
		counter.mu.Unlock()
	}
}

type Watchdog struct {
	heartbeatCh      <-chan Heartbeat
	alertCh          chan<- uint16
	workerHeartbeats map[uint16]time.Time
	threshold        time.Duration
	mu               sync.Mutex
}

func NewWatchdog(heartbeatCh <-chan Heartbeat, alertCh chan<- uint16, threshold time.Duration) *Watchdog {
	return &Watchdog{
		heartbeatCh:      heartbeatCh,
		alertCh:          alertCh,
		workerHeartbeats: make(map[uint16]time.Time),
		threshold:        threshold,
	}
}

func (w *Watchdog) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case hb := <-w.heartbeatCh:
			w.mu.Lock()
			w.workerHeartbeats[hb.WorkerID] = hb.Timestamp
			w.mu.Unlock()
		case <-ticker.C:
			w.checkHeartbeats()
		case <-ctx.Done():
			fmt.Println("Watchdog: Zatrzymuję działanie")
			return
		}
	}
}

func (w *Watchdog) checkHeartbeats() {
	now := time.Now()
	w.mu.Lock()
	for workerID, lastHb := range w.workerHeartbeats {
		if now.Sub(lastHb) > w.threshold {
			fmt.Printf("Watchdog: Worker %d nie odpowiada. Ostatni heartbeat: %v\n", workerID, lastHb)
			w.alertCh <- workerID
			delete(w.workerHeartbeats, workerID)
		}
	}
	w.mu.Unlock()
}

type Supervisor struct {
	desiredWorkerCount int
	workers            map[uint16]*Worker
	nextWorkerID       uint16
	heartbeatCh        chan Heartbeat
	alertCh            chan uint16
	mu                 sync.Mutex
}

func NewSupervisor(desiredCount int) *Supervisor {
	return &Supervisor{
		desiredWorkerCount: desiredCount,
		workers:            make(map[uint16]*Worker),
		nextWorkerID:       1,
		heartbeatCh:        make(chan Heartbeat, 100),
		alertCh:            make(chan uint16, 10),
	}
}

func (s *Supervisor) spawnWorker() {
	s.mu.Lock()
	id := s.nextWorkerID
	s.nextWorkerID++
	s.mu.Unlock()

	worker, err := NewWorker(id, s.heartbeatCh)
	if err != nil {
		fmt.Printf("Supervisor: Błąd przy tworzeniu workera %d: %v\n", id, err)
		return
	}
	s.mu.Lock()
	s.workers[id] = worker
	s.mu.Unlock()
	fmt.Printf("Supervisor: Utworzono workera %d\n", id)
	worker.Start()
}

func (s *Supervisor) removeWorker(id uint16) {
	s.mu.Lock()
	worker, exists := s.workers[id]
	if exists {
		worker.Stop()
		delete(s.workers, id)
		fmt.Printf("Supervisor: Usunięto workera %d\n", id)
	}
	s.mu.Unlock()
}

func (s *Supervisor) StopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, worker := range s.workers {
		worker.Stop()
		delete(s.workers, id)
		fmt.Printf("Supervisor: Zatrzymano workera %d\n", id)
	}
	fmt.Println("Supervisor: Zatrzymano wszystkie workery")
}

func (s *Supervisor) Run(ctx context.Context) {
	watchdog := NewWatchdog(s.heartbeatCh, s.alertCh, 3*time.Second)
	go watchdog.Run(ctx)

	for i := 0; i < s.desiredWorkerCount; i++ {
		s.spawnWorker()
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case workerID := <-s.alertCh:
			fmt.Printf("Supervisor: Otrzymano alert dla workera %d\n", workerID)
			s.removeWorker(workerID)
		case <-ticker.C:
			s.mu.Lock()
			currentCount := len(s.workers)
			s.mu.Unlock()
			if currentCount < s.desiredWorkerCount {
				fmt.Printf("Supervisor: Liczba workerów (%d) poniżej wymaganego (%d) – tworzę nowych workerów\n", currentCount, s.desiredWorkerCount)
				for i := 0; i < s.desiredWorkerCount-currentCount; i++ {
					s.spawnWorker()
				}
			}
		case <-ctx.Done():
			fmt.Println("Supervisor: Otrzymano sygnał zakończenia, zatrzymywanie supervisor")
			s.StopAll()
			return
		}
	}
}

func main() {
	desiredWorkers := flag.Int("workers", 10, "Liczba workerów do uruchomienia")
	flag.Parse()

	counter.mu.Lock()
	counter.count = 0
	counter.mu.Unlock()

	supervisor := NewSupervisor(*desiredWorkers)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	supervisor.Run(ctx)

	counter.mu.Lock()
	totalUIDs := counter.count
	counter.mu.Unlock()

	avgPerSec := float64(totalUIDs) / 30.0

	fmt.Printf("\nSystem zatrzymany. Wygenerowano łącznie %d UID-ów.\n", totalUIDs)
	fmt.Printf("Średnia generowanych UID-ów na sekundę: %.2f\n", avgPerSec)
}
