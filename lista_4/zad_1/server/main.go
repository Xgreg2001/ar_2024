// server/main.go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"server/logger" // Update this to match your module name
)

// Add these constants after the imports
const (
	snapshotDir         = "snapshots"
	retainSnapshotCount = 2
)

// command represents a client operation.
type command struct {
	Op    string `json:"op"`              // "set" or "get"
	Key   string `json:"key"`             // key name
	Value string `json:"value,omitempty"` // value (only for "set")
}

// fsm is our simple finite state machine (in-memory key/value store).
type fsm struct {
	mu     sync.Mutex
	store  map[string]string
	nodeID string // Add this field
}

func newFSM(nodeID string) *fsm { // Modify function signature
	return &fsm{
		store:  make(map[string]string),
		nodeID: nodeID, // Set the node ID
	}
}

// Apply applies a Raft log entry to the FSM.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		log.Printf("[Node %s] failed to unmarshal command: %v", f.nodeID, err)
		return nil
	}
	switch c.Op {
	case "set":
		f.mu.Lock()
		f.store[c.Key] = c.Value
		f.mu.Unlock()
		log.Printf("[Node %s] Set key %q to %q", f.nodeID, c.Key, c.Value)
	}
	return nil
}

// Snapshot creates a point-in-time snapshot of the FSM.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Create a copy of the store.
	clone := make(map[string]string)
	for k, v := range f.store {
		clone[k] = v
	}
	return &fsmSnapshot{
		store:  clone,
		nodeID: f.nodeID,
	}, nil
}

// Restore restores the FSM from a snapshot.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var data struct {
		Store  map[string]string `json:"store"`
		NodeID string            `json:"nodeID"`
	}
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return err
	}
	f.mu.Lock()
	f.store = data.Store
	f.nodeID = data.NodeID
	f.mu.Unlock()
	return nil
}

// fsmSnapshot implements raft.FSMSnapshot.
type fsmSnapshot struct {
	store  map[string]string
	nodeID string
}

// Persist writes the snapshot to the sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data := map[string]interface{}{
		"store":  s.store,
		"nodeID": s.nodeID,
	}

	if err := json.NewEncoder(sink).Encode(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release is a no-op.
func (s *fsmSnapshot) Release() {}

// createRaftNode creates a Raft node with in-memory storage and transport.
func createRaftNode(id string, transport *raft.InmemTransport, existingFSM *fsm) (*raft.Raft, *fsm, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)

	baseLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft-node",
		Level:  hclog.Debug,
		Output: os.Stdout,
	})

	filteredLogger := logger.New(baseLogger)
	config.Logger = filteredLogger

	config.HeartbeatTimeout = 1000 * time.Millisecond
	config.ElectionTimeout = 1000 * time.Millisecond
	config.CommitTimeout = 500 * time.Millisecond

	// Create node-specific snapshot directory
	nodeSnapshotDir := filepath.Join(snapshotDir, id)
	if err := os.MkdirAll(nodeSnapshotDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create snapshot directory: %v", err)
	}

	var f *fsm
	if existingFSM != nil {
		f = existingFSM
	} else {
		f = newFSM(id)
	}

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()

	// Use FileSnapshotStore instead of InmemSnapshotStore
	snapshotStore, err := raft.NewFileSnapshotStore(nodeSnapshotDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create snapshot store: %v", err)
	}

	r, err := raft.NewRaft(config, f, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, nil, err
	}
	return r, f, nil
}

// getLeader returns the Raft instance that is currently leader.
func getLeader(nodes []*raft.Raft) *raft.Raft {
	for _, r := range nodes {
		if r.State() == raft.Leader {
			return r
		}
	}
	return nil
}

// Global variables to hold our nodes and (for simplicity) keep a reference to the leader's FSM.
var (
	raftNodes  []*raft.Raft
	fsms       []*fsm // one per node; the leader's FSM holds the canonical state
	nodeIDs    = []string{"node1", "node2", "node3", "node4", "node5"}
	nodeState  []bool // true if node is running, false if stopped
	transports []*raft.InmemTransport
	addresses  []raft.ServerAddress
)

// Add this function before main()
func cleanSnapshotDirectory() error {
	if err := os.RemoveAll(snapshotDir); err != nil {
		return fmt.Errorf("failed to remove snapshots directory: %v", err)
	}
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshots directory: %v", err)
	}
	return nil
}

func main() {
	// Set up standard logging
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// Clean snapshots directory before starting
	if err := cleanSnapshotDirectory(); err != nil {
		log.Fatalf("failed to clean snapshots directory: %v", err)
	}

	// Initialize nodeState
	nodeState = make([]bool, len(nodeIDs))
	for i := range nodeState {
		nodeState[i] = true
	}

	// Create 5 Raft nodes with in-memory transports.
	for _, id := range nodeIDs {
		addr, trans := raft.NewInmemTransport("")
		addresses = append(addresses, addr)
		transports = append(transports, trans)
		r, f, err := createRaftNode(id, trans, nil)
		if err != nil {
			log.Fatalf("failed to create raft node %s: %v", id, err)
		}
		raftNodes = append(raftNodes, r)
		fsms = append(fsms, f)
	}

	// Connect all in-memory transports with each other.
	for i, t := range transports {
		for j, t2 := range transports {
			if i == j {
				continue
			}
			t.Connect(addresses[j], t2)
		}
	}

	// Bootstrap the cluster using the first node.
	configuration := raft.Configuration{
		Servers: []raft.Server{},
	}
	for i, addr := range addresses {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      raft.ServerID(nodeIDs[i]),
			Address: addr,
		})
	}
	bootstrapFuture := raftNodes[0].BootstrapCluster(configuration)
	if err := bootstrapFuture.Error(); err != nil && err != raft.ErrCantBootstrap {
		log.Fatalf("failed to bootstrap cluster: %v", err)
	}

	// Wait a moment for election to settle.
	time.Sleep(2 * time.Second)

	// Start an HTTP server to handle client requests.
	http.HandleFunc("/command", commandHandler)
	http.HandleFunc("/leader", leaderHandler)
	http.HandleFunc("/stop", stopNodeHandler)
	http.HandleFunc("/start", startNodeHandler) // Add this line
	log.Println("Server is listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// commandHandler forwards write commands to the leader and serves get requests.
func commandHandler(w http.ResponseWriter, r *http.Request) {
	// Decode the incoming JSON command.
	var cmd command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Determine the leader.
	leader := getLeader(raftNodes)
	if leader == nil {
		http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		return
	}

	switch cmd.Op {
	case "set":
		// For writes, marshal the command and apply it to the leader.
		data, err := json.Marshal(cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		applyFuture := leader.Apply(data, 5*time.Second)
		if err := applyFuture.Error(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("set successful"))

	case "get":
		// For reads, read from the leader's FSM state.
		// (In a real system, you might also allow reads from followers with a read index.)
		var leaderFSM *fsm
		for i, r := range raftNodes {
			if r == leader {
				leaderFSM = fsms[i]
				break
			}
		}
		if leaderFSM == nil {
			http.Error(w, "leader FSM not found", http.StatusInternalServerError)
			return
		}
		leaderFSM.mu.Lock()
		value, ok := leaderFSM.store[cmd.Key]
		leaderFSM.mu.Unlock()
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"key": cmd.Key, "value": value})
	default:
		http.Error(w, "unknown operation", http.StatusBadRequest)
	}
}

// Add this new handler function
func leaderHandler(w http.ResponseWriter, r *http.Request) {
	leader := getLeader(raftNodes)
	if leader == nil {
		http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		return
	}

	leaderID := string(leader.LastContact().String())
	for i, node := range raftNodes {
		if node == leader {
			leaderID = nodeIDs[i]
			break
		}
	}

	json.NewEncoder(w).Encode(map[string]string{
		"leader": leaderID,
		"state":  leader.State().String(),
	})
}

// Add this helper function before stopNodeHandler
func countRunningNodes() int {
	count := 0
	for _, state := range nodeState {
		if state {
			count++
		}
	}
	return count
}

// Replace the existing stopNodeHandler
func stopNodeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		NodeID string `json:"node_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	nodeIndex := -1
	for i, id := range nodeIDs {
		if id == req.NodeID {
			nodeIndex = i
			break
		}
	}

	if nodeIndex == -1 {
		http.Error(w, "Node not found", http.StatusNotFound)
		return
	}

	if !nodeState[nodeIndex] {
		http.Error(w, "Node already stopped", http.StatusBadRequest)
		return
	}

	// Check if stopping this node would exceed the limit
	runningNodes := countRunningNodes()
	if runningNodes <= 3 { // 3 is the minimum number of nodes we want to keep running
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Cannot stop more than 2 nodes. At least 3 nodes must be running.",
		})
		return
	}

	// Take a snapshot before shutting down
	raftNode := raftNodes[nodeIndex]
	snapFuture := raftNode.Snapshot()
	if err := snapFuture.Error(); err != nil {
		log.Printf("Warning: failed to create snapshot: %v", err)
	}

	// Wait for snapshot to complete
	time.Sleep(1 * time.Second)

	// Shutdown the node
	if err := raftNode.Shutdown().Error(); err != nil {
		http.Error(w, "Failed to stop node", http.StatusInternalServerError)
		return
	}

	nodeState[nodeIndex] = false
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Node stopped successfully",
		"node_id": req.NodeID,
	})
}

// Add this new handler function at the end of the file
func startNodeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		NodeID string `json:"node_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid request body",
		})
		return
	}

	nodeIndex := -1
	for i, id := range nodeIDs {
		if id == req.NodeID {
			nodeIndex = i
			break
		}
	}

	if nodeIndex == -1 {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Node not found",
		})
		return
	}

	if nodeState[nodeIndex] {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Node is already running",
		})
		return
	}

	// Check if we have enough running nodes for a quorum
	runningNodes := 0
	for _, state := range nodeState {
		if state {
			runningNodes++
		}
	}

	if runningNodes < (len(nodeIDs)/2 + 1) {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Not enough running nodes to form a quorum. Please start more nodes first.",
		})
		return
	}

	// Create new transport
	addr, trans := raft.NewInmemTransport("")

	// Connect transport with all other nodes
	for i, t := range transports {
		if i != nodeIndex && nodeState[i] {
			trans.Connect(addresses[i], t)
			t.Connect(addr, trans)
		}
	}

	// Create new Raft node
	node, f, err := createRaftNode(nodeIDs[nodeIndex], trans, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("Failed to create node: %v", err),
		})
		return
	}

	// Update global state
	raftNodes[nodeIndex] = node
	fsms[nodeIndex] = f
	transports[nodeIndex] = trans
	addresses[nodeIndex] = addr
	nodeState[nodeIndex] = true

	// Add the node back to the cluster configuration
	leader := getLeader(raftNodes)
	if leader == nil {
		node.Shutdown()
		nodeState[nodeIndex] = false
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "No leader available",
		})
		return
	}

	future := leader.AddVoter(
		raft.ServerID(nodeIDs[nodeIndex]),
		addr,
		0,
		0,
	)

	if err := future.Error(); err != nil {
		node.Shutdown()
		nodeState[nodeIndex] = false
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("Failed to join cluster: %v", err),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Node started and joined cluster successfully",
		"node_id": req.NodeID,
	})
}
