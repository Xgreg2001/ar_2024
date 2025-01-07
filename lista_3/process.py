import threading
from channel import Channel


class Process:
    def __init__(self, pid, verbose=False):
        self.pid = pid
        self.state = 0
        self.in_channels = {}
        self.out_channels = {}
        self.has_recorded_state = False
        self.recorded_state = 0
        self.marker_count = 0
        self.lock = threading.Lock()
        self.verbose = verbose

    def connect_to(self, other_process):
        from_channel = Channel()
        to_channel = Channel()
        self.out_channels[other_process.pid] = from_channel
        other_process.in_channels[self.pid] = from_channel
        other_process.out_channels[self.pid] = to_channel
        self.in_channels[other_process.pid] = to_channel

    def send_message(self, to_pid, message):
        if to_pid in self.out_channels:
            if self.verbose:
                print(f"Process {self.pid} sending message: {message}")
            self.out_channels[to_pid].send(message)

    def receive_message(self, from_pid):
        if from_pid in self.in_channels:
            msg = self.in_channels[from_pid].receive()

            if msg == "MARKER":
                self.handle_marker(from_pid)
                if self.verbose:
                    print(
                        f"Process {self.pid} received MARKER from {from_pid}")
            elif msg is not None:
                if self.verbose:
                    print(
                        f"Process {self.pid} received message from {from_pid}: {msg}")
                self.handle_message(msg)
            return msg

    def handle_message(self, msg):
        with self.lock:
            self.state += 1
            if self.verbose:
                print(f"Process {self.pid} incremented state to {self.state}")

    def handle_marker(self, from_pid):
        with self.lock:
            if not self.has_recorded_state:
                self.record_local_state()
                for pid, ch in self.out_channels.items():
                    ch.send("MARKER")
                    if self.verbose:
                        print(f"Process {self.pid} sent MARKER to {pid}")
            self.marker_count += 1
            if self.marker_count == len(self.in_channels):
                for ch in self.in_channels.values():
                    ch.stop_recording()

    def record_local_state(self):
        self.has_recorded_state = True
        self.recorded_state = self.state
        for ch in self.in_channels.values():
            ch.start_recording()

    def update_state(self, amount):
        if self.verbose:
            print(f"Process {self.pid} updated state to {amount}")
        with self.lock:
            self.state = amount

    def take_snapshot(self):
        with self.lock:
            if not self.has_recorded_state:
                self.record_local_state()
                for pid, ch in self.out_channels.items():
                    ch.send("MARKER")
                    if self.verbose:
                        print(f"Process {self.pid} sent MARKER to {pid}")

    def get_snapshot_result(self):
        in_channels_state = {}
        for pid, ch in self.in_channels.items():
            in_channels_state[pid] = ch.get_recorded_messages()
        return {
            "pid": self.pid,
            "state": self.recorded_state,
            "in_channels": in_channels_state
        }

    def run_process_loop(self, processes, stop_event, send_interval=(0.5, 1.5)):
        import random
        import time
        while not stop_event.is_set():
            targets = list(processes.keys())
            targets.remove(self.pid)
            if targets:
                target_pid = random.choice(targets)
                self.send_message(target_pid, f"M_{self.pid}_to_{target_pid}")
            time.sleep(random.uniform(*send_interval))
            self.receive_from_all_in_channels()

    def receive_from_all_in_channels(self):
        for from_pid in list(self.in_channels.keys()):
            self.receive_message(from_pid)
