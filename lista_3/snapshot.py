class SnapshotManager:
    def __init__(self, processes):
        self.processes = processes

    def initiate_snapshot(self, starter_pid):
        self.reset_all_processes()
        starter = self.processes[starter_pid]
        starter.take_snapshot()

    def reset_all_processes(self):
        for p in self.processes.values():
            p.has_recorded_state = False
            p.recorded_state = 0
            p.marker_count = 0
            for c in p.in_channels.values():
                c.stop_recording()
                c.clear_recorded_messages()

