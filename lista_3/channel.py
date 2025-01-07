import threading


class Channel:
    def __init__(self):
        self.queue = []
        self.recording = False
        self.recorded_messages = []
        self.lock = threading.Lock()

    def send(self, message):
        with self.lock:
            self.queue.append(message)

    def receive(self):
        with self.lock:
            if self.queue:
                msg = self.queue.pop(0)
                if self.recording and msg != "MARKER":
                    self.recorded_messages.append(msg)
                return msg
        return None

    def start_recording(self):
        self.recording = True

    def stop_recording(self):
        self.recording = False

    def get_recorded_messages(self):
        with self.lock:
            return list(self.recorded_messages)

    def clear_recorded_messages(self):
        with self.lock:
            self.recorded_messages.clear()

