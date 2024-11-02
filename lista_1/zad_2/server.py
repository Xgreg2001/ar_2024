from concurrent import futures
import threading
import grpc
import queue
import chat_pb2
import chat_pb2_grpc


class ChatServer(chat_pb2_grpc.ChatServerServicer):
    def __init__(self):
        self.subscribers = []
        self.lock = threading.Lock()

    def ChatStream(self, request, context):
        # Create a queue for the new subscriber
        subscriber_queue = queue.Queue()
        with self.lock:
            self.subscribers.append(subscriber_queue)
        try:
            while True:
                note = subscriber_queue.get()
                yield note
        except:
            pass
        finally:
            with self.lock:
                self.subscribers.remove(subscriber_queue)

    def SendNote(self, request, context):
        # Broadcast the note to all subscribers
        with self.lock:
            subscribers_copy = list(self.subscribers)
        for subscriber_queue in subscribers_copy:
            subscriber_queue.put(request)
        return chat_pb2.Empty()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat_pb2_grpc.add_ChatServerServicer_to_server(ChatServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started on port 50051.")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
