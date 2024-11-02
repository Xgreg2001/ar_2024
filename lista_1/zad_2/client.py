import threading
import grpc
import chat_pb2
import chat_pb2_grpc


def receive_messages(stub):
    for note in stub.ChatStream(chat_pb2.Empty()):
        print(f"[{note.name}] {note.message}")


def main():
    channel = grpc.insecure_channel('localhost:50051')
    stub = chat_pb2_grpc.ChatServerStub(channel)
    name = input("Enter your name: ")

    # Start a thread to receive messages from the server
    threading.Thread(target=receive_messages,
                     args=(stub,), daemon=True).start()

    # Send messages entered by the user
    while True:
        message = input()
        if message.lower() == 'exit':
            break
        note = chat_pb2.Note()
        note.name = name
        note.message = message
        stub.SendNote(note)


if __name__ == '__main__':
    main()
