import threading
import socket

topics = ["/home/temperature", "/home/pressure", "/garden/humidity"]


class Broker(threading.Thread):
    def __init__(self, client, addr):
        threading.Thread.__init__(self)
        self.client = client
        self.address = addr

    def run(self):
        while True:
            pass


if __name__ == '__main__':
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)

    host = ""
    port = 6767
    server_socket.bind((host, port))

    server_socket.listen(5)
    print("Multithreaded Server started")

    try:
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Accepted connection from {addr}")
            client = Broker(client_socket, addr)
