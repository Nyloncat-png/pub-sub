import threading
import socket

topics = {} #{sport:[c1, c2, c3]}
topicsLock = threading.Lock()


class Broker(threading.Thread):
    def __init__(self, client, addr):
        threading.Thread.__init__(self)
        self.client = client
        self.address = addr
        self.is_subscriber = False

    def run(self):
        try:
            buffer = b''
            while b'\n' not in buffer:
                data = self.client.recv(1024)
                if not data:
                    raise Exception("Client disconnected")
                buffer += data

            response = buffer.decode().strip()
            # Subscriber
            if response.startswith("sub:"):
                topic = response[4:]
                with topicsLock:
                    if topic not in topics:
                        topics[topic] = []
                    topics[topic].append(self.client)

                self.is_subscriber = True
                while True:
                    if not self.client.recv(1024):
                        raise Exception("Client disconnected")

            # Publisher
            elif response.startswith("pub:"):
                parts = response.split(':', 2)
                dead_clients = []
                sub_clients = []
                if len(parts) == 3:
                    _, topic, message = parts
                    with topicsLock:
                        if topic in topics:
                            for sub_client in topics[topic]:
                                sub_clients.append(sub_client)
                        else:
                            try:
                                self.client.sendall(b'Invalid topic\n')
                            except:
                                pass
                            raise Exception("Invalid topic")
                    for sub_client in sub_clients:
                        try:
                            sub_client.sendall((message+"\n").encode())
                        except:
                            dead_clients.append(sub_client)
                    if dead_clients:
                        with topicsLock:
                            if topic in topics:
                                for dead_client in dead_clients:
                                    if dead_client in topics[topic]:
                                        topics[topic].remove(dead_client)
                                        try:
                                            dead_client.close()
                                        except:
                                            pass
                else:
                    try:
                        self.client.sendall(b'Invalid format\n')
                    except:
                        pass
            else:
                try:
                    self.client.sendall(b'Invalid message\n')
                except:
                    pass
        except Exception as e:
            print(f"Error: {e}")
        finally:
            if not self.is_subscriber:
                self.client.close()
            else:
                with topicsLock:
                    for t_name in list(topics.keys()):
                        if self.client in topics[t_name]:
                            topics[t_name].remove(self.client)
                self.client.close()


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
            client.daemon = True
            client.start()
    except KeyboardInterrupt:
        print("Shutting down")
    finally:
        server_socket.close()