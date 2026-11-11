import socket, sys

if len(sys.argv) != 3:
    print("Try: python3 ClientPublisher.py <IP_Broker> <Topic>")
    sys.exit(1)

host = sys.argv[1]
port = 6767

topic = sys.argv[2]
format = f"sub:{topic}\n"

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((host, port))

try:
    sock.sendall(format.encode())

    while True:
        try:
            buffer = b''
            while b'\n' not in buffer:
                data = sock.recv(1024)
                if not data:
                    raise Exception("Client disconnected")
                buffer += data
            print(buffer.decode().strip())
        except Exception as e:
            print(f"Error: {e}")
            break
        except KeyboardInterrupt:
            print("Closing")
            break
except Exception as e:
    print(f"Error: {e}")
finally:
    sock.close()