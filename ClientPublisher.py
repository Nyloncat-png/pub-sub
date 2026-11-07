import socket, sys

if len(sys.argv) != 4:
    print("Try: python3 ClientPublisher.py <IP_Broker> <Topic> <Message>")
    sys.exit(1)

host = sys.argv[1]
port = 6767

topic = sys.argv[2]
message = sys.argv[3]
format = f"pub:{topic}:{message}\n"

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((host, port))

try:
    sock.sendall(format.encode())
except:
    pass