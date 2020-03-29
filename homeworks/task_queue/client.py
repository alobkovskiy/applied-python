import socket
import time

sock = socket.socket()
sock.connect(('127.0.0.1', 5555))

sock.send(b'ADD 1 5 12345')
data = sock.recv(1024)
print(data)

sock.send(b'ADD 1 3 123')
data = sock.recv(1024)
print(data)

sock.send(b'IN 1 4')
data = sock.recv(1024)
print(data)

sock.send(b'IN 1 2')
data = sock.recv(1024)
print(data)

sock.send(b'GET 1')
data = sock.recv(1024)
print(data)

sock.send(b'ACK 1 2')
data = sock.recv(1024)
print(data)



sock.send(b'stop\n')
data = sock.recv(1024)
print(data)

sock.close()

