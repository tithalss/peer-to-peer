# edge_node.py
import socket
import threading

class EdgeNode:
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.nodes = {}  # {address: {filename: checksum}}

    def handle_client(self, conn, addr):
        print(f'Connected by {addr}')
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            command, *params = data.split()
            if command == 'UPDATE':
                self.update_node(addr, params)
            elif command == 'REQUEST':
                self.handle_request(conn, params)
        conn.close()
        print(f'Disconnected by {addr}')

    def update_node(self, addr, params):
        files = {params[i]: params[i + 1] for i in range(0, len(params), 2)}
        self.nodes[addr] = files
        print(f'Updated node {addr}: {files}')

    def handle_request(self, conn, params):
        filename = params[0]
        for addr, files in self.nodes.items():
            if filename in files:
                conn.sendall(f'FOUND {addr[0]} {addr[1]}'.encode())
                return
        conn.sendall(b'NOT FOUND')

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f'Server started at {self.host}:{self.port}')
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()

if __name__ == '__main__':
    edge_node = EdgeNode()
    edge_node.start()
