# edge_node.py
import socket
import threading

class EdgeNode:
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.nodes = []  # Lista de nós regulares conectados

    def handle_client(self, conn, addr):
        print(f'Connected by {addr}')
        self.nodes.append(addr)  # Adiciona o nó à lista de nós conectados
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            print(f'Received from {addr}: {data}')
        conn.close()
        self.nodes.remove(addr)
        print(f'Disconnected by {addr}')

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
