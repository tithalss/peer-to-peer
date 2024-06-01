import socket
import os
import hashlib
import threading
import time

class RegularNode:
    def __init__(self, edge_host='localhost', edge_port=5000, port=0):
        self.edge_host = edge_host
        self.edge_port = edge_port
        self.port = port
        self.files = self.get_files()

    def get_files(self):
        files = {}
        for file in os.listdir('.'):
            if os.path.isfile(file):
                with open(file, 'rb') as f:
                    checksum = hashlib.md5(f.read()).hexdigest()
                    files[file] = checksum
        return files

    def connect_to_edge_node(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.edge_host, self.edge_port))
            print(f'Connected to edge node at {self.edge_host}:{self.edge_port}')
            update_message = 'UPDATE ' + ' '.join(f'{k} {v}' for k, v in self.files.items())
            s.sendall(update_message.encode())

            # Manter a conexão ativa
            while True:
                time.sleep(60)  # Espera para enviar atualizações periódicas
                update_message = 'UPDATE ' + ' '.join(f'{k} {v}' for k, v in self.get_files().items())
                s.sendall(update_message.encode())

    def request_file(self, filename):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.edge_host, self.edge_port))
            request_message = f'REQUEST {filename}'
            s.sendall(request_message.encode())
            response = s.recv(1024).decode()
            if response.startswith('FOUND'):
                _, ip, port = response.split()
                self.download_file(ip, int(port), filename)
            else:
                print('File not found on the network')

    def download_file(self, ip, port, filename):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip, port))
            s.sendall(f'GET {filename}'.encode())
            with open(f'downloaded_{filename}', 'wb') as f:
                while True:
                    data = s.recv(1024)
                    if not data:
                        break
                    f.write(data)
            print(f'File {filename} downloaded successfully')

    def serve_files(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', self.port))
            s.listen()
            self.port = s.getsockname()[1]
            print(f'Regular node serving files at port {self.port}')
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_request, args=(conn,), daemon=True).start()

    def handle_request(self, conn):
        with conn:
            data = conn.recv(1024).decode()
            command, filename = data.split()
            if command == 'GET' and filename in self.files:
                with open(filename, 'rb') as f:
                    conn.sendfile(f)

    def start(self):
        threading.Thread(target=self.serve_files, daemon=True).start()
        self.connect_to_edge_node()

if __name__ == '__main__':
    regular_node = RegularNode()
    regular_node.start()
    # Request a file for testing purposes
    threading.Thread(target=regular_node.request_file, args=('example.csv',), daemon=True).start()
