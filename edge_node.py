import socket
import threading
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EdgeNode:
    def __init__(self, host='127.0.0.1', port=5000):
        self.host = host
        self.port = port
        self.nodes = {}  # {address: {filename: checksum}}

    def handle_client(self, conn, addr):
        logging.info(f'Connected by {addr}')
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            command, *params = data.split()
            if command == 'UPDATE':
                self.update_node(addr, params)
            elif command == 'REQUEST':
                self.handle_request(conn, params)
            elif command == 'DOWNLOAD_ALL':
                self.send_all_files_list(conn)
        conn.close()
        logging.info(f'Disconnected by {addr}')

    def update_node(self, addr, params):
        files = {params[i]: params[i + 1] for i in range(0, len(params), 2)}
        self.nodes[addr] = files
        logging.info(f'Updated node {addr}: {files}')

    def handle_request(self, conn, params):
        filename = params[0]
        for addr, files in self.nodes.items():
            if filename in files:
                conn.sendall(f'FOUND {addr[0]} {addr[1]}'.encode())
                logging.info(f'File {filename} found at {addr[0]}:{addr[1]}')
                return
        conn.sendall(b'NOT FOUND')
        logging.info(f'File {filename} not found on the network')

    def send_all_files_list(self, conn):
        all_files = {}
        for addr, files in self.nodes.items():
            all_files[addr] = files
        conn.sendall(str(all_files).encode())
        logging.info(f'Sent all files list to {conn.getpeername()}')

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            logging.info(f'Server started at {self.host}:{self.port}')
            with ThreadPoolExecutor(max_workers=10) as executor:
                while True:
                    conn, addr = s.accept()
                    executor.submit(self.handle_client, conn, addr)

if __name__ == '__main__':
    edge_node = EdgeNode()
    edge_node.start()
