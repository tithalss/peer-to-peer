import socket
import os
import hashlib
import threading
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RegularNode:
    def __init__(self, edge_host='127.0.0.1', edge_port=5000, port=0, directory='.'):
        self.edge_host = edge_host
        self.edge_port = edge_port
        self.port = port
        self.directory = directory  # diretório local onde os arquivos estão
        self.files = self.get_files()  # lista inicial de arquivos com checksums

    def get_files(self):
        files = {}
        for file_name in os.listdir(self.directory):
            file_path = os.path.join(self.directory, file_name)
            if os.path.isfile(file_path):
                with open(file_path, 'rb') as f:
                    checksum = hashlib.md5(f.read()).hexdigest()
                    files[file_name] = checksum
        return files

    def connect_to_edge_node(self):
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.edge_host, self.edge_port))
                    logging.info(f'Connected to edge node at {self.edge_host}:{self.edge_port}')
                    while True:
                        update_message = 'UPDATE ' + ' '.join(f'{k} {v}' for k, v in self.get_files().items())
                        s.sendall(update_message.encode())
                        logging.info(f'Sent update to edge node: {update_message}')
                        time.sleep(60)  # envia atualizações a cada 60 segundos
            except Exception as e:
                logging.error(f'Error connecting to edge node: {e}. Retrying in 10 seconds...')
                time.sleep(10)

    def download_file_from_peer(self, file_name):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.edge_host, self.edge_port))
                s.sendall(f'REQUEST {file_name}'.encode())
                response = s.recv(1024).decode()
                if response.startswith('FOUND'):
                    parts = response.split()
                    node_host = parts[1]
                    node_port = int(parts[2])
                    logging.info(f'File {file_name} found at {node_host}:{node_port}. Downloading...')
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as download_socket:
                        download_socket.connect((node_host, node_port))
                        download_socket.sendall(f'GET {file_name}'.encode())
                        with open(os.path.join(self.directory, file_name), 'wb') as f:
                            while True:
                                data = download_socket.recv(1024)
                                if not data:
                                    break
                                f.write(data)
                            logging.info(f'File {file_name} downloaded successfully')
                else:
                    logging.info(f'File {file_name} not found on the network')
        except Exception as e:
            logging.error(f'Error downloading file {file_name}: {e}')

    def input_handler(self):
        while True:
            file_name = input("Digite o nome do arquivo que deseja baixar (ou 'exit' para sair): ")
            if file_name.lower() == 'exit':
                break
            elif file_name in self.files:
                logging.info(f'O arquivo {file_name} já está presente localmente.')
            else:
                logging.info(f'Procurando pelo arquivo {file_name} na rede...')
                self.download_file_from_peer(file_name)

    def start(self):
        threading.Thread(target=self.connect_to_edge_node, daemon=True).start()
        threading.Thread(target=self.input_handler, daemon=True).start()

if __name__ == '__main__':
    directory = os.path.abspath(os.path.dirname(__file__))  # ajuste para o caminho do diretório do nó regular
    regular_node = RegularNode(edge_host='localhost', edge_port=5000, directory=directory)
    regular_node.start()

    # Mantenha o programa em execução para continuar conectado ao nó de borda e aceitar inputs do usuário
    while True:
        time.sleep(1)
