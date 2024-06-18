import socket
import os
import hashlib
import threading
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RegularNode:
    def __init__(self, edge_host='127.0.0.1', edge_port=5000, port=6060, directory=os.path.abspath(os.path.dirname(__file__))):
        self.edge_host = edge_host # host
        self.edge_port = edge_port # porta do nó de borda
        self.port = port  # porta de listen do nó
        self.directory = directory  # diretório local onde os arquivos estão
        self.files = self.get_files()  # lista inicial de arquivos com checksums
        self.server_socket = None  # socket para aceitar conexões de outros nós regulares

    def get_files(self): # Função de aquisição que retornar os arquivos presentes no diretório + checksum utilizando a biblioteca 'hashlib' para validar os dados
        files = {}
        for file_name in os.listdir(self.directory):
            file_path = os.path.join(self.directory, file_name)
            if os.path.isfile(file_path):
                with open(file_path, 'rb') as f:
                    checksum = hashlib.md5(f.read()).hexdigest()
                    files[file_name] = checksum
        return files
    
    def start(self): # Cria duas threads, uma para conexão com o nó de borda e outra para a função de input do usuário
        threading.Thread(target=self.connect_to_edge_node, daemon=True).start()
        threading.Thread(target=self.input_handler, daemon=True).start()

    def connect_to_edge_node(self): # Função de conexão do nó regular com o de borda
        self.start_server() # inicia o bind/listen do nó regular para a troca de arquivos
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.edge_host, self.edge_port)) # conecta ao nó de borda
                    logging.info(f'Conectado ao nó de borda em {self.edge_host}:{self.edge_port}')
                    update_message = 'UPDATE ' + ' '.join(f'{k} {v}' for k, v in self.get_files().items()) # mensagem de update dos arquivos
                    s.sendall(update_message.encode())
                    logging.info(f'Atualização para o nó de borda enviada: {update_message}')
                    time.sleep(30)
            except Exception as e:
                logging.error(f'Erro ao se conectar com o nó de borda: {e}. Tentando novamente em 5 segundos...')
                time.sleep(5)

    def start_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('0.0.0.0', self.port))  # Bind para todos os endereços disponíveis
        self.server_socket.listen(5)
        logging.info(f'Server socket listening for regular nodes on port {self.port}')
        
        threading.Thread(target=self.accept_connections, daemon=True).start()

    def accept_connections(self):
        while True:
            try:
                conn, addr = self.server_socket.accept() # aceitação de conexão de outros nós
                logging.info(f'Conexão aceita de outro nó em {addr}')
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()
            except Exception as e:
                logging.error(f'Erro em receber conexão de outro nó: {e}')
    
    def handle_client(self, conn, addr):
        try:
            # Recebe o comando do cliente (por exemplo, 'GET example.txt')
            request = conn.recv(1024).decode().strip()
            command, filename = request.split(' ', 1)
            
            if command == 'GET' and filename in self.files:
                file_path = os.path.join(self.directory, filename)
                with open(file_path, 'rb') as f:
                    data = f.read(1024)
                    while data:
                        conn.sendall(data)
                        data = f.read(1024)
                logging.info(f'File {filename} sent to {addr}')
            else:
                conn.sendall(b'NOT FOUND')
                logging.info(f'Arquivo {filename} não encontrado na rede')
            
        except Exception as e:
            logging.error(f'Erro ao lidar com a requisição do cliente {addr}: {e}')
        
        finally:
            conn.close()

    def input_handler(self):
        while True:
            file_name = input("Digite o nome do arquivo que deseja baixar:\n")
            if file_name in self.files:
                logging.info(f'O arquivo {file_name} já está presente localmente.')
            else:
                logging.info(f'Solicitando o arquivo {file_name} a um nó regular...')
                self.download_file_from_peer(file_name)

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
                    logging.info(f'File {file_name} encontrado em {node_host}:{node_port}. Baixando...')
                    
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as download_socket:
                        download_socket.connect((node_host, 8080))
                        download_socket.sendall(f'GET {file_name}'.encode())
                        
                        file_path = os.path.join(self.directory, file_name)
                        with open(file_path, 'wb') as f:
                            while True:
                                data = download_socket.recv(1024)
                                if not data:
                                    break
                                f.write(data)
                            
                            logging.info(f'Arquivo {file_name} baixado com sucesso {file_path}')
                
                else:
                    logging.info(f'Arquivo {file_name} não encontrado na rede')
        
        except Exception as e:
            logging.error(f'Erro ao baixar o arquivo {file_name}: {e}')

if __name__ == '__main__':
    regular_node = RegularNode()
    regular_node.start()

    # Mantenha o programa em execução para continuar conectado ao nó de borda e aceitar inputs do usuário
    while True:
        time.sleep(1)
