import socket
import logging
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EdgeNode:
    def __init__(self, host='127.0.0.1', port=5000):
        self.host = host
        self.port = port
        self.nodes = {}  # {address: {filename: checksum}}
        self.file_directory = {}  # {filename: (node_address, node_port)}

    def start(self): # Inicia o socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            logging.info(f'Servidor iniciado em {self.host}:{self.port}')
            while True:
                conn, addr = s.accept() # recebe os parâmentros de conexão e o endereço da porta
                logging.info(f'Conectado por {addr}')
                threading.Thread(target=self.handle_client, args=(conn, addr)).start() # instancia a função com os valores recebidos e a inicializa numa thread

    def handle_client(self, conn, addr): # Função que lida com a requisição do cliente e usa tratamento de erros para evitar encerramento do programa
        try:
            while True:
                data = conn.recv(1024).decode()
                if not data:
                    break
                command, *params = data.split()
                if command == 'UPDATE':
                    self.update_node(addr, params)
                elif command == 'REQUEST':
                    self.handle_request(conn, params)
                elif command == 'GET':
                    self.send_all_files_list(conn)
        except ConnectionResetError as e:
            logging.error(f'ERRO: {e}. Conexão fechada por {addr}')
        except Exception as e:
            logging.error(f'Erro ao lidar com o cliente {addr}: {e}')
        finally:
            conn.close()
            logging.info(f'Desconectado por {addr}')

    def update_node(self, addr, params):
        files = {params[i]: params[i + 1] for i in range(0, len(params), 2)}
        self.nodes[addr] = files
        for filename, checksum in files.items():
            self.file_directory[filename] = (addr[0], addr[1])
        logging.info(f'Nó atualizado {addr}: {files}')

    def handle_request(self, conn, params): #Função que lida com as requisições de arquivos
        filename = params[0]
        if filename in self.file_directory:
            node_host, node_port = self.file_directory[filename]
            conn.sendall(f'FOUND {node_host} {node_port}'.encode())
            logging.info(f'Arquivo {filename} encontrado em {node_host}:{node_port}')
        else:
            conn.sendall(b'NOT FOUND')
            logging.info(f'Arquivo {filename} não encontrado na rede')

    def send_all_files_list(self, conn): # Função de listagem e evnio de todos os arquivos presentes na rede
        all_files = {}
        for addr, files in self.nodes.items():
            all_files[addr] = files
        conn.sendall(str(all_files).encode())
        logging.info(f'Lista de arquivos enviada para {conn.getpeername()}')

if __name__ == '__main__':
    edge_node = EdgeNode()
    edge_node.start()
