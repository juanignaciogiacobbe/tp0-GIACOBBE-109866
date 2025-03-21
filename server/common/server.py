import socket
import logging
import signal
import sys
import threading

from common.utils import store_bets, Bet

U8_SIZE = 1

class Bet:
    def __init__(self, agency, first_name, last_name, document, birthdate, number):
        self.agency = agency
        self.first_name = first_name
        self.last_name = last_name
        self.document = document
        self.birthdate = birthdate
        self.number = number

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._client_sockets = []
        self._server_socket.settimeout(0.1)

        # Handle SIGTERM signal
        signal.signal(signal.SIGTERM, self.handle_signal)

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        while True:
            try:
                client_sock = self.__accept_new_connection()
            except socket.timeout:
                continue
            
                
            self._client_sockets.append(client_sock)
            # threading.Thread(target=self.__handle_client_connection, args=(client_sock,)).start()
            self.__handle_client_connection(client_sock)
    
    def __handle_client_connection(self, client_sock):
        buffer = b''
        read = 0
        processed_batch_size = False
        batch_size = 0
        total_bets_received = 0

        while True:
            try:
                data = client_sock.recv(8192 - len(buffer))
                if not data:
                    logging.info("action: receive_message | result: finished connection")
                    break
            except (BrokenPipeError, TimeoutError) as e:
                logging.info("action: receive_message | result: finished connection | error: %s", e)
                break 
            
            buffer += data
            read += len(data)

            # Process batch size if not done already
            if not processed_batch_size and len(buffer) >= 16:
                batch_size = int.from_bytes(buffer[:16], byteorder='big')
                buffer = buffer[16:]
                read -= 16
                processed_batch_size = True

            # Wait until the whole batch is received
            if read < batch_size:
                continue

            # process chunk
            try:
                batch, buffer = self.parse_batch(buffer, batch_size)
                total_bets_received += len(batch)
                store_bets(batch)

                logging.info("action: apuesta_recibida | result: success | cantidad: %d", len(batch))

                ok = 1
                client_sock.sendall(ok.to_bytes(1, byteorder='big'))
            except Exception as e:
                logging.error("action: apuesta_recibida | result: fail | cantidad: %d | error: %s", len(batch), e)
                try:
                    error = 0
                    client_sock.sendall(error.to_bytes(1, byteorder='big'))
                except BrokenPipeError:
                    logging.error("action: send_message | result: finished connection")
                    return
            
            processed_batch_size = False
            read = 0
        
        logging.info("action: connection_finished | result: success | total_bets_received: %d", total_bets_received)

    def parse_batch(self, buffer, batch_size):
        batch = []

        while batch_size > 0:
            bet_fields = ['agency', 'first_name', 'last_name', 'document', 'birthdate', 'number']
            bet_values = {}

            while bet_fields:
                if len(buffer) >= 8:
                    len_data = int.from_bytes(buffer[:U8_SIZE], byteorder='big')
                    buffer = buffer[U8_SIZE:]
                    batch_size -= 8

                    field_data, buffer = buffer[:len_data], buffer[len_data:]
                    field_name = bet_fields.pop(0) 
                    bet_values[field_name] = field_data.decode('utf-8')

            try:
                batch.append(Bet(**bet_values))
            except TypeError as e:
                logging.info("action: apuesta_recibida | result: fail | cantidad: %d", len(batch))
                return e
            
        logging.info("action: apuesta_recibida | result: success | cantidad: %d", len(batch))
        return batch, buffer


    def handle_signal(self, signum, frame):
        logging.info('action: close_clients_conn | result: in_progress')

        for client_socket in self._client_sockets:
            client_socket.close()
            logging.info('action: close_client_conn | result: success')

        self._server_socket.close()
        logging.info('action: close_server_socket | result: success')
        sys.exit(0)

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
