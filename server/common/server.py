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
        self._server_socket.settimeout(1)

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
            threading.Thread(target=self.__handle_client_connection, args=(client_sock,)).start()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            bets = []
            bet = self.__receive_message(client_sock)

            if bet:
                addr = client_sock.getpeername()
                bets.append(bet)
                logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {bet}')

            if bets:
                store_bets(bets)
                logging.info("action: apuesta_almacenada | result: success | bets_count: %d", len(bets))

        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __receive_message(self, client_sock):
        bet_fields = ['agency', 'first_name', 'last_name', 'document', 'birthdate', 'number']
        bet_values = {}
        buffer = b''

        while bet_fields:
            data = client_sock.recv(1024)
            buffer += data

            while len(buffer) >= U8_SIZE:
                len_data = int.from_bytes(buffer[:U8_SIZE], byteorder='big')
                buffer = buffer[U8_SIZE:]

                if len(buffer) < len_data:
                    break

                field_data, buffer = buffer[:len_data], buffer[len_data:]
                field_name = bet_fields.pop(0) 
                bet_values[field_name] = field_data.decode('utf-8')
        
        return Bet(**bet_values)


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
