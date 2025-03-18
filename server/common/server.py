import socket
import logging
import signal
import sys
import threading

from common.utils import store_bets

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

            while True:
                msg = self.__receive_message(client_sock)
                if msg is None:
                    break  

                addr = client_sock.getpeername()
                bet = self.__process_bet(msg)
                if bet:
                    bets.append(bet)
                    logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {bet}')


                response = f"action: apuesta_enviada | result: success | dni: {bet.document} | numero: {bet.number}\n"
                self.__send_message(client_sock, response)

            if bets:
                store_bets(bets)
                logging.info("action: apuesta_almacenada | result: success | bets_count: %d", len(bets))

        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __receive_message(self, client_sock):
        msg = ''
        while True:
            chunk = client_sock.recv(1024).decode('utf-8')
            if not chunk:
                break
            msg += chunk
            if msg.endswith('\n'):
                break

        if not msg:
            return None

        message_parts = msg.strip().split('|')
        message_dict = {}

        for part in message_parts:
            if '=' in part:
                key, value = part.split('=')
                message_dict[key] = value
            else:
                logging.warning(f"action: receive_message | result: fail | error: invalid part format: {part}")
        
        return message_dict

    def __process_bet(self, msg):
        try:
            bet = Bet(
                agency="Agency X",
                first_name=msg['NOMBRE'],
                last_name=msg['APELLIDO'],
                document=msg['DOCUMENTO'],
                birthdate=msg['NACIMIENTO'],
                number=msg['NUMERO']
            )

            logging.info(f"action: bet_fields | result: success | first_name: {bet.first_name} | "
                     f"last_name: {bet.last_name} | document: {bet.document} | "
                     f"birthdate: {bet.birthdate} | number: {bet.number}")
            
            return bet
        except KeyError as e:
            logging.error(f"action: process_bet | result: fail | error: Missing key {e}")
            return None

    def __send_message(self, client_sock, message):
        total_written = 0
        message_len = len(message)
        while total_written < message_len:
            n = client_sock.send(message[total_written:].encode('utf-8'))
            total_written += n

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
