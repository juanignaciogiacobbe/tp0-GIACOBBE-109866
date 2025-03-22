import socket
import logging
import signal
import sys

from common.utils import store_bets, Bet

U8_SIZE = 1

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
            self.__handle_client_connection(client_sock)


    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        addr = client_sock.getpeername()

        try:
            bet = self.__receive_bet(client_sock)
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {bet}')

            store_bets([bet])
            logging.info(f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}')

        except OSError as e:
            logging.error(f'action: receive_message | result: fail | client_ip: {addr[0]} | error: {str(e)}')
        finally:
            client_sock.close()

    def __receive_bet(self, client_sock):
        """
        Receives a serialized bet from a client and deserializes it into a Bet object.

        This function reads data from the client socket in chunks, processes it to 
        extract the bet fields (agency, first_name, last_name, document, birthdate, number),
        and stores them in a dictionary. Once all the fields have been received, it 
        creates and returns a `Bet` object using the values from the dictionary.

        The function expects the data to be sent in a format where each field is preceded by 
        its length (encoded as an unsigned 8-bit integer). It uses this information to correctly 
        deserialize the data.

        Args:
            client_sock (socket.socket): The client socket from which the bet data is received.

        Returns:
            Bet: A `Bet` object containing the deserialized fields (agency, first_name, last_name, 
                document, birthdate, and number).

        Raises:
            ValueError: If the received data cannot be properly parsed or the expected format is not met.
        """
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
        """
        Gracefully handles the termination signal (SIGTERM).

        This method is called when the server receives a SIGTERM signal.
        It ensures that all client connections are closed properly and
        that the server socket is also closed. Any errors encountered
        during the closing of the sockets are logged. Once all resources
        have been closed, the server process exits.
        """
        
        logging.info('action: close_clients_conn | result: in_progress')

        for client_socket in self._client_sockets:
            try:
                client_socket.close()
                logging.info(f'action: close_client_conn | result: success | ip: {client_socket.getpeername()[0]}')
            except OSError as e:
                logging.error(f'action: close_client_conn | result: fail | error: {e}')


        try:
            self._server_socket.close()
            logging.info('action: close_server_socket | result: success')
        except OSError as e:
            logging.error(f'action: close_server_socket | result: fail | error: {e}')

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
