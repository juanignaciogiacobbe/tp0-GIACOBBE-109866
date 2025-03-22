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

        while True:
            try:
                batch = self.__receive_batch(client_sock)
                if len(batch) == 0:
                    logging.info(f'action: finish_batches_reading | result: success | ip: {addr[0]}')
                    break

                logging.info(f'action: receive_batch | result: success | ip: {addr[0]} | batch_len: {len(batch)}')

                store_bets(batch)
                self.__send_ack(client_sock)
                logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(batch)}')

            except Exception as e:
                logging.error(f'action: apuesta_recibida | result: fail | error: {e}')
                break

        client_sock.close()
    
    
    def __receive_batch(self, client_sock):
        """
        Receives a batch of serialized bets from a client and deserializes them into a list of Bet objects.

        This function ensures that all the data for a single batch is received from the client, even if it is sent
        in multiple chunks (short reads). The method accumulates the data in a buffer and processes it once the
        full batch is received.

        Args:
            client_sock (socket.socket): The client socket from which the batch data is received.

        Returns:
            list: A list of `Bet` objects deserialized from the incoming data.

        Raises:
            ValueError: If the received data cannot be properly parsed or if the expected format is not met.
        """
        bet_fields = ['agency', 'first_name', 'last_name', 'document', 'birthdate', 'number']
        bet_values = {}
        buffer = b''  
        batch = []

        while True:
            # Receive data in chunks (short read handling)
            data = client_sock.recv(8192)  # max 8kB
            if not data:
                break  

            buffer += data

            while len(buffer) >= U8_SIZE:
                len_data = int.from_bytes(buffer[:U8_SIZE], byteorder='big')
                buffer = buffer[U8_SIZE:]  

                if len(buffer) < len_data:
                    break  

                field_data, buffer = buffer[:len_data], buffer[len_data:]
                field_name = bet_fields.pop(0)
                bet_values[field_name] = field_data.decode('utf-8')

                if not bet_fields:
                    batch.append(Bet(**bet_values))
                    bet_values.clear()
                    bet_fields = ['agency', 'first_name', 'last_name', 'document', 'birthdate', 'number']

            if len(data) < 8192:
                break

        return batch


    def __send_ack(self, client_sock):
        """
        Sends an acknowledgment (ACK) message to client.

         This method sends a message containing the value `1` to the client, indicating that the
        bet has been successfully received and processed.

        Args:
            client_sock (socket.socket): El socket del cliente al que se le enviará el ACK.
        """
        try:
            ack_message = b'\x01' 
            client_sock.send(ack_message)
            logging.info(f'action: send_ack | result: success | ip: {client_sock.getpeername()[0]}')
        except OSError as e:
            logging.error(f'action: send_ack | result: fail | error: {e}')


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
