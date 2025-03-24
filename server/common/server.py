import socket
import logging
import signal
import sys

from common.utils import store_bets, Bet, load_bets, has_won

U8_SIZE = 1

class Server:
    def __init__(self, port, listen_backlog, client_count):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._client_sockets = []
        self._server_socket.settimeout(1)
        self._clients_ready = 0
        self._total_clients = client_count
        self._lottery_winners = {}

        # Handle SIGTERM signal
        signal.signal(signal.SIGTERM, self.handle_signal)

    def run(self):
        """
        Server that accepts new connections and establishes a communication with a client.
        After communication finishes, the server starts to accept new connections again.
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
        Reads and processes messages from a specific client socket until it receives the last batch.
        The connection is then closed after receiving all the bets and an acknowledgment is sent.
        """
        addr = client_sock.getpeername()

        while True:
            try:
                batch, is_last_batch = self.__receive_batch(client_sock)
                if len(batch) == 0:
                    logging.info(f'action: finish_batches_reading | result: success | ip: {addr[0]}')
                    break

                store_bets(batch)
                self.__send_ack(client_sock, True)
                logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(batch)}')

                if is_last_batch:
                    logging.info("action: last_batch_received | result: success")
                    break

            except Exception as e:
                logging.error(f'action: apuesta_recibida | result: fail | error: {e}')
                self.__send_ack(client_sock, False)
                break

        # Wait for the notification from the client that it is done sending bets
        self.__wait_for_finish(client_sock)


    def __wait_for_finish(self, client_sock):
        """
        Wait for the notification from the client that it has finished sending all bets.
        This method listens for a 1-byte packet (NotifyBetsEnd) to signal the completion.
        """
        try:
            # Receive a 1-byte packet indicating the client has finished
            notify_packet = client_sock.recv(1) 
            if notify_packet == b'\x01':  # Check if the client sent the "finished" notification
                logging.info(f'action: client_finish_notify | result: success | client_ip: {client_sock.getpeername()[0]}')
                self._clients_ready += 1

                # If all clients have finished, perform the lottery
                if self._clients_ready >= self._total_clients:  
                    self.__perform_lottery()

                    for client_sock in self._client_sockets:
                        self.__send_ack(client_sock, True)
                        self.__handle_winner_queries(client_sock)
            else:
                logging.error(f'action: client_finish_notify | result: fail | invalid packet received | client_ip: {client_sock.getpeername()[0]}')
        except Exception as e:
            logging.error(f'action: client_finish_notify | result: fail | error: {e}')
            return
        
    def __perform_lottery(self):
        """
        Perform the lottery and identify the winners for each agency.
        This function uses load_bets() and has_won() functions to determine the winners.
        """
        logging.info("action: performing_lottery | result: in_progress")

        all_bets = load_bets()

        winners = {}
        for bet in all_bets:
            if has_won(bet):
                if bet.agency not in winners:
                    winners[bet.agency] = []
                winners[bet.agency].append(bet.document)

        self._lottery_winners = winners
        logging.info("action: sorteo | result: success") 

    def __send_winners(self, client_sock, agency_id):
        """
        Sends the winners of the lottery to the corresponding client (agency).
        If no winners are found, it sends an empty array instead of an error message.
        """
        winners = self._lottery_winners.get(agency_id, [])
        
        winners_message = ",".join(winners) if winners else "0"
        
        try:
            client_sock.send(winners_message.encode())
            logging.info(f"action: send_winners | result: success | agencia: {agency_id} | ganadores: {winners_message}")
        except OSError as e:
            logging.error(f'action: send_winners | result: fail | agency: {agency_id} | error: {e}')


    def __handle_winner_queries(self, client_sock):
        """
        This method listens for queries from clients asking for the winners of the lottery.
        When it receives a query request, it sends the winners of that client's agency.
        """
        try:
            query_packet = client_sock.recv(2)  # Expecting 2 bytes for the query
            if query_packet[0] == 0x02:  # Query for winners
                agency_id = query_packet[1]
                logging.info(f'action: query_winners | result: success | client_ip: {client_sock.getpeername()[0]} | agency_id: {agency_id}')
                self.__send_winners(client_sock, agency_id)
            else:
                logging.error(f'action: query_winners | result: fail | invalid query packet | client_ip: {client_sock.getpeername()[0]}')

        except Exception as e:
            logging.error(f'action: query_winners | result: fail | error: {e}')
        finally:
            client_sock.close()


    def __receive_batch(self, client_sock):
        """
        Receives a batch of serialized bets from a client and deserializes them into a list of Bet objects.
        This function ensures that all the data for a single batch is received from the client, even if it is sent
        in multiple chunks (short reads). The method accumulates the data in a buffer and processes it once the
        full batch is received.
        """
        bet_fields = ['agency', 'first_name', 'last_name', 'document', 'birthdate', 'number']
        bet_values = {}
        buffer = b''  
        batch = []

        # Read the first byte of the batch to check if it's the last batch (0x01 means last, 0x00 means not last)
        first_byte = client_sock.recv(1)
        if first_byte:
            is_last_batch = first_byte == b'\x01'
        else:
            is_last_batch = False

        while True:
            data = client_sock.recv(8192)  # max 8kB
            if not data:
                break  # No more data

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

        return batch, is_last_batch

    def __send_ack(self, client_sock, success=True):
        """
        Sends an acknowledgment (ACK) message to client.
        This method sends a message containing the value `1` (success) or `0` (failure) to the client,
        indicating whether the batch was successfully processed.
        """
        try:
            ack_message = b'\x01' if success else b'\x00'
            client_sock.send(ack_message)
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
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
