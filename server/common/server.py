import socket
import logging
import signal
import multiprocessing
import sys

from common.utils import store_bets, Bet, load_bets, has_won

MAX_BATCH_SIZE_BYTES = 8192
QUERY_WINNERS_PACKET_SIZE_BYTES = 2
NOTIFY_PACKET_SIZE_BYTES = 1
CONTROL_BYTE_SIZE_BYTES = 1
U8_SIZE = 1

NOTIFY_PACKET_FLAG = b'\x01' 
QUERY_WINNERS_FLAG = 0x02
EMPTY_BATCH_FLAG = b'\x02'
LAST_BATCH_FLAG = b'\x01'
ACK_FAILED = b'\x00'
ACK_SUCCESS = b'\x01' 


class Server:
    def __init__(self, port, listen_backlog, client_count):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._client_sockets = []
        self._server_socket.settimeout(1)
        self._total_clients = client_count
        self._lottery_winners = {}

        self.manager = multiprocessing.Manager()
        self._lottery_winners = self.manager.dict()

        # Handle SIGTERM signal
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.locks = {
            'store_bets': self.manager.Lock(),
            'load_bets': self.manager.Lock()
        }

        self._barrier = multiprocessing.Barrier(client_count, action=self.__perform_lottery)
        self._lottery_done = multiprocessing.Event()
        self._terminated = multiprocessing.Event()

        self._processes = []

    def run(self):
        """
        Server that accepts new connections and establishes a communication with a client.
        After communication finishes, the server starts to accept new connections again.
        """
        while len(self._processes) < self._total_clients and not self._terminated.is_set():
            try:
                client_sock = self.__accept_new_connection()
            except socket.timeout:
                continue
            except OSError as e:
                if self._terminated.is_set():
                    break 
                else:
                    logging.error(f"action: accept_new_connection | result: fail | error: {e}")
                    continue

            self._client_sockets.append(client_sock)
            client_process = multiprocessing.Process(target=self.__handle_client_connection, args=(client_sock, ))
            self._processes.append(client_process)
            client_process.start()

        logging.info(f"action: all_processes_spawned | result: success | total_clients: {self._total_clients}")

        # Wait for all child processes to finish
        self.__cleanup_processes()


    def __handle_client_connection(self, client_sock):
        """
        Reads and processes messages from a specific client socket until it receives the last batch.
        The connection is then closed after receiving all the bets and an acknowledgment is sent.
        """
        def handle_sigterm(signum, frame):
            logging.warning("action: sigterm_received | result: processing")
            client_sock.close()
            logging.info("action: sigterm_received | result: sucess")

        signal.signal(signal.SIGTERM, handle_sigterm)
        addr = client_sock.getpeername()

        while True:
            try:
                batch, is_last_batch = self.__receive_batch(client_sock)
                if len(batch) == 0:
                    self.__send_ack(client_sock, True)
                    logging.info(f'action: finish_batches_reading | result: success | ip: {addr[0]}')
                    break

                with self.locks['store_bets']:
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

        self._lottery_done.wait()

        try:
            self.__send_ack(client_sock, True)
            self.__handle_winner_queries(client_sock)
        except OSError as e:
            logging.error(f'action: send_notification | result: fail | error: {e}')



    def __wait_for_finish(self, client_sock):
        """
        Wait for the notification from the client that it has finished sending all bets.
        This method listens for a 1-byte packet (NotifyBetsEnd) to signal the completion.
        """
        try:
            notify_packet = self.recv_exact(client_sock, NOTIFY_PACKET_SIZE_BYTES)

            if notify_packet == NOTIFY_PACKET_FLAG:
                logging.info(f'action: client_finish_notify | result: success | client_ip: {client_sock.getpeername()[0]}')
                self._barrier.wait()
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

        with self.locks['load_bets']:
            all_bets = load_bets()

        winners = {}
        for bet in all_bets:
            if has_won(bet):
                if bet.agency not in winners:
                    winners[bet.agency] = []
                winners[bet.agency].append(bet.document)

        self._lottery_winners.update(winners)
        logging.info("action: sorteo | result: success") 
        self._lottery_done.set()


    def __send_winners(self, client_sock, agency_id):
        """
        Sends the winners of the lottery to the corresponding client (agency).
        If no winners are found, it sends an empty array instead of an error message.
        """
        winners = self._lottery_winners.get(agency_id, [])
        
        winners_message = ",".join(winners) if winners else "0"
        encoded_message = winners_message.encode()

        total_sent = 0
        message_length = len(encoded_message)

        try:
            while total_sent < message_length:
                sent = client_sock.send(encoded_message[total_sent:])
                if sent == 0:
                    raise RuntimeError("socket connection broken")
                total_sent += sent
            logging.info(f"action: send_winners | result: success | agencia: {agency_id} | ganadores: {winners_message}")
        except OSError as e:
            logging.error(f'action: send_winners | result: fail | agency: {agency_id} | error: {e}')


    def __handle_winner_queries(self, client_sock):
        """
        This method listens for queries from clients asking for the winners of the lottery.
        When it receives a query request, it sends the winners of that client's agency.
        """
        try:
            query_packet = self.recv_exact(client_sock, QUERY_WINNERS_PACKET_SIZE_BYTES)

            if query_packet[0] == QUERY_WINNERS_FLAG:  
                agency_id = query_packet[1]
                logging.info(f'action: query_winners | result: success | client_ip: {client_sock.getpeername()[0]} | agency_id: {agency_id}')
                self.__send_winners(client_sock, agency_id)
            else:
                logging.error(f'action: query_winners | result: fail | invalid query packet | client_ip: {client_sock.getpeername()[0]}')

        except Exception as e:
            logging.error(f'action: query_winners | result: fail | error: {e}')

    def recv_exact(self, sock, num_bytes):
        data = b''
        while len(data) < num_bytes:
            chunk = sock.recv(num_bytes - len(data))
            if not chunk:
                raise ConnectionError("Connection closed or invalid data received")
            data += chunk
        return data


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
        control_byte = self.recv_exact(client_sock, CONTROL_BYTE_SIZE_BYTES)

        if control_byte == EMPTY_BATCH_FLAG:
            return [], False  
        
        if control_byte not in (b'\x00', LAST_BATCH_FLAG):
            raise ValueError(f"Invalid control byte received: {control_byte!r}")
        
        is_last_batch = control_byte == LAST_BATCH_FLAG

        while True:
            data = client_sock.recv(MAX_BATCH_SIZE_BYTES)  # max 8kB
            if not data:
                break

            buffer += data

            while True:
                if len(buffer) < U8_SIZE:
                    break

                len_data = int.from_bytes(buffer[:U8_SIZE], byteorder='big')

                if len(buffer) < U8_SIZE + len_data:
                    break

                buffer = buffer[U8_SIZE:]
                field_data, buffer = buffer[:len_data], buffer[len_data:]
                field_name = bet_fields.pop(0)
                bet_values[field_name] = field_data.decode('utf-8')

                if not bet_fields:
                    batch.append(Bet(**bet_values))
                    bet_values.clear()
                    bet_fields = ['agency', 'first_name', 'last_name', 'document', 'birthdate', 'number']


            if len(data) < MAX_BATCH_SIZE_BYTES:
                break

        return batch, is_last_batch

    def __send_ack(self, client_sock, success=True):
        """
        Sends an acknowledgment (ACK) message to the client.
        This method sends a message containing the value `1` (success) or `0` (failure) to the client,
        indicating whether the batch was successfully processed.
        """
        ack_message = ACK_SUCCESS if success else ACK_FAILED
        total_sent = 0
        message_len = len(ack_message)

        try:
            while total_sent < message_len:
                sent = client_sock.send(ack_message[total_sent:])
                if sent == 0:
                    raise RuntimeError("socket connection broken")
                total_sent += sent

            logging.info(f"action: send_ack | result: success | ack_message: {ack_message}")

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
        logging.info('action: sigterm_handling | result: in_progress')

        try:
            self._server_socket.close()
            logging.info("action: close_server_socket | result: success")
        except OSError as e:
            logging.error(f"action: close_server_socket | result: fail | error: {e}")

        for process in self._processes:
            if process.is_alive():
                process.terminate()
                logging.warning(f"action: terminate_process | result: success | pid: {process.pid}")
            
        self._terminated.set()


    def __cleanup_processes(self):
        for process in self._processes:
            process.join()
            logging.debug(f'action: join_process | result: success')

        for client_socket in self._client_sockets:
            try:
                ip = 'unknown'
                try:
                    ip = client_socket.getpeername()[0]
                except OSError:
                    pass

                client_socket.close()
                logging.info(f'action: close_client_conn | result: success | ip: {ip}')
            except OSError as e:
                logging.error(f'action: close_client_conn | result: fail | error: {e}')

        self._client_sockets.clear()

        try:
            self._server_socket.close()
            logging.info('action: close_server_socket | result: success')
        except OSError as e:
            logging.error(f'action: close_server_socket | result: fail | error: {e}')


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
