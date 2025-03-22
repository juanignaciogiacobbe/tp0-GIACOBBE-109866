package common

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration

	Nombre     string
	Apellido   string
	Documento  string
	Nacimiento string
	Numero     string
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}

	c.conn = conn
	return nil
}

// sendMessage sends a serialized `Bet` object to the server.
//
// This method creates a new `Bet` object based on the client's configuration, serializes it into bytes,
// and sends it to the server over the established TCP connection. It ensures that the entire message
// is written to the connection, handling any short writes by writing the message in multiple chunks if necessary.
//
// If an error occurs during the write operation, it logs the error and returns it. If the message is successfully sent,
// it logs the successful sending of the bet with the client's document and bet number.
//
// Returns:
//   - `nil` if the message was successfully sent.
//   - An error if the write operation fails.
func (c *Client) sendMessage() error {
	bet := newBet(c.config.ID, c.config.Nombre, c.config.Apellido, c.config.Documento, c.config.Nacimiento, c.config.Numero)

	data := bet.toBytes()

	totalWritten := 0
	messageLen := len(data)

	for totalWritten < messageLen {
		n, err := c.conn.Write(data[totalWritten:])
		if err != nil {
			log.Errorf("action: send_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return err
		}
		totalWritten += n
	}

	ack := make([]byte, 1) // Expecting 1 byte from the server
	_, err := c.conn.Read(ack)
	if err != nil {
		log.Errorf("action: wait_for_ack | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	if ack[0] == 1 {
		log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v", c.config.Documento, c.config.Numero)
	} else {
		log.Infof("action: apuesta_enviada | result: fail | dni: %v | numero: %v", c.config.Documento, c.config.Numero)
	}

	return nil
}

// StartClient starts the mechanism in which the client send its bet to the server.
// The client sends `LoopAmount` messages, waits for a response, and logs the results.
// It gracefully handles termination signals (SIGTERM).
func (c *Client) StartClient() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	// Listen for the SIGTERM signal in a separate goroutine
	go func() {
		<-sigs
		c.HandleSignal(sigs)
		os.Exit(0)
	}()

	err := c.createClientSocket()
	if err != nil {
		log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	err = c.sendMessage()
	if err != nil {
		log.Errorf("action: send_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
	}

	err = c.conn.Close()
	if err != nil {
		log.Errorf("action: close_client_socket | result: fail | client_id: %v | error: %v", c.config.ID, err)
	} else {
		log.Infof("action: close_client_socket | result: success | client_id: %v", c.config.ID)
	}
}

// HandleSignal gracefully handles the termination signal (SIGTERM).
// It ensures that the connection is closed properly, and then it closes the signal channel.
func (c *Client) HandleSignal(sigs chan os.Signal) {
	log.Infof("action: close_client_socket | result: in_progress | client_id: %v", c.config.ID)

	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			log.Errorf("action: close_client_socket | result: fail | client_id: %v | error: %v", c.config.ID, err)
		} else {
			log.Infof("action: close_client_socket | result: success | client_id: %v", c.config.ID)
		}
	}

	if sigs != nil {
		close(sigs)
		log.Infof("action: close_client | result: success | client_id: %v", c.config.ID)
	}
}
