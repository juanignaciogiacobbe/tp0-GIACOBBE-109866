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
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	MaxBatchAmount int
	BatchReader    *BatchReader

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

func (c *Client) sendBatch(len int, batch []byte) error {
	for len > 0 {
		n, err := c.conn.Write(batch)
		if err != nil {
			return nil
		}

		batch = batch[n:]
		len -= n
	}
	return nil
}

func (c *Client) sendMessage() error {
	batchReader, err := newBatchReader(c.config.MaxBatchAmount, c.config.ID)
	if err != nil {
		log.Criticalf(
			"action: read_file | result: fail | agency_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}

	c.config.BatchReader = batchReader
	batchNumber := 0

	for {
		batch, err := c.config.BatchReader.ReadBatch()
		if err != nil {
			log.Criticalf(
				"action: read_file | result: fail | agency_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return err
		}

		if len(batch) == 0 {
			break
		}

		batchNumber++
		batch = addBatchBytesLen(batch)
		batchLen := len(batch)

		sendError := c.sendBatch(batchLen, batch)
		if sendError != nil {
			log.Criticalf(`action: apuestas_enviadas | result: fail | bytes: %v | batch: %v | error: %v`, batchLen, batchNumber, err)
			return sendError
		}

		// waits for server response
		c.conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
		c.conn.Read(make([]byte, 1))
		log.Infof(`action: apuestas_enviadas | result: success | bytes: %v | batch: %v`, batchLen, batchNumber)
	}

	c.config.BatchReader.Close()
	c.config.BatchReader = nil

	return nil
}

func addBatchBytesLen(batch []byte) []byte {
	batchSize := uint16(len(batch))
	batchWithLength := []byte{
		byte(batchSize >> 8),
		byte(batchSize & 0xff),
	}

	return append(batchWithLength, batch...)
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

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

	time.Sleep(c.config.LoopPeriod)

	c.conn.Close()

	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}

func (c *Client) HandleSignal(sigs chan os.Signal) {
	log.Infof("action: close_client_socket | result: in_progress | client_id: %v", c.config.ID)

	if c.conn != nil {
		err := c.conn.Close()
		if err == nil {
			log.Infof("action: close_client_socket | result: success | client_id: %v", c.config.ID)
		}
	}

	if sigs != nil {
		close(sigs)
		log.Infof("action: close_client | result: success | client_id: %v", c.config.ID)
	}
}
