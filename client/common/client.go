package common

import (
	"bufio"
	"encoding/csv"
	"fmt"
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

func (c *Client) LoadBetsFromFile() ([]Bet, error) {
	filePath := "./agency.csv"
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %v: %v", filePath, err)
	}
	defer file.Close()

	var bets []Bet
	reader := csv.NewReader(bufio.NewReader(file))
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		if len(record) < 5 {
			continue
		}

		bet := newBet(c.config.ID, record[0], record[1], record[2], record[3], record[4])
		bets = append(bets, bet)
	}

	return bets, nil
}

func (c *Client) sendMessage() error {
	bets, err := c.LoadBetsFromFile()
	if err != nil {
		log.Errorf("action: load_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	log.Infof("action: load_bets | result: success | bets: %v", len(bets))
	bet := newBet("1", c.config.Nombre, c.config.Apellido, c.config.Documento, c.config.Nacimiento, c.config.Numero)

	data := bet.toBytes()

	maxBatchAmount := c.config.MaxBatchAmount

	log.Infof("action: load_bets | result: success | batch: %v", maxBatchAmount)

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

	log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v", c.config.Documento, c.config.Numero)

	return nil
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
