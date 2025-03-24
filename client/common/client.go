package common

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	notifyPacketFlag       = byte(0x01)
	successCode            = byte(0x01)
	queryWinnersPacketFlag = byte(0x02)
)

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration

	MaxBatchAmount int
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

	batchSender := NewBatchSender(c, c.config.MaxBatchAmount)

	// Send bets from the file
	err = batchSender.SendBatches("./agency.csv")
	if err != nil {
		log.Errorf("action: send_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
	}

	// Notify the server that the client has finished sending bets
	err = c.notifyBetsEnd()
	if err != nil {
		log.Errorf("action: notify_bets_end | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Wait for the server to confirm the lottery (sorteo) has finished
	err = c.waitForLotteryConfirmation()
	if err != nil {
		log.Errorf("action: wait_for_lottery_confirmation | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	time.Sleep(1 * time.Second)
	// Query for winners
	if err := c.QueryWinners(); err != nil {
		log.Errorf("action: query_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
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

// notifyBetsEnd sends a notification to the server indicating that the client has finished sending all bets.
// It sends a 1-byte packet (NotifyBetsEnd) to signal the completion.
func (c *Client) notifyBetsEnd() error {
	notifyPacket := []byte{notifyPacketFlag}
	_, err := c.conn.Write(notifyPacket)
	if err != nil {
		log.Errorf("action: notify_bets_end | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	log.Infof("action: notify_bets_end | result: success | client_id: %v", c.config.ID)
	return nil
}

// waitForLotteryConfirmation waits for the server to confirm that the lottery (sorteo) has been completed.
// The server must respond with a confirmation message once all agencies have notified the completion of their bets.
func (c *Client) waitForLotteryConfirmation() error {
	// Wait for the server to confirm the lottery
	confirmationPacket := make([]byte, 1)
	_, err := c.conn.Read(confirmationPacket)
	if err != nil {
		log.Errorf("action: wait_for_lottery_confirmation | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	// Check if the server has confirmed with a success code
	if confirmationPacket[0] == successCode {
		log.Infof("action: lottery_confirmation | result: success | client_id: %v", c.config.ID)
		return nil
	}

	log.Errorf("action: lottery_confirmation | result: fail | client_id: %v | error: invalid confirmation packet", c.config.ID)
	return fmt.Errorf("invalid confirmation packet from server")
}

// QueryWinners queries the server for the lottery winners for the client's agency
func (c *Client) QueryWinners() error {
	agencyID, err := strconv.Atoi(c.config.ID)
	if err != nil {
		log.Errorf("action: query_winners | result: fail | client_id: %v | error: invalid agency ID %v", c.config.ID, err)
		return err
	}

	// Send the packet type byte (0x02) and the agency ID byte
	queryPacket := []byte{queryWinnersPacketFlag, byte(agencyID)}
	_, err = c.conn.Write(queryPacket)
	if err != nil {
		return err
	}

	winners_data := make([]byte, 1024)
	n, err := c.conn.Read(winners_data)
	if err != nil {
		return err
	}

	winners := string(winners_data[:n])
	if winners == "0" {
		log.Infof("action: consulta_ganadores | result: success | cant_ganadores: 0")
		return nil
	}

	winner_list := strings.Split(winners, ",")

	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %v", len(winner_list))

	return nil
}
