package common

import (
	"encoding/csv"
	"os"
)

// BatchSender is responsible for reading and sending batches of bets to the server.
type BatchSender struct {
	client       *Client
	maxBatchSize int
}

func NewBatchSender(client *Client, maxBatchSize int) *BatchSender {
	return &BatchSender{
		client:       client,
		maxBatchSize: maxBatchSize,
	}
}

func (b *BatchSender) SendBatches(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		log.Errorf("action: open_file | result: fail | client_id: %v | error: %v", b.client.config.ID, err)
		return err
	}

	defer file.Close()

	reader := csv.NewReader(file)

	var batch []Bet
	batchSize := 0

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				if len(batch) > 0 {
					err := b.SendBatch(batch)
					if err != nil {
						log.Errorf("action: send_batch | result: fail | client_id: %v | error: %v", b.client.config.ID, err)
						return err
					}
				}
				break
			}
			log.Errorf("action: read_bets | result: fail | client_id: %v | error: %v", b.client.config.ID, err)
			return err
		}

		bet := newBet(
			b.client.config.ID,
			record[0],
			record[1],
			record[2],
			record[3],
			record[4],
		)

		batch = append(batch, bet)
		batchSize++

		if batchSize >= b.maxBatchSize {
			err := b.SendBatch(batch)
			if err != nil {
				log.Errorf("action: send_batch | result: fail | client_id: %v | error: %v", b.client.config.ID, err)
				return err
			}

			// reset batch
			batch = []Bet{}
			batchSize = 0
		}
	}

	return nil
}

// SendBatch sends a batch of bets to the server. It serializes the bets and sends them in chunks.
func (b *BatchSender) SendBatch(bets []Bet) error {
	data := []byte{}
	for _, bet := range bets {
		data = append(data, bet.toBytes()...)
	}

	// Send the data in chunks to avoid short writes
	totalWritten := 0
	messageLen := len(data)
	for totalWritten < messageLen {
		n, err := b.client.conn.Write(data[totalWritten:])
		if err != nil {
			log.Errorf("action: send_batch | result: fail | client_id: %v | error: %v", b.client.config.ID, err)
			return err
		}
		totalWritten += n
	}

	// Wait for the server's acknowledgment (ACK)
	ack := make([]byte, 1) // Expecting 1 byte from the server
	_, err := b.client.conn.Read(ack)
	if err != nil {
		log.Errorf("action: wait_for_ack | result: fail | client_id: %v | error: %v", b.client.config.ID, err)
		return err
	}

	if ack[0] == 1 {
		log.Infof("action: send_batch | result: success | client_id: %v | batch_size: %d", b.client.config.ID, len(bets))
	} else {
		log.Errorf("action: send_batch | result: fail | client_id: %v | batch_size: %d", b.client.config.ID, len(bets))
	}

	return nil
}
