package common

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

type BatchReader struct {
	File           *os.File
	Reader         *csv.Reader
	MaxBatchAmount int
	AgencyId       string
}

func newBatchReader(maxBatchAmount int, agencyId string) (*BatchReader, error) {
	filePath := "./agency.csv"
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %v: %v", filePath, err)
	}

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 5

	return &BatchReader{
		File:           file,
		Reader:         reader,
		MaxBatchAmount: maxBatchAmount,
		AgencyId:       agencyId,
	}, nil
}

func (b *BatchReader) ReadBatch() ([]byte, error) {
	var batch []byte
	eof := false

	for i := 0; i < b.MaxBatchAmount && !eof; i++ {
		record, err := b.Reader.Read()
		if err == io.EOF {
			eof = true
			break
		}

		if err != nil {
			return nil, err
		}

		bet := newBet(b.AgencyId, record[0], record[1], record[2], record[3], record[4])
		batch = append(batch, bet.toBytes()...)
	}

	return batch, nil
}

func (b *BatchReader) Close() {
	b.File.Close()
}
