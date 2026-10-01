package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type wireRecord struct {
	Fruit  string `json:"fruit"`
	Amount uint32 `json:"amount"`
}

type wireMessage struct {
	ClientId       string       `json:"client_id"`
	Records        []wireRecord `json:"records"`
	Eof            bool         `json:"eof"`
	FlushRemaining int          `json:"flush_remaining,omitempty"`
}

func SerializeMessage(clientId string, fruitRecords []fruititem.FruitItem, eof bool) (*middleware.Message, error) {
	records := make([]wireRecord, 0, len(fruitRecords))
	for _, fruitRecord := range fruitRecords {
		records = append(records, wireRecord{Fruit: fruitRecord.Fruit, Amount: fruitRecord.Amount})
	}

	body, err := json.Marshal(wireMessage{ClientId: clientId, Records: records, Eof: eof})
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func SerializeFlushToken(clientId string, remaining int) (*middleware.Message, error) {
	body, err := json.Marshal(wireMessage{ClientId: clientId, Eof: true, FlushRemaining: remaining})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeMessage(message *middleware.Message) (clientId string, fruitRecords []fruititem.FruitItem, eof bool, flushRemaining int, err error) {
	var wire wireMessage
	if err := json.Unmarshal([]byte(message.Body), &wire); err != nil {
		return "", nil, false, 0, err
	}

	fruitRecords = make([]fruititem.FruitItem, 0, len(wire.Records))
	for _, record := range wire.Records {
		fruitRecords = append(fruitRecords, fruititem.FruitItem{Fruit: record.Fruit, Amount: record.Amount})
	}

	return wire.ClientId, fruitRecords, wire.Eof, wire.FlushRemaining, nil
}
