package messagehandler

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	clientId string
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{clientId: generateClientId()}
}

func generateClientId() string {
	buf := make([]byte, 16)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(messageHandler.clientId, data, false)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeMessage(messageHandler.clientId, nil, true)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientId, fruitRecords, _, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if clientId != messageHandler.clientId {
		return nil, nil
	}
	return fruitRecords, nil
}
