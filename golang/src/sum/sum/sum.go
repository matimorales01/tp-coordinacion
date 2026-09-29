package sum

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue          middleware.Middleware
	outputExchange      middleware.Middleware
	clientFruitItemMaps map[string]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:          inputQueue,
		outputExchange:      outputExchange,
		clientFruitItemMaps: map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if len(fruitRecords) > 0 {
		if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
			slog.Error("While handling data message", "err", err)
		}
	}

	if isEof {
		if err := sum.handleEndOfRecordMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId string) error {
	slog.Info("Received End Of Records message", "clientId", clientId)

	fruitItemMap := sum.clientFruitItemMaps[clientId]
	fruitRecords := make([]fruititem.FruitItem, 0, len(fruitItemMap))
	for _, fruitRecord := range fruitItemMap {
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	message, err := inner.SerializeMessage(clientId, fruitRecords, true)
	if err != nil {
		slog.Debug("While serializing message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending message", "err", err)
		return err
	}

	delete(sum.clientFruitItemMaps, clientId)
	return nil
}

func (sum *Sum) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) error {
	fruitItemMap, ok := sum.clientFruitItemMaps[clientId]
	if !ok {
		fruitItemMap = map[string]fruititem.FruitItem{}
		sum.clientFruitItemMaps[clientId] = fruitItemMap
	}

	for _, fruitRecord := range fruitRecords {
		if existing, ok := fruitItemMap[fruitRecord.Fruit]; ok {
			fruitItemMap[fruitRecord.Fruit] = existing.Sum(fruitRecord)
		} else {
			fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
