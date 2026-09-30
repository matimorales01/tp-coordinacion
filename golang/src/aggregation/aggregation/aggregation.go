package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue         middleware.Middleware
	inputExchange       middleware.Middleware
	clientFruitItemMaps map[string]map[string]fruititem.FruitItem
	clientEofCounts     map[string]int
	sumAmount           int
	topSize             int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:         outputQueue,
		inputExchange:       inputExchange,
		clientFruitItemMaps: map[string]map[string]fruititem.FruitItem{},
		clientEofCounts:     map[string]int{},
		sumAmount:           config.SumAmount,
		topSize:             config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	go aggregation.handleSignals()
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	aggregation.inputExchange.StopConsuming()
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if len(fruitRecords) > 0 {
		aggregation.handleDataMessage(clientId, fruitRecords)
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	}
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string) error {
	aggregation.clientEofCounts[clientId]++
	if aggregation.clientEofCounts[clientId] < aggregation.sumAmount {
		return nil
	}
	delete(aggregation.clientEofCounts, clientId)

	fruitTopRecords := aggregation.buildFruitTop(clientId)
	message, err := inner.SerializeMessage(clientId, fruitTopRecords, true)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	delete(aggregation.clientFruitItemMaps, clientId)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) {
	fruitItemMap, ok := aggregation.clientFruitItemMaps[clientId]
	if !ok {
		fruitItemMap = map[string]fruititem.FruitItem{}
		aggregation.clientFruitItemMaps[clientId] = fruitItemMap
	}

	for _, fruitRecord := range fruitRecords {
		if existing, ok := fruitItemMap[fruitRecord.Fruit]; ok {
			fruitItemMap[fruitRecord.Fruit] = existing.Sum(fruitRecord)
		} else {
			fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientId string) []fruititem.FruitItem {
	fruitItemMap := aggregation.clientFruitItemMaps[clientId]
	fruitItems := make([]fruititem.FruitItem, 0, len(fruitItemMap))
	for _, item := range fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
