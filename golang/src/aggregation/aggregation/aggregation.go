package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/clientfruit"
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
	clientFruitItemMaps clientfruit.Map
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
		clientFruitItemMaps: clientfruit.Map{},
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

	aggregation.inputExchange.Close()
	aggregation.outputQueue.Close()
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	aggregation.inputExchange.StopConsuming()
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if len(fruitRecords) > 0 {
		aggregation.clientFruitItemMaps.Add(clientId, fruitRecords)
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

	fruitRecords := aggregation.clientFruitItemMaps.Take(clientId)
	fruitTopRecords := clientfruit.Top(fruitRecords, aggregation.topSize)

	message, err := inner.SerializeMessage(clientId, fruitTopRecords, true)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}
	return nil
}
