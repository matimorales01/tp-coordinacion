package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/clientfruit"
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
	inputQueue      middleware.Middleware
	outputExchanges []middleware.Middleware

	sumAmount           int
	flushedClients      map[string]bool
	clientFruitItemMaps clientfruit.Map
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchanges := make([]middleware.Middleware, config.AggregationAmount)
	for i := range config.AggregationAmount {
		key := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, i)}
		outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, key, connSettings)
		if err != nil {
			inputQueue.Close()
			for _, exchange := range outputExchanges {
				if exchange != nil {
					exchange.Close()
				}
			}
			return nil, err
		}
		outputExchanges[i] = outputExchange
	}

	return &Sum{
		inputQueue:          inputQueue,
		outputExchanges:     outputExchanges,
		sumAmount:           config.SumAmount,
		flushedClients:      map[string]bool{},
		clientFruitItemMaps: clientfruit.Map{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.handleSignals()
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})

	sum.inputQueue.Close()
	for _, exchange := range sum.outputExchanges {
		exchange.Close()
	}
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	sum.inputQueue.StopConsuming()
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, flushRemaining, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if len(fruitRecords) > 0 {
		sum.clientFruitItemMaps.Add(clientId, fruitRecords)
	}

	if !isEof {
		return
	}

	remaining := flushRemaining
	if remaining == 0 {
		remaining = sum.sumAmount
	}

	if !sum.flushedClients[clientId] {
		sum.flushedClients[clientId] = true
		if err := sum.flushClient(clientId); err != nil {
			slog.Error("While flushing client", "err", err)
		}
		remaining--
	}

	if remaining > 0 {
		if err := sum.forwardFlushToken(clientId, remaining); err != nil {
			slog.Error("While forwarding flush token", "err", err)
		}
	}
}

func (sum *Sum) forwardFlushToken(clientId string, remaining int) error {
	message, err := inner.SerializeFlushToken(clientId, remaining)
	if err != nil {
		return err
	}
	return sum.inputQueue.Send(*message)
}

func (sum *Sum) flushClient(clientId string) error {
	fruitRecords := sum.clientFruitItemMaps.Take(clientId)

	partitions := make([][]fruititem.FruitItem, len(sum.outputExchanges))
	for _, fruitRecord := range fruitRecords {
		partition := aggregationPartition(fruitRecord.Fruit, len(sum.outputExchanges))
		partitions[partition] = append(partitions[partition], fruitRecord)
	}

	for partition, records := range partitions {
		message, err := inner.SerializeMessage(clientId, records, true)
		if err != nil {
			return err
		}
		if err := sum.outputExchanges[partition].Send(*message); err != nil {
			return err
		}
	}
	return nil
}

func aggregationPartition(fruit string, aggregationAmount int) int {
	hash := fnv.New32a()
	hash.Write([]byte(fruit))
	return int(hash.Sum32() % uint32(aggregationAmount))
}
