package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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

type sumDelivery struct {
	msg  middleware.Message
	ack  func()
	nack func()
}

type Sum struct {
	inputQueue      middleware.Middleware
	outputExchanges []middleware.Middleware
	eofBroadcast    middleware.Middleware
	eofListen       middleware.Middleware

	dataDeliveries    chan sumDelivery
	controlDeliveries chan sumDelivery
	done              chan struct{}

	clientFruitItemMaps map[string]map[string]fruititem.FruitItem
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

	eofExchangeName := config.SumPrefix + "_eof"

	sumKeys := make([]string, config.SumAmount)
	for i := range config.SumAmount {
		sumKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
	}

	eofBroadcast, err := middleware.CreateExchangeMiddleware(eofExchangeName, sumKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		for _, exchange := range outputExchanges {
			exchange.Close()
		}
		return nil, err
	}

	ownKey := []string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)}
	eofListen, err := middleware.CreateExchangeMiddleware(eofExchangeName, ownKey, connSettings)
	if err != nil {
		inputQueue.Close()
		for _, exchange := range outputExchanges {
			exchange.Close()
		}
		eofBroadcast.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:          inputQueue,
		outputExchanges:     outputExchanges,
		eofBroadcast:        eofBroadcast,
		eofListen:           eofListen,
		dataDeliveries:      make(chan sumDelivery),
		controlDeliveries:   make(chan sumDelivery),
		done:                make(chan struct{}),
		clientFruitItemMaps: map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.handleSignals()
	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.dataDeliveries <- sumDelivery{msg, ack, nack}
	})
	go sum.eofListen.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.controlDeliveries <- sumDelivery{msg, ack, nack}
	})
	sum.coordinate()
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	sum.inputQueue.StopConsuming()
	sum.eofListen.StopConsuming()
	close(sum.done)
}

func (sum *Sum) coordinate() {
	for {
		sum.drainData()

		select {
		case <-sum.done:
			return
		case delivery := <-sum.dataDeliveries:
			sum.handleData(delivery)
		case delivery := <-sum.controlDeliveries:
			sum.drainData()
			sum.handleControl(delivery)
		}
	}
}

func (sum *Sum) drainData() {
	for {
		select {
		case delivery := <-sum.dataDeliveries:
			sum.handleData(delivery)
		default:
			return
		}
	}
}

func (sum *Sum) handleData(delivery sumDelivery) {
	defer delivery.ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&delivery.msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if len(fruitRecords) > 0 {
		sum.addFruitRecords(clientId, fruitRecords)
	}

	if isEof {
		if err := sum.broadcastEndOfRecords(clientId); err != nil {
			slog.Error("While broadcasting end of records", "err", err)
		}
	}
}

func (sum *Sum) broadcastEndOfRecords(clientId string) error {
	message, err := inner.SerializeMessage(clientId, nil, true)
	if err != nil {
		return err
	}
	return sum.eofBroadcast.Send(*message)
}

func (sum *Sum) handleControl(delivery sumDelivery) {
	defer delivery.ack()

	clientId, _, _, err := inner.DeserializeMessage(&delivery.msg)
	if err != nil {
		slog.Error("While deserializing eof broadcast", "err", err)
		return
	}

	if err := sum.flushClient(clientId); err != nil {
		slog.Error("While flushing client", "err", err)
	}
}

func (sum *Sum) flushClient(clientId string) error {
	fruitItemMap := sum.clientFruitItemMaps[clientId]
	delete(sum.clientFruitItemMaps, clientId)

	partitions := make([][]fruititem.FruitItem, len(sum.outputExchanges))
	for _, fruitRecord := range fruitItemMap {
		partition := aggregationPartition(fruitRecord.Fruit, len(sum.outputExchanges))
		partitions[partition] = append(partitions[partition], fruitRecord)
	}

	for partition, fruitRecords := range partitions {
		message, err := inner.SerializeMessage(clientId, fruitRecords, true)
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

func (sum *Sum) addFruitRecords(clientId string, fruitRecords []fruititem.FruitItem) {
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
}
