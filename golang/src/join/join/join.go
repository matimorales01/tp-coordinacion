package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware

	aggregationAmount int
	topSize           int
	clientFruitItems  map[string][]fruititem.FruitItem
	clientEofCounts   map[string]int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		clientFruitItems:  map[string][]fruititem.FruitItem{},
		clientEofCounts:   map[string]int{},
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if len(fruitRecords) > 0 {
		join.clientFruitItems[clientId] = append(join.clientFruitItems[clientId], fruitRecords...)
	}

	if isEof {
		if err := join.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	}
}

func (join *Join) handleEndOfRecordsMessage(clientId string) error {
	join.clientEofCounts[clientId]++
	if join.clientEofCounts[clientId] < join.aggregationAmount {
		return nil
	}
	delete(join.clientEofCounts, clientId)

	fruitTop := join.buildFruitTop(clientId)
	delete(join.clientFruitItems, clientId)

	message, err := inner.SerializeMessage(clientId, fruitTop, true)
	if err != nil {
		return err
	}
	return join.outputQueue.Send(*message)
}

func (join *Join) buildFruitTop(clientId string) []fruititem.FruitItem {
	fruitItems := join.clientFruitItems[clientId]
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
