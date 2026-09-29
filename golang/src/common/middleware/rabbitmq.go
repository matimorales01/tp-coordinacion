package middleware

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

var consumerTagCounter atomic.Int64

func nextConsumerTag() string {
	return fmt.Sprintf("consumer-%d", consumerTagCounter.Add(1))
}

func dial(settings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	return conn, channel, nil
}

func consume(channel *amqp.Channel, queueName string, callbackFunc func(msg Message, ack func(), nack func())) error {
	consumerTag := nextConsumerTag()
	closeNotify := channel.NotifyClose(make(chan *amqp.Error, 1))

	deliveries, err := channel.Consume(queueName, consumerTag, false, false, false, false, nil)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for delivery := range deliveries {
		delivery := delivery
		callbackFunc(
			Message{Body: string(delivery.Body)},
			func() { delivery.Ack(false) },
			func() { delivery.Nack(false, true) },
		)
	}

	select {
	case closeErr := <-closeNotify:
		if closeErr != nil {
			return ErrMessageMiddlewareDisconnected
		}
	default:
	}
	return nil
}

func cancelConsuming(channel *amqp.Channel) error {
	if channel.IsClosed() {
		return nil
	}
	if err := channel.Cancel("", true); err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func closeConnection(conn *amqp.Connection, channel *amqp.Channel) error {
	if err := channel.Close(); err != nil && err != amqp.ErrClosed {
		return ErrMessageMiddlewareClose
	}
	if err := conn.Close(); err != nil && err != amqp.ErrClosed {
		return ErrMessageMiddlewareClose
	}
	return nil
}

type MessageMiddlewareQueueRabbitMQ struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
	sendMutex sync.Mutex
}

func newQueueMiddleware(queueName string, settings ConnSettings) (*MessageMiddlewareQueueRabbitMQ, error) {
	conn, channel, err := dial(settings)
	if err != nil {
		return nil, err
	}

	if _, err := channel.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		closeConnection(conn, channel)
		return nil, ErrMessageMiddlewareMessage
	}

	return &MessageMiddlewareQueueRabbitMQ{conn: conn, channel: channel, queueName: queueName}, nil
}

func (middleware *MessageMiddlewareQueueRabbitMQ) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return consume(middleware.channel, middleware.queueName, callbackFunc)
}

func (middleware *MessageMiddlewareQueueRabbitMQ) StopConsuming() error {
	return cancelConsuming(middleware.channel)
}

func (middleware *MessageMiddlewareQueueRabbitMQ) Send(msg Message) error {
	middleware.sendMutex.Lock()
	defer middleware.sendMutex.Unlock()

	err := middleware.channel.PublishWithContext(
		context.Background(),
		"",
		middleware.queueName,
		false,
		false,
		amqp.Publishing{ContentType: "text/plain", DeliveryMode: amqp.Persistent, Body: []byte(msg.Body)},
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (middleware *MessageMiddlewareQueueRabbitMQ) Close() error {
	return closeConnection(middleware.conn, middleware.channel)
}

type MessageMiddlewareExchangeRabbitMQ struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName string
	routingKeys  []string
	sendMutex    sync.Mutex
}

func newExchangeMiddleware(exchangeName string, routingKeys []string, settings ConnSettings) (*MessageMiddlewareExchangeRabbitMQ, error) {
	conn, channel, err := dial(settings)
	if err != nil {
		return nil, err
	}

	if err := channel.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil); err != nil {
		closeConnection(conn, channel)
		return nil, ErrMessageMiddlewareMessage
	}

	return &MessageMiddlewareExchangeRabbitMQ{
		conn:         conn,
		channel:      channel,
		exchangeName: exchangeName,
		routingKeys:  routingKeys,
	}, nil
}

func (middleware *MessageMiddlewareExchangeRabbitMQ) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	queue, err := middleware.channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for _, routingKey := range middleware.routingKeys {
		if err := middleware.channel.QueueBind(queue.Name, routingKey, middleware.exchangeName, false, nil); err != nil {
			return ErrMessageMiddlewareMessage
		}
	}

	return consume(middleware.channel, queue.Name, callbackFunc)
}

func (middleware *MessageMiddlewareExchangeRabbitMQ) StopConsuming() error {
	return cancelConsuming(middleware.channel)
}

func (middleware *MessageMiddlewareExchangeRabbitMQ) Send(msg Message) error {
	middleware.sendMutex.Lock()
	defer middleware.sendMutex.Unlock()

	for _, routingKey := range middleware.routingKeys {
		err := middleware.channel.PublishWithContext(
			context.Background(),
			middleware.exchangeName,
			routingKey,
			false,
			false,
			amqp.Publishing{ContentType: "text/plain", DeliveryMode: amqp.Persistent, Body: []byte(msg.Body)},
		)
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (middleware *MessageMiddlewareExchangeRabbitMQ) Close() error {
	return closeConnection(middleware.conn, middleware.channel)
}
