package pubsub

import (
	"context"
	"encoding/json"

	"github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp091.Channel, exchange, key string, val T) error {
	marshaled, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp091.Publishing{
		Body:        marshaled,
		ContentType: "application/json",
	})
	if err != nil {
		return err
	}
	return nil
}

// SimpleQueueType is an "enum" type used to represent whether a queue is
// durable or transient.
type SimpleQueueType int

const (
	QueueDurable SimpleQueueType = iota
	QueueTransient
)

func DeclareAndBind(
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient".
) (*amqp091.Channel, amqp091.Queue, error) {

	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp091.Queue{}, err
	}

	durable := queueType == QueueDurable
	autoDelete := queueType == QueueTransient
	exclusive := queueType == QueueTransient

	createdQueue, err := channel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, amqp091.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})

	if err != nil {
		return nil, amqp091.Queue{}, err
	}

	err = channel.QueueBind(createdQueue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp091.Queue{}, err
	}

	return channel, createdQueue, nil
}
