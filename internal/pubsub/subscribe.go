package pubsub

import (
	"encoding/json"

	"github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	msgs, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			unmarshaled := new(T)
			err := json.Unmarshal(msg.Body, unmarshaled)
			if err != nil {
				// If we can't unmarshal the message, we should nack it so it can be retried or sent to a dead letter queue.
				msg.Nack(false, false)
				continue
			}
			handler(*unmarshaled)
			msg.Ack(false)
		}
	}()

	return nil
}
