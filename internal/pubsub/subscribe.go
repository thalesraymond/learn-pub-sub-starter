package pubsub

import (
	"encoding/json"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType, // a function that takes a T and returns an AckType
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
			ackType := handler(*unmarshaled)
			switch ackType {
			case Ack:
				msg.Ack(false)
				fmt.Println("Message acknowledged.")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("Message nacked and requeued.")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("Message nacked and discarded.")
			}
		}
	}()

	return nil
}
