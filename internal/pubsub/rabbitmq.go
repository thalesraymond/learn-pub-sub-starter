package pubsub

import "github.com/rabbitmq/amqp091-go"

func ConnectToRabbitMQ(connectionString string) (*amqp091.Connection, *amqp091.Channel, error) {
	conn, err := amqp091.Dial(connectionString)
	if err != nil {
		return nil, nil, err
	}

	amqpChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, amqpChannel, nil
}
