package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	rabbitMQConnectionString := "amqp://guest:guest@localhost:5672/" // update later to use env variable

	conn, err := amqp091.Dial(rabbitMQConnectionString)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open a channel:", err)
		return
	}
		defer amqpChannel.Close()
		
	dataToSend := routing.PlayingState{
		IsPaused: false,
	}
	pubsub.PublishJSON(amqpChannel, routing.ExchangePerilDirect, routing.PauseKey, dataToSend)

	fmt.Println("Connected to RabbitMQ successfully!")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	<-signalChan
	fmt.Println("Received interrupt signal. Shutting down...")
}
