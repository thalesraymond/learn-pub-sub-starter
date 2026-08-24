package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("Starting Peril server...")
	godotenv.Load(".env")
	rabbitMQConnectionString := os.Getenv("RABBIT_MQ_CONNECTION_STRING")

	conn, amqpChannel, err := pubsub.ConnectToRabbitMQ(rabbitMQConnectionString)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()
	defer amqpChannel.Close()

	dataToSend := routing.PlayingState{
		IsPaused: true,
	}
	pubsub.PublishJSON(amqpChannel, routing.ExchangePerilDirect, routing.PauseKey, dataToSend)

	fmt.Println("Connected to RabbitMQ successfully!")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	<-signalChan
	fmt.Println("Received interrupt signal. Shutting down...")
}
