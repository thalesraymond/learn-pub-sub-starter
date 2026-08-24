package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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

	queueName := "game_logs"
	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, queueName, routing.GameLogSlug+".*", pubsub.QueueDurable)
	if err != nil {
		fmt.Println("Failed to declare and bind queue:", err)
		return
	}

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()

		switch words[0] {
		case "pause":
			dataToSend := routing.PlayingState{
				IsPaused: true,
			}
			pubsub.PublishJSON(amqpChannel, routing.ExchangePerilDirect, routing.PauseKey, dataToSend)
		case "resume":
			dataToSend := routing.PlayingState{
				IsPaused: false,
			}
			pubsub.PublishJSON(amqpChannel, routing.ExchangePerilDirect, routing.PauseKey, dataToSend)
		case "quit":
			fmt.Println("Quitting server...")
		case "help":
			gamelogic.PrintServerHelp()
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
		}
	}

}
