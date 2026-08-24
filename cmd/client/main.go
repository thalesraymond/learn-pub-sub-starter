package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("Starting Peril client...")
	godotenv.Load(".env")
	rabbitMQConnectionString := os.Getenv("RABBIT_MQ_CONNECTION_STRING")

	conn, channel, err := pubsub.ConnectToRabbitMQ(rabbitMQConnectionString)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()
	defer channel.Close()

	fmt.Println("Connected to RabbitMQ successfully!")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error occurred while welcoming client:", err)
		return
	}

	queueName := routing.PauseKey + "." + userName
	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.QueueTransient)
	if err != nil {
		fmt.Println("Failed to declare and bind queue:", err)
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanned := scanner.Scan()
	if scanned {
		fmt.Println("Scanned input:", scanner.Text())
	}
}
