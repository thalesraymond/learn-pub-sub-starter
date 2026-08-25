package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/joho/godotenv"
	"github.com/rabbitmq/amqp091-go"
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

	gameState := gamelogic.NewGameState(userName)

	err = SubscribeToPause(userName, err, conn, gameState)
	if err != nil {
		fmt.Println("Error subscribing to pause messages:", err)
		return
	}

	err = SubscribeToArmyMoves(userName, err, conn, gameState)
	if err != nil {
		fmt.Println("Error subscribing to army moves messages:", err)
		return
	}

	for {
		words := gamelogic.GetInput()

		switch words[0] {
		case "spawn":
			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Println("Error spawning unit:", err)
			}
		case "move":
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println("Error moving unit:", err)
			}
			pubsub.PublishJSON(channel, routing.ExchangePerilTopic, "army_moves."+userName, move)
			fmt.Println("Move command sent to server.")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(state routing.PlayingState) {
		defer fmt.Print("> ")

		gs.HandlePause(state)
	}
}

func SubscribeToPause(userName string, err error, conn *amqp091.Connection, gameState *gamelogic.GameState) error {
	queueName := routing.PauseKey + "." + userName
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.QueueTransient, handlerPause(gameState))
	if err != nil {
		fmt.Println("Error subscribing to pause messages:", err)
		return err
	}
	return nil
}

func handlerArmyMoves(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		defer fmt.Print("> ")

		gs.HandleMove(move)
	}
}

func SubscribeToArmyMoves(userName string, err error, conn *amqp091.Connection, gameState *gamelogic.GameState) error {
	queueName := "army_moves" + "." + userName
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, queueName, "army_moves.*", pubsub.QueueTransient, handlerArmyMoves(gameState))
	if err != nil {
		fmt.Println("Error subscribing to army moves messages:", err)
		return err
	}
	return nil
}
