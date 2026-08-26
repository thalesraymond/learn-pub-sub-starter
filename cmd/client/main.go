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

	err = SubscribeToArmyMoves(userName, err, conn, channel, gameState)
	if err != nil {
		fmt.Println("Error subscribing to army moves messages:", err)
		return
	}

	// Subscribe to the shared "war" queue so this client can resolve wars.
	err = SubscribeToWar(userName, err, conn, gameState)
	if err != nil {
		fmt.Println("Error subscribing to war messages:", err)
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
			err = pubsub.PublishJSON(channel, routing.ExchangePerilTopic, "army_moves."+userName, move)
			if err != nil {
				fmt.Println("Error publishing move command:", err)
			}
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")

		gs.HandlePause(state)
		return pubsub.Ack
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

func handlerArmyMoves(gs *gamelogic.GameState, ch *amqp091.Channel, userName string) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		outcome := gs.HandleMove(move)
		if outcome == gamelogic.MoveOutcomeMakeWar {
			// Publish the war declaration to the topic exchange so war messages are broadcast.
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+userName, gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				fmt.Println("Error publishing war declaration:", err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		}

		if outcome == gamelogic.MoveOutComeSafe {
			return pubsub.Ack
		}

		return pubsub.NackDiscard
	}
}

func SubscribeToArmyMoves(userName string, err error, conn *amqp091.Connection, ch *amqp091.Channel, gameState *gamelogic.GameState) error {
	queueName := "army_moves" + "." + userName
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, queueName, "army_moves.*", pubsub.QueueTransient, handlerArmyMoves(gameState, ch, userName))
	if err != nil {
		fmt.Println("Error subscribing to army moves messages:", err)
		return err
	}
	return nil
}

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, _, _ := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			// This client isn't in the war; requeue so another client can try to resolve it.
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			// No overlapping units, so there's nothing to fight; discard the message.
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			return pubsub.Ack
		default:
			// Unknown outcome; print an error and discard.
			fmt.Println("Unknown war outcome...")
			return pubsub.NackDiscard
		}
	}
}

func SubscribeToWar(userName string, err error, conn *amqp091.Connection, gameState *gamelogic.GameState) error {
	// All clients share this durable "war" queue, bound to war.* on the topic exchange.
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", routing.WarRecognitionsPrefix+".*", pubsub.QueueDurable, handlerWar(gameState))
	if err != nil {
		fmt.Println("Error subscribing to war messages:", err)
		return err
	}
	return nil
}
