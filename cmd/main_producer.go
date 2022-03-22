package main

import (
	"EgMeln/touchRabbit/internal/producer"
	log "github.com/sirupsen/logrus"
	"os"
)

func main() {
	rabbitURL := os.Getenv("rabbitURL")
	prod := producer.NewProducer(&rabbitURL)
	defer func() {
		if err := prod.Conn.Close(); err != nil {
			log.Fatalf("Failed to close connect to RabbitMQ %v", err)
		}
		if err := prod.Ch.Close(); err != nil {
			log.Fatalf("Failed to close a channel %v", err)
		}
	}()
	log.Println("producer successfully created")
	prod.ProduceMessages()
	log.Println("successfully send messages")
}
