package main

import (
	"EgMeln/touchRabbit/internal/producer"
	log "github.com/sirupsen/logrus"
	"os"
)

func main() {
	rabbitURL := os.Getenv("rabbitURL")
	prod, err := producer.NewProducer(&rabbitURL)
	if err != nil {
		log.Fatalf("producer error %v", err)
	}
	defer func() {
		if err := prod.Conn.Close(); err != nil {
			log.Fatalf("Failed to close connect to RabbitMQ %v", err)
		}
		if err := prod.Ch.Close(); err != nil {
			log.Fatalf("Failed to close a channel %v", err)
		}
	}()
	log.Println("producer successfully created")
	err = prod.ProduceMessages()
	if err != nil {
		log.Fatalf("produce messages error %v", err)
	}
	log.Println("successfully send messages")
}
