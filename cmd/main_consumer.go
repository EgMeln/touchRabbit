package main

import (
	"EgMeln/touchRabbit/internal/consumer"
	"EgMeln/touchRabbit/internal/repository"
	"context"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	rabbitURL := os.Getenv("rabbitURL")
	cons := consumer.NewConsumer(&rabbitURL)

	ctx := context.Background()
	dbNamePostgres := os.Getenv("postgresURL")
	conn, err := repository.GetPostgresConnection(ctx, dbNamePostgres)
	if err != nil {
		log.Fatalf("consumer error %v", err)
	}
	defer func() {
		if err := cons.Conn.Close(); err != nil {
			log.Fatalf("Failed to close connect to RabbitMQ %v", err)
		}
		if err := cons.Ch.Close(); err != nil {
			log.Fatalf("Failed to close a channel %v", err)
		}
	}()
	log.Println("consumer successfully created")

	err = cons.ConsumeMessages(conn)
	if err != nil {
		log.Fatalf("error while consuming messages - %e", err)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	log.Println("successfully consume messages")
	log.Println("received signal", <-c)
}
