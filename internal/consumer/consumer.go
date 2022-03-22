package consumer

import (
	"EgMeln/touchRabbit/internal/model"
	"EgMeln/touchRabbit/internal/repository"
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

type Consumer struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
}

func NewConsumer(url *string) *Consumer {
	conn, err := amqp.Dial(*url)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel %v", err)
	}
	return &Consumer{Conn: conn, Ch: ch}
}
func (cons Consumer) ConsumeMessages(connection *repository.PostgresConnection) error {
	q, err := cons.Ch.QueueDeclare(
		"myQueue", // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue %v", err)
	}
	msgs, err := cons.Ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer %v", err)
	}
	go func() {
		pgxBatch := &pgx.Batch{}
		count := 0
		message := model.RabbitMessage{}
		t := time.Now()
		for d := range msgs {
			log.Infof("Received a message: %s", d.Body)
			err := json.Unmarshal(d.Body, &message)
			if err != nil {
				log.Errorf("can't parse message %v", err)
			}
			log.Info(message.Key)
			log.Info(message.Message)
			pgxBatch.Queue("insert into rabbit(key, message) values($1, $2)", message.Key, message.Message)
			count++
			log.Println("Inserted : ", message)
			if count%2000 == 0 {
				batchResult := connection.Conn.SendBatch(context.Background(), pgxBatch)
				for m := 0; m < 2000; m++ {
					ct, err := batchResult.Exec()
					if err != nil {
						log.Errorf("can't insert messages into repository %v", err)
					}
					if ct.RowsAffected() != 1 {
						log.Errorf("RowsAffected() => %v, want %v", ct.RowsAffected(), 1)
					}
				}
				log.Info("send 2000 messages")
				log.Info(time.Since(t))
				t = time.Now()
				time.Sleep(2 * time.Second)
			}
		}
	}()
	return nil
}
