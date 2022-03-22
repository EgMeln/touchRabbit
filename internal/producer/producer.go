package producer

import (
	"EgMeln/touchRabbit/internal/model"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"strconv"
	"time"
)

type Producer struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
}

func NewProducer(url *string) *Producer {
	conn, err := amqp.Dial(*url)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel %v", err)
	}
	return &Producer{Conn: conn, Ch: ch}
}

func (prod Producer) ProduceMessages() {
	q, err := prod.Ch.QueueDeclare(
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
	log.Printf("Start producing")
	t := time.Now()
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		message := "this is message " + strconv.Itoa(i)
		if err != nil {
			log.Fatalf("marshal error")
		}
		msg := model.RabbitMessage{
			Key:     key,
			Message: message,
		}
		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			log.Fatalf("marshal error")
		}
		err = prod.Ch.Publish(
			"",
			q.Name,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        jsonMsg,
			})
		if err != nil {
			log.Fatalf("Failed to publish a message %v", err)
		} else {
			log.Info("produced", key)
		}
		if i%2000 == 0 {
			log.Info("send 2000 messages")
			log.Info(time.Since(t))
			t = time.Now()
		}
	}
}
