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

func NewProducer(url *string) (*Producer, error) {
	conn, err := amqp.Dial(*url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel %v", err)

	}
	return &Producer{Conn: conn, Ch: ch}, nil
}

func (prod Producer) ProduceMessages() error {
	q, err := prod.Ch.QueueDeclare(
		"myQueue", // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue %v", err)
	}
	log.Printf("Start producing")
	t := time.Now()
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		message := "this is message " + strconv.Itoa(i)

		msg := model.RabbitMessage{
			Key:     key,
			Message: message,
		}
		jsonMsg, ok := json.Marshal(msg)
		if ok != nil {
			return fmt.Errorf("marshal error %v", err)

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
			return fmt.Errorf("failed to publish a message %v", err)

		} else {
			log.Info("produced", key)
		}
		if i%2000 == 0 {
			log.Info("send 2000 messages")
			log.Info(time.Since(t))
			t = time.Now()
			time.Sleep(1 * time.Second)
		}
	}
}
