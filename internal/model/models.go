package model

type RabbitMessage struct {
	Key     string `json:"key"`
	Message string `json:"message"`
}
