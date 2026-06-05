package core_repository_rabbitMQ_queue

import (
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

func DeclareQueue(client *Client, queueName string) error {
	if client == nil {
		return fmt.Errorf("rabbitmq client is not initialized")
	}

	ch, err := client.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return declareQueue(ch, queueName)
}

func declareQueue(ch *amqp091.Channel, queueName string) error {
	if ch == nil {
		return fmt.Errorf("rabbitmq channel is not initialized")
	}

	_, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("declare rabbitmq queue %q: %w", queueName, err)
	}

	return nil
}
