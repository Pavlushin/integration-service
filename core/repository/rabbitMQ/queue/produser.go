package core_repository_rabbitMQ_queue

import (
	"context"
	"encoding/json"
	"fmt"

	"onec-integration/internal/queue"

	"github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	client *Client
}

func NewProducer(client *Client) *Producer {
	return &Producer{client: client}
}

func (p *Producer) PublishJob(ctx context.Context, queueName string, message queue.Message) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("rabbitmq producer is not initialized")
	}

	ch, err := p.client.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := declareQueue(ch, queueName); err != nil {
		return err
	}

	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal queue message: %w", err)
	}

	if err := ch.PublishWithContext(
		ctx,
		"",
		queueName,
		false,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			Body:         body,
		},
	); err != nil {
		return fmt.Errorf("publish rabbitmq message: %w", err)
	}

	return nil
}
