package core_repository_rabbitMQ_queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"onec-integration/internal/logger"
	"onec-integration/internal/queue"

	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type MessageHandler func(ctx context.Context, message queue.Message) error

type Consumer struct {
	client *Client
}

func NewConsumer(client *Client) *Consumer {
	return &Consumer{client: client}
}

func (c *Consumer) ConsumeJobs(ctx context.Context, queueName string, handler MessageHandler) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("rabbitmq consumer is not initialized")
	}
	if handler == nil {
		return fmt.Errorf("message handler is required")
	}

	ch, err := c.client.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := declareQueue(ch, queueName); err != nil {
		return err
	}

	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set rabbitmq qos: %w", err)
	}

	deliveries, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume rabbitmq queue %q: %w", queueName, err)
	}

	log := logger.FromContext(ctx).With(zap.String("queue", queueName))
	log.Info("queue consumer subscribed")
	defer log.Info("queue consumer stopped")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("rabbitmq delivery channel closed")
			}

			if err := c.handleDelivery(ctx, queueName, delivery, handler); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) handleDelivery(ctx context.Context, queueName string, delivery amqp091.Delivery, handler MessageHandler) error {
	log := logger.FromContext(ctx).With(
		zap.String("queue", queueName),
		zap.String("delivery_tag", fmt.Sprintf("%d", delivery.DeliveryTag)),
	)
	startedAt := time.Now()

	var message queue.Message
	if err := json.Unmarshal(delivery.Body, &message); err != nil {
		if ackErr := delivery.Ack(false); ackErr != nil {
			return fmt.Errorf("ack malformed queue message: %w", ackErr)
		}
		log.Error("queue message decode failed", zap.Error(err))
		return nil
	}

	if message.JobID == "" {
		if ackErr := delivery.Ack(false); ackErr != nil {
			return fmt.Errorf("ack queue message with empty job_id: %w", ackErr)
		}
		log.Error("queue message job_id is empty")
		return nil
	}

	log = log.With(zap.String("job_id", message.JobID))
	log.Info("queue message processing started")

	if err := handler(ctx, message); err != nil {
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf("nack failed queue message: %w", nackErr)
		}
		log.Error("queue message handler failed; message requeued", zap.Error(err))
		return nil
	}

	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("ack queue message: %w", err)
	}
	log.Info("queue message processing finished", zap.Duration("latency", time.Since(startedAt)))

	return nil
}
