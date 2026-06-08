package core_repository_rabbitMQ_queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"onec-integration/internal/logger"
	"onec-integration/internal/queue"
	"onec-integration/internal/telemetry"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	messageCtx := telemetry.ExtractContext(ctx, amqpHeadersToMap(delivery.Headers))
	messageCtx, span := telemetry.Tracer().Start(messageCtx, "rabbit.consume", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	span.SetAttributes(
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.destination.name", queueName),
		attribute.Int64("messaging.rabbitmq.delivery_tag", int64(delivery.DeliveryTag)),
	)

	log := logger.FromContext(messageCtx).With(
		zap.String("queue", queueName),
		zap.String("delivery_tag", fmt.Sprintf("%d", delivery.DeliveryTag)),
	)
	startedAt := time.Now()

	var message queue.Message
	if err := json.Unmarshal(delivery.Body, &message); err != nil {
		if ackErr := delivery.Ack(false); ackErr != nil {
			span.RecordError(ackErr)
			span.SetStatus(codes.Error, "ack malformed message")
			return fmt.Errorf("ack malformed queue message: %w", ackErr)
		}
		log.Error("queue message decode failed", zap.Error(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode queue message")
		return nil
	}

	if message.JobID == "" {
		if ackErr := delivery.Ack(false); ackErr != nil {
			span.RecordError(ackErr)
			span.SetStatus(codes.Error, "ack empty job id")
			return fmt.Errorf("ack queue message with empty job_id: %w", ackErr)
		}
		log.Error("queue message job_id is empty")
		span.SetStatus(codes.Error, "queue message job_id is empty")
		return nil
	}

	log = log.With(zap.String("job_id", message.JobID))
	log.Info("queue message processing started")
	span.SetAttributes(attribute.String("job.id", message.JobID))

	if err := handler(logger.IntoContext(messageCtx, log), message); err != nil {
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			span.RecordError(nackErr)
			span.SetStatus(codes.Error, "nack failed queue message")
			return fmt.Errorf("nack failed queue message: %w", nackErr)
		}
		log.Error("queue message handler failed; message requeued", zap.Error(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "queue message handler failed")
		return nil
	}

	if err := delivery.Ack(false); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "ack queue message")
		return fmt.Errorf("ack queue message: %w", err)
	}
	log.Info("queue message processing finished", zap.Duration("latency", time.Since(startedAt)))

	return nil
}

func amqpHeadersToMap(headers amqp091.Table) map[string]string {
	if len(headers) == 0 {
		return nil
	}

	result := make(map[string]string, len(headers))
	for key, value := range headers {
		if stringValue, ok := value.(string); ok {
			result[key] = stringValue
		}
	}
	return result
}
