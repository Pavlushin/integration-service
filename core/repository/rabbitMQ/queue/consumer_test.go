package core_repository_rabbitMQ_queue

import (
	"context"
	"errors"
	"testing"

	"onec-integration/internal/queue"

	"github.com/rabbitmq/amqp091-go"
)

type fakeAcknowledger struct {
	acked       int
	nacked      int
	rejected    int
	nackRequeue bool
}

func (a *fakeAcknowledger) Ack(_ uint64, _ bool) error {
	a.acked++
	return nil
}

func (a *fakeAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.nacked++
	a.nackRequeue = requeue
	return nil
}

func (a *fakeAcknowledger) Reject(_ uint64, _ bool) error {
	a.rejected++
	return nil
}

func TestHandleDeliveryDoesNotReturnFatalErrorWhenHandlerFails(t *testing.T) {
	ack := &fakeAcknowledger{}
	delivery := amqp091.Delivery{
		Acknowledger: ack,
		DeliveryTag:  1,
		Body:         []byte(`{"job_id":"job-1"}`),
	}

	err := (&Consumer{}).handleDelivery(context.Background(), "integration.prepare", delivery, func(context.Context, queue.Message) error {
		return errors.New("product api unavailable")
	})

	if err != nil {
		t.Fatalf("expected handler failure to be non-fatal, got %v", err)
	}
	if ack.nacked != 1 {
		t.Fatalf("expected one nack, got %d", ack.nacked)
	}
	if !ack.nackRequeue {
		t.Fatal("expected failed handler message to be requeued")
	}
	if ack.acked != 0 {
		t.Fatalf("expected no ack, got %d", ack.acked)
	}
}

func TestHandleDeliveryAcksMalformedMessagesWithoutFatalError(t *testing.T) {
	ack := &fakeAcknowledger{}
	delivery := amqp091.Delivery{
		Acknowledger: ack,
		DeliveryTag:  1,
		Body:         []byte(`{`),
	}

	err := (&Consumer{}).handleDelivery(context.Background(), "integration.prepare", delivery, func(context.Context, queue.Message) error {
		t.Fatal("handler should not run for malformed messages")
		return nil
	})

	if err != nil {
		t.Fatalf("expected malformed message to be non-fatal after ack, got %v", err)
	}
	if ack.acked != 1 {
		t.Fatalf("expected one ack for malformed message, got %d", ack.acked)
	}
	if ack.nacked != 0 {
		t.Fatalf("expected no nack for malformed message, got %d", ack.nacked)
	}
}
