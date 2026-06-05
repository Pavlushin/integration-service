package queue

import "context"

type Publisher interface {
	PublishJob(ctx context.Context, queueName string, message Message) error
}
