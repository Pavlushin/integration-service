package queue

import "context"

type Publisher interface {
	PublishJob(ctx context.Context, queueName string, message Message, headers map[string]string) error
}
