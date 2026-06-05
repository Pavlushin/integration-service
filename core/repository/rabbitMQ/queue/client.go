package core_repository_rabbitMQ_queue

import (
	"fmt"

	core_repository_rabbitMQ "onec-integration/core/repository/rabbitMQ"

	"github.com/rabbitmq/amqp091-go"
)

type Client struct {
	conn *amqp091.Connection
}

func NewClient(cfg core_repository_rabbitMQ.Config) (*Client, error) {
	conn, err := amqp091.Dial(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	return &Client{
		conn: conn,
	}, nil
}

func (c *Client) Channel() (*amqp091.Channel, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("rabbitMQ connection is not initialized")
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open rabbitMQ channel: %w", err)
	}

	return ch, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("close rabbitMQ connection: %w", err)
	}

	return nil
}
