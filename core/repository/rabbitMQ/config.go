package core_repository_rabbitMQ

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Host          string `envconfig:"HOST" required:"true"`
	Port          string `envconfig:"PORT" required:"true"`
	User          string `envconfig:"USER" required:"true"`
	Password      string `envconfig:"PASS" required:"true"`
	PrepareQueue  string `envconfig:"PREPARE_QUEUE" default:"integration.prepare"`
	DeliveryQueue string `envconfig:"DELIVERY_QUEUE" default:"integration.delivery"`
}

func (c Config) DSN() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s/", c.User, c.Password, c.Host, c.Port)
}

func NewConfig() (Config, error) {
	var config Config
	if err := envconfig.Process("RABBIT_MQ", &config); err != nil {
		return Config{}, fmt.Errorf("failed to process env var: %w", err)
	}
	return config, nil
}

func NewConfigMust() Config {
	config, err := NewConfig()
	if err != nil {
		panic(fmt.Errorf("failed to create rabbitmq config object: %w", err))
	}

	return config
}
