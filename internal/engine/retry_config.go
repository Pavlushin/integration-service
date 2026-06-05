package engine

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type RetryConfig struct {
	MaxAttempts int           `envconfig:"MAX_ATTEMPTS" default:"3"`
	Backoff     time.Duration `envconfig:"RETRY_BACKOFF" default:"1s"`
}

func NewRetryConfig() (RetryConfig, error) {
	var config RetryConfig
	if err := envconfig.Process("PROCESSING", &config); err != nil {
		return RetryConfig{}, fmt.Errorf("process processing retry config: %w", err)
	}
	if config.MaxAttempts <= 0 {
		return RetryConfig{}, fmt.Errorf("processing max attempts must be positive")
	}
	if config.Backoff <= 0 {
		return RetryConfig{}, fmt.Errorf("processing retry backoff must be positive")
	}
	return config, nil
}

func NewRetryConfigMust() RetryConfig {
	config, err := NewRetryConfig()
	if err != nil {
		panic(fmt.Errorf("get processing retry config: %w", err))
	}
	return config
}

func (c RetryConfig) Policy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: c.MaxAttempts,
		Backoff:     c.Backoff,
	}.WithDefaults()
}
