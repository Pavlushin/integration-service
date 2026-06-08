package telemetry

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Enabled          bool          `envconfig:"ENABLED" default:"true"`
	ExporterEndpoint string        `envconfig:"EXPORTER_OTLP_ENDPOINT" default:"http://jaeger:4318"`
	ExportTimeout    time.Duration `envconfig:"EXPORTER_TIMEOUT" default:"5s"`
}

func NewConfig() (Config, error) {
	var config Config
	if err := envconfig.Process("OTEL", &config); err != nil {
		return Config{}, fmt.Errorf("process otel config: %w", err)
	}
	return config, nil
}

func NewConfigMust() Config {
	config, err := NewConfig()
	if err != nil {
		panic(fmt.Errorf("get otel config: %w", err))
	}
	return config
}
