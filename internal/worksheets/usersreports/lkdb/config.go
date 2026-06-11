package lkdb

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Host     string `envconfig:"HOST" required:"true"`
	Port     int    `envconfig:"PORT" default:"3306"`
	Database string `envconfig:"DATABASE" required:"true"`
	NextDB   string `envconfig:"NEXT_DATABASE" default:"sps_next"`
	User     string `envconfig:"USER" required:"true"`
	Password string `envconfig:"PASSWORD" required:"true"`
}

func NewConfig() (Config, error) {
	var config Config
	if err := envconfig.Process("LK_MARIADB", &config); err != nil {
		return Config{}, fmt.Errorf("process lk mariadb config: %w", err)
	}

	return config, nil
}

func NewConfigMust() Config {
	config, err := NewConfig()
	if err != nil {
		panic(fmt.Errorf("get lk mariadb config: %w", err))
	}

	return config
}
