package product

import (
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	BaseURL            string        `envconfig:"BASE_URL" required:"true"`
	BearerToken        string        `envconfig:"BEARER_TOKEN" required:"true"`
	Timeout            time.Duration `envconfig:"TIMEOUT" default:"60s"`
	InsecureSkipVerify bool          `envconfig:"INSECURE_SKIP_VERIFY" default:"false"`
}

func NewConfig() (Config, error) {
	var config Config
	if err := envconfig.Process("PRODUCT_API", &config); err != nil {
		return Config{}, fmt.Errorf("process product api config: %w", err)
	}

	config.BaseURL = strings.TrimRight(config.BaseURL, "/")

	return config, nil
}

func NewConfigMust() Config {
	config, err := NewConfig()
	if err != nil {
		panic(fmt.Errorf("get product api config: %w", err))
	}

	return config
}
