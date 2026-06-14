package usersreports

import (
	"fmt"
	"strings"

	"github.com/kelseyhightower/envconfig"
)

type ExportSource string

const (
	SourceLKMariaDB ExportSource = "lk_mariadb"
	SourceDisabled  ExportSource = "disabled"
)

type SourceConfig struct {
	Source ExportSource `envconfig:"SOURCE" default:"lk_mariadb"`
}

func NewSourceConfig() (SourceConfig, error) {
	var config SourceConfig
	if err := envconfig.Process("WORKSHEETS_EXPORT", &config); err != nil {
		return SourceConfig{}, fmt.Errorf("process worksheets export config: %w", err)
	}

	source := ExportSource(strings.ToLower(strings.TrimSpace(string(config.Source))))
	if source == "" {
		source = SourceLKMariaDB
	}

	switch source {
	case SourceLKMariaDB, SourceDisabled:
		config.Source = source
		return config, nil
	default:
		return SourceConfig{}, fmt.Errorf("unsupported worksheets export source %q", config.Source)
	}
}

func NewSourceConfigMust() SourceConfig {
	config, err := NewSourceConfig()
	if err != nil {
		panic(fmt.Errorf("get worksheets export config: %w", err))
	}
	return config
}
