package usersreports

import "testing"

func TestNewSourceConfigDefaultsToLKMariaDB(t *testing.T) {
	t.Setenv("WORKSHEETS_EXPORT_SOURCE", "")

	config, err := NewSourceConfig()
	if err != nil {
		t.Fatalf("expected default source config, got %v", err)
	}
	if config.Source != SourceLKMariaDB {
		t.Fatalf("expected default source %q, got %q", SourceLKMariaDB, config.Source)
	}
}

func TestNewSourceConfigAllowsDisabled(t *testing.T) {
	t.Setenv("WORKSHEETS_EXPORT_SOURCE", string(SourceDisabled))

	config, err := NewSourceConfig()
	if err != nil {
		t.Fatalf("expected disabled source config, got %v", err)
	}
	if config.Source != SourceDisabled {
		t.Fatalf("expected source %q, got %q", SourceDisabled, config.Source)
	}
}

func TestNewSourceConfigRejectsUnknownSource(t *testing.T) {
	t.Setenv("WORKSHEETS_EXPORT_SOURCE", "product_api")

	_, err := NewSourceConfig()
	if err == nil {
		t.Fatal("expected unknown source error")
	}
}
