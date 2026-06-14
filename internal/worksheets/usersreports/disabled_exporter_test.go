package usersreports

import (
	"context"
	"strings"
	"testing"

	"onec-integration/internal/engine/failure"
)

func TestDisabledExporterReturnsNonRetryableConfigError(t *testing.T) {
	exporter := NewDisabledExporter()

	_, err := exporter.Export(context.Background(), Request{
		DateFrom: "2026-06-01",
		DateTo:   "2026-06-02",
	})

	if err == nil {
		t.Fatal("expected disabled exporter error")
	}
	if !failure.IsNonRetryable(err) {
		t.Fatalf("expected non-retryable error, got %T %[1]v", err)
	}
	if !strings.Contains(err.Error(), "WORKSHEETS_EXPORT_SOURCE=lk_mariadb") {
		t.Fatalf("expected enablement hint in error, got %q", err.Error())
	}
}
