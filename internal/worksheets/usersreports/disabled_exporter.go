package usersreports

import (
	"context"
	"errors"

	"onec-integration/internal/engine/failure"
)

const disabledExporterError = "worksheets users reports export is disabled: set WORKSHEETS_EXPORT_SOURCE=lk_mariadb and LK_MARIADB_* to enable LK-backed export"

type DisabledExporter struct{}

func NewDisabledExporter() *DisabledExporter {
	return &DisabledExporter{}
}

func (e *DisabledExporter) Export(context.Context, Request) ([]byte, error) {
	return nil, failure.NonRetryable(errors.New(disabledExporterError))
}
