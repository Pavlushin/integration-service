package engine

import (
	"encoding/json"
	"time"
)

const JobAuditActionDLQRequeue = "dlq_requeue"

type JobAuditRecord struct {
	ID           string
	JobID        string
	Action       string
	Actor        string
	Reason       string
	MetadataJSON json.RawMessage
	CreatedAt    time.Time
}

type RequeueDLQInput struct {
	JobID  string
	Actor  string
	Reason string
}
