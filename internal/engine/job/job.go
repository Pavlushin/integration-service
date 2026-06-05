package job

import (
	"encoding/json"
	"time"
)

type Job struct {
	ID             string
	CorrelationID  string
	ParentID       string
	Type           string
	Kind           Kind
	Direction      Direction
	Status         Status
	DedupeKey      string
	IdempotencyKey string
	PayloadJSON    json.RawMessage
	ResultPath     string
	Attempts       int
	LastError      string
	CreatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

func (j Job) IsActive() bool {
	return j.Status.IsActive()
}

func (j Job) IsTerminal() bool {
	return j.Status.IsTerminal()
}
