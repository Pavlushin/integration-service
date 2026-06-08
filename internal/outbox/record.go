package outbox

import (
	"encoding/json"
	"time"
)

type Record struct {
	ID          string
	Topic       string
	PayloadJSON json.RawMessage
	Headers     map[string]string
	CreatedAt   time.Time
	AvailableAt time.Time
	PublishedAt *time.Time
	LastError   string
}
