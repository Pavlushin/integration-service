package outbox

import (
	"encoding/json"
	"time"
)

type Record struct {
	ID          string
	Topic       string
	PayloadJSON json.RawMessage
	CreatedAt   time.Time
	AvailableAt time.Time
	PublishedAt *time.Time
	LastError   string
}
