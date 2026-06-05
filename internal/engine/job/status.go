package job

type Status string

const (
	StatusReceived   Status = "received"
	StatusValidated  Status = "validated"
	StatusPrepared   Status = "prepared"
	StatusDelivering Status = "delivering"
	StatusDone       Status = "done"
	StatusRetrying   Status = "retrying"
	StatusFailed     Status = "failed"
	StatusDLQ        Status = "dlq"
)

func (s Status) IsTerminal() bool {
	switch s {
	case StatusDone, StatusFailed, StatusDLQ:
		return true
	default:
		return false
	}
}

func (s Status) IsActive() bool {
	return !s.IsTerminal()
}
