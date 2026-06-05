package workflow

import (
	"context"
	"encoding/json"

	enginejob "onec-integration/internal/engine/job"
)

type PrepareResult struct {
	ResultData      []byte
	DeliveryPayload json.RawMessage
}

type DeliveryInput struct {
	Job        enginejob.Job
	ResultData []byte
}

type Workflow interface {
	Type() string
	Prepare(ctx context.Context, job enginejob.Job) (PrepareResult, error)
	Deliver(ctx context.Context, input DeliveryInput) error
}
