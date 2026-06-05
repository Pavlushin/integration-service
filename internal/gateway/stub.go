package gateway

import (
	"context"
	"time"

	"onec-integration/internal/engine"
	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/idgen"
)

type StubHeavyStarter struct {
	ids idgen.UUIDGenerator
}

func NewStubHeavyStarter() StubHeavyStarter {
	return StubHeavyStarter{
		ids: idgen.NewUUIDGenerator(),
	}
}

func (s StubHeavyStarter) StartHeavy(_ context.Context, input engine.StartHeavyInput) (engine.StartHeavyResult, error) {
	now := time.Now().UTC()
	jobID := s.ids.NewID()

	return engine.StartHeavyResult{
		Job: enginejob.Job{
			ID:             jobID,
			CorrelationID:  input.CorrelationID,
			Type:           input.Type,
			Kind:           enginejob.KindPrepare,
			Direction:      input.Direction,
			Status:         enginejob.StatusReceived,
			DedupeKey:      input.DedupeKey,
			IdempotencyKey: input.IdempotencyKey,
			PayloadJSON:    input.PayloadJSON,
			CreatedAt:      now,
		},
	}, nil
}
