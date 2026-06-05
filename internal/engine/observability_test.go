package engine

import (
	"context"
	"testing"

	enginejob "onec-integration/internal/engine/job"
)

type observabilityRepository struct {
	jobCounts      map[enginejob.Status]int
	outboxSnapshot OutboxObservability
}

func (r observabilityRepository) CountJobsByStatus(context.Context) (map[enginejob.Status]int, error) {
	return r.jobCounts, nil
}

func (r observabilityRepository) GetOutboxObservability(context.Context) (OutboxObservability, error) {
	return r.outboxSnapshot, nil
}

func TestObservabilityServiceReturnsJobAndOutboxMetrics(t *testing.T) {
	service, err := NewObservabilityService(observabilityRepository{
		jobCounts: map[enginejob.Status]int{
			enginejob.StatusReceived: 1,
			enginejob.StatusRetrying: 2,
			enginejob.StatusDLQ:      3,
			enginejob.StatusDone:     4,
		},
		outboxSnapshot: OutboxObservability{
			Pending: 5,
			Delayed: 6,
			Failed:  7,
		},
	})
	if err != nil {
		t.Fatalf("create observability service: %v", err)
	}

	snapshot, err := service.Snapshot(context.Background())

	if err != nil {
		t.Fatalf("get observability snapshot: %v", err)
	}
	if snapshot.JobsByStatus["received"] != 1 {
		t.Fatalf("expected received=1, got %#v", snapshot.JobsByStatus)
	}
	if snapshot.JobsByStatus["retrying"] != 2 {
		t.Fatalf("expected retrying=2, got %#v", snapshot.JobsByStatus)
	}
	if snapshot.JobsByStatus["dlq"] != 3 {
		t.Fatalf("expected dlq=3, got %#v", snapshot.JobsByStatus)
	}
	if snapshot.JobsByStatus["done"] != 4 {
		t.Fatalf("expected done=4, got %#v", snapshot.JobsByStatus)
	}
	if snapshot.Outbox.Pending != 5 || snapshot.Outbox.Delayed != 6 || snapshot.Outbox.Failed != 7 {
		t.Fatalf("unexpected outbox snapshot: %#v", snapshot.Outbox)
	}
}
