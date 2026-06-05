package engine

import (
	"context"
	"fmt"

	enginejob "onec-integration/internal/engine/job"
)

type OutboxObservability struct {
	Pending int
	Delayed int
	Failed  int
}

type ObservabilitySnapshot struct {
	JobsByStatus map[string]int
	Outbox       OutboxObservability
}

type ObservabilityRepository interface {
	CountJobsByStatus(ctx context.Context) (map[enginejob.Status]int, error)
	GetOutboxObservability(ctx context.Context) (OutboxObservability, error)
}

type ObservabilityService struct {
	repository ObservabilityRepository
}

func NewObservabilityService(repository ObservabilityRepository) (*ObservabilityService, error) {
	if repository == nil {
		return nil, fmt.Errorf("observability repository is required")
	}
	return &ObservabilityService{repository: repository}, nil
}

func (s *ObservabilityService) Snapshot(ctx context.Context) (ObservabilitySnapshot, error) {
	if s == nil {
		return ObservabilitySnapshot{}, fmt.Errorf("observability service is nil")
	}

	jobCounts, err := s.repository.CountJobsByStatus(ctx)
	if err != nil {
		return ObservabilitySnapshot{}, fmt.Errorf("count jobs by status: %w", err)
	}

	outboxSnapshot, err := s.repository.GetOutboxObservability(ctx)
	if err != nil {
		return ObservabilitySnapshot{}, fmt.Errorf("get outbox observability: %w", err)
	}

	jobsByStatus := make(map[string]int, len(jobCounts))
	for status, count := range jobCounts {
		jobsByStatus[string(status)] = count
	}

	return ObservabilitySnapshot{
		JobsByStatus: jobsByStatus,
		Outbox:       outboxSnapshot,
	}, nil
}
