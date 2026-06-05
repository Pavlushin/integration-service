package postgres

import (
	"context"
	"fmt"

	"onec-integration/internal/engine"
	enginejob "onec-integration/internal/engine/job"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ObservabilityRepository struct {
	pool *pgxpool.Pool
}

func NewObservabilityRepository(pool *pgxpool.Pool) *ObservabilityRepository {
	return &ObservabilityRepository{pool: pool}
}

func (r *ObservabilityRepository) CountJobsByStatus(ctx context.Context) (map[enginejob.Status]int, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("postgres observability repository is not initialized")
	}

	rows, err := r.pool.Query(ctx, `
		SELECT status, COUNT(*)::int
		FROM integration_jobs
		GROUP BY status
	`)
	if err != nil {
		return nil, fmt.Errorf("count jobs by status: %w", err)
	}
	defer rows.Close()

	counts := make(map[enginejob.Status]int)
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan job status count: %w", err)
		}
		counts[enginejob.Status(status)] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job status counts: %w", err)
	}

	return counts, nil
}

func (r *ObservabilityRepository) GetOutboxObservability(ctx context.Context) (engine.OutboxObservability, error) {
	if r == nil || r.pool == nil {
		return engine.OutboxObservability{}, fmt.Errorf("postgres observability repository is not initialized")
	}

	row := r.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE published_at IS NULL AND available_at <= NOW())::int AS pending,
			COUNT(*) FILTER (WHERE published_at IS NULL AND available_at > NOW())::int AS delayed,
			COUNT(*) FILTER (WHERE published_at IS NULL AND last_error IS NOT NULL)::int AS failed
		FROM integration_outbox
	`)

	var snapshot engine.OutboxObservability
	if err := row.Scan(&snapshot.Pending, &snapshot.Delayed, &snapshot.Failed); err != nil {
		return engine.OutboxObservability{}, fmt.Errorf("select outbox observability: %w", err)
	}

	return snapshot, nil
}
