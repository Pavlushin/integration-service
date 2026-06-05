package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	enginejob "onec-integration/internal/engine/job"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobRepository struct {
	pool *pgxpool.Pool
}

func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

func (r *JobRepository) Create(ctx context.Context, job enginejob.Job) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres job repository is not initialized")
	}

	_, err := r.pool.Exec(ctx, `
		INSERT INTO integration_jobs (
			id, correlation_id, parent_id, type, kind, direction, status,
			dedupe_key, idempotency_key, payload_json, result_path, attempts,
			last_error, created_at, started_at, finished_at
		) VALUES (
			$1, $2, NULLIF($3, ''), $4, $5, $6, $7,
			$8, NULLIF($9, ''), $10, NULLIF($11, ''), $12,
			NULLIF($13, ''), $14, $15, $16
		)
	`,
		job.ID,
		job.CorrelationID,
		job.ParentID,
		job.Type,
		string(job.Kind),
		string(job.Direction),
		string(job.Status),
		job.DedupeKey,
		job.IdempotencyKey,
		job.PayloadJSON,
		job.ResultPath,
		job.Attempts,
		job.LastError,
		job.CreatedAt,
		job.StartedAt,
		job.FinishedAt,
	)
	if err != nil {
		return fmt.Errorf("insert integration job: %w", err)
	}

	return nil
}

func (r *JobRepository) GetByID(ctx context.Context, id string) (enginejob.Job, error) {
	if r == nil || r.pool == nil {
		return enginejob.Job{}, fmt.Errorf("postgres job repository is not initialized")
	}

	row := r.pool.QueryRow(ctx, `
		SELECT
			id, correlation_id, COALESCE(parent_id, ''), type, kind, direction, status,
			dedupe_key, COALESCE(idempotency_key, ''), payload_json, COALESCE(result_path, ''),
			attempts, COALESCE(last_error, ''), created_at, started_at, finished_at
		FROM integration_jobs
		WHERE id = $1
	`, id)

	job, err := scanJob(row.Scan)
	if err != nil {
		return enginejob.Job{}, fmt.Errorf("select integration job by id: %w", err)
	}

	return job, nil
}

func (r *JobRepository) FindActiveByDedupeKey(ctx context.Context, dedupeKey string) (enginejob.Job, bool, error) {
	if r == nil || r.pool == nil {
		return enginejob.Job{}, false, fmt.Errorf("postgres job repository is not initialized")
	}

	row := r.pool.QueryRow(ctx, `
		SELECT
			id, correlation_id, COALESCE(parent_id, ''), type, kind, direction, status,
			dedupe_key, COALESCE(idempotency_key, ''), payload_json, COALESCE(result_path, ''),
			attempts, COALESCE(last_error, ''), created_at, started_at, finished_at
		FROM integration_jobs
		WHERE dedupe_key = $1
		  AND status IN ('received', 'validated', 'prepared', 'delivering', 'retrying')
		ORDER BY created_at DESC
		LIMIT 1
	`, dedupeKey)

	job, err := scanJob(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return enginejob.Job{}, false, nil
		}
		return enginejob.Job{}, false, fmt.Errorf("select active integration job by dedupe key: %w", err)
	}

	return job, true, nil
}

func (r *JobRepository) UpdateStatus(ctx context.Context, jobID string, status enginejob.Status, lastError string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres job repository is not initialized")
	}

	_, err := r.pool.Exec(ctx, `
		UPDATE integration_jobs
		SET status = $2, last_error = NULLIF($3, '')
		WHERE id = $1
	`, jobID, string(status), lastError)
	if err != nil {
		return fmt.Errorf("update integration job status: %w", err)
	}

	return nil
}

func (r *JobRepository) UpdateResult(ctx context.Context, jobID string, resultPath string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres job repository is not initialized")
	}

	_, err := r.pool.Exec(ctx, `
		UPDATE integration_jobs
		SET result_path = NULLIF($2, '')
		WHERE id = $1
	`, jobID, resultPath)
	if err != nil {
		return fmt.Errorf("update integration job result path: %w", err)
	}

	return nil
}

func (r *JobRepository) IncrementAttempts(ctx context.Context, jobID string, lastError string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres job repository is not initialized")
	}

	_, err := r.pool.Exec(ctx, `
		UPDATE integration_jobs
		SET attempts = attempts + 1, last_error = NULLIF($2, '')
		WHERE id = $1
	`, jobID, lastError)
	if err != nil {
		return fmt.Errorf("increment integration job attempts: %w", err)
	}

	return nil
}

type rowScanner func(dest ...any) error

func scanJob(scan rowScanner) (enginejob.Job, error) {
	var (
		job         enginejob.Job
		kind        string
		direction   string
		status      string
		payloadJSON json.RawMessage
		startedAt   *time.Time
		finishedAt  *time.Time
	)

	err := scan(
		&job.ID,
		&job.CorrelationID,
		&job.ParentID,
		&job.Type,
		&kind,
		&direction,
		&status,
		&job.DedupeKey,
		&job.IdempotencyKey,
		&payloadJSON,
		&job.ResultPath,
		&job.Attempts,
		&job.LastError,
		&job.CreatedAt,
		&startedAt,
		&finishedAt,
	)
	if err != nil {
		return enginejob.Job{}, err
	}

	job.Kind = enginejob.Kind(kind)
	job.Direction = enginejob.Direction(direction)
	job.Status = enginejob.Status(status)
	job.PayloadJSON = payloadJSON
	job.StartedAt = startedAt
	job.FinishedAt = finishedAt

	return job, nil
}
