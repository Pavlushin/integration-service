package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"onec-integration/internal/engine"

	"github.com/jackc/pgx/v5/pgxpool"
)

type JobAuditRepository struct {
	pool *pgxpool.Pool
}

func NewJobAuditRepository(pool *pgxpool.Pool) *JobAuditRepository {
	return &JobAuditRepository{pool: pool}
}

func (r *JobAuditRepository) RecordJobAudit(ctx context.Context, record engine.JobAuditRecord) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres job audit repository is not initialized")
	}
	if record.MetadataJSON == nil {
		record.MetadataJSON = json.RawMessage(`{}`)
	}

	_, err := r.pool.Exec(ctx, `
		INSERT INTO integration_job_audit (
			id, job_id, action, actor, reason, metadata_json, created_at
		) VALUES (
			$1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7
		)
	`,
		record.ID,
		record.JobID,
		record.Action,
		record.Actor,
		record.Reason,
		record.MetadataJSON,
		record.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert integration job audit record: %w", err)
	}

	return nil
}

func (r *JobAuditRepository) ListJobAudit(ctx context.Context, jobID string, limit int) ([]engine.JobAuditRecord, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("postgres job audit repository is not initialized")
	}
	if limit <= 0 {
		limit = 20
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, job_id, action, COALESCE(actor, ''), COALESCE(reason, ''), metadata_json, created_at
		FROM integration_job_audit
		WHERE job_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	`, jobID, limit)
	if err != nil {
		return nil, fmt.Errorf("select integration job audit records: %w", err)
	}
	defer rows.Close()

	var records []engine.JobAuditRecord
	for rows.Next() {
		var record engine.JobAuditRecord
		var metadataJSON json.RawMessage
		var createdAt time.Time
		if err := rows.Scan(
			&record.ID,
			&record.JobID,
			&record.Action,
			&record.Actor,
			&record.Reason,
			&metadataJSON,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan integration job audit record: %w", err)
		}
		record.MetadataJSON = metadataJSON
		record.CreatedAt = createdAt
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate integration job audit records: %w", err)
	}

	return records, nil
}
