package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"onec-integration/internal/outbox"

	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

func (r *OutboxRepository) Enqueue(ctx context.Context, record outbox.Record) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres outbox repository is not initialized")
	}

	return insertOutboxRecord(ctx, r.pool, record)
}

func insertOutboxRecord(ctx context.Context, executor sqlExecutor, record outbox.Record) error {
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if record.AvailableAt.IsZero() {
		record.AvailableAt = record.CreatedAt
	}

	_, err := executor.Exec(ctx, `
		INSERT INTO integration_outbox (
			id, topic, payload_json, created_at, available_at, published_at, last_error
		) VALUES (
			$1, $2, $3, $4, $5, $6, NULLIF($7, '')
		)
	`,
		record.ID,
		record.Topic,
		record.PayloadJSON,
		record.CreatedAt,
		record.AvailableAt,
		record.PublishedAt,
		record.LastError,
	)
	if err != nil {
		return fmt.Errorf("insert integration outbox record: %w", err)
	}

	return nil
}

func (r *OutboxRepository) ListPending(ctx context.Context, limit int) ([]outbox.Record, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("postgres outbox repository is not initialized")
	}
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, topic, payload_json, created_at, available_at, published_at, COALESCE(last_error, '')
		FROM integration_outbox
		WHERE published_at IS NULL
		  AND available_at <= NOW()
		ORDER BY available_at ASC, created_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("select pending outbox records: %w", err)
	}
	defer rows.Close()

	var records []outbox.Record
	for rows.Next() {
		var record outbox.Record
		var payload json.RawMessage

		if err := rows.Scan(
			&record.ID,
			&record.Topic,
			&payload,
			&record.CreatedAt,
			&record.AvailableAt,
			&record.PublishedAt,
			&record.LastError,
		); err != nil {
			return nil, fmt.Errorf("scan pending outbox record: %w", err)
		}

		record.PayloadJSON = payload
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox records: %w", err)
	}

	return records, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres outbox repository is not initialized")
	}

	now := time.Now().UTC()
	_, err := r.pool.Exec(ctx, `
		UPDATE integration_outbox
		SET published_at = $2, last_error = NULL
		WHERE id = $1
	`, id, now)
	if err != nil {
		return fmt.Errorf("mark outbox record published: %w", err)
	}

	return nil
}

func (r *OutboxRepository) MarkFailed(ctx context.Context, id string, lastError string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres outbox repository is not initialized")
	}

	_, err := r.pool.Exec(ctx, `
		UPDATE integration_outbox
		SET last_error = NULLIF($2, '')
		WHERE id = $1
	`, id, lastError)
	if err != nil {
		return fmt.Errorf("mark outbox record failed: %w", err)
	}

	return nil
}
