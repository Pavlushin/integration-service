package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type InboxRepository struct {
	pool *pgxpool.Pool
}

func NewInboxRepository(pool *pgxpool.Pool) *InboxRepository {
	return &InboxRepository{pool: pool}
}

func (r *InboxRepository) SaveResponse(ctx context.Context, source string, idempotencyKey string, responseJSON json.RawMessage) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("postgres inbox repository is not initialized")
	}

	return insertInboxResponse(ctx, r.pool, source, idempotencyKey, responseJSON, time.Now().UTC(), true)
}

func (r *InboxRepository) GetResponse(ctx context.Context, source string, idempotencyKey string) (json.RawMessage, bool, error) {
	if r == nil || r.pool == nil {
		return nil, false, fmt.Errorf("postgres inbox repository is not initialized")
	}

	return getInboxResponse(ctx, r.pool, source, idempotencyKey)
}

func insertInboxResponse(ctx context.Context, executor sqlExecutor, source string, idempotencyKey string, responseJSON json.RawMessage, createdAt time.Time, ignoreConflict bool) error {
	if source == "" {
		return fmt.Errorf("inbox source is required")
	}
	if idempotencyKey == "" {
		return fmt.Errorf("inbox idempotency key is required")
	}
	if len(responseJSON) == 0 {
		return fmt.Errorf("inbox response json is required")
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	sql := `
		INSERT INTO integration_inbox (
			source, idempotency_key, response_json, created_at
		) VALUES (
			$1, $2, $3, $4
		)
	`
	if ignoreConflict {
		sql += " ON CONFLICT (source, idempotency_key) DO NOTHING"
	}

	_, err := executor.Exec(ctx, sql, source, idempotencyKey, responseJSON, createdAt)
	if err != nil {
		return fmt.Errorf("insert integration inbox response: %w", err)
	}

	return nil
}

func getInboxResponse(ctx context.Context, executor sqlQueryExecutor, source string, idempotencyKey string) (json.RawMessage, bool, error) {
	row := executor.QueryRow(ctx, `
		SELECT response_json
		FROM integration_inbox
		WHERE source = $1
		  AND idempotency_key = $2
	`, source, idempotencyKey)

	var responseJSON json.RawMessage
	if err := row.Scan(&responseJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("select integration inbox response: %w", err)
	}

	return responseJSON, true, nil
}
