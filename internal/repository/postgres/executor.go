package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
)

type sqlExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}
