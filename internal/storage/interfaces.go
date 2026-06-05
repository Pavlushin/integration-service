package storage

import "context"

type Storage interface {
	Save(ctx context.Context, key string, data []byte) (string, error)
	Read(ctx context.Context, path string) ([]byte, error)
	Delete(ctx context.Context, path string) error
}
