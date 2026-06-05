package storage

import "context"

type NoopStorage struct{}

func (NoopStorage) Save(_ context.Context, key string, _ []byte) (string, error) {
	return key, nil
}

func (NoopStorage) Read(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}

func (NoopStorage) Delete(_ context.Context, _ string) error {
	return nil
}
