package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

type FilesystemStorage struct {
	baseDir string
}

func NewFilesystemStorage(baseDir string) (*FilesystemStorage, error) {
	if baseDir == "" {
		return nil, fmt.Errorf("base dir is required")
	}

	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir storage base dir: %w", err)
	}

	return &FilesystemStorage{baseDir: baseDir}, nil
}

func (s *FilesystemStorage) Save(_ context.Context, key string, data []byte) (string, error) {
	if s == nil {
		return "", fmt.Errorf("filesystem storage is nil")
	}

	fullPath := filepath.Join(s.baseDir, key)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir storage dir: %w", err)
	}

	if err := os.WriteFile(fullPath, data, 0o644); err != nil {
		return "", fmt.Errorf("write storage file: %w", err)
	}

	return fullPath, nil
}

func (s *FilesystemStorage) Read(_ context.Context, path string) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("filesystem storage is nil")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read storage file: %w", err)
	}

	return data, nil
}

func (s *FilesystemStorage) Delete(_ context.Context, path string) error {
	if s == nil {
		return fmt.Errorf("filesystem storage is nil")
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete storage file: %w", err)
	}

	return nil
}
