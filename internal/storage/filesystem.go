package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"onec-integration/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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

func (s *FilesystemStorage) Save(ctx context.Context, key string, data []byte) (string, error) {
	if s == nil {
		return "", fmt.Errorf("filesystem storage is nil")
	}
	ctx, span := telemetry.Tracer().Start(ctx, "storage.save_file")
	defer span.End()
	span.SetAttributes(
		attribute.String("file.path", key),
		attribute.Int("file.size", len(data)),
	)

	fullPath := filepath.Join(s.baseDir, key)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "mkdir storage dir")
		return "", fmt.Errorf("mkdir storage dir: %w", err)
	}

	if err := os.WriteFile(fullPath, data, 0o644); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "write storage file")
		return "", fmt.Errorf("write storage file: %w", err)
	}

	return fullPath, nil
}

func (s *FilesystemStorage) Read(ctx context.Context, path string) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("filesystem storage is nil")
	}
	ctx, span := telemetry.Tracer().Start(ctx, "storage.read_file")
	defer span.End()
	span.SetAttributes(attribute.String("file.path", path))

	data, err := os.ReadFile(path)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "read storage file")
		return nil, fmt.Errorf("read storage file: %w", err)
	}
	span.SetAttributes(attribute.Int("file.size", len(data)))

	return data, nil
}

func (s *FilesystemStorage) Delete(ctx context.Context, path string) error {
	if s == nil {
		return fmt.Errorf("filesystem storage is nil")
	}
	ctx, span := telemetry.Tracer().Start(ctx, "storage.delete_file")
	defer span.End()
	span.SetAttributes(attribute.String("file.path", path))

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		span.RecordError(err)
		span.SetStatus(codes.Error, "delete storage file")
		return fmt.Errorf("delete storage file: %w", err)
	}

	return nil
}
