package logger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.Logger

	file *os.File
}

type Config struct {
	Level  string `envconfig:"LEVEL" default:"info"`
	Folder string `envconfig:"FOLDER" default:"out/logs"`
}

func NewConfig() (Config, error) {
	var config Config

	if err := envconfig.Process("LOG", &config); err != nil {
		return Config{}, fmt.Errorf("process log config: %w", err)
	}

	return config, nil
}

func NewConfigMust() Config {
	config, err := NewConfig()
	if err != nil {
		panic(fmt.Errorf("get log config: %w", err))
	}

	return config
}

func NewLogger(config Config) (*Logger, error) {
	level := zap.NewAtomicLevel()
	if err := level.UnmarshalText([]byte(config.Level)); err != nil {
		return nil, fmt.Errorf("unmarshal log level: %w", err)
	}

	if err := os.MkdirAll(config.Folder, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir log folder: %w", err)
	}

	timestamp := time.Now().UTC().Format("2006-01-02T15-04-05.000000")
	logFilePath := filepath.Join(config.Folder, timestamp+".log")

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	zapConfig := zap.NewDevelopmentEncoderConfig()
	zapConfig.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02T15:04:05.000000")
	encoder := zapcore.NewConsoleEncoder(zapConfig)

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level),
		zapcore.NewCore(encoder, zapcore.AddSync(logFile), level),
	)

	zapLogger := zap.New(core, zap.AddCaller())

	return &Logger{
		Logger: zapLogger,
		file:   logFile,
	}, nil
}

func parseLevel(level string) zapcore.Level {
	parsed := zapcore.InfoLevel
	if err := parsed.UnmarshalText([]byte(level)); err != nil {
		return zapcore.InfoLevel
	}

	return parsed
}

type contextKey string

const loggerContextKey contextKey = "logger"

func IntoContext(ctx context.Context, log *Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey, log)
}

func FromContext(ctx context.Context) *Logger {
	log, ok := ctx.Value(loggerContextKey).(*Logger)
	if !ok || log == nil {
		return &Logger{Logger: zap.NewNop()}
	}

	return log
}

func (l *Logger) With(fields ...zap.Field) *Logger {
	if l == nil || l.Logger == nil {
		return &Logger{Logger: zap.NewNop()}
	}

	return &Logger{
		Logger: l.Logger.With(fields...),
		file:   l.file,
	}
}

func (l *Logger) Close() error {
	if l == nil {
		return nil
	}

	if l.Logger != nil {
		_ = l.Logger.Sync()
	}

	if l.file != nil {
		if err := l.file.Close(); err != nil {
			return fmt.Errorf("close log file: %w", err)
		}
	}

	return nil
}
