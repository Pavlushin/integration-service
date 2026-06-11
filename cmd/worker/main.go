package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	core_repository_rabbitMQ "onec-integration/core/repository/rabbitMQ"
	core_repository_rabbitMQ_queue "onec-integration/core/repository/rabbitMQ/queue"
	"onec-integration/internal/engine"
	"onec-integration/internal/idgen"
	"onec-integration/internal/logger"
	"onec-integration/internal/outbox"
	"onec-integration/internal/queue"
	postgresrepository "onec-integration/internal/repository/postgres"
	"onec-integration/internal/storage"
	"onec-integration/internal/telemetry"
	"onec-integration/internal/worksheets/usersreports"
	"onec-integration/internal/worksheets/usersreports/lkdb"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

func main() {
	_ = godotenv.Load()

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

	log, err := logger.NewLogger(logger.NewConfigMust())
	if err != nil {
		_, _ = os.Stderr.WriteString("failed to initialize logger: " + err.Error() + "\n")
		os.Exit(1)
	}
	defer func() {
		_ = log.Close()
	}()

	ctx = logger.IntoContext(ctx, log)

	telemetryProvider, err := telemetry.Init(ctx, telemetry.NewConfigMust(), "onec-integration-worker")
	if err != nil {
		log.Error("failed to initialize telemetry", zap.Error(err))
		os.Exit(1)
	}
	defer func() {
		_ = telemetryProvider.Shutdown(context.Background())
	}()

	postgresPool, err := postgresrepository.NewPool(ctx, postgresrepository.NewConfigMust())
	if err != nil {
		log.Error("failed to connect to postgres", zap.Error(err))
		os.Exit(1)
	}
	defer postgresPool.Close()

	rabbitConfig := core_repository_rabbitMQ.NewConfigMust()

	rabbitClient, err := core_repository_rabbitMQ_queue.NewClient(rabbitConfig)
	if err != nil {
		log.Error("failed to connect to rabbitMQ", zap.Error(err))
		os.Exit(1)
	}
	defer func() {
		if err := rabbitClient.Close(); err != nil {
			log.Error("failed to close rabbitMQ connection", zap.Error(err))
		}
	}()

	log.Info(
		"rabbitMQ connected",
		zap.String("host", rabbitConfig.Host),
		zap.String("port", rabbitConfig.Port),
	)

	if err := core_repository_rabbitMQ_queue.DeclareQueue(rabbitClient, rabbitConfig.PrepareQueue); err != nil {
		log.Error("failed to declare rabbitmq prepare queue", zap.Error(err), zap.String("queue", rabbitConfig.PrepareQueue))
		os.Exit(1)
	}
	if err := core_repository_rabbitMQ_queue.DeclareQueue(rabbitClient, rabbitConfig.DeliveryQueue); err != nil {
		log.Error("failed to declare rabbitmq delivery queue", zap.Error(err), zap.String("queue", rabbitConfig.DeliveryQueue))
		os.Exit(1)
	}

	log.Info("rabbitmq queue declared", zap.String("queue", rabbitConfig.PrepareQueue))
	log.Info("rabbitmq queue declared", zap.String("queue", rabbitConfig.DeliveryQueue))

	log.Info("postgres connected")

	lkPool, err := lkdb.NewPool(ctx, lkdb.NewConfigMust())
	if err != nil {
		log.Error("failed to connect to lk mariadb", zap.Error(err))
		os.Exit(1)
	}
	defer lkPool.Close()
	log.Info("lk mariadb connected")

	jobRepository := postgresrepository.NewJobRepository(postgresPool)
	outboxRepository := postgresrepository.NewOutboxRepository(postgresPool)
	fileStorage, err := storage.NewFilesystemStorage("out/json")
	if err != nil {
		log.Error("failed to initialize file storage", zap.Error(err))
		os.Exit(1)
	}
	lkConfig := lkdb.NewConfigMust()
	worksheetsStorage, err := lkdb.NewStorage(lkPool, lkConfig.NextDB)
	if err != nil {
		log.Error("failed to initialize lk worksheets storage", zap.Error(err))
		os.Exit(1)
	}
	worksheetsExporter, err := usersreports.NewService(worksheetsStorage)
	if err != nil {
		log.Error("failed to initialize worksheets exporter", zap.Error(err))
		os.Exit(1)
	}
	retryPolicy := engine.NewRetryConfigMust().Policy()
	publisher := core_repository_rabbitMQ_queue.NewProducer(rabbitClient)
	dispatcher, err := outbox.NewDispatcher(outboxRepository, publisher, time.Second, 100)
	if err != nil {
		log.Error("failed to initialize outbox dispatcher", zap.Error(err))
		os.Exit(1)
	}
	consumer := core_repository_rabbitMQ_queue.NewConsumer(rabbitClient)
	prepareProcessor, err := engine.NewPrepareProcessor(jobRepository, outboxRepository, idgen.NewUUIDGenerator(), fileStorage, worksheetsExporter, retryPolicy)
	if err != nil {
		log.Error("failed to initialize prepare processor", zap.Error(err))
		os.Exit(1)
	}
	deliveryProcessor, err := engine.NewDeliveryProcessor(jobRepository, outboxRepository, idgen.NewUUIDGenerator(), fileStorage, retryPolicy)
	if err != nil {
		log.Error("failed to initialize delivery processor", zap.Error(err))
		os.Exit(1)
	}

	log.Info("outbox dispatcher started", zap.String("queue", rabbitConfig.PrepareQueue))
	log.Info("prepare consumer started", zap.String("queue", rabbitConfig.PrepareQueue))
	log.Info("delivery consumer started", zap.String("queue", rabbitConfig.DeliveryQueue))

	errCh := make(chan error, 3)

	go func() {
		if err := dispatcher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	go func() {
		err := consumer.ConsumeJobs(ctx, rabbitConfig.PrepareQueue, func(ctx context.Context, message queue.Message) error {
			log.Info("prepare job received", zap.String("job_id", message.JobID))
			return prepareProcessor.Process(ctx, message.JobID)
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	go func() {
		err := consumer.ConsumeJobs(ctx, rabbitConfig.DeliveryQueue, func(ctx context.Context, message queue.Message) error {
			log.Info("delivery job received", zap.String("job_id", message.JobID))
			return deliveryProcessor.Process(ctx, message.JobID)
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		log.Error("worker stopped with error", zap.Error(err))
		os.Exit(1)
	}
}
