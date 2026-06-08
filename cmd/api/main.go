package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"onec-integration/internal/engine"
	"onec-integration/internal/gateway"
	integrationhandlers "onec-integration/internal/handlers/integration"
	"onec-integration/internal/idgen"
	"onec-integration/internal/logger"
	postgresrepository "onec-integration/internal/repository/postgres"
	"onec-integration/internal/storage"
	"onec-integration/internal/telemetry"
	httpmiddleware "onec-integration/internal/transport/http/middleware"
	httpserver "onec-integration/internal/transport/http/server"
	"onec-integration/internal/workflow"

	"github.com/go-chi/chi/v5"
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

	telemetryProvider, err := telemetry.Init(ctx, telemetry.NewConfigMust(), "onec-integration-api")
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

	jobRepository := postgresrepository.NewJobRepository(postgresPool)
	inboxRepository := postgresrepository.NewInboxRepository(postgresPool)
	outboxRepository := postgresrepository.NewOutboxRepository(postgresPool)
	auditRepository := postgresrepository.NewJobAuditRepository(postgresPool)
	observabilityRepository := postgresrepository.NewObservabilityRepository(postgresPool)
	workflowRegistry := workflow.MustNewRegistry()
	engineService, err := engine.NewService(
		jobRepository,
		inboxRepository,
		storage.NoopStorage{},
		workflowRegistry,
		idgen.NewUUIDGenerator(),
	)
	if err != nil {
		log.Error("failed to initialize engine service", zap.Error(err))
		os.Exit(1)
	}
	dlqService, err := engine.NewDLQService(jobRepository, outboxRepository, auditRepository, idgen.NewUUIDGenerator())
	if err != nil {
		log.Error("failed to initialize dlq service", zap.Error(err))
		os.Exit(1)
	}
	observabilityService, err := engine.NewObservabilityService(observabilityRepository)
	if err != nil {
		log.Error("failed to initialize observability service", zap.Error(err))
		os.Exit(1)
	}

	router := chi.NewRouter()
	gw := gateway.NewRouter(router, engineService)

	router.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"service":"onec-integration","status":"ok"}`))
	})

	router.Get("/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := postgresPool.Ping(r.Context()); err != nil {
			http.Error(w, `{"status":"postgres_unavailable"}`, http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"service":"onec-integration","status":"ready"}`))
	})

	integrationhandlers.RegisterRoutes(gw)
	integrationhandlers.RegisterDLQRoutes(router, dlqService)
	integrationhandlers.RegisterObservabilityRoutes(router, observabilityService)

	server := httpserver.NewHTTPServer(
		httpserver.NewConfigMust(),
		log,
		router,
		httpmiddleware.RequestID(),
		httpmiddleware.CorrelationID(),
		httpmiddleware.Telemetry(),
		httpmiddleware.Logger(log),
		httpmiddleware.Panic(),
		httpmiddleware.Trace(),
	)

	if err := server.Run(ctx); err != nil {
		log.Error("http server run error", zap.Error(err))
		os.Exit(1)
	}
}
