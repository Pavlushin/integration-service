package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"onec-integration/internal/logger"
	httpmiddleware "onec-integration/internal/transport/http/middleware"

	"go.uber.org/zap"
)

type HTTPServer struct {
	handler     http.Handler
	config      Config
	log         *logger.Logger
	middlewares []httpmiddleware.Middleware
}

func NewHTTPServer(
	config Config,
	log *logger.Logger,
	handler http.Handler,
	middlewares ...httpmiddleware.Middleware,
) *HTTPServer {
	return &HTTPServer{
		handler:     handler,
		config:      config,
		log:         log,
		middlewares: middlewares,
	}
}

func (s *HTTPServer) Run(ctx context.Context) error {
	handler := httpmiddleware.Chain(s.handler, s.middlewares...)

	server := &http.Server{
		Addr:              s.config.Addr,
		Handler:           handler,
		ReadHeaderTimeout: s.config.ShutdownTimeout,
	}

	ch := make(chan error, 1)

	go func() {
		defer close(ch)

		s.log.Info("start http server", zap.String("addr", s.config.Addr))
		err := server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			ch <- err
		}
	}()

	select {
	case err := <-ch:
		if err != nil {
			return fmt.Errorf("listen and serve http: %w", err)
		}
	case <-ctx.Done():
		s.log.Info("shutdown http server...", zap.String("addr", s.config.Addr))

		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return fmt.Errorf("shutdown http server: %w", err)
		}

		s.log.Info("shutdown http server complete", zap.String("addr", s.config.Addr))
	}

	return nil
}
