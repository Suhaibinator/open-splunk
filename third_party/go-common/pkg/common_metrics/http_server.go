package common_metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

const metricsServerShutdownTimeout = 5 * time.Second

// HTTPServer manages the private HTTP listener used for Prometheus scraping.
type HTTPServer struct {
	metricsHandler MetricsHandlerInterface
	logger         *zap.Logger

	mu       sync.Mutex
	server   *http.Server
	listener net.Listener
}

// NewHTTPServer returns a reusable metrics HTTP server wrapper for the provided handler.
func NewHTTPServer(metricsHandler MetricsHandlerInterface, logger *zap.Logger) *HTTPServer {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &HTTPServer{
		metricsHandler: metricsHandler,
		logger:         logger,
	}
}

// Start creates and serves the metrics HTTP listener when pull metrics are enabled.
func (s *HTTPServer) Start() error {
	if s == nil {
		return fmt.Errorf("metrics server is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.server != nil {
		return nil
	}
	if s.metricsHandler == nil {
		return fmt.Errorf("metrics server requires a metrics handler")
	}
	if !s.metricsHandler.IsEnabled() {
		return nil
	}

	handler := s.metricsHandler.GetHandler()
	if handler == nil {
		return fmt.Errorf("metrics server requires a non-nil HTTP handler")
	}

	addr := normalizeMetricsListenAddress(s.metricsHandler.GetPort())
	if addr == "" {
		return fmt.Errorf("metrics server port is empty")
	}

	endpoint := normalizeMetricsEndpoint(s.metricsHandler.GetEndpoint())
	mux := http.NewServeMux()
	mux.Handle(endpoint, handler)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics server listen error on %q: %w", addr, err)
	}

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	s.listener = listener
	s.server = server

	log := s.logger.Named(metricsHTTPServerLoggerName)
	go func(server *http.Server, listener net.Listener, endpoint string, log *zap.Logger) {
		log.Info("Starting metrics server",
			zap.String(appLogger.KeyAddr, listener.Addr().String()),
			zap.String(appLogger.KeyEndpoint, endpoint),
		)
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Error("Metrics server terminated unexpectedly",
				zap.String(logkeys.Operation, "serve_metrics"),
				zap.String(logkeys.Stage, "http_serve"),
				zap.String(appLogger.KeyAddr, listener.Addr().String()),
				zap.String(appLogger.KeyEndpoint, endpoint),
				zap.Error(err),
			)
		}
	}(server, listener, endpoint, log)

	return nil
}

// Close gracefully shuts down the metrics HTTP server if it was started.
func (s *HTTPServer) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	server := s.server
	s.server = nil
	s.listener = nil
	s.mu.Unlock()

	if server == nil {
		return nil
	}

	s.logger.Named(metricsHTTPServerLoggerName).Info("Shutting down metrics server")
	ctx, cancel := context.WithTimeout(context.Background(), metricsServerShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("metrics server shutdown error: %w", err)
	}

	return nil
}

func normalizeMetricsListenAddress(port string) string {
	port = strings.TrimSpace(port)
	if port == "" {
		return ""
	}
	if strings.HasPrefix(port, ":") || strings.Contains(port, ":") {
		return port
	}
	return ":" + port
}

func normalizeMetricsEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "/metrics"
	}
	if !strings.HasPrefix(endpoint, "/") {
		return "/" + endpoint
	}
	return endpoint
}
