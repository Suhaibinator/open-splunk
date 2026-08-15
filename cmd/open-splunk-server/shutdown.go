package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
)

type trackedHandler struct {
	next http.Handler

	mu       sync.Mutex
	stopping bool
	active   sync.WaitGroup

	drainStarted chan struct{}
	drained      chan struct{}
	drainOnce    sync.Once
}

func newTrackedHandler(next http.Handler) *trackedHandler {
	return &trackedHandler{
		next:         next,
		drainStarted: make(chan struct{}),
		drained:      make(chan struct{}),
	}
}

func (handler *trackedHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	handler.mu.Lock()
	if handler.stopping {
		handler.mu.Unlock()
		if ownsHECNamespace(request) {
			// HEC owns its shutdown response taxonomy and health surface. The
			// HEC lifecycle is already closed before tracked shutdown begins.
			handler.next.ServeHTTP(response, request)
			return
		}
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		response.Header().Set("Retry-After", "1")
		http.Error(response, "server is shutting down", http.StatusServiceUnavailable)
		return
	}
	handler.active.Add(1)
	handler.mu.Unlock()
	defer handler.active.Done()
	handler.next.ServeHTTP(response, request)
}

func (handler *trackedHandler) stopAccepting() {
	handler.mu.Lock()
	handler.stopping = true
	handler.mu.Unlock()
}

func (handler *trackedHandler) wait() {
	_ = handler.waitContext(context.Background())
}

func (handler *trackedHandler) waitContext(ctx context.Context) error {
	handler.drainOnce.Do(func() {
		close(handler.drainStarted)
		go func() {
			handler.active.Wait()
			close(handler.drained)
		}()
	})
	select {
	case <-handler.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func ownsHECNamespace(request *http.Request) bool {
	if request == nil || request.URL == nil {
		return false
	}
	for _, path := range []string{request.URL.EscapedPath(), request.URL.Path} {
		if path == "/services/collector" || strings.HasPrefix(path, "/services/collector/") {
			return true
		}
	}
	return false
}

type shutdownServer interface {
	Shutdown(context.Context) error
	Close() error
}

type webSocketShutdown interface {
	Close(context.Context) error
}

type httpAdmissionShutdown interface {
	BeginShutdown()
	Shutdown(context.Context) error
}

// shutdownHTTPServer first permits graceful completion, then force-closes
// connections at the deadline. Admission and handler drains share that bound;
// a handler that ignores cancellation must not make first-signal shutdown
// unbounded.
func shutdownHTTPServer(
	server shutdownServer,
	requests *trackedHandler,
	webSockets webSocketShutdown,
	timeout time.Duration,
	admissions ...httpAdmissionShutdown,
) error {
	if nilcheck.IsNil(webSockets) {
		return errors.New("shutdown HTTP server: websocket service is required")
	}
	if len(admissions) > 1 || len(admissions) == 1 && nilcheck.IsNil(admissions[0]) {
		return errors.New("shutdown HTTP server: admission lifecycle is invalid")
	}
	if len(admissions) == 1 {
		admissions[0].BeginShutdown()
	}
	requests.stopAccepting()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var webSocketErr error
	if err := webSockets.Close(ctx); err != nil {
		webSocketErr = fmt.Errorf("close search websocket service: %w", err)
	}
	shutdownErr := server.Shutdown(ctx)
	if shutdownErr != nil {
		closeErr := server.Close()
		if closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
			closeErr = fmt.Errorf("force close HTTP server: %w", closeErr)
		} else {
			closeErr = nil
		}
		shutdownErr = errors.Join(fmt.Errorf("graceful HTTP shutdown: %w", shutdownErr), closeErr)
	}
	var admissionErr error
	if len(admissions) == 1 {
		if err := admissions[0].Shutdown(ctx); err != nil {
			admissionErr = fmt.Errorf("drain HTTP admission lifecycle: %w", err)
		}
	}
	requestDrainErr := requests.waitContext(ctx)
	if requestDrainErr != nil {
		requestDrainErr = fmt.Errorf("drain HTTP handlers: %w", requestDrainErr)
	}
	return errors.Join(webSocketErr, shutdownErr, admissionErr, requestDrainErr)
}
