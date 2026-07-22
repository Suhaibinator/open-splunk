package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type trackedHandler struct {
	next http.Handler

	mu       sync.Mutex
	stopping bool
	active   sync.WaitGroup
}

func newTrackedHandler(next http.Handler) *trackedHandler {
	return &trackedHandler{next: next}
}

func (handler *trackedHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	handler.mu.Lock()
	if handler.stopping {
		handler.mu.Unlock()
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
	handler.active.Wait()
}

type shutdownServer interface {
	Shutdown(context.Context) error
	Close() error
}

// shutdownHTTPServer first permits graceful completion, then force-closes
// connections at the deadline. In either case it waits for every handler to
// return before run() closes the search manager or its databases.
func shutdownHTTPServer(server shutdownServer, requests *trackedHandler, timeout time.Duration) error {
	requests.stopAccepting()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

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
	requests.wait()
	return shutdownErr
}
