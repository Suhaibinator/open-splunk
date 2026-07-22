package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type runtimeHTTPServer interface {
	shutdownServer
	ListenAndServe() error
}

type runtimeGRPCServer interface {
	gracefulGRPCServer
	Serve(net.Listener) error
}

// serveRuntime starts the browser and optional collector transports together.
// An unexpected failure in either transport shuts down the other before this
// function returns, preserving the lifetime ordering of their shared stores.
func serveRuntime(
	ctx context.Context,
	httpServer runtimeHTTPServer,
	requests *trackedHandler,
	collectorServer runtimeGRPCServer,
	collectorListener net.Listener,
	timeout time.Duration,
) error {
	if ctx == nil || httpServer == nil || requests == nil {
		return errors.New("serve runtime: context, HTTP server, and request tracker are required")
	}
	if timeout <= 0 {
		return errors.New("serve runtime: shutdown timeout must be positive")
	}
	if (collectorServer == nil) != (collectorListener == nil) {
		return errors.New("serve runtime: collector server and listener must be configured together")
	}

	httpResults := make(chan error, 1)
	go func() { httpResults <- httpServer.ListenAndServe() }()
	var collectorResults chan error
	if collectorServer != nil {
		collectorResults = make(chan error, 1)
		go func() { collectorResults <- collectorServer.Serve(collectorListener) }()
	}

	var serveErr error
	httpResultRead := false
	collectorResultRead := false
	select {
	case <-ctx.Done():
	case err := <-httpResults:
		httpResultRead = true
		serveErr = unexpectedServeError("HTTP", normalizeHTTPServeError(err))
	case err := <-collectorResults:
		collectorResultRead = true
		serveErr = unexpectedServeError("collector gRPC", normalizeCollectorServeError(err))
	}

	var shutdownWG sync.WaitGroup
	httpShutdown := make(chan error, 1)
	shutdownWG.Add(1)
	go func() {
		defer shutdownWG.Done()
		httpShutdown <- shutdownHTTPServer(httpServer, requests, timeout)
	}()
	var collectorShutdown chan error
	if collectorServer != nil {
		collectorShutdown = make(chan error, 1)
		shutdownWG.Add(1)
		go func() {
			defer shutdownWG.Done()
			collectorShutdown <- shutdownGRPCServer(collectorServer, timeout)
		}()
	}
	shutdownWG.Wait()
	serveErr = errors.Join(serveErr, <-httpShutdown)
	if collectorShutdown != nil {
		serveErr = errors.Join(serveErr, <-collectorShutdown)
	}

	// Shutdown must make both Serve calls return before the shared search and
	// ingestion stores can be closed by run's deferred cleanup.
	if !httpResultRead {
		serveErr = errors.Join(serveErr, normalizeHTTPServeError(<-httpResults))
	}
	if collectorResults != nil && !collectorResultRead {
		serveErr = errors.Join(serveErr, normalizeCollectorServeError(<-collectorResults))
	}
	return serveErr
}

func unexpectedServeError(name string, err error) error {
	if err == nil {
		return fmt.Errorf("serve %s: server stopped unexpectedly", name)
	}
	return fmt.Errorf("serve %s: %w", name, err)
}

func normalizeHTTPServeError(err error) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func normalizeCollectorServeError(err error) error {
	if err == nil || errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}
