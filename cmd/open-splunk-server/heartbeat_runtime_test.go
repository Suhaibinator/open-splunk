package main

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
)

func newCommandHeartbeatRuntime(
	t *testing.T,
	fleet collectorfleet.HeartbeatWriter,
	heartbeatInterval time.Duration,
) *collectorfleet.HeartbeatRuntime {
	t.Helper()
	return newCommandHeartbeatRuntimeWithFlush(
		t,
		fleet,
		heartbeatInterval,
		5*time.Millisecond,
	)
}

func newCommandHeartbeatRuntimeWithFlush(
	t *testing.T,
	fleet collectorfleet.HeartbeatWriter,
	heartbeatInterval time.Duration,
	flushInterval time.Duration,
) *collectorfleet.HeartbeatRuntime {
	t.Helper()
	runtime, err := collectorfleet.NewHeartbeatRuntime(
		fleet,
		collectorfleet.HeartbeatRuntimeConfig{
			MaxCollectors:     collectorMaxActiveStreams,
			HeartbeatInterval: heartbeatInterval,
			StaleGrace:        2 * heartbeatInterval,
			FlushInterval:     flushInterval,
			WriteTimeout:      time.Second,
			MonotonicNow:      time.Now,
		},
	)
	if err != nil {
		t.Fatalf("NewHeartbeatRuntime(): %v", err)
	}
	t.Cleanup(func() {
		closeCommandHeartbeatRuntime(t, runtime)
	})
	return runtime
}

func closeCommandHeartbeatRuntime(
	t *testing.T,
	runtime *collectorfleet.HeartbeatRuntime,
) {
	t.Helper()
	closeContext, cancelClose := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancelClose()
	if err := runtime.Close(closeContext); err != nil {
		t.Errorf("HeartbeatRuntime.Close(): %v", err)
	}
}
