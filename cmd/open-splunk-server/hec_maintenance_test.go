package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type hecMaintenancePruneCall struct {
	terminalBefore time.Time
	limit          uint32
}

type hecMaintenanceTestPruner struct {
	mu      sync.Mutex
	calls   []hecMaintenancePruneCall
	results []uint32
	err     error
	called  chan struct{}
}

func (pruner *hecMaintenanceTestPruner) PruneHECTerminalRequests(
	_ context.Context,
	terminalBefore time.Time,
	limit uint32,
) (uint32, error) {
	pruner.mu.Lock()
	pruner.calls = append(pruner.calls, hecMaintenancePruneCall{terminalBefore: terminalBefore, limit: limit})
	index := len(pruner.calls) - 1
	result := uint32(0)
	if index < len(pruner.results) {
		result = pruner.results[index]
	}
	err := pruner.err
	called := pruner.called
	pruner.mu.Unlock()
	if called != nil {
		select {
		case called <- struct{}{}:
		default:
		}
	}
	return result, err
}

func (pruner *hecMaintenanceTestPruner) snapshot() []hecMaintenancePruneCall {
	pruner.mu.Lock()
	defer pruner.mu.Unlock()
	return append([]hecMaintenancePruneCall(nil), pruner.calls...)
}

func TestHECTerminalMaintenanceRunsImmediateBoundedRetention(t *testing.T) {
	now := time.Date(2026, time.August, 10, 20, 30, 0, 0, time.UTC)
	pruner := &hecMaintenanceTestPruner{
		results: []uint32{2, 2, 1},
		called:  make(chan struct{}, 4),
	}
	config := defaultHECTerminalMaintenanceConfig()
	config.batchSize = 2
	config.maximumBatches = 4
	config.now = func() time.Time { return now }
	config.ticks = make(chan time.Time)
	maintenance, err := newHECTerminalMaintenance(pruner, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = maintenance.Close(context.Background()) }()
	for range 3 {
		select {
		case <-pruner.called:
		case <-time.After(2 * time.Second):
			t.Fatal("immediate HEC terminal prune did not finish")
		}
	}
	calls := pruner.snapshot()
	if len(calls) != 3 {
		t.Fatalf("HEC terminal prune calls = %d, want 3", len(calls))
	}
	wantBefore := now.Add(-24 * time.Hour)
	for index, call := range calls {
		if !call.terminalBefore.Equal(wantBefore) || call.limit != 2 {
			t.Fatalf("HEC terminal prune call %d = %+v, want cutoff %s limit 2", index, call, wantBefore)
		}
	}
}

func TestHECTerminalMaintenanceTickReportsBoundedErrorAndCloses(t *testing.T) {
	ticks := make(chan time.Time, 1)
	called := make(chan struct{}, 1)
	pruneErr := errors.New("private SQLite details")
	pruner := &hecMaintenanceTestPruner{err: pruneErr, called: called}
	reported := make(chan error, 1)
	config := defaultHECTerminalMaintenanceConfig()
	config.runImmediately = false
	config.ticks = ticks
	config.onError = func(err error) { reported <- err }
	maintenance, err := newHECTerminalMaintenance(pruner, config)
	if err != nil {
		t.Fatal(err)
	}
	ticks <- time.Now()
	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("tick did not run HEC terminal prune")
	}
	select {
	case err := <-reported:
		if !errors.Is(err, pruneErr) {
			t.Fatalf("reported error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HEC terminal prune error was not reported")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := maintenance.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("idempotent Close(): %v", err)
	}
}

func TestHECTerminalMaintenanceErrorCallbackCannotDelayWorkOrShutdown(t *testing.T) {
	ticks := make(chan time.Time, 2)
	called := make(chan struct{}, 2)
	pruner := &hecMaintenanceTestPruner{err: errors.New("prune failed"), called: called}
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	defer close(releaseCallback)
	config := defaultHECTerminalMaintenanceConfig()
	config.runImmediately = false
	config.ticks = ticks
	config.onError = func(error) {
		close(callbackStarted)
		<-releaseCallback
	}
	maintenance, err := newHECTerminalMaintenance(pruner, config)
	if err != nil {
		t.Fatal(err)
	}
	ticks <- time.Now()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("error callback did not start")
	}
	ticks <- time.Now()
	for range 2 {
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Fatal("maintenance work waited for error callback")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestHECTerminalMaintenanceContainsErrorCallbackPanic(t *testing.T) {
	ticks := make(chan time.Time, 1)
	pruner := &hecMaintenanceTestPruner{err: errors.New("prune failed")}
	callbackStarted := make(chan struct{})
	config := defaultHECTerminalMaintenanceConfig()
	config.runImmediately = false
	config.ticks = ticks
	config.onError = func(error) {
		close(callbackStarted)
		panic("callback failure")
	}
	maintenance, err := newHECTerminalMaintenance(pruner, config)
	if err != nil {
		t.Fatal(err)
	}
	ticks <- time.Now()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("error callback did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("Close() after callback panic = %v", err)
	}
}

func TestHECTerminalMaintenanceRejectsInvalidComposition(t *testing.T) {
	valid := defaultHECTerminalMaintenanceConfig()
	if maintenance, err := newHECTerminalMaintenance(nil, valid); err == nil || maintenance != nil {
		t.Fatalf("nil pruner composition = (%v, %v)", maintenance, err)
	}
	invalid := valid
	invalid.retention = 0
	if maintenance, err := newHECTerminalMaintenance(&hecMaintenanceTestPruner{}, invalid); err == nil || maintenance != nil {
		t.Fatalf("invalid retention composition = (%v, %v)", maintenance, err)
	}
}
