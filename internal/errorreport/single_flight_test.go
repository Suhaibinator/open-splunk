package errorreport

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestReportDropsWhileCallbackRuns(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	reporter := SingleFlight{Callback: func(error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
	}}

	reporter.Report(errors.New("first"))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	reporter.Report(errors.New("dropped"))
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls while busy = %d, want 1", got)
	}
	close(release)
}

func TestReportRecoversPanicAndAcceptsLaterReports(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 8)
	var calls atomic.Int64
	reporter := SingleFlight{Callback: func(error) {
		calls.Add(1)
		started <- struct{}{}
		panic("callback failure")
	}}

	reporter.Report(errors.New("first"))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first callback did not start")
	}
	deadline := time.After(2 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("reporter never accepted a report after the panic")
		default:
		}
		reporter.Report(errors.New("second"))
		time.Sleep(time.Millisecond)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second callback did not start")
	}
}

func TestReportIgnoresNilInputs(t *testing.T) {
	t.Parallel()

	var nilReporter *SingleFlight
	nilReporter.Report(errors.New("boom"))

	var noCallback SingleFlight
	noCallback.Report(errors.New("boom"))

	reporter := SingleFlight{Callback: func(error) {
		t.Error("callback ran for a nil error")
	}}
	reporter.Report(nil)
}
