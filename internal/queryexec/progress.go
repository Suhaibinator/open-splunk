package queryexec

import (
	"context"
	"fmt"
	"sync"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// executionProgressReporter serializes ClickHouse progress packets into the
// optional search-job progress sink. The mutex is intentionally held across
// ReportProgress: finish then forms a lifetime barrier that prevents an
// in-flight sink call from outliving Executor.Execute.
type executionProgressReporter struct {
	mu     sync.Mutex
	sink   searchjobs.ProgressSink
	cancel context.CancelFunc

	stopped bool
	err     error
}

func startExecutionProgress(
	ctx context.Context,
	sink searchjobs.ResultSink,
) (context.Context, *executionProgressReporter, clickhousedriver.QueryOption) {
	return startExecutionProgressWith(ctx, sink, clickhousedriver.WithProgress)
}

func startExecutionProgressWith(
	ctx context.Context,
	sink searchjobs.ResultSink,
	withProgress func(func(*clickhousedriver.Progress)) clickhousedriver.QueryOption,
) (context.Context, *executionProgressReporter, clickhousedriver.QueryOption) {
	progressSink, ok := sink.(searchjobs.ProgressSink)
	if !ok {
		return ctx, nil, nil
	}
	driverContext, cancel := context.WithCancel(ctx)
	reporter := &executionProgressReporter{
		sink:   progressSink,
		cancel: cancel,
	}
	return driverContext, reporter, withProgress(reporter.report)
}

func (reporter *executionProgressReporter) report(progress *clickhousedriver.Progress) {
	if progress == nil {
		return
	}
	// ClickHouse owns progress and may reuse its storage after this callback.
	// Copy only the per-packet read deltas before entering any sink code.
	delta := searchjobs.ExecutionProgressDelta{
		ScannedRows:  progress.Rows,
		ScannedBytes: progress.Bytes,
	}
	if delta.ScannedRows == 0 && delta.ScannedBytes == 0 {
		return
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if reporter.stopped || reporter.err != nil {
		return
	}
	if err := reportExecutionProgress(reporter.sink, delta); err != nil {
		// The first sink failure owns the derived driver cancellation. Keeping
		// cancellation inside the lifetime lock ensures finish cannot return
		// while this callback is still active.
		reporter.err = err
		reporter.cancel()
	}
}

func reportExecutionProgress(sink searchjobs.ProgressSink, delta searchjobs.ExecutionProgressDelta) (err error) {
	completed := false
	defer func() {
		if !completed {
			// The completion flag detects panic(nil) even on runtimes where
			// recover cannot distinguish it from an ordinary return.
			_ = recover()
			// A progress sink is part of the executor result contract. Treat a
			// panic like any other malformed executor/sink interaction without
			// retaining the panic value, which may contain sensitive detail.
			err = fmt.Errorf("%w: progress sink panicked", searchjobs.ErrInvalidResult)
		}
	}()
	err = sink.ReportProgress(delta)
	completed = true
	return err
}

func (reporter *executionProgressReporter) finish(callerContext context.Context, driverErr error) error {
	reporter.mu.Lock()
	reporter.stopped = true
	progressErr := reporter.err
	reporter.err = nil
	cancel := reporter.cancel
	reporter.sink = nil
	reporter.cancel = nil
	reporter.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	// A cancellation explicitly requested by the caller is always the most
	// useful outcome. Otherwise a sink failure explains the derived driver
	// cancellation better than the error returned by ClickHouse.
	if callerErr := callerContext.Err(); callerErr != nil {
		return callerErr
	}
	if progressErr != nil {
		return progressErr
	}
	return driverErr
}
