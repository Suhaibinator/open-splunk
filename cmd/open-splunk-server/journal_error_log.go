package main

import (
	"errors"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
)

// journalErrorRepeatWindow bounds how often one unchanged publication root
// cause is logged again. A broken retained-search directory fails every
// search the same way; one line per window with a repeat count keeps the log
// readable.
const journalErrorRepeatWindow = 5 * time.Minute

const (
	journalCauseUnsupportedFilesystem = "unsupported_filesystem"
	journalCauseOther                 = "other"
)

// journalErrorLogger reports search-job journal failures. An artifact
// publication failure (finalize_results) means a completed search lost its
// results, so it is logged at error level; every other journal failure is a
// warning and is never suppressed. Repeated publication failures with the same
// root cause (same cause text, which for filesystem faults includes the
// directory) are suppressed for journalErrorRepeatWindow; the suppressed count
// is reported on the next logged entry and by Flush at shutdown.
type journalErrorLogger struct {
	logger *zap.Logger
	now    func() time.Time
	window time.Duration

	mu         sync.Mutex
	lastKey    string
	lastJobID  string
	lastLogged time.Time
	suppressed uint64
}

func newJournalErrorLogger(logger *zap.Logger, now func() time.Time, window time.Duration) *journalErrorLogger {
	if logger == nil {
		logger = zap.NewNop()
	}
	if now == nil {
		now = time.Now
	}
	if window <= 0 {
		window = journalErrorRepeatWindow
	}
	return &journalErrorLogger{logger: logger, now: now, window: window}
}

// Report is the searchjobs.Config.OnJournalError hook.
func (reporter *journalErrorLogger) Report(err error) {
	if err == nil {
		return
	}
	journalErr, ok := errors.AsType[*searchjobs.JournalError](err)
	if !ok || journalErr == nil {
		reporter.logger.Warn("persist search-job history", zap.Error(err))
		return
	}
	fields := []zap.Field{
		zap.Error(err),
		zap.String("journal_operation", journalErr.Operation.String()),
		zap.String("cause_class", journalErrorCauseClass(err)),
	}
	if journalErr.JobID != "" {
		fields = append(fields, zap.String("job_id", journalErr.JobID))
	}
	if journalErr.Operation != searchjobs.JournalOperationFinalizeResults {
		reporter.logger.Warn("persist search-job history", fields...)
		return
	}

	key := ""
	if journalErr.Err != nil {
		key = journalErr.Err.Error()
	}
	now := reporter.now()
	reporter.mu.Lock()
	if key == reporter.lastKey && now.Sub(reporter.lastLogged) < reporter.window {
		reporter.suppressed++
		reporter.lastJobID = journalErr.JobID
		reporter.mu.Unlock()
		return
	}
	suppressed := reporter.suppressed
	reporter.lastKey = key
	reporter.lastJobID = journalErr.JobID
	reporter.lastLogged = now
	reporter.suppressed = 0
	reporter.mu.Unlock()

	if suppressed > 0 {
		fields = append(fields, zap.Uint64("suppressed_repeats", suppressed))
	}
	reporter.logger.Error("publish retained search results", fields...)
}

// Flush reports publication failures that were suppressed inside the current
// window and never summarized because no later entry was logged. It is called
// once at shutdown after the search manager has stopped.
func (reporter *journalErrorLogger) Flush() {
	reporter.mu.Lock()
	suppressed := reporter.suppressed
	key := reporter.lastKey
	lastJobID := reporter.lastJobID
	reporter.suppressed = 0
	reporter.mu.Unlock()
	if suppressed == 0 {
		return
	}
	reporter.logger.Error(
		"publish retained search results: repeated failures were suppressed",
		zap.Uint64("suppressed_repeats", suppressed),
		zap.String("last_job_id", lastJobID),
		zap.String("cause", key),
	)
}

func journalErrorCauseClass(err error) string {
	if errors.Is(err, privatefs.ErrUnsupportedFilesystem) {
		return journalCauseUnsupportedFilesystem
	}
	return journalCauseOther
}
