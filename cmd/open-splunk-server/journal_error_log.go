package main

import (
	"errors"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
)

// journalErrorRepeatWindow bounds how often one unchanged journal root cause
// is logged again. A broken retained-search directory fails every search the
// same way; one line per window with a repeat count keeps the log readable.
const journalErrorRepeatWindow = 5 * time.Minute

const (
	journalCauseUnsupportedFilesystem = "unsupported_filesystem"
	journalCauseOther                 = "other"
)

// journalErrorLogger reports search-job journal failures. An artifact
// publication failure means a completed search lost its results, so it is an
// error; metadata-only projection failures remain warnings. Repeats of the same
// root cause (same operation and same cause text, which for filesystem faults
// includes the directory) are suppressed for journalErrorRepeatWindow and
// summarized on the next distinct or post-window entry.
type journalErrorLogger struct {
	logger *zap.Logger
	now    func() time.Time
	window time.Duration

	mu         sync.Mutex
	lastKey    string
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
	key, operation, jobID, publication := journalErrorIdentity(err)
	now := reporter.now()

	reporter.mu.Lock()
	if key == reporter.lastKey && now.Sub(reporter.lastLogged) < reporter.window {
		reporter.suppressed++
		reporter.mu.Unlock()
		return
	}
	suppressed := reporter.suppressed
	reporter.lastKey = key
	reporter.lastLogged = now
	reporter.suppressed = 0
	reporter.mu.Unlock()

	fields := []zap.Field{
		zap.Error(err),
		zap.String("journal_operation", operation),
		zap.String("cause_class", journalErrorCauseClass(err)),
	}
	if jobID != "" {
		fields = append(fields, zap.String("job_id", jobID))
	}
	if suppressed > 0 {
		fields = append(fields, zap.Uint64("suppressed_repeats", suppressed))
	}
	if publication {
		reporter.logger.Error("publish retained search results", fields...)
		return
	}
	reporter.logger.Warn("persist search-job history", fields...)
}

// journalErrorIdentity derives the suppression key and log fields. The key
// excludes the job ID so that one root cause failing every job collapses.
func journalErrorIdentity(err error) (key string, operation string, jobID string, publication bool) {
	journalErr, ok := errors.AsType[*searchjobs.JournalError](err)
	if !ok || journalErr == nil {
		return err.Error(), "", "", false
	}
	operation = journalErr.Operation.String()
	cause := ""
	if journalErr.Err != nil {
		cause = journalErr.Err.Error()
	}
	publication = journalErr.Operation == searchjobs.JournalOperationFinalizeResults
	return operation + "\x00" + cause, operation, journalErr.JobID, publication
}

func journalErrorCauseClass(err error) string {
	if errors.Is(err, privatefs.ErrUnsupportedFilesystem) {
		return journalCauseUnsupportedFilesystem
	}
	return journalCauseOther
}
