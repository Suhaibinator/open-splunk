package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sys/unix"
)

func newJournalErrorTestLogger() (*zap.Logger, *bytes.Buffer) {
	var output bytes.Buffer
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&output),
		zapcore.DebugLevel,
	))
	return logger, &output
}

func decodeJournalErrorLines(t *testing.T, output *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for raw := range strings.SplitSeq(strings.TrimSpace(output.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("decode log line %q: %v", raw, err)
		}
		lines = append(lines, line)
	}
	return lines
}

func unsupportedPublicationError(jobID string) error {
	unsupported := &privatefs.UnsupportedFilesystemError{
		Operation:  "no-replace rename",
		Directory:  "/var/lib/open-splunk/state/open-splunk.db.search-artifacts",
		Filesystem: "nfs",
		Err:        unix.EINVAL,
	}
	return &searchjobs.JournalError{
		Operation: searchjobs.JournalOperationFinalizeResults,
		JobID:     jobID,
		State:     searchjobs.StateCompleted,
		Err:       fmt.Errorf("%w: %w", searchjobs.ErrResultStorageUnsupported, unsupported),
	}
}

func TestJournalErrorLoggerReportsPublicationFailureAsErrorWithJobID(t *testing.T) {
	t.Parallel()

	logger, output := newJournalErrorTestLogger()
	reporter := newJournalErrorLogger(logger, func() time.Time { return time.Unix(1_700_000_000, 0) }, time.Minute)
	reporter.Report(unsupportedPublicationError("search_first"))

	lines := decodeJournalErrorLines(t, output)
	if len(lines) != 1 {
		t.Fatalf("log lines = %d, want 1: %s", len(lines), output.String())
	}
	line := lines[0]
	if line["level"] != "error" || line["msg"] != "publish retained search results" {
		t.Fatalf("publication failure line = %v", line)
	}
	if line["job_id"] != "search_first" || line["journal_operation"] != "finalize_results" ||
		line["cause_class"] != journalCauseUnsupportedFilesystem {
		t.Fatalf("publication failure fields = %v", line)
	}
	errorText, _ := line["error"].(string)
	for _, want := range []string{"open-splunk.db.search-artifacts", "nfs", "unsupported by the filesystem"} {
		if !strings.Contains(errorText, want) {
			t.Fatalf("error field %q lacks %q", errorText, want)
		}
	}
	if _, suppressed := line["suppressed_repeats"]; suppressed {
		t.Fatalf("first entry reported suppressed repeats: %v", line)
	}
}

func TestJournalErrorLoggerSuppressesRepeatedRootCause(t *testing.T) {
	t.Parallel()

	logger, output := newJournalErrorTestLogger()
	now := time.Unix(1_700_000_000, 0)
	reporter := newJournalErrorLogger(logger, func() time.Time { return now }, 5*time.Minute)

	// One thousand searches failing the same way within a window (100 ms apart,
	// 100 s in total) must produce exactly one line.
	for index := range 1_000 {
		reporter.Report(unsupportedPublicationError(fmt.Sprintf("search_%04d", index)))
		now = now.Add(100 * time.Millisecond)
	}
	if lines := decodeJournalErrorLines(t, output); len(lines) != 1 {
		t.Fatalf("1000 identical failures inside the window produced %d lines", len(lines))
	}

	// Past the window the same cause is logged once more, carrying the count of
	// suppressed repeats so operators can see the blast radius.
	now = now.Add(5 * time.Minute)
	reporter.Report(unsupportedPublicationError("search_after_window"))
	lines := decodeJournalErrorLines(t, output)
	if len(lines) != 2 {
		t.Fatalf("post-window report produced %d lines, want 2", len(lines))
	}
	if lines[1]["suppressed_repeats"] != float64(999) || lines[1]["job_id"] != "search_after_window" {
		t.Fatalf("post-window line = %v", lines[1])
	}

	// A different root cause is never suppressed, even inside the window.
	reporter.Report(&searchjobs.JournalError{
		Operation: searchjobs.JournalOperationFinalize,
		JobID:     "search_other",
		State:     searchjobs.StateFailed,
		Err:       errors.New("database is locked"),
	})
	lines = decodeJournalErrorLines(t, output)
	if len(lines) != 3 {
		t.Fatalf("distinct cause produced %d lines, want 3", len(lines))
	}
	if lines[2]["level"] != "warn" || lines[2]["msg"] != "persist search-job history" ||
		lines[2]["journal_operation"] != "finalize" || lines[2]["cause_class"] != journalCauseOther {
		t.Fatalf("metadata journal failure line = %v", lines[2])
	}
	if _, suppressed := lines[2]["suppressed_repeats"]; suppressed {
		t.Fatalf("distinct cause carried a suppressed count: %v", lines[2])
	}

	// Returning to the first cause after a different one logs immediately.
	reporter.Report(unsupportedPublicationError("search_again"))
	if lines = decodeJournalErrorLines(t, output); len(lines) != 4 || lines[3]["job_id"] != "search_again" {
		t.Fatalf("cause change did not reset suppression: %d lines", len(lines))
	}
}

func TestJournalErrorLoggerHandlesNilAndNonJournalErrors(t *testing.T) {
	t.Parallel()

	logger, output := newJournalErrorTestLogger()
	reporter := newJournalErrorLogger(logger, nil, 0)
	reporter.Report(nil)
	if output.Len() != 0 {
		t.Fatalf("nil error produced output: %s", output.String())
	}
	reporter.Report(errors.New("opaque journal fault"))
	reporter.Report(errors.New("opaque journal fault"))
	lines := decodeJournalErrorLines(t, output)
	if len(lines) != 1 || lines[0]["level"] != "warn" || lines[0]["journal_operation"] != "" {
		t.Fatalf("non-journal error lines = %v", lines)
	}
	if reporter.window != journalErrorRepeatWindow {
		t.Fatalf("default window = %s, want %s", reporter.window, journalErrorRepeatWindow)
	}
}
