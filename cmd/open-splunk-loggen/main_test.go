package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
)

func TestRunReportsCompiledBuildIdentity(t *testing.T) {
	t.Parallel()

	identity, err := buildinfo.Current()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run(context.Background(), []string{"-version"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatalf("run(-version): %v", err)
	}
	want := "source_revision=" + identity.SourceRevision + "\n"
	if got := output.String(); got != want {
		t.Fatalf("version output = %q, want %q", got, want)
	}
}

func TestVersionHelpDescribesSourceRevisionOutput(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	err := run(
		context.Background(),
		[]string{"-h"},
		&bytes.Buffer{},
		&stderr,
	)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("run(-h) error = %v, want flag.ErrHelp", err)
	}
	if got := stderr.String(); !strings.Contains(got, "print the compiled source revision") ||
		strings.Contains(got, "application version") {
		t.Fatalf("version help is invalid:\n%s", got)
	}
}

func TestRunGeneratesDeterministicNDJSON(t *testing.T) {
	t.Parallel()

	args := []string{
		"-count=3",
		"-format=zap-json",
		"-seed=17",
		"-start=2026-01-02T03:04:05Z",
		"-interval=250ms",
		"-service=gradethis",
		"-environment=integration",
		"-host=test-host",
	}
	var first, second bytes.Buffer
	if err := run(context.Background(), args, &first, &bytes.Buffer{}); err != nil {
		t.Fatalf("run(first): %v", err)
	}
	if err := run(context.Background(), args, &second, &bytes.Buffer{}); err != nil {
		t.Fatalf("run(second): %v", err)
	}
	if first.String() != second.String() {
		t.Fatal("same flags did not produce byte-identical output")
	}

	lines := strings.Split(strings.TrimSuffix(first.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("line count = %d, want 3", len(lines))
	}
	for i, line := range lines {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("line %d is not JSON: %v", i, err)
		}
		if event["service"] != "gradethis" {
			t.Fatalf("line %d service = %#v", i, event["service"])
		}
	}
}

func TestRunRejectsInvalidFlagsWithoutWritingEvents(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"-format=nope"},
		{"-start=tomorrow"},
		{"-rate=-1"},
		{"-rate=1e20"},
	} {
		var output bytes.Buffer
		if err := run(context.Background(), args, &output, &bytes.Buffer{}); err == nil {
			t.Fatalf("run(%v) unexpectedly succeeded", args)
		}
		if output.Len() != 0 {
			t.Fatalf("run(%v) wrote output before validation: %q", args, output.String())
		}
	}
}

func TestRunHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, []string{"-count=1"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("run with canceled context unexpectedly succeeded")
	}
}

func TestIsOnlyCancellationRejectsJoinedFailure(t *testing.T) {
	t.Parallel()
	if !isOnlyCancellation(context.Canceled) {
		t.Fatal("plain cancellation was not recognized")
	}
	if !isOnlyCancellation(fmt.Errorf("wrapped: %w", context.Canceled)) {
		t.Fatal("wrapped cancellation was not recognized")
	}
	if !isOnlyCancellation(errors.Join(context.Canceled, context.Canceled)) {
		t.Fatal("joined cancellations were not recognized")
	}
	if isOnlyCancellation(errors.Join(context.Canceled, errors.New("flush failed"))) {
		t.Fatal("cancellation joined with a real failure was suppressed")
	}
	if isOnlyCancellation(nil) {
		t.Fatal("nil was recognized as cancellation")
	}
}

func TestRunCanceledBeforeOpenPreservesExistingOutput(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "load.log")
	const existing = "do not truncate\n"
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, []string{"-count=1", "-output=" + path}, &bytes.Buffer{}, &bytes.Buffer{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run(canceled) = %v, want context.Canceled", err)
	}
	contents, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(contents) != existing {
		t.Fatalf("pre-canceled output = %q, want %q", contents, existing)
	}
}

func TestRunCancellationDoesNotMaskFinalFlushFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	writeErr := errors.New("final flush failed")
	writer := &cancelThenFailWriter{cancel: cancel, err: writeErr}
	err := run(ctx, []string{
		"-count=0",
		"-format=raw",
		"-service=" + strings.Repeat("x", 300*1024),
	}, writer, &bytes.Buffer{})
	if !errors.Is(err, writeErr) {
		t.Fatalf("run error = %v, want final flush failure", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want cancellation cause to remain available", err)
	}
	if isOnlyCancellation(err) {
		t.Fatalf("run error %v masks final flush failure as ordinary cancellation", err)
	}
	if writer.writes != 2 {
		t.Fatalf("underlying writes = %d, want direct event write and deferred delimiter flush", writer.writes)
	}
}

func TestRunPacedFileIsVisibleBeforeCancellationAndFinalizesOutput(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "load.log")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"-count=0",
			"-format=cardinality-json",
			"-rate=1000",
			"-flush-events=1",
			"-output=" + path,
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	deadline := time.Now().Add(2 * time.Second)
	visible := false
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(path)
		if err == nil && bytes.Contains(contents, []byte{'\n'}) {
			visible = true
			break
		}
		select {
		case err := <-done:
			t.Fatalf("run exited before paced output became visible: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run cancellation = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not finish after cancellation")
	}
	if !visible {
		t.Fatal("paced output stayed buffered until cancellation")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(contents) == 0 || contents[len(contents)-1] != '\n' {
		t.Fatalf("finalized paced output = %q", contents)
	}
}

func TestRunAppendPreservesExistingCompleteLines(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "load.log")
	const existing = "existing complete line\n"
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{
		"-count=1",
		"-format=raw",
		"-append",
		"-output=" + path,
	}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(contents, []byte(existing)) || bytes.Count(contents, []byte{'\n'}) != 2 {
		t.Fatalf("appended output = %q", contents)
	}
}

func TestRunAppendRejectsIncompleteTrailingRecord(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "load.log")
	const existing = "incomplete record"
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), []string{
		"-count=1",
		"-format=raw",
		"-append",
		"-output=" + path,
	}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("append to an incomplete trailing record unexpectedly succeeded")
	}
	contents, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(contents) != existing {
		t.Fatalf("rejected append changed output to %q", contents)
	}
}

type cancelThenFailWriter struct {
	cancel context.CancelFunc
	err    error
	writes int
}

func (w *cancelThenFailWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 1 {
		w.cancel()
		return len(p), nil
	}
	return 0, w.err
}
