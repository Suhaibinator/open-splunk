//go:build !windows

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
)

const backendLoadInputID = "backend-sustained-load"

func backendLoadCollectorYAML(
	address, tokenPath, statePath, logPath, indexName string,
) string {
	return fmt.Sprintf(`logging:
  level: debug
  format: json
server:
  address: %q
  transport: grpc
  token_file: %q
  compression: gzip
  tls:
    enabled: false
state:
  directory: %q
  max_queue_bytes: 64MiB
inputs:
  - id: %s
    type: file
    include:
      - %q
    format: ndjson
    start_at: beginning
    index: %q
    source: backend-load.ndjson
    sourcetype: json
    host: backend-load-host
    poll_interval: 20ms
    fields:
      environment: integration
      service: backend-load
`, address, tokenPath, statePath, backendLoadInputID, logPath, indexName)
}

func validateBackendLoadCollectorConfiguration(
	t *testing.T,
	ctx context.Context,
	repository, collectorBinary, configPath string,
	environment []string,
	plaintextToken string,
) {
	t.Helper()
	validateCollectorConfigurationWithInput(
		t,
		ctx,
		repository,
		collectorBinary,
		configPath,
		environment,
		plaintextToken,
		"backend load",
		backendLoadInputID,
	)
}

func assertBackendLoadDeadLetterEmpty(t *testing.T, stateDir string) {
	t.Helper()
	path := filepath.Join(stateDir, "dead-letter.jsonl")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat backend load dead-letter file: %v", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() != 0 {
		t.Fatalf(
			"backend load dead-letter file = mode %s size %d, want empty owner-only regular file",
			info.Mode(),
			info.Size(),
		)
	}
}

func assertBackendLoadCheckpointDetails(
	t *testing.T,
	stateDir, logPath string,
	wantLine uint64,
) {
	t.Helper()
	checkpoints, err := readCollectorCheckpoints(stateDir)
	if err != nil {
		t.Fatalf("read final backend load checkpoint: %v", err)
	}
	if len(checkpoints) != 1 {
		t.Fatalf("final backend load checkpoints = %+v, want exactly one", checkpoints)
	}
	checkpoint := checkpoints[0]
	if checkpoint.Path != logPath ||
		checkpoint.Offset != uint64(mustFileSize(t, logPath)) ||
		checkpoint.LineNumber != wantLine ||
		checkpoint.NextLineNumber != wantLine+1 ||
		checkpoint.Identity.Generation != 1 ||
		checkpoint.Identity.FingerprintLength == 0 ||
		checkpoint.UpdatedAt.IsZero() {
		t.Fatalf("final backend load checkpoint = %+v", checkpoint)
	}
}

func assertBackendLoadPendingWAL(
	t *testing.T,
	stateDir string,
	minimumQueuedEvents uint64,
) wal.Stats {
	t.Helper()
	queue, err := wal.Open(wal.Options{
		Dir:         filepath.Join(stateDir, "wal"),
		Sync:        wal.SyncAlways,
		CollectorID: readCollectorIdentity(t, stateDir),
	})
	if err != nil {
		t.Fatalf("reopen pending backend load WAL: %v", err)
	}
	stats := queue.Stats()
	if err := queue.Close(); err != nil {
		t.Fatalf("close pending backend load WAL: %v", err)
	}
	if stats.QueuedBatches == 0 ||
		stats.QueuedEvents < minimumQueuedEvents ||
		stats.QueuedBytes == 0 ||
		stats.OldestEventAge <= 0 ||
		stats.LastAckedBatchSequence == 0 ||
		stats.LastAckedBatchSequence == ^uint64(0) ||
		stats.NextBatchSequence <= stats.LastAckedBatchSequence+1 ||
		stats.QuarantinedSegments != 0 {
		t.Fatalf(
			"pending backend load WAL = %+v, want at least %d durable unacknowledged events after a terminal warm acknowledgment",
			stats,
			minimumQueuedEvents,
		)
	}
	return stats
}

func assertBackendLoadCollectorDiagnostics(t *testing.T, logs, plaintextToken string) {
	t.Helper()
	for _, forbidden := range []string{
		"collector: skipping undecodable record",
		"dead-lettering and dropping",
		"dead-letter write failed",
	} {
		if strings.Contains(logs, forbidden) {
			t.Fatalf(
				"backend load collector logged %q:\n%s",
				forbidden,
				redactForFailure(logs, plaintextToken),
			)
		}
	}
	assertProcessLogsDoNotLeak(t, logs, plaintextToken)
}
