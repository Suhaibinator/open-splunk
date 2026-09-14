//go:build !windows

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

// backendNativeMixedLoad shares the compiled-binary/ClickHouse native-format
// harness. Enable both OPEN_SPLUNK_BACKEND_INTEGRATION and OPEN_SPLUNK_BACKEND_LOAD.
// This is a correctness/load gate; latency benchmarks run separately without
// concurrent build or test workloads.
func backendNativeMixedLoad(t *testing.T, ctx context.Context, storage clickhousedriver.Conn, collector *managedProcess, token, stateDir, work string, stamp time.Time, initialCount uint64) {
	t.Helper()
	const batches, batchSize = 300, 100
	const eventsPerFormat = batches * batchSize
	const interval = 100 * time.Millisecond
	paths := []string{filepath.Join(work, "ndjson.log"), filepath.Join(work, "log4j2-pattern.log")}
	files := make([]*os.File, len(paths))
	expectedOffsets := make([]uint64, len(paths))
	for i, path := range paths {
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		files[i] = file
		t.Cleanup(func() { _ = file.Close() })
		info, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		expectedOffsets[i] = uint64(info.Size())
	}
	rfcStamp := stamp.Format(time.RFC3339Nano)
	started := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var ndjson, java strings.Builder
	for batch := range batches {
		if batch > 0 {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-ticker.C:
			}
		}
		if collector.Exited() {
			t.Fatalf("collector exited under mixed load: %v", collector.Err())
		}
		ndjson.Reset()
		java.Reset()
		for slot := range batchSize {
			id := batch*batchSize + slot
			message := fmt.Sprintf("native-load-%08d", id)
			fmt.Fprintf(&ndjson, "{\"timestamp\":%q,\"message\":%q,\"sequence\":%d}\n", rfcStamp, message, id)
			fmt.Fprintf(&java, "%s|INFO|%s\n", rfcStamp, message)
		}
		for i, payload := range []string{ndjson.String(), java.String()} {
			written, err := files[i].WriteString(payload)
			if err != nil || written != len(payload) {
				t.Fatalf("append native load input %d: wrote %d/%d: %v", i, written, len(payload), err)
			}
			if err := files[i].Sync(); err != nil {
				t.Fatal(err)
			}
			expectedOffsets[i] += uint64(written)
		}
	}
	generated := time.Since(started)
	waitForStoredEventCount(t, ctx, storage, collector, token, initialCount+2*eventsPerFormat)
	rows, err := storage.Query(ctx, `SELECT sourcetype, ifNull(body, ''), raw, event_time, event_id FROM open_splunk.events WHERE tenant_id = ? AND index_name = ? AND startsWith(ifNull(body, ''), 'native-load-')`, verticalTenantID, verticalIndexName)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := make(map[string]struct{}, 2*eventsPerFormat)
	ids := make(map[string]struct{}, 2*eventsPerFormat)
	for rows.Next() {
		var format, message, id string
		var raw []byte
		var eventTime time.Time
		if err := rows.Scan(&format, &message, &raw, &eventTime, &id); err != nil {
			t.Fatal(err)
		}
		sequence, err := strconv.Atoi(strings.TrimPrefix(message, "native-load-"))
		if err != nil || sequence < 0 || sequence >= eventsPerFormat {
			t.Fatalf("invalid mixed-load message %q", message)
		}
		var expectedRaw string
		switch format {
		case "ndjson":
			expectedRaw = fmt.Sprintf("{\"timestamp\":%q,\"message\":%q,\"sequence\":%d}", rfcStamp, message, sequence)
		case "log4j2-pattern":
			expectedRaw = rfcStamp + "|INFO|" + message
		default:
			t.Fatalf("unexpected load sourcetype %q", format)
		}
		if string(raw) != expectedRaw || !eventTime.Equal(stamp) {
			t.Fatalf("mixed-load projection mismatch for %s/%s", format, message)
		}
		key := format + "/" + message
		if _, exists := seen[key]; exists {
			t.Fatalf("duplicate mixed-load source record %s", key)
		}
		if _, exists := ids[id]; exists || id == "" {
			t.Fatalf("duplicate or empty mixed-load event ID for %s", key)
		}
		seen[key] = struct{}{}
		ids[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2*eventsPerFormat {
		t.Fatalf("mixed load stored %d records, want %d", len(seen), 2*eventsPerFormat)
	}
	backendNativeWaitLoadCheckpoints(t, ctx, stateDir, paths, expectedOffsets, []uint64{eventsPerFormat, eventsPerFormat + 3})
	assertProcessLogsDoNotLeak(t, collector.Logs(), token)
	t.Logf("mixed native load: events=%d scheduled_events_per_second=%d generation=%s total_with_drain_and_verification=%s", 2*eventsPerFormat, 2*batchSize*int(time.Second/interval), generated, time.Since(started))
}

func backendNativeWaitLoadCheckpoints(t *testing.T, ctx context.Context, stateDir string, paths []string, offsets, lines []uint64) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		checkpoints, err := readCollectorCheckpoints(stateDir)
		if err != nil {
			t.Fatal(err)
		}
		matched := 0
		for i, path := range paths {
			for _, checkpoint := range checkpoints {
				if checkpoint.Path == path && checkpoint.Offset == offsets[i] && checkpoint.LineNumber == lines[i] && checkpoint.NextLineNumber == lines[i]+1 {
					matched++
					break
				}
			}
		}
		if matched == len(paths) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatalf("mixed load checkpoint mismatch: %+v, want paths=%v offsets=%v lines=%v", checkpoints, paths, offsets, lines)
		case <-ticker.C:
		}
	}
}
