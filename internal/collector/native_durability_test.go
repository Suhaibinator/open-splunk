package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collector/config"
	"github.com/Suhaibinator/open-splunk/internal/collector/sender"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"go.uber.org/zap/zapcore"
)

// These records are literal producer output, deliberately independent of the
// decoder under test. Long first records stabilize the file fingerprint across
// appends/restarts, so these tests measure delivery rather than file identity.
type nativeDurabilityFixture struct {
	format, options, first, bad, last, firstMessage, lastMessage string
}

func nativeDurabilityFixtures() []nativeDurabilityFixture {
	pad := strings.Repeat("p", 1200)
	const stamp = "2026-09-07T12:34:56.123456789Z"
	fixtures := []nativeDurabilityFixture{{
		format:       "docker-json-file",
		first:        `{"log":"` + pad + `","stream":"stdout","time":"` + stamp + `","host":"spoof","index":"forbidden","environment":"spoof"}`,
		bad:          `{"log":"private-malformed-sentinel","stream":"stdout","time":"invalid-private-time-sentinel"}`,
		last:         `{"log":"last\n","stream":"stderr","time":"` + stamp + `"}`,
		firstMessage: pad, lastMessage: "last\n",
	}, {
		format:       "logfmt",
		first:        `message="` + pad + `" host=spoof index=forbidden environment=spoof`,
		bad:          `message="private-malformed-sentinel" timestamp=invalid-private-time-sentinel`,
		last:         `message="last" count=9007199254740993`,
		firstMessage: pad, lastMessage: "last",
	}}
	for _, format := range []string{"nginx-combined", "apache-common", "apache-combined"} {
		suffix := ` "-" "agent"`
		if format == "apache-common" {
			suffix = ""
		}
		request := "GET /" + pad + " HTTP/1.1"
		fixtures = append(fixtures, nativeDurabilityFixture{
			format:       format,
			first:        `2001:db8::1 - alice [07/Sep/2026:12:34:56 +0000] "` + request + `" 200 17` + suffix,
			bad:          `private-malformed-sentinel - - [invalid-private-time-sentinel] "GET / HTTP/1.1" 200 1` + suffix,
			last:         `example.test - - [07/Sep/2026:12:34:56 +0000] "GET /last HTTP/1.1" 204 0` + suffix,
			firstMessage: request, lastMessage: "GET /last HTTP/1.1",
		})
	}
	for _, format := range []string{"log4j2-pattern", "logback-pattern"} {
		fixtures = append(fixtures, nativeDurabilityFixture{
			format:       format,
			options:      "    parser:\n      pattern: '%{timestamp}|%{level}|%{message}'\n      timestamp_layout: '2006-01-02T15:04:05.999999999Z07:00'\n",
			first:        stamp + "|INFO |" + pad,
			bad:          "invalid-private-time-sentinel|ERROR|private-malformed-sentinel",
			last:         stamp + "|WARN|last",
			firstMessage: pad, lastMessage: "last",
		})
	}
	return fixtures
}

func nativeConfigPath(t *testing.T, fixture nativeDurabilityFixture, addr, stateDir, logPath, processors string) string {
	t.Helper()
	path := writeE2EConfig(t, addr, stateDir, logPath, filepath.Join(t.TempDir(), "token"))
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	yaml := strings.Replace(string(contents), "    format: ndjson\n", "    format: "+fixture.format+"\n"+fixture.options, 1)
	yaml = strings.Replace(yaml, "    sourcetype: json\n", "", 1)
	if processors != "" {
		yaml = strings.Split(yaml, "processors:\n")[0] + processors
	}
	writeFile(t, path, yaml)
	return path
}

func runNativeDaemon(t *testing.T, d *Daemon) func() {
	t.Helper()
	d.batchLinger = 15 * time.Millisecond
	d.drainWindow = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	stopped := false
	stop := func() {
		t.Helper()
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("collector Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("collector did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func nativeWaitCheckpoint(t *testing.T, d *Daemon, offset uint64) {
	t.Helper()
	waitFor(t, 5*time.Second, fmt.Sprintf("checkpoint offset %d", offset), func() bool {
		list, err := d.checkpoints.List()
		return err == nil && len(list) == 1 && list[0].Offset == offset
	})
}

func nativeArtifacts(t *testing.T, stateDir string) []sender.RejectedSourceRecord {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(stateDir, deadLetterFile))
	if err != nil {
		t.Fatal(err)
	}
	var result []sender.RejectedSourceRecord
	for line := range bytes.SplitSeq(bytes.TrimSpace(contents), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var artifact struct {
			Code         string                      `json:"code"`
			SourceRecord sender.RejectedSourceRecord `json:"source_record"`
		}
		if err := json.Unmarshal(line, &artifact); err != nil {
			t.Fatal(err)
		}
		if artifact.Code != "DECODE_ERROR" {
			t.Fatalf("rejection code = %q", artifact.Code)
		}
		result = append(result, artifact.SourceRecord)
	}
	return result
}

func TestNativeFormatsDurableMalformedRecovery(t *testing.T) {
	for _, fixture := range nativeDurabilityFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			t.Parallel()
			store := newRecordingStore()
			stateDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "native.log")
			content := fixture.first + "\r\n" + fixture.bad + "\n" + fixture.last + "\n"
			writeFile(t, logPath, content)
			path := nativeConfigPath(t, fixture, startIngestServer(t, store), stateDir, logPath, "")
			d := newE2EDaemon(t, path)
			logs := &logCapture{}
			d.log = capturedLogger(zapcore.AddSync(logs))
			stop := runNativeDaemon(t, d)
			waitFor(t, 5*time.Second, "two valid native events", func() bool { return store.count() == 2 })
			nativeWaitCheckpoint(t, d, uint64(len(content)))
			stop()
			if d.DecodeFailures() != 1 || store.duplicates() != 0 {
				t.Fatalf("decode failures=%d duplicate deliveries=%d", d.DecodeFailures(), store.duplicates())
			}
			for _, text := range []string{"private-malformed-sentinel", "invalid-private-time-sentinel"} {
				if strings.Contains(logs.String(), text) {
					t.Error("diagnostics leaked rejected payload")
				}
			}
			events := store.snapshot()
			for i, want := range []string{fixture.firstMessage, fixture.lastMessage} {
				ev := events[i]
				if ev.GetMessage() != want || ev.GetHost() != "test-host" || ev.GetIndexName() != e2eIndex || ev.GetSource() != "app-log" || ev.GetSourcetype() != fixture.format || fieldValue(ev, "environment").GetStringValue() != "prod" {
					t.Fatalf("native projection/trusted metadata mismatch: %+v", ev)
				}
			}
			if string(events[0].GetRaw()) != fixture.first || string(events[1].GetRaw()) != fixture.last {
				t.Fatal("framed raw bytes changed")
			}
			artifacts := nativeArtifacts(t, stateDir)
			if len(artifacts) != 1 || string(artifacts[0].Bytes) != fixture.bad || artifacts[0].StartOffset != uint64(len(fixture.first)+2) || artifacts[0].EndOffset != uint64(len(fixture.first)+2+len(fixture.bad)+1) || artifacts[0].SourcePath != logPath || artifacts[0].InputID != "app" {
				t.Fatalf("rejected source bytes/position: %+v", artifacts)
			}
		})
	}
}

func TestNativeFormatsTrailingAndAllMalformedReplay(t *testing.T) {
	for _, fixture := range nativeDurabilityFixtures() {
		for _, allBad := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/all_bad=%t", fixture.format, allBad), func(t *testing.T) {
				t.Parallel()
				store := newRecordingStore()
				stateDir := t.TempDir()
				logPath := filepath.Join(t.TempDir(), "native.log")
				prefix := fixture.first + "\n"
				wantEvents := 1
				if allBad {
					prefix, wantEvents = "", 0
				}
				writeFile(t, logPath, prefix+fixture.bad+"\n")
				path := nativeConfigPath(t, fixture, startIngestServer(t, store), stateDir, logPath, "")
				for range 2 {
					d := newE2EDaemon(t, path)
					stop := runNativeDaemon(t, d)
					waitFor(t, 5*time.Second, "trailing rejection", func() bool { return d.DecodeFailures() == 1 && store.count() == wantEvents })
					nativeWaitCheckpoint(t, d, uint64(len(prefix)))
					stop()
				}
				if len(nativeArtifacts(t, stateDir)) != 2 || store.duplicates() != 0 {
					t.Fatal("trailing rejection was not replayed, or acknowledged event was replayed")
				}
			})
		}
	}
}

func TestNativeFormatsPendingWALRestart(t *testing.T) {
	for _, fixture := range nativeDurabilityFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			t.Parallel()
			stateDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "native.log")
			content := fixture.first + "\n" + fixture.bad + "\n" + fixture.last + "\n"
			writeFile(t, logPath, content)
			path := nativeConfigPath(t, fixture, deadServerAddr, stateDir, logPath, "")
			d := newE2EDaemon(t, path)
			stop := runNativeDaemon(t, d)
			waitFor(t, 5*time.Second, "two pending native events", func() bool { return d.queue.Stats().QueuedEvents == 2 && d.DecodeFailures() == 1 })
			nativeWaitCheckpoint(t, d, 0)
			stop()
			queue, err := wal.Open(wal.Options{Dir: filepath.Join(stateDir, walSubdir), Sync: wal.SyncAlways, CollectorID: e2eCollectorID})
			if err != nil {
				t.Fatal(err)
			}
			batch, err := queue.NextBatch(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			ids := make(map[string]bool)
			for _, ev := range batch.GetEvents() {
				ids[ev.GetEventId()] = true
			}
			if err := queue.Close(); err != nil {
				t.Fatal(err)
			}
			store := newRecordingStore()
			cfg, err := config.Load(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Server.Address = startIngestServer(t, store)
			d2, err := New(cfg, WithLogger(discardLogger()))
			if err != nil {
				t.Fatal(err)
			}
			stop2 := runNativeDaemon(t, d2)
			waitFor(t, 5*time.Second, "pending WAL delivered after restart", func() bool { return store.count() == 2 })
			nativeWaitCheckpoint(t, d2, uint64(len(content)))
			stop2()
			if store.duplicates() != 0 || d2.DecodeFailures() != 0 {
				t.Fatal("pending source prefix was unnecessarily replayed")
			}
			for _, ev := range store.snapshot() {
				delete(ids, ev.GetEventId())
			}
			if len(ids) != 0 {
				t.Fatal("durable event IDs changed after restart")
			}
		})
	}
}

func TestNativeFormatsWithheldTerminalAck(t *testing.T) {
	for _, fixture := range nativeDurabilityFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			t.Parallel()
			store := newRecordingStore()
			blocked := newBlockingEventStore(store)
			t.Cleanup(blocked.unblock)
			stateDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "native.log")
			content := fixture.first + "\n" + fixture.bad + "\n" + fixture.last + "\n"
			writeFile(t, logPath, content)
			path := nativeConfigPath(t, fixture, startIngestServer(t, blocked), stateDir, logPath, "")
			d := newE2EDaemon(t, path)
			stop := runNativeDaemon(t, d)
			select {
			case <-blocked.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("native batch did not reach authenticated server")
			}
			nativeWaitCheckpoint(t, d, 0)
			waitFor(t, 5*time.Second, "both native records durable behind withheld ack", func() bool { return d.queue.Stats().QueuedEvents == 2 })
			if store.count() != 0 {
				t.Fatal("terminal ack withheld but local durability changed")
			}
			stop()
			select {
			case <-blocked.exited:
			case <-time.After(5 * time.Second):
				t.Fatal("canceled unacknowledged server request did not exit")
			}
			blocked.unblock()
			d2 := newE2EDaemon(t, path)
			stop2 := runNativeDaemon(t, d2)
			waitFor(t, 5*time.Second, "delivery after terminal ack released", func() bool { return store.count() == 2 })
			nativeWaitCheckpoint(t, d2, uint64(len(content)))
			stop2()
			if store.duplicates() != 0 || d2.DecodeFailures() != 0 {
				t.Fatal("restart reread a pending native source prefix")
			}
			assertNoDuplicateEventIDs(t, store.snapshot())
		})
	}
}

type nativeFailingRecoverySink struct{ err error }

func (s nativeFailingRecoverySink) WriteRecords([]sender.DeadLetterRecord) error { return s.err }
func (nativeFailingRecoverySink) Close() error                                   { return nil }

func TestNativeFormatsRecoveryFailureStopsBeforeLaterValidEvent(t *testing.T) {
	for _, fixture := range nativeDurabilityFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			t.Parallel()
			stateDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "native.log")
			writeFile(t, logPath, fixture.bad+"\n"+fixture.last+"\n")
			path := nativeConfigPath(t, fixture, deadServerAddr, stateDir, logPath, "")
			d := newE2EDaemon(t, path)
			if err := d.deadLetter.Close(); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("injected durable rejection failure")
			d.deadLetter = nativeFailingRecoverySink{err: failure}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if err := d.Run(ctx); !errors.Is(err, failure) {
				t.Fatalf("Run error=%v, want recovery failure", err)
			}
			if got := diskCheckpointOffset(t, stateDir); got != 0 {
				t.Fatalf("checkpoint passed unprotected rejection: %d", got)
			}
			queue, err := wal.Open(wal.Options{Dir: filepath.Join(stateDir, walSubdir), Sync: wal.SyncAlways, CollectorID: e2eCollectorID})
			if err != nil {
				t.Fatal(err)
			}
			defer queue.Close()
			if queue.Stats().QueuedEvents != 0 {
				t.Fatal("later valid event crossed failed rejection")
			}
		})
	}
}

func TestNativeFormatsOfflineRedaction(t *testing.T) {
	const secret = "native-secret-value"
	for _, tc := range []struct{ format, options, line, field, encoded string }{
		{"docker-json-file", "", `{"log":"safe","stream":"stdout","time":"2026-09-07T12:34:56Z","credential":"native-secret-value"}`, "credential", secret},
		{"logfmt", "", `message=safe credential="native-secret-\u0076alue"`, "credential", `native-secret-\u0076alue`},
		{"nginx-combined", "", `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET / HTTP/1.1" 200 1 "-" "native-secret-\x76alue"`, "user_agent", `native-secret-\x76alue`},
		{"apache-combined", "", `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET / HTTP/1.1" 200 1 "-" "native-secret-\x76alue"`, "user_agent", `native-secret-\x76alue`},
		{"log4j2-pattern", "    parser:\n      pattern: '%{credential}|%{message}'\n", "native-secret-value|safe", "credential", secret},
		{"logback-pattern", "    parser:\n      pattern: '%{credential}|%{message}'\n", "native-secret-value|safe", "credential", secret},
	} {
		t.Run(tc.format, func(t *testing.T) {
			t.Parallel()
			stateDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "native.log")
			writeFile(t, logPath, tc.line+"\n")
			processors := fmt.Sprintf("processors:\n  - type: rename\n    from: %s\n    to: token\n  - type: redact\n    fields: [token]\n    replacement: '[MASKED]'\n", tc.field)
			path := nativeConfigPath(t, nativeDurabilityFixture{format: tc.format, options: tc.options}, deadServerAddr, stateDir, logPath, processors)
			d := newE2EDaemon(t, path)
			stop := runNativeDaemon(t, d)
			waitFor(t, 5*time.Second, "redacted native event in offline WAL", func() bool { return d.queue.Stats().QueuedEvents == 1 })
			stop()
			queue, err := wal.Open(wal.Options{Dir: filepath.Join(stateDir, walSubdir), Sync: wal.SyncAlways, CollectorID: e2eCollectorID})
			if err != nil {
				t.Fatal(err)
			}
			defer queue.Close()
			batch, err := queue.NextBatch(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			assertNoSecret(t, batch.GetEvents(), secret)
			assertNoSecret(t, batch.GetEvents(), tc.encoded)
			for _, ev := range batch.GetEvents() {
				if fieldValue(ev, "token").GetStringValue() != "[MASKED]" {
					t.Fatal("renamed sensitive field was not redacted")
				}
			}
			files, err := filepath.Glob(filepath.Join(stateDir, walSubdir, "*"))
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range files {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if info.IsDir() {
					continue
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Contains(data, []byte(secret)) || bytes.Contains(data, []byte(tc.encoded)) {
					t.Fatal("WAL disk bytes retained secret")
				}
			}
		})
	}
}

func TestNativeFormatsPartialWritesAndRotation(t *testing.T) {
	for _, fixture := range nativeDurabilityFixtures() {
		for _, copytruncate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/copytruncate=%t", fixture.format, copytruncate), func(t *testing.T) {
				t.Parallel()
				store := newRecordingStore()
				stateDir := t.TempDir()
				logPath := filepath.Join(t.TempDir(), "native.log")
				writeFile(t, logPath, fixture.first+"\n")
				path := nativeConfigPath(t, fixture, startIngestServer(t, store), stateDir, logPath, "")
				d := newE2EDaemon(t, path)
				stop := runNativeDaemon(t, d)
				waitFor(t, 5*time.Second, "initial event", func() bool { return store.count() == 1 })
				nativeWaitCheckpoint(t, d, uint64(len(fixture.first)+1))
				// A completed record without its framing delimiter must remain
				// buffered through multiple polls, even though it is parseable.
				midpoint := len(fixture.last) / 2
				appendFile(t, logPath, fixture.last[:midpoint])
				appendFile(t, logPath, fixture.last[midpoint:])
				time.Sleep(60 * time.Millisecond)
				if store.count() != 1 || d.DecodeFailures() != 0 {
					t.Fatal("partial final line emitted or rejected before delimiter")
				}
				appendFile(t, logPath, "\n")
				waitFor(t, 5*time.Second, "completed partial record", func() bool { return store.count() == 2 })
				nativeWaitCheckpoint(t, d, uint64(len(fixture.first)+len(fixture.last)+2))
				if !copytruncate {
					// Reader must drain the old descriptor after rename.
					appendFile(t, logPath, fixture.last+"\n")
					if err := os.Rename(logPath, logPath+".1"); err != nil {
						t.Fatal(err)
					}
				}
				writeFile(t, logPath, fixture.last+"\n")
				want := 4
				if copytruncate {
					want = 3
				}
				waitFor(t, 5*time.Second, "events after file generation change", func() bool { return store.count() == want })
				waitFor(t, 5*time.Second, "WAL drained after generation change", func() bool { return d.queue.Stats().QueuedEvents == 0 })
				stop()
				if store.duplicates() != 0 || d.DecodeFailures() != 0 {
					t.Fatalf("duplicate=%d failure=%d", store.duplicates(), d.DecodeFailures())
				}
				assertNoDuplicateEventIDs(t, store.snapshot())
			})
		}
	}
}

func TestNativeJavaMultilineOversizeResynchronization(t *testing.T) {
	for _, format := range []string{"log4j2-pattern", "logback-pattern"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			store := newRecordingStore()
			stateDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "native.log")
			const good = "ERROR|exception\n\tat example.Main.run(Main.java:7)\n\nCaused by: java.lang.IllegalStateException: broken"
			oversize := "ERROR|" + strings.Repeat("x", 300) + "\n\tignored continuation\n"
			const last = "INFO|recovered"
			content := good + "\n" + oversize + last + "\n"
			writeFile(t, logPath, content)
			fixture := nativeDurabilityFixture{format: format, options: "    parser:\n      pattern: '%{level}|%{message}'\n    max_event_bytes: 256\n    multiline:\n      line_start_pattern: '^(ERROR|INFO)\\|'\n      flush_after: 30ms\n"}
			path := nativeConfigPath(t, fixture, startIngestServer(t, store), stateDir, logPath, "")
			d := newE2EDaemon(t, path)
			stop := runNativeDaemon(t, d)
			waitFor(t, 5*time.Second, "stack trace and recovered event", func() bool { return store.count() == 2 })
			nativeWaitCheckpoint(t, d, uint64(len(content)))
			stop()
			events := store.snapshot()
			if events[0].GetMessage() != strings.TrimPrefix(good, "ERROR|") || string(events[0].GetRaw()) != good || events[1].GetMessage() != "recovered" || d.DecodeFailures() != 1 {
				t.Fatalf("multiline recovery events=%+v failures=%d", events, d.DecodeFailures())
			}
			data, err := os.ReadFile(filepath.Join(stateDir, deadLetterFile))
			if err != nil {
				t.Fatal(err)
			}
			var artifact struct {
				Code         string                      `json:"code"`
				SourceRecord sender.RejectedSourceRecord `json:"source_record"`
			}
			if err := json.Unmarshal(bytes.TrimSpace(data), &artifact); err != nil {
				t.Fatal(err)
			}
			// Framing reports the boundary where the cap was exceeded, then
			// discards following continuation lines without producing new
			// artifacts. It does not claim to retain the unbounded remainder.
			oversizeBoundary := len(good) + 1 + strings.IndexByte(oversize, '\n') + 1
			if artifact.Code != "FRAMING_EVENT_TOO_LARGE" || !artifact.SourceRecord.Truncated || artifact.SourceRecord.StartOffset != uint64(len(good)+1) || artifact.SourceRecord.EndOffset != uint64(oversizeBoundary) || artifact.SourceRecord.LineNumber != 5 || artifact.SourceRecord.NextLineNumber != 6 || !bytes.Equal(artifact.SourceRecord.Bytes, []byte(oversize[:256])) {
				t.Fatalf("oversized recovery artifact=%+v", artifact)
			}
		})
	}
}
