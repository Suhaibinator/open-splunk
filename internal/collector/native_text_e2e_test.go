package collector

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func nativeTextE2EConfig(t *testing.T, format, parserYAML, multilineYAML, address, stateDir, logPath string) string {
	t.Helper()
	path := writeE2EConfig(t, address, stateDir, logPath, filepath.Join(t.TempDir(), "token"))
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	yaml := strings.Replace(string(body), "    format: ndjson\n", "    format: "+format+"\n"+parserYAML+multilineYAML, 1)
	yaml = strings.Replace(yaml, "    sourcetype: json\n", "", 1)
	writeFile(t, path, yaml)
	return path
}

func nativeTextRunDaemon(t *testing.T, d *Daemon) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-done; err != nil {
			t.Errorf("daemon Run: %v", err)
		}
	}
	t.Cleanup(stop)
	return stop
}

func TestNativeTextE2EValidMalformedValid(t *testing.T) {
	cases := []struct{ format, parser, first, bad, last string }{
		{"logfmt", "", `message=first token="SUPERSECRETTOKENVALUE-do-not-leak" n=18446744073709551616`, `message="unterminated`, "message=last level=ERROR"},
		{"log4j2-pattern", "    parser:\n      pattern: '%{level} [%{thread}] - %{message}'\n", "INFO [main] - first", "WARN [missing", "ERROR [worker] - last"},
		{"logback-pattern", "    parser:\n      pattern: '%{level} [%{thread}] - %{message}'\n", "INFO [main] - first", "WARN [missing", "ERROR [worker] - last"},
	}
	for _, tc := range cases {
		t.Run(tc.format, func(t *testing.T) {
			store := newRecordingStore()
			addr := startIngestServer(t, store)
			stateDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "native.log")
			// CRLF and LF alternate; the empty physical record must be rejected too.
			content := tc.first + "\r\n" + tc.bad + "\n\r\n" + tc.last + "\n"
			writeFile(t, logPath, content)
			configPath := nativeTextE2EConfig(t, tc.format, tc.parser, "", addr, stateDir, logPath)
			d := newE2EDaemon(t, configPath)
			stop := nativeTextRunDaemon(t, d)
			waitFor(t, 10*time.Second, "native text events delivered", func() bool { return store.count() == 2 })
			waitFor(t, 5*time.Second, "native malformed and empty frames rejected", func() bool { return d.DecodeFailures() == 2 })
			waitFor(t, 5*time.Second, "native WAL acknowledged and checkpoint durable", func() bool {
				list, err := d.checkpoints.List()
				return err == nil && len(list) == 1 && list[0].Offset == uint64(len(content)) && d.queue.Stats().QueuedBatches == 0
			})
			stop()
			events := store.snapshot()
			firstMessage, firstRaw := "first", tc.first
			if tc.format == "logfmt" {
				// Explicit redaction sanitizes the entire native raw/message pair;
				// independently encoded copies cannot retain the sensitive value.
				firstMessage, firstRaw = "[REDACTED]", "[REDACTED]"
			}
			first := eventByMessage(events, firstMessage)
			last := eventByMessage(events, "last")
			if first == nil || last == nil {
				t.Fatalf("canonical messages not delivered: %v", events)
			}
			if first.GetMessage() != firstMessage || string(first.GetRaw()) != firstRaw {
				t.Fatalf("first native raw/message mismatch: %v", first)
			}
			if first.GetOrigin().GetStartOffset() != 0 || first.GetOrigin().GetEndOffset() != uint64(len(tc.first)+2) || last.GetOrigin().GetStartOffset() != uint64(len(content)-len(tc.last)-1) || last.GetOrigin().GetEndOffset() != uint64(len(content)) {
				t.Fatal("source offsets do not cover physical delimiters exactly")
			}
			if string(last.GetRaw()) != tc.last || last.GetSourcetype() != tc.format {
				t.Fatalf("raw or default sourcetype mismatch: %v", last)
			}
			if store.duplicates() != 0 {
				t.Fatalf("unexpected transport duplicates: %d", store.duplicates())
			}
			assertNoSecret(t, events, e2eSecret)
			if tc.format == "logfmt" && fieldValue(first, "n").GetDecimalValue().GetValue() != "18446744073709551616" {
				t.Fatal("numeric precision lost across WAL/gRPC")
			}
			// Restart the real daemon against the fully acknowledged cursor.
			restarted := newE2EDaemon(t, configPath)
			stopRestart := nativeTextRunDaemon(t, restarted)
			time.Sleep(100 * time.Millisecond)
			stopRestart()
			if store.count() != 2 || store.duplicates() != 0 {
				t.Fatalf("restart redelivered acknowledged records: events=%d duplicates=%d", store.count(), store.duplicates())
			}
		})
	}
}

func TestNativeTextE2EPartialLogfmtWrite(t *testing.T) {
	store := newRecordingStore()
	addr := startIngestServer(t, store)
	stateDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "partial.log")
	prefix := `message="partial`
	writeFile(t, logPath, prefix)
	d := newE2EDaemon(t, nativeTextE2EConfig(t, "logfmt", "", "", addr, stateDir, logPath))
	stop := nativeTextRunDaemon(t, d)
	time.Sleep(100 * time.Millisecond)
	if store.count() != 0 || d.DecodeFailures() != 0 {
		t.Fatal("unterminated physical frame was decoded prematurely")
	}
	appendFile(t, logPath, " complete\"\n")
	waitFor(t, 10*time.Second, "completed partial logfmt record", func() bool { return store.count() == 1 })
	stop()
	event := store.snapshot()[0]
	if event.GetMessage() != "partial complete" || string(event.GetRaw()) != `message="partial complete"` || event.GetOrigin().GetEndOffset() != uint64(len(prefix)+len(" complete\"\n")) {
		t.Fatalf("partial-write assembly corrupted: %v", event)
	}
}

func TestNativeTextE2EJavaMultilineNoFinalNewline(t *testing.T) {
	for _, format := range []string{"log4j2-pattern", "logback-pattern"} {
		t.Run(format, func(t *testing.T) {
			store := newRecordingStore()
			addr := startIngestServer(t, store)
			stateDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "java.log")
			parser := "    parser:\n      pattern: '%{level} [%{thread}] - %{message}'\n"
			multiline := "    multiline:\n      line_start_pattern: '^(INFO|WARN|ERROR) '\n      flush_after: 40ms\n"
			first := "ERROR [worker] - failed\r\njava.lang.IllegalStateException: boom\r\n\tat app.run(App.java:9)\r\n\r\nCaused by: bad value"
			bad := "WARN [missing"
			last := "INFO [main] - recovered"
			content := first + "\r\n" + bad + "\n" + last
			writeFile(t, logPath, content)
			d := newE2EDaemon(t, nativeTextE2EConfig(t, format, parser, multiline, addr, stateDir, logPath))
			stop := nativeTextRunDaemon(t, d)
			waitFor(t, 10*time.Second, "Java multiline and final partial frame delivered", func() bool { return store.count() == 2 })
			waitFor(t, 5*time.Second, "malformed Java start line rejected", func() bool { return d.DecodeFailures() == 1 })
			waitFor(t, 5*time.Second, "Java final cursor acknowledged", func() bool {
				list, err := d.checkpoints.List()
				return err == nil && len(list) == 1 && list[0].Offset == uint64(len(content))
			})
			stop()
			events := store.snapshot()
			message := "failed\r\njava.lang.IllegalStateException: boom\r\n\tat app.run(App.java:9)\r\n\r\nCaused by: bad value"
			event := eventByMessage(events, message)
			if event == nil || string(event.GetRaw()) != first {
				t.Fatalf("multiline bytes changed: %v", events)
			}
			recovered := eventByMessage(events, "recovered")
			if recovered == nil || recovered.GetOrigin().GetStartOffset() != uint64(len(first)+2+len(bad)+1) {
				t.Fatalf("did not resynchronize at valid start: %v", recovered)
			}
			if store.duplicates() != 0 {
				t.Fatal("Java input duplicated on transport")
			}
		})
	}
}

func TestNativeTextE2EAllMalformedAndTrailingMalformed(t *testing.T) {
	for _, format := range []string{"logfmt", "log4j2-pattern", "logback-pattern"} {
		for _, validPrefix := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prefix_%t", format, validPrefix), func(t *testing.T) {
				store := newRecordingStore()
				addr := startIngestServer(t, store)
				stateDir := t.TempDir()
				logPath := filepath.Join(t.TempDir(), "malformed.log")
				parser := ""
				prefix := "message=valid\n"
				bad := "bare-token\n"
				if format != "logfmt" {
					parser = "    parser:\n      pattern: '%{level} [%{thread}] - %{message}'\n"
					prefix = "INFO [main] - valid\n"
					bad = "WARN [missing\n"
				}
				content := bad
				wantEvents := 0
				if validPrefix {
					content = prefix + bad
					wantEvents = 1
				}
				writeFile(t, logPath, content)
				d := newE2EDaemon(t, nativeTextE2EConfig(t, format, parser, "", addr, stateDir, logPath))
				stop := nativeTextRunDaemon(t, d)
				waitFor(t, 10*time.Second, "malformed tail rejected durably", func() bool { return d.DecodeFailures() == 1 && store.count() == wantEvents })
				if validPrefix {
					waitFor(t, 5*time.Second, "valid prefix checkpoint", func() bool {
						list, err := d.checkpoints.List()
						return err == nil && len(list) == 1 && list[0].Offset == uint64(len(prefix))
					})
				}
				list, err := d.checkpoints.List()
				if err != nil {
					t.Fatal(err)
				}
				for _, checkpoint := range list {
					if checkpoint.Offset != uint64(len(content)-len(bad)) {
						t.Fatalf("checkpoint advanced over uncovered malformed tail: got=%d", checkpoint.Offset)
					}
				}
				stop()
			})
		}
	}
}

func TestNativeTextE2ESizeBoundaryAndMultilineResynchronization(t *testing.T) {
	const limit = 128
	for _, format := range []string{"logfmt", "log4j2-pattern", "logback-pattern"} {
		t.Run(format, func(t *testing.T) {
			store := newRecordingStore()
			address := startIngestServer(t, store)
			stateDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "bounded.log")
			parser, multiline, prefix := "", "", "message="
			if format != "logfmt" {
				parser = "    parser:\n      pattern: '%{level} [%{thread}] - %{message}'\n"
				multiline = "    multiline:\n      line_start_pattern: '^INFO '\n      flush_after: 40ms\n"
				prefix = "INFO [main] - "
			}
			below := prefix + strings.Repeat("a", limit-1-len(prefix))
			exact := prefix + strings.Repeat("b", limit-len(prefix))
			oversized := prefix + strings.Repeat("c", limit+1-len(prefix))
			rejected := oversized + "\n"
			if format != "logfmt" {
				rejected += "\tat app.run(App.java:1)\n\nCaused by: oversized continuation\n"
			}
			last := prefix + "recovered"
			content := below + "\n" + exact + "\r\n" + rejected + last + "\n"
			writeFile(t, logPath, content)
			path := nativeTextE2EConfig(t, format, parser, multiline, address, stateDir, logPath)
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			writeFile(t, path, strings.Replace(string(body), "    poll_interval: 15ms\n", "    poll_interval: 15ms\n    max_event_bytes: 128\n", 1))
			d := newE2EDaemon(t, path)
			stop := nativeTextRunDaemon(t, d)
			waitFor(t, 10*time.Second, "bounded native records delivered", func() bool { return store.count() == 3 })
			waitFor(t, 5*time.Second, "oversized native frame covered by acknowledged recovery", func() bool {
				list, err := d.checkpoints.List()
				return err == nil && len(list) == 1 && list[0].Offset == uint64(len(content)) && d.queue.Stats().QueuedBatches == 0
			})
			stop()
			events := store.snapshot()
			if string(events[0].GetRaw()) != below || string(events[1].GetRaw()) != exact || string(events[2].GetRaw()) != last {
				t.Fatalf("size-boundary records or resynchronization corrupted: %v", events)
			}
			if events[2].GetOrigin().GetStartOffset() != uint64(len(content)-len(last)-1) || store.duplicates() != 0 {
				t.Fatal("oversized resynchronization corrupted source position or redelivered events")
			}
		})
	}
}
