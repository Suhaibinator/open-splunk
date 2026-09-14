package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collector/sender"
)

// Real files, config loading, processors, disk WAL, authenticated gRPC, and the
// real ingest service all participate. The expected records are hand-authored.
func TestNativeFixedE2EFramingRejectionAndCheckpoint(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"docker-json-file", "nginx-combined", "apache-common", "apache-combined"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			store := newRecordingStore()
			addr := startIngestServer(t, store)
			stateDir, logDir := t.TempDir(), t.TempDir()
			logPath := filepath.Join(logDir, "native.log")
			cfgPath := writeE2EConfig(t, addr, stateDir, logPath, filepath.Join(t.TempDir(), "token"))
			cfg, err := os.ReadFile(cfgPath)
			if err != nil {
				t.Fatal(err)
			}
			writeFile(t, cfgPath, strings.Replace(string(cfg), "format: ndjson", "format: "+format, 1))
			first, firstMessage := nativeFixedE2ERecord(format, "one")
			second, secondMessage := nativeFixedE2ERecord(format, "two")
			third, thirdMessage := nativeFixedE2ERecord(format, "three")
			malformed := "MALFORMED sensitive-recovery-bytes"
			// Mixed newline encodings and an empty physical record exercise framing.
			content := first + "\r\n" + malformed + "\n\n" + second + "\n"
			writeFile(t, logPath, content)
			d := newE2EDaemon(t, cfgPath)
			ctx, cancel := context.WithCancel(context.Background())
			runErr := make(chan error, 1)
			go func() { runErr <- d.Run(ctx) }()
			stopped := false
			t.Cleanup(func() {
				cancel()
				if !stopped {
					<-runErr
				}
			})
			waitFor(t, 10*time.Second, "two valid native events", func() bool { return store.count() == 2 })
			waitFor(t, 5*time.Second, "malformed and empty records rejected", func() bool { return d.DecodeFailures() == 2 })
			waitFor(t, 5*time.Second, "native checkpoint reaches acknowledged second event", func() bool {
				cps, err := d.checkpoints.List()
				return err == nil && len(cps) == 1 && cps[0].Offset == uint64(len(content))
			})
			// A source snapshot ending without a delimiter is not a complete line.
			// Complete a split write only after the manager has observed its prefix.
			half := len(third) / 2
			appendFile(t, logPath, third[:half])
			time.Sleep(75 * time.Millisecond)
			if store.count() != 2 || d.DecodeFailures() != 2 {
				t.Fatal("partial native record was prematurely emitted or rejected")
			}
			appendFile(t, logPath, third[half:])
			time.Sleep(75 * time.Millisecond)
			if store.count() != 2 || d.DecodeFailures() != 2 {
				t.Fatal("unterminated native record was prematurely emitted or rejected")
			}
			appendFile(t, logPath, "\r\n")
			waitFor(t, 5*time.Second, "split native event delivered", func() bool { return store.count() == 3 })
			finalOffset := uint64(len(content) + len(third) + 2)
			waitFor(t, 5*time.Second, "final native checkpoint and WAL drained", func() bool {
				cps, err := d.checkpoints.List()
				return err == nil && len(cps) == 1 && cps[0].Offset == finalOffset && d.queue.Stats().QueuedBatches == 0
			})
			cancel()
			if err := <-runErr; err != nil {
				t.Fatalf("Run: %v", err)
			}
			stopped = true
			for i, want := range []struct {
				raw, message     string
				start, end, line uint64
			}{
				{first, firstMessage, 0, uint64(len(first) + 2), 1},
				{second, secondMessage, uint64(len(first) + 2 + len(malformed) + 2), uint64(len(content)), 4},
				{third, thirdMessage, uint64(len(content)), finalOffset, 5},
			} {
				event := eventByMessage(store.snapshot(), want.message)
				if event == nil {
					t.Fatalf("event %d missing message %q", i, want.message)
				}
				if !bytes.Equal(event.GetRaw(), []byte(want.raw)) {
					t.Errorf("event %d raw=%q, want %q", i, event.GetRaw(), want.raw)
				}
				origin := event.GetOrigin()
				if origin.GetStartOffset() != want.start || origin.GetEndOffset() != want.end || origin.GetLineNumber() != want.line || origin.GetNextLineNumber() != want.line+1 {
					t.Errorf("event %d source position=%v", i, origin)
				}
				assertStringValue(t, fieldValue(event, "environment"), "prod")
			}
			assertNoDuplicateEventIDs(t, store.snapshot())
			if store.duplicates() != 0 {
				t.Fatalf("duplicate transport deliveries=%d", store.duplicates())
			}
			if got := diskCheckpointOffset(t, stateDir); got != finalOffset {
				t.Fatalf("disk checkpoint=%d want %d", got, finalOffset)
			}
			artifact, err := os.ReadFile(filepath.Join(stateDir, deadLetterFile))
			if err != nil {
				t.Fatal(err)
			}
			lines := bytes.Split(bytes.TrimSpace(artifact), []byte{'\n'})
			if len(lines) != 2 {
				t.Fatalf("dead letters=%d want 2", len(lines))
			}
			for i, line := range lines {
				var record struct {
					Code         string                      `json:"code"`
					SourceRecord sender.RejectedSourceRecord `json:"source_record"`
				}
				if err := json.Unmarshal(line, &record); err != nil {
					t.Fatal(err)
				}
				want := malformed
				if i == 1 {
					want = ""
				}
				if record.Code != "DECODE_ERROR" || string(record.SourceRecord.Bytes) != want || record.SourceRecord.SourcePath != logPath || record.SourceRecord.InputID != "app" {
					t.Errorf("rejection %d=%+v", i, record)
				}
			}
		})
	}
}

func nativeFixedE2ERecord(format, name string) (string, string) {
	if format == "docker-json-file" {
		return `{"log":"` + name + `\n","stream":"stdout","time":"2026-02-28T20:30:12Z"}`, name + "\n"
	}
	message := "GET /" + name + " HTTP/1.1"
	raw := `host - - [28/Feb/2026:20:30:12 +0000] "` + message + `" 200 0`
	if format != "apache-common" {
		raw += ` "-" "agent"`
	}
	return raw, message
}

func TestNativeFixedE2ERejectedTailDoesNotInventCheckpoint(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"docker-json-file", "nginx-combined", "apache-common", "apache-combined"} {
		for _, trailing := range []bool{false, true} {
			name := format + "/all-malformed"
			if trailing {
				name = format + "/trailing-malformed"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				store := newRecordingStore()
				addr := startIngestServer(t, store)
				stateDir, logDir := t.TempDir(), t.TempDir()
				logPath := filepath.Join(logDir, "native.log")
				cfgPath := writeE2EConfig(t, addr, stateDir, logPath, filepath.Join(t.TempDir(), "token"))
				cfg, err := os.ReadFile(cfgPath)
				if err != nil {
					t.Fatal(err)
				}
				writeFile(t, cfgPath, strings.Replace(string(cfg), "format: ndjson", "format: "+format, 1))
				prefix := ""
				wantEvents := 0
				if trailing {
					valid, _ := nativeFixedE2ERecord(format, "before-tail")
					prefix = valid + "\n"
					wantEvents = 1
				}
				writeFile(t, logPath, prefix+"broken-one\r\nbroken-two\n")
				d := newE2EDaemon(t, cfgPath)
				ctx, cancel := context.WithCancel(context.Background())
				runErr := make(chan error, 1)
				go func() { runErr <- d.Run(ctx) }()
				stopped := false
				t.Cleanup(func() {
					cancel()
					if !stopped {
						<-runErr
					}
				})
				waitFor(t, 10*time.Second, "two source rejections and terminal acknowledgments", func() bool {
					return d.DecodeFailures() == 2 && store.count() == wantEvents && d.queue.Stats().QueuedBatches == 0
				})
				waitFor(t, 5*time.Second, "checkpoint remains before rejected tail", func() bool {
					cps, err := d.checkpoints.List()
					return err == nil && len(cps) == 1 && cps[0].Offset == uint64(len(prefix))
				})
				cancel()
				if err := <-runErr; err != nil {
					t.Fatal(err)
				}
				stopped = true
				if got := diskCheckpointOffset(t, stateDir); got != uint64(len(prefix)) {
					t.Fatalf("checkpoint passed rejected tail: %d", got)
				}
				data, err := os.ReadFile(filepath.Join(stateDir, deadLetterFile))
				if err != nil {
					t.Fatal(err)
				}
				lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
				if len(lines) != 2 {
					t.Fatalf("durable rejections=%d want 2", len(lines))
				}
				for i, line := range lines {
					var record struct {
						SourceRecord sender.RejectedSourceRecord `json:"source_record"`
					}
					if err := json.Unmarshal(line, &record); err != nil {
						t.Fatal(err)
					}
					want := []string{"broken-one", "broken-two"}[i]
					if string(record.SourceRecord.Bytes) != want {
						t.Fatalf("recovery bytes=%q want %q", record.SourceRecord.Bytes, want)
					}
				}
			})
		}
	}
}
