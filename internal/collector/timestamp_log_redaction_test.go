package collector

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collector/input"
)

// redactionLogBuffer is a concurrency-safe io.Writer for capturing log output.
type redactionLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *redactionLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *redactionLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestDecodeTimestampErrorDoesNotLeakPayload proves FIX 8: a decode failure from
// a secret-bearing invalid timestamp must never carry the payload value into the
// decode error nor into the daemon's decode-failure log.
func TestDecodeTimestampErrorDoesNotLeakPayload(t *testing.T) {
	t.Parallel()
	const secret = "topsecret-value-8f3a2b-DO-NOT-LEAK"

	dec, err := NewDecoder(DecodeConfig{
		Format: InputFormatNDJSON, InputID: "app", IndexName: "main",
		Source: "s", Sourcetype: "st", Host: "h",
	})
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}

	line := []byte(`{"timestamp":"not-a-date-` + secret + `","message":"m"}`)
	pos := SourcePosition{FileIdentity: "dev=1;ino=2;fp=abc", StartOffset: 0, EndOffset: uint64(len(line)), LineNumber: 1}
	_, derr := dec.Decode(line, pos, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if derr == nil {
		t.Fatal("expected a decode error for the invalid timestamp")
	}
	if strings.Contains(derr.Error(), secret) {
		t.Fatalf("decode error leaked the payload secret: %v", derr)
	}

	// The daemon logs the decode failure; that log must not carry the secret.
	buf := &redactionLogBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	d := &Daemon{log: logger}
	src := input.SourceRef{
		Identity:    input.FileIdentity{Device: 1, Inode: 2, Fingerprint: "abc"},
		StartOffset: 0, EndOffset: uint64(len(line)), LineNumber: 1,
	}
	d.recordDecodeFailure("app", src, len(line), derr)

	if got := buf.String(); strings.Contains(got, secret) {
		t.Fatalf("daemon decode-failure log leaked the payload secret:\n%s", got)
	}
}
