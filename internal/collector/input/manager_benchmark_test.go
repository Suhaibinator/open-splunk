package input

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var (
	benchmarkPaths       []string
	benchmarkFingerprint fingerprintDigest
)

func BenchmarkTailerPollTimer(b *testing.B) {
	timer := tailerPollTimer{}
	defer timer.stop()
	ctx := context.Background()
	if !timer.wait(ctx) {
		b.Fatal("initial poll wait unexpectedly canceled")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if !timer.wait(ctx) {
			b.Fatal("poll wait unexpectedly canceled")
		}
	}
}

func BenchmarkTailerGuardFingerprint(b *testing.B) {
	path := filepath.Join(b.TempDir(), "app.log")
	content := make([]byte, defaultFingerprintBytes)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		b.Fatalf("create source: %v", err)
	}
	tracked, _ := newTrackedTailerForTest(b, path, uint64(len(content)), 2)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		fingerprint, err := tracked.readGuardFingerprint()
		if err != nil {
			b.Fatal(err)
		}
		benchmarkFingerprint = fingerprint
	}
}

func BenchmarkTailerCleanEOFTracking(b *testing.B) {
	path := filepath.Join(b.TempDir(), "app.log")
	content := make([]byte, defaultFingerprintBytes)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		b.Fatalf("create source: %v", err)
	}
	tracked, _ := newTrackedTailerForTest(b, path, uint64(len(content)), 2)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		size, trackable := tracked.trackGrowthAndTruncate()
		if !trackable || !tracked.canWaitAtCleanBoundary(size) {
			b.Fatalf(
				"clean EOF state = size %d offset %d trackable %t",
				size,
				tracked.offset,
				trackable,
			)
		}
	}
}

func BenchmarkMatchPathsHighSourceCount(b *testing.B) {
	const sourceCount = 1_000
	dir := b.TempDir()
	for index := range sourceCount {
		path := filepath.Join(dir, fmt.Sprintf("source-%08d.log", index))
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			b.Fatalf("create source %d: %v", index, err)
		}
	}
	manager := &manager{
		cfg: Config{Include: []string{filepath.Join(dir, "*.log")}},
	}
	if paths := manager.matchPaths(); len(paths) != sourceCount {
		b.Fatalf("matched %d sources, want %d", len(paths), sourceCount)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkPaths = manager.matchPaths()
	}
}
