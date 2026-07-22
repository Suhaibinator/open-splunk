package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

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
