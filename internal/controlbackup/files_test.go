//go:build darwin || linux

package controlbackup

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestContextReaderStopsAtCancellationBoundary(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	reader := contextReader{ctx: ctx, reader: bytes.NewReader([]byte("payload"))}
	buffer := make([]byte, 3)
	read, err := reader.Read(buffer)
	if err != nil || read != len(buffer) {
		t.Fatalf("initial read = (%d, %v), want (%d, nil)", read, err, len(buffer))
	}
	cancel()
	read, err = reader.Read(buffer)
	if read != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read = (%d, %v), want (0, context.Canceled)", read, err)
	}
}
