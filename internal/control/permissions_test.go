package control

import (
	"context"
	"errors"
	"testing"
)

func TestOpenRejectsFilesystemAndUnconfiguredConnections(t *testing.T) {
	for _, dsn := range []string{"", ":memory:", "/tmp/control.sqlite", "file:///tmp/control.sqlite"} {
		if _, err := Open(context.Background(), dsn); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("Open(%q)=%v", dsn, err)
		}
	}
}
