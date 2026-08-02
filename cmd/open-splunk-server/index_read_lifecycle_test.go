package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
)

func TestIndexReadLifecycleSharesOneRegistryAcrossAdmissionAndRetirement(
	t *testing.T,
) {
	t.Parallel()

	catalog := &catalogIndexReadTestCatalog{indexes: []control.Index{
		readableTestIndex("main", control.IndexStateActive),
	}}
	lifecycle, err := newIndexReadLifecycle(catalog, "tenant-a")
	if err != nil {
		t.Fatalf("newIndexReadLifecycle(): %v", err)
	}
	readContext, release, err := lifecycle.admission.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"main"},
	)
	if err != nil {
		t.Fatalf("Acquire(): %v", err)
	}

	retired := make(chan error, 1)
	go func() {
		retired <- lifecycle.retirement.Retire(
			context.Background(),
			"tenant-a",
			"main",
		)
	}()
	select {
	case <-readContext.Done():
		if cause := context.Cause(readContext); !errors.Is(
			cause,
			indexread.ErrUnavailable,
		) {
			t.Fatalf("read cancellation cause = %v, want ErrUnavailable", cause)
		}
	case <-time.After(time.Second):
		t.Fatal("lifecycle retirement did not cancel lifecycle admission")
	}
	select {
	case err := <-retired:
		t.Fatalf("Retire returned before lifecycle read release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case err := <-retired:
		if err != nil {
			t.Fatalf("Retire(): %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lifecycle retirement did not join released read")
	}
}

func TestNewIndexReadLifecycleRejectsInvalidComposition(t *testing.T) {
	t.Parallel()

	if lifecycle, err := newIndexReadLifecycle(nil, "tenant-a"); err == nil || lifecycle != nil {
		t.Fatalf(
			"newIndexReadLifecycle() = (%v, %v), want nil and error",
			lifecycle,
			err,
		)
	}
}
