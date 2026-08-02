package indexread

import (
	"context"
	"errors"
	"testing"
)

func TestUnfencedAdmissionIsExplicitContextOnlyBypass(t *testing.T) {
	t.Parallel()

	admitted, release, err := (UnfencedAdmission{}).Acquire(
		context.Background(),
		"",
		nil,
	)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if admitted == nil || release == nil {
		t.Fatal("Acquire returned an incomplete lease")
	}
	release()

	var nilContext context.Context
	if _, _, err := (UnfencedAdmission{}).Acquire(nilContext, "", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Acquire(nil) error = %v, want ErrInvalidArgument", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := (UnfencedAdmission{}).Acquire(canceled, "", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire(canceled) error = %v, want context.Canceled", err)
	}
}
