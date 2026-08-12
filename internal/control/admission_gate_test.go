package control

import (
	"errors"
	"testing"
)

func TestSharedAdmissionGateIsUniqueBoundedAndFailFast(t *testing.T) {
	database := openTestDB(t)
	gate, err := database.SharedAdmissionGate("knowledge-resolution", 2)
	if err != nil {
		t.Fatalf("SharedAdmissionGate(): %v", err)
	}
	same, err := database.SharedAdmissionGate("knowledge-resolution", 2)
	if err != nil || same != gate {
		t.Fatalf("SharedAdmissionGate(same) = (%p, %v), want %p", same, err, gate)
	}
	if _, err := database.SharedAdmissionGate("knowledge-resolution", 3); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("SharedAdmissionGate(mismatched capacity) error = %v, want ErrInvalidArgument", err)
	}
	if !gate.TryAcquire() {
		t.Fatal("shared gate did not grant its first permit")
	}
	if !gate.TryAcquire() {
		t.Fatal("shared gate did not grant its second permit")
	}
	if gate.TryAcquire() {
		t.Fatal("shared gate did not enforce its exact fail-fast capacity")
	}
	gate.Release()
	if !gate.TryAcquire() {
		t.Fatal("shared gate did not return a released permit")
	}
	gate.Release()
	gate.Release()
}

func TestSharedAdmissionGateRejectsInvalidConstruction(t *testing.T) {
	if _, err := (*DB)(nil).SharedAdmissionGate("gate", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil DB error = %v, want ErrInvalidArgument", err)
	}
	database := openTestDB(t)
	for _, test := range []struct {
		name     string
		capacity int
	}{
		{name: "", capacity: 1},
		{name: "   ", capacity: 1},
		{name: "gate", capacity: 0},
	} {
		if _, err := database.SharedAdmissionGate(test.name, test.capacity); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("SharedAdmissionGate(%q, %d) error = %v, want ErrInvalidArgument", test.name, test.capacity, err)
		}
	}
}
