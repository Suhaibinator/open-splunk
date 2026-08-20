package gradethiscorpus

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

func TestStoreCanonicalStoresAndReturnsExactFixture(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "store-canonical")
	const tenantID = "tenant-gradethis"
	var (
		storeCalls int
		stored     ingest.StoreBatch
	)
	store := ingest.EventStoreFunc(func(
		gotContext context.Context,
		batch ingest.StoreBatch,
	) (ingest.StoreResult, error) {
		storeCalls++
		if gotContext != ctx {
			t.Fatal("StoreCanonical did not pass the caller context to EventStore")
		}
		stored = batch
		return ingest.StoreResult{
			Accepted:           uint32(len(batch.Events)),
			OriginalEventCount: batch.OriginalEventCount,
		}, nil
	})

	got, err := StoreCanonical(ctx, store, tenantID)
	if err != nil {
		t.Fatalf("StoreCanonical(): %v", err)
	}
	if storeCalls != 1 {
		t.Fatalf("EventStore calls = %d, want 1", storeCalls)
	}

	wantProfile := Fixture()
	if !reflect.DeepEqual(got.Profile, wantProfile) {
		t.Fatal("stored fixture profile does not exactly match Fixture()")
	}
	if stored.TenantID != tenantID ||
		stored.CollectorID != "gradethis-corpus" ||
		stored.BatchID != "gradethis-corpus-batch" ||
		stored.BatchSequence != 1 {
		t.Fatalf(
			"stored batch identity = tenant %q, collector %q, batch %q, sequence %d",
			stored.TenantID,
			stored.CollectorID,
			stored.BatchID,
			stored.BatchSequence,
		)
	}
	if stored.OriginalEventCount != uint32(len(wantProfile.Events)) ||
		len(stored.Events) != len(wantProfile.Events) {
		t.Fatalf(
			"stored event counts = original %d, events %d, want %d",
			stored.OriginalEventCount,
			len(stored.Events),
			len(wantProfile.Events),
		)
	}
	if len(stored.RejectedEvents) != 0 {
		t.Fatalf("stored rejected events = %d, want 0", len(stored.RejectedEvents))
	}
	if !stored.ReceivedAt.Equal(wantProfile.IndexTime) {
		t.Fatalf(
			"stored received time = %v, want %v",
			stored.ReceivedAt,
			wantProfile.IndexTime,
		)
	}
	if wantDigest := sha256.Sum256(wantProfile.NDJSON); stored.SourceBatchSHA256 != wantDigest {
		t.Fatalf(
			"stored source digest = %x, want %x",
			stored.SourceBatchSHA256,
			wantDigest,
		)
	}
	if len(got.EventsByID) != len(wantProfile.Events) {
		t.Fatalf(
			"returned event map size = %d, want %d",
			len(got.EventsByID),
			len(wantProfile.Events),
		)
	}

	seenEventIDs := make(map[string]struct{}, len(wantProfile.Events))
	for index, expected := range wantProfile.Events {
		storedEvent := stored.Events[index]
		if storedEvent == nil || storedEvent.Event == nil {
			t.Fatalf("stored event %d is nil", index)
		}
		if storedEvent.TenantID != tenantID ||
			storedEvent.CollectorID != "gradethis-corpus" ||
			storedEvent.BatchID != "gradethis-corpus-batch" ||
			!storedEvent.IndexTime.Equal(wantProfile.IndexTime) {
			t.Fatalf(
				"stored metadata for %q = %#v",
				expected.ID,
				storedEvent,
			)
		}
		if storedEvent.Event.GetIndexName() != IndexName ||
			!storedEvent.Event.GetEventTime().AsTime().Equal(
				wantProfile.BaseTime.Add(expected.Offset),
			) {
			t.Fatalf(
				"stored canonical event for %q = %#v",
				expected.ID,
				storedEvent.Event,
			)
		}
		if mapped, exists := got.EventsByID[expected.ID]; !exists {
			t.Fatalf("returned event map is missing %q", expected.ID)
		} else if mapped != storedEvent.Event {
			t.Fatalf(
				"returned event map value for %q is not the stored event",
				expected.ID,
			)
		}
		eventID := storedEvent.Event.GetEventId()
		if eventID == "" {
			t.Fatalf("stored canonical event %q has no event ID", expected.ID)
		}
		if _, duplicate := seenEventIDs[eventID]; duplicate {
			t.Fatalf("stored canonical event ID %q is duplicated", eventID)
		}
		seenEventIDs[eventID] = struct{}{}
	}
}

func TestStoreCanonicalPropagatesStoreError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("store unavailable")
	store := ingest.EventStoreFunc(func(
		context.Context,
		ingest.StoreBatch,
	) (ingest.StoreResult, error) {
		return ingest.StoreResult{}, sentinel
	})

	got, err := StoreCanonical(context.Background(), store, "tenant")
	if !errors.Is(err, sentinel) {
		t.Fatalf("StoreCanonical() error = %v, want wrapped sentinel", err)
	}
	if !reflect.DeepEqual(got, StoredFixture{}) {
		t.Fatalf("StoreCanonical() fixture on error = %#v, want zero value", got)
	}
}

func TestStoreCanonicalRejectsInvalidInputsBeforeStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      context.Context
		nilStore bool
		tenant   string
	}{
		{
			name:   "nil context",
			tenant: "tenant",
		},
		{
			name:     "nil event store",
			ctx:      context.Background(),
			nilStore: true,
			tenant:   "tenant",
		},
		{
			name:   "empty tenant",
			ctx:    context.Background(),
			tenant: "",
		},
		{
			name:   "tenant with surrounding whitespace",
			ctx:    context.Background(),
			tenant: " tenant ",
		},
		{
			name:   "tenant with control character",
			ctx:    context.Background(),
			tenant: "tenant\nother",
		},
		{
			name:   "tenant over byte limit",
			ctx:    context.Background(),
			tenant: strings.Repeat("t", maximumInspectionTenantBytes+1),
		},
		{
			name:   "tenant with invalid UTF-8",
			ctx:    context.Background(),
			tenant: string([]byte{0xff}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			storeCalls := 0
			var store ingest.EventStore
			if !test.nilStore {
				store = ingest.EventStoreFunc(func(
					context.Context,
					ingest.StoreBatch,
				) (ingest.StoreResult, error) {
					storeCalls++
					return ingest.StoreResult{}, nil
				})
			}
			got, err := StoreCanonical(test.ctx, store, test.tenant)
			if err == nil {
				t.Fatal("StoreCanonical() error = nil, want rejection")
			}
			if storeCalls != 0 {
				t.Fatalf("EventStore calls = %d, want 0", storeCalls)
			}
			if !reflect.DeepEqual(got, StoredFixture{}) {
				t.Fatalf(
					"StoreCanonical() fixture on invalid input = %#v, want zero value",
					got,
				)
			}
		})
	}
}

func TestStoreCanonicalRejectsInexactStoreResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		result ingest.StoreResult
	}{
		{
			name: "accepted count",
			result: ingest.StoreResult{
				Accepted:           19,
				OriginalEventCount: 20,
			},
		},
		{
			name: "duplicate count",
			result: ingest.StoreResult{
				Accepted:           20,
				Duplicate:          1,
				OriginalEventCount: 20,
			},
		},
		{
			name: "original event count",
			result: ingest.StoreResult{
				Accepted:           20,
				OriginalEventCount: 19,
			},
		},
		{
			name: "rejected events",
			result: ingest.StoreResult{
				Accepted:           20,
				OriginalEventCount: 20,
				RejectedEvents: []*opensplunk.EventRejection{
					{},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := ingest.EventStoreFunc(func(
				context.Context,
				ingest.StoreBatch,
			) (ingest.StoreResult, error) {
				return test.result, nil
			})
			got, err := StoreCanonical(
				context.Background(),
				store,
				"tenant",
			)
			if err == nil {
				t.Fatal("StoreCanonical() error = nil, want contract rejection")
			}
			if !reflect.DeepEqual(got, StoredFixture{}) {
				t.Fatalf(
					"StoreCanonical() fixture on inexact result = %#v, want zero value",
					got,
				)
			}
		})
	}
}
