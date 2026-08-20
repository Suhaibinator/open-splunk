package searchhistory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func futureHistoryField() protoreflect.RawFields {
	return protowire.AppendVarint(protowire.AppendTag(nil, 1000, protowire.VarintType), 1)
}

// TestRecordRejectsUnknownFieldsAtEveryTreeDepth walks every message-shaped
// slot reachable from a search-history entry and proves the strict walker still
// sees each of them through the store's public write path.
func TestRecordRejectsUnknownFieldsAtEveryTreeDepth(t *testing.T) {
	created := time.Now().UTC().Add(-time.Minute)
	sample := func() *opensplunk.SearchHistoryEntry {
		return historyEntry(
			"depth", "index=main", "search",
			opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created,
		)
	}
	tests := map[string]func(*opensplunk.SearchHistoryEntry) protoreflect.Message{
		"root": func(entry *opensplunk.SearchHistoryEntry) protoreflect.Message {
			return entry.ProtoReflect()
		},
		"definition": func(entry *opensplunk.SearchHistoryEntry) protoreflect.Message {
			return entry.Definition.ProtoReflect()
		},
		"definition time range": func(entry *opensplunk.SearchHistoryEntry) protoreflect.Message {
			return entry.Definition.TimeRange.ProtoReflect()
		},
		"source": func(entry *opensplunk.SearchHistoryEntry) protoreflect.Message {
			return entry.Source.ProtoReflect()
		},
		"resolved time range": func(entry *opensplunk.SearchHistoryEntry) protoreflect.Message {
			return entry.ResolvedTimeRange.ProtoReflect()
		},
		"resolved earliest well-known timestamp": func(entry *opensplunk.SearchHistoryEntry) protoreflect.Message {
			return entry.ResolvedTimeRange.Earliest.ProtoReflect()
		},
		"created-at well-known timestamp": func(entry *opensplunk.SearchHistoryEntry) protoreflect.Message {
			return entry.CreatedAt.ProtoReflect()
		},
	}
	for name, target := range tests {
		t.Run(name, func(t *testing.T) {
			_, store := openTestStore(t, Options{})
			entry := sample()
			target(entry).SetUnknown(futureHistoryField())
			if _, err := store.Record(context.Background(), AccessScope{
				TenantID: "tenant", OwnerID: "owner",
			}, entry); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("Record() error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	t.Run("clean control entry is accepted", func(t *testing.T) {
		_, store := openTestStore(t, Options{})
		if _, err := store.Record(context.Background(), AccessScope{
			TenantID: "tenant", OwnerID: "owner",
		}, sample()); err != nil {
			t.Fatalf("Record() on a clean entry error = %v", err)
		}
	})
}

// TestRecordSeparatesTypedNilFromUnknownFields pins the walker contract that a
// typed-nil singular field is absent (not unknown) and that a typed-nil or nil
// top-level entry is rejected before any protoreflect walk happens.
func TestRecordSeparatesTypedNilFromUnknownFields(t *testing.T) {
	_, store := openTestStore(t, Options{})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	created := time.Now().UTC().Add(-time.Minute)

	typedNilDefinition := historyEntry(
		"typed-nil", "index=main", "search",
		opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created,
	)
	typedNilDefinition.Definition = (*opensplunk.SearchDefinition)(nil)
	_, err := store.Record(ctx, scope, typedNilDefinition)
	if !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("typed-nil definition error = %v, want ErrInvalidArgument", err)
	}
	if got := err.Error(); !strings.Contains(got, "search definition is required") {
		t.Fatalf("typed-nil definition error = %q, want the missing-definition reason", got)
	}

	var typedNilEntry *opensplunk.SearchHistoryEntry
	if _, err := store.Record(ctx, scope, typedNilEntry); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("typed-nil entry error = %v, want ErrInvalidArgument", err)
	}
}

// TestRecordRejectsUnknownFieldsInsideRepeatedKnowledgeObjects proves the
// repeated-message branch survives when only the last element is poisoned and
// when the poisoned element sits behind a clone.
func TestRecordRejectsUnknownFieldsInsideRepeatedKnowledgeObjects(t *testing.T) {
	_, store := openTestStore(t, Options{})
	created := time.Now().UTC().Add(-time.Minute)
	entry := historyEntry(
		"repeated", "index=main", "search",
		opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created,
	)
	entry.KnowledgeSnapshot = historyKnowledgeSnapshotSummary(3)
	clean := proto.Clone(entry).(*opensplunk.SearchHistoryEntry)
	objects := entry.KnowledgeSnapshot.Objects
	objects[len(objects)-1].ProtoReflect().SetUnknown(futureHistoryField())

	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	if _, err := store.Record(context.Background(), scope, entry); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Record() error = %v, want ErrInvalidArgument", err)
	}
	if _, err := store.Record(context.Background(), scope, clean); err != nil {
		t.Fatalf("Record() on the pre-poison clone error = %v", err)
	}
}
