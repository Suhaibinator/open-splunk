package searchhistory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestJobJournalPersistsExactDetachedKnowledgeSnapshotSummary(t *testing.T) {
	_, store := openTestStore(t, Options{})
	journal, err := NewJobJournal(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 8, 12, 0, 0, 123_456_789, time.UTC)
	want := historyKnowledgeSnapshotSummary(2)
	queued := journalJob("journal-knowledge", searchjobs.StateQueued, now)
	queued.KnowledgeSnapshot = proto.Clone(want).(*opensplunk.KnowledgeSnapshotSummary)
	if err := journal.Admit(context.Background(), queued); err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	queued.KnowledgeSnapshot.Ref.SnapshotSha256[0] ^= 0xff
	queued.KnowledgeSnapshot.Objects[0].GetAuthorizedObject().KnowledgeObjectId = "mutated"
	queued.KnowledgeSnapshot.LookupAssets[0].Asset.ContentSha256[0] ^= 0xff

	terminal := journalJob("journal-knowledge", searchjobs.StateCompleted, now)
	terminal.KnowledgeSnapshot = proto.Clone(want).(*opensplunk.KnowledgeSnapshotSummary)
	terminal.EffectiveIndexes = []string{"main"}
	terminal.StartedAt = now.Add(-30 * time.Second)
	terminal.FinishedAt = now.Add(-10 * time.Second)
	if err := journal.Finalize(context.Background(), terminal); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	terminal.KnowledgeSnapshot.Objects[1].GetAuthorizedObject().Name = "mutated"
	terminal.KnowledgeSnapshot.LookupAssets[0].Asset.LookupAssetId = "mutated"

	got, err := store.Get(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, terminal.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.GetKnowledgeSnapshot(), want) || got.GetKnowledgeSnapshot() == terminal.KnowledgeSnapshot {
		t.Fatalf("persisted knowledge snapshot = %+v, want exact detached summary", got.GetKnowledgeSnapshot())
	}
}

func TestKnowledgeSnapshotSummaryIsDetachedIdempotentAndImmutableAcrossLifecycle(t *testing.T) {
	_, store := openTestStore(t, Options{})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	created := time.Now().UTC().Add(-time.Minute)
	want := historyKnowledgeSnapshotSummary(2)
	pending := pendingHistoryEntry("snapshot-lifecycle", "index=main", created)
	pending.KnowledgeSnapshot = proto.Clone(want).(*opensplunk.KnowledgeSnapshotSummary)

	admitted, err := store.BeginAttempt(ctx, scope, pending)
	if err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}
	pending.KnowledgeSnapshot.Ref.TenantCatalogStateToken[0] ^= 0xff
	pending.KnowledgeSnapshot.Objects[0].GetAuthorizedObject().Name = "caller mutation"
	if !proto.Equal(admitted.GetKnowledgeSnapshot(), want) {
		t.Fatal("admitted summary aliases caller-owned storage")
	}
	admitted.KnowledgeSnapshot.Ref.SnapshotSha256[0] ^= 0xff

	canonicalPending := pendingHistoryEntry("snapshot-lifecycle", "index=main", created)
	canonicalPending.KnowledgeSnapshot = proto.Clone(want).(*opensplunk.KnowledgeSnapshotSummary)
	if retried, retryErr := store.BeginAttempt(ctx, scope, canonicalPending); retryErr != nil || !proto.Equal(retried.GetKnowledgeSnapshot(), want) {
		t.Fatalf("idempotent BeginAttempt() = (%+v, %v)", retried, retryErr)
	}

	terminal := historyEntry("snapshot-lifecycle", canonicalPending.Definition.Spl, "search", opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created)
	terminal.KnowledgeSnapshot = proto.Clone(want).(*opensplunk.KnowledgeSnapshotSummary)
	completed, err := store.CompleteAttempt(ctx, scope, terminal)
	if err != nil {
		t.Fatalf("CompleteAttempt() error = %v", err)
	}
	completed.KnowledgeSnapshot.Objects[0].GetAuthorizedObject().KnowledgeObjectId = "result mutation"
	got, err := store.Get(ctx, scope, terminal.SearchJobId)
	if err != nil || !proto.Equal(got.GetKnowledgeSnapshot(), want) {
		t.Fatalf("Get() knowledge snapshot = (%+v, %v)", got.GetKnowledgeSnapshot(), err)
	}
	if _, err := store.CompleteAttempt(ctx, scope, terminal); err != nil {
		t.Fatalf("idempotent CompleteAttempt() error = %v", err)
	}
}

func TestCompleteAttemptRejectsChangedOrRemovedKnowledgeSnapshotAdmission(t *testing.T) {
	tests := map[string]func(*opensplunk.SearchHistoryEntry){
		"changed digest": func(entry *opensplunk.SearchHistoryEntry) {
			entry.KnowledgeSnapshot.Ref.SnapshotSha256[0] ^= 0xff
		},
		"removed configured summary": func(entry *opensplunk.SearchHistoryEntry) {
			entry.KnowledgeSnapshot = nil
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			_, store := openTestStore(t, Options{})
			ctx := context.Background()
			scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
			created := time.Now().UTC().Add(-time.Minute)
			pending := pendingHistoryEntry("snapshot-conflict", "index=main", created)
			pending.KnowledgeSnapshot = historyKnowledgeSnapshotSummary(0)
			if _, err := store.BeginAttempt(ctx, scope, pending); err != nil {
				t.Fatal(err)
			}
			terminal := historyEntry("snapshot-conflict", pending.Definition.Spl, "search", opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created)
			terminal.KnowledgeSnapshot = proto.Clone(pending.KnowledgeSnapshot).(*opensplunk.KnowledgeSnapshotSummary)
			mutate(terminal)
			if _, err := store.CompleteAttempt(ctx, scope, terminal); !errors.Is(err, control.ErrVersionConflict) {
				t.Fatalf("CompleteAttempt() error = %v, want ErrVersionConflict", err)
			}
			if _, err := store.Get(ctx, scope, terminal.SearchJobId); !errors.Is(err, control.ErrNotFound) {
				t.Fatalf("changed completion became visible: %v", err)
			}
		})
	}
}

func TestHistoryNormalizationRejectsInvalidKnowledgeSummaryBeforeClone(t *testing.T) {
	unknown := historyKnowledgeSnapshotSummary(1)
	unknown.Objects[0].ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 1))

	oversized := historyKnowledgeSnapshotSummary(0)
	oversized.ProtoReflect().SetUnknown(protowire.AppendBytes(
		protowire.AppendTag(nil, 100, protowire.BytesType),
		bytes.Repeat([]byte{0x7f}, knowledgesnapshot.MaximumSummaryBytes),
	))

	amplified := historyKnowledgeSnapshotSummary(0)
	amplified.Ref.ObjectCount = knowledgesnapshot.MaximumSummaryObjects + 1
	amplified.Objects = make([]*opensplunk.KnowledgeSnapshotObjectSummary, knowledgesnapshot.MaximumSummaryObjects+1)

	for name, summary := range map[string]*opensplunk.KnowledgeSnapshotSummary{
		"unknown field":            unknown,
		"oversized wire shape":     oversized,
		"amplified repeated shape": amplified,
	} {
		t.Run(name, func(t *testing.T) {
			entry := pendingHistoryEntry("invalid-snapshot", "index=main", time.Now().UTC())
			entry.KnowledgeSnapshot = summary
			if _, _, err := normalizePendingEntry(entry); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("normalizePendingEntry() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestPersistedKnowledgeSnapshotCorruptionIsNotCallerInvalidArgument(t *testing.T) {
	database, store := openTestStore(t, Options{})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	created := time.Now().UTC().Add(-time.Minute)
	entry := historyEntry("snapshot-corruption", "index=main", "search", opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created)
	entry.KnowledgeSnapshot = historyKnowledgeSnapshotSummary(1)
	if _, err := store.Record(ctx, scope, entry); err != nil {
		t.Fatal(err)
	}
	rewriteStoredHistoryProto(t, database, &historyRecord{}, entry.SearchJobId, func(stored *opensplunk.SearchHistoryEntry) {
		stored.KnowledgeSnapshot.Objects[0].ProtoReflect().SetUnknown(
			protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 1),
		)
	})
	_, err := store.Get(ctx, scope, entry.SearchJobId)
	assertPersistedCorruption(t, "Get knowledge snapshot", err)
}

func historyKnowledgeSnapshotSummary(objectCount int) *opensplunk.KnowledgeSnapshotSummary {
	ref := &opensplunk.KnowledgeSnapshotRef{
		SnapshotSha256:          bytes.Repeat([]byte{0x42}, sha256.Size),
		TenantCatalogRevision:   7,
		TenantCatalogStateToken: bytes.Repeat([]byte{0x73}, sha256.Size),
		ObjectCount:             uint32(objectCount),
		LookupAssetCount:        1,
	}
	objects := make([]*opensplunk.KnowledgeSnapshotObjectSummary, objectCount)
	for index := range objects {
		objects[index] = &opensplunk.KnowledgeSnapshotObjectSummary{
			ResolutionOrdinal: uint32(index),
			ObjectType:        opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			Stage:             opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
			Disclosure: &opensplunk.KnowledgeSnapshotObjectSummary_AuthorizedObject{
				AuthorizedObject: &opensplunk.KnowledgeSnapshotAuthorizedObjectSummary{
					KnowledgeObjectId: "object-" + string(rune('a'+index)),
					Version:           uint64(index + 1),
					Name:              "Object " + string(rune('A'+index)),
				},
			},
		}
	}
	return &opensplunk.KnowledgeSnapshotSummary{
		Ref:     ref,
		Objects: objects,
		LookupAssets: []*opensplunk.KnowledgeSnapshotLookupAsset{{
			AssetOrdinal:  0,
			LookupId:      "lookup-history",
			LookupVersion: 6,
			Asset: &opensplunk.KnowledgeLookupAssetVersionReference{
				LookupAssetId: "asset-history",
				Version:       4,
				SizeBytes:     128,
				ContentSha256: bytes.Repeat([]byte{0x5c}, sha256.Size),
			},
		}},
	}
}
