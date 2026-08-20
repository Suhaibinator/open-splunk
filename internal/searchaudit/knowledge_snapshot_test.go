package searchaudit

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
)

func searchAuditTestKnowledgeSnapshotRef() *opensplunk.KnowledgeSnapshotRef {
	return &opensplunk.KnowledgeSnapshotRef{
		SnapshotSha256:          bytes.Repeat([]byte{0x42}, 32),
		TenantCatalogRevision:   7,
		TenantCatalogStateToken: bytes.Repeat([]byte{0x73}, 32),
		ObjectCount:             2,
		LookupAssetCount:        1,
	}
}

func TestKnowledgeSnapshotReferenceRoundTripsAndDetaches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
	appendSearchAuditTestEvent(
		t, store, database, ctx, "tenant-snapshot",
		searchAuditTestDefinition("owner", "job-without-snapshot", 0),
	)
	want := searchAuditTestKnowledgeSnapshotRef()
	definition := searchAuditTestDefinition("owner", "job-snapshot", time.Microsecond)
	definition.KnowledgeSnapshot = proto.Clone(want).(*opensplunk.KnowledgeSnapshotRef)
	appendSearchAuditTestEvent(t, store, database, ctx, "tenant-snapshot", definition)
	definition.KnowledgeSnapshot.SnapshotSha256[0] ^= 0xff
	definition.KnowledgeSnapshot.TenantCatalogStateToken[0] ^= 0xff

	page, err := store.List(ctx, "tenant-snapshot", ListRequest{IncludeTotal: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 || page.Events[0].Sequence != 2 ||
		!proto.Equal(page.Events[0].KnowledgeSnapshot, want) ||
		page.Events[1].Sequence != 1 || page.Events[1].KnowledgeSnapshot != nil {
		t.Fatalf("snapshot page = %+v", page)
	}
	if err := page.Events[0].ValidateForTenant("tenant-snapshot"); err != nil {
		t.Fatalf("ValidateForTenant: %v", err)
	}
	page.Events[0].KnowledgeSnapshot.SnapshotSha256[0] ^= 0xff
	page.Events[0].KnowledgeSnapshot.TenantCatalogStateToken[0] ^= 0xff
	again, err := store.List(ctx, "tenant-snapshot", ListRequest{})
	if err != nil || len(again.Events) != 2 ||
		!proto.Equal(again.Events[0].KnowledgeSnapshot, want) {
		t.Fatalf("detached second page = (%+v, %v)", again, err)
	}
}

func TestExactZeroLookupCountRoundTrips(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
	want := searchAuditTestKnowledgeSnapshotRef()
	want.LookupAssetCount = 0
	definition := searchAuditTestDefinition("owner", "job-zero-lookups", 0)
	definition.KnowledgeSnapshot = proto.Clone(want).(*opensplunk.KnowledgeSnapshotRef)
	appendSearchAuditTestEvent(t, store, database, ctx, "tenant-zero-lookups", definition)

	var count sql.NullInt64
	if err := database.GORMDB().Raw(`
		SELECT knowledge_snapshot_lookup_asset_count
		FROM search_attempt_audit_events
		WHERE tenant_id = ? AND sequence = 1
	`, "tenant-zero-lookups").Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if !count.Valid || count.Int64 != 0 {
		t.Fatalf("persisted lookup count = %+v, want exact zero", count)
	}
	page, err := store.List(ctx, "tenant-zero-lookups", ListRequest{})
	if err != nil || len(page.Events) != 1 ||
		!proto.Equal(page.Events[0].KnowledgeSnapshot, want) {
		t.Fatalf("exact-zero page = (%+v, %v)", page, err)
	}
}

func TestAppendRejectsMalformedKnowledgeSnapshotReferenceBeforeMutation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
	tests := []struct {
		name   string
		mutate func(*opensplunk.KnowledgeSnapshotRef)
	}{
		{name: "digest", mutate: func(ref *opensplunk.KnowledgeSnapshotRef) { ref.SnapshotSha256 = ref.SnapshotSha256[:31] }},
		{name: "revision", mutate: func(ref *opensplunk.KnowledgeSnapshotRef) { ref.TenantCatalogRevision = math.MaxInt64 }},
		{name: "token", mutate: func(ref *opensplunk.KnowledgeSnapshotRef) {
			ref.TenantCatalogStateToken = ref.TenantCatalogStateToken[:31]
		}},
		{name: "object count", mutate: func(ref *opensplunk.KnowledgeSnapshotRef) { ref.ObjectCount = maximumKnowledgeObjects + 1 }},
		{name: "lookup asset count", mutate: func(ref *opensplunk.KnowledgeSnapshotRef) {
			ref.LookupAssetCount = maximumKnowledgeLookupAssets + 1
		}},
		{name: "unknown field", mutate: func(ref *opensplunk.KnowledgeSnapshotRef) { ref.ProtoReflect().SetUnknown([]byte{0x30, 0x01}) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ref := searchAuditTestKnowledgeSnapshotRef()
			test.mutate(ref)
			definition := searchAuditTestDefinition("owner", "job-"+test.name, 0)
			definition.KnowledgeSnapshot = ref
			tx := database.GORMDB().WithContext(ctx).Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			err := store.AppendSearchAttemptInTransaction(
				ctx, tx, "tenant-invalid-snapshot", definition,
			)
			_ = tx.Rollback().Error
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("AppendSearchAttemptInTransaction error = %v", err)
			}
		})
	}
	var states, events int64
	if err := database.GORMDB().Model(&searchAttemptTenantStateRecord{}).
		Where("tenant_id = ?", "tenant-invalid-snapshot").Count(&states).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.GORMDB().Model(&searchAttemptEventRecord{}).
		Where("tenant_id = ?", "tenant-invalid-snapshot").Count(&events).Error; err != nil {
		t.Fatal(err)
	}
	if states != 0 || events != 0 {
		t.Fatalf("rejected append persisted %d states and %d events", states, events)
	}
}

func TestEventDigestBindsKnowledgeSnapshot(t *testing.T) {
	t.Parallel()
	record := searchAttemptEventRecord{
		TenantID:            "tenant-a",
		Sequence:            7,
		OccurredAtUnixMicro: 1786224000000000,
		ActorKind:           audit.ActorKindSystem,
		ActorID:             defaultSystemActorID,
		ActorRole:           audit.ActorRoleSystem,
		OwnerID:             "owner-a",
		SearchJobID:         "job-a",
	}
	withoutSnapshotDigest, err := eventDigest(record)
	if err != nil {
		t.Fatal(err)
	}
	const wantWithoutSnapshotDigest = "cQd5CKb3UMLShaLSkG2qexYtMsfXVjV2fuIFPofQAnk"
	if withoutSnapshotDigest != wantWithoutSnapshotDigest {
		t.Fatalf("digest without snapshot = %q, want %q", withoutSnapshotDigest, wantWithoutSnapshotDigest)
	}
	snapshotRecord := record
	setRecordKnowledgeSnapshot(
		&snapshotRecord,
		searchAuditTestKnowledgeSnapshotRef(),
	)
	snapshotDigest, err := eventDigest(snapshotRecord)
	if err != nil {
		t.Fatal(err)
	}
	const wantSnapshotDigest = "bqu4rV_IKDjEAGsk60swI6W6gBQvA88Jt6pQA9ijnGE"
	if snapshotDigest != wantSnapshotDigest {
		t.Fatalf("snapshot digest = %q, want %q", snapshotDigest, wantSnapshotDigest)
	}

	variants := []*opensplunk.KnowledgeSnapshotRef{
		searchAuditTestKnowledgeSnapshotRef(),
		searchAuditTestKnowledgeSnapshotRef(),
		searchAuditTestKnowledgeSnapshotRef(),
		searchAuditTestKnowledgeSnapshotRef(),
		searchAuditTestKnowledgeSnapshotRef(),
	}
	variants[0].SnapshotSha256[0] ^= 0xff
	variants[1].TenantCatalogRevision++
	variants[2].TenantCatalogStateToken[0] ^= 0xff
	variants[3].ObjectCount++
	variants[4].LookupAssetCount++
	seen := map[string]struct{}{withoutSnapshotDigest: {}, snapshotDigest: {}}
	for index, ref := range variants {
		candidate := record
		setRecordKnowledgeSnapshot(&candidate, ref)
		digest, err := eventDigest(candidate)
		if err != nil {
			t.Fatalf("eventDigest(variant %d): %v", index, err)
		}
		if _, exists := seen[digest]; exists {
			t.Fatalf("variant %d did not change the event digest", index)
		}
		seen[digest] = struct{}{}
	}
}

func TestListCursorBindsKnowledgeSnapshotIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
	for index := 1; index <= 4; index++ {
		definition := searchAuditTestDefinition(
			"owner",
			"job-"+string(rune('0'+index)),
			time.Duration(index)*time.Microsecond,
		)
		definition.KnowledgeSnapshot = searchAuditTestKnowledgeSnapshotRef()
		appendSearchAuditTestEvent(
			t, store, database, ctx, "tenant-snapshot-cursor", definition,
		)
	}
	first, err := store.List(
		ctx, "tenant-snapshot-cursor", ListRequest{PageSize: 2},
	)
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("List(first) = (%+v, %v)", first, err)
	}
	if err := database.GORMDB().Exec(
		"DROP TRIGGER search_attempt_audit_event_update_is_forbidden",
	).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.GORMDB().Exec(`
		UPDATE search_attempt_audit_events
		SET knowledge_snapshot_sha256 = ?
		WHERE tenant_id = ? AND sequence = 4
	`, bytes.Repeat([]byte{0x24}, 32), "tenant-snapshot-cursor").Error; err != nil {
		t.Fatal(err)
	}
	assertInvalidSearchAuditCursor(t, store, "tenant-snapshot-cursor", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken,
	})
}

func TestStartupIntegrityRejectsPartialKnowledgeSnapshotTuple(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
	definition := searchAuditTestDefinition("owner", "job-corrupt-snapshot", 0)
	definition.KnowledgeSnapshot = searchAuditTestKnowledgeSnapshotRef()
	appendSearchAuditTestEvent(
		t, store, database, ctx, "tenant-corrupt-snapshot", definition,
	)
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if _, err := connection.ExecContext(
		ctx, "DROP TRIGGER search_attempt_audit_event_update_is_forbidden",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE search_attempt_audit_events
		SET knowledge_snapshot_tenant_catalog_revision = NULL,
		    knowledge_snapshot_tenant_catalog_state_token = NULL,
		    knowledge_snapshot_object_count = NULL,
		    knowledge_snapshot_lookup_asset_count = NULL
		WHERE tenant_id = 'tenant-corrupt-snapshot'
	`); err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := New(database, Options{CursorKey: searchAuditTestCursorKey()}); reopened != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("New(partial snapshot tuple) = (%v, %v)", reopened, err)
	}
}
