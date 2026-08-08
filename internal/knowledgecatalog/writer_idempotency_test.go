package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const idempotencyUnitApp = "app_000000000000000000000A"

func TestWriterOutcomeReferenceIsCompactCanonicalAndStrict(t *testing.T) {
	t.Parallel()

	digest := bytes.Repeat([]byte{0x5a}, persistedKnowledgeDefinitionDigestBytes)
	record := idempotencyUnitRecord(t, mutationRouteUpdate, "update")
	record.KnowledgeObjectID = "ko_compact_reference"
	record.ObjectVersion = 17
	encoded := encodeIdempotencyUnitOutcome(t, record, digest)
	record.OutcomeProto = encoded
	if len(encoded) == 0 || len(encoded) > maximumMutationOutcomeBytes {
		t.Fatalf("encoded outcome bytes = %d", len(encoded))
	}
	reference, err := decodeOutcomeReference(record)
	if err != nil {
		t.Fatalf("decodeOutcomeReference: %v", err)
	}
	if reference.GetKnowledgeObjectId() != record.KnowledgeObjectID ||
		reference.GetVersion() != uint64(record.ObjectVersion) ||
		!bytes.Equal(reference.GetDefinitionSha256(), digest) {
		t.Fatalf("decoded compact outcome = %#v", reference)
	}
	outcome := &opensplunkv1.KnowledgeMutationOutcomeRecord{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, outcome); err != nil {
		t.Fatalf("decode outcome envelope: %v", err)
	}
	reencoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(outcome)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("outcome is non-deterministic: %x != %x, %v", reencoded, encoded, err)
	}

	withUnknown := append([]byte(nil), encoded...)
	withUnknown = protowire.AppendTag(withUnknown, 127, protowire.VarintType)
	withUnknown = protowire.AppendVarint(withUnknown, 1)
	record.OutcomeProto = withUnknown
	if _, err := decodeOutcomeReference(record); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decode outcome with unknown field = %v, want ErrCorrupt", err)
	}

	withNestedUnknown := proto.Clone(outcome).(*opensplunkv1.KnowledgeMutationOutcomeRecord)
	nestedUnknown := protowire.AppendTag(nil, 127, protowire.VarintType)
	nestedUnknown = protowire.AppendVarint(nestedUnknown, 1)
	withNestedUnknown.GetObject().ProtoReflect().SetUnknown(nestedUnknown)
	record.OutcomeProto, err = (proto.MarshalOptions{Deterministic: true}).Marshal(withNestedUnknown)
	if err != nil {
		t.Fatalf("encode nested unknown outcome: %v", err)
	}
	if _, err := decodeOutcomeReference(record); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decode outcome with nested unknown field = %v, want ErrCorrupt", err)
	}

	withDuplicateKnownField := append([]byte(nil), encoded...)
	withDuplicateKnownField = protowire.AppendTag(withDuplicateKnownField, 4, protowire.VarintType)
	withDuplicateKnownField = protowire.AppendVarint(withDuplicateKnownField, uint64(record.CommittedCatalogRevision))
	record.OutcomeProto = withDuplicateKnownField
	if _, err := decodeOutcomeReference(record); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decode noncanonical duplicate field = %v, want ErrCorrupt", err)
	}

	record.OutcomeProto = bytes.Repeat([]byte{0x01}, maximumMutationOutcomeBytes+1)
	if _, err := decodeOutcomeReference(record); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decode oversized outcome = %v, want ErrCorrupt", err)
	}
	if _, err := encodeOutcomeReference(mutationOutcomeAuthority{
		route:                    mutationRouteUpdate,
		mutationKind:             "update",
		objectID:                 "ko_compact_reference",
		version:                  17,
		digest:                   digest[:31],
		catalogRevision:          1,
		catalogStateToken:        bytes.Repeat([]byte{0x77}, catalogStateTokenBytes),
		successfulAuditSequence:  1,
		occurredAtUnixMicro:      1,
		retentionAnchorUnixMicro: 1,
		retainUntilUnixMicro:     1 + int64(minimumIdempotencyRetention/time.Microsecond),
	}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("encode short digest = %v, want invalid argument", err)
	}
}

func TestWriterQuarantineOutcomeReferenceUsesBodylessRecoveryAuthority(t *testing.T) {
	t.Parallel()

	record := idempotencyUnitRecord(t, mutationRouteUpdate, "update")
	recoverySequence := int64(7)
	record.Route = mutationRouteQuarantine
	record.MutationKind = "quarantine"
	record.SuccessfulAuditSequence = nil
	record.RecoveryAuditSequence = &recoverySequence
	record.OutcomeProto = encodeIdempotencyUnitOutcome(t, record, nil)
	reference, err := decodeOutcomeReference(record)
	if err != nil {
		t.Fatalf("decode quarantine outcome: %v", err)
	}
	if reference.GetKnowledgeObjectId() != record.KnowledgeObjectID ||
		reference.GetVersion() != uint64(record.ObjectVersion) ||
		len(reference.GetDefinitionSha256()) != 0 {
		t.Fatalf("quarantine outcome reference = %#v, want exact bodyless authority", reference)
	}
	if _, err := encodeOutcomeReference(mutationOutcomeAuthority{
		route:                    mutationRouteQuarantine,
		mutationKind:             "quarantine",
		objectID:                 record.KnowledgeObjectID,
		version:                  uint64(record.ObjectVersion),
		digest:                   bytes.Repeat([]byte{0x5a}, persistedKnowledgeDefinitionDigestBytes),
		catalogRevision:          uint64(record.CommittedCatalogRevision),
		catalogStateToken:        record.CommittedCatalogStateToken,
		recoveryAuditSequence:    uint64(recoverySequence),
		occurredAtUnixMicro:      record.CreatedAtUnixMicro,
		retentionAnchorUnixMicro: record.RetentionAnchorUnixMicro,
		retainUntilUnixMicro:     record.RetainUntilUnixMicro,
	}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("encode quarantine outcome with definition digest = %v, want invalid argument", err)
	}
}

func TestWriterIdempotencyValidationDistinguishesDigestConflictFromCorruption(t *testing.T) {
	t.Parallel()

	prepared := idempotencyUnitPrepared()
	record := idempotencyUnitRecord(t, mutationRouteCreate, "create")
	if err := validateIdempotencyRecord(&prepared, mutationRouteCreate, record); err != nil {
		t.Fatalf("validate exact receipt: %v", err)
	}

	conflict := record
	conflict.RequestDigest = bytes.Clone(record.RequestDigest)
	conflict.RequestDigest[0] ^= 0xff
	if err := validateIdempotencyRecord(&prepared, mutationRouteCreate, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("validate different digest = %v, want idempotency conflict", err)
	}

	corrupt := record
	corrupt.CommittedCatalogStateToken = corrupt.CommittedCatalogStateToken[:31]
	if err := validateIdempotencyRecord(&prepared, mutationRouteCreate, corrupt); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("validate short state token = %v, want ErrCorrupt", err)
	}

	maximumRetention := record
	maximumRetention.RetainUntilUnixMicro = record.RetentionAnchorUnixMicro +
		int64(maximumIdempotencyRetention/time.Microsecond)
	maximumRetention.OutcomeProto = encodeIdempotencyUnitOutcome(
		t,
		maximumRetention,
		bytes.Repeat([]byte{0x5a}, persistedKnowledgeDefinitionDigestBytes),
	)
	if err := validateIdempotencyRecord(&prepared, mutationRouteCreate, maximumRetention); err != nil {
		t.Fatalf("validate exact maximum retention: %v", err)
	}
	tooLongRetention := maximumRetention
	tooLongRetention.RetainUntilUnixMicro++
	if err := validateIdempotencyRecord(&prepared, mutationRouteCreate, tooLongRetention); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("validate excessive retention = %v, want ErrCorrupt", err)
	}

	if validIdempotencyWidth(idempotencyWidthRecord{
		TenantIDBytes:      8,
		ActorKindBytes:     int64(len(audit.ActorKindBrowser)),
		ActorIDBytes:       5,
		RouteBytes:         int64(len(mutationRouteCreate)),
		RequestIDBytes:     minimumClientRequestIDBytes,
		MutationKindBytes:  int64(len("create")),
		RequestDigestBytes: 32,
		OutcomeProtoBytes:  maximumMutationOutcomeBytes + 1,
		StateTokenBytes:    32,
		ObjectIDBytes:      8,
	}) {
		t.Fatal("width preflight accepted an oversized outcome")
	}
}

func TestReadIdempotencyRecordUsesBoundedPreflightAndConstantTimeIdentity(t *testing.T) {
	t.Parallel()

	database, err := gorm.Open(sqlite.Open("file:writer-idempotency-read?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open idempotency database: %v", err)
	}
	rawDatabase, err := database.DB()
	if err != nil {
		t.Fatalf("read idempotency SQL database: %v", err)
	}
	t.Cleanup(func() { _ = rawDatabase.Close() })
	if err := database.Exec(`CREATE TABLE knowledge_mutation_idempotency (
		tenant_id TEXT NOT NULL,
		actor_kind TEXT NOT NULL,
		actor_id TEXT NOT NULL,
		route TEXT NOT NULL,
		client_request_id TEXT NOT NULL,
		mutation_kind TEXT NOT NULL,
		request_digest_format_version INTEGER NOT NULL,
		request_digest BLOB NOT NULL,
		outcome_format_version INTEGER NOT NULL,
		outcome_proto BLOB NOT NULL,
		committed_catalog_revision INTEGER NOT NULL,
		committed_catalog_state_token BLOB NOT NULL,
		knowledge_object_id TEXT NOT NULL,
		object_version INTEGER NOT NULL,
		successful_audit_sequence INTEGER,
		recovery_audit_sequence INTEGER,
		created_at_unix_micro INTEGER NOT NULL,
		retention_anchor_unix_micro INTEGER NOT NULL,
		retain_until_unix_micro INTEGER NOT NULL,
		PRIMARY KEY (tenant_id, actor_kind, actor_id, route, client_request_id)
	)`).Error; err != nil {
		t.Fatalf("create idempotency table: %v", err)
	}
	prepared := idempotencyUnitPrepared()
	record := idempotencyUnitRecord(t, mutationRouteCreate, "create")
	if err := database.Create(&record).Error; err != nil {
		t.Fatalf("insert idempotency receipt: %v", err)
	}

	got, found, err := readIdempotencyRecord(database, &prepared, mutationRouteCreate)
	if err != nil || !found {
		t.Fatalf("read exact idempotency receipt = (%#v, %t, %v)", got, found, err)
	}
	if got.KnowledgeObjectID != record.KnowledgeObjectID ||
		!bytes.Equal(got.CommittedCatalogStateToken, record.CommittedCatalogStateToken) {
		t.Fatalf("read idempotency receipt = %#v", got)
	}

	conflict := prepared
	conflict.requestDigest[0] ^= 0xff
	if _, found, err := readIdempotencyRecord(database, &conflict, mutationRouteCreate); !found || !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("read reused identity = (found:%t, err:%v), want conflict", found, err)
	}

	if err := database.Exec(`UPDATE knowledge_mutation_idempotency
		SET outcome_proto = zeroblob(?)`, maximumMutationOutcomeBytes+1).Error; err != nil {
		t.Fatalf("widen idempotency outcome: %v", err)
	}
	if _, found, err := readIdempotencyRecord(database, &prepared, mutationRouteCreate); !found || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("read oversized outcome = (found:%t, err:%v), want ErrCorrupt", found, err)
	}
}

func TestReplayChecksCurrentAuthorizationAndQuarantineBeforeHistoricalBody(t *testing.T) {
	t.Parallel()

	database, err := gorm.Open(sqlite.Open("file:writer-idempotency-order?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open replay database: %v", err)
	}
	rawDatabase, err := database.DB()
	if err != nil {
		t.Fatalf("read replay SQL database: %v", err)
	}
	t.Cleanup(func() { _ = rawDatabase.Close() })
	if err := database.Exec(`CREATE TABLE knowledge_objects (
		tenant_id TEXT NOT NULL,
		knowledge_object_id TEXT NOT NULL,
		current_version INTEGER NOT NULL,
		app_id TEXT NOT NULL,
		owner_id TEXT NOT NULL,
		object_type TEXT NOT NULL,
		name TEXT NOT NULL,
		sharing_scope TEXT NOT NULL,
		state TEXT NOT NULL,
		definition_digest BLOB,
		created_at_unix_micro INTEGER NOT NULL,
		updated_at_unix_micro INTEGER NOT NULL,
		disabled_at_unix_micro INTEGER,
		quarantined_at_unix_micro INTEGER,
		deleted_at_unix_micro INTEGER,
		quarantine_reason TEXT,
		PRIMARY KEY (tenant_id, knowledge_object_id)
	)`).Error; err != nil {
		t.Fatalf("create replay registry: %v", err)
	}
	if err := database.Exec(`CREATE TABLE knowledge_object_versions (
		tenant_id TEXT NOT NULL,
		knowledge_object_id TEXT NOT NULL,
		object_version INTEGER NOT NULL,
		app_id TEXT NOT NULL,
		owner_id TEXT NOT NULL,
		object_type TEXT NOT NULL,
		name TEXT NOT NULL,
		sharing_scope TEXT NOT NULL,
		state TEXT NOT NULL,
		definition_digest BLOB,
		dependency_count INTEGER NOT NULL,
		mutation_kind TEXT NOT NULL,
		quarantine_reason TEXT,
		created_at_unix_micro INTEGER NOT NULL,
		PRIMARY KEY (tenant_id, knowledge_object_id, object_version)
	)`).Error; err != nil {
		t.Fatalf("create replay versions: %v", err)
	}

	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	created := now.Add(-time.Second).UnixMicro()
	if err := database.Exec(`INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id,
		object_type, name, sharing_scope, state, definition_digest,
		created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro,
		deleted_at_unix_micro, quarantine_reason
	) VALUES (?, ?, 2, ?, ?, 'field_alias', 'redacted', 'private',
		'quarantined', NULL, ?, ?, NULL, ?, NULL, 'root_corruption')`,
		"tenant-a", "ko_redacted_replay", idempotencyUnitApp, "owner-a",
		created, now.UnixMicro(), now.UnixMicro(),
	).Error; err != nil {
		t.Fatalf("insert quarantined registry: %v", err)
	}
	if err := database.Exec(`INSERT INTO knowledge_object_versions (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id,
		object_type, name, sharing_scope, state, definition_digest,
		dependency_count, mutation_kind, quarantine_reason, created_at_unix_micro
	) VALUES (?, ?, 2, ?, ?, 'field_alias', 'redacted', 'private',
		'quarantined', NULL, 0, 'quarantine', 'root_corruption', ?)`,
		"tenant-a", "ko_redacted_replay", idempotencyUnitApp, "owner-a", now.UnixMicro(),
	).Error; err != nil {
		t.Fatalf("insert quarantined version: %v", err)
	}

	prepared := idempotencyUnitPrepared()
	record := idempotencyUnitRecord(t, mutationRouteCreate, "create")
	record.KnowledgeObjectID = "ko_redacted_replay"
	record.OutcomeProto = encodeIdempotencyUnitOutcome(
		t, record, bytes.Repeat([]byte{0x44}, persistedKnowledgeDefinitionDigestBytes),
	)
	writer := &Writer{reader: &Store{orm: database}}
	if _, err := writer.readReplayAuthority(
		context.Background(), database, prepared, record, mutationRouteCreate,
	); !errors.Is(err, ErrIdempotentOutcomeRedacted) {
		t.Fatalf("quarantined replay = %v, want redacted", err)
	}

	if err := database.Exec(`INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id,
		object_type, name, sharing_scope, state, definition_digest,
		created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro,
		deleted_at_unix_micro, quarantine_reason
	) VALUES (?, ?, 1, ?, ?, 'field_alias', 'hidden', 'private',
		'active', ?, ?, ?, NULL, NULL, NULL, NULL)`,
		"tenant-a", "ko_hidden_replay", idempotencyUnitApp, "owner-b",
		bytes.Repeat([]byte{0x55}, persistedKnowledgeDefinitionDigestBytes),
		created, now.UnixMicro(),
	).Error; err != nil {
		t.Fatalf("insert hidden registry: %v", err)
	}
	if err := database.Exec(`INSERT INTO knowledge_object_versions (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id,
		object_type, name, sharing_scope, state, definition_digest,
		dependency_count, mutation_kind, quarantine_reason, created_at_unix_micro
	) VALUES (?, ?, 1, ?, ?, 'field_alias', 'hidden', 'private',
		'active', ?, 0, 'create', NULL, ?)`,
		"tenant-a", "ko_hidden_replay", idempotencyUnitApp, "owner-b",
		bytes.Repeat([]byte{0x55}, persistedKnowledgeDefinitionDigestBytes), now.UnixMicro(),
	).Error; err != nil {
		t.Fatalf("insert hidden version: %v", err)
	}
	record.KnowledgeObjectID = "ko_hidden_replay"
	record.OutcomeProto = encodeIdempotencyUnitOutcome(
		t, record, bytes.Repeat([]byte{0x55}, persistedKnowledgeDefinitionDigestBytes),
	)
	if _, err := writer.readReplayAuthority(
		context.Background(), database, prepared, record, mutationRouteCreate,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("hidden replay = %v, want not found", err)
	}

	if err := database.Exec(`INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id,
		object_type, name, sharing_scope, state, definition_digest,
		created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro,
		deleted_at_unix_micro, quarantine_reason
	) VALUES (?, ?, 1, ?, ?, 'field_alias', 'wrong-owner-global', 'global',
		'active', ?, ?, ?, NULL, NULL, NULL, NULL)`,
		"tenant-a", "ko_wrong_owner_global_replay", idempotencyUnitApp, "owner-b",
		bytes.Repeat([]byte{0x56}, persistedKnowledgeDefinitionDigestBytes),
		created, now.UnixMicro(),
	).Error; err != nil {
		t.Fatalf("insert wrong-owner global registry: %v", err)
	}
	if err := database.Exec(`INSERT INTO knowledge_object_versions (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id,
		object_type, name, sharing_scope, state, definition_digest,
		dependency_count, mutation_kind, quarantine_reason, created_at_unix_micro
	) VALUES (?, ?, 1, ?, ?, 'field_alias', 'wrong-owner-global', 'global',
		'active', ?, 0, 'create', NULL, ?)`,
		"tenant-a", "ko_wrong_owner_global_replay", idempotencyUnitApp, "owner-b",
		bytes.Repeat([]byte{0x56}, persistedKnowledgeDefinitionDigestBytes), now.UnixMicro(),
	).Error; err != nil {
		t.Fatalf("insert wrong-owner global version: %v", err)
	}
	record.KnowledgeObjectID = "ko_wrong_owner_global_replay"
	record.OutcomeProto = encodeIdempotencyUnitOutcome(
		t, record, bytes.Repeat([]byte{0x56}, persistedKnowledgeDefinitionDigestBytes),
	)
	if _, err := writer.readReplayAuthority(
		context.Background(), database, prepared, record, mutationRouteCreate,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("wrong-owner global replay = %v, want not found", err)
	}
}

func TestReplayRequestBindingRejectsRedirectedTargetsVersionsAndTransitions(t *testing.T) {
	t.Parallel()

	prepared := idempotencyUnitPrepared()
	prepared.requestBytes = mustCanonicalReplayRequest(t, &opensplunkv1.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: "ko_request_target",
		ExpectedVersion:   8,
		Definition:        &opensplunkv1.KnowledgeObjectDefinition{},
	})
	matchingUpdate := versionRecord{
		KnowledgeObjectID: "ko_request_target",
		ObjectVersion:     9,
		MutationKind:      "update",
		State:             StateDraft,
	}
	if err := validateReplayScalarRequestBinding(prepared, mutationRouteUpdate, matchingUpdate); err != nil {
		t.Fatalf("validate matching update request binding: %v", err)
	}
	redirected := matchingUpdate
	redirected.KnowledgeObjectID = "ko_other_authorized_target"
	if err := validateReplayScalarRequestBinding(prepared, mutationRouteUpdate, redirected); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("validate redirected update receipt = %v, want ErrCorrupt", err)
	}
	wrongVersion := matchingUpdate
	wrongVersion.ObjectVersion++
	if err := validateReplayScalarRequestBinding(prepared, mutationRouteUpdate, wrongVersion); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("validate wrong update successor = %v, want ErrCorrupt", err)
	}

	prepared.requestBytes = mustCanonicalReplayRequest(t, &opensplunkv1.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: "ko_request_target",
		ExpectedVersion:   9,
		State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	})
	matchingDisable := versionRecord{
		KnowledgeObjectID: "ko_request_target",
		ObjectVersion:     10,
		MutationKind:      "disable",
		State:             StateDisabled,
	}
	if err := validateReplayScalarRequestBinding(prepared, mutationRouteSetState, matchingDisable); err != nil {
		t.Fatalf("validate matching disable request binding: %v", err)
	}
	wrongTransition := matchingDisable
	wrongTransition.MutationKind = "enable"
	wrongTransition.State = StateActive
	if err := validateReplayScalarRequestBinding(prepared, mutationRouteSetState, wrongTransition); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("validate wrong state transition = %v, want ErrCorrupt", err)
	}

	prepared.requestBytes = mustCanonicalReplayRequest(t, &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition:   &opensplunkv1.KnowledgeObjectDefinition{},
		InitialState: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
	})
	matchingCreate := versionRecord{ObjectVersion: 1, MutationKind: "create", State: StateDraft}
	if err := validateReplayScalarRequestBinding(prepared, mutationRouteCreate, matchingCreate); err != nil {
		t.Fatalf("validate matching create request binding: %v", err)
	}
	wrongInitialState := matchingCreate
	wrongInitialState.State = StateActive
	if err := validateReplayScalarRequestBinding(prepared, mutationRouteCreate, wrongInitialState); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("validate wrong create initial state = %v, want ErrCorrupt", err)
	}

	prepared.requestBytes = mustCanonicalReplayRequest(t, &opensplunkv1.DeleteKnowledgeObjectRequest{
		KnowledgeObjectId: "ko_request_target",
		ExpectedVersion:   10,
	})
	matchingDelete := versionRecord{
		KnowledgeObjectID: "ko_request_target",
		ObjectVersion:     11,
		MutationKind:      "delete",
		State:             StateDeleted,
	}
	if err := validateReplayScalarRequestBinding(prepared, mutationRouteDelete, matchingDelete); err != nil {
		t.Fatalf("validate matching delete request binding: %v", err)
	}
}

func TestKnowledgeObjectToProtoDetachesDefinitionDigestAndLifecycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 7, 12, 0, 0, 123000, time.UTC)
	disabled := now.Add(time.Second)
	digest := bytes.Repeat([]byte{0x66}, persistedKnowledgeDefinitionDigestBytes)
	definition := &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        idempotencyUnitApp,
		Name:         "response-object",
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{},
		},
	}
	object := Object{
		KnowledgeObjectID: "ko_response_object",
		TenantID:          "tenant-a",
		AppID:             idempotencyUnitApp,
		OwnerID:           "owner-a",
		ObjectType:        ObjectTypeFieldAlias,
		Name:              "response-object",
		Version:           2,
		SharingScope:      SharingScopePrivate,
		State:             StateDisabled,
		Definition:        definition,
		DefinitionSHA256:  digest,
		CreatedAt:         now,
		UpdatedAt:         disabled,
		DisabledAt:        &disabled,
	}
	projection, err := knowledgeObjectToProto(object)
	if err != nil {
		t.Fatalf("knowledgeObjectToProto: %v", err)
	}
	if projection.GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED ||
		projection.GetObjectType() != opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS ||
		projection.GetSharingScope() != opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE ||
		projection.GetDisabledAt() == nil || projection.GetDefinition() == nil ||
		!bytes.Equal(projection.GetDefinitionSha256(), digest) {
		t.Fatalf("knowledge response projection = %#v", projection)
	}
	definition.Name = "mutated-after-conversion"
	digest[0] ^= 0xff
	if projection.GetDefinition().GetName() != "response-object" || projection.GetDefinitionSha256()[0] != 0x66 {
		t.Fatal("knowledge response retained caller-owned definition or digest storage")
	}

	bad := object
	bad.CreatedAt = bad.CreatedAt.Add(time.Nanosecond)
	if _, err := knowledgeObjectToProto(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("sub-microsecond response time = %v, want ErrCorrupt", err)
	}
}

func mustCanonicalReplayRequest(t *testing.T, message proto.Message) []byte {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil || len(encoded) == 0 {
		t.Fatalf("encode canonical replay request: bytes=%d err=%v", len(encoded), err)
	}
	return encoded
}

func idempotencyUnitPrepared() preparedMutation {
	digest := [32]byte{}
	for index := range digest {
		digest[index] = byte(index + 1)
	}
	return preparedMutation{
		scope: normalizedWriteScope{
			tenantID:       "tenant-a",
			ownerID:        "owner-a",
			writableAppIDs: []string{idempotencyUnitApp},
		},
		actor: audit.Actor{
			Kind: audit.ActorKindBrowser,
			ID:   "admin-a",
			Role: audit.ActorRoleAdministrator,
		},
		clientRequestID: "request-identity-0001",
		requestDigest:   digest,
	}
}

func idempotencyUnitRecord(t *testing.T, route, mutationKind string) idempotencyRecord {
	t.Helper()
	prepared := idempotencyUnitPrepared()
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	digest := bytes.Repeat([]byte{0x44}, persistedKnowledgeDefinitionDigestBytes)
	auditSequence := int64(1)
	record := idempotencyRecord{
		TenantID:                   prepared.scope.tenantID,
		ActorKind:                  prepared.actor.Kind,
		ActorID:                    prepared.actor.ID,
		Route:                      route,
		ClientRequestID:            prepared.clientRequestID,
		MutationKind:               mutationKind,
		RequestDigestFormatVersion: mutationRequestDigestFormatVersion,
		RequestDigest:              bytes.Clone(prepared.requestDigest[:]),
		OutcomeFormatVersion:       mutationOutcomeFormatVersion,
		CommittedCatalogRevision:   1,
		CommittedCatalogStateToken: bytes.Repeat([]byte{0x77}, 32),
		KnowledgeObjectID:          "ko_receipt_object",
		ObjectVersion:              1,
		SuccessfulAuditSequence:    &auditSequence,
		CreatedAtUnixMicro:         now.UnixMicro(),
		RetentionAnchorUnixMicro:   now.UnixMicro(),
		RetainUntilUnixMicro:       now.Add(minimumIdempotencyRetention).UnixMicro(),
	}
	record.OutcomeProto = encodeIdempotencyUnitOutcome(t, record, digest)
	return record
}

func encodeIdempotencyUnitOutcome(
	t *testing.T,
	record idempotencyRecord,
	digest []byte,
) []byte {
	t.Helper()
	authority := mutationOutcomeAuthority{
		route:                    record.Route,
		mutationKind:             record.MutationKind,
		objectID:                 record.KnowledgeObjectID,
		version:                  uint64(record.ObjectVersion),
		digest:                   digest,
		catalogRevision:          uint64(record.CommittedCatalogRevision),
		catalogStateToken:        record.CommittedCatalogStateToken,
		occurredAtUnixMicro:      record.CreatedAtUnixMicro,
		retentionAnchorUnixMicro: record.RetentionAnchorUnixMicro,
		retainUntilUnixMicro:     record.RetainUntilUnixMicro,
	}
	if record.SuccessfulAuditSequence != nil {
		authority.successfulAuditSequence = uint64(*record.SuccessfulAuditSequence)
	}
	if record.RecoveryAuditSequence != nil {
		authority.recoveryAuditSequence = uint64(*record.RecoveryAuditSequence)
	}
	encoded, err := encodeOutcomeReference(authority)
	if err != nil {
		t.Fatalf("encode test outcome: %v", err)
	}
	return encoded
}
