package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type integrationFutureDefinition struct {
	bytes      []byte
	digest     [sha256.Size]byte
	metadata   knowledgedefinition.Normalized
	bodyField  []byte
	definition *opensplunkv1.KnowledgeObjectDefinition
}

// newIntegrationFutureDefinition starts from canonical known metadata, removes
// the recognized body, and appends one compatibility-reserved body field. A
// positive targetBytes makes the complete stored message exactly that size.
func newIntegrationFutureDefinition(
	t *testing.T,
	name string,
	targetBytes int,
) integrationFutureDefinition {
	t.Helper()
	description := "opaque future definition for " + name
	metadata, err := knowledgedefinition.Normalize(aliasDefinition(
		testApp,
		name,
		SharingScopePrivate,
		&description,
		"future-"+name+"-*",
	))
	if err != nil {
		t.Fatalf("normalize future metadata: %v", err)
	}
	definition, ok := proto.Clone(metadata.Definition).(*opensplunkv1.KnowledgeObjectDefinition)
	if !ok || definition == nil {
		t.Fatal("clone future metadata")
	}
	definition.Body = nil
	knownBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil {
		t.Fatalf("marshal future metadata: %v", err)
	}

	payload := []byte{0x08, 0x01}
	if targetBytes > 0 {
		payloadBytes := futurePayloadBytesForTarget(t, len(knownBytes), targetBytes)
		payload = bytes.Repeat([]byte{0xa5}, payloadBytes)
	}
	tag := protowire.AppendTag(nil, 13, protowire.BytesType)
	bodyField := protowire.AppendBytes(bytes.Clone(tag), payload)
	stored := append(bytes.Clone(knownBytes), bodyField...)
	if targetBytes > 0 && len(stored) != targetBytes {
		t.Fatalf("future definition bytes = %d, want %d", len(stored), targetBytes)
	}
	if len(stored) > knowledgedefinition.MaximumCanonicalBytes {
		t.Fatalf("future definition bytes = %d, maximum %d", len(stored), knowledgedefinition.MaximumCanonicalBytes)
	}
	digest := sha256.Sum256(stored)
	decoded := &opensplunkv1.KnowledgeObjectDefinition{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(stored, decoded); err != nil {
		t.Fatalf("unmarshal future fixture: %v", err)
	}
	return integrationFutureDefinition{
		bytes:      stored,
		digest:     digest,
		metadata:   metadata,
		bodyField:  bodyField,
		definition: decoded,
	}
}

func futurePayloadBytesForTarget(t *testing.T, knownBytes, targetBytes int) int {
	t.Helper()
	tagBytes := len(protowire.AppendTag(nil, 13, protowire.BytesType))
	payloadBytes := targetBytes - knownBytes - tagBytes - 1
	for attempts := 0; attempts < 8; attempts++ {
		if payloadBytes < 0 {
			t.Fatalf("future target %d is smaller than metadata overhead", targetBytes)
		}
		next := targetBytes - knownBytes - tagBytes - protowire.SizeVarint(uint64(payloadBytes))
		if next == payloadBytes {
			return payloadBytes
		}
		payloadBytes = next
	}
	t.Fatalf("future target %d did not converge", targetBytes)
	return 0
}

// insertIntegrationFutureObject publishes a schema-valid object whose current
// definition contains one opaque future body. Disabled and deleted fixtures
// retain a recognized active v1 followed by the opaque state-only v2.
func insertIntegrationFutureObject(
	t *testing.T,
	database *control.DB,
	objectID string,
	state State,
	fixture integrationFutureDefinition,
	timestamp int64,
) {
	t.Helper()
	if state != StateDraft && state != StateActive && state != StateDisabled && state != StateDeleted {
		t.Fatalf("unsupported future fixture state %q", state)
	}
	tx, err := database.SQLDB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin future fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	ensureIntegrationCatalogLedgers(t, tx)

	currentVersion := int64(1)
	createdAt := timestamp
	if state == StateDisabled || state == StateDeleted {
		currentVersion = 2
		createdAt = timestamp - 1
		insertIntegrationDefinitionBlob(t, tx, fixture.metadata.Bytes, fixture.metadata.Digest[:], createdAt)
		insertIntegrationVersion(
			t,
			tx,
			objectID,
			1,
			StateActive,
			"create",
			fixture.metadata.Digest[:],
			fixture.metadata,
			createdAt,
		)
	}
	insertIntegrationDefinitionBlob(t, tx, fixture.bytes, fixture.digest[:], timestamp)
	mutation := "create"
	if state == StateDisabled {
		mutation = "disable"
	}
	if state == StateDeleted {
		mutation = "delete"
	}
	insertIntegrationVersion(
		t,
		tx,
		objectID,
		currentVersion,
		state,
		mutation,
		fixture.digest[:],
		fixture.metadata,
		timestamp,
	)
	insertIntegrationProjection(t, tx, objectID, currentVersion, testOwner, state, fixture.metadata)

	var disabledAt, deletedAt any
	if state == StateDisabled {
		disabledAt = timestamp
	}
	if state == StateDeleted {
		deletedAt = timestamp
	}
	objectType, ok := objectTypeFromProto(fixture.metadata.ObjectType)
	sharingScope, scopeOK := sharingScopeFromProto(fixture.metadata.SharingScope)
	if !ok || !scopeOK {
		t.Fatal("future fixture metadata has unsupported identity")
	}
	if _, err := tx.Exec(`INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL)`,
		testTenant,
		objectID,
		currentVersion,
		fixture.metadata.AppID,
		testOwner,
		objectType,
		fixture.metadata.Name,
		sharingScope,
		state,
		fixture.digest[:],
		createdAt,
		timestamp,
		disabledAt,
		deletedAt,
	); err != nil {
		t.Fatalf("insert future registry: %v", err)
	}
	advanceIntegrationCatalogRevision(t, tx)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit future fixture: %v", err)
	}
}

// stageIntegrationKnownPublication stages every immutable authority and the
// current projection for one recognized vNext. The caller controls commit so
// reads can be exercised while the complete publication remains uncommitted.
func stageIntegrationKnownPublication(
	t *testing.T,
	database *control.DB,
	objectID string,
	definition *opensplunkv1.KnowledgeObjectDefinition,
	state State,
	mutation string,
	timestamp int64,
) (*sql.Tx, knowledgedefinition.Normalized) {
	t.Helper()
	normalized, err := knowledgedefinition.Normalize(definition)
	if err != nil {
		t.Fatalf("normalize staged publication: %v", err)
	}
	tx, err := database.SQLDB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin staged publication: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	var currentVersion int64
	if err := tx.QueryRow(`SELECT current_version FROM knowledge_objects
		WHERE tenant_id = ? AND knowledge_object_id = ?`, testTenant, objectID).Scan(&currentVersion); err != nil {
		t.Fatalf("read staged current version: %v", err)
	}
	nextVersion := currentVersion + 1
	insertIntegrationDefinitionBlob(t, tx, normalized.Bytes, normalized.Digest[:], timestamp)
	insertIntegrationVersion(
		t,
		tx,
		objectID,
		nextVersion,
		state,
		mutation,
		normalized.Digest[:],
		normalized,
		timestamp,
	)
	insertIntegrationProjection(t, tx, objectID, nextVersion, testOwner, state, normalized)

	objectType, ok := objectTypeFromProto(normalized.ObjectType)
	sharingScope, scopeOK := sharingScopeFromProto(normalized.SharingScope)
	if !ok || !scopeOK {
		t.Fatal("staged publication has unsupported identity")
	}
	var disabledAt, deletedAt any
	if state == StateDisabled {
		disabledAt = timestamp
	}
	if state == StateDeleted {
		deletedAt = timestamp
	}
	result, err := tx.Exec(`UPDATE knowledge_objects SET
		current_version = ?, app_id = ?, owner_id = ?, object_type = ?, name = ?,
		sharing_scope = ?, state = ?, definition_digest = ?, updated_at_unix_micro = ?,
		disabled_at_unix_micro = ?, quarantined_at_unix_micro = NULL,
		deleted_at_unix_micro = ?, quarantine_reason = NULL
		WHERE tenant_id = ? AND knowledge_object_id = ?`,
		nextVersion,
		normalized.AppID,
		testOwner,
		objectType,
		normalized.Name,
		sharingScope,
		state,
		normalized.Digest[:],
		timestamp,
		disabledAt,
		deletedAt,
		testTenant,
		objectID,
	)
	if err != nil {
		t.Fatalf("stage current registry: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("stage current registry rows = %d, %v", affected, err)
	}
	advanceIntegrationCatalogRevision(t, tx)
	return tx, normalized
}

func ensureIntegrationCatalogLedgers(t *testing.T, tx *sql.Tx) {
	t.Helper()
	if _, err := tx.Exec(`INSERT INTO knowledge_catalog_tenants (tenant_id)
		SELECT ? WHERE NOT EXISTS (SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = ?)`, testTenant, testTenant); err != nil {
		t.Fatalf("ensure catalog ledger: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO knowledge_projection_tenant_ledgers (tenant_id)
		SELECT ? WHERE NOT EXISTS (SELECT 1 FROM knowledge_projection_tenant_ledgers WHERE tenant_id = ?)`, testTenant, testTenant); err != nil {
		t.Fatalf("ensure projection ledger: %v", err)
	}
}

func insertIntegrationDefinitionBlob(
	t *testing.T,
	tx *sql.Tx,
	definition, digest []byte,
	timestamp int64,
) {
	t.Helper()
	if _, err := tx.Exec(`INSERT INTO knowledge_definition_blobs (
		tenant_id, definition_digest, definition_proto, definition_bytes, created_at_unix_micro
	) VALUES (?, ?, ?, ?, ?)`, testTenant, digest, definition, len(definition), timestamp); err != nil {
		t.Fatalf("insert integration definition blob: %v", err)
	}
}

func insertIntegrationVersion(
	t *testing.T,
	tx *sql.Tx,
	objectID string,
	version int64,
	state State,
	mutation string,
	digest []byte,
	metadata knowledgedefinition.Normalized,
	timestamp int64,
) {
	t.Helper()
	objectType, ok := objectTypeFromProto(metadata.ObjectType)
	sharingScope, scopeOK := sharingScopeFromProto(metadata.SharingScope)
	if !ok || !scopeOK {
		t.Fatal("integration version has unsupported identity")
	}
	if _, err := tx.Exec(`INSERT INTO knowledge_object_versions (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, dependency_count, mutation_kind,
		quarantine_reason, created_at_unix_micro
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, ?)`,
		testTenant,
		objectID,
		version,
		metadata.AppID,
		testOwner,
		objectType,
		metadata.Name,
		sharingScope,
		state,
		digest,
		mutation,
		timestamp,
	); err != nil {
		t.Fatalf("insert integration version: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO knowledge_object_dependency_seals (
		tenant_id, knowledge_object_id, object_version, dependency_count
	) VALUES (?, ?, ?, 0)`, testTenant, objectID, version); err != nil {
		t.Fatalf("seal integration dependencies: %v", err)
	}
}

func insertIntegrationProjection(
	t *testing.T,
	tx *sql.Tx,
	objectID string,
	version int64,
	owner string,
	state State,
	metadata knowledgedefinition.Normalized,
) {
	t.Helper()
	description := ""
	descriptionPresent := 0
	if metadata.Description != nil {
		description = *metadata.Description
		descriptionPresent = 1
	}
	dimensions := []knowledge.Dimension{
		knowledge.DimensionIndex,
		knowledge.DimensionHost,
		knowledge.DimensionSource,
		knowledge.DimensionSourcetype,
	}
	counts := [4]int{}
	selectorValueBytes := 0
	for index, dimension := range dimensions {
		values := metadata.Selector.Patterns(dimension)
		counts[index] = len(values)
		for _, value := range values {
			selectorValueBytes += len(value)
		}
	}
	canonicalSelectorBytes := len(metadata.Selector.CanonicalBytes())
	objectType, ok := objectTypeFromProto(metadata.ObjectType)
	sharingScope, scopeOK := sharingScopeFromProto(metadata.SharingScope)
	if !ok || !scopeOK {
		t.Fatal("integration projection has unsupported identity")
	}
	if _, err := tx.Exec(`INSERT INTO knowledge_object_list_projections (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id, object_type, name,
		sharing_scope, state, description_present, description, index_selector_count,
		host_selector_count, source_selector_count, sourcetype_selector_count,
		selector_value_bytes, canonical_selector_bytes
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		testTenant,
		objectID,
		version,
		metadata.AppID,
		owner,
		objectType,
		metadata.Name,
		sharingScope,
		state,
		descriptionPresent,
		description,
		counts[0],
		counts[1],
		counts[2],
		counts[3],
		selectorValueBytes,
		canonicalSelectorBytes,
	); err != nil {
		t.Fatalf("insert integration projection: %v", err)
	}
	insertSelectorRows(t, tx, objectID, version, metadata)
	if _, err := tx.Exec(`INSERT INTO knowledge_object_list_projection_seals (
		tenant_id, knowledge_object_id, object_version, projection_bytes, canonical_selector_bytes
	) VALUES (?, ?, ?, ?, ?)`,
		testTenant,
		objectID,
		version,
		len(description)+selectorValueBytes,
		canonicalSelectorBytes,
	); err != nil {
		t.Fatalf("seal integration projection: %v", err)
	}
}

func advanceIntegrationCatalogRevision(t *testing.T, tx *sql.Tx) {
	t.Helper()
	result, err := tx.Exec(`UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1 WHERE tenant_id = ?`, testTenant)
	if err != nil {
		t.Fatalf("advance integration catalog revision: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("advance integration catalog revision rows = %d, %v", affected, err)
	}
}

func integrationUnknownBody(definition *opensplunkv1.KnowledgeObjectDefinition) []byte {
	if definition == nil {
		return nil
	}
	return bytes.Clone(definition.ProtoReflect().GetUnknown())
}

func describeIntegrationObject(object Object) string {
	description := ""
	if object.Definition != nil && object.Definition.Description != nil {
		description = object.Definition.GetDescription()
	}
	return fmt.Sprintf(
		"id=%q version=%d name=%q state=%q description=%q digest=%x",
		object.KnowledgeObjectID,
		object.Version,
		object.Name,
		object.State,
		description,
		object.DefinitionSHA256,
	)
}
