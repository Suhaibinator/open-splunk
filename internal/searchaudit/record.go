package searchaudit

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"gorm.io/gorm"
)

const (
	maximumActorKindBytes = len(audit.ActorKindBrowser)
	maximumActorRoleBytes = len(audit.ActorRoleAdministrator)
)

// eventSizeRecord is scalar-only so a corrupted text value is rejected before
// the SQLite driver is asked to allocate it.
type eventSizeRecord struct {
	Sequence                                      int64         `gorm:"column:sequence"`
	TenantIDBytes                                 int64         `gorm:"column:tenant_id_bytes"`
	ActorKindBytes                                int64         `gorm:"column:actor_kind_bytes"`
	ActorIDBytes                                  int64         `gorm:"column:actor_id_bytes"`
	ActorRoleBytes                                int64         `gorm:"column:actor_role_bytes"`
	OwnerIDBytes                                  int64         `gorm:"column:owner_id_bytes"`
	SearchJobIDBytes                              int64         `gorm:"column:search_job_id_bytes"`
	KnowledgeSnapshotPresentFields                int64         `gorm:"column:knowledge_snapshot_present_fields"`
	KnowledgeSnapshotSHA256Bytes                  sql.NullInt64 `gorm:"column:knowledge_snapshot_sha256_bytes"`
	KnowledgeSnapshotTenantCatalogRevision        sql.NullInt64 `gorm:"column:knowledge_snapshot_tenant_catalog_revision"`
	KnowledgeSnapshotTenantCatalogStateTokenBytes sql.NullInt64 `gorm:"column:knowledge_snapshot_tenant_catalog_state_token_bytes"`
	KnowledgeSnapshotObjectCount                  sql.NullInt64 `gorm:"column:knowledge_snapshot_object_count"`
	KnowledgeSnapshotLookupAssetCount             sql.NullInt64 `gorm:"column:knowledge_snapshot_lookup_asset_count"`
}

type tenantAggregate struct {
	RetainedCount int64  `gorm:"column:retained_count"`
	MinSequence   *int64 `gorm:"column:min_sequence"`
	MaxSequence   *int64 `gorm:"column:max_sequence"`
}

func readPreflightedRecords(query *gorm.DB, limit int) ([]searchAttemptEventRecord, error) {
	if query == nil || limit < 1 || limit > maximumIntegrityBatch {
		return nil, fmt.Errorf("%w: invalid bounded search-attempt audit read", ErrCorrupt)
	}
	var sizes []eventSizeRecord
	preflight := query.Session(&gorm.Session{}).
		Select(`
			sequence,
			length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
			length(CAST(actor_kind AS BLOB)) AS actor_kind_bytes,
			length(CAST(actor_id AS BLOB)) AS actor_id_bytes,
			length(CAST(actor_role AS BLOB)) AS actor_role_bytes,
			length(CAST(owner_id AS BLOB)) AS owner_id_bytes,
			length(CAST(search_job_id AS BLOB)) AS search_job_id_bytes,
			(knowledge_snapshot_sha256 IS NOT NULL)
			  + (knowledge_snapshot_tenant_catalog_revision IS NOT NULL)
			  + (knowledge_snapshot_tenant_catalog_state_token IS NOT NULL)
			  + (knowledge_snapshot_object_count IS NOT NULL)
			  + (knowledge_snapshot_lookup_asset_count IS NOT NULL)
			    AS knowledge_snapshot_present_fields,
			length(knowledge_snapshot_sha256)
			    AS knowledge_snapshot_sha256_bytes,
			knowledge_snapshot_tenant_catalog_revision,
			length(knowledge_snapshot_tenant_catalog_state_token)
			    AS knowledge_snapshot_tenant_catalog_state_token_bytes,
			knowledge_snapshot_object_count,
			knowledge_snapshot_lookup_asset_count
		`).
		Limit(limit).
		Find(&sizes)
	if preflight.Error != nil {
		return nil, preflight.Error
	}
	for _, size := range sizes {
		if !validEventSize(size) {
			return nil, fmt.Errorf(
				"%w: search-attempt audit field width or snapshot shape is invalid",
				ErrCorrupt,
			)
		}
	}
	var records []searchAttemptEventRecord
	hydrated := query.Session(&gorm.Session{}).Limit(limit).Find(&records)
	if hydrated.Error != nil {
		return nil, hydrated.Error
	}
	if len(records) != len(sizes) {
		return nil, fmt.Errorf("%w: search-attempt audit preflight result changed", ErrCorrupt)
	}
	for index := range records {
		if records[index].Sequence != sizes[index].Sequence {
			return nil, fmt.Errorf("%w: search-attempt audit preflight order changed", ErrCorrupt)
		}
	}
	return records, nil
}

func validEventSize(record eventSizeRecord) bool {
	if record.Sequence < 1 || record.Sequence > maximumPersistedSequence ||
		record.TenantIDBytes < 1 || record.TenantIDBytes > maximumTenantIDBytes ||
		record.ActorKindBytes < 1 || record.ActorKindBytes > int64(maximumActorKindBytes) ||
		record.ActorIDBytes < 1 || record.ActorIDBytes > maximumOwnerIDBytes ||
		record.ActorRoleBytes < 1 || record.ActorRoleBytes > int64(maximumActorRoleBytes) ||
		record.OwnerIDBytes < 1 || record.OwnerIDBytes > maximumOwnerIDBytes ||
		record.SearchJobIDBytes < 1 || record.SearchJobIDBytes > maximumSearchJobIDBytes {
		return false
	}
	if record.KnowledgeSnapshotPresentFields == 0 {
		return !record.KnowledgeSnapshotSHA256Bytes.Valid &&
			!record.KnowledgeSnapshotTenantCatalogRevision.Valid &&
			!record.KnowledgeSnapshotTenantCatalogStateTokenBytes.Valid &&
			!record.KnowledgeSnapshotObjectCount.Valid &&
			!record.KnowledgeSnapshotLookupAssetCount.Valid
	}
	commonValid := record.KnowledgeSnapshotSHA256Bytes.Valid &&
		record.KnowledgeSnapshotSHA256Bytes.Int64 == sha256.Size &&
		record.KnowledgeSnapshotTenantCatalogRevision.Valid &&
		record.KnowledgeSnapshotTenantCatalogRevision.Int64 >= 0 &&
		record.KnowledgeSnapshotTenantCatalogRevision.Int64 <= maximumPersistedSequence &&
		record.KnowledgeSnapshotTenantCatalogStateTokenBytes.Valid &&
		record.KnowledgeSnapshotTenantCatalogStateTokenBytes.Int64 == sha256.Size &&
		record.KnowledgeSnapshotObjectCount.Valid &&
		record.KnowledgeSnapshotObjectCount.Int64 >= 0 &&
		record.KnowledgeSnapshotObjectCount.Int64 <= maximumKnowledgeObjects
	if !commonValid {
		return false
	}
	return record.KnowledgeSnapshotPresentFields == 5 &&
		record.KnowledgeSnapshotLookupAssetCount.Valid &&
		record.KnowledgeSnapshotLookupAssetCount.Int64 >= 0 &&
		record.KnowledgeSnapshotLookupAssetCount.Int64 <= maximumKnowledgeLookupAssets
}

func setRecordKnowledgeSnapshot(
	record *searchAttemptEventRecord,
	knowledgeSnapshot *opensplunk.KnowledgeSnapshotRef,
) {
	if record == nil || knowledgeSnapshot == nil {
		return
	}
	record.KnowledgeSnapshotSHA256 = bytes.Clone(knowledgeSnapshot.GetSnapshotSha256())
	// #nosec G115 -- normalizeKnowledgeSnapshotRef capped this at MaxInt64-1.
	record.KnowledgeSnapshotTenantCatalogRevision = sql.NullInt64{
		Int64: int64(knowledgeSnapshot.GetTenantCatalogRevision()),
		Valid: true,
	}
	record.KnowledgeSnapshotTenantCatalogStateToken = bytes.Clone(
		knowledgeSnapshot.GetTenantCatalogStateToken(),
	)
	record.KnowledgeSnapshotObjectCount = sql.NullInt64{
		Int64: int64(knowledgeSnapshot.GetObjectCount()),
		Valid: true,
	}
	record.KnowledgeSnapshotLookupAssetCount = sql.NullInt64{
		Int64: int64(knowledgeSnapshot.GetLookupAssetCount()),
		Valid: true,
	}
}

func knowledgeSnapshotFromRecord(
	record searchAttemptEventRecord,
) (*opensplunk.KnowledgeSnapshotRef, error) {
	present := 0
	if record.KnowledgeSnapshotSHA256 != nil {
		present++
	}
	if record.KnowledgeSnapshotTenantCatalogRevision.Valid {
		present++
	}
	if record.KnowledgeSnapshotTenantCatalogStateToken != nil {
		present++
	}
	if record.KnowledgeSnapshotObjectCount.Valid {
		present++
	}
	if record.KnowledgeSnapshotLookupAssetCount.Valid {
		present++
	}
	if present == 0 {
		return nil, nil
	}
	lookupCountInvalid := record.KnowledgeSnapshotLookupAssetCount.Valid &&
		(record.KnowledgeSnapshotLookupAssetCount.Int64 < 0 ||
			record.KnowledgeSnapshotLookupAssetCount.Int64 > maximumKnowledgeLookupAssets)
	if present != 5 ||
		record.KnowledgeSnapshotTenantCatalogRevision.Int64 < 0 ||
		record.KnowledgeSnapshotTenantCatalogRevision.Int64 > maximumPersistedSequence ||
		record.KnowledgeSnapshotObjectCount.Int64 < 0 ||
		record.KnowledgeSnapshotObjectCount.Int64 > maximumKnowledgeObjects ||
		lookupCountInvalid {
		return nil, fmt.Errorf(
			"%w: search-attempt audit knowledge snapshot tuple is incomplete",
			ErrCorrupt,
		)
	}
	// #nosec G115 -- negative values are rejected immediately above.
	knowledgeSnapshot := &opensplunk.KnowledgeSnapshotRef{
		SnapshotSha256: bytes.Clone(record.KnowledgeSnapshotSHA256),
		TenantCatalogRevision: uint64(
			record.KnowledgeSnapshotTenantCatalogRevision.Int64,
		),
		TenantCatalogStateToken: bytes.Clone(
			record.KnowledgeSnapshotTenantCatalogStateToken,
		),
		ObjectCount:      uint32(record.KnowledgeSnapshotObjectCount.Int64),
		LookupAssetCount: uint32(record.KnowledgeSnapshotLookupAssetCount.Int64),
	}
	if err := validateKnowledgeSnapshotRef(knowledgeSnapshot); err != nil {
		return nil, fmt.Errorf(
			"%w: persisted search-attempt audit knowledge snapshot is invalid",
			ErrCorrupt,
		)
	}
	return knowledgeSnapshot, nil
}

func validateAllTenantIntegrity(database *gorm.DB) error {
	var invalidTenantWidth int64
	if err := database.Raw(`
		SELECT EXISTS (
			SELECT 1
			FROM search_attempt_audit_tenant_state
			WHERE typeof(tenant_id) <> 'text'
			   OR length(CAST(tenant_id AS BLOB)) NOT BETWEEN 1 AND ?
			LIMIT 1
		)
	`, maximumTenantIDBytes).Scan(&invalidTenantWidth).Error; err != nil {
		return err
	}
	if invalidTenantWidth != 0 {
		return fmt.Errorf("%w: search-attempt audit tenant identity width is invalid", ErrCorrupt)
	}
	var orphaned int64
	if err := database.Raw(`
		SELECT EXISTS (
			SELECT 1
			FROM search_attempt_audit_events AS event
			LEFT JOIN search_attempt_audit_tenant_state AS state
			  ON state.tenant_id = event.tenant_id
			WHERE state.tenant_id IS NULL
			LIMIT 1
		)
	`).Scan(&orphaned).Error; err != nil {
		return err
	}
	if orphaned != 0 {
		return fmt.Errorf("%w: search-attempt audit event has no tenant state", ErrCorrupt)
	}

	var after string
	haveAfter := false
	for {
		var states []searchAttemptTenantStateRecord
		query := database.Model(&searchAttemptTenantStateRecord{}).
			Order("tenant_id ASC").Limit(maximumIntegrityBatch)
		if haveAfter {
			query = query.Where("tenant_id > ?", after)
		}
		if err := query.Find(&states).Error; err != nil {
			return err
		}
		if len(states) == 0 {
			return nil
		}
		for index, state := range states {
			if !validTenantState(state, state.TenantID) ||
				(haveAfter && state.TenantID <= after) ||
				(index > 0 && state.TenantID <= states[index-1].TenantID) {
				return fmt.Errorf("%w: search-attempt audit tenant scan is invalid", ErrCorrupt)
			}
			if err := validateTenantIntegrity(database, state); err != nil {
				return err
			}
		}
		after = strings.Clone(states[len(states)-1].TenantID)
		haveAfter = true
		if len(states) < maximumIntegrityBatch {
			return nil
		}
	}
}

func validateTenantIntegrity(database *gorm.DB, state searchAttemptTenantStateRecord) error {
	var aggregate tenantAggregate
	if err := database.Raw(`
		SELECT
			COUNT(*) AS retained_count,
			MIN(sequence) AS min_sequence,
			MAX(sequence) AS max_sequence
		FROM (
			SELECT sequence
			FROM search_attempt_audit_events
			WHERE tenant_id = ?
			ORDER BY sequence
			LIMIT ?
		)
	`, state.TenantID, MaximumRetainedAttempts+1).Scan(&aggregate).Error; err != nil {
		return err
	}
	if aggregate.RetainedCount != state.RetainedCount {
		return fmt.Errorf("%w: search-attempt audit retained count does not match events", ErrCorrupt)
	}
	if state.RetainedCount == 0 {
		if aggregate.MinSequence != nil || aggregate.MaxSequence != nil {
			return fmt.Errorf("%w: empty search-attempt audit tenant has sequence bounds", ErrCorrupt)
		}
		return nil
	}
	if aggregate.MinSequence == nil || aggregate.MaxSequence == nil ||
		*aggregate.MinSequence != state.FirstSequence ||
		*aggregate.MaxSequence != state.NextSequence-1 {
		return fmt.Errorf("%w: search-attempt audit retained sequence is not dense", ErrCorrupt)
	}
	for expected := state.FirstSequence; expected < state.NextSequence; {
		limit := int(min(int64(maximumIntegrityBatch), state.NextSequence-expected))
		records, err := readPreflightedRecords(database.
			Model(&searchAttemptEventRecord{}).
			Where("tenant_id = ? AND sequence >= ?", state.TenantID, expected).
			Order("sequence ASC"), limit)
		if err != nil {
			return err
		}
		if len(records) != limit {
			return fmt.Errorf("%w: search-attempt audit tenant scan ended early", ErrCorrupt)
		}
		for _, record := range records {
			if record.TenantID != state.TenantID || record.Sequence != expected {
				return fmt.Errorf("%w: search-attempt audit sequence scan is not dense", ErrCorrupt)
			}
			if _, err := eventFromRecord(record); err != nil {
				return err
			}
			expected++
		}
	}
	return nil
}

func eventDigest(record searchAttemptEventRecord) (string, error) {
	knowledgeSnapshot, err := knowledgeSnapshotFromRecord(record)
	if err != nil {
		return "", err
	}
	type snapshotIdentity struct {
		SHA256                  []byte `json:"d"`
		TenantCatalogRevision   uint64 `json:"r"`
		TenantCatalogStateToken []byte `json:"t"`
		ObjectCount             uint32 `json:"c"`
		LookupAssetCount        uint32 `json:"l"`
	}
	var snapshot *snapshotIdentity
	if knowledgeSnapshot != nil {
		snapshot = &snapshotIdentity{
			SHA256:                  knowledgeSnapshot.GetSnapshotSha256(),
			TenantCatalogRevision:   knowledgeSnapshot.GetTenantCatalogRevision(),
			TenantCatalogStateToken: knowledgeSnapshot.GetTenantCatalogStateToken(),
			ObjectCount:             knowledgeSnapshot.GetObjectCount(),
			LookupAssetCount:        knowledgeSnapshot.GetLookupAssetCount(),
		}
	}
	payload, err := json.Marshal(struct {
		TenantID            string          `json:"n"`
		Sequence            int64           `json:"s"`
		OccurredAtUnixMicro int64           `json:"o"`
		ActorKind           audit.ActorKind `json:"k"`
		ActorID             string          `json:"i"`
		ActorRole           audit.ActorRole `json:"r"`
		OwnerID             string          `json:"w"`
		SearchJobID         string          `json:"j"`
		// Keep this last and omitted when no knowledge snapshot was admitted.
		KnowledgeSnapshot *snapshotIdentity `json:"x,omitempty"`
	}{
		TenantID:            record.TenantID,
		Sequence:            record.Sequence,
		OccurredAtUnixMicro: record.OccurredAtUnixMicro,
		ActorKind:           record.ActorKind,
		ActorID:             record.ActorID,
		ActorRole:           record.ActorRole,
		OwnerID:             record.OwnerID,
		SearchJobID:         record.SearchJobID,
		KnowledgeSnapshot:   snapshot,
	})
	if err != nil {
		return "", fmt.Errorf("encode search-attempt audit event identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}
