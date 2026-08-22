package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"fortio.org/safecast"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
)

const (
	mutationRequestDigestFormatVersion int64 = 1
	mutationOutcomeFormatVersion       int64 = 1
	maximumMutationOutcomeBytes              = 1024
	maximumMutationRouteBytes                = len(mutationRouteQuarantine)
)

// idempotencyRecord is the exact GORM projection of the migration-owned
// replay receipt. It contains only a compact immutable version reference; the
// public response is reconstructed from catalog authorities after current
// authorization and quarantine checks.
type idempotencyRecord struct {
	TenantID                   string          `gorm:"column:tenant_id"`
	ActorKind                  audit.ActorKind `gorm:"column:actor_kind"`
	ActorID                    string          `gorm:"column:actor_id"`
	Route                      string          `gorm:"column:route"`
	ClientRequestID            string          `gorm:"column:client_request_id"`
	MutationKind               string          `gorm:"column:mutation_kind"`
	RequestDigestFormatVersion int64           `gorm:"column:request_digest_format_version"`
	RequestDigest              []byte          `gorm:"column:request_digest"`
	OutcomeFormatVersion       int64           `gorm:"column:outcome_format_version"`
	OutcomeProto               []byte          `gorm:"column:outcome_proto"`
	CommittedCatalogRevision   int64           `gorm:"column:committed_catalog_revision"`
	CommittedCatalogStateToken []byte          `gorm:"column:committed_catalog_state_token"`
	KnowledgeObjectID          string          `gorm:"column:knowledge_object_id"`
	ObjectVersion              int64           `gorm:"column:object_version"`
	SuccessfulAuditSequence    *int64          `gorm:"column:successful_audit_sequence"`
	RecoveryAuditSequence      *int64          `gorm:"column:recovery_audit_sequence"`
	CreatedAtUnixMicro         int64           `gorm:"column:created_at_unix_micro"`
	RetentionAnchorUnixMicro   int64           `gorm:"column:retention_anchor_unix_micro"`
	RetainUntilUnixMicro       int64           `gorm:"column:retain_until_unix_micro"`
}

func (idempotencyRecord) TableName() string { return "knowledge_mutation_idempotency" }

type idempotencyWidthRecord struct {
	TenantIDBytes      int64 `gorm:"column:tenant_id_bytes"`
	ActorKindBytes     int64 `gorm:"column:actor_kind_bytes"`
	ActorIDBytes       int64 `gorm:"column:actor_id_bytes"`
	RouteBytes         int64 `gorm:"column:route_bytes"`
	RequestIDBytes     int64 `gorm:"column:request_id_bytes"`
	MutationKindBytes  int64 `gorm:"column:mutation_kind_bytes"`
	RequestDigestBytes int64 `gorm:"column:request_digest_bytes"`
	OutcomeProtoBytes  int64 `gorm:"column:outcome_proto_bytes"`
	StateTokenBytes    int64 `gorm:"column:state_token_bytes"`
	ObjectIDBytes      int64 `gorm:"column:object_id_bytes"`
}

// idempotencyWidthProjectionSQL is the scalar-width projection that hydrates
// idempotencyWidthRecord. Shared by the replay preflight and the expired
// receipt reclamation preflight.
const idempotencyWidthProjectionSQL = `
		length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
		length(CAST(actor_kind AS BLOB)) AS actor_kind_bytes,
		length(CAST(actor_id AS BLOB)) AS actor_id_bytes,
		length(CAST(route AS BLOB)) AS route_bytes,
		length(CAST(client_request_id AS BLOB)) AS request_id_bytes,
		length(CAST(mutation_kind AS BLOB)) AS mutation_kind_bytes,
		length(request_digest) AS request_digest_bytes,
		length(outcome_proto) AS outcome_proto_bytes,
		length(committed_catalog_state_token) AS state_token_bytes,
		length(CAST(knowledge_object_id AS BLOB)) AS object_id_bytes
	`

type idempotencyObjectIdentity struct {
	KnowledgeObjectIDBytes int64  `gorm:"column:knowledge_object_id_bytes"`
	KnowledgeObjectID      string `gorm:"column:knowledge_object_id"`
}

type idempotencyRequestDigest struct {
	RequestDigestBytes int64  `gorm:"column:request_digest_bytes"`
	RequestDigest      []byte `gorm:"column:request_digest"`
}

// idempotencyReceiptQuery builds the exact-receipt lookup shared by every
// replay reader. Callers keep their own precondition guard so the failure
// diagnostic names the reader that rejected the request.
func idempotencyReceiptQuery(tx *gorm.DB, prepared *preparedMutation, route string) *gorm.DB {
	return tx.Model(&idempotencyRecord{}).Where(
		"tenant_id = ? AND actor_kind = ? AND actor_id = ? AND route = ? AND client_request_id = ?",
		prepared.scope.tenantID,
		prepared.actor.Kind,
		prepared.actor.ID,
		route,
		prepared.clientRequestID,
	)
}

// readIdempotencyObjectIdentity is the only receipt read allowed before
// current authorization. Its projection is memory-bounded even if a damaged
// database contains an oversized object identity; request digests, outcome
// bytes, revisions, tokens, and historical references remain untouched.
func readIdempotencyObjectIdentity(
	tx *gorm.DB,
	prepared *preparedMutation,
	route string,
) (string, bool, error) {
	if tx == nil || prepared == nil || !validPreparedReplayIdentity(prepared, route) {
		return "", false, fmt.Errorf(
			"%w: knowledge idempotency lookup authority is invalid",
			control.ErrInvalidArgument,
		)
	}
	var identities []idempotencyObjectIdentity
	if err := idempotencyReceiptQuery(tx, prepared, route).Select(`
		length(CAST(knowledge_object_id AS BLOB)) AS knowledge_object_id_bytes,
		CAST(substr(CAST(knowledge_object_id AS BLOB), 1, ?) AS TEXT) AS knowledge_object_id
	`, maximumObjectIDBytes+1).Limit(2).Find(&identities).Error; err != nil {
		return "", false, err
	}
	if len(identities) == 0 {
		return "", false, nil
	}
	if len(identities) != 1 ||
		identities[0].KnowledgeObjectIDBytes < 1 ||
		identities[0].KnowledgeObjectIDBytes > maximumObjectIDBytes ||
		int64(len(identities[0].KnowledgeObjectID)) != identities[0].KnowledgeObjectIDBytes ||
		!validIdentity(identities[0].KnowledgeObjectID, maximumObjectIDBytes) {
		// Without one valid current object identity there is no safe authority
		// lookup. Preserve the same public absence response as an unauthorized
		// or missing object, regardless of receipt corruption.
		return "", true, control.ErrNotFound
	}
	return strings.Clone(identities[0].KnowledgeObjectID), true, nil
}

// readAuthorizedIdempotencyRecord separates identity discovery from receipt
// hydration. Public retries first prove current owner/app authority and apply
// quarantine's permanent redaction rule; only then may strict receipt widths,
// request-digest equality, and the canonical outcome envelope be inspected.
func (writer *Writer) readAuthorizedIdempotencyRecord(
	ctx context.Context,
	tx *gorm.DB,
	prepared *preparedMutation,
	route string,
) (
	record idempotencyRecord,
	found bool,
	authorized *AuthorizedContext,
	returnedErr error,
) {
	defer func() {
		if returnedErr != nil && authorized != nil {
			returnedErr = withAuthorizedContext(returnedErr, *authorized)
		}
	}()
	if err := validateContext(ctx); err != nil {
		return idempotencyRecord{}, false, nil, err
	}
	if writer == nil || writer.reader == nil || tx == nil || prepared == nil {
		return idempotencyRecord{}, false, nil, fmt.Errorf(
			"%w: knowledge replay store is unavailable",
			control.ErrInvalidArgument,
		)
	}
	objectID, found, err := readIdempotencyObjectIdentity(tx, prepared, route)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return idempotencyRecord{}, found, nil, err
		}
		return idempotencyRecord{}, found, nil, withErrorDisposition(
			err,
			ErrorDispositionIndeterminate,
		)
	}
	if !found {
		return idempotencyRecord{}, false, nil, nil
	}
	database := tx.WithContext(ctx)
	if database.Error != nil {
		return idempotencyRecord{}, true, nil, withErrorDisposition(
			database.Error,
			ErrorDispositionIndeterminate,
		)
	}
	current, isAuthorized, err := readAuthorizedReplayRegistry(database, *prepared, objectID)
	if err != nil {
		return idempotencyRecord{}, true, nil, withErrorDisposition(
			err,
			ErrorDispositionIndeterminate,
		)
	}
	if !isAuthorized {
		return idempotencyRecord{}, true, nil, control.ErrNotFound
	}
	if err := validateAuthorizedReplayCurrent(database, *prepared, current); err != nil {
		if !errors.Is(err, control.ErrNotFound) &&
			!errors.Is(err, ErrIdempotentOutcomeRedacted) {
			err = withErrorDisposition(err, ErrorDispositionIndeterminate)
		}
		return idempotencyRecord{}, true, nil, err
	}
	authorizedValue := authorizedReplayContext(route, current)
	authorized = &authorizedValue

	storedDigest, stillFound, err := readIdempotencyRequestDigest(
		database,
		prepared,
		route,
	)
	if err != nil || !stillFound {
		if err == nil {
			err = fmt.Errorf(
				"%w: knowledge idempotency receipt disappeared after authorization",
				ErrCorrupt,
			)
		}
		return idempotencyRecord{}, true, authorized, withErrorDisposition(
			err,
			ErrorDispositionIndeterminate,
		)
	}
	if subtle.ConstantTimeCompare(storedDigest, prepared.requestDigest[:]) != 1 {
		return idempotencyRecord{}, true, authorized, withErrorDisposition(
			ErrIdempotencyConflict,
			ErrorDispositionDefinitiveRejection,
		)
	}

	record, stillFound, err = readIdempotencyRecord(database, prepared, route)
	if err != nil {
		disposition := ErrorDispositionKnownCommitted
		if errors.Is(err, ErrIdempotencyConflict) {
			disposition = ErrorDispositionIndeterminate
		}
		return idempotencyRecord{}, true, authorized, withErrorDisposition(
			err,
			disposition,
		)
	}
	if !stillFound || record.KnowledgeObjectID != objectID {
		return idempotencyRecord{}, true, authorized, withErrorDisposition(
			fmt.Errorf(
				"%w: knowledge idempotency receipt changed after authorization",
				ErrCorrupt,
			),
			ErrorDispositionIndeterminate,
		)
	}
	return record, true, authorized, nil
}

// readIdempotencyRequestDigest is the first receipt authority opened after
// current-policy authorization and quarantine redaction. Its capped projection
// lets callers distinguish a proven different request from an exact committed
// retry without hydrating or trusting the outcome envelope first.
func readIdempotencyRequestDigest(
	tx *gorm.DB,
	prepared *preparedMutation,
	route string,
) ([]byte, bool, error) {
	if tx == nil || prepared == nil || !validPreparedReplayIdentity(prepared, route) {
		return nil, false, fmt.Errorf(
			"%w: knowledge idempotency digest lookup authority is invalid",
			control.ErrInvalidArgument,
		)
	}
	var digests []idempotencyRequestDigest
	if err := idempotencyReceiptQuery(tx, prepared, route).Select(`
		length(request_digest) AS request_digest_bytes,
		substr(request_digest, 1, ?) AS request_digest
	`, sha256.Size+1).Limit(2).Find(&digests).Error; err != nil {
		return nil, false, err
	}
	if len(digests) == 0 {
		return nil, false, nil
	}
	if len(digests) != 1 || digests[0].RequestDigestBytes != sha256.Size ||
		len(digests[0].RequestDigest) != sha256.Size {
		return nil, true, fmt.Errorf(
			"%w: knowledge idempotency request digest is invalid",
			ErrCorrupt,
		)
	}
	return bytes.Clone(digests[0].RequestDigest), true, nil
}

// readIdempotencyRecord performs a scalar-width preflight before detaching the
// bounded receipt. A found identity with a different request digest is a
// conflict, compared in constant time, and never reaches response hydration.
func readIdempotencyRecord(
	tx *gorm.DB,
	prepared *preparedMutation,
	route string,
) (idempotencyRecord, bool, error) {
	if tx == nil || prepared == nil || !validPreparedReplayIdentity(prepared, route) {
		return idempotencyRecord{}, false, fmt.Errorf(
			"%w: knowledge idempotency lookup authority is invalid",
			control.ErrInvalidArgument,
		)
	}
	query := idempotencyReceiptQuery(tx, prepared, route)
	var widths []idempotencyWidthRecord
	if err := query.Session(&gorm.Session{}).
		Select(idempotencyWidthProjectionSQL).
		Limit(2).Find(&widths).Error; err != nil {
		return idempotencyRecord{}, false, err
	}
	if len(widths) == 0 {
		return idempotencyRecord{}, false, nil
	}
	if len(widths) != 1 || !validIdempotencyWidth(widths[0]) {
		return idempotencyRecord{}, true, fmt.Errorf(
			"%w: knowledge idempotency receipt width is invalid",
			ErrCorrupt,
		)
	}

	var records []idempotencyRecord
	if err := query.Session(&gorm.Session{}).Limit(2).Find(&records).Error; err != nil {
		return idempotencyRecord{}, true, err
	}
	if len(records) != 1 {
		return idempotencyRecord{}, true, fmt.Errorf(
			"%w: knowledge idempotency receipt changed after preflight",
			ErrCorrupt,
		)
	}
	record := records[0]
	if err := validateIdempotencyRecord(prepared, route, record); err != nil {
		return idempotencyRecord{}, true, err
	}
	return detachIdempotencyRecord(record), true, nil
}

func validPreparedReplayIdentity(prepared *preparedMutation, route string) bool {
	if prepared == nil || !validMutationRoute(route) ||
		!validIdentity(prepared.scope.tenantID, maximumTenantIDBytes) ||
		!validIdentity(prepared.scope.ownerID, maximumOwnerIDBytes) ||
		len(prepared.scope.writableAppIDs) == 0 ||
		len(prepared.scope.writableAppIDs) > maximumReadableApps ||
		!prepared.actor.Valid() || !validClientRequestID(prepared.clientRequestID) {
		return false
	}
	for index, appID := range prepared.scope.writableAppIDs {
		if !control.ValidCanonicalAppID(appID) || index > 0 && prepared.scope.writableAppIDs[index-1] >= appID {
			return false
		}
	}
	return prepared.actor.Kind != audit.ActorKindBrowser ||
		prepared.actor.Role == audit.ActorRoleAdministrator
}

func validMutationRoute(route string) bool {
	switch route {
	case mutationRouteCreate, mutationRouteUpdate, mutationRouteSetState, mutationRouteDelete:
		return true
	default:
		return false
	}
}

func validIdempotencyWidth(width idempotencyWidthRecord) bool {
	return width.TenantIDBytes >= 1 && width.TenantIDBytes <= maximumTenantIDBytes &&
		width.ActorKindBytes >= int64(len(audit.ActorKindSystem)) &&
		width.ActorKindBytes <= int64(len(audit.ActorKindBrowser)) &&
		width.ActorIDBytes >= 1 && width.ActorIDBytes <= maximumOwnerIDBytes &&
		width.RouteBytes >= 1 && width.RouteBytes <= int64(maximumMutationRouteBytes) &&
		width.RequestIDBytes >= minimumClientRequestIDBytes &&
		width.RequestIDBytes <= maximumClientRequestIDBytes &&
		width.MutationKindBytes >= minimumPersistedMutationKindBytes &&
		width.MutationKindBytes <= maximumPersistedMutationKindBytes &&
		width.RequestDigestBytes == 32 &&
		width.OutcomeProtoBytes >= 1 && width.OutcomeProtoBytes <= maximumMutationOutcomeBytes &&
		width.StateTokenBytes == 32 &&
		width.ObjectIDBytes >= 1 && width.ObjectIDBytes <= maximumObjectIDBytes
}

func validateIdempotencyRecord(
	prepared *preparedMutation,
	route string,
	record idempotencyRecord,
) error {
	if prepared == nil || !validPreparedReplayIdentity(prepared, route) {
		return fmt.Errorf("%w: knowledge replay authority is invalid", control.ErrInvalidArgument)
	}
	if err := validateStoredIdempotencyRecord(record); err != nil {
		return err
	}
	if record.TenantID != prepared.scope.tenantID || record.ActorKind != prepared.actor.Kind ||
		record.ActorID != prepared.actor.ID || record.Route != route ||
		record.ClientRequestID != prepared.clientRequestID ||
		!mutationKindMatchesRoute(route, record.MutationKind) {
		return fmt.Errorf("%w: knowledge idempotency receipt is invalid", ErrCorrupt)
	}
	if subtle.ConstantTimeCompare(record.RequestDigest, prepared.requestDigest[:]) != 1 {
		return ErrIdempotencyConflict
	}
	return nil
}

func validateStoredIdempotencyRecord(record idempotencyRecord) error {
	ordinaryAudit := record.MutationKind != "quarantine" &&
		record.SuccessfulAuditSequence != nil && record.RecoveryAuditSequence == nil &&
		*record.SuccessfulAuditSequence >= 1 &&
		*record.SuccessfulAuditSequence <= audit.MaximumEventsPerTenant
	recoveryAudit := record.MutationKind == "quarantine" &&
		record.SuccessfulAuditSequence == nil && record.RecoveryAuditSequence != nil &&
		*record.RecoveryAuditSequence >= 1 &&
		*record.RecoveryAuditSequence <= maximumRecoveryAuditRecords
	if !validIdentity(record.TenantID, maximumTenantIDBytes) ||
		record.ActorKind != audit.ActorKindSystem && record.ActorKind != audit.ActorKindBrowser ||
		!validIdentity(record.ActorID, maximumOwnerIDBytes) ||
		!mutationReceiptRouteMatchesKind(record.Route, record.MutationKind) ||
		!validClientRequestID(record.ClientRequestID) ||
		record.RequestDigestFormatVersion != mutationRequestDigestFormatVersion ||
		len(record.RequestDigest) != sha256.Size ||
		record.OutcomeFormatVersion != mutationOutcomeFormatVersion ||
		len(record.OutcomeProto) < 1 || len(record.OutcomeProto) > maximumMutationOutcomeBytes ||
		record.CommittedCatalogRevision < 1 || record.CommittedCatalogRevision > math.MaxInt64-1 ||
		len(record.CommittedCatalogStateToken) != catalogStateTokenBytes ||
		!validIdentity(record.KnowledgeObjectID, maximumObjectIDBytes) ||
		record.ObjectVersion < 1 || record.ObjectVersion > maximumVersionsPerTenant ||
		!ordinaryAudit && !recoveryAudit {
		return fmt.Errorf("%w: knowledge idempotency receipt is invalid", ErrCorrupt)
	}
	if _, err := canonicalTime(record.CreatedAtUnixMicro); err != nil {
		return fmt.Errorf("%w: knowledge idempotency receipt timestamp is invalid", ErrCorrupt)
	}
	retentionMicros := record.RetainUntilUnixMicro - record.RetentionAnchorUnixMicro
	if _, err := canonicalTime(record.RetentionAnchorUnixMicro); err != nil ||
		record.RetentionAnchorUnixMicro < record.CreatedAtUnixMicro {
		return fmt.Errorf("%w: knowledge idempotency retention anchor is invalid", ErrCorrupt)
	}
	if _, err := canonicalTime(record.RetainUntilUnixMicro); err != nil ||
		record.RetainUntilUnixMicro < record.RetentionAnchorUnixMicro ||
		retentionMicros < int64(minimumIdempotencyRetention/time.Microsecond) ||
		retentionMicros > int64(maximumIdempotencyRetention/time.Microsecond) {
		return fmt.Errorf("%w: knowledge idempotency retention is invalid", ErrCorrupt)
	}
	if _, err := decodeOutcomeReference(record); err != nil {
		return err
	}
	return nil
}

func mutationKindMatchesRoute(route, mutationKind string) bool {
	return route != "" && route == routeForMutationKind(mutationKind)
}

func routeForMutationKind(mutationKind string) string {
	switch mutationKind {
	case "create":
		return mutationRouteCreate
	case "update", "scope_change":
		return mutationRouteUpdate
	case "enable", "disable":
		return mutationRouteSetState
	case "delete":
		return mutationRouteDelete
	default:
		return ""
	}
}

func detachIdempotencyRecord(record idempotencyRecord) idempotencyRecord {
	record.TenantID = strings.Clone(record.TenantID)
	record.ActorID = strings.Clone(record.ActorID)
	record.Route = strings.Clone(record.Route)
	record.ClientRequestID = strings.Clone(record.ClientRequestID)
	record.MutationKind = strings.Clone(record.MutationKind)
	record.RequestDigest = bytes.Clone(record.RequestDigest)
	record.OutcomeProto = bytes.Clone(record.OutcomeProto)
	record.CommittedCatalogStateToken = bytes.Clone(record.CommittedCatalogStateToken)
	record.KnowledgeObjectID = strings.Clone(record.KnowledgeObjectID)
	record.SuccessfulAuditSequence = cloneInt64(record.SuccessfulAuditSequence)
	record.RecoveryAuditSequence = cloneInt64(record.RecoveryAuditSequence)
	return record
}

type mutationOutcomeAuthority struct {
	route                    string
	mutationKind             string
	objectID                 string
	version                  uint64
	digest                   []byte
	catalogRevision          uint64
	catalogStateToken        []byte
	successfulAuditSequence  uint64
	recoveryAuditSequence    uint64
	occurredAtUnixMicro      int64
	retentionAnchorUnixMicro int64
	retainUntilUnixMicro     int64
}

func encodeOutcomeReference(authority mutationOutcomeAuthority) ([]byte, error) {
	quarantine := authority.mutationKind == "quarantine"
	if !mutationReceiptRouteMatchesKind(authority.route, authority.mutationKind) ||
		!validIdentity(authority.objectID, maximumObjectIDBytes) ||
		authority.version == 0 || authority.version > math.MaxInt64 ||
		!validOutcomeDefinitionDigest(quarantine, authority.digest) ||
		authority.catalogRevision == 0 || authority.catalogRevision > math.MaxInt64-1 ||
		len(authority.catalogStateToken) != catalogStateTokenBytes ||
		(authority.successfulAuditSequence == 0) == (authority.recoveryAuditSequence == 0) ||
		authority.successfulAuditSequence > audit.MaximumEventsPerTenant ||
		authority.recoveryAuditSequence > maximumRecoveryAuditRecords ||
		!validOutcomeRetentionAuthority(authority) {
		return nil, fmt.Errorf("%w: knowledge outcome reference is invalid", control.ErrInvalidArgument)
	}
	if authority.mutationKind == "quarantine" && authority.recoveryAuditSequence == 0 ||
		authority.mutationKind != "quarantine" && authority.successfulAuditSequence == 0 {
		return nil, fmt.Errorf("%w: knowledge outcome audit reference is invalid", control.ErrInvalidArgument)
	}
	outcome := &opensplunk.KnowledgeMutationOutcomeRecord{
		Route:        strings.Clone(authority.route),
		MutationKind: strings.Clone(authority.mutationKind),
		Object: &opensplunk.KnowledgeObjectVersionReference{
			KnowledgeObjectId: strings.Clone(authority.objectID),
			Version:           authority.version,
			DefinitionSha256:  bytes.Clone(authority.digest),
		},
		TenantCatalogRevision:    authority.catalogRevision,
		TenantCatalogStateToken:  bytes.Clone(authority.catalogStateToken),
		OccurredAtUnixMicro:      authority.occurredAtUnixMicro,
		RetentionAnchorUnixMicro: authority.retentionAnchorUnixMicro,
		RetainUntilUnixMicro:     authority.retainUntilUnixMicro,
	}
	if authority.successfulAuditSequence != 0 {
		outcome.AuditAuthority = &opensplunk.KnowledgeMutationOutcomeRecord_SuccessfulAuditSequence{
			SuccessfulAuditSequence: authority.successfulAuditSequence,
		}
	} else {
		outcome.AuditAuthority = &opensplunk.KnowledgeMutationOutcomeRecord_RecoveryAuditSequence{
			RecoveryAuditSequence: authority.recoveryAuditSequence,
		}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(outcome)
	if err != nil || len(encoded) < 1 || len(encoded) > maximumMutationOutcomeBytes {
		return nil, fmt.Errorf("%w: knowledge outcome reference cannot be encoded", control.ErrInvalidArgument)
	}
	return encoded, nil
}

func validOutcomeRetentionAuthority(authority mutationOutcomeAuthority) bool {
	if _, err := canonicalTime(authority.occurredAtUnixMicro); err != nil {
		return false
	}
	if _, err := canonicalTime(authority.retentionAnchorUnixMicro); err != nil ||
		authority.retentionAnchorUnixMicro < authority.occurredAtUnixMicro {
		return false
	}
	if _, err := canonicalTime(authority.retainUntilUnixMicro); err != nil ||
		authority.retainUntilUnixMicro < authority.retentionAnchorUnixMicro {
		return false
	}
	retentionMicros := authority.retainUntilUnixMicro - authority.retentionAnchorUnixMicro
	return retentionMicros >= int64(minimumIdempotencyRetention/time.Microsecond) &&
		retentionMicros <= int64(maximumIdempotencyRetention/time.Microsecond)
}

func decodeOutcomeReference(record idempotencyRecord) (*opensplunk.KnowledgeObjectVersionReference, error) {
	if record.OutcomeFormatVersion != mutationOutcomeFormatVersion ||
		len(record.OutcomeProto) < 1 || len(record.OutcomeProto) > maximumMutationOutcomeBytes {
		return nil, fmt.Errorf("%w: knowledge outcome receipt format is invalid", ErrCorrupt)
	}
	outcome := &opensplunk.KnowledgeMutationOutcomeRecord{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.OutcomeProto, outcome); err != nil {
		return nil, fmt.Errorf("%w: knowledge outcome reference is invalid", ErrCorrupt)
	}
	reference := outcome.GetObject()
	quarantine := record.MutationKind == "quarantine"
	referenceVersion := safecast.MustConv[int64](reference.GetVersion())
	if len(outcome.ProtoReflect().GetUnknown()) != 0 || reference == nil ||
		len(reference.ProtoReflect().GetUnknown()) != 0 ||
		outcome.GetRoute() != record.Route || outcome.GetMutationKind() != record.MutationKind ||
		record.CommittedCatalogRevision < 1 || record.CommittedCatalogRevision > math.MaxInt64-1 ||
		outcome.GetTenantCatalogRevision() != uint64(record.CommittedCatalogRevision) ||
		len(record.CommittedCatalogStateToken) != catalogStateTokenBytes ||
		subtle.ConstantTimeCompare(outcome.GetTenantCatalogStateToken(), record.CommittedCatalogStateToken) != 1 ||
		outcome.GetOccurredAtUnixMicro() != record.CreatedAtUnixMicro ||
		outcome.GetRetentionAnchorUnixMicro() != record.RetentionAnchorUnixMicro ||
		outcome.GetRetainUntilUnixMicro() != record.RetainUntilUnixMicro ||
		!outcomeAuditAuthorityAgrees(outcome, record) ||
		!mutationReceiptRouteMatchesKind(outcome.GetRoute(), outcome.GetMutationKind()) ||
		!validIdentity(reference.GetKnowledgeObjectId(), maximumObjectIDBytes) ||
		reference.GetVersion() == 0 || reference.GetVersion() > math.MaxInt64 ||
		!validOutcomeDefinitionDigest(quarantine, reference.GetDefinitionSha256()) ||
		reference.GetKnowledgeObjectId() != record.KnowledgeObjectID ||
		referenceVersion != record.ObjectVersion {
		return nil, fmt.Errorf("%w: knowledge outcome reference is invalid", ErrCorrupt)
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(outcome)
	if err != nil || !bytes.Equal(canonical, record.OutcomeProto) {
		return nil, fmt.Errorf("%w: knowledge outcome reference is noncanonical", ErrCorrupt)
	}
	return proto.Clone(reference).(*opensplunk.KnowledgeObjectVersionReference), nil
}

func validOutcomeDefinitionDigest(quarantine bool, digest []byte) bool {
	if quarantine {
		return len(digest) == 0
	}
	return len(digest) == persistedKnowledgeDefinitionDigestBytes
}

func outcomeAuditAuthorityAgrees(
	outcome *opensplunk.KnowledgeMutationOutcomeRecord,
	record idempotencyRecord,
) bool {
	if outcome == nil {
		return false
	}
	if record.MutationKind == "quarantine" {
		return record.SuccessfulAuditSequence == nil && record.RecoveryAuditSequence != nil &&
			*record.RecoveryAuditSequence >= 1 && *record.RecoveryAuditSequence <= maximumRecoveryAuditRecords &&
			outcome.GetSuccessfulAuditSequence() == 0 &&
			outcome.GetRecoveryAuditSequence() == uint64(*record.RecoveryAuditSequence)
	}
	return record.SuccessfulAuditSequence != nil && record.RecoveryAuditSequence == nil &&
		*record.SuccessfulAuditSequence >= 1 && *record.SuccessfulAuditSequence <= audit.MaximumEventsPerTenant &&
		outcome.GetRecoveryAuditSequence() == 0 &&
		outcome.GetSuccessfulAuditSequence() == uint64(*record.SuccessfulAuditSequence)
}

type mutationCommitAuthorityWidth struct {
	TenantIDBytes      int64 `gorm:"column:tenant_id_bytes"`
	ActorKindBytes     int64 `gorm:"column:actor_kind_bytes"`
	ActorIDBytes       int64 `gorm:"column:actor_id_bytes"`
	RouteBytes         int64 `gorm:"column:route_bytes"`
	RequestIDBytes     int64 `gorm:"column:request_id_bytes"`
	RequestDigestBytes int64 `gorm:"column:request_digest_bytes"`
	StateTokenBytes    int64 `gorm:"column:state_token_bytes"`
	MutationKindBytes  int64 `gorm:"column:mutation_kind_bytes"`
	ObjectIDBytes      int64 `gorm:"column:object_id_bytes"`
}

// validateReplayCommitAuthority proves the receipt's historical branch pair
// against a separately immutable row. The current catalog head intentionally
// cannot provide this proof because it rotates after every later mutation.
func validateReplayCommitAuthority(tx *gorm.DB, record idempotencyRecord) error {
	if tx == nil || !validIdentity(record.TenantID, maximumTenantIDBytes) ||
		record.CommittedCatalogRevision < 1 || record.CommittedCatalogRevision > math.MaxInt64-1 {
		return fmt.Errorf("%w: knowledge replay commit lookup is invalid", ErrCorrupt)
	}
	query := tx.Model(&mutationCommitAuthorityRecord{}).Where(
		"tenant_id = ? AND catalog_revision = ?",
		record.TenantID,
		record.CommittedCatalogRevision,
	)
	var widths []mutationCommitAuthorityWidth
	if err := query.Session(&gorm.Session{}).Select(`
		length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
		length(CAST(actor_kind AS BLOB)) AS actor_kind_bytes,
		length(CAST(actor_id AS BLOB)) AS actor_id_bytes,
		length(CAST(route AS BLOB)) AS route_bytes,
		length(CAST(client_request_id AS BLOB)) AS request_id_bytes,
		length(request_digest) AS request_digest_bytes,
		length(catalog_state_token) AS state_token_bytes,
		length(CAST(mutation_kind AS BLOB)) AS mutation_kind_bytes,
		length(CAST(knowledge_object_id AS BLOB)) AS object_id_bytes
	`).Limit(2).Find(&widths).Error; err != nil {
		return err
	}
	if len(widths) != 1 || widths[0].TenantIDBytes < 1 ||
		widths[0].TenantIDBytes > maximumTenantIDBytes ||
		widths[0].ActorKindBytes < int64(len(audit.ActorKindSystem)) ||
		widths[0].ActorKindBytes > int64(len(audit.ActorKindBrowser)) ||
		widths[0].ActorIDBytes < 1 || widths[0].ActorIDBytes > maximumOwnerIDBytes ||
		widths[0].RouteBytes < 1 || widths[0].RouteBytes > int64(maximumMutationRouteBytes) ||
		widths[0].RequestIDBytes < minimumClientRequestIDBytes ||
		widths[0].RequestIDBytes > maximumClientRequestIDBytes ||
		widths[0].RequestDigestBytes != sha256.Size ||
		widths[0].StateTokenBytes != catalogStateTokenBytes ||
		widths[0].MutationKindBytes < minimumPersistedMutationKindBytes ||
		widths[0].MutationKindBytes > maximumPersistedMutationKindBytes ||
		widths[0].ObjectIDBytes < 1 || widths[0].ObjectIDBytes > maximumObjectIDBytes {
		return fmt.Errorf("%w: knowledge replay commit authority width is invalid", ErrCorrupt)
	}
	var authorities []mutationCommitAuthorityRecord
	if err := query.Session(&gorm.Session{}).Limit(2).Find(&authorities).Error; err != nil {
		return err
	}
	if len(authorities) != 1 {
		return fmt.Errorf("%w: knowledge replay commit authority changed after preflight", ErrCorrupt)
	}
	authority := authorities[0]
	if authority.TenantID != record.TenantID ||
		authority.ActorKind != record.ActorKind ||
		authority.ActorID != record.ActorID ||
		authority.Route != record.Route ||
		authority.ClientRequestID != record.ClientRequestID ||
		len(authority.RequestDigest) != sha256.Size ||
		subtle.ConstantTimeCompare(authority.RequestDigest, record.RequestDigest) != 1 ||
		authority.CatalogRevision != record.CommittedCatalogRevision ||
		len(authority.CatalogStateToken) != catalogStateTokenBytes ||
		subtle.ConstantTimeCompare(authority.CatalogStateToken, record.CommittedCatalogStateToken) != 1 ||
		authority.MutationKind != record.MutationKind ||
		authority.KnowledgeObjectID != record.KnowledgeObjectID ||
		authority.ObjectVersion != record.ObjectVersion ||
		authority.OccurredAtUnixMicro != record.CreatedAtUnixMicro ||
		authority.RetentionAnchorUnixMicro != record.RetentionAnchorUnixMicro ||
		authority.RetainUntilUnixMicro != record.RetainUntilUnixMicro ||
		!equalOptionalInt64(authority.SuccessfulAuditSequence, record.SuccessfulAuditSequence) ||
		!equalOptionalInt64(authority.RecoveryAuditSequence, record.RecoveryAuditSequence) {
		return fmt.Errorf("%w: knowledge replay commit authority disagrees", ErrCorrupt)
	}
	if _, err := canonicalTime(authority.OccurredAtUnixMicro); err != nil {
		return fmt.Errorf("%w: knowledge replay commit occurrence is invalid", ErrCorrupt)
	}
	return nil
}

type replayAuthority struct {
	current   registryRecord
	version   versionRecord
	reference *opensplunk.KnowledgeObjectVersionReference
}

func (writer *Writer) readReplayAuthority(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	record idempotencyRecord,
	expectedRoute string,
) (replayAuthority, error) {
	if err := validateContext(ctx); err != nil {
		return replayAuthority{}, err
	}
	if writer == nil || writer.reader == nil || tx == nil {
		return replayAuthority{}, fmt.Errorf("%w: knowledge replay store is unavailable", ErrCorrupt)
	}
	if err := validateIdempotencyRecord(&prepared, expectedRoute, record); err != nil {
		return replayAuthority{}, err
	}
	reference, err := decodeOutcomeReference(record)
	if err != nil {
		return replayAuthority{}, err
	}
	database := tx.WithContext(ctx)
	if database.Error != nil {
		return replayAuthority{}, database.Error
	}
	current, found, err := readAuthorizedReplayRegistry(database, prepared, reference.GetKnowledgeObjectId())
	if err != nil {
		return replayAuthority{}, err
	}
	if !found {
		return replayAuthority{}, control.ErrNotFound
	}
	// Quarantine is a terminal response-redaction override. It is evaluated
	// from both mutable current scalars and the latest retained immutable
	// version before any historical receipt body, blob, dependency, selector,
	// or projection is read.
	if err := validateAuthorizedReplayCurrent(database, prepared, current); err != nil {
		return replayAuthority{}, err
	}
	if err := validateReplayCommitAuthority(database, record); err != nil {
		return replayAuthority{}, err
	}

	version, found, err := readVersionRecord(
		database,
		record.TenantID,
		record.KnowledgeObjectID,
		record.ObjectVersion,
	)
	if err != nil {
		return replayAuthority{}, err
	}
	if !found || validateVersion(version) != nil ||
		version.TenantID != record.TenantID || version.KnowledgeObjectID != record.KnowledgeObjectID ||
		version.ObjectVersion != record.ObjectVersion || version.MutationKind != record.MutationKind ||
		version.CreatedAtUnixMicro != record.CreatedAtUnixMicro ||
		version.OwnerID != current.OwnerID || version.OwnerID != prepared.scope.ownerID ||
		version.ObjectVersion > current.CurrentVersion ||
		subtle.ConstantTimeCompare(version.DefinitionDigest, reference.GetDefinitionSha256()) != 1 {
		return replayAuthority{}, fmt.Errorf("%w: knowledge replay version authority disagrees", ErrCorrupt)
	}
	if err := writer.validateReplayRequestBinding(database, prepared, expectedRoute, version); err != nil {
		return replayAuthority{}, err
	}
	lifecycle, err := readVersionLifecycle(database, version)
	if err != nil {
		return replayAuthority{}, err
	}
	if err := validateVersionLifecycle(version, lifecycle); err != nil {
		return replayAuthority{}, err
	}
	if current.CurrentVersion == version.ObjectVersion {
		if err := validateCurrentVersion(current, version); err != nil {
			return replayAuthority{}, fmt.Errorf(
				"%w: current replay version authority disagrees",
				ErrCorrupt,
			)
		}
		if err := validateCurrentLifecycle(current, version, lifecycle); err != nil {
			return replayAuthority{}, err
		}
	}
	if err := validateReplayAuditLink(database, prepared, record, version); err != nil {
		return replayAuthority{}, err
	}
	return replayAuthority{current: current, version: version, reference: reference}, nil
}

// validateAuthorizedReplayCurrent binds the authorization-driving registry to
// the latest retained immutable version. A stored terminal quarantine is a
// permanent response-redaction authority even if a damaged registry was
// rolled back or had its state scalar rewritten. Only scalar/version/lifecycle
// authorities are read here; receipt envelopes and definition bodies remain
// unopened until this check succeeds.
func validateAuthorizedReplayCurrent(
	database *gorm.DB,
	prepared preparedMutation,
	current registryRecord,
) error {
	if database == nil || !validIdentity(current.TenantID, maximumTenantIDBytes) ||
		!validIdentity(current.KnowledgeObjectID, maximumObjectIDBytes) {
		return fmt.Errorf("%w: current replay authority is invalid", ErrCorrupt)
	}
	var latest []struct {
		ObjectVersion int64 `gorm:"column:object_version"`
	}
	if err := database.Model(&versionRecord{}).
		Select("object_version").
		Where("tenant_id = ? AND knowledge_object_id = ?", current.TenantID, current.KnowledgeObjectID).
		Order("object_version DESC").Limit(2).Find(&latest).Error; err != nil {
		return err
	}
	if len(latest) == 0 || latest[0].ObjectVersion < 1 ||
		latest[0].ObjectVersion > maximumVersionsPerTenant {
		return fmt.Errorf("%w: latest replay version is invalid", ErrCorrupt)
	}
	var authorized []struct {
		ObjectVersion int64 `gorm:"column:object_version"`
	}
	if err := database.Model(&versionRecord{}).
		Select("object_version").
		Where("tenant_id = ? AND knowledge_object_id = ? AND object_version = ?",
			current.TenantID, current.KnowledgeObjectID, latest[0].ObjectVersion).
		Where("owner_id = ?", prepared.scope.ownerID).
		Where("app_id IN ?", prepared.scope.writableAppIDs).
		Limit(2).Find(&authorized).Error; err != nil {
		return err
	}
	if len(authorized) != 1 || authorized[0].ObjectVersion != latest[0].ObjectVersion {
		return control.ErrNotFound
	}
	version, found, err := readVersionRecord(
		database,
		current.TenantID,
		current.KnowledgeObjectID,
		latest[0].ObjectVersion,
	)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: latest replay version is missing", ErrCorrupt)
	}
	// Either mutable or immutable quarantine authority is sufficient to keep
	// the outcome permanently redacted. This intentionally precedes broader
	// coherence diagnostics so corruption cannot turn redaction into an oracle.
	if current.State == StateQuarantined || version.State == StateQuarantined {
		return ErrIdempotentOutcomeRedacted
	}
	if err := validateRegistry(current); err != nil {
		return err
	}
	if validateVersion(version) != nil || version.ObjectVersion != current.CurrentVersion {
		return fmt.Errorf("%w: latest replay version disagrees with registry", ErrCorrupt)
	}
	if err := validateCurrentVersion(current, version); err != nil {
		return err
	}
	lifecycle, err := readVersionLifecycle(database, version)
	if err != nil {
		return err
	}
	if err := validateCurrentLifecycle(current, version, lifecycle); err != nil {
		return err
	}
	return nil
}

// validateReplayRequestBinding closes the part of the replay relationship that
// SQLite cannot derive from a SHA-256 request digest. A receipt must name the
// exact target and successor version authorized by the already-canonical
// request, and state mutations must agree with the requested transition.
func (writer *Writer) validateReplayRequestBinding(
	database *gorm.DB,
	prepared preparedMutation,
	route string,
	version versionRecord,
) error {
	if err := validateReplayScalarRequestBinding(prepared, route, version); err != nil {
		return err
	}
	invalid := func() error {
		return fmt.Errorf("%w: knowledge replay request and version disagree", ErrCorrupt)
	}

	switch route {
	case mutationRouteCreate:
		request := &opensplunk.CreateKnowledgeObjectRequest{}
		if decodeCanonicalPreparedRequest(prepared.requestBytes, request) != nil {
			return invalid()
		}
		normalized, err := normalizeMutationDefinition(request.GetDefinition())
		if err != nil {
			return invalid()
		}
		authority, err := authorityFromNormalized(normalized)
		if err != nil || replayDefinitionAuthorityAgrees(database, authority, version) != nil {
			return invalid()
		}
	case mutationRouteUpdate:
		request := &opensplunk.UpdateKnowledgeObjectRequest{}
		if decodeCanonicalPreparedRequest(prepared.requestBytes, request) != nil {
			return invalid()
		}
		paths, err := normalizeKnowledgeUpdateMask(request.GetUpdateMask())
		if err != nil || !slices.Equal(paths, prepared.updatePaths) {
			return invalid()
		}
		previous, found, err := readVersionRecord(
			database,
			version.TenantID,
			version.KnowledgeObjectID,
			version.ObjectVersion-1,
		)
		if err != nil || !found || validateVersion(previous) != nil ||
			previous.ObjectVersion+1 != version.ObjectVersion {
			return invalid()
		}
		previousAuthority, err := authorityFromStoredVersion(database, previous)
		if err != nil {
			return invalid()
		}
		patched, err := applyKnowledgeDefinitionMask(
			previousAuthority.definition,
			request.GetDefinition(),
			paths,
		)
		if err != nil {
			return invalid()
		}
		var authority definitionAuthority
		if previousAuthority.opaque {
			for _, path := range paths {
				if path == "field_extraction" || path == "field_alias" || path == "calculated_field" {
					return invalid()
				}
			}
			authority, err = authorityFromOpaqueMetadataUpdate(patched, previous.State, previous.ObjectType)
		} else {
			var normalized knowledgedefinition.Normalized
			normalized, err = normalizeMutationDefinition(patched)
			if err == nil {
				authority, err = authorityFromNormalized(normalized)
			}
		}
		if err != nil || authority.objectType != previous.ObjectType ||
			bytes.Equal(authority.bytes, previousAuthority.bytes) {
			return invalid()
		}
		mutationKind := "update"
		if authority.appID != previous.AppID || authority.sharingScope != previous.SharingScope {
			mutationKind = "scope_change"
		}
		if version.MutationKind != mutationKind ||
			replayDefinitionAuthorityAgrees(database, authority, version) != nil {
			return invalid()
		}
	case mutationRouteSetState, mutationRouteDelete:
		// State-only request authority is fully scalar. Migration 0030 binds the
		// immutable body and exact ordered dependency set to the prior version.
		return nil
	}
	return nil
}

// validateReplayScalarRequestBinding is the unit-testable scalar layer. The Writer
// method above additionally reconstructs Create and Update definition bytes.
func validateReplayScalarRequestBinding(
	prepared preparedMutation,
	route string,
	version versionRecord,
) error {
	invalid := func() error {
		return fmt.Errorf("%w: knowledge replay request and version disagree", ErrCorrupt)
	}
	successorMatches := func(objectID string, expectedVersion uint64) bool {
		return objectID == version.KnowledgeObjectID &&
			expectedVersion > 0 && expectedVersion < math.MaxInt64 &&
			int64(expectedVersion)+1 == version.ObjectVersion
	}
	switch route {
	case mutationRouteCreate:
		request := &opensplunk.CreateKnowledgeObjectRequest{}
		if decodeCanonicalPreparedRequest(prepared.requestBytes, request) != nil ||
			request.GetClientRequestId() != "" || version.ObjectVersion != 1 ||
			version.MutationKind != "create" {
			return invalid()
		}
		switch request.GetInitialState() {
		case opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT:
			if version.State != StateDraft {
				return invalid()
			}
		case opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE:
			if version.State != StateActive {
				return invalid()
			}
		default:
			return invalid()
		}
	case mutationRouteUpdate:
		request := &opensplunk.UpdateKnowledgeObjectRequest{}
		if decodeCanonicalPreparedRequest(prepared.requestBytes, request) != nil ||
			request.GetClientRequestId() != "" ||
			!successorMatches(request.GetKnowledgeObjectId(), request.GetExpectedVersion()) ||
			(version.MutationKind != "update" && version.MutationKind != "scope_change") {
			return invalid()
		}
	case mutationRouteSetState:
		request := &opensplunk.SetKnowledgeObjectStateRequest{}
		if decodeCanonicalPreparedRequest(prepared.requestBytes, request) != nil ||
			request.GetClientRequestId() != "" ||
			!successorMatches(request.GetKnowledgeObjectId(), request.GetExpectedVersion()) {
			return invalid()
		}
		switch request.GetState() {
		case opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE:
			if version.MutationKind != "enable" || version.State != StateActive {
				return invalid()
			}
		case opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED:
			if version.MutationKind != "disable" || version.State != StateDisabled {
				return invalid()
			}
		default:
			return invalid()
		}
	case mutationRouteDelete:
		request := &opensplunk.DeleteKnowledgeObjectRequest{}
		if decodeCanonicalPreparedRequest(prepared.requestBytes, request) != nil ||
			request.GetClientRequestId() != "" ||
			!successorMatches(request.GetKnowledgeObjectId(), request.GetExpectedVersion()) ||
			version.MutationKind != "delete" || version.State != StateDeleted {
			return invalid()
		}
	default:
		return invalid()
	}
	return nil
}

func replayDefinitionAuthorityAgrees(
	database *gorm.DB,
	authority definitionAuthority,
	version versionRecord,
) error {
	if database == nil || authority.definition == nil ||
		len(authority.bytes) < 1 || len(authority.bytes) > knowledgedefinition.MaximumCanonicalBytes ||
		len(authority.digest) != persistedKnowledgeDefinitionDigestBytes ||
		authority.objectType != version.ObjectType || authority.appID != version.AppID ||
		authority.name != version.Name || authority.sharingScope != version.SharingScope ||
		subtle.ConstantTimeCompare(authority.digest, version.DefinitionDigest) != 1 {
		return fmt.Errorf("%w: replayed definition authority disagrees", ErrCorrupt)
	}
	blob, err := readDefinitionBlob(database, version.TenantID, version.DefinitionDigest)
	if err != nil {
		return err
	}
	if blob.DefinitionBytes != int64(len(authority.bytes)) ||
		!bytes.Equal(blob.DefinitionProto, authority.bytes) ||
		subtle.ConstantTimeCompare(blob.DefinitionDigest, authority.digest) != 1 {
		return fmt.Errorf("%w: replayed definition blob disagrees", ErrCorrupt)
	}
	return nil
}

func decodeCanonicalPreparedRequest(encoded []byte, message proto.Message) error {
	if message == nil || len(encoded) < 1 || len(encoded) > maximumMutationRequestBytes {
		return fmt.Errorf("%w: canonical replay request is absent", ErrCorrupt)
	}
	if err := (proto.UnmarshalOptions{
		DiscardUnknown: false,
		RecursionLimit: 64,
	}).Unmarshal(encoded, message); err != nil {
		return fmt.Errorf("%w: canonical replay request cannot be decoded", ErrCorrupt)
	}
	if err := rejectMutationUnknownFields(message.ProtoReflect(), 0); err != nil {
		return fmt.Errorf("%w: canonical replay request contains unknown fields", ErrCorrupt)
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return fmt.Errorf("%w: canonical replay request encoding is invalid", ErrCorrupt)
	}
	return nil
}

func readAuthorizedReplayRegistry(
	database *gorm.DB,
	prepared preparedMutation,
	objectID string,
) (registryRecord, bool, error) {
	query := database.Model(&registryRecord{}).
		Where("tenant_id = ? AND knowledge_object_id = ?", prepared.scope.tenantID, objectID).
		Where("owner_id = ?", prepared.scope.ownerID).
		Where("app_id IN ?", prepared.scope.writableAppIDs)
	return readRegistryRecord(query)
}

type replayAuditRecord struct {
	TenantID            string                       `gorm:"column:tenant_id"`
	Sequence            int64                        `gorm:"column:sequence"`
	OccurredAtUnixMicro int64                        `gorm:"column:occurred_at_unix_micro"`
	ActorKind           audit.ActorKind              `gorm:"column:actor_kind"`
	ActorID             string                       `gorm:"column:actor_id"`
	ActorRole           audit.ActorRole              `gorm:"column:actor_role"`
	Action              audit.Action                 `gorm:"column:action"`
	TargetKind          audit.TargetKind             `gorm:"column:target_kind"`
	TargetID            string                       `gorm:"column:target_id"`
	TargetVersion       int64                        `gorm:"column:target_version"`
	AppID               *string                      `gorm:"column:app_id"`
	ObjectType          *audit.KnowledgeObjectType   `gorm:"column:object_type"`
	SharingScope        *audit.KnowledgeSharingScope `gorm:"column:sharing_scope"`
}

type replayAuditWidthRecord struct {
	TenantIDBytes     int64 `gorm:"column:tenant_id_bytes"`
	ActorKindBytes    int64 `gorm:"column:actor_kind_bytes"`
	ActorIDBytes      int64 `gorm:"column:actor_id_bytes"`
	ActorRoleBytes    int64 `gorm:"column:actor_role_bytes"`
	ActionBytes       int64 `gorm:"column:action_bytes"`
	TargetKindBytes   int64 `gorm:"column:target_kind_bytes"`
	TargetIDBytes     int64 `gorm:"column:target_id_bytes"`
	AppIDBytes        int64 `gorm:"column:app_id_bytes"`
	ObjectTypeBytes   int64 `gorm:"column:object_type_bytes"`
	SharingScopeBytes int64 `gorm:"column:sharing_scope_bytes"`
}

func validateReplayAuditLink(
	database *gorm.DB,
	prepared preparedMutation,
	record idempotencyRecord,
	version versionRecord,
) error {
	if record.SuccessfulAuditSequence == nil {
		return fmt.Errorf("%w: knowledge replay audit link is absent", ErrCorrupt)
	}
	query := database.Table("audit_events").Where(
		"tenant_id = ? AND sequence = ?",
		record.TenantID,
		*record.SuccessfulAuditSequence,
	)
	var widths []replayAuditWidthRecord
	if err := query.Session(&gorm.Session{}).Select(`
		length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
		length(CAST(actor_kind AS BLOB)) AS actor_kind_bytes,
		length(CAST(actor_id AS BLOB)) AS actor_id_bytes,
		length(CAST(actor_role AS BLOB)) AS actor_role_bytes,
		length(CAST(action AS BLOB)) AS action_bytes,
		length(CAST(target_kind AS BLOB)) AS target_kind_bytes,
		length(CAST(target_id AS BLOB)) AS target_id_bytes,
		coalesce(length(CAST(app_id AS BLOB)), 0) AS app_id_bytes,
		coalesce(length(CAST(object_type AS BLOB)), 0) AS object_type_bytes,
		coalesce(length(CAST(sharing_scope AS BLOB)), 0) AS sharing_scope_bytes
	`).Limit(2).Find(&widths).Error; err != nil {
		return err
	}
	if len(widths) != 1 || !validReplayAuditWidth(widths[0]) {
		return fmt.Errorf("%w: knowledge replay audit width is invalid", ErrCorrupt)
	}
	var events []replayAuditRecord
	if err := query.Session(&gorm.Session{}).Limit(2).Find(&events).Error; err != nil {
		return err
	}
	if len(events) != 1 {
		return fmt.Errorf("%w: knowledge replay audit changed after preflight", ErrCorrupt)
	}
	event := events[0]
	expectedAction, ok := auditActionForMutationKind(record.MutationKind)
	if !ok || event.TenantID != record.TenantID || event.Sequence != *record.SuccessfulAuditSequence ||
		event.OccurredAtUnixMicro != record.CreatedAtUnixMicro ||
		event.ActorKind != prepared.actor.Kind || event.ActorID != prepared.actor.ID ||
		event.ActorRole != prepared.actor.Role || event.Action != expectedAction ||
		event.TargetKind != audit.TargetKindKnowledgeObject ||
		event.TargetID != version.KnowledgeObjectID || event.TargetVersion != version.ObjectVersion ||
		event.AppID == nil || *event.AppID != version.AppID ||
		event.ObjectType == nil || *event.ObjectType != version.ObjectType ||
		event.SharingScope == nil || *event.SharingScope != version.SharingScope {
		return fmt.Errorf("%w: knowledge replay audit authority disagrees", ErrCorrupt)
	}
	if _, err := canonicalTime(event.OccurredAtUnixMicro); err != nil {
		return fmt.Errorf("%w: knowledge replay audit timestamp is invalid", ErrCorrupt)
	}
	return nil
}

func validReplayAuditWidth(width replayAuditWidthRecord) bool {
	return width.TenantIDBytes >= 1 && width.TenantIDBytes <= maximumTenantIDBytes &&
		width.ActorKindBytes >= int64(len(audit.ActorKindSystem)) &&
		width.ActorKindBytes <= int64(len(audit.ActorKindBrowser)) &&
		width.ActorIDBytes >= 1 && width.ActorIDBytes <= maximumOwnerIDBytes &&
		width.ActorRoleBytes >= int64(len(audit.ActorRoleUser)) &&
		width.ActorRoleBytes <= int64(len(audit.ActorRoleAdministrator)) &&
		width.ActionBytes >= 1 && width.ActionBytes <= int64(len(audit.ActionKnowledgeObjectScopeChange)) &&
		width.TargetKindBytes == int64(len(audit.TargetKindKnowledgeObject)) &&
		width.TargetIDBytes >= 1 && width.TargetIDBytes <= maximumObjectIDBytes &&
		width.AppIDBytes >= 1 && width.AppIDBytes <= maximumAppIDBytes &&
		width.ObjectTypeBytes >= 1 && width.ObjectTypeBytes <= int64(len(ObjectTypeFieldExtraction)) &&
		width.SharingScopeBytes >= int64(len(SharingScopeApp)) &&
		width.SharingScopeBytes <= int64(len(SharingScopePrivate))
}

func auditActionForMutationKind(mutationKind string) (audit.Action, bool) {
	switch mutationKind {
	case "create":
		return audit.ActionKnowledgeObjectCreate, true
	case "update":
		return audit.ActionKnowledgeObjectUpdate, true
	case "scope_change":
		return audit.ActionKnowledgeObjectScopeChange, true
	case "enable":
		return audit.ActionKnowledgeObjectEnable, true
	case "disable":
		return audit.ActionKnowledgeObjectDisable, true
	case "delete":
		return audit.ActionKnowledgeObjectDelete, true
	default:
		return "", false
	}
}
