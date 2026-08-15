package knowledgecatalog

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"gorm.io/gorm"
)

const (
	minimumIdempotencyRetention  = 7 * 24 * time.Hour
	maximumIdempotencyRetention  = 365 * 24 * time.Hour
	maximumWriterIDAttempts      = 8
	normalIdempotencyCapacity    = 16_384
	absoluteIdempotencyCapacity  = 20_480
	maximumIdempotencyReclaim    = absoluteIdempotencyCapacity - normalIdempotencyCapacity + 1
	maximumRecoveryAuditRecords  = 8_192
	writerCommitReconcileTimeout = 500 * time.Millisecond
)

var (
	// ErrIdempotencyConflict means a client request identity was already used
	// for different submitted mutation authority. It remains distinguishable
	// for rejected-attempt auditing while mapping to the ordinary conflict
	// family at transport boundaries.
	ErrIdempotencyConflict = fmt.Errorf(
		"%w: knowledge mutation idempotency identity was reused",
		control.ErrAlreadyExists,
	)
	// ErrActivePublicationUnavailable keeps ACTIVE definitions fail-closed at a
	// management boundary that lacks the concrete receipt-first Writer and its
	// same-transaction publication authority.
	ErrActivePublicationUnavailable = errors.New(
		"knowledgecatalog: active publication is unavailable",
	)
	// ErrIdempotentOutcomeRedacted preserves quarantine's permanent body
	// nondisclosure rule without re-executing an already committed mutation.
	ErrIdempotentOutcomeRedacted = errors.New(
		"knowledgecatalog: committed idempotent outcome is redacted",
	)
)

// WriteScope is trusted caller-derived mutation authority. TenantID, OwnerID,
// and WritableAppIDs must come from the authenticated server context, never a
// request body. The initial single-user policy still validates every app and
// current object identity before definition bytes are decoded.
type WriteScope struct {
	TenantID       string
	OwnerID        string
	WritableAppIDs []string
}

// WriterOptions configures deterministic dependencies used by the mutation
// writer. Zero values select cryptographic IDs, the wall clock, and the
// normative seven-day idempotency fence.
type WriterOptions struct {
	Clock                func() time.Time
	IDGenerator          func() (string, error)
	IdempotencyRetention time.Duration
}

// Writer publishes immutable catalog versions. It is separate from Store so
// read-only callers never need mutation audit dependencies.
type Writer struct {
	orm                  *gorm.DB
	reader               *Store
	validationGate       *control.AdmissionGate
	auditAppender        audit.TransactionAppender
	clock                func() time.Time
	idGenerator          func() (string, error)
	idempotencyRetention time.Duration
	hook                 writerHook
	commit               writerCommit
}

// ReadyForManagement reports whether writer retains the complete
// constructor-established authority required by public mutation routes. It is
// side-effect free and does not probe the continued availability of storage.
func (writer *Writer) ReadyForManagement() bool {
	return writer != nil &&
		writer.orm != nil &&
		writer.reader != nil &&
		writer.reader.orm == writer.orm &&
		writer.validationGate != nil &&
		!nilcheck.IsNil(writer.auditAppender) &&
		writer.clock != nil &&
		writer.idGenerator != nil &&
		writer.idempotencyRetention >= minimumIdempotencyRetention &&
		writer.idempotencyRetention <= maximumIdempotencyRetention &&
		writer.idempotencyRetention%time.Microsecond == 0
}

// NewWriter constructs a mutation writer without changing schema. Successful
// audit append is mandatory because catalog publication and its journal event
// are one atomic control-plane authority.
func NewWriter(
	database *control.DB,
	auditAppender audit.TransactionAppender,
	options WriterOptions,
) (*Writer, error) {
	if database == nil || database.GORMDB() == nil {
		return nil, fmt.Errorf(
			"%w: knowledge writer control database is required",
			control.ErrInvalidArgument,
		)
	}
	if nilcheck.IsNil(auditAppender) {
		return nil, fmt.Errorf(
			"%w: knowledge writer audit appender is required",
			control.ErrInvalidArgument,
		)
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	idGenerator := options.IDGenerator
	if idGenerator == nil {
		idGenerator = newKnowledgeObjectID
	}
	retention := options.IdempotencyRetention
	if retention == 0 {
		retention = minimumIdempotencyRetention
	}
	if retention < minimumIdempotencyRetention || retention > maximumIdempotencyRetention {
		return nil, fmt.Errorf(
			"%w: knowledge idempotency retention must be between %s and %s",
			control.ErrInvalidArgument,
			minimumIdempotencyRetention,
			maximumIdempotencyRetention,
		)
	}
	// SQLite authorities are canonical microseconds. Round upward so the
	// persisted replay fence is never shorter than a caller's configured retry
	// window; the validated maximum is itself microsecond-aligned.
	retention = ((retention + time.Microsecond - 1) / time.Microsecond) * time.Microsecond
	validationGate, err := database.SharedAdmissionGate(
		"knowledge-catalog-validation",
		MaximumConcurrentValidations,
	)
	if err != nil {
		return nil, fmt.Errorf("construct knowledge validation admission: %w", err)
	}
	return &Writer{
		orm:                  database.GORMDB(),
		reader:               &Store{orm: database.GORMDB()},
		validationGate:       validationGate,
		auditAppender:        auditAppender,
		clock:                clock,
		idGenerator:          idGenerator,
		idempotencyRetention: retention,
	}, nil
}

// Create commits immutable version one or returns a previously committed
// outcome for an exact idempotent retry.
func (writer *Writer) Create(
	ctx context.Context,
	scope WriteScope,
	request *opensplunkv1.CreateKnowledgeObjectRequest,
) (result *opensplunkv1.CreateKnowledgeObjectResponse, returnedErr error) {
	result, returnedErr = writer.create(ctx, scope, request)
	returnedErr = withDefaultErrorDisposition(
		returnedErr,
		ErrorDispositionDefinitiveRejection,
	)
	return result, returnedErr
}

// Update applies one exact, top-level KnowledgeObjectDefinition field mask
// under optimistic concurrency and appends one immutable version.
func (writer *Writer) Update(
	ctx context.Context,
	scope WriteScope,
	request *opensplunkv1.UpdateKnowledgeObjectRequest,
) (result *opensplunkv1.UpdateKnowledgeObjectResponse, returnedErr error) {
	result, returnedErr = writer.update(ctx, scope, request)
	returnedErr = withDefaultErrorDisposition(
		returnedErr,
		ErrorDispositionDefinitiveRejection,
	)
	return result, returnedErr
}

// SetState enables or disables an object under optimistic concurrency.
func (writer *Writer) SetState(
	ctx context.Context,
	scope WriteScope,
	request *opensplunkv1.SetKnowledgeObjectStateRequest,
) (result *opensplunkv1.SetKnowledgeObjectStateResponse, returnedErr error) {
	result, returnedErr = writer.setState(ctx, scope, request)
	returnedErr = withDefaultErrorDisposition(
		returnedErr,
		ErrorDispositionDefinitiveRejection,
	)
	return result, returnedErr
}

// Delete appends a terminal immutable tombstone; it never removes retained
// definition or version authorities.
func (writer *Writer) Delete(
	ctx context.Context,
	scope WriteScope,
	request *opensplunkv1.DeleteKnowledgeObjectRequest,
) (result *opensplunkv1.DeleteKnowledgeObjectResponse, returnedErr error) {
	result, returnedErr = writer.delete(ctx, scope, request)
	returnedErr = withDefaultErrorDisposition(
		returnedErr,
		ErrorDispositionDefinitiveRejection,
	)
	return result, returnedErr
}

type normalizedWriteScope struct {
	tenantID       string
	ownerID        string
	writableAppIDs []string
}

func normalizeWriteScope(scope WriteScope) (normalizedWriteScope, error) {
	if !validIdentity(scope.TenantID, maximumTenantIDBytes) ||
		!validIdentity(scope.OwnerID, maximumOwnerIDBytes) ||
		len(scope.WritableAppIDs) == 0 || len(scope.WritableAppIDs) > maximumReadableApps {
		return normalizedWriteScope{}, fmt.Errorf(
			"%w: knowledge mutation scope is invalid",
			control.ErrInvalidArgument,
		)
	}
	apps := slices.Clone(scope.WritableAppIDs)
	for _, appID := range apps {
		if !control.ValidCanonicalAppID(appID) {
			return normalizedWriteScope{}, fmt.Errorf(
				"%w: knowledge writable app identity is invalid",
				control.ErrInvalidArgument,
			)
		}
	}
	slices.Sort(apps)
	apps = slices.Compact(apps)
	return normalizedWriteScope{
		tenantID:       scope.TenantID,
		ownerID:        scope.OwnerID,
		writableAppIDs: apps,
	}, nil
}

// ValidateWriteScope verifies trusted caller-derived mutation authority using
// the same normalization contract as Writer. It does not mutate scope or its
// WritableAppIDs backing array.
func ValidateWriteScope(scope WriteScope) error {
	_, err := normalizeWriteScope(scope)
	return err
}

func (scope normalizedWriteScope) canWriteApp(appID string) bool {
	_, found := slices.BinarySearch(scope.writableAppIDs, appID)
	return found
}

func requireMutationActor(ctx context.Context) (audit.Actor, error) {
	actor, found := audit.ActorFromContext(ctx)
	if !found || actor.Kind == audit.ActorKindBrowser && actor.Role != audit.ActorRoleAdministrator ||
		actor.Kind == audit.ActorKindSystem && actor.Role != audit.ActorRoleSystem {
		return audit.Actor{}, fmt.Errorf(
			"%w: knowledge mutation requires an explicit administrative actor",
			control.ErrInvalidArgument,
		)
	}
	return actor, nil
}

func newKnowledgeObjectID() (string, error) {
	random := make([]byte, 18)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return "ko_" + base64.RawURLEncoding.EncodeToString(random), nil
}

type writerHookBoundary string

const (
	writerHookPrepared                   writerHookBoundary = "prepared"
	writerHookIdempotencyChecked         writerHookBoundary = "idempotency_checked"
	writerHookCatalogLedgersReady        writerHookBoundary = "catalog_ledgers_ready"
	writerHookCapacityChecked            writerHookBoundary = "capacity_checked"
	writerHookDefinitionBlobReady        writerHookBoundary = "definition_blob_ready"
	writerHookVersionInserted            writerHookBoundary = "version_inserted"
	writerHookDependencyRowsInserted     writerHookBoundary = "dependency_rows_inserted"
	writerHookDependencySealed           writerHookBoundary = "dependency_sealed"
	writerHookProjectionInserted         writerHookBoundary = "projection_inserted"
	writerHookSelectorRowsInserted       writerHookBoundary = "selector_rows_inserted"
	writerHookProjectionSealed           writerHookBoundary = "projection_sealed"
	writerHookRegistryPublished          writerHookBoundary = "registry_published"
	writerHookSuccessAuditAppended       writerHookBoundary = "success_audit_appended"
	writerHookCatalogRevisionAdvanced    writerHookBoundary = "catalog_revision_advanced"
	writerHookCommitAuthorityRecorded    writerHookBoundary = "commit_authority_recorded"
	writerHookIdempotencyOutcomeRecorded writerHookBoundary = "idempotency_outcome_recorded"
	writerHookBeforeCommit               writerHookBoundary = "before_commit"
	writerHookAfterCommit                writerHookBoundary = "after_commit"
)

type writerHookEvent struct {
	Boundary          writerHookBoundary
	Route             string
	KnowledgeObjectID string
	Version           uint64
	IDAttempt         int
}

type writerHook func(context.Context, writerHookEvent) error

type writerCommit func(*gorm.DB) error

func (writer *Writer) callHook(ctx context.Context, event writerHookEvent) error {
	if writer == nil || writer.hook == nil {
		return nil
	}
	return writer.hook(ctx, event)
}
