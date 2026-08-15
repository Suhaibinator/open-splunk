package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexpolicy"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/tokenconstraint"
	"gorm.io/gorm"
)

const (
	collectorTokenPrefix               = "ost_v1_"
	tokenRandomBytes                   = 32
	tokenIDRandomBytes                 = 16
	minimumDigestKeyBytes              = 32
	maximumTokenScopes                 = 256
	maximumTokenIDBytes                = 128
	maximumTokenNameBytes              = 255
	maximumDescriptionBytes            = 8 << 10
	minimumTokenPrefixBytes            = 8
	maximumTokenPrefixBytes            = 32
	maximumCollectorIDBytes            = 128
	maximumHECMetadataBytes            = 255
	defaultRetainedRevokedTokenLimit   = 256
	defaultTotalTokenRecordLimit       = 1024
	maximumTotalTokenRecordLimit       = 1024
	maximumRetainedRevokedTokenLimit   = maximumTotalTokenRecordLimit - 1
	maximumTotalTokenScopeRecordLimit  = 16_384
	maximumTokenConstraintPatterns     = tokenconstraint.MaximumPatternsPerDimension
	maximumTokenConstraintPatternBytes = tokenconstraint.MaximumPatternBytes
	maximumTokenConstraintRecords      = 2 * maximumTokenConstraintPatterns
	maximumTotalTokenConstraintRecords = maximumTotalTokenRecordLimit *
		maximumTokenConstraintRecords
	redactedValue = "[REDACTED]"
)

var (
	// ErrInvalidDigestKey means the configured token-digest key is too short
	// to provide the intended security margin.
	ErrInvalidDigestKey = errors.New("auth: collector token digest key must contain at least 32 bytes")
	// ErrUnauthorized intentionally combines invalid credentials, inactive
	// credentials, expired credentials, and forbidden indexes into one safe
	// externally reportable error.
	ErrUnauthorized = errors.New("auth: collector authentication or index authorization failed")
	// ErrNoActiveIndexAuthority is returned with a verified collector identity
	// only by lease revalidation when none of its bounded index scopes is
	// currently ingestion-enabled. Callers may use that identity solely to
	// recover an exact durable batch outcome before rejecting a fresh batch.
	ErrNoActiveIndexAuthority = errors.New("auth: collector has no active index authority")
	// ErrInvalidIndexAuthority is returned with a verified collector identity
	// only by lease revalidation when its bounded index projection is corrupt.
	// It is never permission to ingest with a partial or stale scope.
	ErrInvalidIndexAuthority = errors.New("auth: collector index authority is invalid")
	// ErrInvalidEventAuthority is returned with a verified collector identity
	// only by lease revalidation when its host/source constraint projection is
	// corrupt. Callers may use the identity solely to recover an exact durable
	// batch outcome before rejecting every fresh event.
	ErrInvalidEventAuthority = errors.New("auth: collector event authority is invalid")
	// ErrInactiveToken means an operation that requires an active ingestion
	// credential could not proceed. Accepted-use recording deliberately uses
	// this one sentinel for missing, disabled, revoked, and expired IDs so the
	// stream-admission path does not disclose credential existence or state.
	// HEC authentication also returns it for a digest-matching, structurally
	// valid, unexpired HEC token whose explicit state is disabled; the HEC
	// compatibility contract exposes that one closed protocol distinction.
	ErrInactiveToken = errors.New("auth: collector token is inactive")
	// ErrAuditActorUnavailable means a security-sensitive administrative
	// mutation reached the production store without the trusted actor that the
	// authenticated route must install. It is a server wiring failure, not a
	// malformed client request.
	ErrAuditActorUnavailable = errors.New("auth: audit actor is unavailable")

	errCollectorTokenScopeUnavailable = errors.New("collector token scope is unavailable")
	errCollectorTokenCatalogOverflow  = errors.New(
		"auth: collector token catalog exceeds its structural record limit",
	)
	errCollectorTokenCatalogInconsistent = errors.New(
		"auth: collector token catalog hydration is inconsistent",
	)
)

// CollectorTokenState is the administrative/effective state of a token.
type CollectorTokenState string

const (
	CollectorTokenStateActive   CollectorTokenState = "active"
	CollectorTokenStateDisabled CollectorTokenState = "disabled"
	CollectorTokenStateRevoked  CollectorTokenState = "revoked"
	CollectorTokenStateExpired  CollectorTokenState = "expired"
)

// IngestionTokenPurpose is the immutable transport boundary for an ingestion
// credential. The empty value is accepted only by legacy internal native-token
// callers and is persisted canonically as NativeCollector.
type IngestionTokenPurpose string

const (
	IngestionTokenPurposeNativeCollector IngestionTokenPurpose = "native_collector"
	IngestionTokenPurposeHEC             IngestionTokenPurpose = "hec"
)

// HECTokenProfile contains HEC-only request metadata defaults. Empty strings
// mean no default is configured. IndexerAcknowledgment is immutable after
// creation while the other fields may change under token optimistic locking.
type HECTokenProfile struct {
	DefaultIndexName      string
	DefaultHost           string
	DefaultSource         string
	DefaultSourcetype     string
	IndexerAcknowledgment bool
}

// CollectorToken contains safe token metadata. It never contains a secret or
// digest.
type CollectorToken struct {
	ID                   string
	Version              uint64
	Name                 string
	Description          string
	Prefix               string
	State                CollectorTokenState
	Purpose              IngestionTokenPurpose
	HECProfile           HECTokenProfile
	BoundCollectorID     string
	AllowedIndexNames    []string
	AllowedHostRegexes   []string
	AllowedSourceRegexes []string
	IngestionRateLimits  ingestquota.Limits
	CreatedAt            time.Time
	UpdatedAt            time.Time
	LastUsedAt           time.Time
	ExpiresAt            time.Time
	RevokedAt            time.Time
}

// Secret is an opaque newly-issued collector credential. Plaintext is
// deliberately available only through the explicitly named Plaintext method;
// ordinary formatting and JSON serialization are redacted.
type Secret struct {
	plaintext string
}

// Plaintext returns the credential for its one-time presentation to the
// operator. Callers must not log or persist this value.
func (secret Secret) Plaintext() string { return secret.plaintext }

func (Secret) String() string   { return redactedValue }
func (Secret) GoString() string { return redactedValue }

// MarshalJSON prevents generic API/log serializers from disclosing a secret.
func (Secret) MarshalJSON() ([]byte, error) { return json.Marshal(redactedValue) }

// IssuedCollectorToken is returned only at creation time.
type IssuedCollectorToken struct {
	Token  CollectorToken
	Secret Secret
}

func (issued IssuedCollectorToken) String() string {
	return fmt.Sprintf("IssuedCollectorToken{TokenID:%q Secret:%s}", issued.Token.ID, redactedValue)
}

func (issued IssuedCollectorToken) GoString() string { return issued.String() }

// CreateCollectorTokenRequest describes a collector token and its explicit
// ingestion-index scope.
type CreateCollectorTokenRequest struct {
	Name                 string
	Description          string
	AllowedIndexNames    []string
	AllowedHostRegexes   []string
	AllowedSourceRegexes []string
	Purpose              IngestionTokenPurpose
	HECProfile           HECTokenProfile
	BoundCollectorID     string
	ExpiresAt            time.Time
	IngestionRateLimits  ingestquota.Limits
}

// UpdateCollectorTokenRequest replaces the mutable definition of an existing
// collector token. The credential digest and safe prefix are immutable.
type UpdateCollectorTokenRequest struct {
	Name                 string
	Description          string
	AllowedIndexNames    []string
	AllowedHostRegexes   []string
	AllowedSourceRegexes []string
	Purpose              IngestionTokenPurpose
	HECProfile           HECTokenProfile
	BoundCollectorID     string
	ExpiresAt            time.Time
	IngestionRateLimits  ingestquota.Limits
}

// Principal is the safe result of a collector authorization check.
type Principal struct {
	TokenID          string
	TokenName        string
	IndexName        string
	BoundCollectorID string
}

// AuthorizedIndexPolicy is one current, active, ingestion-enabled index scope
// resolved in the same control-plane snapshot as its collector credential.
// Version identifies the exact mutable index generation represented by the
// remaining policy fields.
type AuthorizedIndexPolicy = indexpolicy.Policy

// Authentication is a safe credential resolution snapshot. AuthorizedIndexes
// is the single authoritative index scope and policy projection. It must be
// refreshed at each security boundary where index or credential changes need
// to take effect.
type Authentication struct {
	TokenID              string
	TokenVersion         uint64
	TokenName            string
	Purpose              IngestionTokenPurpose
	HECProfile           HECTokenProfile
	BoundCollectorID     string
	TokenRateLimits      ingestquota.Limits
	AuthorizedIndexes    []AuthorizedIndexPolicy
	AllowedHostRegexes   []string
	AllowedSourceRegexes []string
}

// AuthorizedIndexNames returns a detached name projection in authoritative
// snapshot order for APIs whose durable format intentionally records only
// scope names.
func (authentication Authentication) AuthorizedIndexNames() []string {
	names := make([]string, len(authentication.AuthorizedIndexes))
	for index, policy := range authentication.AuthorizedIndexes {
		names[index] = policy.Name
	}
	return names
}

// Store owns collector credential creation, persistence, revocation, and
// per-index authorization.
type Store struct {
	orm                       *gorm.DB
	digestKey                 []byte
	random                    io.Reader
	now                       func() time.Time
	retainedRevokedTokenLimit int
	totalTokenRecordLimit     int
	auditAppender             audit.TransactionAppender
	auditTenantID             string
	requireExplicitAuditActor bool
}

// StoreOptions configures bounded collector-token lifecycle behavior. Zero
// values select production defaults.
type StoreOptions struct {
	RetainedRevokedTokenLimit int
	TotalTokenRecordLimit     int
	// AuditAppender records successful administrative token mutations inside
	// the same SQLite transaction. Nil selects the control database's audit
	// store. A typed nil is rejected.
	AuditAppender audit.TransactionAppender
	// AuditTenantID scopes every event emitted by this token store. Empty
	// selects the single-tenant default used by direct/internal callers.
	AuditTenantID string
	// RequireExplicitAuditActor makes mutation calls fail before generating a
	// credential or opening a write transaction unless a trusted actor was
	// installed in the context. Production enables this option.
	RequireExplicitAuditActor bool
}

// NewStore constructs a collector-token store. digestKey is copied so caller
// mutation cannot silently invalidate every credential.
func NewStore(db *control.DB, digestKey []byte) (*Store, error) {
	return NewStoreWithOptions(db, digestKey, StoreOptions{})
}

// NewStoreWithOptions constructs a collector-token store with explicit
// lifecycle bounds. Ordinary revocation retains the current tombstone, while a
// later successful create may reclaim every revoked row needed for admission.
func NewStoreWithOptions(db *control.DB, digestKey []byte, options StoreOptions) (*Store, error) {
	if db == nil || db.GORMDB() == nil {
		return nil, fmt.Errorf("%w: control-plane database is required", control.ErrInvalidArgument)
	}
	if len(digestKey) < minimumDigestKeyBytes {
		return nil, ErrInvalidDigestKey
	}
	retainedRevokedTokenLimit := options.RetainedRevokedTokenLimit
	if retainedRevokedTokenLimit == 0 {
		retainedRevokedTokenLimit = defaultRetainedRevokedTokenLimit
	}
	if retainedRevokedTokenLimit < 1 ||
		retainedRevokedTokenLimit > maximumRetainedRevokedTokenLimit {
		return nil, fmt.Errorf(
			"%w: retained revoked token limit must be between 1 and %d",
			control.ErrInvalidArgument,
			maximumRetainedRevokedTokenLimit,
		)
	}
	totalTokenRecordLimit := options.TotalTokenRecordLimit
	if totalTokenRecordLimit == 0 {
		totalTokenRecordLimit = defaultTotalTokenRecordLimit
	}
	if totalTokenRecordLimit < 1 ||
		totalTokenRecordLimit > maximumTotalTokenRecordLimit {
		return nil, fmt.Errorf(
			"%w: total token record limit must be between 1 and %d",
			control.ErrInvalidArgument,
			maximumTotalTokenRecordLimit,
		)
	}
	if retainedRevokedTokenLimit >= totalTokenRecordLimit {
		return nil, fmt.Errorf(
			"%w: retained revoked token limit must be less than total token record limit",
			control.ErrInvalidArgument,
		)
	}
	auditTenantID := options.AuditTenantID
	if auditTenantID == "" {
		auditTenantID = "default"
	}
	if err := audit.ValidateTenantID(auditTenantID); err != nil {
		return nil, fmt.Errorf("configure collector-token audit tenant: %w", err)
	}
	auditAppender := options.AuditAppender
	if auditAppender == nil {
		var err error
		auditAppender, err = audit.NewStore(db, audit.StoreOptions{})
		if err != nil {
			return nil, fmt.Errorf("configure collector-token audit store: %w", err)
		}
	} else if nilcheck.IsNil(auditAppender) {
		return nil, fmt.Errorf(
			"%w: collector-token audit appender is nil",
			control.ErrInvalidArgument,
		)
	}
	return &Store{
		orm:                       db.GORMDB(),
		digestKey:                 append([]byte(nil), digestKey...),
		random:                    rand.Reader,
		now:                       time.Now,
		retainedRevokedTokenLimit: retainedRevokedTokenLimit,
		totalTokenRecordLimit:     totalTokenRecordLimit,
		auditAppender:             auditAppender,
		auditTenantID:             auditTenantID,
		requireExplicitAuditActor: options.RequireExplicitAuditActor,
	}, nil
}

func (store *Store) validateTokenMutationActor(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf(
			"%w: collector-token mutation context is nil",
			control.ErrInvalidArgument,
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	actor, explicit := audit.ActorFromContext(ctx)
	if !explicit {
		if store.requireExplicitAuditActor {
			return ErrAuditActorUnavailable
		}
		return nil
	}
	if actor.Kind == audit.ActorKindBrowser &&
		actor.Role != audit.ActorRoleAdministrator {
		return fmt.Errorf(
			"%w: collector-token mutation requires an administrator actor",
			control.ErrInvalidArgument,
		)
	}
	return nil
}

// CreateCollectorToken generates a cryptographically random token, persists
// only its HMAC-SHA-256 digest, and returns the plaintext exactly once.
func (store *Store) CreateCollectorToken(ctx context.Context, request CreateCollectorTokenRequest) (issued IssuedCollectorToken, err error) {
	if err := store.validateTokenMutationActor(ctx); err != nil {
		return IssuedCollectorToken{}, err
	}
	now := databaseTime(store.now())
	name, description, allowedNames, expiresAt, err := normalizeTokenDefinition(
		request.Name, request.Description, request.AllowedIndexNames, request.ExpiresAt, now,
	)
	if err != nil {
		return IssuedCollectorToken{}, err
	}
	allowedHostRegexes, allowedSourceRegexes, err := normalizeCollectorTokenConstraints(
		request.AllowedHostRegexes,
		request.AllowedSourceRegexes,
	)
	if err != nil {
		return IssuedCollectorToken{}, err
	}
	if err := request.IngestionRateLimits.Validate(); err != nil {
		return IssuedCollectorToken{}, fmt.Errorf(
			"%w: ingestion token rate limits: %w",
			control.ErrInvalidArgument,
			err,
		)
	}
	purpose, hecProfile, err := normalizeCreatedTokenPurpose(
		request.Purpose,
		request.HECProfile,
		request.BoundCollectorID,
		allowedNames,
	)
	if err != nil {
		return IssuedCollectorToken{}, err
	}

	plaintext, err := store.generatePlaintext()
	if err != nil {
		return IssuedCollectorToken{}, errors.New("generate collector token: secure randomness unavailable")
	}
	tokenID, err := store.generateID()
	if err != nil {
		return IssuedCollectorToken{}, errors.New("generate collector token ID: secure randomness unavailable")
	}
	digest := store.digest(plaintext)
	prefix := plaintext[:len(collectorTokenPrefix)+8]

	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return IssuedCollectorToken{}, fmt.Errorf("begin collector token creation: %w", tx.Error)
	}
	transactionFinished := false
	defer finishTokenTransaction(tx, &transactionFinished, &err)

	indexIDs, err := resolveCollectorTokenScopes(tx, allowedNames)
	if errors.Is(err, errCollectorTokenScopeUnavailable) {
		return IssuedCollectorToken{}, fmt.Errorf("%w: every token scope must name an active ingestion-enabled index", control.ErrInvalidArgument)
	}
	if err != nil {
		return IssuedCollectorToken{}, fmt.Errorf("validate collector token scope: %w", err)
	}
	if err := store.ensureCollectorTokenCreateCapacity(tx, len(indexIDs)); err != nil {
		if errors.Is(err, control.ErrCapacityExceeded) {
			return IssuedCollectorToken{}, err
		}
		return IssuedCollectorToken{}, fmt.Errorf(
			"prepare collector token catalog capacity: %w",
			err,
		)
	}

	var expiration *int64
	if !expiresAt.IsZero() {
		expirationUnixMicro := expiresAt.UnixMicro()
		expiration = &expirationUnixMicro
	}
	// #nosec G115 -- validated ingestion rate ceilings fit in signed SQLite integers.
	maxIngestEventsPerSecond := int64(request.IngestionRateLimits.MaxEventsPerSecond)
	// #nosec G115 -- validated ingestion rate ceilings fit in signed SQLite integers.
	maxIngestUncompressedBytesPerSecond := int64(
		request.IngestionRateLimits.MaxUncompressedBytesPerSecond,
	)
	var boundCollectorID *string
	if purpose == IngestionTokenPurposeNativeCollector {
		value := request.BoundCollectorID
		boundCollectorID = &value
	}
	record := collectorTokenRecord{
		IngestionTokenID:                    tokenID,
		Version:                             1,
		Name:                                name,
		Description:                         description,
		TokenPrefix:                         prefix,
		TokenDigest:                         digest,
		State:                               CollectorTokenStateActive,
		CreatedAtUnixMicro:                  now.UnixMicro(),
		UpdatedAtUnixMicro:                  now.UnixMicro(),
		ExpiresAtUnixMicro:                  expiration,
		BoundCollectorID:                    boundCollectorID,
		Purpose:                             purpose,
		MaxIngestEventsPerSecond:            maxIngestEventsPerSecond,
		MaxIngestUncompressedBytesPerSecond: maxIngestUncompressedBytesPerSecond,
	}
	if err := tx.Create(&record).Error; err != nil {
		// No bound SQL value contains plaintext: only the HMAC digest is sent
		// to SQLite, so wrapping a driver error cannot disclose the token.
		return IssuedCollectorToken{}, fmt.Errorf("store collector token digest: %w", err)
	}
	memberships := make([]collectorTokenIndexRecord, len(indexIDs))
	for index, indexID := range indexIDs {
		memberships[index] = collectorTokenIndexRecord{
			IngestionTokenID: tokenID,
			IndexID:          indexID,
		}
	}
	if err := tx.Create(&memberships).Error; err != nil {
		return IssuedCollectorToken{}, fmt.Errorf("store collector token scope: %w", err)
	}
	if purpose == IngestionTokenPurposeHEC {
		profileRecord, profileErr := newCollectorTokenHECProfileRecord(
			tokenID,
			hecProfile,
			allowedNames,
			indexIDs,
		)
		if profileErr != nil {
			return IssuedCollectorToken{}, fmt.Errorf(
				"prepare HEC token profile: %w",
				profileErr,
			)
		}
		if err := tx.Create(&profileRecord).Error; err != nil {
			return IssuedCollectorToken{}, fmt.Errorf(
				"store HEC token profile: %w",
				err,
			)
		}
	}
	constraints := collectorTokenConstraintRecords(
		tokenID,
		allowedHostRegexes,
		allowedSourceRegexes,
	)
	if len(constraints) != 0 {
		if err := tx.Create(&constraints).Error; err != nil {
			return IssuedCollectorToken{}, fmt.Errorf(
				"store collector token constraints: %w",
				err,
			)
		}
	}
	if _, err := store.auditAppender.AppendInTransaction(
		ctx,
		tx,
		store.auditTenantID,
		audit.SuccessfulEvent{
			OccurredAt:    now,
			Action:        audit.ActionIngestionTokenCreate,
			TargetKind:    audit.TargetKindIngestionToken,
			TargetID:      tokenID,
			TargetVersion: 1,
		},
	); err != nil {
		return IssuedCollectorToken{}, fmt.Errorf(
			"append collector token creation audit event: %w",
			err,
		)
	}
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return IssuedCollectorToken{}, fmt.Errorf("commit collector token creation: %w", commitErr)
	}

	metadata := CollectorToken{
		ID: tokenID, Version: 1, Name: name, Description: description,
		Prefix: prefix, State: CollectorTokenStateActive,
		Purpose:              purpose,
		HECProfile:           hecProfile,
		BoundCollectorID:     request.BoundCollectorID,
		AllowedIndexNames:    append([]string(nil), allowedNames...),
		AllowedHostRegexes:   append([]string(nil), allowedHostRegexes...),
		AllowedSourceRegexes: append([]string(nil), allowedSourceRegexes...),
		IngestionRateLimits:  request.IngestionRateLimits,
		CreatedAt:            now, UpdatedAt: now, ExpiresAt: expiresAt,
	}
	return IssuedCollectorToken{Token: metadata, Secret: Secret{plaintext: plaintext}}, nil
}

// UpdateCollectorToken atomically replaces mutable metadata and explicit
// index scopes under optimistic locking. Revoked and effectively expired
// credentials remain immutable so an administrative edit cannot accidentally
// reactivate them.
func (store *Store) UpdateCollectorToken(ctx context.Context, tokenID string, expectedVersion uint64, request UpdateCollectorTokenRequest) (result CollectorToken, err error) {
	if err := store.validateTokenMutationActor(ctx); err != nil {
		return CollectorToken{}, err
	}
	if strings.TrimSpace(tokenID) == "" {
		return CollectorToken{}, fmt.Errorf("%w: token ID is required", control.ErrInvalidArgument)
	}
	if expectedVersion == 0 || expectedVersion > math.MaxInt64 {
		return CollectorToken{}, fmt.Errorf("%w: expected token version is outside the supported range", control.ErrInvalidArgument)
	}
	now := databaseTime(store.now())
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return CollectorToken{}, fmt.Errorf("begin collector token update: %w", tx.Error)
	}
	transactionFinished := false
	defer finishTokenTransaction(tx, &transactionFinished, &err)

	current, err := takeCollectorTokenMetadata(tx, tokenID, now)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return CollectorToken{}, control.ErrNotFound
	}
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read collector token for update: %w", err)
	}
	if current.Version != expectedVersion {
		return CollectorToken{}, control.ErrVersionConflict
	}
	if current.State == CollectorTokenStateRevoked || current.State == CollectorTokenStateExpired {
		return CollectorToken{}, ErrInactiveToken
	}
	if err := request.IngestionRateLimits.Validate(); err != nil {
		return CollectorToken{}, fmt.Errorf(
			"%w: ingestion token rate limits: %w",
			control.ErrInvalidArgument,
			err,
		)
	}
	name, description, allowedNames, expiresAt, err := normalizeTokenDefinition(
		request.Name, request.Description, request.AllowedIndexNames, request.ExpiresAt, now,
	)
	if err != nil {
		return CollectorToken{}, err
	}
	allowedHostRegexes, allowedSourceRegexes, err := normalizeCollectorTokenConstraints(
		request.AllowedHostRegexes,
		request.AllowedSourceRegexes,
	)
	if err != nil {
		return CollectorToken{}, err
	}
	purpose, hecProfile, boundCollectorID, err := normalizeUpdatedTokenPurpose(
		current,
		request.Purpose,
		request.HECProfile,
		request.BoundCollectorID,
		allowedNames,
	)
	if err != nil {
		return CollectorToken{}, err
	}
	if !expiresAt.IsZero() && !expiresAt.After(current.CreatedAt) {
		return CollectorToken{}, fmt.Errorf("%w: token expiration must be after its creation time", control.ErrInvalidArgument)
	}

	indexIDs, err := resolveCollectorTokenScopes(tx, allowedNames)
	if errors.Is(err, errCollectorTokenScopeUnavailable) {
		return CollectorToken{}, fmt.Errorf("%w: every token scope must name an active ingestion-enabled index", control.ErrInvalidArgument)
	}
	if err != nil {
		return CollectorToken{}, fmt.Errorf("validate collector token update scope: %w", err)
	}
	if err := ensureCollectorTokenScopeUpdateCapacity(
		tx,
		len(current.AllowedIndexNames),
		len(indexIDs),
	); err != nil {
		if errors.Is(err, control.ErrCapacityExceeded) {
			return CollectorToken{}, err
		}
		return CollectorToken{}, fmt.Errorf(
			"prepare collector token scope capacity: %w",
			err,
		)
	}
	if purpose == IngestionTokenPurposeHEC {
		clearDefault := tx.Model(&collectorTokenHECProfileRecord{}).
			Where("ingestion_token_id = ?", tokenID).
			UpdateColumn("default_index_id", nil)
		if clearDefault.Error != nil {
			return CollectorToken{}, fmt.Errorf(
				"prepare HEC token profile scope replacement: %w",
				clearDefault.Error,
			)
		}
		if clearDefault.RowsAffected != 1 {
			return CollectorToken{}, fmt.Errorf(
				"%w: HEC token profile is missing",
				errCollectorTokenCatalogInconsistent,
			)
		}
	}

	// #nosec G115 -- expectedVersion is bounded above by math.MaxInt64.
	expectedVersionDB := int64(expectedVersion)
	var expiration *int64
	if !expiresAt.IsZero() {
		expirationUnixMicro := expiresAt.UnixMicro()
		expiration = &expirationUnixMicro
	}
	// #nosec G115 -- validated ingestion rate ceilings fit in signed SQLite integers.
	maxIngestEventsPerSecond := int64(request.IngestionRateLimits.MaxEventsPerSecond)
	// #nosec G115 -- validated ingestion rate ceilings fit in signed SQLite integers.
	maxIngestUncompressedBytesPerSecond := int64(
		request.IngestionRateLimits.MaxUncompressedBytesPerSecond,
	)
	updateValues := map[string]any{
		"name":                         name,
		"description":                  description,
		"expires_at_unix_micro":        expiration,
		"updated_at_unix_micro":        now.UnixMicro(),
		"max_ingest_events_per_second": maxIngestEventsPerSecond,
		"max_ingest_uncompressed_bytes_per_second": maxIngestUncompressedBytesPerSecond,
		"version": gorm.Expr("version + 1"),
	}
	if current.BoundCollectorID == "" && boundCollectorID != "" {
		updateValues["bound_collector_id"] = boundCollectorID
	}
	update := tx.Model(&collectorTokenRecord{}).
		Where(
			"ingestion_token_id = ? AND version = ? AND state != ?",
			tokenID,
			expectedVersionDB,
			CollectorTokenStateRevoked,
		).
		Updates(updateValues)
	if update.Error != nil {
		return CollectorToken{}, fmt.Errorf("update collector token: %w", update.Error)
	}
	if update.RowsAffected != 1 {
		return CollectorToken{}, control.ErrVersionConflict
	}
	if deleteErr := tx.Where("ingestion_token_id = ?", tokenID).
		Delete(&collectorTokenIndexRecord{}).Error; deleteErr != nil {
		return CollectorToken{}, fmt.Errorf("replace collector token scopes: %w", deleteErr)
	}
	memberships := make([]collectorTokenIndexRecord, len(indexIDs))
	for index, indexID := range indexIDs {
		memberships[index] = collectorTokenIndexRecord{
			IngestionTokenID: tokenID,
			IndexID:          indexID,
		}
	}
	if err := tx.Create(&memberships).Error; err != nil {
		return CollectorToken{}, fmt.Errorf("store updated collector token scope: %w", err)
	}
	if purpose == IngestionTokenPurposeHEC {
		profileRecord, profileErr := newCollectorTokenHECProfileRecord(
			tokenID,
			hecProfile,
			allowedNames,
			indexIDs,
		)
		if profileErr != nil {
			return CollectorToken{}, fmt.Errorf(
				"prepare updated HEC token profile: %w",
				profileErr,
			)
		}
		profileUpdate := tx.Model(&collectorTokenHECProfileRecord{}).
			Where("ingestion_token_id = ?", tokenID).
			Updates(map[string]any{
				"default_index_id":   profileRecord.DefaultIndexID,
				"default_host":       profileRecord.DefaultHost,
				"default_source":     profileRecord.DefaultSource,
				"default_sourcetype": profileRecord.DefaultSourcetype,
			})
		if profileUpdate.Error != nil {
			return CollectorToken{}, fmt.Errorf(
				"update HEC token profile: %w",
				profileUpdate.Error,
			)
		}
		if profileUpdate.RowsAffected != 1 {
			return CollectorToken{}, fmt.Errorf(
				"%w: HEC token profile is missing",
				errCollectorTokenCatalogInconsistent,
			)
		}
	}
	if deleteErr := tx.Where("ingestion_token_id = ?", tokenID).
		Delete(&collectorTokenConstraintRecord{}).Error; deleteErr != nil {
		return CollectorToken{}, fmt.Errorf(
			"replace collector token constraints: %w",
			deleteErr,
		)
	}
	constraints := collectorTokenConstraintRecords(
		tokenID,
		allowedHostRegexes,
		allowedSourceRegexes,
	)
	if len(constraints) != 0 {
		if err := tx.Create(&constraints).Error; err != nil {
			return CollectorToken{}, fmt.Errorf(
				"store updated collector token constraints: %w",
				err,
			)
		}
	}

	result, err = takeCollectorTokenMetadata(tx, tokenID, now)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read updated collector token: %w", err)
	}
	if _, err := store.auditAppender.AppendInTransaction(
		ctx,
		tx,
		store.auditTenantID,
		audit.SuccessfulEvent{
			OccurredAt:    now,
			Action:        audit.ActionIngestionTokenUpdate,
			TargetKind:    audit.TargetKindIngestionToken,
			TargetID:      result.ID,
			TargetVersion: result.Version,
		},
	); err != nil {
		return CollectorToken{}, fmt.Errorf(
			"append collector token update audit event: %w",
			err,
		)
	}
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return CollectorToken{}, fmt.Errorf("commit collector token update: %w", commitErr)
	}
	return result, nil
}

// Authorize atomically checks a collector credential and one target index.
// All credential and scope failures deliberately return the same error.
func (store *Store) Authorize(ctx context.Context, plaintext, indexName string) (Principal, error) {
	normalizedIndex, err := control.NormalizeIndexName(indexName)
	if err != nil || plaintext == "" {
		return Principal{}, ErrUnauthorized
	}
	digest := store.digest(plaintext)
	now := databaseTime(store.now())
	var principal Principal
	result := store.orm.WithContext(ctx).
		Table("ingestion_tokens AS token").
		Select(`
			token.ingestion_token_id AS token_id,
			token.name AS token_name,
			target.name AS index_name,
			token.bound_collector_id AS bound_collector_id`).
		Joins(`
			JOIN ingestion_token_indexes AS scope
			  ON scope.ingestion_token_id = token.ingestion_token_id`).
		Joins("JOIN indexes AS target ON target.index_id = scope.index_id").
		Where(
			`token.token_digest = ?
			 AND token.state = ?
			 AND token.purpose = ?
			 AND token.bound_collector_id IS NOT NULL
			 AND (token.expires_at_unix_micro IS NULL OR token.expires_at_unix_micro > ?)
			 AND target.name = ?
			 AND target.state = ?
			 AND target.ingestion_enabled = 1`,
			digest,
			CollectorTokenStateActive,
			IngestionTokenPurposeNativeCollector,
			now.UnixMicro(),
			normalizedIndex,
			control.IndexStateActive,
		).
		Take(&principal)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return Principal{}, ErrUnauthorized
	}
	if result.Error != nil {
		return Principal{}, fmt.Errorf("authorize collector token: %w", result.Error)
	}
	if !validCollectorID(principal.BoundCollectorID) {
		return Principal{}, errors.New("authorize collector token: invalid bound collector ID in control-plane database")
	}
	return principal, nil
}

// Authenticate validates one collector credential and resolves its complete
// current ingestion scope in a single database snapshot. Invalid, disabled,
// revoked, expired, and scope-less credentials all return ErrUnauthorized.
func (store *Store) Authenticate(
	ctx context.Context,
	plaintext string,
) (
	authentication Authentication,
	returnedErr error,
) {
	if ctx == nil {
		return Authentication{}, fmt.Errorf("%w: nil context", control.ErrInvalidArgument)
	}
	tx := store.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return Authentication{}, fmt.Errorf(
			"begin collector token authentication: %w",
			tx.Error,
		)
	}
	finished := false
	defer finishTokenTransaction(tx, &finished, &returnedErr)

	authentication, err := store.authenticate(
		tx,
		plaintext,
		databaseTime(store.now()),
	)
	if err != nil {
		return Authentication{}, err
	}
	if err := tx.Commit().Error; err != nil {
		return Authentication{}, fmt.Errorf(
			"commit collector token authentication: %w",
			err,
		)
	}
	finished = true
	return authentication, nil
}

// AuthenticateHEC validates one HEC credential, resolves its versioned
// profile and complete current ingestion-policy snapshot, and records a
// monotonic last-use observation in the same transaction. Unknown,
// wrong-purpose, expired, revoked, and scope-less credentials return
// ErrUnauthorized. A structurally valid, unexpired HEC credential in the
// explicit disabled state returns ErrInactiveToken. The returned value
// contains no credential material.
func (store *Store) AuthenticateHEC(
	ctx context.Context,
	plaintext string,
) (
	authentication Authentication,
	returnedErr error,
) {
	if ctx == nil {
		return Authentication{}, fmt.Errorf("%w: nil context", control.ErrInvalidArgument)
	}
	checkedAt := databaseTime(store.now())
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return Authentication{}, fmt.Errorf(
			"begin HEC token authentication: %w",
			tx.Error,
		)
	}
	finished := false
	defer finishTokenTransaction(tx, &finished, &returnedErr)

	authentication, err := store.authenticateHEC(tx, plaintext, checkedAt)
	if err != nil {
		return Authentication{}, err
	}
	if err := recordIngestionTokenUse(
		tx,
		authentication.TokenID,
		IngestionTokenPurposeHEC,
		checkedAt,
	); err != nil {
		return Authentication{}, err
	}
	if err := tx.Commit().Error; err != nil {
		return Authentication{}, fmt.Errorf(
			"commit HEC token authentication: %w",
			err,
		)
	}
	finished = true
	return authentication, nil
}

func (store *Store) authenticate(
	database *gorm.DB,
	plaintext string,
	now time.Time,
) (Authentication, error) {
	return store.authenticateWithIndexAuthority(database, plaintext, now, false)
}

func (store *Store) authenticateForLease(
	database *gorm.DB,
	plaintext string,
	now time.Time,
) (Authentication, error) {
	return store.authenticateWithIndexAuthority(database, plaintext, now, true)
}

func (store *Store) authenticateWithIndexAuthority(
	database *gorm.DB,
	plaintext string,
	now time.Time,
	deferAuthorityErrors bool,
) (Authentication, error) {
	if plaintext == "" {
		return Authentication{}, ErrUnauthorized
	}
	digest := store.digest(plaintext)
	var row collectorTokenAuthenticationRow
	result := database.
		Table("ingestion_tokens AS token").
		Select(`
			token.ingestion_token_id,
			token.version,
			token.name,
			token.bound_collector_id,
			token.max_ingest_events_per_second,
			token.max_ingest_uncompressed_bytes_per_second`).
		Where(
			`token.token_digest = ?
			 AND token.state = ?
			 AND token.purpose = ?
			 AND token.bound_collector_id IS NOT NULL
			 AND length(CAST(token.ingestion_token_id AS BLOB)) BETWEEN 1 AND ?
			 AND instr(token.ingestion_token_id, char(0)) = 0
			 AND (token.expires_at_unix_micro IS NULL OR token.expires_at_unix_micro > ?)`,
			digest,
			CollectorTokenStateActive,
			IngestionTokenPurposeNativeCollector,
			maximumTokenIDBytes,
			now.UnixMicro(),
		).
		Take(&row)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return Authentication{}, ErrUnauthorized
	}
	if result.Error != nil {
		return Authentication{}, fmt.Errorf("authenticate collector token: %w", result.Error)
	}
	if !validAuthenticationTokenID(row.IngestionTokenID) {
		return Authentication{}, errors.New(
			"authenticate collector token: invalid token ID in control-plane database",
		)
	}
	if row.Version < 1 {
		return Authentication{}, errors.New(
			"authenticate collector token: invalid token version in control-plane database",
		)
	}
	if !validCollectorID(row.BoundCollectorID) {
		return Authentication{}, errors.New("authenticate collector token: invalid bound collector ID in control-plane database")
	}
	if row.MaxIngestEventsPerSecond < 0 ||
		row.MaxIngestUncompressedBytesPerSecond < 0 {
		return Authentication{}, errors.New(
			"authenticate collector token: invalid rate limits in control-plane database",
		)
	}
	tokenRateLimits := ingestquota.Limits{
		MaxEventsPerSecond: uint64(row.MaxIngestEventsPerSecond),
		MaxUncompressedBytesPerSecond: uint64(
			row.MaxIngestUncompressedBytesPerSecond,
		),
	}
	if err := tokenRateLimits.Validate(); err != nil {
		return Authentication{}, errors.New(
			"authenticate collector token: invalid rate limits in control-plane database",
		)
	}
	authentication := Authentication{
		TokenID:          row.IngestionTokenID,
		TokenVersion:     uint64(row.Version),
		TokenName:        row.Name,
		Purpose:          IngestionTokenPurposeNativeCollector,
		BoundCollectorID: row.BoundCollectorID,
		TokenRateLimits:  tokenRateLimits,
	}
	return hydrateTokenAuthenticationAuthority(
		database,
		authentication,
		now,
		deferAuthorityErrors,
	)
}

func (store *Store) authenticateHEC(
	database *gorm.DB,
	plaintext string,
	now time.Time,
) (Authentication, error) {
	if plaintext == "" {
		return Authentication{}, ErrUnauthorized
	}
	digest := store.digest(plaintext)
	base := func() *gorm.DB {
		return database.
			Table("ingestion_tokens AS token").
			Joins(`
				LEFT JOIN ingestion_token_hec_profiles AS hec_profile
				  ON hec_profile.ingestion_token_id = token.ingestion_token_id`).
			Joins(`
				LEFT JOIN indexes AS hec_default_index
				  ON hec_default_index.index_id = hec_profile.default_index_id`).
			Joins(`
				LEFT JOIN ingestion_token_indexes AS hec_default_scope
				  ON hec_default_scope.ingestion_token_id = token.ingestion_token_id
				 AND hec_default_scope.index_id = hec_profile.default_index_id`).
			Where(
				`token.token_digest = ?
				 AND token.state IN (?, ?)
				 AND token.purpose = ?
				 AND (token.expires_at_unix_micro IS NULL OR token.expires_at_unix_micro > ?)`,
				digest,
				CollectorTokenStateActive,
				CollectorTokenStateDisabled,
				IngestionTokenPurposeHEC,
				now.UnixMicro(),
			)
	}

	var widths collectorTokenHECAuthenticationWidths
	widthResult := base().
		Select(`
			length(CAST(token.ingestion_token_id AS BLOB))
				AS ingestion_token_id_bytes,
			token.version,
			CASE
				WHEN token.state = 'active' THEN 1
				WHEN token.state = 'disabled' THEN 2
				ELSE 0
			END AS state_kind,
			length(CAST(token.name AS BLOB)) AS name_bytes,
			CASE WHEN token.bound_collector_id IS NULL THEN 0 ELSE 1 END
				AS bound_collector_id_present,
			CASE WHEN hec_profile.ingestion_token_id IS NULL THEN 0 ELSE 1 END
				AS hec_profile_present,
			CASE
				WHEN hec_profile.default_index_id IS NULL THEN 0
				WHEN hec_default_index.index_id IS NULL
				  OR hec_default_scope.index_id IS NULL THEN -1
				ELSE 1
			END AS hec_default_index_target_present,
			length(CAST(hec_default_index.name AS BLOB))
				AS hec_default_index_name_bytes,
			length(CAST(hec_profile.default_host AS BLOB))
				AS hec_default_host_bytes,
			length(CAST(hec_profile.default_source AS BLOB))
				AS hec_default_source_bytes,
			length(CAST(hec_profile.default_sourcetype AS BLOB))
				AS hec_default_sourcetype_bytes,
			hec_profile.indexer_acknowledgment
				AS hec_indexer_acknowledgment,
			token.max_ingest_events_per_second,
			token.max_ingest_uncompressed_bytes_per_second`).
		Take(&widths)
	if errors.Is(widthResult.Error, gorm.ErrRecordNotFound) {
		return Authentication{}, ErrUnauthorized
	}
	if widthResult.Error != nil {
		return Authentication{}, fmt.Errorf(
			"preflight HEC token authentication: %w",
			widthResult.Error,
		)
	}
	if err := validateHECAuthenticationWidths(widths); err != nil {
		return Authentication{}, fmt.Errorf(
			"authenticate HEC token: %w",
			err,
		)
	}
	if widths.StateKind == 2 {
		return Authentication{}, ErrInactiveToken
	}

	var row collectorTokenHECAuthenticationRow
	rowResult := base().
		Select(`
			token.ingestion_token_id,
			token.version,
			token.name,
			hec_default_index.name AS hec_default_index_name,
			hec_profile.default_host AS hec_default_host,
			hec_profile.default_source AS hec_default_source,
			hec_profile.default_sourcetype AS hec_default_sourcetype,
			hec_profile.indexer_acknowledgment
				AS hec_indexer_acknowledgment,
			token.max_ingest_events_per_second,
			token.max_ingest_uncompressed_bytes_per_second`).
		Take(&row)
	if errors.Is(rowResult.Error, gorm.ErrRecordNotFound) {
		return Authentication{}, ErrUnauthorized
	}
	if rowResult.Error != nil {
		return Authentication{}, fmt.Errorf(
			"read HEC token authentication snapshot: %w",
			rowResult.Error,
		)
	}
	authentication, err := hecAuthenticationFromRow(row)
	if err != nil {
		return Authentication{}, fmt.Errorf("authenticate HEC token: %w", err)
	}
	return hydrateTokenAuthenticationAuthority(
		database,
		authentication,
		now,
		false,
	)
}

func validateHECAuthenticationWidths(
	widths collectorTokenHECAuthenticationWidths,
) error {
	if widths.IngestionTokenIDBytes < 1 ||
		widths.IngestionTokenIDBytes > maximumTokenIDBytes ||
		widths.Version < 1 ||
		(widths.StateKind != 1 && widths.StateKind != 2) ||
		widths.NameBytes < 1 ||
		widths.NameBytes > maximumTokenNameBytes ||
		widths.BoundCollectorIDPresent != 0 ||
		widths.HECProfilePresent != 1 ||
		widths.HECIndexerAcknowledgment == nil ||
		(*widths.HECIndexerAcknowledgment != 0 &&
			*widths.HECIndexerAcknowledgment != 1) {
		return errors.New("HEC token or profile projection is inconsistent")
	}
	switch widths.HECDefaultIndexTargetPresent {
	case 0:
		if widths.HECDefaultIndexNameBytes != nil {
			return errors.New("HEC token default index projection is inconsistent")
		}
	case 1:
		if widths.HECDefaultIndexNameBytes == nil ||
			*widths.HECDefaultIndexNameBytes < 1 ||
			*widths.HECDefaultIndexNameBytes > maximumTokenNameBytes {
			return errors.New("HEC token default index projection exceeds its byte bounds")
		}
	default:
		return errors.New("HEC token default index target is unavailable")
	}
	for _, width := range []*int64{
		widths.HECDefaultHostBytes,
		widths.HECDefaultSourceBytes,
		widths.HECDefaultSourcetypeBytes,
	} {
		if width != nil && (*width < 1 || *width > maximumHECMetadataBytes) {
			return errors.New("HEC token metadata projection exceeds its byte bounds")
		}
	}
	if widths.MaxIngestEventsPerSecond < 0 ||
		widths.MaxIngestUncompressedBytesPerSecond < 0 {
		return errors.New("HEC token rate limits are invalid")
	}
	return nil
}

func hecAuthenticationFromRow(
	row collectorTokenHECAuthenticationRow,
) (Authentication, error) {
	if !validAuthenticationTokenID(row.IngestionTokenID) || row.Version < 1 {
		return Authentication{}, errors.New("HEC token identity is invalid")
	}
	if len(row.Name) < 1 || len(row.Name) > maximumTokenNameBytes ||
		!utf8.ValidString(row.Name) || strings.IndexByte(row.Name, 0) >= 0 {
		return Authentication{}, errors.New("HEC token name is invalid")
	}
	if row.HECIndexerAcknowledgment == nil ||
		(*row.HECIndexerAcknowledgment != 0 &&
			*row.HECIndexerAcknowledgment != 1) {
		return Authentication{}, errors.New("HEC token acknowledgment mode is invalid")
	}
	profile := HECTokenProfile{
		IndexerAcknowledgment: *row.HECIndexerAcknowledgment == 1,
	}
	if row.HECDefaultIndexName != nil {
		canonical, err := control.NormalizeIndexName(*row.HECDefaultIndexName)
		if err != nil || canonical != *row.HECDefaultIndexName {
			return Authentication{}, errors.New("HEC token default index is invalid")
		}
		profile.DefaultIndexName = strings.Clone(*row.HECDefaultIndexName)
	}
	for index, stored := range []*string{
		row.HECDefaultHost,
		row.HECDefaultSource,
		row.HECDefaultSourcetype,
	} {
		if stored == nil {
			continue
		}
		if !validHECMetadataDefault(*stored) {
			return Authentication{}, fmt.Errorf(
				"HEC token metadata default %d is invalid",
				index,
			)
		}
		switch index {
		case 0:
			profile.DefaultHost = strings.Clone(*stored)
		case 1:
			profile.DefaultSource = strings.Clone(*stored)
		case 2:
			profile.DefaultSourcetype = strings.Clone(*stored)
		}
	}
	if row.MaxIngestEventsPerSecond < 0 ||
		row.MaxIngestUncompressedBytesPerSecond < 0 {
		return Authentication{}, errors.New("HEC token rate limits are invalid")
	}
	rateLimits := ingestquota.Limits{
		MaxEventsPerSecond: uint64(row.MaxIngestEventsPerSecond),
		MaxUncompressedBytesPerSecond: uint64(
			row.MaxIngestUncompressedBytesPerSecond,
		),
	}
	if err := rateLimits.Validate(); err != nil {
		return Authentication{}, errors.New("HEC token rate limits are invalid")
	}
	return Authentication{
		TokenID:          row.IngestionTokenID,
		TokenVersion:     uint64(row.Version),
		TokenName:        strings.Clone(row.Name),
		Purpose:          IngestionTokenPurposeHEC,
		HECProfile:       profile,
		TokenRateLimits:  rateLimits,
		BoundCollectorID: "",
	}, nil
}

func hydrateTokenAuthenticationAuthority(
	database *gorm.DB,
	authentication Authentication,
	now time.Time,
	deferAuthorityErrors bool,
) (Authentication, error) {
	constraintTokens, constraintErr := hydrateCollectorTokenConstraints(
		database,
		[]CollectorToken{{ID: authentication.TokenID}},
		map[string]int{authentication.TokenID: 0},
		[]string{authentication.TokenID},
		0,
		false,
	)
	if constraintErr != nil {
		if deferAuthorityErrors &&
			(errors.Is(constraintErr, errCollectorTokenCatalogInconsistent) ||
				errors.Is(constraintErr, errCollectorTokenCatalogOverflow)) {
			return authentication, ErrInvalidEventAuthority
		}
		return Authentication{}, fmt.Errorf(
			"authenticate collector token constraints: %w",
			constraintErr,
		)
	}
	if len(constraintTokens) != 1 {
		if deferAuthorityErrors {
			return authentication, ErrInvalidEventAuthority
		}
		return Authentication{}, errors.New(
			"authenticate collector token constraints: invalid projection cardinality",
		)
	}
	authentication.AllowedHostRegexes = constraintTokens[0].AllowedHostRegexes
	authentication.AllowedSourceRegexes = constraintTokens[0].AllowedSourceRegexes
	var scopeWidths []collectorTokenAuthenticationScopeWidths
	widthResult := database.
		Table("ingestion_token_indexes AS scope").
		Select(
			"target.index_id IS NOT NULL AS target_present",
			"length(CAST(target.name AS BLOB)) AS name_bytes",
			"length(CAST(target.default_sourcetype AS BLOB)) AS default_sourcetype_bytes",
			"length(CAST(target.state AS BLOB)) AS state_bytes",
		).
		Joins("LEFT JOIN indexes AS target ON target.index_id = scope.index_id").
		Where("scope.ingestion_token_id = ?", authentication.TokenID).
		Limit(maximumTokenScopes + 1).
		Find(&scopeWidths)
	if widthResult.Error != nil {
		return Authentication{}, fmt.Errorf(
			"preflight ingestion token authentication scopes: %w",
			widthResult.Error,
		)
	}
	if len(scopeWidths) > maximumTokenScopes {
		if deferAuthorityErrors {
			return authentication, ErrInvalidIndexAuthority
		}
		return Authentication{}, errors.New(
			"authenticate collector token: scope count exceeds the supported maximum",
		)
	}
	invalidScopeProjection := false
	for _, widths := range scopeWidths {
		if widths.TargetPresent != 1 ||
			widths.NameBytes == nil || *widths.NameBytes < 1 ||
			*widths.NameBytes > maximumTokenNameBytes ||
			widths.DefaultSourcetypeBytes == nil ||
			*widths.DefaultSourcetypeBytes < 0 ||
			*widths.DefaultSourcetypeBytes >
				indexpolicy.MaximumDefaultSourcetypeBytes ||
			widths.StateBytes == nil || *widths.StateBytes < 1 ||
			*widths.StateBytes > 16 {
			invalidScopeProjection = true
			break
		}
	}
	if invalidScopeProjection {
		if deferAuthorityErrors {
			return authentication, ErrInvalidIndexAuthority
		}
		return Authentication{}, errors.New(
			"authenticate ingestion token: scope projection exceeds persisted byte bounds or has no target",
		)
	}
	var scopes []collectorTokenAuthenticationScopeRow
	scopeResult := database.
		Table("ingestion_token_indexes AS scope").
		Select(
			"target.index_id IS NOT NULL AS target_present",
			"COALESCE(target.name, '') AS name",
			"COALESCE(target.version, 0) AS version",
			"COALESCE(target.retention_nanoseconds, -1) AS retention_nanoseconds",
			"COALESCE(target.default_sourcetype, '') AS default_sourcetype",
			"COALESCE(target.max_event_bytes, -1) AS max_event_bytes",
			"COALESCE(target.max_field_count, -1) AS max_field_count",
			"COALESCE(target.max_nesting_depth, -1) AS max_nesting_depth",
			"COALESCE(target.maximum_future_skew_nanoseconds, -1) AS maximum_future_skew_nanoseconds",
			"COALESCE(target.maximum_event_age_nanoseconds, -1) AS maximum_event_age_nanoseconds",
			"COALESCE(target.max_ingest_events_per_second, -1) AS max_ingest_events_per_second",
			"COALESCE(target.max_ingest_uncompressed_bytes_per_second, -1) AS max_ingest_uncompressed_bytes_per_second",
			"COALESCE(target.state, '') AS state",
			"COALESCE(target.ingestion_enabled, -1) AS ingestion_enabled",
		).
		Joins("LEFT JOIN indexes AS target ON target.index_id = scope.index_id").
		Where("scope.ingestion_token_id = ?", authentication.TokenID).
		Order("target.name").
		Order("scope.index_id").
		Limit(maximumTokenScopes + 1).
		Find(&scopes)
	if scopeResult.Error != nil {
		return Authentication{}, fmt.Errorf(
			"authenticate collector token scopes: %w",
			scopeResult.Error,
		)
	}
	if len(scopes) == 0 {
		if deferAuthorityErrors {
			return authentication, ErrInvalidIndexAuthority
		}
		return Authentication{}, ErrUnauthorized
	}
	if len(scopes) > maximumTokenScopes {
		if deferAuthorityErrors {
			return authentication, ErrInvalidIndexAuthority
		}
		return Authentication{}, errors.New(
			"authenticate collector token: scope count exceeds the supported maximum",
		)
	}
	authorizedIndexes := make([]AuthorizedIndexPolicy, 0, len(scopes))
	for scopeIndex, scope := range scopes {
		if scopeIndex > 0 && scopes[scopeIndex-1].Name >= scope.Name {
			if deferAuthorityErrors {
				return authentication, ErrInvalidIndexAuthority
			}
			return Authentication{}, errors.New(
				"authenticate collector token: duplicate scope in control-plane database",
			)
		}
		policy, policyErr := authorizedIndexPolicyFromScope(scope, now)
		if scope.TargetPresent != 1 || policyErr != nil ||
			(scope.State != string(control.IndexStateActive) &&
				scope.State != string(control.IndexStateArchived) &&
				scope.State != string(control.IndexStateDeleting)) ||
			(scope.IngestionEnabled != 0 && scope.IngestionEnabled != 1) {
			if deferAuthorityErrors {
				return authentication, ErrInvalidIndexAuthority
			}
			return Authentication{}, errors.New(
				"authenticate collector token: invalid scope in control-plane database",
			)
		}
		if scope.State == string(control.IndexStateActive) &&
			scope.IngestionEnabled == 1 {
			authorizedIndexes = append(authorizedIndexes, policy)
		}
	}
	if len(authorizedIndexes) == 0 {
		if deferAuthorityErrors {
			return authentication, ErrNoActiveIndexAuthority
		}
		return Authentication{}, ErrUnauthorized
	}
	authentication.AuthorizedIndexes = authorizedIndexes
	return authentication, nil
}

func authorizedIndexPolicyFromScope(
	scope collectorTokenAuthenticationScopeRow,
	reference time.Time,
) (AuthorizedIndexPolicy, error) {
	if scope.Version < 1 ||
		scope.MaxEventBytes < 0 ||
		scope.MaxFieldCount < 0 ||
		scope.MaxFieldCount > math.MaxUint32 ||
		scope.MaxNestingDepth < 0 ||
		scope.MaxNestingDepth > math.MaxUint32 ||
		scope.MaximumFutureSkewNanoseconds < 0 ||
		scope.MaximumEventAgeNanoseconds < 0 ||
		scope.MaxIngestEventsPerSecond < 0 ||
		scope.MaxIngestUncompressedBytesPerSecond < 0 {
		return AuthorizedIndexPolicy{}, errors.New("invalid authorized index policy")
	}
	policy := AuthorizedIndexPolicy{
		Name:              scope.Name,
		Version:           uint64(scope.Version),
		RetentionPeriod:   time.Duration(scope.RetentionNanoseconds),
		DefaultSourcetype: scope.DefaultSourcetype,
		Limits: indexpolicy.Limits{
			MaxEventBytes:     uint64(scope.MaxEventBytes),
			MaxFieldCount:     uint32(scope.MaxFieldCount),
			MaxNestingDepth:   uint32(scope.MaxNestingDepth),
			MaximumFutureSkew: time.Duration(scope.MaximumFutureSkewNanoseconds),
			MaximumEventAge:   time.Duration(scope.MaximumEventAgeNanoseconds),
		},
		IngestionRateLimits: ingestquota.Limits{
			MaxEventsPerSecond: uint64(scope.MaxIngestEventsPerSecond),
			MaxUncompressedBytesPerSecond: uint64(
				scope.MaxIngestUncompressedBytesPerSecond,
			),
		},
	}
	if err := policy.ValidateStoredAt(reference); err != nil {
		return AuthorizedIndexPolicy{}, errors.New("invalid authorized index policy")
	}
	return policy, nil
}

func validAuthenticationTokenID(value string) bool {
	return len(value) >= 1 &&
		len(value) <= maximumTokenIDBytes &&
		utf8.ValidString(value) &&
		strings.IndexByte(value, 0) < 0
}

// RecordCollectorTokenUse records when the server accepted a collector stream
// for an active, unexpired token. Observations only move forward and are
// operational telemetry: they deliberately do not change the administrative
// version or updated-at timestamp used for optimistic locking. The caller
// supplies a server-owned admission time captured immediately after successful
// authentication. If the wall clock has moved behind the token's persisted
// creation time, the observation is clamped to that durable lower bound rather
// than falsely deauthorizing an already-authenticated credential.
func (store *Store) RecordCollectorTokenUse(ctx context.Context, tokenID string, acceptedAt time.Time) error {
	return recordCollectorTokenUse(
		store.orm.WithContext(ctx),
		tokenID,
		acceptedAt,
	)
}

func recordCollectorTokenUse(
	database *gorm.DB,
	tokenID string,
	acceptedAt time.Time,
) error {
	return recordIngestionTokenUse(
		database,
		tokenID,
		IngestionTokenPurposeNativeCollector,
		acceptedAt,
	)
}

func recordIngestionTokenUse(
	database *gorm.DB,
	tokenID string,
	purpose IngestionTokenPurpose,
	acceptedAt time.Time,
) error {
	acceptedAt = databaseTime(acceptedAt)
	if acceptedAt.IsZero() {
		return fmt.Errorf("%w: ingestion token acceptance time is required", control.ErrInvalidArgument)
	}
	if purpose != IngestionTokenPurposeNativeCollector &&
		purpose != IngestionTokenPurposeHEC {
		return fmt.Errorf("%w: ingestion token purpose is invalid", control.ErrInvalidArgument)
	}
	acceptedAtUnixMicro := acceptedAt.UnixMicro()
	update := database.
		Model(&collectorTokenRecord{}).
		Where("ingestion_token_id = ?", tokenID).
		Where("state = ?", CollectorTokenStateActive).
		Where("purpose = ?", purpose).
		Where(
			"expires_at_unix_micro IS NULL OR expires_at_unix_micro > ?",
			acceptedAtUnixMicro,
		)
	if purpose == IngestionTokenPurposeNativeCollector {
		update = update.Where("bound_collector_id IS NOT NULL")
	} else {
		update = update.
			Where("bound_collector_id IS NULL").
			Where(`EXISTS (
				SELECT 1
				FROM ingestion_token_hec_profiles AS hec_profile
				WHERE hec_profile.ingestion_token_id = ingestion_tokens.ingestion_token_id
			)`)
	}
	result := update.
		UpdateColumn(
			"last_used_at_unix_micro",
			gorm.Expr(`
				CASE
					WHEN last_used_at_unix_micro IS NULL
					THEN MAX(created_at_unix_micro, ?)
					WHEN last_used_at_unix_micro < ?
					THEN MAX(created_at_unix_micro, ?)
					ELSE last_used_at_unix_micro
				END`,
				acceptedAtUnixMicro,
				acceptedAtUnixMicro,
				acceptedAtUnixMicro,
			),
		)
	if result.Error != nil {
		return fmt.Errorf("record ingestion token use: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		// The stream-admission path must not reveal whether an identifier is
		// missing, disabled, revoked, expired, or predates the token.
		return ErrInactiveToken
	}
	return nil
}

// GetCollectorToken returns safe metadata and explicit index scopes.
func (store *Store) GetCollectorToken(
	ctx context.Context,
	tokenID string,
) (
	result CollectorToken,
	returnedErr error,
) {
	tx := store.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return CollectorToken{}, fmt.Errorf(
			"begin collector token get: %w",
			tx.Error,
		)
	}
	transactionFinished := false
	defer finishTokenTransaction(tx, &transactionFinished, &returnedErr)

	result, err := takeCollectorTokenMetadata(
		tx,
		tokenID,
		databaseTime(store.now()),
	)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return CollectorToken{}, control.ErrNotFound
	}
	if err != nil {
		return CollectorToken{}, fmt.Errorf("get collector token: %w", err)
	}
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return CollectorToken{}, fmt.Errorf(
			"commit collector token get: %w",
			commitErr,
		)
	}
	return result, nil
}

func (store *Store) ensureCollectorTokenCreateCapacity(
	tx *gorm.DB,
	newScopeCount int,
) error {
	if newScopeCount < 1 || newScopeCount > maximumTokenScopes {
		return fmt.Errorf(
			"%w: collector token creation scope is outside per-token bounds",
			errCollectorTokenCatalogInconsistent,
		)
	}
	catalog, err := inspectCollectorTokenCatalog(tx)
	if err != nil {
		return fmt.Errorf("inspect collector token catalog capacity: %w", err)
	}
	if err := preflightAllCollectorTokenProjectionWidths(
		tx,
		catalog.tokenRecords,
	); err != nil {
		return fmt.Errorf("inspect collector token catalog capacity: %w", err)
	}
	distribution, err := collectorTokenScopeDistribution(tx, catalog)
	if err != nil {
		return fmt.Errorf("inspect collector token scope distribution: %w", err)
	}

	if distribution.nonRevokedRecords+1 > store.totalTokenRecordLimit ||
		distribution.nonRevokedScopes+newScopeCount >
			maximumTotalTokenScopeRecordLimit {
		return fmt.Errorf(
			"%w: collector token catalog reached its configured record limit",
			control.ErrCapacityExceeded,
		)
	}

	rowAllowance := store.totalTokenRecordLimit -
		1 -
		distribution.nonRevokedRecords
	scopeAllowance := maximumTotalTokenScopeRecordLimit -
		newScopeCount -
		distribution.nonRevokedScopes
	retainedLimit := min(store.retainedRevokedTokenLimit, rowAllowance)
	retainedRevoked := 0
	retainedScopes := 0
	for retainedRevoked < len(distribution.revokedScopes) &&
		retainedRevoked < retainedLimit {
		nextScopes := distribution.revokedScopes[retainedRevoked]
		if retainedScopes+nextScopes > scopeAllowance {
			break
		}
		retainedScopes += nextScopes
		retainedRevoked++
	}

	// The distribution is newest-first. Delete its suffix in one transaction,
	// so every reclaimed row is revoked and older than every retained tombstone.
	if retainedRevoked < len(distribution.revokedScopes) {
		deleted := deleteRevokedCollectorTokenVictims(
			tx,
			retainedRevoked,
			"",
		)
		if deleted.Error != nil {
			return fmt.Errorf(
				"compact revoked collector tokens for catalog capacity: %w",
				deleted.Error,
			)
		}
	}

	verified, err := inspectCollectorTokenCatalog(tx)
	if err != nil {
		return fmt.Errorf("verify collector token catalog capacity: %w", err)
	}
	expectedTokenRecords := distribution.nonRevokedRecords + retainedRevoked
	expectedScopeRecords := distribution.nonRevokedScopes + retainedScopes
	if verified.tokenRecords != expectedTokenRecords ||
		verified.scopeRecords != expectedScopeRecords {
		return fmt.Errorf(
			"%w: compacted token catalog has %d/%d parent and %d/%d scope rows",
			errCollectorTokenCatalogInconsistent,
			verified.tokenRecords,
			expectedTokenRecords,
			verified.scopeRecords,
			expectedScopeRecords,
		)
	}
	if verified.tokenRecords+1 > store.totalTokenRecordLimit ||
		verified.scopeRecords+newScopeCount >
			maximumTotalTokenScopeRecordLimit {
		return fmt.Errorf(
			"%w: collector token catalog reached its configured record limit",
			control.ErrCapacityExceeded,
		)
	}
	return nil
}

type collectorTokenScopeDistributionSummary struct {
	nonRevokedRecords int
	nonRevokedScopes  int
	revokedScopes     []int
}

func collectorTokenScopeDistribution(
	database *gorm.DB,
	catalog collectorTokenCatalogCounts,
) (collectorTokenScopeDistributionSummary, error) {
	var rows []collectorTokenScopeDistributionRow
	query := database.
		Table("ingestion_tokens AS token").
		Select(`
			CASE
				WHEN token.state = 'revoked' THEN 1
				WHEN token.state IN ('active', 'disabled') THEN 0
				ELSE -1
			END AS state_kind,
			COUNT(scope.index_id) AS scope_count,
			COUNT(target.index_id) AS target_count`).
		Joins(`
			LEFT JOIN ingestion_token_indexes AS scope
			  ON scope.ingestion_token_id = token.ingestion_token_id`).
		Joins(`
			LEFT JOIN indexes AS target
			  ON target.index_id = scope.index_id`).
		Group("token.ingestion_token_id").
		Order("state_kind DESC").
		Order("token.revoked_at_unix_micro DESC").
		Order("token.ingestion_token_id DESC").
		Limit(maximumTotalTokenRecordLimit + 1).
		Scan(&rows)
	if query.Error != nil {
		return collectorTokenScopeDistributionSummary{}, query.Error
	}
	if len(rows) != catalog.tokenRecords {
		return collectorTokenScopeDistributionSummary{}, fmt.Errorf(
			"%w: physical token rows = %d, scope distribution rows = %d",
			errCollectorTokenCatalogInconsistent,
			catalog.tokenRecords,
			len(rows),
		)
	}

	result := collectorTokenScopeDistributionSummary{
		revokedScopes: make([]int, 0, len(rows)),
	}
	totalScopes := 0
	seenNonRevoked := false
	for _, row := range rows {
		if row.StateKind != 0 && row.StateKind != 1 ||
			row.ScopeCount < 1 ||
			row.ScopeCount > maximumTokenScopes ||
			row.TargetCount != row.ScopeCount {
			return collectorTokenScopeDistributionSummary{}, fmt.Errorf(
				"%w: collector token scope distribution is malformed",
				errCollectorTokenCatalogInconsistent,
			)
		}
		scopeCount := int(row.ScopeCount)
		totalScopes += scopeCount
		if row.StateKind == 1 {
			if seenNonRevoked {
				return collectorTokenScopeDistributionSummary{}, fmt.Errorf(
					"%w: revoked scope distribution order is unstable",
					errCollectorTokenCatalogInconsistent,
				)
			}
			result.revokedScopes = append(result.revokedScopes, scopeCount)
			continue
		}
		seenNonRevoked = true
		result.nonRevokedRecords++
		result.nonRevokedScopes += scopeCount
	}
	if totalScopes != catalog.scopeRecords {
		return collectorTokenScopeDistributionSummary{}, fmt.Errorf(
			"%w: physical scope rows = %d, known-parent scope rows = %d",
			errCollectorTokenCatalogInconsistent,
			catalog.scopeRecords,
			totalScopes,
		)
	}
	return result, nil
}

func collectorTokenRecordCountProbe(
	database *gorm.DB,
	limit int,
) (int, bool, error) {
	return boundedCollectorTokenQueryCount(
		database,
		database.Model(&collectorTokenRecord{}),
		limit,
	)
}

func collectorTokenScopeRecordCountProbe(
	database *gorm.DB,
	limit int,
) (int, bool, error) {
	return boundedCollectorTokenQueryCount(
		database,
		database.Model(&collectorTokenIndexRecord{}),
		limit,
	)
}

func collectorTokenConstraintRecordCountProbe(
	database *gorm.DB,
	limit int,
) (int, bool, error) {
	return boundedCollectorTokenQueryCount(
		database,
		database.Model(&collectorTokenConstraintRecord{}),
		limit,
	)
}

func boundedCollectorTokenQueryCount(
	database *gorm.DB,
	query *gorm.DB,
	limit int,
) (int, bool, error) {
	if limit < 0 {
		return 0, false, errors.New("collector token count limit is negative")
	}
	limited := query.Select("1").Limit(limit + 1)
	var projection struct {
		RecordCount int64 `gorm:"column:record_count"`
	}
	result := database.
		Table("(?) AS bounded_records", limited).
		Select("COUNT(*) AS record_count").
		Scan(&projection)
	if result.Error != nil {
		return 0, false, result.Error
	}
	if projection.RecordCount < 0 || projection.RecordCount > int64(limit+1) {
		return 0, false, errors.New("collector token count is outside the supported range")
	}
	count := int(projection.RecordCount)
	return count, count > limit, nil
}

type collectorTokenCatalogCounts struct {
	tokenRecords int
	scopeRecords int
}

func inspectCollectorTokenCatalog(
	database *gorm.DB,
) (collectorTokenCatalogCounts, error) {
	tokenRecords, overLimit, err := collectorTokenRecordCountProbe(
		database,
		maximumTotalTokenRecordLimit,
	)
	if err != nil {
		return collectorTokenCatalogCounts{}, fmt.Errorf(
			"count physical collector token records: %w",
			err,
		)
	}
	if overLimit {
		return collectorTokenCatalogCounts{}, fmt.Errorf(
			"%w: token records exceed the structural maximum of %d",
			errCollectorTokenCatalogOverflow,
			maximumTotalTokenRecordLimit,
		)
	}
	scopeRecords, overLimit, err := collectorTokenScopeRecordCountProbe(
		database,
		maximumTotalTokenScopeRecordLimit,
	)
	if err != nil {
		return collectorTokenCatalogCounts{}, fmt.Errorf(
			"count physical collector token scope records: %w",
			err,
		)
	}
	if overLimit {
		return collectorTokenCatalogCounts{}, fmt.Errorf(
			"%w: token scope records exceed the structural maximum of %d",
			errCollectorTokenCatalogOverflow,
			maximumTotalTokenScopeRecordLimit,
		)
	}
	return collectorTokenCatalogCounts{
		tokenRecords: tokenRecords,
		scopeRecords: scopeRecords,
	}, nil
}

func ensureCollectorTokenScopeUpdateCapacity(
	database *gorm.DB,
	currentScopes int,
	replacementScopes int,
) error {
	if currentScopes < 1 || currentScopes > maximumTokenScopes ||
		replacementScopes < 1 || replacementScopes > maximumTokenScopes {
		return fmt.Errorf(
			"%w: collector token scope replacement is outside per-token bounds",
			errCollectorTokenCatalogInconsistent,
		)
	}
	scopeRecords, overLimit, err := collectorTokenScopeRecordCountProbe(
		database,
		maximumTotalTokenScopeRecordLimit,
	)
	if err != nil {
		return fmt.Errorf("count collector token scopes for update: %w", err)
	}
	if overLimit {
		return fmt.Errorf(
			"%w: token scope records exceed the structural maximum of %d",
			errCollectorTokenCatalogOverflow,
			maximumTotalTokenScopeRecordLimit,
		)
	}
	projected := scopeRecords - currentScopes + replacementScopes
	if projected < 0 {
		return fmt.Errorf(
			"%w: collector token scope replacement underflowed physical state",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if projected > maximumTotalTokenScopeRecordLimit {
		return fmt.Errorf(
			"%w: collector token scope catalog reached its record limit",
			control.ErrCapacityExceeded,
		)
	}
	return nil
}

// ListCollectorTokens lists safe metadata in creation order.
func (store *Store) ListCollectorTokens(
	ctx context.Context,
) (
	tokens []CollectorToken,
	returnedErr error,
) {
	tx := store.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return nil, fmt.Errorf("begin collector token list: %w", tx.Error)
	}
	transactionFinished := false
	defer finishTokenTransaction(tx, &transactionFinished, &returnedErr)

	now := databaseTime(store.now())
	tokens, err := listCollectorTokenMetadata(tx, now)
	if err != nil {
		return nil, fmt.Errorf("list collector tokens: %w", err)
	}
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return nil, fmt.Errorf("commit collector token list: %w", commitErr)
	}
	return tokens, nil
}

// SetCollectorTokenEnabled atomically transitions a live token between the
// active and disabled states under optimistic locking. Expiration and
// revocation are terminal for this operation: neither state can be used to
// reactivate a credential. The successful audit event is appended in the same
// transaction and uses the existing ingestion_token.update taxonomy.
func (store *Store) SetCollectorTokenEnabled(
	ctx context.Context,
	tokenID string,
	expectedVersion uint64,
	enabled bool,
) (result CollectorToken, err error) {
	if err := store.validateTokenMutationActor(ctx); err != nil {
		return CollectorToken{}, err
	}
	if strings.TrimSpace(tokenID) == "" {
		return CollectorToken{}, fmt.Errorf(
			"%w: token ID is required",
			control.ErrInvalidArgument,
		)
	}
	if expectedVersion == 0 || expectedVersion > math.MaxInt64 {
		return CollectorToken{}, fmt.Errorf(
			"%w: expected token version is outside the supported range",
			control.ErrInvalidArgument,
		)
	}

	now := databaseTime(store.now())
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return CollectorToken{}, fmt.Errorf(
			"begin collector token state update: %w",
			tx.Error,
		)
	}
	transactionFinished := false
	defer finishTokenTransaction(tx, &transactionFinished, &err)

	current, err := takeCollectorTokenMetadata(tx, tokenID, now)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return CollectorToken{}, control.ErrNotFound
	}
	if err != nil {
		return CollectorToken{}, fmt.Errorf(
			"read collector token for state update: %w",
			err,
		)
	}
	if current.Version != expectedVersion {
		return CollectorToken{}, control.ErrVersionConflict
	}
	if current.State == CollectorTokenStateRevoked ||
		!current.ExpiresAt.IsZero() && !current.ExpiresAt.After(now) {
		return CollectorToken{}, ErrInactiveToken
	}

	targetState := CollectorTokenStateDisabled
	if enabled {
		targetState = CollectorTokenStateActive
	}
	// #nosec G115 -- expectedVersion is bounded above by math.MaxInt64.
	expectedVersionDB := int64(expectedVersion)
	update := tx.Model(&collectorTokenRecord{}).
		Where(
			`ingestion_token_id = ?
			 AND version = ?
			 AND state IN (?, ?)
			 AND (expires_at_unix_micro IS NULL OR expires_at_unix_micro > ?)`,
			tokenID,
			expectedVersionDB,
			CollectorTokenStateActive,
			CollectorTokenStateDisabled,
			now.UnixMicro(),
		).
		Updates(map[string]any{
			"state":                 targetState,
			"version":               gorm.Expr("version + 1"),
			"updated_at_unix_micro": now.UnixMicro(),
		})
	if update.Error != nil {
		return CollectorToken{}, fmt.Errorf(
			"set collector token enabled state: %w",
			update.Error,
		)
	}
	if update.RowsAffected != 1 {
		return CollectorToken{}, control.ErrVersionConflict
	}

	result, err = takeCollectorTokenMetadata(tx, tokenID, now)
	if err != nil {
		return CollectorToken{}, fmt.Errorf(
			"read state-updated collector token: %w",
			err,
		)
	}
	if result.State != targetState || !result.RevokedAt.IsZero() {
		return CollectorToken{}, fmt.Errorf(
			"%w: collector token state transition produced an invalid lifecycle",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if _, err := store.auditAppender.AppendInTransaction(
		ctx,
		tx,
		store.auditTenantID,
		audit.SuccessfulEvent{
			OccurredAt:    now,
			Action:        audit.ActionIngestionTokenUpdate,
			TargetKind:    audit.TargetKindIngestionToken,
			TargetID:      result.ID,
			TargetVersion: result.Version,
		},
	); err != nil {
		return CollectorToken{}, fmt.Errorf(
			"append collector token state update audit event: %w",
			err,
		)
	}
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return CollectorToken{}, fmt.Errorf(
			"commit collector token state update: %w",
			commitErr,
		)
	}
	return result, nil
}

// RevokeCollectorToken irreversibly revokes a token under optimistic locking.
func (store *Store) RevokeCollectorToken(ctx context.Context, tokenID string, expectedVersion uint64) (result CollectorToken, err error) {
	if err := store.validateTokenMutationActor(ctx); err != nil {
		return CollectorToken{}, err
	}
	if expectedVersion == 0 || expectedVersion > math.MaxInt64 {
		return CollectorToken{}, fmt.Errorf("%w: expected token version is outside the supported range", control.ErrInvalidArgument)
	}
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return CollectorToken{}, fmt.Errorf("begin collector token revocation: %w", tx.Error)
	}
	transactionFinished := false
	defer finishTokenTransaction(tx, &transactionFinished, &err)

	var current collectorTokenRecord
	queryErr := tx.Select("ingestion_token_id", "version").
		Where("ingestion_token_id = ?", tokenID).
		Take(&current).Error
	if errors.Is(queryErr, gorm.ErrRecordNotFound) {
		return CollectorToken{}, control.ErrNotFound
	}
	if queryErr != nil {
		return CollectorToken{}, fmt.Errorf("read collector token for revocation: %w", queryErr)
	}
	if current.Version < 1 {
		return CollectorToken{}, errors.New("invalid collector token version in control-plane database")
	}
	// #nosec G115 -- expectedVersion is bounded above by math.MaxInt64.
	expectedVersionDB := int64(expectedVersion)
	if current.Version != expectedVersionDB {
		return CollectorToken{}, control.ErrVersionConflict
	}
	catalog, err := inspectCollectorTokenCatalog(tx)
	if err != nil {
		return CollectorToken{}, fmt.Errorf(
			"inspect collector token catalog before revocation: %w",
			err,
		)
	}
	if err := preflightAllCollectorTokenProjectionWidths(
		tx,
		catalog.tokenRecords,
	); err != nil {
		return CollectorToken{}, fmt.Errorf(
			"inspect collector token catalog before revocation: %w",
			err,
		)
	}
	if _, err := collectorTokenScopeDistribution(tx, catalog); err != nil {
		return CollectorToken{}, fmt.Errorf(
			"inspect collector token scopes before revocation: %w",
			err,
		)
	}

	now := databaseTime(store.now())
	update := tx.Model(&collectorTokenRecord{}).
		Where(
			"ingestion_token_id = ? AND version = ? AND state != ?",
			tokenID,
			expectedVersionDB,
			CollectorTokenStateRevoked,
		).
		Updates(map[string]any{
			"state":                 CollectorTokenStateRevoked,
			"version":               gorm.Expr("version + 1"),
			"updated_at_unix_micro": now.UnixMicro(),
			"revoked_at_unix_micro": now.UnixMicro(),
		})
	if update.Error != nil {
		return CollectorToken{}, fmt.Errorf("revoke collector token: %w", update.Error)
	}
	if update.RowsAffected != 1 {
		return CollectorToken{}, control.ErrVersionConflict
	}

	result, err = takeCollectorTokenMetadata(tx, tokenID, now)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read revoked collector token: %w", err)
	}
	if err := store.pruneRevokedCollectorTokenTombstones(tx, tokenID); err != nil {
		return CollectorToken{}, fmt.Errorf("prune revoked collector token tombstones: %w", err)
	}
	if _, err := store.auditAppender.AppendInTransaction(
		ctx,
		tx,
		store.auditTenantID,
		audit.SuccessfulEvent{
			OccurredAt:    now,
			Action:        audit.ActionIngestionTokenRevoke,
			TargetKind:    audit.TargetKindIngestionToken,
			TargetID:      result.ID,
			TargetVersion: result.Version,
		},
	); err != nil {
		return CollectorToken{}, fmt.Errorf(
			"append collector token revocation audit event: %w",
			err,
		)
	}
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return CollectorToken{}, fmt.Errorf("commit collector token revocation: %w", commitErr)
	}
	return result, nil
}

func (store *Store) pruneRevokedCollectorTokenTombstones(
	tx *gorm.DB,
	currentTokenID string,
) error {
	return deleteRevokedCollectorTokenVictims(
		tx,
		store.retainedRevokedTokenLimit-1,
		currentTokenID,
	).Error
}

func revokedCollectorTokenVictimQuery(
	tx *gorm.DB,
	retainedTokens int,
	currentTokenID string,
) *gorm.DB {
	victims := tx.
		Model(&collectorTokenRecord{}).
		Select("ingestion_token_id").
		Where("state = ?", CollectorTokenStateRevoked).
		Order("revoked_at_unix_micro DESC").
		Order("ingestion_token_id DESC")
	if currentTokenID != "" {
		victims = victims.Where(
			"ingestion_token_id != ?",
			currentTokenID,
		)
	}
	if retainedTokens > 0 {
		victims = victims.Offset(retainedTokens)
	}
	return victims
}

func deleteRevokedCollectorTokenVictims(
	tx *gorm.DB,
	retainedTokens int,
	currentTokenID string,
) *gorm.DB {
	victims := revokedCollectorTokenVictimQuery(
		tx,
		retainedTokens,
		currentTokenID,
	)
	deletion := tx.Where("state = ?", CollectorTokenStateRevoked)
	if currentTokenID != "" {
		deletion = deletion.Where(
			"ingestion_token_id != ?",
			currentTokenID,
		)
	}
	return deletion.
		Where("ingestion_token_id IN (?)", victims).
		Delete(&collectorTokenRecord{})
}

func collectorTokenParentProjectionQuery(database *gorm.DB) *gorm.DB {
	return database.
		Table("ingestion_tokens AS token").
		Select(`
			token.ingestion_token_id,
			token.version,
			token.name,
			token.description,
			token.token_prefix,
			token.state,
			token.created_at_unix_micro,
			token.updated_at_unix_micro,
			token.expires_at_unix_micro,
			token.revoked_at_unix_micro,
			token.last_used_at_unix_micro,
			token.bound_collector_id,
			token.purpose,
			CASE WHEN hec_profile.ingestion_token_id IS NULL THEN 0 ELSE 1 END
				AS hec_profile_present,
			CASE
				WHEN hec_profile.default_index_id IS NULL THEN 0
				WHEN hec_default_index.index_id IS NULL THEN -1
				ELSE 1
			END AS hec_default_index_target_present,
			hec_default_index.name AS hec_default_index_name,
			hec_profile.default_host AS hec_default_host,
			hec_profile.default_source AS hec_default_source,
			hec_profile.default_sourcetype AS hec_default_sourcetype,
			hec_profile.indexer_acknowledgment AS hec_indexer_acknowledgment,
			token.max_ingest_events_per_second,
			token.max_ingest_uncompressed_bytes_per_second`).
		Joins(`
			LEFT JOIN ingestion_token_hec_profiles AS hec_profile
			  ON hec_profile.ingestion_token_id = token.ingestion_token_id`).
		Joins(`
			LEFT JOIN indexes AS hec_default_index
			  ON hec_default_index.index_id = hec_profile.default_index_id`)
}

func collectorTokenProjectionWidthQuery(database *gorm.DB) *gorm.DB {
	return database.
		Table("ingestion_tokens AS token").
		Select(`
			length(CAST(token.ingestion_token_id AS BLOB))
				AS ingestion_token_id_bytes,
			length(CAST(token.name AS BLOB)) AS name_bytes,
			length(CAST(token.description AS BLOB)) AS description_bytes,
			length(CAST(token.token_prefix AS BLOB)) AS token_prefix_bytes,
			length(CAST(token.bound_collector_id AS BLOB))
				AS bound_collector_id_bytes,
			length(CAST(token.purpose AS BLOB)) AS purpose_bytes,
			length(CAST(hec_default_index.name AS BLOB))
				AS hec_default_index_name_bytes,
			length(CAST(hec_profile.default_host AS BLOB))
				AS hec_default_host_bytes,
			length(CAST(hec_profile.default_source AS BLOB))
				AS hec_default_source_bytes,
			length(CAST(hec_profile.default_sourcetype AS BLOB))
				AS hec_default_sourcetype_bytes`).
		Joins(`
			LEFT JOIN ingestion_token_hec_profiles AS hec_profile
			  ON hec_profile.ingestion_token_id = token.ingestion_token_id`).
		Joins(`
			LEFT JOIN indexes AS hec_default_index
			  ON hec_default_index.index_id = hec_profile.default_index_id`)
}

func validateCollectorTokenProjectionWidths(
	projection collectorTokenProjectionWidths,
) error {
	if projection.IngestionTokenIDBytes < 1 ||
		projection.IngestionTokenIDBytes > maximumTokenIDBytes {
		return fmt.Errorf(
			"%w: collector token ID exceeds persisted byte bounds",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if projection.NameBytes < 1 ||
		projection.NameBytes > maximumTokenNameBytes {
		return fmt.Errorf(
			"%w: collector token name exceeds persisted byte bounds",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if projection.DescriptionBytes < 0 ||
		projection.DescriptionBytes > maximumDescriptionBytes {
		return fmt.Errorf(
			"%w: collector token description exceeds persisted byte bounds",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if projection.TokenPrefixBytes < minimumTokenPrefixBytes ||
		projection.TokenPrefixBytes > maximumTokenPrefixBytes {
		return fmt.Errorf(
			"%w: collector token prefix exceeds persisted byte bounds",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if projection.BoundCollectorIDBytes != nil &&
		(*projection.BoundCollectorIDBytes < 1 ||
			*projection.BoundCollectorIDBytes > maximumCollectorIDBytes) {
		return fmt.Errorf(
			"%w: collector token bound collector ID exceeds persisted byte bounds",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if projection.PurposeBytes != int64(len(IngestionTokenPurposeHEC)) &&
		projection.PurposeBytes != int64(len(IngestionTokenPurposeNativeCollector)) {
		return fmt.Errorf(
			"%w: ingestion token purpose exceeds persisted byte bounds",
			errCollectorTokenCatalogInconsistent,
		)
	}
	for _, width := range []*int64{
		projection.HECDefaultIndexNameBytes,
		projection.HECDefaultHostBytes,
		projection.HECDefaultSourceBytes,
		projection.HECDefaultSourcetypeBytes,
	} {
		if width != nil && (*width < 1 || *width > maximumHECMetadataBytes) {
			return fmt.Errorf(
				"%w: HEC token profile exceeds persisted byte bounds",
				errCollectorTokenCatalogInconsistent,
			)
		}
	}
	return nil
}

func preflightAllCollectorTokenProjectionWidths(
	database *gorm.DB,
	expectedRecords int,
) error {
	var widths []collectorTokenProjectionWidths
	query := collectorTokenProjectionWidthQuery(database).
		Limit(maximumTotalTokenRecordLimit + 1).
		Scan(&widths)
	if query.Error != nil {
		return fmt.Errorf("preflight collector token metadata widths: %w", query.Error)
	}
	if len(widths) != expectedRecords {
		return fmt.Errorf(
			"%w: physical token rows = %d, width rows = %d",
			errCollectorTokenCatalogInconsistent,
			expectedRecords,
			len(widths),
		)
	}
	for _, projection := range widths {
		if err := validateCollectorTokenProjectionWidths(projection); err != nil {
			return err
		}
	}
	return nil
}

func listCollectorTokenMetadata(
	database *gorm.DB,
	now time.Time,
) ([]CollectorToken, error) {
	catalog, err := inspectCollectorTokenCatalog(database)
	if err != nil {
		return nil, err
	}

	if err := preflightAllCollectorTokenProjectionWidths(
		database,
		catalog.tokenRecords,
	); err != nil {
		return nil, err
	}

	var rows []collectorTokenMetadataRow
	parentQuery := collectorTokenParentProjectionQuery(database).
		Order("token.created_at_unix_micro").
		Order("token.ingestion_token_id").
		Limit(maximumTotalTokenRecordLimit + 1).
		Scan(&rows)
	if parentQuery.Error != nil {
		return nil, fmt.Errorf("read collector token parent metadata: %w", parentQuery.Error)
	}
	if len(rows) != catalog.tokenRecords {
		return nil, fmt.Errorf(
			"%w: physical token rows = %d, parent rows = %d",
			errCollectorTokenCatalogInconsistent,
			catalog.tokenRecords,
			len(rows),
		)
	}
	constraintRecords, overLimit, err := collectorTokenConstraintRecordCountProbe(
		database,
		maximumTotalTokenConstraintRecords,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"count physical collector token constraint records: %w",
			err,
		)
	}
	if overLimit {
		return nil, fmt.Errorf(
			"%w: token constraint records exceed the structural maximum of %d",
			errCollectorTokenCatalogOverflow,
			maximumTotalTokenConstraintRecords,
		)
	}
	return hydrateCollectorTokenScopes(
		database,
		rows,
		catalog.scopeRecords,
		constraintRecords,
		true,
		now,
	)
}

func takeCollectorTokenMetadata(
	database *gorm.DB,
	tokenID string,
	now time.Time,
) (CollectorToken, error) {
	catalog, err := inspectCollectorTokenCatalog(database)
	if err != nil {
		return CollectorToken{}, err
	}

	var widths collectorTokenProjectionWidths
	widthQuery := collectorTokenProjectionWidthQuery(database).
		Where("token.ingestion_token_id = ?", tokenID).
		Take(&widths)
	if widthQuery.Error != nil {
		return CollectorToken{}, widthQuery.Error
	}
	if err := validateCollectorTokenProjectionWidths(widths); err != nil {
		return CollectorToken{}, err
	}

	var row collectorTokenMetadataRow
	parentQuery := collectorTokenParentProjectionQuery(database).
		Where("token.ingestion_token_id = ?", tokenID).
		Take(&row)
	if parentQuery.Error != nil {
		return CollectorToken{}, parentQuery.Error
	}
	tokens, err := hydrateCollectorTokenScopes(
		database,
		[]collectorTokenMetadataRow{row},
		catalog.scopeRecords,
		0,
		false,
		now,
	)
	if err != nil {
		return CollectorToken{}, err
	}
	if len(tokens) != 1 {
		return CollectorToken{}, fmt.Errorf(
			"%w: token metadata lookup returned %d parents",
			errCollectorTokenCatalogInconsistent,
			len(tokens),
		)
	}
	return tokens[0], nil
}

func hydrateCollectorTokenScopes(
	database *gorm.DB,
	parents []collectorTokenMetadataRow,
	globalPhysicalScopeCount int,
	globalPhysicalConstraintCount int,
	requireCompleteCatalog bool,
	now time.Time,
) ([]CollectorToken, error) {
	tokens := make([]CollectorToken, len(parents))
	parentIndexes := make(map[string]int, len(parents))
	parentIDs := make([]string, len(parents))
	for index, parent := range parents {
		token, err := collectorTokenFromMetadataRow(parent, now)
		if err != nil {
			return nil, err
		}
		if _, duplicate := parentIndexes[token.ID]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate collector token parent projection",
				errCollectorTokenCatalogInconsistent,
			)
		}
		tokens[index] = token
		parentIndexes[token.ID] = index
		parentIDs[index] = token.ID
	}
	if len(parents) == 0 {
		if requireCompleteCatalog && globalPhysicalScopeCount != 0 {
			return nil, fmt.Errorf(
				"%w: scope rows exist without collector token parents",
				errCollectorTokenCatalogInconsistent,
			)
		}
		return hydrateCollectorTokenConstraints(
			database,
			tokens,
			parentIndexes,
			parentIDs,
			globalPhysicalConstraintCount,
			requireCompleteCatalog,
		)
	}

	physicalScopeCount, overLimit, err := boundedCollectorTokenQueryCount(
		database,
		database.
			Model(&collectorTokenIndexRecord{}).
			Where("ingestion_token_id IN ?", parentIDs),
		maximumTotalTokenScopeRecordLimit,
	)
	if err != nil {
		return nil, fmt.Errorf("count selected collector token scopes: %w", err)
	}
	if overLimit {
		return nil, fmt.Errorf(
			"%w: selected scope rows exceed the structural maximum of %d",
			errCollectorTokenCatalogOverflow,
			maximumTotalTokenScopeRecordLimit,
		)
	}
	aggregateLimit := min(
		len(parents)*maximumTokenScopes,
		maximumTotalTokenScopeRecordLimit,
	)
	if physicalScopeCount > aggregateLimit {
		return nil, fmt.Errorf(
			"%w: selected scope rows exceed aggregate per-token bounds",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if requireCompleteCatalog &&
		physicalScopeCount != globalPhysicalScopeCount {
		return nil, fmt.Errorf(
			"%w: physical scope rows = %d, selected scope rows = %d",
			errCollectorTokenCatalogInconsistent,
			globalPhysicalScopeCount,
			physicalScopeCount,
		)
	}

	var widthRows []collectorTokenMetadataScopeWidths
	widthQuery := database.
		Table("ingestion_token_indexes AS scope").
		Select(`
			length(CAST(scope.ingestion_token_id AS BLOB))
				AS ingestion_token_id_bytes,
			length(CAST(target.name AS BLOB)) AS index_name_bytes`).
		Joins("LEFT JOIN indexes AS target ON target.index_id = scope.index_id").
		Where("scope.ingestion_token_id IN ?", parentIDs).
		Limit(aggregateLimit + 1).
		Scan(&widthRows)
	if widthQuery.Error != nil {
		return nil, fmt.Errorf("preflight collector token scope widths: %w", widthQuery.Error)
	}
	if len(widthRows) != physicalScopeCount {
		return nil, fmt.Errorf(
			"%w: physical scope rows = %d, width rows = %d",
			errCollectorTokenCatalogInconsistent,
			physicalScopeCount,
			len(widthRows),
		)
	}
	for _, projection := range widthRows {
		if projection.IngestionTokenIDBytes < 1 ||
			projection.IngestionTokenIDBytes > maximumTokenIDBytes ||
			projection.IndexNameBytes == nil ||
			*projection.IndexNameBytes < 1 ||
			*projection.IndexNameBytes > maximumTokenNameBytes {
			return nil, fmt.Errorf(
				"%w: collector token scope projection exceeds persisted byte bounds or has no target",
				errCollectorTokenCatalogInconsistent,
			)
		}
	}

	var scopes []collectorTokenMetadataScopeRow
	scopeQuery := database.
		Table("ingestion_token_indexes AS scope").
		Select(`
			scope.ingestion_token_id,
			target.name AS index_name`).
		Joins("LEFT JOIN indexes AS target ON target.index_id = scope.index_id").
		Where("scope.ingestion_token_id IN ?", parentIDs).
		Limit(aggregateLimit + 1).
		Scan(&scopes)
	if scopeQuery.Error != nil {
		return nil, fmt.Errorf("read collector token scopes: %w", scopeQuery.Error)
	}
	if len(scopes) != physicalScopeCount {
		return nil, fmt.Errorf(
			"%w: physical scope rows = %d, hydrated scope rows = %d",
			errCollectorTokenCatalogInconsistent,
			physicalScopeCount,
			len(scopes),
		)
	}
	sort.Slice(scopes, func(left, right int) bool {
		if scopes[left].IngestionTokenID != scopes[right].IngestionTokenID {
			return scopes[left].IngestionTokenID < scopes[right].IngestionTokenID
		}
		return scopes[left].IndexName.String < scopes[right].IndexName.String
	})
	for _, scope := range scopes {
		parentIndex, requested := parentIndexes[scope.IngestionTokenID]
		if !requested || !scope.IndexName.Valid {
			return nil, fmt.Errorf(
				"%w: collector token scope has an unknown parent or target",
				errCollectorTokenCatalogInconsistent,
			)
		}
		name, err := control.NormalizeIndexName(scope.IndexName.String)
		if err != nil || name != scope.IndexName.String {
			return nil, fmt.Errorf(
				"%w: collector token scope name is malformed",
				errCollectorTokenCatalogInconsistent,
			)
		}
		names := tokens[parentIndex].AllowedIndexNames
		if len(names) >= maximumTokenScopes ||
			len(names) != 0 && names[len(names)-1] >= name {
			return nil, fmt.Errorf(
				"%w: collector token scopes exceed per-token bounds or are duplicated",
				errCollectorTokenCatalogInconsistent,
			)
		}
		tokens[parentIndex].AllowedIndexNames = append(names, name)
	}
	for _, token := range tokens {
		if len(token.AllowedIndexNames) == 0 {
			return nil, fmt.Errorf(
				"%w: collector token is missing its required scope",
				errCollectorTokenCatalogInconsistent,
			)
		}
		if token.Purpose == IngestionTokenPurposeHEC &&
			token.HECProfile.DefaultIndexName != "" {
			if _, found := slices.BinarySearch(
				token.AllowedIndexNames,
				token.HECProfile.DefaultIndexName,
			); !found {
				return nil, fmt.Errorf(
					"%w: HEC default index is outside the token scope",
					errCollectorTokenCatalogInconsistent,
				)
			}
		}
	}
	return hydrateCollectorTokenConstraints(
		database,
		tokens,
		parentIndexes,
		parentIDs,
		globalPhysicalConstraintCount,
		requireCompleteCatalog,
	)
}

func hydrateCollectorTokenConstraints(
	database *gorm.DB,
	tokens []CollectorToken,
	parentIndexes map[string]int,
	parentIDs []string,
	globalPhysicalConstraintCount int,
	requireCompleteCatalog bool,
) ([]CollectorToken, error) {
	if len(parentIDs) == 0 {
		if requireCompleteCatalog && globalPhysicalConstraintCount != 0 {
			return nil, fmt.Errorf(
				"%w: constraint rows exist without collector token parents",
				errCollectorTokenCatalogInconsistent,
			)
		}
		return tokens, nil
	}

	aggregateLimit := len(parentIDs) * maximumTokenConstraintRecords
	physicalConstraintCount, overLimit, err := boundedCollectorTokenQueryCount(
		database,
		database.Model(&collectorTokenConstraintRecord{}).
			Where("ingestion_token_id IN ?", parentIDs),
		aggregateLimit,
	)
	if err != nil {
		return nil, fmt.Errorf("count selected collector token constraints: %w", err)
	}
	if overLimit {
		return nil, fmt.Errorf(
			"%w: selected constraint rows exceed aggregate per-token bounds",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if requireCompleteCatalog &&
		physicalConstraintCount != globalPhysicalConstraintCount {
		return nil, fmt.Errorf(
			"%w: physical constraint rows = %d, selected constraint rows = %d",
			errCollectorTokenCatalogInconsistent,
			globalPhysicalConstraintCount,
			physicalConstraintCount,
		)
	}

	var widths []collectorTokenConstraintWidths
	widthQuery := database.
		Table("ingestion_token_constraints AS constraint_row").
		Select(`
			length(CAST(constraint_row.ingestion_token_id AS BLOB))
				AS ingestion_token_id_bytes,
			length(CAST(constraint_row.constraint_kind AS BLOB))
				AS constraint_kind_bytes,
			length(CAST(constraint_row.pattern AS BLOB)) AS pattern_bytes`).
		Where("constraint_row.ingestion_token_id IN ?", parentIDs).
		Limit(aggregateLimit + 1).
		Scan(&widths)
	if widthQuery.Error != nil {
		return nil, fmt.Errorf(
			"preflight collector token constraint widths: %w",
			widthQuery.Error,
		)
	}
	if len(widths) != physicalConstraintCount {
		return nil, fmt.Errorf(
			"%w: physical constraint rows = %d, width rows = %d",
			errCollectorTokenCatalogInconsistent,
			physicalConstraintCount,
			len(widths),
		)
	}
	for _, projection := range widths {
		if projection.IngestionTokenIDBytes < 1 ||
			projection.IngestionTokenIDBytes > maximumTokenIDBytes ||
			projection.ConstraintKindBytes < int64(len(collectorTokenConstraintKindHost)) ||
			projection.ConstraintKindBytes > int64(len(collectorTokenConstraintKindSource)) ||
			projection.PatternBytes < 1 ||
			projection.PatternBytes > tokenconstraint.MaximumPatternBytes {
			return nil, fmt.Errorf(
				"%w: collector token constraint projection exceeds persisted byte bounds",
				errCollectorTokenCatalogInconsistent,
			)
		}
	}

	var rows []collectorTokenConstraintRow
	rowQuery := database.
		Table("ingestion_token_constraints AS constraint_row").
		Select(
			"constraint_row.ingestion_token_id",
			"constraint_row.constraint_kind",
			"constraint_row.ordinal",
			"constraint_row.pattern",
		).
		Where("constraint_row.ingestion_token_id IN ?", parentIDs).
		Order("constraint_row.ingestion_token_id").
		Order("constraint_row.constraint_kind").
		Order("constraint_row.ordinal").
		Limit(aggregateLimit + 1).
		Scan(&rows)
	if rowQuery.Error != nil {
		return nil, fmt.Errorf("read collector token constraints: %w", rowQuery.Error)
	}
	if len(rows) != physicalConstraintCount {
		return nil, fmt.Errorf(
			"%w: physical constraint rows = %d, hydrated constraint rows = %d",
			errCollectorTokenCatalogInconsistent,
			physicalConstraintCount,
			len(rows),
		)
	}
	for _, row := range rows {
		parentIndex, requested := parentIndexes[row.IngestionTokenID]
		if !requested {
			return nil, fmt.Errorf(
				"%w: collector token constraint has an unknown parent",
				errCollectorTokenCatalogInconsistent,
			)
		}
		var dimension *[]string
		switch row.ConstraintKind {
		case collectorTokenConstraintKindHost:
			dimension = &tokens[parentIndex].AllowedHostRegexes
		case collectorTokenConstraintKindSource:
			dimension = &tokens[parentIndex].AllowedSourceRegexes
		default:
			return nil, fmt.Errorf(
				"%w: collector token constraint has an unknown kind",
				errCollectorTokenCatalogInconsistent,
			)
		}
		if row.Ordinal != int64(len(*dimension)) ||
			len(*dimension) >= tokenconstraint.MaximumPatternsPerDimension {
			return nil, fmt.Errorf(
				"%w: collector token constraint ordinals are malformed",
				errCollectorTokenCatalogInconsistent,
			)
		}
		*dimension = append(*dimension, row.Pattern)
	}
	for index := range tokens {
		if err := tokenconstraint.ValidateNormalized(
			tokens[index].AllowedHostRegexes,
		); err != nil {
			return nil, fmt.Errorf(
				"%w: invalid collector token host constraints: %w",
				errCollectorTokenCatalogInconsistent,
				err,
			)
		}
		if err := tokenconstraint.ValidateNormalized(
			tokens[index].AllowedSourceRegexes,
		); err != nil {
			return nil, fmt.Errorf(
				"%w: invalid collector token source constraints: %w",
				errCollectorTokenCatalogInconsistent,
				err,
			)
		}
	}
	return tokens, nil
}

func collectorTokenFromMetadataRow(
	row collectorTokenMetadataRow,
	now time.Time,
) (CollectorToken, error) {
	if !validAuthenticationTokenID(row.IngestionTokenID) {
		return CollectorToken{}, fmt.Errorf(
			"%w: invalid collector token ID in control-plane database",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if row.Version < 1 {
		return CollectorToken{}, fmt.Errorf(
			"%w: invalid collector token version in control-plane database",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if len(row.Name) < 1 || len(row.Name) > maximumTokenNameBytes ||
		!utf8.ValidString(row.Name) || strings.IndexByte(row.Name, 0) >= 0 {
		return CollectorToken{}, fmt.Errorf(
			"%w: invalid collector token name in control-plane database",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if len(row.Description) > maximumDescriptionBytes ||
		!utf8.ValidString(row.Description) ||
		strings.IndexByte(row.Description, 0) >= 0 {
		return CollectorToken{}, fmt.Errorf(
			"%w: invalid collector token description in control-plane database",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if len(row.TokenPrefix) < minimumTokenPrefixBytes ||
		len(row.TokenPrefix) > maximumTokenPrefixBytes ||
		!utf8.ValidString(row.TokenPrefix) ||
		strings.IndexByte(row.TokenPrefix, 0) >= 0 {
		return CollectorToken{}, fmt.Errorf(
			"%w: invalid collector token prefix in control-plane database",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if row.State != CollectorTokenStateActive &&
		row.State != CollectorTokenStateDisabled &&
		row.State != CollectorTokenStateRevoked {
		return CollectorToken{}, fmt.Errorf(
			"%w: invalid collector token state in control-plane database",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if row.Purpose != IngestionTokenPurposeNativeCollector &&
		row.Purpose != IngestionTokenPurposeHEC {
		return CollectorToken{}, fmt.Errorf(
			"%w: invalid ingestion token purpose in control-plane database",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if row.MaxIngestEventsPerSecond < 0 ||
		row.MaxIngestUncompressedBytesPerSecond < 0 {
		return CollectorToken{}, fmt.Errorf(
			"%w: invalid collector token rate limits in control-plane database",
			errCollectorTokenCatalogInconsistent,
		)
	}
	rateLimits := ingestquota.Limits{
		MaxEventsPerSecond: uint64(row.MaxIngestEventsPerSecond),
		MaxUncompressedBytesPerSecond: uint64(
			row.MaxIngestUncompressedBytesPerSecond,
		),
	}
	if err := rateLimits.Validate(); err != nil {
		return CollectorToken{}, fmt.Errorf(
			"%w: invalid collector token rate limits in control-plane database",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if row.UpdatedAtUnixMicro < row.CreatedAtUnixMicro ||
		row.ExpiresAtUnixMicro != nil &&
			*row.ExpiresAtUnixMicro <= row.CreatedAtUnixMicro ||
		row.LastUsedAtUnixMicro != nil &&
			*row.LastUsedAtUnixMicro < row.CreatedAtUnixMicro ||
		(row.State == CollectorTokenStateRevoked) !=
			(row.RevokedAtUnixMicro != nil) ||
		row.RevokedAtUnixMicro != nil &&
			*row.RevokedAtUnixMicro < row.CreatedAtUnixMicro {
		return CollectorToken{}, fmt.Errorf(
			"%w: invalid collector token lifecycle in control-plane database",
			errCollectorTokenCatalogInconsistent,
		)
	}

	token := CollectorToken{
		ID:                  row.IngestionTokenID,
		Version:             uint64(row.Version),
		Name:                row.Name,
		Description:         row.Description,
		Prefix:              row.TokenPrefix,
		State:               row.State,
		Purpose:             row.Purpose,
		IngestionRateLimits: rateLimits,
		CreatedAt:           time.UnixMicro(row.CreatedAtUnixMicro).UTC(),
		UpdatedAt:           time.UnixMicro(row.UpdatedAtUnixMicro).UTC(),
	}
	if row.BoundCollectorID != nil {
		if !validCollectorID(*row.BoundCollectorID) {
			return CollectorToken{}, fmt.Errorf(
				"%w: invalid collector token bound collector ID in control-plane database",
				errCollectorTokenCatalogInconsistent,
			)
		}
		token.BoundCollectorID = *row.BoundCollectorID
	}
	if row.Purpose == IngestionTokenPurposeNativeCollector {
		if row.HECProfilePresent != 0 ||
			row.HECDefaultIndexTargetPresent != 0 ||
			row.HECDefaultIndexName != nil ||
			row.HECDefaultHost != nil ||
			row.HECDefaultSource != nil ||
			row.HECDefaultSourcetype != nil ||
			row.HECIndexerAcknowledgment != nil {
			return CollectorToken{}, fmt.Errorf(
				"%w: native collector token has a HEC profile",
				errCollectorTokenCatalogInconsistent,
			)
		}
	} else {
		if token.BoundCollectorID != "" ||
			row.HECProfilePresent != 1 ||
			row.HECIndexerAcknowledgment == nil ||
			(*row.HECIndexerAcknowledgment != 0 &&
				*row.HECIndexerAcknowledgment != 1) ||
			(row.HECDefaultIndexName == nil &&
				row.HECDefaultIndexTargetPresent != 0) ||
			(row.HECDefaultIndexName != nil &&
				row.HECDefaultIndexTargetPresent != 1) {
			return CollectorToken{}, fmt.Errorf(
				"%w: HEC token profile is missing or inconsistent",
				errCollectorTokenCatalogInconsistent,
			)
		}
		if row.HECDefaultIndexName != nil {
			canonical, err := control.NormalizeIndexName(*row.HECDefaultIndexName)
			if err != nil || canonical != *row.HECDefaultIndexName {
				return CollectorToken{}, fmt.Errorf(
					"%w: HEC token default index is invalid",
					errCollectorTokenCatalogInconsistent,
				)
			}
			token.HECProfile.DefaultIndexName = *row.HECDefaultIndexName
		}
		for label, stored := range []struct {
			value *string
			set   *string
		}{
			{row.HECDefaultHost, &token.HECProfile.DefaultHost},
			{row.HECDefaultSource, &token.HECProfile.DefaultSource},
			{row.HECDefaultSourcetype, &token.HECProfile.DefaultSourcetype},
		} {
			if stored.value == nil {
				continue
			}
			if !validHECMetadataDefault(*stored.value) {
				return CollectorToken{}, fmt.Errorf(
					"%w: HEC token metadata default %d is invalid",
					errCollectorTokenCatalogInconsistent,
					label,
				)
			}
			*stored.set = *stored.value
		}
		token.HECProfile.IndexerAcknowledgment =
			*row.HECIndexerAcknowledgment == 1
	}
	if row.ExpiresAtUnixMicro != nil {
		token.ExpiresAt = time.UnixMicro(*row.ExpiresAtUnixMicro).UTC()
		if token.State == CollectorTokenStateActive &&
			!token.ExpiresAt.After(now) {
			token.State = CollectorTokenStateExpired
		}
	}
	if row.RevokedAtUnixMicro != nil {
		token.RevokedAt = time.UnixMicro(*row.RevokedAtUnixMicro).UTC()
	}
	if row.LastUsedAtUnixMicro != nil {
		token.LastUsedAt = time.UnixMicro(*row.LastUsedAtUnixMicro).UTC()
	}
	return token, nil
}

func normalizeTokenScopes(inputs []string) ([]string, error) {
	if len(inputs) == 0 || len(inputs) > maximumTokenScopes {
		return nil, fmt.Errorf("%w: collector tokens require between 1 and %d explicit index scopes", control.ErrInvalidArgument, maximumTokenScopes)
	}
	unique := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		name, err := control.NormalizeIndexName(input)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid collector token index scope", control.ErrInvalidArgument)
		}
		unique[name] = struct{}{}
	}
	names := make([]string, 0, len(unique))
	for name := range unique {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func normalizeCollectorTokenConstraints(
	hostPatterns []string,
	sourcePatterns []string,
) ([]string, []string, error) {
	normalizedHosts, err := tokenconstraint.Normalize(hostPatterns)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"%w: invalid collector token host constraints: %w",
			control.ErrInvalidArgument,
			err,
		)
	}
	normalizedSources, err := tokenconstraint.Normalize(sourcePatterns)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"%w: invalid collector token source constraints: %w",
			control.ErrInvalidArgument,
			err,
		)
	}
	return normalizedHosts, normalizedSources, nil
}

func collectorTokenConstraintRecords(
	tokenID string,
	hostPatterns []string,
	sourcePatterns []string,
) []collectorTokenConstraintRecord {
	records := make(
		[]collectorTokenConstraintRecord,
		0,
		len(hostPatterns)+len(sourcePatterns),
	)
	appendDimension := func(
		kind collectorTokenConstraintKind,
		patterns []string,
	) {
		for ordinal, pattern := range patterns {
			records = append(records, collectorTokenConstraintRecord{
				IngestionTokenID: tokenID,
				ConstraintKind:   kind,
				Ordinal:          int64(ordinal),
				Pattern:          pattern,
			})
		}
	}
	appendDimension(collectorTokenConstraintKindHost, hostPatterns)
	appendDimension(collectorTokenConstraintKindSource, sourcePatterns)
	return records
}

func normalizeCreatedTokenPurpose(
	purpose IngestionTokenPurpose,
	profile HECTokenProfile,
	boundCollectorID string,
	allowedIndexNames []string,
) (IngestionTokenPurpose, HECTokenProfile, error) {
	// Direct/internal callers created native tokens before purpose existed.
	// Preserve that Go API behavior while every persisted row receives the
	// canonical native purpose. The protobuf administrator boundary separately
	// permits this inference only when the legacy required binding proves native
	// intent; an unbound request can never be inferred as HEC.
	if purpose == "" {
		purpose = IngestionTokenPurposeNativeCollector
	}
	switch purpose {
	case IngestionTokenPurposeNativeCollector:
		if profile != (HECTokenProfile{}) {
			return "", HECTokenProfile{}, fmt.Errorf(
				"%w: native collector tokens cannot have a HEC profile",
				control.ErrInvalidArgument,
			)
		}
		if !validCollectorID(boundCollectorID) {
			return "", HECTokenProfile{}, fmt.Errorf(
				"%w: bound collector ID must be a canonical identifier containing between 1 and %d ASCII bytes",
				control.ErrInvalidArgument,
				maximumCollectorIDBytes,
			)
		}
		return purpose, HECTokenProfile{}, nil
	case IngestionTokenPurposeHEC:
		if boundCollectorID != "" {
			return "", HECTokenProfile{}, fmt.Errorf(
				"%w: HEC tokens cannot have a bound collector ID",
				control.ErrInvalidArgument,
			)
		}
		normalized, err := normalizeHECTokenProfile(profile, allowedIndexNames)
		if err != nil {
			return "", HECTokenProfile{}, err
		}
		return purpose, normalized, nil
	default:
		return "", HECTokenProfile{}, fmt.Errorf(
			"%w: ingestion token purpose is invalid",
			control.ErrInvalidArgument,
		)
	}
}

func normalizeUpdatedTokenPurpose(
	current CollectorToken,
	requestedPurpose IngestionTokenPurpose,
	requestedProfile HECTokenProfile,
	requestedBoundCollectorID string,
	allowedIndexNames []string,
) (IngestionTokenPurpose, HECTokenProfile, string, error) {
	if current.Purpose != IngestionTokenPurposeNativeCollector &&
		current.Purpose != IngestionTokenPurposeHEC {
		return "", HECTokenProfile{}, "", fmt.Errorf(
			"%w: ingestion token purpose is invalid",
			errCollectorTokenCatalogInconsistent,
		)
	}
	if requestedPurpose == "" {
		requestedPurpose = current.Purpose
	}
	if requestedPurpose != current.Purpose {
		return "", HECTokenProfile{}, "", fmt.Errorf(
			"%w: ingestion token purpose is immutable",
			control.ErrInvalidArgument,
		)
	}
	switch current.Purpose {
	case IngestionTokenPurposeNativeCollector:
		if requestedProfile != (HECTokenProfile{}) {
			return "", HECTokenProfile{}, "", fmt.Errorf(
				"%w: native collector tokens cannot have a HEC profile",
				control.ErrInvalidArgument,
			)
		}
		binding, err := replacementCollectorID(
			current.BoundCollectorID,
			requestedBoundCollectorID,
		)
		return current.Purpose, HECTokenProfile{}, binding, err
	case IngestionTokenPurposeHEC:
		if current.BoundCollectorID != "" || requestedBoundCollectorID != "" {
			return "", HECTokenProfile{}, "", fmt.Errorf(
				"%w: HEC tokens cannot have a bound collector ID",
				control.ErrInvalidArgument,
			)
		}
		if requestedProfile.IndexerAcknowledgment !=
			current.HECProfile.IndexerAcknowledgment {
			return "", HECTokenProfile{}, "", fmt.Errorf(
				"%w: HEC token acknowledgment mode is immutable",
				control.ErrInvalidArgument,
			)
		}
		profile, err := normalizeHECTokenProfile(
			requestedProfile,
			allowedIndexNames,
		)
		return current.Purpose, profile, "", err
	default:
		panic("unreachable ingestion token purpose")
	}
}

func normalizeHECTokenProfile(
	profile HECTokenProfile,
	allowedIndexNames []string,
) (HECTokenProfile, error) {
	if profile.DefaultIndexName != "" {
		canonical, err := control.NormalizeIndexName(profile.DefaultIndexName)
		_, allowed := slices.BinarySearch(allowedIndexNames, profile.DefaultIndexName)
		if err != nil || canonical != profile.DefaultIndexName || !allowed {
			return HECTokenProfile{}, fmt.Errorf(
				"%w: HEC default index must be a canonical allowed index",
				control.ErrInvalidArgument,
			)
		}
	}
	for _, candidate := range []struct {
		label string
		value string
	}{
		{label: "host", value: profile.DefaultHost},
		{label: "source", value: profile.DefaultSource},
		{label: "sourcetype", value: profile.DefaultSourcetype},
	} {
		if candidate.value != "" && !validHECMetadataDefault(candidate.value) {
			return HECTokenProfile{}, fmt.Errorf(
				"%w: HEC default %s must contain between 1 and %d bounded UTF-8 bytes without surrounding whitespace or control characters",
				control.ErrInvalidArgument,
				candidate.label,
				maximumHECMetadataBytes,
			)
		}
	}
	return HECTokenProfile{
		DefaultIndexName:      strings.Clone(profile.DefaultIndexName),
		DefaultHost:           strings.Clone(profile.DefaultHost),
		DefaultSource:         strings.Clone(profile.DefaultSource),
		DefaultSourcetype:     strings.Clone(profile.DefaultSourcetype),
		IndexerAcknowledgment: profile.IndexerAcknowledgment,
	}, nil
}

func validHECMetadataDefault(value string) bool {
	return len(value) >= 1 &&
		len(value) <= maximumHECMetadataBytes &&
		utf8.ValidString(value) &&
		!hecASCIIEdgeWhitespace(value[0]) &&
		!hecASCIIEdgeWhitespace(value[len(value)-1]) &&
		strings.IndexByte(value, 0) < 0 &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func hecASCIIEdgeWhitespace(value byte) bool {
	switch value {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	default:
		return false
	}
}

func newCollectorTokenHECProfileRecord(
	tokenID string,
	profile HECTokenProfile,
	allowedIndexNames []string,
	allowedIndexIDs []string,
) (collectorTokenHECProfileRecord, error) {
	if len(allowedIndexNames) == 0 ||
		len(allowedIndexNames) != len(allowedIndexIDs) {
		return collectorTokenHECProfileRecord{}, errors.New(
			"HEC token allowed-index projection is inconsistent",
		)
	}
	record := collectorTokenHECProfileRecord{
		IngestionTokenID:      tokenID,
		DefaultHost:           optionalStoredString(profile.DefaultHost),
		DefaultSource:         optionalStoredString(profile.DefaultSource),
		DefaultSourcetype:     optionalStoredString(profile.DefaultSourcetype),
		IndexerAcknowledgment: profile.IndexerAcknowledgment,
	}
	if profile.DefaultIndexName != "" {
		position, found := slices.BinarySearch(
			allowedIndexNames,
			profile.DefaultIndexName,
		)
		if !found {
			return collectorTokenHECProfileRecord{}, errors.New(
				"HEC default index is absent from allowed-index projection",
			)
		}
		value := strings.Clone(allowedIndexIDs[position])
		record.DefaultIndexID = &value
	}
	return record, nil
}

func optionalStoredString(value string) *string {
	if value == "" {
		return nil
	}
	cloned := strings.Clone(value)
	return &cloned
}

func replacementCollectorID(current, requested string) (string, error) {
	if requested == "" {
		return current, nil
	}
	if !validCollectorID(requested) {
		return "", fmt.Errorf(
			"%w: bound collector ID must be a canonical identifier containing between 1 and %d ASCII bytes",
			control.ErrInvalidArgument,
			maximumCollectorIDBytes,
		)
	}
	if current != "" && requested != current {
		return "", fmt.Errorf("%w: bound collector ID is immutable", control.ErrInvalidArgument)
	}
	return requested, nil
}

func validCollectorID(value string) bool {
	if len(value) == 0 || len(value) > maximumCollectorIDBytes || !utf8.ValidString(value) {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		isASCIIAlphaNumeric := character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9'
		if isASCIIAlphaNumeric {
			continue
		}
		if index == 0 || !strings.ContainsRune("._:-", rune(character)) {
			return false
		}
	}
	return true
}

func normalizeTokenDefinition(name, description string, scopes []string, expiration, now time.Time) (string, string, []string, time.Time, error) {
	name = strings.TrimSpace(name)
	if len(name) == 0 || len(name) > 255 || !utf8.ValidString(name) || strings.IndexByte(name, 0) >= 0 {
		return "", "", nil, time.Time{}, fmt.Errorf("%w: token name must contain between 1 and 255 valid UTF-8 bytes", control.ErrInvalidArgument)
	}
	if len(description) > maximumDescriptionBytes || !utf8.ValidString(description) || strings.IndexByte(description, 0) >= 0 {
		return "", "", nil, time.Time{}, fmt.Errorf("%w: token description is invalid or exceeds %d bytes", control.ErrInvalidArgument, maximumDescriptionBytes)
	}
	allowedNames, err := normalizeTokenScopes(scopes)
	if err != nil {
		return "", "", nil, time.Time{}, err
	}
	expiresAt := databaseTime(expiration)
	now = databaseTime(now)
	if !expiration.IsZero() && !expiresAt.After(now) {
		return "", "", nil, time.Time{}, fmt.Errorf("%w: token expiration must be in the future", control.ErrInvalidArgument)
	}
	return name, description, allowedNames, expiresAt, nil
}

func (store *Store) generatePlaintext() (string, error) {
	randomBytes := make([]byte, tokenRandomBytes)
	if _, err := io.ReadFull(store.random, randomBytes); err != nil {
		return "", err
	}
	return collectorTokenPrefix + base64.RawURLEncoding.EncodeToString(randomBytes), nil
}

func (store *Store) generateID() (string, error) {
	randomBytes := make([]byte, tokenIDRandomBytes)
	if _, err := io.ReadFull(store.random, randomBytes); err != nil {
		return "", err
	}
	return "tok_" + base64.RawURLEncoding.EncodeToString(randomBytes), nil
}

func (store *Store) digest(plaintext string) []byte {
	mac := hmac.New(sha256.New, store.digestKey)
	_, _ = mac.Write([]byte(plaintext))
	return mac.Sum(nil)
}

func resolveCollectorTokenScopes(db *gorm.DB, names []string) ([]string, error) {
	var targets []collectorTokenScopeTarget
	query := db.
		Table("indexes").
		Select("index_id", "name").
		Where(
			"name IN ? AND state = ? AND ingestion_enabled = 1",
			names,
			control.IndexStateActive,
		).
		Order("name").
		Find(&targets)
	if query.Error != nil {
		return nil, query.Error
	}
	if len(targets) != len(names) {
		return nil, errCollectorTokenScopeUnavailable
	}
	indexIDs := make([]string, len(targets))
	for index, target := range targets {
		if target.Name != names[index] {
			return nil, errCollectorTokenScopeUnavailable
		}
		indexIDs[index] = target.IndexID
	}
	return indexIDs, nil
}

func finishTokenTransaction(tx *gorm.DB, transactionFinished *bool, returnedErr *error) {
	if tx == nil || transactionFinished == nil || *transactionFinished ||
		returnedErr == nil || *returnedErr == nil {
		return
	}
	if err := tx.Rollback().Error; err != nil {
		*returnedErr = errors.Join(*returnedErr, fmt.Errorf("roll back transaction: %w", err))
	}
}

func databaseTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return time.UnixMicro(value.UTC().UnixMicro()).UTC()
}
