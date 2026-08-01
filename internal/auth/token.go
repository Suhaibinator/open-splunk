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
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexpolicy"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"gorm.io/gorm"
)

const (
	collectorTokenPrefix              = "ost_v1_"
	tokenRandomBytes                  = 32
	tokenIDRandomBytes                = 16
	minimumDigestKeyBytes             = 32
	maximumTokenScopes                = 256
	maximumTokenIDBytes               = 128
	maximumTokenNameBytes             = 255
	maximumDescriptionBytes           = 8 << 10
	minimumTokenPrefixBytes           = 8
	maximumTokenPrefixBytes           = 32
	maximumCollectorIDBytes           = 128
	defaultRetainedRevokedTokenLimit  = 256
	defaultTotalTokenRecordLimit      = 1024
	maximumTotalTokenRecordLimit      = 1024
	maximumRetainedRevokedTokenLimit  = maximumTotalTokenRecordLimit - 1
	maximumTotalTokenScopeRecordLimit = 16_384
	redactedValue                     = "[REDACTED]"
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
	// ErrInactiveToken means an operation that requires an active collector
	// credential could not proceed. Accepted-use recording deliberately uses
	// this one sentinel for missing, disabled, revoked, and expired IDs so the
	// stream-admission path does not disclose credential existence or state.
	ErrInactiveToken = errors.New("auth: collector token is inactive")

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

// CollectorToken contains safe token metadata. It never contains a secret or
// digest.
type CollectorToken struct {
	ID                  string
	Version             uint64
	Name                string
	Description         string
	Prefix              string
	State               CollectorTokenState
	BoundCollectorID    string
	AllowedIndexNames   []string
	IngestionRateLimits ingestquota.Limits
	CreatedAt           time.Time
	UpdatedAt           time.Time
	LastUsedAt          time.Time
	ExpiresAt           time.Time
	RevokedAt           time.Time
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
	Name                string
	Description         string
	AllowedIndexNames   []string
	BoundCollectorID    string
	ExpiresAt           time.Time
	IngestionRateLimits ingestquota.Limits
}

// UpdateCollectorTokenRequest replaces the mutable definition of an existing
// collector token. The credential digest and safe prefix are immutable.
type UpdateCollectorTokenRequest struct {
	Name                string
	Description         string
	AllowedIndexNames   []string
	BoundCollectorID    string
	ExpiresAt           time.Time
	IngestionRateLimits ingestquota.Limits
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
	TokenID           string
	TokenName         string
	BoundCollectorID  string
	TokenRateLimits   ingestquota.Limits
	AuthorizedIndexes []AuthorizedIndexPolicy
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
}

// StoreOptions configures bounded collector-token lifecycle behavior. Zero
// values select production defaults.
type StoreOptions struct {
	RetainedRevokedTokenLimit int
	TotalTokenRecordLimit     int
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
	return &Store{
		orm:                       db.GORMDB(),
		digestKey:                 append([]byte(nil), digestKey...),
		random:                    rand.Reader,
		now:                       time.Now,
		retainedRevokedTokenLimit: retainedRevokedTokenLimit,
		totalTokenRecordLimit:     totalTokenRecordLimit,
	}, nil
}

// CreateCollectorToken generates a cryptographically random token, persists
// only its HMAC-SHA-256 digest, and returns the plaintext exactly once.
func (store *Store) CreateCollectorToken(ctx context.Context, request CreateCollectorTokenRequest) (issued IssuedCollectorToken, err error) {
	now := databaseTime(store.now())
	name, description, allowedNames, expiresAt, err := normalizeTokenDefinition(
		request.Name, request.Description, request.AllowedIndexNames, request.ExpiresAt, now,
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
	if !validCollectorID(request.BoundCollectorID) {
		return IssuedCollectorToken{}, fmt.Errorf(
			"%w: bound collector ID must be a canonical identifier containing between 1 and %d ASCII bytes",
			control.ErrInvalidArgument,
			maximumCollectorIDBytes,
		)
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
		BoundCollectorID:                    &request.BoundCollectorID,
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
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return IssuedCollectorToken{}, fmt.Errorf("commit collector token creation: %w", commitErr)
	}

	metadata := CollectorToken{
		ID: tokenID, Version: 1, Name: name, Description: description,
		Prefix: prefix, State: CollectorTokenStateActive,
		BoundCollectorID:    request.BoundCollectorID,
		AllowedIndexNames:   append([]string(nil), allowedNames...),
		IngestionRateLimits: request.IngestionRateLimits,
		CreatedAt:           now, UpdatedAt: now, ExpiresAt: expiresAt,
	}
	return IssuedCollectorToken{Token: metadata, Secret: Secret{plaintext: plaintext}}, nil
}

// UpdateCollectorToken atomically replaces mutable metadata and explicit
// index scopes under optimistic locking. Revoked and effectively expired
// credentials remain immutable so an administrative edit cannot accidentally
// reactivate them.
func (store *Store) UpdateCollectorToken(ctx context.Context, tokenID string, expectedVersion uint64, request UpdateCollectorTokenRequest) (result CollectorToken, err error) {
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
	boundCollectorID, err := replacementCollectorID(
		current.BoundCollectorID,
		request.BoundCollectorID,
	)
	if err != nil {
		return CollectorToken{}, err
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

	result, err = takeCollectorTokenMetadata(tx, tokenID, now)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read updated collector token: %w", err)
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
			 AND token.bound_collector_id IS NOT NULL
			 AND (token.expires_at_unix_micro IS NULL OR token.expires_at_unix_micro > ?)
			 AND target.name = ?
			 AND target.state = ?
			 AND target.ingestion_enabled = 1`,
			digest,
			CollectorTokenStateActive,
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
	deferIndexAuthorityErrors bool,
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
			token.name,
			token.bound_collector_id,
			token.max_ingest_events_per_second,
			token.max_ingest_uncompressed_bytes_per_second`).
		Where(
			`token.token_digest = ?
			 AND token.state = ?
			 AND token.bound_collector_id IS NOT NULL
			 AND length(CAST(token.ingestion_token_id AS BLOB)) BETWEEN 1 AND ?
			 AND instr(token.ingestion_token_id, char(0)) = 0
			 AND (token.expires_at_unix_micro IS NULL OR token.expires_at_unix_micro > ?)`,
			digest,
			CollectorTokenStateActive,
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
		TokenName:        row.Name,
		BoundCollectorID: row.BoundCollectorID,
		TokenRateLimits:  tokenRateLimits,
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
		Where("scope.ingestion_token_id = ?", row.IngestionTokenID).
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
		if deferIndexAuthorityErrors {
			return authentication, ErrInvalidIndexAuthority
		}
		return Authentication{}, ErrUnauthorized
	}
	if len(scopes) > maximumTokenScopes {
		if deferIndexAuthorityErrors {
			return authentication, ErrInvalidIndexAuthority
		}
		return Authentication{}, errors.New(
			"authenticate collector token: scope count exceeds the supported maximum",
		)
	}
	authorizedIndexes := make([]AuthorizedIndexPolicy, 0, len(scopes))
	for scopeIndex, scope := range scopes {
		if scopeIndex > 0 && scopes[scopeIndex-1].Name >= scope.Name {
			if deferIndexAuthorityErrors {
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
			if deferIndexAuthorityErrors {
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
		if deferIndexAuthorityErrors {
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
	acceptedAt = databaseTime(acceptedAt)
	if acceptedAt.IsZero() {
		return fmt.Errorf("%w: collector token acceptance time is required", control.ErrInvalidArgument)
	}
	acceptedAtUnixMicro := acceptedAt.UnixMicro()
	result := database.
		Model(&collectorTokenRecord{}).
		Where("ingestion_token_id = ?", tokenID).
		Where("state = ?", CollectorTokenStateActive).
		Where("bound_collector_id IS NOT NULL").
		Where(
			"expires_at_unix_micro IS NULL OR expires_at_unix_micro > ?",
			acceptedAtUnixMicro,
		).
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
		return fmt.Errorf("record collector token use: %w", result.Error)
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

// RevokeCollectorToken irreversibly revokes a token under optimistic locking.
func (store *Store) RevokeCollectorToken(ctx context.Context, tokenID string, expectedVersion uint64) (result CollectorToken, err error) {
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
			token.max_ingest_events_per_second,
			token.max_ingest_uncompressed_bytes_per_second`)
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
				AS bound_collector_id_bytes`)
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
	return hydrateCollectorTokenScopes(
		database,
		rows,
		catalog.scopeRecords,
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
		return tokens, nil
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
