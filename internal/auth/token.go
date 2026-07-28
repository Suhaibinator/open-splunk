package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
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
	"gorm.io/gorm"
)

const (
	collectorTokenPrefix    = "ost_v1_"
	tokenRandomBytes        = 32
	tokenIDRandomBytes      = 16
	minimumDigestKeyBytes   = 32
	maximumTokenScopes      = 256
	maximumDescriptionBytes = 8 << 10
	redactedValue           = "[REDACTED]"
)

var (
	// ErrInvalidDigestKey means the configured token-digest key is too short
	// to provide the intended security margin.
	ErrInvalidDigestKey = errors.New("auth: collector token digest key must contain at least 32 bytes")
	// ErrUnauthorized intentionally combines invalid credentials, inactive
	// credentials, expired credentials, and forbidden indexes into one safe
	// externally reportable error.
	ErrUnauthorized = errors.New("auth: collector authentication or index authorization failed")
	// ErrInactiveToken means an operation that requires an active collector
	// credential could not proceed. Accepted-use recording deliberately uses
	// this one sentinel for missing, disabled, revoked, and expired IDs so the
	// stream-admission path does not disclose credential existence or state.
	ErrInactiveToken = errors.New("auth: collector token is inactive")

	errCollectorTokenScopeUnavailable = errors.New("collector token scope is unavailable")
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
	ID                string
	Version           uint64
	Name              string
	Description       string
	Prefix            string
	State             CollectorTokenState
	AllowedIndexNames []string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	LastUsedAt        time.Time
	ExpiresAt         time.Time
	RevokedAt         time.Time
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
	Name              string
	Description       string
	AllowedIndexNames []string
	ExpiresAt         time.Time
}

// UpdateCollectorTokenRequest replaces the mutable definition of an existing
// collector token. The credential digest and safe prefix are immutable.
type UpdateCollectorTokenRequest struct {
	Name              string
	Description       string
	AllowedIndexNames []string
	ExpiresAt         time.Time
}

// Principal is the safe result of a collector authorization check.
type Principal struct {
	TokenID   string
	TokenName string
	IndexName string
}

// Authentication is a safe credential resolution snapshot. AllowedIndexNames
// includes only currently active, ingestion-enabled indexes and must be
// refreshed at each security boundary where revocation needs to take effect.
type Authentication struct {
	TokenID           string
	TokenName         string
	AllowedIndexNames []string
}

// Store owns collector credential creation, persistence, revocation, and
// per-index authorization.
type Store struct {
	orm       *gorm.DB
	digestKey []byte
	random    io.Reader
	now       func() time.Time
}

// NewStore constructs a collector-token store. digestKey is copied so caller
// mutation cannot silently invalidate every credential.
func NewStore(db *control.DB, digestKey []byte) (*Store, error) {
	if db == nil || db.GORMDB() == nil {
		return nil, fmt.Errorf("%w: control-plane database is required", control.ErrInvalidArgument)
	}
	if len(digestKey) < minimumDigestKeyBytes {
		return nil, ErrInvalidDigestKey
	}
	return &Store{
		orm:       db.GORMDB(),
		digestKey: append([]byte(nil), digestKey...),
		random:    rand.Reader,
		now:       time.Now,
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

	var expiration *int64
	if !expiresAt.IsZero() {
		expirationUnixMicro := expiresAt.UnixMicro()
		expiration = &expirationUnixMicro
	}
	record := collectorTokenRecord{
		IngestionTokenID:   tokenID,
		Version:            1,
		Name:               name,
		Description:        description,
		TokenPrefix:        prefix,
		TokenDigest:        digest,
		State:              CollectorTokenStateActive,
		CreatedAtUnixMicro: now.UnixMicro(),
		UpdatedAtUnixMicro: now.UnixMicro(),
		ExpiresAtUnixMicro: expiration,
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
		AllowedIndexNames: append([]string(nil), allowedNames...),
		CreatedAt:         now, UpdatedAt: now, ExpiresAt: expiresAt,
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

	currentRow, err := takeCollectorTokenMetadata(tx, tokenID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return CollectorToken{}, control.ErrNotFound
	}
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read collector token for update: %w", err)
	}
	current, err := collectorTokenFromMetadataRow(currentRow, now)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read collector token for update: %w", err)
	}
	if current.Version != expectedVersion {
		return CollectorToken{}, control.ErrVersionConflict
	}
	if current.State == CollectorTokenStateRevoked || current.State == CollectorTokenStateExpired {
		return CollectorToken{}, ErrInactiveToken
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

	// #nosec G115 -- expectedVersion is bounded above by math.MaxInt64.
	expectedVersionDB := int64(expectedVersion)
	var expiration *int64
	if !expiresAt.IsZero() {
		expirationUnixMicro := expiresAt.UnixMicro()
		expiration = &expirationUnixMicro
	}
	update := tx.Model(&collectorTokenRecord{}).
		Where(
			"ingestion_token_id = ? AND version = ? AND state != ?",
			tokenID,
			expectedVersionDB,
			CollectorTokenStateRevoked,
		).
		Updates(map[string]any{
			"name":                  name,
			"description":           description,
			"expires_at_unix_micro": expiration,
			"updated_at_unix_micro": now.UnixMicro(),
			"version":               gorm.Expr("version + 1"),
		})
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

	updatedRow, err := takeCollectorTokenMetadata(tx, tokenID)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read updated collector token: %w", err)
	}
	result, err = collectorTokenFromMetadataRow(updatedRow, now)
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
			target.name AS index_name`).
		Joins(`
			JOIN ingestion_token_indexes AS scope
			  ON scope.ingestion_token_id = token.ingestion_token_id`).
		Joins("JOIN indexes AS target ON target.index_id = scope.index_id").
		Where(
			`token.token_digest = ?
			 AND token.state = ?
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
	return principal, nil
}

// Authenticate validates one collector credential and resolves its complete
// current ingestion scope in a single database snapshot. Invalid, disabled,
// revoked, expired, and scope-less credentials all return ErrUnauthorized.
func (store *Store) Authenticate(ctx context.Context, plaintext string) (Authentication, error) {
	if plaintext == "" {
		return Authentication{}, ErrUnauthorized
	}
	digest := store.digest(plaintext)
	now := databaseTime(store.now())
	var row collectorTokenAuthenticationRow
	result := store.orm.WithContext(ctx).
		Table("ingestion_tokens AS token").
		Select(`
			token.ingestion_token_id,
			token.name,
			group_concat(target.name, ',') AS allowed_index_names`).
		Joins(`
			JOIN ingestion_token_indexes AS scope
			  ON scope.ingestion_token_id = token.ingestion_token_id`).
		Joins("JOIN indexes AS target ON target.index_id = scope.index_id").
		Where(
			`token.token_digest = ?
			 AND token.state = ?
			 AND (token.expires_at_unix_micro IS NULL OR token.expires_at_unix_micro > ?)
			 AND target.state = ?
			 AND target.ingestion_enabled = 1`,
			digest,
			CollectorTokenStateActive,
			now.UnixMicro(),
			control.IndexStateActive,
		).
		Group("token.ingestion_token_id").
		Take(&row)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return Authentication{}, ErrUnauthorized
	}
	if result.Error != nil {
		return Authentication{}, fmt.Errorf("authenticate collector token: %w", result.Error)
	}
	authentication := Authentication{
		TokenID:           row.IngestionTokenID,
		TokenName:         row.Name,
		AllowedIndexNames: strings.Split(row.AllowedIndexNames, ","),
	}
	sort.Strings(authentication.AllowedIndexNames)
	return authentication, nil
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
	acceptedAt = databaseTime(acceptedAt)
	if acceptedAt.IsZero() {
		return fmt.Errorf("%w: collector token acceptance time is required", control.ErrInvalidArgument)
	}
	acceptedAtUnixMicro := acceptedAt.UnixMicro()
	result := store.orm.WithContext(ctx).
		Model(&collectorTokenRecord{}).
		Where("ingestion_token_id = ?", tokenID).
		Where("state = ?", CollectorTokenStateActive).
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
func (store *Store) GetCollectorToken(ctx context.Context, tokenID string) (CollectorToken, error) {
	row, err := takeCollectorTokenMetadata(store.orm.WithContext(ctx), tokenID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return CollectorToken{}, control.ErrNotFound
	}
	if err != nil {
		return CollectorToken{}, fmt.Errorf("get collector token: %w", err)
	}
	token, err := collectorTokenFromMetadataRow(row, databaseTime(store.now()))
	if err != nil {
		return CollectorToken{}, fmt.Errorf("get collector token: %w", err)
	}
	return token, nil
}

// ListCollectorTokens lists safe metadata in creation order.
func (store *Store) ListCollectorTokens(ctx context.Context) ([]CollectorToken, error) {
	var rows []collectorTokenMetadataRow
	query := collectorTokenMetadataQuery(store.orm.WithContext(ctx)).
		Group("token.ingestion_token_id").
		Order("token.created_at_unix_micro").
		Order("token.ingestion_token_id").
		Scan(&rows)
	if query.Error != nil {
		return nil, fmt.Errorf("list collector tokens: %w", query.Error)
	}
	now := databaseTime(store.now())
	tokens := make([]CollectorToken, 0, len(rows))
	for _, row := range rows {
		token, err := collectorTokenFromMetadataRow(row, now)
		if err != nil {
			return nil, fmt.Errorf("scan collector token: %w", err)
		}
		tokens = append(tokens, token)
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

	revokedRow, err := takeCollectorTokenMetadata(tx, tokenID)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read revoked collector token: %w", err)
	}
	result, err = collectorTokenFromMetadataRow(revokedRow, now)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read revoked collector token: %w", err)
	}
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return CollectorToken{}, fmt.Errorf("commit collector token revocation: %w", commitErr)
	}
	return result, nil
}

func collectorTokenMetadataQuery(db *gorm.DB) *gorm.DB {
	return db.
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
			group_concat(target.name, ',') AS allowed_index_names`).
		Joins(`
			JOIN ingestion_token_indexes AS scope
			  ON scope.ingestion_token_id = token.ingestion_token_id`).
		Joins("JOIN indexes AS target ON target.index_id = scope.index_id")
}

func takeCollectorTokenMetadata(db *gorm.DB, tokenID string) (collectorTokenMetadataRow, error) {
	var row collectorTokenMetadataRow
	result := collectorTokenMetadataQuery(db).
		Where("token.ingestion_token_id = ?", tokenID).
		Group("token.ingestion_token_id").
		Take(&row)
	return row, result.Error
}

func collectorTokenFromMetadataRow(row collectorTokenMetadataRow, now time.Time) (CollectorToken, error) {
	if row.Version < 1 {
		return CollectorToken{}, errors.New("invalid collector token version in control-plane database")
	}
	token := CollectorToken{
		ID:          row.IngestionTokenID,
		Version:     uint64(row.Version),
		Name:        row.Name,
		Description: row.Description,
		Prefix:      row.TokenPrefix,
		State:       row.State,
		CreatedAt:   time.UnixMicro(row.CreatedAtUnixMicro).UTC(),
		UpdatedAt:   time.UnixMicro(row.UpdatedAtUnixMicro).UTC(),
	}
	if row.ExpiresAtUnixMicro != nil {
		token.ExpiresAt = time.UnixMicro(*row.ExpiresAtUnixMicro).UTC()
		if token.State == CollectorTokenStateActive && !token.ExpiresAt.After(now) {
			token.State = CollectorTokenStateExpired
		}
	}
	if row.RevokedAtUnixMicro != nil {
		token.RevokedAt = time.UnixMicro(*row.RevokedAtUnixMicro).UTC()
	}
	if row.LastUsedAtUnixMicro != nil {
		token.LastUsedAt = time.UnixMicro(*row.LastUsedAtUnixMicro).UTC()
		if token.LastUsedAt.Before(token.CreatedAt) {
			return CollectorToken{}, errors.New("invalid collector token last-use time in control-plane database")
		}
	}
	if row.AllowedIndexNames != "" {
		token.AllowedIndexNames = strings.Split(row.AllowedIndexNames, ",")
		sort.Strings(token.AllowedIndexNames)
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
