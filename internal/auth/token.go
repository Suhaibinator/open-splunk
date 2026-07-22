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
	// ErrInactiveToken means an administrative mutation targeted a revoked or
	// effectively expired token. Neither state can safely be made active again
	// by changing token metadata.
	ErrInactiveToken = errors.New("auth: collector token is inactive")
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
	db        *sql.DB
	digestKey []byte
	random    io.Reader
	now       func() time.Time
}

// NewStore constructs a collector-token store. digestKey is copied so caller
// mutation cannot silently invalidate every credential.
func NewStore(db *control.DB, digestKey []byte) (*Store, error) {
	if db == nil || db.SQLDB() == nil {
		return nil, fmt.Errorf("%w: control-plane database is required", control.ErrInvalidArgument)
	}
	if len(digestKey) < minimumDigestKeyBytes {
		return nil, ErrInvalidDigestKey
	}
	return &Store{
		db:        db.SQLDB(),
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

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return IssuedCollectorToken{}, fmt.Errorf("begin collector token creation: %w", err)
	}
	defer finishTx(tx, &err)

	indexIDs := make([]string, 0, len(allowedNames))
	for _, indexName := range allowedNames {
		var indexID string
		queryErr := tx.QueryRowContext(ctx, `
			SELECT index_id
			FROM indexes
			WHERE name = ? AND state = 'active' AND ingestion_enabled = 1`, indexName).Scan(&indexID)
		if errors.Is(queryErr, sql.ErrNoRows) {
			return IssuedCollectorToken{}, fmt.Errorf("%w: every token scope must name an active ingestion-enabled index", control.ErrInvalidArgument)
		}
		if queryErr != nil {
			return IssuedCollectorToken{}, fmt.Errorf("validate collector token scope: %w", queryErr)
		}
		indexIDs = append(indexIDs, indexID)
	}

	var expiration any
	if !request.ExpiresAt.IsZero() {
		expiration = expiresAt.UnixMicro()
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ingestion_tokens (
			ingestion_token_id, version, name, description, token_prefix,
			token_digest, state, created_at_unix_micro, updated_at_unix_micro,
			expires_at_unix_micro, revoked_at_unix_micro
		) VALUES (?, 1, ?, ?, ?, ?, 'active', ?, ?, ?, NULL)`,
		tokenID, name, description, prefix, digest,
		now.UnixMicro(), now.UnixMicro(), expiration,
	)
	if err != nil {
		// No bound SQL value contains plaintext: only the HMAC digest is sent
		// to SQLite, so wrapping a driver error cannot disclose the token.
		return IssuedCollectorToken{}, fmt.Errorf("store collector token digest: %w", err)
	}
	for _, indexID := range indexIDs {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
			VALUES (?, ?)`, tokenID, indexID); err != nil {
			return IssuedCollectorToken{}, fmt.Errorf("store collector token scope: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return IssuedCollectorToken{}, fmt.Errorf("commit collector token creation: %w", err)
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
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("begin collector token update: %w", err)
	}
	defer finishTx(tx, &err)

	current, err := scanCollectorToken(tx.QueryRowContext(ctx, tokenSelect+` WHERE t.ingestion_token_id = ? GROUP BY t.ingestion_token_id`, tokenID), now)
	if errors.Is(err, sql.ErrNoRows) {
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
	name, description, allowedNames, expiresAt, err := normalizeTokenDefinition(
		request.Name, request.Description, request.AllowedIndexNames, request.ExpiresAt, now,
	)
	if err != nil {
		return CollectorToken{}, err
	}
	if !expiresAt.IsZero() && !expiresAt.After(current.CreatedAt) {
		return CollectorToken{}, fmt.Errorf("%w: token expiration must be after its creation time", control.ErrInvalidArgument)
	}

	indexIDs := make([]string, 0, len(allowedNames))
	for _, indexName := range allowedNames {
		var indexID string
		queryErr := tx.QueryRowContext(ctx, `
			SELECT index_id
			FROM indexes
			WHERE name = ? AND state = 'active' AND ingestion_enabled = 1`, indexName).Scan(&indexID)
		if errors.Is(queryErr, sql.ErrNoRows) {
			return CollectorToken{}, fmt.Errorf("%w: every token scope must name an active ingestion-enabled index", control.ErrInvalidArgument)
		}
		if queryErr != nil {
			return CollectorToken{}, fmt.Errorf("validate collector token update scope: %w", queryErr)
		}
		indexIDs = append(indexIDs, indexID)
	}

	var expiration any
	if !expiresAt.IsZero() {
		expiration = expiresAt.UnixMicro()
	}
	update, err := tx.ExecContext(ctx, `
		UPDATE ingestion_tokens
		SET name = ?, description = ?, expires_at_unix_micro = ?,
			version = version + 1, updated_at_unix_micro = ?
		WHERE ingestion_token_id = ? AND version = ? AND state != 'revoked'`,
		name, description, expiration, now.UnixMicro(), tokenID, int64(expectedVersion))
	if err != nil {
		return CollectorToken{}, fmt.Errorf("update collector token: %w", err)
	}
	rows, err := update.RowsAffected()
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read updated collector token row count: %w", err)
	}
	if rows != 1 {
		return CollectorToken{}, control.ErrVersionConflict
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM ingestion_token_indexes WHERE ingestion_token_id = ?`, tokenID); err != nil {
		return CollectorToken{}, fmt.Errorf("replace collector token scopes: %w", err)
	}
	for _, indexID := range indexIDs {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
			VALUES (?, ?)`, tokenID, indexID); err != nil {
			return CollectorToken{}, fmt.Errorf("store updated collector token scope: %w", err)
		}
	}

	result, err = scanCollectorToken(tx.QueryRowContext(ctx, tokenSelect+` WHERE t.ingestion_token_id = ? GROUP BY t.ingestion_token_id`, tokenID), now)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read updated collector token: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return CollectorToken{}, fmt.Errorf("commit collector token update: %w", err)
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
	err = store.db.QueryRowContext(ctx, `
		SELECT t.ingestion_token_id, t.name, i.name
		FROM ingestion_tokens AS t
		JOIN ingestion_token_indexes AS ti
			ON ti.ingestion_token_id = t.ingestion_token_id
		JOIN indexes AS i ON i.index_id = ti.index_id
		WHERE t.token_digest = ?
			AND t.state = 'active'
			AND (t.expires_at_unix_micro IS NULL OR t.expires_at_unix_micro > ?)
			AND i.name = ?
			AND i.state = 'active'
			AND i.ingestion_enabled = 1`,
		digest, now.UnixMicro(), normalizedIndex,
	).Scan(&principal.TokenID, &principal.TokenName, &principal.IndexName)
	if errors.Is(err, sql.ErrNoRows) {
		return Principal{}, ErrUnauthorized
	}
	if err != nil {
		return Principal{}, fmt.Errorf("authorize collector token: %w", err)
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
	var authentication Authentication
	var allowedIndexes string
	err := store.db.QueryRowContext(ctx, `
		SELECT t.ingestion_token_id, t.name, group_concat(i.name, ',')
		FROM ingestion_tokens AS t
		JOIN ingestion_token_indexes AS ti
			ON ti.ingestion_token_id = t.ingestion_token_id
		JOIN indexes AS i ON i.index_id = ti.index_id
		WHERE t.token_digest = ?
			AND t.state = 'active'
			AND (t.expires_at_unix_micro IS NULL OR t.expires_at_unix_micro > ?)
			AND i.state = 'active'
			AND i.ingestion_enabled = 1
		GROUP BY t.ingestion_token_id`, digest, now.UnixMicro()).Scan(
		&authentication.TokenID, &authentication.TokenName, &allowedIndexes,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Authentication{}, ErrUnauthorized
	}
	if err != nil {
		return Authentication{}, fmt.Errorf("authenticate collector token: %w", err)
	}
	authentication.AllowedIndexNames = strings.Split(allowedIndexes, ",")
	sort.Strings(authentication.AllowedIndexNames)
	return authentication, nil
}

// GetCollectorToken returns safe metadata and explicit index scopes.
func (store *Store) GetCollectorToken(ctx context.Context, tokenID string) (CollectorToken, error) {
	token, err := scanCollectorToken(store.db.QueryRowContext(ctx, tokenSelect+` WHERE t.ingestion_token_id = ? GROUP BY t.ingestion_token_id`, tokenID), databaseTime(store.now()))
	if errors.Is(err, sql.ErrNoRows) {
		return CollectorToken{}, control.ErrNotFound
	}
	if err != nil {
		return CollectorToken{}, fmt.Errorf("get collector token: %w", err)
	}
	return token, nil
}

// ListCollectorTokens lists safe metadata in creation order.
func (store *Store) ListCollectorTokens(ctx context.Context) ([]CollectorToken, error) {
	rows, err := store.db.QueryContext(ctx, tokenSelect+` GROUP BY t.ingestion_token_id ORDER BY t.created_at_unix_micro, t.ingestion_token_id`)
	if err != nil {
		return nil, fmt.Errorf("list collector tokens: %w", err)
	}
	defer rows.Close()
	now := databaseTime(store.now())
	tokens := make([]CollectorToken, 0)
	for rows.Next() {
		token, err := scanCollectorToken(rows, now)
		if err != nil {
			return nil, fmt.Errorf("scan collector token: %w", err)
		}
		tokens = append(tokens, token)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate collector tokens: %w", err)
	}
	return tokens, nil
}

// RevokeCollectorToken irreversibly revokes a token under optimistic locking.
func (store *Store) RevokeCollectorToken(ctx context.Context, tokenID string, expectedVersion uint64) (result CollectorToken, err error) {
	if expectedVersion == 0 || expectedVersion > math.MaxInt64 {
		return CollectorToken{}, fmt.Errorf("%w: expected token version is outside the supported range", control.ErrInvalidArgument)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("begin collector token revocation: %w", err)
	}
	defer finishTx(tx, &err)

	var currentVersion int64
	if err := tx.QueryRowContext(ctx, `
		SELECT version FROM ingestion_tokens WHERE ingestion_token_id = ?`, tokenID).Scan(&currentVersion); errors.Is(err, sql.ErrNoRows) {
		return CollectorToken{}, control.ErrNotFound
	} else if err != nil {
		return CollectorToken{}, fmt.Errorf("read collector token for revocation: %w", err)
	}
	if uint64(currentVersion) != expectedVersion {
		return CollectorToken{}, control.ErrVersionConflict
	}

	now := databaseTime(store.now())
	update, err := tx.ExecContext(ctx, `
		UPDATE ingestion_tokens
		SET state = 'revoked', version = version + 1,
			updated_at_unix_micro = ?, revoked_at_unix_micro = ?
		WHERE ingestion_token_id = ? AND version = ? AND state != 'revoked'`,
		now.UnixMicro(), now.UnixMicro(), tokenID, int64(expectedVersion))
	if err != nil {
		return CollectorToken{}, fmt.Errorf("revoke collector token: %w", err)
	}
	rows, err := update.RowsAffected()
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read revoked token row count: %w", err)
	}
	if rows != 1 {
		return CollectorToken{}, control.ErrVersionConflict
	}

	result, err = scanCollectorToken(tx.QueryRowContext(ctx, tokenSelect+` WHERE t.ingestion_token_id = ? GROUP BY t.ingestion_token_id`, tokenID), now)
	if err != nil {
		return CollectorToken{}, fmt.Errorf("read revoked collector token: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return CollectorToken{}, fmt.Errorf("commit collector token revocation: %w", err)
	}
	return result, nil
}

const tokenSelect = `
	SELECT
		t.ingestion_token_id, t.version, t.name, t.description,
		t.token_prefix, t.state, t.created_at_unix_micro,
		t.updated_at_unix_micro, t.expires_at_unix_micro,
		t.revoked_at_unix_micro, group_concat(i.name, ',')
	FROM ingestion_tokens AS t
	JOIN ingestion_token_indexes AS ti
		ON ti.ingestion_token_id = t.ingestion_token_id
	JOIN indexes AS i ON i.index_id = ti.index_id`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanCollectorToken(row rowScanner, now time.Time) (CollectorToken, error) {
	var (
		token            CollectorToken
		version          int64
		state            string
		created, updated int64
		expires, revoked sql.NullInt64
		allowedIndexes   string
	)
	if err := row.Scan(
		&token.ID, &version, &token.Name, &token.Description,
		&token.Prefix, &state, &created, &updated, &expires, &revoked,
		&allowedIndexes,
	); err != nil {
		return CollectorToken{}, err
	}
	if version < 1 {
		return CollectorToken{}, errors.New("invalid collector token version in control-plane database")
	}
	token.Version = uint64(version)
	token.State = CollectorTokenState(state)
	token.CreatedAt = time.UnixMicro(created).UTC()
	token.UpdatedAt = time.UnixMicro(updated).UTC()
	if expires.Valid {
		token.ExpiresAt = time.UnixMicro(expires.Int64).UTC()
		if token.State == CollectorTokenStateActive && !token.ExpiresAt.After(now) {
			token.State = CollectorTokenStateExpired
		}
	}
	if revoked.Valid {
		token.RevokedAt = time.UnixMicro(revoked.Int64).UTC()
	}
	if allowedIndexes != "" {
		token.AllowedIndexNames = strings.Split(allowedIndexes, ",")
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

func finishTx(tx *sql.Tx, returnedErr *error) {
	if tx == nil || returnedErr == nil || *returnedErr == nil {
		return
	}
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		*returnedErr = errors.Join(*returnedErr, fmt.Errorf("roll back transaction: %w", err))
	}
}

func databaseTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return time.UnixMicro(value.UTC().UnixMicro()).UTC()
}
