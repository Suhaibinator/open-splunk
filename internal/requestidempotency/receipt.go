// Package requestidempotency owns durable, actor-scoped API mutation receipts.
package requestidempotency

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

const (
	CanonicalVersion = uint32(1)
	MaximumReceipts  = uint64(100_000)
	MaximumBytes     = uint64(64 << 20)
	MinimumRequestID = 16
	MaximumRequestID = 128

	defaultRetention  = 7 * 24 * time.Hour
	maximumUnixMicro  = int64(253_402_300_799_999_999)
	receiptFixedBytes = uint64(96)
	digestDomain      = "open-splunk/api-mutation-intent\x00"
	reclaimBatchSize  = 256
)

const (
	RouteCreateApp            = "/api/apps/create"
	RouteCreateIndex          = "/api/indexes/create"
	RouteCreateIngestionToken = "/api/ingestion-tokens/create"
	RouteCreateSearchJob      = "/api/search/jobs/create"
	RouteCreateExportJob      = "/api/search/exports/create"
	RouteCreateSavedSearch    = "/api/saved-searches/create"
	RouteDuplicateSavedSearch = "/api/saved-searches/duplicate"
	RouteCreateLookup         = "/api/knowledge/lookups/create"
)

const (
	TargetApp            = "app"
	TargetIndex          = "index"
	TargetIngestionToken = "ingestion_token"
	TargetSearchJob      = "search_job"
	TargetExportJob      = "export_job"
	TargetSavedSearch    = "saved_search"
	TargetLookup         = "lookup"
)

var (
	ErrInvalid     = errors.New("request idempotency input is invalid")
	ErrConflict    = errors.New("client request ID was already used for different intent")
	ErrCapacity    = errors.New("request idempotency capacity is exhausted")
	ErrUnavailable = errors.New("request idempotency outcome is unavailable")
	ErrCorrupt     = errors.New("request idempotency state is corrupt")
)

// Intent is the immutable identity of one caller-authored logical mutation.
// ActorID is a stable authenticated principal identity and never a credential.
type Intent struct {
	TenantID         string
	ActorKind        string
	ActorID          string
	Route            string
	ClientRequestID  string
	CanonicalVersion uint32
	RequestSHA256    [sha256.Size]byte
}

// Target identifies the resource created by a successfully committed intent.
// Version is the version at creation; replay reads and authorizes current
// metadata rather than returning this historical projection.
type Target struct {
	Kind    string
	ID      string
	Version uint64
}

// Receipt is a detached persisted mutation outcome.
type Receipt struct {
	Intent        Intent
	Target        Target
	AuditSequence *uint64
	CreatedAt     time.Time
	RetainUntil   time.Time
	TerminalAt    *time.Time
	EncodedBytes  uint64
}

type receiptRecord struct {
	TenantID                string `gorm:"column:tenant_id"`
	ActorKind               string `gorm:"column:actor_kind"`
	ActorID                 string `gorm:"column:actor_id"`
	Route                   string `gorm:"column:route"`
	ClientRequestID         string `gorm:"column:client_request_id"`
	CanonicalVersion        int64  `gorm:"column:canonical_version"`
	RequestSHA256           []byte `gorm:"column:request_sha256"`
	TargetKind              string `gorm:"column:target_kind"`
	TargetID                string `gorm:"column:target_id"`
	TargetVersion           int64  `gorm:"column:target_version"`
	AuditSequence           *int64 `gorm:"column:audit_sequence"`
	CreatedAtUnixMicro      int64  `gorm:"column:created_at_unix_micro"`
	RetainUntilUnixMicro    int64  `gorm:"column:retain_until_unix_micro"`
	TargetTerminalUnixMicro *int64 `gorm:"column:target_terminal_at_unix_micro"`
	EncodedBytes            int64  `gorm:"column:encoded_bytes"`
}

func (receiptRecord) TableName() string { return "api_mutation_receipts" }

type tenantStateRecord struct {
	TenantID           string `gorm:"column:tenant_id"`
	ReceiptCount       int64  `gorm:"column:receipt_count"`
	EncodedBytes       int64  `gorm:"column:encoded_bytes"`
	HighWaterUnixMicro int64  `gorm:"column:high_water_unix_micro"`
}

// SQLTransaction is the narrow transaction surface used by stores that own a
// database/sql cross-layer commit rather than a GORM transaction.
type SQLTransaction interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (tenantStateRecord) TableName() string {
	return "api_mutation_receipt_tenant_state"
}

// NewIntent hashes one already sanitized canonical request. The caller must
// clone the request and clear client_request_id before calling this function.
// Dynamic defaults, resolved time, quotas, and generated IDs must be absent.
func NewIntent(
	tenantID string,
	actorKind string,
	actorID string,
	route string,
	clientRequestID string,
	canonical proto.Message,
) (Intent, error) {
	intent := Intent{
		TenantID: tenantID, ActorKind: actorKind, ActorID: actorID,
		Route: route, ClientRequestID: clientRequestID,
		CanonicalVersion: CanonicalVersion,
	}
	if err := validateIntentIdentity(intent); err != nil || canonical == nil {
		return Intent{}, ErrInvalid
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
	if err != nil {
		return Intent{}, fmt.Errorf("%w: encode canonical request", ErrInvalid)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(digestDomain))
	var number [4]byte
	binary.BigEndian.PutUint32(number[:], CanonicalVersion)
	_, _ = hash.Write(number[:])
	binary.BigEndian.PutUint32(number[:], uint32(len(route)))
	_, _ = hash.Write(number[:])
	_, _ = hash.Write([]byte(route))
	_, _ = hash.Write(encoded)
	copy(intent.RequestSHA256[:], hash.Sum(nil))
	return intent, nil
}

// ValidateClientRequestID applies the public request-key wire contract.
func ValidateClientRequestID(value string) error {
	if len(value) < MinimumRequestID || len(value) > MaximumRequestID {
		return fmt.Errorf(
			"%w: client request ID must contain between %d and %d bytes",
			ErrInvalid,
			MinimumRequestID,
			MaximumRequestID,
		)
	}
	for index := range len(value) {
		if value[index] < 0x20 || value[index] > 0x7e {
			return fmt.Errorf("%w: client request ID must be printable ASCII", ErrInvalid)
		}
	}
	return nil
}

// Read returns the exact actor-scoped receipt for intent. A key collision with
// different intent is a conflict. Unknown canonical versions fail closed and
// cannot cause the mutation to run again.
func Read(
	ctx context.Context,
	database *gorm.DB,
	intent Intent,
) (Receipt, bool, error) {
	if ctx == nil || database == nil || validateIntentIdentity(intent) != nil {
		return Receipt{}, false, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, false, err
	}
	var record receiptRecord
	err := database.WithContext(ctx).
		Where(
			"tenant_id = ? AND actor_kind = ? AND actor_id = ? AND route = ? AND client_request_id = ?",
			intent.TenantID,
			intent.ActorKind,
			intent.ActorID,
			intent.Route,
			intent.ClientRequestID,
		).
		Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Receipt{}, false, nil
	}
	if err != nil {
		return Receipt{}, false, mapDatabaseError(ctx, err)
	}
	receipt, err := receiptFromRecord(record)
	if err != nil {
		return Receipt{}, false, err
	}
	if receipt.Intent.CanonicalVersion != CanonicalVersion {
		return Receipt{}, true, ErrUnavailable
	}
	if receipt.Intent.CanonicalVersion != intent.CanonicalVersion ||
		subtle.ConstantTimeCompare(
			receipt.Intent.RequestSHA256[:],
			intent.RequestSHA256[:],
		) != 1 {
		return Receipt{}, true, ErrConflict
	}
	return receipt, true, nil
}

// ReadSQL is Read for a database/sql transaction or connection.
func ReadSQL(
	ctx context.Context,
	database SQLTransaction,
	intent Intent,
) (Receipt, bool, error) {
	if ctx == nil || database == nil || validateIntentIdentity(intent) != nil {
		return Receipt{}, false, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, false, err
	}
	record, err := scanReceipt(database.QueryRowContext(
		ctx,
		`SELECT tenant_id, actor_kind, actor_id, route, client_request_id,
		        canonical_version, request_sha256, target_kind, target_id,
		        target_version, audit_sequence, created_at_unix_micro,
		        retain_until_unix_micro, target_terminal_at_unix_micro,
		        encoded_bytes
		 FROM api_mutation_receipts
		 WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		   AND route = ? AND client_request_id = ?`,
		intent.TenantID,
		intent.ActorKind,
		intent.ActorID,
		intent.Route,
		intent.ClientRequestID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, false, nil
	}
	if err != nil {
		return Receipt{}, false, mapDatabaseError(ctx, err)
	}
	receipt, err := receiptFromRecord(record)
	if err != nil {
		return Receipt{}, false, err
	}
	if receipt.Intent.CanonicalVersion != CanonicalVersion {
		return Receipt{}, true, ErrUnavailable
	}
	if receipt.Intent.CanonicalVersion != intent.CanonicalVersion ||
		subtle.ConstantTimeCompare(receipt.Intent.RequestSHA256[:], intent.RequestSHA256[:]) != 1 {
		return Receipt{}, true, ErrConflict
	}
	return receipt, true, nil
}

// AppendInTransaction records a committed target through the caller-owned SQL
// transaction. It never commits or rolls back tx.
func AppendInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	intent Intent,
	target Target,
	auditSequence *uint64,
	now time.Time,
) (Receipt, error) {
	if ctx == nil || !activeTransaction(tx) ||
		validateIntentIdentity(intent) != nil || validateTarget(intent.Route, target) != nil {
		return Receipt{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	canonicalNow, err := canonicalTime(now)
	if err != nil {
		return Receipt{}, ErrInvalid
	}
	database := tx.WithContext(ctx)
	if database.Error != nil {
		return Receipt{}, mapDatabaseError(ctx, database.Error)
	}
	if err := database.Exec(
		`INSERT INTO api_mutation_receipt_tenant_state (
			tenant_id, receipt_count, encoded_bytes, high_water_unix_micro
		) VALUES (?, 0, 0, ?)
		ON CONFLICT (tenant_id) DO NOTHING`,
		intent.TenantID,
		canonicalNow.UnixMicro(),
	).Error; err != nil {
		return Receipt{}, mapDatabaseError(ctx, err)
	}
	if err := database.Exec(
		`UPDATE api_mutation_receipt_tenant_state
		 SET high_water_unix_micro = MAX(high_water_unix_micro, ?)
		 WHERE tenant_id = ?`,
		canonicalNow.UnixMicro(),
		intent.TenantID,
	).Error; err != nil {
		return Receipt{}, mapDatabaseError(ctx, err)
	}
	if err := reclaimExpiredGORM(ctx, database, intent.TenantID); err != nil {
		return Receipt{}, err
	}
	var state tenantStateRecord
	if err := database.Take(&state, "tenant_id = ?", intent.TenantID).Error; err != nil {
		return Receipt{}, mapDatabaseError(ctx, err)
	}
	createdAt := time.UnixMicro(state.HighWaterUnixMicro).UTC()
	retainMicro, ok := addDurationMicro(createdAt.UnixMicro(), defaultRetention)
	if !ok {
		return Receipt{}, ErrUnavailable
	}
	record, err := recordFromReceiptInput(intent, target, auditSequence, createdAt.UnixMicro(), retainMicro)
	if err != nil {
		return Receipt{}, err
	}
	if err := database.Create(&record).Error; err != nil {
		existing, found, readErr := Read(ctx, database, intent)
		if readErr != nil {
			return Receipt{}, readErr
		}
		if found {
			if existing.Target != target {
				return Receipt{}, ErrConflict
			}
			return existing, nil
		}
		return Receipt{}, mapDatabaseError(ctx, err)
	}
	return receiptFromRecord(record)
}

// AppendInSQLTransaction is AppendInTransaction for a caller-owned
// database/sql transaction. It never commits or rolls back transaction.
func AppendInSQLTransaction(
	ctx context.Context,
	transaction SQLTransaction,
	intent Intent,
	target Target,
	auditSequence *uint64,
	now time.Time,
) (Receipt, error) {
	if ctx == nil || transaction == nil || validateIntentIdentity(intent) != nil ||
		validateTarget(intent.Route, target) != nil {
		return Receipt{}, ErrInvalid
	}
	canonicalNow, err := canonicalTime(now)
	if err != nil {
		return Receipt{}, ErrInvalid
	}
	if _, err := transaction.ExecContext(
		ctx,
		`INSERT INTO api_mutation_receipt_tenant_state (
			tenant_id, receipt_count, encoded_bytes, high_water_unix_micro
		 ) VALUES (?, 0, 0, ?)
		 ON CONFLICT (tenant_id) DO NOTHING`,
		intent.TenantID,
		canonicalNow.UnixMicro(),
	); err != nil {
		return Receipt{}, mapDatabaseError(ctx, err)
	}
	if err := reclaimExpiredSQL(ctx, transaction, intent.TenantID); err != nil {
		return Receipt{}, err
	}
	if _, err := transaction.ExecContext(
		ctx,
		`UPDATE api_mutation_receipt_tenant_state
		 SET high_water_unix_micro = MAX(high_water_unix_micro, ?)
		 WHERE tenant_id = ?`,
		canonicalNow.UnixMicro(),
		intent.TenantID,
	); err != nil {
		return Receipt{}, mapDatabaseError(ctx, err)
	}
	var highWater int64
	if err := transaction.QueryRowContext(
		ctx,
		`SELECT high_water_unix_micro
		 FROM api_mutation_receipt_tenant_state WHERE tenant_id = ?`,
		intent.TenantID,
	).Scan(&highWater); err != nil {
		return Receipt{}, mapDatabaseError(ctx, err)
	}
	retainMicro, ok := addDurationMicro(highWater, defaultRetention)
	if !ok {
		return Receipt{}, ErrUnavailable
	}
	record, err := recordFromReceiptInput(
		intent, target, auditSequence, highWater, retainMicro,
	)
	if err != nil {
		return Receipt{}, err
	}
	_, err = transaction.ExecContext(
		ctx,
		`INSERT INTO api_mutation_receipts (
			tenant_id, actor_kind, actor_id, route, client_request_id,
			canonical_version, request_sha256, target_kind, target_id,
			target_version, audit_sequence, created_at_unix_micro,
			retain_until_unix_micro, target_terminal_at_unix_micro,
			encoded_bytes
		 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.TenantID,
		record.ActorKind,
		record.ActorID,
		record.Route,
		record.ClientRequestID,
		record.CanonicalVersion,
		record.RequestSHA256,
		record.TargetKind,
		record.TargetID,
		record.TargetVersion,
		record.AuditSequence,
		record.CreatedAtUnixMicro,
		record.RetainUntilUnixMicro,
		record.TargetTerminalUnixMicro,
		record.EncodedBytes,
	)
	if err != nil {
		existing, found, readErr := ReadSQL(ctx, transaction, intent)
		if readErr != nil {
			return Receipt{}, readErr
		}
		if found {
			if existing.Target != target {
				return Receipt{}, ErrConflict
			}
			return existing, nil
		}
		return Receipt{}, mapDatabaseError(ctx, err)
	}
	return receiptFromRecord(record)
}

// MarkTargetTerminalInTransaction closes every receipt for one asynchronous
// target and extends its fence through seven days after the rollback-safe
// terminal observation. A target without an idempotent receipt is a no-op.
func MarkTargetTerminalInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	targetKind string,
	targetID string,
	now time.Time,
) error {
	if ctx == nil || !activeTransaction(tx) || !validIdentity(tenantID, 255) ||
		(targetKind != TargetSearchJob && targetKind != TargetExportJob) ||
		!validIdentity(targetID, 256) {
		return ErrInvalid
	}
	canonicalNow, err := canonicalTime(now)
	if err != nil {
		return ErrInvalid
	}
	database := tx.WithContext(ctx)
	if err := database.Exec(
		`UPDATE api_mutation_receipt_tenant_state
		 SET high_water_unix_micro = MAX(high_water_unix_micro, ?)
		 WHERE tenant_id = ?`,
		canonicalNow.UnixMicro(), tenantID,
	).Error; err != nil {
		return mapDatabaseError(ctx, err)
	}
	var highWater int64
	if err := database.Raw(
		`SELECT high_water_unix_micro
		 FROM api_mutation_receipt_tenant_state WHERE tenant_id = ?`,
		tenantID,
	).Scan(&highWater).Error; err != nil {
		return mapDatabaseError(ctx, err)
	}
	if highWater == 0 {
		return nil
	}
	retainUntil, ok := addDurationMicro(highWater, defaultRetention)
	if !ok {
		return ErrUnavailable
	}
	result := database.Exec(
		`UPDATE api_mutation_receipts
		 SET target_terminal_at_unix_micro = ?,
		     retain_until_unix_micro = MAX(retain_until_unix_micro, ?)
		 WHERE tenant_id = ? AND target_kind = ? AND target_id = ?
		   AND target_terminal_at_unix_micro IS NULL`,
		highWater, retainUntil, tenantID, targetKind, targetID,
	)
	return mapDatabaseError(ctx, result.Error)
}

// ExtendRetentionInTransaction moves a receipt fence forward. Identity,
// request digest, target and accounting bytes remain immutable.
func ExtendRetentionInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	intent Intent,
	until time.Time,
) error {
	if ctx == nil || !activeTransaction(tx) || validateIntentIdentity(intent) != nil {
		return ErrInvalid
	}
	canonicalUntil, err := canonicalTime(until)
	if err != nil {
		return ErrInvalid
	}
	result := tx.WithContext(ctx).Exec(
		`UPDATE api_mutation_receipts
		 SET retain_until_unix_micro = MAX(retain_until_unix_micro, ?)
		 WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		   AND route = ? AND client_request_id = ?
		   AND canonical_version = ? AND request_sha256 = ?`,
		canonicalUntil.UnixMicro(),
		intent.TenantID,
		intent.ActorKind,
		intent.ActorID,
		intent.Route,
		intent.ClientRequestID,
		intent.CanonicalVersion,
		intent.RequestSHA256[:],
	)
	if result.Error != nil {
		return mapDatabaseError(ctx, result.Error)
	}
	if result.RowsAffected != 1 {
		return ErrUnavailable
	}
	return nil
}

func validateIntentIdentity(intent Intent) error {
	if intent.CanonicalVersion == 0 || intent.CanonicalVersion > math.MaxUint16 ||
		!validIdentity(intent.TenantID, 255) ||
		(intent.ActorKind != "system" && intent.ActorKind != "browser") ||
		!validIdentity(intent.ActorID, 255) || !validRoute(intent.Route) ||
		ValidateClientRequestID(intent.ClientRequestID) != nil {
		return ErrInvalid
	}
	return nil
}

func validateTarget(route string, target Target) error {
	if !validIdentity(target.ID, 256) || target.Version == 0 ||
		target.Version > math.MaxInt64 || routeTargetKind(route) != target.Kind {
		return ErrInvalid
	}
	return nil
}

func validRoute(route string) bool { return routeTargetKind(route) != "" }

func routeTargetKind(route string) string {
	switch route {
	case RouteCreateApp:
		return TargetApp
	case RouteCreateIndex:
		return TargetIndex
	case RouteCreateIngestionToken:
		return TargetIngestionToken
	case RouteCreateSearchJob:
		return TargetSearchJob
	case RouteCreateExportJob:
		return TargetExportJob
	case RouteCreateSavedSearch, RouteDuplicateSavedSearch:
		return TargetSavedSearch
	case RouteCreateLookup:
		return TargetLookup
	default:
		return ""
	}
}

func validIdentity(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value && !strings.ContainsRune(value, 0) &&
		!strings.ContainsFunc(value, func(character rune) bool {
			return character < 0x20 || (character >= 0x7f && character <= 0x9f)
		})
}

func activeTransaction(tx *gorm.DB) bool {
	if tx == nil || tx.Statement == nil || tx.Config == nil {
		return false
	}
	_, ok := tx.Statement.ConnPool.(*sql.Tx)
	return ok
}

func canonicalTime(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, ErrInvalid
	}
	micro := value.UnixMicro()
	if micro < 1 || micro > maximumUnixMicro {
		return time.Time{}, ErrInvalid
	}
	return time.UnixMicro(micro).UTC(), nil
}

func addDurationMicro(value int64, duration time.Duration) (int64, bool) {
	delta := duration.Microseconds()
	if value < 1 || delta <= 0 || value > maximumUnixMicro-delta {
		return 0, false
	}
	return value + delta, true
}

func recordFromReceiptInput(
	intent Intent,
	target Target,
	auditSequence *uint64,
	createdAt int64,
	retainUntil int64,
) (receiptRecord, error) {
	var storedAudit *int64
	if auditSequence != nil {
		if *auditSequence == 0 || *auditSequence > MaximumReceipts {
			return receiptRecord{}, ErrInvalid
		}
		value := int64(*auditSequence)
		storedAudit = &value
	}
	encodedBytes := receiptFixedBytes + uint64(len(intent.TenantID)) +
		uint64(len(intent.ActorKind)) + uint64(len(intent.ActorID)) +
		uint64(len(intent.Route)) + uint64(len(intent.ClientRequestID)) +
		sha256.Size + uint64(len(target.Kind)) + uint64(len(target.ID))
	if encodedBytes > math.MaxInt64 {
		return receiptRecord{}, ErrInvalid
	}
	var terminalAt *int64
	if target.Kind != TargetSearchJob && target.Kind != TargetExportJob {
		value := createdAt
		terminalAt = &value
	}
	return receiptRecord{
		TenantID: intent.TenantID, ActorKind: intent.ActorKind,
		ActorID: intent.ActorID, Route: intent.Route,
		ClientRequestID:  intent.ClientRequestID,
		CanonicalVersion: int64(intent.CanonicalVersion),
		RequestSHA256:    append([]byte(nil), intent.RequestSHA256[:]...),
		TargetKind:       target.Kind, TargetID: target.ID,
		TargetVersion: int64(target.Version), AuditSequence: storedAudit,
		CreatedAtUnixMicro: createdAt, RetainUntilUnixMicro: retainUntil,
		TargetTerminalUnixMicro: terminalAt, EncodedBytes: int64(encodedBytes),
	}, nil
}

func reclaimExpiredGORM(ctx context.Context, database *gorm.DB, tenantID string) error {
	result := database.Exec(
		`DELETE FROM api_mutation_receipts
		 WHERE (tenant_id, actor_kind, actor_id, route, client_request_id) IN (
		     SELECT tenant_id, actor_kind, actor_id, route, client_request_id
		     FROM api_mutation_receipts
		     WHERE tenant_id = ?
		       AND target_terminal_at_unix_micro IS NOT NULL
		       AND retain_until_unix_micro <= (
		           SELECT high_water_unix_micro
		           FROM api_mutation_receipt_tenant_state WHERE tenant_id = ?
		       )
		     ORDER BY retain_until_unix_micro, actor_kind, actor_id, route,
		              client_request_id
		     LIMIT ?
		 )`,
		tenantID, tenantID, reclaimBatchSize,
	)
	return mapDatabaseError(ctx, result.Error)
}

func reclaimExpiredSQL(ctx context.Context, transaction SQLTransaction, tenantID string) error {
	_, err := transaction.ExecContext(
		ctx,
		`DELETE FROM api_mutation_receipts
		 WHERE (tenant_id, actor_kind, actor_id, route, client_request_id) IN (
		     SELECT tenant_id, actor_kind, actor_id, route, client_request_id
		     FROM api_mutation_receipts
		     WHERE tenant_id = ?
		       AND target_terminal_at_unix_micro IS NOT NULL
		       AND retain_until_unix_micro <= (
		           SELECT high_water_unix_micro
		           FROM api_mutation_receipt_tenant_state WHERE tenant_id = ?
		       )
		     ORDER BY retain_until_unix_micro, actor_kind, actor_id, route,
		              client_request_id
		     LIMIT ?
		 )`,
		tenantID, tenantID, reclaimBatchSize,
	)
	return mapDatabaseError(ctx, err)
}

func receiptFromRecord(record receiptRecord) (Receipt, error) {
	if record.CanonicalVersion < 1 || record.CanonicalVersion > math.MaxUint16 ||
		len(record.RequestSHA256) != sha256.Size || record.TargetVersion < 1 ||
		record.EncodedBytes < 1 {
		return Receipt{}, ErrCorrupt
	}
	var digest [sha256.Size]byte
	copy(digest[:], record.RequestSHA256)
	intent := Intent{
		TenantID: record.TenantID, ActorKind: record.ActorKind,
		ActorID: record.ActorID, Route: record.Route,
		ClientRequestID:  record.ClientRequestID,
		CanonicalVersion: uint32(record.CanonicalVersion), RequestSHA256: digest,
	}
	target := Target{
		Kind: record.TargetKind, ID: record.TargetID,
		Version: uint64(record.TargetVersion),
	}
	if validateIntentIdentity(intent) != nil || validateTarget(intent.Route, target) != nil {
		return Receipt{}, ErrCorrupt
	}
	createdAt, err := canonicalTime(time.UnixMicro(record.CreatedAtUnixMicro))
	if err != nil {
		return Receipt{}, ErrCorrupt
	}
	retainUntil, err := canonicalTime(time.UnixMicro(record.RetainUntilUnixMicro))
	if err != nil || retainUntil.Before(createdAt) {
		return Receipt{}, ErrCorrupt
	}
	var auditSequence *uint64
	if record.AuditSequence != nil {
		if *record.AuditSequence < 1 || *record.AuditSequence > int64(MaximumReceipts) {
			return Receipt{}, ErrCorrupt
		}
		value := uint64(*record.AuditSequence)
		auditSequence = &value
	}
	var terminalAt *time.Time
	if record.TargetTerminalUnixMicro != nil {
		value, terminalErr := canonicalTime(time.UnixMicro(*record.TargetTerminalUnixMicro))
		if terminalErr != nil || value.Before(createdAt) {
			return Receipt{}, ErrCorrupt
		}
		terminalAt = &value
	}
	return Receipt{
		Intent: intent, Target: target, AuditSequence: auditSequence,
		CreatedAt: createdAt, RetainUntil: retainUntil, TerminalAt: terminalAt,
		EncodedBytes: uint64(record.EncodedBytes),
	}, nil
}

func scanReceipt(row *sql.Row) (receiptRecord, error) {
	var record receiptRecord
	var auditSequence sql.NullInt64
	var terminalAt sql.NullInt64
	err := row.Scan(
		&record.TenantID,
		&record.ActorKind,
		&record.ActorID,
		&record.Route,
		&record.ClientRequestID,
		&record.CanonicalVersion,
		&record.RequestSHA256,
		&record.TargetKind,
		&record.TargetID,
		&record.TargetVersion,
		&auditSequence,
		&record.CreatedAtUnixMicro,
		&record.RetainUntilUnixMicro,
		&terminalAt,
		&record.EncodedBytes,
	)
	if err != nil {
		return receiptRecord{}, err
	}
	if auditSequence.Valid {
		record.AuditSequence = &auditSequence.Int64
	}
	if terminalAt.Valid {
		record.TargetTerminalUnixMicro = &terminalAt.Int64
	}
	return record, nil
}

func mapDatabaseError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	message := err.Error()
	switch {
	case strings.Contains(message, "capacity is exhausted"):
		return ErrCapacity
	case strings.Contains(message, "UNIQUE constraint failed"),
		strings.Contains(message, "identity already exists"):
		return ErrConflict
	case strings.Contains(message, "CHECK constraint failed"),
		strings.Contains(message, "state transition is invalid"),
		strings.Contains(message, "accounting failed"):
		return ErrCorrupt
	default:
		return fmt.Errorf("request idempotency storage unavailable: %w", err)
	}
}
