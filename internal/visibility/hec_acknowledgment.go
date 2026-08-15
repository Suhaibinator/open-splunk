package visibility

import (
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/auth"
)

const (
	// One random block covers every acknowledgment that a single token may
	// retain. Per-scope block state prevents unrelated channel traffic from
	// weakening the rollback-branch collision bound, while 36+17 bits keep the
	// resulting ID within JavaScript's exact-integer range.
	hecAcknowledgmentCounterBits                 = 17
	hecAcknowledgmentIDsPerBlock                 = uint64(1) << hecAcknowledgmentCounterBits
	hecAcknowledgmentPrefixMask                  = (uint64(1) << 36) - 1
	maximumHECAcknowledgmentID                   = (uint64(1) << 53) - 1
	maximumHECAcknowledgmentIDGenerationAttempts = 8
	maximumHECAcknowledgmentIDCollisionRetries   = 16
)

type hecAcknowledgmentScope struct {
	tenantID string
	tokenID  string
	channel  string
}

type hecAcknowledgmentIDSource interface {
	ID(hecAcknowledgmentScope, uint64, uint64) (uint64, error)
}

type keyedHECAcknowledgmentIDSource struct {
	key [sha256.Size]byte
}

// HECAcknowledgmentReader is the bounded channel-scoped acknowledgment lookup
// surface used by the HEC transport after it authenticates the same token.
type HECAcknowledgmentReader interface {
	LookupHECAcknowledgments(
		context.Context,
		string,
		string,
		string,
		[]uint64,
	) (map[uint64]bool, error)
}

func persistHECAdmission(
	ctx context.Context,
	tx *sql.Tx,
	request *HECAdmissionRequest,
	visibilitySequence int64,
	acknowledgmentIDs hecAcknowledgmentIDSource,
) (uint64, uint64, error) {
	if request == nil {
		return 0, 0, nil
	}
	createdAtMicros := request.CreatedAt.Round(0).UTC().UnixMicro()

	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO hec_source_sequences (
			tenant_id, ingestion_token_id, next_request_sequence,
			updated_at_unix_micro
		) VALUES (?, ?, 1, ?)`,
		request.TenantID,
		request.TokenID,
		createdAtMicros,
	); err != nil {
		return 0, 0, fmt.Errorf("initialize HEC request sequence: %w", err)
	}
	var requestSequence int64
	if err := tx.QueryRowContext(ctx, `
		SELECT next_request_sequence
		FROM hec_source_sequences
		WHERE tenant_id = ? AND ingestion_token_id = ?`,
		request.TenantID,
		request.TokenID,
	).Scan(&requestSequence); err != nil {
		return 0, 0, fmt.Errorf("read HEC request sequence: %w", err)
	}
	if requestSequence < 1 || requestSequence == math.MaxInt64 {
		return 0, 0, ErrHECRequestCapacity
	}
	sequenceResult, err := tx.ExecContext(ctx, `
		UPDATE hec_source_sequences
		SET next_request_sequence = ?, updated_at_unix_micro = ?
		WHERE tenant_id = ?
		  AND ingestion_token_id = ?
		  AND next_request_sequence = ?`,
		requestSequence+1,
		createdAtMicros,
		request.TenantID,
		request.TokenID,
		requestSequence,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("advance HEC request sequence: %w", err)
	}
	if err := requireExactlyOneRow(sequenceResult); err != nil {
		return 0, 0, fmt.Errorf("advance HEC request sequence: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO hec_requests (
			tenant_id, ingestion_token_id, request_sequence, request_id,
			visibility_sequence, state, created_at_unix_micro,
			terminal_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, 'pending', ?, NULL)`,
		request.TenantID,
		request.TokenID,
		requestSequence,
		request.RequestID,
		visibilitySequence,
		createdAtMicros,
	); err != nil {
		if sqliteConstraint(err) {
			return 0, 0, ErrConflict
		}
		return 0, 0, fmt.Errorf("persist staged HEC request: %w", err)
	}

	if !request.Acknowledgment {
		return uint64(requestSequence), 0, nil
	}
	acknowledgmentID, err := allocateHECAcknowledgment(
		ctx,
		tx,
		request,
		requestSequence,
		createdAtMicros,
		acknowledgmentIDs,
	)
	if err != nil {
		return 0, 0, err
	}
	return uint64(requestSequence), acknowledgmentID, nil
}

// preflightHECAdmission closes authority and HEC-specific capacity before
// quota planning or durable outbox allocation. Reserve retains one SQLite
// transaction across this preflight and persistence, so the observed bounds
// cannot change between the two steps.
func preflightHECAdmission(
	ctx context.Context,
	tx *sql.Tx,
	request *HECAdmissionRequest,
	checkedAt time.Time,
) error {
	if request == nil {
		return nil
	}
	authority := auth.HECAdmissionAuthority{
		TokenID:               request.TokenID,
		TokenVersion:          request.TokenVersion,
		IndexerAcknowledgment: request.Acknowledgment,
		Indexes:               make([]auth.HECIndexAuthoritySnapshot, len(request.AuthorizedIndexes)),
	}
	for index, selected := range request.AuthorizedIndexes {
		authority.Indexes[index] = auth.HECIndexAuthoritySnapshot{
			Name: selected.Name, Version: selected.Version,
		}
	}
	if err := auth.RevalidateHECAdmissionInTransaction(
		ctx,
		tx,
		authority,
		checkedAt,
	); err != nil {
		if errors.Is(err, auth.ErrStaleHECAdmission) {
			return ErrHECAdmissionStale
		}
		return fmt.Errorf("revalidate HEC admission snapshot: %w", err)
	}

	var retainedRequests int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*)
		FROM hec_requests
		WHERE tenant_id = ? AND ingestion_token_id = ?`,
		request.TenantID,
		request.TokenID,
	).Scan(&retainedRequests); err != nil {
		return fmt.Errorf("read retained HEC request capacity: %w", err)
	}
	if retainedRequests >= MaxHECRequestsPerToken {
		return ErrHECRequestCapacity
	}
	if !request.Acknowledgment {
		return nil
	}
	var retainedAcknowledgments int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*)
		FROM hec_acknowledgments
		WHERE tenant_id = ? AND ingestion_token_id = ?`,
		request.TenantID,
		request.TokenID,
	).Scan(&retainedAcknowledgments); err != nil {
		return fmt.Errorf("read retained HEC acknowledgment capacity: %w", err)
	}
	if retainedAcknowledgments >= MaxHECAcknowledgmentsPerToken {
		return ErrHECAcknowledgmentCapacity
	}
	var channelExists int
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM hec_channels
			WHERE tenant_id = ?
			  AND ingestion_token_id = ?
			  AND channel_id = ?
		)`,
		request.TenantID,
		request.TokenID,
		request.AcknowledgmentChannel,
	).Scan(&channelExists); err != nil {
		return fmt.Errorf("read HEC channel existence: %w", err)
	}
	if channelExists != 0 {
		return nil
	}
	var retainedChannels int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*)
		FROM hec_channels
		WHERE tenant_id = ? AND ingestion_token_id = ?`,
		request.TenantID,
		request.TokenID,
	).Scan(&retainedChannels); err != nil {
		return fmt.Errorf("read retained HEC channel capacity: %w", err)
	}
	if retainedChannels >= MaxHECChannelsPerToken {
		return ErrHECAcknowledgmentCapacity
	}
	return nil
}

func allocateHECAcknowledgment(
	ctx context.Context,
	tx *sql.Tx,
	request *HECAdmissionRequest,
	requestSequence int64,
	createdAtMicros int64,
	acknowledgmentIDs hecAcknowledgmentIDSource,
) (uint64, error) {
	var retained int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*)
		FROM hec_acknowledgments
		WHERE tenant_id = ? AND ingestion_token_id = ?`,
		request.TenantID,
		request.TokenID,
	).Scan(&retained); err != nil {
		return 0, fmt.Errorf("read retained HEC acknowledgment capacity: %w", err)
	}
	if retained >= MaxHECAcknowledgmentsPerToken {
		return 0, ErrHECAcknowledgmentCapacity
	}

	var allocationOrdinal int64
	err := tx.QueryRowContext(ctx, `
		SELECT next_acknowledgment_id
		FROM hec_channels
		WHERE tenant_id = ?
		  AND ingestion_token_id = ?
		  AND channel_id = ?`,
		request.TenantID,
		request.TokenID,
		request.AcknowledgmentChannel,
	).Scan(&allocationOrdinal)
	if errors.Is(err, sql.ErrNoRows) {
		var channels int64
		if countErr := tx.QueryRowContext(ctx, `
			SELECT count(*)
			FROM hec_channels
			WHERE tenant_id = ? AND ingestion_token_id = ?`,
			request.TenantID,
			request.TokenID,
		).Scan(&channels); countErr != nil {
			return 0, fmt.Errorf("read HEC channel capacity: %w", countErr)
		}
		if channels >= MaxHECChannelsPerToken {
			return 0, ErrHECAcknowledgmentCapacity
		}
		if _, insertErr := tx.ExecContext(ctx, `
			INSERT INTO hec_channels (
				tenant_id, ingestion_token_id, channel_id,
				next_acknowledgment_id, created_at_unix_micro,
				last_used_at_unix_micro
			) VALUES (?, ?, ?, 1, ?, ?)`,
			request.TenantID,
			request.TokenID,
			request.AcknowledgmentChannel,
			createdAtMicros,
			createdAtMicros,
		); insertErr != nil {
			return 0, fmt.Errorf("persist HEC channel: %w", insertErr)
		}
		allocationOrdinal = 1
	} else if err != nil {
		return 0, fmt.Errorf("read HEC acknowledgment allocation ordinal: %w", err)
	}
	if allocationOrdinal < 1 || allocationOrdinal == math.MaxInt64 {
		return 0, ErrHECAcknowledgmentCapacity
	}
	acknowledgmentID, err := selectUniqueHECAcknowledgmentID(
		ctx,
		tx,
		request,
		acknowledgmentIDs,
		uint64(allocationOrdinal),
	)
	if err != nil {
		return 0, err
	}
	acknowledgmentResult, err := tx.ExecContext(ctx, `
		UPDATE hec_channels
		SET next_acknowledgment_id = ?, last_used_at_unix_micro = ?
		WHERE tenant_id = ?
		  AND ingestion_token_id = ?
		  AND channel_id = ?
		  AND next_acknowledgment_id = ?`,
		allocationOrdinal+1,
		createdAtMicros,
		request.TenantID,
		request.TokenID,
		request.AcknowledgmentChannel,
		allocationOrdinal,
	)
	if err != nil {
		return 0, fmt.Errorf("advance HEC acknowledgment allocation ordinal: %w", err)
	}
	if err := requireExactlyOneRow(acknowledgmentResult); err != nil {
		return 0, fmt.Errorf("advance HEC acknowledgment allocation ordinal: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO hec_acknowledgments (
			tenant_id, ingestion_token_id, channel_id,
			acknowledgment_id, request_sequence, created_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, ?)`,
		request.TenantID,
		request.TokenID,
		request.AcknowledgmentChannel,
		acknowledgmentID,
		requestSequence,
		createdAtMicros,
	); err != nil {
		return 0, fmt.Errorf("persist HEC acknowledgment: %w", err)
	}
	return acknowledgmentID, nil
}

func newKeyedHECAcknowledgmentIDSource() (*keyedHECAcknowledgmentIDSource, error) {
	source := &keyedHECAcknowledgmentIDSource{}
	if _, err := cryptorand.Read(source.key[:]); err != nil {
		return nil, fmt.Errorf("initialize HEC acknowledgment ID source: %w", err)
	}
	return source, nil
}

// ID returns an opaque positive value that fits SQLite's signed INTEGER domain,
// HEC's JSON integer wire contract, and JavaScript's exact integer range. A
// process-random key namespaces the durable allocation ordinal, so restoring an
// older SQLite snapshot under a new process does not deterministically reissue
// a discarded branch's public ID. The source keeps constant memory regardless
// of scope churn or process lifetime.
func (source *keyedHECAcknowledgmentIDSource) ID(
	scope hecAcknowledgmentScope,
	allocationOrdinal uint64,
	collisionAttempt uint64,
) (uint64, error) {
	if source == nil {
		return 0, errors.New("HEC acknowledgment ID source is unavailable")
	}
	if allocationOrdinal < 1 {
		return 0, errors.New("HEC acknowledgment allocation ordinal is invalid")
	}
	zeroBasedOrdinal := allocationOrdinal - 1
	blockOrdinal := zeroBasedOrdinal >> hecAcknowledgmentCounterBits
	counter := zeroBasedOrdinal & (hecAcknowledgmentIDsPerBlock - 1)
	for derivationAttempt := range uint64(maximumHECAcknowledgmentIDGenerationAttempts) {
		mac := hmac.New(sha256.New, source.key[:])
		_, _ = mac.Write([]byte("open-splunk/hec-ack/v1\x00"))
		writeHECAcknowledgmentIDScope(mac, scope)
		var numbers [24]byte
		binary.BigEndian.PutUint64(numbers[0:8], blockOrdinal)
		binary.BigEndian.PutUint64(numbers[8:16], collisionAttempt)
		binary.BigEndian.PutUint64(numbers[16:24], derivationAttempt)
		_, _ = mac.Write(numbers[:])
		digest := mac.Sum(nil)
		prefix := binary.BigEndian.Uint64(digest[:8]) & hecAcknowledgmentPrefixMask
		if prefix != 0 {
			return prefix<<hecAcknowledgmentCounterBits | counter, nil
		}
	}
	return 0, errors.New("derive HEC acknowledgment ID block: repeatedly produced zero prefix")
}

func writeHECAcknowledgmentIDScope(hash hash.Hash, scope hecAcknowledgmentScope) {
	var length [4]byte
	for _, value := range []string{scope.tenantID, scope.tokenID, scope.channel} {
		binary.BigEndian.PutUint32(length[:], uint32(len(value))) // #nosec G115 -- scope components are admission-bounded far below MaxUint32.
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
}

func selectUniqueHECAcknowledgmentID(
	ctx context.Context,
	tx *sql.Tx,
	request *HECAdmissionRequest,
	source hecAcknowledgmentIDSource,
	allocationOrdinal uint64,
) (uint64, error) {
	if source == nil {
		return 0, errors.New("generate HEC acknowledgment ID: generator is unavailable")
	}
	scope := hecAcknowledgmentScope{
		tenantID: request.TenantID,
		tokenID:  request.TokenID,
		channel:  request.AcknowledgmentChannel,
	}
	for attempt := range maximumHECAcknowledgmentIDCollisionRetries {
		acknowledgmentID, err := source.ID(scope, allocationOrdinal, uint64(attempt))
		if err != nil {
			return 0, fmt.Errorf("generate HEC acknowledgment ID: %w", err)
		}
		if acknowledgmentID == 0 || acknowledgmentID > maximumHECAcknowledgmentID {
			return 0, errors.New("generate HEC acknowledgment ID: value is outside the exact positive JSON integer range")
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM hec_acknowledgments
				WHERE tenant_id = ?
				  AND ingestion_token_id = ?
				  AND channel_id = ?
				  AND acknowledgment_id = ?
			)`,
			request.TenantID,
			request.TokenID,
			request.AcknowledgmentChannel,
			acknowledgmentID,
		).Scan(&exists); err != nil {
			return 0, fmt.Errorf("check HEC acknowledgment ID collision: %w", err)
		}
		if exists == 0 {
			return acknowledgmentID, nil
		}
	}
	return 0, fmt.Errorf("allocate unique HEC acknowledgment ID: %w", ErrConflict)
}

func requireExactlyOneRow(result sql.Result) error {
	if result == nil {
		return ErrConflict
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrConflict
	}
	return nil
}

// LookupHECAcknowledgments returns false for pending, failed, expired, and
// unknown IDs without revealing rows from another token or channel.
func (sequencer *SQLiteSequencer) LookupHECAcknowledgments(
	ctx context.Context,
	tenantID string,
	tokenID string,
	channel string,
	acknowledgmentIDs []uint64,
) (result map[uint64]bool, resultErr error) {
	if err := sequencer.beginOperation(); err != nil {
		return nil, err
	}
	defer sequencer.endOperation()
	if err := validateHECAcknowledgmentLookup(ctx, tenantID, tokenID, channel, acknowledgmentIDs); err != nil {
		return nil, err
	}
	result = make(map[uint64]bool, len(acknowledgmentIDs))
	var statement strings.Builder
	statement.Grow(512 + len(acknowledgmentIDs)*3)
	statement.WriteString(`
		SELECT acknowledgment.acknowledgment_id, request.state
		FROM hec_acknowledgments AS acknowledgment
		JOIN hec_requests AS request
		  ON request.tenant_id = acknowledgment.tenant_id
		 AND request.ingestion_token_id = acknowledgment.ingestion_token_id
		 AND request.request_sequence = acknowledgment.request_sequence
		WHERE acknowledgment.tenant_id = ?
		  AND acknowledgment.ingestion_token_id = ?
		  AND acknowledgment.channel_id = ?
		  AND acknowledgment.acknowledgment_id IN (`)
	arguments := make([]any, 0, len(acknowledgmentIDs)+3)
	arguments = append(arguments, tenantID, tokenID, channel)
	for index, acknowledgmentID := range acknowledgmentIDs {
		if index > 0 {
			statement.WriteString(", ")
		}
		statement.WriteByte('?')
		arguments = append(arguments, acknowledgmentID)
		result[acknowledgmentID] = false
	}
	statement.WriteByte(')')
	// #nosec G201 -- the dynamic portion contains only one placeholder per
	// already bounded acknowledgment ID; all values remain bound arguments.
	rows, err := sequencer.db.QueryContext(ctx, statement.String(), arguments...)
	if err != nil {
		return nil, fmt.Errorf("query HEC acknowledgments: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var acknowledgmentID uint64
		var state string
		if err := rows.Scan(&acknowledgmentID, &state); err != nil {
			return nil, fmt.Errorf("scan HEC acknowledgment: %w", err)
		}
		result[acknowledgmentID] = state == "indexed"
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate HEC acknowledgments: %w", err)
	}
	return result, nil
}

func validateHECAcknowledgmentLookup(
	ctx context.Context,
	tenantID string,
	tokenID string,
	channel string,
	acknowledgmentIDs []uint64,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if tenantID == "" || len(tenantID) > 255 || !utf8.ValidString(tenantID) ||
		strings.TrimSpace(tenantID) != tenantID || strings.IndexByte(tenantID, 0) >= 0 {
		return fmt.Errorf("%w: HEC tenant ID is invalid", ErrInvalidArgument)
	}
	if tokenID == "" || len(tokenID) > 128 || !utf8.ValidString(tokenID) ||
		strings.TrimSpace(tokenID) != tokenID || strings.IndexByte(tokenID, 0) >= 0 {
		return fmt.Errorf("%w: HEC token ID is invalid", ErrInvalidArgument)
	}
	if channel == "" || len(channel) > 128 || !utf8.ValidString(channel) ||
		strings.TrimSpace(channel) != channel || strings.IndexByte(channel, 0) >= 0 {
		return fmt.Errorf("%w: HEC channel is invalid", ErrInvalidArgument)
	}
	if len(acknowledgmentIDs) == 0 || len(acknowledgmentIDs) > MaxHECAcknowledgmentsPerQuery {
		return fmt.Errorf("%w: HEC acknowledgment query count is invalid", ErrInvalidArgument)
	}
	seen := make(map[uint64]struct{}, len(acknowledgmentIDs))
	for _, acknowledgmentID := range acknowledgmentIDs {
		if acknowledgmentID == 0 {
			return fmt.Errorf("%w: HEC acknowledgment ID is invalid", ErrInvalidArgument)
		}
		if _, duplicate := seen[acknowledgmentID]; duplicate {
			return fmt.Errorf("%w: HEC acknowledgment ID is duplicated", ErrInvalidArgument)
		}
		seen[acknowledgmentID] = struct{}{}
	}
	return nil
}

// PruneHECTerminalRequests deletes only terminal request rows older than the
// supplied retention boundary. Pending acknowledgments are never deleted.
func (sequencer *SQLiteSequencer) PruneHECTerminalRequests(
	ctx context.Context,
	terminalBefore time.Time,
	limit uint32,
) (deleted uint32, resultErr error) {
	if err := sequencer.beginOperation(); err != nil {
		return 0, err
	}
	defer sequencer.endOperation()
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if terminalBefore.IsZero() || limit == 0 || limit > MaxPruneLimit {
		return 0, fmt.Errorf("%w: invalid HEC terminal retention request", ErrInvalidArgument)
	}
	result, err := sequencer.db.ExecContext(ctx, `
		DELETE FROM hec_requests
		WHERE (tenant_id, ingestion_token_id, request_sequence) IN (
			SELECT tenant_id, ingestion_token_id, request_sequence
			FROM hec_requests
			WHERE state IN ('indexed', 'terminal_failure')
			  AND terminal_at_unix_micro < ?
			ORDER BY terminal_at_unix_micro,
			         tenant_id,
			         ingestion_token_id,
			         request_sequence
			LIMIT ?
		)`,
		terminalBefore.UTC().UnixMicro(),
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("prune terminal HEC requests: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil || count < 0 || count > int64(limit) {
		return 0, errors.New("prune terminal HEC requests returned an invalid row count")
	}
	return uint32(count), nil // #nosec G115 -- count is nonnegative and bounded by the uint32 limit above.
}

var _ HECAcknowledgmentReader = (*SQLiteSequencer)(nil)
