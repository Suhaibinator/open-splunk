package clickhouse

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strings"
	"unicode/utf8"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/indexname"
	"github.com/Suhaibinator/open-splunk/internal/protocolid"
	"github.com/google/uuid"
)

const indexDataDeletionProtocolVersion uint32 = 1

const (
	maximumDeletionTenantBytes             = 255
	maximumDeletionPhysicalIdentifierBytes = 255
	maximumDeletionDiagnosticBytes         = 4096
	maximumDeletionFailureCodeBytes        = 128
)

var (
	// ErrIndexDataDeletionTargetChanged means the configured database/table
	// now resolves to a different physical UUID than the durable attempt.
	ErrIndexDataDeletionTargetChanged = errors.New(
		"ClickHouse index data deletion target changed",
	)
)

// IndexDataDeletionTarget identifies the physical ClickHouse table generation
// that a control-plane mutation attempt must bind before its first ALTER.
type IndexDataDeletionTarget struct {
	Database  string
	Table     string
	TableUUID string
}

// IndexDataDeletionRequest is the native, non-GORM input reconstructed from
// one durable control-plane operation and its immutable mutation attempt.
// TenantID is the owning single-node deployment tenant; rows under another
// tenant key are deliberately outside this deletion operation's scope.
type IndexDataDeletionRequest struct {
	OperationID     string
	CorrelationID   string
	TenantID        string
	IndexName       string
	Database        string
	Table           string
	TableUUID       string
	ProtocolVersion uint32
}

// IndexDataDeletionState describes reconciliation and frozen-advancement
// outcomes. Ready means no correlated mutation is pending, so the coordinator
// should reacquire the write freeze, drain, and call AdvanceIndexDataDeletion.
type IndexDataDeletionState uint8

const (
	IndexDataDeletionPending IndexDataDeletionState = iota + 1
	IndexDataDeletionReady
	IndexDataDeletionPhysicallyEmpty
)

// IndexDataDeletionProgress is a bounded reconciliation snapshot. A failure
// reason is diagnostic only while PendingMutations is nonzero; ClickHouse may
// continue retrying that mutation.
type IndexDataDeletionProgress struct {
	State               IndexDataDeletionState
	MatchingMutations   uint64
	PendingMutations    uint64
	PendingParts        int64
	LatestMutationBlock int64
	LatestFailureCode   string
	LatestFailureReason string
	SubmissionAttempted bool
	SubmissionAccepted  bool
}

type indexDataDeletionMutationSummary struct {
	matching        uint64
	pending         uint64
	pendingParts    int64
	latestBlock     int64
	invalidCommands uint64
	failureCode     string
	failureReason   string
}

// IndexDataDeletionStatus polls read-only ClickHouse mutation state without
// acquiring the global write freeze. It never submits a mutation or performs a
// terminal physical-absence proof.
func (s *Store) IndexDataDeletionStatus(
	ctx context.Context,
	request IndexDataDeletionRequest,
) (result IndexDataDeletionProgress, resultErr error) {
	if frozenCallbackActive(ctx, s) {
		return IndexDataDeletionProgress{}, ErrWriteFreezeReentrant
	}
	operationContext, finishOperation, err := s.beginOperation(
		ctx,
		&resultErr,
	)
	if err != nil {
		return IndexDataDeletionProgress{}, err
	}
	defer finishOperation()

	return s.indexDataDeletionStatus(operationContext, request)
}

func (s *Store) indexDataDeletionStatus(
	ctx context.Context,
	request IndexDataDeletionRequest,
) (IndexDataDeletionProgress, error) {
	if err := validateIndexDataDeletionRequest(request); err != nil {
		return IndexDataDeletionProgress{}, err
	}
	if request.Database != s.database || request.Table != s.table {
		return IndexDataDeletionProgress{}, fmt.Errorf(
			"%w: durable database/table does not match this Store",
			ErrIndexDataDeletionTargetChanged,
		)
	}
	summary, err := s.indexDataDeletionReconciliation(
		ctx,
		request,
		indexDataDeletionCorrelationMarker(request),
	)
	if err != nil {
		return IndexDataDeletionProgress{}, err
	}
	progress := progressForIndexDataDeletionSummary(summary)
	if summary.invalidCommands != 0 {
		return progress, errors.New(
			"reconcile ClickHouse index data deletion: correlated mutation has an unexpected command",
		)
	}
	if summary.pending != 0 {
		progress.State = IndexDataDeletionPending
	} else {
		progress.State = IndexDataDeletionReady
	}
	return progress, nil
}

// IndexDataDeletionTarget resolves the Store's configured physical table
// generation after a successful callback-scoped outbox drain.
func (frozen *frozenWrites) IndexDataDeletionTarget(
	ctx context.Context,
) (IndexDataDeletionTarget, error) {
	operationContext, finish, err := frozen.beginPrivilegedOperation(
		ctx,
		"resolve ClickHouse index data deletion target",
		true,
	)
	if err != nil {
		return IndexDataDeletionTarget{}, err
	}
	defer finish()

	target, err := frozen.store.indexDataDeletionTarget(operationContext)
	return target, frozen.store.operationError(ctx, err)
}

// AdvanceIndexDataDeletion reconciles a stable mutation marker and performs at
// most one asynchronous ALTER DELETE. It never infers completion from missing
// mutation history: an exact key-aligned physical absence check is the final
// authority for this frozen advancement. The Store's documented exclusive-DDL
// ownership invariant must hold throughout the operation.
func (frozen *frozenWrites) AdvanceIndexDataDeletion(
	ctx context.Context,
	request IndexDataDeletionRequest,
) (IndexDataDeletionProgress, error) {
	operationContext, finish, err := frozen.beginPrivilegedOperation(
		ctx,
		"advance ClickHouse index data deletion",
		true,
	)
	if err != nil {
		return IndexDataDeletionProgress{}, err
	}
	defer finish()

	progress, err := frozen.store.advanceIndexDataDeletion(
		operationContext,
		request,
	)
	return progress, frozen.store.operationError(ctx, err)
}

func (s *Store) advanceIndexDataDeletion(
	ctx context.Context,
	request IndexDataDeletionRequest,
) (IndexDataDeletionProgress, error) {
	if err := validateIndexDataDeletionRequest(request); err != nil {
		return IndexDataDeletionProgress{}, err
	}
	if request.Database != s.database || request.Table != s.table {
		return IndexDataDeletionProgress{}, fmt.Errorf(
			"%w: durable database/table does not match this Store",
			ErrIndexDataDeletionTargetChanged,
		)
	}
	marker := indexDataDeletionCorrelationMarker(request)
	before, err := s.indexDataDeletionReconciliation(ctx, request, marker)
	if err != nil {
		return IndexDataDeletionProgress{}, err
	}
	progress := progressForIndexDataDeletionSummary(before)
	if before.invalidCommands != 0 {
		return progress, errors.New(
			"reconcile ClickHouse index data deletion: correlated mutation has an unexpected command",
		)
	}
	if before.pending != 0 {
		progress.State = IndexDataDeletionPending
		return progress, nil
	}

	exists, err := s.indexDataDeletionRowsExist(ctx, request)
	if err != nil {
		return progress, err
	}
	if !exists {
		progress.State = IndexDataDeletionPhysicallyEmpty
		return progress, nil
	}

	progress.SubmissionAttempted = true
	submitErr := s.submitIndexDataDeletionMutation(ctx, request, marker)

	after, reconcileErr := s.indexDataDeletionReconciliation(
		ctx,
		request,
		marker,
	)
	if reconcileErr != nil {
		if submitErr != nil {
			return progress, errors.Join(submitErr, reconcileErr)
		}
		return progress, reconcileErr
	}
	progress = progressForIndexDataDeletionSummary(after)
	progress.SubmissionAttempted = true
	if after.invalidCommands != 0 {
		commandErr := errors.New(
			"reconcile ClickHouse index data deletion after submission: correlated mutation has an unexpected command",
		)
		if submitErr != nil {
			return progress, errors.Join(submitErr, commandErr)
		}
		return progress, commandErr
	}
	if after.latestBlock > before.latestBlock {
		progress.SubmissionAccepted = true
		if after.pending != 0 {
			progress.State = IndexDataDeletionPending
			return progress, nil
		}
		exists, err = s.indexDataDeletionRowsExist(ctx, request)
		if err != nil {
			return progress, err
		}
		if !exists {
			progress.State = IndexDataDeletionPhysicallyEmpty
			return progress, nil
		}
		return progress, errors.New(
			"advance ClickHouse index data deletion: accepted mutation completed but target rows remain under the write freeze",
		)
	}

	exists, err = s.indexDataDeletionRowsExist(ctx, request)
	if err != nil {
		if submitErr != nil {
			return progress, errors.Join(submitErr, err)
		}
		return progress, err
	}
	if !exists {
		progress.State = IndexDataDeletionPhysicallyEmpty
		return progress, nil
	}
	if submitErr != nil {
		return progress, fmt.Errorf(
			"submit ClickHouse index data deletion mutation: %w",
			submitErr,
		)
	}
	return progress, errors.New(
		"submit ClickHouse index data deletion mutation: ClickHouse returned without a newer correlated mutation",
	)
}

func (s *Store) indexDataDeletionTarget(
	ctx context.Context,
) (IndexDataDeletionTarget, error) {
	parameters := clickhousedriver.Parameters{
		"database": s.database,
		"table":    s.table,
	}
	var tableUUID, engine string
	err := s.connection.queryRow(
		ctx,
		`SELECT toString(uuid), engine
FROM system.tables
WHERE database = {database:String}
  AND name = {table:String}
LIMIT 1`,
		parameters,
	).Scan(&tableUUID, &engine)
	if err != nil {
		return IndexDataDeletionTarget{}, fmt.Errorf(
			"resolve ClickHouse index data deletion target: %w",
			err,
		)
	}
	return s.validatedIndexDataDeletionTarget(tableUUID, engine)
}

func (s *Store) indexDataDeletionReconciliation(
	ctx context.Context,
	request IndexDataDeletionRequest,
	marker string,
) (indexDataDeletionMutationSummary, error) {
	parameters := clickhousedriver.Parameters{
		"database":    request.Database,
		"table":       request.Table,
		"correlation": marker,
	}
	var tableUUID, engine string
	var summary indexDataDeletionMutationSummary
	err := s.connection.queryRow(
		ctx,
		`SELECT
    any(toString(target.uuid)),
    any(target.engine),
    countIf(position(mutation.command, {correlation:String}) != 0),
    countIf(
        position(mutation.command, {correlation:String}) != 0
        AND mutation.is_done = 0
    ),
    maxIf(
        mutation.parts_to_do,
        position(mutation.command, {correlation:String}) != 0
        AND mutation.is_done = 0
    ),
    maxIf(
        if(
            empty(mutation.block_numbers.number),
            toInt64(0),
            mutation.block_numbers.number[1]
        ),
        position(mutation.command, {correlation:String}) != 0
    ),
    countIf(
        position(mutation.command, {correlation:String}) != 0
        AND (
            countSubstrings(
                mutation.command,
                {correlation:String}
            ) != 2
            OR NOT startsWith(mutation.command, '(DELETE WHERE ')
            OR position(mutation.command, 'tenant_id') = 0
            OR position(mutation.command, 'index_name') = 0
        )
    ),
    substring(
        argMaxIf(
            mutation.latest_fail_error_code_name,
            tuple(
                mutation.latest_fail_time,
                mutation.mutation_id
            ),
            position(
                mutation.command,
                {correlation:String}
            ) != 0
            AND mutation.is_done = 0
            AND mutation.latest_fail_reason != ''
        ),
        1,
        128
    ),
    substring(
        argMaxIf(
            mutation.latest_fail_reason,
            tuple(
                mutation.latest_fail_time,
                mutation.mutation_id
            ),
            position(
                mutation.command,
                {correlation:String}
            ) != 0
            AND mutation.is_done = 0
            AND mutation.latest_fail_reason != ''
        ),
        1,
        4096
    )
FROM system.tables AS target
LEFT JOIN system.mutations AS mutation
  ON mutation.database = target.database
 AND mutation.table = target.name
WHERE target.database = {database:String}
  AND target.name = {table:String}`,
		parameters,
	).Scan(
		&tableUUID,
		&engine,
		&summary.matching,
		&summary.pending,
		&summary.pendingParts,
		&summary.latestBlock,
		&summary.invalidCommands,
		&summary.failureCode,
		&summary.failureReason,
	)
	if err != nil {
		return indexDataDeletionMutationSummary{}, fmt.Errorf(
			"reconcile ClickHouse index data deletion mutations: %w",
			err,
		)
	}
	target, err := s.validatedIndexDataDeletionTarget(
		tableUUID,
		engine,
	)
	if err != nil {
		return indexDataDeletionMutationSummary{}, err
	}
	if target.Database != request.Database ||
		target.Table != request.Table ||
		target.TableUUID != request.TableUUID {
		return indexDataDeletionMutationSummary{}, fmt.Errorf(
			"%w: durable UUID %q resolves as %q",
			ErrIndexDataDeletionTargetChanged,
			request.TableUUID,
			target.TableUUID,
		)
	}
	summary, err = validatedIndexDataDeletionMutationSummary(summary)
	if err != nil {
		return indexDataDeletionMutationSummary{}, err
	}
	return summary, nil
}

func (s *Store) validatedIndexDataDeletionTarget(
	tableUUID string,
	engine string,
) (IndexDataDeletionTarget, error) {
	parsedUUID, parseErr := uuid.Parse(tableUUID)
	if parseErr != nil ||
		parsedUUID == uuid.Nil ||
		parsedUUID.String() != tableUUID {
		return IndexDataDeletionTarget{}, fmt.Errorf(
			"%w: configured table has an invalid UUID",
			ErrIndexDataDeletionTargetChanged,
		)
	}
	if engine != "MergeTree" {
		return IndexDataDeletionTarget{}, fmt.Errorf(
			"resolve ClickHouse index data deletion target: unsupported engine %q",
			engine,
		)
	}
	return IndexDataDeletionTarget{
		Database:  s.database,
		Table:     s.table,
		TableUUID: tableUUID,
	}, nil
}

func validatedIndexDataDeletionMutationSummary(
	summary indexDataDeletionMutationSummary,
) (indexDataDeletionMutationSummary, error) {
	if summary.pending > summary.matching ||
		summary.invalidCommands > summary.matching ||
		summary.pendingParts < 0 ||
		summary.latestBlock < 0 ||
		(summary.matching == 0 && summary.latestBlock != 0) ||
		!utf8.ValidString(summary.failureCode) ||
		!utf8.ValidString(summary.failureReason) {
		return indexDataDeletionMutationSummary{}, errors.New(
			"reconcile ClickHouse index data deletion mutations: invalid system.mutations result",
		)
	}
	summary.failureCode = truncateUTF8Bytes(
		summary.failureCode,
		maximumDeletionFailureCodeBytes,
	)
	summary.failureReason = truncateUTF8Bytes(
		summary.failureReason,
		maximumDeletionDiagnosticBytes,
	)
	return summary, nil
}

func (s *Store) indexDataDeletionRowsExist(
	ctx context.Context,
	request IndexDataDeletionRequest,
) (bool, error) {
	parameters := clickhousedriver.Parameters{
		"database": request.Database,
		"table":    request.Table,
		"tenant":   request.TenantID,
		"index":    request.IndexName,
	}
	query := `SELECT
    toString(target.uuid),
    target.engine,
    (
        SELECT count()
        FROM (
            SELECT 1
            FROM ` + quoteIdentifier(request.Database) + `.` +
		quoteIdentifier(request.Table) + `
            PREWHERE tenant_id = {tenant:String}
              AND index_name = {index:String}
            LIMIT 1
        )
    )
FROM system.tables AS target
WHERE target.database = {database:String}
  AND target.name = {table:String}
LIMIT 1`
	var tableUUID, engine string
	var count uint64
	if err := s.connection.queryRow(
		ctx,
		query,
		parameters,
	).Scan(&tableUUID, &engine, &count); err != nil {
		return false, fmt.Errorf(
			"verify ClickHouse index data deletion physical absence: %w",
			err,
		)
	}
	target, err := s.validatedIndexDataDeletionTarget(tableUUID, engine)
	if err != nil {
		return false, err
	}
	if target.Database != request.Database ||
		target.Table != request.Table ||
		target.TableUUID != request.TableUUID {
		return false, fmt.Errorf(
			"%w: durable UUID %q resolves as %q",
			ErrIndexDataDeletionTargetChanged,
			request.TableUUID,
			target.TableUUID,
		)
	}
	if count > 1 {
		return false, errors.New(
			"verify ClickHouse index data deletion physical absence: invalid bounded count",
		)
	}
	return count == 1, nil
}

func (s *Store) submitIndexDataDeletionMutation(
	ctx context.Context,
	request IndexDataDeletionRequest,
	marker string,
) error {
	query := `ALTER TABLE ` + quoteIdentifier(request.Database) + `.` +
		quoteIdentifier(request.Table) + ` DELETE WHERE
    tenant_id = {tenant:String}
    AND index_name = {index:String}
    AND {correlation:String} = {correlation:String}`
	parameters := clickhousedriver.Parameters{
		"tenant":      request.TenantID,
		"index":       request.IndexName,
		"correlation": marker,
	}
	settings := clickhousedriver.Settings{
		"mutations_sync": uint8(0),
	}
	return s.connection.exec(
		ctx,
		query,
		settings,
		parameters,
		indexDataDeletionQueryID(request),
	)
}

func progressForIndexDataDeletionSummary(
	summary indexDataDeletionMutationSummary,
) IndexDataDeletionProgress {
	return IndexDataDeletionProgress{
		MatchingMutations:   summary.matching,
		PendingMutations:    summary.pending,
		PendingParts:        summary.pendingParts,
		LatestMutationBlock: summary.latestBlock,
		LatestFailureCode:   summary.failureCode,
		LatestFailureReason: summary.failureReason,
	}
}

func validateIndexDataDeletionRequest(
	request IndexDataDeletionRequest,
) error {
	if !protocolid.Valid(request.OperationID) ||
		!protocolid.Valid(request.CorrelationID) {
		return errors.New(
			"advance ClickHouse index data deletion: invalid durable operation identity",
		)
	}
	if request.ProtocolVersion != indexDataDeletionProtocolVersion {
		return errors.New(
			"advance ClickHouse index data deletion: unsupported mutation protocol version",
		)
	}
	if request.TenantID == "" ||
		len(request.TenantID) > maximumDeletionTenantBytes ||
		!utf8.ValidString(request.TenantID) ||
		strings.IndexByte(request.TenantID, 0) >= 0 {
		return errors.New(
			"advance ClickHouse index data deletion: invalid tenant ID",
		)
	}
	if !indexname.ValidCanonical(request.IndexName) {
		return errors.New(
			"advance ClickHouse index data deletion: invalid canonical index name",
		)
	}
	if len(request.Database) > maximumDeletionPhysicalIdentifierBytes ||
		len(request.Table) > maximumDeletionPhysicalIdentifierBytes ||
		!physicalIdentifier.MatchString(request.Database) ||
		!physicalIdentifier.MatchString(request.Table) {
		return errors.New(
			"advance ClickHouse index data deletion: invalid physical table identifier",
		)
	}
	parsedUUID, err := uuid.Parse(request.TableUUID)
	if err != nil ||
		parsedUUID == uuid.Nil ||
		parsedUUID.String() != request.TableUUID {
		return errors.New(
			"advance ClickHouse index data deletion: invalid table UUID",
		)
	}
	return nil
}

func indexDataDeletionCorrelationMarker(
	request IndexDataDeletionRequest,
) string {
	digest := indexDataDeletionIdentityDigest(request)
	return "__open_splunk_delete_v1_" + hex.EncodeToString(digest[:]) + "__"
}

func indexDataDeletionQueryID(request IndexDataDeletionRequest) string {
	digest := indexDataDeletionIdentityDigest(request)
	return "os-del-" + hex.EncodeToString(digest[:])
}

func indexDataDeletionIdentityDigest(
	request IndexDataDeletionRequest,
) [sha256.Size]byte {
	digest := sha256.New()
	writeIndexDataDeletionIdentityField(
		digest,
		[]byte("open-splunk/index-data-deletion/request"),
	)
	var protocol [4]byte
	binary.BigEndian.PutUint32(protocol[:], request.ProtocolVersion)
	writeIndexDataDeletionIdentityField(digest, protocol[:])
	for _, field := range []string{
		request.OperationID,
		request.CorrelationID,
		request.TenantID,
		request.IndexName,
		request.Database,
		request.Table,
		request.TableUUID,
	} {
		writeIndexDataDeletionIdentityField(digest, []byte(field))
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func writeIndexDataDeletionIdentityField(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func truncateUTF8Bytes(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}
	end := maximum
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}
