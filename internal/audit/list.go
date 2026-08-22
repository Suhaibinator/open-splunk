package audit

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"fortio.org/safecast"
	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

type normalizedListRequest struct {
	tenantID      string
	pageSize      uint32
	pageToken     string
	includeTotal  bool
	actionFilters []Action
	actorID       *string
	targetKind    *TargetKind
}

type tenantEventAggregate struct {
	EventCount  int64  `gorm:"column:event_count"`
	MinSequence *int64 `gorm:"column:min_sequence"`
	MaxSequence *int64 `gorm:"column:max_sequence"`
}

func validateAllTenantIntegrity(database *gorm.DB) error {
	var invalidTenantWidth int64
	query := database.Raw(`
		SELECT EXISTS (
			SELECT 1
			FROM audit_tenant_state
			WHERE typeof(tenant_id) <> 'text'
			   OR length(CAST(tenant_id AS BLOB)) NOT BETWEEN 1 AND ?
			LIMIT 1
		)
	`, maximumTenantIDBytes).Scan(&invalidTenantWidth)
	if query.Error != nil {
		return query.Error
	}
	if invalidTenantWidth != 0 {
		return fmt.Errorf("%w: audit tenant identity width is invalid", ErrCorrupt)
	}

	var orphanedEvent int64
	query = database.Raw(`
		SELECT EXISTS (
			SELECT 1
			FROM audit_events AS event
			LEFT JOIN audit_tenant_state AS state
			  ON state.tenant_id = event.tenant_id
			WHERE state.tenant_id IS NULL
			LIMIT 1
		)
	`).Scan(&orphanedEvent)
	if query.Error != nil {
		return query.Error
	}
	if orphanedEvent != 0 {
		return fmt.Errorf("%w: audit event has no tenant state", ErrCorrupt)
	}

	var afterTenantID string
	haveAfterTenantID := false
	for {
		var states []auditTenantStateRecord
		stateQuery := database.
			Model(&auditTenantStateRecord{}).
			Order("tenant_id ASC").
			Limit(maximumIntegrityBatch)
		if haveAfterTenantID {
			stateQuery = stateQuery.Where("tenant_id > ?", afterTenantID)
		}
		stateQuery = stateQuery.Find(&states)
		if stateQuery.Error != nil {
			return stateQuery.Error
		}
		if len(states) == 0 {
			return nil
		}

		for index, state := range states {
			if !validTenantState(state, state.TenantID) ||
				(haveAfterTenantID && state.TenantID <= afterTenantID) ||
				(index > 0 && state.TenantID <= states[index-1].TenantID) {
				return fmt.Errorf("%w: audit tenant state scan is invalid", ErrCorrupt)
			}
			if err := validateTenantIntegrity(
				database,
				state.TenantID,
				state,
			); err != nil {
				return err
			}
		}
		afterTenantID = strings.Clone(states[len(states)-1].TenantID)
		haveAfterTenantID = true
		if len(states) < maximumIntegrityBatch {
			return nil
		}
	}
}

// List returns one descending tenant-local sequence keyset. Appends after a
// cursor is issued have higher sequences and therefore cannot enter, duplicate,
// or displace that continuation.
func (store *Store) List(
	ctx context.Context,
	tenantID string,
	request ListRequest,
) (page ListPage, returnedErr error) {
	if ctx == nil {
		return ListPage{}, fmt.Errorf("%w: audit list context is nil", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return ListPage{}, err
	}
	if store == nil || store.orm == nil {
		return ListPage{}, fmt.Errorf("%w: audit store is unavailable", control.ErrInvalidArgument)
	}
	if len(store.cursorKey) == 0 {
		return ListPage{}, fmt.Errorf(
			"%w: audit list cursor key is unavailable",
			control.ErrInvalidArgument,
		)
	}
	normalized, err := normalizeListRequest(tenantID, request)
	if err != nil {
		return ListPage{}, err
	}
	filterHash, err := listFilterHash(normalized)
	if err != nil {
		return ListPage{}, err
	}
	cursor := listCursor{}
	if normalized.pageToken != "" {
		cursor, err = decodeListCursor(
			store.cursorKey,
			normalized.pageToken,
			filterHash,
			normalized.includeTotal,
		)
		if err != nil {
			return ListPage{}, err
		}
	}

	tx := store.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return ListPage{}, mapStoreError(ctx, "begin audit list", tx.Error)
	}
	transactionFinished := false
	defer finishAuditTransaction(tx, "list", &transactionFinished, &returnedErr)

	state, exists, err := readOptionalTenantState(tx, normalized.tenantID)
	if err != nil {
		return ListPage{}, mapStoreError(ctx, "read audit tenant state", err)
	}
	var highWater int64
	var highWaterDigest string
	if normalized.pageToken == "" {
		if !exists {
			if err := validateAbsentTenantBoundary(tx, normalized.tenantID); err != nil {
				return ListPage{}, mapStoreError(ctx, "validate absent audit tenant", err)
			}
		} else {
			tail, boundaryErr := readValidatedTenantTail(tx, state)
			if boundaryErr != nil {
				return ListPage{}, mapStoreError(
					ctx,
					"validate audit tenant boundary",
					boundaryErr,
				)
			}
			if state.EventCount != 0 {
				highWater = state.EventCount
				highWaterDigest, err = auditEventDigest(*tail)
				if err != nil {
					return ListPage{}, mapStoreError(ctx, "encode audit high-water", err)
				}
			}
		}
	} else {
		highWater = cursor.HighWater
		highWaterDigest = cursor.HighWaterDigest
		if err := validateContinuationIntegrity(
			tx,
			normalized.tenantID,
			state,
			exists,
			cursor,
		); err != nil {
			return ListPage{}, mapStoreError(ctx, "validate audit continuation", err)
		}
	}

	query := applyListFilters(
		tx.Model(&auditEventRecord{}),
		normalized,
	)
	if cursor.Sequence != 0 {
		query = query.Where("sequence < ?", cursor.Sequence)
	}
	records, err := readPreflightedAuditRecords(query.
		Order("sequence DESC").
		Limit(int(normalized.pageSize)+1), int(normalized.pageSize)+1)
	if err != nil {
		return ListPage{}, mapStoreError(ctx, "list audit events", err)
	}

	pageSize := int(normalized.pageSize)
	hasNextPage := len(records) > pageSize
	events := make([]Event, 0, min(len(records), pageSize))
	for index, record := range records {
		event, conversionErr := eventFromRecord(record)
		if conversionErr != nil {
			return ListPage{}, conversionErr
		}
		if event.TenantID != normalized.tenantID {
			return ListPage{}, fmt.Errorf("%w: audit list crossed tenant scope", ErrCorrupt)
		}
		if index < pageSize {
			events = append(events, event)
		}
	}

	page = ListPage{Events: events}
	if normalized.includeTotal {
		var total uint64
		if cursor.TotalSize != nil {
			total = *cursor.TotalSize
		} else if len(normalized.actionFilters) == 0 &&
			normalized.actorID == nil && normalized.targetKind == nil {

			total = safecast.MustConv[uint64](state.EventCount)
		} else {
			total, err = countFilteredEvents(tx, normalized)
			if err != nil {
				return ListPage{}, err
			}
		}
		page.TotalSize = &total
		page.TotalSizeExact = true
	}
	if hasNextPage {
		last := page.Events[len(page.Events)-1]
		next := listCursor{
			FilterHash: filterHash,

			Sequence:        safecast.MustConv[int64](last.Sequence),
			HighWater:       highWater,
			HighWaterDigest: highWaterDigest,
		}
		if normalized.includeTotal {
			total := *page.TotalSize
			next.TotalSize = &total
		}
		page.NextPageToken, err = encodeListCursor(store.cursorKey, next)
		if err != nil {
			return ListPage{}, err
		}
	}
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return ListPage{}, mapStoreError(ctx, "commit audit list", commitErr)
	}
	return page, nil
}

func validateAbsentTenantBoundary(database *gorm.DB, tenantID string) error {
	records, err := readPreflightedAuditRecords(database.
		Model(&auditEventRecord{}).
		Where("tenant_id = ?", tenantID).
		Order("sequence DESC"), 1)
	if err != nil {
		return err
	}
	if len(records) != 0 {
		return fmt.Errorf("%w: audit events have no tenant state", ErrCorrupt)
	}
	return nil
}

func normalizeListRequest(
	tenantID string,
	request ListRequest,
) (normalizedListRequest, error) {
	if !validIdentity(tenantID, maximumTenantIDBytes) {
		return normalizedListRequest{}, fmt.Errorf(
			"%w: audit tenant ID is invalid",
			control.ErrInvalidArgument,
		)
	}
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	if pageSize > MaximumListPageSize {
		return normalizedListRequest{}, fmt.Errorf(
			"%w: audit page size cannot exceed %d",
			control.ErrInvalidArgument,
			MaximumListPageSize,
		)
	}
	if len(request.PageToken) > maximumListCursorBytes {
		return normalizedListRequest{}, fmt.Errorf(
			"%w: audit page token is too large",
			control.ErrInvalidArgument,
		)
	}
	if len(request.ActionFilters) > MaximumActionFilters {
		return normalizedListRequest{}, fmt.Errorf(
			"%w: too many audit action filters",
			control.ErrInvalidArgument,
		)
	}
	actions := slices.Clone(request.ActionFilters)
	for _, action := range actions {
		if !action.Valid() {
			return normalizedListRequest{}, fmt.Errorf(
				"%w: audit action filter is invalid",
				control.ErrInvalidArgument,
			)
		}
	}
	slices.Sort(actions)
	actions = slices.Compact(actions)
	if len(actions) == 0 {
		actions = nil
	}

	actorID := clonePointer(request.ActorID)
	if actorID != nil && !validIdentity(*actorID, maximumActorIDBytes) {
		return normalizedListRequest{}, fmt.Errorf(
			"%w: audit actor ID filter is invalid",
			control.ErrInvalidArgument,
		)
	}
	targetKind := clonePointer(request.TargetKind)
	if targetKind != nil && !targetKind.Valid() {
		return normalizedListRequest{}, fmt.Errorf(
			"%w: audit target-kind filter is invalid",
			control.ErrInvalidArgument,
		)
	}
	return normalizedListRequest{
		tenantID:      strings.Clone(tenantID),
		pageSize:      pageSize,
		pageToken:     strings.Clone(request.PageToken),
		includeTotal:  request.IncludeTotal,
		actionFilters: actions,
		actorID:       actorID,
		targetKind:    targetKind,
	}, nil
}

func applyListFilters(
	query *gorm.DB,
	request normalizedListRequest,
) *gorm.DB {
	query = query.Where("tenant_id = ?", request.tenantID)
	if len(request.actionFilters) != 0 {
		query = query.Where("action IN ?", request.actionFilters)
	}
	if request.actorID != nil {
		query = query.Where("actor_id = ?", *request.actorID)
	}
	if request.targetKind != nil {
		query = query.Where("target_kind = ?", *request.targetKind)
	}
	return query
}

func readOptionalTenantState(
	database *gorm.DB,
	tenantID string,
) (auditTenantStateRecord, bool, error) {
	var records []auditTenantStateRecord
	query := database.
		Where("tenant_id = ?", tenantID).
		Limit(2).
		Find(&records)
	if query.Error != nil {
		return auditTenantStateRecord{}, false, query.Error
	}
	if len(records) == 0 {
		return auditTenantStateRecord{}, false, nil
	}
	if len(records) != 1 || !validTenantState(records[0], tenantID) {
		return auditTenantStateRecord{}, false, fmt.Errorf(
			"%w: audit tenant state is invalid",
			ErrCorrupt,
		)
	}
	return records[0], true, nil
}

func validateTenantIntegrity(
	database *gorm.DB,
	tenantID string,
	state auditTenantStateRecord,
) error {
	var aggregate tenantEventAggregate
	query := database.Raw(`
		SELECT
			COUNT(*) AS event_count,
			MIN(sequence) AS min_sequence,
			MAX(sequence) AS max_sequence
		FROM (
			SELECT sequence
			FROM audit_events
			WHERE tenant_id = ?
			ORDER BY sequence
			LIMIT ?
		)
	`, tenantID, MaximumEventsPerTenant+1).Scan(&aggregate)
	if query.Error != nil {
		return query.Error
	}
	if aggregate.EventCount > MaximumEventsPerTenant {
		return fmt.Errorf("%w: audit tenant exceeds its event bound", ErrCorrupt)
	}
	if aggregate.EventCount != state.EventCount {
		return fmt.Errorf("%w: audit tenant count does not match events", ErrCorrupt)
	}
	if state.EventCount == 0 {
		if aggregate.MinSequence != nil || aggregate.MaxSequence != nil {
			return fmt.Errorf("%w: empty audit tenant has sequence bounds", ErrCorrupt)
		}
		return nil
	}
	if aggregate.MinSequence == nil || aggregate.MaxSequence == nil ||
		*aggregate.MinSequence != 1 ||
		*aggregate.MaxSequence != state.EventCount {
		return fmt.Errorf("%w: audit tenant sequence is not dense", ErrCorrupt)
	}
	return validateTenantEventRecords(database, tenantID, state.EventCount)
}

func validateTenantEventRecords(
	database *gorm.DB,
	tenantID string,
	eventCount int64,
) error {
	for expected := int64(1); expected <= eventCount; {
		limit := int(min(int64(maximumIntegrityBatch), eventCount-expected+1))
		records, err := readPreflightedAuditRecords(database.
			Model(&auditEventRecord{}).
			Where("tenant_id = ? AND sequence >= ?", tenantID, expected).
			Order("sequence ASC"), limit)
		if err != nil {
			return err
		}
		if len(records) != limit {
			return fmt.Errorf("%w: audit tenant scan ended early", ErrCorrupt)
		}
		for _, record := range records {
			if record.TenantID != tenantID || record.Sequence != expected {
				return fmt.Errorf("%w: audit tenant sequence scan is not dense", ErrCorrupt)
			}
			if _, err := eventFromRecord(record); err != nil {
				return err
			}
			expected++
		}
	}
	return nil
}

func validateContinuationIntegrity(
	database *gorm.DB,
	tenantID string,
	state auditTenantStateRecord,
	exists bool,
	cursor listCursor,
) error {
	if !exists || state.EventCount < cursor.HighWater {
		return fmt.Errorf(
			"%w: token is newer than persisted tenant state",
			ErrInvalidCursor,
		)
	}
	digest, err := readHighWaterDigest(database, tenantID, cursor.HighWater)
	if err != nil {
		return err
	}
	if digest != cursor.HighWaterDigest {
		return fmt.Errorf(
			"%w: token high-water does not match persisted state",
			ErrInvalidCursor,
		)
	}
	if state.EventCount == cursor.HighWater {
		return nil
	}
	_, err = readAuditRecordAt(database, tenantID, state.EventCount)
	return err
}

func readHighWaterDigest(
	database *gorm.DB,
	tenantID string,
	sequence int64,
) (string, error) {
	record, err := readAuditRecordAt(database, tenantID, sequence)
	if err != nil {
		return "", err
	}
	return auditEventDigest(record)
}

func readAuditRecordAt(
	database *gorm.DB,
	tenantID string,
	sequence int64,
) (auditEventRecord, error) {
	records, err := readPreflightedAuditRecords(database.
		Model(&auditEventRecord{}).
		Where("tenant_id = ? AND sequence = ?", tenantID, sequence).
		Order("sequence DESC"), 2)
	if err != nil {
		return auditEventRecord{}, err
	}
	if len(records) != 1 ||
		records[0].TenantID != tenantID ||
		records[0].Sequence != sequence {
		return auditEventRecord{}, fmt.Errorf(
			"%w: audit boundary event is missing or ambiguous",
			ErrCorrupt,
		)
	}
	if _, err := eventFromRecord(records[0]); err != nil {
		return auditEventRecord{}, err
	}
	return records[0], nil
}

func countFilteredEvents(
	database *gorm.DB,
	request normalizedListRequest,
) (uint64, error) {
	var count int64
	query := applyListFilters(
		database.Model(&auditEventRecord{}),
		request,
	).Count(&count)
	if query.Error != nil {
		return 0, mapStoreError(database.Statement.Context, "count audit events", query.Error)
	}
	if count < 0 || count > MaximumEventsPerTenant {
		return 0, fmt.Errorf("%w: filtered audit count is invalid", ErrCorrupt)
	}
	return uint64(count), nil
}

func clonePointer[T ~string](input *T) *T {
	if input == nil {
		return nil
	}
	value := T(strings.Clone(string(*input)))
	return &value
}
