package searchaudit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

type normalizedListRequest struct {
	tenantID     string
	pageSize     uint32
	pageToken    string
	includeTotal bool
	actorID      *string
	ownerID      *string
}

// List returns one descending tenant-local sequence keyset. Appends above the
// captured high-water are excluded. If rolling retention removes any row from
// the captured window, continuation fails with ErrInvalidCursor rather than
// silently returning an incomplete traversal.
func (store *Store) List(
	ctx context.Context,
	tenantID string,
	request ListRequest,
) (page ListPage, returnedErr error) {
	if ctx == nil {
		return ListPage{}, fmt.Errorf("%w: search-attempt audit list context is nil", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return ListPage{}, err
	}
	if store == nil || store.orm == nil {
		return ListPage{}, fmt.Errorf("%w: search-attempt audit store is unavailable", control.ErrInvalidArgument)
	}
	if len(store.cursorKey) == 0 {
		return ListPage{}, fmt.Errorf("%w: search-attempt audit cursor key is unavailable", control.ErrInvalidArgument)
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
		return ListPage{}, mapStoreError(ctx, "begin search-attempt audit list", tx.Error)
	}
	finished := false
	defer func() {
		if finished {
			return
		}
		if rollbackErr := tx.Rollback().Error; rollbackErr != nil &&
			!errors.Is(rollbackErr, gorm.ErrInvalidTransaction) {
			rollbackErr = fmt.Errorf("rollback search-attempt audit list: %w", rollbackErr)
			if returnedErr == nil {
				returnedErr = rollbackErr
			} else {
				returnedErr = errors.Join(returnedErr, rollbackErr)
			}
		}
	}()

	state, exists, err := readOptionalTenantState(tx, normalized.tenantID)
	if err != nil {
		return ListPage{}, mapStoreError(ctx, "read search-attempt audit tenant state", err)
	}
	var firstSequence, highWater int64
	var highWaterRecord searchAttemptEventRecord
	highWaterDigest := cursor.HighWaterDigest
	if normalized.pageToken == "" {
		if !exists {
			if err := validateAbsentTenantBoundary(tx, normalized.tenantID); err != nil {
				return ListPage{}, mapStoreError(ctx, "validate absent search-attempt audit tenant", err)
			}
		} else {
			if state.RetainedCount != 0 {
				firstSequence = state.FirstSequence
				highWater = state.NextSequence - 1
			}
			highWaterRecord, err = readTenantSnapshotBoundary(tx, state, highWater)
			if err != nil {
				return ListPage{}, mapStoreError(ctx, "validate search-attempt audit tenant boundary", err)
			}
		}
	} else {
		firstSequence = cursor.FirstSequence
		highWater = cursor.HighWater
		if err := validateContinuationIntegrity(
			tx,
			normalized.tenantID,
			state,
			exists,
			cursor,
		); err != nil {
			return ListPage{}, mapStoreError(ctx, "validate search-attempt audit continuation", err)
		}
	}

	query := applyListFilters(tx.Model(&searchAttemptEventRecord{}), normalized)
	if highWater != 0 {
		query = query.Where("sequence <= ?", highWater)
	}
	if cursor.Sequence != 0 {
		query = query.Where("sequence < ?", cursor.Sequence)
	}
	records, err := readPreflightedRecords(
		query.Order("sequence DESC").Limit(int(normalized.pageSize)+1),
		int(normalized.pageSize)+1,
	)
	if err != nil {
		return ListPage{}, mapStoreError(ctx, "list search-attempt audit events", err)
	}
	events := make([]Event, 0, min(len(records), int(normalized.pageSize)))
	for _, record := range records {
		event, conversionErr := eventFromRecord(record)
		if conversionErr != nil {
			return ListPage{}, conversionErr
		}
		if event.TenantID != normalized.tenantID || event.Sequence > uint64(highWater) {
			return ListPage{}, fmt.Errorf("%w: search-attempt audit list crossed snapshot scope", ErrCorrupt)
		}
		events = append(events, event)
	}
	page = ListPage{Events: events}
	if normalized.includeTotal {
		var total uint64
		if cursor.TotalSize != nil {
			total = *cursor.TotalSize
		} else if normalized.actorID == nil && normalized.ownerID == nil {
			if exists {
				// #nosec G115 -- validTenantState bounds retained_count to [0, 100000].
				total = uint64(state.RetainedCount)
			}
		} else {
			total, err = countFilteredEvents(tx, normalized, highWater)
			if err != nil {
				return ListPage{}, err
			}
		}
		page.TotalSize = &total
		page.TotalSizeExact = true
	}
	if len(page.Events) > int(normalized.pageSize) {
		page.Events = page.Events[:normalized.pageSize]
		if normalized.pageToken == "" {
			highWaterDigest, err = eventDigest(highWaterRecord)
			if err != nil {
				return ListPage{}, err
			}
		}
		last := page.Events[len(page.Events)-1]
		next := listCursor{
			FilterHash: filterHash,
			// #nosec G115 -- eventFromRecord rejects sequences above MaxInt64-1.
			Sequence:        int64(last.Sequence),
			FirstSequence:   firstSequence,
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
	finished = true
	if commitErr != nil {
		return ListPage{}, mapStoreError(ctx, "commit search-attempt audit list", commitErr)
	}
	return detachListPage(page), nil
}

func normalizeListRequest(tenantID string, request ListRequest) (normalizedListRequest, error) {
	if !validIdentity(tenantID, maximumTenantIDBytes) {
		return normalizedListRequest{}, fmt.Errorf("%w: search-attempt audit tenant ID is invalid", control.ErrInvalidArgument)
	}
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	if pageSize > MaximumListPageSize {
		return normalizedListRequest{}, fmt.Errorf(
			"%w: search-attempt audit page size cannot exceed %d",
			control.ErrInvalidArgument,
			MaximumListPageSize,
		)
	}
	if len(request.PageToken) > maximumListCursorBytes {
		return normalizedListRequest{}, fmt.Errorf("%w: search-attempt audit page token is too large", control.ErrInvalidArgument)
	}
	actorID := cloneStringPointer(request.ActorID)
	if actorID != nil && !validIdentity(*actorID, maximumOwnerIDBytes) {
		return normalizedListRequest{}, fmt.Errorf("%w: search-attempt audit actor filter is invalid", control.ErrInvalidArgument)
	}
	ownerID := cloneStringPointer(request.OwnerID)
	if ownerID != nil && !validIdentity(*ownerID, maximumOwnerIDBytes) {
		return normalizedListRequest{}, fmt.Errorf("%w: search-attempt audit owner filter is invalid", control.ErrInvalidArgument)
	}
	return normalizedListRequest{
		tenantID:     strings.Clone(tenantID),
		pageSize:     pageSize,
		pageToken:    strings.Clone(request.PageToken),
		includeTotal: request.IncludeTotal,
		actorID:      actorID,
		ownerID:      ownerID,
	}, nil
}

func applyListFilters(query *gorm.DB, request normalizedListRequest) *gorm.DB {
	query = query.Where("tenant_id = ?", request.tenantID)
	if request.actorID != nil {
		query = query.Where("actor_id = ?", *request.actorID)
	}
	if request.ownerID != nil {
		query = query.Where("owner_id = ?", *request.ownerID)
	}
	return query
}

func validateAbsentTenantBoundary(database *gorm.DB, tenantID string) error {
	var eventExists int64
	if err := database.Raw(`
		SELECT EXISTS (
			SELECT 1
			FROM search_attempt_audit_events
			WHERE tenant_id = ?
			LIMIT 1
		)
	`, tenantID).Scan(&eventExists).Error; err != nil {
		return err
	}
	if eventExists != 0 {
		return fmt.Errorf("%w: search-attempt audit events have no tenant state", ErrCorrupt)
	}
	return nil
}

func validateContinuationIntegrity(
	database *gorm.DB,
	tenantID string,
	state searchAttemptTenantStateRecord,
	exists bool,
	cursor listCursor,
) error {
	if !exists {
		return fmt.Errorf("%w: token snapshot is not retained", ErrInvalidCursor)
	}
	if !validTenantState(state, tenantID) {
		return fmt.Errorf("%w: search-attempt audit tenant state is invalid", ErrCorrupt)
	}
	if state.FirstSequence != cursor.FirstSequence ||
		state.NextSequence <= cursor.HighWater {
		return fmt.Errorf("%w: token snapshot is not retained", ErrInvalidCursor)
	}
	record, err := readTenantSnapshotBoundary(database, state, cursor.HighWater)
	if err != nil {
		return err
	}
	digest, err := eventDigest(record)
	if err != nil {
		return err
	}
	if digest != cursor.HighWaterDigest {
		return fmt.Errorf("%w: token high-water identity changed", ErrInvalidCursor)
	}
	return nil
}

// readTenantSnapshotBoundary validates the retained endpoints and one snapshot
// high-water in a single bounded preflight/hydration pair. The high-water can
// be the current tail, so duplicate sequence keys are compacted before query.
func readTenantSnapshotBoundary(
	database *gorm.DB,
	state searchAttemptTenantStateRecord,
	highWater int64,
) (searchAttemptEventRecord, error) {
	if !validTenantState(state, state.TenantID) {
		return searchAttemptEventRecord{}, fmt.Errorf(
			"%w: search-attempt audit tenant state is invalid",
			ErrCorrupt,
		)
	}
	if state.RetainedCount == 0 {
		if highWater != 0 {
			return searchAttemptEventRecord{}, fmt.Errorf(
				"%w: empty search-attempt audit tenant has a snapshot high-water",
				ErrCorrupt,
			)
		}
		if err := validateAbsentTenantBoundary(database, state.TenantID); err != nil {
			return searchAttemptEventRecord{}, err
		}
		return searchAttemptEventRecord{}, nil
	}
	if highWater < state.FirstSequence || highWater >= state.NextSequence {
		return searchAttemptEventRecord{}, fmt.Errorf(
			"%w: search-attempt audit snapshot high-water is outside the retained interval",
			ErrCorrupt,
		)
	}

	sequences := []int64{state.FirstSequence, state.NextSequence - 1, highWater}
	slices.Sort(sequences)
	sequences = slices.Compact(sequences)
	records, err := readPreflightedRecords(database.
		Model(&searchAttemptEventRecord{}).
		Where("tenant_id = ? AND sequence IN ?", state.TenantID, sequences).
		Order("sequence ASC"), len(sequences))
	if err != nil {
		return searchAttemptEventRecord{}, err
	}
	if len(records) != len(sequences) {
		return searchAttemptEventRecord{}, fmt.Errorf(
			"%w: search-attempt audit snapshot boundary is incomplete",
			ErrCorrupt,
		)
	}
	var highWaterRecord searchAttemptEventRecord
	for index, record := range records {
		if record.TenantID != state.TenantID || record.Sequence != sequences[index] {
			return searchAttemptEventRecord{}, fmt.Errorf(
				"%w: search-attempt audit snapshot boundary is invalid",
				ErrCorrupt,
			)
		}
		if _, err := eventFromRecord(record); err != nil {
			return searchAttemptEventRecord{}, err
		}
		if record.Sequence == highWater {
			highWaterRecord = record
		}
	}
	return highWaterRecord, nil
}

func countFilteredEvents(
	database *gorm.DB,
	request normalizedListRequest,
	highWater int64,
) (uint64, error) {
	var count int64
	query := applyListFilters(database.Model(&searchAttemptEventRecord{}), request)
	if highWater != 0 {
		query = query.Where("sequence <= ?", highWater)
	}
	if err := query.Count(&count).Error; err != nil {
		return 0, mapStoreError(database.Statement.Context, "count search-attempt audit events", err)
	}
	if count < 0 || count > MaximumRetainedAttempts {
		return 0, fmt.Errorf("%w: filtered search-attempt audit count is invalid", ErrCorrupt)
	}
	return uint64(count), nil
}

func detachListPage(page ListPage) ListPage {
	result := ListPage{
		Events:         make([]Event, len(page.Events)),
		NextPageToken:  strings.Clone(page.NextPageToken),
		TotalSizeExact: page.TotalSizeExact,
	}
	for index, event := range page.Events {
		result.Events[index] = event.detached()
	}
	if page.TotalSize != nil {
		total := *page.TotalSize
		result.TotalSize = &total
	}
	return result
}

func cloneStringPointer(input *string) *string {
	if input == nil {
		return nil
	}
	value := strings.Clone(*input)
	return &value
}
