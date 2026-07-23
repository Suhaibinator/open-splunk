package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	defaultJobListPageSize            = 15
	maximumJobListPageSize            = 15
	maximumJobListTokenSize           = 4096
	maximumJobListTextBytes           = 1024
	maximumJobListFailureMessageBytes = 4 << 10
	maximumJobListStates              = 16
	jobListIndexChunkSize             = 256
)

type normalizedJobListRequest struct {
	pageSize     int
	pageToken    string
	includeTotal bool
	states       []State
	stateSet     [StateExpired + 1]bool
	appID        *string
	text         *string
}

type retainedJobListEntry struct {
	entry      *jobEntry
	generation uint64
	key        jobListBoundary
}

type jobListBoundary struct {
	createdAt time.Time
	id        string
}

// ListPageFor returns a bounded owner-scoped page of retained transient jobs.
// The complete implementation lives in this file so transport handlers cannot
// accidentally compose the unbounded administrative ListFor method.
func (manager *Manager) ListPageFor(ctx context.Context, access AccessScope, request JobListRequest) (JobListPage, error) {
	return manager.listPageFor(ctx, access, request)
}

func (manager *Manager) listPageFor(ctx context.Context, access AccessScope, request JobListRequest) (JobListPage, error) {
	if ctx == nil {
		return JobListPage{}, errors.New("list search jobs: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return JobListPage{}, err
	}
	normalizedAccess, normalized, err := normalizeJobListRequest(access, request)
	if err != nil {
		return JobListPage{}, err
	}
	filterHash, err := jobListFilterHash(normalizedAccess, normalized)
	if err != nil {
		return JobListPage{}, err
	}
	var cursor jobListCursor
	if normalized.pageToken != "" {
		cursor, err = decodeJobListCursor(manager.cursorKey, normalized.pageToken, manager.listCursorEpoch, filterHash)
		if err != nil {
			return JobListPage{}, err
		}
	}
	if err := manager.acquireJobListGate(ctx); err != nil {
		return JobListPage{}, err
	}
	defer func() { <-manager.listGate }()

	manager.mu.RLock()
	if manager.closed {
		manager.mu.RUnlock()
		return JobListPage{}, ErrClosed
	}
	highWater := manager.nextGeneration
	if normalized.pageToken != "" {
		highWater = cursor.HighWater
		if highWater > manager.nextGeneration {
			manager.mu.RUnlock()
			return JobListPage{}, ErrInvalidCursor
		}
	}
	var exactEntries []retainedJobListEntry
	if normalized.includeTotal {
		root := manager.jobsByScope[normalizedAccess]
		exactEntries = make([]retainedJobListEntry, 0, jobListIndexSize(root))
		jobListIndexCollectBefore(root, nil, &exactEntries, jobListIndexSize(root))
	}
	manager.mu.RUnlock()

	now := manager.nowUTC()
	matcher := newJobListTextMatcher("")
	if normalized.text != nil {
		matcher = newJobListTextMatcher(*normalized.text)
	}
	capacity := normalized.pageSize + 1
	selected := make([]Job, 0, capacity)
	var total uint64
	visited := 0
	process := func(entries []retainedJobListEntry) (bool, error) {
		for _, retained := range entries {
			if visited&31 == 0 {
				if contextErr := manager.jobListContextError(ctx); contextErr != nil {
					return false, contextErr
				}
			}
			visited++
			if retained.generation == 0 || retained.generation > highWater {
				continue
			}
			entry := retained.entry
			entry.mu.Lock()
			if entry.job.TenantID != normalizedAccess.TenantID || entry.job.OwnerID != normalizedAccess.OwnerID {
				entry.mu.Unlock()
				continue
			}
			if canExpireLocked(entry, now) {
				manager.expireLocked(entry, now)
			}
			snapshot := shallowJobListSnapshot(entry.job)
			entry.mu.Unlock()

			if !jobMatchesListMetadata(snapshot, normalized) {
				continue
			}
			followsCursor := normalized.pageToken == "" || jobListFollowsCursor(snapshot, cursor)
			if !normalized.includeTotal && !followsCursor {
				continue
			}
			if normalized.text != nil {
				matches, matchErr := matcher.containsContext(ctx, manager.ctx, snapshot.SPL)
				if matchErr != nil {
					return false, matchErr
				}
				if !matches {
					continue
				}
			}
			if normalized.includeTotal {
				total++
			}
			if followsCursor && len(selected) < capacity {
				selected = append(selected, snapshot)
				if !normalized.includeTotal && len(selected) == capacity {
					return true, nil
				}
			}
		}
		return false, nil
	}

	if normalized.includeTotal {
		if _, err := process(exactEntries); err != nil {
			return JobListPage{}, err
		}
	} else {
		var boundary *jobListBoundary
		if normalized.pageToken != "" {
			boundary = &jobListBoundary{createdAt: cursor.lastCreatedAt(), id: cursor.LastID}
		}
		for {
			entries, more, chunkErr := manager.jobListIndexChunk(normalizedAccess, boundary)
			if chunkErr != nil {
				return JobListPage{}, chunkErr
			}
			if len(entries) == 0 {
				break
			}
			stop, processErr := process(entries)
			if processErr != nil {
				return JobListPage{}, processErr
			}
			nextBoundary := entries[len(entries)-1].key
			boundary = &nextBoundary
			if stop || !more {
				break
			}
		}
	}
	if err := manager.jobListContextError(ctx); err != nil {
		return JobListPage{}, err
	}

	hasMore := len(selected) > normalized.pageSize
	if hasMore {
		selected = selected[:normalized.pageSize]
	}
	page := JobListPage{Jobs: make([]JobListItem, len(selected))}
	for index := range selected {
		page.Jobs[index] = cloneJobListItem(selected[index])
	}
	if hasMore {
		last := selected[len(selected)-1]
		page.NextPageToken, err = encodeJobListCursor(
			manager.cursorKey, manager.listCursorEpoch, filterHash, highWater, last,
		)
		if err != nil {
			return JobListPage{}, err
		}
	}
	if normalized.includeTotal {
		page.TotalSize = new(uint64)
		*page.TotalSize = total
		page.TotalSizeExact = true
	}
	return page, nil
}

func (manager *Manager) insertJobListEntryLocked(entry *jobEntry) {
	scope := AccessScope{TenantID: entry.job.TenantID, OwnerID: entry.job.OwnerID}
	manager.jobsByScope[scope] = jobListIndexInsert(manager.jobsByScope[scope], newJobListIndexNode(entry))
}

func (manager *Manager) removeJobListEntryLocked(entry *jobEntry) {
	scope := AccessScope{TenantID: entry.job.TenantID, OwnerID: entry.job.OwnerID}
	root := jobListIndexRemove(manager.jobsByScope[scope], entry)
	if root == nil {
		delete(manager.jobsByScope, scope)
		return
	}
	manager.jobsByScope[scope] = root
}

func (manager *Manager) jobListIndexChunk(access AccessScope, before *jobListBoundary) ([]retainedJobListEntry, bool, error) {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if manager.closed {
		return nil, false, ErrClosed
	}
	result := make([]retainedJobListEntry, 0, jobListIndexChunkSize+1)
	jobListIndexCollectBefore(manager.jobsByScope[access], before, &result, jobListIndexChunkSize+1)
	more := len(result) > jobListIndexChunkSize
	if more {
		result = result[:jobListIndexChunkSize]
	}
	return result, more, nil
}

func retainedJobListSnapshot(entry *jobEntry) retainedJobListEntry {
	return retainedJobListEntry{
		entry:      entry,
		generation: entry.generation,
		key:        jobListKey(entry),
	}
}

// jobListEntriesComeBefore reads only immutable admission fields. It must not
// accept Job values: copying a whole Job would race with execution updates to
// unrelated mutable fields. Reading these fields directly also avoids adding
// entry locks inside Manager.mu-protected tree traversal, preserving the
// manager's existing lock scope and O(log N) mutation cost.
func jobListEntriesComeBefore(left, right *jobEntry) bool {
	return jobListKeyComesBefore(jobListKey(left), jobListKey(right))
}

func jobListEntryComesBeforeBoundary(entry *jobEntry, boundary jobListBoundary) bool {
	return jobListKeyComesBefore(jobListKey(entry), boundary)
}

func jobListKey(entry *jobEntry) jobListBoundary {
	return jobListBoundary{createdAt: entry.job.CreatedAt, id: entry.job.ID}
}

func jobListKeyComesBefore(left, right jobListBoundary) bool {
	if left.createdAt.Equal(right.createdAt) {
		return left.id < right.id
	}
	return left.createdAt.Before(right.createdAt)
}

func normalizeJobListRequest(access AccessScope, request JobListRequest) (AccessScope, normalizedJobListRequest, error) {
	invalid := func(detail string) (AccessScope, normalizedJobListRequest, error) {
		return AccessScope{}, normalizedJobListRequest{}, fmt.Errorf("%w: %s", ErrInvalidListFilter, detail)
	}
	if !validAccessIdentity(access.TenantID) {
		return invalid("tenant ID is invalid")
	}
	if !validAccessIdentity(access.OwnerID) {
		return invalid("owner ID is invalid")
	}
	access = AccessScope{
		TenantID: strings.Clone(access.TenantID),
		OwnerID:  strings.Clone(access.OwnerID),
	}
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = defaultJobListPageSize
	}
	if pageSize < 0 || pageSize > maximumJobListPageSize {
		return AccessScope{}, normalizedJobListRequest{}, ErrPageSize
	}
	if len(request.PageToken) > maximumJobListTokenSize {
		return AccessScope{}, normalizedJobListRequest{}, ErrInvalidCursor
	}
	if len(request.StateFilters) > maximumJobListStates {
		return invalid(fmt.Sprintf("state filters cannot contain more than %d values", maximumJobListStates))
	}
	states := slices.Clone(request.StateFilters)
	for _, state := range states {
		if state < StateQueued || state > StateExpired {
			return invalid("state filter is invalid")
		}
	}
	slices.Sort(states)
	states = slices.Compact(states)
	if len(states) == 0 {
		states = nil
	}
	normalized := normalizedJobListRequest{
		pageSize:     pageSize,
		pageToken:    strings.Clone(request.PageToken),
		includeTotal: request.IncludeTotal,
		states:       states,
	}
	for _, state := range states {
		normalized.stateSet[state] = true
	}
	if request.AppIDFilter != nil {
		if len(*request.AppIDFilter) > maximumJobAppIDBytes {
			return invalid("app ID filter is invalid")
		}
		appID := strings.Clone(*request.AppIDFilter)
		if !canonicalJobMetadataIdentifier(appID, maximumJobAppIDBytes, true) {
			return invalid("app ID filter is invalid")
		}
		normalized.appID = &appID
	}
	if request.TextFilter != nil {
		if len(*request.TextFilter) > maximumJobListTextBytes {
			return invalid(fmt.Sprintf("text filter must be valid UTF-8 without control characters and contain at most %d bytes", maximumJobListTextBytes))
		}
		text := strings.TrimSpace(*request.TextFilter)
		if text != "" {
			if !utf8.ValidString(text) || jobListContainsControl(text) {
				return invalid(fmt.Sprintf("text filter must be valid UTF-8 without control characters and contain at most %d bytes", maximumJobListTextBytes))
			}
			text = strings.Clone(text)
			normalized.text = &text
		}
	}
	return access, normalized, nil
}

func jobListContainsControl(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) {
			return true
		}
	}
	return false
}

func (manager *Manager) acquireJobListGate(ctx context.Context) error {
	select {
	case manager.listGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-manager.ctx.Done():
		return ErrClosed
	}
}

func (manager *Manager) jobListContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if manager.ctx.Err() != nil {
		return ErrClosed
	}
	return nil
}

func jobMatchesListMetadata(job Job, request normalizedJobListRequest) bool {
	if job.State < StateQueued || job.State > StateExpired {
		return false
	}
	if len(request.states) != 0 && !request.stateSet[job.State] {
		return false
	}
	if request.appID != nil && job.AppID != *request.appID {
		return false
	}
	return true
}

func shallowJobListSnapshot(source Job) Job {
	result := source
	result.Schema = nil
	return result
}

func cloneJobListItem(source Job) JobListItem {
	result := JobListItem{
		ID:               source.ID,
		Version:          source.Version,
		OwnerID:          source.OwnerID,
		TenantID:         source.TenantID,
		SPL:              source.SPL,
		NormalizedSPL:    source.NormalizedSPL,
		RequestedIndexes: cloneStrings(source.RequestedIndexes),
		EffectiveIndexes: cloneStrings(source.EffectiveIndexes),
		TimeRange:        source.TimeRange,
		AppID:            source.AppID,
		Source:           source.Source,
		Earliest:         source.Earliest,
		Latest:           source.Latest,
		IndexTimeCutoff:  source.IndexTimeCutoff,
		State:            source.State,
		ScannedRows:      source.ScannedRows,
		ScannedBytes:     source.ScannedBytes,
		RowCount:         source.RowCount,
		ResultBytes:      source.ResultBytes,
		ResultsTruncated: source.ResultsTruncated,
		CreatedAt:        source.CreatedAt,
		StartedAt:        source.StartedAt,
		FinishedAt:       source.FinishedAt,
		ExpiresAt:        source.ExpiresAt,
	}
	if source.Failure != nil {
		result.Failure = &JobListFailure{
			Code:      source.Failure.Code,
			Message:   safeJobListFailureMessage(*source.Failure),
			Retryable: source.Failure.Retryable,
		}
	}
	return result
}

func safeJobListFailureMessage(failure Failure) string {
	message := failure.Message
	if message != "" && len(message) <= maximumJobListFailureMessageBytes && utf8.ValidString(message) &&
		strings.TrimSpace(message) != "" && !strings.ContainsRune(message, '\x00') {
		return strings.Clone(message)
	}
	switch failure.Code {
	case FailureInvalidSPL:
		return "search SPL is invalid"
	case FailureUnsupportedSPL:
		return "search SPL contains an unsupported command"
	case FailureInvalidTimeRange:
		return "search time range is invalid"
	case FailureIndexForbidden:
		return "search index access is forbidden"
	case FailureResourceLimit:
		return "search exceeded a configured resource limit"
	case FailureTimeout:
		return "search execution timed out"
	case FailureStorageUnavailable:
		return "search storage is unavailable"
	case FailureExecution:
		return "search execution failed"
	default:
		return "search failed internally"
	}
}

func jobListComesBefore(left, right Job) bool {
	if left.CreatedAt.Equal(right.CreatedAt) {
		return left.ID > right.ID
	}
	return left.CreatedAt.After(right.CreatedAt)
}

func jobListFollowsCursor(job Job, cursor jobListCursor) bool {
	createdAt := cursor.lastCreatedAt()
	return job.CreatedAt.Before(createdAt) ||
		(job.CreatedAt.Equal(createdAt) && job.ID < cursor.LastID)
}

// jobListTextMatcher is a fixed-capacity KMP matcher. ASCII letters compare
// case-insensitively while every non-ASCII UTF-8 byte compares exactly, which
// matches SQLite lower() semantics without allocating a lower-cased SPL copy.
type jobListTextMatcher struct {
	pattern string
	prefix  [maximumJobListTextBytes]uint16
}

func newJobListTextMatcher(pattern string) jobListTextMatcher {
	matcher := jobListTextMatcher{pattern: pattern}
	for index, matched := 1, 0; index < len(pattern); index++ {
		for matched > 0 && !jobListFoldEqual(pattern[index], pattern[matched]) {
			matched = int(matcher.prefix[matched-1])
		}
		if jobListFoldEqual(pattern[index], pattern[matched]) {
			matched++
		}
		matcher.prefix[index] = uint16(matched)
	}
	return matcher
}

func (matcher *jobListTextMatcher) Contains(source string) bool {
	matches, _ := matcher.containsContext(context.Background(), context.Background(), source)
	return matches
}

func (matcher *jobListTextMatcher) containsContext(ctx, managerContext context.Context, source string) (bool, error) {
	if matcher.pattern == "" {
		return true, nil
	}
	matched := 0
	for index := 0; index < len(source); index++ {
		if index&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if managerContext.Err() != nil {
				return false, ErrClosed
			}
		}
		for matched > 0 && !jobListFoldEqual(source[index], matcher.pattern[matched]) {
			matched = int(matcher.prefix[matched-1])
		}
		if jobListFoldEqual(source[index], matcher.pattern[matched]) {
			matched++
			if matched == len(matcher.pattern) {
				return true, nil
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if managerContext.Err() != nil {
		return false, ErrClosed
	}
	return false, nil
}

func jobListFoldEqual(left, right byte) bool {
	if left >= 'A' && left <= 'Z' {
		left += 'a' - 'A'
	}
	if right >= 'A' && right <= 'Z' {
		right += 'a' - 'A'
	}
	return left == right
}
