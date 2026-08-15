package export

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	defaultMaxConcurrentExportLists = 4
	defaultExportListPageSize       = MaximumListPageSize
	maximumExportListTokenSize      = 4096
	maximumExportListStates         = 16
	exportListIndexChunkSize        = 256

	// MaximumListPageTokenBytes is the manager and transport hard cursor bound.
	MaximumListPageTokenBytes = maximumExportListTokenSize
	// MaximumListStateFilters is the pre-normalization state-filter bound.
	MaximumListStateFilters = maximumExportListStates
	// MaximumSearchJobIDBytes is the retained exact-selector byte bound.
	MaximumSearchJobIDBytes = maximumSearchIDBytes
	// MaximumListPageSize is the hard number of full export-job projections one
	// manager list response can contain. Export definitions may carry a large
	// selected-column set, so this intentionally stays smaller than generic
	// control-plane catalog pages.
	MaximumListPageSize = 15
)

type normalizedExportListRequest struct {
	pageSize     int
	pageToken    string
	includeTotal bool
	states       []State
	stateSet     [StateExpired + 1]bool
	searchJobID  *string
}

type retainedExportListEntry struct {
	entry      *jobEntry
	generation uint64
	key        exportListBoundary
}

type exportListBoundary struct {
	createdAt time.Time
	id        string
}

// List returns a bounded newest-first page of retained export jobs belonging
// to access. New admissions cannot enter an existing cursor traversal, while
// mutable lifecycle state is read and expiration is applied at each request.
func (manager *Manager) List(
	ctx context.Context,
	access searchjobs.AccessScope,
	request ListRequest,
) (ListPage, error) {
	if ctx == nil {
		return ListPage{}, errors.New("list export jobs: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return ListPage{}, err
	}
	normalizedAccess, normalized, err := normalizeExportListRequest(access, request)
	if err != nil {
		return ListPage{}, err
	}
	filterHash, err := exportListFilterHash(normalizedAccess, normalized)
	if err != nil {
		return ListPage{}, err
	}
	var cursor exportListCursor
	if normalized.pageToken != "" {
		cursor, err = decodeExportListCursor(
			manager.cursorKey,
			normalized.pageToken,
			manager.listCursorEpoch,
			filterHash,
		)
		if err != nil {
			return ListPage{}, err
		}
	}
	if err := manager.acquireExportListGate(ctx); err != nil {
		return ListPage{}, err
	}
	defer func() { <-manager.listGate }()

	manager.mu.RLock()
	if manager.closed {
		manager.mu.RUnlock()
		return ListPage{}, ErrClosed
	}
	highWater := manager.nextGeneration
	if normalized.pageToken != "" {
		highWater = cursor.HighWater
		if highWater > manager.nextGeneration {
			manager.mu.RUnlock()
			return ListPage{}, ErrInvalidCursor
		}
	}
	var exactEntries []retainedExportListEntry
	if normalized.includeTotal {
		root := manager.jobsByScope[normalizedAccess]
		exactEntries = make([]retainedExportListEntry, 0, exportListIndexSize(root))
		exportListIndexCollectBefore(root, nil, &exactEntries, exportListIndexSize(root))
	}
	manager.mu.RUnlock()

	now := manager.nowUTC()
	capacity := normalized.pageSize + 1
	selected := make([]ListItem, 0, capacity)
	var total uint64
	visited := 0
	process := func(entries []retainedExportListEntry) (bool, error) {
		for _, retained := range entries {
			if visited&31 == 0 {
				if contextErr := manager.exportListContextError(ctx); contextErr != nil {
					return false, contextErr
				}
			}
			visited++
			if retained.generation == 0 || retained.generation > highWater {
				continue
			}
			entry := retained.entry
			entry.mu.Lock()
			if contextErr := manager.exportListContextError(ctx); contextErr != nil {
				entry.mu.Unlock()
				return false, contextErr
			}
			if entry.access != normalizedAccess {
				entry.mu.Unlock()
				continue
			}
			priorState := entry.job.State
			_, _ = manager.expireLocked(entry, now)
			if entry.job.State != priorState {
				manager.noteCleanupStateChange()
			}
			if !exportMatchesListFiltersLocked(entry, normalized) {
				entry.mu.Unlock()
				continue
			}
			followsCursor := normalized.pageToken == "" ||
				exportListKeyFollowsCursor(retained.key, cursor)
			if normalized.includeTotal {
				total++
			}
			if followsCursor && len(selected) < capacity {
				selected = append(selected, ListItem{
					Job:      cloneJob(entry.job),
					TenantID: strings.Clone(entry.access.TenantID),
					OwnerID:  strings.Clone(entry.access.OwnerID),
				})
				entry.mu.Unlock()
				if !normalized.includeTotal && len(selected) == capacity {
					return true, nil
				}
				continue
			}
			entry.mu.Unlock()
		}
		return false, nil
	}

	if normalized.includeTotal {
		if _, err := process(exactEntries); err != nil {
			return ListPage{}, err
		}
	} else {
		var boundary *exportListBoundary
		if normalized.pageToken != "" {
			boundary = &exportListBoundary{
				createdAt: cursor.lastCreatedAt(),
				id:        cursor.LastID,
			}
		}
		for {
			entries, more, chunkErr := manager.exportListIndexChunk(
				normalizedAccess,
				boundary,
			)
			if chunkErr != nil {
				return ListPage{}, chunkErr
			}
			if len(entries) == 0 {
				break
			}
			stop, processErr := process(entries)
			if processErr != nil {
				return ListPage{}, processErr
			}
			nextBoundary := entries[len(entries)-1].key
			boundary = &nextBoundary
			if stop || !more {
				break
			}
		}
	}
	if err := manager.exportListContextError(ctx); err != nil {
		return ListPage{}, err
	}

	hasMore := len(selected) > normalized.pageSize
	if hasMore {
		selected = selected[:normalized.pageSize]
	}
	page := ListPage{Jobs: selected}
	if hasMore {
		last := selected[len(selected)-1]
		page.NextPageToken, err = encodeExportListCursor(
			manager.cursorKey,
			manager.listCursorEpoch,
			filterHash,
			highWater,
			last.Job,
		)
		if err != nil {
			return ListPage{}, err
		}
	}
	if normalized.includeTotal {
		page.TotalSize = new(uint64)
		*page.TotalSize = total
		page.TotalSizeExact = true
	}
	return page, nil
}

func normalizeExportListRequest(
	access searchjobs.AccessScope,
	request ListRequest,
) (searchjobs.AccessScope, normalizedExportListRequest, error) {
	invalid := func(detail string) (
		searchjobs.AccessScope,
		normalizedExportListRequest,
		error,
	) {
		return searchjobs.AccessScope{},
			normalizedExportListRequest{},
			fmt.Errorf("%w: %s", ErrInvalidListFilter, detail)
	}
	if !validExportMetadataIdentifier(access.TenantID, maximumAccessIDBytes) {
		return invalid("tenant ID is invalid")
	}
	if !validExportMetadataIdentifier(access.OwnerID, maximumAccessIDBytes) {
		return invalid("owner ID is invalid")
	}
	access = searchjobs.AccessScope{
		TenantID: strings.Clone(access.TenantID),
		OwnerID:  strings.Clone(access.OwnerID),
	}
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = defaultExportListPageSize
	}
	if pageSize < 0 || pageSize > MaximumListPageSize {
		return searchjobs.AccessScope{}, normalizedExportListRequest{}, ErrPageSize
	}
	if len(request.PageToken) > maximumExportListTokenSize {
		return searchjobs.AccessScope{}, normalizedExportListRequest{}, ErrInvalidCursor
	}
	if len(request.StateFilters) > maximumExportListStates {
		return invalid(
			fmt.Sprintf(
				"state filters cannot contain more than %d values",
				maximumExportListStates,
			),
		)
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
	normalized := normalizedExportListRequest{
		pageSize:     pageSize,
		pageToken:    strings.Clone(request.PageToken),
		includeTotal: request.IncludeTotal,
		states:       states,
	}
	for _, state := range states {
		normalized.stateSet[state] = true
	}
	if request.SearchJobIDFilter != nil {
		searchJobID := *request.SearchJobIDFilter
		if !validExportMetadataIdentifier(searchJobID, maximumSearchIDBytes) {
			return invalid("search job ID filter is invalid")
		}
		searchJobID = strings.Clone(searchJobID)
		normalized.searchJobID = &searchJobID
	}
	return access, normalized, nil
}

func validExportMetadataIdentifier(value string, maximumBytes int) bool {
	if value == "" ||
		len(value) > maximumBytes ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

// exportMatchesListFiltersLocked examines only fixed-size or immutable fields
// under entry.mu. In particular, exact totals and sparse filters do not clone
// every retained Columns slice; only the bounded response candidates are
// detached with cloneJob.
func exportMatchesListFiltersLocked(
	entry *jobEntry,
	request normalizedExportListRequest,
) bool {
	state := entry.job.State
	if state < StateQueued || state > StateExpired {
		return false
	}
	if len(request.states) != 0 && !request.stateSet[state] {
		return false
	}
	if request.searchJobID != nil &&
		entry.job.SearchJobID != *request.searchJobID {
		return false
	}
	return true
}

func exportListKeyFollowsCursor(key exportListBoundary, cursor exportListCursor) bool {
	createdAt := cursor.lastCreatedAt()
	return key.createdAt.Before(createdAt) ||
		(key.createdAt.Equal(createdAt) && key.id < cursor.LastID)
}

func (manager *Manager) acquireExportListGate(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-manager.ctx.Done():
		return ErrClosed
	default:
	}
	select {
	case manager.listGate <- struct{}{}:
		return nil
	default:
		return ErrListCapacity
	}
}

func (manager *Manager) exportListContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if manager.ctx.Err() != nil {
		return ErrClosed
	}
	return nil
}

func (manager *Manager) insertExportListEntryLocked(entry *jobEntry) {
	scope := entry.access
	manager.jobsByScope[scope] = exportListIndexInsert(
		manager.jobsByScope[scope],
		newExportListIndexNode(entry),
	)
	manager.scopeIndexHighWater = max(
		manager.scopeIndexHighWater,
		len(manager.jobsByScope),
	)
}

func (manager *Manager) removeExportListEntryLocked(entry *jobEntry) {
	scope := entry.access
	root := exportListIndexRemove(manager.jobsByScope[scope], entry)
	// A map update preserves the existing equal key's string headers. Delete
	// first so removing the job that originally supplied this scope key cannot
	// keep its cloned identities alive after their metadata charge is released.
	delete(manager.jobsByScope, scope)
	if root == nil {
		if len(manager.jobsByScope) == 0 {
			manager.jobsByScope = make(
				map[searchjobs.AccessScope]*exportListIndexNode,
			)
			manager.scopeIndexHighWater = 0
		}
		return
	}
	manager.jobsByScope[root.Value().access] = root
}

// compactExportListScopeIndexLocked releases bucket storage retained after
// churn through distinct scopes. Every live job carries exactly one
// conservative scope-map allowance, so cleanup rebuilds after any scope
// deletion instead of retaining a high-water table charged to removed jobs.
// Cleanup already scans all retained jobs; this adds no worse asymptotic work
// and keeps compaction off Create and individual-removal hot paths.
func (manager *Manager) compactExportListScopeIndexLocked() {
	live := len(manager.jobsByScope)
	if live == 0 {
		manager.jobsByScope = make(
			map[searchjobs.AccessScope]*exportListIndexNode,
		)
		manager.scopeIndexHighWater = 0
		return
	}
	if manager.scopeIndexHighWater <= live {
		return
	}
	compacted := make(
		map[searchjobs.AccessScope]*exportListIndexNode,
		live,
	)
	for _, root := range manager.jobsByScope {
		compacted[root.Value().access] = root
	}
	manager.jobsByScope = compacted
	manager.scopeIndexHighWater = live
}

func (manager *Manager) exportListIndexChunk(
	access searchjobs.AccessScope,
	before *exportListBoundary,
) ([]retainedExportListEntry, bool, error) {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if manager.closed {
		return nil, false, ErrClosed
	}
	result := make(
		[]retainedExportListEntry,
		0,
		exportListIndexChunkSize+1,
	)
	exportListIndexCollectBefore(
		manager.jobsByScope[access],
		before,
		&result,
		exportListIndexChunkSize+1,
	)
	more := len(result) > exportListIndexChunkSize
	if more {
		result = result[:exportListIndexChunkSize]
	}
	return result, more, nil
}

func retainedExportListSnapshot(entry *jobEntry) retainedExportListEntry {
	return retainedExportListEntry{
		entry:      entry,
		generation: entry.generation,
		key:        exportListKey(entry),
	}
}

// exportListEntriesComeBefore reads only immutable admission fields. It must
// not copy Job: doing so would race with lifecycle updates to unrelated fields.
func exportListEntriesComeBefore(left, right *jobEntry) bool {
	return exportListKeyComesBefore(exportListKey(left), exportListKey(right))
}

func exportListEntryComesBeforeBoundary(
	entry *jobEntry,
	boundary exportListBoundary,
) bool {
	return exportListKeyComesBefore(exportListKey(entry), boundary)
}

func exportListKey(entry *jobEntry) exportListBoundary {
	return exportListBoundary{
		createdAt: entry.job.CreatedAt,
		id:        entry.job.ID,
	}
}

func exportListKeyComesBefore(left, right exportListBoundary) bool {
	if left.createdAt.Equal(right.createdAt) {
		return left.id < right.id
	}
	return left.createdAt.Before(right.createdAt)
}
