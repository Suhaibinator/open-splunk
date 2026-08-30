package searchartifacts

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/asciifold"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	artifactListCursorDomain  = "search-artifact-list-cursor"
	artifactListCursorVersion = 1
	artifactListFixedSort     = "created_at_us_desc,id_desc"
	maximumListStates         = int(StateInterrupted)
	maximumListAppIDBytes     = 1024
	maximumListTextBytes      = 1024
)

type artifactListCursor struct {
	Version       int    `json:"v"`
	FilterHash    string `json:"f"`
	HighWater     int64  `json:"h"`
	LastCreatedUS int64  `json:"c"`
	LastID        string `json:"i"`
}

type artifactListFilterFingerprint struct {
	Version  int     `json:"v"`
	TenantID string  `json:"t"`
	OwnerID  string  `json:"o"`
	States   []State `json:"s"`
	AppID    *string `json:"a"`
	Text     *string `json:"q"`
	Sort     string  `json:"k"`
}

type normalizedArtifactListRequest struct {
	pageSize int
	token    string
	states   []State
	appID    *string
	text     *string
}

// ListPage returns one bounded durable metadata page without refreshing or
// otherwise changing retention. The query opens no artifact files.
func (store *Store) ListPage(
	ctx context.Context,
	access searchjobs.AccessScope,
	request ListRequest,
) (ListPage, error) {
	if ctx == nil {
		return ListPage{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return ListPage{}, err
	}
	normalized, err := normalizeArtifactListRequest(access, request)
	if err != nil {
		return ListPage{}, err
	}
	filterHash, err := artifactListFilterHash(access, normalized)
	if err != nil {
		return ListPage{}, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ListPage{}, ErrClosed
	}

	var cursor artifactListCursor
	if normalized.token != "" {
		cursor, err = store.decodeArtifactListCursor(normalized.token, filterHash)
		if err != nil {
			return ListPage{}, err
		}
	} else if err := store.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(rowid), 0) FROM durable_search_jobs`).Scan(&cursor.HighWater); err != nil {
		return ListPage{}, err
	}
	if cursor.HighWater == 0 {
		return ListPage{Items: []ListItem{}}, nil
	}
	firstPageToken, err := store.encodeArtifactListCursor(filterHash, cursor.HighWater, 0, "")
	if err != nil {
		return ListPage{}, err
	}

	query, arguments := artifactListQuery(access, normalized, cursor, store.nowUTC().UnixMicro())
	rows, err := store.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return ListPage{}, err
	}
	defer rows.Close()

	items := make([]ListItem, 0, normalized.pageSize+1)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return ListPage{}, err
		}
		var jobID string
		var tenantID string
		var ownerID string
		var createdUS int64
		var columns artifactRecordColumns
		targets := append([]any{&jobID, &tenantID, &ownerID}, columns.scanTargets()...)
		targets = append(targets, &createdUS)
		if err := rows.Scan(targets...); err != nil {
			return ListPage{}, err
		}
		record, err := columns.record(store.nowUTC())
		if err != nil {
			return ListPage{}, err
		}
		if !validListIdentity(jobID, searchjobs.MaximumJobIDBytes) ||
			record.Job.ID != jobID || record.Job.TenantID != tenantID ||
			record.Job.OwnerID != ownerID || record.Job.CreatedAt.UnixMicro() != createdUS ||
			!artifactListRecordMatches(record, normalized) {
			return ListPage{}, ErrCorrupt
		}
		after, err := store.encodeArtifactListCursor(filterHash, cursor.HighWater, createdUS, jobID)
		if err != nil {
			return ListPage{}, err
		}
		items = append(items, ListItem{Record: record, AfterPageToken: after})
	}
	if err := rows.Err(); err != nil {
		return ListPage{}, err
	}
	hasMore := len(items) > normalized.pageSize
	if hasMore {
		items = items[:normalized.pageSize]
	}
	page := ListPage{Items: items, FirstPageToken: firstPageToken}
	if hasMore {
		page.NextPageToken = items[len(items)-1].AfterPageToken
	}
	return page, nil
}

func normalizeArtifactListRequest(
	access searchjobs.AccessScope,
	request ListRequest,
) (normalizedArtifactListRequest, error) {
	invalid := func() (normalizedArtifactListRequest, error) {
		return normalizedArtifactListRequest{}, ErrInvalid
	}
	if !validListIdentity(access.TenantID, 1024) || !validListIdentity(access.OwnerID, 255) ||
		request.PageSize <= 0 || request.PageSize > MaximumListPageSize ||
		len(request.PageToken) > MaximumListTokenBytes ||
		len(request.StateFilters) > maximumListStates {
		return invalid()
	}
	states := slices.Clone(request.StateFilters)
	for _, state := range states {
		if state < StateQueued || state > StateInterrupted {
			return invalid()
		}
	}
	slices.Sort(states)
	states = slices.Compact(states)
	appID := cloneListString(request.AppIDFilter)
	if appID != nil && (len(*appID) > maximumListAppIDBytes || !validListString(*appID)) {
		return invalid()
	}
	text := cloneListString(request.TextFilter)
	if text != nil && (len(*text) > maximumListTextBytes || !validListString(*text)) {
		return invalid()
	}
	return normalizedArtifactListRequest{
		pageSize: request.PageSize,
		token:    request.PageToken,
		states:   states,
		appID:    appID,
		text:     text,
	}, nil
}

func artifactListQuery(
	access searchjobs.AccessScope,
	request normalizedArtifactListRequest,
	cursor artifactListCursor,
	nowUS int64,
) (string, []any) {
	arguments := []any{cursor.HighWater, access.TenantID, access.OwnerID, VisibilityEveryone}
	predicates := []string{
		"rowid <= ?",
		"tenant_id = ?",
		"(owner_id = ? OR visibility = ?)",
	}
	if cursor.LastID != "" {
		predicates = append(predicates, "(created_at_us < ? OR (created_at_us = ? AND id < ?))")
		arguments = append(arguments, cursor.LastCreatedUS, cursor.LastCreatedUS, cursor.LastID)
	}
	if statePredicate, stateArguments := artifactListStatePredicate(request.states, nowUS); statePredicate != "" {
		predicates = append(predicates, statePredicate)
		arguments = append(arguments, stateArguments...)
	}
	if request.appID != nil {
		predicates = append(predicates, "json_extract(CAST(job_payload AS TEXT), '$.job.AppID') = ?")
		arguments = append(arguments, *request.appID)
	}
	if request.text != nil {
		predicates = append(predicates, "instr(lower(json_extract(CAST(job_payload AS TEXT), '$.job.SPL')), lower(?)) > 0")
		arguments = append(arguments, *request.text)
	}
	arguments = append(arguments, request.pageSize+1)
	return `
		SELECT id, tenant_id, owner_id, state, visibility, retention_class, lifetime_ns, job_payload,
			artifact_name, artifact_sha256, artifact_size_bytes,
			last_accessed_at_us, expires_at_us, version, created_at_us
		FROM durable_search_jobs
		WHERE ` + strings.Join(predicates, " AND ") + `
		ORDER BY created_at_us DESC, id DESC
		LIMIT ?`, arguments
}

func artifactListStatePredicate(states []State, nowUS int64) (string, []any) {
	if len(states) == 0 {
		return "", nil
	}
	direct := make([]State, 0, len(states)+3)
	hasActive := false
	hasExpired := false
	for _, state := range states {
		switch state {
		case StateQueued, StateParsing, StatePlanning, StateRunning:
			hasActive = true
		case StateExpired:
			hasExpired = true
		default:
			direct = append(direct, state)
		}
	}
	if hasActive {
		direct = append(direct, StateQueued, StateParsing, StatePlanning, StateRunning)
	}
	slices.Sort(direct)
	direct = slices.Compact(direct)
	parts := make([]string, 0, 2)
	arguments := make([]any, 0, len(direct)+1)
	if len(direct) != 0 {
		parts = append(parts, "state IN ("+strings.TrimSuffix(strings.Repeat("?,", len(direct)), ",")+")")
		for _, state := range direct {
			arguments = append(arguments, state)
		}
	}
	if hasExpired {
		parts = append(parts, "(state = ? OR (expires_at_us IS NOT NULL AND expires_at_us <= ?))")
		arguments = append(arguments, StateExpired, nowUS)
	}
	return "(" + strings.Join(parts, " OR ") + ")", arguments
}

func artifactListRecordMatches(record Record, request normalizedArtifactListRequest) bool {
	if request.appID != nil && record.Job.AppID != *request.appID {
		return false
	}
	if request.text != nil {
		matcher := asciifold.New(*request.text)
		if !matcher.Contains(record.Job.SPL) {
			return false
		}
	}
	return true
}

func artifactListFilterHash(
	access searchjobs.AccessScope,
	request normalizedArtifactListRequest,
) (string, error) {
	payload, err := json.Marshal(artifactListFilterFingerprint{
		Version:  artifactListCursorVersion,
		TenantID: access.TenantID,
		OwnerID:  access.OwnerID,
		States:   request.states,
		AppID:    request.appID,
		Text:     request.text,
		Sort:     artifactListFixedSort,
	})
	if err != nil {
		return "", fmt.Errorf("encode search artifact list filters: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func (store *Store) encodeArtifactListCursor(
	filterHash string,
	highWater, createdUS int64,
	jobID string,
) (string, error) {
	return cursorcodec.Encode(store.listCursorKey[:], artifactListCursorDomain,
		artifactListCursorVersion, MaximumListTokenBytes, artifactListCursor{
			Version:       artifactListCursorVersion,
			FilterHash:    filterHash,
			HighWater:     highWater,
			LastCreatedUS: createdUS,
			LastID:        jobID,
		})
}

func (store *Store) decodeArtifactListCursor(token, filterHash string) (artifactListCursor, error) {
	invalid := func() (artifactListCursor, error) { return artifactListCursor{}, ErrInvalidCursor }
	var cursor artifactListCursor
	if err := cursorcodec.Decode(store.listCursorKey[:], artifactListCursorDomain,
		artifactListCursorVersion, MaximumListTokenBytes, token, &cursor); err != nil {
		return invalid()
	}
	digest, err := base64.RawURLEncoding.DecodeString(cursor.FilterHash)
	if err != nil || len(digest) != sha256.Size ||
		base64.RawURLEncoding.EncodeToString(digest) != cursor.FilterHash ||
		cursor.Version != artifactListCursorVersion || cursor.FilterHash != filterHash ||
		cursor.HighWater <= 0 || cursor.LastCreatedUS < 0 ||
		((cursor.LastID == "") != (cursor.LastCreatedUS == 0)) ||
		len(cursor.LastID) > searchjobs.MaximumJobIDBytes || !utf8.ValidString(cursor.LastID) {
		return invalid()
	}
	return cursor, nil
}

func cloneListString(value *string) *string {
	if value == nil {
		return nil
	}
	return new(strings.Clone(*value))
}

func validListString(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

func validListIdentity(value string, maximumBytes int) bool {
	if value == "" || len(value) > maximumBytes || !utf8.ValidString(value) ||
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
