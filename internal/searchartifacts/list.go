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
	Version           int     `json:"v"`
	TenantID          string  `json:"t"`
	OwnerID           string  `json:"o"`
	OrganizationID    string  `json:"g"`
	SiteAdministrator bool    `json:"z"`
	States            []State `json:"s"`
	AppID             *string `json:"a"`
	Text              *string `json:"q"`
	Sort              string  `json:"k"`
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
	closed := store.closed
	store.mu.Unlock()
	if closed {
		return ListPage{}, ErrClosed
	}

	var cursor artifactListCursor
	if normalized.token != "" {
		cursor, err = store.decodeArtifactListCursor(normalized.token, filterHash)
		if err != nil {
			return ListPage{}, err
		}
	} else if err := store.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(catalog_sequence), 0) FROM durable_search_jobs`).Scan(&cursor.HighWater); err != nil {
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
	scanned := 0
	lastScannedToken := ""
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
		scanned++
		lastScannedToken = after
		if store.captureOptions.Authorize != nil {
			if err := store.captureOptions.Authorize(ctx, record.Job); err != nil {
				if ctx.Err() != nil {
					return ListPage{}, ctx.Err()
				}
				continue
			}
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
	} else if scanned == normalized.pageSize+1 {
		page.NextPageToken = lastScannedToken
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
	arguments := []any{}
	bind := func(value any) string {
		arguments = append(arguments, value)
		return fmt.Sprintf("$%d", len(arguments))
	}
	predicates := []string{
		"catalog_sequence <= " + bind(cursor.HighWater),
		"tenant_id = " + bind(access.TenantID),
		"(owner_id = " + bind(access.OwnerID) + " OR visibility = " + bind(VisibilityEveryone) + " OR " + bind(access.SiteAdministrator) + ")",
	}
	if !access.SiteAdministrator && access.OrganizationID != "" {
		predicates = append(predicates, "convert_from(job_payload,'UTF8')::jsonb->'job'->>'OrganizationID' = "+bind(access.OrganizationID))
	}
	if cursor.LastID != "" {
		predicates = append(predicates, "(created_at_us < "+bind(cursor.LastCreatedUS)+" OR (created_at_us = "+bind(cursor.LastCreatedUS)+" AND id < "+bind(cursor.LastID)+"))")
	}
	if len(request.states) != 0 {
		parts := []string{}
		for _, state := range request.states {
			if state == StateExpired {
				parts = append(parts, "(state = "+bind(state)+" OR (expires_at_us IS NOT NULL AND expires_at_us <= "+bind(nowUS)+"))")
			} else {
				parts = append(parts, "state = "+bind(state))
			}
		}
		predicates = append(predicates, "("+strings.Join(parts, " OR ")+")")
	}
	if request.appID != nil {
		predicates = append(predicates, "convert_from(job_payload,'UTF8')::jsonb->'job'->>'AppID' = "+bind(*request.appID))
	}
	if request.text != nil {
		predicates = append(predicates, "strpos(lower(convert_from(job_payload,'UTF8')::jsonb->'job'->>'SPL'),lower("+bind(*request.text)+")) > 0")
	}
	limit := bind(request.pageSize + 1)
	return `SELECT id, tenant_id, owner_id, state, visibility, retention_class, lifetime_ns, job_payload,
		artifact_name, artifact_sha256, artifact_size_bytes,
		last_accessed_at_us, expires_at_us, version, created_at_us
		FROM durable_search_jobs WHERE ` + strings.Join(predicates, " AND ") + ` ORDER BY created_at_us DESC,id DESC LIMIT ` + limit, arguments
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
		Version:        artifactListCursorVersion,
		TenantID:       access.TenantID,
		OwnerID:        access.OwnerID,
		OrganizationID: access.OrganizationID, SiteAdministrator: access.SiteAdministrator,
		States: request.states,
		AppID:  request.appID,
		Text:   request.text,
		Sort:   artifactListFixedSort,
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
