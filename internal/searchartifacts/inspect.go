package searchartifacts

import (
	"context"
	"encoding/json"
	"slices"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// InspectMany returns the visible subset of a bounded ID set using one
// metadata query. It canonicalizes and deduplicates IDs, never refreshes
// retention, never changes tombstone state, and never opens result files.
func (store *Store) InspectMany(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobIDs []string,
) (map[string]Record, error) {
	if ctx == nil || access.TenantID == "" || access.OwnerID == "" ||
		!utf8.ValidString(access.TenantID) || !utf8.ValidString(access.OwnerID) ||
		len(jobIDs) > MaximumInspectManyJobs {
		return nil, ErrInvalid
	}
	canonical := append([]string(nil), jobIDs...)
	for _, jobID := range canonical {
		if !validIdentity(access, jobID) {
			return nil, ErrInvalid
		}
	}
	slices.Sort(canonical)
	canonical = slices.Compact(canonical)

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, ErrClosed
	}
	if len(canonical) == 0 {
		return map[string]Record{}, nil
	}
	encodedIDs, err := json.Marshal(canonical)
	if err != nil {
		return nil, ErrInvalid
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT id, state, visibility, retention_class, lifetime_ns, job_payload,
			artifact_name, artifact_sha256, artifact_size_bytes,
			last_accessed_at_us, expires_at_us, version
		FROM durable_search_jobs
		WHERE tenant_id = ? AND (owner_id = ? OR visibility = ?)
			AND id IN (SELECT value FROM json_each(?) WHERE type = 'text')
		ORDER BY id`, access.TenantID, access.OwnerID, VisibilityEveryone, string(encodedIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := store.nowUTC()
	result := make(map[string]Record, len(canonical))
	for rows.Next() {
		var jobID string
		var columns artifactRecordColumns
		targets := append([]any{&jobID}, columns.scanTargets()...)
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		record, err := columns.record(now)
		if err != nil {
			return nil, err
		}
		result[jobID] = record
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
