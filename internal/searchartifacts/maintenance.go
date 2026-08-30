package searchartifacts

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"fortio.org/safecast"
	"github.com/Suhaibinator/open-splunk/internal/featureops"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// Reconcile removes unpublished temporary/orphan files, verifies completed
// artifacts, and marks work that was active at shutdown as interrupted.
func (store *Store) Reconcile(ctx context.Context) (returnedErr error) {
	if ctx == nil {
		return ErrInvalid
	}
	var observedItems, observedBytes uint64
	defer func() {
		store.observe(
			featureops.OperationReconciliation,
			operationOutcome(returnedErr),
			observedItems,
			observedBytes,
		)
	}()
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	clear(store.verified)
	rows, err := store.db.QueryContext(ctx, `
		SELECT id, state, job_payload, artifact_name, artifact_sha256,
			artifact_size_bytes, lifetime_ns, version
		FROM durable_search_jobs`)
	if err != nil {
		return err
	}
	type candidate struct {
		id       string
		state    State
		payload  []byte
		name     sql.NullString
		digest   []byte
		bytes    int64
		lifetime int64
		version  int64
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.id, &item.state, &item.payload, &item.name,
			&item.digest, &item.bytes, &item.lifetime, &item.version); err != nil {
			_ = rows.Close()
			return err
		}
		candidates = append(candidates, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	referenced := make(map[string]struct{}, len(candidates))
	capacityBytes := uint64(0)
	directoryChanged := false
	now := store.nowUTC()
	for _, item := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		if item.name.Valid {
			referenced[item.name.String] = struct{}{}
		}
		if item.bytes < 0 {
			return ErrCorrupt
		}
		capacityBytes = saturatingAdd(capacityBytes, uint64(item.bytes))
		switch item.state {
		case StateQueued, StateParsing, StatePlanning, StateRunning:
			if err := store.markInterruptedLocked(ctx, item.id, item.payload, time.Duration(item.lifetime), now); err != nil {
				return err
			}
			observedItems++
		case StateCompleted:
			if !item.name.Valid || item.name.String != artifactName(item.id) ||
				item.bytes <= 0 || len(item.digest) != sha256.Size || item.version <= 0 ||
				!store.validArtifactLocked(
					ctx, item.id, item.name.String, item.digest, item.bytes, uint64(item.version),
				) {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := store.markInterruptedLocked(ctx, item.id, item.payload, time.Duration(item.lifetime), now); err != nil {
					return err
				}
				observedItems++
			}
		}
	}
	entries, err := store.directory.List(store.maximumJobs*2 + 1024)
	if err != nil {
		return err
	}
	for _, name := range entries {
		if name == lockFileName {
			continue
		}
		_, retained := referenced[name]
		if validTemporaryName(name) || (validArtifactName(name) && !retained) {
			removedBytes, removed, err := store.removeFileLocked(name)
			if err != nil {
				return err
			}
			directoryChanged = directoryChanged || removed
			observedItems++
			observedBytes = saturatingAdd(observedBytes, removedBytes)
			continue
		}
		if !retained {
			return fmt.Errorf("reconcile search artifacts: unexpected private-directory entry %q", name)
		}
	}
	store.jobs = uint64(len(candidates))
	store.artifactBytes = capacityBytes
	if directoryChanged {
		return store.directory.Sync()
	}
	return nil
}

// Reap expires due jobs at the exact database deadline and removes tombstones
// only after their grace period and final result lease.
func (store *Store) Reap(ctx context.Context) (returnedErr error) {
	if ctx == nil {
		return ErrInvalid
	}
	var observedItems, observedBytes uint64
	defer func() {
		if returnedErr == nil && observedItems == 0 && observedBytes == 0 {
			return
		}
		store.observe(
			featureops.OperationCleanup,
			operationOutcome(returnedErr),
			observedItems,
			observedBytes,
		)
	}()
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	now := store.nowUTC()
	expiredRows, err := store.db.QueryContext(ctx, `
		UPDATE durable_search_jobs
		SET state = ?, tombstoned_at_us = ?
		WHERE id IN (
			SELECT id FROM durable_search_jobs
			WHERE tombstoned_at_us IS NULL AND expires_at_us IS NOT NULL
				AND expires_at_us <= ? AND state <> ?
			ORDER BY expires_at_us, id
			LIMIT ?
		)
		RETURNING id`,
		StateExpired, toUnixMicro(now), toUnixMicro(now), StateExpired,
		store.reapBatchSize)
	if err != nil {
		return err
	}
	var expiredIDs []string
	for expiredRows.Next() {
		var jobID string
		if err := expiredRows.Scan(&jobID); err != nil {
			_ = expiredRows.Close()
			return err
		}
		expiredIDs = append(expiredIDs, jobID)
	}
	if err := expiredRows.Close(); err != nil {
		return err
	}
	observedItems = uint64(len(expiredIDs))
	for _, jobID := range expiredIDs {
		store.invalidateArtifactLocked(jobID)
	}
	cutoff := now.Add(-store.tombstoneRetention)
	rows, err := store.db.QueryContext(ctx, `
		SELECT id, artifact_name, artifact_size_bytes
		FROM durable_search_jobs
		WHERE state = ? AND tombstoned_at_us IS NOT NULL AND tombstoned_at_us <= ?
		ORDER BY tombstoned_at_us, id
		LIMIT ?`,
		StateExpired, toUnixMicro(cutoff), store.reapBatchSize)
	if err != nil {
		return err
	}
	type tombstone struct {
		id    string
		name  sql.NullString
		bytes int64
	}
	var tombstones []tombstone
	for rows.Next() {
		var item tombstone
		if err := rows.Scan(&item.id, &item.name, &item.bytes); err != nil {
			_ = rows.Close()
			return err
		}
		tombstones = append(tombstones, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	directoryChanged := false
	for _, item := range tombstones {
		if item.bytes < 0 {
			return ErrCorrupt
		}
		if store.pins[item.id] != 0 {
			if _, err := store.db.ExecContext(ctx, `
				UPDATE durable_search_jobs SET tombstoned_at_us = ?
				WHERE id = ? AND state = ?`,
				toUnixMicro(now), item.id, StateExpired); err != nil {
				return err
			}
			continue
		}
		store.invalidateArtifactLocked(item.id)
		if item.name.Valid {
			removedBytes, removed, err := store.removeFileLocked(item.name.String)
			if err != nil {
				return err
			}
			directoryChanged = directoryChanged || removed
			if removedBytes == 0 && item.bytes > 0 {
				removedBytes = uint64(item.bytes)
			}
			observedBytes = saturatingAdd(observedBytes, removedBytes)
		}
		deleted, err := store.db.ExecContext(ctx, `
			DELETE FROM durable_search_jobs
			WHERE id = ? AND state = ? AND tombstoned_at_us IS NOT NULL AND tombstoned_at_us <= ?`,
			item.id, StateExpired, toUnixMicro(cutoff))
		if err != nil {
			return err
		}
		if err := requireChanged(deleted); err != nil {
			return err
		}
		store.jobs, err = subtractCapacity(store.jobs, 1)
		if err != nil {
			return err
		}
		store.artifactBytes, err = subtractCapacity(store.artifactBytes, uint64(item.bytes))
		if err != nil {
			return err
		}
		observedItems++
	}
	if directoryChanged {
		return store.directory.Sync()
	}
	return nil
}

func (store *Store) cleanupWorker() {
	defer store.workers.Done()
	ticker := time.NewTicker(store.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-store.ctx.Done():
			return
		case <-ticker.C:
			_ = store.Reap(store.ctx)
		}
	}
}

func (store *Store) validArtifactLocked(
	ctx context.Context,
	jobID string,
	name string,
	digest []byte,
	expectedBytes int64,
	version uint64,
) bool {
	if expectedBytes <= 0 || uint64(expectedBytes) > store.maximumBytes {
		return false
	}
	file, err := store.directory.OpenRegular(name, privatefs.FilePolicy{
		AllowedModes: []fs.FileMode{0o600},
		MinimumSize:  expectedBytes,
		MaximumSize:  expectedBytes,
	})
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()
	fileIdentity, err := statArtifactFile(file)
	if err != nil {
		return false
	}
	catalog, err := newArtifactCatalogIdentity(
		jobID, name, digest, uint64(expectedBytes), version,
	)
	if err != nil {
		return false
	}
	verification, err := store.verify(ctx, file, catalog)
	if err != nil {
		return false
	}
	currentIdentity, err := statArtifactFile(file)
	if err != nil || currentIdentity != fileIdentity {
		return false
	}
	store.cacheArtifactLocked(catalog, fileIdentity, verification)
	return true
}

func (store *Store) markInterruptedLocked(
	ctx context.Context,
	jobID string,
	payload []byte,
	lifetime time.Duration,
	now time.Time,
) error {
	store.invalidateArtifactLocked(jobID)
	job, err := decodeJob(payload)
	if err != nil {
		return ErrCorrupt
	}
	if !validLifetime(lifetime) {
		lifetime = DefaultTombstoneRetention
	}
	job.State = searchjobs.StateFailed
	job.FinishedAt = now
	job.ExpiresAt = now.Add(lifetime)
	job.Failure = &searchjobs.Failure{
		Code:      searchjobs.FailureInternal,
		Message:   "search was interrupted by server restart",
		Retryable: true,
	}
	encoded, err := encodeJob(job)
	if err != nil {
		return err
	}
	_, err = store.db.ExecContext(ctx, `
		UPDATE durable_search_jobs
		SET state = ?, job_payload = ?, finished_at_us = ?, expires_at_us = ?,
			tombstoned_at_us = NULL, version = ?
		WHERE id = ?`,
		StateInterrupted, encoded, toUnixMicro(now), toUnixMicro(job.ExpiresAt),
		checkedVersion(job.Version), jobID,
	)
	return err
}

func (store *Store) removeFileLocked(name string) (uint64, bool, error) {
	store.invalidateArtifactNameLocked(name)
	maximumSize, err := safecast.Conv[int64](store.maximumBytes)
	if err != nil {
		return 0, false, ErrCorrupt
	}
	file, err := store.directory.OpenRegular(name, privatefs.FilePolicy{
		AllowedModes: []fs.FileMode{0o600},
		MaximumSize:  maximumSize,
	})
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return 0, false, err
	}
	if err := store.directory.UnlinkPinnedRegular(name, file); err != nil {
		return 0, false, err
	}
	size, err := safecast.Conv[uint64](info.Size())
	if err != nil {
		return 0, false, ErrCorrupt
	}
	return size, true, nil
}
