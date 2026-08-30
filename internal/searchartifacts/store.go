package searchartifacts

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"fortio.org/safecast"
	"github.com/Suhaibinator/open-splunk/internal/featureops"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
	"golang.org/x/sys/unix"
)

const (
	lockFileName             = ".open-splunk-search-artifacts.lock"
	temporaryPrefix          = ".search-result-"
	temporarySuffix          = ".partial"
	artifactPrefix           = "job-"
	artifactSuffix           = ".results.json"
	maximumArtifactNameBytes = len(artifactPrefix) + sha256.Size*2 + len(artifactSuffix)
)

// Store is a concurrency-safe durable result repository. The database remains
// caller-owned. One process exclusively owns Directory until Close.
type Store struct {
	db                 *sql.DB
	directory          *privatefs.Directory
	lock               *os.File
	clock              func() time.Time
	maximumJobs        int
	maximumBytes       uint64
	tombstoneRetention time.Duration
	cleanupInterval    time.Duration
	reapBatchSize      int
	observer           featureops.Observer
	listCursorKey      [sha256.Size]byte

	mu            sync.Mutex
	closed        bool
	pins          map[string]uint64
	publishing    map[string]struct{}
	verified      map[string]cachedArtifact
	verifying     map[artifactCatalogIdentity]*artifactVerificationFlight
	jobs          uint64
	artifactBytes uint64
	reservedBytes uint64
	loads         sync.WaitGroup
	verify        artifactVerifier
	load          artifactLoader
	ctx           context.Context
	cancel        context.CancelFunc
	workers       sync.WaitGroup
	closeOnce     sync.Once
	closeErr      error
}

var _ searchjobs.JobJournal = (*Store)(nil)

// New opens, locks, and reconciles one durable artifact store.
func New(ctx context.Context, config Config) (*Store, error) {
	if ctx == nil {
		return nil, fmt.Errorf("open search artifact store: %w", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resolved, err := resolvedConfig(config)
	if err != nil {
		return nil, fmt.Errorf("open search artifact store: %w", err)
	}
	directory, err := openArtifactDirectory(resolved.Directory)
	if err != nil {
		return nil, fmt.Errorf("open search artifact store: %w", err)
	}
	lock, err := acquireDirectoryLock(directory)
	if err != nil {
		_ = directory.Close()
		return nil, fmt.Errorf("open search artifact store: %w", err)
	}
	workerContext, cancel := context.WithCancel(context.Background())
	var listCursorKey [sha256.Size]byte
	if _, err := rand.Read(listCursorKey[:]); err != nil {
		cancel()
		_ = lock.Close()
		_ = directory.Close()
		return nil, fmt.Errorf("open search artifact store: generate list cursor key: %w", err)
	}
	store := &Store{
		db:                 resolved.DB,
		directory:          directory,
		lock:               lock,
		clock:              resolved.Clock,
		maximumJobs:        resolved.MaximumJobs,
		maximumBytes:       resolved.MaximumBytes,
		tombstoneRetention: resolved.TombstoneRetention,
		cleanupInterval:    resolved.CleanupInterval,
		reapBatchSize:      resolved.ReapBatchSize,
		observer:           resolved.Observer,
		listCursorKey:      listCursorKey,
		pins:               make(map[string]uint64),
		publishing:         make(map[string]struct{}),
		verified:           make(map[string]cachedArtifact),
		verifying:          make(map[artifactCatalogIdentity]*artifactVerificationFlight),
		verify:             verifyArtifact,
		load:               loadVerifiedArtifact,
		ctx:                workerContext,
		cancel:             cancel,
	}
	if err := store.Reconcile(ctx); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open search artifact store: %w", err)
	}
	if store.cleanupInterval > 0 {
		store.workers.Add(1)
		go store.cleanupWorker()
	}
	return store, nil
}

// Admit persists a queued search before it becomes executable.
func (store *Store) Admit(ctx context.Context, job searchjobs.Job) (returnedErr error) {
	if ctx == nil || job.ID == "" || job.TenantID == "" || job.OwnerID == "" ||
		!utf8.ValidString(job.ID) || job.State != searchjobs.StateQueued {
		return ErrInvalid
	}
	defer func() {
		store.observe(featureops.OperationAdmission, operationOutcome(returnedErr), 1, 0)
	}()
	payload, err := encodeJob(job)
	if err != nil {
		return fmt.Errorf("encode admitted search job: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	usedBytes := store.artifactBytes + store.reservedBytes
	maximumJobs, err := safecast.Conv[uint64](store.maximumJobs)
	if err != nil {
		return ErrCorrupt
	}
	if store.jobs >= maximumJobs ||
		usedBytes < store.artifactBytes || usedBytes >= store.maximumBytes {
		return ErrCapacity
	}
	retentionClass := RetentionManual
	switch job.Source.Origin {
	case searchjobs.JobOriginScheduledReport:
		retentionClass = RetentionScheduledReport
	case searchjobs.JobOriginAlert:
		retentionClass = RetentionScheduledAlert
	}
	lifetime := job.RetentionLifetime
	if lifetime <= 0 {
		lifetime = searchretention.ManualLifetime
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO durable_search_jobs (
			id, tenant_id, owner_id, state, visibility, retention_class,
			lifetime_ns, job_payload, artifact_size_bytes, created_at_us, version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`,
		job.ID, job.TenantID, job.OwnerID, StateQueued, VisibilityPrivate,
		retentionClass, int64(lifetime), payload,
		toUnixMicro(job.CreatedAt), checkedVersion(job.Version),
	)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	store.jobs++
	return nil
}

// Finalize durably records the first terminal search snapshot. Completed rows
// are published separately through PersistResults.
func (store *Store) Finalize(ctx context.Context, job searchjobs.Job) error {
	if ctx == nil || !job.State.Terminal() || job.State == searchjobs.StateExpired {
		return ErrInvalid
	}
	payload, err := encodeJob(job)
	if err != nil {
		return fmt.Errorf("encode terminal search job: %w", err)
	}
	lifetime := searchretention.ManualLifetime
	if !job.FinishedAt.IsZero() && job.ExpiresAt.After(job.FinishedAt) {
		lifetime = job.ExpiresAt.Sub(job.FinishedAt)
	}
	if !validLifetime(lifetime) {
		return ErrInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	persistedState := stateFromJob(job.State)
	if job.State == searchjobs.StateCompleted {
		// Completion is not externally visible until PersistResults atomically
		// publishes the exact artifact and the completed state together. A
		// restart in this staging window safely marks the job interrupted.
		persistedState = StateQueued
	}
	terminalState := stateFromJob(job.State)
	result, err := store.db.ExecContext(ctx, `
		UPDATE durable_search_jobs
		SET state = CASE
				WHEN artifact_name IS NOT NULL AND ? = ? THEN state
				ELSE ?
			END,
			lifetime_ns = ?, job_payload = ?, started_at_us = ?,
			finished_at_us = ?, expires_at_us = ?, version = ?
		WHERE id = ? AND tenant_id = ? AND owner_id = ?`,
		terminalState, StateCompleted, persistedState,
		int64(lifetime), payload, nullableUnixMicro(job.StartedAt),
		nullableUnixMicro(job.FinishedAt), nullableUnixMicro(job.ExpiresAt),
		checkedVersion(job.Version), job.ID, job.TenantID, job.OwnerID,
	)
	if err != nil {
		return err
	}
	return requireChanged(result)
}

// PersistResults atomically publishes the completed immutable result snapshot.
func (store *Store) PersistResults(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	results searchjobs.ResultLease,
) (record Record, returnedErr error) {
	if ctx == nil || results == nil || !validIdentity(access, jobID) {
		return Record{}, ErrInvalid
	}
	var attemptedBytes uint64
	defer func() {
		if errors.Is(returnedErr, ErrCapacity) {
			store.observe(
				featureops.OperationAdmission,
				featureops.OutcomeCapacityRejected,
				1,
				attemptedBytes,
			)
		}
	}()
	fileName := artifactName(jobID)
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return Record{}, ErrClosed
	}
	record, storedName, _, err := store.recordLocked(ctx, access, jobID, AccessInspect)
	if err != nil {
		store.mu.Unlock()
		return Record{}, err
	}
	stagedCompletion := publicationPending(record)
	if record.State != StateCompleted && !stagedCompletion {
		store.mu.Unlock()
		return Record{}, ErrNotReady
	}
	if storedName != "" {
		store.mu.Unlock()
		if storedName != fileName {
			return Record{}, ErrCorrupt
		}
		return record, nil
	}
	if _, active := store.publishing[jobID]; active {
		store.mu.Unlock()
		return Record{}, ErrConflict
	}
	store.publishing[jobID] = struct{}{}
	store.loads.Add(1)
	store.mu.Unlock()
	defer func() {
		store.mu.Lock()
		delete(store.publishing, jobID)
		store.mu.Unlock()
		store.loads.Done()
	}()
	store.mu.Lock()
	closed := store.closed
	store.mu.Unlock()
	if closed {
		return Record{}, ErrClosed
	}

	temporaryName, temporary, err := store.directory.CreateTemporaryFile(randomTemporaryName)
	if err != nil {
		return Record{}, err
	}
	published := false
	reserved := uint64(0)
	defer func() {
		if !published {
			_ = store.directory.UnlinkPinnedRegular(temporaryName, temporary)
		}
		returnedErr = errors.Join(returnedErr, temporary.Close())
		if reserved != 0 {
			store.releaseArtifactReservation(reserved)
		}
	}()
	hasher := sha256.New()
	reservationWriter := &artifactReservationWriter{
		store: store, destination: temporary, digest: hasher,
		reserved: &reserved, attempted: &attemptedBytes,
	}
	writer := bufio.NewWriterSize(reservationWriter, artifactWriteBufferBytes)
	verification, err := writeVerifiedArtifact(ctx, writer, jobID, results)
	if err != nil {
		return Record{}, err
	}
	if err := writer.Flush(); err != nil {
		return Record{}, err
	}
	if err := privatefs.SyncFile(temporary); err != nil {
		return Record{}, err
	}
	digest := hasher.Sum(nil)

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Record{}, ErrClosed
	}
	record, storedName, _, err = store.recordLocked(ctx, access, jobID, AccessInspect)
	if err != nil {
		return Record{}, err
	}
	stagedCompletion = publicationPending(record)
	if !stagedCompletion || storedName != "" {
		return Record{}, ErrConflict
	}
	if err := store.directory.RenameNoReplace(temporaryName, store.directory, fileName); err != nil {
		return Record{}, err
	}
	published = true
	if err := store.directory.Sync(); err != nil {
		_ = store.directory.UnlinkPinnedRegular(fileName, temporary)
		published = false
		return Record{}, err
	}
	fileIdentity, err := statArtifactFile(temporary)
	if err != nil {
		_ = store.directory.UnlinkPinnedRegular(fileName, temporary)
		published = false
		return Record{}, err
	}
	catalog, err := newArtifactCatalogIdentity(
		jobID, fileName, digest, reserved, record.Job.Version,
	)
	if err != nil {
		_ = store.directory.UnlinkPinnedRegular(fileName, temporary)
		published = false
		return Record{}, err
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE durable_search_jobs
		SET state = ?, artifact_name = ?, artifact_sha256 = ?, artifact_size_bytes = ?
		WHERE id = ? AND tenant_id = ? AND owner_id = ? AND artifact_name IS NULL`,
		StateCompleted, fileName, digest, reserved, jobID, access.TenantID, access.OwnerID,
	)
	if err != nil {
		_ = store.directory.UnlinkPinnedRegular(fileName, temporary)
		published = false
		return Record{}, err
	}
	if err := requireChanged(result); err != nil {
		_ = store.directory.UnlinkPinnedRegular(fileName, temporary)
		published = false
		return Record{}, err
	}
	store.commitArtifactReservation(reserved)
	reserved = 0
	store.cacheArtifactLocked(catalog, fileIdentity, verification)
	record.ArtifactBytes = attemptedBytes
	record.ArtifactPresent = true
	record.State = StateCompleted
	record.Job.State = searchjobs.StateCompleted
	return record, nil
}

// FinalizeResults adapts PersistResults to an optional completed-result journal
// hook without exposing repository-specific metadata to the search manager.
// The lifecycle Finalize call must have succeeded first.
func (store *Store) FinalizeResults(
	ctx context.Context,
	job searchjobs.Job,
	results searchjobs.ResultLease,
) error {
	_, err := store.PersistResults(ctx, searchjobs.AccessScope{
		TenantID: job.TenantID,
		OwnerID:  job.OwnerID,
	}, job.ID, results)
	return err
}

// Get reads durable metadata. AccessRefresh applies its snapshotted sliding
// lifetime; AccessInspect never changes retention.
func (store *Store) Get(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	mode AccessMode,
) (Record, error) {
	if ctx == nil || !validIdentity(access, jobID) ||
		(mode != AccessInspect && mode != AccessRefresh && mode != AccessLaunch) {
		return Record{}, ErrInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Record{}, ErrClosed
	}
	return store.getLocked(ctx, access, jobID, mode)
}

// Acquire pins a completed artifact and refreshes its expiry under the store
// mutex, then validates the opened file identity and reuses verified metadata
// without blocking unrelated metadata operations.
func (store *Store) Acquire(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
) (ResultLease, error) {
	if ctx == nil || !validIdentity(access, jobID) {
		return nil, ErrInvalid
	}
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return nil, ErrClosed
	}
	record, name, digest, err := store.recordLocked(ctx, access, jobID, AccessInspect)
	if err != nil {
		store.mu.Unlock()
		return nil, err
	}
	if record.State != StateCompleted || !record.ArtifactPresent || name == "" ||
		len(digest) != sha256.Size || record.ArtifactBytes == 0 || record.ArtifactBytes > store.maximumBytes {
		store.mu.Unlock()
		return nil, ErrNotReady
	}
	artifactSize, err := safecast.Conv[int64](record.ArtifactBytes)
	if err != nil {
		store.mu.Unlock()
		return nil, ErrCorrupt
	}
	file, err := store.directory.OpenRegular(name, privatefs.FilePolicy{
		AllowedModes: []fs.FileMode{0o600},
		MinimumSize:  artifactSize,
		MaximumSize:  artifactSize,
	})
	if err != nil {
		store.mu.Unlock()
		return nil, ErrCorrupt
	}
	now := store.nowUTC()
	expires := now.Add(record.Lifetime)
	result, err := store.db.ExecContext(ctx, `
		UPDATE durable_search_jobs
		SET last_accessed_at_us = ?, expires_at_us = ?
		WHERE id = ? AND state = ? AND expires_at_us > ?`,
		toUnixMicro(now), toUnixMicro(expires), jobID, StateCompleted, toUnixMicro(now))
	if err != nil {
		_ = file.Close()
		store.mu.Unlock()
		return nil, err
	}
	if err := requireChanged(result); err != nil {
		_ = file.Close()
		store.mu.Unlock()
		return nil, ErrExpired
	}
	catalog, err := newArtifactCatalogIdentity(
		jobID, name, digest, record.ArtifactBytes, record.Job.Version,
	)
	if err != nil {
		_ = file.Close()
		store.mu.Unlock()
		return nil, err
	}
	fileIdentity, err := statArtifactFile(file)
	if err != nil {
		_ = file.Close()
		store.mu.Unlock()
		return nil, ErrCorrupt
	}
	store.pins[jobID]++
	store.loads.Add(1)
	load := store.load
	store.mu.Unlock()

	metadata, rows, loadErr := func() (artifactMetadata, artifactRowSource, error) {
		defer store.loads.Done()
		verified, verifyErr := store.artifactVerification(ctx, file, catalog, fileIdentity)
		if verifyErr != nil {
			return artifactMetadata{}, nil, verifyErr
		}
		return load(ctx, file, jobID, verified)
	}()
	if loadErr != nil {
		_ = file.Close()
		store.releasePin(jobID)
		return nil, loadErr
	}
	return &resultLease{
		store:      store,
		jobID:      jobID,
		generation: metadata.Generation,
		schema:     cloneSchema(metadata.Schema),
		rowCount:   metadata.RowCount,
		rowExact:   metadata.RowCountExact,
		rows:       rows,
		truncated:  metadata.ResultsTruncated,
	}, nil
}

// Share atomically makes a completed retained job visible to its tenant and
// switches it to Splunk's seven-day sliding shared lifetime.
func (store *Store) Share(ctx context.Context, access searchjobs.AccessScope, jobID string) (Record, error) {
	return store.updateSettings(ctx, access, jobID, Settings{
		Visibility:     VisibilityEveryone,
		RetentionClass: RetentionShared,
		Lifetime:       searchretention.SharedLifetime,
	}, 0, featureops.OperationShare)
}

// ShareExpected applies Share only when the durable state version matches.
func (store *Store) ShareExpected(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	expectedVersion uint64,
) (Record, error) {
	return store.updateSettings(ctx, access, jobID, Settings{
		Visibility:     VisibilityEveryone,
		RetentionClass: RetentionShared,
		Lifetime:       searchretention.SharedLifetime,
	}, expectedVersion, featureops.OperationShare)
}

// UpdateSettings atomically replaces owner-controlled sharing and retention.
func (store *Store) UpdateSettings(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	settings Settings,
) (Record, error) {
	return store.updateSettings(
		ctx,
		access,
		jobID,
		settings,
		0,
		featureops.OperationRetentionChange,
	)
}

// UpdateSettingsExpected uses the durable state version as an optimistic
// concurrency token. A zero expected version is invalid for this method.
func (store *Store) UpdateSettingsExpected(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	settings Settings,
	expectedVersion uint64,
) (Record, error) {
	if expectedVersion == 0 {
		return Record{}, ErrInvalid
	}
	return store.updateSettings(
		ctx,
		access,
		jobID,
		settings,
		expectedVersion,
		featureops.OperationRetentionChange,
	)
}

func (store *Store) updateSettings(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	settings Settings,
	expectedVersion uint64,
	operation featureops.Operation,
) (record Record, returnedErr error) {
	if ctx == nil || !validIdentity(access, jobID) || !validSettings(settings) {
		return Record{}, ErrInvalid
	}
	defer func() {
		store.observe(operation, operationOutcome(returnedErr), 1, 0)
	}()
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Record{}, ErrClosed
	}
	now := store.nowUTC()
	expires := now.Add(settings.Lifetime)
	query := `
		UPDATE durable_search_jobs
		SET visibility = ?, retention_class = ?, lifetime_ns = ?,
			last_accessed_at_us = ?, expires_at_us = ?, version = version + 1
		WHERE id = ? AND tenant_id = ? AND owner_id = ? AND state = ?
			AND artifact_name IS NOT NULL AND expires_at_us > ?`
	arguments := []any{
		settings.Visibility, settings.RetentionClass, int64(settings.Lifetime),
		toUnixMicro(now), toUnixMicro(expires), jobID, access.TenantID,
		access.OwnerID, StateCompleted, toUnixMicro(now),
	}
	if expectedVersion != 0 {
		query += " AND version = ?"
		arguments = append(arguments, checkedVersion(expectedVersion))
	}
	result, err := store.db.ExecContext(ctx, query, arguments...)
	if err != nil {
		return Record{}, err
	}
	if err := requireChanged(result); err != nil {
		current, getErr := store.getLocked(ctx, access, jobID, AccessInspect)
		if errors.Is(getErr, ErrExpired) {
			return Record{}, ErrExpired
		}
		if getErr == nil && expectedVersion != 0 && current.Job.Version != expectedVersion {
			return Record{}, ErrConflict
		}
		return Record{}, ErrNotFound
	}
	return store.getLocked(ctx, access, jobID, AccessInspect)
}

// Stats reports current physical capacity without refreshing any job.
func (store *Store) Stats(ctx context.Context) (Stats, error) {
	if ctx == nil {
		return Stats{}, ErrInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Stats{}, ErrClosed
	}
	result := Stats{Jobs: store.jobs, ArtifactBytes: store.artifactBytes}
	for _, pins := range store.pins {
		result.ActiveLeases += pins
	}
	return result, nil
}

// Close stops cleanup and releases the exclusive directory lock. It leaves
// durable metadata and artifacts intact.
func (store *Store) Close() error {
	if store == nil {
		return nil
	}
	store.closeOnce.Do(func() {
		store.mu.Lock()
		store.closed = true
		store.cancel()
		clear(store.verified)
		store.mu.Unlock()
		store.workers.Wait()
		store.loads.Wait()
		if store.lock != nil {
			unlockErr := unix.Flock(int(store.lock.Fd()), unix.LOCK_UN)
			store.closeErr = errors.Join(store.closeErr, unlockErr, store.lock.Close())
		}
		store.closeErr = errors.Join(store.closeErr, store.directory.Close())
	})
	return store.closeErr
}

func (store *Store) releasePin(jobID string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if pins := store.pins[jobID]; pins <= 1 {
		delete(store.pins, jobID)
	} else {
		store.pins[jobID] = pins - 1
	}
}

func (store *Store) getLocked(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	mode AccessMode,
) (Record, error) {
	record, _, _, err := store.recordLocked(ctx, access, jobID, mode)
	return record, err
}

func (store *Store) recordLocked(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	mode AccessMode,
) (Record, string, []byte, error) {
	row := store.db.QueryRowContext(ctx, `
		SELECT state, visibility, retention_class, lifetime_ns, job_payload,
			artifact_name, artifact_sha256, artifact_size_bytes,
			last_accessed_at_us, expires_at_us, version
		FROM durable_search_jobs
		WHERE id = ? AND tenant_id = ? AND (owner_id = ? OR visibility = ?)`,
		jobID, access.TenantID, access.OwnerID, VisibilityEveryone,
	)
	var columns artifactRecordColumns
	if err := row.Scan(columns.scanTargets()...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, "", nil, ErrNotFound
		}
		return Record{}, "", nil, err
	}
	now := store.nowUTC()
	record, err := columns.record(now)
	if err != nil {
		return Record{}, "", nil, err
	}
	if record.State == StateExpired {
		if columns.state != StateExpired {
			if err := store.expireLocked(ctx, jobID, now); err != nil {
				return Record{}, "", nil, err
			}
		}
		if mode == AccessLaunch {
			record.State = StateExpired
			return record, "", nil, nil
		}
		return Record{}, "", nil, ErrExpired
	}
	if (mode == AccessRefresh || mode == AccessLaunch) && !record.ExpiresAt.IsZero() {
		expires := now.Add(record.Lifetime)
		if _, err := store.db.ExecContext(ctx, `
			UPDATE durable_search_jobs
			SET last_accessed_at_us = ?, expires_at_us = ?
			WHERE id = ? AND state <> ?`,
			toUnixMicro(now), toUnixMicro(expires), jobID, StateExpired); err != nil {
			return Record{}, "", nil, err
		}
		record.LastAccessedAt = now
		record.ExpiresAt = expires
	}
	return record, columns.artifactName.String, columns.artifactDigest, nil
}

type artifactRecordColumns struct {
	state                       State
	visibility                  Visibility
	retention                   RetentionClass
	lifetime                    int64
	payload                     []byte
	artifactName                sql.NullString
	artifactDigest              []byte
	artifactBytes               int64
	lastAccessedUS, expiresAtUS sql.NullInt64
	version                     int64
}

func (columns *artifactRecordColumns) scanTargets() []any {
	return []any{
		&columns.state, &columns.visibility, &columns.retention, &columns.lifetime,
		&columns.payload, &columns.artifactName, &columns.artifactDigest,
		&columns.artifactBytes, &columns.lastAccessedUS, &columns.expiresAtUS,
		&columns.version,
	}
}

func (columns artifactRecordColumns) record(now time.Time) (Record, error) {
	job, err := decodeJob(columns.payload)
	if err != nil || columns.lifetime <= 0 || columns.artifactBytes < 0 || columns.version <= 0 {
		return Record{}, ErrCorrupt
	}
	record := Record{
		Job:             job,
		State:           columns.state,
		Visibility:      columns.visibility,
		RetentionClass:  columns.retention,
		Lifetime:        time.Duration(columns.lifetime),
		LastAccessedAt:  fromNullableUnixMicro(columns.lastAccessedUS),
		ExpiresAt:       fromNullableUnixMicro(columns.expiresAtUS),
		ArtifactBytes:   uint64(columns.artifactBytes),
		ArtifactPresent: columns.artifactName.Valid,
	}
	record.Job.Version = uint64(columns.version)
	record.Job.ExpiresAt = record.ExpiresAt
	if columns.state == StateExpired || (!record.ExpiresAt.IsZero() && !record.ExpiresAt.After(now)) {
		record.State = StateExpired
	}
	record.Job.State = jobStateFromArtifactState(record.State)
	return record, nil
}

func jobStateFromArtifactState(state State) searchjobs.State {
	switch state {
	case StateQueued:
		return searchjobs.StateQueued
	case StateParsing:
		return searchjobs.StateParsing
	case StatePlanning:
		return searchjobs.StatePlanning
	case StateRunning:
		return searchjobs.StateRunning
	case StateCompleted:
		return searchjobs.StateCompleted
	case StateFailed, StateInterrupted:
		return searchjobs.StateFailed
	case StateCanceled:
		return searchjobs.StateCanceled
	case StateExpired:
		return searchjobs.StateExpired
	default:
		return searchjobs.StateInvalid
	}
}

func (store *Store) expireLocked(ctx context.Context, jobID string, now time.Time) error {
	_, err := store.db.ExecContext(ctx, `
		UPDATE durable_search_jobs
		SET state = ?, tombstoned_at_us = ?
		WHERE id = ? AND state <> ?`,
		StateExpired, toUnixMicro(now), jobID, StateExpired,
	)
	return err
}

func openArtifactDirectory(path string) (*privatefs.Directory, error) {
	if strings.IndexByte(path, 0) >= 0 || !utf8.ValidString(path) {
		return nil, ErrInvalid
	}
	absolute, err := filepath.Abs(path)
	if err != nil || absolute == filepath.VolumeName(absolute)+string(filepath.Separator) {
		return nil, ErrInvalid
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, err
	}
	if err := privatefs.SecureDirectory(absolute); err != nil {
		return nil, err
	}
	return privatefs.OpenDirectory(absolute)
}

func acquireDirectoryLock(directory *privatefs.Directory) (*os.File, error) {
	path := filepath.Join(directory.Path(), lockFileName)
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), lockFileName)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("search artifact lock descriptor is invalid")
	}
	cleanup := func() { _ = file.Close() }
	if err := unix.Fchmod(fd, 0o600); err != nil {
		cleanup()
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		cleanup()
		return nil, err
	}
	if err := privatefs.ValidateExactLockFileInfo(info); err != nil {
		cleanup()
		return nil, err
	}
	if err := privatefs.ValidateNoExtendedACL(file); err != nil {
		cleanup()
		return nil, err
	}
	if err := directory.RequirePinnedRegular(lockFileName, file); err != nil {
		cleanup()
		return nil, err
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		cleanup()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrDirectoryInUse
		}
		return nil, err
	}
	return file, nil
}

func randomTemporaryName() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return temporaryPrefix + hex.EncodeToString(entropy[:]) + temporarySuffix, nil
}

func artifactName(jobID string) string {
	digest := sha256.Sum256([]byte(jobID))
	return artifactPrefix + hex.EncodeToString(digest[:]) + artifactSuffix
}

func validArtifactName(name string) bool {
	if len(name) != maximumArtifactNameBytes || !strings.HasPrefix(name, artifactPrefix) ||
		!strings.HasSuffix(name, artifactSuffix) {
		return false
	}
	raw := name[len(artifactPrefix) : len(name)-len(artifactSuffix)]
	_, err := hex.DecodeString(raw)
	return err == nil
}

func validTemporaryName(name string) bool {
	return strings.HasPrefix(name, temporaryPrefix) && strings.HasSuffix(name, temporarySuffix) && len(name) <= 128
}

func validIdentity(access searchjobs.AccessScope, jobID string) bool {
	return access.TenantID != "" && access.OwnerID != "" && jobID != "" &&
		utf8.ValidString(access.TenantID) && utf8.ValidString(access.OwnerID) &&
		utf8.ValidString(jobID) && len(jobID) <= searchjobs.MaximumJobIDBytes
}

func validSettings(settings Settings) bool {
	if !validLifetime(settings.Lifetime) {
		return false
	}
	switch settings.Visibility {
	case VisibilityPrivate, VisibilityEveryone:
	default:
		return false
	}
	switch settings.RetentionClass {
	case RetentionManual, RetentionShared, RetentionScheduledReport,
		RetentionScheduledAlert, RetentionTriggeredWebhook:
		return true
	default:
		return false
	}
}

func validLifetime(lifetime time.Duration) bool {
	return lifetime > 0 && lifetime <= searchretention.MaximumLifetime
}

func publicationPending(record Record) bool {
	return record.State == StateQueued && !record.ArtifactPresent &&
		!record.Job.FinishedAt.IsZero() && !record.ExpiresAt.IsZero()
}

func stateFromJob(state searchjobs.State) State {
	switch state {
	case searchjobs.StateQueued:
		return StateQueued
	case searchjobs.StateParsing:
		return StateParsing
	case searchjobs.StatePlanning:
		return StatePlanning
	case searchjobs.StateRunning:
		return StateRunning
	case searchjobs.StateCompleted:
		return StateCompleted
	case searchjobs.StateFailed:
		return StateFailed
	case searchjobs.StateCanceled:
		return StateCanceled
	case searchjobs.StateExpired:
		return StateExpired
	default:
		return StateInvalid
	}
}

func checkedVersion(version uint64) int64 {
	if version > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(version)
}

func toUnixMicro(value time.Time) int64 { return value.Round(0).UTC().UnixMicro() }

func nullableUnixMicro(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return toUnixMicro(value)
}

func fromNullableUnixMicro(value sql.NullInt64) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return time.UnixMicro(value.Int64).UTC()
}

func requireChanged(result sql.Result) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return ErrNotFound
	}
	return nil
}

func (store *Store) nowUTC() time.Time { return store.clock().Round(0).UTC() }
