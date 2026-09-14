package export

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func (manager *Manager) durableEntryLocked(entry *jobEntry) (DurableJob, error) {
	result := DurableJob{Access: entry.access, Job: cloneJob(entry.job), MetadataBytes: entry.accountedMetadata}
	if entry.job.State != StateCompleted || entry.artifactPath == "" {
		return result, nil
	}
	result.ArtifactName = filepath.Base(entry.artifactPath)
	file, err := manager.artifactRoot.Open(result.ArtifactName)
	if err != nil {
		return DurableJob{}, ErrArtifactUnavailable
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || entry.artifactIdentity == nil || !os.SameFile(info, entry.artifactIdentity) {
		return DurableJob{}, ErrArtifactUnavailable
	}
	digest := sha256.New()
	size, err := io.Copy(digest, io.LimitReader(file, safecast.MustConv[int64](entry.job.ByteLimit)+1))
	if err != nil || size < 0 || uint64(size) != entry.job.Artifact.SizeBytes {
		return DurableJob{}, ErrArtifactUnavailable
	}
	result.ArtifactSHA256 = digest.Sum(nil)
	return result, nil
}

// Call with entry.mu held. A persistence failure makes the artifact unavailable
// rather than exposing an unrecorded completed outcome after restart.
func (manager *Manager) persistEntryLocked(entry *jobEntry) {
	if manager.journal == nil {
		return
	}
	projection, err := manager.durableEntryLocked(entry)
	if err == nil {
		err = manager.journal.Update(context.Background(), projection)
	}
	if err == nil {
		return
	}
	if entry.cancel != nil {
		entry.cancel()
	}
	entry.job.State = StateFailed
	entry.job.Version++
	entry.job.Artifact = nil
	entry.job.Failure = &Failure{Code: FailureStorageUnavailable, Message: "export metadata could not be persisted", Retryable: false}
	entry.job.FinishedAt = manager.nowUTC()
	entry.job.ExpiresAt = entry.job.FinishedAt.Add(manager.artifactTTL)
	entry.job.Progress.UpdatedAt = entry.job.FinishedAt
	projection = DurableJob{Access: entry.access, Job: cloneJob(entry.job), MetadataBytes: entry.accountedMetadata}
	retryErr := manager.journal.Update(context.Background(), projection)
	manager.reportCleanupError(errors.Join(err, retryErr))
}

func (manager *Manager) restoreJournal(ctx context.Context) error {
	if manager.journal == nil {
		return nil
	}
	entries, err := manager.journal.Restore(ctx, manager.maxJobs)
	if err != nil {
		return fmt.Errorf("restore export metadata: %w", err)
	}
	if len(entries) > manager.maxJobs {
		return ErrCapacity
	}
	manager.nowUTC()
	for _, retained := range entries {
		if err := validateDurableJob(retained); err != nil {
			return err
		}
		job := cloneJob(retained.Job)
		now := manager.observeTime(maxTime(job.CreatedAt, job.Progress.UpdatedAt))
		entryContext, cancel := context.WithCancel(manager.ctx)
		entry := &jobEntry{access: retained.Access, job: job, ctx: entryContext, cancel: cancel, workerDone: true, leaseReleased: true}
		metadata, err := requestedMetadataBytes(manager.artifactDir, retained.Access, job.SearchJobID, job.Columns)
		if err != nil {
			return err
		}
		entry.accountedMetadata = max(metadata+patternMetadataBytes(job.Pattern), retained.MetadataBytes)
		unavailable := job.State == StateQueued || job.State == StateRunning
		if job.State == StateCompleted {
			unavailable = !manager.restoreArtifact(entry, retained)
		}
		if unavailable {
			entry.job.State = StateFailed
			entry.job.Version++
			entry.job.Artifact = nil
			entry.job.Failure = &Failure{Code: FailureStorageUnavailable, Message: "export was interrupted or its retained artifact is unavailable", Retryable: false}
			entry.job.FinishedAt = maxTime(now, job.CreatedAt)
			entry.job.ExpiresAt = entry.job.FinishedAt.Add(manager.artifactTTL)
			entry.job.Progress.UpdatedAt = entry.job.FinishedAt
			entry.artifactPath = ""
			entry.accountedBytes = 0
			manager.persistEntryLocked(entry)
		}
		if !entry.job.ExpiresAt.IsZero() && !entry.job.ExpiresAt.After(now) {
			entry.job.State = StateExpired
			entry.job.Artifact = nil
			entry.job.Version++
			entry.expiredAt = now
			manager.persistEntryLocked(entry)
		}
		if entry.accountedMetadata > manager.maxTotalMetadata-manager.totalMetadata || entry.accountedBytes > manager.maxTotalBytes-manager.totalBytes {
			return ErrCapacity
		}
		manager.totalMetadata += entry.accountedMetadata
		manager.totalBytes += entry.accountedBytes
		manager.nextGeneration++
		entry.generation = manager.nextGeneration
		manager.jobs[entry.job.ID] = entry
		manager.insertExportListEntryLocked(entry)
	}
	return manager.cleanDurableDirectory()
}

func (manager *Manager) restoreArtifact(entry *jobEntry, retained DurableJob) bool {
	job := entry.job
	expectedName, _, _ := CanonicalArtifactMetadata(job.ID, job.Format)
	if retained.ArtifactName != expectedName || len(retained.ArtifactSHA256) != sha256.Size || job.Artifact == nil {
		return false
	}
	info, err := manager.artifactRoot.Lstat(expectedName)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	file, err := manager.artifactRoot.Open(expectedName)
	if err != nil {
		return false
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return false
	}
	digest := sha256.New()
	size, err := io.Copy(digest, io.LimitReader(file, safecast.MustConv[int64](job.ByteLimit)+1))
	if err != nil || size < 0 || uint64(size) != job.Artifact.SizeBytes || !bytes.Equal(digest.Sum(nil), retained.ArtifactSHA256) {
		return false
	}
	entry.artifactPath = filepath.Join(manager.artifactDir, expectedName)
	entry.artifactIdentity = opened
	entry.accountedBytes = uint64(size)
	return true
}

func validateDurableJob(retained DurableJob) error {
	job := retained.Job
	request := CreateRequest{SearchJobID: job.SearchJobID, SourceKind: job.SourceKind, Pattern: job.Pattern}
	if err := normalizePatternSource(&request); err != nil {
		return err
	}
	if validateAccessScope(retained.Access) != nil || !validID(job.ID) || job.Version == 0 || job.CreatedAt.IsZero() || job.SearchJobID == "" || len(job.SearchJobID) > maximumSearchIDBytes || job.ByteLimit == 0 || job.ByteLimit > hardMaximumByteLimit || job.RowLimit == 0 || job.RowLimit > hardMaximumRowLimit || job.State < StateQueued || job.State > StateExpired || (job.Format != FormatCSV && job.Format != FormatJSONLines) || len(job.Columns) > maximumColumns || retained.MetadataBytes > hardMaximumMetadata {
		return errors.New("invalid durable export metadata")
	}
	return nil
}

func maxTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return right
	}
	return left
}

func (manager *Manager) ReplayIdempotent(ctx context.Context, access searchjobs.AccessScope, intent requestidempotency.Intent) (Job, bool, error) {
	journal, ok := manager.journal.(IdempotentJournal)
	if !ok {
		return Job{}, false, requestidempotency.ErrUnavailable
	}
	job, found, err := journal.Lookup(ctx, access, intent)
	if err != nil || !found {
		return job, found, err
	}
	current, err := manager.Get(ctx, access, job.ID)
	return current, true, err
}

func (manager *Manager) CreateIdempotent(ctx context.Context, access searchjobs.AccessScope, request CreateRequest, intent requestidempotency.Intent) (Job, bool, error) {
	manager.idempotencyMu.Lock()
	defer manager.idempotencyMu.Unlock()
	job, found, err := manager.ReplayIdempotent(ctx, access, intent)
	if err != nil || found {
		return job, found, err
	}
	job, err = manager.create(ctx, access, request, &intent)
	return job, false, err
}

func (manager *Manager) admitJournal(ctx context.Context, access searchjobs.AccessScope, job Job, intent *requestidempotency.Intent) error {
	if intent == nil {
		return manager.journal.Admit(ctx, access, job)
	}
	journal, ok := manager.journal.(IdempotentJournal)
	if !ok {
		return requestidempotency.ErrUnavailable
	}
	return journal.AdmitIdempotent(ctx, access, job, *intent)
}

func (manager *Manager) observeTime(value time.Time) time.Time {
	manager.clockMu.Lock()
	defer manager.clockMu.Unlock()
	if value.After(manager.clockHighWater) {
		manager.clockHighWater = value
	}
	return manager.clockHighWater
}

// Startup holds the artifact directory lock and has not started workers. Only
// manager-owned names are reclaimed; interrupted partials are never resumed.
func (manager *Manager) cleanDurableDirectory() error {
	directory, err := manager.artifactRoot.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	for {
		entries, readErr := directory.ReadDir(128)
		for _, entry := range entries {
			name := entry.Name()
			partial := strings.HasPrefix(name, ".open-splunk-export-") && strings.HasSuffix(name, ".partial")
			id := strings.TrimSuffix(strings.TrimSuffix(name, ".jsonl"), ".csv")
			artifact := id != name && validID(id)
			if !partial && !artifact {
				continue
			}
			if artifact {
				retained := manager.jobs[id]
				if retained != nil && retained.job.State == StateCompleted && retained.job.Artifact != nil && retained.job.Artifact.FileName == name {
					continue
				}
			}
			if err := manager.artifactRoot.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	return directory.Sync()
}
