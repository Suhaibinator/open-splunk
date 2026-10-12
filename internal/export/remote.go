package export

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func sameAccess(owner, caller searchjobs.AccessScope) bool {
	return owner.TenantID == caller.TenantID && (caller.SiteAdministrator || (owner.OwnerID == caller.OwnerID && (caller.OrganizationID == "" || owner.OrganizationID == "" || caller.OrganizationID == owner.OrganizationID)))
}

func (manager *Manager) runRemote(entry *jobEntry, rowLimit, byteLimit uint64) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		manager.finishFailure(entry, err)
		return
	}
	key := "open-splunk/exports/" + entry.job.ID + "/" + hex.EncodeToString(random[:])
	planned := searchartifacts.BlobIdentity{Key: key}
	if manager.retentionQuota != nil {
		if err := manager.retentionQuota.ReserveExport(entry.ctx, entry.access.OrganizationID, entry.job.ID, byteLimit); err != nil {
			manager.finishFailure(entry, err)
			return
		}
	}
	entry.mu.Lock()
	entry.remoteIdentity = &planned
	entry.artifactPath = key
	persistErr := manager.persistEntryLocked(entry)
	state := entry.job.State
	entry.mu.Unlock()
	if persistErr != nil || state != StateRunning {
		if err := manager.removeTrackedArtifact(entry, key); err != nil {
			manager.reportCleanupError(err)
		}
		return
	}
	reader, writer := io.Pipe()
	type uploadResult struct {
		identity searchartifacts.BlobIdentity
		err      error
	}
	uploaded := make(chan uploadResult, 1)
	go func() {
		identity, err := manager.blobs.Put(entry.ctx, key, reader, -1)
		_ = reader.CloseWithError(err)
		uploaded <- uploadResult{identity, err}
	}()
	hash := sha256.New()
	limited := &exactLimitWriter{output: io.MultiWriter(writer, hash), limit: byteLimit}
	serializer, err := manager.newSerializer(limited, entry)
	var rows uint64
	if err == nil {
		for {
			if err = entry.ctx.Err(); err != nil {
				break
			}
			var row searchjobs.ResultRow
			var present bool
			row, present, err = entry.lease.Next(entry.ctx)
			if err != nil || !present {
				break
			}
			if rows >= rowLimit {
				err = ErrRowLimit
				break
			}
			if err = serializer.WriteRow(row); err != nil {
				break
			}
			rows++
			if rows%128 == 0 {
				manager.updateProgress(entry, rows, limited.written)
			}
		}
		if err == nil {
			err = serializer.Close()
		}
	}
	_ = writer.CloseWithError(err)
	result := <-uploaded
	if err == nil {
		err = result.err
	}
	if err == nil && (result.identity.Key != key || result.identity.Bytes < 0 || uint64(result.identity.Bytes) != limited.written || (result.identity.ETag == "" && result.identity.Version == "")) {
		err = ErrArtifactUnavailable
	}
	if err == nil {
		err = entry.ctx.Err()
	}
	if err == nil && manager.authorize != nil {
		err = manager.authorize(entry.ctx, entry.access, entry.job.SearchJobID)
	}
	if err != nil {
		identity := result.identity
		if identity.Key == "" {
			identity = planned
		}
		entry.mu.Lock()
		entry.remoteIdentity = &identity
		entry.mu.Unlock()
		if errors.Is(err, searchartifacts.ErrConflict) {
			entry.mu.Lock()
			entry.remoteIdentity.Version = "!conflict"
			entry.mu.Unlock()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, searchjobs.ErrResultLeaseClosed) {
			manager.finishCanceled(entry)
		} else {
			manager.finishFailure(entry, err)
		}
		if cleanupErr := manager.removeTrackedArtifact(entry, key); cleanupErr != nil {
			manager.reportCleanupError(cleanupErr)
		}
		return
	}
	if manager.retentionQuota != nil {
		if err := manager.retentionQuota.ReserveExport(entry.ctx, entry.access.OrganizationID, entry.job.ID, limited.written); err != nil {
			entry.mu.Lock()
			entry.remoteIdentity = &result.identity
			entry.mu.Unlock()
			manager.finishFailure(entry, err)
			if cleanupErr := manager.removeTrackedArtifact(entry, key); cleanupErr != nil {
				manager.reportCleanupError(cleanupErr)
			}
			return
		}
	}
	entry.mu.Lock()
	if entry.job.State != StateRunning || entry.ctx.Err() != nil {
		entry.remoteIdentity = &result.identity
		entry.mu.Unlock()
		manager.finishCanceled(entry)
		if cleanupErr := manager.removeTrackedArtifact(entry, key); cleanupErr != nil {
			manager.reportCleanupError(cleanupErr)
		}
		return
	}
	now := manager.nowUTC()
	name, _, _ := CanonicalArtifactMetadata(entry.job.ID, entry.job.Format)
	entry.remoteIdentity = &result.identity
	entry.remoteDigest = hash.Sum(nil)
	entry.artifactPath = key
	entry.job.State = StateCompleted
	entry.job.Version++
	entry.job.FinishedAt = now
	entry.job.ExpiresAt = now.Add(manager.artifactTTL)
	entry.job.Progress = Progress{RowsWritten: rows, BytesWritten: limited.written, UpdatedAt: now}
	entry.job.Artifact = &Artifact{FileName: name, MediaType: mediaType(entry.job.Format), SizeBytes: limited.written, RowCount: rows, ExpiresAt: entry.job.ExpiresAt}
	entry.job.Failure = nil
	manager.reconcileEntryBytesLocked(entry, limited.written)
	persistErr = manager.persistEntryLocked(entry)
	entry.mu.Unlock()
	if persistErr != nil {
		if cleanupErr := manager.removeTrackedArtifact(entry, key); cleanupErr != nil {
			manager.reportCleanupError(cleanupErr)
		}
	}
}

// AcquireDownloadFor streams one current-user-authorized artifact. Browser
// sessions use this route without minting a reusable storage or bearer URL.
func (manager *Manager) AcquireDownloadFor(ctx context.Context, access searchjobs.AccessScope, id string) (*DownloadLease, error) {
	job, err := manager.Get(ctx, access, id)
	if err != nil {
		return nil, err
	}
	if manager.authorize != nil {
		if err := manager.authorize(ctx, access, job.SearchJobID); err != nil {
			return nil, err
		}
	}
	manager.mu.Lock()
	if manager.closed {
		manager.mu.Unlock()
		return nil, ErrClosed
	}
	entry := manager.jobs[id]
	if entry == nil {
		manager.mu.Unlock()
		return nil, ErrNotFound
	}
	entry.mu.Lock()
	if !sameAccess(entry.access, access) {
		entry.mu.Unlock()
		manager.mu.Unlock()
		return nil, ErrNotFound
	}
	if err := downloadableStateErrorLocked(entry, manager.nowUTC()); err != nil {
		entry.mu.Unlock()
		manager.mu.Unlock()
		return nil, err
	}
	if manager.activeDownloads >= manager.maxActiveDownloads || manager.downloadsByJob[id] >= manager.maxActivePerJob {
		entry.mu.Unlock()
		manager.mu.Unlock()
		return nil, ErrDownloadGrantCapacity
	}
	readCtx, readCancel := context.WithCancel(ctx)
	stopManager := context.AfterFunc(manager.ctx, readCancel)
	lease := &DownloadLease{cancel: readCancel, stop: stopManager, artifact: *entry.job.Artifact, manager: manager, entry: entry, jobID: id}
	if entry.downloads == nil {
		entry.downloads = make(map[*DownloadLease]struct{})
	}
	entry.downloads[lease] = struct{}{}
	manager.activeDownloads++
	manager.downloadsByJob[id]++
	identity := entry.remoteIdentity
	artifactPath := entry.artifactPath
	fileIdentity := entry.artifactIdentity
	manager.admissions.Add(1)
	entry.mu.Unlock()
	manager.mu.Unlock()
	defer manager.admissions.Done()
	var reader io.ReadCloser
	if identity != nil {
		reader, err = manager.blobs.Open(readCtx, *identity)
	} else {
		reader, err = manager.openArtifact(lease.artifact, artifactPath, fileIdentity)
	}
	if err != nil {
		_ = lease.Close()
		return nil, ErrArtifactUnavailable
	}
	if manager.authorize != nil {
		reader = &authorizedDownloadReader{ctx: readCtx, source: reader, check: func() error { return manager.authorize(ctx, access, job.SearchJobID) }}
	}
	lease.mu.Lock()
	if lease.closed || readCtx.Err() != nil {
		lease.mu.Unlock()
		_ = reader.Close()
		_ = lease.Close()
		return nil, ErrClosed
	}
	lease.file = reader
	lease.mu.Unlock()
	if err := ctx.Err(); err != nil {
		_ = lease.Close()
		return nil, err
	}
	return lease, nil
}

type authorizedDownloadReader struct {
	ctx             context.Context
	source          io.ReadCloser
	check           func() error
	lastCheck       time.Time
	bytesSinceCheck uint64
}

func (reader *authorizedDownloadReader) Read(payload []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	if reader.lastCheck.IsZero() || reader.bytesSinceCheck >= 1<<20 || time.Since(reader.lastCheck) >= time.Second {
		if err := reader.check(); err != nil {
			return 0, err
		}
		reader.lastCheck = time.Now()
		reader.bytesSinceCheck = 0
	}
	count, err := reader.source.Read(payload)
	if count < 0 || count > len(payload) {
		return 0, ErrArtifactUnavailable
	}
	reader.bytesSinceCheck += safecast.MustConv[uint64](count)
	return count, err
}
func (reader *authorizedDownloadReader) Close() error { return reader.source.Close() }

func remoteProjection(identity *searchartifacts.BlobIdentity) (string, error) {
	if identity == nil {
		return "", nil
	}
	payload, err := json.Marshal(identity)
	return string(payload), err
}
func parseRemoteProjection(encoded string) (searchartifacts.BlobIdentity, bool) {
	var identity searchartifacts.BlobIdentity
	if len(encoded) > 4096 || json.Unmarshal([]byte(encoded), &identity) != nil || !strings.HasPrefix(identity.Key, "open-splunk/exports/") {
		return identity, false
	}
	return identity, true
}
