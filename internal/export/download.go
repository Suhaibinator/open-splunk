package export

import (
	"container/heap"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	downloadTokenBytes = 32
	// Include the map key, value, job-ID backing bytes, and a conservative map
	// and allocator allowance. This is charged to MaxTotalMetadataBytes for the
	// full lifetime of every outstanding grant.
	downloadGrantMetadataFixed = uint64(unsafe.Sizeof([sha256.Size]byte{})) +
		uint64(unsafe.Sizeof(downloadGrantEntry{})) +
		uint64(unsafe.Sizeof(DownloadLease{})) + 128
)

type downloadGrantEntry struct {
	digest        [sha256.Size]byte
	jobID         string
	jobVersion    uint64
	expiresAt     time.Time
	metadataBytes uint64
	heapIndex     int
}

type downloadGrantExpiryHeap []*downloadGrantEntry

func (items downloadGrantExpiryHeap) Len() int { return len(items) }

func (items downloadGrantExpiryHeap) Less(left, right int) bool {
	return items[left].expiresAt.Before(items[right].expiresAt)
}

func (items downloadGrantExpiryHeap) Swap(left, right int) {
	items[left], items[right] = items[right], items[left]
	items[left].heapIndex = left
	items[right].heapIndex = right
}

func (items *downloadGrantExpiryHeap) Push(value any) {
	grant := value.(*downloadGrantEntry)
	grant.heapIndex = len(*items)
	*items = append(*items, grant)
}

func (items *downloadGrantExpiryHeap) Pop() any {
	old := *items
	last := len(old) - 1
	grant := old[last]
	old[last] = nil
	grant.heapIndex = -1
	*items = old[:last]
	return grant
}

var invalidDownloadTokenDigest = sha256.Sum256(nil)

// CreateDownloadGrant mints a scoped, short-lived, one-time bearer grant for
// a completed artifact. Only the token's SHA-256 digest is retained. The
// returned expiration never exceeds the artifact expiration.
func (manager *Manager) CreateDownloadGrant(ctx context.Context, access searchjobs.AccessScope, id string) (DownloadGrant, error) {
	if ctx == nil {
		return DownloadGrant{}, errors.New("create export download grant: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return DownloadGrant{}, err
	}
	if err := validateAccessScope(access); err != nil {
		return DownloadGrant{}, err
	}
	if !validID(id) {
		return DownloadGrant{}, ErrNotFound
	}

	for range maximumIDAttempts {
		token, digest, err := newDownloadToken()
		if err != nil {
			return DownloadGrant{}, ErrArtifactUnavailable
		}
		manager.mu.Lock()
		now := manager.nowUTC()
		if manager.closed {
			manager.mu.Unlock()
			return DownloadGrant{}, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			manager.mu.Unlock()
			return DownloadGrant{}, err
		}
		manager.purgeExpiredGrantsLocked(now)
		entry := manager.jobs[id]
		if entry == nil {
			manager.mu.Unlock()
			return DownloadGrant{}, ErrNotFound
		}
		entry.mu.Lock()
		now = manager.nowUTC()
		if err := ctx.Err(); err != nil {
			entry.mu.Unlock()
			manager.mu.Unlock()
			return DownloadGrant{}, err
		}
		manager.purgeExpiredGrantsLocked(now)
		if entry.access != access {
			entry.mu.Unlock()
			manager.mu.Unlock()
			return DownloadGrant{}, ErrNotFound
		}
		if err := downloadableStateErrorLocked(entry, now); err != nil {
			entry.mu.Unlock()
			manager.mu.Unlock()
			return DownloadGrant{}, err
		}
		canonicalID := entry.job.ID
		if _, collision := manager.grants[digest]; collision {
			entry.mu.Unlock()
			manager.mu.Unlock()
			continue
		}
		if len(manager.grants) >= manager.maxDownloadGrants || manager.grantsByJob[canonicalID] >= manager.maxGrantsPerJob {
			entry.mu.Unlock()
			manager.mu.Unlock()
			return DownloadGrant{}, ErrDownloadGrantCapacity
		}
		metadataBytes := downloadGrantMetadataFixed + uint64(len(canonicalID))
		manager.budgetMu.Lock()
		metadataFull := manager.totalMetadata > manager.maxTotalMetadata ||
			metadataBytes > manager.maxTotalMetadata-manager.totalMetadata
		if metadataFull {
			manager.budgetMu.Unlock()
			entry.mu.Unlock()
			manager.mu.Unlock()
			return DownloadGrant{}, ErrDownloadGrantCapacity
		}
		manager.totalMetadata += metadataBytes
		manager.budgetMu.Unlock()

		expiresAt := now.Add(manager.downloadGrantTTL)
		if entry.job.Artifact.ExpiresAt.Before(expiresAt) {
			expiresAt = entry.job.Artifact.ExpiresAt
		}
		storedGrant := &downloadGrantEntry{
			digest:        digest,
			jobID:         canonicalID,
			jobVersion:    entry.job.Version,
			expiresAt:     expiresAt,
			metadataBytes: metadataBytes,
		}
		manager.grants[digest] = storedGrant
		heap.Push(&manager.grantExpirations, storedGrant)
		if len(manager.grants) > manager.grantMapHighWater {
			manager.grantMapHighWater = len(manager.grants)
		}
		manager.grantsByJob[canonicalID]++
		entry.mu.Unlock()
		manager.mu.Unlock()
		return DownloadGrant{Token: token, ExpiresAt: expiresAt}, nil
	}
	return DownloadGrant{}, ErrDownloadGrantCapacity
}

func downloadableStateErrorLocked(entry *jobEntry, now time.Time) error {
	switch entry.job.State {
	case StateQueued, StateRunning:
		return ErrSourceNotReady
	case StateFailed, StateCanceled:
		return ErrSourceUnavailable
	case StateExpired:
		return ErrSourceExpired
	case StateCompleted:
		if entry.job.Artifact == nil || entry.artifactPath == "" || entry.artifactIdentity == nil {
			return ErrArtifactUnavailable
		}
		if !entry.job.Artifact.ExpiresAt.After(now) {
			return ErrSourceExpired
		}
		return nil
	default:
		return ErrSourceUnavailable
	}
}

func newDownloadToken() (string, [sha256.Size]byte, error) {
	var entropy [downloadTokenBytes]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", [sha256.Size]byte{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(entropy[:])
	return token, sha256.Sum256([]byte(token)), nil
}

// RedeemDownload atomically consumes a one-time bearer grant and returns a
// pinned artifact reader. Malformed, unknown, expired, replayed, and revoked
// grants all return ErrInvalidDownloadGrant. A successfully returned lease
// remains readable after artifact TTL expiration until the lease or manager is
// closed.
func (manager *Manager) RedeemDownload(ctx context.Context, token string) (ArtifactDownload, error) {
	if ctx == nil {
		return nil, errors.New("redeem export download: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	digest, validSyntax := downloadTokenDigest(token)

	manager.mu.Lock()
	now := manager.nowUTC()
	if manager.closed {
		manager.mu.Unlock()
		return nil, ErrInvalidDownloadGrant
	}
	if err := ctx.Err(); err != nil {
		manager.mu.Unlock()
		return nil, err
	}
	grant, exists := manager.grants[digest]
	if !validSyntax || !exists || !grant.expiresAt.After(now) {
		if exists {
			manager.removeGrantLocked(digest, grant)
		}
		manager.mu.Unlock()
		return nil, ErrInvalidDownloadGrant
	}
	entry := manager.jobs[grant.jobID]
	if entry == nil {
		manager.removeGrantLocked(digest, grant)
		manager.mu.Unlock()
		return nil, ErrInvalidDownloadGrant
	}
	entry.mu.Lock()
	now = manager.nowUTC()
	if err := ctx.Err(); err != nil {
		entry.mu.Unlock()
		manager.mu.Unlock()
		return nil, err
	}
	if !grant.expiresAt.After(now) {
		entry.mu.Unlock()
		manager.removeGrantLocked(digest, grant)
		manager.mu.Unlock()
		return nil, ErrInvalidDownloadGrant
	}
	if entry.job.Version != grant.jobVersion || entry.job.State != StateCompleted ||
		entry.job.Artifact == nil || !entry.job.Artifact.ExpiresAt.After(now) ||
		entry.artifactPath == "" || entry.artifactIdentity == nil {
		entry.mu.Unlock()
		manager.removeGrantLocked(digest, grant)
		manager.mu.Unlock()
		return nil, ErrInvalidDownloadGrant
	}
	if manager.activeDownloads >= manager.maxActiveDownloads || manager.downloadsByJob[grant.jobID] >= manager.maxActivePerJob {
		entry.mu.Unlock()
		manager.mu.Unlock()
		return nil, ErrDownloadGrantCapacity
	}
	manager.detachGrantLocked(digest, grant)
	artifact := *entry.job.Artifact
	artifactPath := entry.artifactPath
	identity := entry.artifactIdentity
	lease := &DownloadLease{
		artifact:      artifact,
		manager:       manager,
		entry:         entry,
		jobID:         grant.jobID,
		metadataBytes: grant.metadataBytes,
	}
	if entry.downloads == nil {
		entry.downloads = make(map[*DownloadLease]struct{})
	}
	entry.downloads[lease] = struct{}{}
	manager.activeDownloads++
	manager.downloadsByJob[grant.jobID]++
	// Close takes manager.mu before waiting, so Add under this lock cannot race
	// its Wait. The admission covers secure open and lease publication.
	manager.admissions.Add(1)
	entry.mu.Unlock()
	manager.mu.Unlock()
	defer manager.admissions.Done()

	file, err := manager.openArtifact(artifact, artifactPath, identity)
	if err != nil {
		_ = lease.Close()
		return nil, ErrArtifactUnavailable
	}
	lease.mu.Lock()
	lease.file = file
	lease.mu.Unlock()
	if err := ctx.Err(); err != nil {
		_ = lease.Close()
		return nil, err
	}
	return lease, nil
}

func downloadTokenDigest(token string) ([sha256.Size]byte, bool) {
	if len(token) != base64.RawURLEncoding.EncodedLen(downloadTokenBytes) {
		return invalidDownloadTokenDigest, false
	}
	var decoded [downloadTokenBytes]byte
	n, err := base64.RawURLEncoding.Decode(decoded[:], []byte(token))
	if err != nil || n != len(decoded) {
		return invalidDownloadTokenDigest, false
	}
	return sha256.Sum256([]byte(token)), true
}

func (manager *Manager) openArtifact(artifact Artifact, artifactPath string, identity os.FileInfo) (*os.File, error) {
	name := artifact.FileName
	if name == "" || name == "." || filepath.Base(name) != name || filepath.Base(artifactPath) != name {
		return nil, ErrArtifactUnavailable
	}
	pathInfo, err := manager.artifactRoot.Lstat(name)
	if err != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() ||
		pathInfo.Size() < 0 || uint64(pathInfo.Size()) != artifact.SizeBytes || !os.SameFile(identity, pathInfo) {
		return nil, ErrArtifactUnavailable
	}
	file, err := manager.artifactRoot.Open(name)
	if err != nil {
		return nil, ErrArtifactUnavailable
	}
	openedInfo, statErr := file.Stat()
	if statErr != nil || !openedInfo.Mode().IsRegular() || openedInfo.Size() < 0 ||
		uint64(openedInfo.Size()) != artifact.SizeBytes || !os.SameFile(pathInfo, openedInfo) || !os.SameFile(identity, openedInfo) {
		_ = file.Close()
		return nil, ErrArtifactUnavailable
	}
	return file, nil
}

func (manager *Manager) purgeExpiredGrantsLocked(now time.Time) {
	for len(manager.grantExpirations) != 0 && !manager.grantExpirations[0].expiresAt.After(now) {
		grant := manager.grantExpirations[0]
		manager.removeGrantLocked(grant.digest, grant)
	}
}

func (manager *Manager) revokeJobGrantsLocked(jobID string) {
	if manager.grantsByJob[jobID] == 0 {
		return
	}
	for digest, grant := range manager.grants {
		if grant.jobID == jobID {
			manager.removeGrantLocked(digest, grant)
		}
	}
}

func (manager *Manager) revokeAllGrantsLocked() {
	for digest, grant := range manager.grants {
		manager.removeGrantLocked(digest, grant)
	}
}

func (manager *Manager) removeGrantLocked(digest [sha256.Size]byte, grant *downloadGrantEntry) {
	manager.detachGrantLocked(digest, grant)
	manager.releaseMetadata(grant.metadataBytes)
}

func (manager *Manager) detachGrantLocked(digest [sha256.Size]byte, grant *downloadGrantEntry) {
	delete(manager.grants, digest)
	if grant.heapIndex >= 0 {
		heap.Remove(&manager.grantExpirations, grant.heapIndex)
	}
	if count := manager.grantsByJob[grant.jobID]; count <= 1 {
		delete(manager.grantsByJob, grant.jobID)
	} else {
		manager.grantsByJob[grant.jobID] = count - 1
	}
	manager.compactGrantStorageLocked()
}

func (manager *Manager) compactGrantStorageLocked() {
	live := len(manager.grants)
	if live == 0 {
		manager.grants = make(map[[sha256.Size]byte]*downloadGrantEntry)
		manager.grantsByJob = make(map[string]int)
		manager.grantExpirations = nil
		manager.grantMapHighWater = 0
		return
	}
	if manager.grantMapHighWater <= 64 || live*4 >= manager.grantMapHighWater {
		return
	}
	compacted := make(map[[sha256.Size]byte]*downloadGrantEntry, live)
	counts := make(map[string]int)
	for digest, retained := range manager.grants {
		compacted[digest] = retained
		counts[retained.jobID]++
	}
	manager.grants = compacted
	manager.grantsByJob = counts
	manager.grantMapHighWater = live
	retainedHeap := make(downloadGrantExpiryHeap, live)
	copy(retainedHeap, manager.grantExpirations)
	manager.grantExpirations = retainedHeap
	for index, retained := range manager.grantExpirations {
		retained.heapIndex = index
	}
}

// Read implements io.Reader without exposing path-bearing filesystem errors.
func (lease *DownloadLease) Read(payload []byte) (int, error) {
	if lease == nil {
		return 0, ErrArtifactUnavailable
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.file == nil {
		return 0, ErrArtifactUnavailable
	}
	read, err := lease.file.Read(payload)
	if err != nil && !errors.Is(err, io.EOF) {
		return read, ErrArtifactUnavailable
	}
	return read, err
}

// Close releases the read pin. If the artifact expired while pinned, the final
// lease to close performs the deferred unlink. Close is idempotent.
func (lease *DownloadLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.closeOnce.Do(func() {
		lease.mu.Lock()
		file := lease.file
		lease.file = nil
		var fileErr error
		if file != nil {
			fileErr = file.Close()
		}
		lease.mu.Unlock()

		entry := lease.entry
		manager := lease.manager
		manager.mu.Lock()
		entry.mu.Lock()
		delete(entry.downloads, lease)
		if len(entry.downloads) == 0 {
			entry.downloads = nil
		}
		if manager.activeDownloads > 0 {
			manager.activeDownloads--
		}
		if count := manager.downloadsByJob[lease.jobID]; count <= 1 {
			delete(manager.downloadsByJob, lease.jobID)
		} else {
			manager.downloadsByJob[lease.jobID] = count - 1
		}
		if manager.activeDownloads == 0 {
			manager.downloadsByJob = make(map[string]int)
		}
		manager.releaseMetadata(lease.metadataBytes)
		artifactPath, tempPath := manager.expireLocked(entry, manager.nowUTC())
		entry.mu.Unlock()
		manager.mu.Unlock()
		artifactErr := manager.removeTrackedArtifact(entry, artifactPath)
		tempErr := manager.removeTrackedTemp(entry, tempPath)
		if fileErr != nil || artifactErr != nil || tempErr != nil {
			lease.closeErr = ErrArtifactUnavailable
		}
		lease.manager = nil
		lease.entry = nil
		lease.jobID = ""
		lease.metadataBytes = 0
	})
	return lease.closeErr
}
