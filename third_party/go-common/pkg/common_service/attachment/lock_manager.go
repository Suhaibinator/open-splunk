package attachment

import (
	"fmt"
	"os"
	"sync"
	"time"
	"uuid"

	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

// AttachmentLockManager provides thread-safe in-memory locking for attachment processing.
// It uses TTL cache to automatically expire locks and prevent stale locks from blocking processing.
// This prevents race conditions when multiple workers attempt to process the same attachment.
type AttachmentLockManager struct {
	cache  *ttlcache.Cache[uint64, string] // attachmentID -> workerID
	logger *zap.Logger
}

// LockMetrics provides statistics about lock operations for monitoring.
type LockMetrics struct {
	Insertions  uint64 // Total lock acquisitions attempted
	Hits        uint64 // Lock status checks that found existing locks
	Misses      uint64 // Lock status checks that found no locks
	Evictions   uint64 // Locks that were automatically expired
	CurrentSize int    // Current number of active locks
}

// NewAttachmentLockManager creates a new lock manager with TTL-based automatic cleanup.
//
// Parameters:
//   - logger: Logger for debugging and monitoring lock operations
//   - lockTTL: Duration after which locks automatically expire (default: 10 minutes)
//   - capacity: Maximum number of concurrent locks (default: 10000)
//
// Returns:
//   - *AttachmentLockManager: Initialized lock manager with background cleanup running.
//
// The lock manager automatically starts background cleanup and provides thread-safe operations.
func NewAttachmentLockManager(logger *zap.Logger, lockTTL time.Duration, capacity uint64) *AttachmentLockManager {
	// Apply defaults if not specified
	if lockTTL == 0 {
		lockTTL = 10 * time.Minute
	}
	if capacity == 0 {
		capacity = 30000
	}
	namedLogger := logger.Named(attachmentLockManagerLoggerName)
	cache := ttlcache.New(
		ttlcache.WithTTL[uint64, string](lockTTL),
		ttlcache.WithCapacity[uint64, string](capacity),
		// Disable touch-on-hit to prevent lock extension on status checks
		ttlcache.WithDisableTouchOnHit[uint64, string](),
	)

	// Start automatic cleanup
	go cache.Start()

	manager := &AttachmentLockManager{
		cache:  cache,
		logger: logger,
	}

	namedLogger.Info("Attachment lock manager initialized",
		appLogger.DurationSeconds(appLogger.KeyLockTTLSeconds, lockTTL),
		zap.Uint64(appLogger.KeyCapacity, capacity))

	return manager
}

// TryLock attempts to acquire a lock for the specified attachment.
func (alm *AttachmentLockManager) TryLock(attachmentID uint64, workerID string) bool {
	// GetOrSet is atomic - returns (item, wasAlreadyPresent)
	item, wasAlreadyPresent := alm.cache.GetOrSet(attachmentID, workerID)

	if !wasAlreadyPresent {
		// We successfully acquired the lock
		if appLogger.DebugEnabled(alm.logger) {
			alm.logger.Named(attachmentLockManagerLoggerName).Debug("Lock acquired",
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyWorkerID, workerID),
				zap.Time(appLogger.KeyExpiresAt, item.ExpiresAt()))
		}
		return true
	}

	// Lock already exists - check if we own it
	lockOwner := item.Value()
	if lockOwner == workerID {
		if appLogger.DebugEnabled(alm.logger) {
			alm.logger.Named(attachmentLockManagerLoggerName).Debug("Lock already owned by this worker",
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyWorkerID, workerID))
		}
		return true
	}

	// Lock is owned by another worker
	if appLogger.DebugEnabled(alm.logger) {
		alm.logger.Named(attachmentLockManagerLoggerName).Debug("Lock conflict - attachment already locked",
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyWorkerID, workerID),
			zap.String(appLogger.KeyWorkerID, lockOwner),
			zap.Time(appLogger.KeyLockExpiresAt, item.ExpiresAt()))
	}
	return false
}

// TryLockWithTTL attempts to acquire a lock with a custom TTL.
func (alm *AttachmentLockManager) TryLockWithTTL(attachmentID uint64, workerID string, ttl time.Duration) bool {
	// GetOrSet with custom TTL - returns (item, wasAlreadyPresent)
	item, wasAlreadyPresent := alm.cache.GetOrSet(attachmentID, workerID, ttlcache.WithTTL[uint64, string](ttl))

	if !wasAlreadyPresent {
		// We successfully acquired the lock
		if appLogger.DebugEnabled(alm.logger) {
			alm.logger.Named(attachmentLockManagerLoggerName).Debug("Lock acquired with custom TTL",
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyWorkerID, workerID),
				appLogger.DurationSeconds(appLogger.KeyLockTTLSeconds, ttl),
				zap.Time(appLogger.KeyExpiresAt, item.ExpiresAt()))
		}
		return true
	}

	// Lock already exists - check if we own it
	lockOwner := item.Value()
	if lockOwner == workerID {
		if appLogger.DebugEnabled(alm.logger) {
			alm.logger.Named(attachmentLockManagerLoggerName).Debug("Lock already owned by this worker (custom TTL)",
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyWorkerID, workerID),
				appLogger.DurationSeconds(appLogger.KeyRequestedTTLSeconds, ttl))
		}
		return true
	}

	// Lock is owned by another worker
	if appLogger.DebugEnabled(alm.logger) {
		alm.logger.Named(attachmentLockManagerLoggerName).Debug("Lock conflict with custom TTL request",
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyWorkerID, workerID),
			zap.String(appLogger.KeyWorkerID, lockOwner),
			appLogger.DurationSeconds(appLogger.KeyRequestedTTLSeconds, ttl))
	}
	return false
}

// Unlock releases a lock if it's owned by the specified worker.
func (alm *AttachmentLockManager) Unlock(attachmentID uint64, workerID string) bool {
	item := alm.cache.Get(attachmentID)
	if item != nil && item.Value() == workerID {
		alm.cache.Delete(attachmentID)
		if appLogger.DebugEnabled(alm.logger) {
			alm.logger.Named(attachmentLockManagerLoggerName).Debug("Lock released",
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyWorkerID, workerID))
		}
		return true
	} else if item != nil {
		alm.logger.Named(attachmentLockManagerLoggerName).Warn("Attempted to unlock attachment owned by different worker",
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyWorkerID, workerID),
			zap.String(appLogger.KeyWorkerID, item.Value()))
		return false
	}

	// Lock doesn't exist (already expired or never created)
	if appLogger.DebugEnabled(alm.logger) {
		alm.logger.Named(attachmentLockManagerLoggerName).Debug("Attempted to unlock non-existent lock",
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyWorkerID, workerID))
	}
	return false
}

// IsLocked checks if an attachment is currently locked by any worker.
//
// Parameters:
//   - attachmentID: The attachment to check
//
// Returns:
//   - bool: true if the attachment is locked, false otherwise
func (alm *AttachmentLockManager) IsLocked(attachmentID uint64) bool {
	return alm.cache.Has(attachmentID)
}

// GetLockOwner returns the worker ID that currently owns the lock for an attachment.
//
// Parameters:
//   - attachmentID: The attachment to check
//
// Returns:
//   - string: The worker ID that owns the lock, or empty string if not locked
//   - time.Time: When the lock expires, zero time if not locked
func (alm *AttachmentLockManager) GetLockOwner(attachmentID uint64) (string, time.Time) {
	item := alm.cache.Get(attachmentID)
	if item != nil {
		return item.Value(), item.ExpiresAt()
	}
	return "", time.Time{}
}

// ExtendLock extends the TTL of an existing lock if owned by the specified worker.
func (alm *AttachmentLockManager) ExtendLock(attachmentID uint64, workerID string, additionalTime time.Duration) bool {
	item := alm.cache.Get(attachmentID)
	if item != nil && item.Value() == workerID {
		// Calculate new TTL based on current expiration
		currentTTL := time.Until(item.ExpiresAt())
		newTTL := currentTTL + additionalTime

		alm.cache.Set(attachmentID, workerID, newTTL)
		if appLogger.DebugEnabled(alm.logger) {
			alm.logger.Named(attachmentLockManagerLoggerName).Debug("Lock extended",
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyWorkerID, workerID),
				appLogger.DurationSeconds(appLogger.KeyAdditionalTimeSeconds, additionalTime),
				appLogger.DurationSeconds(appLogger.KeyNewTTLSeconds, newTTL))
		}
		return true
	}
	return false
}

// GetMetrics returns current cache metrics for monitoring and debugging.
func (alm *AttachmentLockManager) GetMetrics() LockMetrics {
	metrics := alm.cache.Metrics()
	return LockMetrics{
		Insertions:  metrics.Insertions,
		Hits:        metrics.Hits,
		Misses:      metrics.Misses,
		Evictions:   metrics.Evictions,
		CurrentSize: alm.cache.Len(),
	}
}

// LogMetrics logs current cache metrics for monitoring purposes.
// This should be called periodically to track lock usage patterns.
func (alm *AttachmentLockManager) LogMetrics() {
	metrics := alm.GetMetrics()
	alm.logger.Named(attachmentLockManagerLoggerName).Info("Attachment lock manager metrics",
		zap.Uint64(appLogger.KeyInsertions, metrics.Insertions),
		zap.Uint64(appLogger.KeyHits, metrics.Hits),
		zap.Uint64(appLogger.KeyMisses, metrics.Misses),
		zap.Uint64(appLogger.KeyEvictions, metrics.Evictions),
		zap.Int(appLogger.KeyCurrentLocks, metrics.CurrentSize))
}

// GetActiveLocks returns a snapshot of all currently active locks.
func (alm *AttachmentLockManager) GetActiveLocks() map[uint64]string {
	activeLocks := make(map[uint64]string)

	// Iterate through all items in the cache
	for attachmentID, item := range alm.cache.Items() {
		if item != nil {
			activeLocks[attachmentID] = item.Value()
		}
	}

	return activeLocks
}

// ForceClearLock forcefully removes a lock regardless of ownership.
func (alm *AttachmentLockManager) ForceClearLock(attachmentID uint64) {
	owner, expiresAt := alm.GetLockOwner(attachmentID)
	alm.cache.Delete(attachmentID)
	alm.logger.Named(attachmentLockManagerLoggerName).Warn("Lock forcefully cleared",
		zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
		zap.String(appLogger.KeyWorkerID, owner),
		zap.Time(appLogger.KeyWasExpiringAt, expiresAt))
}

// ClearAllLocks removes all active locks.
// This should only be used for administrative purposes or system shutdown.
//
// This operation is logged as a warning since it affects all active processing.
func (alm *AttachmentLockManager) ClearAllLocks() {
	currentCount := alm.cache.Len()
	alm.cache.DeleteAll()
	alm.logger.Named(attachmentLockManagerLoggerName).Warn("All locks cleared",
		zap.Int(appLogger.KeyPreviousCount, currentCount))
}

// Close stops the lock manager and cleans up resources.
func (alm *AttachmentLockManager) Close() error {
	alm.logger.Named(attachmentLockManagerLoggerName).Info("Shutting down attachment lock manager")
	alm.cache.Stop()
	return nil
}

// cachedHostname resolves the process hostname exactly once; the hostname is
// process-constant, so repeated os.Hostname syscalls are unnecessary.
var cachedHostname = sync.OnceValue(func() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
})

// GenerateWorkerID creates a unique identifier for a worker.
func GenerateWorkerID() string {
	return fmt.Sprintf("%s-%d-%s",
		cachedHostname(),
		time.Now().UTC().UnixNano(),
		uuid.New().String()[:8])
}
