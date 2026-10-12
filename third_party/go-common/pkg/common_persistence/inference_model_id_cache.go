package common_persistence

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// ledgerInferenceModelIDCacheTTL bounds how long a resolved (or unresolved) catalog id is cached. A
// modest window keeps the inference-ledger write path off the database in steady state while letting
// a newly catalogued model start stamping within a few minutes without a process restart.
const ledgerInferenceModelIDCacheTTL = 5 * time.Minute

// inferenceModelLookup resolves a (provider, model id) pair to its catalog row. It mirrors
// GetInferenceModelByProviderModelID so the cache is unit-testable without a live database.
type inferenceModelLookup func(ctx context.Context, provider string, providerModelID string) (*common_persistence_models.InferenceModel, error)

type ledgerInferenceModelIDCacheKey struct {
	provider        string
	providerModelID string
}

// ledgerInferenceModelIDCacheEntry stores the resolved catalog id (id/found, not a shared pointer, so
// callers never alias cached state) with its expiry.
type ledgerInferenceModelIDCacheEntry struct {
	id       uint64
	found    bool // false = negative cache: no catalog row for this key
	expireAt time.Time
}

// ledgerInferenceModelIDCache is a small in-process (provider, model id) -> catalog row id cache for
// best-effort inference-ledger stamping. It caches negatives too -- both an uncatalogued model AND a
// lookup error are cached identically -- so the ledger write path neither re-queries a known-absent
// model nor hammers a failing database, and it expires entries after a modest TTL so a newly
// catalogued model (or a recovered database) eventually stamps. The zero value is not ready; use
// newLedgerInferenceModelIDCache.
type ledgerInferenceModelIDCache struct {
	mu      sync.Mutex
	now     func() time.Time // injectable clock for tests
	ttl     time.Duration
	entries map[ledgerInferenceModelIDCacheKey]ledgerInferenceModelIDCacheEntry
}

func newLedgerInferenceModelIDCache() *ledgerInferenceModelIDCache {
	return &ledgerInferenceModelIDCache{
		now:     time.Now,
		ttl:     ledgerInferenceModelIDCacheTTL,
		entries: make(map[ledgerInferenceModelIDCacheKey]ledgerInferenceModelIDCacheEntry),
	}
}

// resolve returns the catalog row id for (provider, model id), best effort: a lookup miss OR error
// both yield a nil id and are negatively cached identically for the full TTL. Caching lookup errors
// is deliberate -- the ledger path must never hammer a failing database -- at the cost of not
// stamping for up to one TTL after the database recovers. It never returns an error and only touches
// the database on a cold or expired key.
func (c *ledgerInferenceModelIDCache) resolve(ctx context.Context, provider string, providerModelID string, lookup inferenceModelLookup) *uint64 {
	id, _ := c.resolveWithLookupStatus(ctx, provider, providerModelID, lookup)
	return id
}

// resolveWithLookupStatus is resolve plus a one-shot cold-lookup status. Cache
// hits never replay the prior error, allowing the handler to report a swallowed
// lookup failure once per key/TTL instead of on every inference call.
func (c *ledgerInferenceModelIDCache) resolveWithLookupStatus(ctx context.Context, provider string, providerModelID string, lookup inferenceModelLookup) (*uint64, error) {
	if c == nil || lookup == nil {
		return nil, nil
	}
	key := ledgerInferenceModelIDCacheKey{provider: provider, providerModelID: providerModelID}
	now := c.now()

	c.mu.Lock()
	if entry, ok := c.entries[key]; ok && now.Before(entry.expireAt) {
		c.mu.Unlock()
		return entryID(entry), nil
	}
	c.mu.Unlock()

	entry := ledgerInferenceModelIDCacheEntry{expireAt: now.Add(c.ttl)}
	model, lookupErr := lookup(ctx, provider, providerModelID)
	if lookupErr == nil && model != nil && model.ID != 0 {
		entry.id = model.ID
		entry.found = true
	}

	c.mu.Lock()
	c.entries[key] = entry
	c.mu.Unlock()
	return entryID(entry), lookupErr
}

// entryID returns a fresh pointer to the cached id, or nil for a negative entry, so callers never
// share a mutable pointer with the cache.
func entryID(entry ledgerInferenceModelIDCacheEntry) *uint64 {
	if !entry.found {
		return nil
	}
	id := entry.id
	return &id
}

// ResolveLedgerInferenceModelID resolves the inference_models catalog row id for a (provider, model
// id) pair for best-effort ledger stamping (ChatInferenceCall.InferenceModelID). It is best effort:
// an uncatalogued model or a lookup error yields nil, never an error, and results are cached
// (positives and negatives) so the hot ledger write path does not add a database read per call in
// steady state. Wire it at insert sites through StampLedgerInferenceModelID.
func (h *CommonPersistenceHandler[U]) ResolveLedgerInferenceModelID(ctx context.Context, provider string, providerModelID string) *uint64 {
	if h == nil || h.ledgerInferenceModelIDCache == nil {
		return nil
	}
	id, err := h.ledgerInferenceModelIDCache.resolveWithLookupStatus(ctx, provider, providerModelID, h.GetInferenceModelByProviderModelID)
	if err == nil || h.logger == nil {
		return id
	}

	log := appLogger.For(ctx, h.logger, loggerName)
	fields := []zap.Field{
		zap.String(logkeys.Stage, "resolve_ledger_inference_model_id"),
		zap.String(appLogger.KeyProvider, provider),
		zap.String(appLogger.KeyProviderModelID, providerModelID),
		appLogger.DurationSeconds(appLogger.KeyCacheTTLSeconds, h.ledgerInferenceModelIDCache.ttl),
	}
	if errors.Is(err, ErrInferenceModelNotFound) {
		// An uncatalogued provider model is valid best-effort behavior and leaves
		// the optional ledger foreign key NULL.
		if ce := log.Check(zap.DebugLevel, "Inference model is not catalogued; omitting ledger model ID"); ce != nil {
			ce.Write(append(fields, zap.String(appLogger.KeyOutcome, "uncatalogued_model"))...)
		}
		return id
	}

	// This error is intentionally swallowed so inference can continue. WARN is
	// appropriate because ledger attribution is degraded until the negative
	// cache expires and there is no caller-visible error to report it.
	fields = append(fields,
		zap.String(logkeys.Fallback, "omit_ledger_inference_model_id_until_cache_expiry"),
		zap.Error(err))
	log.Warn("Inference model catalog lookup failed; temporarily omitting ledger model ID", fields...)
	return id
}

// LedgerInferenceModelResolver is implemented by persistence backends that can resolve a
// (provider, model id) pair to the configured inference_models catalog row id. It is intentionally
// NOT part of CommonPersistenceInterface: a backend or test double that cannot resolve simply leaves
// ChatInferenceCall.InferenceModelID NULL (the best-effort contract) without every implementer having
// to provide the method.
type LedgerInferenceModelResolver interface {
	ResolveLedgerInferenceModelID(ctx context.Context, provider string, providerModelID string) *uint64
}

// StampLedgerInferenceModelID is the shared best-effort resolver the ChatInferenceCall insert sites
// use to fill InferenceModelID. It returns nil (leaving the column NULL) when the backend does not
// support resolution or when no catalog row matches, and never errors or blocks the ledger write
// beyond the lookup itself.
func StampLedgerInferenceModelID(ctx context.Context, persistence CommonPersistenceInterface, provider string, providerModelID string) *uint64 {
	resolver, ok := persistence.(LedgerInferenceModelResolver)
	if !ok {
		return nil
	}
	return resolver.ResolveLedgerInferenceModelID(ctx, provider, providerModelID)
}
