package organization

import (
	"context"
	stdErrors "errors"
	"sync"
	"sync/atomic"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/jellydator/ttlcache/v3"
)

// OrgRoleProvider provides organization role lookups using persistence.
//
// Cache strategy:
//   - GetUserRole owns cache population, observing the generation before its
//     persistence read so concurrent invalidation can reject stale fills.
//   - Membership mutations (add, remove, role change, invitation acceptance) call
//     OrganizationHandler.InvalidateRoleCacheForDescendants, which evicts that one
//     user's entries for the organization and its descendants in a single generation
//     bump after the transaction commits. Only organization deletion, and the fallback
//     when descendant resolution fails, clear the whole cache via InvalidateAll.
//   - A miss falls back to persistence and repopulates the cache; when cache is disabled
//     via config the provider simply proxies to persistence without caching.
type OrgRoleProvider struct {
	persistenceHandler persistence.CommonPersistenceInterface
	roleCache          *generationCache[orgRoleCacheKey, OrgRoleInfo]
	cacheGenerationGuard
	// txDetector reports whether the request carries a usable ambient database
	// transaction. It is injected at construction so the provider itself does
	// not need the handler's user type parameter.
	txDetector     func(context.Context) bool
	cacheHits      atomic.Int64
	cacheMisses    atomic.Int64
	cacheEvictions atomic.Int64
	runtime        func() config.OrganizationsView

	// stopCh ends the expiration-sweeper goroutine when Stop is called;
	// stopOnce keeps Stop idempotent.
	stopCh   chan struct{}
	workerWG sync.WaitGroup
	stopOnce sync.Once
}

// orgRoleCacheKey identifies a cached role entry by user and organization IDs in OrgRoleProvider's in-memory TTL cache.
type orgRoleCacheKey struct {
	userID common_service_models.UserIdType
	orgID  common_service_models.OrganizationIdType
}

// OrgRoleInfo carries explicit and effective role resolution results returned by OrgRoleProvider in membership and permission flows.
type OrgRoleInfo struct {
	ExplicitRole                proto_models.OrganizationRole
	EffectiveRole               proto_models.OrganizationRole
	InheritedFromOrganizationID *common_service_models.OrganizationIdType
	EffectiveRoleDepth          *uint32
}

// NewOrgRoleProvider installs one live organization source. It rejects a nil
// source and a source whose startup view is nil or the zero RootView.
func NewOrgRoleProvider(
	persistenceHandler persistence.CommonPersistenceInterface,
	source func() config.OrganizationsView,
) (*OrgRoleProvider, error) {
	if source == nil {
		return nil, stdErrors.New("organization configuration provider is required")
	}
	organizations := source()
	if organizations == nil || organizations == config.OrganizationsView(config.RootView{}) {
		return nil, stdErrors.New("organization configuration view is required")
	}
	cacheTTL := organizations.OrganizationRoleCacheTTL()
	cacheCapacity := organizations.OrganizationRoleCacheCapacity()
	// MULTI-INSTANCE: organization role caches are process-local because the current
	// architecture supports only a single service instance. Before supporting multiple
	// application instances, propagate membership and role invalidation to every replica.
	roleCache := ttlcache.New(
		ttlcache.WithTTL[orgRoleCacheKey, OrgRoleInfo](cacheTTL),
		ttlcache.WithCapacity[orgRoleCacheKey, OrgRoleInfo](cacheCapacity),
	)

	provider := &OrgRoleProvider{
		persistenceHandler: persistenceHandler,
		txDetector:         hasAmbientTransaction,
		runtime:            source,
	}

	provider.roleCache = newGenerationCache(&provider.cacheGenerationGuard, roleCache)
	provider.roleCache.applied = orgCachePolicy{cacheCapacity, cacheTTL, organizations.OrgRoleInheritanceEnabled()}
	// The expiration sweeper is owned here instead of via ttlcache's
	// go Start()/Stop() pair: ttlcache.Stop is a no-op until the freshly
	// launched worker flips its internal state, so a constructor that
	// fails immediately after launching it would leak the goroutine.
	// Reads already reject expired entries; the sweeper only reclaims
	// memory, so it is skipped entirely for a non-expiring cache.
	provider.stopCh = make(chan struct{})
	if cacheTTL > 0 && cacheCapacity > 0 {
		provider.workerWG.Go(func() {
			ticker := time.NewTicker(cacheTTL)
			defer ticker.Stop()
			for {
				select {
				case <-provider.stopCh:
					return
				case <-ticker.C:
					provider.roleCache.cache.Load().DeleteExpired()
				}
			}
		})
	}
	provider.roleCache.configure = func(cache *ttlcache.Cache[orgRoleCacheKey, OrgRoleInfo]) {
		cache.OnEviction(func(context.Context, ttlcache.EvictionReason, *ttlcache.Item[orgRoleCacheKey, OrgRoleInfo]) {
			provider.cacheEvictions.Add(1)
		})
	}
	provider.roleCache.configure(roleCache)
	provider.roleCache.policy = func() orgCachePolicy {
		v := provider.runtime()
		return orgCachePolicy{v.OrganizationRoleCacheCapacity(), v.OrganizationRoleCacheTTL(), v.OrgRoleInheritanceEnabled()}
	}
	return provider, nil
}

// inAmbientTransaction reports whether the request context carries a usable
// database transaction; transactional reads must observe their transaction's
// snapshot and must never read from or populate process-wide caches.
func (p *OrgRoleProvider) inAmbientTransaction(ctx context.Context) bool {
	return p.txDetector != nil && p.txDetector(ctx)
}

// GetUserRole returns the user's resolved role information and membership flag for an organization, using cache when enabled.
//
// Parameters:
//   - ctx: Request context used for persistence reads.
//   - userID: User whose role should be resolved.
//   - orgID: Organization where membership is evaluated.
//
// Returns:
//   - OrgRoleInfo: Explicit and effective role information for the user.
//   - bool: `true` when the user is considered a member of the organization.
//   - error: Persistence or resolution error.
func (p *OrgRoleProvider) GetUserRole(
	ctx context.Context,
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) (OrgRoleInfo, bool, error) {
	if userID == 0 || orgID == 0 {
		return OrgRoleInfo{EffectiveRole: proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED}, false, nil
	}

	cacheable := !p.inAmbientTransaction(ctx)
	var generation uint64
	var inheritance bool
	if cacheable {
		cached, found, observed, policy := p.roleCache.lookupWithPolicy(orgRoleCacheKey{userID: userID, orgID: orgID})
		inheritance = policy.inheritance
		if found {
			p.cacheHits.Add(1)
			return cached, cached.EffectiveRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, nil
		}
		generation = observed
		p.cacheMisses.Add(1)
	} else {
		inheritance = p.runtime().OrgRoleInheritanceEnabled()
	}

	info, err := p.resolveRoleInfo(ctx, userID, orgID, inheritance)
	if err != nil {
		return OrgRoleInfo{EffectiveRole: proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED}, false, err
	}

	if cacheable {
		p.roleCache.FillIfCurrent(orgRoleCacheKey{userID: userID, orgID: orgID}, info, generation)
	}

	return info, info.EffectiveRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, nil
}

// CacheEnabled reports whether OrgRoleProvider has an active in-memory role cache configured.
//
// Returns:
//   - bool: `true` when role caching is enabled.
func (p *OrgRoleProvider) CacheEnabled() bool {
	return p.runtime().OrganizationRoleCacheCapacity() > 0
}

// CacheCapacity returns the configured maximum number of cached role entries.
//
// Returns:
//   - uint64: Cache capacity value from configuration.
func (p *OrgRoleProvider) CacheCapacity() uint64 {
	return p.runtime().OrganizationRoleCacheCapacity()
}

// CacheStats returns current cache size and cumulative hit/miss/eviction counters for monitoring organization role lookup performance.
//
// Returns:
//   - int: Current number of cached entries.
//   - int64: Total cache hits.
//   - int64: Total cache misses.
//   - int64: Total cache evictions.
func (p *OrgRoleProvider) CacheStats() (int, int64, int64, int64) {
	return p.roleCache.Len(),
		p.cacheHits.Load(),
		p.cacheMisses.Load(),
		p.cacheEvictions.Load()
}

// Invalidate removes one cached role entry and is called by mutating organization operations to prevent stale permission reads.
//
// Parameters:
//   - userID: User key component for the cache entry.
//   - orgID: Organization key component for the cache entry.
func (p *OrgRoleProvider) Invalidate(
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) {
	p.invalidateMany(userID, []common_service_models.OrganizationIdType{orgID})
}

// invalidateMany removes one user's role entries for a tree with a single
// generation change, so a lifecycle hook cannot interleave partial eviction.
func (p *OrgRoleProvider) invalidateMany(
	userID common_service_models.UserIdType,
	orgIDs []common_service_models.OrganizationIdType,
) {
	if len(orgIDs) == 0 {
		return
	}
	keys := make([]orgRoleCacheKey, 0, len(orgIDs))
	for _, orgID := range orgIDs {
		keys = append(keys, orgRoleCacheKey{userID: userID, orgID: orgID})
	}
	p.roleCache.Invalidate(keys...)
}

// InvalidateAll clears all cached role entries, typically used after broad organization mutations like deletion.
func (p *OrgRoleProvider) InvalidateAll() {
	p.roleCache.InvalidateAll()
}

// Stop halts the expiration-sweeper goroutine to support graceful shutdown.
func (p *OrgRoleProvider) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		p.workerWG.Wait()
	})
}

// orgRoleInfoFromResolution converts persistence-layer effective-role resolution into OrgRoleInfo used by service-layer permission and membership responses.
//
// Parameters:
//   - orgID: Organization ID requested by the caller.
//   - roleResult: Persistence effective-role resolution payload.
//
// Returns:
//   - OrgRoleInfo: Normalized role information with inherited metadata when applicable.
func (p *OrgRoleProvider) orgRoleInfoFromResolution(
	orgID common_service_models.OrganizationIdType,
	roleResult persistence.OrganizationEffectiveRoleResult,
) OrgRoleInfo {
	info := OrgRoleInfo{
		ExplicitRole:  roleResult.ExplicitRole,
		EffectiveRole: roleResult.EffectiveRole,
	}
	if roleResult.InheritedFromOrganizationID != nil {
		converted := common_service_models.OrganizationIdType(*roleResult.InheritedFromOrganizationID)
		if converted != orgID {
			info.InheritedFromOrganizationID = &converted
		}
	}
	if roleResult.EffectiveRoleDepth != nil && *roleResult.EffectiveRoleDepth >= 0 {
		converted := uint32(*roleResult.EffectiveRoleDepth)
		info.EffectiveRoleDepth = &converted
	}
	return info
}

// resolveRoleInfo resolves explicit and effective role data from persistence, applying inheritance mode when enabled.
//
// Parameters:
//   - ctx: Request context used for persistence queries.
//   - userID: User whose role information is being resolved.
//   - orgID: Organization where membership is evaluated.
//
// Returns:
//   - OrgRoleInfo: Resolved explicit/effective role payload.
//   - error: Persistence error encountered during role resolution.
func (p *OrgRoleProvider) resolveRoleInfo(
	ctx context.Context,
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	inheritance bool,
) (OrgRoleInfo, error) {
	if inheritance {
		roleResult, err := p.persistenceHandler.GetUserEffectiveOrganizationRoleByLineage(ctx, userID, orgID)
		if err != nil {
			return OrgRoleInfo{EffectiveRole: proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED}, err
		}
		return p.orgRoleInfoFromResolution(orgID, roleResult), nil
	}

	explicitRole, err := p.persistenceHandler.GetUserExplicitOrganizationRole(ctx, userID, orgID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrUserOrganizationNotFound) {
			explicitRole = proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
		} else {
			return OrgRoleInfo{EffectiveRole: proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED}, err
		}
	}

	return OrgRoleInfo{
		ExplicitRole:                explicitRole,
		EffectiveRole:               explicitRole,
		InheritedFromOrganizationID: nil,
		EffectiveRoleDepth:          nil,
	}, nil
}

// GetUserRoleFresh resolves persisted authority without reading or filling the
// role cache. Realtime leases must not extend the lifetime of cached grants.
func (p *OrgRoleProvider) GetUserRoleFresh(ctx context.Context, userID common_service_models.UserIdType, orgID common_service_models.OrganizationIdType) (OrgRoleInfo, bool, error) {
	if userID == 0 || orgID == 0 {
		return OrgRoleInfo{}, false, nil
	}
	inheritance := p.runtime().OrgRoleInheritanceEnabled()
	info, err := p.resolveRoleInfo(ctx, userID, orgID, inheritance)
	return info, err == nil && info.EffectiveRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, err
}

// ResolveUserRoleFresh resolves persisted authority like GetUserRoleFresh, but under the caller's
// inheritance mode, so a caller holding one captured configuration generation evaluates role and
// permission thresholds from that same generation. A non-member resolves to an UNSPECIFIED
// effective role; the role cache is neither read nor filled.
func (p *OrgRoleProvider) ResolveUserRoleFresh(ctx context.Context, userID common_service_models.UserIdType, orgID common_service_models.OrganizationIdType, inheritance bool) (OrgRoleInfo, error) {
	return p.resolveRoleInfo(ctx, userID, orgID, inheritance)
}
