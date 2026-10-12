package auth

import (
	"context"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/jellydator/ttlcache/v3"
)

// sessionLifetime is sampled when issuing a token. Existing tokens retain their
// persisted expiry, so shortening policy does not retroactively revoke sessions.
func (h *AuthHandler[U]) sessionLifetime() time.Duration {
	return h.runtimeConfigProvider().SessionValidity()
}

// authCacheLookup owns the handles and admission fence for one cache operation.
// In-flight reads may finish against retired caches; fills require current
// handles and an unchanged invalidation generation.
type authCacheLookup[U common_service_models.UserLike] struct {
	h          *AuthHandler[U]
	tokens     *ttlcache.Cache[string, common_service_models.UserIdType]
	users      *ttlcache.Cache[common_service_models.UserIdType, U]
	ttl        time.Duration
	generation uint64
}

func (h *AuthHandler[U]) authCaches() authCacheLookup[U] {
	h.cachePolicyMu.Lock()
	defer h.cachePolicyMu.Unlock()
	view := h.runtimeConfigProvider()
	h.resizeAuthCachesLocked(view)
	ttl := view.AuthCacheTtl()
	if ttl != h.cachePolicyTTL {
		if h.authTokenToUserIDCache != nil {
			h.authTokenToUserIDCache.DeleteAll()
		}
		if h.userIDToUserCache != nil {
			h.userIDToUserCache.DeleteAll()
		}
		h.cachePolicyTTL = ttl
		h.cacheGeneration++
	}
	lookup := authCacheLookup[U]{h: h, ttl: ttl, generation: h.cacheGeneration}
	if ttl > 0 && h.cacheTokenCapacity > 0 {
		lookup.tokens = h.authTokenToUserIDCache
	}
	if ttl > 0 && h.cacheUserCapacity > 0 {
		lookup.users = h.userIDToUserCache
	}
	return lookup
}

// store admits only the results authorized by this lookup's cache policy. Every
// cache replacement bumps cacheGeneration, so an unchanged generation also
// proves the captured handles are still current.
func (c authCacheLookup[U]) store(key string, userID common_service_models.UserIdType, token *common_persistence_models.SessionToken, user *U) {
	h := c.h
	h.cachePolicyMu.Lock()
	defer h.cachePolicyMu.Unlock()
	if c.generation != h.cacheGeneration {
		return
	}
	if c.tokens != nil && token != nil {
		if ttl := tokenCacheTTLFor(token, c.ttl); ttl > 0 {
			c.tokens.Set(key, userID, ttl)
		}
	}
	if c.users != nil && user != nil && c.ttl > 0 {
		c.users.Set(userID, *user, c.ttl)
	}
}

// A pending revocation must cap cache lifetime just like persisted expiry.
func tokenCacheTTLFor(token *common_persistence_models.SessionToken, ttl time.Duration) time.Duration {
	remaining := time.Until(token.ExpiresAt)
	if token.RevokedAt != nil {
		remaining = min(remaining, time.Until(*token.RevokedAt))
	}
	return min(remaining, ttl)
}

// Cache pointers are copied under the policy mutex. In-flight users may finish
// against a retired disposable cache; revocation and refresh handoff state live
// separately and are never replaced here.
func (h *AuthHandler[U]) tokenCache() *ttlcache.Cache[string, common_service_models.UserIdType] {
	h.cachePolicyMu.Lock()
	defer h.cachePolicyMu.Unlock()
	h.resizeAuthCachesLocked(h.runtimeConfigProvider())
	return h.authTokenToUserIDCache
}
func (h *AuthHandler[U]) userCache() *ttlcache.Cache[common_service_models.UserIdType, U] {
	h.cachePolicyMu.Lock()
	defer h.cachePolicyMu.Unlock()
	h.resizeAuthCachesLocked(h.runtimeConfigProvider())
	return h.userIDToUserCache
}
func (h *AuthHandler[U]) resizeAuthCachesLocked(view RuntimeConfigView) {
	tokens, users := view.AuthTokenCacheCapacity(), view.AuthUserCacheCapacity()
	if tokens != h.cacheTokenCapacity && h.authTokenToUserIDCache != nil {
		h.authTokenToUserIDCache.Stop()
		h.authTokenToUserIDCache = ttlcache.New(ttlcache.WithCapacity[string, common_service_models.UserIdType](tokens), ttlcache.WithDisableTouchOnHit[string, common_service_models.UserIdType]())
		h.authTokenToUserIDCache.OnEviction(func(context.Context, ttlcache.EvictionReason, *ttlcache.Item[string, common_service_models.UserIdType]) {
			h.tokenCacheEvictions.Add(1)
		})
		h.cacheTokenCapacity = tokens
		h.cacheGeneration++
	}
	if users != h.cacheUserCapacity && h.userIDToUserCache != nil {
		h.userIDToUserCache.Stop()
		h.userIDToUserCache = ttlcache.New(ttlcache.WithCapacity[common_service_models.UserIdType, U](users), ttlcache.WithDisableTouchOnHit[common_service_models.UserIdType, U]())
		h.userIDToUserCache.OnEviction(func(context.Context, ttlcache.EvictionReason, *ttlcache.Item[common_service_models.UserIdType, U]) {
			h.userCacheEvictions.Add(1)
		})
		h.cacheUserCapacity = users
		h.cacheGeneration++
	}
}

// cacheAdmission captures the invalidation generation before a cache miss reads
// session or user state. Admitting with a stale generation is skipped, so a
// lookup that raced a revocation cannot repopulate the caches afterwards.
//
// TODO(multi-instance-v2): invalidations are process-local; other instances
// keep serving cached sessions until TTL. Distribute invalidations when
// multiple instances are supported.
func (h *AuthHandler[U]) cacheAdmission() uint64 {
	h.cachePolicyMu.Lock()
	defer h.cachePolicyMu.Unlock()
	return h.cacheGeneration
}

// bumpCacheGeneration fences in-flight admissions captured before an invalidation.
func (h *AuthHandler[U]) bumpCacheGeneration() {
	h.cachePolicyMu.Lock()
	h.cacheGeneration++
	h.cachePolicyMu.Unlock()
}

func (h *AuthHandler[U]) cacheToken(key string, value common_service_models.UserIdType, ttl time.Duration, admission uint64) {
	h.cachePolicyMu.Lock()
	defer h.cachePolicyMu.Unlock()
	if admission != h.cacheGeneration {
		return
	}
	h.resizeAuthCachesLocked(h.runtimeConfigProvider())
	capacity := h.cacheTokenCapacity
	if capacity > 0 && ttl > 0 {
		h.authTokenToUserIDCache.Set(key, value, ttl)
	}
}
