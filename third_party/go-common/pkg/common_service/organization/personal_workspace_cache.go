package organization

import (
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
)

// PersonalWorkspaceCacheKey is the immutable identity needed to evict an
// organization snapshot after a personal-workspace lifecycle transaction.
type PersonalWorkspaceCacheKey struct {
	ID   common_service_models.OrganizationIdType
	Slug string
}

// PersonalWorkspaceCacheInvalidation describes the cache state affected by a
// committed personal-workspace lifecycle mutation. It is built from immutable
// identities and delivered only after the transaction commits; receivers must
// not retain or mutate it.
type PersonalWorkspaceCacheInvalidation struct {
	// TreeKeys identifies every organization in the affected personal tree.
	TreeKeys []PersonalWorkspaceCacheKey
	// OwnerUserID is the personal workspace owner's user ID.
	OwnerUserID common_service_models.UserIdType
	// VisibilityChanged reports whether the transaction created, archived, or
	// restored organization rows, which can change email-domain visibility.
	VisibilityChanged bool
}

// PersonalWorkspaceCacheInvalidator receives the lifecycle snapshot only after
// the transaction commits.
type PersonalWorkspaceCacheInvalidator interface {
	InvalidatePersonalWorkspaceCaches(PersonalWorkspaceCacheInvalidation)
}

// InvalidatePersonalWorkspaceCaches clears only the snapshots affected by a
// committed personal-workspace lifecycle mutation.
func (h *OrganizationHandler[U]) InvalidatePersonalWorkspaceCaches(
	invalidation PersonalWorkspaceCacheInvalidation,
) {
	if h == nil {
		return
	}
	if h.realtimeInvalidator != nil {
		h.realtimeInvalidator.InvalidateChatRealtimeAccess(ports.ChatRealtimeAccessInvalidation{})
	}
	keys := invalidation.TreeKeys
	h.invalidateOrganizationTreeCaches(keys, invalidation.VisibilityChanged)
	if h.orgRoleProvider != nil {
		h.orgRoleProvider.invalidateMany(
			invalidation.OwnerUserID,
			common_util.ApplyToEach(keys, func(key PersonalWorkspaceCacheKey) common_service_models.OrganizationIdType {
				return key.ID
			}),
		)
	}
}

// invalidateOrganizationTreeCaches clears row, depth, and slug snapshots for a
// whole organization tree in one cache generation. Email-domain snapshots are
// global because negative entries cannot be mapped back to an organization;
// clear them only when organization visibility changed.
func (h *OrganizationHandler[U]) invalidateOrganizationTreeCaches(
	keys []PersonalWorkspaceCacheKey,
	visibilityChanged bool,
) {
	if h == nil || (len(keys) == 0 && !visibilityChanged) {
		return
	}
	h.cacheMu.Lock()
	defer h.cacheMu.Unlock()
	h.cacheGeneration++
	for _, key := range keys {
		if h.orgByIDCache != nil {
			h.orgByIDCache.Delete(key.ID)
		}
		if h.orgSlugIDCache != nil && key.Slug != "" {
			h.orgSlugIDCache.Delete(key.Slug)
		}
		if h.orgDepthCache != nil {
			h.orgDepthCache.Delete(key.ID)
		}
	}
	// Domain mappings are retained with archived personal trees. Clear both
	// positive and negative entries after create/archive/restore so visibility
	// tracks the just-committed organization state.
	if visibilityChanged && h.orgEmailDomainCache != nil {
		h.orgEmailDomainCache.DeleteAll()
	}
}

var _ PersonalWorkspaceCacheInvalidator = (*OrganizationHandler[common_service_models.User])(nil)
