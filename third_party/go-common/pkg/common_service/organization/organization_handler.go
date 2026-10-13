package organization

import (
	"context"
	stdErrors "errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_converter"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

const organizationLoggerName = "common_service.organization"

// Organization service responses

// OrganizationResponse represents the response containing organization data.
type OrganizationResponse struct {
	*common_service_models.Organization
	// MemberCount is the total number of members in the organization.
	MemberCount int64 `json:"member_count"`
}

type AdminOrganizationFilterParentOption struct {
	ID   common_service_models.OrganizationIdType `json:"id"`
	Name string                                   `json:"name"`
	Slug string                                   `json:"slug"`
}

type AdminOrganizationFilterMetadata struct {
	MaxDepth uint32 `json:"max_depth"`
}

type OrganizationHierarchyResult struct {
	Hierarchy  *common_service_models.OrganizationHierarchyNode
	Pagination *proto_models.PaginationResponse
	Truncated  bool
}

// adminListAllOrganizationsOrderColumn maps API sort enums to persistence column names for ListAllOrganizations admin queries.
//
// Parameters:
//   - sortBy: Requested sort field enum from the API layer.
//
// Returns:
//   - string: Persistence column name used for ordering results.
func adminListAllOrganizationsOrderColumn(sortBy proto_models.AdminListAllOrganizationsSortBy) string {
	switch sortBy {
	case proto_models.AdminListAllOrganizationsSortBy_ADMIN_LIST_ALL_ORGANIZATIONS_SORT_BY_NAME:
		return "name"
	case proto_models.AdminListAllOrganizationsSortBy_ADMIN_LIST_ALL_ORGANIZATIONS_SORT_BY_SLUG:
		return "slug"
	case proto_models.AdminListAllOrganizationsSortBy_ADMIN_LIST_ALL_ORGANIZATIONS_SORT_BY_ORGANIZATION_TYPE:
		return "organization_type"
	case proto_models.AdminListAllOrganizationsSortBy_ADMIN_LIST_ALL_ORGANIZATIONS_SORT_BY_UPDATED_AT:
		return "updated_at"
	default:
		return "created_at"
	}
}

// getOrganizationViewRolesByDepth computes minimum viewer roles per hierarchy depth and is used by member/list response filtering.
//
// Returns:
//   - []proto_models.OrganizationRole: Required organization view role for each configured depth.
//   - error: Permission configuration lookup error.
func (h *OrganizationHandler[U]) getOrganizationViewRolesByDepth() ([]proto_models.OrganizationRole, error) {
	maxDepth := h.runtimeConfig().OrgHierarchyMaxDepth()
	requiredRoles := make([]proto_models.OrganizationRole, maxDepth+1)
	for depth := 0; depth <= maxDepth; depth++ {
		requiredRole, err := h.permissionHandler.GetRequiredOrgRole(permission.PermissionOrganizationView, depth)
		if err != nil {
			return nil, err
		}
		requiredRoles[depth] = requiredRole
	}
	return requiredRoles, nil
}

type organizationCreateMode int

const (
	organizationCreateModeUser  organizationCreateMode = 0
	organizationCreateModeAdmin organizationCreateMode = 1
)

// OrganizationPrivilegedHandlerInterface exposes trusted non-API organization
// operations that intentionally bypass org-type capability gating while reusing
// the normal business logic paths.
type OrganizationPrivilegedHandlerInterface interface {
	AddMemberPrivileged(ctx context.Context, targetUserID common_service_models.UserIdType, orgID common_service_models.OrganizationIdType, role proto_models.OrganizationRole) (*common_service_models.UserOrganizationResponse, error)
	UpdateMemberRolePrivileged(ctx context.Context, targetUserID common_service_models.UserIdType, orgID common_service_models.OrganizationIdType, newRole proto_models.OrganizationRole) (*common_service_models.UserOrganizationResponse, error)
	AddUserToOrganizationByIdentifierPrivileged(ctx context.Context, orgID common_service_models.OrganizationIdType, identifierType proto_models.UserIdentifierType, identifier string, role proto_models.OrganizationRole) (*AddUserResult, error)
	GetUserOrganizationMembershipPrivileged(ctx context.Context, userID common_service_models.UserIdType, orgID common_service_models.OrganizationIdType) (*common_service_models.OrganizationMemberDetails, error)
}

// OrganizationHandlerInterface defines the interface for organization operations.
// It provides methods for creating, updating, deleting organizations, and managing
// organization membership with role-based access control.
type OrganizationHandlerInterface interface {
	// Organization CRUD operations
	CreateOrganizationByUser(ctx context.Context, organization *common_service_models.Organization) (*OrganizationResponse, error)
	CreateOrganizationByAdmin(ctx context.Context, organization *common_service_models.Organization) (*OrganizationResponse, error)
	GetOrganization(ctx context.Context, id common_service_models.OrganizationIdType) (*OrganizationResponse, error)
	GetOrganizationBySlug(ctx context.Context, slug string) (*OrganizationResponse, error)
	GetOrganizationDepth(ctx context.Context, id common_service_models.OrganizationIdType) (int, error)
	UpdateOrganization(ctx context.Context, organization *common_service_models.Organization) (*OrganizationResponse, error)
	DeleteOrganization(ctx context.Context, id common_service_models.OrganizationIdType) error
	CheckSlugAvailability(ctx context.Context, slug string) (available bool, message string, err error)
	ListAllOrganizations(ctx context.Context, limit, offset int, filter *proto_models.FilterRequest, organizationTypeFilter proto_models.OrganizationType, parentOrganizationIDFilter *common_service_models.OrganizationIdType, depthFilter *uint32, sortBy proto_models.AdminListAllOrganizationsSortBy, sort *proto_models.SortRequest, includeTotalCount bool) ([]*OrganizationResponse, int64, error)
	ListAdminOrganizationParentOptions(ctx context.Context, limit, offset int, filter, tableFilter *proto_models.FilterRequest, organizationTypeFilter proto_models.OrganizationType) ([]*AdminOrganizationFilterParentOption, bool, error)
	GetAdminOrganizationParentOptionsByIDs(ctx context.Context, organizationIDs []common_service_models.OrganizationIdType) ([]*AdminOrganizationFilterParentOption, error)
	GetAdminOrganizationFilterMetadata(ctx context.Context, filter *proto_models.FilterRequest, organizationTypeFilter proto_models.OrganizationType) (*AdminOrganizationFilterMetadata, error)
	ListOrganizationsByParent(ctx context.Context, parentOrganizationID *common_service_models.OrganizationIdType, limit, offset int) ([]*OrganizationResponse, int64, error)
	GetOrganizationHierarchy(ctx context.Context, organizationID common_service_models.OrganizationIdType, includeDescendants bool) (*common_service_models.OrganizationHierarchyNode, error)
	GetOrganizationDeletionImpactPage(ctx context.Context, organizationID, afterOrganizationID common_service_models.OrganizationIdType, limit int) ([]*common_service_models.Organization, bool, error)
	GetOrganizationHierarchyPage(ctx context.Context, organizationID common_service_models.OrganizationIdType, includeDescendants bool, page uint32, limit, offset int) (*OrganizationHierarchyResult, error)
	ListUserOrganizationsWithRolesPage(ctx context.Context, userID common_service_models.UserIdType, limit, offset int, sortBy proto_models.ListOrganizationsSortBy, sort *proto_models.SortRequest, filter *proto_models.FilterRequest, organizationTypeFilter proto_models.OrganizationType, parentOrganizationIDFilter *common_service_models.OrganizationIdType, depthFilter *uint32, includeTotalCount bool, maskFilters []*proto_models.OrganizationMembershipMaskFilter) ([]*common_service_models.UserOrganizationResponse, *proto_models.PaginationResponse, error)
	ChangeOrganizationType(ctx context.Context, organizationID common_service_models.OrganizationIdType, newType proto_models.OrganizationType, reason string) error

	// Organization membership operations
	AddMember(ctx context.Context, targetUserId common_service_models.UserIdType, orgID common_service_models.OrganizationIdType, role proto_models.OrganizationRole) (*common_service_models.UserOrganizationResponse, error)
	RemoveMember(ctx context.Context, targetUserId common_service_models.UserIdType, orgID common_service_models.OrganizationIdType) error
	UpdateMemberRole(ctx context.Context, targetUserId common_service_models.UserIdType, orgID common_service_models.OrganizationIdType, newRole proto_models.OrganizationRole) (*common_service_models.UserOrganizationResponse, error)
	GetOrganizationMembers(ctx context.Context, orgID common_service_models.OrganizationIdType, limit, offset int) ([]*common_service_models.OrganizationMemberDetails, int64, error)
	GetUserOrganizations(ctx context.Context, userID common_service_models.UserIdType) ([]*common_service_models.UserOrganizationResponse, error)
	GetUserOrganizationMembership(ctx context.Context, userID common_service_models.UserIdType, orgID common_service_models.OrganizationIdType) (*common_service_models.OrganizationMemberDetails, error)
	GetUserRoleInOrganization(ctx context.Context, userID common_service_models.UserIdType, orgID common_service_models.OrganizationIdType) (proto_models.OrganizationRole, bool, error)
	GetUserRoleInfo(ctx context.Context, userID common_service_models.UserIdType, orgID common_service_models.OrganizationIdType) (OrgRoleInfo, bool, error)

	// Organization invitation operations
	AddUserToOrganizationByIdentifier(ctx context.Context, orgID common_service_models.OrganizationIdType, identifierType proto_models.UserIdentifierType, identifier string, role proto_models.OrganizationRole) (*AddUserResult, error)
	InviteUserToOrganization(ctx context.Context, orgID common_service_models.OrganizationIdType, identifierType proto_models.UserIdentifierType, identifier string, role proto_models.OrganizationRole) (uint64, error)
	GetPendingInvitations(ctx context.Context) ([]*common_service_models.OrganizationInvitation, error)
	GetPendingInvitationsPage(ctx context.Context, pagination *proto_models.PaginationRequest) ([]*common_service_models.OrganizationInvitation, *proto_models.PaginationResponse, error)
	AcceptInvitation(ctx context.Context, invitationID uint64) (*common_service_models.UserOrganizationResponse, error)
	DeclineInvitation(ctx context.Context, invitationID uint64) error
	CancelInvitation(ctx context.Context, orgID common_service_models.OrganizationIdType, invitationID uint64) error
	ListOrganizationInvitations(ctx context.Context, orgID common_service_models.OrganizationIdType, limit, offset int) ([]*common_service_models.OrganizationInvitation, int64, error)
	ProcessPendingInvitationsForUser(ctx context.Context, userID common_service_models.UserIdType, email string) ([]*common_service_models.OrganizationInvitation, error)

	// Organization email domain operations
	CreateOrganizationEmailDomain(ctx context.Context, orgID common_service_models.OrganizationIdType, domain string, createdBy *common_service_models.UserIdType) (*common_service_models.OrganizationEmailDomain, error)
	DeleteOrganizationEmailDomain(ctx context.Context, orgID common_service_models.OrganizationIdType, emailDomainID uint64) error
	ListOrganizationEmailDomains(ctx context.Context, orgID common_service_models.OrganizationIdType) ([]*common_service_models.OrganizationEmailDomain, error)
	ListOrganizationEmailDomainsPage(ctx context.Context, orgID common_service_models.OrganizationIdType, pagination *proto_models.PaginationRequest) ([]*common_service_models.OrganizationEmailDomain, *proto_models.PaginationResponse, error)
	GetOrganizationEmailDomainByDomain(ctx context.Context, domain string) (*common_service_models.OrganizationEmailDomain, error)
	AutoAssociateUserWithOrganizationByEmail(ctx context.Context, userID common_service_models.UserIdType, email string) error

	// Organization permission checks
	HasOrganizationPermission(ctx context.Context, userID common_service_models.UserIdType, orgID common_service_models.OrganizationIdType, requiredRole proto_models.OrganizationRole) (bool, error)
	GetMemberViewPermissions(ctx context.Context, orgID common_service_models.OrganizationIdType) (*config.UserAttributePermissions, error)

	// Stop gracefully shuts down the handler.
	Stop()
}

// OrganizationHandler implements OrganizationHandlerInterface for managing organizations.
// It uses the persistence layer to store and retrieve data, with support for
// database transactions through context propagation. It includes a TTL cache
// for organization role lookups to improve performance.
type OrganizationHandler[U common_service_models.UserLike] struct {
	realtimeInvalidator ports.ChatRealtimeAccessInvalidator
	notificationSink    ports.OrganizationNotificationSink
	persistenceHandler  persistence.CommonPersistenceInterface
	permissionHandler   permission.PermissionHandlerInterface
	orgRoleProvider     *OrgRoleProvider
	orgDepthCache       *generationCache[common_service_models.OrganizationIdType, int]
	converter           common_converter.ConverterInterface
	config              *startupPolicy
	runtimeConfig       PolicyProvider
	collections         *config.GenerationMemo[PolicyView, *organizationCollections]
	// attributeTables samples the attribute-permission provider once per call
	// and returns the compiled tables, memoized per configuration generation.
	attributeTables func() attributePermissionTables
	callbackConfig  *ports.OrganizationCallbackConfig[U]
	logger          *zap.Logger

	// orgByIDCache and orgSlugIDCache serve the read-only organization lookup
	// paths (GetOrganization / GetOrganizationBySlug); mutation flows always
	// read fresh rows. orgEmailDomainCache serves the registration-path domain
	// lookup and caches negative results (nil value) since most user email
	// domains have no organization mapping. Cached values are immutable.
	// All caches share the embedded generation guard so one invalidation
	// rejects every in-flight fill across the handler.
	orgByIDCache        *generationCache[common_service_models.OrganizationIdType, *common_persistence_models.Organization]
	orgSlugIDCache      *generationCache[string, common_service_models.OrganizationIdType]
	orgEmailDomainCache *generationCache[string, *common_service_models.OrganizationEmailDomain]
	cacheGenerationGuard

	// stopCh ends the cache-metrics goroutine when Stop is called; stopOnce
	// keeps Stop idempotent.
	stopCh         chan struct{}
	cacheMetricsWG sync.WaitGroup
	stopOnce       sync.Once
}

// Compile-time checks to ensure OrganizationHandler implements both surfaces.
var _ OrganizationHandlerInterface = (*OrganizationHandler[common_service_models.User])(nil)
var _ OrganizationPrivilegedHandlerInterface = (*OrganizationHandler[common_service_models.User])(nil)

// NewOrganizationHandler builds the handler around one live PolicyProvider. It
// rejects a nil provider and a provider whose startup view is nil or the zero
// RootView.
func NewOrganizationHandler[U common_service_models.UserLike](
	persistenceHandler persistence.CommonPersistenceInterface,
	permissionHandler permission.PermissionHandlerInterface,
	orgRoleProvider *OrgRoleProvider,
	converter common_converter.ConverterInterface,
	provider PolicyProvider,
	callbackConfig *ports.OrganizationCallbackConfig[U],
	logger *zap.Logger,
) (*OrganizationHandler[U], error) {
	if provider == nil {
		return nil, stdErrors.New("organization configuration provider is required")
	}
	view := provider()
	if view == nil || view == PolicyView(config.RootView{}) {
		return nil, stdErrors.New("organization configuration view is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	organizationConfig := newStartupPolicy(view)
	attributeTables := config.NewGenerationMemo(
		func(view PolicyView) (attributePermissionTables, error) {
			return compileAttributePermissionTables(view), nil
		},
		func(err error) {
			logger.Named(organizationLoggerName).Warn("attribute permission policy unavailable; keeping last valid tables",
				zap.String(logkeys.Operation, "compile_organization_attribute_permission_policy"),
				zap.String(logkeys.Stage, "compile_hot_configuration"),
				zap.String(logkeys.Fallback, "keep_last_valid_tables"),
				zap.Error(err))
		},
	)
	depthCacheTTL := view.OrganizationRoleCacheTTL()
	depthCacheCapacity := view.OrganizationRoleCacheCapacity()
	depthCache := ttlcache.New(
		ttlcache.WithTTL[common_service_models.OrganizationIdType, int](depthCacheTTL),
		ttlcache.WithCapacity[common_service_models.OrganizationIdType, int](depthCacheCapacity),
		ttlcache.WithDisableTouchOnHit[common_service_models.OrganizationIdType, int](),
	)

	handler := &OrganizationHandler[U]{
		persistenceHandler: persistenceHandler,
		permissionHandler:  permissionHandler,
		orgRoleProvider:    orgRoleProvider,
		converter:          converter,
		config:             organizationConfig,
		runtimeConfig:      provider,
		collections:        newOrganizationCollectionsMemo(),
		attributeTables:    attributeTables.Provider(provider),
		callbackConfig:     callbackConfig,
		logger:             logger,
		stopCh:             make(chan struct{}),
	}

	handler.orgDepthCache = newGenerationCache(&handler.cacheGenerationGuard, depthCache)
	// The expiration sweeper is owned here instead of via ttlcache's
	// go Start()/Stop() pair: ttlcache.Stop is a no-op until the freshly
	// launched worker flips its internal state, so a constructor that
	// fails immediately after launching it would leak the goroutine.
	// Reads already reject expired entries; the sweeper only reclaims
	// memory, so it is skipped entirely for a non-expiring cache.
	if depthCacheTTL > 0 {
		handler.cacheMetricsWG.Go(func() {
			ticker := time.NewTicker(depthCacheTTL)
			defer ticker.Stop()
			for {
				select {
				case <-handler.stopCh:
					return
				case <-ticker.C:
					handler.orgDepthCache.cache.Load().DeleteExpired()
				}
			}
		})
	}

	// Organization rows and email-domain mappings are near-static; reuse
	// the role-cache TTL/capacity knobs. Not Start()ed: expired entries are
	// rejected on read and evicted on capacity pressure.
	handler.orgByIDCache = newGenerationCache(&handler.cacheGenerationGuard, ttlcache.New(
		ttlcache.WithTTL[common_service_models.OrganizationIdType, *common_persistence_models.Organization](depthCacheTTL),
		ttlcache.WithCapacity[common_service_models.OrganizationIdType, *common_persistence_models.Organization](depthCacheCapacity),
		ttlcache.WithDisableTouchOnHit[common_service_models.OrganizationIdType, *common_persistence_models.Organization](),
	))
	handler.orgSlugIDCache = newGenerationCache(&handler.cacheGenerationGuard, ttlcache.New(
		ttlcache.WithTTL[string, common_service_models.OrganizationIdType](depthCacheTTL),
		ttlcache.WithCapacity[string, common_service_models.OrganizationIdType](depthCacheCapacity),
		ttlcache.WithDisableTouchOnHit[string, common_service_models.OrganizationIdType](),
	))
	handler.orgEmailDomainCache = newGenerationCache(&handler.cacheGenerationGuard, ttlcache.New(
		ttlcache.WithTTL[string, *common_service_models.OrganizationEmailDomain](depthCacheTTL),
		ttlcache.WithCapacity[string, *common_service_models.OrganizationEmailDomain](depthCacheCapacity),
		ttlcache.WithDisableTouchOnHit[string, *common_service_models.OrganizationEmailDomain](),
	))

	policy := func() orgCachePolicy {
		v := handler.runtimeConfig()
		return orgCachePolicy{capacity: v.OrganizationRoleCacheCapacity(), ttl: v.OrganizationRoleCacheTTL()}
	}

	handler.orgDepthCache.applied = orgCachePolicy{capacity: depthCacheCapacity, ttl: depthCacheTTL}
	handler.orgDepthCache.policy = policy

	handler.orgByIDCache.applied = orgCachePolicy{capacity: depthCacheCapacity, ttl: depthCacheTTL}
	handler.orgByIDCache.policy = policy

	handler.orgSlugIDCache.applied = orgCachePolicy{capacity: depthCacheCapacity, ttl: depthCacheTTL}
	handler.orgSlugIDCache.policy = policy

	handler.orgEmailDomainCache.applied = orgCachePolicy{capacity: depthCacheCapacity, ttl: depthCacheTTL}
	handler.orgEmailDomainCache.policy = policy

	if handler.orgRoleProvider != nil && handler.orgRoleProvider.CacheEnabled() {
		handler.cacheMetricsWG.Go(func() {
			handler.collectCacheMetrics()
		})
	}

	return handler, nil
}

func (h *OrganizationHandler[U]) hierarchyMaxNodes() int {
	return h.runtimeConfig().OrgHierarchyMaxNodes()
}

// organizationCallbacks returns the configured callback set used by post-operation hook invocations in organization CRUD and membership flows.
//
// Returns:
//   - *ports.OrganizationCallbacks: Callback set, or `nil` when callbacks are not configured.
func (h *OrganizationHandler[U]) organizationCallbacks() *ports.OrganizationCallbacks {
	if h.callbackConfig == nil {
		return nil
	}
	return h.callbackConfig.Callbacks
}

// invokeOnOrganizationCreated triggers the configured post-create callback after successful organization creation.
//
// Parameters:
//   - ctx: Request context propagated to callback execution.
//   - org: Newly created organization model.
//
// Returns:
//   - error: Callback error when a callback is configured and fails.
func (h *OrganizationHandler[U]) invokeOnOrganizationCreated(ctx context.Context, org *common_service_models.Organization) error {
	callbacks := h.organizationCallbacks()
	if callbacks == nil || callbacks.OnOrganizationCreated == nil {
		return nil
	}
	return callbacks.OnOrganizationCreated(ctx, org)
}

// invokeOnOrganizationUpdated triggers the configured post-update callback after successful organization updates.
//
// Parameters:
//   - ctx: Request context propagated to callback execution.
//   - org: Updated organization model.
//
// Returns:
//   - error: Callback error when a callback is configured and fails.
func (h *OrganizationHandler[U]) invokeOnOrganizationUpdated(ctx context.Context, org *common_service_models.Organization) error {
	callbacks := h.organizationCallbacks()
	if callbacks == nil || callbacks.OnOrganizationUpdated == nil {
		return nil
	}
	return callbacks.OnOrganizationUpdated(ctx, org)
}

// invokeOnOrganizationDeleted triggers the configured post-delete callback after successful organization deletion.
//
// Parameters:
//   - ctx: Request context propagated to callback execution.
//   - orgID: Deleted organization ID.
//
// Returns:
//   - error: Callback error when a callback is configured and fails.
func (h *OrganizationHandler[U]) invokeOnOrganizationDeleted(ctx context.Context, orgID common_service_models.OrganizationIdType) error {
	callbacks := h.organizationCallbacks()
	if callbacks == nil || callbacks.OnOrganizationDeleted == nil {
		return nil
	}
	return callbacks.OnOrganizationDeleted(ctx, orgID)
}

// invokeOnOrganizationMemberAdded triggers the configured membership-added callback after a successful member insert.
//
// Parameters:
//   - ctx: Request context propagated to callback execution.
//   - membership: Membership model that was created.
//
// Returns:
//   - error: Callback error when a callback is configured and fails.
func (h *OrganizationHandler[U]) invokeOnOrganizationMemberAdded(ctx context.Context, membership *common_service_models.UserOrganization) error {
	callbacks := h.organizationCallbacks()
	if callbacks == nil || callbacks.OnOrganizationMemberAdded == nil {
		return nil
	}
	return callbacks.OnOrganizationMemberAdded(ctx, membership)
}

// invokeOnOrganizationMemberRemoved triggers the configured membership-removed callback after a successful member removal.
//
// Parameters:
//   - ctx: Request context propagated to callback execution.
//   - membership: Membership model that was removed.
//
// Returns:
//   - error: Callback error when a callback is configured and fails.
func (h *OrganizationHandler[U]) invokeOnOrganizationMemberRemoved(ctx context.Context, membership *common_service_models.UserOrganization) error {
	callbacks := h.organizationCallbacks()
	if callbacks == nil || callbacks.OnOrganizationMemberRemoved == nil {
		return nil
	}
	return callbacks.OnOrganizationMemberRemoved(ctx, membership)
}

// actorUserIDFromContext extracts the acting user ID from request context for privileged helper calls.
//
// Parameters:
//   - ctx: Request context that may carry authenticated user data.
//
// Returns:
//   - common_service_models.UserIdType: Actor user ID, or `0` when no user is available in context.
func (h *OrganizationHandler[U]) actorUserIDFromContext(ctx context.Context) common_service_models.UserIdType {
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found || requestingUser == nil {
		return 0
	}
	return (*requestingUser).GetID()
}

// CreateOrganizationByUser creates a new organization through the user endpoint with permission checks.
func (h *OrganizationHandler[U]) CreateOrganizationByUser(
	ctx context.Context,
	organization *common_service_models.Organization,
) (*OrganizationResponse, error) {
	return h.createOrganizationWithMode(ctx, organization, organizationCreateModeUser)
}

// CreateOrganizationByAdmin creates a new organization through the admin endpoint with permission checks.
// The admin is NOT automatically added as a member.
func (h *OrganizationHandler[U]) CreateOrganizationByAdmin(
	ctx context.Context,
	organization *common_service_models.Organization,
) (*OrganizationResponse, error) {
	return h.createOrganizationWithMode(ctx, organization, organizationCreateModeAdmin)
}

// createOrganizationWithMode executes shared organization-creation logic for user and admin entry points, including actor resolution, validation, and optional auto-membership.
//
// Parameters:
//   - ctx: Request context used for actor, permission, and persistence operations.
//   - organization: Organization payload to validate and create.
//   - mode: Creation mode indicating user or admin flow semantics.
//
// Returns:
//   - *OrganizationResponse: Created organization response including membership count.
//   - error: Validation, permission, callback, or persistence error.
func (h *OrganizationHandler[U]) createOrganizationWithMode(
	ctx context.Context,
	organization *common_service_models.Organization,
	mode organizationCreateMode,
) (*OrganizationResponse, error) {
	if organization == nil {
		return nil, appLogger.InvariantError(
			"organization_create_payload_non_nil",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "create_organization"),
			zap.String(logkeys.Stage, "validate_payload"),
			zap.String(logkeys.Expected, "non-nil organization payload"),
			zap.String(logkeys.Actual, "nil organization payload"),
		)
	}
	if organization.PersonalOwnerUserID != nil {
		return nil, errors.ErrPersonalWorkspaceOwnershipImmutable
	}
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, errors.ErrUserNotFound
	}
	actorID := (*requestingUser).GetID()
	runtimeConfig := h.runtimeConfig()

	logFields := []zap.Field{
		zap.String(appLogger.KeyName, organization.Name),
		zap.String(appLogger.KeySlug, organization.Slug),
	}
	if mode == organizationCreateModeAdmin {
		logFields = append(logFields, zap.Uint64(appLogger.KeyAdminUserID, uint64(actorID)))
	} else {
		logFields = append(logFields, zap.Uint64(appLogger.KeyCreatorID, uint64(actorID)))
	}
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(logFields...)
	log.Debug("CreateOrganization called")

	user, err := h.persistenceHandler.GetUserByID(ctx, uint64(actorID))
	if err != nil {
		if stdErrors.Is(err, errors.ErrUserNotFound) {
			return nil, errors.ErrUserNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "create_organization"),
			zap.String(logkeys.Stage, "load_actor"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.String(appLogger.KeyName, organization.Name),
			zap.String(appLogger.KeySlug, organization.Slug),
		)
	}

	orgDepth, autoAddCreator, err := h.prepareOrganizationForCreate(ctx, *requestingUser, user, organization, mode, runtimeConfig)
	if err != nil {
		return nil, err
	}

	orgResponse, err := h.createOrganizationInternal(ctx, organization)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "create_organization"),
			zap.String(logkeys.Stage, "persist_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.String(appLogger.KeyName, organization.Name),
			zap.String(appLogger.KeySlug, organization.Slug),
		)
	}

	memberCount := int64(0)
	var defaultRole proto_models.OrganizationRole
	if autoAddCreator {
		defaultRole = getRoleAtDepth(h.collectionPolicy(runtimeConfig).creatorRoles, orgDepth)
		if common_service_models.IsPersonalOrganization(organization) {
			defaultRole = h.config.PersonalWorkspaceOwnerRole
		}
		_, err = h.addMemberDirect(ctx, actorID, actorID, orgResponse.ID, defaultRole)
		if err != nil {
			return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
				zap.String(logkeys.Operation, "create_organization"),
				zap.String(logkeys.Stage, "add_creator_membership"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Int64(appLogger.KeyOrganizationID, orgResponse.ID),
				zap.Stringer(appLogger.KeyRole, defaultRole),
			)
		}
		memberCount = 1
	}
	orgResponse.MemberCount = memberCount

	if err := h.invokeOnOrganizationCreated(ctx, orgResponse.Organization); err != nil {
		return nil, appLogger.WithErrorFields(
			fmt.Errorf("%w: %w", errors.ErrOrganizationCallbackFailed, err),
			zap.String(logkeys.Operation, "create_organization"),
			zap.String(logkeys.Stage, "organization_created_callback"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Int64(appLogger.KeyOrganizationID, orgResponse.ID),
		)
	}

	logFields = []zap.Field{
		zap.Uint64(appLogger.KeyOrganizationID, uint64(orgResponse.ID)),
	}
	if mode == organizationCreateModeAdmin {
		logFields = append(logFields, zap.Uint64(appLogger.KeyAdminUserID, uint64(actorID)))
		log.Info("organization created by admin successfully", logFields...)
	} else {
		logFields = append(logFields, zap.Stringer(appLogger.KeyCreatorRole, defaultRole))
		log.Info("organization created by user successfully", logFields...)
	}

	return orgResponse, nil
}

// prepareOrganizationForCreate validates organization payload and evaluates create permissions, default role, and creator auto-add policy before persistence writes.
//
// Parameters:
//   - ctx: Request context used for validation and permission checks.
//   - requestingUser: Authenticated service-layer actor from context.
//   - user: Persistence-layer actor record used for user-type evaluation.
//   - organization: Organization payload being created.
//   - mode: Creation mode indicating user or admin flow semantics.
//   - runtimeConfig: The caller's sample; the depth and creator auto-add
//     decision must come from the generation whose creator roles it assigns.
//
// Returns:
//   - int: Resolved organization depth used in downstream permission checks.
//   - bool: Whether creator membership should be auto-added after creation.
//   - error: Validation or permission error blocking organization creation.
func (h *OrganizationHandler[U]) prepareOrganizationForCreate(
	ctx context.Context,
	requestingUser U,
	user *common_persistence_models.User,
	organization *common_service_models.Organization,
	mode organizationCreateMode,
	runtimeConfig PolicyView,
) (int, bool, error) {
	if h.permissionHandler == nil {
		return 0, false, errors.ErrInternalError
	}
	if organization.OrganizationType == proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED {
		organization.OrganizationType = runtimeConfig.DefaultOrganizationType()
	}

	if err := h.validateOrganization(ctx, organization); err != nil {
		return 0, false, err
	}

	available, err := h.IsSlugAvailable(ctx, organization.Slug)
	if err != nil {
		return 0, false, errors.ErrInternalError
	}
	if !available {
		return 0, false, errors.ErrOrganizationSlugTaken
	}

	switch mode {
	case organizationCreateModeAdmin:
		if !h.permissionHandler.HasSitePermission(ctx, user.UserType, permission.PermissionAdminCreateOrganization) {
			return 0, false, errors.ErrInsufficientPermissions
		}
	case organizationCreateModeUser:
		canCreate, _ := h.permissionHandler.HasOrgPermission(
			ctx,
			user.UserType,
			proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED,
			permission.PermissionOrganizationCreate,
			0,
		)
		if !canCreate {
			return 0, false, errors.ErrInsufficientPermissions
		}
	default:
		return 0, false, errors.ErrInvalidRequest
	}

	autoAddCreator := mode == organizationCreateModeUser
	if organization.ParentOrganizationID == nil {
		organization.Lineage = nil
		return 0, autoAddCreator, nil
	}
	if *organization.ParentOrganizationID == 0 {
		return 0, false, errors.ErrOrganizationParentInvalid
	}

	resolvedParent, err := h.resolveOrganization(ctx, *organization.ParentOrganizationID)
	if err != nil {
		if stdErrors.Is(err, errors.ErrOrganizationNotFound) {
			return 0, false, errors.ErrOrganizationParentNotFound
		}
		if stdErrors.Is(err, errors.ErrInvalidOrganizationID) {
			return 0, false, errors.ErrOrganizationParentInvalid
		}
		return 0, false, err
	}

	if ownerID, personal := personalOwnerUserID(resolvedParent.Organization); personal {
		if requestingUser.GetID() != ownerID {
			return 0, false, errors.ErrPersonalWorkspaceOwnerRequired
		}
		lockedParent, lockErr := h.lockPersonalWorkspaceParentForCreate(ctx, resolvedParent.Organization, ownerID)
		if lockErr != nil {
			return 0, false, lockErr
		}
		resolvedParent.Organization = lockedParent
		inheritedOwnerID := ownerID
		organization.PersonalOwnerUserID = &inheritedOwnerID
		organization.OrganizationType = h.config.PersonalWorkspaceOrganizationType
		autoAddCreator = true
	}

	childDepth := organizationDepth(resolvedParent.Organization) + 1
	if childDepth > runtimeConfig.OrgHierarchyMaxDepth() {
		return 0, false, errors.ErrOrganizationHierarchyDepthExceeded
	}

	actorType, actorRole, _, memberErr := h.getActorOrgMembership(ctx, requestingUser, *organization.ParentOrganizationID)
	if memberErr != nil {
		return 0, false, errors.ErrInternalError
	}
	canCreateChild, permErr := h.permissionHandler.HasOrgPermission(
		ctx,
		actorType,
		actorRole,
		permission.PermissionOrganizationCreateChild,
		childDepth,
	)
	if permErr != nil {
		return 0, false, errors.ErrInternalError
	}
	if !canCreateChild {
		return 0, false, errors.ErrOrganizationCreateChildNotAllowed
	}
	if err := h.ensureOrganizationTypeCapabilityForOrganization(resolvedParent.Organization, organizationTypeCapabilityCreateChildOrgs); err != nil {
		return 0, false, err
	}

	lineage := append([]common_service_models.OrganizationIdType{}, resolvedParent.Organization.GetLineage()...)
	lineage = append(lineage, resolvedParent.Organization.ID)
	organization.Lineage = lineage

	if common_service_models.IsPersonalOrganization(organization) {
		autoAddCreator = true
	} else if mode == organizationCreateModeUser {
		autoAddCreator = boolAtDepth(h.collectionPolicy(runtimeConfig).creatorAutoAdd, childDepth)
	}

	return childDepth, autoAddCreator, nil
}

// createOrganizationInternal persists an already validated and prepared organization record and returns the corresponding service response.
//
// Parameters:
//   - ctx: Request context used for persistence and conversion.
//   - organization: Validated organization payload ready to persist.
//
// Returns:
//   - *OrganizationResponse: Persisted organization response.
//   - error: Persistence or conversion error.
func (h *OrganizationHandler[U]) createOrganizationInternal(
	ctx context.Context,
	organization *common_service_models.Organization,
) (*OrganizationResponse, error) {
	// Create persistence model using converter
	now := time.Now().UTC()
	// Set timestamps for new organization
	organization.CreatedAt = now
	organization.UpdatedAt = now

	persistenceOrg := h.converter.ServiceOrganizationToPersistence(organization)

	// Create organization in persistence layer
	err := h.persistenceHandler.CreateOrganization(ctx, persistenceOrg)
	if err != nil {
		return nil, errors.ErrInternalError
	}

	// Convert to service model and create response
	serviceOrg := h.converter.PersistenceOrganizationToService(persistenceOrg)
	response := &OrganizationResponse{
		Organization: serviceOrg,
	}

	return response, nil
}

// hasAmbientTransaction reports whether the request carries a usable database
// transaction for the same concrete user type as the organization handler.
// Transactional reads must observe their transaction's snapshot and must never
// read from or populate process-wide caches.
func hasAmbientTransaction(ctx context.Context) bool {
	tx, ok := scontext.GetTransaction(ctx)
	return ok && tx != nil && tx.GetDB() != nil
}

// getOrganizationByIDCached returns the organization row from the read cache,
// falling back to persistence and populating the cache on a miss. Only the
// read-only lookup paths use this; mutation flows must read fresh rows.
func (h *OrganizationHandler[U]) getOrganizationByIDCached(
	ctx context.Context,
	id common_service_models.OrganizationIdType,
) (*common_persistence_models.Organization, error) {
	inTransaction := hasAmbientTransaction(ctx)
	cacheable := h.orgByIDCache != nil && !inTransaction
	var generation uint64
	if cacheable {
		cached, found, observed := h.orgByIDCache.Lookup(id)
		if found {
			return cached, nil
		}
		generation = observed
	}
	org, err := h.persistenceHandler.GetOrganizationByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if cacheable && org != nil {
		// Row and slug mapping fill under one generation check so an
		// invalidation cannot interleave between the two writes.
		h.fillIfCurrent(generation, func() {
			h.orgByIDCache.Set(id, org, ttlcache.DefaultTTL)
			h.orgSlugIDCache.Set(org.Slug, id, ttlcache.DefaultTTL)
		})
	}
	return org, nil
}

// getOrganizationBySlugCached resolves a slug through the slug→ID mapping and
// the ID cache. A stale mapping (slug renamed since being cached) is detected
// by re-checking the row's slug and falls back to a fresh lookup.
func (h *OrganizationHandler[U]) getOrganizationBySlugCached(
	ctx context.Context,
	slug string,
) (*common_persistence_models.Organization, error) {
	inTransaction := hasAmbientTransaction(ctx)
	cacheable := h.orgSlugIDCache != nil && !inTransaction
	if cacheable {
		mappedID, found, _ := h.orgSlugIDCache.Lookup(slug)
		if found {
			org, err := h.getOrganizationByIDCached(ctx, mappedID)
			if err == nil && org != nil && org.Slug == slug {
				return org, nil
			}
			h.orgSlugIDCache.Drop(slug)
		}
	}
	var generation uint64
	if cacheable {
		generation = h.orgSlugIDCache.Generation()
	}
	org, err := h.persistenceHandler.GetOrganizationBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	if cacheable && org != nil {
		h.fillIfCurrent(generation, func() {
			h.orgByIDCache.Set(common_service_models.OrganizationIdType(org.ID), org, ttlcache.DefaultTTL)
			h.orgSlugIDCache.Set(slug, common_service_models.OrganizationIdType(org.ID), ttlcache.DefaultTTL)
		})
	}
	return org, nil
}

// invalidateOrganizationCache drops the cached row for an organization and any
// of the given slug mappings after a mutation on this instance. Mutations made
// elsewhere (admin persistence paths, other instances) converge after TTL.
// The row and slug caches are created together, so one nil check covers both.
func (h *OrganizationHandler[U]) invalidateOrganizationCache(id common_service_models.OrganizationIdType, slugs ...string) {
	if h.orgByIDCache == nil {
		return
	}
	h.cacheMu.Lock()
	defer h.cacheMu.Unlock()
	h.cacheGeneration++
	h.orgByIDCache.Delete(id)
	for _, slug := range slugs {
		if slug != "" {
			h.orgSlugIDCache.Delete(slug)
		}
	}
}

// invalidateDeletedOrganizationCaches clears metadata after deletion commits.
// A subtree snapshot taken before deletion can miss concurrently added children,
// so deletion conservatively clears all metadata under one generation change.
func (h *OrganizationHandler[U]) invalidateDeletedOrganizationCaches() {
	h.cacheMu.Lock()
	defer h.cacheMu.Unlock()
	h.cacheGeneration++
	if h.orgByIDCache != nil {
		h.orgByIDCache.DeleteAll()
	}
	if h.orgSlugIDCache != nil {
		h.orgSlugIDCache.DeleteAll()
	}
	if h.orgDepthCache != nil {
		h.orgDepthCache.DeleteAll()
	}
	if h.orgEmailDomainCache != nil {
		h.orgEmailDomainCache.DeleteAll()
	}
}

func (h *OrganizationHandler[U]) invalidateOrganizationEmailDomainCache(domains ...string) {
	keys := make([]string, 0, len(domains))
	for _, domain := range domains {
		if domain != "" {
			keys = append(keys, domain)
		}
	}
	h.orgEmailDomainCache.Invalidate(keys...)
}

// GetOrganization retrieves an organization by ID with proper authorization.
func (h *OrganizationHandler[U]) GetOrganization(
	ctx context.Context,
	id common_service_models.OrganizationIdType,
) (*OrganizationResponse, error) {
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Uint64(appLogger.KeyOrganizationID, uint64(id)),
	)
	log.Debug("GetOrganization called")

	// Get user from request context
	user, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, appLogger.InvariantError(
			"organization_request_actor_in_context",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "get_organization"),
			zap.String(logkeys.Stage, "resolve_actor"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(id)),
			zap.String(logkeys.Expected, "authenticated actor in context"),
			zap.String(logkeys.Actual, "actor absent"),
		)
	}

	// Authorization check - delegate to permission handler
	canView, err := h.CanViewOrganization(ctx, id)
	if err != nil {
		return nil, err
	}
	if !canView {
		return nil, appLogger.WithErrorFields(errors.ErrOrganizationNotFound,
			zap.String(logkeys.Operation, "get_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*user).GetID())),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(id)),
			zap.String(logkeys.Reason, "insufficient_permission"),
		) // Prevent enumeration
	}

	// Get organization (read cache backed by the persistence layer)
	persistenceOrg, err := h.getOrganizationByIDCached(ctx, id)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return nil, errors.ErrOrganizationNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "get_organization"),
			zap.String(logkeys.Stage, "load_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*user).GetID())),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(id)),
		)
	}

	// Get member count
	memberCount, err := h.persistenceHandler.GetOrganizationMemberCount(ctx, id)
	if err != nil {
		log.Warn("failed to get member count",
			zap.String(logkeys.Operation, "get_organization"),
			zap.String(logkeys.Stage, "load_member_count"),
			zap.String(logkeys.Fallback, "member_count_zero"),
			zap.Error(err))
		memberCount = 0 // Default to 0 on error
	}

	// Convert to service model and create response
	serviceOrg := h.converter.PersistenceOrganizationToService(persistenceOrg)
	response := &OrganizationResponse{
		Organization: serviceOrg,
		MemberCount:  memberCount,
	}

	log.Debug("organization retrieved successfully")
	return response, nil
}

// GetOrganizationBySlug retrieves an organization by slug with proper authorization.
func (h *OrganizationHandler[U]) GetOrganizationBySlug(
	ctx context.Context,
	slug string,
) (*OrganizationResponse, error) {
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.String(appLogger.KeySlug, slug),
	)
	log.Debug("GetOrganizationBySlug called")

	// Length bounds are creation policy, not lookup policy. A hot-reload that
	// tightens those bounds must not make an existing organization unreadable.
	slug = strings.ToLower(strings.TrimSpace(slug))

	if !isValidOrganizationSlugSyntax(slug) {
		return nil, errors.ErrInvalidOrganizationSlug
	}

	// Get user from request context for authorization
	user, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, appLogger.InvariantError(
			"organization_request_actor_in_context",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "get_organization_by_slug"),
			zap.String(logkeys.Stage, "resolve_actor"),
			zap.String(appLogger.KeySlug, slug),
			zap.String(logkeys.Expected, "authenticated actor in context"),
			zap.String(logkeys.Actual, "actor absent"),
		)
	}

	// Get organization first (needed for authorization); served from the read
	// cache when possible.
	persistenceOrg, err := h.getOrganizationBySlugCached(ctx, slug)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return nil, errors.ErrOrganizationNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "get_organization_by_slug"),
			zap.String(logkeys.Stage, "load_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*user).GetID())),
			zap.String(appLogger.KeySlug, slug),
		)
	}

	// Authorization check - delegate to permission handler
	canView, err := h.CanViewOrganization(ctx, common_service_models.OrganizationIdType(persistenceOrg.ID))
	if err != nil {
		return nil, err
	}
	if !canView {
		return nil, appLogger.WithErrorFields(errors.ErrOrganizationNotFound,
			zap.String(logkeys.Operation, "get_organization_by_slug"),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*user).GetID())),
			zap.Int64(appLogger.KeyOrganizationID, persistenceOrg.ID),
			zap.String(appLogger.KeySlug, slug),
			zap.String(logkeys.Reason, "insufficient_permission"),
		) // Prevent enumeration
	}

	// Get member count
	memberCount, err := h.persistenceHandler.GetOrganizationMemberCount(ctx, persistenceOrg.ID)
	if err != nil {
		log.Warn("failed to get member count",
			zap.String(logkeys.Operation, "get_organization_by_slug"),
			zap.String(logkeys.Stage, "load_member_count"),
			zap.Int64(appLogger.KeyOrganizationID, persistenceOrg.ID),
			zap.String(logkeys.Fallback, "member_count_zero"),
			zap.Error(err))
		memberCount = 0 // Default to 0 on error
	}

	// Convert to service model and create response
	serviceOrg := h.converter.PersistenceOrganizationToService(persistenceOrg)
	response := &OrganizationResponse{
		Organization: serviceOrg,
		MemberCount:  memberCount,
	}

	log.Debug("organization retrieved successfully by slug")
	return response, nil
}

// GetOrganizationDepth returns the hierarchy depth of an organization.
//
// Parameters:
//   - ctx: Request context used for persistence and cache lookup.
//   - id: Organization ID whose depth should be resolved.
//
// Returns:
//   - int: Organization depth derived from its lineage.
//   - error: Validation or persistence error.
func (h *OrganizationHandler[U]) GetOrganizationDepth(
	ctx context.Context,
	id common_service_models.OrganizationIdType,
) (int, error) {
	if id == 0 {
		return 0, errors.ErrInvalidOrganizationID
	}

	depth, err := h.getOrgDepth(ctx, id)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return 0, errors.ErrOrganizationNotFound
		}
		if stdErrors.Is(err, errors.ErrInvalidOrganizationID) {
			return 0, errors.ErrInvalidOrganizationID
		}
		return 0, errors.ErrInternalError
	}
	return depth, nil
}

// UpdateOrganization updates an existing organization.
func (h *OrganizationHandler[U]) UpdateOrganization(
	ctx context.Context,
	organization *common_service_models.Organization,
) (*OrganizationResponse, error) {
	if organization == nil {
		return nil, appLogger.InvariantError(
			"organization_update_payload_non_nil",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "update_organization"),
			zap.String(logkeys.Stage, "validate_payload"),
			zap.String(logkeys.Expected, "non-nil organization payload"),
			zap.String(logkeys.Actual, "nil organization payload"),
		)
	}
	if organization.PersonalOwnerUserID != nil {
		return nil, errors.ErrPersonalWorkspaceOwnershipImmutable
	}
	// Get actor from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, errors.ErrUserNotFound
	}
	actorID := (*requestingUser).GetID()

	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
		zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
	)
	log.Debug("UpdateOrganization called")

	// Validate fields that are independent of the current creation policy. Slug
	// length bounds are hot configuration, so they are applied below only when
	// the normalized slug actually changes. Otherwise tightening those bounds
	// would prevent unrelated edits to an organization that was valid when it
	// was created.
	if err := h.validateOrganizationUpdateShape(ctx, organization); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "update_organization"),
			zap.String(logkeys.Stage, "validate_payload"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
			zap.String(appLogger.KeySlug, organization.Slug),
		)
	}

	// Authorization check - delegate to permission handler
	canUpdate, err := h.CanUpdateOrganization(ctx, organization.ID)
	if err != nil {
		return nil, err
	}
	if !canUpdate {
		return nil, appLogger.WithErrorFields(errors.ErrInsufficientPermissions,
			zap.String(logkeys.Operation, "update_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
			zap.String(logkeys.Reason, "insufficient_permission"),
		)
	}

	// Get existing organization
	existingOrg, err := h.persistenceHandler.GetOrganizationByID(ctx, organization.ID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return nil, errors.ErrOrganizationNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "update_organization"),
			zap.String(logkeys.Stage, "load_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
		)
	}

	previousSlug := existingOrg.Slug
	existingServiceOrg := h.converter.PersistenceOrganizationToService(existingOrg)
	isPersonal := common_service_models.IsPersonalOrganization(existingServiceOrg)
	isPersonalRoot := common_service_models.IsPersonalWorkspaceRoot(existingServiceOrg)
	if h.isReservedPersonalWorkspaceSlug(organization.Slug) && !(isPersonal && organization.Slug == existingOrg.Slug) {
		return nil, errors.ErrOrganizationSlugReserved
	}
	// UpdateOrganizationRequest uses an optional description, but the service
	// model predates presence tracking. For managed personal roots, an empty
	// value is therefore treated as omitted and the retained description is
	// copied forward; non-empty attempts to change it remain rejected.
	if isPersonalRoot && organization.Description == "" {
		organization.Description = persistenceOrganizationDescription(existingOrg)
	}
	if isPersonalRoot && (organization.Slug != existingOrg.Slug || organization.Description != persistenceOrganizationDescription(existingOrg)) {
		return nil, errors.ErrPersonalWorkspaceManagedFieldsImmutable
	}

	// Check if slug is changing and if new slug is available
	if organization.Slug != existingOrg.Slug {
		if !h.isValidSlug(organization.Slug) {
			return nil, errors.ErrInvalidOrganizationSlug
		}
		available, err := h.IsSlugAvailable(ctx, organization.Slug, organization.ID)
		if err != nil {
			return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
				zap.String(logkeys.Operation, "update_organization"),
				zap.String(logkeys.Stage, "check_slug_availability"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
				zap.String(appLogger.KeySlug, organization.Slug),
			)
		}
		if !available {
			return nil, appLogger.WithErrorFields(errors.ErrOrganizationSlugTaken,
				zap.String(logkeys.Operation, "update_organization"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
				zap.String(appLogger.KeySlug, organization.Slug),
				zap.String(logkeys.Reason, "slug_taken"),
			)
		}
	}

	if organization.ParentOrganizationID != nil {
		existingParent := existingOrg.ParentOrganizationID
		if existingParent == nil || *organization.ParentOrganizationID != common_service_models.OrganizationIdType(*existingParent) {
			return nil, appLogger.WithErrorFields(errors.ErrOrganizationParentImmutable,
				zap.String(logkeys.Operation, "update_organization"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
				zap.String(logkeys.Reason, "immutable_parent_change"),
			)
		}
	}
	if organization.Lineage != nil {
		existingLineage := existingOrg.Lineage
		if len(organization.Lineage) != len(existingLineage) {
			return nil, appLogger.WithErrorFields(errors.ErrOrganizationLineageImmutable,
				zap.String(logkeys.Operation, "update_organization"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
				zap.String(logkeys.Reason, "immutable_lineage_change"),
			)
		}
		for i := range organization.Lineage {
			if organization.Lineage[i] != existingLineage[i] {
				return nil, appLogger.WithErrorFields(errors.ErrOrganizationLineageImmutable,
					zap.String(logkeys.Operation, "update_organization"),
					zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
					zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
					zap.String(logkeys.Reason, "immutable_lineage_change"),
				)
			}
		}
	}

	// Update organization
	existingOrg.Name = organization.Name
	existingOrg.Slug = organization.Slug
	var description *string
	if organization.Description != "" {
		description = &organization.Description
	}
	existingOrg.Description = description
	existingOrg.UpdatedAt = time.Now().UTC()

	err = h.persistenceHandler.UpdateOrganization(ctx, existingOrg)
	if err != nil {
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "update_organization"),
			zap.String(logkeys.Stage, "persist_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
		)
	}

	h.invalidateOrganizationCache(organization.ID, previousSlug, existingOrg.Slug)

	// Get member count
	memberCount, err := h.persistenceHandler.GetOrganizationMemberCount(ctx, organization.ID)
	if err != nil {
		log.Warn("failed to get member count",
			zap.String(logkeys.Operation, "update_organization"),
			zap.String(logkeys.Stage, "load_member_count"),
			zap.String(logkeys.Fallback, "member_count_zero"),
			zap.Error(err))
		memberCount = 0 // Default to 0 on error
	}

	// Convert to service model and create response
	serviceOrg := h.converter.PersistenceOrganizationToService(existingOrg)
	response := &OrganizationResponse{
		Organization: serviceOrg,
		MemberCount:  memberCount,
	}

	if err := h.invokeOnOrganizationUpdated(ctx, response.Organization); err != nil {
		return nil, appLogger.WithErrorFields(
			fmt.Errorf("%w: %w", errors.ErrOrganizationCallbackFailed, err),
			zap.String(logkeys.Operation, "update_organization"),
			zap.String(logkeys.Stage, "organization_updated_callback"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(organization.ID)),
		)
	}

	log.Info("organization updated successfully")
	return response, nil
}

// DeleteOrganization deletes an organization and invalidates caches after commit.
// Caller-managed ambient transactions must support and run post-commit hooks.
func (h *OrganizationHandler[U]) DeleteOrganization(
	ctx context.Context,
	id common_service_models.OrganizationIdType,
) error {
	// Get actor from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return errors.ErrUserNotFound
	}
	actorID := (*requestingUser).GetID()

	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
		zap.Uint64(appLogger.KeyOrganizationID, uint64(id)),
	)
	log.Debug("DeleteOrganization called")

	// Authorization check - delegate to permission handler
	canDelete, err := h.CanDeleteOrganization(ctx, id)
	if err != nil {
		return err
	}
	if !canDelete {
		return appLogger.WithErrorFields(errors.ErrInsufficientPermissions,
			zap.String(logkeys.Operation, "delete_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(id)),
			zap.String(logkeys.Reason, "insufficient_permission"),
		)
	}

	existingOrg, err := h.persistenceHandler.GetOrganizationByID(ctx, id)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return errors.ErrOrganizationNotFound
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "delete_organization"),
			zap.String(logkeys.Stage, "load_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(id)),
		)
	}
	// Only the managed personal workspace root is protected. Sub-organizations
	// a user created inside their workspace remain deletable through the
	// ordinary policy-gated path, with the usual membership cascade.
	if common_service_models.IsPersonalWorkspaceRoot(h.converter.PersistenceOrganizationToService(existingOrg)) {
		return errors.ErrPersonalWorkspaceDeletionRestricted
	}

	// Reject unsupported ambient transactions before callbacks or persistence
	// mutate anything. Successful nested persistence writes have not committed.
	postCommitHooks, canDefer := persistence.DatabasePostCommitHookRegistrarFromContext(ctx)
	if hasAmbientTransaction(ctx) && !canDefer {
		return appLogger.WithErrorFields(errors.ErrTransactionNotFound,
			zap.String(logkeys.Operation, "delete_organization"),
			zap.String(logkeys.Stage, "require_post_commit_hooks"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(id)),
		)
	}

	if err := h.invokeOnOrganizationDeleted(ctx, id); err != nil {
		return appLogger.WithErrorFields(
			fmt.Errorf("%w: %w", errors.ErrOrganizationCallbackFailed, err),
			zap.String(logkeys.Operation, "delete_organization"),
			zap.String(logkeys.Stage, "organization_deleted_callback"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(id)),
		)
	}

	// Delete organization (should cascade to delete memberships)
	err = h.persistenceHandler.DeleteOrganization(ctx, id)
	if err != nil {
		return appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "delete_organization"),
			zap.String(logkeys.Stage, "persist_deletion"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(id)),
		)
	}

	// The hook only touches process-local memory. Generation changes reject
	// fills from reads that observed the organization before this invalidation.
	invalidate := func(context.Context) error {
		h.InvalidateAllRoleCache()
		h.invalidateDeletedOrganizationCaches()
		if h.realtimeInvalidator != nil {
			h.realtimeInvalidator.InvalidateChatRealtimeAccess(ports.ChatRealtimeAccessInvalidation{})
		}
		return nil
	}
	if canDefer {
		postCommitHooks.AddPostCommitHook(invalidate)
	} else {
		// With no ambient transaction, persistence committed before returning.
		_ = invalidate(ctx)
	}

	log.Info("organization deleted successfully")
	return nil
}

// CheckSlugAvailability checks if an organization slug is available for use.
func (h *OrganizationHandler[U]) CheckSlugAvailability(ctx context.Context, slug string) (available bool, message string, err error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.String(appLogger.KeySlug, slug),
		).Debug("checking slug availability")
	}

	// Use the existing IsSlugAvailable method from permission handler
	available, err = h.IsSlugAvailable(ctx, slug)
	if err != nil {
		// Reserved slugs are a classification result, not a failure: report
		// them through the same unavailable-with-reason shape as taken slugs.
		// The create and update paths keep hard-rejecting reserved slugs with
		// ErrOrganizationSlugReserved through their own validation.
		if stdErrors.Is(err, errors.ErrOrganizationSlugReserved) {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, organizationLoggerName).With(
					zap.String(appLogger.KeySlug, slug),
				).Debug("slug is reserved")
			}
			return false, "Slug is reserved", nil
		}
		return false, "Unable to check slug availability", appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "check_organization_slug_availability"),
			zap.String(logkeys.Stage, "check_availability"),
			zap.String(appLogger.KeySlug, slug),
		)
	}

	if available {
		message = "Slug is available"
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, organizationLoggerName).With(
				zap.String(appLogger.KeySlug, slug),
			).Debug("slug is available")
		}
	} else {
		message = "Slug is already taken"
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, organizationLoggerName).With(
				zap.String(appLogger.KeySlug, slug),
			).Debug("slug is not available")
		}
	}

	return available, message, nil
}

// ListAllOrganizations retrieves a paginated list of ALL organizations in the system.
// This is an admin-only operation that requires appropriate admin user type: it
// lists ALL organizations regardless of user membership.
func (h *OrganizationHandler[U]) ListAllOrganizations(
	ctx context.Context,
	limit, offset int,
	filter *proto_models.FilterRequest,
	organizationTypeFilter proto_models.OrganizationType,
	parentOrganizationIDFilter *common_service_models.OrganizationIdType,
	depthFilter *uint32,
	sortBy proto_models.AdminListAllOrganizationsSortBy,
	sortReq *proto_models.SortRequest,
	includeTotalCount bool,
) ([]*OrganizationResponse, int64, error) {
	// Get admin from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, 0, errors.ErrUserNotFound
	}
	adminID := (*requestingUser).GetID()

	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
		zap.Int(logkeys.Limit, limit),
		zap.Int(appLogger.KeyOffset, offset),
	)
	log.Debug("ListAllOrganizations called")

	// Get user to check their admin type (following established pattern)
	user, err := h.persistenceHandler.GetUserByID(ctx, uint64(adminID))
	if err != nil {
		if stdErrors.Is(err, errors.ErrUserNotFound) {
			return nil, 0, errors.ErrUserNotFound
		}
		return nil, 0, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "list_all_organizations"), zap.String(logkeys.Stage, "load_admin"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)))
	}

	if h.permissionHandler == nil {
		return nil, 0, appLogger.InvariantError("organization_permission_handler_initialized", errors.ErrInternalError, zap.String(logkeys.Operation, "list_all_organizations"), zap.String(logkeys.Stage, "permission_check"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.String(logkeys.Expected, "permission handler configured"), zap.String(logkeys.Actual, "nil permission handler"))
	}

	if !h.permissionHandler.HasSitePermission(ctx, user.UserType, permission.PermissionAdminListOrganizations) {
		return nil, 0, appLogger.WithErrorFields(errors.ErrInsufficientPermissions, zap.String(logkeys.Operation, "list_all_organizations"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.Stringer(appLogger.KeyUserType, user.UserType), zap.String(logkeys.Reason, "insufficient_permission"))
	}

	if err := requireListLimit("list_all_organizations", limit); err != nil {
		return nil, 0, err
	}
	if offset < 0 {
		offset = 0
	}

	search := ""
	if filter != nil {
		search = strings.TrimSpace(filter.Search)
	}
	var parentOrganizationIDFilterValue *int64
	if parentOrganizationIDFilter != nil {
		value := int64(*parentOrganizationIDFilter)
		parentOrganizationIDFilterValue = &value
	}

	persistenceOrgs, total, err := h.persistenceHandler.ListAdminOrganizations(ctx, &persistence.AdminOrganizationsQuery{
		Offset:                     offset,
		Limit:                      limit,
		Search:                     search,
		OrganizationTypeFilter:     organizationTypeFilter,
		ParentOrganizationIDFilter: parentOrganizationIDFilterValue,
		DepthFilter:                depthFilter,
		SortColumn:                 adminListAllOrganizationsOrderColumn(sortBy),
		SortDesc:                   common_util.SortDesc(sortReq, true),
		SkipExactCount:             !includeTotalCount,
	})
	if err != nil {
		return nil, 0, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "list_all_organizations"), zap.String(logkeys.Stage, "load_organizations"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.Int(logkeys.Limit, limit), zap.Int(appLogger.KeyOffset, offset))
	}

	// Convert to service models and create responses with batched member counts
	responses := h.buildOrganizationResponses(ctx, log, persistenceOrgs)

	if ce := log.Check(zap.DebugLevel, "admin successfully listed all organizations"); ce != nil {
		ce.Write(
			zap.Int(appLogger.KeyCount, len(responses)),
			zap.Int64(appLogger.KeyTotal, total),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)))
	}

	return responses, total, nil
}

// requireListLimit enforces the list contract for page-sized organization reads:
// the request sanitizer defaults and bounds every page size from configuration,
// and the service passes it through as-is. Organization persistence treats a
// zero limit as "no limit", so a non-positive value here means a caller skipped
// the sanitizer; it is rejected rather than replaced with a page size of our own.
func requireListLimit(operation string, limit int) error {
	if limit > 0 {
		return nil
	}
	return appLogger.InvariantError("organization_list_limit_positive", errors.ErrInvalidRequest,
		zap.String(logkeys.Operation, operation),
		zap.Int(logkeys.Limit, limit))
}

func (h *OrganizationHandler[U]) requireAdminOrganizationListPermission(ctx context.Context) (common_service_models.UserIdType, error) {
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return 0, errors.ErrUserNotFound
	}
	adminID := (*requestingUser).GetID()

	user, err := h.persistenceHandler.GetUserByID(ctx, uint64(adminID))
	if err != nil {
		if stdErrors.Is(err, errors.ErrUserNotFound) {
			return 0, errors.ErrUserNotFound
		}
		return 0, errors.ErrInternalError
	}

	if h.permissionHandler == nil ||
		!h.permissionHandler.HasSitePermission(ctx, user.UserType, permission.PermissionAdminListOrganizations) {
		return 0, errors.ErrInsufficientPermissions
	}

	return adminID, nil
}

// ListAdminOrganizationParentOptions searches a single bounded page of
// organizations that are direct parents of records matching the current table
// filters. One additional record is fetched to determine whether another page
// is available without an exact count query.
func (h *OrganizationHandler[U]) ListAdminOrganizationParentOptions(
	ctx context.Context,
	limit, offset int,
	filter, tableFilter *proto_models.FilterRequest,
	organizationTypeFilter proto_models.OrganizationType,
) ([]*AdminOrganizationFilterParentOption, bool, error) {
	adminID, err := h.requireAdminOrganizationListPermission(ctx)
	if err != nil {
		return nil, false, err
	}

	if err := requireListLimit("list_admin_organization_parent_options", limit); err != nil {
		return nil, false, err
	}
	if offset < 0 {
		offset = 0
	}

	parentSearch := ""
	if filter != nil {
		parentSearch = strings.TrimSpace(filter.Search)
	}
	tableSearch := ""
	if tableFilter != nil {
		tableSearch = strings.TrimSpace(tableFilter.Search)
	}

	parentOptions, err := h.persistenceHandler.ListAdminOrganizationParentOptions(ctx, &persistence.AdminOrganizationParentOptionsQuery{
		Offset:                 offset,
		Limit:                  common_util.QueryLimitWithProbe(limit),
		ParentSearch:           parentSearch,
		TableSearch:            tableSearch,
		OrganizationTypeFilter: organizationTypeFilter,
	})
	if err != nil {
		return nil, false, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "list_admin_organization_parent_options"), zap.String(logkeys.Stage, "load_parent_options"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.Int(logkeys.Limit, limit), zap.Int(appLogger.KeyOffset, offset))
	}

	hasMore := len(parentOptions) > limit
	if hasMore {
		parentOptions = parentOptions[:limit]
	}

	options := make([]*AdminOrganizationFilterParentOption, 0, len(parentOptions))
	for _, option := range parentOptions {
		if option == nil {
			continue
		}
		options = append(options, &AdminOrganizationFilterParentOption{
			ID:   common_service_models.OrganizationIdType(option.ID),
			Name: option.Name,
			Slug: option.Slug,
		})
	}

	return options, hasMore, nil
}

// GetAdminOrganizationParentOptionsByIDs resolves only the parent references
// present on the current organization-table page.
func (h *OrganizationHandler[U]) GetAdminOrganizationParentOptionsByIDs(
	ctx context.Context,
	organizationIDs []common_service_models.OrganizationIdType,
) ([]*AdminOrganizationFilterParentOption, error) {
	if len(organizationIDs) == 0 {
		return []*AdminOrganizationFilterParentOption{}, nil
	}
	if _, err := h.requireAdminOrganizationListPermission(ctx); err != nil {
		return nil, err
	}

	maxIDs := h.runtimeConfig().MaxPaginationLimit()
	uniqueIDs := make([]int64, 0, len(organizationIDs))
	seen := make(map[int64]struct{}, len(organizationIDs))
	for _, organizationID := range organizationIDs {
		id := int64(organizationID)
		if id <= 0 {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		uniqueIDs = append(uniqueIDs, id)
		if len(uniqueIDs) == maxIDs {
			break
		}
	}
	if len(uniqueIDs) == 0 {
		return []*AdminOrganizationFilterParentOption{}, nil
	}

	organizations, err := h.persistenceHandler.GetOrganizationsByIDs(ctx, uniqueIDs)
	if err != nil {
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "get_admin_organization_parent_options_by_ids"), zap.String(logkeys.Stage, "load_organizations"), zap.Int(appLogger.KeyCount, len(uniqueIDs)))
	}

	options := make([]*AdminOrganizationFilterParentOption, 0, len(organizations))
	for _, organization := range organizations {
		if organization == nil {
			continue
		}
		options = append(options, &AdminOrganizationFilterParentOption{
			ID:   common_service_models.OrganizationIdType(organization.ID),
			Name: organization.Name,
			Slug: organization.Slug,
		})
	}
	sort.Slice(options, func(i, j int) bool {
		if options[i].Name == options[j].Name {
			return options[i].ID < options[j].ID
		}
		return options[i].Name < options[j].Name
	})

	return options, nil
}

func (h *OrganizationHandler[U]) GetAdminOrganizationFilterMetadata(
	ctx context.Context,
	filter *proto_models.FilterRequest,
	organizationTypeFilter proto_models.OrganizationType,
) (*AdminOrganizationFilterMetadata, error) {
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, errors.ErrUserNotFound
	}
	adminID := (*requestingUser).GetID()

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
		).Debug("GetAdminOrganizationFilterMetadata called")
	}

	user, err := h.persistenceHandler.GetUserByID(ctx, uint64(adminID))
	if err != nil {
		if stdErrors.Is(err, errors.ErrUserNotFound) {
			return nil, errors.ErrUserNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "get_admin_organization_filter_metadata"), zap.String(logkeys.Stage, "load_admin"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)))
	}

	if h.permissionHandler == nil {
		return nil, appLogger.InvariantError("organization_permission_handler_initialized", errors.ErrInternalError, zap.String(logkeys.Operation, "get_admin_organization_filter_metadata"), zap.String(logkeys.Stage, "permission_check"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.String(logkeys.Expected, "permission handler configured"), zap.String(logkeys.Actual, "nil permission handler"))
	}

	if !h.permissionHandler.HasSitePermission(ctx, user.UserType, permission.PermissionAdminListOrganizations) {
		return nil, appLogger.WithErrorFields(errors.ErrInsufficientPermissions, zap.String(logkeys.Operation, "get_admin_organization_filter_metadata"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.Stringer(appLogger.KeyUserType, user.UserType), zap.String(logkeys.Reason, "insufficient_permission"))
	}

	search := ""
	if filter != nil {
		search = strings.TrimSpace(filter.Search)
	}

	metadata, err := h.persistenceHandler.GetAdminOrganizationFilterMetadata(ctx, search, organizationTypeFilter)
	if err != nil {
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "get_admin_organization_filter_metadata"), zap.String(logkeys.Stage, "load_metadata"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)))
	}
	if metadata == nil {
		return &AdminOrganizationFilterMetadata{}, nil
	}

	return &AdminOrganizationFilterMetadata{
		MaxDepth: metadata.MaxDepth,
	}, nil
}

// ChangeOrganizationType changes an organization's type through the admin surface.
//
// Parameters:
//   - ctx: Request context used for authorization and persistence.
//   - organizationID: Organization whose type should be changed.
//   - newType: New organization type to assign.
//   - reason: Audit reason describing the type change.
//
// Returns:
//   - error: Validation, authorization, or persistence error.
func (h *OrganizationHandler[U]) ChangeOrganizationType(
	ctx context.Context,
	organizationID common_service_models.OrganizationIdType,
	newType proto_models.OrganizationType,
	reason string,
) error {
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return errors.ErrUserNotFound
	}
	adminID := (*requestingUser).GetID()

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(organizationID)),
			zap.Stringer(appLogger.KeyNewOrganizationType, newType),
			zap.String(logkeys.Reason, reason),
		).Debug("ChangeOrganizationType called")
	}

	if organizationID == 0 {
		return errors.ErrInvalidOrganizationID
	}
	if newType == proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED {
		return errors.ErrInvalidRequest
	}

	user, err := h.persistenceHandler.GetUserByID(ctx, uint64(adminID))
	if err != nil {
		if stdErrors.Is(err, errors.ErrUserNotFound) {
			return errors.ErrUserNotFound
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "change_organization_type"), zap.String(logkeys.Stage, "load_admin"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.Uint64(appLogger.KeyOrganizationID, uint64(organizationID)))
	}
	if h.permissionHandler == nil {
		return errors.ErrInsufficientPermissions
	}
	if !h.permissionHandler.HasSitePermission(ctx, user.UserType, permission.PermissionAdminCreateOrganization) {
		return errors.ErrInsufficientPermissions
	}

	org, err := h.persistenceHandler.GetOrganizationByID(ctx, organizationID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return errors.ErrOrganizationNotFound
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "change_organization_type"), zap.String(logkeys.Stage, "load_organization"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.Uint64(appLogger.KeyOrganizationID, uint64(organizationID)))
	}

	// Personal organizations carry a system-managed type; changing it would
	// silently diverge the managed tier, so the whole tree is immutable here.
	if common_service_models.IsPersonalOrganization(h.converter.PersistenceOrganizationToService(org)) {
		return appLogger.WithErrorFields(errors.ErrPersonalWorkspaceManagedFieldsImmutable, zap.String(logkeys.Operation, "change_organization_type"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.Uint64(appLogger.KeyOrganizationID, uint64(organizationID)), zap.String(logkeys.Reason, "personal_workspace_type_immutable"))
	}

	// Always perform the update rather than short-circuiting on apparent no-op.
	// GetOrganizationByID normalizes UNSPECIFIED to BUSINESS_TIER_0, which would
	// hide the fact that the raw stored value still needs repair. The persistence
	// layer normalizes before writing, so true no-ops are harmless.

	if err := h.persistenceHandler.UpdateOrganizationType(ctx, organizationID, newType); err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return errors.ErrOrganizationNotFound
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "change_organization_type"), zap.String(logkeys.Stage, "persist_type"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.Uint64(appLogger.KeyOrganizationID, uint64(organizationID)), zap.Stringer(appLogger.KeyNewOrganizationType, newType))
	}

	org.OrganizationType = newType
	if err := h.invokeOnOrganizationUpdated(ctx, h.converter.PersistenceOrganizationToService(org)); err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("%w: %w", errors.ErrOrganizationCallbackFailed, err), zap.String(logkeys.Operation, "change_organization_type"), zap.String(logkeys.Stage, "organization_updated_callback"), zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)), zap.Uint64(appLogger.KeyOrganizationID, uint64(organizationID)), zap.Stringer(appLogger.KeyNewOrganizationType, newType))
	}

	return nil
}

// ListOrganizationsByParent retrieves organizations under a given parent (or roots when parent is nil).
func (h *OrganizationHandler[U]) ListOrganizationsByParent(
	ctx context.Context,
	parentOrganizationID *common_service_models.OrganizationIdType,
	limit, offset int,
) ([]*OrganizationResponse, int64, error) {
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Int(logkeys.Limit, limit),
		zap.Int(appLogger.KeyOffset, offset),
	)
	if parentOrganizationID != nil {
		log = log.With(zap.Int64(appLogger.KeyOrganizationID, *parentOrganizationID))
	}
	log.Debug("ListOrganizationsByParent called")

	// Get user from request context
	user, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, 0, appLogger.InvariantError("organization_request_actor_in_context", errors.ErrUserNotFound, zap.String(logkeys.Operation, "list_organizations_by_parent"), zap.String(logkeys.Stage, "resolve_actor"), zap.String(logkeys.Expected, "authenticated actor in context"), zap.String(logkeys.Actual, "actor absent"))
	}
	userID := (*user).GetID()
	userType := (*user).GetUserType()

	canListAllOrganizations, permissionErr := h.CanListAllOrganizations(ctx, userType)
	if permissionErr != nil {
		return nil, 0, appLogger.WithErrorCause(errors.ErrInternalError, permissionErr, zap.String(logkeys.Operation, "list_organizations_by_parent"), zap.String(logkeys.Stage, "permission_check"), zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	if err := requireListLimit("list_organizations_by_parent", limit); err != nil {
		return nil, 0, err
	}
	if offset < 0 {
		offset = 0
	}

	// Convert parent ID to persistence type
	var parentID *int64
	if parentOrganizationID != nil {
		value := int64(*parentOrganizationID)
		parentID = &value
	}

	var (
		persistenceOrgs []*common_persistence_models.Organization
		total           int64
		err             error
	)
	if canListAllOrganizations {
		persistenceOrgs, total, err = h.persistenceHandler.GetOrganizationsByParent(ctx, parentID, offset, limit)
	} else {
		persistenceOrgs, total, err = h.persistenceHandler.GetOrganizationsByParentForUser(ctx, uint64(userID), parentID, offset, limit)
	}
	if err != nil {
		return nil, 0, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "list_organizations_by_parent"), zap.String(logkeys.Stage, "load_organizations"), zap.Uint64(appLogger.KeyUserID, uint64(userID)), zap.Int(logkeys.Limit, limit), zap.Int(appLogger.KeyOffset, offset))
	}

	// Convert to service models and create responses with batched member counts
	responses := h.buildOrganizationResponses(ctx, log, persistenceOrgs)

	return responses, total, nil
}

// GetOrganizationHierarchy returns an organization hierarchy node with immediate children or full descendants, depending on the request.
//
// Parameters:
//   - ctx: Request context used for authorization and persistence.
//   - organizationID: Root organization for the hierarchy query.
//   - includeDescendants: Whether to recursively include all descendants.
//
// Returns:
//   - *common_service_models.OrganizationHierarchyNode: Hierarchy tree rooted at the requested organization.
//   - error: Validation, authorization, or persistence error.
func (h *OrganizationHandler[U]) GetOrganizationHierarchy(
	ctx context.Context,
	organizationID common_service_models.OrganizationIdType,
	includeDescendants bool,
) (*common_service_models.OrganizationHierarchyNode, error) {
	result, err := h.GetOrganizationHierarchyPage(
		ctx,
		organizationID,
		includeDescendants,
		1,
		// This convenience read has no request sanitizer in front of it, so it
		// sizes its one page from default_pagination_limit itself.
		h.runtimeConfig().DefaultPaginationLimit(),
		0,
	)
	if err != nil {
		return nil, err
	}
	return result.Hierarchy, nil
}

// GetOrganizationHierarchyPage returns a bounded hierarchy response. Immediate
// children use standard page/limit semantics; full descendant trees use the
// configured defensive node cap and a limit+one truncation probe.
func (h *OrganizationHandler[U]) GetOrganizationHierarchyPage(
	ctx context.Context,
	organizationID common_service_models.OrganizationIdType,
	includeDescendants bool,
	page uint32,
	limit, offset int,
) (*OrganizationHierarchyResult, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
		).Debug("GetOrganizationHierarchyPage called")
	}

	if organizationID == 0 {
		return nil, errors.ErrInvalidOrganizationID
	}

	// Only callers with the explicit site-wide listing permission may use
	// hierarchy queries that are not scoped to their organization memberships.
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, errors.ErrUserNotFound
	}
	userID := (*requestingUser).GetID()
	userType := (*requestingUser).GetUserType()
	canListAllOrganizations, permissionErr := h.CanListAllOrganizations(ctx, userType)
	if permissionErr != nil {
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, permissionErr, zap.String(logkeys.Operation, "get_organization_hierarchy"), zap.String(logkeys.Stage, "permission_check"), zap.Uint64(appLogger.KeyUserID, uint64(userID)), zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}

	if !includeDescendants {
		if err := requireListLimit("get_organization_hierarchy", limit); err != nil {
			return nil, err
		}
		if offset < 0 {
			offset = 0
		}
		if page < 1 {
			page = 1
		}

		if !canListAllOrganizations {
			if _, err := h.persistenceHandler.GetUserExplicitOrganizationRole(ctx, uint64(userID), organizationID); err != nil {
				if stdErrors.Is(err, persistence.ErrUserOrganizationNotFound) ||
					stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
					return nil, errors.ErrOrganizationNotFound
				}
				return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "get_organization_hierarchy"), zap.String(logkeys.Stage, "verify_root_membership"), zap.Uint64(appLogger.KeyUserID, uint64(userID)), zap.Int64(appLogger.KeyOrganizationID, organizationID))
			}
		}
		rootOrg, err := h.persistenceHandler.GetOrganizationByID(ctx, organizationID)
		if err != nil {
			if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
				return nil, errors.ErrOrganizationNotFound
			}
			return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "get_organization_hierarchy"), zap.String(logkeys.Stage, "load_root"), zap.Uint64(appLogger.KeyUserID, uint64(userID)), zap.Int64(appLogger.KeyOrganizationID, organizationID))
		}

		parentID := int64(organizationID)
		var (
			children []*common_persistence_models.Organization
			total    int64
		)
		if canListAllOrganizations {
			children, total, err = h.persistenceHandler.GetOrganizationsByParent(ctx, &parentID, offset, limit)
		} else {
			children, total, err = h.persistenceHandler.GetOrganizationsByParentForUser(ctx, uint64(userID), &parentID, offset, limit)
		}
		if err != nil {
			return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "get_organization_hierarchy"), zap.String(logkeys.Stage, "load_children"), zap.Uint64(appLogger.KeyUserID, uint64(userID)), zap.Int64(appLogger.KeyOrganizationID, organizationID), zap.Int(logkeys.Limit, limit), zap.Int(appLogger.KeyOffset, offset))
		}

		rootNode := &common_service_models.OrganizationHierarchyNode{
			Organization: h.converter.PersistenceOrganizationToService(rootOrg),
		}
		for _, child := range children {
			if child == nil {
				continue
			}
			rootNode.Children = append(rootNode.Children, &common_service_models.OrganizationHierarchyNode{
				Organization: h.converter.PersistenceOrganizationToService(child),
			})
		}

		pagination := common_util.CreatePaginationResponse(total, uint32(limit), page)
		return &OrganizationHierarchyResult{
			Hierarchy:  rootNode,
			Pagination: pagination,
			Truncated:  pagination.HasNextPage,
		}, nil
	}

	maxNodes := h.hierarchyMaxNodes()
	var (
		organizations []*common_persistence_models.Organization
		truncated     bool
		err           error
	)
	if canListAllOrganizations {
		organizations, truncated, err = h.persistenceHandler.GetOrganizationsByLineageLimited(ctx, organizationID, maxNodes)
	} else {
		organizations, truncated, err = h.persistenceHandler.GetOrganizationsByLineageForUserLimited(ctx, uint64(userID), organizationID, maxNodes)
	}
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return nil, errors.ErrOrganizationNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "get_organization_hierarchy"), zap.String(logkeys.Stage, "load_descendants"), zap.Uint64(appLogger.KeyUserID, uint64(userID)), zap.Int64(appLogger.KeyOrganizationID, organizationID), zap.Int(logkeys.Limit, maxNodes))
	}

	nodes := make(map[int64]*common_service_models.OrganizationHierarchyNode, len(organizations))
	for _, org := range organizations {
		if org != nil {
			nodes[org.ID] = &common_service_models.OrganizationHierarchyNode{
				Organization: h.converter.PersistenceOrganizationToService(org),
			}
		}
	}
	for _, org := range organizations {
		if org == nil || org.ParentOrganizationID == nil {
			continue
		}
		parent := nodes[*org.ParentOrganizationID]
		child := nodes[org.ID]
		if parent != nil && child != nil {
			parent.Children = append(parent.Children, child)
		}
	}
	root, ok := nodes[int64(organizationID)]
	if !ok {
		return nil, errors.ErrOrganizationNotFound
	}
	return &OrganizationHierarchyResult{
		Hierarchy: root,
		Truncated: truncated,
	}, nil
}

func (h *OrganizationHandler[U]) ListUserOrganizationsWithRolesPage(
	ctx context.Context,
	userID common_service_models.UserIdType,
	limit, offset int,
	sortBy proto_models.ListOrganizationsSortBy,
	sortReq *proto_models.SortRequest,
	filter *proto_models.FilterRequest,
	organizationTypeFilter proto_models.OrganizationType,
	parentOrganizationIDFilter *common_service_models.OrganizationIdType,
	depthFilter *uint32,
	includeTotalCount bool,
	maskFilters []*proto_models.OrganizationMembershipMaskFilter,
) ([]*common_service_models.UserOrganizationResponse, *proto_models.PaginationResponse, error) {
	responses, pagination, _, err := h.listUserOrganizationsWithRolesInternal(
		ctx,
		userID,
		limit,
		offset,
		sortBy,
		sortReq,
		filter,
		organizationTypeFilter,
		parentOrganizationIDFilter,
		depthFilter,
		includeTotalCount,
		maskFilters,
		true,
	)
	return responses, pagination, err
}

func buildUserOrganizationsHasNextOnlyPagination(limit, offset int, hasNext bool) *proto_models.PaginationResponse {
	currentPage := uint32(1)
	if limit > 0 {
		currentPage = uint32(offset/limit + 1)
	}

	return common_util.CreatePaginationResponseWithoutTotalCount(uint32(limit), currentPage, hasNext)
}

func (h *OrganizationHandler[U]) listUserOrganizationsWithRolesInternal(
	ctx context.Context,
	userID common_service_models.UserIdType,
	limit, offset int,
	sortBy proto_models.ListOrganizationsSortBy,
	sortReq *proto_models.SortRequest,
	filter *proto_models.FilterRequest,
	organizationTypeFilter proto_models.OrganizationType,
	parentOrganizationIDFilter *common_service_models.OrganizationIdType,
	depthFilter *uint32,
	includeTotalCount bool,
	maskFilters []*proto_models.OrganizationMembershipMaskFilter,
	allowCountSkip bool,
) ([]*common_service_models.UserOrganizationResponse, *proto_models.PaginationResponse, int64, error) {
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		zap.Int(logkeys.Limit, limit),
		zap.Int(appLogger.KeyOffset, offset),
	)
	log.Debug("ListUserOrganizationsWithRoles called")

	// Validate user ID
	if userID == 0 {
		return nil, nil, 0, errors.ErrInvalidUserID
	}

	if err := requireListLimit("list_user_organizations_with_roles", limit); err != nil {
		return nil, nil, 0, err
	}
	if offset < 0 {
		offset = 0
	}

	requiredViewRolesByDepth, err := h.getOrganizationViewRolesByDepth()
	if err != nil {
		return nil, nil, 0, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "list_user_organizations_with_roles"), zap.String(logkeys.Stage, "resolve_view_roles"), zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}
	search := ""
	if filter != nil {
		search = strings.TrimSpace(filter.Search)
	}
	skipExactCount := allowCountSkip && !includeTotalCount
	var parentOrganizationIDFilterValue *int64
	if parentOrganizationIDFilter != nil {
		value := int64(*parentOrganizationIDFilter)
		parentOrganizationIDFilterValue = &value
	}
	userOrgs, totalCount, err := h.persistenceHandler.GetUserOrganizations(ctx, uint64(userID), &persistence.UserOrganizationsQuery{
		Offset:                           offset,
		Limit:                            limit,
		SortColumn:                       sortBy,
		SortDesc:                         common_util.SortDesc(sortReq, true),
		Search:                           search,
		SkipExactCount:                   skipExactCount,
		ApplyVisibilityFilters:           true,
		MinOrganizationType:              h.runtimeConfig().OrgMinTypeWorkspaceNavigation(),
		OrganizationTypeFilter:           organizationTypeFilter,
		ParentOrganizationIDFilter:       parentOrganizationIDFilterValue,
		DepthFilter:                      depthFilter,
		RequiredViewRolesByDepth:         requiredViewRolesByDepth,
		OrganizationMembershipMaskFilter: maskFilters,
	})
	if err != nil {
		return nil, nil, 0, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "list_user_organizations_with_roles"), zap.String(logkeys.Stage, "load_memberships"), zap.Uint64(appLogger.KeyUserID, uint64(userID)), zap.Int(logkeys.Limit, limit), zap.Int(appLogger.KeyOffset, offset))
	}

	hasNextPage := false
	if skipExactCount && limit > 0 && len(userOrgs) > limit {
		hasNextPage = true
		userOrgs = userOrgs[:limit]
	}

	// Filter out any nil responses from failed organization fetches
	actualResponses := common_util.ApplyToEachFilteredOnResult(userOrgs, func(userOrg *persistence.UserOrganizationWithRoleInfo) *common_service_models.UserOrganizationResponse {
		if userOrg == nil || userOrg.UserOrganization == nil {
			return nil
		}
		if userOrg.UserOrganization.Organization.ID == 0 {
			appLogger.LogInvariantViolation(appLogger.For(ctx,
				h.logger, organizationLoggerName), "organization_membership_contains_organization",
				nil,
				zap.String(logkeys.Operation, "list_user_organizations_with_roles"),
				zap.String(logkeys.Stage, "build_response"),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.Int64(appLogger.KeyOrganizationID, userOrg.UserOrganization.OrganizationID),
				zap.String(logkeys.Expected, "membership organization id is non-zero"),
				zap.String(logkeys.Actual, "embedded organization id is zero"),
				zap.String(logkeys.Fallback, "skip_membership"),
			)
			// Skip this organization rather than failing the entire request
			return nil
		}

		// Convert to service models
		serviceOrg := h.converter.PersistenceOrganizationToService(&userOrg.UserOrganization.Organization)
		serviceUserOrg := h.converter.PersistenceUserOrganizationToService(userOrg.UserOrganization)
		serviceUserOrg.ExplicitRole = userOrg.RoleInfo.ExplicitRole
		serviceUserOrg.EffectiveRole = userOrg.RoleInfo.EffectiveRole
		serviceUserOrg.InheritedFromOrganizationID = userOrg.RoleInfo.InheritedFromOrganizationID
		if userOrg.RoleInfo.EffectiveRoleDepth != nil {
			depth := uint32(*userOrg.RoleInfo.EffectiveRoleDepth)
			serviceUserOrg.EffectiveRoleDepth = &depth
		}
		return &common_service_models.UserOrganizationResponse{
			UserOrganization: serviceUserOrg,
			Organization:     serviceOrg,
		}
	}, func(currentResult *common_service_models.UserOrganizationResponse) bool {
		return currentResult != nil
	}, new(len(userOrgs)))

	currentPage := uint32(1)
	if limit > 0 {
		currentPage = uint32(offset/limit + 1)
	}
	var pagination *proto_models.PaginationResponse
	if skipExactCount {
		pagination = buildUserOrganizationsHasNextOnlyPagination(limit, offset, hasNextPage)
	} else {
		pagination = common_util.CreatePaginationResponse(totalCount, uint32(limit), currentPage)
	}

	if totalCount == 0 {
		return []*common_service_models.UserOrganizationResponse{}, pagination, totalCount, nil
	}

	if ce := log.Check(zap.DebugLevel, "user organizations with roles retrieved successfully"); ce != nil {
		ce.Write(
			zap.Int(appLogger.KeyCount, len(actualResponses)),
			zap.Int64(appLogger.KeyTotalFiltered, totalCount),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	return actualResponses, pagination, totalCount, nil
}

// CreateOrganizationEmailDomain creates a new email-domain mapping for an organization.
//
// Parameters:
//   - ctx: Request context used for authorization and persistence.
//   - orgID: Organization that will own the domain mapping.
//   - domain: Email domain to associate with the organization.
//   - createdBy: Optional actor user ID for auditing.
//
// Returns:
//   - *common_service_models.OrganizationEmailDomain: Created domain mapping.
//   - error: Validation, authorization, or persistence error.
func (h *OrganizationHandler[U]) CreateOrganizationEmailDomain(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	domain string,
	createdBy *common_service_models.UserIdType,
) (*common_service_models.OrganizationEmailDomain, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.String(appLogger.KeyDomain, domain),
		).Debug("CreateOrganizationEmailDomain called")
	}

	if !h.runtimeConfig().EnableOrganizationEmailDomains() {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, organizationLoggerName).With(
				zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
				zap.String(appLogger.KeyDomain, domain),
			).Debug("organization email domains disabled in configuration")
		}
		return nil, errors.ErrOrganizationEmailDomainsDisabled
	}
	if err := h.ensureOrganizationTypeCapability(ctx, orgID, organizationTypeCapabilityManageEmailDomains); err != nil {
		return nil, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "create_organization_email_domain"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(appLogger.KeyDomain, domain), zap.String(logkeys.Reason, "organization_type_capability_denied"))
	}

	allowed, err := h.CanManageEmailDomains(ctx, orgID)
	if err != nil {
		return nil, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "create_organization_email_domain"), zap.String(logkeys.Stage, "permission_check"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(appLogger.KeyDomain, domain))
	}
	if !allowed {
		return nil, appLogger.WithErrorFields(errors.ErrInsufficientPermissions, zap.String(logkeys.Operation, "create_organization_email_domain"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(appLogger.KeyDomain, domain), zap.String(logkeys.Reason, "insufficient_permission"))
	}

	org, err := h.persistenceHandler.GetOrganizationByID(ctx, orgID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return nil, errors.ErrOrganizationNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "create_organization_email_domain"), zap.String(logkeys.Stage, "load_organization"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(appLogger.KeyDomain, domain))
	}

	// Domain-based admission never applies to personal trees (the auto-associate
	// path rejects them), so a personal workspace must not claim a globally
	// unique managed domain it can never use.
	if err := ensurePersonalInvitationAllowed(h.converter.PersistenceOrganizationToService(org)); err != nil {
		return nil, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "create_organization_email_domain"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(appLogger.KeyDomain, domain), zap.String(logkeys.Reason, "personal_workspace_domain_disallowed"))
	}

	var creatorPtr *uint64
	if createdBy != nil {
		val := uint64(*createdBy)
		creatorPtr = &val
	}

	persistenceDomain := &common_persistence_models.OrganizationEmailDomain{
		OrganizationID:  uint64(orgID),
		Domain:          strings.ToLower(strings.TrimSpace(domain)),
		CreatedByUserID: creatorPtr,
	}

	if err := h.persistenceHandler.CreateOrganizationEmailDomain(ctx, persistenceDomain); err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationEmailDomainExists) {
			return nil, errors.ErrOrganizationEmailDomainExists
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "create_organization_email_domain"), zap.String(logkeys.Stage, "persist_domain"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(appLogger.KeyDomain, persistenceDomain.Domain))
	}

	// Drop a possibly cached negative entry for this domain.
	h.invalidateOrganizationEmailDomainCache(persistenceDomain.Domain)

	return h.converter.PersistenceOrganizationEmailDomainToService(persistenceDomain), nil
}

// DeleteOrganizationEmailDomain removes an email-domain mapping from an organization.
//
// Parameters:
//   - ctx: Request context used for authorization and persistence.
//   - orgID: Organization that owns the domain mapping.
//   - emailDomainID: Domain-mapping ID to delete.
//
// Returns:
//   - error: Validation, authorization, or persistence error.
func (h *OrganizationHandler[U]) DeleteOrganizationEmailDomain(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	emailDomainID uint64,
) error {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.Uint64(appLogger.KeyEmailDomainID, emailDomainID),
		).Debug("DeleteOrganizationEmailDomain called")
	}

	if !h.runtimeConfig().EnableOrganizationEmailDomains() {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, organizationLoggerName).With(
				zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
				zap.Uint64(appLogger.KeyEmailDomainID, emailDomainID),
			).Debug("organization email domains disabled in configuration")
		}
		return errors.ErrOrganizationEmailDomainsDisabled
	}

	record, err := h.persistenceHandler.GetOrganizationEmailDomainByID(ctx, emailDomainID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationEmailDomainNotFound) {
			return errors.ErrOrganizationEmailDomainNotFound
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "delete_organization_email_domain"), zap.String(logkeys.Stage, "load_domain"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyEmailDomainID, emailDomainID))
	}

	if orgID != 0 && record.OrganizationID != uint64(orgID) {
		return appLogger.WithErrorFields(errors.ErrInsufficientPermissions, zap.String(logkeys.Operation, "delete_organization_email_domain"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyEmailDomainID, emailDomainID), zap.Uint64(appLogger.KeyActualOrganizationID, record.OrganizationID), zap.String(logkeys.Reason, "organization_mismatch"))
	}
	if err := h.ensureOrganizationTypeCapability(ctx, common_service_models.OrganizationIdType(record.OrganizationID), organizationTypeCapabilityManageEmailDomains); err != nil {
		return appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "delete_organization_email_domain"), zap.Uint64(appLogger.KeyOrganizationID, record.OrganizationID), zap.Uint64(appLogger.KeyEmailDomainID, emailDomainID), zap.String(logkeys.Reason, "organization_type_capability_denied"))
	}

	allowed, permErr := h.CanManageEmailDomains(ctx, common_service_models.OrganizationIdType(record.OrganizationID))
	if permErr != nil {
		return appLogger.WithErrorFields(permErr, zap.String(logkeys.Operation, "delete_organization_email_domain"), zap.String(logkeys.Stage, "permission_check"), zap.Uint64(appLogger.KeyOrganizationID, record.OrganizationID), zap.Uint64(appLogger.KeyEmailDomainID, emailDomainID))
	}
	if !allowed {
		return appLogger.WithErrorFields(errors.ErrInsufficientPermissions, zap.String(logkeys.Operation, "delete_organization_email_domain"), zap.Uint64(appLogger.KeyOrganizationID, record.OrganizationID), zap.Uint64(appLogger.KeyEmailDomainID, emailDomainID), zap.String(logkeys.Reason, "insufficient_permission"))
	}

	if err := h.persistenceHandler.DeleteOrganizationEmailDomain(ctx, emailDomainID); err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationEmailDomainNotFound) {
			return errors.ErrOrganizationEmailDomainNotFound
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "delete_organization_email_domain"), zap.String(logkeys.Stage, "persist_deletion"), zap.Uint64(appLogger.KeyOrganizationID, record.OrganizationID), zap.Uint64(appLogger.KeyEmailDomainID, emailDomainID), zap.String(appLogger.KeyDomain, record.Domain))
	}

	h.invalidateOrganizationEmailDomainCache(record.Domain)

	return nil
}

// ListOrganizationEmailDomains lists email-domain mappings associated with an organization.
//
// Parameters:
//   - ctx: Request context used for authorization and persistence.
//   - orgID: Organization whose domain mappings should be listed.
//
// Returns:
//   - []*common_service_models.OrganizationEmailDomain: Domain mappings for the organization.
//   - error: Validation, authorization, or persistence error.
func (h *OrganizationHandler[U]) ListOrganizationEmailDomains(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
) ([]*common_service_models.OrganizationEmailDomain, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
		).Debug("ListOrganizationEmailDomains called")
	}

	if !h.runtimeConfig().EnableOrganizationEmailDomains() {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, organizationLoggerName).With(
				zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			).Debug("organization email domains disabled in configuration")
		}
		return nil, errors.ErrOrganizationEmailDomainsDisabled
	}
	if err := h.ensureOrganizationTypeCapability(ctx, orgID, organizationTypeCapabilityManageEmailDomains); err != nil {
		return nil, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "list_organization_email_domains"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(logkeys.Reason, "organization_type_capability_denied"))
	}

	allowed, err := h.CanManageEmailDomains(ctx, orgID)
	if err != nil {
		return nil, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "list_organization_email_domains"), zap.String(logkeys.Stage, "permission_check"), zap.Int64(appLogger.KeyOrganizationID, orgID))
	}
	if !allowed {
		return nil, appLogger.WithErrorFields(errors.ErrInsufficientPermissions, zap.String(logkeys.Operation, "list_organization_email_domains"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(logkeys.Reason, "insufficient_permission"))
	}

	records, err := h.persistenceHandler.ListOrganizationEmailDomains(ctx, orgID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return nil, errors.ErrOrganizationNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "list_organization_email_domains"), zap.String(logkeys.Stage, "load_domains"), zap.Int64(appLogger.KeyOrganizationID, orgID))
	}

	domains := common_util.ApplyToEach(records, func(record *common_persistence_models.OrganizationEmailDomain) *common_service_models.OrganizationEmailDomain {
		return h.converter.PersistenceOrganizationEmailDomainToService(record)
	})
	return domains, nil
}

// ListOrganizationEmailDomainsPage is the bounded API-facing email-domain
// listing path. ListOrganizationEmailDomains remains available for trusted
// internal callers that explicitly require all mappings.
func (h *OrganizationHandler[U]) ListOrganizationEmailDomainsPage(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	pagination *proto_models.PaginationRequest,
) ([]*common_service_models.OrganizationEmailDomain, *proto_models.PaginationResponse, error) {
	if !h.runtimeConfig().EnableOrganizationEmailDomains() {
		return nil, nil, errors.ErrOrganizationEmailDomainsDisabled
	}
	if err := h.ensureOrganizationTypeCapability(ctx, orgID, organizationTypeCapabilityManageEmailDomains); err != nil {
		return nil, nil, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "list_organization_email_domains_page"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(logkeys.Reason, "organization_type_capability_denied"))
	}
	allowed, err := h.CanManageEmailDomains(ctx, orgID)
	if err != nil {
		return nil, nil, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "list_organization_email_domains_page"), zap.String(logkeys.Stage, "permission_check"), zap.Int64(appLogger.KeyOrganizationID, orgID))
	}
	if !allowed {
		return nil, nil, appLogger.WithErrorFields(errors.ErrInsufficientPermissions, zap.String(logkeys.Operation, "list_organization_email_domains_page"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(logkeys.Reason, "insufficient_permission"))
	}

	records, paginationResponse, err := h.persistenceHandler.ListOrganizationEmailDomainsPage(ctx, orgID, pagination)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return nil, nil, errors.ErrOrganizationNotFound
		}
		return nil, nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "list_organization_email_domains_page"), zap.String(logkeys.Stage, "load_domains"), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint32(logkeys.Limit, pagination.GetLimit()), zap.Uint32(appLogger.KeyPage, pagination.GetPage()))
	}

	domains := common_util.ApplyToEach(records, func(record *common_persistence_models.OrganizationEmailDomain) *common_service_models.OrganizationEmailDomain {
		return h.converter.PersistenceOrganizationEmailDomainToService(record)
	})
	return domains, paginationResponse, nil
}

// GetOrganizationEmailDomainByDomain retrieves an organization email-domain mapping by domain value.
//
// Parameters:
//   - ctx: Request context used for persistence.
//   - domain: Email domain to look up.
//
// Returns:
//   - *common_service_models.OrganizationEmailDomain: Domain mapping for the requested domain.
//   - error: Validation or persistence error.
func (h *OrganizationHandler[U]) GetOrganizationEmailDomainByDomain(
	ctx context.Context,
	domain string,
) (*common_service_models.OrganizationEmailDomain, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.String(appLogger.KeyDomain, domain),
		).Debug("GetOrganizationEmailDomainByDomain called")
	}

	if !h.runtimeConfig().EnableOrganizationEmailDomains() {
		return nil, errors.ErrOrganizationEmailDomainsDisabled
	}

	normalizedDomain := strings.ToLower(strings.TrimSpace(domain))

	// This runs on the registration path for every signup; most domains have
	// no mapping, so negative results (nil value) are cached too.
	inTransaction := hasAmbientTransaction(ctx)
	cacheable := h.orgEmailDomainCache != nil && !inTransaction
	var generation uint64
	if cacheable {
		cached, found, observed := h.orgEmailDomainCache.Lookup(normalizedDomain)
		if found {
			if cached != nil {
				return cached, nil
			}
			return nil, errors.ErrOrganizationEmailDomainNotFound
		}
		generation = observed
	}

	record, err := h.persistenceHandler.GetOrganizationEmailDomainByDomain(ctx, normalizedDomain)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationEmailDomainNotFound) {
			if cacheable {
				h.orgEmailDomainCache.FillIfCurrent(normalizedDomain, nil, generation)
			}
			return nil, errors.ErrOrganizationEmailDomainNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "get_organization_email_domain"), zap.String(logkeys.Stage, "load_domain"), zap.String(appLogger.KeyDomain, normalizedDomain))
	}

	converted := h.converter.PersistenceOrganizationEmailDomainToService(record)
	if cacheable && converted != nil {
		h.orgEmailDomainCache.FillIfCurrent(normalizedDomain, converted, generation)
	}
	return converted, nil
}

// AutoAssociateUserWithOrganizationByEmail attempts to auto-associate a user with an organization based on their email domain.
//
// Parameters:
//   - ctx: Request context used for persistence and membership operations.
//   - userID: User to auto-associate.
//   - email: Email address used to resolve a matching domain mapping.
//
// Returns:
//   - error: Validation, lookup, or membership error.
func (h *OrganizationHandler[U]) AutoAssociateUserWithOrganizationByEmail(
	ctx context.Context,
	userID common_service_models.UserIdType,
	email string,
) error {
	runtimeConfig := h.runtimeConfig()
	if !runtimeConfig.EnableOrganizationEmailDomains() || !runtimeConfig.EnableOrganizationEmailAutoAssociation() {
		return nil
	}

	if email == "" {
		return nil
	}

	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return nil
	}
	name, domain := parts[0], strings.ToLower(strings.TrimSpace(parts[1]))
	if domain == "" || name == "" {
		return nil
	}

	record, err := h.GetOrganizationEmailDomainByDomain(ctx, domain)
	if err != nil {
		if stdErrors.Is(err, errors.ErrOrganizationEmailDomainNotFound) {
			return nil
		}
		return err
	}

	userOrg, err := h.persistenceHandler.GetUserOrganization(ctx, userID, record.OrganizationID)
	if err != nil && !stdErrors.Is(err, persistence.ErrUserOrganizationNotFound) {
		return appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "auto_associate_user_by_email_domain"),
			zap.String(logkeys.Stage, "check_existing_membership"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(record.OrganizationID)),
			zap.String(appLogger.KeyDomain, domain),
		)
	}
	if userOrg != nil {
		return nil
	}
	resolvedOrg, err := h.resolveOrganization(ctx, record.OrganizationID)
	if err != nil {
		return err
	}
	// Email-domain association is an invitation-like admission path and remains
	// blocked for every personal organization, including its owner.
	if err := ensurePersonalInvitationAllowed(resolvedOrg.Organization); err != nil {
		return err
	}

	role := getRoleAtDepth(h.collectionPolicy(runtimeConfig).associationRoles, organizationDepth(resolvedOrg.Organization))
	_, addErr := h.addMemberDirectWithResolvedOrg(ctx, userID, userID, resolvedOrg, role)
	if addErr != nil {
		if stdErrors.Is(addErr, errors.ErrUserAlreadyMember) {
			return nil
		}
		return addErr
	}

	appLogger.For(ctx, h.logger, organizationLoggerName).Info("Auto-associated user with organization via email domain",
		zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		zap.Uint64(appLogger.KeyOrganizationID, uint64(record.OrganizationID)),
		zap.String(appLogger.KeyDomain, domain))

	return nil
}

// validateOrganization validates an organization service model before an
// ordinary create operation proceeds.
//
// Parameters:
//   - ctx: Request context used for logging.
//   - org: Organization payload to validate and normalize.
//
// Returns:
//   - error: Validation error describing the first failed rule.
func (h *OrganizationHandler[U]) validateOrganization(
	ctx context.Context,
	org *common_service_models.Organization,
) error {
	if err := h.validateOrganizationCreateShape(ctx, org); err != nil {
		return err
	}
	if h.isReservedPersonalWorkspaceSlug(org.Slug) {
		return errors.ErrOrganizationSlugReserved
	}
	return nil
}

// validateOrganizationCreateShape normalizes and validates ordinary
// organization fields against the current creation policy without applying the
// reserved-slug policy.
func (h *OrganizationHandler[U]) validateOrganizationCreateShape(
	ctx context.Context,
	org *common_service_models.Organization,
) error {
	return h.validateOrganizationShape(ctx, org, h.isValidSlug)
}

// validateOrganizationUpdateShape applies only immutable stored-slug rules.
// Current hot min/max bounds govern newly selected slugs and are checked after
// the existing row is loaded, when the handler can distinguish a rename from
// an unchanged retained value.
func (h *OrganizationHandler[U]) validateOrganizationUpdateShape(
	ctx context.Context,
	org *common_service_models.Organization,
) error {
	return h.validateOrganizationShape(ctx, org, isValidOrganizationSlugSyntax)
}

func (h *OrganizationHandler[U]) validateOrganizationShape(
	ctx context.Context,
	org *common_service_models.Organization,
	isValidSlug func(string) bool,
) error {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.String(appLogger.KeyName, org.Name),
			zap.String(appLogger.KeySlug, org.Slug),
		).Debug("validating organization")
	}
	if strings.TrimSpace(org.Name) == "" {
		return errors.ErrOrganizationNameRequired
	}

	if utf8.RuneCountInString(org.Name) > common_service_models.MaxOrganizationNameLength {
		return errors.ErrOrganizationNameTooLong
	}

	// Sanitize and validate slug
	slug := strings.ToLower(strings.TrimSpace(org.Slug))
	if slug == "" {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, organizationLoggerName).With(
				zap.String(appLogger.KeyName, org.Name),
				zap.String(appLogger.KeySlug, org.Slug),
			).Debug("organization slug is required")
		}
		return errors.ErrOrganizationSlugRequired
	}

	if !isValidSlug(slug) {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, organizationLoggerName).With(
				zap.String(appLogger.KeyName, org.Name),
				zap.String(appLogger.KeySlug, org.Slug),
			).Debug("organization slug is invalid")
		}
		return errors.ErrInvalidOrganizationSlug
	}

	// Update the org with the sanitized slug
	org.Slug = slug

	if utf8.RuneCountInString(org.Description) > common_service_models.MaxOrganizationDescriptionLength {
		return errors.ErrOrganizationDescriptionTooLong
	}

	return nil
}

// isValidSlug validates organization slug format against the current length
// bounds.
func (h *OrganizationHandler[U]) isValidSlug(slug string) bool {
	runtimeConfig := h.runtimeConfig()
	if len(slug) < runtimeConfig.OrganizationSlugMinLength() || len(slug) > runtimeConfig.OrganizationSlugMaxLength() {
		return false
	}

	return isValidOrganizationSlugSyntax(slug)
}

func isValidOrganizationSlugSyntax(slug string) bool {
	// This immutable ceiling mirrors the varchar(255) persistence column. Hot
	// creation bounds may change, but no stored slug can legitimately exceed it.
	if len(slug) > 255 {
		return false
	}
	if slug != strings.ToLower(slug) {
		return false
	}

	// Alphanumeric and hyphens, but not starting or ending with a hyphen.
	match, _ := regexp.MatchString(`^[a-z0-9]([a-z0-9\-]*[a-z0-9])?$`, slug)
	return match
}

// getUserRoleInOrganization is an internal method that retrieves just the role.
func (h *OrganizationHandler[U]) getUserRoleInOrganization(
	ctx context.Context,
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) (proto_models.OrganizationRole, error) {
	if h.orgRoleProvider == nil {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, errors.ErrInternalError
	}

	roleInfo, isMember, err := h.orgRoleProvider.GetUserRole(ctx, userID, orgID)
	if err != nil {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, err
	}
	if !isMember {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, errors.ErrUserNotMember
	}

	return roleInfo.EffectiveRole, nil
}

// getOrgDepth resolves and caches organization depth for permission and capability checks across organization flows.
//
// Parameters:
//   - ctx: Request context used for persistence queries.
//   - orgID: Organization whose depth is requested.
//
// Returns:
//   - int: Depth derived from organization lineage length.
//   - error: Invalid ID or persistence lookup error.
func (h *OrganizationHandler[U]) getOrgDepth(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
) (int, error) {
	if orgID == 0 {
		return 0, errors.ErrInvalidOrganizationID
	}
	inTransaction := hasAmbientTransaction(ctx)
	cacheable := h.orgDepthCache != nil && !inTransaction
	var generation uint64
	if cacheable {
		cached, found, observed := h.orgDepthCache.Lookup(orgID)
		if found {
			return cached, nil
		}
		generation = observed
	}

	org, err := h.persistenceHandler.GetOrganizationByID(ctx, orgID)
	if err != nil {
		return 0, err
	}

	depth := len(org.Lineage)
	if cacheable {
		h.orgDepthCache.FillIfCurrent(orgID, depth, generation)
	}
	return depth, nil
}

// getOrgDepthMapped resolves organization depth and maps persistence errors to domain
// errors, so callers can return the error directly instead of repeating the mapping.
//
// Parameters:
//   - ctx: Request context used for persistence queries.
//   - orgID: Organization whose depth is requested.
//
// Returns:
//   - int: Depth derived from organization lineage length.
//   - error: ErrOrganizationNotFound, ErrInvalidOrganizationID, or ErrInternalError.
func (h *OrganizationHandler[U]) getOrgDepthMapped(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
) (int, error) {
	depth, err := h.getOrgDepth(ctx, orgID)
	if err == nil {
		return depth, nil
	}
	if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
		return 0, errors.ErrOrganizationNotFound
	}
	if stdErrors.Is(err, errors.ErrInvalidOrganizationID) {
		return 0, errors.ErrInvalidOrganizationID
	}
	return 0, appLogger.WithErrorCause(errors.ErrInternalError, err,
		zap.String(logkeys.Operation, "resolve_organization_depth"),
		zap.String(logkeys.Stage, "load_organization"),
		zap.Int64(appLogger.KeyOrganizationID, int64(orgID)),
	)
}

// buildOrganizationResponses converts persistence organizations into responses, fetching all
// member counts in a single batch query instead of one query per organization.
//
// Parameters:
//   - ctx: Request context used for persistence queries.
//   - log: Logger scoped to the calling list flow.
//   - orgs: Organizations to convert.
//
// Returns:
//   - []*OrganizationResponse: Responses with member counts (0 when the batch lookup fails).
func (h *OrganizationHandler[U]) buildOrganizationResponses(
	ctx context.Context,
	log *zap.Logger,
	orgs []*common_persistence_models.Organization,
) []*OrganizationResponse {
	orgIDs := make([]int64, 0, len(orgs))
	for _, org := range orgs {
		if org == nil {
			continue
		}
		orgIDs = append(orgIDs, org.ID)
	}

	memberCounts := map[int64]int64{}
	if len(orgIDs) > 0 {
		counts, err := h.persistenceHandler.GetOrganizationMemberCounts(ctx, orgIDs)
		if err != nil {
			log.Warn("failed to get member counts, using 0",
				zap.String(logkeys.Operation, "build_organization_responses"),
				zap.String(logkeys.Stage, "load_member_counts"),
				zap.Int(appLogger.KeyCount, len(orgIDs)),
				zap.String(logkeys.Fallback, "member_counts_zero"),
				zap.Error(err))
		} else {
			memberCounts = counts
		}
	}

	return common_util.ApplyToEach(orgs, func(persistenceOrg *common_persistence_models.Organization) *OrganizationResponse {
		return &OrganizationResponse{
			Organization: h.converter.PersistenceOrganizationToService(persistenceOrg),
			MemberCount:  memberCounts[persistenceOrg.ID],
		}
	})
}

// ensureAncestorMembershipForLineage applies ancestor-membership policy using
// lineage already loaded with the target organization. runtimeConfig is the
// caller's sample: the invitation path decides whether to defer auto-adds from
// the same generation this helper adds under, so a pending invitation never
// grants ancestor access.
func (h *OrganizationHandler[U]) ensureAncestorMembershipForLineage(
	ctx context.Context,
	userID common_service_models.UserIdType,
	lineage []common_service_models.OrganizationIdType,
	runtimeConfig PolicyView,
) error {
	policy := h.collectionPolicy(runtimeConfig)
	parentRole := proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
	for depth, ancestorID := range lineage {
		if ancestorID == 0 {
			continue
		}
		defaultRole := getRoleAtDepth(policy.associationRoles, depth)
		membership, err := h.persistenceHandler.GetUserOrganization(ctx, userID, ancestorID)
		if err == nil {
			role := membership.Role
			if h.orgRoleProvider != nil {
				roleInfo, _, roleErr := h.orgRoleProvider.GetUserRole(ctx, userID, ancestorID)
				if roleErr != nil {
					return errors.ErrInternalError
				}
				role = roleInfo.EffectiveRole
			}
			if role != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
				parentRole = role
			}
			continue
		}
		if !stdErrors.Is(err, persistence.ErrUserOrganizationNotFound) {
			return errors.ErrInternalError
		}
		if !runtimeConfig.OrgAncestorMembershipAutoAdd() {
			return errors.ErrOrganizationAncestorMembershipRequired
		}

		roleToAssign := defaultRole
		if parentRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED && parentRole < roleToAssign {
			roleToAssign = parentRole
		}
		_, addErr := h.addMemberDirect(ctx, userID, userID, ancestorID, roleToAssign)
		if addErr != nil {
			if stdErrors.Is(addErr, errors.ErrUserAlreadyMember) {
				continue
			}
			return addErr
		}
		parentRole = roleToAssign
	}
	return nil
}

// InvalidateRoleCacheForDescendants evicts roles after a successful membership
// write, deferring until commit when the transaction supports post-commit hooks.
// Callers managing transactions without hooks must call this after committing.
func (h *OrganizationHandler[U]) InvalidateRoleCacheForDescendants(
	ctx context.Context,
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) {
	h.invalidateRealtimeAfterCommit(ctx, ports.ChatRealtimeAccessInvalidation{UserID: userID})
	if h.orgRoleProvider == nil {
		return
	}
	descendantIDs, lookupErr := h.persistenceHandler.GetDescendantOrganizationIDs(ctx, orgID)
	if lookupErr != nil {
		appLogger.For(ctx, h.logger, organizationLoggerName).Warn("failed to get descendant organization IDs for cache invalidation",
			zap.String(logkeys.Operation, "invalidate_descendant_role_cache"),
			zap.String(logkeys.Stage, "resolve_descendants"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Fallback, "invalidate_all_roles_after_commit"),
			zap.Error(lookupErr))
	}
	orgIDs := []common_service_models.OrganizationIdType{orgID}
	for _, descendantID := range descendantIDs {
		if common_service_models.OrganizationIdType(descendantID) != orgID {
			orgIDs = append(orgIDs, common_service_models.OrganizationIdType(descendantID))
		}
	}

	// Capture IDs now; the hook must not query a completed transaction.
	invalidate := func(context.Context) error {
		if lookupErr != nil {
			h.orgRoleProvider.InvalidateAll()
		} else {
			h.orgRoleProvider.invalidateMany(userID, orgIDs)
		}
		return nil
	}
	if !persistence.AddPostCommitHookFromContext(ctx, invalidate) {
		_ = invalidate(ctx)
	}
}

// InvalidateAllRoleCache clears the entire organization role cache.
// This should be called when an organization is deleted to ensure no stale cache entries remain.
func (h *OrganizationHandler[U]) InvalidateAllRoleCache() {
	if h.orgRoleProvider == nil {
		return
	}
	h.orgRoleProvider.InvalidateAll()
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(context.Background(), h.logger, organizationLoggerName).Debug("Cleared entire organization role cache")
	}
}

// collectCacheMetrics periodically logs role cache statistics for monitoring.
// It runs in a background goroutine until Stop closes stopCh and logs cache
// size, capacity, hits, misses, and evictions every minute.
func (h *OrganizationHandler[U]) collectCacheMetrics() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			if h.orgRoleProvider == nil || !h.orgRoleProvider.CacheEnabled() {
				return // Caching disabled
			}

			cacheSize, hits, misses, evictions := h.orgRoleProvider.CacheStats()

			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(context.Background(), h.logger, organizationLoggerName).Debug("Organization role cache stats",
					zap.Int(appLogger.KeySize, cacheSize),
					zap.Uint64(appLogger.KeyCapacity, h.orgRoleProvider.CacheCapacity()),
					zap.Int64(appLogger.KeyHits, hits),
					zap.Int64(appLogger.KeyMisses, misses),
					zap.Int64(appLogger.KeyEvictions, evictions))
			}
		}
	}
}

// Stop gracefully shuts down the organization handler.
// This stops the role cache and any background goroutines.
func (h *OrganizationHandler[U]) Stop() {
	h.stopOnce.Do(func() {
		// End the cache-metrics and depth-sweeper goroutines. The nil check
		// keeps Stop safe on handlers built without NewOrganizationHandler.
		if h.stopCh != nil {
			close(h.stopCh)
		}

		if h.orgRoleProvider != nil {
			h.orgRoleProvider.Stop()
		}
		h.cacheMetricsWG.Wait()
	})
}
