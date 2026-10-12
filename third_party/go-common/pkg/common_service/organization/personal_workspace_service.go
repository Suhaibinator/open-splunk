package organization

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jellydator/ttlcache/v3"

	"github.com/Suhaibinator/go-common/pkg/common_converter"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

const (
	personalWorkspaceDescription = "System-managed personal workspace."
)

// PersonalWorkspaceService is the trusted account-lifecycle boundary for a
// user's one lifetime personal workspace. Mutations require an ambient
// transaction; callers must not route these operations through ordinary
// permission-checked organization CRUD.
type PersonalWorkspaceService interface {
	EnsureForUser(ctx context.Context, user *common_service_models.User) (*common_service_models.Organization, error)
	GetForUser(ctx context.Context, ownerID common_service_models.UserIdType) (*common_service_models.Organization, error)
	// RootIDForUser returns the active lifetime root's organization ID, or nil
	// when the user has no active personal workspace root. It is a cached
	// read-through over GetForUser for request-path enrichment; callers that
	// must distinguish "unprovisioned" from "archived" use GetForUser instead.
	RootIDForUser(ctx context.Context, ownerID common_service_models.UserIdType) (*common_service_models.OrganizationIdType, error)
	SoftDeleteForUser(ctx context.Context, ownerID common_service_models.UserIdType) error
	RestoreForUser(ctx context.Context, ownerID common_service_models.UserIdType) (*common_service_models.Organization, error)
}

// PersonalWorkspaceServiceConfig is restart-bound managed-workspace policy.
type PersonalWorkspaceServiceConfig struct {
	OrganizationType proto_models.OrganizationType
	OwnerRole        proto_models.OrganizationRole
	// RootIDCacheCapacity and RootIDCacheTTL bound the owner→root-ID cache
	// behind RootIDForUser. The cache is disabled unless both are positive.
	RootIDCacheCapacity uint64
	RootIDCacheTTL      time.Duration
}

type personalWorkspaceService[U common_service_models.UserLike] struct {
	persistence      common_persistence.PersonalWorkspacePersistence
	converter        common_converter.ConverterInterface
	callbacks        *ports.OrganizationCallbackConfig[U]
	config           PersonalWorkspaceServiceConfig
	cacheInvalidator PersonalWorkspaceCacheInvalidator

	// rootIDCache memoizes RootIDForUser under the standard generation-guarded
	// fill protocol; every lifecycle mutation evicts the owner's entry in the
	// same post-commit hook that notifies cacheInvalidator. The service has no
	// Stop lifecycle, so no expiry janitor runs: expired entries are ignored on
	// read and memory stays bounded by RootIDCacheCapacity.
	rootIDCacheGuard cacheGenerationGuard
	rootIDCache      *generationCache[common_service_models.UserIdType, personalRootCacheEntry]
}

// personalRootCacheEntry is the immutable cached classification of a user's
// lifetime root: the active root's ID, or a negative entry meaning no active
// root exists (unprovisioned or archived).
type personalRootCacheEntry struct {
	rootID common_service_models.OrganizationIdType
	active bool
}

// idPointer returns a fresh pointer per call so callers can never mutate the
// cached entry through the returned value.
func (e personalRootCacheEntry) idPointer() *common_service_models.OrganizationIdType {
	if !e.active {
		return nil
	}
	id := e.rootID
	return &id
}

type activePersonalWorkspaceState struct {
	root                 *common_service_models.Organization
	tree                 []*common_service_models.Organization
	rootMembership       *common_persistence_models.UserOrganization
	rootMembershipChange common_persistence.PersonalWorkspaceMembershipChange
	changed              bool
}

// personalWorkspaceLifecycleFinish carries the validated state a lifecycle
// mutation hands to finishLifecycle for callback dispatch and invalidation.
type personalWorkspaceLifecycleFinish struct {
	root              *common_service_models.Organization
	tree              []*common_service_models.Organization
	rootMembership    *common_persistence_models.UserOrganization
	membershipCreated bool
	rootCreated       bool
	stateChanged      bool
	visibilityChanged bool
}

type personalTreeOwnerValidation uint8

const (
	personalTreeOwnerRequired personalTreeOwnerValidation = iota
	personalTreeMissingDescendantOwnerRepairable
)

var _ PersonalWorkspaceService = (*personalWorkspaceService[common_service_models.User])(nil)

// NewPersonalWorkspaceService constructs the trusted lifecycle component.
func NewPersonalWorkspaceService[U common_service_models.UserLike](
	persistence common_persistence.PersonalWorkspacePersistence,
	converter common_converter.ConverterInterface,
	callbacks *ports.OrganizationCallbackConfig[U],
	config PersonalWorkspaceServiceConfig,
	cacheInvalidator PersonalWorkspaceCacheInvalidator,
) (PersonalWorkspaceService, error) {
	if persistence == nil {
		return nil, fmt.Errorf("managed personal workspaces require persistence")
	}
	if converter == nil {
		return nil, fmt.Errorf("managed personal workspaces require a converter")
	}
	if config.OrganizationType == proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED {
		return nil, fmt.Errorf("managed personal workspaces require an organization type")
	}
	if config.OwnerRole == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		return nil, fmt.Errorf("managed personal workspaces require an owner role")
	}
	service := &personalWorkspaceService[U]{
		persistence:      persistence,
		converter:        converter,
		callbacks:        callbacks,
		config:           config,
		cacheInvalidator: cacheInvalidator,
	}
	if config.RootIDCacheCapacity > 0 && config.RootIDCacheTTL > 0 {
		service.rootIDCache = newGenerationCache(
			&service.rootIDCacheGuard,
			ttlcache.New(
				ttlcache.WithTTL[common_service_models.UserIdType, personalRootCacheEntry](config.RootIDCacheTTL),
				ttlcache.WithCapacity[common_service_models.UserIdType, personalRootCacheEntry](config.RootIDCacheCapacity),
				// Without this, steady reads extend an entry's life indefinitely,
				// voiding the TTL bound that limits cross-replica staleness
				// (post-commit eviction is per-instance).
				ttlcache.WithDisableTouchOnHit[common_service_models.UserIdType, personalRootCacheEntry](),
			),
		)
	}
	return service, nil
}

func (s *personalWorkspaceService[U]) requireTransaction(ctx context.Context) error {
	if _, ok := common_persistence.DatabasePostCommitHookRegistrarFromContext(ctx); !ok {
		return fmt.Errorf("%w: personal workspace lifecycle requires post-commit hook support", serviceerrors.ErrTransactionNotFound)
	}
	return nil
}

// EnsureForUser creates or repairs the active root and owner membership. All
// uniqueness races use conflict-safe persistence primitives, so a losing insert
// can reload and validate without aborting the ambient PostgreSQL transaction.
func (s *personalWorkspaceService[U]) EnsureForUser(
	ctx context.Context,
	user *common_service_models.User,
) (*common_service_models.Organization, error) {
	if err := s.requireTransaction(ctx); err != nil {
		return nil, err
	}
	if user == nil || user.ID == 0 {
		return nil, serviceerrors.ErrInvalidUserID
	}

	ownerID := user.ID
	if err := s.lockOwnerForLifecycle(ctx, ownerID, false); err != nil {
		return nil, err
	}
	root, err := s.persistence.GetPersonalWorkspaceRootByOwner(
		ctx,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceReadOptions{IncludeDeleted: true, ForUpdate: true},
	)
	created := false
	if err != nil && !stderrors.Is(err, common_persistence.ErrOrganizationNotFound) {
		return nil, s.mapPersistenceError(err)
	}
	if root != nil && root.DeletedAt.Valid {
		return nil, serviceerrors.ErrPersonalWorkspaceArchived
	}
	if root == nil {
		// With no lifetime root, any retained owner-marked row is orphaned
		// corruption. Reject it before treating a successful insert as a known
		// one-row tree.
		if err := s.ensureNoOwnerMarkedOrganizations(ctx, ownerID); err != nil {
			return nil, err
		}
		owner := ownerID
		managed := &common_service_models.Organization{
			Name:                personalWorkspaceName(user),
			Slug:                personalWorkspaceSlug(ownerID),
			Description:         personalWorkspaceDescription,
			OrganizationType:    s.config.OrganizationType,
			PersonalOwnerUserID: &owner,
		}
		// Managed creation bypasses ordinary organization CRUD, so the minted
		// fields must satisfy the same constraints validateOrganizationShape
		// applies to ordinary organizations.
		if err := validateManagedOrganizationFields(managed.Name, managed.Description); err != nil {
			return nil, err
		}
		persistenceRoot := s.converter.ServiceOrganizationToPersistence(managed)
		created, err = s.persistence.CreatePersonalWorkspaceRoot(ctx, persistenceRoot)
		if err != nil {
			return nil, s.mapPersistenceError(err)
		}
		if created {
			root = persistenceRoot
		} else {
			root, err = s.persistence.GetPersonalWorkspaceRootByOwner(
				ctx,
				uint64(ownerID),
				common_persistence.PersonalWorkspaceReadOptions{IncludeDeleted: true, ForUpdate: true},
			)
			if stderrors.Is(err, common_persistence.ErrOrganizationNotFound) {
				return nil, serviceerrors.ErrPersonalWorkspaceConflict
			}
			if err != nil {
				return nil, s.mapPersistenceError(err)
			}
			// The in-tree persistence never returns (nil, nil); guard against a
			// custom implementation doing so instead of dereferencing it.
			if root == nil {
				return nil, serviceerrors.ErrPersonalWorkspaceConflict
			}
			if root.DeletedAt.Valid {
				return nil, serviceerrors.ErrPersonalWorkspaceArchived
			}
		}
	}

	var state *activePersonalWorkspaceState
	if created {
		serviceRoot, err := s.validateRoot(root, ownerID)
		if err != nil {
			return nil, err
		}
		membership, membershipChange, err := s.ensureRootOwnerMembership(ctx, ownerID, serviceRoot.ID)
		if err != nil {
			return nil, err
		}
		state = &activePersonalWorkspaceState{
			root:                 serviceRoot,
			tree:                 []*common_service_models.Organization{serviceRoot},
			rootMembership:       membership,
			rootMembershipChange: membershipChange,
			changed:              membershipChange != common_persistence.PersonalWorkspaceMembershipUnchanged,
		}
	} else {
		state, err = s.validateAndRepairActiveTree(ctx, root, ownerID)
		if err != nil {
			return nil, err
		}
	}

	return s.finishLifecycle(ctx, ownerID, personalWorkspaceLifecycleFinish{
		root:              state.root,
		tree:              state.tree,
		rootMembership:    state.rootMembership,
		membershipCreated: state.rootMembershipChange == common_persistence.PersonalWorkspaceMembershipCreated,
		rootCreated:       created,
		stateChanged:      created || state.changed,
		visibilityChanged: created,
	})
}

// finishLifecycle owns the shared tail of every lifecycle mutation: consumer
// callback dispatch, post-callback revalidation, and post-commit cache
// invalidation whenever any change was observed.
func (s *personalWorkspaceService[U]) finishLifecycle(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
	finish personalWorkspaceLifecycleFinish,
) (*common_service_models.Organization, error) {
	callbackRan := false
	if finish.membershipCreated {
		ran, err := s.invokeMemberAdded(ctx, finish.rootMembership)
		if err != nil {
			return nil, err
		}
		callbackRan = callbackRan || ran
	}
	if finish.rootCreated {
		ran, err := s.invokeOrganizationCreated(ctx, finish.root)
		if err != nil {
			return nil, err
		}
		callbackRan = callbackRan || ran
	}
	ran, err := s.invokePersonalWorkspaceEnsured(ctx, finish.root)
	if err != nil {
		return nil, err
	}
	callbackRan = callbackRan || ran
	root, tree := finish.root, finish.tree
	if callbackRan {
		// The ports callback contract cannot distinguish a mutating callback
		// from a read-only one, so any executed callback forces revalidation
		// of the tree it may have changed inside the ambient transaction.
		state, err := s.reloadAndRevalidateActiveTree(ctx, ownerID)
		if err != nil {
			return nil, err
		}
		root, tree = state.root, state.tree
	}
	if finish.stateChanged || callbackRan {
		if err := s.invalidateAfterCommit(ctx, ownerID, tree, finish.visibilityChanged); err != nil {
			return nil, err
		}
	}
	return root, nil
}

// GetForUser returns only an active root. Archived state is classified so a
// caller cannot mistake it for an unprovisioned account.
func (s *personalWorkspaceService[U]) GetForUser(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
) (*common_service_models.Organization, error) {
	if ownerID == 0 {
		return nil, serviceerrors.ErrInvalidUserID
	}
	root, err := s.readActiveRoot(ctx, ownerID)
	if err == nil {
		return root, nil
	}
	if !stderrors.Is(err, serviceerrors.ErrPersonalWorkspaceNotFound) {
		return nil, err
	}
	exists, err := s.persistence.HasPersonalWorkspaceOrganizationsByOwner(ctx, uint64(ownerID))
	if err != nil {
		return nil, s.mapPersistenceError(err)
	}
	if !exists {
		return nil, serviceerrors.ErrPersonalWorkspaceNotFound
	}
	// This read runs without the lifecycle lock, so a concurrent EnsureForUser
	// can commit the root between the miss and the owner-marker probe. Re-read
	// the root once before classifying the marker as corruption.
	root, err = s.readActiveRoot(ctx, ownerID)
	if err == nil {
		return root, nil
	}
	if !stderrors.Is(err, serviceerrors.ErrPersonalWorkspaceNotFound) {
		return nil, err
	}
	return nil, fmt.Errorf("%w: user %d has owner-marked organizations without a lifetime root", serviceerrors.ErrPersonalWorkspaceConflict, ownerID)
}

// RootIDForUser returns the active lifetime root's ID for ownerID, or nil when
// no active root exists. "Never provisioned" and "archived" both collapse to
// nil because request-path callers surface the two identically; GetForUser
// keeps the full classification. Negative results are cached too: the
// post-commit eviction that follows every lifecycle mutation drops them, so a
// freshly ensured or backfilled workspace becomes visible on this instance
// immediately rather than after TTL expiry. Eviction is per-instance; other
// replicas converge within the TTL.
func (s *personalWorkspaceService[U]) RootIDForUser(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
) (*common_service_models.OrganizationIdType, error) {
	if ownerID == 0 {
		return nil, serviceerrors.ErrInvalidUserID
	}
	// An ambient transaction must observe its own snapshot and must never
	// populate the process-wide cache: its read could reflect uncommitted state
	// that later rolls back (no eviction would follow), or a snapshot older
	// than an eviction it observed. Bypass both lookup and fill, mirroring the
	// organization handler's cached reads.
	cacheable := s.rootIDCache != nil && !hasAmbientTransaction(ctx)
	var generation uint64
	if cacheable {
		entry, found, observed := s.rootIDCache.Lookup(ownerID)
		if found {
			return entry.idPointer(), nil
		}
		generation = observed
	}
	entry, err := s.classifyRootID(ctx, ownerID)
	if err != nil {
		// Unexpected errors are never cached; the next call re-reads.
		return nil, err
	}
	if cacheable {
		s.rootIDCache.FillIfCurrent(ownerID, entry, generation)
	}
	return entry.idPointer(), nil
}

// classifyRootID maps GetForUser's classification onto the cacheable
// entry shape: absence variants become a negative entry, everything else is an
// error the caller must not cache.
func (s *personalWorkspaceService[U]) classifyRootID(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
) (personalRootCacheEntry, error) {
	root, err := s.GetForUser(ctx, ownerID)
	switch {
	case err == nil:
		return personalRootCacheEntry{rootID: root.ID, active: true}, nil
	case stderrors.Is(err, serviceerrors.ErrPersonalWorkspaceNotFound),
		stderrors.Is(err, serviceerrors.ErrPersonalWorkspaceArchived):
		return personalRootCacheEntry{}, nil
	default:
		return personalRootCacheEntry{}, err
	}
}

// readActiveRoot fetches and classifies the lifetime root without locking:
// active roots validate, archived roots classify distinctly, and a missing row
// maps to ErrPersonalWorkspaceNotFound.
func (s *personalWorkspaceService[U]) readActiveRoot(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
) (*common_service_models.Organization, error) {
	root, err := s.persistence.GetPersonalWorkspaceRootByOwner(
		ctx,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceReadOptions{IncludeDeleted: true},
	)
	if err != nil {
		return nil, s.mapPersistenceError(err)
	}
	// The in-tree persistence never returns (nil, nil); guard against a custom
	// implementation doing so instead of dereferencing it.
	if root == nil {
		return nil, serviceerrors.ErrPersonalWorkspaceNotFound
	}
	if root.DeletedAt.Valid {
		return nil, serviceerrors.ErrPersonalWorkspaceArchived
	}
	return s.validateRoot(root, ownerID)
}

// ensureNoOwnerMarkedOrganizations distinguishes a genuinely unprovisioned
// user from a corrupt tree whose owner marker survived without a lifetime
// root. Lifecycle callers must quarantine the latter rather than ignore it.
func (s *personalWorkspaceService[U]) ensureNoOwnerMarkedOrganizations(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
) error {
	exists, err := s.persistence.HasPersonalWorkspaceOrganizationsByOwner(ctx, uint64(ownerID))
	if err != nil {
		return s.mapPersistenceError(err)
	}
	if exists {
		return serviceerrors.ErrPersonalWorkspaceConflict
	}
	return nil
}

// SoftDeleteForUser archives the authoritative root and every validated row
// discovered by parent traversal, lineage, or owner marker while retaining
// ownership, memberships, and dependent data.
func (s *personalWorkspaceService[U]) SoftDeleteForUser(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
) error {
	if err := s.requireTransaction(ctx); err != nil {
		return err
	}
	if ownerID == 0 {
		return serviceerrors.ErrInvalidUserID
	}
	if err := s.lockOwnerForLifecycle(ctx, ownerID, true); err != nil {
		return err
	}
	root, err := s.persistence.GetPersonalWorkspaceRootByOwner(
		ctx,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceReadOptions{IncludeDeleted: true, ForUpdate: true},
	)
	if stderrors.Is(err, common_persistence.ErrOrganizationNotFound) {
		if err := s.ensureNoOwnerMarkedOrganizations(ctx, ownerID); err != nil {
			return err
		}
		return nil
	}
	if err != nil {
		return s.mapPersistenceError(err)
	}
	if _, err := s.validateRoot(root, ownerID); err != nil {
		return err
	}
	tree, err := s.persistence.GetPersonalWorkspaceTree(
		ctx,
		root.ID,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceReadOptions{IncludeDeleted: true, ForUpdate: true},
	)
	if err != nil {
		return s.mapPersistenceError(err)
	}
	// A null-owner descendant is the same repairable shape EnsureForUser
	// accepts. Repair it before archiving so account deletion cannot wedge on
	// state a provisioning retry would have fixed.
	if err := validatePersonalTreeRows(tree, root.ID, ownerID, personalTreeMissingDescendantOwnerRepairable); err != nil {
		return err
	}
	missingOwnerIDs := make([]int64, 0, len(tree))
	for _, organization := range tree {
		if organization != nil && organization.PersonalOwnerUserID == nil {
			missingOwnerIDs = append(missingOwnerIDs, organization.ID)
		}
	}
	if len(missingOwnerIDs) != 0 {
		if err := s.persistence.SetMissingPersonalWorkspaceTreeOwner(ctx, uint64(ownerID), root.ID, missingOwnerIDs); err != nil {
			return s.mapPersistenceError(err)
		}
		tree, err = s.persistence.GetPersonalWorkspaceTree(
			ctx,
			root.ID,
			uint64(ownerID),
			common_persistence.PersonalWorkspaceReadOptions{IncludeDeleted: true, ForUpdate: true},
		)
		if err != nil {
			return s.mapPersistenceError(err)
		}
		if err := validatePersonalTreeRows(tree, root.ID, ownerID, personalTreeOwnerRequired); err != nil {
			return err
		}
	}
	activeIDs := make([]int64, 0, len(tree))
	for _, organization := range tree {
		if organization != nil && !organization.DeletedAt.Valid {
			activeIDs = append(activeIDs, organization.ID)
		}
	}
	if len(activeIDs) == 0 {
		return nil
	}
	if err := s.persistence.SoftDeletePersonalWorkspaceOrganizations(ctx, activeIDs, uint64(ownerID)); err != nil {
		return s.mapPersistenceError(err)
	}
	if err := s.invalidateAfterCommit(
		ctx,
		ownerID,
		persistenceOrganizationsToService(s.converter, tree),
		true,
	); err != nil {
		return err
	}
	return nil
}

// RestoreForUser restores the archived subset of the retained tree without
// allocating replacement rows. The core user must be active in the same
// transaction before restoration begins.
func (s *personalWorkspaceService[U]) RestoreForUser(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
) (*common_service_models.Organization, error) {
	if err := s.requireTransaction(ctx); err != nil {
		return nil, err
	}
	if ownerID == 0 {
		return nil, serviceerrors.ErrInvalidUserID
	}
	if err := s.lockOwnerForLifecycle(ctx, ownerID, false); err != nil {
		return nil, err
	}
	root, err := s.persistence.GetPersonalWorkspaceRootByOwner(
		ctx,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceReadOptions{IncludeDeleted: true, ForUpdate: true},
	)
	if stderrors.Is(err, common_persistence.ErrOrganizationNotFound) {
		if err := s.ensureNoOwnerMarkedOrganizations(ctx, ownerID); err != nil {
			return nil, err
		}
		return nil, serviceerrors.ErrPersonalWorkspaceNotFound
	}
	if err != nil {
		return nil, s.mapPersistenceError(err)
	}
	serviceRoot, err := s.validateRoot(root, ownerID)
	if err != nil {
		return nil, err
	}
	tree, err := s.persistence.GetPersonalWorkspaceTree(
		ctx,
		root.ID,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceReadOptions{IncludeDeleted: true, ForUpdate: true},
	)
	if err != nil {
		return nil, s.mapPersistenceError(err)
	}
	if err := validatePersonalTreeRows(tree, root.ID, ownerID, personalTreeOwnerRequired); err != nil {
		return nil, err
	}
	ids := personalTreeIDs(tree)
	memberships, err := s.validateSoloMembership(ctx, ids, ownerID)
	if err != nil {
		return nil, err
	}
	descendantRolesRepaired, err := s.repairDescendantOwnerMembershipRoles(ctx, memberships, root.ID, ownerID)
	if err != nil {
		return nil, err
	}
	// Restore reverses lifecycle archival only, never user deletions.
	// SoftDeletePersonalWorkspaceOrganizations archives the whole tree in a
	// single UPDATE, so every row it touched — including the root — shares the
	// root's exact stored deleted_at. A descendant the user deleted earlier
	// through ordinary organization deletion was archived by a different
	// statement and carries an earlier timestamp, so it stays deleted. The
	// equality check is reliable because both values are read back from the
	// same stored column rather than recomputed in Go. An active root has no
	// archival timestamp, so nothing matches and nothing is resurrected.
	archivedIDs := make([]int64, 0, len(tree))
	for _, organization := range tree {
		if organization != nil && organization.DeletedAt.Valid &&
			root.DeletedAt.Valid && organization.DeletedAt.Time.Equal(root.DeletedAt.Time) {
			archivedIDs = append(archivedIDs, organization.ID)
		}
	}
	restored := len(archivedIDs) != 0
	if restored {
		if err := s.persistence.RestorePersonalWorkspaceOrganizations(ctx, archivedIDs, uint64(ownerID)); err != nil {
			return nil, s.mapPersistenceError(err)
		}
		// Reload the authoritative root instead of patching the stale
		// in-memory rows; the pre-restore tree rows are retained only for
		// their immutable cache identities.
		root, err = s.persistence.GetPersonalWorkspaceRootByOwner(
			ctx,
			uint64(ownerID),
			common_persistence.PersonalWorkspaceReadOptions{ForUpdate: true},
		)
		if err != nil {
			return nil, s.mapPersistenceError(err)
		}
		serviceRoot, err = s.validateRoot(root, ownerID)
		if err != nil {
			return nil, err
		}
	}
	membership, membershipChange, err := s.ensureRootOwnerMembership(ctx, ownerID, root.ID)
	if err != nil {
		return nil, err
	}
	return s.finishLifecycle(ctx, ownerID, personalWorkspaceLifecycleFinish{
		root:              serviceRoot,
		tree:              persistenceOrganizationsToService(s.converter, tree),
		rootMembership:    membership,
		membershipCreated: membershipChange == common_persistence.PersonalWorkspaceMembershipCreated,
		stateChanged: restored || descendantRolesRepaired ||
			membershipChange != common_persistence.PersonalWorkspaceMembershipUnchanged,
		visibilityChanged: restored,
	})
}

func (s *personalWorkspaceService[U]) validateAndRepairActiveTree(
	ctx context.Context,
	root *common_persistence_models.Organization,
	ownerID common_service_models.UserIdType,
) (*activePersonalWorkspaceState, error) {
	serviceRoot, err := s.validateRoot(root, ownerID)
	if err != nil {
		return nil, err
	}
	treeRows, err := s.persistence.GetPersonalWorkspaceTree(
		ctx,
		root.ID,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceReadOptions{IncludeDeleted: true, ForUpdate: true},
	)
	if err != nil {
		return nil, s.mapPersistenceError(err)
	}
	// Archived rows under an active root are legitimate user deletions, not
	// corruption: ordinary organization deletion archives a descendant's whole
	// subtree in place (deleteOrganizationsByLineage) while the root stays
	// active. The active lifecycle treats those rows as absent — excluded from
	// shape validation, owner repair, membership audit, and cache keys — and
	// the remaining active rows must form a valid tree on their own.
	activeRows := activePersonalTreeRows(treeRows)
	if err := validatePersonalTreeRows(activeRows, root.ID, ownerID, personalTreeMissingDescendantOwnerRepairable); err != nil {
		return nil, err
	}
	missingOwnerIDs := make([]int64, 0, len(activeRows))
	for _, organization := range activeRows {
		if organization != nil && organization.PersonalOwnerUserID == nil {
			missingOwnerIDs = append(missingOwnerIDs, organization.ID)
		}
	}
	missingOwner := len(missingOwnerIDs) != 0
	if missingOwner {
		if err := s.persistence.SetMissingPersonalWorkspaceTreeOwner(ctx, uint64(ownerID), root.ID, missingOwnerIDs); err != nil {
			return nil, s.mapPersistenceError(err)
		}
		treeRows, err = s.persistence.GetPersonalWorkspaceTree(
			ctx,
			root.ID,
			uint64(ownerID),
			common_persistence.PersonalWorkspaceReadOptions{IncludeDeleted: true, ForUpdate: true},
		)
		if err != nil {
			return nil, s.mapPersistenceError(err)
		}
		activeRows = activePersonalTreeRows(treeRows)
		if err := validatePersonalTreeRows(activeRows, root.ID, ownerID, personalTreeOwnerRequired); err != nil {
			return nil, err
		}
	}
	for _, organization := range activeRows {
		if organization.ID == root.ID {
			serviceRoot = s.converter.PersistenceOrganizationToService(organization)
		}
	}
	ids := personalTreeIDs(activeRows)
	memberships, err := s.validateSoloMembership(ctx, ids, ownerID)
	if err != nil {
		return nil, err
	}
	descendantRolesRepaired, err := s.repairDescendantOwnerMembershipRoles(ctx, memberships, root.ID, ownerID)
	if err != nil {
		return nil, err
	}
	rootMembership := findPersonalWorkspaceMembership(memberships, root.ID)
	membershipChange := common_persistence.PersonalWorkspaceMembershipUnchanged
	if rootMembership == nil || rootMembership.Role != s.config.OwnerRole {
		rootMembership, membershipChange, err = s.ensureRootOwnerMembership(ctx, ownerID, root.ID)
		if err != nil {
			return nil, err
		}
	}
	return &activePersonalWorkspaceState{
		root:                 serviceRoot,
		tree:                 persistenceOrganizationsToService(s.converter, activeRows),
		rootMembership:       rootMembership,
		rootMembershipChange: membershipChange,
		changed: missingOwner || descendantRolesRepaired ||
			membershipChange != common_persistence.PersonalWorkspaceMembershipUnchanged,
	}, nil
}

func (s *personalWorkspaceService[U]) lockOwnerForLifecycle(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
	includeDeleted bool,
) error {
	err := s.persistence.LockPersonalWorkspaceOwner(
		ctx,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceOwnerLockOptions{IncludeDeleted: includeDeleted},
	)
	if err == nil {
		return nil
	}
	if stderrors.Is(err, common_persistence.ErrUserNotFound) {
		return serviceerrors.ErrUserNotFound
	}
	return s.mapPersistenceError(err)
}

func (s *personalWorkspaceService[U]) reloadAndRevalidateActiveTree(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
) (*activePersonalWorkspaceState, error) {
	root, err := s.persistence.GetPersonalWorkspaceRootByOwner(
		ctx,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceReadOptions{ForUpdate: true},
	)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrOrganizationNotFound) {
			return nil, serviceerrors.ErrPersonalWorkspaceConflict
		}
		return nil, s.mapPersistenceError(err)
	}
	// The audit repairs callback mutations to the owner membership without
	// emitting another member callback and creating a callback loop.
	return s.validateAndRepairActiveTree(ctx, root, ownerID)
}

func findPersonalWorkspaceMembership(
	memberships []*common_persistence_models.UserOrganization,
	organizationID int64,
) *common_persistence_models.UserOrganization {
	for _, membership := range memberships {
		if membership != nil && membership.OrganizationID == organizationID {
			return membership
		}
	}
	return nil
}

func (s *personalWorkspaceService[U]) ensureRootOwnerMembership(
	ctx context.Context,
	ownerID common_service_models.UserIdType,
	rootOrganizationID int64,
) (*common_persistence_models.UserOrganization, common_persistence.PersonalWorkspaceMembershipChange, error) {
	result, err := s.persistence.EnsurePersonalWorkspaceOwnerMembership(
		ctx,
		uint64(ownerID),
		rootOrganizationID,
		s.config.OwnerRole,
	)
	if err != nil {
		return nil, common_persistence.PersonalWorkspaceMembershipUnchanged, s.mapPersistenceError(err)
	}
	if result == nil {
		return nil, common_persistence.PersonalWorkspaceMembershipUnchanged, serviceerrors.ErrPersonalWorkspaceConflict
	}
	membership := result.Membership
	if membership == nil || membership.UserID != uint64(ownerID) ||
		membership.OrganizationID != rootOrganizationID || membership.Role != s.config.OwnerRole {
		return nil, common_persistence.PersonalWorkspaceMembershipUnchanged, serviceerrors.ErrPersonalWorkspaceConflict
	}
	if result.Change > common_persistence.PersonalWorkspaceMembershipRepaired {
		return nil, common_persistence.PersonalWorkspaceMembershipUnchanged, serviceerrors.ErrPersonalWorkspaceConflict
	}
	return membership, result.Change, nil
}

// validateRoot enforces the root's identity invariants: ID, hierarchy
// position, owner marker, and minted slug. The managed description is
// cosmetic — UpdateOrganization already keeps it immutable — so it is
// deliberately not part of root identity: a copy change to the constant must
// never invalidate existing workspaces' lifecycles.
func (s *personalWorkspaceService[U]) validateRoot(
	root *common_persistence_models.Organization,
	ownerID common_service_models.UserIdType,
) (*common_service_models.Organization, error) {
	if root == nil || root.ID == 0 {
		return nil, fmt.Errorf("%w: personal root row for user %d is missing or has no ID", serviceerrors.ErrPersonalWorkspaceConflict, ownerID)
	}
	if root.ParentOrganizationID != nil || len(root.Lineage) != 0 {
		return nil, fmt.Errorf("%w: personal root organization %d for user %d is not a hierarchy root", serviceerrors.ErrPersonalWorkspaceConflict, root.ID, ownerID)
	}
	if root.PersonalOwnerUserID == nil || *root.PersonalOwnerUserID != uint64(ownerID) {
		return nil, fmt.Errorf("%w: personal root organization %d is not owner-marked for user %d", serviceerrors.ErrPersonalWorkspaceConflict, root.ID, ownerID)
	}
	if root.Slug != personalWorkspaceSlug(ownerID) {
		return nil, fmt.Errorf("%w: personal root organization %d slug %q is not the minted slug for user %d", serviceerrors.ErrPersonalWorkspaceConflict, root.ID, root.Slug, ownerID)
	}
	return s.converter.PersistenceOrganizationToService(root), nil
}

func (s *personalWorkspaceService[U]) validateSoloMembership(
	ctx context.Context,
	organizationIDs []int64,
	ownerID common_service_models.UserIdType,
) ([]*common_persistence_models.UserOrganization, error) {
	memberships, err := s.persistence.ListPersonalWorkspaceMemberships(ctx, organizationIDs)
	if err != nil {
		return nil, s.mapPersistenceError(err)
	}
	knownOrganizations := make(map[int64]struct{}, len(organizationIDs))
	for _, organizationID := range organizationIDs {
		knownOrganizations[organizationID] = struct{}{}
	}
	for _, membership := range memberships {
		if membership == nil || membership.UserID != uint64(ownerID) {
			return nil, serviceerrors.ErrPersonalWorkspaceConflict
		}
		if _, known := knownOrganizations[membership.OrganizationID]; !known {
			return nil, serviceerrors.ErrPersonalWorkspaceConflict
		}
	}
	return memberships, nil
}

func (s *personalWorkspaceService[U]) repairDescendantOwnerMembershipRoles(
	ctx context.Context,
	memberships []*common_persistence_models.UserOrganization,
	rootOrganizationID int64,
	ownerID common_service_models.UserIdType,
) (bool, error) {
	wrongRoleOrganizationIDs := make([]int64, 0)
	for _, membership := range memberships {
		if membership.OrganizationID != rootOrganizationID && membership.Role != s.config.OwnerRole {
			wrongRoleOrganizationIDs = append(wrongRoleOrganizationIDs, membership.OrganizationID)
		}
	}
	if len(wrongRoleOrganizationIDs) == 0 {
		return false, nil
	}
	if err := s.persistence.RepairPersonalWorkspaceOwnerMembershipRoles(
		ctx,
		uint64(ownerID),
		wrongRoleOrganizationIDs,
		s.config.OwnerRole,
	); err != nil {
		return false, s.mapPersistenceError(err)
	}
	return true, nil
}

func validatePersonalTreeRows(
	organizations []*common_persistence_models.Organization,
	rootID int64,
	ownerID common_service_models.UserIdType,
	ownerValidation personalTreeOwnerValidation,
) error {
	organizationsByID := make(map[int64]*common_persistence_models.Organization, len(organizations))
	for _, organization := range organizations {
		if organization == nil || organization.ID == 0 {
			return fmt.Errorf("%w: personal tree under root %d contains a row without an ID", serviceerrors.ErrPersonalWorkspaceConflict, rootID)
		}
		if _, duplicate := organizationsByID[organization.ID]; duplicate {
			return fmt.Errorf("%w: personal tree under root %d contains organization %d twice", serviceerrors.ErrPersonalWorkspaceConflict, rootID, organization.ID)
		}
		organizationsByID[organization.ID] = organization
	}

	rootCount := 0
	for _, organization := range organizations {
		if organization.ID == rootID {
			rootCount++
			if organization.ParentOrganizationID != nil || len(organization.Lineage) != 0 {
				return fmt.Errorf("%w: personal root organization %d is not a hierarchy root", serviceerrors.ErrPersonalWorkspaceConflict, rootID)
			}
		} else {
			if organization.ParentOrganizationID == nil || *organization.ParentOrganizationID == organization.ID {
				return fmt.Errorf("%w: personal descendant organization %d has no valid parent", serviceerrors.ErrPersonalWorkspaceConflict, organization.ID)
			}
			parent, parentPresent := organizationsByID[*organization.ParentOrganizationID]
			if !parentPresent || parent == nil {
				return fmt.Errorf("%w: personal descendant organization %d references parent %d outside the tree under root %d", serviceerrors.ErrPersonalWorkspaceConflict, organization.ID, *organization.ParentOrganizationID, rootID)
			}
			expectedLineage := append(append([]int64(nil), parent.Lineage...), parent.ID)
			if len(organization.Lineage) != len(expectedLineage) {
				return fmt.Errorf("%w: personal descendant organization %d lineage disagrees with parent %d adjacency", serviceerrors.ErrPersonalWorkspaceConflict, organization.ID, parent.ID)
			}
			for index := range expectedLineage {
				if organization.Lineage[index] != expectedLineage[index] {
					return fmt.Errorf("%w: personal descendant organization %d lineage disagrees with parent %d adjacency", serviceerrors.ErrPersonalWorkspaceConflict, organization.ID, parent.ID)
				}
			}
			seenAncestors := make(map[int64]struct{}, len(organization.Lineage))
			for _, ancestorID := range organization.Lineage {
				if ancestorID == organization.ID {
					return fmt.Errorf("%w: personal descendant organization %d lineage contains itself", serviceerrors.ErrPersonalWorkspaceConflict, organization.ID)
				}
				if _, duplicate := seenAncestors[ancestorID]; duplicate {
					return fmt.Errorf("%w: personal descendant organization %d lineage repeats ancestor %d", serviceerrors.ErrPersonalWorkspaceConflict, organization.ID, ancestorID)
				}
				seenAncestors[ancestorID] = struct{}{}
			}
			if len(organization.Lineage) == 0 || organization.Lineage[0] != rootID {
				return fmt.Errorf("%w: personal descendant organization %d lineage does not start at root %d", serviceerrors.ErrPersonalWorkspaceConflict, organization.ID, rootID)
			}
		}
		if organization.PersonalOwnerUserID == nil {
			if ownerValidation != personalTreeMissingDescendantOwnerRepairable || organization.ID == rootID {
				return fmt.Errorf("%w: personal organization %d under root %d has no owner marker", serviceerrors.ErrPersonalWorkspaceConflict, organization.ID, rootID)
			}
		} else if *organization.PersonalOwnerUserID != uint64(ownerID) {
			return fmt.Errorf("%w: personal organization %d is owner-marked for user %d, not user %d", serviceerrors.ErrPersonalWorkspaceConflict, organization.ID, *organization.PersonalOwnerUserID, ownerID)
		}
	}
	if rootCount != 1 {
		return fmt.Errorf("%w: personal tree rows contain %d copies of root %d", serviceerrors.ErrPersonalWorkspaceConflict, rootCount, rootID)
	}
	return nil
}

// activePersonalTreeRows keeps the rows the active-tree lifecycle reasons
// about: unarchived rows, plus any nil row so shape validation still rejects
// it as corruption. Archived rows under an active root are user deletions and
// are treated as absent.
func activePersonalTreeRows(organizations []*common_persistence_models.Organization) []*common_persistence_models.Organization {
	active := make([]*common_persistence_models.Organization, 0, len(organizations))
	for _, organization := range organizations {
		if organization == nil || !organization.DeletedAt.Valid {
			active = append(active, organization)
		}
	}
	return active
}

func personalTreeIDs(organizations []*common_persistence_models.Organization) []int64 {
	return common_util.ApplyToEachFiltered(
		organizations,
		func(organization *common_persistence_models.Organization) int64 { return organization.ID },
		func(organization *common_persistence_models.Organization) bool { return organization != nil },
		nil,
	)
}

func persistenceOrganizationsToService(
	converter common_converter.ConverterInterface,
	organizations []*common_persistence_models.Organization,
) []*common_service_models.Organization {
	return common_util.ApplyToEachFiltered(
		organizations,
		converter.PersistenceOrganizationToService,
		func(organization *common_persistence_models.Organization) bool { return organization != nil },
		nil,
	)
}

func (s *personalWorkspaceService[U]) organizationCallbacks() *ports.OrganizationCallbacks {
	if s.callbacks == nil {
		return nil
	}
	return s.callbacks.Callbacks
}

func (s *personalWorkspaceService[U]) invokeMemberAdded(
	ctx context.Context,
	membership *common_persistence_models.UserOrganization,
) (bool, error) {
	callbacks := s.organizationCallbacks()
	if callbacks == nil || callbacks.OnOrganizationMemberAdded == nil {
		return false, nil
	}
	if err := callbacks.OnOrganizationMemberAdded(ctx, s.converter.PersistenceUserOrganizationToService(membership)); err != nil {
		return true, fmt.Errorf("%w: %w", serviceerrors.ErrOrganizationCallbackFailed, err)
	}
	return true, nil
}

func (s *personalWorkspaceService[U]) invokeOrganizationCreated(
	ctx context.Context,
	root *common_service_models.Organization,
) (bool, error) {
	callbacks := s.organizationCallbacks()
	if callbacks == nil || callbacks.OnOrganizationCreated == nil {
		return false, nil
	}
	if err := callbacks.OnOrganizationCreated(ctx, root); err != nil {
		return true, fmt.Errorf("%w: %w", serviceerrors.ErrOrganizationCallbackFailed, err)
	}
	return true, nil
}

func (s *personalWorkspaceService[U]) invokePersonalWorkspaceEnsured(
	ctx context.Context,
	root *common_service_models.Organization,
) (bool, error) {
	callbacks := s.organizationCallbacks()
	if callbacks == nil || callbacks.OnPersonalWorkspaceEnsured == nil {
		return false, nil
	}
	if err := callbacks.OnPersonalWorkspaceEnsured(ctx, root); err != nil {
		return true, fmt.Errorf("%w: %w", serviceerrors.ErrOrganizationCallbackFailed, err)
	}
	return true, nil
}

func (s *personalWorkspaceService[U]) invalidateAfterCommit(
	ctx context.Context,
	ownerUserID common_service_models.UserIdType,
	organizations []*common_service_models.Organization,
	visibilityChanged bool,
) error {
	if s.cacheInvalidator == nil && s.rootIDCache == nil {
		return nil
	}
	keys := common_util.ApplyToEachFiltered(
		organizations,
		func(organization *common_service_models.Organization) PersonalWorkspaceCacheKey {
			return PersonalWorkspaceCacheKey{ID: organization.ID, Slug: organization.Slug}
		},
		func(organization *common_service_models.Organization) bool { return organization != nil },
		nil,
	)
	// keys is built fresh above, so the post-commit hook owns the only
	// reference and the payload stays immutable without defensive copies.
	invalidation := PersonalWorkspaceCacheInvalidation{
		TreeKeys:          keys,
		OwnerUserID:       ownerUserID,
		VisibilityChanged: visibilityChanged,
	}
	if !common_persistence.AddPostCommitHookFromContext(ctx, func(context.Context) error {
		// Evict the owner's root-ID entry under a generation bump before the
		// external invalidator runs, so an in-flight RootIDForUser fill that
		// read pre-commit state is rejected. Invalidate is nil-safe.
		s.rootIDCache.Invalidate(ownerUserID)
		if s.cacheInvalidator != nil {
			s.cacheInvalidator.InvalidatePersonalWorkspaceCaches(invalidation)
		}
		return nil
	}) {
		return fmt.Errorf("%w: personal workspace cache invalidation hook unavailable", serviceerrors.ErrTransactionNotFound)
	}
	return nil
}

func (s *personalWorkspaceService[U]) mapPersistenceError(err error) error {
	switch {
	case err == nil:
		return nil
	case stderrors.Is(err, common_persistence.ErrPersonalWorkspaceConflict):
		// Preserve the persistence detail (e.g. "repaired 1 of 2 owner
		// markers"). The persistence error is deliberately not %w-wrapped so
		// errors.Is matches only the service sentinel across the boundary.
		return fmt.Errorf("%w: %v", serviceerrors.ErrPersonalWorkspaceConflict, err)
	case stderrors.Is(err, common_persistence.ErrOrganizationNotFound):
		return serviceerrors.ErrPersonalWorkspaceNotFound
	default:
		return fmt.Errorf("%w: %v", serviceerrors.ErrInternalError, err)
	}
}

func personalWorkspaceSlug(ownerID common_service_models.UserIdType) string {
	return fmt.Sprintf("%s-%d", personalWorkspaceSlugNamespace, ownerID)
}

func personalWorkspaceName(user *common_service_models.User) string {
	name := strings.TrimSpace(strings.TrimSpace(user.FirstName) + " " + strings.TrimSpace(user.LastName))
	if name == "" {
		name = strings.TrimSpace(user.Username)
	}
	if name == "" {
		local, _, _ := strings.Cut(strings.TrimSpace(user.Email), "@")
		name = strings.TrimSpace(local)
	}
	if name == "" {
		return "Personal Workspace"
	}
	const suffix = "'s Workspace"
	// Truncate the base name first, reserving room for the suffix, so a long
	// name can never cut the suffix mid-word.
	base := common_util.TruncateRunes(name, common_service_models.MaxOrganizationNameLength-utf8.RuneCountInString(suffix))
	return base + suffix
}

// validateManagedOrganizationFields mirrors the name and description
// constraints of OrganizationHandler.validateOrganizationShape for the managed
// creation path, which bypasses ordinary organization CRUD validation. Slug
// validation is unnecessary: the minted "personal-<ownerID>" slug is
// well-formed by construction.
func validateManagedOrganizationFields(name, description string) error {
	if strings.TrimSpace(name) == "" {
		return serviceerrors.ErrOrganizationNameRequired
	}
	if utf8.RuneCountInString(name) > common_service_models.MaxOrganizationNameLength {
		return serviceerrors.ErrOrganizationNameTooLong
	}
	if utf8.RuneCountInString(description) > common_service_models.MaxOrganizationDescriptionLength {
		return serviceerrors.ErrOrganizationDescriptionTooLong
	}
	return nil
}
