package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Organization-specific error variables for consistent error handling.
var (
	// ErrOrganizationNotFound is returned when an organization lookup fails to find a matching organization.
	ErrOrganizationNotFound = errors.New("organization not found")

	// ErrOrganizationAlreadyExists is returned when attempting to create an organization with a slug
	// that already exists in the system.
	ErrOrganizationAlreadyExists = errors.New("organization already exists")

	// ErrUserOrganizationNotFound is returned when a user-organization relationship lookup fails.
	ErrUserOrganizationNotFound = errors.New("user organization relationship not found")

	// ErrUserAlreadyInOrganization is returned when attempting to add a user to an organization
	// they are already a member of.
	ErrUserAlreadyInOrganization = errors.New("user already in organization")

	// ErrOrganizationLastOwner is returned by the guarded membership mutations when removing or
	// demoting the target would leave the organization without an explicit owner.
	ErrOrganizationLastOwner = errors.New("operation would leave the organization without an owner")

	// ErrOrganizationEmailDomainExists is returned when attempting to add a domain that is already mapped to an organization.
	ErrOrganizationEmailDomainExists = errors.New("organization email domain already exists")

	// ErrOrganizationEmailDomainNotFound is returned when a domain mapping lookup fails.
	ErrOrganizationEmailDomainNotFound = errors.New("organization email domain not found")

	// ErrPersonalWorkspaceConflict indicates that persisted personal-workspace
	// rows violate an invariant that cannot be repaired safely by the shared
	// lifecycle service.
	ErrPersonalWorkspaceConflict = errors.New("personal workspace state conflicts with managed invariants")
)

// normalizeOrganizationTypeValue executes its operation in the persistence flow between service handlers and the database layer.
func normalizeOrganizationTypeValue(organizationType proto_models.OrganizationType) proto_models.OrganizationType {
	if organizationType == proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED {
		return proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0
	}
	return organizationType
}

func applyOrganizationTypeFilter(
	query *gorm.DB,
	organizationTypeFilter proto_models.OrganizationType,
) *gorm.DB {
	if organizationTypeFilter == proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED {
		return query
	}

	normalizedFilter := normalizeOrganizationTypeValue(organizationTypeFilter)
	if normalizedFilter == organizationTypeFilter {
		defaultType := normalizeOrganizationTypeValue(proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED)
		if normalizedFilter == defaultType {
			return query.Where(
				"organization_type = ? OR organization_type = ?",
				organizationTypeFilter,
				proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED,
			)
		}
		return query.Where("organization_type = ?", organizationTypeFilter)
	}

	return query.Where("organization_type = ?", organizationTypeFilter)
}

func buildAdminOrganizationsBaseQuery(
	db *gorm.DB,
	search string,
	organizationTypeFilter proto_models.OrganizationType,
	parentOrganizationIDFilter *int64,
	depthFilter *uint32,
) *gorm.DB {
	query := db.Model(&common_persistence_models.Organization{})

	search = strings.TrimSpace(search)
	if search != "" {
		searchPattern := "%" + search + "%"
		searchQuery := db.Where(
			"name ILIKE ? OR slug ILIKE ? OR COALESCE(description, '') ILIKE ?",
			searchPattern,
			searchPattern,
			searchPattern,
		)
		if id, err := strconv.ParseInt(search, 10, 64); err == nil {
			searchQuery = searchQuery.Or("id = ?", id)
		}
		query = query.Where(searchQuery)
	}

	query = applyOrganizationTypeFilter(query, organizationTypeFilter)

	if parentOrganizationIDFilter != nil {
		query = query.Where("parent_organization_id = ?", *parentOrganizationIDFilter)
	}

	if depthFilter != nil {
		query = query.Where("cardinality(COALESCE(lineage, ARRAY[]::bigint[])) = ?", *depthFilter)
	}

	return query
}

// buildUserOrganizationsViewThresholdsCTE executes its operation in the persistence flow between service handlers and the database layer.
func buildUserOrganizationsViewThresholdsCTE(values []proto_models.OrganizationRole) (string, []any) {
	if len(values) == 0 {
		return "SELECT NULL::integer AS depth, NULL::integer AS required_view_role WHERE FALSE", nil
	}

	placeholders := make([]string, len(values))
	args := make([]any, 0, len(values)*2)
	for depth, role := range values {
		placeholders[depth] = "(?::integer, ?::integer)"
		args = append(args, depth, int32(role))
	}

	return "VALUES " + strings.Join(placeholders, ", "), args
}

// buildUserOrganizationsRequestFiltersCTE executes its operation in the persistence flow between service handlers and the database layer.
func buildUserOrganizationsRequestFiltersCTE(filters []*proto_models.OrganizationMembershipMaskFilter) (string, []any) {
	if len(filters) == 0 {
		return "SELECT NULL::integer AS depth, NULL::bigint AS explicit_role_mask, NULL::bigint AS organization_type_mask WHERE FALSE", nil
	}

	placeholders := make([]string, len(filters))
	args := make([]any, 0, len(filters)*3)
	for i, filter := range filters {
		placeholders[i] = "(?::integer, ?::bigint, ?::bigint)"
		args = append(args, int32(filter.GetDepth()), int64(filter.GetExplicitRoleMask()), int64(filter.GetOrganizationTypeMask()))
	}

	return "VALUES " + strings.Join(placeholders, ", "), args
}

// buildUserOrganizationsOrderClause executes its operation in the persistence flow between service handlers and the database layer.
func buildUserOrganizationsOrderClause(sortColumn proto_models.ListOrganizationsSortBy, sortDesc bool) string {
	direction := "ASC"
	if sortDesc {
		direction = "DESC"
	}

	switch sortColumn {
	case proto_models.ListOrganizationsSortBy_LIST_ORGANIZATIONS_SORT_BY_NAME:
		return fmt.Sprintf("LOWER(org_name) %s, org_name %s, organization_id %s", direction, direction, direction)
	case proto_models.ListOrganizationsSortBy_LIST_ORGANIZATIONS_SORT_BY_SLUG:
		return fmt.Sprintf("LOWER(org_slug) %s, org_slug %s, organization_id %s", direction, direction, direction)
	case proto_models.ListOrganizationsSortBy_LIST_ORGANIZATIONS_SORT_BY_ORGANIZATION_TYPE:
		return fmt.Sprintf("normalized_organization_type %s, organization_id %s", direction, direction)
	case proto_models.ListOrganizationsSortBy_LIST_ORGANIZATIONS_SORT_BY_UPDATED_AT:
		return fmt.Sprintf("org_updated_at %s, organization_id %s", direction, direction)
	case proto_models.ListOrganizationsSortBy_LIST_ORGANIZATIONS_SORT_BY_CREATED_AT,
		proto_models.ListOrganizationsSortBy_LIST_ORGANIZATIONS_SORT_BY_UNSPECIFIED:
		return fmt.Sprintf("org_created_at %s, organization_id %s", direction, direction)
	case proto_models.ListOrganizationsSortBy_LIST_ORGANIZATIONS_SORT_BY_JOINED_AT:
		return fmt.Sprintf("joined_at %s, organization_id %s", direction, direction)
	default:
		return fmt.Sprintf("org_created_at %s, organization_id %s", direction, direction)
	}
}

// userOrganizationListRow defines data used in the persistence flow between service handlers and the database layer.
// It is consumed by handlers and helpers that participate in this backend call path.
type userOrganizationListRow struct {
	UserID                     uint64        `gorm:"column:user_id"`
	OrganizationID             int64         `gorm:"column:organization_id"`
	JoinedAt                   time.Time     `gorm:"column:joined_at"`
	UpdatedAt                  time.Time     `gorm:"column:updated_at"`
	ExplicitRole               int32         `gorm:"column:explicit_role"`
	EffectiveRole              int32         `gorm:"column:effective_role"`
	InheritedFromOrganization  *int64        `gorm:"column:inherited_from_organization_id"`
	EffectiveRoleDepth         *int32        `gorm:"column:effective_role_depth"`
	OrgName                    string        `gorm:"column:org_name"`
	OrgSlug                    string        `gorm:"column:org_slug"`
	OrgDescription             *string       `gorm:"column:org_description"`
	OrgParentOrganizationID    *int64        `gorm:"column:org_parent_organization_id"`
	OrgPersonalOwnerUserID     *uint64       `gorm:"column:org_personal_owner_user_id"`
	OrgLineage                 pq.Int64Array `gorm:"column:org_lineage;type:bigint[]"`
	NormalizedOrganizationType int32         `gorm:"column:normalized_organization_type"`
	OrgCreatedAt               time.Time     `gorm:"column:org_created_at"`
	OrgUpdatedAt               time.Time     `gorm:"column:org_updated_at"`
	// TotalCount carries COUNT(*) OVER () when an exact count is requested, so
	// the lineage CTE executes once instead of once for COUNT and once for data.
	TotalCount int64 `gorm:"column:total_count"`
}

// userOrganizationListRowToResult executes its operation in the persistence flow between service handlers and the database layer.
func userOrganizationListRowToResult(row *userOrganizationListRow) *UserOrganizationWithRoleInfo {
	if row == nil {
		return nil
	}

	return &UserOrganizationWithRoleInfo{
		UserOrganization: &common_persistence_models.UserOrganization{
			UserID:         row.UserID,
			OrganizationID: row.OrganizationID,
			Role:           proto_models.OrganizationRole(row.ExplicitRole),
			JoinedAt:       row.JoinedAt,
			UpdatedAt:      row.UpdatedAt,
			Organization: common_persistence_models.Organization{
				ID:                   row.OrganizationID,
				Name:                 row.OrgName,
				Slug:                 row.OrgSlug,
				Description:          row.OrgDescription,
				ParentOrganizationID: row.OrgParentOrganizationID,
				PersonalOwnerUserID:  row.OrgPersonalOwnerUserID,
				Lineage:              row.OrgLineage,
				OrganizationType:     proto_models.OrganizationType(row.NormalizedOrganizationType),
				CreatedAt:            row.OrgCreatedAt,
				UpdatedAt:            row.OrgUpdatedAt,
			},
		},
		RoleInfo: OrganizationEffectiveRoleResult{
			ExplicitRole:                proto_models.OrganizationRole(row.ExplicitRole),
			EffectiveRole:               proto_models.OrganizationRole(row.EffectiveRole),
			InheritedFromOrganizationID: row.InheritedFromOrganization,
			EffectiveRoleDepth:          row.EffectiveRoleDepth,
		},
	}
}

// CreateOrganization executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreateOrganization(ctx context.Context, organization *common_persistence_models.Organization) error {
	// Normalize slug to lowercase for case-insensitive uniqueness
	organization.Slug = strings.ToLower(strings.TrimSpace(organization.Slug))
	organization.OrganizationType = normalizeOrganizationTypeValue(organization.OrganizationType)

	db := h.getDBFromContext(ctx)
	result := db.Create(organization)
	if result.Error != nil {
		if isUniqueViolation(result.Error) {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("Unique constraint violation during organization creation",
					zap.String(logkeys.Operation, "create_organization"),
					zap.String(appLogger.KeySlug, organization.Slug),
					zap.String(appLogger.KeyName, organization.Name),
					zap.Error(result.Error))
			}
			return ErrOrganizationAlreadyExists
		}
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "create_organization"),
			zap.String(appLogger.KeySlug, organization.Slug),
			zap.String(appLogger.KeyName, organization.Name))
	}
	return nil
}

// CreatePersonalWorkspaceRoot inserts a trusted personal-workspace root without
// poisoning the ambient PostgreSQL transaction when either the lifetime-owner
// or slug uniqueness authority wins a concurrent race. A false created result
// requires the caller to reload by owner and fully validate the winning row.
func (h *CommonPersistenceHandler[U]) CreatePersonalWorkspaceRoot(
	ctx context.Context,
	organization *common_persistence_models.Organization,
) (bool, error) {
	if organization == nil || organization.PersonalOwnerUserID == nil || organization.ParentOrganizationID != nil {
		return false, ErrPersonalWorkspaceConflict
	}

	organization.Slug = strings.ToLower(strings.TrimSpace(organization.Slug))
	organization.OrganizationType = normalizeOrganizationTypeValue(organization.OrganizationType)
	result := h.getDBFromContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(organization)
	if result.Error != nil {
		return false, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "create_personal_workspace_root"),
			zap.Uint64(appLogger.KeyUserID, *organization.PersonalOwnerUserID),
			zap.String(appLogger.KeySlug, organization.Slug))
	}
	if result.RowsAffected == 0 && appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Personal workspace root insert lost a uniqueness race",
			zap.String(logkeys.Operation, "create_personal_workspace_root"),
			zap.Uint64(appLogger.KeyUserID, *organization.PersonalOwnerUserID))
	}
	return result.RowsAffected == 1, nil
}

// LockPersonalWorkspaceOwner establishes the first lock in every lifecycle
// mutation. The stable user row exists before a workspace root and serializes
// provisioning, archival, and restoration even when no root exists yet. The
// lock strength is FOR NO KEY UPDATE: it still mutually excludes concurrent
// lifecycle operations and FOR UPDATE takers, but it does not conflict with
// the FOR KEY SHARE locks that foreign-key checks referencing the user row
// take, so unrelated inserts are never serialized behind a lifecycle
// transaction. The lifecycle never mutates the user's key, so the weaker
// strength loses nothing.
func (h *CommonPersistenceHandler[U]) LockPersonalWorkspaceOwner(
	ctx context.Context,
	ownerUserID uint64,
	options PersonalWorkspaceOwnerLockOptions,
) error {
	db := h.getDBFromContext(ctx)
	if options.IncludeDeleted {
		db = db.Unscoped()
	}
	var user common_persistence_models.User
	result := db.
		Select("id").
		Clauses(clause.Locking{Strength: "NO KEY UPDATE"}).
		Where("id = ?", ownerUserID).
		First(&user)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return ErrUserNotFound
		}
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "lock_personal_workspace_owner"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID))
	}
	return nil
}

// GetPersonalWorkspaceRootByOwner retrieves the unique lifetime root for an
// owner. IncludeDeleted deliberately opts into GORM's unscoped visibility;
// ForUpdate serializes lifecycle repair once a row exists.
func (h *CommonPersistenceHandler[U]) GetPersonalWorkspaceRootByOwner(
	ctx context.Context,
	ownerUserID uint64,
	options PersonalWorkspaceReadOptions,
) (*common_persistence_models.Organization, error) {
	db := h.getDBFromContext(ctx)
	if options.IncludeDeleted {
		db = db.Unscoped()
	}
	if options.ForUpdate {
		db = db.Clauses(clause.Locking{Strength: "UPDATE"})
	}

	var roots []*common_persistence_models.Organization
	result := db.
		Where("personal_owner_user_id = ? AND parent_organization_id IS NULL", ownerUserID).
		Order("id ASC").
		Limit(2).
		Find(&roots)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_personal_workspace_root_by_owner"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID))
	}
	if len(roots) == 0 {
		return nil, ErrOrganizationNotFound
	}
	if len(roots) != 1 {
		return nil, appLogger.InvariantError("personal_workspace_owner_has_single_root", ErrPersonalWorkspaceConflict,
			zap.String(logkeys.Stage, "get_personal_workspace_root_by_owner"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.String(logkeys.Expected, "exactly one personal workspace root"),
			zap.Int(logkeys.Actual, len(roots)))
	}
	roots[0].OrganizationType = normalizeOrganizationTypeValue(roots[0].OrganizationType)
	return roots[0], nil
}

// HasPersonalWorkspaceOrganizationsByOwner audits retained owner markers
// without assuming a valid root exists. It stops after the first row because
// lifecycle callers only distinguish corruption from true absence.
func (h *CommonPersistenceHandler[U]) HasPersonalWorkspaceOrganizationsByOwner(
	ctx context.Context,
	ownerUserID uint64,
) (bool, error) {
	var organization struct{ ID int64 }
	result := h.getDBFromContext(ctx).
		Unscoped().
		Model(&common_persistence_models.Organization{}).
		Select("id").
		Where("personal_owner_user_id = ?", ownerUserID).
		Limit(1).
		Take(&organization)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "audit_personal_workspace_organizations_by_owner"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID))
	}
	return true, nil
}

// GetPersonalWorkspaceTree discovers every corruption signal the lifecycle
// service must audit: rows connected through parent IDs, rows whose lineage
// names the root, and rows carrying the owner's marker. Parent traversal is a
// recursive CTE so a callback cannot hide a null-owner descendant merely by
// omitting or forging lineage. UNION (rather than UNION ALL) makes malformed
// cycles terminate, and the outer ordered SELECT owns the row locks. The three
// discovery arms are collected into personal_candidate_ids first because an OR
// mixing a CTE semi-join with indexable predicates degenerates to a sequential
// scan (a hashed SubPlan cannot join a BitmapOr); the id-set UNION keeps each
// arm on its own index while preserving the exact row set, ordering, and
// locking semantics.
func (h *CommonPersistenceHandler[U]) GetPersonalWorkspaceTree(
	ctx context.Context,
	rootOrganizationID int64,
	ownerUserID uint64,
	options PersonalWorkspaceReadOptions,
) ([]*common_persistence_models.Organization, error) {
	organizationTable := common_persistence_models.TableNameOrganizations
	activeAnchorPredicate := ""
	activeChildPredicate := ""
	activeOuterPredicate := ""
	if !options.IncludeDeleted {
		activeAnchorPredicate = " AND deleted_at IS NULL"
		activeChildPredicate = " WHERE child.deleted_at IS NULL"
		activeOuterPredicate = " AND organization_row.deleted_at IS NULL"
	}
	lockingClause := ""
	if options.ForUpdate {
		lockingClause = " FOR UPDATE"
	}
	query := fmt.Sprintf(`
WITH RECURSIVE personal_tree_ids(id) AS (
    SELECT id
    FROM %s
    WHERE id = ?%s
    UNION
    SELECT child.id
    FROM %s AS child
    JOIN personal_tree_ids AS parent_tree
      ON child.parent_organization_id = parent_tree.id%s
), personal_candidate_ids(id) AS (
    SELECT id FROM personal_tree_ids
    UNION
    SELECT id FROM %s WHERE lineage @> ARRAY[?]::bigint[]
    UNION
    SELECT id FROM %s WHERE personal_owner_user_id = ?
)
SELECT organization_row.*
FROM %s AS organization_row
WHERE organization_row.id IN (SELECT id FROM personal_candidate_ids)%s
ORDER BY CASE WHEN organization_row.id = ? THEN 0 ELSE 1 END,
         organization_row.id ASC%s`,
		organizationTable,
		activeAnchorPredicate,
		organizationTable,
		activeChildPredicate,
		organizationTable,
		organizationTable,
		organizationTable,
		activeOuterPredicate,
		lockingClause,
	)

	var organizations []*common_persistence_models.Organization
	result := h.getDBFromContext(ctx).
		Raw(query, rootOrganizationID, rootOrganizationID, ownerUserID, rootOrganizationID).
		Scan(&organizations)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_personal_workspace_tree"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int64(appLogger.KeyOrganizationID, rootOrganizationID))
	}
	return organizations, nil
}

// SetMissingPersonalWorkspaceTreeOwner repairs only null owner markers on the
// explicit tree rows the caller discovered and locked. Operating on
// caller-supplied IDs rather than re-deriving membership by lineage keeps the
// repair aligned with discovery, so this layer is capable of repairing even a
// descendant found through parent traversal alone (empty or forged lineage).
// The in-tree service callers never request that: they validate lineage first
// and quarantine lineage-inconsistent shapes, so only lineage-consistent rows
// reach this repair in practice. A non-null conflicting owner is never
// overwritten, and every requested row must still carry a null marker under
// the caller's locks.
func (h *CommonPersistenceHandler[U]) SetMissingPersonalWorkspaceTreeOwner(
	ctx context.Context,
	ownerUserID uint64,
	rootOrganizationID int64,
	organizationIDs []int64,
) error {
	if len(organizationIDs) == 0 {
		return nil
	}
	result := h.getDBFromContext(ctx).
		Unscoped().
		Model(&common_persistence_models.Organization{}).
		Where("id = ANY(?) AND personal_owner_user_id IS NULL", pq.Array(organizationIDs)).
		Update("personal_owner_user_id", ownerUserID)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "set_missing_personal_workspace_tree_owner"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int64(appLogger.KeyOrganizationID, rootOrganizationID),
			zap.Int(appLogger.KeyCount, len(organizationIDs)))
	}
	if result.RowsAffected != int64(len(organizationIDs)) {
		return appLogger.InvariantError("personal_workspace_owner_marker_repair_matches_requested_rows", ErrPersonalWorkspaceConflict,
			zap.String(logkeys.Stage, "set_missing_personal_workspace_tree_owner"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int64(appLogger.KeyOrganizationID, rootOrganizationID),
			zap.Int(appLogger.KeyCount, len(organizationIDs)),
			zap.Int64(logkeys.Expected, int64(len(organizationIDs))),
			zap.Int64(logkeys.Actual, result.RowsAffected))
	}
	return nil
}

// EnsurePersonalWorkspaceOwnerMembership inserts or repairs the managed owner
// membership using conflict-safe SQL. An inserted row is returned directly;
// an existing row is locked and updated only when its role is wrong.
func (h *CommonPersistenceHandler[U]) EnsurePersonalWorkspaceOwnerMembership(
	ctx context.Context,
	ownerUserID uint64,
	organizationID int64,
	role proto_models.OrganizationRole,
) (*PersonalWorkspaceMembershipEnsureResult, error) {
	membership := &common_persistence_models.UserOrganization{
		UserID:         ownerUserID,
		OrganizationID: organizationID,
		Role:           role,
	}
	result := h.getDBFromContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "user_id"}, {Name: "organization_id"}},
			DoNothing: true,
		}).
		Create(membership)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "ensure_personal_workspace_owner_membership"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Stringer(appLogger.KeyNewRole, role))
	}
	if result.RowsAffected == 1 {
		return &PersonalWorkspaceMembershipEnsureResult{
			Membership: membership,
			Change:     PersonalWorkspaceMembershipCreated,
		}, nil
	}

	var existing common_persistence_models.UserOrganization
	if err := h.getDBFromContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("user_id = ? AND organization_id = ?", ownerUserID, organizationID).
		First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, appLogger.InvariantError("personal_workspace_conflicting_membership_insert_has_winner", ErrPersonalWorkspaceConflict,
				zap.String(logkeys.Stage, "load_personal_workspace_owner_membership"),
				zap.Uint64(appLogger.KeyUserID, ownerUserID),
				zap.Int64(appLogger.KeyOrganizationID, organizationID),
				zap.String(logkeys.Expected, "membership row after conflicting insert"),
				zap.String(logkeys.Actual, "membership row not found"))
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "load_personal_workspace_owner_membership"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	if existing.Role == role {
		return &PersonalWorkspaceMembershipEnsureResult{
			Membership: &existing,
			Change:     PersonalWorkspaceMembershipUnchanged,
		}, nil
	}

	result = h.getDBFromContext(ctx).
		Model(&common_persistence_models.UserOrganization{}).
		Where("user_id = ? AND organization_id = ?", ownerUserID, organizationID).
		Update("role", role)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "repair_personal_workspace_owner_membership_role"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Stringer(appLogger.KeyNewRole, role))
	}
	if result.RowsAffected != 1 {
		return nil, appLogger.InvariantError("personal_workspace_owner_role_repair_matches_one_row", ErrPersonalWorkspaceConflict,
			zap.String(logkeys.Stage, "repair_personal_workspace_owner_membership_role"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.String(logkeys.Expected, "one updated membership row"),
			zap.Int64(logkeys.Actual, result.RowsAffected))
	}
	existing.Role = role
	return &PersonalWorkspaceMembershipEnsureResult{
		Membership: &existing,
		Change:     PersonalWorkspaceMembershipRepaired,
	}, nil
}

// RepairPersonalWorkspaceOwnerMembershipRoles repairs only owner membership
// rows already proven to exist on descendants. It never creates a missing
// child membership.
func (h *CommonPersistenceHandler[U]) RepairPersonalWorkspaceOwnerMembershipRoles(
	ctx context.Context,
	ownerUserID uint64,
	organizationIDs []int64,
	role proto_models.OrganizationRole,
) error {
	if len(organizationIDs) == 0 {
		return nil
	}
	result := h.getDBFromContext(ctx).
		Model(&common_persistence_models.UserOrganization{}).
		Where("user_id = ? AND organization_id = ANY(?)", ownerUserID, pq.Array(organizationIDs)).
		Update("role", role)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "repair_personal_workspace_owner_membership_roles"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int(appLogger.KeyCount, len(organizationIDs)),
			zap.Stringer(appLogger.KeyNewRole, role))
	}
	if result.RowsAffected != int64(len(organizationIDs)) {
		return appLogger.InvariantError("personal_workspace_owner_role_repairs_match_requested_rows", ErrPersonalWorkspaceConflict,
			zap.String(logkeys.Stage, "repair_personal_workspace_owner_membership_roles"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int(appLogger.KeyCount, len(organizationIDs)),
			zap.Int64(logkeys.Expected, int64(len(organizationIDs))),
			zap.Int64(logkeys.Actual, result.RowsAffected))
	}
	return nil
}

// ListPersonalWorkspaceMemberships retrieves explicit membership rows for a
// locked tree so the lifecycle service can quarantine non-owner members rather
// than silently deleting or rewriting them.
func (h *CommonPersistenceHandler[U]) ListPersonalWorkspaceMemberships(
	ctx context.Context,
	organizationIDs []int64,
) ([]*common_persistence_models.UserOrganization, error) {
	if len(organizationIDs) == 0 {
		return nil, nil
	}
	var memberships []*common_persistence_models.UserOrganization
	if err := h.getDBFromContext(ctx).
		Where("organization_id = ANY(?)", pq.Array(organizationIDs)).
		Order("organization_id ASC, user_id ASC").
		Find(&memberships).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_personal_workspace_memberships"),
			zap.Int(appLogger.KeyCount, len(organizationIDs)))
	}
	return memberships, nil
}

// SoftDeletePersonalWorkspaceOrganizations archives exactly the previously
// locked tree rows in one owner-scoped write, mirroring
// RestorePersonalWorkspaceOrganizations so a stale or forged ID can never
// archive a row the owner does not hold. Memberships and dependent history are
// intentionally left untouched for restoration and audit.
func (h *CommonPersistenceHandler[U]) SoftDeletePersonalWorkspaceOrganizations(
	ctx context.Context,
	organizationIDs []int64,
	ownerUserID uint64,
) error {
	if len(organizationIDs) == 0 {
		return nil
	}
	result := h.getDBFromContext(ctx).
		Unscoped().
		Model(&common_persistence_models.Organization{}).
		Where("id = ANY(?) AND personal_owner_user_id = ?", pq.Array(organizationIDs), ownerUserID).
		Update("deleted_at", time.Now().UTC())
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "archive_personal_workspace_organizations"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int(appLogger.KeyCount, len(organizationIDs)))
	}
	if result.RowsAffected != int64(len(organizationIDs)) {
		return appLogger.InvariantError("personal_workspace_archive_matches_requested_rows", ErrPersonalWorkspaceConflict,
			zap.String(logkeys.Stage, "archive_personal_workspace_organizations"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int(appLogger.KeyCount, len(organizationIDs)),
			zap.Int64(logkeys.Expected, int64(len(organizationIDs))),
			zap.Int64(logkeys.Actual, result.RowsAffected))
	}
	return nil
}

// RestorePersonalWorkspaceOrganizations restores the retained archived rows in
// one owner-scoped write. Every requested row must still be archived and carry
// the expected owner marker.
func (h *CommonPersistenceHandler[U]) RestorePersonalWorkspaceOrganizations(
	ctx context.Context,
	organizationIDs []int64,
	ownerUserID uint64,
) error {
	if len(organizationIDs) == 0 {
		return nil
	}
	result := h.getDBFromContext(ctx).
		Unscoped().
		Model(&common_persistence_models.Organization{}).
		Where("id = ANY(?) AND personal_owner_user_id = ? AND deleted_at IS NOT NULL", pq.Array(organizationIDs), ownerUserID).
		Update("deleted_at", nil)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "restore_personal_workspace_organizations"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int(appLogger.KeyCount, len(organizationIDs)))
	}
	if result.RowsAffected != int64(len(organizationIDs)) {
		return appLogger.InvariantError("personal_workspace_restore_matches_requested_rows", ErrPersonalWorkspaceConflict,
			zap.String(logkeys.Stage, "restore_personal_workspace_organizations"),
			zap.Uint64(appLogger.KeyUserID, ownerUserID),
			zap.Int(appLogger.KeyCount, len(organizationIDs)),
			zap.Int64(logkeys.Expected, int64(len(organizationIDs))),
			zap.Int64(logkeys.Actual, result.RowsAffected))
	}
	return nil
}

// GetOrganizationByID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetOrganizationByID(ctx context.Context, id int64) (*common_persistence_models.Organization, error) {
	var organization common_persistence_models.Organization
	db := h.getDBFromContext(ctx)
	result := db.Where("id = ?", id).First(&organization)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrOrganizationNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_organization_by_id"),
			zap.Int64(appLogger.KeyOrganizationID, id))
	}
	organization.OrganizationType = normalizeOrganizationTypeValue(organization.OrganizationType)
	return &organization, nil
}

// GetOrganizationBySlug executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetOrganizationBySlug(ctx context.Context, slug string) (*common_persistence_models.Organization, error) {
	var organization common_persistence_models.Organization
	db := h.getDBFromContext(ctx)
	// Normalize input slug to lowercase for case-insensitive lookup
	slug = strings.ToLower(strings.TrimSpace(slug))
	result := db.Where("slug = ?", slug).First(&organization)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrOrganizationNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_organization_by_slug"),
			zap.String(appLogger.KeySlug, slug))
	}
	organization.OrganizationType = normalizeOrganizationTypeValue(organization.OrganizationType)
	return &organization, nil
}

// UpdateOrganization executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateOrganization(ctx context.Context, organization *common_persistence_models.Organization) error {
	if organization == nil || organization.ID == 0 {
		return ErrOrganizationNotFound
	}
	// Normalize slug to lowercase for case-insensitive uniqueness
	organization.Slug = strings.ToLower(strings.TrimSpace(organization.Slug))
	organization.UpdatedAt = time.Now().UTC()

	db := h.getDBFromContext(ctx)
	// Ordinary organization updates intentionally write only user-editable
	// columns. Saving the loaded struct would let a stale read overwrite a
	// concurrently backfilled/repaired owner, parent, lineage, or capability.
	result := db.Model(&common_persistence_models.Organization{}).
		Where("id = ?", organization.ID).
		Select("name", "slug", "description", "updated_at").
		Updates(struct {
			Name        string
			Slug        string
			Description *string
			UpdatedAt   time.Time `gorm:"autoUpdateTime:false"`
		}{
			Name:        organization.Name,
			Slug:        organization.Slug,
			Description: organization.Description,
			UpdatedAt:   organization.UpdatedAt,
		})
	if result.Error != nil {
		if isUniqueViolation(result.Error) {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("Unique constraint violation during organization update",
					zap.String(logkeys.Operation, "update_organization"),
					zap.Int64(appLogger.KeyOrganizationID, organization.ID),
					zap.String(appLogger.KeySlug, organization.Slug),
					zap.Error(result.Error))
			}
			return ErrOrganizationAlreadyExists
		}
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_organization"),
			zap.Int64(appLogger.KeyOrganizationID, organization.ID),
			zap.String(appLogger.KeySlug, organization.Slug))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Organization not found for update",
				zap.String(logkeys.Operation, "update_organization"),
				zap.Int64(appLogger.KeyOrganizationID, organization.ID))
		}
		return ErrOrganizationNotFound
	}
	return nil
}

// UpdateOrganizationType executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateOrganizationType(ctx context.Context, organizationID int64, organizationType proto_models.OrganizationType) error {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.Organization{}).
		Where("id = ?", organizationID).
		Select("organization_type", "updated_at").
		Updates(struct {
			OrganizationType proto_models.OrganizationType
			UpdatedAt        time.Time `gorm:"autoUpdateTime:false"`
		}{
			OrganizationType: normalizeOrganizationTypeValue(organizationType),
			UpdatedAt:        time.Now().UTC(),
		})
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_organization_type"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Stringer(appLogger.KeyType, organizationType))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Organization not found for type update",
				zap.String(logkeys.Operation, "update_organization_type"),
				zap.Int64(appLogger.KeyOrganizationID, organizationID))
		}
		return ErrOrganizationNotFound
	}
	return nil
}

// DeleteOrganization executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) DeleteOrganization(ctx context.Context, id int64) error {
	db := h.getDBFromContext(ctx)

	// Start a nested transaction if not already in one
	return db.Transaction(func(tx *gorm.DB) error {
		if err := h.deleteOrganizationMembershipsByLineage(ctx, tx, id); err != nil {
			return err
		}
		if err := h.deleteOrganizationInvitationsByLineage(ctx, tx, id); err != nil {
			return err
		}
		if err := h.deleteOrganizationEmailDomainsByLineage(ctx, tx, id); err != nil {
			return err
		}
		if err := h.deleteOrganizationsByLineage(ctx, tx, id); err != nil {
			return err
		}
		return nil
	})
}

// ListOrganizations executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ListOrganizations(ctx context.Context, offset, limit int, search string, organizationTypeFilter proto_models.OrganizationType, sortColumn string, sortDesc bool) ([]*common_persistence_models.Organization, int64, error) {
	var organizations []*common_persistence_models.Organization
	var totalCount int64

	db := h.getDBFromContext(ctx)
	query := db.Model(&common_persistence_models.Organization{})

	// Apply search filter if provided
	if search != "" {
		searchPattern := fmt.Sprintf("%%%s%%", strings.ToLower(search))
		query = query.Where("name ILIKE ? OR slug ILIKE ?", searchPattern, searchPattern)
	}
	query = applyOrganizationTypeFilter(query, organizationTypeFilter)

	// Get total count for pagination
	if err := query.Count(&totalCount).Error; err != nil {
		return nil, 0, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "count_organizations"),
			zap.String(appLogger.KeySearch, search),
			zap.Stringer(appLogger.KeyType, organizationTypeFilter))
	}

	// Apply pagination and ordering
	query = query.Order(common_util.BuildOrderClause(&proto_models.SortRequest{SortDesc: sortDesc}, sortColumn, true)).Offset(offset)
	if limit > 0 {
		query = query.Limit(limit)
	}

	if err := query.Find(&organizations).Error; err != nil {
		return nil, 0, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_organizations"),
			zap.String(appLogger.KeySearch, search),
			zap.Stringer(appLogger.KeyType, organizationTypeFilter),
			zap.Int(appLogger.KeyOffset, offset),
			zap.Int(logkeys.Limit, limit))
	}
	for _, organization := range organizations {
		if organization != nil {
			organization.OrganizationType = normalizeOrganizationTypeValue(organization.OrganizationType)
		}
	}

	return organizations, totalCount, nil
}

// ListAdminOrganizations executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ListAdminOrganizations(
	ctx context.Context,
	query *AdminOrganizationsQuery,
) ([]*common_persistence_models.Organization, int64, error) {
	var organizations []*common_persistence_models.Organization
	var totalCount int64

	db := h.getDBFromContext(ctx)
	if query == nil {
		query = &AdminOrganizationsQuery{}
	}

	baseQuery := buildAdminOrganizationsBaseQuery(
		db,
		query.Search,
		query.OrganizationTypeFilter,
		query.ParentOrganizationIDFilter,
		query.DepthFilter,
	)

	if !query.SkipExactCount {
		if err := baseQuery.Count(&totalCount).Error; err != nil {
			return nil, 0, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "count_admin_organizations"),
				zap.String(appLogger.KeySearch, query.Search),
				zap.Stringer(appLogger.KeyType, query.OrganizationTypeFilter))
		}
	} else {
		totalCount = common_util.UnknownTotalCount
	}

	dataQuery := baseQuery.Order(common_util.BuildOrderClause(&proto_models.SortRequest{SortDesc: query.SortDesc}, query.SortColumn, true)).Offset(query.Offset)
	if query.Limit > 0 {
		dataLimit := query.Limit
		if query.SkipExactCount {
			dataLimit++
		}
		dataQuery = dataQuery.Limit(dataLimit)
	}

	if err := dataQuery.Find(&organizations).Error; err != nil {
		return nil, 0, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_admin_organizations"),
			zap.String(appLogger.KeySearch, query.Search),
			zap.Stringer(appLogger.KeyType, query.OrganizationTypeFilter),
			zap.Int(appLogger.KeyOffset, query.Offset),
			zap.Int(logkeys.Limit, query.Limit))
	}

	for _, organization := range organizations {
		if organization != nil {
			organization.OrganizationType = normalizeOrganizationTypeValue(organization.OrganizationType)
		}
	}

	return organizations, totalCount, nil
}

// ListAdminOrganizationParentOptions returns a bounded page of non-deleted
// organizations that are direct parents of records matching the current admin
// organization table filters. The caller owns the page bound: Limit is fetched
// as given and must be positive.
func (h *CommonPersistenceHandler[U]) ListAdminOrganizationParentOptions(
	ctx context.Context,
	query *AdminOrganizationParentOptionsQuery,
) ([]*AdminOrganizationFilterParentOption, error) {
	if query == nil {
		return nil, appLogger.InvariantError("admin_organization_parent_options_query_present", ErrDatabaseError,
			zap.String(logkeys.Operation, "list_admin_organization_parent_options"))
	}
	if query.Limit <= 0 {
		return nil, appLogger.InvariantError("admin_organization_parent_options_limit_positive", ErrDatabaseError,
			zap.String(logkeys.Operation, "list_admin_organization_parent_options"),
			zap.Int(logkeys.Limit, query.Limit))
	}
	db := h.getDBFromContext(ctx)

	matchingOrganizations := buildAdminOrganizationsBaseQuery(
		db,
		query.TableSearch,
		query.OrganizationTypeFilter,
		nil,
		nil,
	).Select("parent_organization_id").
		Where("parent_organization_id IS NOT NULL").
		Distinct()

	parentQuery := db.Table("(?) AS matching_organizations", matchingOrganizations).
		Joins("JOIN " + common_persistence_models.TableNameOrganizations + " parent_orgs ON parent_orgs.id = matching_organizations.parent_organization_id").
		Where("parent_orgs.deleted_at IS NULL")

	parentSearch := strings.TrimSpace(query.ParentSearch)
	if parentSearch != "" {
		searchPattern := "%" + parentSearch + "%"
		parentQuery = parentQuery.Where(
			"(parent_orgs.name ILIKE ? OR parent_orgs.slug ILIKE ?)",
			searchPattern,
			searchPattern,
		)
	}

	offset := max(query.Offset, 0)
	limit := query.Limit

	var parentOptions []*AdminOrganizationFilterParentOption
	parentQuery = parentQuery.
		Select("parent_orgs.id AS id, parent_orgs.name AS name, parent_orgs.slug AS slug").
		Order("parent_orgs.name ASC, parent_orgs.id ASC").
		Offset(offset).
		Limit(limit)
	if err := parentQuery.Scan(&parentOptions).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_admin_organization_parent_options"),
			zap.String(appLogger.KeySearch, parentSearch),
			zap.Int(appLogger.KeyOffset, offset),
			zap.Int(logkeys.Limit, limit))
	}

	return parentOptions, nil
}

// GetAdminOrganizationFilterMetadata executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetAdminOrganizationFilterMetadata(
	ctx context.Context,
	search string,
	organizationTypeFilter proto_models.OrganizationType,
) (*AdminOrganizationFilterMetadata, error) {
	db := h.getDBFromContext(ctx)

	type maxDepthRow struct {
		MaxDepth uint32
	}
	var maxDepthResult maxDepthRow
	maxDepthBaseQuery := buildAdminOrganizationsBaseQuery(db, search, organizationTypeFilter, nil, nil)
	maxDepthQuery := db.Table("(?) AS filtered_orgs", maxDepthBaseQuery.Select("lineage")).
		Select("COALESCE(MAX(cardinality(COALESCE(filtered_orgs.lineage, ARRAY[]::bigint[]))), 0) AS max_depth")
	if err := maxDepthQuery.Scan(&maxDepthResult).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_admin_organization_filter_metadata"),
			zap.String(appLogger.KeySearch, search),
			zap.Stringer(appLogger.KeyType, organizationTypeFilter))
	}

	return &AdminOrganizationFilterMetadata{
		MaxDepth: maxDepthResult.MaxDepth,
	}, nil
}

// CreateOrganizationEmailDomain executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreateOrganizationEmailDomain(ctx context.Context, domain *common_persistence_models.OrganizationEmailDomain) error {
	domain.Domain = strings.ToLower(strings.TrimSpace(domain.Domain))

	db := h.getDBFromContext(ctx)
	if err := db.Create(domain).Error; err != nil {
		if isUniqueViolation(err) {
			return ErrOrganizationEmailDomainExists
		}
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "create_organization_email_domain"),
			zap.Uint64(appLogger.KeyOrganizationID, domain.OrganizationID),
			zap.String(appLogger.KeyDomain, domain.Domain))
	}
	return nil
}

// DeleteOrganizationEmailDomain executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) DeleteOrganizationEmailDomain(ctx context.Context, id uint64) error {
	db := h.getDBFromContext(ctx)
	result := db.Delete(&common_persistence_models.OrganizationEmailDomain{}, id)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "delete_organization_email_domain"),
			zap.Uint64(appLogger.KeyEmailDomainID, id))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Organization email domain not found for delete",
				zap.String(logkeys.Operation, "delete_organization_email_domain"),
				zap.Uint64(appLogger.KeyEmailDomainID, id))
		}
		return ErrOrganizationEmailDomainNotFound
	}
	return nil
}

// GetOrganizationEmailDomainByDomain executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetOrganizationEmailDomainByDomain(ctx context.Context, domain string) (*common_persistence_models.OrganizationEmailDomain, error) {
	var record common_persistence_models.OrganizationEmailDomain
	domain = strings.ToLower(strings.TrimSpace(domain))

	db := h.getDBFromContext(ctx)
	domainTable := common_persistence_models.TableNameOrganizationEmailDomains
	result := db.
		Scopes(ActiveOrganizationScope(domainTable, "organization_id")).
		Where(domainTable+".domain = ?", domain).
		First(&record)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrOrganizationEmailDomainNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_organization_email_domain_by_domain"),
			zap.String(appLogger.KeyDomain, domain))
	}
	return &record, nil
}

// GetOrganizationEmailDomainByID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetOrganizationEmailDomainByID(ctx context.Context, id uint64) (*common_persistence_models.OrganizationEmailDomain, error) {
	var record common_persistence_models.OrganizationEmailDomain
	db := h.getDBFromContext(ctx)
	domainTable := common_persistence_models.TableNameOrganizationEmailDomains
	result := db.
		Scopes(ActiveOrganizationScope(domainTable, "organization_id")).
		Where(domainTable+".id = ?", id).
		First(&record)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrOrganizationEmailDomainNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_organization_email_domain_by_id"),
			zap.Uint64(appLogger.KeyEmailDomainID, id))
	}
	return &record, nil
}

// ListOrganizationEmailDomains executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ListOrganizationEmailDomains(ctx context.Context, organizationID int64) ([]*common_persistence_models.OrganizationEmailDomain, error) {
	var domains []*common_persistence_models.OrganizationEmailDomain
	db := h.getDBFromContext(ctx)
	domainTable := common_persistence_models.TableNameOrganizationEmailDomains
	if err := db.
		Scopes(ActiveOrganizationScope(domainTable, "organization_id")).
		Where(domainTable+".organization_id = ?", organizationID).
		Order(domainTable + ".domain ASC").
		Find(&domains).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_organization_email_domains"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	return domains, nil
}

// ListOrganizationEmailDomainsPage returns a bounded, stably ordered page while
// leaving ListOrganizationEmailDomains available to trusted internal callers
// that require the complete set.
func (h *CommonPersistenceHandler[U]) ListOrganizationEmailDomainsPage(
	ctx context.Context,
	organizationID int64,
	pagination *proto_models.PaginationRequest,
) ([]*common_persistence_models.OrganizationEmailDomain, *proto_models.PaginationResponse, error) {
	if err := requirePageLimit("list_organization_email_domains_page", pagination); err != nil {
		return nil, nil, err
	}
	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	db := h.getDBFromContext(ctx)
	domainTable := common_persistence_models.TableNameOrganizationEmailDomains
	baseQuery := db.Model(&common_persistence_models.OrganizationEmailDomain{}).
		Scopes(ActiveOrganizationScope(domainTable, "organization_id")).
		Where(domainTable+".organization_id = ?", organizationID)

	var total int64
	if includeTotalCount {
		if err := baseQuery.Count(&total).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "count_organization_email_domains"),
				zap.Int64(appLogger.KeyOrganizationID, organizationID))
		}
	}

	var domains []*common_persistence_models.OrganizationEmailDomain
	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	dataQuery := db.Model(&common_persistence_models.OrganizationEmailDomain{}).
		Scopes(ActiveOrganizationScope(domainTable, "organization_id")).
		Where(domainTable+".organization_id = ?", organizationID).
		Order(domainTable + ".domain ASC, " + domainTable + ".id ASC").
		Offset(offset).
		Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount))
	if err := dataQuery.Find(&domains).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_organization_email_domain_page"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	domains, hasNextPage := common_util.TrimPaginationProbe(domains, pagination.Limit, includeTotalCount)

	if includeTotalCount {
		return domains, common_util.CreatePaginationResponse(total, pagination.Limit, pagination.Page), nil
	}
	return domains, common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage), nil
}

// AddUserToOrganization executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AddUserToOrganization(ctx context.Context, userID uint64, organizationID int64, role proto_models.OrganizationRole) error {
	db := h.getDBFromContext(ctx)

	userOrg := &common_persistence_models.UserOrganization{
		UserID:         userID,
		OrganizationID: organizationID,
		Role:           role,
	}

	result := db.Create(userOrg)
	if result.Error != nil {
		// A unique violation means the user is already in the organization.
		if isUniqueViolation(result.Error) {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("User already in organization",
					zap.String(logkeys.Operation, "add_user_to_organization"),
					zap.Uint64(appLogger.KeyUserID, userID),
					zap.Int64(appLogger.KeyOrganizationID, organizationID),
					zap.Error(result.Error))
			}
			return ErrUserAlreadyInOrganization
		}
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "add_user_to_organization"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Stringer(appLogger.KeyNewRole, role))
	}
	return nil
}

// RemoveUserFromOrganizationAndDescendants executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) RemoveUserFromOrganizationAndDescendants(ctx context.Context, userID uint64, organizationID int64) error {
	db := h.getDBFromContext(ctx)

	return db.Transaction(func(tx *gorm.DB) error {
		subquery := tx.Model(&common_persistence_models.Organization{}).
			Select("id").
			Where("id = ? OR lineage @> ARRAY[?]::bigint[]", organizationID, organizationID)
		result := tx.Where("user_id = ? AND organization_id IN (?)", userID, subquery).
			Delete(&common_persistence_models.UserOrganization{})
		if result.Error != nil {
			return databaseErrorWithFields(result.Error,
				zap.String(logkeys.Operation, "remove_user_from_organization_and_descendants"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Int64(appLogger.KeyOrganizationID, organizationID))
		}
		if result.RowsAffected == 0 {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("No organization memberships found for hierarchy removal",
					zap.String(logkeys.Operation, "remove_user_from_organization_and_descendants"),
					zap.Uint64(appLogger.KeyUserID, userID),
					zap.Int64(appLogger.KeyOrganizationID, organizationID))
			}
			return ErrUserOrganizationNotFound
		}
		return nil
	})
}

// UpdateUserOrganizationRole executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateUserOrganizationRole(ctx context.Context, userID uint64, organizationID int64, role proto_models.OrganizationRole) error {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.UserOrganization{}).
		Where("user_id = ? AND organization_id = ?", userID, organizationID).
		Update("role", role)

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_user_organization_role"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Stringer(appLogger.KeyNewRole, role))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Organization membership not found for role update",
				zap.String(logkeys.Operation, "update_user_organization_role"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Int64(appLogger.KeyOrganizationID, organizationID))
		}
		return ErrUserOrganizationNotFound
	}
	return nil
}

// lockOrganizationOwnerGuard takes a transaction-scoped Postgres advisory lock keyed by the
// organization ID so the last-owner check and the membership mutation that follows it are
// serialized across concurrent requests (and replicas). The lock is released automatically when
// the surrounding transaction commits or rolls back.
func lockOrganizationOwnerGuard(tx *gorm.DB, organizationID int64) error {
	return tx.Exec("SELECT pg_advisory_xact_lock(hashtext(?))", fmt.Sprintf("org-owner-guard:%d", organizationID)).Error
}

// lockOrganizationOwnerGuards takes the same per-organization locks used by the single-org role
// guard in deterministic order. Hierarchy removals can overlap in arbitrary ancestor/descendant
// combinations, so consistent ordering is required to prevent otherwise-valid concurrent
// operations from deadlocking while they acquire their shared descendant locks.
func lockOrganizationOwnerGuards(tx *gorm.DB, organizationIDs []int64) (int64, error) {
	orderedOrganizationIDs := append([]int64(nil), organizationIDs...)
	slices.Sort(orderedOrganizationIDs)

	for _, organizationID := range orderedOrganizationIDs {
		if err := lockOrganizationOwnerGuard(tx, organizationID); err != nil {
			return organizationID, err
		}
	}
	return 0, nil
}

// userOrganizationHierarchyMembershipIDsTx returns every explicit membership row that the
// hierarchy DELETE can remove. The caller locks all returned organization IDs before evaluating
// ownership, including memberships that are currently below ownerRole: a concurrent guarded role
// update could otherwise promote one of those rows between discovery and deletion.
func userOrganizationHierarchyMembershipIDsTx(tx *gorm.DB, userID uint64, organizationID int64) ([]int64, error) {
	organizationSubquery := tx.Model(&common_persistence_models.Organization{}).
		Select("id").
		Where("id = ? OR lineage @> ARRAY[?]::bigint[]", organizationID, organizationID)

	var organizationIDs []int64
	err := tx.Model(&common_persistence_models.UserOrganization{}).
		Where("user_id = ? AND organization_id IN (?)", userID, organizationSubquery).
		Pluck("organization_id", &organizationIDs).Error
	return organizationIDs, err
}

// isSoleExplicitOwnerTx reports whether the user is the organization's only explicit owner: their
// EXPLICIT role in the organization is at or above ownerRole and no other explicit member holds an
// owner role. It considers explicit UserOrganization rows only, so a member who merely inherits
// ownership from a parent organization is not treated as an owner. The caller must hold the
// organization owner guard lock so the count cannot change before the guarded mutation commits.
func isSoleExplicitOwnerTx(tx *gorm.DB, userID uint64, organizationID int64, ownerRole proto_models.OrganizationRole) (bool, error) {
	var explicit common_persistence_models.UserOrganization
	err := tx.Where("user_id = ? AND organization_id = ?", userID, organizationID).First(&explicit).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	if explicit.Role < ownerRole {
		return false, nil
	}

	var ownerCount int64
	if err := tx.Model(&common_persistence_models.UserOrganization{}).
		Where("organization_id = ? AND role >= ?", organizationID, ownerRole).
		Count(&ownerCount).Error; err != nil {
		return false, err
	}
	return ownerCount <= 1, nil
}

// isSoleExplicitOwnerInOrganizationsTx batches the post-lock ownership reads. The caller
// must already hold every affected organization's owner guard until the mutation commits.
func isSoleExplicitOwnerInOrganizationsTx(tx *gorm.DB, userID uint64, organizationIDs []int64, ownerRole proto_models.OrganizationRole) (bool, error) {
	var lastOwner bool
	// COALESCE matches the zero value produced when GORM reads a legacy NULL role.
	// The count deliberately uses the raw role, just like the single-org guard.
	err := tx.Raw(`SELECT EXISTS (
    SELECT 1 FROM gocommon_user_organizations AS target
    WHERE target.user_id = ? AND target.organization_id = ANY(?::bigint[])
      AND COALESCE(target.role, 0) >= ?
      AND (SELECT count(*) FROM gocommon_user_organizations AS owners
           WHERE owners.organization_id = target.organization_id AND owners.role >= ?) <= 1
)`, userID, pq.Array(organizationIDs), ownerRole, ownerRole).Scan(&lastOwner).Error
	return lastOwner, err
}

// RemoveUserFromOrganizationAndDescendantsGuardingLastOwner removes a user's membership from an
// organization and all descendant organizations, refusing the removal when any deleted membership
// is that organization's only explicit owner. Ownership is evaluated from the user's EXPLICIT role
// in each affected organization (>= ownerRole) and the count of explicit owner rows, so a member
// who merely inherits ownership from a parent organization is never blocked. The owner checks and
// delete run inside one transaction guarded by per-organization advisory locks for every affected
// membership, so hierarchy removals and single-organization role updates cannot race to leave an
// organization without an explicit owner.
//
// Returns ErrOrganizationLastOwner when the removal is blocked and ErrUserOrganizationNotFound when
// the user has no membership in the organization or its descendants.
func (h *CommonPersistenceHandler[U]) RemoveUserFromOrganizationAndDescendantsGuardingLastOwner(ctx context.Context, userID uint64, organizationID int64, ownerRole proto_models.OrganizationRole) error {
	db := h.getDBFromContext(ctx)

	return db.Transaction(func(tx *gorm.DB) error {
		affectedOrganizationIDs, err := userOrganizationHierarchyMembershipIDsTx(tx, userID, organizationID)
		if err != nil {
			return databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "identify_organization_memberships_for_owner_guard"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Int64(appLogger.KeyOrganizationID, organizationID))
		}
		if len(affectedOrganizationIDs) == 0 {
			return ErrUserOrganizationNotFound
		}

		failedOrganizationID, err := lockOrganizationOwnerGuards(tx, affectedOrganizationIDs)
		if err != nil {
			return databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "lock_organization_owner_guards"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Int64(appLogger.KeyOrganizationID, failedOrganizationID))
		}

		// Keep this statement separate from lock acquisition: after a lock wait it must
		// see committed membership changes before deciding whether deletion is safe.
		lastOwner, err := isSoleExplicitOwnerInOrganizationsTx(tx, userID, affectedOrganizationIDs, ownerRole)
		if err != nil {
			return databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "evaluate_last_owner_guard_for_removal"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Int64s("affected_organization_ids", affectedOrganizationIDs))
		}
		if lastOwner {
			return ErrOrganizationLastOwner
		}

		subquery := tx.Model(&common_persistence_models.Organization{}).
			Select("id").
			Where("id = ? OR lineage @> ARRAY[?]::bigint[]", organizationID, organizationID)
		result := tx.Where("user_id = ? AND organization_id IN (?)", userID, subquery).
			Delete(&common_persistence_models.UserOrganization{})
		if result.Error != nil {
			return databaseErrorWithFields(result.Error,
				zap.String(logkeys.Operation, "remove_user_from_organization_and_descendants_guarding_last_owner"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Int64(appLogger.KeyOrganizationID, organizationID))
		}
		if result.RowsAffected == 0 {
			return ErrUserOrganizationNotFound
		}
		return nil
	})
}

// UpdateUserOrganizationRoleGuardingLastOwner updates a user's explicit role within an
// organization, refusing a demotion below ownerRole when the user is the organization's only
// explicit owner. Ownership is evaluated from explicit UserOrganization rows only, matching
// RemoveUserFromOrganizationAndDescendantsGuardingLastOwner, so inherited ownership never blocks
// the change. The owner check and the update run inside one transaction guarded by a
// per-organization advisory lock so concurrent demotions cannot both leave the organization
// ownerless. Updates that keep the role at or above ownerRole skip the guard.
//
// Returns ErrOrganizationLastOwner when the demotion is blocked and ErrUserOrganizationNotFound
// when the membership does not exist.
func (h *CommonPersistenceHandler[U]) UpdateUserOrganizationRoleGuardingLastOwner(ctx context.Context, userID uint64, organizationID int64, newRole, ownerRole proto_models.OrganizationRole) error {
	db := h.getDBFromContext(ctx)

	return db.Transaction(func(tx *gorm.DB) error {
		if err := lockOrganizationOwnerGuard(tx, organizationID); err != nil {
			return databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "lock_organization_owner_guard"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Int64(appLogger.KeyOrganizationID, organizationID))
		}

		if newRole < ownerRole {
			lastOwner, err := isSoleExplicitOwnerTx(tx, userID, organizationID, ownerRole)
			if err != nil {
				return databaseErrorWithFields(err,
					zap.String(logkeys.Operation, "evaluate_last_owner_guard_for_demotion"),
					zap.Uint64(appLogger.KeyUserID, userID),
					zap.Int64(appLogger.KeyOrganizationID, organizationID),
					zap.Stringer(appLogger.KeyNewRole, newRole))
			}
			if lastOwner {
				return ErrOrganizationLastOwner
			}
		}

		result := tx.Model(&common_persistence_models.UserOrganization{}).
			Where("user_id = ? AND organization_id = ?", userID, organizationID).
			Update("role", newRole)
		if result.Error != nil {
			return databaseErrorWithFields(result.Error,
				zap.String(logkeys.Operation, "update_user_organization_role_guarding_last_owner"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Int64(appLogger.KeyOrganizationID, organizationID),
				zap.Stringer(appLogger.KeyNewRole, newRole))
		}
		if result.RowsAffected == 0 {
			return ErrUserOrganizationNotFound
		}
		return nil
	})
}

// GetUserExplicitOrganizationRole executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetUserExplicitOrganizationRole(ctx context.Context, userID uint64, organizationID int64) (proto_models.OrganizationRole, error) {
	var userOrg common_persistence_models.UserOrganization
	db := h.getDBFromContext(ctx)
	membershipTable := common_persistence_models.TableNameUserOrganizations

	result := db.
		Scopes(ActiveOrganizationScope(membershipTable, "organization_id")).
		Where(membershipTable+".user_id = ? AND "+membershipTable+".organization_id = ?", userID, organizationID).
		First(&userOrg)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, ErrUserOrganizationNotFound
		}
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_user_explicit_organization_role"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	return userOrg.Role, nil
}

// GetUserEffectiveOrganizationRoleByLineage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetUserEffectiveOrganizationRoleByLineage(
	ctx context.Context,
	userID uint64,
	organizationID int64,
) (OrganizationEffectiveRoleResult, error) {
	// roleResolutionRow carries the raw SQL projection used while
	// GetUserEffectiveOrganizationRoleByLineage resolves explicit and inherited roles for one user.
	type roleResolutionRow struct {
		ExplicitRole         int32  `gorm:"column:explicit_role"`
		EffectiveRole        int32  `gorm:"column:effective_role"`
		SourceOrganizationID *int64 `gorm:"column:source_organization_id"`
		EffectiveRoleDepth   *int32 `gorm:"column:effective_role_depth"`
	}

	const query = `
WITH target_org AS (
    SELECT
        id,
        COALESCE(lineage, ARRAY[]::bigint[]) AS lineage
    FROM gocommon_organizations
    WHERE id = ? AND deleted_at IS NULL
),
ranked AS (
    SELECT
        uo.organization_id,
        uo.role,
        CASE WHEN uo.organization_id = t.id THEN 0 ELSE 1 END AS source_rank,
        array_position(t.lineage, uo.organization_id) AS lineage_pos,
        CASE
            WHEN uo.organization_id = t.id THEN cardinality(t.lineage)
            ELSE array_position(t.lineage, uo.organization_id) - 1
        END AS source_depth
    FROM target_org t
    JOIN gocommon_user_organizations uo
      ON uo.user_id = ?
     AND uo.organization_id = ANY(t.lineage || ARRAY[t.id]::bigint[])
	JOIN gocommon_organizations source_org
	  ON source_org.id = uo.organization_id
	 AND source_org.deleted_at IS NULL
),
best AS (
    SELECT organization_id, role, source_depth
    FROM ranked
    ORDER BY role DESC, source_rank ASC, lineage_pos DESC NULLS LAST
    LIMIT 1
),
explicit AS (
    SELECT role
    FROM ranked
    WHERE source_rank = 0
    LIMIT 1
)
SELECT
    COALESCE((SELECT role FROM explicit), 0) AS explicit_role,
    COALESCE((SELECT role FROM best), 0) AS effective_role,
    (SELECT organization_id FROM best) AS source_organization_id,
    (SELECT source_depth FROM best) AS effective_role_depth
FROM target_org
LIMIT 1;
`

	db := h.getDBFromContext(ctx)
	var row roleResolutionRow
	result := db.Raw(query, organizationID, userID).Scan(&row)
	if result.Error != nil {
		return OrganizationEffectiveRoleResult{}, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_user_effective_organization_role_by_lineage"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	if result.RowsAffected == 0 {
		return OrganizationEffectiveRoleResult{}, ErrOrganizationNotFound
	}

	roleResult := OrganizationEffectiveRoleResult{
		ExplicitRole:  proto_models.OrganizationRole(row.ExplicitRole),
		EffectiveRole: proto_models.OrganizationRole(row.EffectiveRole),
	}
	if row.SourceOrganizationID != nil && *row.SourceOrganizationID != organizationID {
		roleResult.InheritedFromOrganizationID = row.SourceOrganizationID
	}
	if row.EffectiveRoleDepth != nil {
		roleResult.EffectiveRoleDepth = row.EffectiveRoleDepth
	}

	return roleResult, nil
}

// GetUsersEffectiveOrganizationRolesByLineage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetUsersEffectiveOrganizationRolesByLineage(
	ctx context.Context,
	userIDs []uint64,
	organizationID int64,
) (map[uint64]OrganizationEffectiveRoleResult, error) {
	if len(userIDs) == 0 {
		return map[uint64]OrganizationEffectiveRoleResult{}, nil
	}

	db := h.getDBFromContext(ctx)

	var orgExists bool
	if err := db.Model(&common_persistence_models.Organization{}).
		Select("count(1) > 0").
		Where("id = ? AND deleted_at IS NULL", organizationID).
		Scan(&orgExists).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "verify_organization_for_bulk_role_resolution"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Int(appLogger.KeyCount, len(userIDs)))
	}
	if !orgExists {
		return nil, ErrOrganizationNotFound
	}

	userIDValues := common_util.ApplyToEach(userIDs, func(u uint64) int64 { return int64(u) })

	// roleResolutionRow carries the raw SQL projection used while
	// GetUsersEffectiveOrganizationRolesByLineage resolves explicit and inherited roles for many users.
	type roleResolutionRow struct {
		UserID               uint64 `gorm:"column:user_id"`
		ExplicitRole         int32  `gorm:"column:explicit_role"`
		EffectiveRole        int32  `gorm:"column:effective_role"`
		SourceOrganizationID *int64 `gorm:"column:source_organization_id"`
		EffectiveRoleDepth   *int32 `gorm:"column:effective_role_depth"`
	}

	const query = `
WITH target_org AS (
    SELECT
        id,
        COALESCE(lineage, ARRAY[]::bigint[]) AS lineage
    FROM gocommon_organizations
    WHERE id = ? AND deleted_at IS NULL
),
input_users AS (
    SELECT UNNEST(?::bigint[]) AS user_id
),
ranked AS (
    SELECT
        iu.user_id,
        uo.organization_id,
        uo.role,
        CASE WHEN uo.organization_id = t.id THEN 0 ELSE 1 END AS source_rank,
        array_position(t.lineage, uo.organization_id) AS lineage_pos,
        CASE
            WHEN uo.organization_id = t.id THEN cardinality(t.lineage)
            ELSE array_position(t.lineage, uo.organization_id) - 1
        END AS source_depth
    FROM target_org t
    JOIN input_users iu ON TRUE
    JOIN gocommon_user_organizations uo
      ON uo.user_id = iu.user_id
     AND uo.organization_id = ANY(t.lineage || ARRAY[t.id]::bigint[])
	JOIN gocommon_organizations source_org
	  ON source_org.id = uo.organization_id
	 AND source_org.deleted_at IS NULL
),
best AS (
    SELECT user_id, organization_id, role, source_depth
    FROM (
        SELECT
            user_id,
            organization_id,
            role,
            source_depth,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY role DESC, source_rank ASC, lineage_pos DESC NULLS LAST
            ) AS rn
        FROM ranked
    ) ranked_best
    WHERE rn = 1
),
explicit AS (
    SELECT user_id, role
    FROM ranked
    WHERE source_rank = 0
)
SELECT
    iu.user_id,
    COALESCE(explicit.role, 0) AS explicit_role,
    COALESCE(best.role, 0) AS effective_role,
    CASE
        WHEN best.organization_id IS NOT NULL AND best.organization_id <> ? THEN best.organization_id
        ELSE NULL
    END AS source_organization_id,
    best.source_depth AS effective_role_depth
FROM input_users iu
LEFT JOIN explicit ON explicit.user_id = iu.user_id
LEFT JOIN best ON best.user_id = iu.user_id;
`

	var rows []roleResolutionRow
	result := db.Raw(query, organizationID, pq.Array(userIDValues), organizationID).Scan(&rows)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_users_effective_organization_roles_by_lineage"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Int(appLogger.KeyCount, len(userIDs)))
	}

	roleResults := make(map[uint64]OrganizationEffectiveRoleResult, len(rows))
	for _, row := range rows {
		current := OrganizationEffectiveRoleResult{
			ExplicitRole:  proto_models.OrganizationRole(row.ExplicitRole),
			EffectiveRole: proto_models.OrganizationRole(row.EffectiveRole),
		}
		if row.SourceOrganizationID != nil {
			current.InheritedFromOrganizationID = row.SourceOrganizationID
		}
		if row.EffectiveRoleDepth != nil {
			current.EffectiveRoleDepth = row.EffectiveRoleDepth
		}
		roleResults[row.UserID] = current
	}

	return roleResults, nil
}

// GetUserOrganization executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetUserOrganization(ctx context.Context, userID uint64, organizationID int64) (*common_persistence_models.UserOrganization, error) {
	var userOrg common_persistence_models.UserOrganization
	db := h.getDBFromContext(ctx)
	membershipTable := common_persistence_models.TableNameUserOrganizations

	result := db.
		Scopes(ActiveOrganizationScope(membershipTable, "organization_id")).
		Preload(common_persistence_models.PreloadUser, preloadUnscoped).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment, preloadLiveAttachment).
		Where(membershipTable+".user_id = ? AND "+membershipTable+".organization_id = ?", userID, organizationID).
		First(&userOrg)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUserOrganizationNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_user_organization"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}

	return &userOrg, nil
}

// GetUserOrganizations executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetUserOrganizations(ctx context.Context, userID uint64, query *UserOrganizationsQuery) ([]*UserOrganizationWithRoleInfo, int64, error) {
	db := h.getDBFromContext(ctx)

	if query == nil {
		query = &UserOrganizationsQuery{
			SortColumn: proto_models.ListOrganizationsSortBy_LIST_ORGANIZATIONS_SORT_BY_JOINED_AT,
			SortDesc:   true,
		}
	}

	viewThresholdsCTE, viewThresholdArgs := buildUserOrganizationsViewThresholdsCTE(query.RequiredViewRolesByDepth)
	requestFiltersCTE, requestFilterArgs := buildUserOrganizationsRequestFiltersCTE(query.OrganizationMembershipMaskFilter)
	orderClause := buildUserOrganizationsOrderClause(query.SortColumn, query.SortDesc)

	baseArgs := []any{
		int32(proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED),
		int32(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0),
		userID,
	}
	baseArgs = append(baseArgs, viewThresholdArgs...)
	baseArgs = append(baseArgs, requestFilterArgs...)
	baseArgs = append(baseArgs,
		query.ApplyVisibilityFilters,
		int32(query.MinOrganizationType),
		len(query.OrganizationMembershipMaskFilter) > 0,
	)
	searchEnabled := strings.TrimSpace(query.Search) != ""
	searchPattern := "%" + strings.ToLower(strings.TrimSpace(query.Search)) + "%"
	var parentOrganizationIDFilter any
	if query.ParentOrganizationIDFilter != nil {
		parentOrganizationIDFilter = *query.ParentOrganizationIDFilter
	}
	var depthFilter any
	if query.DepthFilter != nil {
		depthFilter = int32(*query.DepthFilter)
	}
	baseArgs = append(baseArgs,
		searchEnabled,
		searchPattern,
		searchPattern,
		searchPattern,
		query.OrganizationTypeFilter == proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED,
		int32(normalizeOrganizationTypeValue(query.OrganizationTypeFilter)),
		query.ParentOrganizationIDFilter == nil,
		parentOrganizationIDFilter,
		query.DepthFilter == nil,
		depthFilter,
	)

	baseQuery := fmt.Sprintf(`
WITH target_orgs AS (
    SELECT
        uo.user_id,
        uo.organization_id,
        uo.role AS explicit_role,
        uo.joined_at,
        uo.updated_at,
        o.name AS org_name,
        o.slug AS org_slug,
        o.description AS org_description,
        o.parent_organization_id AS org_parent_organization_id,
		o.personal_owner_user_id AS org_personal_owner_user_id,
        COALESCE(o.lineage, ARRAY[]::bigint[]) AS org_lineage,
        CASE
            WHEN o.organization_type = ? THEN ?
            ELSE o.organization_type
        END AS normalized_organization_type,
        o.created_at AS org_created_at,
        o.updated_at AS org_updated_at
    FROM gocommon_user_organizations uo
    JOIN gocommon_organizations o
      ON o.id = uo.organization_id
    WHERE uo.user_id = ?
      AND o.deleted_at IS NULL
),
ranked_roles AS (
    SELECT
        t.organization_id AS target_organization_id,
        uo.organization_id AS source_organization_id,
        uo.role,
        CASE WHEN uo.organization_id = t.organization_id THEN 0 ELSE 1 END AS source_rank,
        array_position(t.org_lineage, uo.organization_id) AS lineage_pos,
        CASE
            WHEN uo.organization_id = t.organization_id THEN cardinality(t.org_lineage)
            ELSE array_position(t.org_lineage, uo.organization_id) - 1
        END AS source_depth
    FROM target_orgs t
    JOIN gocommon_user_organizations uo
      ON uo.user_id = t.user_id
     AND uo.organization_id = ANY(t.org_lineage || ARRAY[t.organization_id]::bigint[])
	JOIN gocommon_organizations source_org
	  ON source_org.id = uo.organization_id
	 AND source_org.deleted_at IS NULL
),
best_roles AS (
    SELECT
        target_organization_id,
        source_organization_id,
        role AS effective_role,
        source_depth AS effective_role_depth
    FROM (
        SELECT
            target_organization_id,
            source_organization_id,
            role,
            source_depth,
            ROW_NUMBER() OVER (
                PARTITION BY target_organization_id
                ORDER BY role DESC, source_rank ASC, lineage_pos DESC NULLS LAST
            ) AS rn
        FROM ranked_roles
    ) ranked_best
    WHERE rn = 1
),
view_thresholds(depth, required_view_role) AS (
    %s
),
request_filters(depth, explicit_role_mask, organization_type_mask) AS (
    %s
),
filtered AS (
    SELECT
        t.user_id,
        t.organization_id,
        t.joined_at,
        t.updated_at,
        t.explicit_role,
        b.effective_role,
        CASE
            WHEN b.source_organization_id IS NOT NULL AND b.source_organization_id <> t.organization_id
                THEN b.source_organization_id
            ELSE NULL
        END AS inherited_from_organization_id,
        b.effective_role_depth,
        t.org_name,
        t.org_slug,
        t.org_description,
        t.org_parent_organization_id,
		t.org_personal_owner_user_id,
        t.org_lineage,
        t.normalized_organization_type,
        t.org_created_at,
        t.org_updated_at
    FROM target_orgs t
    JOIN best_roles b ON b.target_organization_id = t.organization_id
    LEFT JOIN view_thresholds vt ON vt.depth = cardinality(t.org_lineage)
    LEFT JOIN request_filters rf ON rf.depth = cardinality(t.org_lineage)
    WHERE (
        ? = FALSE
        OR (
            b.effective_role >= COALESCE(vt.required_view_role, 0)
            AND t.normalized_organization_type >= ?
            AND (
                ? = FALSE
                OR (
                    rf.depth IS NOT NULL
                    AND
                    (rf.explicit_role_mask = 0 OR (rf.explicit_role_mask & (1::bigint << t.explicit_role)) <> 0)
                    AND (rf.organization_type_mask = 0 OR (rf.organization_type_mask & (1::bigint << t.normalized_organization_type)) <> 0)
                )
            )
        )
    )
    AND (
        ? = FALSE
        OR LOWER(COALESCE(t.org_name, '')) LIKE ?
        OR LOWER(COALESCE(t.org_slug, '')) LIKE ?
        OR LOWER(COALESCE(t.org_description, '')) LIKE ?
    )
    AND (
        ? = TRUE
        OR t.normalized_organization_type = ?
    )
    AND (
        ? = TRUE
        OR t.org_parent_organization_id = ?
    )
    AND (
        ? = TRUE
        OR cardinality(t.org_lineage) = ?
    )
)
`, viewThresholdsCTE, requestFiltersCTE)

	skipExactCount := query.SkipExactCount && query.Limit > 0
	totalCount := common_util.UnknownTotalCount

	// When an exact count is needed, COUNT(*) OVER () on the data query lets a
	// single execution of the lineage CTE serve both the rows and the total,
	// instead of running the whole CTE once for COUNT and once for data.
	totalCountColumn := ""
	if !skipExactCount {
		totalCountColumn = ",\n    COUNT(*) OVER () AS total_count"
	}

	dataArgs := append([]any{}, baseArgs...)
	dataQuery := baseQuery + fmt.Sprintf(`
SELECT
    user_id,
    organization_id,
    joined_at,
    updated_at,
    explicit_role,
    effective_role,
    inherited_from_organization_id,
    effective_role_depth,
    org_name,
    org_slug,
    org_description,
    org_parent_organization_id,
	org_personal_owner_user_id,
    org_lineage,
    normalized_organization_type,
    org_created_at,
    org_updated_at%s
FROM filtered
ORDER BY %s
`, totalCountColumn, orderClause)

	dataLimit := query.Limit
	if skipExactCount && dataLimit > 0 {
		dataLimit++
	}
	if dataLimit > 0 {
		dataQuery += "LIMIT ?\n"
		dataArgs = append(dataArgs, dataLimit)
	}
	if query.Offset > 0 {
		dataQuery += "OFFSET ?\n"
		dataArgs = append(dataArgs, query.Offset)
	}

	var rows []*userOrganizationListRow
	if err := db.Raw(dataQuery, dataArgs...).Scan(&rows).Error; err != nil {
		return nil, 0, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_user_organizations"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeySearch, query.Search),
			zap.Int(appLogger.KeyOffset, query.Offset),
			zap.Int(logkeys.Limit, query.Limit))
	}

	if !skipExactCount {
		switch {
		case len(rows) > 0:
			totalCount = rows[0].TotalCount
		case query.Offset > 0:
			// The requested page is past the end of the result set, so the
			// window count is unavailable; fall back to a dedicated COUNT.
			countQuery := baseQuery + "SELECT COUNT(*) FROM filtered"
			if err := db.Raw(countQuery, baseArgs...).Scan(&totalCount).Error; err != nil {
				return nil, 0, databaseErrorWithFields(err,
					zap.String(logkeys.Operation, "count_user_organizations"),
					zap.Uint64(appLogger.KeyUserID, userID),
					zap.String(appLogger.KeySearch, query.Search),
					zap.Int(appLogger.KeyOffset, query.Offset))
			}
		default:
			totalCount = 0
		}
	}

	results := make([]*UserOrganizationWithRoleInfo, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		results = append(results, userOrganizationListRowToResult(row))
	}

	return results, totalCount, nil
}

// GetOrganizationUsers executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetOrganizationUsers(ctx context.Context, organizationID int64, roleFilter proto_models.OrganizationRole, offset, limit int) ([]*common_persistence_models.UserOrganization, int64, error) {
	var userOrganizations []*common_persistence_models.UserOrganization
	var totalCount int64

	db := h.getDBFromContext(ctx)
	membershipTable := common_persistence_models.TableNameUserOrganizations
	query := db.Model(&common_persistence_models.UserOrganization{}).
		Scopes(ActiveOrganizationScope(membershipTable, "organization_id")).
		Where(membershipTable+".organization_id = ?", organizationID)

	// Apply role filter if specified
	if roleFilter != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		query = query.Where(membershipTable+".role = ?", roleFilter)
	}

	// Get total count for pagination
	if err := query.Count(&totalCount).Error; err != nil {
		return nil, 0, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "count_organization_users"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Stringer(appLogger.KeyRole, roleFilter))
	}

	// Apply pagination and preload user data with profile picture attachment
	query = query.
		Preload(common_persistence_models.PreloadUser, preloadUnscoped).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment, preloadLiveAttachment).
		Order(membershipTable + ".joined_at DESC").
		Offset(offset)
	if limit > 0 {
		query = query.Limit(limit)
	}

	if err := query.Find(&userOrganizations).Error; err != nil {
		return nil, 0, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_organization_users"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Stringer(appLogger.KeyRole, roleFilter),
			zap.Int(appLogger.KeyOffset, offset),
			zap.Int(logkeys.Limit, limit))
	}

	return userOrganizations, totalCount, nil
}

// GetOrganizationsByIDs executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetOrganizationsByIDs(ctx context.Context, organizationIDs []int64) ([]*common_persistence_models.Organization, error) {
	if len(organizationIDs) == 0 {
		return []*common_persistence_models.Organization{}, nil
	}

	var organizations []*common_persistence_models.Organization
	db := h.getDBFromContext(ctx)

	result := db.Where("id IN (?)", organizationIDs).Find(&organizations)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_organizations_by_ids"),
			zap.Int(appLogger.KeyCount, len(organizationIDs)))
	}
	return organizations, nil
}

// GetOrganizationsByParent executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetOrganizationsByParent(ctx context.Context, parentOrganizationID *int64, offset, limit int) ([]*common_persistence_models.Organization, int64, error) {
	var organizations []*common_persistence_models.Organization
	var totalCount int64
	db := h.getDBFromContext(ctx)

	query := db.Model(&common_persistence_models.Organization{})
	if parentOrganizationID == nil {
		query = query.Where("parent_organization_id IS NULL")
	} else {
		query = query.Where("parent_organization_id = ?", *parentOrganizationID)
	}

	if err := query.Count(&totalCount).Error; err != nil {
		return nil, 0, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "count_organizations_by_parent"),
			zap.Int64p(appLogger.KeyParentOrganizationID, parentOrganizationID))
	}

	query = query.Order("created_at DESC, id DESC").Offset(offset)
	if limit > 0 {
		query = query.Limit(limit)
	}

	if err := query.Find(&organizations).Error; err != nil {
		return nil, 0, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_organizations_by_parent"),
			zap.Int64p(appLogger.KeyParentOrganizationID, parentOrganizationID),
			zap.Int(appLogger.KeyOffset, offset),
			zap.Int(logkeys.Limit, limit))
	}

	return organizations, totalCount, nil
}

// GetOrganizationsByParentForUser executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetOrganizationsByParentForUser(ctx context.Context, userID uint64, parentOrganizationID *int64, offset, limit int) ([]*common_persistence_models.Organization, int64, error) {
	var organizations []*common_persistence_models.Organization
	var totalCount int64

	orgTable := common_persistence_models.TableNameOrganizations
	userOrgTable := common_persistence_models.TableNameUserOrganizations

	db := h.getDBFromContext(ctx)
	query := db.Table(orgTable+" AS o").
		Joins("INNER JOIN "+userOrgTable+" AS uo ON o.id = uo.organization_id").
		Where("uo.user_id = ?", userID).
		Where("o.deleted_at IS NULL")

	if parentOrganizationID == nil {
		query = query.Where("o.parent_organization_id IS NULL")
	} else {
		query = query.Where("o.parent_organization_id = ?", *parentOrganizationID)
	}

	if err := query.Count(&totalCount).Error; err != nil {
		return nil, 0, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "count_organizations_by_parent_for_user"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Int64p(appLogger.KeyParentOrganizationID, parentOrganizationID))
	}

	query = query.Order("o.created_at DESC, o.id DESC").Offset(offset)
	if limit > 0 {
		query = query.Limit(limit)
	}

	if err := query.Find(&organizations).Error; err != nil {
		return nil, 0, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_organizations_by_parent_for_user"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Int64p(appLogger.KeyParentOrganizationID, parentOrganizationID),
			zap.Int(appLogger.KeyOffset, offset),
			zap.Int(logkeys.Limit, limit))
	}

	return organizations, totalCount, nil
}

// GetOrganizationDescendantsPage uses the lineage index and an ID cursor so a
// deletion preview reads bounded subtree pages without visiting every parent.
// Callers own the page-size bound.
func (h *CommonPersistenceHandler[U]) GetOrganizationDescendantsPage(ctx context.Context, organizationID, afterOrganizationID int64, limit int) ([]*common_persistence_models.Organization, bool, error) {
	var organizations []*common_persistence_models.Organization
	if err := h.getDBFromContext(ctx).
		Where("lineage @> ARRAY[?]::bigint[] AND id <> ? AND id > ?", organizationID, organizationID, afterOrganizationID).
		Order("id ASC").Limit(common_util.QueryLimitWithProbe(limit)).Find(&organizations).Error; err != nil {
		return nil, false, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_organization_descendants_page"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	hasMore := len(organizations) > limit
	if hasMore {
		organizations = organizations[:limit]
	}
	return organizations, hasMore, nil
}

// GetOrganizationsByLineageLimited retrieves a root and descendants with a
// limit+one probe so full hierarchy API responses never materialize an
// unbounded organization set.
func (h *CommonPersistenceHandler[U]) GetOrganizationsByLineageLimited(
	ctx context.Context,
	organizationID int64,
	maxNodes int,
) ([]*common_persistence_models.Organization, bool, error) {
	if organizationID == 0 {
		return []*common_persistence_models.Organization{}, false, nil
	}
	if maxNodes <= 0 {
		maxNodes = 1
	}

	var organizations []*common_persistence_models.Organization
	db := h.getDBFromContext(ctx)
	query := db.
		Where("id = ? OR lineage @> ARRAY[?]::bigint[]", organizationID, organizationID).
		Order("cardinality(COALESCE(lineage, ARRAY[]::bigint[])) ASC, id ASC").
		Limit(common_util.QueryLimitWithProbe(maxNodes))
	if err := query.Find(&organizations).Error; err != nil {
		return nil, false, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_organizations_by_lineage_limited"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Int(logkeys.Limit, maxNodes))
	}
	if len(organizations) == 0 {
		return nil, false, ErrOrganizationNotFound
	}

	truncated := len(organizations) > maxNodes
	if truncated {
		organizations = organizations[:maxNodes]
	}
	return organizations, truncated, nil
}

// GetOrganizationsByLineageForUserLimited is the membership-filtered variant
// of GetOrganizationsByLineageLimited.
func (h *CommonPersistenceHandler[U]) GetOrganizationsByLineageForUserLimited(
	ctx context.Context,
	userID uint64,
	organizationID int64,
	maxNodes int,
) ([]*common_persistence_models.Organization, bool, error) {
	if organizationID == 0 {
		return []*common_persistence_models.Organization{}, false, nil
	}
	if maxNodes <= 0 {
		maxNodes = 1
	}

	var organizations []*common_persistence_models.Organization
	orgTable := common_persistence_models.TableNameOrganizations
	userOrgTable := common_persistence_models.TableNameUserOrganizations
	db := h.getDBFromContext(ctx)
	query := db.Table(orgTable+" AS o").
		Select("o.*").
		Joins("INNER JOIN "+userOrgTable+" AS uo ON o.id = uo.organization_id").
		Where("uo.user_id = ?", userID).
		Where("o.id = ? OR o.lineage @> ARRAY[?]::bigint[]", organizationID, organizationID).
		Where("o.deleted_at IS NULL").
		Order("cardinality(COALESCE(o.lineage, ARRAY[]::bigint[])) ASC, o.id ASC").
		Limit(common_util.QueryLimitWithProbe(maxNodes))

	if err := query.Find(&organizations).Error; err != nil {
		return nil, false, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_organizations_by_lineage_for_user_limited"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Int(logkeys.Limit, maxNodes))
	}
	if len(organizations) == 0 {
		return nil, false, ErrOrganizationNotFound
	}

	truncated := len(organizations) > maxNodes
	if truncated {
		organizations = organizations[:maxNodes]
	}
	return organizations, truncated, nil
}

// GetDescendantOrganizationIDs executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetDescendantOrganizationIDs(ctx context.Context, organizationID int64) ([]uint64, error) {
	var ids []uint64
	db := h.getDBFromContext(ctx)

	if organizationID == 0 {
		return []uint64{}, nil
	}

	if err := db.Model(&common_persistence_models.Organization{}).
		Where("id = ? OR lineage @> ARRAY[?]::bigint[]", organizationID, organizationID).
		Pluck("id", &ids).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_descendant_organization_ids"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}

	if len(ids) == 0 {
		return nil, ErrOrganizationNotFound
	}

	return ids, nil
}

// deleteOrganizationMembershipsByLineage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) deleteOrganizationMembershipsByLineage(ctx context.Context, db *gorm.DB, organizationID int64) error {
	db = db.WithContext(ctx)
	if organizationID == 0 {
		return ErrOrganizationNotFound
	}
	subquery := db.Model(&common_persistence_models.Organization{}).
		Select("id").
		Where("id = ? OR lineage @> ARRAY[?]::bigint[]", organizationID, organizationID)
	if err := db.Where("organization_id IN (?)", subquery).
		Delete(&common_persistence_models.UserOrganization{}).Error; err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "delete_organization_memberships_by_lineage"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	return nil
}

// deleteOrganizationInvitationsByLineage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) deleteOrganizationInvitationsByLineage(ctx context.Context, db *gorm.DB, organizationID int64) error {
	db = db.WithContext(ctx)
	if organizationID == 0 {
		return ErrOrganizationNotFound
	}
	subquery := db.Model(&common_persistence_models.Organization{}).
		Select("id").
		Where("id = ? OR lineage @> ARRAY[?]::bigint[]", organizationID, organizationID)
	if err := db.Where("organization_id IN (?)", subquery).
		Delete(&common_persistence_models.OrganizationInvitation{}).Error; err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "delete_organization_invitations_by_lineage"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	return nil
}

// deleteOrganizationEmailDomainsByLineage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) deleteOrganizationEmailDomainsByLineage(ctx context.Context, db *gorm.DB, organizationID int64) error {
	db = db.WithContext(ctx)
	if organizationID == 0 {
		return ErrOrganizationNotFound
	}
	subquery := db.Model(&common_persistence_models.Organization{}).
		Select("id").
		Where("id = ? OR lineage @> ARRAY[?]::bigint[]", organizationID, organizationID)
	if err := db.Where("organization_id IN (?)", subquery).
		Delete(&common_persistence_models.OrganizationEmailDomain{}).Error; err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "delete_organization_email_domains_by_lineage"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	return nil
}

// deleteOrganizationsByLineage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) deleteOrganizationsByLineage(ctx context.Context, db *gorm.DB, organizationID int64) error {
	if organizationID == 0 {
		return ErrOrganizationNotFound
	}
	result := db.Where("id = ? OR lineage @> ARRAY[?]::bigint[]", organizationID, organizationID).
		Delete(&common_persistence_models.Organization{})
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "delete_organizations_by_lineage"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Organization hierarchy not found for delete",
				zap.String(logkeys.Operation, "delete_organizations_by_lineage"),
				zap.Int64(appLogger.KeyOrganizationID, organizationID))
		}
		return ErrOrganizationNotFound
	}
	return nil
}

// UpdateOrganizationAvatar executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateOrganizationAvatar(ctx context.Context, organizationID int64, attachmentID *uint64) error {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.Organization{}).
		Where("id = ?", organizationID).
		Update("avatar_attachment_id", attachmentID)

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_organization_avatar"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Uint64p(appLogger.KeyAttachmentID, attachmentID))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Organization not found for avatar update",
				zap.String(logkeys.Operation, "update_organization_avatar"),
				zap.Int64(appLogger.KeyOrganizationID, organizationID))
		}
		return ErrOrganizationNotFound
	}
	return nil
}

// GetOrganizationMemberCountAtOrAboveRole returns how many members of an organization hold
// the given role or a higher one. Passing the lowest owner tier counts every owner, which
// backs the last-owner removal/demotion guard.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - organizationID: Organization whose members are counted.
//   - minRole: Minimum (inclusive) role to count.
//
// Returns:
//   - int64: Number of members with role >= minRole.
//   - error: ErrDatabaseError when the count query fails.
func (h *CommonPersistenceHandler[U]) GetOrganizationMemberCountAtOrAboveRole(ctx context.Context, organizationID int64, minRole proto_models.OrganizationRole) (int64, error) {
	var count int64
	db := h.getDBFromContext(ctx)
	membershipTable := common_persistence_models.TableNameUserOrganizations

	result := db.Model(&common_persistence_models.UserOrganization{}).
		Scopes(ActiveOrganizationScope(membershipTable, "organization_id")).
		Where(membershipTable+".organization_id = ? AND "+membershipTable+".role >= ?", organizationID, minRole).
		Count(&count)

	if result.Error != nil {
		return 0, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_organization_member_count_at_or_above_role"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID),
			zap.Stringer(appLogger.KeyMinRole, minRole))
	}
	return count, nil
}

// GetOrganizationMemberCount executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetOrganizationMemberCount(ctx context.Context, organizationID int64) (int64, error) {
	var count int64
	db := h.getDBFromContext(ctx)
	membershipTable := common_persistence_models.TableNameUserOrganizations

	result := db.Model(&common_persistence_models.UserOrganization{}).
		Scopes(ActiveOrganizationScope(membershipTable, "organization_id")).
		Where(membershipTable+".organization_id = ?", organizationID).
		Count(&count)

	if result.Error != nil {
		return 0, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_organization_member_count"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	return count, nil
}

// GetOrganizationMemberCounts executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetOrganizationMemberCounts(ctx context.Context, organizationIDs []int64) (map[int64]int64, error) {
	counts := make(map[int64]int64)
	if len(organizationIDs) == 0 {
		return counts, nil
	}

	// organizationMemberCount carries grouped membership totals while
	// GetOrganizationMemberCounts maps aggregate query results back to organization IDs.
	// OrganizationID must be exported so GORM can scan the grouped organization_id into
	// it; an unexported field is skipped and would leave every count keyed to org 0.
	type organizationMemberCount struct {
		OrganizationID int64 `gorm:"column:organization_id"`
		Count          int64 `gorm:"column:member_count"`
	}

	var results []organizationMemberCount
	db := h.getDBFromContext(ctx)
	membershipTable := common_persistence_models.TableNameUserOrganizations

	result := db.Model(&common_persistence_models.UserOrganization{}).
		Select(membershipTable+".organization_id AS organization_id, COUNT(*) AS member_count").
		Scopes(ActiveOrganizationScope(membershipTable, "organization_id")).
		Where(membershipTable+".organization_id IN (?)", organizationIDs).
		Group(membershipTable + ".organization_id").
		Scan(&results)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_organization_member_counts"),
			zap.Int(appLogger.KeyCount, len(organizationIDs)))
	}

	for _, row := range results {
		counts[row.OrganizationID] = row.Count
	}

	return counts, nil
}
