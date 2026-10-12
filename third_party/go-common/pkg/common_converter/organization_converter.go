package common_converter

import (
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/nyaruka/phonenumbers"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (h *ConverterHandler[U]) userOrganizationCreatedAt(
	pUserOrg *common_persistence_models.UserOrganization,
	stage string,
) time.Time {
	if !pUserOrg.JoinedAt.IsZero() {
		return pUserOrg.JoinedAt
	}
	appLogger.LogInvariantViolation(
		appLogger.For(context.Background(), h.logger, loggerName),
		"user_organization_joined_at_present",
		fmt.Errorf("user organization joined_at is zero"),
		zap.String(logkeys.Stage, stage),
		zap.Uint64(appLogger.KeyUserID, pUserOrg.UserID),
		zap.Int64(appLogger.KeyOrganizationID, pUserOrg.OrganizationID),
		zap.String(logkeys.Expected, "non-zero joined_at"),
		zap.String(logkeys.Actual, "zero joined_at"),
		zap.String(logkeys.Fallback, "updated_at"),
		zap.Time(appLogger.KeyUpdatedAt, pUserOrg.UpdatedAt),
	)
	return pUserOrg.UpdatedAt
}

// PersistenceOrganizationToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceOrganizationToService(pOrg *common_persistence_models.Organization) *common_service_models.Organization {
	if pOrg == nil {
		return nil
	}

	// An organization avatar is not restricted to the profile-picture object type,
	// so it is gated on public availability alone.
	avatarPath := publicAssetObjectName(pOrg.AvatarAttachment)

	description := ""
	if pOrg.Description != nil {
		description = *pOrg.Description
	}

	var parentID *common_service_models.OrganizationIdType
	if pOrg.ParentOrganizationID != nil {
		converted := common_service_models.OrganizationIdType(*pOrg.ParentOrganizationID)
		parentID = &converted
	}
	var personalOwnerUserID *common_service_models.UserIdType
	if pOrg.PersonalOwnerUserID != nil {
		converted := common_service_models.UserIdType(*pOrg.PersonalOwnerUserID)
		personalOwnerUserID = &converted
	}

	// pOrg.ID is uint64 (from GORM model)
	// serviceOrg.ID is OrganizationIdType (uint64)
	return &common_service_models.Organization{
		ID:                   common_service_models.OrganizationIdType(pOrg.ID),
		ParentOrganizationID: parentID,
		Lineage:              pOrg.Lineage,
		Name:                 pOrg.Name,
		Slug:                 pOrg.Slug,
		Description:          description,
		AvatarUrl:            avatarPath, // Public FinalObjectName, or empty when not publicly available
		OrganizationType:     pOrg.OrganizationType,
		PersonalOwnerUserID:  personalOwnerUserID,
		CreatedAt:            pOrg.CreatedAt,
		UpdatedAt:            pOrg.UpdatedAt,
	}
}

// ServiceOrganizationToPersistence executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceOrganizationToPersistence(org common_service_models.OrganizationLike) *common_persistence_models.Organization {
	if org == nil {
		return nil
	}

	persistenceOrg := &common_persistence_models.Organization{
		Name:                 org.GetName(),
		Slug:                 org.GetSlug(),
		ParentOrganizationID: nil,
		Lineage:              org.GetLineage(),
		OrganizationType:     org.GetOrganizationType(),
		// AvatarAttachmentID is NOT set here from AvatarUrl.
		CreatedAt: org.GetCreatedAt(),
		UpdatedAt: org.GetUpdatedAt(),
	}
	if ownerID := org.GetPersonalOwnerUserID(); ownerID != nil {
		personalOwnerUserID := uint64(*ownerID)
		persistenceOrg.PersonalOwnerUserID = &personalOwnerUserID
	}

	// Handle optional description field
	description := org.GetDescription()
	if description != "" {
		persistenceOrg.Description = &description
	}

	if org.GetID() != 0 { // OrganizationLike.GetID() returns uint64 (OrganizationIdType)
		// common_persistence_models.Organization.ID is uint64
		persistenceOrg.ID = org.GetID()
	}

	if org.GetParentOrganizationID() != nil {
		parentID := *org.GetParentOrganizationID()
		persistenceOrg.ParentOrganizationID = &parentID
	}

	return persistenceOrg
}

// ServiceOrganizationToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceOrganizationToProto(org common_service_models.OrganizationLike) *proto_models.Organization {

	// Avatars are attachment-backed public objects, so their URL carries the public bucket
	avatarURL := common_service_models.ComposePublicAttachmentURL(org.GetAvatarUrl(), h.publicBaseURL(), h.publicBucket)

	// Handle optional fields for proto
	var description *string
	if desc := org.GetDescription(); desc != "" {
		description = &desc
	}

	var avatarUrlPtr *string
	if avatarURL != "" {
		avatarUrlPtr = &avatarURL
	}

	var parentOrganizationID *uint64
	if org.GetParentOrganizationID() != nil {
		converted := uint64(*org.GetParentOrganizationID())
		parentOrganizationID = &converted
	}
	var personalOwnerUserID *uint64
	if ownerID := org.GetPersonalOwnerUserID(); ownerID != nil {
		converted := uint64(*ownerID)
		personalOwnerUserID = &converted
	}

	lineage := common_util.ApplyToEach(org.GetLineage(), func(val common_service_models.OrganizationIdType) uint64 {
		return uint64(val)
	})

	return &proto_models.Organization{
		Id:                   org.GetID(), // OrganizationLike.GetID() returns uint64
		Name:                 org.GetName(),
		Slug:                 org.GetSlug(),
		Description:          description,
		AvatarUrl:            avatarUrlPtr,
		CreatedAt:            timestamppb.New(org.GetCreatedAt()),
		UpdatedAt:            timestamppb.New(org.GetUpdatedAt()),
		ParentOrganizationId: parentOrganizationID,
		Lineage:              lineage,
		OrganizationType:     org.GetOrganizationType(),
		PersonalOwnerUserId:  personalOwnerUserID,
	}
}

// ProtoOrganizationToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ProtoOrganizationToService(org *proto_models.Organization) (*common_service_models.Organization, error) {
	if org == nil {
		return nil, fmt.Errorf("cannot convert nil proto organization to service organization")
	}

	description := ""
	if org.Description != nil {
		description = *org.Description
	}

	avatarUrl := ""
	if org.AvatarUrl != nil {
		avatarUrl = *org.AvatarUrl
	}

	createdAt := org.CreatedAt.AsTime()
	updatedAt := org.UpdatedAt.AsTime()

	var parentID *common_service_models.OrganizationIdType
	if org.ParentOrganizationId != nil {
		converted := common_service_models.OrganizationIdType(*org.ParentOrganizationId)
		parentID = &converted
	}
	var personalOwnerUserID *common_service_models.UserIdType
	if org.PersonalOwnerUserId != nil {
		converted := common_service_models.UserIdType(*org.PersonalOwnerUserId)
		personalOwnerUserID = &converted
	}

	lineage := common_util.ApplyToEach(org.Lineage, func(val uint64) common_service_models.OrganizationIdType {
		return common_service_models.OrganizationIdType(val)
	})

	return &common_service_models.Organization{
		ID:                   org.Id,
		ParentOrganizationID: parentID,
		Lineage:              lineage,
		Name:                 org.Name,
		Slug:                 org.Slug,
		Description:          description,
		AvatarUrl:            avatarUrl, // Store object path as is
		OrganizationType:     org.OrganizationType,
		PersonalOwnerUserID:  personalOwnerUserID,
		CreatedAt:            createdAt,
		UpdatedAt:            updatedAt,
	}, nil
}

// PersistenceUserOrganizationToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceUserOrganizationToService(pUserOrg *common_persistence_models.UserOrganization) *common_service_models.UserOrganization {
	if pUserOrg == nil {
		return nil
	}

	createdAt := h.userOrganizationCreatedAt(pUserOrg, "convert_user_organization_to_service")

	return &common_service_models.UserOrganization{
		UserID:         common_service_models.UserIdType(pUserOrg.UserID),
		OrganizationID: common_service_models.OrganizationIdType(pUserOrg.OrganizationID),
		ExplicitRole:   pUserOrg.Role,
		EffectiveRole:  pUserOrg.Role,
		CreatedAt:      createdAt,
		UpdatedAt:      pUserOrg.UpdatedAt,
	}
}

// PersistenceUserOrganizationToServiceMemberDetails executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceUserOrganizationToServiceMemberDetails(pUserOrg *common_persistence_models.UserOrganization) *common_service_models.OrganizationMemberDetails {
	if pUserOrg == nil {
		return nil
	}

	createdAt := h.userOrganizationCreatedAt(pUserOrg, "convert_user_organization_to_member_details")

	// Extract avatar path from preloaded ProfilePictureAttachment
	avatarPath := publicAvatarObjectName(pUserOrg.User.ProfilePictureAttachment)

	return &common_service_models.OrganizationMemberDetails{
		OrganizationID: common_service_models.OrganizationIdType(pUserOrg.OrganizationID),
		UserID:         common_service_models.UserIdType(pUserOrg.UserID),
		ExplicitRole:   pUserOrg.Role,
		EffectiveRole:  pUserOrg.Role,
		CreatedAt:      createdAt,
		UpdatedAt:      pUserOrg.UpdatedAt,

		// User profile information from preloaded User
		Username:  pUserOrg.User.Username,
		Email:     pUserOrg.User.Email,
		FirstName: pUserOrg.User.FirstName,
		LastName:  pUserOrg.User.LastName,
		AvatarUrl: avatarPath,
		UserType:  pUserOrg.User.UserType,
		Banned:    pUserOrg.User.Banned,

		// Activity information
		IsActive: !pUserOrg.User.Banned,
	}
}

// ServiceOrganizationMemberDetailsToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceOrganizationMemberDetailsToProto(member *common_service_models.OrganizationMemberDetails) *proto_models.OrganizationMemberDetails {
	if member == nil {
		return nil
	}

	result := &proto_models.OrganizationMemberDetails{
		OrganizationId: member.OrganizationID,
		UserId:         member.UserID,
		ExplicitRole:   member.ExplicitRole,
		EffectiveRole:  member.EffectiveRole,
		CreatedAt:      timestamppb.New(member.CreatedAt),
		UpdatedAt:      timestamppb.New(member.UpdatedAt),
		IsActive:       member.IsActive,
	}
	if member.InheritedFromOrganizationID != nil {
		converted := uint64(*member.InheritedFromOrganizationID)
		result.InheritedFromOrganizationId = &converted
	}
	if member.EffectiveRoleDepth != nil {
		converted := *member.EffectiveRoleDepth
		result.EffectiveRoleDepth = &converted
	}

	userLike := organizationMemberUserLike{
		id:        member.UserID,
		username:  member.Username,
		email:     member.Email,
		firstName: member.FirstName,
		lastName:  member.LastName,
		avatarUrl: member.AvatarUrl,
		userType:  member.UserType,
		banned:    member.Banned,
		isBot:     member.Bot,
	}
	result.User = h.ConvertUserLikeToPublicProto(&userLike)

	if member.LastActivity != nil {
		result.LastActivity = timestamppb.New(*member.LastActivity)
	}

	return result
}

// ServiceOrganizationMemberDetailsToFilteredProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceOrganizationMemberDetailsToFilteredProto(member *common_service_models.OrganizationMemberDetails, permissions *config.UserAttributePermissions) *proto_models.OrganizationMemberDetails {
	if member == nil {
		return nil
	}

	result := &proto_models.OrganizationMemberDetails{
		OrganizationId: member.OrganizationID,
		UserId:         member.UserID,
		ExplicitRole:   member.ExplicitRole,
		EffectiveRole:  member.EffectiveRole,
		CreatedAt:      timestamppb.New(member.CreatedAt),
		UpdatedAt:      timestamppb.New(member.UpdatedAt),
		IsActive:       member.IsActive,
	}
	if member.InheritedFromOrganizationID != nil {
		converted := uint64(*member.InheritedFromOrganizationID)
		result.InheritedFromOrganizationId = &converted
	}
	if member.EffectiveRoleDepth != nil {
		converted := *member.EffectiveRoleDepth
		result.EffectiveRoleDepth = &converted
	}

	userLike := organizationMemberUserLike{
		id:        member.UserID,
		username:  member.Username,
		email:     member.Email,
		firstName: member.FirstName,
		lastName:  member.LastName,
		avatarUrl: member.AvatarUrl,
		userType:  member.UserType,
		banned:    member.Banned,
		isBot:     member.Bot,
	}
	// Apply permission-based filtering to user attributes
	result.User = h.ConvertUserLikeToPublicProtoWithPermissions(&userLike, permissions)

	if member.LastActivity != nil && permissions != nil && permissions.CanViewLastActivity() {
		result.LastActivity = timestamppb.New(*member.LastActivity)
	}

	return result
}

// organizationMemberUserLike defines the data structure used in the backend conversion flow between persistence, service, and transport models.
// It carries state across layer boundaries handled by this package.
type organizationMemberUserLike struct {
	id        common_service_models.UserIdType
	username  string
	email     string
	firstName string
	lastName  string
	avatarUrl string
	userType  proto_models.UserType
	banned    bool
	isBot     bool
}

// GetID executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *organizationMemberUserLike) GetID() common_service_models.UserIdType {
	return u.id
}

// GetEmail executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *organizationMemberUserLike) GetEmail() string {
	return u.email
}

// GetPhoneNumber executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *organizationMemberUserLike) GetPhoneNumber() *phonenumbers.PhoneNumber {
	return nil
}

// GetFirstName executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *organizationMemberUserLike) GetFirstName() string {
	return u.firstName
}

// GetLastName executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *organizationMemberUserLike) GetLastName() string {
	return u.lastName
}

// GetUsername executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *organizationMemberUserLike) GetUsername() string {
	return u.username
}

// GetAvatarUrl executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *organizationMemberUserLike) GetAvatarUrl() string {
	return u.avatarUrl
}

// GetUserType executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *organizationMemberUserLike) GetUserType() proto_models.UserType {
	return u.userType
}

// IsBanned executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *organizationMemberUserLike) IsBanned() bool {
	return u.banned
}

// IsBot executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *organizationMemberUserLike) IsBot() bool {
	return u.isBot
}

// ServiceUserOrganizationToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceUserOrganizationToProto(userOrg common_service_models.UserOrganizationLike) *proto_models.OrganizationUser {
	if userOrg == nil {
		return nil
	}

	var inheritedFrom *uint64
	if userOrg.GetInheritedFromOrganizationID() != nil {
		converted := uint64(*userOrg.GetInheritedFromOrganizationID())
		inheritedFrom = &converted
	}
	var effectiveRoleDepth *uint32
	if userOrg.GetEffectiveRoleDepth() != nil {
		converted := *userOrg.GetEffectiveRoleDepth()
		effectiveRoleDepth = &converted
	}

	return &proto_models.OrganizationUser{
		OrganizationId:              userOrg.GetOrganizationID(),
		UserId:                      userOrg.GetUserID(),
		ExplicitRole:                userOrg.GetExplicitRole(),
		EffectiveRole:               userOrg.GetEffectiveRole(),
		CreatedAt:                   timestamppb.New(userOrg.GetCreatedAt()),
		UpdatedAt:                   timestamppb.New(userOrg.GetUpdatedAt()),
		InheritedFromOrganizationId: inheritedFrom,
		EffectiveRoleDepth:          effectiveRoleDepth,
	}
}

// PersistenceOrganizationEmailDomainToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceOrganizationEmailDomainToService(domain *common_persistence_models.OrganizationEmailDomain) *common_service_models.OrganizationEmailDomain {
	if domain == nil {
		return nil
	}

	var createdBy *common_service_models.UserIdType
	if domain.CreatedByUserID != nil {
		value := common_service_models.UserIdType(*domain.CreatedByUserID)
		createdBy = &value
	}

	return &common_service_models.OrganizationEmailDomain{
		ID:              domain.ID,
		OrganizationID:  common_service_models.OrganizationIdType(domain.OrganizationID),
		Domain:          domain.Domain,
		CreatedByUserID: createdBy,
		CreatedAt:       domain.CreatedAt,
		UpdatedAt:       domain.UpdatedAt,
	}
}

// ServiceOrganizationEmailDomainToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceOrganizationEmailDomainToProto(domain common_service_models.OrganizationEmailDomainLike) *proto_models.OrganizationEmailDomain {
	if domain == nil {
		return nil
	}

	var createdBy *uint64
	if domain.GetCreatedByUserID() != nil {
		value := uint64(*domain.GetCreatedByUserID())
		createdBy = &value
	}

	return &proto_models.OrganizationEmailDomain{
		Id:              domain.GetID(),
		OrganizationId:  domain.GetOrganizationID(),
		Domain:          domain.GetDomain(),
		CreatedByUserId: createdBy,
		CreatedAt:       timestamppb.New(domain.GetCreatedAt()),
		UpdatedAt:       timestamppb.New(domain.GetUpdatedAt()),
	}
}

// ServiceUserOrganizationResponseToOrganizationWithUserRole executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceUserOrganizationResponseToOrganizationWithUserRole(userOrgResponse *common_service_models.UserOrganizationResponse) *proto_models.OrganizationWithUserRole {
	if userOrgResponse == nil || userOrgResponse.UserOrganization == nil || userOrgResponse.Organization == nil {
		return nil
	}

	// Convert the embedded Organization to proto
	orgProto := h.ServiceOrganizationToProto(userOrgResponse.Organization)

	return &proto_models.OrganizationWithUserRole{
		Organization:  orgProto,
		ExplicitRole:  userOrgResponse.ExplicitRole,
		EffectiveRole: userOrgResponse.EffectiveRole,
		InheritedFromOrganizationId: func() *uint64 {
			if userOrgResponse.GetInheritedFromOrganizationID() == nil {
				return nil
			}
			converted := uint64(*userOrgResponse.GetInheritedFromOrganizationID())
			return &converted
		}(),
		EffectiveRoleDepth: func() *uint32 {
			if userOrgResponse.GetEffectiveRoleDepth() == nil {
				return nil
			}
			converted := *userOrgResponse.GetEffectiveRoleDepth()
			return &converted
		}(),
	}
}

// PersistenceOrganizationInvitationToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceOrganizationInvitationToService(pInvite *common_persistence_models.OrganizationInvitation) *common_service_models.OrganizationInvitation {
	if pInvite == nil {
		return nil
	}

	invite := &common_service_models.OrganizationInvitation{
		ID:                 pInvite.ID,
		OrganizationID:     common_service_models.OrganizationIdType(pInvite.OrganizationID),
		Email:              pInvite.Email,
		Role:               pInvite.Role,
		InvitedByUserID:    common_service_models.UserIdType(pInvite.InvitedByUserID),
		CreatedAt:          pInvite.CreatedAt,
		ExpiresAt:          pInvite.ExpiresAt,
		AcceptedAt:         pInvite.AcceptedAt,
		DeclinedAt:         pInvite.DeclinedAt,
		RequiresAcceptance: pInvite.RequiresAcceptance,
	}

	// Convert embedded organization if preloaded
	if pInvite.Organization.ID != 0 {
		invite.Organization = h.PersistenceOrganizationToService(&pInvite.Organization)
	}

	return invite
}

// ServiceOrganizationInvitationToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceOrganizationInvitationToProto(invite *common_service_models.OrganizationInvitation, org *common_service_models.Organization) *proto_models.OrganizationInvitation {
	if invite == nil {
		return nil
	}

	protoInvite := &proto_models.OrganizationInvitation{
		Id:                 invite.GetID(),
		OrganizationId:     invite.GetOrganizationID(),
		Email:              invite.GetEmail(),
		Role:               invite.GetRole(),
		InvitedByUserId:    invite.GetInvitedByUserID(),
		CreatedAt:          timestamppb.New(invite.GetCreatedAt()),
		ExpiresAt:          timestamppb.New(invite.GetExpiresAt()),
		RequiresAcceptance: invite.RequiresAcceptanceFlag(),
	}

	// Handle optional accepted/declined timestamps
	if invite.GetAcceptedAt() != nil {
		protoInvite.AcceptedAt = timestamppb.New(*invite.GetAcceptedAt())
	}
	if invite.GetDeclinedAt() != nil {
		protoInvite.DeclinedAt = timestamppb.New(*invite.GetDeclinedAt())
	}

	// Convert organization if provided
	if org != nil {
		protoInvite.Organization = h.ServiceOrganizationToProto(org)
	}

	return protoInvite
}
