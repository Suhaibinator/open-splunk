package common_converter

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/nyaruka/phonenumbers"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ConvertPersistenceToUser executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ConvertPersistenceToUser(pUser *common_persistence_models.User) *common_service_models.User {
	if pUser == nil {
		return nil
	}
	avatarPath := publicAvatarObjectName(pUser.ProfilePictureAttachment)

	// Handle phone number conversion from *string to string
	var phoneNumber *phonenumbers.PhoneNumber
	if pUser.PhoneNumber != nil {
		// Parse the phone number from E.164 format
		var err error
		phoneNumber, err = phonenumbers.Parse(*pUser.PhoneNumber, "")
		if err != nil {
			phoneNumber = nil // Set to nil if parsing fails
			// Continue with conversion - don't return nil User
		}
	}

	// Both the explicit persistence ID and the service UserIdType are uint64.
	return &common_service_models.User{
		ID:          common_service_models.UserIdType(pUser.ID),
		Username:    pUser.Username,
		AvatarUrl:   avatarPath, // Public FinalObjectName, or empty when not publicly available
		UserType:    pUser.UserType,
		Email:       pUser.Email,
		Banned:      pUser.Banned,
		Bot:         pUser.IsBot,
		FirstName:   pUser.FirstName,
		LastName:    pUser.LastName,
		PhoneNumber: phoneNumber,
	}
}

// ConvertUserToPersistence executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ConvertUserToPersistence(user common_service_models.UserLike) *common_persistence_models.User {
	if user == nil {
		return nil
	}
	persistenceUser := &common_persistence_models.User{
		// Persisted timestamps are handled by GORM; the explicit ID is set below.
		Username:  user.GetUsername(),
		UserType:  user.GetUserType(),
		Email:     user.GetEmail(),
		Banned:    user.IsBanned(),
		IsBot:     user.IsBot(),
		FirstName: user.GetFirstName(),
		LastName:  user.GetLastName(),
		// ProfilePictureAttachmentID is NOT set here from AvatarUrl.
	}

	// Handle phone number conversion from string to *string
	phoneNumber := user.GetPhoneNumber()
	if phoneNumber != nil {
		phoneNumberStr := phonenumbers.Format(phoneNumber, phonenumbers.E164)
		persistenceUser.PhoneNumber = &phoneNumberStr
	}
	if user.GetID() != 0 { // UserLike.GetID() returns uint64 (UserIdType)
		persistenceUser.ID = user.GetID()
	}
	return persistenceUser
}

// ConvertUserLikeToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ConvertUserLikeToProto(ctx context.Context, user common_service_models.UserLike) *proto_models.User {
	result := h.convertUserToProtoNoPermissions(user)
	// Unauthenticated viewers get the "public" policy (UserType UNSPECIFIED).
	viewerType := proto_models.UserType_USER_TYPE_UNSPECIFIED
	if requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx); found && requestingUser != nil {
		viewerType = (*requestingUser).GetUserType()
	}
	// One policy sample per conversion, compiled once per configuration
	// generation. Rows of one response may straddle a publish.
	if permissions := h.globalPermissions().GetPermissions(viewerType); permissions != nil {
		return h.FilterProtoUserWithPermissions(result, permissions)
	}
	return result
}

// ConvertUserLikeToPublicProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ConvertUserLikeToPublicProto(user common_service_models.UserLike) *proto_models.PublicUser {
	if user == nil {
		return nil
	}
	return h.convertUserLikeToPublicProtoWithIdentifiers(user, h.publicIdentifierTypes())
}

func (h *ConverterHandler[U]) convertUserLikeToPublicProtoWithIdentifiers(
	user common_service_models.UserLike,
	identifiers publicIdentifierTypes,
) *proto_models.PublicUser {
	if user == nil {
		return nil
	}
	avatarURL := common_service_models.ComposePublicAttachmentURL(user.GetAvatarUrl(), h.publicBaseURL(), h.publicBucket)
	primary := h.userIdentifierDisplayValue(user, identifiers.primary)
	secondary := ""
	if identifiers.secondary != proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_UNSPECIFIED &&
		identifiers.secondary != identifiers.primary {
		secondary = h.userIdentifierDisplayValue(user, identifiers.secondary)
		if secondary == primary {
			secondary = ""
		}
	}

	return &proto_models.PublicUser{
		Id:               user.GetID(),
		UserType:         user.GetUserType(),
		Banned:           user.IsBanned(),
		IsBot:            user.IsBot(),
		DisplayPrimary:   primary,
		DisplaySecondary: secondary,
		AvatarUrl:        avatarURL,
	}
}

// ConvertUserLikeToPublicProtoWithPermissions executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ConvertUserLikeToPublicProtoWithPermissions(user common_service_models.UserLike, permissions *config.UserAttributePermissions) *proto_models.PublicUser {
	if user == nil {
		return nil
	}

	filtered := &userLikeWithPermissions{
		user:        user,
		permissions: permissions,
	}

	return h.convertUserLikeToPublicProtoWithIdentifiers(filtered, h.publicIdentifierTypes())
}

// convertUserToProtoNoPermissions executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) convertUserToProtoNoPermissions(user common_service_models.UserLike) *proto_models.User {
	if user == nil {
		return nil
	}

	// Avatars are attachment-backed public objects, so their URL carries the public bucket
	avatarURL := common_service_models.ComposePublicAttachmentURL(user.GetAvatarUrl(), h.publicBaseURL(), h.publicBucket)

	var phoneNumberStr *string
	if user.GetPhoneNumber() != nil {
		// Format the phone number to E.164 string representation
		phoneNumber := phonenumbers.Format(user.GetPhoneNumber(), phonenumbers.E164)
		phoneNumberStr = &phoneNumber
	} else {
		phoneNumberStr = nil
	}

	return &proto_models.User{
		Id:          user.GetID(), // UserLike.GetID() returns uint64
		Username:    user.GetUsername(),
		AvatarUrl:   avatarURL,
		UserType:    user.GetUserType(),
		Email:       user.GetEmail(),
		Banned:      user.IsBanned(),
		IsBot:       user.IsBot(),
		FirstName:   user.GetFirstName(),
		LastName:    user.GetLastName(),
		PhoneNumber: phoneNumberStr,
	}
}

// userIdentifierDisplayValue executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) userIdentifierDisplayValue(user common_service_models.UserLike, identifierType proto_models.UserIdentifierType) string {
	if user == nil {
		return ""
	}

	switch identifierType {
	case proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_USERNAME:
		return strings.TrimSpace(user.GetUsername())
	case proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_EMAIL:
		return strings.TrimSpace(user.GetEmail())
	case proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_REAL_NAME:
		first := strings.TrimSpace(user.GetFirstName())
		last := strings.TrimSpace(user.GetLastName())
		if first != "" && last != "" {
			return first + " " + last
		}
		if first != "" {
			return first
		}
		return last
	case proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_USER_ID:
		return strconv.FormatUint(user.GetID(), 10)
	case proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_FIRST_NAME:
		return strings.TrimSpace(user.GetFirstName())
	case proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_FIRST_NAME_LAST_INITIAL:
		return formatFirstNameLastInitial(user.GetFirstName(), user.GetLastName())
	case proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_INITIALS_ONLY:
		return formatInitialsOnly(user.GetFirstName(), user.GetLastName())
	default:
		return ""
	}
}

// userLikeWithPermissions defines the data structure used in the backend conversion flow between persistence, service, and transport models.
// It carries state across layer boundaries handled by this package.
type userLikeWithPermissions struct {
	user        common_service_models.UserLike
	permissions *config.UserAttributePermissions
}

// GetID executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) GetID() common_service_models.UserIdType {
	return u.user.GetID()
}

// GetEmail executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) GetEmail() string {
	if !u.canViewEmail() {
		return ""
	}
	return u.user.GetEmail()
}

// GetPhoneNumber executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) GetPhoneNumber() *phonenumbers.PhoneNumber {
	if !u.canViewPhoneNumber() {
		return nil
	}
	return u.user.GetPhoneNumber()
}

// GetFirstName executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) GetFirstName() string {
	if !u.canViewFirstName() {
		return ""
	}
	return u.user.GetFirstName()
}

// GetLastName executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) GetLastName() string {
	if !u.canViewLastName() {
		return ""
	}
	return u.user.GetLastName()
}

// GetUsername executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) GetUsername() string {
	if !u.canViewUsername() {
		return ""
	}
	return u.user.GetUsername()
}

// GetAvatarUrl executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) GetAvatarUrl() string {
	if !u.canViewAvatarUrl() {
		return ""
	}
	return u.user.GetAvatarUrl()
}

// GetUserType executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) GetUserType() proto_models.UserType {
	return u.user.GetUserType()
}

// IsBanned executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) IsBanned() bool {
	return u.user.IsBanned()
}

// IsBot executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) IsBot() bool {
	return u.user.IsBot()
}

// canViewEmail executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) canViewEmail() bool {
	return u.permissions != nil && u.permissions.CanViewEmail()
}

// canViewPhoneNumber executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) canViewPhoneNumber() bool {
	return u.permissions != nil && u.permissions.CanViewPhoneNumber()
}

// canViewFirstName executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) canViewFirstName() bool {
	return u.permissions != nil && u.permissions.CanViewFirstName()
}

// canViewLastName executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) canViewLastName() bool {
	return u.permissions != nil && u.permissions.CanViewLastName()
}

// canViewUsername executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) canViewUsername() bool {
	return u.permissions != nil && u.permissions.CanViewUsername()
}

// canViewAvatarUrl executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (u *userLikeWithPermissions) canViewAvatarUrl() bool {
	return u.permissions != nil && u.permissions.CanViewAvatarUrl()
}

// formatFirstNameLastInitial executes this function as part of the backend conversion flow between persistence, service, and transport models.
func formatFirstNameLastInitial(firstName, lastName string) string {
	first := strings.TrimSpace(firstName)
	if first == "" {
		return ""
	}
	lastInitial := firstRune(strings.TrimSpace(lastName))
	if lastInitial == "" {
		return first
	}
	return first + " " + strings.ToUpper(lastInitial)
}

// formatInitialsOnly executes this function as part of the backend conversion flow between persistence, service, and transport models.
func formatInitialsOnly(firstName, lastName string) string {
	firstInitial := firstRune(strings.TrimSpace(firstName))
	lastInitial := firstRune(strings.TrimSpace(lastName))
	if firstInitial == "" && lastInitial == "" {
		return ""
	}
	initials := firstInitial + lastInitial
	return strings.ToUpper(initials)
}

// firstRune executes this function as part of the backend conversion flow between persistence, service, and transport models.
func firstRune(value string) string {
	if value == "" {
		return ""
	}
	for _, r := range value {
		return string(r)
	}
	return ""
}

// ConvertProtoToUser executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ConvertProtoToUser(user *proto_models.User) (*common_service_models.User, error) {
	if user == nil {
		return nil, fmt.Errorf("cannot convert nil proto user to service user")
	}
	var phoneNumber *phonenumbers.PhoneNumber
	if user.PhoneNumber != nil {
		// Parse the phone number from E.164 format
		var err error
		phoneNumber, err = phonenumbers.Parse(*user.PhoneNumber, "")
		if err != nil {
			return nil, fmt.Errorf("invalid phone number format: %w", err)
		}
	}
	return &common_service_models.User{
		ID:          user.Id,
		Username:    user.Username,
		AvatarUrl:   user.AvatarUrl, // Store object path as is
		UserType:    user.UserType,
		Email:       strings.ToLower(user.Email),
		Banned:      user.Banned,
		Bot:         user.IsBot,
		FirstName:   user.FirstName,
		LastName:    user.LastName,
		PhoneNumber: phoneNumber,
	}, nil
}

// ConvertUserLikeToProtoWithPermissions executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ConvertUserLikeToProtoWithPermissions(user common_service_models.UserLike, permissions *config.UserAttributePermissions) *proto_models.User {
	// First do the standard conversion
	protoUser := h.convertUserToProtoNoPermissions(user)

	// Then apply permission filtering
	return h.FilterProtoUserWithPermissions(protoUser, permissions)
}

// FilterProtoUserWithPermissions executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) FilterProtoUserWithPermissions(user *proto_models.User, permissions *config.UserAttributePermissions) *proto_models.User {
	if user == nil {
		return nil
	}

	// Use zero-value permissions if nil (all fields hidden)
	var perms config.UserAttributePermissions
	if permissions != nil {
		perms = *permissions
	}

	// Create filtered copy with always-visible fields
	filtered := &proto_models.User{
		Id:       user.Id,
		UserType: user.UserType,
		Banned:   user.Banned,
		IsBot:    user.IsBot,
	}

	// Apply attribute filtering based on permissions
	if perms.CanViewEmail() {
		filtered.Email = user.Email
	}
	if perms.CanViewPhoneNumber() {
		filtered.PhoneNumber = user.PhoneNumber
	}
	if perms.CanViewFirstName() {
		filtered.FirstName = user.FirstName
	}
	if perms.CanViewLastName() {
		filtered.LastName = user.LastName
	}
	if perms.CanViewUsername() {
		filtered.Username = user.Username
	}
	if perms.CanViewAvatarUrl() {
		filtered.AvatarUrl = user.AvatarUrl
	}

	return filtered
}

// FilterOrganizationMemberDetailsWithPermissions executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) FilterOrganizationMemberDetailsWithPermissions(memberDetails *proto_models.OrganizationMemberDetails, permissions *config.UserAttributePermissions) *proto_models.OrganizationMemberDetails {
	if memberDetails == nil {
		return nil
	}

	// Use zero-value permissions if nil (all fields hidden)
	var perms config.UserAttributePermissions
	if permissions != nil {
		perms = *permissions
	}

	// Create filtered copy with always-visible fields
	filtered := &proto_models.OrganizationMemberDetails{
		OrganizationId: memberDetails.OrganizationId,
		UserId:         memberDetails.UserId,
		ExplicitRole:   memberDetails.ExplicitRole,
		EffectiveRole:  memberDetails.EffectiveRole,
		CreatedAt:      memberDetails.CreatedAt,
		UpdatedAt:      memberDetails.UpdatedAt,
		IsActive:       memberDetails.IsActive,
		User:           memberDetails.User,
	}

	// Apply attribute filtering based on permissions
	if perms.CanViewLastActivity() {
		filtered.LastActivity = memberDetails.LastActivity
	}

	return filtered
}

// ConvertAdminUserDetailsToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ConvertAdminUserDetailsToProto(ctx context.Context, domain *common_service_models.AdminUserDetails) *proto_models.AdminUserDetails {
	// Use the converter to transform the user to proto format
	userProto := h.ConvertUserLikeToProto(ctx, domain.User)

	return &proto_models.AdminUserDetails{
		User:              userProto,
		CreatedAt:         timestamppb.New(domain.CreatedAt),
		UpdatedAt:         timestamppb.New(domain.UpdatedAt),
		LastLogin:         timestampOrNil(domain.LastLogin),
		TotalLogins:       uint32(domain.TotalLogins),
		ActiveSessions:    uint32(domain.ActiveSessions),
		AuthMethods:       domain.AuthMethods,
		TotalAttachments:  domain.TotalAttachments,
		StorageUsedBytes:  domain.StorageUsedBytes,
		EmailVerified:     domain.EmailVerified,
		InvitationPending: domain.InvitationPending,
	}
}

// ConvertAdminSessionTokenToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ConvertAdminSessionTokenToProto(ctx context.Context, domain *common_service_models.AdminSessionToken) *proto_models.AdminSessionToken {
	if domain == nil {
		return nil
	}

	var lastUsedAt *timestamppb.Timestamp
	if domain.LastUsedAt != nil {
		lastUsedAt = timestamppb.New(*domain.LastUsedAt)
	}

	var user *proto_models.User
	if domain.User != nil {
		user = h.ConvertUserLikeToProto(ctx, domain.User)
	}

	return &proto_models.AdminSessionToken{
		TokenId:    fmt.Sprintf("%d", domain.TokenID),
		UserId:     fmt.Sprintf("%d", domain.UserID),
		TokenType:  domain.TokenType,
		Status:     domain.Status,
		CreatedAt:  timestamppb.New(domain.CreatedAt),
		ExpiresAt:  timestamppb.New(domain.ExpiresAt),
		LastUsedAt: lastUsedAt,
		IpAddress:  domain.IPAddress,
		UserAgent:  domain.UserAgent,
		User:       user,
	}
}

// PersistenceUserAuthMethodsToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceUserAuthMethodsToService(pUserAuthMethods *common_persistence_models.UserAuthMethods) *common_service_models.UserAuthMethods {
	if pUserAuthMethods == nil {
		return nil
	}

	// Convert OAuthLogin slice using the converter
	serviceOAuthProviders := h.PersistenceOAuthLoginsToService(pUserAuthMethods.OAuthProviders)

	return &common_service_models.UserAuthMethods{
		UserID:         pUserAuthMethods.UserID,
		HasPassword:    pUserAuthMethods.HasPassword,
		OAuthProviders: serviceOAuthProviders,
		OTPEnabled:     pUserAuthMethods.OTPEnabled,
		OTPVerifiedAt:  pUserAuthMethods.OTPVerifiedAt,
	}
}

// PersistenceOAuthLoginToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceOAuthLoginToService(pOAuthLogin *common_persistence_models.OAuthLogin) *common_service_models.OAuthLogin {
	if pOAuthLogin == nil {
		return nil
	}

	return &common_service_models.OAuthLogin{
		ID:           pOAuthLogin.ID,
		UserID:       pOAuthLogin.UserID,
		ProviderSlug: pOAuthLogin.ProviderSlug,
		Email:        pOAuthLogin.Email,
		LoginCount:   pOAuthLogin.LoginCount,
		FirstLogin:   pOAuthLogin.FirstLogin,
		LastLogin:    pOAuthLogin.LastLogin,
	}
}

// PersistenceOAuthLoginsToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceOAuthLoginsToService(pOAuthLogins []common_persistence_models.OAuthLogin) []common_service_models.OAuthLogin {
	if pOAuthLogins == nil {
		return nil
	}

	return common_util.ApplyToEach(pOAuthLogins, func(pOAuthLogin common_persistence_models.OAuthLogin) common_service_models.OAuthLogin {
		return *h.PersistenceOAuthLoginToService(&pOAuthLogin)
	})
}
