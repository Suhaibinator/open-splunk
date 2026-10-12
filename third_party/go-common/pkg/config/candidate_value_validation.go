package config

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_util/origin"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/nyaruka/phonenumbers"
)

// Keep checks in Config declaration order. The authorization inventory tests
// exercise every enum field, including fields added in the future.
func (c *Config) validateAuthorizationEnums() error {
	checks := []func() error{
		userType("UserSearchMinUserType", c.UserSearchMinUserType),
		roleSlice("UserSearchMinOrgRole", c.UserSearchMinOrgRole, true),
		userType("SiteMinUserTypeAdminAccess", c.SiteMinUserTypeAdminAccess),
		userType("SiteMinUserTypeUserListing", c.SiteMinUserTypeUserListing),
		userType("SiteMinUserTypeUserViewing", c.SiteMinUserTypeUserViewing),
		userType("SiteMinUserTypeUserBanning", c.SiteMinUserTypeUserBanning),
		userType("SiteMinUserTypeUserModification", c.SiteMinUserTypeUserModification),
		userType("SiteMinUserTypeUserDeletion", c.SiteMinUserTypeUserDeletion),
		userType("SiteMinUserTypeUserTypeChange", c.SiteMinUserTypeUserTypeChange),
		userType("SiteMinUserTypeSystemAccess", c.SiteMinUserTypeSystemAccess),
		userType("SiteMinUserTypeContentModeration", c.SiteMinUserTypeContentModeration),
		userType("SiteMinUserTypeAttachmentAccess", c.SiteMinUserTypeAttachmentAccess),
		userType("SiteMinUserTypeAttachmentModification", c.SiteMinUserTypeAttachmentModification),
		userType("SiteMinUserTypeAnalyticsAccess", c.SiteMinUserTypeAnalyticsAccess),
		userType("SiteMinUserTypeAdminCreateOrganization", c.SiteMinUserTypeAdminCreateOrganization),
		userType("SiteMinUserTypeAdminListOrganizations", c.SiteMinUserTypeAdminListOrganizations),
		userType("SiteMinUserTypeAdminManageOrganizationMembers", c.SiteMinUserTypeAdminManageOrganizationMembers),
		userType("SiteMinUserTypeAdminGetOrganizationMember", c.SiteMinUserTypeAdminGetOrganizationMember),
		userType("SiteMinUserTypeAdminChatChannels", c.SiteMinUserTypeAdminChatChannels),
		userType("SiteMinUserTypeCareersManagement", c.SiteMinUserTypeCareersManagement),
		userType("SiteMinUserTypeChatToolApproval", c.SiteMinUserTypeChatToolApproval),
		userType("SiteMinUserTypeInferenceViewing", c.SiteMinUserTypeInferenceViewing),
		userType("SiteMinUserTypeInferenceModelOverride", c.SiteMinUserTypeInferenceModelOverride),
		func() error {
			if c.SiteMinUserTypeInferenceModelOverride == nil {
				return nil
			}
			return ValidateInferenceModelOverrideThreshold(*c.SiteMinUserTypeInferenceModelOverride)
		},
		userType("SiteMinUserTypeInferenceAdministration", c.SiteMinUserTypeInferenceAdministration),
		roleSlice("DefaultOrgCreatorRole", c.DefaultOrgCreatorRole, false),
		orgType("PersonalWorkspaceOrganizationType", c.PersonalWorkspaceOrganizationType),
		orgRole("PersonalWorkspaceOwnerRole", c.PersonalWorkspaceOwnerRole),
		roleSlice("OrganizationAutoAssociationDefaultRole", c.OrganizationAutoAssociationDefaultRole, false),
		userType("OrgMinUserTypeManageEmailDomains", c.OrgMinUserTypeManageEmailDomains),
		roleSlice("OrgMinRoleManageEmailDomains", c.OrgMinRoleManageEmailDomains, false),
		userType("DefaultUserType", c.DefaultUserType),
		orgType("DefaultOrganizationType", c.DefaultOrganizationType),
		userType("OrgMinUserTypeCreate", c.OrgMinUserTypeCreate),
		userType("OrgMinUserTypeCreateChild", c.OrgMinUserTypeCreateChild),
		userType("OrgMinUserTypeView", c.OrgMinUserTypeView),
		roleSlice("OrgMinRoleView", c.OrgMinRoleView, true),
		userType("OrgMinUserTypeListMembers", c.OrgMinUserTypeListMembers),
		orgType("OrgMinTypeListMembers", c.OrgMinTypeListMembers),
		roleSlice("OrgMinRoleListMembers", c.OrgMinRoleListMembers, false),
		userType("OrgMinUserTypeDelete", c.OrgMinUserTypeDelete),
		userType("OrgMinUserTypeUpdate", c.OrgMinUserTypeUpdate),
		roleSlice("OrgMinRoleUpdate", c.OrgMinRoleUpdate, false),
		roleSlice("OrgMinRoleDelete", c.OrgMinRoleDelete, false),
		userType("OrgMinUserTypeInviteMembers", c.OrgMinUserTypeInviteMembers),
		orgType("OrgMinTypeInviteMembers", c.OrgMinTypeInviteMembers),
		userType("OrgMinUserTypeAddMembers", c.OrgMinUserTypeAddMembers),
		orgType("OrgMinTypeAddMembers", c.OrgMinTypeAddMembers),
		roleSlice("OrgMinRoleInviteMembers", c.OrgMinRoleInviteMembers, false),
		roleSlice("OrgMinRoleAddMembers", c.OrgMinRoleAddMembers, false),
		userType("OrgMinUserTypeRemoveMembers", c.OrgMinUserTypeRemoveMembers),
		orgType("OrgMinTypeRemoveMembers", c.OrgMinTypeRemoveMembers),
		roleSlice("OrgMinRoleRemoveMembers", c.OrgMinRoleRemoveMembers, false),
		userType("OrgMinUserTypeUpdateMemberRoles", c.OrgMinUserTypeUpdateMemberRoles),
		orgType("OrgMinTypeUpdateMemberRoles", c.OrgMinTypeUpdateMemberRoles),
		roleSlice("OrgMinRoleUpdateMemberRoles", c.OrgMinRoleUpdateMemberRoles, false),
		userType("OrgMinUserTypeCancelInvitation", c.OrgMinUserTypeCancelInvitation),
		orgType("OrgMinTypeCancelInvitation", c.OrgMinTypeCancelInvitation),
		roleSlice("OrgMinRoleCancelInvitation", c.OrgMinRoleCancelInvitation, false),
		userType("OrgMinUserTypeViewInvitations", c.OrgMinUserTypeViewInvitations),
		orgType("OrgMinTypeViewInvitations", c.OrgMinTypeViewInvitations),
		roleSlice("OrgMinRoleViewInvitations", c.OrgMinRoleViewInvitations, false),
		orgType("OrgMinTypeManageEmailDomains", c.OrgMinTypeManageEmailDomains),
		orgType("OrgMinTypeWorkspaceNavigation", c.OrgMinTypeWorkspaceNavigation),
		orgType("OrgMinTypeCreateChildOrganizations", c.OrgMinTypeCreateChildOrganizations),
		orgType("OrgMinTypeBulkMemberManagement", c.OrgMinTypeBulkMemberManagement),
		roleSlice("OrgMinRoleCreateChild", c.OrgMinRoleCreateChild, false),
		roleSlice("OrgMinRoleSelfJoin", c.OrgMinRoleSelfJoin, false),
		roleSlice("OrgMinRoleViewInferencePolicy", c.OrgMinRoleViewInferencePolicy, false),
		roleSlice("OrgMinRoleManageInferencePolicy", c.OrgMinRoleManageInferencePolicy, false),
		roleSlice("OrgMinRoleChooseInferenceModel", c.OrgMinRoleChooseInferenceModel, false),
		roleSlice("OrgMinRoleReadAgentTranscript", c.OrgMinRoleReadAgentTranscript, true),
	}
	for _, check := range checks {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func userType(name string, value *proto_models.UserType) func() error {
	return func() error { return validateEnumField(name, "UserType", value, proto_models.UserType_name) }
}

func orgType(name string, value *proto_models.OrganizationType) func() error {
	return func() error {
		return validateEnumField(name, "OrganizationType", value, proto_models.OrganizationType_name)
	}
}

func orgRole(name string, value *proto_models.OrganizationRole) func() error {
	return func() error {
		return validateEnumField(name, "OrganizationRole", value, proto_models.OrganizationRole_name)
	}
}

func roleSlice(name string, values []proto_models.OrganizationRole, allowUnspecified bool) func() error {
	return func() error { return validateOrganizationRoleSlice(name, values, allowUnspecified) }
}

// validateEnumField rejects unknown and unspecified (zero) values; nil means unset.
func validateEnumField[T ~int32](name, kind string, value *T, names map[int32]string) error {
	if value == nil {
		return nil
	}
	if _, known := names[int32(*value)]; !known {
		return fmt.Errorf("%s must be a known %s, got: %d", name, kind, *value)
	}
	if *value == 0 {
		return fmt.Errorf("%s must be a specified %s", name, kind)
	}
	return nil
}

func validateOrganizationRoleSlice(name string, values []proto_models.OrganizationRole, allowUnspecified bool) error {
	for depth, value := range values {
		if _, known := proto_models.OrganizationRole_name[int32(value)]; !known {
			return fmt.Errorf("%s[%d] must be a known OrganizationRole, got: %d", name, depth, value)
		}
		if value == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED && !allowUnspecified {
			return fmt.Errorf("%s[%d] must be a specified OrganizationRole", name, depth)
		}
	}
	return nil
}

func validatePhoneNumberDefaultRegion(region string) error {
	if len(region) != 2 || region[0] < 'A' || region[0] > 'Z' || region[1] < 'A' || region[1] > 'Z' {
		return fmt.Errorf("phone number default region must be a canonical uppercase two-letter country code, got: %q", region)
	}
	if phonenumbers.GetCountryCodeForRegion(region) == 0 {
		return fmt.Errorf("phone number default region must identify a geographic calling region, got: %q", region)
	}
	return nil
}

func validateRealtimeWebSocketOrigins(origins []string) error {
	for index, configuredOrigin := range origins {
		if configuredOrigin == "*" {
			continue
		}
		if _, err := origin.Normalize(configuredOrigin); err != nil {
			return fmt.Errorf("RealtimeWSAllowedOrigins[%d] must be '*' or a valid HTTP(S) origin: %w", index, err)
		}
	}
	return nil
}

// Code-owned fallbacks for the realtime keepalive invariant. A candidate generation may leave keys
// unset that GetDefaults would supply, so the invariant is evaluated against the values the hub
// would actually run with rather than skipped whenever a key is absent.
//
// defaultRealtimeWSCommandAuthBudget must match common_ws.DefaultCommandAuthBudget, which is the
// fallback the hub actually applies. This package cannot import common_ws (the dependency runs the
// other way), so the value is duplicated here deliberately; if the two drift, this validator judges
// candidates against a budget nobody spends.
const (
	defaultRealtimeWSPingInterval      = 30 * time.Second
	defaultRealtimeWSPongTimeout       = 10 * time.Second
	defaultRealtimeWSCommandAuthBudget = 10 * time.Second
)

// validateRealtimeCommandAuthBudget enforces that one subscribe/unsubscribe command cannot occupy
// the read goroutine for longer than the keepalive window. Topic authorizers run synchronously on
// that goroutine and gorilla dispatches pong control frames from inside ReadMessage, so a command
// budget at or above PingInterval + PongTimeout lets the ping loop disconnect the very connection
// it is serving.
//
// All three keys are hot and each handshake captures them together, so this check is about the
// generation as a whole: it rejects any candidate whose own three values are incoherent. Because a
// connection never mixes generations, a candidate that passes here cannot break a live connection —
// which is exactly why the budget is hot rather than restart-bound.
func validateRealtimeCommandAuthBudget(commandAuthBudget, pingInterval, pongTimeout *time.Duration) error {
	ping := durationOrDefault(pingInterval, defaultRealtimeWSPingInterval)
	if ping <= 0 {
		// Pinging disabled: nothing times the connection out while an authorizer runs.
		return nil
	}
	keepalive := ping + durationOrDefault(pongTimeout, defaultRealtimeWSPongTimeout)
	budget := durationOrDefault(commandAuthBudget, defaultRealtimeWSCommandAuthBudget)
	if budget >= keepalive {
		return fmt.Errorf(
			"realtime websocket command auth budget (%v) must be shorter than the keepalive budget of ping interval + pong timeout (%v)",
			budget, keepalive,
		)
	}
	return nil
}

func durationOrDefault(value *time.Duration, fallback time.Duration) time.Duration {
	if value == nil {
		return fallback
	}
	return *value
}

func validateSanitizerDomains(name string, domains []string) error {
	for index, domain := range domains {
		if !isSanitizerDomain(domain) {
			return fmt.Errorf("%s[%d] must be a valid email domain, got: %q", name, index, domain)
		}
	}
	return nil
}

func isSanitizerDomain(domain string) bool {
	if domain == "" || strings.TrimSpace(domain) != domain || len(domain) > 253 {
		return false
	}
	if strings.HasPrefix(domain, "[") && strings.HasSuffix(domain, "]") {
		address := domain[1 : len(domain)-1]
		address = strings.TrimPrefix(strings.ToLower(address), "ipv6:")
		return net.ParseIP(address) != nil
	}
	for label := range strings.SplitSeq(domain, ".") {
		if len(label) == 0 || len(label) > 63 || !isDomainLetterOrDigit(label[0]) || !isDomainLetterOrDigit(label[len(label)-1]) {
			return false
		}
		for index := 1; index < len(label)-1; index++ {
			if !isDomainLetterOrDigit(label[index]) && label[index] != '-' {
				return false
			}
		}
	}
	return true
}

func isDomainLetterOrDigit(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

// validateLivePersistenceAndAuthPolicy prevents candidate values from turning
// expiring authorization data into permanent entries or overflowing DB budgets.
func (c *Config) validateLivePersistenceAndAuthPolicy() error {
	for _, field := range []struct {
		name  string
		value *time.Duration
	}{
		{"SessionValidity", c.SessionValidity}, {"VerificationTokenValidity", c.VerificationTokenValidity},
	} {
		if field.value != nil && *field.value <= 0 {
			return fmt.Errorf("%s must be positive", field.name)
		}
	}
	if c.AuthCacheTtl != nil && *c.AuthCacheTtl < 0 {
		return fmt.Errorf("AuthCacheTtl must be nonnegative")
	}
	const maxDuration = time.Duration(1<<63 - 1)
	for _, field := range []struct {
		name  string
		value *int
	}{
		{"DBConnMaxLifetime", c.DBConnMaxLifetime}, {"DBConnMaxIdleTime", c.DBConnMaxIdleTime},
	} {
		if field.value != nil && int64(*field.value) > int64(maxDuration/time.Second) {
			return fmt.Errorf("%s exceeds the supported duration", field.name)
		}
	}
	if c.DBQueryTimeout != nil && *c.DBQueryTimeout > maxDuration/2 {
		return fmt.Errorf("DBQueryTimeout exceeds the supported cleanup budget")
	}
	return nil
}

// ValidateInferenceModelOverrideThreshold limits overrides to defined administrator tiers.
func ValidateInferenceModelOverrideThreshold(value proto_models.UserType) error {
	name, known := proto_models.UserType_name[int32(value)]
	if !known || (!strings.HasPrefix(name, "USER_TYPE_ADMIN_") && !strings.HasPrefix(name, "USER_TYPE_SUPER_ADMIN")) {
		return fmt.Errorf("SiteMinUserTypeInferenceModelOverride must be a defined administrator UserType, got %d", value)
	}
	return nil
}
