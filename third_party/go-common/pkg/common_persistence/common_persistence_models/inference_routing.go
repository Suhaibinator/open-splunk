package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

const (
	TableNameInferenceOperationModelOptions         = "gocommon_inference_operation_model_options"
	TableNameOrganizationInferencePolicies          = "gocommon_organization_inference_policies"
	TableNameOrganizationInferenceOperationPolicies = "gocommon_organization_inference_operation_policies"
	TableNameOrganizationInferenceModelOptions      = "gocommon_organization_inference_model_options"
	TableNameOrganizationInferenceProviders         = "gocommon_organization_inference_providers"
)

// InferenceOperationModelOption is a site-admin model choice for one operation.
// An empty collection locks the operation to its route's default model.
type InferenceOperationModelOption struct {
	Operation        int32                                       `json:"operation" gorm:"type:integer;primaryKey;autoIncrement:false"`
	InferenceModelID uint64                                      `json:"inference_model_id" gorm:"primaryKey;autoIncrement:false"`
	PayerEligibility proto_models.InferenceModelPayerEligibility `json:"payer_eligibility" gorm:"type:smallint;not null"`
	CreatedAt        time.Time                                   `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
	UpdatedAt        time.Time                                   `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

func (InferenceOperationModelOption) TableName() string {
	return TableNameInferenceOperationModelOptions
}

// OrganizationInferenceOperationPolicy only affects work owned by this org.
// Closing the operation's choice gate parks this row without deleting it.
// MemberRetarget applies only under the operation's ORG_DECIDES re-target gate;
// under any other gate it is parked: kept but ignored.
type OrganizationInferenceOperationPolicy struct {
	OrganizationID int64     `json:"organization_id" gorm:"primaryKey;autoIncrement:false"`
	Operation      int32     `json:"operation" gorm:"type:integer;primaryKey;autoIncrement:false"`
	DefaultModelID *uint64   `json:"default_model_id,omitempty"`
	MemberChoice   bool      `json:"member_choice" gorm:"not null;default:false"`
	MemberRetarget bool      `json:"member_retarget" gorm:"not null;default:false"`
	Version        uint64    `json:"version" gorm:"not null;default:0"`
	UpdaterUserID  uint64    `json:"updater_user_id" gorm:"not null"`
	CreatedAt      time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
	UpdatedAt      time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

func (OrganizationInferenceOperationPolicy) TableName() string {
	return TableNameOrganizationInferenceOperationPolicies
}

// OrganizationInferenceModelOption narrows the site's operation list. No child
// rows means the entire site list, rather than an empty eligible set.
type OrganizationInferenceModelOption struct {
	OrganizationID   int64  `json:"organization_id" gorm:"primaryKey;autoIncrement:false"`
	Operation        int32  `json:"operation" gorm:"type:integer;primaryKey;autoIncrement:false"`
	InferenceModelID uint64 `json:"inference_model_id" gorm:"primaryKey;autoIncrement:false"`
}

func (OrganizationInferenceModelOption) TableName() string {
	return TableNameOrganizationInferenceModelOptions
}

// OrganizationInferencePolicy versions the org-wide provider restriction.
type OrganizationInferencePolicy struct {
	OrganizationID int64     `json:"organization_id" gorm:"primaryKey;autoIncrement:false"`
	Version        uint64    `json:"version" gorm:"not null;default:0"`
	UpdaterUserID  uint64    `json:"updater_user_id" gorm:"not null"`
	CreatedAt      time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
	UpdatedAt      time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

func (OrganizationInferencePolicy) TableName() string { return TableNameOrganizationInferencePolicies }

// OrganizationInferenceProvider restricts the org and all its descendants.
// No rows means this organization adds no provider restriction.
type OrganizationInferenceProvider struct {
	OrganizationID int64  `json:"organization_id" gorm:"primaryKey;autoIncrement:false"`
	ProviderID     string `json:"provider_id" gorm:"type:varchar(128);primaryKey"`
}

func (OrganizationInferenceProvider) TableName() string {
	return TableNameOrganizationInferenceProviders
}

// InferenceRoutingSnapshot is captured by one PostgreSQL statement. Model policy
// and provider rows include inactive and parked settings so resolution can report
// why a choice is blocked rather than silently widening it.
type InferenceRoutingSnapshot struct {
	Routes                        []InferenceOperationRoute              `json:"routes"`
	Options                       []InferenceOperationModelOption        `json:"options"`
	Models                        []InferenceModel                       `json:"models"`
	RateLimits                    []InferenceModelRateLimitPolicy        `json:"rate_limits"`
	OrganizationPolicies          []OrganizationInferencePolicy          `json:"organization_policies"`
	OrganizationOperationPolicies []OrganizationInferenceOperationPolicy `json:"organization_operation_policies"`
	OrganizationModelOptions      []OrganizationInferenceModelOption     `json:"organization_model_options"`
	ProviderAllowlist             []OrganizationInferenceProvider        `json:"provider_allowlist"`
	OrganizationIDs               []int64                                `json:"organization_ids"`
}

// InferenceRoutingSessionCount is a bounded count of active pins for one model.
// Complete is false if the global sample was truncated or an operation was
// unknown for any active session in the preview's organizations.
type InferenceRoutingSessionCount struct {
	MemberChoiceSelection bool                                  `json:"member_choice_selection"`
	SiteOverride          bool                                  `json:"site_override"`
	InferenceAPI          proto_models.InferenceAPI             `json:"inference_api"`
	Operation             int32                                 `json:"operation"`
	InferenceModelID      uint64                                `json:"inference_model_id"`
	SelectionSource       proto_models.InferenceSelectionSource `json:"selection_source"`
	Count                 uint64                                `json:"count"`
	Complete              bool                                  `json:"complete"`
}

// InferenceRoutingPreviewSnapshot carries one affected organization's consistent
// routing state and its bounded active-session counts.
type InferenceRoutingPreviewSnapshot struct {
	OrganizationID int64                          `json:"organization_id"`
	Snapshot       InferenceRoutingSnapshot       `json:"snapshot"`
	SessionCounts  []InferenceRoutingSessionCount `json:"session_counts"`
}
