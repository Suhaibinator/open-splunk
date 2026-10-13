package common_persistence_models

import (
	"fmt"
	"time"

	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ChatAgentSessionInferencePin captures the resolved execution identity and policy generation.
// A wholly empty pin belongs to a legacy session; partial pins never fall back to live routing.
type ChatAgentSessionInferencePin struct {
	InferenceOperation        int32                      `json:"inference_operation" gorm:"not null;default:0"`
	ExecutionInferenceModelID *uint64                    `json:"execution_inference_model_id,omitempty"`
	ExecutionInferenceAPI     *proto_models.InferenceAPI `json:"execution_inference_api,omitempty"`
	ExecutionProviderID       *string                    `json:"execution_provider_id,omitempty" gorm:"type:varchar(255)"`
	ExecutionProviderModelID  *string                    `json:"execution_provider_model_id,omitempty" gorm:"type:varchar(255)"`
	// MemberChoiceSelection retains choice authority after the source becomes RETARGET.
	MemberChoiceSelection              bool                                  `json:"member_choice_selection" gorm:"not null;default:false"`
	SelectionSource                    proto_models.InferenceSelectionSource `json:"selection_source" gorm:"type:smallint;not null;default:0"`
	SiteOverrideUserID                 *uint64                               `json:"site_override_user_id,omitempty"`
	SelectingUserID                    *uint64                               `json:"selecting_user_id,omitempty"`
	OperationListVersion               uint64                                `json:"operation_list_version" gorm:"not null;default:0"`
	OrganizationPolicyVersion          uint64                                `json:"organization_policy_version" gorm:"not null;default:0"`
	OrganizationOperationPolicyVersion uint64                                `json:"organization_operation_policy_version" gorm:"not null;default:0"`
	InferencePinVersion                uint64                                `json:"inference_pin_version" gorm:"not null;default:0"`
}

// IsEmpty reports the wholly empty pin of a legacy session.
func (p ChatAgentSessionInferencePin) IsEmpty() bool {
	return p == ChatAgentSessionInferencePin{}
}

// ExecutionModelSelection validates completeness and returns an independent normalized selection.
// A wholly empty pin has no selection.
func (p ChatAgentSessionInferencePin) ExecutionModelSelection() (*inference.ModelSelection, error) {
	if p.IsEmpty() {
		return nil, nil
	}
	if p.InferenceOperation == 0 || p.ExecutionInferenceModelID == nil || p.ExecutionInferenceAPI == nil || p.ExecutionProviderID == nil || p.ExecutionProviderModelID == nil || p.InferencePinVersion == 0 {
		return nil, fmt.Errorf("incomplete execution model pin")
	}
	if p.SelectionSource == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_UNSPECIFIED {
		return nil, fmt.Errorf("invalid inference selection source")
	}
	if *p.ExecutionInferenceAPI == 0 || *p.ExecutionProviderID == "" || *p.ExecutionProviderModelID == "" || (p.SelectingUserID != nil && *p.SelectingUserID == 0) {
		return nil, fmt.Errorf("invalid execution model pin")
	}
	if p.MemberChoiceSelection && p.SelectionSource != proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE && p.SelectionSource != proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET {
		return nil, fmt.Errorf("invalid member choice provenance")
	}
	if p.SiteOverrideUserID != nil {
		if *p.SiteOverrideUserID == 0 || (p.SelectionSource != proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE && p.SelectionSource != proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET) {
			return nil, fmt.Errorf("invalid site override provenance")
		}
	}
	if p.SelectionSource == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE && (p.SiteOverrideUserID == nil || p.SelectingUserID == nil || *p.SiteOverrideUserID != *p.SelectingUserID) {
		return nil, fmt.Errorf("site override pin requires matching override actor")
	}
	switch p.SelectionSource {
	case proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE, proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE, proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET:
		if p.SelectingUserID == nil {
			return nil, fmt.Errorf("execution model pin requires a selecting user")
		}
	}
	selected := &inference.ModelSelection{InferenceModelID: *p.ExecutionInferenceModelID, API: p.ExecutionInferenceAPI, SelectionSource: p.SelectionSource}
	if err := selected.Validate(); err != nil {
		return nil, err
	}
	return selected.Clone(), nil
}

// ExecutionModelSelection validates the session's pin; see ChatAgentSessionInferencePin.ExecutionModelSelection.
func (s *ChatAgentSession) ExecutionModelSelection() (*inference.ModelSelection, error) {
	if s == nil {
		return nil, nil
	}
	selected, err := s.ChatAgentSessionInferencePin.ExecutionModelSelection()
	if err != nil {
		return nil, fmt.Errorf("session %d: %w", s.ID, err)
	}
	return selected, nil
}

// ValidateExecutionModel compares the pinned catalog identity and effective adapter.
func (s *ChatAgentSession) ValidateExecutionModel(id uint64, provider, model string, api proto_models.InferenceAPI) error {
	selected, err := s.ExecutionModelSelection()
	if err != nil || selected == nil {
		return err
	}
	if selected.InferenceModelID != id || *selected.API != api || *s.ExecutionProviderID != provider || *s.ExecutionProviderModelID != model {
		return fmt.Errorf("execution model identity changed for session %d", s.ID)
	}
	return nil
}

// IsInferenceBlocked reports a durable dispatch block independently of archival and Stop.
func (s *ChatAgentSession) IsInferenceBlocked() bool {
	return s != nil && s.InferenceBlockedReason != proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_UNSPECIFIED
}

// ChatAgentSessionInferenceRetarget is an append-only audit of an explicit execution change.
// OldPin can be empty when an owner retargets a legacy session before its first drive.
type ChatAgentSessionInferenceRetarget struct {
	ID             uint64                       `gorm:"primaryKey;autoIncrement"`
	AgentSessionID uint64                       `gorm:"not null;index:idx_chat_agent_session_inference_retargets_session,priority:1;uniqueIndex:idx_chat_agent_session_inference_retargets_version,priority:1"`
	ActorUserID    uint64                       `gorm:"not null"`
	OldPin         ChatAgentSessionInferencePin `gorm:"embedded;embeddedPrefix:old_"`
	NewPin         ChatAgentSessionInferencePin `gorm:"embedded;embeddedPrefix:new_"`
	// Version is the resulting pin version, unique within the session.
	Version   uint64    `gorm:"not null;uniqueIndex:idx_chat_agent_session_inference_retargets_version,priority:2"`
	CreatedAt time.Time `gorm:"type:timestamptz;autoCreateTime;index:idx_chat_agent_session_inference_retargets_session,priority:2"`
}

func (ChatAgentSessionInferenceRetarget) TableName() string {
	return "gocommon_chat_agent_session_inference_retargets"
}
