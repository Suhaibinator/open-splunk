package chat

import (
	"context"
	json "encoding/json/v2"
	"fmt"
	"strings"
	"unicode/utf8"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
)

func validateAgentPromptCustomization(custom *ports.AgentPromptCustomization) error {
	if custom == nil {
		return nil
	}
	if (custom.Mode != ports.AgentPromptAppend && custom.Mode != ports.AgentPromptReplace) ||
		!utf8.ValidString(custom.Text) || strings.TrimSpace(custom.Text) == "" {
		return serviceErrors.ErrInvalidRequest
	}
	return nil
}

func matchesAgentPromptCustomization(raw []byte, custom *ports.AgentPromptCustomization) bool {
	if len(raw) == 0 {
		return custom == nil
	}
	var stored ports.AgentPromptCustomization
	return custom != nil && json.Unmarshal(raw, &stored) == nil && stored == *custom
}

func resolveAgentSessionPrompt(ctx context.Context, session *models.ChatAgentSession, agent ports.AgentDefinition, custom *ports.AgentPromptCustomization, metadata map[string]string) (string, error) {
	if err := validateAgentPromptCustomization(custom); err != nil {
		return "", err
	}
	var prompt string
	if custom != nil && custom.Mode == ports.AgentPromptReplace {
		prompt = custom.Text
	} else {
		prompt = agent.SystemPrompt
		if agent.SystemPromptBuilder != nil {
			input := ports.AgentSystemPromptInput{
				OrganizationID: sm.OrganizationIdType(session.OrganizationID), AgentType: session.AgentType,
				ChannelID: session.ChannelID, ParentMessageID: session.ParentMessageID, AgentSessionID: session.ID,
				ActorUserID: sm.UserIdType(session.CreatedByUserID), Metadata: common_util.CloneMapOrNil(metadata),
			}
			if session.ContextID != nil {
				input.ContextID = *session.ContextID
			}
			if session.ContextEntityID != nil {
				input.ContextEntityID = *session.ContextEntityID
			}
			var err error
			prompt, err = agent.SystemPromptBuilder(ctx, input)
			if err != nil {
				return "", fmt.Errorf("resolve session system prompt: %w", err)
			}
		}
		if custom != nil {
			prompt += "\n\n" + custom.Text
		}
	}
	if !utf8.ValidString(prompt) || strings.TrimSpace(prompt) == "" {
		return "", serviceErrors.ErrInvalidRequest
	}
	return prompt, nil
}

func (h *Handler[U]) finalizeAgentSessionPrompt(ctx context.Context, session *models.ChatAgentSession, agent ports.AgentDefinition, custom *ports.AgentPromptCustomization, metadata map[string]string) error {
	prompt, err := resolveAgentSessionPrompt(ctx, session, agent, custom, metadata)
	if err != nil {
		return err
	}
	var raw []byte
	if custom != nil {
		raw, err = json.Marshal(custom)
		if err != nil {
			return fmt.Errorf("encode prompt customization: %w", err)
		}
	}
	if err := h.persistenceHandler.FinalizeChatAgentSessionPrompt(ctx, session.ID, prompt, raw); err != nil {
		return fmt.Errorf("persist session system prompt: %w", err)
	}
	session.EffectiveSystemPrompt, session.PromptCustomizationJSON = prompt, raw
	return nil
}
