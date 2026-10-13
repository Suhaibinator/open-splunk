package chat

import (
	"context"
	"errors"
	"fmt"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// CreateWorkflowChannelInput describes a channel an application workflow creates for its
// transcript before any agent session exists, for example one channel whose threads each host a
// session placed with CreateAgentChatSessionInLocation.
type CreateWorkflowChannelInput struct {
	Name               string
	Description        string
	ContextID          *uint64
	ContextDisplayName *string
	ContextEntityID    *uint64
	// Features are the channel's exact feature flags; the ChannelFeatures hook is not consulted.
	// Each flag must be on in configuration (chat_support_* / chat_allow_*), or the create fails
	// with ErrInvalidRequest before any write. A channel that will host threaded agent sessions
	// needs ChatChannelSupportThreads.
	Features common_persistence_models.ChatChannelFeatures
	// OrganizationAccess, when non-nil, lets effective members of its organization at MinRoleRead
	// or above read the channel without participating, as OrganizationReadMinRole does for an
	// agent session's channel. An UNSPECIFIED MinRoleRead takes the org_min_role_read_agent_transcript
	// entry at the organization's hierarchy depth and is rejected when that entry is UNSPECIFIED too.
	// The actor need not belong to the organization.
	OrganizationAccess *common_service_models.ChatChannelOrganizationAccess
}

// CreateWorkflowChannel creates a group channel owned by the context actor, its only participant.
// It is a server-side Go service method with no HTTP/proto API: the application validates its own
// domain permissions, and its authority in the access organization, before calling. Unlike
// CreateChannel it stores the exact feature flags and writes organization access in the same
// transaction without requiring the actor to be an organization member, so a service bot can own
// a transcript that organization staff read. Authorize is consulted with
// ChatPermissionCreateChannel and ProposedOrganizationID set to the access organization (zero
// without access). Requires an ambient transaction.
func (h *Handler[U]) CreateWorkflowChannel(ctx context.Context, input CreateWorkflowChannelInput) (*common_service_models.ChatChannel, error) {
	limits := h.policy()
	if !limits.EnableChat() {
		return nil, serviceErrors.ErrChatDisabled
	}
	name, description, err := newChannelText(limits, input.Name, input.Description, input.ContextDisplayName)
	if err != nil {
		return nil, err
	}
	if disabled := input.Features &^ h.configuredChannelFeatures(); disabled != 0 {
		return nil, fmt.Errorf("%w: workflow channel features %d are disabled by chat configuration", serviceErrors.ErrInvalidRequest, disabled)
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}
	var access *common_service_models.ChatChannelOrganizationAccess
	if input.OrganizationAccess != nil {
		if h.organizationRoleResolver == nil {
			// Without a resolver the access could never grant a read; fail loudly instead of silently.
			return nil, fmt.Errorf("%w: channel organization access requires organizations to be enabled", serviceErrors.ErrInvalidRequest)
		}
		resolved := *input.OrganizationAccess
		if resolved.OrganizationID > 0 {
			if resolved.MinRoleRead, err = h.organizationReadMinRoleOrDefault(ctx, limits.OrgMinRoleReadAgentTranscript(), resolved.OrganizationID, resolved.MinRoleRead); err != nil {
				return nil, err
			}
		}
		if err := validateChannelOrganizationAccess(&resolved); err != nil {
			return nil, err
		}
		access = &resolved
	}
	var organizationID common_service_models.OrganizationIdType
	if access != nil {
		organizationID = access.OrganizationID
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:                  ports.ChatPermissionCreateChannel,
		Actor:                      actor,
		ChannelType:                proto_models.ChannelType_CHANNEL_TYPE_GROUP,
		ProposedChannelName:        &name,
		ProposedChannelDescription: description,
		ProposedContextID:          input.ContextID,
		ProposedContextDisplayName: input.ContextDisplayName,
		ProposedContextEntityID:    input.ContextEntityID,
		ProposedOrganizationID:     organizationID,
	}); err != nil {
		return nil, err
	}

	created, err := h.persistenceHandler.CreateChatChannel(
		ctx,
		actorID,
		name,
		description,
		input.ContextID,
		input.ContextDisplayName,
		input.ContextEntityID,
		proto_models.ChannelType_CHANNEL_TYPE_GROUP,
		nil,
		common_persistence_models.ChatChannelFeatureFlags{Features: input.Features},
	)
	if err != nil {
		if errors.Is(err, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}
	if access != nil {
		if err := h.grantNewChannelOrganizationRead(ctx, created, access.OrganizationID, access.MinRoleRead); err != nil {
			return nil, err
		}
	}

	out := h.converter.PersistenceChatChannelToService(created)
	h.publishChannelCreated(ctx, actorID, out, []uint64{actorID})
	return out, nil
}
