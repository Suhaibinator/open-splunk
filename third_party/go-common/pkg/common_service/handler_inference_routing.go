package common_service

import (
	"context"
	"errors"
	"math"
	"sort"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/inference_policy"
	"github.com/Suhaibinator/go-common/pkg/common_service/internal/inferenceerr"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

func (h *CommonServiceHandler[U]) inferencePermissions(view config.ServiceView) (*permission.PermissionHandler, error) {
	h.inferenceAuthorityOnce.Do(func() {
		h.inferenceAuthorityMemo = config.NewStrictGenerationMemo(func(view config.ServiceView) (*permission.PermissionHandler, error) {
			policy, err := permission.NewPermissionConfigFromViews(view, view)
			if err != nil {
				return nil, err
			}
			return permission.NewPermissionHandler(policy, h.logger)
		}, nil)
	})
	return h.inferenceAuthorityMemo.Get(view)
}

func (h *CommonServiceHandler[U]) inferenceActor(ctx context.Context, actorID uint64) (U, error) {
	if contextID, present := scontext.GetUserID[common_service_models.UserIdType](ctx); present && uint64(contextID) != actorID {
		var empty U
		return empty, apperrors.ErrInsufficientPermissions
	}
	return h.loadInferenceUser(ctx, common_service_models.UserIdType(actorID))
}

// inferenceOrganization loads the organization an inference decision is scoped to. A malformed ID
// is a permission failure; a missing or soft-deleted organization is ErrOrganizationNotFound for
// every actor, matching the organization endpoints.
func (h *CommonServiceHandler[U]) inferenceOrganization(ctx context.Context, organizationID uint64) (*cpm.Organization, error) {
	if organizationID == 0 || organizationID > math.MaxInt64 || h.commonPersistenceHandler == nil {
		return nil, apperrors.ErrInsufficientPermissions
	}
	organization, err := h.commonPersistenceHandler.GetOrganizationByID(ctx, int64(organizationID))
	if errors.Is(err, persistence.ErrOrganizationNotFound) {
		return nil, apperrors.ErrOrganizationNotFound
	}
	if err != nil {
		return nil, err
	}
	if organization == nil || organization.ID != int64(organizationID) || organization.DeletedAt.Valid {
		return nil, apperrors.ErrOrganizationNotFound
	}
	return organization, nil
}

// inferenceOrgRole resolves actorID's fresh (uncached) effective role in organization under the
// captured view's inheritance mode, with the organization depth its thresholds are indexed by. A
// non-member or a malformed role is ErrInsufficientPermissions.
func (h *CommonServiceHandler[U]) inferenceOrgRole(ctx context.Context, view config.ServiceView, organization *cpm.Organization, actorID uint64) (proto_models.OrganizationRole, int, error) {
	if h.orgRoleProvider == nil {
		return 0, 0, apperrors.ErrInsufficientPermissions
	}
	info, err := h.orgRoleProvider.ResolveUserRoleFresh(ctx, common_service_models.UserIdType(actorID), common_service_models.OrganizationIdType(organization.ID), view.OrgRoleInheritanceEnabled())
	switch {
	case errors.Is(err, persistence.ErrOrganizationNotFound):
		return 0, 0, apperrors.ErrOrganizationNotFound
	case errors.Is(err, persistence.ErrUserOrganizationNotFound):
		return 0, 0, apperrors.ErrInsufficientPermissions
	case err != nil:
		return 0, 0, err
	}
	role := info.EffectiveRole
	if _, valid := proto_models.OrganizationRole_name[int32(role)]; !valid || role == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		return 0, 0, apperrors.ErrInsufficientPermissions
	}
	return role, len(organization.Lineage), nil
}

// inferenceSubject loads actorID fresh and the permission handler for the captured view.
func (h *CommonServiceHandler[U]) inferenceSubject(ctx context.Context, view config.ServiceView, actorID uint64) (U, *permission.PermissionHandler, error) {
	actor, err := h.inferenceActor(ctx, actorID)
	if err != nil {
		return actor, nil, err
	}
	permissions, err := h.inferencePermissions(view)
	return actor, permissions, err
}

// authenticatedInferenceSubject is inferenceSubject for the request's authenticated user.
func (h *CommonServiceHandler[U]) authenticatedInferenceSubject(ctx context.Context, view config.ServiceView) (uint64, U, *permission.PermissionHandler, error) {
	actorID, present := scontext.GetUserID[common_service_models.UserIdType](ctx)
	if !present {
		var empty U
		return 0, empty, nil, apperrors.ErrInsufficientPermissions
	}
	actor, permissions, err := h.inferenceSubject(ctx, view, uint64(actorID))
	return uint64(actorID), actor, permissions, err
}

func (h *CommonServiceHandler[U]) authorizeInferencePolicy(ctx context.Context, view config.ServiceView, organizationID uint64, siteOperation permission.SitePermissionOperation, orgOperation permission.OrganizationPermissionOperation) (uint64, error) {
	actorID, actor, permissions, err := h.authenticatedInferenceSubject(ctx, view)
	if err != nil {
		return 0, err
	}
	if siteOperation != permission.SitePermissionOperationUnknown {
		if permissions.HasSitePermission(ctx, actor.GetUserType(), siteOperation) {
			return actorID, nil
		}
		return 0, apperrors.ErrInsufficientPermissions
	}
	organization, err := h.inferenceOrganization(ctx, organizationID)
	if err != nil {
		return 0, err
	}
	role, depth, err := h.inferenceOrgRole(ctx, view, organization, actorID)
	if err != nil {
		return 0, err
	}
	allowed, err := permissions.HasOrgPermission(ctx, actor.GetUserType(), role, orgOperation, depth)
	if err != nil {
		return 0, err
	}
	if !allowed {
		return 0, apperrors.ErrInsufficientPermissions
	}
	return actorID, nil
}

func (h *CommonServiceHandler[U]) AuthorizeInferenceRequest(ctx context.Context, request inference.GenerationRequest) error {
	if request.OrganizationID == 0 && request.ActorUserID == 0 && request.ModelSelection == nil {
		return nil
	}
	if h == nil || h.runtimeConfig == nil {
		return apperrors.ErrInferenceControlPlaneUnavailable
	}
	view := h.runtimeConfig()
	return h.authorizeInferenceRequestWithView(ctx, request, view)
}

func (h *CommonServiceHandler[U]) authorizeInferenceRequestWithView(ctx context.Context, request inference.GenerationRequest, view config.ServiceView) error {
	if request.OrganizationID == 0 || request.OrganizationID > math.MaxInt64 || h.commonPersistenceHandler == nil {
		return apperrors.ErrInsufficientPermissions
	}
	actor, permissions, err := h.inferenceSubject(ctx, view, request.ActorUserID)
	if err != nil {
		return err
	}
	privilegedSelection := false
	if request.ModelSelection != nil {
		source := request.ModelSelection.SelectionSource
		if !request.Pinned && source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET {
			privilegedSelection = permissions.HasSitePermission(ctx, actor.GetUserType(), permission.PermissionInferenceAdministration)
		}
		if source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE {
			privilegedSelection = permissions.HasSitePermission(ctx, actor.GetUserType(), permission.PermissionInferenceModelOverride)
		}
	}
	if request.SiteOverrideUserID != 0 {
		if !request.Pinned {
			return apperrors.ErrInsufficientPermissions
		}
		selector := actor
		if request.SiteOverrideUserID != request.ActorUserID {
			if selector, err = h.loadInferenceUser(ctx, common_service_models.UserIdType(request.SiteOverrideUserID)); err != nil {
				return err
			}
		}
		if !permissions.HasSitePermission(ctx, selector.GetUserType(), permission.PermissionInferenceModelOverride) {
			return apperrors.ErrInsufficientPermissions
		}
		if request.SiteOverrideUserID == request.ActorUserID {
			privilegedSelection = true
		}
	}
	organization, err := h.inferenceOrganization(ctx, request.OrganizationID)
	if err != nil || privilegedSelection {
		return err
	}
	role, depth, err := h.inferenceOrgRole(ctx, view, organization, request.ActorUserID)
	if err != nil {
		return err
	}
	if request.ModelSelection == nil || request.Pinned {
		return nil
	}
	source := request.ModelSelection.SelectionSource
	if source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_UNSPECIFIED || source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE {
		return apperrors.ErrInsufficientPermissions
	}
	operation := permission.PermissionChooseInferenceModel
	if source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET {
		operation = permission.PermissionManageInferencePolicy
	}
	allowed, err := permissions.HasOrgPermission(ctx, actor.GetUserType(), role, operation, depth)
	if err != nil {
		return err
	}
	if !allowed {
		return apperrors.ErrInsufficientPermissions
	}
	return nil
}

// inferenceStanding classifies the authenticated actor for one organization. management is the
// re-targeting manager predicate: site InferenceAdministration or the organization's
// ManageInferencePolicy. choose is the organization's ChooseInferenceModel. A missing
// organization fails for every actor. A site administrator needs no organization role, so a
// failed role lookup only leaves choose false for one; any other actor without a role fails.
func (h *CommonServiceHandler[U]) inferenceStanding(ctx context.Context, view config.ServiceView, organizationID uint64) (uint64, bool, bool, error) {
	actorID, actor, permissions, err := h.authenticatedInferenceSubject(ctx, view)
	if err != nil {
		return 0, false, false, err
	}
	organization, err := h.inferenceOrganization(ctx, organizationID)
	if err != nil {
		return 0, false, false, err
	}
	site := permissions.HasSitePermission(ctx, actor.GetUserType(), permission.PermissionInferenceAdministration)
	var manage, choose bool
	role, depth, err := h.inferenceOrgRole(ctx, view, organization, actorID)
	if err == nil {
		manage, err = permissions.HasOrgPermission(ctx, actor.GetUserType(), role, permission.PermissionManageInferencePolicy, depth)
	}
	if err == nil {
		choose, err = permissions.HasOrgPermission(ctx, actor.GetUserType(), role, permission.PermissionChooseInferenceModel, depth)
	}
	if err != nil {
		if site {
			return actorID, true, false, nil
		}
		return 0, false, false, err
	}
	return actorID, site || manage, choose, nil
}

// AuthorizeInferenceSessionRetarget authorizes moving sessions; see
// ports.InferenceSessionRetargetAuthorizer.
func (h *CommonServiceHandler[U]) AuthorizeInferenceSessionRetarget(ctx context.Context, organizationID, ownerUserID uint64, operations ...int32) (common_service_models.UserIdType, bool, error) {
	if h == nil || h.runtimeConfig == nil {
		return 0, false, apperrors.ErrInferenceControlPlaneUnavailable
	}
	view := h.runtimeConfig()
	return h.authorizeInferenceSessionRetargetWithView(ctx, organizationID, ownerUserID, operations, view)
}

// authorizeInferenceSessionRetargetWithView classifies the actor as a re-targeting manager or the
// owner holding ChooseInferenceModel, then applies each operation's application re-target gate in
// order. Without configured inference routing a gate is unknown, so moves fail closed.
func (h *CommonServiceHandler[U]) authorizeInferenceSessionRetargetWithView(ctx context.Context, organizationID, ownerUserID uint64, operations []int32, view config.ServiceView) (common_service_models.UserIdType, bool, error) {
	actorID, management, choose, err := h.inferenceStanding(ctx, view, organizationID)
	if err != nil {
		return 0, false, err
	}
	if !management && (ownerUserID != actorID || !choose) {
		return 0, false, apperrors.ErrInsufficientPermissions
	}
	for _, operation := range operations {
		if h.inferencePolicyHandler == nil {
			return 0, false, apperrors.ErrInferenceControlPlaneUnavailable
		}
		if err := h.inferencePolicyHandler.AuthorizeSessionRetarget(ctx, organizationID, operation, management); err != nil {
			return 0, false, inferenceerr.Translate(err)
		}
	}
	return common_service_models.UserIdType(actorID), management, nil
}

// InferenceSessionRetarget returns the operation's application re-target gate, or UNSPECIFIED
// (treated as NEVER) when inference routing is not configured.
func (h *CommonServiceHandler[U]) InferenceSessionRetarget(operation int32) proto_models.InferenceSessionRetarget {
	if h == nil || h.inferencePolicyHandler == nil {
		return proto_models.InferenceSessionRetarget_INFERENCE_SESSION_RETARGET_UNSPECIFIED
	}
	return h.inferencePolicyHandler.SessionRetarget(operation)
}

type capturedInferenceAuthority[U common_service_models.UserLike] struct {
	handler *CommonServiceHandler[U]
	view    config.ServiceView
}

func (h *CommonServiceHandler[U]) CaptureInferenceAuthority() (ports.InferenceRoutingAuthority, error) {
	if h == nil || h.runtimeConfig == nil {
		return nil, apperrors.ErrInferenceControlPlaneUnavailable
	}
	view := h.runtimeConfig()
	if _, err := h.inferencePermissions(view); err != nil {
		return nil, err
	}
	return &capturedInferenceAuthority[U]{handler: h, view: view}, nil
}

func (authority *capturedInferenceAuthority[U]) AuthorizeInferenceRequest(ctx context.Context, request inference.GenerationRequest) error {
	return authority.handler.authorizeInferenceRequestWithView(ctx, request, authority.view)
}

func (authority *capturedInferenceAuthority[U]) AuthorizeInferenceSessionRetarget(ctx context.Context, organizationID, ownerUserID uint64, operations ...int32) (common_service_models.UserIdType, bool, error) {
	return authority.handler.authorizeInferenceSessionRetargetWithView(ctx, organizationID, ownerUserID, operations, authority.view)
}

func (authority *capturedInferenceAuthority[U]) AuthorizeInferenceModelOverride(ctx context.Context) (common_service_models.UserIdType, error) {
	actorID, err := authority.handler.authorizeInferencePolicy(ctx, authority.view, 0, permission.PermissionInferenceModelOverride, 0)
	return common_service_models.UserIdType(actorID), err
}

func (authority *capturedInferenceAuthority[U]) InferenceRetargetBatchSize() int {
	return authority.view.InferenceRetargetBatchSize()
}

func (h *CommonServiceHandler[U]) ConfigureInferenceRouting(store inference_policy.Store, registry *controlplane.Registry, base *controlplane.PersistenceRouteResolver) *controlplane.LayeredRouteResolver {
	if h == nil {
		return nil
	}
	resolver := controlplane.NewLayeredRouteResolver(base, store, h)
	var sessionAction inference_policy.SessionActionHandler
	if sessions, supported := store.(inferencePolicySessionStore); supported {
		sessionAction = func(ctx context.Context, view config.ServiceView, input inference_policy.SessionActionInput) error {
			return h.applyInferencePolicySessionAction(ctx, sessions, view, input)
		}
	}
	h.inferencePolicyHandler = inference_policy.NewHandler(store, registry, resolver, h.runtimeConfig, h.authorizeInferencePolicy, h.inferenceStanding, sessionAction)
	return resolver
}

type inferencePolicySessionStore interface {
	ListInferencePolicySessions(context.Context, uint64, int32, []uint64, bool, uint64, int) ([]cpm.ChatAgentSession, error)
	BlockInferencePolicySessions(context.Context, []uint64, map[uint64]uint64, proto_models.InferenceBlockReason) error
}

// applyInferencePolicySessionAction pages through the sessions a policy change strands and blocks
// them or moves them to input.MoveToModelID. Site-override pins are left alone.
func (h *CommonServiceHandler[U]) applyInferencePolicySessionAction(ctx context.Context, store inferencePolicySessionStore, view config.ServiceView, input inference_policy.SessionActionInput) error {
	if h.chatHandler == nil || input.BatchSize < 1 {
		return apperrors.ErrInferenceControlPlaneUnavailable
	}
	block := input.Action == proto_models.InferencePolicySessionAction_INFERENCE_POLICY_SESSION_ACTION_BLOCK
	if !block && (input.Action != proto_models.InferencePolicySessionAction_INFERENCE_POLICY_SESSION_ACTION_MOVE || input.MoveToModelID == nil) {
		return apperrors.ErrInvalidRequest
	}
	reason := inferencePolicyBlockReason(input)
	// A nil model list is the storage wildcard; a policy action always names its models.
	modelIDs := input.ModelIDs
	if modelIDs == nil {
		modelIDs = []uint64{}
	}
	var cursor uint64
	for {
		sessions, err := store.ListInferencePolicySessions(ctx, input.OrganizationID, input.Operation, modelIDs, input.IncludeUserChoices, cursor, input.BatchSize)
		if err != nil {
			return err
		}
		if len(sessions) == 0 {
			return nil
		}
		cursor = sessions[len(sessions)-1].ID
		// Expected pin versions by organization, then session.
		groups := make(map[uint64]map[uint64]uint64)
		for _, session := range sessions {
			if session.SiteOverrideUserID != nil {
				continue
			}
			organizationID := uint64(session.OrganizationID)
			if groups[organizationID] == nil {
				groups[organizationID] = make(map[uint64]uint64)
			}
			groups[organizationID][session.ID] = session.InferencePinVersion
		}
		if len(groups) == 0 {
			continue
		}
		if block {
			ids := make([]uint64, 0, len(sessions))
			versions := make(map[uint64]uint64, len(sessions))
			for _, group := range groups {
				for sessionID, version := range group {
					ids = append(ids, sessionID)
					versions[sessionID] = version
				}
			}
			sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
			if err := store.BlockInferencePolicySessions(ctx, ids, versions, reason); err != nil {
				return err
			}
			continue
		}
		orgIDs := make([]uint64, 0, len(groups))
		for organizationID := range groups {
			orgIDs = append(orgIDs, organizationID)
		}
		sort.Slice(orgIDs, func(left, right int) bool { return orgIDs[left] < orgIDs[right] })
		for _, organizationID := range orgIDs {
			versions := groups[organizationID]
			ids := make([]uint64, 0, len(versions))
			for sessionID := range versions {
				ids = append(ids, sessionID)
			}
			sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
			_, err := h.chatHandler.BulkRetargetAgentChatSessions(ctx, chatpkg.BulkRetargetAgentChatSessionsInput{
				Authority:   &capturedInferenceAuthority[U]{handler: h, view: view},
				ActorUserID: input.ActorUserID, OrganizationID: organizationID, AgentSessionIDs: ids, ExpectedPinVersions: versions,
				ModelSelection: &inference.ModelSelection{InferenceModelID: *input.MoveToModelID, SelectionSource: proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET},
			})
			if err != nil {
				return err
			}
		}
	}
}

// inferencePolicyBlockReason names why a policy action blocks sessions: closed member choice when
// no models were removed, otherwise the layer whose list dropped the session's model.
func inferencePolicyBlockReason(input inference_policy.SessionActionInput) proto_models.InferenceBlockReason {
	switch {
	case input.IncludeUserChoices && len(input.ModelIDs) == 0:
		return proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_MEMBER_CHOICE_CLOSED
	case input.OrganizationID != 0:
		return proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_MODEL_NOT_PERMITTED_BY_ORG
	default:
		return proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_MODEL_NOT_IN_OPERATION_LIST
	}
}

// retargetSelection is the model selection a re-target request asks for: a site override when
// requested, otherwise a source the chat handler derives from the actor's standing.
func retargetSelection(inferenceModelID uint64, api *proto_models.InferenceAPI, siteOverride bool) *inference.ModelSelection {
	selection := &inference.ModelSelection{InferenceModelID: inferenceModelID, API: api}
	if siteOverride {
		selection.SelectionSource = proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE
	}
	return selection
}

func (h *CommonServiceHandler[U]) RetargetInferenceSession(ctx context.Context, request *proto_models.RetargetInferenceSessionRequest) (*proto_models.RetargetInferenceSessionResponse, error) {
	if request == nil {
		return nil, apperrors.ErrInvalidRequest
	}
	if h.chatHandler == nil {
		return nil, apperrors.ErrChatDisabled
	}
	session, err := h.chatHandler.RetargetAgentChatSession(ctx, chatpkg.RetargetAgentChatSessionInput{
		AgentSessionID: request.AgentSessionId, ExpectedPinVersion: request.ExpectedPinVersion,
		ModelSelection: retargetSelection(request.InferenceModelId, request.Api, request.SiteOverride),
	})
	if err != nil {
		return nil, inferenceerr.Translate(err)
	}
	return &proto_models.RetargetInferenceSessionResponse{Session: h.chatHandler.AgentSessionSummary(session)}, nil
}

func (h *CommonServiceHandler[U]) RetargetInferenceSessions(ctx context.Context, request *proto_models.RetargetInferenceSessionsRequest) (*proto_models.RetargetInferenceSessionsResponse, error) {
	if request == nil {
		return nil, apperrors.ErrInvalidRequest
	}
	if h.chatHandler == nil {
		return nil, apperrors.ErrChatDisabled
	}
	ids := make([]uint64, len(request.Sessions))
	versions := make(map[uint64]uint64, len(request.Sessions))
	for index, session := range request.Sessions {
		if session == nil {
			return nil, apperrors.ErrInvalidRequest
		}
		ids[index] = session.AgentSessionId
		versions[session.AgentSessionId] = session.ExpectedPinVersion
	}
	sessions, err := h.chatHandler.BulkRetargetAgentChatSessions(ctx, chatpkg.BulkRetargetAgentChatSessionsInput{
		OrganizationID: request.OrganizationId, AgentSessionIDs: ids, ExpectedPinVersions: versions,
		ModelSelection: retargetSelection(request.InferenceModelId, request.Api, request.SiteOverride),
	})
	if err != nil {
		return nil, inferenceerr.Translate(err)
	}
	response := &proto_models.RetargetInferenceSessionsResponse{Sessions: make([]*proto_models.AgentSessionSummary, len(sessions))}
	for index, session := range sessions {
		response.Sessions[index] = h.chatHandler.AgentSessionSummary(session)
	}
	return response, nil
}

func (h *CommonServiceHandler[U]) GetInferenceOperationModels(ctx context.Context, request *proto_models.GetInferenceOperationModelsRequest) (*proto_models.GetInferenceOperationModelsResponse, error) {
	if h.inferencePolicyHandler == nil {
		return nil, apperrors.ErrInferenceControlPlaneUnavailable
	}
	response, err := h.inferencePolicyHandler.GetInferenceOperationModels(ctx, request)
	return response, inferenceerr.Translate(err)
}

func (h *CommonServiceHandler[U]) UpdateInferenceOperationModels(ctx context.Context, request *proto_models.UpdateInferenceOperationModelsRequest) (*proto_models.UpdateInferenceOperationModelsResponse, error) {
	if h.inferencePolicyHandler == nil {
		return nil, apperrors.ErrInferenceControlPlaneUnavailable
	}
	response, err := h.inferencePolicyHandler.UpdateInferenceOperationModels(ctx, request)
	return response, inferenceerr.Translate(err)
}

func (h *CommonServiceHandler[U]) GetOrganizationInferencePolicy(ctx context.Context, request *proto_models.GetOrganizationInferencePolicyRequest) (*proto_models.GetOrganizationInferencePolicyResponse, error) {
	if h.inferencePolicyHandler == nil {
		return nil, apperrors.ErrInferenceControlPlaneUnavailable
	}
	response, err := h.inferencePolicyHandler.GetOrganizationInferencePolicy(ctx, request)
	return response, inferenceerr.Translate(err)
}

func (h *CommonServiceHandler[U]) UpdateOrganizationInferencePolicy(ctx context.Context, request *proto_models.UpdateOrganizationInferencePolicyRequest) (*proto_models.UpdateOrganizationInferencePolicyResponse, error) {
	if h.inferencePolicyHandler == nil {
		return nil, apperrors.ErrInferenceControlPlaneUnavailable
	}
	response, err := h.inferencePolicyHandler.UpdateOrganizationInferencePolicy(ctx, request)
	return response, inferenceerr.Translate(err)
}

func (h *CommonServiceHandler[U]) UpdateOrganizationInferenceOperationPolicy(ctx context.Context, request *proto_models.UpdateOrganizationInferenceOperationPolicyRequest) (*proto_models.UpdateOrganizationInferenceOperationPolicyResponse, error) {
	if h.inferencePolicyHandler == nil {
		return nil, apperrors.ErrInferenceControlPlaneUnavailable
	}
	response, err := h.inferencePolicyHandler.UpdateOrganizationInferenceOperationPolicy(ctx, request)
	return response, inferenceerr.Translate(err)
}

func (h *CommonServiceHandler[U]) ListInferenceModelChoices(ctx context.Context, request *proto_models.ListInferenceModelChoicesRequest) (*proto_models.ListInferenceModelChoicesResponse, error) {
	if h.inferencePolicyHandler == nil {
		return nil, apperrors.ErrInferenceControlPlaneUnavailable
	}
	response, err := h.inferencePolicyHandler.ListInferenceModelChoices(ctx, request)
	return response, inferenceerr.Translate(err)
}
