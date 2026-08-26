package server

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
)

func (handler *apiHandler) prepareKnowledgeObjectQuarantine(
	request *http.Request,
	input *opensplunk.PrepareKnowledgeObjectQuarantineRequest,
) (*serializedPrepareKnowledgeObjectQuarantineResponse, error) {
	release, ok := handler.acquireSerialization()
	if !ok {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonResourceLimit,
			router.NewHTTPError(http.StatusTooManyRequests, "knowledge response capacity is exhausted"),
			nil,
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	submitted, ok := cloneKnowledgeMessage(input)
	if !ok || !validKnowledgeIdentity(submitted.GetKnowledgeObjectId(), maximumKnowledgeObjectIDBytes) {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge quarantine preparation request is invalid"),
			nil,
		)
	}
	scopes, err := handler.knowledgeScopes(request)
	if err != nil {
		return nil, handler.rejectKnowledgeScopeError(request)
	}
	service, ready := readyKnowledgeQuarantine(handler.knowledgeWriter)
	if !ready {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			errors.New("knowledge quarantine is unavailable"),
			nil,
		)
	}
	binding := knowledgeRejectionBinding{
		kind:     knowledgeRejectionBindingObject,
		scopes:   scopes,
		targetID: strings.Clone(submitted.GetKnowledgeObjectId()),
	}
	response, err := service.PrepareQuarantine(request.Context(), scopes.write, submitted)
	if err != nil {
		return nil, handler.rejectKnowledgeOperationError(request, err, nil, binding)
	}
	cloned, ok := cloneKnowledgeMessageBounded(response, maximumKnowledgeObjectResponseBytes)
	if !ok || !validPrepareKnowledgeObjectQuarantineResponse(cloned, submitted) {
		return nil, unavailableError(knowledgeManagementUnavailableText)
	}
	transferred = true
	return &serializedPrepareKnowledgeObjectQuarantineResponse{
		message: cloned,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) quarantineKnowledgeObject(
	request *http.Request,
	input *opensplunk.QuarantineKnowledgeObjectRequest,
) (*serializedQuarantineKnowledgeObjectResponse, error) {
	release, ok := handler.acquireSerialization()
	if !ok {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonResourceLimit,
			router.NewHTTPError(http.StatusTooManyRequests, "knowledge response capacity is exhausted"),
			nil,
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	submitted, ok := cloneKnowledgeMessage(input)
	if !ok {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge quarantine request is invalid"),
			nil,
		)
	}
	scopes, err := handler.knowledgeScopes(request)
	if err != nil {
		return nil, handler.rejectKnowledgeScopeError(request)
	}
	if err := knowledgecatalog.ValidateQuarantineKnowledgeObjectRequest(submitted); err != nil {
		return nil, handler.rejectKnowledgeOperationError(request, err, nil)
	}
	service, ready := readyKnowledgeQuarantine(handler.knowledgeWriter)
	if !ready {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			errors.New("knowledge quarantine is unavailable"),
			nil,
		)
	}
	response, err := service.Quarantine(request.Context(), scopes.write, submitted)
	if err != nil {
		binding := quarantineRejectionBinding(scopes, err)
		return nil, handler.rejectKnowledgeMutationError(request, err, binding)
	}
	// A nil error is a durable commit. Never fabricate a rejected-attempt row
	// if a dependency returns an invalid response after persistence.
	markKnowledgeMutationCommitted(request)
	cloned, ok := cloneKnowledgeMessageBounded(response, maximumKnowledgeObjectResponseBytes)
	if !ok || !validQuarantineKnowledgeObjectResponse(cloned) {
		return nil, unavailableError(knowledgeManagementUnavailableText)
	}
	transferred = true
	return &serializedQuarantineKnowledgeObjectResponse{
		message: cloned,
		ctx:     context.WithoutCancel(request.Context()),
		release: release,
	}, nil
}

func quarantineRejectionBinding(
	scopes knowledgeScopes,
	err error,
) knowledgeRejectionBinding {
	binding := knowledgeRejectionBinding{
		kind:   knowledgeRejectionBindingObject,
		scopes: scopes,
	}
	if authorized, found := knowledgecatalog.AuthorizedContextFromError(err); found &&
		authorized.Object != nil {
		binding.targetID = strings.Clone(authorized.Object.KnowledgeObjectID)
	}
	return binding
}

func validPrepareKnowledgeObjectQuarantineResponse(
	response *opensplunk.PrepareKnowledgeObjectQuarantineResponse,
	request *opensplunk.PrepareKnowledgeObjectQuarantineRequest,
) bool {
	return response != nil && request != nil &&
		response.GetRootKnowledgeObjectId() == request.GetKnowledgeObjectId() &&
		validKnowledgeIdentity(response.GetRootKnowledgeObjectId(), maximumKnowledgeObjectIDBytes) &&
		validKnowledgeRecoveryToken(response.GetRecoveryToken()) &&
		response.GetExpiresAt() != nil && response.GetExpiresAt().CheckValid() == nil &&
		response.GetDependentCount() < knowledgecatalog.MaximumObjectsPerTenant &&
		response.GetTenantCatalogRevision() >= 1 &&
		response.GetTenantCatalogRevision() <= math.MaxInt64
}

func validKnowledgeRecoveryToken(value string) bool {
	if len(value) < 64 || len(value) > 1<<10 {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if !(character >= 'A' && character <= 'Z') &&
			!(character >= 'a' && character <= 'z') &&
			!(character >= '0' && character <= '9') &&
			character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func validQuarantineKnowledgeObjectResponse(
	response *opensplunk.QuarantineKnowledgeObjectResponse,
) bool {
	if response == nil ||
		!validKnowledgeIdentity(response.GetRootKnowledgeObjectId(), maximumKnowledgeObjectIDBytes) ||
		response.GetTenantCatalogRevision() < 1 ||
		response.GetTenantCatalogRevision() > math.MaxInt64 ||
		len(response.GetTransitions()) < 1 ||
		len(response.GetTransitions()) > knowledgecatalog.MaximumObjectsPerTenant {
		return false
	}
	seen := make(map[string]struct{}, len(response.GetTransitions()))
	rootCount := 0
	for ordinal, transition := range response.GetTransitions() {
		if transition == nil || transition.GetCascadeOrdinal() != uint32(ordinal) ||
			!validKnowledgeIdentity(transition.GetKnowledgeObjectId(), maximumKnowledgeObjectIDBytes) ||
			transition.GetPreviousVersion() < 1 ||
			transition.GetPreviousVersion() >= math.MaxInt64 ||
			transition.GetQuarantinedVersion() != transition.GetPreviousVersion()+1 {
			return false
		}
		if _, duplicate := seen[transition.GetKnowledgeObjectId()]; duplicate {
			return false
		}
		seen[transition.GetKnowledgeObjectId()] = struct{}{}
		if transition.GetKnowledgeObjectId() == response.GetRootKnowledgeObjectId() {
			rootCount++
			if transition.GetReason() != opensplunk.KnowledgeQuarantineReason_KNOWLEDGE_QUARANTINE_REASON_ROOT_CORRUPTION ||
				ordinal != len(response.GetTransitions())-1 {
				return false
			}
		} else if transition.GetReason() != opensplunk.KnowledgeQuarantineReason_KNOWLEDGE_QUARANTINE_REASON_DEPENDENCY_RECOVERY {
			return false
		}
	}
	return rootCount == 1
}
