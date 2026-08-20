package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgepreview"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func (handler *apiHandler) previewKnowledgeObject(
	request *http.Request,
	input *opensplunk.PreviewKnowledgeObjectRequest,
) (*serializedPreviewKnowledgeObjectResponse, error) {
	if err := validatePreviewKnowledgeObjectRequestEnvelope(input); err != nil {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge preview request is invalid"),
			nil,
		)
	}
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonResourceLimit,
			router.NewHTTPError(
				http.StatusTooManyRequests,
				"knowledge response capacity is exhausted",
			),
			nil,
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	if !handler.knowledgePreviewConfigured() {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			knowledgepreview.ErrUnavailable,
			nil,
		)
	}
	scopes, err := handler.knowledgeScopes(request)
	if err != nil {
		return nil, handler.rejectKnowledgeScopeError(request)
	}
	authority := detachKnowledgeValidationRequestAuthority(
		previewKnowledgeObjectActiveValidationView(input),
	)
	binding := authority.rejectionBinding(scopes)
	sealed, err := handler.knowledgePreview.Preview(
		request.Context(),
		searchjobs.AccessScope{
			TenantID: scopes.read.TenantID,
			OwnerID:  scopes.read.OwnerID,
		},
		cloneKnowledgeValidationScope(scopes),
		input,
	)
	if err != nil {
		return nil, handler.rejectPreviewKnowledgeError(
			request,
			err,
			authority,
			binding,
		)
	}
	if err := request.Context().Err(); err != nil {
		return nil, handler.rejectPreviewKnowledgeError(
			request,
			err,
			authority,
			binding,
		)
	}
	transferred = true
	return newSerializedPreviewKnowledgeObjectResponse(
		request.Context(),
		sealed,
		release,
	), nil
}

func (handler *apiHandler) rejectPreviewKnowledgeError(
	request *http.Request,
	err error,
	authority knowledgeValidationRequestAuthority,
	binding knowledgeRejectionBinding,
) error {
	switch {
	case errors.Is(err, knowledgepreview.ErrInvalidRequest):
		return handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge preview request is invalid"),
			nil,
		)
	case errors.Is(err, knowledgepreview.ErrNotFoundOrForbidden):
		return handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonNotFoundOrForbidden,
			router.NewHTTPError(
				http.StatusNotFound,
				"knowledge preview execution not found",
			),
			nil,
		)
	case errors.Is(err, knowledgepreview.ErrResourceLimit),
		errors.Is(err, knowledgepreview.ErrResponseTooLarge):
		return handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonResourceLimit,
			router.NewHTTPError(
				http.StatusTooManyRequests,
				"knowledge preview capacity is exhausted",
			),
			nil,
		)
	case errors.Is(err, knowledgepreview.ErrUnavailable),
		errors.Is(err, knowledgepreview.ErrInvariant):
		return handler.rejectKnowledgeOperationError(request, err, nil)
	case errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return handler.rejectKnowledgeOperationError(request, err, nil, binding)
	default:
		return handler.rejectKnowledgeOperationError(
			request,
			sanitizeKnowledgeValidationError(err, authority),
			nil,
			binding,
		)
	}
}
