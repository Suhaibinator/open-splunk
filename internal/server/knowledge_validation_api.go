package server

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
)

// validateKnowledgeObject consumes only the bounded projection produced by
// the Validate-specific codec before it reaches catalog authority.
func (handler *apiHandler) validateKnowledgeObject(
	request *http.Request,
	input *opensplunkv1.ValidateKnowledgeObjectRequest,
) (*serializedValidateKnowledgeObjectResponse, error) {
	if err := knowledgecatalog.ValidateKnowledgeObjectRequest(input); err != nil {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge validation request is invalid"),
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

	writer, ready := knowledgeValidationWriter(handler)
	if !ready {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			errors.New("knowledge validation writer is unavailable"),
			nil,
		)
	}
	authority := detachKnowledgeValidationRequestAuthority(input)
	scopes, err := handler.knowledgeScopes(request)
	if err != nil {
		return nil, handler.rejectKnowledgeScopeError(request)
	}
	binding := authority.rejectionBinding(scopes)
	sealed, err := writer.Validate(
		request.Context(),
		cloneKnowledgeValidationScope(scopes),
		input,
	)
	if err != nil {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			sanitizeKnowledgeValidationError(err, authority),
			nil,
			binding,
		)
	}
	if err := request.Context().Err(); err != nil {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			err,
			nil,
			binding,
		)
	}

	transferred = true
	return newSerializedValidateKnowledgeObjectResponse(
		request.Context(),
		sealed,
		release,
	), nil
}

func knowledgeValidationWriter(
	handler *apiHandler,
) (*knowledgecatalog.Writer, bool) {
	if handler == nil || isNilDependency(handler.knowledgeWriter) {
		return nil, false
	}
	writer, exact := handler.knowledgeWriter.(*knowledgecatalog.Writer)
	return writer, exact && writer.ReadyForManagement()
}

func cloneKnowledgeValidationScope(
	scopes knowledgeScopes,
) knowledgecatalog.ValidationScope {
	return knowledgecatalog.ValidationScope{
		Read: knowledgecatalog.ReadScope{
			TenantID:       strings.Clone(scopes.read.TenantID),
			OwnerID:        strings.Clone(scopes.read.OwnerID),
			ReadableAppIDs: slices.Clone(scopes.read.ReadableAppIDs),
		},
		Write: knowledgecatalog.WriteScope{
			TenantID:       strings.Clone(scopes.write.TenantID),
			OwnerID:        strings.Clone(scopes.write.OwnerID),
			WritableAppIDs: slices.Clone(scopes.write.WritableAppIDs),
		},
	}
}

type knowledgeValidationRequestAuthority struct {
	update   bool
	objectID string
	appID    string
	intent   opensplunkv1.KnowledgeValidationIntent
}

func detachKnowledgeValidationRequestAuthority(
	request *opensplunkv1.ValidateKnowledgeObjectRequest,
) knowledgeValidationRequestAuthority {
	authority := knowledgeValidationRequestAuthority{
		intent: request.GetIntent(),
	}
	if request.KnowledgeObjectId != nil {
		authority.update = true
		authority.objectID = strings.Clone(request.GetKnowledgeObjectId())
		return authority
	}
	appID := trimKnowledgeASCIIWhitespace(request.GetDefinition().GetAppId())
	if validKnowledgeIdentity(appID, maximumKnowledgeAppIDBytes) {
		authority.appID = strings.Clone(appID)
	}
	return authority
}

func (authority knowledgeValidationRequestAuthority) rejectionBinding(
	scopes knowledgeScopes,
) knowledgeRejectionBinding {
	if authority.update {
		return knowledgeRejectionBinding{
			kind:     knowledgeRejectionBindingObject,
			scopes:   scopes,
			targetID: strings.Clone(authority.objectID),
		}
	}
	return knowledgeRejectionBinding{
		kind:   knowledgeRejectionBindingCreate,
		scopes: scopes,
		appID:  strings.Clone(authority.appID),
	}
}

type knowledgeValidationErrorKind uint8

const (
	knowledgeValidationErrorInvalid knowledgeValidationErrorKind = iota
	knowledgeValidationErrorNotFound
	knowledgeValidationErrorVersionConflict
	knowledgeValidationErrorInvalidArgument
	knowledgeValidationErrorDependencyConflict
	knowledgeValidationErrorCapacity
	knowledgeValidationErrorCanceled
	knowledgeValidationErrorDeadline
	knowledgeValidationErrorResponseTooLarge
)

// sanitizeKnowledgeValidationError accepts only the closed error authority of
// Writer.Validate. In particular, a rollback failure joined to an otherwise
// public sentinel must never inherit that sentinel's public HTTP mapping.
func sanitizeKnowledgeValidationError(
	err error,
	authority knowledgeValidationRequestAuthority,
) error {
	if err == nil {
		return errors.New("knowledge validation returned an absent error")
	}
	disposition, found := knowledgecatalog.ErrorDispositionFromError(err)
	if !found || disposition != knowledgecatalog.ErrorDispositionDefinitiveRejection {
		return errors.New("knowledge validation returned an unsafe disposition")
	}
	kind, safe := classifyKnowledgeValidationError(err)
	if !safe || !knowledgeValidationErrorApplies(kind, authority) {
		return errors.New("knowledge validation returned an impossible error")
	}
	return err
}

func classifyKnowledgeValidationError(
	err error,
) (knowledgeValidationErrorKind, bool) {
	for depth := 0; depth < 32 && err != nil; depth++ {
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			branches := joined.Unwrap()
			if len(branches) != 2 {
				return knowledgeValidationErrorInvalid, false
			}
			if exactKnowledgeValidationError(branches[0], control.ErrCapacityExceeded) &&
				exactKnowledgeValidationError(branches[1], knowledgevalidation.ErrResponseTooLarge) ||
				exactKnowledgeValidationError(branches[1], control.ErrCapacityExceeded) &&
					exactKnowledgeValidationError(branches[0], knowledgevalidation.ErrResponseTooLarge) {
				return knowledgeValidationErrorResponseTooLarge, true
			}
			return knowledgeValidationErrorInvalid, false
		}
		if unwrapped, ok := err.(interface{ Unwrap() error }); ok {
			next := unwrapped.Unwrap()
			if next == nil {
				return knowledgeValidationErrorInvalid, false
			}
			err = next
			continue
		}
		switch {
		case exactKnowledgeValidationError(err, control.ErrNotFound):
			return knowledgeValidationErrorNotFound, true
		case exactKnowledgeValidationError(err, control.ErrVersionConflict):
			return knowledgeValidationErrorVersionConflict, true
		case exactKnowledgeValidationError(err, control.ErrInvalidArgument):
			return knowledgeValidationErrorInvalidArgument, true
		case exactKnowledgeValidationError(err, control.ErrDependencyConflict):
			return knowledgeValidationErrorDependencyConflict, true
		case exactKnowledgeValidationError(err, control.ErrCapacityExceeded):
			return knowledgeValidationErrorCapacity, true
		case exactKnowledgeValidationError(err, context.Canceled):
			return knowledgeValidationErrorCanceled, true
		case exactKnowledgeValidationError(err, context.DeadlineExceeded):
			return knowledgeValidationErrorDeadline, true
		default:
			return knowledgeValidationErrorInvalid, false
		}
	}
	return knowledgeValidationErrorInvalid, false
}

func exactKnowledgeValidationError(err, target error) bool {
	if err == nil || target == nil {
		return false
	}
	value, targetValue := reflect.ValueOf(err), reflect.ValueOf(target)
	return value.Type() == targetValue.Type() && value.Comparable() && value.Equal(targetValue)
}

func knowledgeValidationErrorApplies(
	kind knowledgeValidationErrorKind,
	authority knowledgeValidationRequestAuthority,
) bool {
	switch kind {
	case knowledgeValidationErrorNotFound:
		return true
	case knowledgeValidationErrorVersionConflict:
		return authority.update
	case knowledgeValidationErrorDependencyConflict:
		return authority.intent == opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION
	case knowledgeValidationErrorInvalidArgument,
		knowledgeValidationErrorCapacity,
		knowledgeValidationErrorCanceled,
		knowledgeValidationErrorDeadline,
		knowledgeValidationErrorResponseTooLarge:
		return true
	default:
		return false
	}
}
