package server

import (
	"fmt"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
)

// validatePreviewKnowledgeObjectRequestEnvelope checks only Preview's
// structural request authority. Candidate semantics are delegated to the
// registered Validate envelope with ACTIVE_PUBLICATION forced by the server.
// Row-limit policy remains a later service concern and is deliberately not
// inspected here.
func validatePreviewKnowledgeObjectRequestEnvelope(
	request *opensplunkv1.PreviewKnowledgeObjectRequest,
) error {
	if request == nil {
		return fmt.Errorf(
			"%w: knowledge preview request is required",
			control.ErrInvalidArgument,
		)
	}
	if len(request.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf(
			"%w: knowledge preview request contains unknown envelope fields",
			control.ErrInvalidArgument,
		)
	}
	if !validSearchInspectionJobID(request.GetRetainedSearchJobId()) {
		return fmt.Errorf(
			"%w: knowledge preview retained search job ID is invalid",
			control.ErrInvalidArgument,
		)
	}

	return knowledgecatalog.ValidateKnowledgeObjectRequest(
		previewKnowledgeObjectActiveValidationView(request),
	)
}

// previewKnowledgeObjectActiveValidationView is synchronous and nonescaping.
// Writer.Validate remains the sole boundary which applies an update to current
// catalog authority and detaches the bounded selected candidate.
func previewKnowledgeObjectActiveValidationView(
	request *opensplunkv1.PreviewKnowledgeObjectRequest,
) *opensplunkv1.ValidateKnowledgeObjectRequest {
	return &opensplunkv1.ValidateKnowledgeObjectRequest{
		Definition:        request.Definition,
		KnowledgeObjectId: request.KnowledgeObjectId,
		ExpectedVersion:   request.ExpectedVersion,
		UpdateMask:        request.UpdateMask,
		Intent:            opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	}
}
