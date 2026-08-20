package server

import (
	"fmt"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
)

// validatePreviewKnowledgeObjectRequestEnvelope checks only Preview's
// structural request authority. Candidate semantics are delegated to the
// registered Validate envelope with ACTIVE_PUBLICATION forced by the server.
// It performs no retained-job lookup or authorization, and maximum_rows is
// preserved without assigning a default, bound, or execution meaning.
func validatePreviewKnowledgeObjectRequestEnvelope(
	request *opensplunk.PreviewKnowledgeObjectRequest,
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
	request *opensplunk.PreviewKnowledgeObjectRequest,
) *opensplunk.ValidateKnowledgeObjectRequest {
	return &opensplunk.ValidateKnowledgeObjectRequest{
		Definition:        request.Definition,
		KnowledgeObjectId: request.KnowledgeObjectId,
		ExpectedVersion:   request.ExpectedVersion,
		UpdateMask:        request.UpdateMask,
		Intent:            opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	}
}
