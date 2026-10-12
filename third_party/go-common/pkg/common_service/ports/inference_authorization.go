package ports

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// InferenceModelOverrideAuthorizer is a trusted service boundary, never client input.
// It reloads the authenticated actor and checks the current site permission.
type InferenceModelOverrideAuthorizer interface {
	AuthorizeInferenceModelOverride(context.Context) (common_service_models.UserIdType, error)
}

// InferenceSessionRetargetAuthorizer authorizes moving sessions' pinned models in one call. It
// classifies the actor as a re-targeting manager (site InferenceAdministration or organization
// ManageInferencePolicy), reported as true, or as the session owner holding ChooseInferenceModel
// (ownerUserID 0 admits managers only). It then applies each operation's application re-target
// gate in order and, for owners, the organization's member policy. With no operations it only
// classifies, so a bulk move can reject non-managers before reading any session.
type InferenceSessionRetargetAuthorizer interface {
	AuthorizeInferenceSessionRetarget(ctx context.Context, organizationID, ownerUserID uint64, operations ...int32) (common_service_models.UserIdType, bool, error)
}

// InferenceSessionRetargetGates reports each operation's application re-target gate so session
// summaries can tell clients when re-targeting is never available.
type InferenceSessionRetargetGates interface {
	InferenceSessionRetarget(operation int32) proto_models.InferenceSessionRetarget
}

type InferenceRoutingAuthority interface {
	inference.RequestAuthority
	InferenceSessionRetargetAuthorizer
	InferenceModelOverrideAuthorizer
	InferenceRetargetBatchSize() int
}

type InferenceAuthorityCapturer interface {
	CaptureInferenceAuthority() (InferenceRoutingAuthority, error)
}
