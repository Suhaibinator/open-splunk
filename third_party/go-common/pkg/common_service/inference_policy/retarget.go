package inference_policy

import (
	"context"
	"math"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// SessionRetarget returns the operation's application re-target gate, or UNSPECIFIED (treated as
// NEVER) when the operation is not registered.
func (h *Handler) SessionRetarget(operation int32) pb.InferenceSessionRetarget {
	if h == nil || h.registry == nil {
		return pb.InferenceSessionRetarget_INFERENCE_SESSION_RETARGET_UNSPECIFIED
	}
	gate, _ := h.registry.SessionRetarget(controlplane.Operation(operation))
	return gate
}

// AuthorizeSessionRetarget applies the operation's application re-target gate to one move.
// Managers (site inference administrators and ManageInferencePolicy holders) may move sessions
// under MANAGERS and ORG_DECIDES. Members may move their own only under ORG_DECIDES when the
// organization enabled member_retarget and member choice is open. The caller has already
// authorized the actor's kind; this reads the organization policy in the caller's transaction.
func (h *Handler) AuthorizeSessionRetarget(ctx context.Context, organizationID uint64, operation int32, management bool) error {
	if h == nil || h.store == nil || h.registry == nil {
		return ErrUnavailable
	}
	gate := h.SessionRetarget(operation)
	if !managersMayRetarget(gate) {
		return ErrSessionRetargetNotAllowed
	}
	if management {
		return nil
	}
	if gate != pb.InferenceSessionRetarget_INFERENCE_SESSION_RETARGET_ORG_DECIDES || organizationID == 0 || organizationID > math.MaxInt64 {
		return ErrSessionRetargetNotAllowed
	}
	snapshot, err := h.load(ctx, organizationID, []int32{operation}, nil)
	if err != nil {
		return err
	}
	if !h.memberRetarget(gate, organizationID, operation, snapshot) {
		return ErrSessionRetargetNotAllowed
	}
	return nil
}

func managersMayRetarget(gate pb.InferenceSessionRetarget) bool {
	return gate == pb.InferenceSessionRetarget_INFERENCE_SESSION_RETARGET_MANAGERS || gate == pb.InferenceSessionRetarget_INFERENCE_SESSION_RETARGET_ORG_DECIDES
}

// validateMoveGate rejects a MOVE session action, dry runs included, when the operation is not
// registered or its gate forbids re-targeting altogether.
func validateMoveGate(gate pb.InferenceSessionRetarget, registered bool, action pb.InferencePolicySessionAction) error {
	if action != pb.InferencePolicySessionAction_INFERENCE_POLICY_SESSION_ACTION_MOVE {
		return nil
	}
	if !registered {
		return ErrInvalidPolicy
	}
	if !managersMayRetarget(gate) {
		return ErrSessionRetargetNotAllowed
	}
	return nil
}

// memberRetarget reports whether members may move their own sessions: the gate is ORG_DECIDES,
// member choice is open, and the organization enabled member_retarget.
func (h *Handler) memberRetarget(gate pb.InferenceSessionRetarget, organizationID uint64, operation int32, snapshot *cpm.InferenceRoutingSnapshot) bool {
	if gate != pb.InferenceSessionRetarget_INFERENCE_SESSION_RETARGET_ORG_DECIDES || h.parked(operation, optionCount(snapshot, operation)) {
		return false
	}
	policy := operationPolicy(snapshot, organizationID, operation)
	return policy != nil && policy.MemberChoice && policy.MemberRetarget
}
