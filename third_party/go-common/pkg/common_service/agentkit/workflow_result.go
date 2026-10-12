package agentkit

import (
	"context"
	"fmt"

	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// WorkflowAgentResultCallbacks keeps runtime completion distinct from durable
// application acceptance. Validate must reload the owned durable result and
// reject missing completion markers; a completed model turn alone is insufficient.
// Wait may impose an application timeout. Archive and Validate receive the
// original parent context so cleanup is not tied to an expired wait timeout.
type WorkflowAgentResultCallbacks[T any] struct {
	Wait     func(context.Context) (pb.ChatAgentRunStatus, error)
	Archive  func(context.Context) error
	Validate func(context.Context, pb.ChatAgentRunStatus) (T, error)
}

// AwaitWorkflowAgentResult archives terminal sessions and validates application
// output. Archive errors are separate so they remain observable and retryable
// without discarding an accepted result. Worker cancellation never archives.
func AwaitWorkflowAgentResult[T any](ctx context.Context, callbacks WorkflowAgentResultCallbacks[T]) (result T, archiveErr error, err error) {
	if callbacks.Wait == nil || callbacks.Archive == nil || callbacks.Validate == nil {
		return result, nil, fmt.Errorf("workflow agent wait, archive, and result validator are required")
	}
	status, err := callbacks.Wait(ctx)
	if err != nil {
		return result, nil, err
	}
	if !IsTerminalWorkflowAgentStatus(status) {
		return result, nil, fmt.Errorf("workflow agent wait returned nonterminal status %s", status)
	}
	archiveErr = callbacks.Archive(ctx)
	result, err = callbacks.Validate(ctx, status)
	return result, archiveErr, err
}
