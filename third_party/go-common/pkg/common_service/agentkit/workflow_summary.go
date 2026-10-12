package agentkit

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
)

// BuildWorkflowProgressSummary restores application-defined structured progress
// for a prompt. A nil callback disables the summary. Oversized JSON is rejected,
// never truncated into invalid or misleading state. The application remains
// responsible for excluding untrusted instructions and sensitive source data.
// The state is encoded with encoding/json/v2 and sorted map keys, keeping the
// prompt stable across runs.
func BuildWorkflowProgressSummary[S any](ctx context.Context, maxBytes int, restore func(context.Context) (S, error)) (jsontext.Value, error) {
	if restore == nil {
		return nil, nil
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("workflow progress summary limit must be positive")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	state, err := restore(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(state, json.Deterministic(true))
	if err != nil {
		return nil, fmt.Errorf("encode workflow progress summary: %w", err)
	}
	if len(raw) > maxBytes {
		return nil, fmt.Errorf("workflow progress summary exceeds %d byte limit", maxBytes)
	}
	return raw, nil
}
