package agentkit

import (
	"context"
	"crypto/sha256"
	"encoding/json/jsontext"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// WorkflowMetadata is persisted with application state in one parent checkpoint.
// Version is application-selected; InputFingerprint describes semantic inputs.
type WorkflowMetadata struct {
	Version          int                            `json:"version"`
	CurrentRunID     uint64                         `json:"current_run_id"`
	InputFingerprint string                         `json:"input_fingerprint"`
	Receipts         map[string]WorkflowToolReceipt `json:"receipts,omitempty"`
	ReadReceipts     map[string]WorkflowReadReceipt `json:"read_receipts,omitempty"`
}

type WorkflowProgress[S any] struct {
	WorkflowMetadata
	State S `json:"state"`
}

// WorkflowToolReceipt stores textual mutation results. Media read results are
// reconstructed after authorization, never copied into durable receipts.
type WorkflowToolReceipt struct {
	ArgumentsHash string         `json:"arguments_hash"`
	ToolName      string         `json:"tool_name"`
	ContentJSON   jsontext.Value `json:"content_json"`
	Content       string         `json:"content,omitempty"`
	IsError       bool           `json:"is_error,omitzero"`
}
type WorkflowReadReceipt struct {
	ArgumentsHash string               `json:"arguments_hash"`
	ToolName      string               `json:"tool_name"`
	Blocked       *WorkflowToolReceipt `json:"blocked,omitempty"`
}

// WorkflowStateStore must lock and validate the parent claim, stage, session,
// run and semantic inputs BEFORE calling fn, including for receipt replays.
// It must commit state, receipts and all callback domain writes in the SAME
// transaction, rolling back on error. Saving progress must not refresh a worker
// heartbeat or overwrite unrelated parent fields.
type WorkflowStateStore[S any] func(context.Context, ports.AgentToolInvocation, func(context.Context, *WorkflowProgress[S]) error) error

type WorkflowToolPolicy[S any] struct {
	Mutating bool
	Read     bool
	Complete func(S) bool
	// BeforeRead runs only on a first read admission. A non-nil result is a
	// refusal, persisted and replayed without recounting it.
	BeforeRead func(context.Context, *WorkflowProgress[S]) (*ports.AgentToolResult, error)
	// Execute receives replay=true for a previously admitted read. It should
	// reconstruct authorized content but must not count the read again.
	Execute func(context.Context, *WorkflowProgress[S], bool) (*ports.AgentToolResult, error)
}

func workflowReceipt(result *ports.AgentToolResult, hash, name string) WorkflowToolReceipt {
	return WorkflowToolReceipt{ArgumentsHash: hash, ToolName: name, ContentJSON: result.ContentJSON.Clone(), Content: result.Content, IsError: result.IsError}
}
func (r WorkflowToolReceipt) result() *ports.AgentToolResult {
	return &ports.AgentToolResult{ContentJSON: r.ContentJSON.Clone(), Content: r.Content, IsError: r.IsError}
}

// ExecuteWorkflowTool serializes durable admission, execution and replay through
// the application transaction. Returning a context cancellation unchanged lets
// the agent runtime suspend and recover instead of terminally failing the tool.
func ExecuteWorkflowTool[S any](ctx context.Context, store WorkflowStateStore[S], inv ports.AgentToolInvocation, policy WorkflowToolPolicy[S]) (*ports.AgentToolResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store == nil || policy.Execute == nil {
		return nil, fmt.Errorf("workflow tool store and executor are required")
	}
	if policy.Mutating && policy.Read {
		return nil, fmt.Errorf("workflow tool cannot be both a mutation and a read")
	}
	var result *ports.AgentToolResult
	err := store(ctx, inv, func(txCtx context.Context, p *WorkflowProgress[S]) error {
		if p == nil || inv.AgentRunID == 0 || p.CurrentRunID != inv.AgentRunID {
			return fmt.Errorf("workflow agent run does not own persisted progress")
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(inv.ArgumentsJSON))
		// A stable invocation identity cannot change category on recovery.
		if inv.IdempotencyKey != "" {
			if _, exists := p.ReadReceipts["key:"+inv.IdempotencyKey]; exists && policy.Mutating {
				return fmt.Errorf("workflow tool identity was reused with a different execution policy")
			}
			if _, exists := p.Receipts[inv.IdempotencyKey]; exists && !policy.Mutating {
				return fmt.Errorf("workflow tool identity was reused with a different execution policy")
			}
		}
		if policy.Mutating {
			if inv.IdempotencyKey == "" {
				return fmt.Errorf("workflow mutation requires a durable tool idempotency key")
			}
			if r, ok := p.Receipts[inv.IdempotencyKey]; ok {
				if r.ArgumentsHash != hash || r.ToolName != inv.ToolName {
					return fmt.Errorf("workflow tool identity was reused with different arguments")
				}
				result = r.result()
				return nil
			}
			if policy.Complete != nil && policy.Complete(p.State) {
				return fmt.Errorf("workflow result is complete and immutable")
			}
		}
		key := ""
		if policy.Read {
			if inv.IdempotencyKey != "" {
				key = "key:" + inv.IdempotencyKey
			} else if inv.ToolCallID != "" {
				key = "call:" + inv.ToolCallID
			}
		}
		replay := false
		if key != "" {
			if r, ok := p.ReadReceipts[key]; ok {
				if r.ArgumentsHash != hash || r.ToolName != inv.ToolName {
					return fmt.Errorf("workflow read identity was reused with different arguments")
				}
				replay = true
				if r.Blocked != nil {
					result = r.Blocked.result()
					return nil
				}
			}
		}
		recordRead := func(blocked *ports.AgentToolResult) {
			if key == "" {
				return
			}
			if p.ReadReceipts == nil {
				p.ReadReceipts = map[string]WorkflowReadReceipt{}
			}
			receipt := WorkflowReadReceipt{ArgumentsHash: hash, ToolName: inv.ToolName}
			if blocked != nil {
				r := workflowReceipt(blocked, hash, inv.ToolName)
				receipt.Blocked = &r
			}
			p.ReadReceipts[key] = receipt
		}
		if policy.Read && !replay && policy.BeforeRead != nil {
			blocked, e := policy.BeforeRead(txCtx, p)
			if e != nil {
				return e
			}
			if blocked != nil {
				result = blocked
				recordRead(blocked)
				return nil
			}
		}
		var e error
		result, e = policy.Execute(txCtx, p, replay)
		if e != nil {
			return e
		}
		if result == nil {
			return fmt.Errorf("workflow tool returned no result")
		}
		if !replay {
			recordRead(nil)
		}
		if policy.Mutating {
			if len(result.ContentParts) > 0 || len(result.Embeds) > 0 {
				return fmt.Errorf("durable mutation results must use textual content")
			}
			if p.Receipts == nil {
				p.Receipts = map[string]WorkflowToolReceipt{}
			}
			p.Receipts[inv.IdempotencyKey] = workflowReceipt(result, hash, inv.ToolName)
		}
		return txCtx.Err()
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return result, nil
}
