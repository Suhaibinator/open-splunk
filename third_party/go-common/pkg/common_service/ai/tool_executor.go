package ai

import (
	"context"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"mime"
	"net/url"
	"strconv"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/partdata"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitai "github.com/firebase/genkit/go/ai"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const defaultMaxToolResultBytes = 100 * 1024 * 1024

// Charge fixed serialized metadata for every embed even when its data/text fields are empty. The
// exact database row is larger; this conservative portable charge prevents empty entries from
// bypassing a deliberately small per-tool result limit.
const toolResultEmbedAccountingBytes uint64 = 16

const toolResultContentPartsFieldNumber protowire.Number = 4

// maxProjectedAttachmentID is the longest decimal uint64 reference the transcript attachment
// externalizer can place in a MediaPart. Projecting inline media to this value lets the executor
// validate the durable descriptor without counting the binary bytes that are stored separately.
const maxProjectedAttachmentID = "18446744073709551615"

// Operational execution errors. These are returned with a non-nil err from
// ExecuteClaimedToolCall and indicate the tool did not produce a model-visible result.
// A tool DOMAIN error (one the model should see) is NOT one of these; it is returned as
// a successful *ports.AgentToolResult with IsError=true and a nil err.
var (
	// ErrToolNotRegistered means the claimed tool name is not in the AgentToolRegistry.
	// Terminal: re-running cannot fix a missing registration.
	ErrToolNotRegistered = errors.New("tool executor: tool is not registered")
	// ErrToolNotEnabledInSnapshot means the tool was not enabled in the run's captured
	// session config version. Terminal: the run must use the policy it started with.
	ErrToolNotEnabledInSnapshot = errors.New("tool executor: tool is not enabled in the run's captured session config")
	// ErrToolApprovalNotGranted means the captured session config marks the tool
	// approval-required but the claimed call was never approved. Terminal: re-running cannot
	// grant an approval that a human did not give.
	ErrToolApprovalNotGranted = errors.New("tool executor: tool requires approval and the claimed call was not approved")
	// ErrToolNoExecutionPath means the registered definition has neither a Genkit tool
	// nor an Execute callback. Terminal: it is a registration defect.
	ErrToolNoExecutionPath = errors.New("tool executor: tool has no execution path")
	// ErrToolExecutorNotInitialized means the executor was constructed without a
	// registry.
	ErrToolExecutorNotInitialized = errors.New("tool executor: not initialized")
	// ErrToolIdempotencyUnavailable means a Genkit call has neither an explicit
	// idempotency key nor a globally unique persisted TOOL_CALL message ID. Running
	// that callback would make safe downstream replay protection impossible.
	ErrToolIdempotencyUnavailable = errors.New("tool executor: durable idempotency key unavailable")
	// ErrToolExecutionAttemptsExceeded means a retryable tool execution exhausted the
	// registered tool's MaxExecutionAttempts budget. It is terminal: the durable caller
	// records the executor error on the TOOL_CALL and fails the owning run.
	ErrToolExecutionAttemptsExceeded = ports.ErrToolExecutionAttemptsExceeded
)

// SessionToolConfigReader reads the per-session tool config rows for one captured
// config snapshot version. It is the authoritative source for whether a tool was
// enabled (and whether it required approval) at the moment a run started — the
// executor must consult the captured version, never the session's current mutable
// config. The persistence handler satisfies this via GetChatAgentSessionConfigSnapshot;
// tests supply a fake.
type SessionToolConfigReader interface {
	GetSessionToolConfigs(ctx context.Context, agentSessionID uint64, version uint64) ([]SessionToolConfig, error)
}

// SessionToolConfigReaderFunc adapts a plain function to SessionToolConfigReader so the service
// handler can supply a persistence-backed closure without a named adapter type.
type SessionToolConfigReaderFunc func(ctx context.Context, agentSessionID uint64, version uint64) ([]SessionToolConfig, error)

// GetSessionToolConfigs calls the underlying function.
func (f SessionToolConfigReaderFunc) GetSessionToolConfigs(ctx context.Context, agentSessionID uint64, version uint64) ([]SessionToolConfig, error) {
	return f(ctx, agentSessionID, version)
}

// SessionToolConfig is the executor's minimal view of one captured per-session tool
// row: which tool, whether it was enabled, and whether it required approval in that
// snapshot version. It is intentionally decoupled from the persistence row type so the
// ai package does not import persistence models.
type SessionToolConfig struct {
	ToolName         string
	Enabled          bool
	RequiresApproval bool
}

// ToolCallExecutor is the pure, persistence-free executor for one claimed tool call.
// It resolves the registered tool, verifies the tool was enabled in the run's captured
// session config snapshot, runs the tool (Genkit RunRaw/RunRawMultipart or the app's
// AgentToolExecutor), and maps the outcome to (result, retryable, err). It performs no
// persistence, leasing, or run-state writes; the durable work queue owns those.
//
// The same executor is used inline by the continuation loop for non-approval tools and
// by the approved-work queue for approved tools, so there is exactly one execution path.
type ToolCallExecutor struct {
	registry      ports.AgentToolRegistry
	sessionConfig SessionToolConfigReader
	attachActor   ports.AgentToolActorAttacher
}

// ToolCallExecutor satisfies the ports.ToolCallExecutor seam contract consumed by the
// durable approved-work queue and the inline continuation loop.
var _ ports.ToolCallExecutor = (*ToolCallExecutor)(nil)

// NewToolCallExecutor constructs a ToolCallExecutor. registry is required. sessionConfig
// may be nil only when no claimed call will carry a session-scoped snapshot
// (AgentSessionID == 0); a call that carries a session but has no reader is an
// operational terminal failure. A Genkit execution also requires attachActor so it
// cannot inherit an ambient identity after the explicit invocation is converted to
// Genkit's context-only callback contract. The variadic form preserves source
// compatibility for Execute-only callers; a Genkit call without an attacher fails
// closed at dispatch.
func NewToolCallExecutor(
	registry ports.AgentToolRegistry,
	sessionConfig SessionToolConfigReader,
	actorAttachers ...ports.AgentToolActorAttacher,
) *ToolCallExecutor {
	var attachActor ports.AgentToolActorAttacher
	if len(actorAttachers) > 0 {
		attachActor = actorAttachers[0]
	}
	return &ToolCallExecutor{
		registry:      registry,
		sessionConfig: sessionConfig,
		attachActor:   attachActor,
	}
}

// ExecuteClaimedToolCall runs one claimed tool call. See the ports.ToolCallExecutor
// contract for the (result, retryable, err) semantics:
//   - tool domain error -> (&AgentToolResult{IsError:true}, false, nil)
//   - operational failure -> (nil, retryable, err)
//
// Verification order: resolve the registered tool, revalidate the persisted arguments against the
// registry-owned schema, verify captured enablement and approval policy, then dispatch. Revalidation
// is required for crash
// recovery: a process can die after persisting a fresh inline row but before recording the normal
// schema-error TOOL_RESULT. A tool that is missing from the registry, not enabled in the captured
// snapshot, or not approved when the snapshot requires it is a terminal operational failure
// (retryable=false): re-running the same claimed work cannot change the registry, the
// captured policy, or the row's approval status.
func (e *ToolCallExecutor) ExecuteClaimedToolCall(
	ctx context.Context,
	claimed ports.ClaimedToolCall,
) (*ports.AgentToolResult, bool, error) {
	if e == nil || e.registry == nil {
		return nil, false, ErrToolExecutorNotInitialized
	}
	if claimed.TriggeringUserID == 0 {
		return nil, false, fmt.Errorf("%w: triggering user ID is zero", ports.ErrAgentToolActorUnavailable)
	}

	def, ok := e.registry.GetTool(claimed.ToolName)
	if !ok {
		return nil, false, fmt.Errorf("%w: %q", ErrToolNotRegistered, claimed.ToolName)
	}

	maxAttempts := def.MaxExecutionAttempts
	if maxAttempts == 0 {
		maxAttempts = ports.DefaultAgentToolMaxExecutionAttempts
	}
	attempt := claimed.ExecutionAttemptCount
	if attempt == 0 {
		// Compatibility for direct/legacy callers that predate the durable attempt field. Production
		// claims always carry the exact one-based count.
		attempt = 1
	}
	if attempt > maxAttempts {
		// This can occur when the prior claimant died after starting the final allowed execution but
		// before it could durably settle the result. Do not invoke a possibly side-effecting tool again.
		return nil, false, fmt.Errorf("%w for %q before attempt %d (limit %d)",
			ErrToolExecutionAttemptsExceeded, claimed.ToolName, attempt, maxAttempts)
	}

	violations, schemaErr := validateToolArgs(def.InputSchemaJSON, claimed.ArgumentsJSON)
	if schemaErr != nil {
		return nil, false, fmt.Errorf("%w: %q: %w", ErrToolSchemaInvalid, claimed.ToolName, schemaErr)
	}
	allowUnapprovedInvalidReport := len(violations) > 0 && def.ReportInvalidArguments != nil
	if err := e.verifyCapturedPolicy(ctx, claimed, allowUnapprovedInvalidReport); err != nil {
		// Terminal: the run must honor the policy it captured at start. The sole approval
		// exception is an invalid-argument reporter, which cannot apply the business mutation.
		// Revalidation happens first so a schema change that makes the persisted arguments valid
		// restores the normal approval gate before dispatch.
		return nil, false, err
	}
	if len(violations) > 0 {
		// This is a model-visible domain result, matching the live continuation path. Most calls were
		// already validated before persistence; the repeat check protects manually seeded rows and
		// stale fresh rows recovered after a crash without ever invoking the app/Genkit tool.
		if def.ReportInvalidArguments != nil {
			invocation := withAppExecutorIdempotencyKey(buildToolInvocation(claimed, def.RendererKey))
			result, retryable, err := runAppExecutor(ctx, func(reportCtx context.Context, reportInvocation ports.AgentToolInvocation) (*ports.AgentToolResult, error) {
				if err := def.ReportInvalidArguments(reportCtx, reportInvocation, violations); err != nil {
					return nil, err
				}
				return schemaViolationResult(claimed.ToolName, violations), nil
			}, invocation)
			if err != nil && retryable && attempt >= maxAttempts {
				return nil, false, fmt.Errorf("%w for %q reporting invalid arguments: %w", ErrToolExecutionAttemptsExceeded, claimed.ToolName, err)
			}
			return result, retryable, err
		}
		return schemaViolationResult(claimed.ToolName, violations), false, nil
	}

	invocation := buildToolInvocation(claimed, def.RendererKey)
	result, retryable, err := e.dispatch(ctx, def, invocation)
	if err != nil && retryable && attempt >= maxAttempts {
		// Preserve the executor's concrete error for errors.Is/As and durable diagnostics while
		// making the final allowed failure terminal to both inline and recovery execution lanes.
		return nil, false, fmt.Errorf("%w for %q on attempt %d of %d: %w",
			ErrToolExecutionAttemptsExceeded, claimed.ToolName, attempt, maxAttempts, err)
	}
	return result, retryable, err
}

// verifyCapturedPolicy confirms the claimed call still satisfies the policy the run captured
// at start: the tool was enabled in that snapshot version, and a tool the snapshot marks
// approval-required carries an APPROVED row. Calls with no session scope (AgentSessionID == 0)
// are legacy/manual runs that do not have a per-session allowlist; they are allowed through
// (the registry resolution above already gates them). A session-scoped call with no reader
// configured is an operational terminal failure.
//
// The approval check is defense in depth at the last gate before business execution. Being
// claimable is not evidence of approval: a call recorded WITHOUT a card (approval status
// NOT_REQUIRED) also
// becomes claimable once it carries a last_execution_error, so a row that reached the queue by
// that route would otherwise execute an approval-required tool with no human decision. The
// snapshot's RequiresApproval is the authority here, not the registry's DefaultRequiresApproval:
// the run must honor the policy captured at its start, which is the same rule the enablement
// check follows. allowUnapprovedInvalidReport applies only after current server-owned schema
// validation proved the persisted arguments invalid and a reporter exists; enablement is always
// enforced, and valid arguments still require approval.
func (e *ToolCallExecutor) verifyCapturedPolicy(ctx context.Context, claimed ports.ClaimedToolCall, allowUnapprovedInvalidReport bool) error {
	if claimed.AgentSessionID == 0 {
		return nil
	}
	if e.sessionConfig == nil {
		return fmt.Errorf("%w: session %d has no tool-config reader", ErrToolNotEnabledInSnapshot, claimed.AgentSessionID)
	}
	configs, err := e.capturedToolConfigs(ctx, claimed.AgentSessionID, claimed.AgentSessionConfigVersion)
	if err != nil {
		return fmt.Errorf("tool executor: reading captured session config: %w", err)
	}
	for _, cfg := range configs {
		if cfg.ToolName != claimed.ToolName {
			continue
		}
		if !cfg.Enabled {
			return fmt.Errorf("%w: %q (session %d v%d)", ErrToolNotEnabledInSnapshot, claimed.ToolName, claimed.AgentSessionID, claimed.AgentSessionConfigVersion)
		}
		if cfg.RequiresApproval &&
			claimed.ApprovalStatus != proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_APPROVED &&
			!allowUnapprovedInvalidReport {
			return fmt.Errorf("%w: %q (session %d v%d) is %s", ErrToolApprovalNotGranted, claimed.ToolName, claimed.AgentSessionID, claimed.AgentSessionConfigVersion, claimed.ApprovalStatus)
		}
		return nil
	}
	return fmt.Errorf("%w: %q absent from session %d v%d", ErrToolNotEnabledInSnapshot, claimed.ToolName, claimed.AgentSessionID, claimed.AgentSessionConfigVersion)
}

// dispatch runs the tool via its Genkit tool (RunRaw/RunRawMultipart) or its app
// AgentToolExecutor, and normalizes the outcome. A registration with neither path is a
// terminal defect.
func (e *ToolCallExecutor) dispatch(
	ctx context.Context,
	def ports.AgentToolDefinition,
	invocation ports.AgentToolInvocation,
) (*ports.AgentToolResult, bool, error) {
	limit := def.MaxResultBytes
	if limit <= 0 {
		limit = defaultMaxToolResultBytes
	}
	var (
		result    *ports.AgentToolResult
		retryable bool
		err       error
	)
	switch {
	case def.GenkitTool != nil:
		invocation, err = withGenkitIdempotencyKey(invocation)
		if err != nil {
			return nil, false, err
		}
		result, retryable, err = e.runGenkitTool(ctx, def.GenkitTool, invocation, limit)
	case def.Execute != nil:
		invocation = withAppExecutorIdempotencyKey(invocation)
		result, retryable, err = runAppExecutor(ctx, def.Execute, invocation)
	default:
		return nil, false, fmt.Errorf("%w: %q", ErrToolNoExecutionPath, def.Name)
	}
	if err != nil {
		return result, retryable, err
	}
	if result != nil {
		if err := ports.ValidateToolResultContentPartCount(len(result.ContentParts)); err != nil {
			return nil, false, fmt.Errorf("tool executor: invalid result from %q: %w", def.Name, err)
		}
		if err := ports.ValidateToolResultText(result.Content); err != nil {
			return nil, false, fmt.Errorf("tool executor: invalid result from %q: %w", def.Name, err)
		}
	}
	if err := validateToolResultMediaWithDecodeLimit(result, limit); err != nil {
		return nil, false, fmt.Errorf("tool executor: invalid result from %q: %w", def.Name, err)
	}
	if err := ports.ValidateToolResultEmbeds(result.Embeds); err != nil {
		return nil, false, fmt.Errorf("tool executor: invalid result from %q: %w", def.Name, err)
	}
	declaredAttachmentBytes, err := validateToolResultAttachmentByteBudget(result)
	if err != nil {
		return nil, false, fmt.Errorf("tool executor: result from %q cannot be persisted: %w", def.Name, err)
	}
	size, ok := toolResultSize(result)
	if !ok {
		return nil, false, fmt.Errorf("tool executor: result from %q exceeds size accounting range", def.Name)
	}
	// Inline bytes are already present in the serialized content part. Attachment-backed media
	// carries only a short reference in that descriptor, so charge its declared durable byte size
	// separately against the tool-specific result ceiling.
	size, ok = addToolResultSize(size, declaredAttachmentBytes)
	if !ok {
		return nil, false, fmt.Errorf("tool executor: result from %q exceeds size accounting range", def.Name)
	}
	if size > uint64(limit) {
		return nil, false, fmt.Errorf("tool executor: result from %q is %d bytes, exceeding limit %d", def.Name, size, limit)
	}
	if err := validateToolResultTranscriptDescriptor(invocation.ToolCallID, result); err != nil {
		// Descriptor shape is deterministic for this completed result. Re-executing a side-effecting
		// tool cannot make it persistable, so this failure is always terminal.
		return nil, false, fmt.Errorf("tool executor: result from %q cannot be persisted: %w", def.Name, err)
	}
	return result, retryable, nil
}

// validateToolResultAttachmentByteBudget mirrors the attachment externalizer's preflight for both
// inline media and already-externalized attachment references. It returns only the referenced
// attachment bytes because inline bytes are already charged by toolResultSize's protobuf size.
func validateToolResultAttachmentByteBudget(result *ports.AgentToolResult) (uint64, error) {
	if result == nil {
		return 0, nil
	}
	var total, declaredAttachmentBytes uint64
	for partIndex, part := range result.ContentParts {
		if part == nil || part.GetMedia() == nil {
			continue
		}
		media := part.GetMedia()
		var size uint64
		switch media.GetSource().(type) {
		case *proto_models.MediaPart_InlineData:
			size = uint64(len(media.GetInlineData()))
		case *proto_models.MediaPart_AttachmentId:
			size = media.GetSizeBytes()
			if size == 0 {
				return 0, fmt.Errorf("tool media part %d has no declared attachment size", partIndex)
			}
			var ok bool
			declaredAttachmentBytes, ok = addToolResultSize(declaredAttachmentBytes, size)
			if !ok {
				return 0, fmt.Errorf("combined tool media byte count overflow")
			}
		default:
			continue
		}
		isInputFile := !strings.HasPrefix(strings.ToLower(strings.TrimSpace(media.GetContentType())), "image/")
		if isInputFile && size >= ports.MaxInternalAttachmentHydrationBytes {
			return 0, fmt.Errorf("non-image tool media part %d must be smaller than %d bytes", partIndex, ports.MaxInternalAttachmentHydrationBytes)
		}
		if size > ports.MaxInternalAttachmentHydrationBytes || total > ports.MaxInternalAttachmentHydrationBytes-size {
			return 0, fmt.Errorf("combined tool media exceeds %d bytes", ports.MaxInternalAttachmentHydrationBytes)
		}
		total += size
	}
	return declaredAttachmentBytes, nil
}

func toolResultSize(result *ports.AgentToolResult) (uint64, bool) {
	if result == nil {
		return 0, true
	}
	var size uint64
	var ok bool
	if size, ok = addToolResultSize(size, uint64(len(result.ContentJSON))); !ok {
		return 0, false
	}
	if size, ok = addToolResultSize(size, uint64(len(result.Content))); !ok {
		return 0, false
	}
	for _, part := range result.ContentParts {
		partSize := 0
		if part != nil {
			partSize = proto.Size(part)
		}
		// ContentParts is field 4 of ToolResultPayload. Include the exact outer tag and length
		// prefix even for nil/empty entries, which protobuf preserves as zero-length messages.
		framing := uint64(protowire.SizeTag(toolResultContentPartsFieldNumber) + protowire.SizeVarint(uint64(partSize)))
		if size, ok = addToolResultSize(size, framing); !ok {
			return 0, false
		}
		if partSize > 0 {
			if size, ok = addToolResultSize(size, uint64(partSize)); !ok {
				return 0, false
			}
		}
	}
	for _, embed := range result.Embeds {
		if size, ok = addToolResultSize(size, toolResultEmbedAccountingBytes); !ok {
			return 0, false
		}
		if embed == nil {
			continue
		}
		if size, ok = addToolResultSize(size, uint64(len(embed.EmbedData))); !ok {
			return 0, false
		}
		if size, ok = addToolResultSize(size, uint64(len(embed.FallbackText))); !ok {
			return 0, false
		}
	}
	return size, true
}

func addToolResultSize(total, value uint64) (uint64, bool) {
	if value > ^uint64(0)-total {
		return 0, false
	}
	return total + value, true
}

// validateToolResultTranscriptDescriptor enforces the transcript's structured-payload limit at
// the execution boundary shared by inline and approval-resume work. Inline media is projected to
// its externalized attachment reference; JSON, text, data, URLs, and metadata retain their durable
// encoded size. This lets large binary media proceed to synchronous externalization while rejecting
// non-media results that could otherwise run repeatedly and fail the same 256 KiB transcript write.
func validateToolResultTranscriptDescriptor(toolCallID string, result *ports.AgentToolResult) error {
	if result == nil {
		return nil
	}
	if err := ports.ValidateToolResultPayload(result.ContentJSON, nil); err != nil {
		return err
	}
	parts := make([]*proto_models.MessagePart, len(result.ContentParts))
	for i, part := range result.ContentParts {
		if part == nil {
			continue
		}
		cloned := proto.Clone(part).(*proto_models.MessagePart)
		if media := cloned.GetMedia(); media != nil && len(media.GetInlineData()) > 0 {
			// Externalization persists the trusted byte size beside the attachment ID. Project the
			// largest possible varint before execution so adding size_bytes cannot push a descriptor
			// that passed here over the durable transcript cap after the tool side effect.
			media.SizeBytes = ^uint64(0)
			media.Source = &proto_models.MediaPart_AttachmentId{AttachmentId: maxProjectedAttachmentID}
			// Externalization fills an empty name with a deterministic filename. Include that
			// addition in the projection so a descriptor at the limit cannot pass here and grow
			// past the cap only after the tool's side effect has completed.
			if strings.TrimSpace(media.Name) == "" {
				media.Name = projectedToolMediaFilename(media.ContentType, i)
			}
		}
		parts[i] = cloned
	}
	descriptor := &proto_models.MessageModelPayload{
		Payload: &proto_models.MessageModelPayload_ToolResult{
			ToolResult: &proto_models.ToolResultPayload{
				ToolCallId:   toolCallID,
				ContentJson:  string(result.ContentJSON),
				IsError:      result.IsError,
				ContentParts: parts,
			},
		},
	}
	if proto.Size(descriptor) > ports.MaxTranscriptModelPayloadBytes {
		return ports.ErrToolResultPayloadTooLarge
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(descriptor)
	if err != nil {
		return fmt.Errorf("tool result transcript descriptor is not serializable: %w", err)
	}
	if len(encoded) > ports.MaxTranscriptModelPayloadBytes {
		return ports.ErrToolResultPayloadTooLarge
	}
	return nil
}

func projectedToolMediaFilename(contentType string, partIndex int) string {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil {
		mediaType = ""
	}
	ext := ""
	if extensions, err := mime.ExtensionsByType(strings.ToLower(mediaType)); err == nil && len(extensions) > 0 {
		ext = extensions[0]
	}
	return fmt.Sprintf("tool-output-%d%s", partIndex, ext)
}

// runGenkitTool executes a Genkit tool with the persisted JSON arguments via
// RunRawMultipart (which preserves multipart content parts for provider round-trips)
// and maps the multipart response to an AgentToolResult. The complete persisted
// invocation is attached to the callback context so Genkit tools can use a stable
// idempotency key, matching the capability available to app executors.
func (e *ToolCallExecutor) runGenkitTool(
	ctx context.Context,
	tool genkitai.Tool,
	invocation ports.AgentToolInvocation,
	resultLimit int,
) (result *ports.AgentToolResult, retryable bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			retryable = false
			err = ports.NewAgentToolPanicError(invocation.ToolName, recovered)
		}
	}()
	if e.attachActor == nil {
		return nil, false, fmt.Errorf("%w: no actor attacher configured for Genkit tool %q", ports.ErrAgentToolActorUnavailable, invocation.ToolName)
	}
	toolCtx, attachErr := e.attachActor(ctx, invocation.TriggeringUserID)
	if attachErr != nil {
		return nil, !errors.Is(attachErr, ports.ErrAgentToolActorUnavailable),
			fmt.Errorf("tool executor: attaching triggering actor for Genkit tool %q: %w", invocation.ToolName, attachErr)
	}
	if toolCtx == nil {
		return nil, false, fmt.Errorf("%w: actor attacher returned a nil context for Genkit tool %q", ports.ErrAgentToolActorUnavailable, invocation.ToolName)
	}
	toolCtx = ports.WithAgentToolInvocation(toolCtx, invocation)
	input := toolInputFromArguments(invocation.ArgumentsJSON)
	resp, runErr := tool.RunRawMultipart(toolCtx, input)
	if runErr != nil {
		if errors.Is(runErr, ports.ErrAgentToolPanicked) {
			return nil, false, fmt.Errorf("tool executor: genkit tool %q panicked: %w", invocation.ToolName, runErr)
		}
		// Genkit/tool transport failure. Treat as retryable: a transient provider/tool
		// error may succeed on a reclaim. A tool that wants to report a permanent domain
		// error to the model should return a value with an error payload, not panic.
		return nil, true, fmt.Errorf("tool executor: genkit tool %q failed: %w", invocation.ToolName, runErr)
	}
	result, err = multipartToolResponseToResultWithDecodeLimit(resp, resultLimit)
	if err != nil {
		return nil, false, fmt.Errorf("tool executor: genkit tool %q returned invalid multipart output: %w", invocation.ToolName, err)
	}
	return result, false, nil
}

// runAppExecutor invokes an app-registered AgentToolExecutor and maps its
// (result, error) to the executor contract. A non-nil error from the app executor is an
// operational failure; the app expresses a model-visible domain error as
// AgentToolResult{IsError:true}, nil. The invocation is also attached to the context, as on
// the Genkit path, so helpers deeper in the executor's call graph can read it without being
// handed the invocation explicitly.
func runAppExecutor(
	ctx context.Context,
	execute ports.AgentToolExecutor,
	invocation ports.AgentToolInvocation,
) (result *ports.AgentToolResult, retryable bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			retryable = false
			err = ports.NewAgentToolPanicError(invocation.ToolName, recovered)
		}
	}()

	result, execErr := execute(ports.WithAgentToolInvocation(ctx, invocation), invocation)
	if execErr != nil {
		if errors.Is(execErr, ports.ErrAgentToolPanicked) {
			return nil, false, fmt.Errorf("tool executor: app executor for %q panicked: %w", invocation.ToolName, execErr)
		}
		if errors.Is(execErr, ports.ErrAgentToolActorUnavailable) {
			return nil, false, fmt.Errorf("tool executor: app executor for %q has no triggering actor: %w", invocation.ToolName, execErr)
		}
		// Unknown operational failures remain retryable for compatibility. Apps can return a
		// ports.AgentToolExecutionError (or any wrapped error implementing Retryable() bool) to
		// classify deterministic failures as terminal without turning them into model-visible results.
		return nil, ports.AgentToolExecutionRetryable(execErr), fmt.Errorf("tool executor: app executor for %q failed: %w", invocation.ToolName, execErr)
	}
	if result == nil {
		// A nil result with a nil error is a defect; surface it rather than recording an
		// empty TOOL_RESULT.
		return nil, false, fmt.Errorf("tool executor: app executor for %q returned no result", invocation.ToolName)
	}
	return result, false, nil
}

// buildToolInvocation converts a ClaimedToolCall into the AgentToolInvocation handed to
// the tool. The idempotency key falls back to the globally unique durable TOOL_CALL
// message id. ProviderID call refs are unique only within one response/run and therefore
// cannot safely key a downstream idempotency store shared by users or runs. The scope is
// cloned: every call of the session shares it, and one tool must not change the next one's.
func buildToolInvocation(claimed ports.ClaimedToolCall, rendererKey string) ports.AgentToolInvocation {
	return ports.AgentToolInvocation{
		ToolName:                  claimed.ToolName,
		ToolCallID:                claimed.ToolCallID,
		ToolCallMessageID:         claimed.ToolCallMessageID,
		AgentRunID:                claimed.AgentRunID,
		AgentSessionID:            claimed.AgentSessionID,
		AgentSessionConfigVersion: claimed.AgentSessionConfigVersion,
		ChannelID:                 claimed.ChannelID,
		ParentMessageID:           claimed.ParentMessageID,
		TriggeringUserID:          claimed.TriggeringUserID,
		BotUserID:                 claimed.BotUserID,
		OrganizationID:            claimed.OrganizationID,
		AgentType:                 claimed.AgentType,
		Scope:                     claimed.Scope.Clone(),
		ArgumentsJSON:             claimed.ArgumentsJSON,
		RendererKey:               rendererKey,
		IdempotencyKey:            claimed.IdempotencyKey,
	}
}

// withAppExecutorIdempotencyKey preserves the released app-executor contract exactly:
// explicit key, then provider ToolCallID, then persisted TOOL_CALL message ID. Changing
// this order would give an in-flight call a new key after deploy and could replay a
// side effect during lease reclaim.
func withAppExecutorIdempotencyKey(invocation ports.AgentToolInvocation) ports.AgentToolInvocation {
	if invocation.IdempotencyKey == "" {
		invocation.IdempotencyKey = invocation.ToolCallID
	}
	if invocation.IdempotencyKey == "" {
		invocation.IdempotencyKey = fmt.Sprintf("tool-call-message-%d", invocation.ToolCallMessageID)
	}
	return invocation
}

// withGenkitIdempotencyKey derives the newly exposed Genkit callback key from the
// globally unique persisted TOOL_CALL message ID. ProviderID ToolCallID values are only
// run-scoped and are unsafe as downstream keys. No prior Genkit callback key contract
// exists to preserve.
func withGenkitIdempotencyKey(invocation ports.AgentToolInvocation) (ports.AgentToolInvocation, error) {
	if invocation.IdempotencyKey != "" {
		return invocation, nil
	}
	if invocation.ToolCallMessageID == 0 {
		return ports.AgentToolInvocation{}, fmt.Errorf(
			"%w: Genkit tool %q has no explicit key or persisted TOOL_CALL message ID",
			ErrToolIdempotencyUnavailable,
			invocation.ToolName,
		)
	}
	invocation.IdempotencyKey = fmt.Sprintf("tool-call-message-%d", invocation.ToolCallMessageID)
	return invocation, nil
}

// toolInputFromArguments decodes the persisted JSON arguments into the map/any shape
// Genkit's RunRaw/RunRawMultipart expect. Invalid or empty JSON yields nil, which
// Genkit treats as an empty input object.
func toolInputFromArguments(argumentsJSON []byte) any {
	if len(argumentsJSON) == 0 {
		return nil
	}
	return decodeJSONArguments(string(argumentsJSON))
}

// multipartToolResponseToResult maps a Genkit MultipartToolResponse to an
// AgentToolResult, carrying the structured output as ContentJSON and any multipart
// content parts through ContentParts so they survive provider round-trips.
func multipartToolResponseToResult(resp *genkitai.MultipartToolResponse) (*ports.AgentToolResult, error) {
	return multipartToolResponseToResultWithDecodeLimit(resp, defaultMaxToolResultBytes)
}

func multipartToolResponseToResultWithDecodeLimit(resp *genkitai.MultipartToolResponse, resultLimit int) (*ports.AgentToolResult, error) {
	if resp == nil {
		return &ports.AgentToolResult{}, nil
	}
	if err := ports.ValidateToolResultContentPartCount(len(resp.Content)); err != nil {
		return nil, err
	}
	contentParts, err := messagePartsFromGenkitPartsWithDecodeBudget(resp.Content, newInlineMediaDecodeBudget(resultLimit))
	if err != nil {
		return nil, err
	}
	result := &ports.AgentToolResult{
		ContentParts: contentParts,
	}
	if resp.Output != nil {
		encoded, err := json.Marshal(resp.Output)
		if err != nil {
			return nil, fmt.Errorf("encode structured tool output: %w", err)
		}
		result.ContentJSON = encoded
	}
	return result, nil
}

func messagePartsFromGenkitPartsWithDecodeBudget(parts []*genkitai.Part, decodeBudget *inlineMediaDecodeBudget) ([]*proto_models.MessagePart, error) {
	if len(parts) == 0 {
		return nil, nil
	}
	out := make([]*proto_models.MessagePart, 0, len(parts))
	for i, part := range parts {
		if part == nil {
			continue
		}
		switch {
		case part.IsText():
			if strings.TrimSpace(part.Text) == "" {
				continue
			}
			converted := &proto_models.MessagePart{
				Part: &proto_models.MessagePart_Text{Text: &proto_models.TextPart{Text: part.Text}},
			}
			if err := decodeBudget.reserveConvertedPart(converted, 0); err != nil {
				return nil, fmt.Errorf("content part %d: %w", i, err)
			}
			out = append(out, converted)
		case part.IsMedia():
			mediaPart, err := mediaPartFromGenkitPartWithDecodeBudget(part, decodeBudget)
			if err != nil {
				return nil, fmt.Errorf("content part %d: %w", i, err)
			}
			if mediaPart != nil {
				converted := &proto_models.MessagePart{
					Part: &proto_models.MessagePart_Media{Media: mediaPart},
				}
				if err := decodeBudget.reserveConvertedPart(converted, uint64(len(mediaPart.GetInlineData()))); err != nil {
					return nil, fmt.Errorf("content part %d: %w", i, err)
				}
				out = append(out, converted)
			}
		case part.IsData():
			dataPart, err := dataPartFromGenkitPartWithDecodeBudget(part, decodeBudget)
			if err != nil {
				return nil, fmt.Errorf("content part %d: %w", i, err)
			}
			out = append(out, &proto_models.MessagePart{
				Part: &proto_models.MessagePart_Data{Data: dataPart},
			})
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// dataPartFromGenkitPartWithDecodeBudget maps a Genkit data part to a durable proto DataPart.
// Genkit defines the data: URI as the one encoded form of a data part and every provider adapter
// decodes it before sending, so it is decoded here too: the durable payload then equals what the
// same part would have produced live. Any other string, including HTTP- or file-ID-looking text,
// is literal bytes. Structured values arrive as their JSON representation.
func dataPartFromGenkitPartWithDecodeBudget(part *genkitai.Part, decodeBudget *inlineMediaDecodeBudget) (*proto_models.DataPart, error) {
	data, err := partdata.String(part)
	if err != nil {
		return nil, err
	}
	contentType := partdata.ContentType(part)
	ref := strings.TrimSpace(data)
	if !common_util.HasDataURIScheme(ref) {
		if err := decodeBudget.reserveDataPart(contentType, len(data)); err != nil {
			return nil, err
		}
		return &proto_models.DataPart{ContentType: contentType, Data: []byte(data)}, nil
	}
	mediaType, encodedPayload, ok := parseBase64DataURI(ref)
	if !ok {
		return nil, fmt.Errorf("malformed base64 data URI data payload")
	}
	if contentType == "" {
		contentType = mediaType
	} else {
		declared, err := canonicalToolResultMediaType(contentType)
		if err != nil {
			return nil, err
		}
		if declared != mediaType {
			return nil, fmt.Errorf("data URI media type %q does not match part content type %q", mediaType, declared)
		}
	}
	size, err := base64DataURIDecodedSize(ref)
	if err != nil {
		return nil, err
	}
	// Reserve against the decoded size before allocating it so an oversized payload fails
	// without the copy, matching the raw-string path.
	if err := decodeBudget.reserveDataPart(contentType, int(size)); err != nil {
		return nil, err
	}
	decoded, err := base64.StdEncoding.DecodeString(encodedPayload)
	if err != nil {
		return nil, fmt.Errorf("malformed base64 data URI data payload: %w", err)
	}
	return &proto_models.DataPart{ContentType: contentType, Data: decoded}, nil
}

// mediaPartFromGenkitPart maps a Genkit media part to a proto MediaPart. Genkit carries
// the media reference in Part.Text as either a data: URI (inline base64) or a plain URL.
// Inline data URIs are decoded to bytes; everything else is treated as a URL reference.
func mediaPartFromGenkitPart(part *genkitai.Part) (*proto_models.MediaPart, error) {
	return mediaPartFromGenkitPartWithDecodeBudget(part, newInlineMediaDecodeBudget(defaultMaxToolResultBytes))
}

func mediaPartFromGenkitPartWithDecodeBudget(part *genkitai.Part, decodeBudget *inlineMediaDecodeBudget) (*proto_models.MediaPart, error) {
	ref := strings.TrimSpace(part.Text)
	if ref == "" {
		return nil, nil
	}
	contentType, err := canonicalToolResultMediaType(part.ContentType)
	if err != nil {
		return nil, err
	}
	media := &proto_models.MediaPart{ContentType: contentType}
	for _, key := range []string{"filename", "name"} {
		if name, ok := part.Metadata[key].(string); ok && strings.TrimSpace(name) != "" {
			media.Name = strings.TrimSpace(name)
			break
		}
	}
	if detail, ok := part.Metadata["detail"].(string); ok {
		media.Detail = strings.TrimSpace(detail)
		if err := validateMediaDetail(media.GetContentType(), media.GetDetail()); err != nil {
			return nil, err
		}
	}
	if common_util.HasDataURIScheme(ref) {
		if err := decodeBudget.reserveDataURI(ref); err != nil {
			return nil, err
		}
		data, dataType, ok := decodeDataURIBytes(ref)
		if !ok {
			return nil, fmt.Errorf("malformed base64 data URI media source")
		}
		if dataType != contentType {
			return nil, fmt.Errorf("data URI media type %q does not match part content type %q", dataType, contentType)
		}
		media.Source = &proto_models.MediaPart_InlineData{InlineData: data}
		return media, nil
	}
	if err := validateDurableToolMediaReference(ref); err != nil {
		return nil, err
	}
	media.Source = &proto_models.MediaPart_Url{Url: ref}
	return media, nil
}

func validateToolResultMediaWithDecodeLimit(result *ports.AgentToolResult, resultLimit int) error {
	if result == nil {
		return nil
	}
	decodeBudget := newInlineMediaDecodeBudget(resultLimit)
	for i, part := range result.ContentParts {
		if part == nil || part.GetMedia() == nil {
			continue
		}
		media := part.GetMedia()
		contentType, err := canonicalToolResultMediaType(media.GetContentType())
		if err != nil {
			return fmt.Errorf("content part %d: %w", i, err)
		}
		media.ContentType = contentType
		media.Detail = strings.TrimSpace(media.GetDetail())
		if err := validateMediaDetail(media.GetContentType(), media.GetDetail()); err != nil {
			return fmt.Errorf("content part %d: %w", i, err)
		}
		switch source := media.GetSource().(type) {
		case *proto_models.MediaPart_InlineData:
			if len(source.InlineData) == 0 {
				return fmt.Errorf("content part %d: inline media source is empty", i)
			}
		case *proto_models.MediaPart_AttachmentId:
			attachmentID, parseErr := strconv.ParseUint(strings.TrimSpace(source.AttachmentId), 10, 64)
			if parseErr != nil || attachmentID == 0 || media.GetSizeBytes() == 0 {
				return fmt.Errorf("content part %d: invalid internal attachment reference", i)
			}
		case *proto_models.MediaPart_Url:
			ref := strings.TrimSpace(source.Url)
			if common_util.HasDataURIScheme(ref) {
				if err := decodeBudget.reserveDataURI(ref); err != nil {
					return fmt.Errorf("content part %d: %w", i, err)
				}
				data, dataType, ok := decodeDataURIBytes(ref)
				if !ok {
					return fmt.Errorf("content part %d: malformed base64 data URI media source", i)
				}
				if dataType != contentType {
					return fmt.Errorf("content part %d: data URI media type %q does not match part content type %q", i, dataType, contentType)
				}
				media.Source = &proto_models.MediaPart_InlineData{InlineData: data}
				media.SizeBytes = 0
				continue
			}
			if err := validateDurableToolMediaReference(ref); err != nil {
				return fmt.Errorf("content part %d: %w", i, err)
			}
			if media.GetSizeBytes() != 0 {
				return fmt.Errorf("content part %d: external media reference cannot declare internal attachment size", i)
			}
			source.Url = ref
		default:
			return fmt.Errorf("content part %d: media source is missing", i)
		}
	}
	return nil
}

// inlineMediaDecodeBudget bounds allocations performed by this adapter while normalizing
// tool-returned data URIs. The callback already owns the encoded string, but decoding it creates a
// second byte slice; enforce both the tool's configured result ceiling and the aggregate internal
// attachment ceiling before that allocation occurs.
type inlineMediaDecodeBudget struct {
	remainingInlineMedia uint64
	remainingResult      uint64
}

func newInlineMediaDecodeBudget(resultLimit int) *inlineMediaDecodeBudget {
	resultBytes := uint64(0)
	if resultLimit > 0 {
		resultBytes = uint64(resultLimit)
	}
	inlineMediaBytes := min(resultBytes, ports.MaxInternalAttachmentHydrationBytes)
	return &inlineMediaDecodeBudget{
		remainingInlineMedia: inlineMediaBytes,
		remainingResult:      resultBytes,
	}
}

func (b *inlineMediaDecodeBudget) reserveDataURI(dataURI string) error {
	size, err := base64DataURIDecodedSize(dataURI)
	if err != nil {
		return err
	}
	if b == nil || size > b.remainingInlineMedia || size > b.remainingResult {
		remaining := uint64(0)
		if b != nil {
			remaining = min(b.remainingResult, b.remainingInlineMedia)
		}
		return fmt.Errorf("decoded inline media is %d bytes, exceeding the remaining pre-decode limit %d", size, remaining)
	}
	b.remainingInlineMedia -= size
	b.remainingResult -= size
	return nil
}

// reserveDataPart charges the exact ToolResultPayload.content_parts wire footprint before
// converting the callback-owned string to []byte. This prevents a low MaxResultBytes setting from
// allowing a large duplicate allocation that would only be rejected by dispatch after conversion.
func (b *inlineMediaDecodeBudget) reserveDataPart(contentType string, dataBytes int) error {
	size, ok := encodedToolResultDataPartSize(len(contentType), dataBytes)
	if !ok {
		return fmt.Errorf("data content part exceeds size accounting range")
	}
	remaining := uint64(0)
	if b != nil {
		remaining = b.remainingResult
	}
	if b == nil || size > remaining {
		return fmt.Errorf("data content part is %d encoded bytes, exceeding the remaining pre-copy result limit %d", size, remaining)
	}
	b.remainingResult -= size
	return nil
}

// reserveConvertedPart charges a non-data part's exact eventual repeated-field footprint. Inline
// media bytes were reserved before base64 decoding, so only their framing and metadata are charged
// here; this keeps aggregate result accounting exact without double counting the allocation.
func (b *inlineMediaDecodeBudget) reserveConvertedPart(part *proto_models.MessagePart, alreadyReserved uint64) error {
	size := encodedToolResultContentPartSize(part)
	if size < alreadyReserved {
		return fmt.Errorf("content part exceeds size accounting range")
	}
	additional := size - alreadyReserved
	remaining := uint64(0)
	if b != nil {
		remaining = b.remainingResult
	}
	if b == nil || additional > remaining {
		available, ok := addToolResultSize(remaining, alreadyReserved)
		if !ok {
			available = ^uint64(0)
		}
		return fmt.Errorf("content part is %d encoded bytes, exceeding the remaining result limit %d", size, available)
	}
	b.remainingResult -= additional
	return nil
}

func encodedToolResultContentPartSize(part *proto_models.MessagePart) uint64 {
	partSize := 0
	if part != nil {
		partSize = proto.Size(part)
	}
	return uint64(protowire.SizeTag(toolResultContentPartsFieldNumber)+protowire.SizeVarint(uint64(partSize))) + uint64(partSize)
}

// encodedToolResultDataPartSize computes the exact nested protobuf footprint using lengths only:
// DataPart fields, MessagePart.data, then ToolResultPayload.content_parts. It deliberately avoids
// constructing the []byte field whose allocation this preflight protects.
func encodedToolResultDataPartSize(contentTypeBytes, dataBytes int) (uint64, bool) {
	if contentTypeBytes < 0 || dataBytes < 0 {
		return 0, false
	}
	var dataPartSize uint64
	if contentTypeBytes > 0 {
		fieldSize, ok := encodedLengthDelimitedFieldSize(1, uint64(contentTypeBytes))
		if !ok {
			return 0, false
		}
		dataPartSize = fieldSize
	}
	if dataBytes > 0 {
		fieldSize, ok := encodedLengthDelimitedFieldSize(2, uint64(dataBytes))
		if !ok {
			return 0, false
		}
		if dataPartSize, ok = addToolResultSize(dataPartSize, fieldSize); !ok {
			return 0, false
		}
	}
	messagePartSize, ok := encodedLengthDelimitedFieldSize(3, dataPartSize)
	if !ok {
		return 0, false
	}
	return encodedLengthDelimitedFieldSize(toolResultContentPartsFieldNumber, messagePartSize)
}

func encodedLengthDelimitedFieldSize(fieldNumber protowire.Number, payloadBytes uint64) (uint64, bool) {
	overhead := uint64(protowire.SizeTag(fieldNumber) + protowire.SizeVarint(payloadBytes))
	return addToolResultSize(overhead, payloadBytes)
}

// base64DataURIDecodedSize validates the allocation-relevant shape of a data URI and computes its
// exact decoded length without allocating the decoded payload. decodeDataURIBytes performs the
// complete MIME/base64 validation after this preflight passes.
func base64DataURIDecodedSize(dataURI string) (uint64, error) {
	comma := strings.IndexByte(dataURI, ',')
	if comma < 0 || !common_util.HasDataURIScheme(dataURI) {
		return 0, fmt.Errorf("malformed base64 data URI media source")
	}
	const maxDataURIDescriptorBytes = 4096
	if comma-len("data:") > maxDataURIDescriptorBytes {
		return 0, fmt.Errorf("malformed base64 data URI media descriptor is too large")
	}
	descriptor := dataURI[len("data:"):comma]
	if !strings.HasSuffix(strings.ToLower(descriptor), ";base64") {
		return 0, fmt.Errorf("malformed base64 data URI media source")
	}
	payload := dataURI[comma+1:]
	if payload == "" || len(payload)%4 != 0 || strings.ContainsAny(payload, " \t\r\n") {
		return 0, fmt.Errorf("malformed base64 data URI media source")
	}
	padding := 0
	if strings.HasSuffix(payload, "=") {
		padding = 1
		if strings.HasSuffix(payload, "==") {
			padding = 2
		}
	}
	decoded := uint64(len(payload)/4*3 - padding)
	if decoded == 0 {
		return 0, fmt.Errorf("malformed base64 data URI media source")
	}
	return decoded, nil
}

func canonicalToolResultMediaType(contentType string) (string, error) {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil || mediaType == "" {
		return "", fmt.Errorf("invalid media content type %q", contentType)
	}
	return strings.ToLower(mediaType), nil
}

// validateDurableToolMediaReference accepts only references that can be replayed by a provider
// after the tool invocation has completed. data: references are handled separately and normalized
// to inline bytes before this function is called.
func validateDurableToolMediaReference(ref string) error {
	ref = strings.TrimSpace(ref)
	if strings.HasPrefix(ref, "file-") && len(ref) > len("file-") && !strings.ContainsAny(ref, " \t\r\n") {
		return nil
	}
	parsed, err := url.Parse(ref)
	if err == nil && (strings.EqualFold(parsed.Scheme, "http") || strings.EqualFold(parsed.Scheme, "https")) && parsed.Host != "" {
		return nil
	}
	return fmt.Errorf("media source must be a data URI, file ID, or absolute HTTP(S) URL")
}

func validateMediaDetail(contentType, detail string) error {
	if detail == "" {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil || mediaType == "" {
		return fmt.Errorf("media detail %q requires a valid image or PDF content type, got %q", detail, contentType)
	}
	mediaType = strings.ToLower(mediaType)
	if strings.HasPrefix(mediaType, "image/") {
		switch detail {
		case "auto", "low", "high", "original":
			return nil
		default:
			return fmt.Errorf("unsupported image detail %q", detail)
		}
	}
	if mediaType == "application/pdf" {
		switch detail {
		case "auto", "low", "high":
			return nil
		default:
			return fmt.Errorf("unsupported PDF detail %q", detail)
		}
	}
	return fmt.Errorf("media detail %q requires an image or PDF content type, got %q", detail, contentType)
}

// decodeDataURIBytes extracts the raw bytes from a base64 data: URI. It returns false
// for a non-base64, empty, or malformed data URI; callers must reject that source rather
// than persisting it as though it were a remotely fetchable URL.
func decodeDataURIBytes(dataURI string) ([]byte, string, bool) {
	mediaType, payload, ok := parseBase64DataURI(dataURI)
	if !ok {
		return nil, "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil || len(decoded) == 0 {
		return nil, "", false
	}
	return decoded, mediaType, true
}

// parseBase64DataURI splits a base64 data: URI into its canonical lowercase media type and the
// still-encoded payload without decoding it, so callers can reserve budget before allocating.
func parseBase64DataURI(dataURI string) (mediaType, payload string, ok bool) {
	comma := strings.IndexByte(dataURI, ',')
	if comma < 0 || !common_util.HasDataURIScheme(dataURI) {
		return "", "", false
	}
	const maxDataURIDescriptorBytes = 4096
	if comma-len("data:") > maxDataURIDescriptorBytes {
		return "", "", false
	}
	descriptor, payload := dataURI[len("data:"):comma], dataURI[comma+1:]
	segments := strings.Split(descriptor, ";")
	if len(segments) < 2 || !strings.EqualFold(segments[len(segments)-1], "base64") {
		return "", "", false
	}
	for _, parameter := range segments[1 : len(segments)-1] {
		if parameter == "" || strings.EqualFold(parameter, "base64") {
			return "", "", false
		}
	}
	mediaDescriptor := strings.Join(segments[:len(segments)-1], ";")
	if segments[0] == "" {
		mediaDescriptor = "text/plain" + mediaDescriptor
	}
	parsed, _, err := mime.ParseMediaType(mediaDescriptor)
	if err != nil || parsed == "" {
		return "", "", false
	}
	return strings.ToLower(parsed), payload, true
}
