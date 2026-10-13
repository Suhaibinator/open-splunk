package bot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	attachmentpkg "github.com/Suhaibinator/go-common/pkg/common_service/attachment"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	// maxTranscriptModelPayloadBytes caps the serialized tool-call/result payload. Tool
	// arguments/results are descriptors, not bulk data; large media should be a stored attachment or
	// URL, not inlined here.
	maxTranscriptModelPayloadBytes = ports.MaxTranscriptModelPayloadBytes

	// defaultApprovalExpiry is the fallback pending-approval lifetime when the agent's approval
	// policy does not set one (Decision 6, 1 hour).
	defaultApprovalExpiry = time.Hour

	// A stream delta briefly holds the run row while handing the ephemeral event to the publisher.
	// Keep that critical section far below the drive heartbeat/lease windows. A publisher that
	// honors ctx cannot retain the lock beyond this bound; a broken custom publisher is detached at
	// the deadline so it cannot turn websocket fanout into a run/takeover availability failure.
	defaultAssistantStreamPublishFenceTimeout = 2 * time.Second
	defaultMaxAssistantStreamPublishHandoffs  = 32
)

// agentTranscriptWriter is the single implementation of ports.AgentTranscriptWriter. The agent
// runtime uses it for both buffered and streamed assistant output, so the RecordToolCall /
// RecordToolResult surface comes from one source.
//
// It is the authority-bearing write path (Decision 5): it applies per-kind visibility defaults,
// validates payload bytes/JSON, assembles the structured proto payload, and inserts the transcript
// row + its tool-call side row through the persistence CreateTranscriptMessage primitive, which is
// idempotent on (agent_run_id, tool_call_id). A MESSAGE_SENT websocket event is fanned out ONLY on a
// real insert (created==true), so an idempotent crash-resume retry does not emit a duplicate
// transcript event.
type agentTranscriptWriter[U common_service_models.UserLike] struct {
	ctx context.Context
	// Non-nil only on the transaction-local writer used by RecordRejectedToolCall.
	rejectedPublishes *[]uint64
	d                 *AsyncDispatcher[U]

	// channel/botUserID/actorUserID are the run context every row is scoped to: the channel the
	// transcript belongs to, the agent's bot user (row sender), and the triggering user stamped as
	// the approver-eligible actor on an approval-required TOOL_CALL.
	channel     *common_service_models.ChatChannel
	botUserID   common_service_models.UserIdType
	actorUserID common_service_models.UserIdType

	streamMu              sync.Mutex
	streamStarted         bool
	streamTerminal        bool
	streamMessageID       uint64
	streamParentMessageID uint64
	streamID              string
	streamSequence        uint64
	streamAccumulated     strings.Builder
	streamHiddenFromUsers bool
	// streamPublishFenceTimeout is a focused test seam; zero selects the production default.
	streamPublishFenceTimeout time.Duration
}

var _ ports.AgentTranscriptWriter = (*agentTranscriptWriter[common_service_models.User])(nil)
var _ ports.RejectedToolCallWriter = (*agentTranscriptWriter[common_service_models.User])(nil)
var _ ports.ClaimedToolResultWriter = (*agentTranscriptWriter[common_service_models.User])(nil)
var _ ports.AssistantStreamWriter = (*agentTranscriptWriter[common_service_models.User])(nil)
var _ ports.AgentNoteWriter = (*agentTranscriptWriter[common_service_models.User])(nil)
var _ ports.ProviderStateWriter = (*agentTranscriptWriter[common_service_models.User])(nil)
var _ ports.ProviderStateResolver = (*agentTranscriptWriter[common_service_models.User])(nil)
var _ ports.ToolMediaResolver = (*agentTranscriptWriter[common_service_models.User])(nil)

// pendingPublish carries a stream event queued under the writer's lock for publication after the
// lock is released.
type pendingPublish struct {
	eventType proto_models.ChatEventType
	event     *ports.ChatMessageStreamEvent
}

// newAgentTranscriptWriter constructs a transcript writer bound to one agent run's channel, bot
// sender, and triggering user.
func newAgentTranscriptWriter[U common_service_models.UserLike](
	ctx context.Context,
	dispatcher *AsyncDispatcher[U],
	channel *common_service_models.ChatChannel,
	botUserID common_service_models.UserIdType,
	actorUserID common_service_models.UserIdType,
) *agentTranscriptWriter[U] {
	return &agentTranscriptWriter[U]{
		ctx:         ctx,
		d:           dispatcher,
		channel:     channel,
		botUserID:   botUserID,
		actorUserID: actorUserID,
		streamID:    uuid.New().String(),
	}
}

// channelID returns the channel the writer is scoped to, or 0.
func (w *agentTranscriptWriter[U]) channelID() uint64 {
	if w.channel == nil {
		return 0
	}
	return uint64(w.channel.ID)
}

// RecordToolCall persists a TOOL_CALL transcript row (and its tool-call side row) for the bot's run.
// With RequireApproval() the row is written PENDING/WAITING_FOR_APPROVAL with a stamped
// approval_expires_at; otherwise it is NOT_REQUIRED/PENDING_EXECUTION. Idempotent on
// (AgentRunID, ToolCallID); MESSAGE_SENT fanout only on a real insert.
//
// For an approval-required call the owning run is moved [RUNNING, RETRYABLE] -> WAITING_FOR_APPROVAL
// transactionally inside the persistence CreateTranscriptMessage primitive (Decision 6), so the
// approval lane's later resolve/expiry CAS has a real WAITING_FOR_APPROVAL state to transition out
// of. The writer does not issue a separate run-status update.
func (w *agentTranscriptWriter[U]) RecordToolCall(call *ports.ToolCallRecord, opts ...ports.ToolCallOption) (uint64, error) {
	if w.d == nil || w.d.persistenceHandler == nil {
		return 0, ErrTranscriptPersistenceNotConfigured
	}
	if call == nil {
		return 0, ErrTranscriptInvalidRecord
	}
	channelID := w.channelID()
	if channelID == 0 || call.AgentRunID == 0 || call.ToolCallID == "" || call.ToolName == "" {
		return 0, ErrTranscriptInvalidRecord
	}
	if len(call.ArgumentsJSON) > 0 && !call.ArgumentsJSON.IsValid() {
		return 0, ErrTranscriptInvalidPayloadJSON
	}

	resolved := ports.ApplyToolCallOptions(opts...)
	if rejection := call.RejectedArguments; rejection != nil {
		if w.rejectedPublishes == nil || resolved.RequireApproval || len(call.ArgumentsJSON) != 0 || rejection.CallId != call.ToolCallID || rejection.ToolName != call.ToolName || rejection.ItemId == "" || rejection.Diagnostic == "" {
			return 0, ErrTranscriptInvalidRecord
		}
	}

	// Build the immutable ToolCallPayload and serialize it into the model-payload blob.
	payloadBytes, err := marshalModelPayload(&proto_models.MessageModelPayload{
		Payload: &proto_models.MessageModelPayload_ToolCall{
			ToolCall: &proto_models.ToolCallPayload{
				ToolCallId:        call.ToolCallID,
				RejectedArguments: call.RejectedArguments,
				ToolName:          call.ToolName,
				ArgumentsJson:     string(call.ArgumentsJSON),
				RendererKey:       call.RendererKey,
				ThoughtSignature:  append([]byte(nil), call.ThoughtSignature...),
				ProviderCallId:    copyOptionalString(call.ProviderCallID),
			},
		},
	})
	if err != nil {
		if errors.Is(err, ErrTranscriptPayloadTooLarge) {
			return 0, err
		}
		return 0, ErrTranscriptInvalidRecord
	}
	if len(payloadBytes) > maxTranscriptModelPayloadBytes {
		return 0, ErrTranscriptPayloadTooLarge
	}

	approvalStatus := proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_NOT_REQUIRED
	execState := proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION
	var triggeringUserID *uint64
	var approvalExpiresAt *time.Time
	if resolved.RequireApproval {
		approvalStatus = proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING
		execState = proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_WAITING_FOR_APPROVAL
		// triggering_user_id is the approver-eligible actor (the human whose action caused the run).
		if actor := uint64(w.actorUserID); actor != 0 {
			triggeringUserID = &actor
		}
		// G5: stamp approval_expires_at at RECORD time so the approval lane's compare-only expiry
		// sweep never sees NULL. The window is supplied by the caller via the RequireApproval option
		// (the runtime passes the run's captured AgentApprovalPolicy.ApprovalExpiresAfter); it is NOT
		// re-stamped downstream, because expiry must be correct the moment the row is written. When the
		// option carries no positive duration we fall back to the service default (1h).
		expiry := resolved.ApprovalExpiresAfter
		if expiry <= 0 {
			expiry = defaultApprovalExpiry
		}
		exp := time.Now().Add(expiry)
		approvalExpiresAt = &exp
	}

	toolCallID := call.ToolCallID
	excludeFromAI, hiddenFromUsers := resolveKindVisibility(proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, call.ExcludeFromAIContext, call.HiddenFromUsers)
	in := common_persistence.CreateTranscriptMessageInput{
		ArgumentsRejected:    call.RejectedArguments != nil,
		ChannelID:            channelID,
		SenderID:             uint64(w.botUserID),
		ParentMessageID:      call.ParentMessageID,
		Content:              call.FallbackContent,
		ContentType:          proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		Kind:                 proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
		Role:                 proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT,
		ExcludeFromAIContext: excludeFromAI,
		HiddenFromUsers:      hiddenFromUsers,
		AgentRunID:           &call.AgentRunID,
		AgentStepIndex:       &call.AgentStepIndex,
		HasToolCall:          true,
		ToolCallID:           &toolCallID,
		ModelPayload:         payloadBytes,
		PayloadType:          proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_CALL,
		ApprovalStatus:       approvalStatus,
		TriggeringUserID:     triggeringUserID,
		ApprovalExpiresAt:    approvalExpiresAt,
		ExecutionState:       execState,
	}

	messageID, _, err := w.createAndPublish(in)
	return messageID, err
}

func copyOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// createAndPublish is the shared write tail for every non-streaming transcript record: insert
// through the persistence primitive, normalize the error, and fan out MESSAGE_SENT only on a
// real insert (idempotent retries stay silent).
func (w *agentTranscriptWriter[U]) createAndPublish(in common_persistence.CreateTranscriptMessageInput) (uint64, bool, error) {
	messageID, created, err := w.createTranscriptWithNotification(in)
	if err != nil {
		return 0, false, mapTranscriptWriteError(err)
	}
	if created {
		if w.rejectedPublishes != nil {
			*w.rejectedPublishes = append(*w.rejectedPublishes, messageID)
		} else {
			w.publishMessageSent(messageID)
		}
	}
	return messageID, created, nil
}

// RecordAssistantText persists the assistant text turn for the bot's run as a STANDARD/ASSISTANT
// transcript row tagged with the run, step, and detected content type. It is parallel to
// RecordToolCall but writes no side row (HasToolCall=false) and carries no payload. MESSAGE_SENT
// fanout only on a real insert. Inference-call usage is recorded separately by the runtime.
func (w *agentTranscriptWriter[U]) RecordAssistantText(rec *ports.AssistantTextRecord) (uint64, bool, error) {
	if w.d == nil || w.d.persistenceHandler == nil {
		return 0, false, ErrTranscriptPersistenceNotConfigured
	}
	if rec == nil {
		return 0, false, ErrTranscriptInvalidRecord
	}
	channelID := w.channelID()
	if channelID == 0 || rec.AgentRunID == 0 {
		return 0, false, ErrTranscriptInvalidRecord
	}
	// Only a thread reply can also appear on the main timeline; the persistence primitive rejects
	// the same shape, but failing here keeps the record error deterministic.
	if rec.BroadcastToChannel && common_util.Deref(rec.ParentMessageID) == 0 {
		return 0, false, ErrTranscriptInvalidRecord
	}

	agentRunID := rec.AgentRunID
	agentStepIndex := rec.AgentStepIndex
	excludeFromAI, hiddenFromUsers := resolveKindVisibility(proto_models.MessageKind_MESSAGE_KIND_STANDARD, rec.ExcludeFromAIContext, rec.HiddenFromUsers)
	in := common_persistence.CreateTranscriptMessageInput{
		ChannelID:            channelID,
		SenderID:             uint64(w.botUserID),
		ParentMessageID:      rec.ParentMessageID,
		BroadcastToChannel:   rec.BroadcastToChannel,
		Content:              rec.Content,
		ContentType:          common_util.DetectMessageContentType(rec.Content),
		Kind:                 proto_models.MessageKind_MESSAGE_KIND_STANDARD,
		Role:                 proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT,
		ExcludeFromAIContext: excludeFromAI,
		HiddenFromUsers:      hiddenFromUsers,
		AgentRunID:           &agentRunID,
		AgentStepIndex:       &agentStepIndex,
		HasToolCall:          false,
	}

	return w.createAndPublish(in)
}

// RecordAgentNote persists an AGENT_NOTE transcript row for the bot's run: a bare LLM-only row
// (no tool side row, no payload) used for carried reasoning, reasoning audit dumps, and
// runtime-injected instructions. Kind defaults keep the note in the owning run's AI context but
// hidden from users. Carried reasoning must leave ExcludeFromAIContext nil/false so reconstruction
// folds it into the model message; only audit-only final-turn reasoning sets it to true. Callers
// can set HiddenFromUsers=false for user-visible assistant reasoning traces. MESSAGE_SENT fanout
// occurs only on a real insert — the recipient resolver's hidden/privileged split routes hidden
// rows to privileged participants only.
func (w *agentTranscriptWriter[U]) RecordAgentNote(rec *ports.AgentNoteRecord) (uint64, bool, error) {
	if w.d == nil || w.d.persistenceHandler == nil {
		return 0, false, ErrTranscriptPersistenceNotConfigured
	}
	if rec == nil {
		return 0, false, ErrTranscriptInvalidRecord
	}
	channelID := w.channelID()
	// app_resume is reserved for the application's note ResumeParent writes.
	if channelID == 0 || rec.AgentRunID == 0 || strings.TrimSpace(rec.Content) == "" || rec.Subtype == common_service_models.AgentNoteSubtypeAppResume {
		return 0, false, ErrTranscriptInvalidRecord
	}

	role := rec.Role
	if role == proto_models.MessageRole_MESSAGE_ROLE_UNSPECIFIED {
		role = proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT
	}
	agentRunID := rec.AgentRunID
	agentStepIndex := rec.AgentStepIndex
	excludeFromAI, hiddenFromUsers := resolveKindVisibility(proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE, rec.ExcludeFromAIContext, rec.HiddenFromUsers)
	subtype := rec.Subtype
	if subtype == "" {
		subtype = common_service_models.AgentNoteSubtypeNote
	}
	in := common_persistence.CreateTranscriptMessageInput{
		ChannelID:            channelID,
		SenderID:             uint64(w.botUserID),
		ParentMessageID:      rec.ParentMessageID,
		Content:              rec.Content,
		ContentType:          proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		Kind:                 proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE,
		AgentNoteSubtype:     subtype,
		Role:                 role,
		ExcludeFromAIContext: excludeFromAI,
		HiddenFromUsers:      hiddenFromUsers,
		AgentRunID:           &agentRunID,
		AgentStepIndex:       &agentStepIndex,
		HasToolCall:          false,
	}

	return w.createAndPublish(in)
}

// RecordProviderState externalizes opaque provider bytes, then persists a categorically hidden,
// AI-excluded reference row. It deliberately bypasses createAndPublish: provider state is service
// protocol data and must never produce a websocket MESSAGE_SENT event, even for privileged users.
func (w *agentTranscriptWriter[U]) RecordProviderState(rec *ports.ProviderStateRecord) (uint64, bool, error) {
	if w.d == nil || w.d.persistenceHandler == nil || w.d.internalAttachmentStore == nil {
		return 0, false, ErrTranscriptPersistenceNotConfigured
	}
	if rec == nil || rec.State == nil || rec.AgentRunID == 0 {
		return 0, false, ErrTranscriptInvalidRecord
	}
	channelID := w.channelID()
	state := rec.State.Clone()
	if channelID == 0 || state.ProviderID == "" ||
		state.Format != common_service_models.OpenAIResponsesStateFormat || state.Version != common_service_models.OpenAIResponsesStateVersion ||
		strings.TrimSpace(state.VisibleProjectionSHA256) == "" {
		return 0, false, ErrTranscriptInvalidRecord
	}
	if state.AttachmentID == 0 {
		var outputItems []jsontext.Value
		if !providerStateSizeAllowed(uint64(len(state.Data))) {
			return 0, false, fmt.Errorf("%w: provider-state data size is outside the allowed range", ErrTranscriptInvalidRecord)
		}
		if json.Unmarshal(state.Data, &outputItems) != nil || outputItems == nil {
			return 0, false, fmt.Errorf("%w: provider-state output items must be a JSON array", ErrTranscriptInvalidRecord)
		}
		digest := sha256.Sum256(state.Data)
		state.ContentSHA256 = hex.EncodeToString(digest[:])
		state.SizeBytes = uint64(len(state.Data))
		previewPayload, err := marshalModelPayload(&proto_models.MessageModelPayload{
			Payload: &proto_models.MessageModelPayload_ProviderState{ProviderState: &proto_models.ProviderStatePayload{
				ProviderId: state.ProviderID, Api: state.API, ProviderModelId: state.ProviderModelID, DestinationFingerprint: state.DestinationFingerprint, Format: state.Format, Version: state.Version,
				ResponseId: state.ResponseID, AttachmentId: ^uint64(0), ContentSha256: state.ContentSHA256,
				SizeBytes: state.SizeBytes, VisibleProjectionSha256: state.VisibleProjectionSHA256,
				ToolManifestSha256: state.ToolManifestSHA256,
			}},
		})
		if err != nil || len(previewPayload) > maxTranscriptModelPayloadBytes {
			return 0, false, ErrTranscriptInvalidRecord
		}
	}

	if state.AttachmentID == 0 {
		if len(state.Data) == 0 {
			return 0, false, ErrTranscriptInvalidRecord
		}
		entityID := providerStateEntityIDForContext(w.ctx, rec.AgentRunID, rec.AgentStepIndex)
		storeInput := ports.InternalAttachmentInput{
			UserID:      uint64(w.botUserID),
			ObjectType:  ports.InternalAttachmentObjectTypeProviderState,
			EntityID:    entityID,
			Filename:    fmt.Sprintf("provider-state-run-%d-step-%d.json", rec.AgentRunID, rec.AgentStepIndex),
			ContentType: "application/json",
			Data:        state.Data,
			// Provider state must remain reconstructable under the same aggregate private
			// blob ceiling enforced before context hydration.
			MaxBytes: int64(ports.MaxInternalAttachmentHydrationBytes),
		}
		if err := w.d.internalAttachmentStore.ValidateInternal(w.ctx, storeInput); err != nil {
			return 0, false, fmt.Errorf("validate provider state: %w", err)
		}
		ref, err := w.d.internalAttachmentStore.StoreInternal(w.ctx, storeInput)
		if err != nil {
			return 0, false, fmt.Errorf("store provider state: %w", err)
		}
		if ref == nil {
			return 0, false, fmt.Errorf("store provider state: empty attachment reference")
		}
		if ref.OwnerUserID != uint64(w.botUserID) || ref.ObjectType != ports.InternalAttachmentObjectTypeProviderState ||
			ref.EntityID != entityID || ref.ContentType != "application/json" {
			return 0, false, ErrTranscriptInvalidRecord
		}
		if !providerStateSizeAllowed(ref.SizeBytes) {
			return 0, false, ErrTranscriptInvalidRecord
		}
		state.AttachmentID = ref.AttachmentID
		state.ContentSHA256 = ref.ContentSHA256
		state.SizeBytes = ref.SizeBytes
	} else {
		if !providerStateSizeAllowed(state.SizeBytes) {
			return 0, false, ErrTranscriptInvalidRecord
		}
		data, ref, err := w.d.internalAttachmentStore.ResolveInternal(w.ctx, state.AttachmentID, state.SizeBytes)
		if err != nil {
			return 0, false, fmt.Errorf("resolve provider state: %w", err)
		}
		if ref == nil {
			return 0, false, fmt.Errorf("resolve provider state: empty attachment reference")
		}
		if ref.OwnerUserID != uint64(w.botUserID) || ref.ObjectType != ports.InternalAttachmentObjectTypeProviderState ||
			ref.EntityID != providerStateEntityIDForContext(w.ctx, rec.AgentRunID, rec.AgentStepIndex) || ref.ContentType != "application/json" {
			return 0, false, ErrTranscriptInvalidRecord
		}
		if (state.ContentSHA256 != "" && !strings.EqualFold(state.ContentSHA256, ref.ContentSHA256)) ||
			(state.SizeBytes != 0 && state.SizeBytes != ref.SizeBytes) {
			return 0, false, ErrTranscriptInvalidRecord
		}
		state.Data = data
		state.ContentSHA256 = ref.ContentSHA256
		state.SizeBytes = ref.SizeBytes
	}
	var outputItems []jsontext.Value
	if err := json.Unmarshal(state.Data, &outputItems); err != nil || outputItems == nil {
		return 0, false, fmt.Errorf("%w: provider-state output items must be a JSON array", ErrTranscriptInvalidRecord)
	}
	digest := sha256.Sum256(state.Data)
	if state.SizeBytes != uint64(len(state.Data)) || !strings.EqualFold(state.ContentSHA256, hex.EncodeToString(digest[:])) {
		return 0, false, ErrTranscriptInvalidRecord
	}

	payloadBytes, err := marshalModelPayload(&proto_models.MessageModelPayload{
		Payload: &proto_models.MessageModelPayload_ProviderState{
			ProviderState: &proto_models.ProviderStatePayload{
				ProviderId:              state.ProviderID,
				Api:                     state.API,
				ProviderModelId:         state.ProviderModelID,
				DestinationFingerprint:  state.DestinationFingerprint,
				Format:                  state.Format,
				Version:                 state.Version,
				ResponseId:              state.ResponseID,
				AttachmentId:            state.AttachmentID,
				ContentSha256:           state.ContentSHA256,
				SizeBytes:               state.SizeBytes,
				VisibleProjectionSha256: state.VisibleProjectionSHA256,
				ToolManifestSha256:      state.ToolManifestSHA256,
			},
		},
	})
	if err != nil || len(payloadBytes) > maxTranscriptModelPayloadBytes {
		return 0, false, ErrTranscriptInvalidRecord
	}

	trueValue := true
	agentRunID := rec.AgentRunID
	agentStepIndex := rec.AgentStepIndex
	messageID, created, writeErr := w.d.persistenceHandler.CreateTranscriptMessage(w.ctx, common_persistence.CreateTranscriptMessageInput{
		ChannelID:            channelID,
		SenderID:             uint64(w.botUserID),
		ParentMessageID:      rec.ParentMessageID,
		ContentType:          proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		Kind:                 proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE,
		Role:                 proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT,
		ExcludeFromAIContext: &trueValue,
		HiddenFromUsers:      &trueValue,
		AgentRunID:           &agentRunID,
		AgentStepIndex:       &agentStepIndex,
		AttachmentIDs:        []uint64{state.AttachmentID},
		HasToolCall:          true,
		ModelPayload:         payloadBytes,
		PayloadType:          proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_PROVIDER_STATE,
	})
	return messageID, created, writeErr
}

func providerStateSizeAllowed(size uint64) bool {
	return size > 0 && size <= ports.MaxInternalAttachmentHydrationBytes
}

func providerStateEntityID(runID uint64, step uint32) string {
	return fmt.Sprintf("run:%d:step:%d", runID, step)
}

func providerStateEntityIDForContext(ctx context.Context, runID uint64, step uint32) string {
	base := providerStateEntityID(runID, step)
	lease, ok := common_context.GetChatAgentRunDriveLease(ctx)
	if !ok || lease.RunID != runID {
		return base
	}
	digest := sha256.Sum256([]byte(lease.Owner))
	return base + ":drive:" + hex.EncodeToString(digest[:])
}

func matchesProviderStateEntityID(entityID string, runID uint64, step uint32) bool {
	base := providerStateEntityID(runID, step)
	if entityID == base {
		return true
	}
	prefix := base + ":drive:"
	if !strings.HasPrefix(entityID, prefix) || len(entityID) != len(prefix)+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(entityID, prefix))
	return err == nil
}

// ResolveProviderState reads a privileged internal provider-state attachment and verifies it is
// bound to the originating transcript sender plus the expected run/step before returning opaque
// bytes. That sender can differ from this writer's bot while rebuilding prior-run context.
func (w *agentTranscriptWriter[U]) ResolveProviderState(ctx context.Context, attachmentID, ownerUserID, agentRunID uint64, agentStepIndex uint32, maxBytes uint64) ([]byte, error) {
	if w == nil || w.d == nil || w.d.internalAttachmentStore == nil || ownerUserID == 0 {
		return nil, ErrTranscriptPersistenceNotConfigured
	}
	data, ref, err := w.d.internalAttachmentStore.ResolveInternal(ctx, attachmentID, maxBytes)
	if err != nil {
		return nil, err
	}
	if ref == nil || ref.AttachmentID != attachmentID || ref.OwnerUserID != ownerUserID ||
		ref.ObjectType != ports.InternalAttachmentObjectTypeProviderState || !matchesProviderStateEntityID(ref.EntityID, agentRunID, agentStepIndex) ||
		ref.ContentType != "application/json" {
		return nil, ErrTranscriptInvalidRecord
	}
	return data, nil
}

// StartAssistantStream creates the STREAMING assistant transcript row and publishes the stream-start
// event. It intentionally does not fan out MESSAGE_SENT; clients already synthesize the shell row
// from MESSAGE_STREAM_STARTED.
func (w *agentTranscriptWriter[U]) StartAssistantStream(rec *ports.AssistantStreamStartRecord) (uint64, error) {
	var pending []pendingPublish
	err := func() error {
		w.streamMu.Lock()
		defer w.streamMu.Unlock()

		if w.streamStarted && !w.streamTerminal {
			return ErrBotStreamStarted
		}
		if w.d == nil || w.d.persistenceHandler == nil {
			return ErrTranscriptPersistenceNotConfigured
		}
		if rec == nil {
			return ErrTranscriptInvalidRecord
		}
		channelID := w.channelID()
		if channelID == 0 || rec.AgentRunID == 0 || w.botUserID == 0 {
			return ErrTranscriptInvalidRecord
		}

		agentRunID := rec.AgentRunID
		agentStepIndex := rec.AgentStepIndex
		excludeFromAI, hiddenFromUsers := resolveKindVisibility(proto_models.MessageKind_MESSAGE_KIND_STANDARD, rec.ExcludeFromAIContext, rec.HiddenFromUsers)
		streamID := uuid.New().String()
		messageID, created, err := w.d.persistenceHandler.CreateTranscriptMessage(w.ctx, common_persistence.CreateTranscriptMessageInput{
			ChannelID:            channelID,
			SenderID:             uint64(w.botUserID),
			ParentMessageID:      rec.ParentMessageID,
			Content:              "",
			ContentType:          proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_MARKDOWN,
			StreamState:          proto_models.MessageStreamState_MESSAGE_STREAM_STATE_STREAMING,
			Kind:                 proto_models.MessageKind_MESSAGE_KIND_STANDARD,
			Role:                 proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT,
			ExcludeFromAIContext: excludeFromAI,
			HiddenFromUsers:      hiddenFromUsers,
			AgentRunID:           &agentRunID,
			AgentStepIndex:       &agentStepIndex,
			HasToolCall:          false,
		})
		if err != nil {
			return mapTranscriptWriteError(err)
		}
		if !created {
			// The (agent_run_id, agent_step_index) assistant row already exists: a concurrent drive
			// of the same run won the insert race (idx_chat_messages_run_step_assistant). Streaming
			// into the winner's row would interleave two generations, so this writer aborts instead.
			return ErrBotStreamStarted
		}
		w.streamHiddenFromUsers = hiddenFromUsers != nil && *hiddenFromUsers
		w.streamStarted = true
		w.streamTerminal = false
		w.streamMessageID = messageID
		w.streamParentMessageID = common_util.Deref(rec.ParentMessageID)
		w.streamID = streamID
		w.streamSequence = 0
		w.streamAccumulated.Reset()
		w.enqueueStreamPublishLocked(&pending, proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_STARTED, &ports.ChatMessageStreamEvent{
			MessageID: common_service_models.ChatMessageIdType(messageID),
			ChannelID: common_service_models.ChatChannelIdType(channelID),
			StreamID:  w.streamID,
			Sequence:  w.nextStreamSequenceLocked(),
		})
		return nil
	}()
	if err == nil {
		err = w.publishStreamPendingWithDriveFence(pending)
	}
	if err != nil {
		return 0, err
	}
	return w.streamMessageID, nil
}

// AppendAssistantStreamDelta publishes one assistant text delta for a started stream.
func (w *agentTranscriptWriter[U]) AppendAssistantStreamDelta(delta string) error {
	var pending []pendingPublish
	w.streamMu.Lock()
	defer w.streamMu.Unlock()

	if w.streamTerminal {
		return ErrBotStreamClosed
	}
	if !w.streamStarted || w.streamMessageID == 0 {
		return ErrTranscriptInvalidRecord
	}
	if delta == "" {
		return nil
	}

	w.appendAssistantStreamTextLocked(delta)
	w.enqueueStreamPublishLocked(&pending, proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_DELTA, &ports.ChatMessageStreamEvent{
		MessageID: common_service_models.ChatMessageIdType(w.streamMessageID),
		ChannelID: common_service_models.ChatChannelIdType(w.channelID()),
		StreamID:  w.streamID,
		Sequence:  w.nextStreamSequenceLocked(),
		TextDelta: delta,
	})
	return w.publishStreamPendingWithDriveFence(pending)
}

func (w *agentTranscriptWriter[U]) appendAssistantStreamTextLocked(delta string) {
	_, _ = w.streamAccumulated.WriteString(delta)
}

// FinishAssistantStream finalizes the streamed assistant row with canonical final content and
// publishes MESSAGE_STREAM_FINISHED. With BroadcastToChannel the sealed thread row is also flipped
// onto the main timeline in the same transaction and published once more as a full-row
// MESSAGE_SENT: stream frames never carry the broadcast flag, so main-timeline caches only learn of
// the row from that event.
func (w *agentTranscriptWriter[U]) FinishAssistantStream(finish *ports.AssistantStreamFinishRecord) (uint64, error) {
	broadcast := finish != nil && finish.BroadcastToChannel
	return w.finalizeAssistantStream(finishContent(finish), proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE, proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_FINISHED, broadcast)
}

// FailAssistantStream marks the streamed assistant row failed and publishes MESSAGE_STREAM_FAILED.
func (w *agentTranscriptWriter[U]) FailAssistantStream(fail *ports.AssistantStreamFailRecord) (uint64, error) {
	content := ""
	if fail != nil {
		content = fail.Content
	}
	return w.finalizeAssistantStream(content, proto_models.MessageStreamState_MESSAGE_STREAM_STATE_FAILED, proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_FAILED, false)
}

func finishContent(finish *ports.AssistantStreamFinishRecord) string {
	if finish == nil {
		return ""
	}
	return finish.Content
}

func (w *agentTranscriptWriter[U]) finalizeAssistantStream(content string, streamState proto_models.MessageStreamState, eventType proto_models.ChatEventType, broadcast bool) (uint64, error) {
	var pending []pendingPublish
	var postCommitTx *common_persistence.GormTransactionWithHooks
	useAccumulatedContent := content == ""
	err := func() error {
		w.streamMu.Lock()
		defer w.streamMu.Unlock()

		if w.d == nil || w.d.persistenceHandler == nil {
			return ErrTranscriptPersistenceNotConfigured
		}
		if w.streamTerminal {
			return ErrBotStreamClosed
		}
		if !w.streamStarted || w.streamMessageID == 0 {
			return ErrTranscriptInvalidRecord
		}
		// A main-timeline stream has nowhere to broadcast to; refuse before the stream is closed so
		// the caller can still fail it cleanly.
		if broadcast && w.streamParentMessageID == 0 {
			return ErrTranscriptInvalidRecord
		}
		w.streamTerminal = true
		if useAccumulatedContent {
			content = w.streamAccumulated.String()
		}

		tx, err := w.d.persistenceHandler.BeginTransaction(w.ctx)
		if err != nil {
			w.streamTerminal = false
			return err
		}
		gormTx := &common_persistence.GormTransactionWithHooks{Tx: tx}
		txCtx := scontext.CopySRouterContext[common_service_models.UserIdType, U](w.ctx, w.ctx)
		txCtx = scontext.WithTransaction[common_service_models.UserIdType, U](txCtx, gormTx)
		if lease, ok := common_context.GetChatAgentRunDriveLease(w.ctx); ok {
			if err := w.d.persistenceHandler.AssertChatAgentRunDriveLease(txCtx, lease.RunID); err != nil {
				_ = gormTx.Rollback()
				w.streamTerminal = false
				return mapTranscriptWriteError(err)
			}
		}

		// EditChatMessage refreshes the channel preview only when this row is the newest visible
		// preview-eligible message, so hidden and superseded streams never reach it.
		expectedStreamState := proto_models.MessageStreamState_MESSAGE_STREAM_STATE_STREAMING
		if _, err := w.d.persistenceHandler.EditChatMessage(txCtx, common_persistence.EditChatMessageInput{
			MessageID:           w.streamMessageID,
			EditedByUserID:      uint64(w.botUserID),
			NewContent:          content,
			NewContentType:      common_util.DetectMessageContentType(content),
			NewStreamState:      &streamState,
			ExpectedStreamState: &expectedStreamState,
		}); err != nil {
			_ = gormTx.Rollback()
			w.streamTerminal = false
			return err
		}
		if broadcast {
			if err := w.d.persistenceHandler.MarkChatMessageBroadcastToChannel(txCtx, w.streamMessageID); err != nil {
				_ = gormTx.Rollback()
				w.streamTerminal = false
				return mapTranscriptWriteError(err)
			}
		}
		if w.d.notificationSink != nil && streamState == proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE {
			if err := w.d.notificationSink.ChatMessageRecorded(txCtx, w.streamMessageID); err != nil {
				_ = gormTx.Rollback()
				w.streamTerminal = false
				return err
			}
		}
		if err := gormTx.Commit(); err != nil {
			w.streamTerminal = false
			return err
		}
		postCommitTx = gormTx

		if !useAccumulatedContent {
			w.streamAccumulated.Reset()
			_, _ = w.streamAccumulated.WriteString(content)
		}
		w.enqueueStreamPublishLocked(&pending, eventType, &ports.ChatMessageStreamEvent{
			MessageID:       common_service_models.ChatMessageIdType(w.streamMessageID),
			ChannelID:       common_service_models.ChatChannelIdType(w.channelID()),
			StreamID:        w.streamID,
			Sequence:        w.nextStreamSequenceLocked(),
			AccumulatedText: content,
		})
		return nil
	}()
	if err == nil {
		err = w.publishStreamPendingWithDriveFence(pending)
	}
	if postCommitTx != nil {
		w.d.runPostCommitHooks(w.ctx, postCommitTx)
	}
	if err == nil && broadcast {
		// The full row, now durable with its broadcast flag, is what main-timeline subscribers need;
		// the FINISHED frame above only reaches the thread.
		w.publishMessageSent(w.streamMessageID)
	}
	return w.streamMessageID, err
}

// RecordToolResult persists the paired TOOL_RESULT row for an existing TOOL_CALL in the same run.
// Idempotent on (AgentRunID, ToolCallID); MESSAGE_SENT fanout only on a real insert; returns the
// created flag so the approval lane can avoid double-firing the resolution event/resume nudge.
func (w *agentTranscriptWriter[U]) RecordToolResult(result *ports.ToolResultRecord) (uint64, bool, error) {
	in, err := w.prepareToolResult(result, "")
	if err != nil {
		return 0, false, classifyToolResultWriteError(err)
	}
	messageID, created, writeErr := w.createAndPublish(in)
	// Do not clean up after persistence has started. A failed call can be
	// commit-ambiguous, and created=false can mean a concurrent writer won with the
	// same deterministic attachment. Cleanup here could dangle the persisted row.
	return messageID, created, classifyToolResultWriteError(writeErr)
}

// ClaimInlineTool establishes a per-attempt fencing lease before the live continuation invokes an
// inline tool. Fresh inline work is not scanner-claimable until its handoff delay elapses, but this
// exact-id predicate lets the live owner claim immediately without broadening the scanner query.
func (w *agentTranscriptWriter[U]) ClaimInlineTool(toolCallMessageID uint64, leaseToken string, leaseExpiresAt time.Time) (bool, error) {
	if w == nil || w.d == nil || w.d.persistenceHandler == nil {
		return false, ErrTranscriptPersistenceNotConfigured
	}
	if toolCallMessageID == 0 || strings.TrimSpace(leaseToken) == "" {
		return false, ErrTranscriptInvalidRecord
	}
	now := time.Now()
	claimed, err := w.d.persistenceHandler.ClaimFreshInlineToolCall(w.ctx, toolCallMessageID, leaseToken, leaseExpiresAt, now)
	if err != nil {
		return false, ErrTranscriptWriteFailed
	}
	return claimed, nil
}

func (w *agentTranscriptWriter[U]) ReleaseInlineToolClaim(toolCallMessageID uint64, leaseToken string, retryable bool, lastErr string) (bool, error) {
	if w == nil || w.d == nil || w.d.persistenceHandler == nil {
		return false, ErrTranscriptPersistenceNotConfigured
	}
	if toolCallMessageID == 0 || strings.TrimSpace(leaseToken) == "" {
		return false, ErrTranscriptInvalidRecord
	}
	released, err := w.d.persistenceHandler.ReleaseToolCallLease(w.ctx, toolCallMessageID, leaseToken, retryable, &lastErr)
	if err != nil {
		return false, ErrTranscriptWriteFailed
	}
	return released, nil
}

// RecordClaimedToolResult externalizes media under the attempt token, then atomically verifies that
// same token, writes/idempotently validates the result, and marks the call EXECUTED. A stale owner
// gets committed=false and cannot publish a TOOL_RESULT.
func (w *agentTranscriptWriter[U]) RecordClaimedToolResult(toolCallMessageID uint64, leaseToken string, result *ports.ToolResultRecord) (uint64, bool, bool, error) {
	if toolCallMessageID == 0 || strings.TrimSpace(leaseToken) == "" {
		return 0, false, false, ErrTranscriptInvalidRecord
	}
	in, err := w.prepareToolResult(result, leaseToken)
	if err != nil {
		return 0, false, false, classifyToolResultWriteError(err)
	}
	messageID, created, committed, err := w.d.persistenceHandler.CommitClaimedToolCallResult(
		w.ctx, toolCallMessageID, leaseToken, time.Now(), in,
	)
	if err != nil {
		return 0, false, false, classifyToolResultWriteError(mapTranscriptWriteError(err))
	}
	if committed && created {
		w.publishMessageSent(messageID)
	}
	return messageID, created, committed, nil
}

func (w *agentTranscriptWriter[U]) prepareToolResult(result *ports.ToolResultRecord, leaseToken string) (common_persistence.CreateTranscriptMessageInput, error) {
	var zero common_persistence.CreateTranscriptMessageInput
	if w.d == nil || w.d.persistenceHandler == nil {
		return zero, ErrTranscriptPersistenceNotConfigured
	}
	if result == nil {
		return zero, ErrTranscriptInvalidRecord
	}
	channelID := w.channelID()
	if channelID == 0 || result.AgentRunID == 0 || result.ToolCallID == "" {
		return zero, ErrTranscriptInvalidRecord
	}
	if err := ports.ValidateToolResultContentPartCount(len(result.ContentParts)); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrTranscriptInvalidRecord, err)
	}
	if err := ports.ValidateToolResultText(result.FallbackContent); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrTranscriptInvalidRecord, err)
	}
	if err := ports.ValidateToolResultEmbeds(result.Embeds); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrTranscriptInvalidRecord, err)
	}
	if err := ports.ValidateToolResultPayload(result.ContentJSON, nil); errors.Is(err, ports.ErrToolResultContentJSONInvalid) {
		return zero, ErrTranscriptInvalidPayloadJSON
	}
	previewParts, err := attachmentpkg.PreviewExternalizedToolResultMedia(attachmentpkg.ToolMediaExternalizeInput{
		OwnerUserID: uint64(w.botUserID),
		AgentRunID:  result.AgentRunID,
		ToolCallID:  result.ToolCallID,
		LeaseOwner:  leaseToken,
		Parts:       result.ContentParts,
	})
	if err != nil {
		return zero, err
	}
	previewPayload, err := marshalModelPayload(&proto_models.MessageModelPayload{
		Payload: &proto_models.MessageModelPayload_ToolResult{ToolResult: &proto_models.ToolResultPayload{
			ToolCallId: result.ToolCallID, ContentJson: string(result.ContentJSON), IsError: result.IsError,
			ContentParts: previewParts, RendererKey: result.RendererKey,
		}},
	})
	if err != nil {
		if errors.Is(err, ErrTranscriptPayloadTooLarge) {
			return zero, err
		}
		return zero, ErrTranscriptInvalidRecord
	}
	if err := ports.ValidateToolResultPayload(result.ContentJSON, previewPayload); errors.Is(err, ports.ErrToolResultPayloadTooLarge) {
		return zero, ErrTranscriptPayloadTooLarge
	}
	contentParts, attachmentIDs, err := w.externalizeToolResultMedia(result, leaseToken)
	if err != nil {
		return zero, err
	}

	payloadBytes, err := marshalModelPayload(&proto_models.MessageModelPayload{
		Payload: &proto_models.MessageModelPayload_ToolResult{
			ToolResult: &proto_models.ToolResultPayload{
				ToolCallId:   result.ToolCallID,
				ContentJson:  string(result.ContentJSON),
				IsError:      result.IsError,
				ContentParts: contentParts,
				RendererKey:  result.RendererKey,
			},
		},
	})
	if err != nil {
		if errors.Is(err, ErrTranscriptPayloadTooLarge) {
			return zero, err
		}
		return zero, ErrTranscriptInvalidRecord
	}
	if err := ports.ValidateToolResultPayload(result.ContentJSON, payloadBytes); errors.Is(err, ports.ErrToolResultPayloadTooLarge) {
		return zero, ErrTranscriptPayloadTooLarge
	}

	toolCallID := result.ToolCallID
	embeds := common_persistence.CreateChatMessageEmbedsFromService(result.Embeds)
	// The persistence primitive UNCONDITIONALLY mirrors a TOOL_RESULT's visibility from its paired
	// TOOL_CALL, so these resolved defaults are a pre-default only; the mirror is the final authority.
	excludeFromAI, hiddenFromUsers := resolveKindVisibility(proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT, result.ExcludeFromAIContext, result.HiddenFromUsers)
	in := common_persistence.CreateTranscriptMessageInput{
		ChannelID:            channelID,
		SenderID:             uint64(w.botUserID),
		ParentMessageID:      result.ParentMessageID,
		Content:              result.FallbackContent,
		ContentType:          proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		Kind:                 proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT,
		Role:                 proto_models.MessageRole_MESSAGE_ROLE_TOOL,
		ExcludeFromAIContext: excludeFromAI,
		HiddenFromUsers:      hiddenFromUsers,
		AgentRunID:           &result.AgentRunID,
		AttachmentIDs:        attachmentIDs,
		// AgentStepIndex is copied from the paired TOOL_CALL by the persistence primitive.
		Embeds:       embeds,
		HasToolCall:  true,
		ToolCallID:   &toolCallID,
		ModelPayload: payloadBytes,
		PayloadType:  proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_RESULT,
	}
	return in, nil
}

func (w *agentTranscriptWriter[U]) nextStreamSequenceLocked() uint64 {
	w.streamSequence++
	return w.streamSequence
}

func (w *agentTranscriptWriter[U]) enqueueStreamPublishLocked(pending *[]pendingPublish, eventType proto_models.ChatEventType, event *ports.ChatMessageStreamEvent) {
	if pending == nil || event == nil {
		return
	}
	if w.d == nil || w.d.chatEventPublisher == nil {
		return
	}
	event.HiddenFromUsers = w.streamHiddenFromUsers
	// Copy the value so a later stream on this writer cannot change queued events.
	parentID := w.streamParentMessageID
	event.ParentMessageID = &parentID
	*pending = append(*pending, pendingPublish{eventType: eventType, event: event})
}

func (w *agentTranscriptWriter[U]) publishStreamPending(events []pendingPublish) {
	if len(events) == 0 || w.d == nil || w.d.chatEventPublisher == nil {
		return
	}
	// WARNING: this is one of the three documented exceptions to post-commit publishing. Token
	// deltas are mid-stream ephemera that must reach the client immediately, so they publish on a
	// deliberately transaction-cleared context and must never be routed through
	// ports.PublishAfterCommit. A refactor that leaves a transaction on this context would silently
	// defer every delta to commit time and break streaming.
	publishCtx := txClearedSRouterContext[U](context.WithoutCancel(w.ctx))
	w.publishStreamPendingWithContext(publishCtx, events)
}

// publishStreamPendingWithDriveFence linearizes every run-owned stream event handoff against drive
// takeover. START and terminal events need the same boundary as DELTA: without it, recovery may
// mark the durable shell FAILED while a stale owner subsequently emits START/FINISHED. The row lock
// is held only for the bounded publisher handoff below, never for model generation.
func (w *agentTranscriptWriter[U]) publishStreamPendingWithDriveFence(events []pendingPublish) error {
	if len(events) == 0 || w.d == nil || w.d.chatEventPublisher == nil {
		return nil
	}
	lease, hasLease := common_context.GetChatAgentRunDriveLease(w.ctx)
	if !hasLease {
		w.publishStreamPending(events)
		return nil
	}
	if w.d.persistenceHandler == nil {
		return ErrTranscriptPersistenceNotConfigured
	}
	tx, err := w.d.persistenceHandler.BeginTransaction(w.ctx)
	if err != nil {
		return err
	}
	publishFence := &common_persistence.GormTransactionWithHooks{Tx: tx}
	txCtx := scontext.CopySRouterContext[common_service_models.UserIdType, U](w.ctx, w.ctx)
	txCtx = scontext.WithTransaction[common_service_models.UserIdType, U](txCtx, publishFence)
	if err := w.d.persistenceHandler.AssertChatAgentRunDriveLease(txCtx, lease.RunID); err != nil {
		_ = publishFence.Rollback()
		return mapTranscriptWriteError(err)
	}
	w.publishStreamPendingFenced(events)
	if err := publishFence.Commit(); err != nil {
		return err
	}
	return nil
}

func (w *agentTranscriptWriter[U]) publishStreamPendingFenced(events []pendingPublish) {
	if len(events) == 0 || w.d == nil || w.d.chatEventPublisher == nil {
		return
	}
	timeout := w.streamPublishFenceTimeout
	if timeout <= 0 {
		timeout = defaultAssistantStreamPublishFenceTimeout
	}
	baseCtx := txClearedSRouterContext[U](context.WithoutCancel(w.ctx))
	if !w.d.tryAcquireStreamPublishHandoff() {
		if w.d.logger != nil {
			appLogger.For(baseCtx, w.d.logger, botDispatcherLoggerName).Warn("assistant stream publisher circuit is saturated; dropping ephemeral event",
				zap.Int(appLogger.KeyOutstandingPublishLimit, cap(w.d.streamPublishHandoffs)))
		}
		return
	}
	publishCtx, cancel := context.WithTimeout(baseCtx, timeout)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer w.d.releaseStreamPublishHandoff()
		w.publishStreamPendingWithContext(publishCtx, events)
	}()
	select {
	case <-done:
	case <-publishCtx.Done():
		// The built-in publisher observes ctx and exits. A custom implementation may violate that
		// contract; abandon its goroutine so the row lock and drive heartbeat cannot be pinned. Such
		// an implementation may still hand off one late ephemeral delta, but it cannot mutate durable
		// transcript state and the next reconnect reads the fenced canonical row.
		if w.d.logger != nil {
			appLogger.For(baseCtx, w.d.logger, botDispatcherLoggerName).Warn("assistant stream publisher exceeded drive-fence deadline; releasing run lock",
				appLogger.DurationSeconds(appLogger.KeyPublishTimeoutSeconds, timeout))
		}
	}
	cancel()
}

func (w *agentTranscriptWriter[U]) publishStreamPendingWithContext(publishCtx context.Context, events []pendingPublish) {
	if len(events) == 0 || w.d == nil || w.d.chatEventPublisher == nil {
		return
	}
	var publishLogger *zap.Logger
	if w.d.logger != nil {
		publishLogger = appLogger.For(publishCtx, w.d.logger, botDispatcherLoggerName)
	}
	for _, p := range events {
		if publishCtx.Err() != nil {
			return
		}
		if p.event == nil {
			continue
		}
		if err := w.d.chatEventPublisher.PublishChatEvent(publishCtx, &ports.ChatEvent{
			Type:          p.eventType,
			OccurredAt:    time.Now(),
			ActorUserID:   w.botUserID,
			MessageStream: p.event,
		}); err != nil && publishLogger != nil {
			if ce := publishLogger.Check(zap.DebugLevel, "failed to publish assistant stream event"); ce != nil {
				ce.Write(
					zap.Uint64(appLogger.KeyBotUserID, uint64(w.botUserID)),
					zap.Stringer(appLogger.KeyEventType, p.eventType),
					zap.Uint64(appLogger.KeyChannelID, uint64(w.channel.ID)),
					zap.Error(err),
				)
			}
		}
	}
}

// publishMessageSent fans out a MESSAGE_SENT event for a freshly-inserted transcript row. The
// recipient resolver applies the hidden/privileged split based on the row's visibility, so this is
// safe to call for hidden rows. Best-effort: a publish failure does not fail the write.
func (w *agentTranscriptWriter[U]) publishMessageSent(messageID uint64) {
	if w.d == nil || w.d.chatEventPublisher == nil {
		return
	}
	// Fetch hidden-inclusive: the writer may have just created a hidden_from_users row, and the
	// actor-facing GetChatMessageByID (VisibleToActor) would filter it to nil, killing fanout for the
	// exact rows the split-recipient resolver exists to deliver. ServiceTranscript reads every row;
	// the hidden/privileged recipient split happens in the recipient resolver at publish time, NOT here.
	msg, err := w.d.persistenceHandler.GetChatMessageByIDWithOptions(w.ctx, messageID, common_persistence.ChatMessageReadOptions{
		VisibilityMode: common_persistence.ChatMessageVisibilityServiceTranscript,
	})
	if err != nil {
		if w.d.logger != nil {
			appLogger.For(w.ctx, w.d.logger, botDispatcherLoggerName).Error("failed to load transcript message for realtime publication",
				zap.String(logkeys.Stage, "load_message_for_realtime_publication"),
				zap.Uint64(appLogger.KeyMessageID, messageID),
				zap.Uint64(appLogger.KeyChannelID, uint64(w.channel.ID)),
				zap.Error(err))
		}
		return
	}
	if msg == nil {
		appLogger.LogInvariantViolation(appLogger.For(w.ctx, w.d.logger, botDispatcherLoggerName), "transcript_message_reload_returns_row", nil,
			zap.String(logkeys.Stage, "load_message_for_realtime_publication"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyChannelID, uint64(w.channel.ID)),
			zap.String(logkeys.Expected, "persisted transcript message"),
			zap.String(logkeys.Actual, "nil transcript message"),
			zap.String(logkeys.Fallback, "skip_realtime_publication"))
		return
	}
	serviceMsg := w.d.converterMessageToService(msg)
	if serviceMsg == nil {
		appLogger.LogInvariantViolation(appLogger.For(w.ctx, w.d.logger, botDispatcherLoggerName), "transcript_message_conversion_returns_service_message", nil,
			zap.String(logkeys.Stage, "convert_message_for_realtime_publication"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyChannelID, uint64(w.channel.ID)),
			zap.String(logkeys.Expected, "service chat message"),
			zap.String(logkeys.Actual, "nil converted message"),
			zap.String(logkeys.Fallback, "skip_realtime_publication"))
		return
	}
	publishCtx := context.WithoutCancel(w.ctx)
	w.hydrateToolResultMediaForDisplay(publishCtx, serviceMsg)
	if err := w.d.chatEventPublisher.PublishChatEvent(publishCtx, &ports.ChatEvent{
		Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_SENT,
		OccurredAt:  time.Now(),
		ActorUserID: w.botUserID,
		Message:     serviceMsg,
	}); err != nil && w.d.logger != nil {
		appLogger.For(publishCtx, w.d.logger, botDispatcherLoggerName).Error("failed to publish transcript MESSAGE_SENT",
			zap.String(logkeys.Stage, "publish_transcript_message_sent"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyChannelID, uint64(w.channel.ID)),
			zap.Uint64(appLogger.KeyBotUserID, uint64(w.botUserID)),
			zap.Error(err),
		)
	}
}

// resolveKindVisibility resolves the (exclude_from_ai_context, hidden_from_users) pointers for a row
// of the given kind: an explicit per-message override always wins (field presence preserves an
// explicit false over a kind default of true), otherwise the per-kind default from the single
// persistence authority is applied. This replaces scattered false literals at the call sites so the
// spec's per-kind defaults table has one source of truth.
func resolveKindVisibility(kind proto_models.MessageKind, excludeOverride, hiddenOverride *bool) (excludeFromAIContext, hiddenFromUsers *bool) {
	excludeDefault, hiddenDefault := common_persistence.KindVisibilityDefaults(kind)
	return common_persistence.ApplyKindVisibilityDefault(excludeOverride, excludeDefault),
		common_persistence.ApplyKindVisibilityDefault(hiddenOverride, hiddenDefault)
}

// marshalModelPayload serializes a MessageModelPayload deterministically into the blob bytes.
// Reject by protobuf's exact encoded-size calculation before allocating the serialized buffer;
// callers can still rely on the real marshal below to reject invalid UTF-8 strings.
func marshalModelPayload(p *proto_models.MessageModelPayload) ([]byte, error) {
	if proto.Size(p) > maxTranscriptModelPayloadBytes {
		return nil, ErrTranscriptPayloadTooLarge
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(p)
}

// mapTranscriptWriteError normalizes persistence errors from CreateTranscriptMessage into the
// transcript-writer error vocabulary.
func mapTranscriptWriteError(err error) error {
	switch {
	case errors.Is(err, common_persistence.ErrChatToolCallConflict):
		return ErrTranscriptPayloadConflict
	case errors.Is(err, common_persistence.ErrChatToolResultOrphan):
		return ErrTranscriptOrphanResult
	case errors.Is(err, common_persistence.ErrChatTranscriptRunMismatch):
		return ErrTranscriptRunMismatch
	case errors.Is(err, common_persistence.ErrChatTranscriptPayloadInvalid):
		return ErrTranscriptInvalidRecord
	case errors.Is(err, common_persistence.ErrChatAgentRunDriveLeaseLost),
		errors.Is(err, common_persistence.ErrChatAgentRunStepSealInvalid),
		errors.Is(err, common_persistence.ErrChatMessageStreamStateConflict):
		return fmt.Errorf("%w: %w", ErrTranscriptWriteFailed, err)
	default:
		return ErrTranscriptWriteFailed
	}
}

// classifyToolResultWriteError distinguishes deterministic result defects from operational write
// failures before an execution lane decides whether it is safe/useful to invoke the tool again.
func classifyToolResultWriteError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[interface {
		error
		Retryable() bool
	}](err); ok {
		return err
	}
	retryable := true
	switch {
	case errors.Is(err, attachmentpkg.ErrInvalidInternalAttachment),
		errors.Is(err, attachmentpkg.ErrInternalAttachmentAccess),
		errors.Is(err, ErrTranscriptPersistenceNotConfigured),
		errors.Is(err, ErrTranscriptInvalidRecord),
		errors.Is(err, ErrTranscriptInvalidPayloadJSON),
		errors.Is(err, ErrTranscriptPayloadTooLarge),
		errors.Is(err, ErrTranscriptPayloadConflict),
		errors.Is(err, ErrTranscriptOrphanResult),
		errors.Is(err, ErrTranscriptRunMismatch):
		retryable = false
	}
	return &ports.ToolResultWriteError{Cause: err, CanRetry: retryable}
}

// RecordRejectedToolCall never exposes the transient pending call outside its transaction.
func (w *agentTranscriptWriter[U]) RecordRejectedToolCall(call *ports.ToolCallRecord, result *ports.ToolResultRecord) (uint64, error) {
	if w.d == nil || w.d.persistenceHandler == nil {
		return 0, ErrTranscriptPersistenceNotConfigured
	}
	if call == nil || call.RejectedArguments == nil || result == nil || !result.IsError || call.AgentRunID != result.AgentRunID || call.ToolCallID != result.ToolCallID {
		return 0, ErrTranscriptInvalidRecord
	}
	tx, err := w.d.persistenceHandler.BeginTransaction(w.ctx)
	if err != nil {
		return 0, err
	}
	hooks := &common_persistence.GormTransactionWithHooks{Tx: tx}
	defer hooks.Rollback()
	txCtx := scontext.CopySRouterContext[common_service_models.UserIdType, U](w.ctx, w.ctx)
	txCtx = scontext.WithTransaction[common_service_models.UserIdType, U](txCtx, hooks)
	writer := newAgentTranscriptWriter(txCtx, w.d, w.channel, w.botUserID, w.actorUserID)
	var published []uint64
	writer.rejectedPublishes = &published
	id, err := writer.RecordToolCall(call)
	if err != nil {
		return 0, err
	}
	if _, _, err = writer.RecordToolResult(result); err != nil {
		return 0, err
	}
	if err = hooks.Commit(); err != nil {
		return 0, err
	}
	w.d.runPostCommitHooks(w.ctx, hooks)
	for _, messageID := range published {
		w.publishMessageSent(messageID)
	}
	return id, nil
}

func (w *agentTranscriptWriter[U]) CountRejectedArgumentSteps(runID uint64) (uint32, error) {
	if w.d == nil || w.d.persistenceHandler == nil {
		return 0, ErrTranscriptPersistenceNotConfigured
	}
	return w.d.persistenceHandler.CountRejectedArgumentSteps(w.ctx, runID)
}
