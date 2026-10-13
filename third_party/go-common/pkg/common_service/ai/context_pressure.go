package ai

import (
	"errors"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	"github.com/Suhaibinator/go-common/pkg/genai/openaiinput"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

var ErrContextLimit = errors.New("agent context cannot fit without discarding protected history")

func contextPressure(body []*reconstructedMessage, tokens, limit int, opts AgentContextOptions) []string {
	var reasons []string
	if tokens > limit {
		reasons = append(reasons, "tokens")
	}
	var files, images, count, hydrated uint64
	for _, m := range body {
		files += m.inputFileBytes
		images += m.imageInputBytes
		count += m.imageInputCount
		hydrated += m.hydratedBytes
	}
	if opts.CurrentAPI == apishape.Responses {
		if files > openaiinput.MaxFileInputBytes {
			reasons = append(reasons, "file_bytes")
		}
		if images > openaiinput.MaxImageInputPayloadBytes {
			reasons = append(reasons, "image_bytes")
		}
		if count > openaiinput.MaxImageInputsPerRequest {
			reasons = append(reasons, "image_count")
		}
	}
	if hydrated > resolveHydrationLimit(opts) {
		reasons = append(reasons, "hydration_bytes")
	}
	return reasons
}

// Track original row identities, not just the number of reconstructed provider messages. rows
// must already be in sortMessagesByID order.
func droppedTranscriptIDs(rows []*common_service_models.ChatMessage, before, after []*reconstructedMessage, currentRunID uint64) []uint64 {
	kept := map[*reconstructedMessage]bool{}
	for _, m := range after {
		kept[m] = true
	}
	droppedIDs := map[uint64]bool{}
	droppedGroups := map[stepKey]bool{}
	for _, m := range before {
		if !kept[m] {
			droppedIDs[m.minID] = true
			if m.atomicGroup.valid() {
				droppedGroups[m.atomicGroup] = true
			}
		}
	}
	var ids []uint64
	for _, row := range rows {
		if excludedFromAIContext(row) && kindOf(row) != proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE {
			continue
		}
		if kindOf(row) == proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE && classifyAgentNote(row, currentRunID) == noteSkipped {
			continue
		}
		group := stepKey{}
		if row.AgentRunID != nil && row.AgentStepIndex != nil {
			group = stepKey{*row.AgentRunID, *row.AgentStepIndex}
		}
		if droppedIDs[uint64(row.ID)] || droppedGroups[group] {
			ids = append(ids, uint64(row.ID))
		}
	}
	return ids
}
