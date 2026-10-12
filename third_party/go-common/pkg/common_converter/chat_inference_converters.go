package common_converter

import (
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// PersistenceChatInferenceTotalsToProto maps the per-channel inference totals cache to its proto
// read model. A nil input yields zero totals scoped to the requested channel.
//
// Parameters:
//   - totals: the persistence totals row, or nil.
//
// Returns:
//   - result: the proto ChatInferenceTotals.
func (h *ConverterHandler[U]) PersistenceChatInferenceTotalsToProto(
	channelID uint64,
	totals *common_persistence_models.ChatInferenceTotals,
) *proto_models.ChatInferenceTotals {
	if totals == nil {
		return &proto_models.ChatInferenceTotals{ChannelId: channelID}
	}
	return &proto_models.ChatInferenceTotals{
		ChannelId:                channelID,
		CallCount:                totals.CallCount,
		InputTokensTotal:         totals.InputTokensTotal,
		OutputTokensTotal:        totals.OutputTokensTotal,
		TotalTokensTotal:         totals.TotalTokensTotal,
		ThoughtsTokensTotal:      totals.ThoughtsTokensTotal,
		CachedContentTokensTotal: totals.CachedContentTokensTotal,
		InputCharactersTotal:     totals.InputCharactersTotal,
		OutputCharactersTotal:    totals.OutputCharactersTotal,
		InputImagesTotal:         totals.InputImagesTotal,
		OutputImagesTotal:        totals.OutputImagesTotal,
		InputVideosTotal:         totals.InputVideosTotal,
		OutputVideosTotal:        totals.OutputVideosTotal,
		InputAudioFilesTotal:     totals.InputAudioFilesTotal,
		OutputAudioFilesTotal:    totals.OutputAudioFilesTotal,
		UpdatedAt:                timestampOrNil(totals.UpdatedAt),
	}
}

// PersistenceMessageInferenceAggregateToProto maps a derived per-message usage aggregate to the
// proto MessageInferenceSummary. The latest-call label fields (provider/model/finish/latency) are
// carried straight through.
//
// Parameters:
//   - agg: the derived per-message aggregate, or nil.
//
// Returns:
//   - result: the proto MessageInferenceSummary, or nil.
func (h *ConverterHandler[U]) PersistenceMessageInferenceAggregateToProto(
	agg *persistence.MessageInferenceAggregate,
) *proto_models.MessageInferenceSummary {
	if agg == nil {
		return nil
	}
	return &proto_models.MessageInferenceSummary{
		MessageId:           agg.MessageID,
		ChannelId:           agg.ChannelID,
		CallCount:           agg.CallCount,
		InputTokens:         agg.InputTokens,
		OutputTokens:        agg.OutputTokens,
		TotalTokens:         agg.TotalTokens,
		ThoughtsTokens:      agg.ThoughtsTokens,
		CachedContentTokens: agg.CachedContentTokens,
		ProviderId:          agg.ProviderID,
		ProviderModelId:     agg.ProviderModelID,
		FinishReason:        agg.FinishReason,
		LatencyMs:           agg.LatencyMs,
	}
}

// PersistenceInferenceUsageBreakdownToProto maps one grouped usage aggregate to the proto
// AgentSessionUsageBreakdown.
//
// Parameters:
//   - b: the grouped usage aggregate, or nil.
//
// Returns:
//   - result: the proto AgentSessionUsageBreakdown, or nil.
func (h *ConverterHandler[U]) PersistenceInferenceUsageBreakdownToProto(
	b *persistence.InferenceUsageBreakdown,
) *proto_models.AgentSessionUsageBreakdown {
	if b == nil {
		return nil
	}
	return &proto_models.AgentSessionUsageBreakdown{
		AgentSessionId:      b.AgentSessionID,
		AgentRunId:          b.AgentRunID,
		Purpose:             b.Purpose,
		ProviderId:          b.ProviderID,
		ProviderModelId:     b.ProviderModelID,
		CallCount:           b.CallCount,
		InputTokens:         b.InputTokens,
		OutputTokens:        b.OutputTokens,
		TotalTokens:         b.TotalTokens,
		ThoughtsTokens:      b.ThoughtsTokens,
		CachedContentTokens: b.CachedContentTokens,
	}
}

// PersistenceChatInferenceCallToProto maps an append-only inference-call ledger row to its proto
// read model.
//
// Parameters:
//   - call: the ledger row, or nil.
//
// Returns:
//   - result: the proto ChatInferenceCall, or nil.
func (h *ConverterHandler[U]) PersistenceChatInferenceCallToProto(
	call *common_persistence_models.ChatInferenceCall,
) *proto_models.ChatInferenceCall {
	if call == nil {
		return nil
	}
	finishMessage := ""
	if call.FinishMessage != nil {
		finishMessage = *call.FinishMessage
	}
	var organizationID *int64
	if call.OrganizationID > 0 {
		value := call.OrganizationID
		organizationID = &value
	}
	return &proto_models.ChatInferenceCall{
		Id:                        call.ID,
		ChannelId:                 call.ChannelID,
		MessageId:                 call.MessageID,
		AgentRunId:                call.AgentRunID,
		AgentSessionId:            call.AgentSessionID,
		AgentSessionConfigVersion: call.AgentSessionConfigVersion,
		AgentType:                 call.AgentType,
		TriggeringUserId:          call.TriggeringUserID,
		ActorUserId:               call.ActorUserID,
		OrganizationId:            organizationID,
		AgentStepIndex:            call.AgentStepIndex,
		CallId:                    call.CallID,
		CallIndex:                 call.CallIndex,
		Purpose:                   call.Purpose,
		ProviderId:                call.ProviderID,
		ProviderModelId:           call.ProviderModelID,
		InferenceModelId:          call.InferenceModelID,
		FinishReason:              call.FinishReason,
		FinishMessage:             finishMessage,
		LatencyMs:                 call.LatencyMs,
		InputTokens:               call.InputTokens,
		OutputTokens:              call.OutputTokens,
		TotalTokens:               call.TotalTokens,
		ThoughtsTokens:            call.ThoughtsTokens,
		CachedContentTokens:       call.CachedContentTokens,
		InputCharacters:           call.InputCharacters,
		OutputCharacters:          call.OutputCharacters,
		InputImages:               call.InputImages,
		OutputImages:              call.OutputImages,
		InputVideos:               call.InputVideos,
		OutputVideos:              call.OutputVideos,
		InputAudioFiles:           call.InputAudioFiles,
		OutputAudioFiles:          call.OutputAudioFiles,
		CustomUsageJson:           call.CustomUsageJSON,
		GenkitSpanId:              call.GenkitSpanID,
		MetadataJson:              call.MetadataJSON,
		CreatedAt:                 timestampOrNil(call.CreatedAt),
	}
}
