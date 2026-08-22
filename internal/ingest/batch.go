package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"fortio.org/safecast"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

func (s *Service) processBatch(
	ctx context.Context,
	batch *opensplunk.EventBatch,
	state *streamState,
	boundaryAt time.Time,
) (*opensplunk.CollectResponse, error) {
	return s.processBatchWithDeferredAuthority(ctx, batch, state, boundaryAt, nil)
}

func (s *Service) processBatchWithDeferredAuthority(
	ctx context.Context,
	batch *opensplunk.EventBatch,
	state *streamState,
	boundaryAt time.Time,
	deferredAuthority error,
) (*opensplunk.CollectResponse, error) {
	state.pendingThrottle = nil
	uncompressedBytes, eventSizes, rejection := s.validateBatchHardEnvelope(batch, state)
	if rejection != nil {
		if deferredAuthority != nil {
			return nil, authorityRPCError(deferredAuthority)
		}
		return responseWithBatchReject(rejection), nil
	}

	identity, err := batchFingerprint(batch)
	if err != nil {
		if deferredAuthority != nil {
			return nil, authorityRPCError(deferredAuthority)
		}
		return responseWithBatchReject(batchRejection(
			batch,
			opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION,
			"batch cannot be deterministically decoded",
			"batch",
			"invalid_protobuf",
		)), nil
	}
	if conflictRejection := pendingBatchIdentityConflict(state, batch.GetBatchSequence(), identity); conflictRejection != nil {
		if deferredAuthority != nil {
			return nil, authorityRPCError(deferredAuthority)
		}
		return responseWithRetryBatch(
			batch,
			opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			s.config.DefaultRetryAfter,
			"batch identity conflicts with an unresolved stream-local batch; reconnect and retry",
		), nil
	}
	durableIdentity := StoreBatchIdentity{
		TenantID:          state.authorization.TenantID,
		CollectorID:       state.collectorID,
		BatchID:           batch.GetBatchId(),
		BatchSequence:     batch.GetBatchSequence(),
		SourceBatchSHA256: identity.contentHash,
	}
	if recoverable, ok := s.store.(RecoverableEventStore); ok {
		storedState, result, lookupErr := recoverable.LookupBatch(ctx, durableIdentity)
		if lookupErr != nil {
			if deferredAuthority != nil {
				return nil, authorityRPCError(deferredAuthority)
			}
			return s.storeFailure(batch, state, lookupErr)
		}
		switch storedState {
		case StoredBatchCommitted:
			if result.BatchRejection != nil {
				return nil, status.Error(codes.Internal, "event store returned a rejection for a committed batch")
			}
			observeBatchSequence(state, batch.GetBatchSequence())
			completeBatchIdentity(state, batch.GetBatchSequence(), identity)
			return s.responseForStoredBatch(batch, result, nil)
		case StoredBatchRejected:
			if result.BatchRejection == nil {
				return nil, status.Error(codes.Internal, "event store omitted a rejected batch disposition")
			}
			observeBatchSequence(state, batch.GetBatchSequence())
			completeBatchIdentity(state, batch.GetBatchSequence(), identity)
			return s.responseForStoredBatch(batch, result, nil)
		case StoredBatchPending:
			result, resumeErr := recoverable.ResumeBatch(ctx, durableIdentity)
			if resumeErr != nil {
				if isStoredBatchGone(resumeErr) {
					if deferredAuthority != nil {
						return nil, authorityRPCError(deferredAuthority)
					}
					break
				}
				if isDurableIdentityConflict(resumeErr) {
					observeBatchSequence(state, batch.GetBatchSequence())
					if deferredAuthority != nil {
						return nil, authorityRPCError(deferredAuthority)
					}
					completeBatchIdentity(state, batch.GetBatchSequence(), identity)
					return s.storeFailure(batch, state, resumeErr)
				}
				if !rememberDurablePendingBatchIdentity(
					state,
					batch.GetBatchSequence(),
					identity,
					boundaryAt,
				) {
					if _, retryable := retryDetails(resumeErr, s.config.DefaultRetryAfter); retryable {
						return nil, status.Error(
							codes.Unavailable,
							"collector stream cannot retain another durable pending batch; reconnect and retry",
						)
					}
				}
				return s.storeFailure(batch, state, resumeErr)
			}
			observeBatchSequence(state, batch.GetBatchSequence())
			completeBatchIdentity(state, batch.GetBatchSequence(), identity)
			return s.responseForStoredBatch(batch, result, nil)
		case StoredBatchNotFound:
		default:
			if deferredAuthority != nil {
				return nil, authorityRPCError(deferredAuthority)
			}
			return nil, status.Error(codes.Internal, "event store returned an invalid durable batch state")
		}
	}
	if deferredAuthority != nil {
		return nil, authorityRPCError(deferredAuthority)
	}

	receivedAt, rejection, atCapacity := recordBatchIdentity(
		state,
		batch.GetBatchSequence(),
		identity,
		boundaryAt,
		s.config.MaxInFlightBatches,
	)
	if rejection != nil {
		return responseWithRetryBatch(
			batch,
			opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			s.config.DefaultRetryAfter,
			"batch identity conflicts with stream-local sequence history; reconnect and retry",
		), nil
	}
	if atCapacity {
		return responseWithRetryBatch(
			batch,
			opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			s.config.DefaultRetryAfter,
			"maximum in-flight batch limit reached",
		), nil
	}
	if rejection := s.validateBatchPolicy(batch, receivedAt, uncompressedBytes); rejection != nil {
		return s.rejectRecordedBatch(
			ctx, batch, state, identity, durableIdentity, receivedAt, rejection,
		)
	}

	normalized := make([]*StoredEvent, 0, len(batch.GetEvents()))
	rejections := make([]*opensplunk.EventRejection, 0)
	seenEventIDs := make(map[string]struct{}, len(batch.GetEvents()))
	retentionByIndex := make(map[string]time.Duration)
	quotaByIndex := make(map[string]*ingestquota.Charge)
	var admittedUncompressedBytes uint64
	for eventIndex, event := range batch.GetEvents() {
		var policy resolvedIndexPolicy
		if event != nil {
			if !validIdentifier(event.GetEventId(), s.config.Limits.MaxIDBytes) {
				rejections = append(rejections, toProtoRejection(
					uint32(eventIndex),
					event.GetEventId(),
					eventFailure(
						opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID,
						"event_id is empty or has an invalid format",
						"event_id",
						"invalid_event_id",
					),
				))
				continue
			}
			if _, duplicate := seenEventIDs[event.GetEventId()]; duplicate {
				rejections = append(rejections, toProtoRejection(uint32(eventIndex), event.GetEventId(), eventFailure(
					opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID,
					"event_id is duplicated within the batch", "event_id", "duplicate_event_id",
				)))
				continue
			}
			seenEventIDs[event.GetEventId()] = struct{}{}
			if !validIndexName(event.GetIndexName()) {
				rejections = append(rejections, toProtoRejection(
					uint32(eventIndex),
					event.GetEventId(),
					eventFailure(
						opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_INDEX,
						"index_name is empty or has an invalid format",
						"index_name",
						"invalid_index",
					),
				))
				continue
			}
			var authorized bool
			policy, authorized = state.indexPolicies[event.GetIndexName()]
			if !authorized {
				rejections = append(rejections, toProtoRejection(
					uint32(eventIndex),
					event.GetEventId(),
					eventFailure(
						opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX,
						"token is not authorized for the requested index",
						"index_name",
						"unauthorized_index",
					),
				))
				continue
			}
			if policy.retentionPeriod <= 0 {
				return nil, status.Error(codes.Internal, "resolved index policy is invalid")
			}
		}
		validator := s.validator
		if event != nil {
			validator = &policy.validator
		}
		normalizedEvent, eventErr := validator.validateAndNormalizeEventWithSize(event, EventContext{
			ReceivedAt:         receivedAt,
			TimestampReference: receivedAt,
			DefaultSourcetype:  policy.defaultSourcetype,
			TenantID:           state.authorization.TenantID,
			CollectorID:        state.collectorID,
			BatchID:            batch.GetBatchId(),
		}, eventSizes[eventIndex])
		if eventErr != nil {
			eventID := ""
			if event != nil {
				eventID = event.GetEventId()
			}
			rejections = append(rejections, toProtoRejection(uint32(eventIndex), eventID, eventErr))
			continue
		}
		if eventErr := state.eventAuthorization.rejection(normalizedEvent); eventErr != nil {
			rejections = append(rejections, toProtoRejection(
				uint32(eventIndex),
				normalizedEvent.Event.GetEventId(),
				eventErr,
			))
			continue
		}
		normalized = append(normalized, normalizedEvent)
		indexName := normalizedEvent.Event.GetIndexName()
		retentionByIndex[indexName] = policy.retentionPeriod
		sourceBytes := eventSizes[eventIndex]
		if sourceBytes == 0 ||
			admittedUncompressedBytes > HardMaxBatchBytes-sourceBytes {
			return nil, status.Error(codes.Internal, "admitted quota charge is outside the durable range")
		}
		admittedUncompressedBytes += sourceBytes
		charge := quotaByIndex[indexName]
		if charge == nil {
			charge = &ingestquota.Charge{
				Scope: ingestquota.ScopeKey{
					Kind:     ingestquota.ScopeKindIndex,
					TenantID: state.authorization.TenantID,
					Identity: indexName,
				},
				Limits: policy.ingestionRateLimits,
			}
			quotaByIndex[indexName] = charge
		}
		charge.Events++
		charge.UncompressedBytes += sourceBytes
	}

	if len(normalized) == 0 {
		return s.rejectRecordedBatch(ctx, batch, state, identity, durableIdentity, receivedAt, &opensplunk.BatchReject{
			BatchId:       batch.GetBatchId(),
			BatchSequence: batch.GetBatchSequence(),
			Code:          opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
			Message:       "batch contains no authorized valid events",
			Violations:    rejectionViolations(rejections),
		})
	}
	if !durableOutboxFits(state, batch, normalized) || !durableOutcomeFits(normalized, rejections) {
		return s.rejectRecordedBatch(
			ctx,
			batch,
			state,
			identity,
			durableIdentity,
			receivedAt,
			batchRejection(
				batch,
				opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE,
				"normalized batch outcome exceeds the durable replay limit",
				"events",
				"durable_replay_too_large",
			),
		)
	}
	originalEventCount, countOK := boundedBatchEventCount(batch)
	if !countOK {
		return nil, status.Error(codes.Internal, "validated batch event count is outside the durable range")
	}
	admittedEventCount, admittedCountOK := nonNegativeIntUint64(len(normalized))
	if !admittedCountOK || admittedEventCount == 0 ||
		admittedEventCount > uint64(HardMaxBatchEvents) {
		return nil, status.Error(codes.Internal, "admitted quota count is outside the durable range")
	}
	quotaCharges := make([]ingestquota.Charge, 0, len(quotaByIndex)+1)
	quotaCharges = append(quotaCharges, ingestquota.Charge{
		Scope: ingestquota.ScopeKey{
			Kind:     ingestquota.ScopeKindToken,
			TenantID: state.authorization.TenantID,
			Identity: state.authorization.SubjectID,
		},
		Limits:            state.authorization.TokenRateLimits,
		Events:            admittedEventCount,
		UncompressedBytes: admittedUncompressedBytes,
	})
	for _, authorizedPolicy := range state.authorization.AuthorizedIndexes {
		if charge := quotaByIndex[authorizedPolicy.Name]; charge != nil {
			quotaCharges = append(quotaCharges, *charge)
		}
	}
	quotaAdmission := &ingestquota.Admission{Charges: quotaCharges}

	result, err := s.store.Store(ctx, StoreBatch{
		TenantID:           state.authorization.TenantID,
		CollectorID:        state.collectorID,
		BatchID:            batch.GetBatchId(),
		BatchSequence:      batch.GetBatchSequence(),
		OriginalEventCount: originalEventCount,
		SourceBatchSHA256:  identity.contentHash,
		ReceivedAt:         receivedAt,
		Events:             normalized,
		RetentionByIndex:   retentionByIndex,
		RejectedEvents:     rejections,
		QuotaAdmission:     quotaAdmission,
		QuotaEvaluatedAt:   boundaryAt.UTC(),
	})
	if err != nil {
		if isDurableIdentityConflict(err) {
			completeBatchIdentity(state, batch.GetBatchSequence(), identity)
		}
		return s.storeFailure(batch, state, err)
	}
	completeBatchIdentity(state, batch.GetBatchSequence(), identity)
	return s.responseForStoredBatch(batch, result, rejections)
}

func (s *Service) rejectRecordedBatch(
	ctx context.Context,
	batch *opensplunk.EventBatch,
	state *streamState,
	identity batchIdentity,
	durableIdentity StoreBatchIdentity,
	receivedAt time.Time,
	rejection *opensplunk.BatchReject,
) (*opensplunk.CollectResponse, error) {
	recoverable, ok := s.store.(RecoverableEventStore)
	if !ok {
		return nil, status.Error(codes.Internal, "event store cannot durably record terminal batch rejections")
	}
	durableRejection, err := canonicalDurableBatchRejection(rejection)
	if err != nil {
		return nil, status.Error(codes.Internal, "terminal batch rejection is invalid")
	}
	result, err := recoverable.RejectBatch(ctx, StoreBatchRejection{
		Identity:   durableIdentity,
		ReceivedAt: receivedAt,
		Rejection:  durableRejection,
	})
	if err != nil {
		if isDurableIdentityConflict(err) {
			completeBatchIdentity(state, batch.GetBatchSequence(), identity)
		}
		return s.storeFailure(batch, state, err)
	}
	completeBatchIdentity(state, batch.GetBatchSequence(), identity)
	return s.responseForStoredBatch(batch, result, nil)
}

func (s *Service) storeFailure(
	batch *opensplunk.EventBatch,
	state *streamState,
	err error,
) (*opensplunk.CollectResponse, error) {
	if isDurableIdentityConflict(err) {
		return responseWithBatchReject(batchRejection(
			batch,
			opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT,
			"batch ID or sequence is already bound to different durable source bytes",
			"batch_sequence",
			"sequence_conflict",
		)), nil
	}
	if retry, ok := retryDetails(err, s.config.DefaultRetryAfter); ok {
		message := "temporary storage failure"
		var storeError *TransientStoreError
		if errors.As(err, &storeError) &&
			storeError.ThrottleReason != opensplunk.ThrottleReason_THROTTLE_REASON_UNSPECIFIED {
			throttleMessage := "ingestion rate limit reached"
			state.pendingThrottle = &opensplunk.Throttle{
				Reason:           storeError.ThrottleReason,
				MinimumSendDelay: durationpb.New(retry.after),
				Message:          &throttleMessage,
			}
			message = throttleMessage
		}
		return responseWithRetryBatch(batch, retry.reason, retry.after, message), nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, status.FromContextError(err).Err()
	}
	return nil, status.Error(codes.Internal, "event storage failed before acknowledgment")
}

func isDurableIdentityConflict(err error) bool {
	var conflict *DurableIdentityConflictError
	return errors.As(err, &conflict)
}

func isStoredBatchGone(err error) bool {
	var gone *StoredBatchGoneError
	return errors.As(err, &gone)
}

func (s *Service) responseForStoredBatch(
	batch *opensplunk.EventBatch,
	result StoreResult,
	fallbackRejections []*opensplunk.EventRejection,
) (*opensplunk.CollectResponse, error) {
	if result.BatchRejection != nil {
		if result.Accepted != 0 ||
			result.Duplicate != 0 ||
			result.AcknowledgedThrough != nil ||
			!result.CommittedAt.IsZero() ||
			result.OriginalEventCount != 0 ||
			result.RejectedEvents != nil ||
			!validStoredBatchRejection(batch, result.BatchRejection) {
			return nil, status.Error(codes.Internal, "event store returned an inconsistent durable batch rejection")
		}
		return &opensplunk.CollectResponse{
			Payload: &opensplunk.CollectResponse_BatchReject{
				BatchReject: proto.Clone(result.BatchRejection).(*opensplunk.BatchReject),
			},
		}, nil
	}
	batchEventCount, countOK := boundedBatchEventCount(batch)
	if !countOK {
		return nil, status.Error(codes.Internal, "event store returned an outcome for an oversized batch")
	}
	rejections := result.RejectedEvents
	if rejections == nil {
		rejections = fallbackRejections
	}
	originalEventCount := result.OriginalEventCount
	if originalEventCount == 0 {
		originalEventCount = batchEventCount
	}
	if originalEventCount != batchEventCount ||
		uint64(result.Accepted)+uint64(result.Duplicate)+uint64(len(rejections)) != uint64(originalEventCount) ||
		!validStoredRejections(batch, batchEventCount, rejections) {
		return nil, status.Error(codes.Internal, "event store returned an inconsistent durable batch outcome")
	}
	committedAt := result.CommittedAt
	if committedAt.IsZero() {
		committedAt = s.config.Clock()
	}
	committedTimestamp := timestamppb.New(committedAt.UTC())
	if committedTimestamp.CheckValid() != nil {
		return nil, status.Error(codes.Internal, "event store returned an invalid commit timestamp")
	}
	return &opensplunk.CollectResponse{Payload: &opensplunk.CollectResponse_BatchAck{BatchAck: &opensplunk.BatchAck{
		BatchId:                          batch.GetBatchId(),
		BatchSequence:                    batch.GetBatchSequence(),
		AcknowledgedThroughBatchSequence: result.AcknowledgedThrough,
		Durability:                       opensplunk.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
		AcceptedEventCount:               result.Accepted,
		DuplicateEventCount:              result.Duplicate,
		RejectedEvents:                   rejections,
		CommittedAt:                      committedTimestamp,
	}}}, nil
}

func validStoredBatchRejection(
	batch *opensplunk.EventBatch,
	rejection *opensplunk.BatchReject,
) bool {
	if batch == nil || rejection == nil ||
		rejection.GetBatchId() != batch.GetBatchId() ||
		rejection.GetBatchSequence() != batch.GetBatchSequence() {
		return false
	}
	return ValidateDurableBatchRejection(rejection) == nil
}

func validStoredRejections(
	batch *opensplunk.EventBatch,
	eventCount uint32,
	rejections []*opensplunk.EventRejection,
) bool {
	seen := make(map[uint32]struct{}, len(rejections))
	for _, rejection := range rejections {
		if rejection == nil || rejection.GetEventIndex() >= eventCount {
			return false
		}
		if _, duplicate := seen[rejection.GetEventIndex()]; duplicate {
			return false
		}
		seen[rejection.GetEventIndex()] = struct{}{}
		event := batch.GetEvents()[rejection.GetEventIndex()]
		if event != nil && rejection.GetEventId() != event.GetEventId() {
			return false
		}
	}
	return true
}

func durableOutboxFits(state *streamState, batch *opensplunk.EventBatch, events []*StoredEvent) bool {
	// OSOB header/checksum; three length-prefixed IDs; sequence, source digest,
	// receive time, and two event counts. Keep this byte accounting in lockstep
	// with clickhouse.encodeStoreOutbox.
	total := uint64(5 + sha256.Size + 8*3 + 8 + sha256.Size + 8 + 4 + 4)
	for _, value := range []string{state.authorization.TenantID, state.collectorID, batch.GetBatchId()} {
		length, ok := nonNegativeIntUint64(len(value))
		if !ok || !boundedSizeAdd(&total, length, HardMaxDurableOutboxBytes) {
			return false
		}
	}
	for _, stored := range events {
		if stored == nil || stored.Event == nil {
			return false
		}
		size, ok := protobufSizeUint64(stored.Event)
		if !ok || !boundedSizeAdd(&total, 8+size, HardMaxDurableOutboxBytes) {
			return false
		}
	}
	return true
}

func durableOutcomeFits(events []*StoredEvent, rejections []*opensplunk.EventRejection) bool {
	// OSVM header/checksum, index count, sequence, and outcome counts. Retention
	// values are fixed-width; rejection payloads are deterministic protobufs.
	total := uint64(5 + sha256.Size + 8 + 8 + 4 + 4)
	indexes := make(map[string]struct{}, len(events))
	for _, stored := range events {
		if stored == nil || stored.Event == nil {
			return false
		}
		indexes[stored.Event.GetIndexName()] = struct{}{}
	}
	for index := range indexes {
		length, ok := nonNegativeIntUint64(len(index))
		if !ok || !boundedSizeAdd(&total, 8+length+8, HardMaxDurableMetadataBytes) {
			return false
		}
	}
	for _, rejection := range rejections {
		if rejection == nil {
			return false
		}
		size, ok := protobufSizeUint64(rejection)
		if !ok || !boundedSizeAdd(&total, 8+size, HardMaxDurableMetadataBytes) {
			return false
		}
	}
	return true
}

func boundedSizeAdd(total *uint64, value, limit uint64) bool {
	if total == nil || *total > limit || value > limit-*total {
		return false
	}
	*total += value
	return true
}

const (
	batchRejectResponseBudget                 = HardMaxCollectResponseBytes - 64
	durableBatchRejectEncodingHeadroom        = 512
	durableBatchRejectResponseBudget          = HardMaxDurableMetadataBytes - durableBatchRejectEncodingHeadroom
	maximumBatchRejectMessageBytes            = 8 << 10
	maximumBatchRejectViolationFieldPathBytes = 16 << 10
	maximumBatchRejectViolationCodeBytes      = 256
	maximumBatchRejectViolationMessageBytes   = 8 << 10
)

func responseWithBatchReject(rejection *opensplunk.BatchReject) *opensplunk.CollectResponse {
	// Collect adds a uint64 sequence and a protobuf Timestamp after this helper
	// returns. Their maximum valid wire encoding is below 64 bytes, including
	// tags and lengths, so reserve that space inside the transport ceiling.
	return boundedBatchRejectResponse(rejection, batchRejectResponseBudget)
}

func durableBatchRejectResponse(rejection *opensplunk.BatchReject) *opensplunk.CollectResponse {
	// Durable stores add an envelope, identity fields, receive time, checksum,
	// and length prefixes around this protobuf. Keep explicit headroom so the
	// persisted terminal disposition always fits the metadata ceiling.
	return boundedBatchRejectResponse(rejection, durableBatchRejectResponseBudget)
}

func canonicalDurableBatchRejection(
	rejection *opensplunk.BatchReject,
) (*opensplunk.BatchReject, error) {
	canonical := durableBatchRejectResponse(rejection).GetBatchReject()
	if err := ValidateDurableBatchRejection(canonical); err != nil {
		return nil, err
	}
	return canonical, nil
}

// ValidateDurableBatchRejection verifies that rejection is a canonical,
// bounded whole-batch disposition which a durable store may replay directly.
// Callers which have an expected source identity must additionally compare the
// batch ID and sequence with that identity.
func ValidateDurableBatchRejection(rejection *opensplunk.BatchReject) error {
	if rejection == nil {
		return errors.New("durable batch rejection is required")
	}
	if !validIdentifier(rejection.GetBatchId(), HardMaxIDBytes) {
		return errors.New("durable batch rejection has an invalid batch ID")
	}
	if rejection.GetBatchSequence() == 0 {
		return errors.New("durable batch rejection has an invalid batch sequence")
	}
	code := rejection.GetCode()
	if code == opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_UNSPECIFIED {
		return errors.New("durable batch rejection has an unspecified code")
	}
	if _, known := opensplunk.BatchRejectionCode_name[int32(code)]; !known {
		return errors.New("durable batch rejection has an unknown code")
	}
	if !validDurableBatchRejectText(rejection.GetMessage(), maximumBatchRejectMessageBytes) {
		return errors.New("durable batch rejection has an invalid message")
	}
	if len(rejection.GetViolations()) > int(HardMaxBatchEvents)+1 {
		return errors.New("durable batch rejection has too many field violations")
	}
	for index, violation := range rejection.GetViolations() {
		if violation == nil {
			return fmt.Errorf("durable batch rejection field violation %d is nil", index)
		}
		if !validDurableBatchRejectText(
			violation.GetFieldPath(),
			maximumBatchRejectViolationFieldPathBytes,
		) || !validDurableBatchRejectText(
			violation.GetCode(),
			maximumBatchRejectViolationCodeBytes,
		) || !validDurableBatchRejectText(
			violation.GetMessage(),
			maximumBatchRejectViolationMessageBytes,
		) {
			return fmt.Errorf("durable batch rejection field violation %d is invalid", index)
		}
		if len(violation.ProtoReflect().GetUnknown()) != 0 {
			return fmt.Errorf("durable batch rejection field violation %d contains unknown fields", index)
		}
	}
	if len(rejection.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("durable batch rejection contains unknown fields")
	}
	response := &opensplunk.CollectResponse{
		Payload: &opensplunk.CollectResponse_BatchReject{BatchReject: rejection},
	}
	responseSize, sizeOK := protobufSizeUint64(response)
	if !sizeOK || responseSize > durableBatchRejectResponseBudget {
		return errors.New("durable batch rejection exceeds the replay size limit")
	}
	return nil
}

func validDurableBatchRejectText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && utf8.ValidString(value)
}

func boundedBatchRejectResponse(
	rejection *opensplunk.BatchReject,
	budget uint64,
) *opensplunk.CollectResponse {
	if rejection == nil {
		return &opensplunk.CollectResponse{
			Payload: &opensplunk.CollectResponse_BatchReject{},
		}
	}
	boundedRejection := &opensplunk.BatchReject{
		BatchId:       rejection.GetBatchId(),
		BatchSequence: rejection.GetBatchSequence(),
		Code:          rejection.GetCode(),
		Message: boundedProtocolText(
			rejection.GetMessage(),
			maximumBatchRejectMessageBytes,
			"batch rejected; oversized message omitted",
		),
		Violations: make([]*opensplunk.FieldViolation, len(rejection.GetViolations())),
	}
	// A batch can fail an earlier envelope check (for example collector-ID
	// mismatch) before batch_id itself is validated. Never reflect an unbounded
	// or malformed request scalar into the server response.
	if !validIdentifier(boundedRejection.BatchId, HardMaxIDBytes) {
		boundedRejection.BatchId = ""
	}
	for index, violation := range rejection.GetViolations() {
		if violation == nil {
			boundedRejection.Violations[index] = &opensplunk.FieldViolation{
				FieldPath: "violations", Code: "invalid", Message: "invalid violation detail omitted",
			}
			continue
		}
		boundedRejection.Violations[index] = &opensplunk.FieldViolation{
			FieldPath: boundedProtocolText(
				violation.GetFieldPath(),
				maximumBatchRejectViolationFieldPathBytes,
				"violations",
			),
			Code: boundedProtocolText(
				violation.GetCode(),
				maximumBatchRejectViolationCodeBytes,
				"invalid",
			),
			Message: boundedProtocolText(
				violation.GetMessage(),
				maximumBatchRejectViolationMessageBytes,
				"oversized violation detail omitted",
			),
		}
	}
	response := &opensplunk.CollectResponse{
		Payload: &opensplunk.CollectResponse_BatchReject{BatchReject: boundedRejection},
	}
	responseSize, sizeOK := protobufSizeUint64(response)
	if sizeOK && responseSize <= budget {
		return response
	}

	// An invalid event contributes at most one violation, but a maximum-size
	// batch can still make their expanded nested field paths larger than the
	// response transport limit. Retain the largest ordered prefix that fits and
	// append an explicit summary marker. Binary search avoids repeatedly sizing
	// every prefix under adversarial input.
	summary := &opensplunk.FieldViolation{
		FieldPath: "violations",
		Code:      "truncated",
		Message:   "additional field violations omitted to stay within the protocol response limit",
	}
	build := func(prefix int) *opensplunk.CollectResponse {
		bounded := &opensplunk.BatchReject{
			BatchId:       boundedRejection.GetBatchId(),
			BatchSequence: boundedRejection.GetBatchSequence(),
			Code:          boundedRejection.GetCode(),
			Message:       boundedRejection.GetMessage(),
			Violations:    make([]*opensplunk.FieldViolation, prefix+1),
		}
		copy(bounded.Violations, boundedRejection.GetViolations()[:prefix])
		bounded.Violations[prefix] = summary
		return &opensplunk.CollectResponse{
			Payload: &opensplunk.CollectResponse_BatchReject{BatchReject: bounded},
		}
	}
	low, high := 0, len(boundedRejection.GetViolations())
	for low < high {
		middle := low + (high-low+1)/2
		candidateSize, candidateOK := protobufSizeUint64(build(middle))
		if candidateOK && candidateSize <= budget {
			low = middle
		} else {
			high = middle - 1
		}
	}
	result := build(low)
	resultSize, resultOK := protobufSizeUint64(result)
	if resultOK && resultSize <= budget {
		return result
	}
	// All fields above are bounded, so this is defensive against future proto
	// growth. Preserve only the stable rejection category and sequence.
	return &opensplunk.CollectResponse{Payload: &opensplunk.CollectResponse_BatchReject{BatchReject: &opensplunk.BatchReject{
		BatchSequence: boundedRejection.GetBatchSequence(),
		Code:          boundedRejection.GetCode(),
		Message:       "batch rejected; response details omitted",
	}}}
}

func boundedProtocolText(value string, maximum int, fallback string) string {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return fallback
	}
	return value
}

func responseWithRetryBatch(
	batch *opensplunk.EventBatch,
	reason opensplunk.RetryBatchReason,
	retryAfter time.Duration,
	message string,
) *opensplunk.CollectResponse {
	return &opensplunk.CollectResponse{
		Payload: &opensplunk.CollectResponse_RetryBatch{RetryBatch: &opensplunk.RetryBatch{
			BatchId:       batch.GetBatchId(),
			BatchSequence: batch.GetBatchSequence(),
			Reason:        reason,
			RetryAfter:    durationpb.New(retryAfter),
			Message:       &message,
		}},
	}
}

// validateBatchHardEnvelope enforces immutable protocol and resource-safety
// bounds before hashing or consulting durable identity state. These checks are
// intentionally independent of mutable deployment policy so a committed retry
// can recover its original acknowledgment after limits or wall time change.
func (s *Service) validateBatchHardEnvelope(batch *opensplunk.EventBatch, state *streamState) (uint64, []uint64, *opensplunk.BatchReject) {
	if batch == nil {
		return 0, nil, batchRejection(nil, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION, "batch payload is required", "batch", "required")
	}
	if batch.GetCollectorId() != state.collectorID {
		return 0, nil, batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_COLLECTOR_ID_MISMATCH, "batch collector_id does not match hello", "collector_id", "collector_id_mismatch")
	}
	if !validIdentifier(batch.GetBatchId(), HardMaxIDBytes) {
		return 0, nil, batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_INVALID_BATCH_ID, "batch_id is empty or has an invalid format", "batch_id", "invalid_batch_id")
	}
	if batch.GetBatchSequence() == 0 {
		return 0, nil, batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "batch_sequence must be positive", "batch_sequence", "invalid_sequence")
	}
	if batch.GetCreatedAt() == nil || batch.GetCreatedAt().CheckValid() != nil {
		return 0, nil, batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION, "batch created_at is invalid", "created_at", "invalid_protobuf_timestamp")
	}
	if len(batch.GetEvents()) == 0 {
		return 0, nil, batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS, "batch must contain at least one event", "events", "required")
	}
	if _, ok := boundedBatchEventCount(batch); !ok {
		return 0, nil, batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS, "batch contains too many events", "events", "too_many_events")
	}
	sizes := make([]uint64, len(batch.GetEvents()))
	var totalBytes uint64
	for eventIndex, event := range batch.GetEvents() {
		size, ok := protobufSizeUint64(event)
		if !ok || size > HardMaxEventBytes {
			return 0, nil, batchRejection(
				batch,
				opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE,
				"batch contains an event which exceeds the hard size limit",
				fmt.Sprintf("events[%d]", eventIndex),
				"event_too_large",
			)
		}
		sizes[eventIndex] = size
		totalBytes += size
	}
	actualBytes := totalBytes
	if actualBytes > HardMaxBatchBytes || batch.GetUncompressedSizeBytes() > HardMaxBatchBytes || batch.GetUncompressedSizeBytes() != actualBytes {
		return 0, nil, batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE, "batch size is invalid or exceeds the hard limit", "uncompressed_size_bytes", "batch_size_mismatch_or_limit")
	}
	if !digestsEqual(batch.GetEventIdsSha256(), EventIDDigest(batch.GetEvents())) {
		return 0, nil, batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_EVENT_ID_DIGEST_MISMATCH, "event ID digest does not match the ordered events", "event_ids_sha256", "digest_mismatch")
	}
	return totalBytes, sizes, nil
}

func boundedBatchEventCount(batch *opensplunk.EventBatch) (uint32, bool) {
	if batch == nil {
		return 0, false
	}
	length, ok := nonNegativeIntUint64(len(batch.GetEvents()))
	if !ok || length > uint64(HardMaxBatchEvents) {
		return 0, false
	}

	return safecast.MustConv[uint32](len(batch.GetEvents())), true
}

// validateBatchPolicy enforces deployment-configurable limits only after an
// exact durable lookup has missed. It must never precede recovery of a stored
// acknowledgment because these limits and the wall clock can change.
func (s *Service) validateBatchPolicy(batch *opensplunk.EventBatch, receivedAt time.Time, uncompressedBytes uint64) *opensplunk.BatchReject {
	if !validIdentifier(batch.GetBatchId(), s.config.Limits.MaxIDBytes) {
		return batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_INVALID_BATCH_ID, "batch_id exceeds the configured limit", "batch_id", "invalid_batch_id")
	}
	if err := s.validator.validateTimestamp(batch.GetCreatedAt(), receivedAt); err != nil {
		return batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION, "batch created_at is outside accepted bounds", "created_at", err.Error())
	}
	if uint64(len(batch.GetEvents())) > uint64(s.config.Limits.MaxBatchEvents) {
		return batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS, "batch contains too many events", "events", "too_many_events")
	}
	actualBytes := uncompressedBytes
	if actualBytes > s.config.Limits.MaxBatchBytes || batch.GetUncompressedSizeBytes() > s.config.Limits.MaxBatchBytes {
		return batchRejection(batch, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE, "batch exceeds the configured size limit", "uncompressed_size_bytes", "batch_size_limit")
	}
	return nil
}

func recordBatchIdentity(
	state *streamState,
	sequence uint64,
	identity batchIdentity,
	receivedAt time.Time,
	maxInFlight uint32,
) (time.Time, *opensplunk.BatchReject, bool) {
	if state.pendingBatches == nil {
		state.pendingBatches = make(map[uint64]pendingBatchIdentity)
	}
	if state.pendingSequencesByID == nil {
		state.pendingSequencesByID = make(map[string]uint64)
	}
	if rejection := pendingBatchIdentityConflict(state, sequence, identity); rejection != nil {
		return time.Time{}, rejection, false
	}
	if pending, ok := state.pendingBatches[sequence]; ok {
		return pending.receivedAt, nil, false
	}
	if state.hasHighestBatchSequence && sequence <= state.highestBatchSequence {
		return time.Time{}, batchRejectionValues(identity.batchID, sequence, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "batch_sequence was already completed or used for a different durable batch", "batch_sequence", "sequence_conflict"), false
	}
	if uint64(len(state.pendingBatches)) >= uint64(maxInFlight) {
		return time.Time{}, nil, true
	}
	state.hasHighestBatchSequence = true
	state.highestBatchSequence = sequence
	state.pendingBatches[sequence] = pendingBatchIdentity{identity: identity, receivedAt: receivedAt}
	state.pendingSequencesByID[identity.batchID] = sequence
	return receivedAt, nil, false
}

// rememberDurablePendingBatchIdentity preserves the exact wire identity and a
// stable receive boundary after the durable store has proved that identity is
// pending. That proof makes it safe to bypass the stream's soft in-flight limit
// and high-water check: a later lookup may observe that the unsent reservation
// was safely abandoned and must then be allowed to restart fresh admission.
// The hard bound still caps memory retained by one stream.
func rememberDurablePendingBatchIdentity(
	state *streamState,
	sequence uint64,
	identity batchIdentity,
	receivedAt time.Time,
) bool {
	if state.pendingBatches == nil {
		state.pendingBatches = make(map[uint64]pendingBatchIdentity)
	}
	if state.pendingSequencesByID == nil {
		state.pendingSequencesByID = make(map[string]uint64)
	}
	if pendingBatchIdentityConflict(state, sequence, identity) != nil {
		return false
	}
	if _, exists := state.pendingBatches[sequence]; exists {
		observeBatchSequence(state, sequence)
		return true
	}
	if uint64(len(state.pendingBatches)) >= uint64(HardMaxInFlightBatches) {
		return false
	}
	state.pendingBatches[sequence] = pendingBatchIdentity{identity: identity, receivedAt: receivedAt}
	state.pendingSequencesByID[identity.batchID] = sequence
	observeBatchSequence(state, sequence)
	return true
}

func pendingBatchIdentityConflict(state *streamState, sequence uint64, identity batchIdentity) *opensplunk.BatchReject {
	if pending, ok := state.pendingBatches[sequence]; ok && pending.identity != identity {
		return batchRejectionValues(identity.batchID, sequence, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "retry changed the durable batch payload", "batch_sequence", "sequence_conflict")
	}
	if previousSequence, ok := state.pendingSequencesByID[identity.batchID]; ok && previousSequence != sequence {
		return batchRejectionValues(identity.batchID, sequence, opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "batch_id is already pending with a different sequence", "batch_id", "sequence_conflict")
	}
	return nil
}

func completeBatchIdentity(state *streamState, sequence uint64, identity batchIdentity) {
	pending, ok := state.pendingBatches[sequence]
	if !ok || pending.identity != identity {
		return
	}
	delete(state.pendingBatches, sequence)
	if state.pendingSequencesByID[identity.batchID] == sequence {
		delete(state.pendingSequencesByID, identity.batchID)
	}
}

func observeBatchSequence(state *streamState, sequence uint64) {
	if !state.hasHighestBatchSequence || sequence > state.highestBatchSequence {
		state.hasHighestBatchSequence = true
		state.highestBatchSequence = sequence
	}
}

func batchFingerprint(batch *opensplunk.EventBatch) (batchIdentity, error) {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(batch)
	if err != nil {
		return batchIdentity{}, err
	}
	return batchIdentity{
		batchID:     batch.GetBatchId(),
		contentHash: sha256.Sum256(encoded),
	}, nil
}

func batchRejection(batch *opensplunk.EventBatch, code opensplunk.BatchRejectionCode, message, path, violationCode string) *opensplunk.BatchReject {
	if batch == nil {
		return batchRejectionValues("", 0, code, message, path, violationCode)
	}
	return batchRejectionValues(batch.GetBatchId(), batch.GetBatchSequence(), code, message, path, violationCode)
}

func batchRejectionValues(batchID string, sequence uint64, code opensplunk.BatchRejectionCode, message, path, violationCode string) *opensplunk.BatchReject {
	return &opensplunk.BatchReject{
		BatchId:       batchID,
		BatchSequence: sequence,
		Code:          code,
		Message:       message,
		Violations: []*opensplunk.FieldViolation{{
			FieldPath: path,
			Code:      violationCode,
			Message:   message,
		}},
	}
}

func toProtoRejection(index uint32, eventID string, eventErr *EventError) *opensplunk.EventRejection {
	return &opensplunk.EventRejection{
		EventIndex: index,
		EventId:    eventID,
		Code:       eventErr.Code,
		Message:    eventErr.Message,
		Violations: eventErr.Violations,
	}
}

func rejectionViolations(rejections []*opensplunk.EventRejection) []*opensplunk.FieldViolation {
	result := make([]*opensplunk.FieldViolation, 0, len(rejections))
	for _, rejection := range rejections {
		for _, violation := range rejection.GetViolations() {
			if violation == nil {
				continue
			}
			result = append(result, &opensplunk.FieldViolation{
				FieldPath: fmt.Sprintf("events[%d].%s", rejection.GetEventIndex(), violation.GetFieldPath()),
				Code:      violation.GetCode(),
				Message:   violation.GetMessage(),
			})
		}
	}
	return result
}

type retryInfo struct {
	reason opensplunk.RetryBatchReason
	after  time.Duration
}

func retryDetails(err error, defaultAfter time.Duration) (retryInfo, bool) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return retryInfo{}, false
	}
	if storeError, ok := errors.AsType[*TransientStoreError](err); ok {
		reason := storeError.Reason
		if reason == opensplunk.RetryBatchReason_RETRY_BATCH_REASON_UNSPECIFIED {
			reason = opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE
		}
		after := storeError.RetryAfter
		if after <= 0 {
			after = defaultAfter
		}
		if reason == opensplunk.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED &&
			after > ingestquota.MaximumRetryAfter {
			after = ingestquota.MaximumRetryAfter
		}
		return retryInfo{reason: reason, after: after}, true
	}
	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) && temporary.Temporary() {
		return retryInfo{
			reason: opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE,
			after:  defaultAfter,
		}, true
	}
	if code := status.Code(err); code == codes.Unavailable || code == codes.ResourceExhausted {
		reason := opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE
		if code == codes.ResourceExhausted {
			reason = opensplunk.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED
		}
		return retryInfo{reason: reason, after: defaultAfter}, true
	}
	return retryInfo{}, false
}
