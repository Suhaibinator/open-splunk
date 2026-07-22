package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Service) processBatch(ctx context.Context, batch *opensplunkv1.EventBatch, state *streamState) (*opensplunkv1.CollectResponse, error) {
	if rejection := s.validateBatchHardEnvelope(batch, state); rejection != nil {
		return responseWithBatchReject(rejection), nil
	}

	identity, err := batchFingerprint(batch)
	if err != nil {
		return responseWithBatchReject(batchRejection(
			batch,
			opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION,
			"batch cannot be deterministically decoded",
			"batch",
			"invalid_protobuf",
		)), nil
	}
	if rejection := pendingBatchIdentityConflict(state, batch.GetBatchSequence(), identity); rejection != nil {
		return responseWithBatchReject(rejection), nil
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
			return s.storeFailure(batch, lookupErr)
		}
		switch storedState {
		case StoredBatchCommitted:
			observeBatchSequence(state, batch.GetBatchSequence())
			completeBatchIdentity(state, batch.GetBatchSequence(), identity)
			return s.responseForStoredBatch(batch, result, nil)
		case StoredBatchPending:
			observeBatchSequence(state, batch.GetBatchSequence())
			result, resumeErr := recoverable.ResumeBatch(ctx, durableIdentity)
			if resumeErr != nil {
				if isDurableIdentityConflict(resumeErr) {
					completeBatchIdentity(state, batch.GetBatchSequence(), identity)
				}
				return s.storeFailure(batch, resumeErr)
			}
			completeBatchIdentity(state, batch.GetBatchSequence(), identity)
			return s.responseForStoredBatch(batch, result, nil)
		case StoredBatchNotFound:
		default:
			return nil, status.Error(codes.Internal, "event store returned an invalid durable batch state")
		}
	}

	receivedAt, rejection, atCapacity := recordBatchIdentity(
		state,
		batch.GetBatchSequence(),
		identity,
		s.config.Clock().UTC(),
		s.config.MaxInFlightBatches,
	)
	if rejection != nil {
		return responseWithBatchReject(rejection), nil
	}
	if atCapacity {
		return responseWithRetryBatch(
			batch,
			opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			s.config.DefaultRetryAfter,
			"maximum in-flight batch limit reached",
		), nil
	}
	if rejection := s.validateBatchPolicy(batch, receivedAt); rejection != nil {
		completeBatchIdentity(state, batch.GetBatchSequence(), identity)
		return responseWithBatchReject(rejection), nil
	}

	normalized := make([]*StoredEvent, 0, len(batch.GetEvents()))
	rejections := make([]*opensplunkv1.EventRejection, 0)
	seenEventIDs := make(map[string]struct{}, len(batch.GetEvents()))
	for eventIndex, event := range batch.GetEvents() {
		if event != nil {
			if _, duplicate := seenEventIDs[event.GetEventId()]; duplicate {
				rejections = append(rejections, toProtoRejection(uint32(eventIndex), event.GetEventId(), eventFailure(
					opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID,
					"event_id is duplicated within the batch", "event_id", "duplicate_event_id",
				)))
				continue
			}
			seenEventIDs[event.GetEventId()] = struct{}{}
		}
		normalizedEvent, eventErr := s.validator.ValidateAndNormalizeEvent(event, EventContext{
			ReceivedAt:         receivedAt,
			TimestampReference: batch.GetCreatedAt().AsTime(),
			TenantID:           state.authorization.TenantID,
			CollectorID:        state.collectorID,
			BatchID:            batch.GetBatchId(),
		})
		if eventErr != nil {
			eventID := ""
			if event != nil {
				eventID = event.GetEventId()
			}
			rejections = append(rejections, toProtoRejection(uint32(eventIndex), eventID, eventErr))
			continue
		}
		if _, authorized := state.authorizedIndexes[normalizedEvent.Event.GetIndexName()]; !authorized {
			rejections = append(rejections, &opensplunkv1.EventRejection{
				EventIndex: uint32(eventIndex),
				EventId:    normalizedEvent.Event.GetEventId(),
				Code:       opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX,
				Message:    "token is not authorized for the requested index",
				Violations: []*opensplunkv1.FieldViolation{{
					FieldPath: "index_name",
					Code:      "unauthorized_index",
					Message:   "token is not authorized for the requested index",
				}},
			})
			continue
		}
		normalized = append(normalized, normalizedEvent)
	}

	if len(normalized) == 0 {
		completeBatchIdentity(state, batch.GetBatchSequence(), identity)
		return responseWithBatchReject(&opensplunkv1.BatchReject{
			BatchId:       batch.GetBatchId(),
			BatchSequence: batch.GetBatchSequence(),
			Code:          opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
			Message:       "batch contains no authorized valid events",
			Violations:    rejectionViolations(rejections),
		}), nil
	}
	if !durableOutboxFits(state, batch, normalized) || !durableOutcomeFits(normalized, rejections) {
		completeBatchIdentity(state, batch.GetBatchSequence(), identity)
		return responseWithBatchReject(batchRejection(
			batch,
			opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE,
			"normalized batch outcome exceeds the durable replay limit",
			"events",
			"durable_replay_too_large",
		)), nil
	}

	result, err := s.store.Store(ctx, StoreBatch{
		TenantID:           state.authorization.TenantID,
		CollectorID:        state.collectorID,
		BatchID:            batch.GetBatchId(),
		BatchSequence:      batch.GetBatchSequence(),
		OriginalEventCount: uint32(len(batch.GetEvents())),
		SourceBatchSHA256:  identity.contentHash,
		ReceivedAt:         receivedAt,
		Events:             normalized,
		RejectedEvents:     rejections,
	})
	if err != nil {
		if isDurableIdentityConflict(err) {
			completeBatchIdentity(state, batch.GetBatchSequence(), identity)
		}
		return s.storeFailure(batch, err)
	}
	completeBatchIdentity(state, batch.GetBatchSequence(), identity)
	return s.responseForStoredBatch(batch, result, rejections)
}

func (s *Service) storeFailure(batch *opensplunkv1.EventBatch, err error) (*opensplunkv1.CollectResponse, error) {
	if isDurableIdentityConflict(err) {
		return responseWithBatchReject(batchRejection(
			batch,
			opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT,
			"batch ID or sequence is already bound to different durable source bytes",
			"batch_sequence",
			"sequence_conflict",
		)), nil
	}
	if retry, ok := retryDetails(err, s.config.DefaultRetryAfter); ok {
		return responseWithRetryBatch(batch, retry.reason, retry.after, "temporary storage failure"), nil
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

func (s *Service) responseForStoredBatch(
	batch *opensplunkv1.EventBatch,
	result StoreResult,
	fallbackRejections []*opensplunkv1.EventRejection,
) (*opensplunkv1.CollectResponse, error) {
	rejections := result.RejectedEvents
	if rejections == nil {
		rejections = fallbackRejections
	}
	originalEventCount := result.OriginalEventCount
	if originalEventCount == 0 {
		originalEventCount = uint32(len(batch.GetEvents()))
	}
	if originalEventCount != uint32(len(batch.GetEvents())) ||
		uint64(result.Accepted)+uint64(result.Duplicate)+uint64(len(rejections)) != uint64(originalEventCount) ||
		!validStoredRejections(batch, rejections) {
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
	return &opensplunkv1.CollectResponse{Payload: &opensplunkv1.CollectResponse_BatchAck{BatchAck: &opensplunkv1.BatchAck{
		BatchId:                          batch.GetBatchId(),
		BatchSequence:                    batch.GetBatchSequence(),
		AcknowledgedThroughBatchSequence: result.AcknowledgedThrough,
		Durability:                       opensplunkv1.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
		AcceptedEventCount:               result.Accepted,
		DuplicateEventCount:              result.Duplicate,
		RejectedEvents:                   rejections,
		CommittedAt:                      committedTimestamp,
	}}}, nil
}

func validStoredRejections(batch *opensplunkv1.EventBatch, rejections []*opensplunkv1.EventRejection) bool {
	seen := make(map[uint32]struct{}, len(rejections))
	for _, rejection := range rejections {
		if rejection == nil || rejection.GetEventIndex() >= uint32(len(batch.GetEvents())) {
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

func durableOutboxFits(state *streamState, batch *opensplunkv1.EventBatch, events []*StoredEvent) bool {
	// OSOB header/checksum; three length-prefixed IDs; sequence, source digest,
	// receive time, and two event counts. Keep this byte accounting in lockstep
	// with clickhouse.encodeStoreOutbox.
	total := uint64(5 + sha256.Size + 8*3 + 8 + sha256.Size + 8 + 4 + 4)
	for _, value := range []string{state.authorization.TenantID, state.collectorID, batch.GetBatchId()} {
		if !boundedSizeAdd(&total, uint64(len(value)), HardMaxDurableOutboxBytes) {
			return false
		}
	}
	for _, stored := range events {
		if stored == nil || stored.Event == nil ||
			!boundedSizeAdd(&total, 8+uint64(proto.Size(stored.Event)), HardMaxDurableOutboxBytes) {
			return false
		}
	}
	return true
}

func durableOutcomeFits(events []*StoredEvent, rejections []*opensplunkv1.EventRejection) bool {
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
		if !boundedSizeAdd(&total, 8+uint64(len(index))+8, HardMaxDurableMetadataBytes) {
			return false
		}
	}
	for _, rejection := range rejections {
		if rejection == nil ||
			!boundedSizeAdd(&total, 8+uint64(proto.Size(rejection)), HardMaxDurableMetadataBytes) {
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

func responseWithBatchReject(rejection *opensplunkv1.BatchReject) *opensplunkv1.CollectResponse {
	// Collect adds a uint64 sequence and a protobuf Timestamp after this helper
	// returns. Their maximum valid wire encoding is below 64 bytes, including
	// tags and lengths, so reserve that space inside the transport ceiling.
	const batchRejectResponseBudget = HardMaxCollectResponseBytes - 64
	if rejection == nil {
		return &opensplunkv1.CollectResponse{
			Payload: &opensplunkv1.CollectResponse_BatchReject{},
		}
	}
	boundedRejection := &opensplunkv1.BatchReject{
		BatchId:       rejection.GetBatchId(),
		BatchSequence: rejection.GetBatchSequence(),
		Code:          rejection.GetCode(),
		Message:       boundedProtocolText(rejection.GetMessage(), 8<<10, "batch rejected; oversized message omitted"),
		Violations:    make([]*opensplunkv1.FieldViolation, len(rejection.GetViolations())),
	}
	// A batch can fail an earlier envelope check (for example collector-ID
	// mismatch) before batch_id itself is validated. Never reflect an unbounded
	// or malformed request scalar into the server response.
	if !validIdentifier(boundedRejection.BatchId, HardMaxIDBytes) {
		boundedRejection.BatchId = ""
	}
	for index, violation := range rejection.GetViolations() {
		if violation == nil {
			boundedRejection.Violations[index] = &opensplunkv1.FieldViolation{
				FieldPath: "violations", Code: "invalid", Message: "invalid violation detail omitted",
			}
			continue
		}
		boundedRejection.Violations[index] = &opensplunkv1.FieldViolation{
			FieldPath: boundedProtocolText(violation.GetFieldPath(), 16<<10, "violations"),
			Code:      boundedProtocolText(violation.GetCode(), 256, "invalid"),
			Message:   boundedProtocolText(violation.GetMessage(), 8<<10, "oversized violation detail omitted"),
		}
	}
	response := &opensplunkv1.CollectResponse{
		Payload: &opensplunkv1.CollectResponse_BatchReject{BatchReject: boundedRejection},
	}
	if uint64(proto.Size(response)) <= batchRejectResponseBudget {
		return response
	}

	// An invalid event contributes at most one violation, but a maximum-size
	// batch can still make their expanded nested field paths larger than the
	// response transport limit. Retain the largest ordered prefix that fits and
	// append an explicit summary marker. Binary search avoids repeatedly sizing
	// every prefix under adversarial input.
	summary := &opensplunkv1.FieldViolation{
		FieldPath: "violations",
		Code:      "truncated",
		Message:   "additional field violations omitted to stay within the protocol response limit",
	}
	build := func(prefix int) *opensplunkv1.CollectResponse {
		bounded := &opensplunkv1.BatchReject{
			BatchId:       boundedRejection.GetBatchId(),
			BatchSequence: boundedRejection.GetBatchSequence(),
			Code:          boundedRejection.GetCode(),
			Message:       boundedRejection.GetMessage(),
			Violations:    make([]*opensplunkv1.FieldViolation, prefix+1),
		}
		copy(bounded.Violations, boundedRejection.GetViolations()[:prefix])
		bounded.Violations[prefix] = summary
		return &opensplunkv1.CollectResponse{
			Payload: &opensplunkv1.CollectResponse_BatchReject{BatchReject: bounded},
		}
	}
	low, high := 0, len(boundedRejection.GetViolations())
	for low < high {
		middle := low + (high-low+1)/2
		if uint64(proto.Size(build(middle))) <= batchRejectResponseBudget {
			low = middle
		} else {
			high = middle - 1
		}
	}
	result := build(low)
	if uint64(proto.Size(result)) <= batchRejectResponseBudget {
		return result
	}
	// All fields above are bounded, so this is defensive against future proto
	// growth. Preserve only the stable rejection category and sequence.
	return &opensplunkv1.CollectResponse{Payload: &opensplunkv1.CollectResponse_BatchReject{BatchReject: &opensplunkv1.BatchReject{
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
	batch *opensplunkv1.EventBatch,
	reason opensplunkv1.RetryBatchReason,
	retryAfter time.Duration,
	message string,
) *opensplunkv1.CollectResponse {
	return &opensplunkv1.CollectResponse{
		Payload: &opensplunkv1.CollectResponse_RetryBatch{RetryBatch: &opensplunkv1.RetryBatch{
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
func (s *Service) validateBatchHardEnvelope(batch *opensplunkv1.EventBatch, state *streamState) *opensplunkv1.BatchReject {
	if batch == nil {
		return batchRejection(nil, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION, "batch payload is required", "batch", "required")
	}
	if batch.GetCollectorId() != state.collectorID {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_COLLECTOR_ID_MISMATCH, "batch collector_id does not match hello", "collector_id", "collector_id_mismatch")
	}
	if !validIdentifier(batch.GetBatchId(), HardMaxIDBytes) {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_INVALID_BATCH_ID, "batch_id is empty or has an invalid format", "batch_id", "invalid_batch_id")
	}
	if batch.GetBatchSequence() == 0 {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "batch_sequence must be positive", "batch_sequence", "invalid_sequence")
	}
	if batch.GetProtocolMajor() != state.protocolMajor || batch.GetProtocolMinor() != state.protocolMinor {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION, "batch protocol version does not match hello", "protocol_major", "protocol_mismatch")
	}
	if batch.GetCreatedAt() == nil || batch.GetCreatedAt().CheckValid() != nil {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION, "batch created_at is invalid", "created_at", "invalid_protobuf_timestamp")
	}
	if len(batch.GetEvents()) == 0 {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS, "batch must contain at least one event", "events", "required")
	}
	if uint64(len(batch.GetEvents())) > uint64(HardMaxBatchEvents) {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS, "batch contains too many events", "events", "too_many_events")
	}
	for eventIndex, event := range batch.GetEvents() {
		if uint64(proto.Size(event)) > HardMaxEventBytes {
			return batchRejection(
				batch,
				opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE,
				"batch contains an event which exceeds the hard size limit",
				fmt.Sprintf("events[%d]", eventIndex),
				"event_too_large",
			)
		}
	}
	actualBytes := UncompressedEventBytes(batch.GetEvents())
	if actualBytes > HardMaxBatchBytes || batch.GetUncompressedSizeBytes() > HardMaxBatchBytes || batch.GetUncompressedSizeBytes() != actualBytes {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE, "batch size is invalid or exceeds the hard limit", "uncompressed_size_bytes", "batch_size_mismatch_or_limit")
	}
	if !digestsEqual(batch.GetEventIdsSha256(), EventIDDigest(batch.GetEvents())) {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_EVENT_ID_DIGEST_MISMATCH, "event ID digest does not match the ordered events", "event_ids_sha256", "digest_mismatch")
	}
	return nil
}

// validateBatchPolicy enforces deployment-configurable limits only after an
// exact durable lookup has missed. It must never precede recovery of a stored
// acknowledgment because these limits and the wall clock can change.
func (s *Service) validateBatchPolicy(batch *opensplunkv1.EventBatch, receivedAt time.Time) *opensplunkv1.BatchReject {
	if !validIdentifier(batch.GetBatchId(), s.config.Limits.MaxIDBytes) {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_INVALID_BATCH_ID, "batch_id exceeds the configured limit", "batch_id", "invalid_batch_id")
	}
	if err := s.validator.validateTimestamp(batch.GetCreatedAt(), receivedAt); err != nil {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION, "batch created_at is outside accepted bounds", "created_at", err.Error())
	}
	if uint64(len(batch.GetEvents())) > uint64(s.config.Limits.MaxBatchEvents) {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS, "batch contains too many events", "events", "too_many_events")
	}
	actualBytes := UncompressedEventBytes(batch.GetEvents())
	if actualBytes > s.config.Limits.MaxBatchBytes || batch.GetUncompressedSizeBytes() > s.config.Limits.MaxBatchBytes {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE, "batch exceeds the configured size limit", "uncompressed_size_bytes", "batch_size_limit")
	}
	return nil
}

func recordBatchIdentity(
	state *streamState,
	sequence uint64,
	identity batchIdentity,
	receivedAt time.Time,
	maxInFlight uint32,
) (time.Time, *opensplunkv1.BatchReject, bool) {
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
		return time.Time{}, batchRejectionValues(identity.batchID, sequence, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "batch_sequence was already completed or used for a different durable batch", "batch_sequence", "sequence_conflict"), false
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

func pendingBatchIdentityConflict(state *streamState, sequence uint64, identity batchIdentity) *opensplunkv1.BatchReject {
	if pending, ok := state.pendingBatches[sequence]; ok && pending.identity != identity {
		return batchRejectionValues(identity.batchID, sequence, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "retry changed the durable batch payload", "batch_sequence", "sequence_conflict")
	}
	if previousSequence, ok := state.pendingSequencesByID[identity.batchID]; ok && previousSequence != sequence {
		return batchRejectionValues(identity.batchID, sequence, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "batch_id is already pending with a different sequence", "batch_id", "sequence_conflict")
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

func batchFingerprint(batch *opensplunkv1.EventBatch) (batchIdentity, error) {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(batch)
	if err != nil {
		return batchIdentity{}, err
	}
	return batchIdentity{
		batchID:     batch.GetBatchId(),
		contentHash: sha256.Sum256(encoded),
	}, nil
}

func batchRejection(batch *opensplunkv1.EventBatch, code opensplunkv1.BatchRejectionCode, message, path, violationCode string) *opensplunkv1.BatchReject {
	if batch == nil {
		return batchRejectionValues("", 0, code, message, path, violationCode)
	}
	return batchRejectionValues(batch.GetBatchId(), batch.GetBatchSequence(), code, message, path, violationCode)
}

func batchRejectionValues(batchID string, sequence uint64, code opensplunkv1.BatchRejectionCode, message, path, violationCode string) *opensplunkv1.BatchReject {
	return &opensplunkv1.BatchReject{
		BatchId:       batchID,
		BatchSequence: sequence,
		Code:          code,
		Message:       message,
		Violations: []*opensplunkv1.FieldViolation{{
			FieldPath: path,
			Code:      violationCode,
			Message:   message,
		}},
	}
}

func toProtoRejection(index uint32, eventID string, eventErr *EventError) *opensplunkv1.EventRejection {
	return &opensplunkv1.EventRejection{
		EventIndex: index,
		EventId:    eventID,
		Code:       eventErr.Code,
		Message:    eventErr.Message,
		Violations: eventErr.Violations,
	}
}

func rejectionViolations(rejections []*opensplunkv1.EventRejection) []*opensplunkv1.FieldViolation {
	result := make([]*opensplunkv1.FieldViolation, 0, len(rejections))
	for _, rejection := range rejections {
		for _, violation := range rejection.GetViolations() {
			if violation == nil {
				continue
			}
			result = append(result, &opensplunkv1.FieldViolation{
				FieldPath: fmt.Sprintf("events[%d].%s", rejection.GetEventIndex(), violation.GetFieldPath()),
				Code:      violation.GetCode(),
				Message:   violation.GetMessage(),
			})
		}
	}
	return result
}

type retryInfo struct {
	reason opensplunkv1.RetryBatchReason
	after  time.Duration
}

func retryDetails(err error, defaultAfter time.Duration) (retryInfo, bool) {
	var storeError *TransientStoreError
	if errors.As(err, &storeError) {
		reason := storeError.Reason
		if reason == opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_UNSPECIFIED {
			reason = opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE
		}
		after := storeError.RetryAfter
		if after <= 0 {
			after = defaultAfter
		}
		return retryInfo{reason: reason, after: after}, true
	}
	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) && temporary.Temporary() {
		return retryInfo{
			reason: opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE,
			after:  defaultAfter,
		}, true
	}
	if code := status.Code(err); code == codes.Unavailable || code == codes.ResourceExhausted {
		reason := opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE
		if code == codes.ResourceExhausted {
			reason = opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED
		}
		return retryInfo{reason: reason, after: defaultAfter}, true
	}
	return retryInfo{}, false
}
