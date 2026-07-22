package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Service) processBatch(ctx context.Context, batch *opensplunkv1.EventBatch, state *streamState) (*opensplunkv1.CollectResponse, error) {
	if rejection := s.validateBatchEnvelope(batch, state); rejection != nil {
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
	receivedAt, rejection := recordBatchIdentity(state, batch.GetBatchSequence(), identity, s.config.Clock().UTC())
	if rejection != nil {
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
		return responseWithBatchReject(&opensplunkv1.BatchReject{
			BatchId:       batch.GetBatchId(),
			BatchSequence: batch.GetBatchSequence(),
			Code:          opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
			Message:       "batch contains no authorized valid events",
			Violations:    rejectionViolations(rejections),
		}), nil
	}

	result, err := s.store.Store(ctx, StoreBatch{
		TenantID:      state.authorization.TenantID,
		CollectorID:   state.collectorID,
		BatchID:       batch.GetBatchId(),
		BatchSequence: batch.GetBatchSequence(),
		ReceivedAt:    receivedAt,
		Events:        normalized,
	})
	if err != nil {
		if retry, ok := retryDetails(err, s.config.DefaultRetryAfter); ok {
			message := "temporary storage failure"
			return &opensplunkv1.CollectResponse{Payload: &opensplunkv1.CollectResponse_RetryBatch{RetryBatch: &opensplunkv1.RetryBatch{
				BatchId:       batch.GetBatchId(),
				BatchSequence: batch.GetBatchSequence(),
				Reason:        retry.reason,
				RetryAfter:    durationpb.New(retry.after),
				Message:       &message,
			}}}, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Error(codes.Internal, "event storage failed before acknowledgment")
	}
	if uint64(result.Accepted)+uint64(result.Duplicate) != uint64(len(normalized)) {
		return nil, status.Error(codes.Internal, "event store returned inconsistent accepted and duplicate counts")
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

func responseWithBatchReject(rejection *opensplunkv1.BatchReject) *opensplunkv1.CollectResponse {
	return &opensplunkv1.CollectResponse{
		Payload: &opensplunkv1.CollectResponse_BatchReject{BatchReject: rejection},
	}
}

func (s *Service) validateBatchEnvelope(batch *opensplunkv1.EventBatch, state *streamState) *opensplunkv1.BatchReject {
	if batch == nil {
		return batchRejection(nil, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION, "batch payload is required", "batch", "required")
	}
	if batch.GetCollectorId() != state.collectorID {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_COLLECTOR_ID_MISMATCH, "batch collector_id does not match hello", "collector_id", "collector_id_mismatch")
	}
	if !validIdentifier(batch.GetBatchId(), s.config.Limits.MaxIDBytes) {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_INVALID_BATCH_ID, "batch_id is empty or has an invalid format", "batch_id", "invalid_batch_id")
	}
	if batch.GetBatchSequence() == 0 {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "batch_sequence must be positive", "batch_sequence", "invalid_sequence")
	}
	if batch.GetProtocolMajor() != state.protocolMajor || batch.GetProtocolMinor() != state.protocolMinor {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION, "batch protocol version does not match hello", "protocol_major", "protocol_mismatch")
	}
	if err := s.validator.validateTimestamp(batch.GetCreatedAt(), s.config.Clock()); err != nil {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION, "batch created_at is invalid or outside accepted bounds", "created_at", err.Error())
	}
	if len(batch.GetEvents()) == 0 {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS, "batch must contain at least one event", "events", "required")
	}
	if uint64(len(batch.GetEvents())) > uint64(s.config.Limits.MaxBatchEvents) {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS, "batch contains too many events", "events", "too_many_events")
	}
	actualBytes := UncompressedEventBytes(batch.GetEvents())
	if actualBytes > s.config.Limits.MaxBatchBytes || batch.GetUncompressedSizeBytes() > s.config.Limits.MaxBatchBytes || batch.GetUncompressedSizeBytes() != actualBytes {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE, "batch size is invalid or exceeds the configured limit", "uncompressed_size_bytes", "batch_size_mismatch_or_limit")
	}
	if !digestsEqual(batch.GetEventIdsSha256(), EventIDDigest(batch.GetEvents())) {
		return batchRejection(batch, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_EVENT_ID_DIGEST_MISMATCH, "event ID digest does not match the ordered events", "event_ids_sha256", "digest_mismatch")
	}
	return nil
}

func recordBatchIdentity(state *streamState, sequence uint64, identity batchIdentity, receivedAt time.Time) (time.Time, *opensplunkv1.BatchReject) {
	if state.hasLastBatch {
		if sequence == state.lastBatchSequence {
			if state.lastBatchIdentity != identity {
				return time.Time{}, batchRejectionValues(identity.batchID, sequence, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "retry changed the durable batch payload", "batch_sequence", "sequence_conflict")
			}
			return state.lastBatchReceivedAt, nil
		}
		if sequence < state.lastBatchSequence {
			return time.Time{}, batchRejectionValues(identity.batchID, sequence, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "batch_sequence was already used for a different durable batch", "batch_sequence", "sequence_conflict")
		}
		if identity.batchID == state.lastBatchIdentity.batchID {
			return time.Time{}, batchRejectionValues(identity.batchID, sequence, opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT, "batch_id was already used with a different sequence", "batch_id", "sequence_conflict")
		}
	}
	state.hasLastBatch = true
	state.lastBatchSequence = sequence
	state.lastBatchIdentity = identity
	state.lastBatchReceivedAt = receivedAt
	return receivedAt, nil
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
