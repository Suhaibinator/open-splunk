package ingest

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDurableBatchRejectResponseFitsMetadataAndDoesNotMutateSource(t *testing.T) {
	t.Parallel()

	violations := make([]*opensplunk.FieldViolation, HardMaxBatchEvents)
	for index := range violations {
		violations[index] = &opensplunk.FieldViolation{
			FieldPath: strings.Repeat("nested.", int(HardMaxNestingDepth)) +
				strings.Repeat("x", 4<<10),
			Code:    "invalid_field",
			Message: "field is invalid",
		}
	}
	rejection := &opensplunk.BatchReject{
		BatchId:       "durable-large-terminal-rejection",
		BatchSequence: 17,
		Code:          opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
		Message:       "batch contains no valid events",
		Violations:    violations,
	}

	response := durableBatchRejectResponse(rejection)
	if response.GetBatchReject() == nil {
		t.Fatal("durable response omitted BatchReject")
	}
	if err := ValidateDurableBatchRejection(response.GetBatchReject()); err != nil {
		t.Fatalf("canonical durable rejection is invalid: %v", err)
	}
	if size := uint64(proto.Size(response)); size > durableBatchRejectResponseBudget ||
		size+durableBatchRejectEncodingHeadroom > HardMaxDurableMetadataBytes {
		t.Fatalf(
			"durable response size = %d, budget = %d, metadata limit = %d",
			size,
			durableBatchRejectResponseBudget,
			HardMaxDurableMetadataBytes,
		)
	}
	got := response.GetBatchReject().GetViolations()
	if len(got) == 0 || len(got) >= len(violations) ||
		got[len(got)-1].GetCode() != "truncated" {
		t.Fatalf("durable violations = %d, final = %#v", len(got), got[len(got)-1])
	}
	if len(rejection.GetViolations()) != len(violations) {
		t.Fatal("durable response bounding mutated the source rejection")
	}
}

func TestValidateDurableBatchRejectionRejectsNonCanonicalState(t *testing.T) {
	t.Parallel()

	valid := &opensplunk.BatchReject{
		BatchId:       "batch-durable-validation",
		BatchSequence: 23,
		Code:          opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
		Message:       "batch contains no authorized valid events",
		Violations: []*opensplunk.FieldViolation{{
			FieldPath: "events[0].index_name",
			Code:      "unauthorized_index",
			Message:   "token is not authorized for the requested index",
		}},
	}
	if err := ValidateDurableBatchRejection(valid); err != nil {
		t.Fatalf("valid durable rejection: %v", err)
	}

	const aggregateViolationCount = 160
	unknownField := []byte{0xa0, 0x06, 0x01}
	tests := []struct {
		name   string
		mutate func(*opensplunk.BatchReject)
	}{
		{name: "invalid batch ID", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.BatchId = "bad batch id"
		}},
		{name: "zero batch sequence", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.BatchSequence = 0
		}},
		{name: "unspecified code", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.Code = opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_UNSPECIFIED
		}},
		{name: "unknown code", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.Code = opensplunk.BatchRejectionCode(255)
		}},
		{name: "empty message", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.Message = ""
		}},
		{name: "invalid UTF-8 message", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.Message = string([]byte{0xff})
		}},
		{name: "oversized violation field", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.Violations[0].FieldPath = strings.Repeat(
				"p",
				maximumBatchRejectViolationFieldPathBytes+1,
			)
		}},
		{name: "nil violation", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.Violations[0] = nil
		}},
		{name: "invalid UTF-8 violation", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.Violations[0].Message = string([]byte{0xff})
		}},
		{name: "unknown rejection fields", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.ProtoReflect().SetUnknown(unknownField)
		}},
		{name: "unknown violation fields", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.Violations[0].ProtoReflect().SetUnknown(unknownField)
		}},
		{name: "too many violations", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.Violations = make(
				[]*opensplunk.FieldViolation,
				int(HardMaxBatchEvents)+2,
			)
			for index := range rejection.Violations {
				rejection.Violations[index] = &opensplunk.FieldViolation{
					FieldPath: "events", Code: "invalid", Message: "invalid event",
				}
			}
		}},
		{name: "aggregate response too large", mutate: func(rejection *opensplunk.BatchReject) {
			rejection.Violations = make(
				[]*opensplunk.FieldViolation,
				aggregateViolationCount,
			)
			for index := range rejection.Violations {
				rejection.Violations[index] = &opensplunk.FieldViolation{
					FieldPath: "events",
					Code:      "invalid",
					Message: strings.Repeat(
						"m",
						maximumBatchRejectViolationMessageBytes,
					),
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rejection := proto.Clone(valid).(*opensplunk.BatchReject)
			test.mutate(rejection)
			if err := ValidateDurableBatchRejection(rejection); err == nil {
				t.Fatalf("ValidateDurableBatchRejection accepted %#v", rejection)
			}
		})
	}
}

func TestResponseForStoredBatchClonesStrictRejectionOnlyResult(t *testing.T) {
	t.Parallel()

	config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
	service, err := NewService(config, staticTestAuthorizer(), acceptingStore())
	if err != nil {
		t.Fatal(err)
	}
	batch := validTestBatch(
		"collector-a",
		"batch-stored-rejection",
		11,
		validTestEvent("event-a", "main"),
	)
	rejection := batchRejection(
		batch,
		opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
		"batch contains no authorized valid events",
		"events",
		"no_authorized_events",
	)
	result := StoreResult{BatchRejection: rejection}

	response, err := service.responseForStoredBatch(batch, result, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := response.GetBatchReject()
	if !proto.Equal(got, rejection) {
		t.Fatalf("stored rejection = %#v, want %#v", got, rejection)
	}
	if got == rejection || got.GetViolations()[0] == rejection.GetViolations()[0] {
		t.Fatal("stored rejection response aliases store-owned protobuf state")
	}
	rejection.Message = "mutated after response"
	rejection.Violations[0].Message = "mutated after response"
	if got.GetMessage() == rejection.GetMessage() ||
		got.GetViolations()[0].GetMessage() == rejection.GetViolations()[0].GetMessage() {
		t.Fatal("stored rejection response changed after source mutation")
	}
}

func TestResponseForStoredBatchRejectsMalformedRejectionOnlyResult(t *testing.T) {
	t.Parallel()

	config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
	service, err := NewService(config, staticTestAuthorizer(), acceptingStore())
	if err != nil {
		t.Fatal(err)
	}
	batch := validTestBatch(
		"collector-a",
		"batch-malformed-stored-rejection",
		19,
		validTestEvent("event-a", "main"),
	)
	validRejection := batchRejection(
		batch,
		opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
		"batch contains no authorized valid events",
		"events",
		"no_authorized_events",
	)
	acknowledged := batch.GetBatchSequence()
	tests := []struct {
		name   string
		mutate func(*StoreResult)
	}{
		{name: "accepted", mutate: func(result *StoreResult) { result.Accepted = 1 }},
		{name: "duplicate", mutate: func(result *StoreResult) { result.Duplicate = 1 }},
		{name: "acknowledged through", mutate: func(result *StoreResult) {
			result.AcknowledgedThrough = &acknowledged
		}},
		{name: "committed at", mutate: func(result *StoreResult) {
			result.CommittedAt = validationTestNow
		}},
		{name: "original event count", mutate: func(result *StoreResult) {
			result.OriginalEventCount = 1
		}},
		{name: "event rejections", mutate: func(result *StoreResult) {
			result.RejectedEvents = []*opensplunk.EventRejection{}
		}},
		{name: "wrong batch ID", mutate: func(result *StoreResult) {
			result.BatchRejection.BatchId = "other-batch"
		}},
		{name: "wrong sequence", mutate: func(result *StoreResult) {
			result.BatchRejection.BatchSequence++
		}},
		{name: "unspecified code", mutate: func(result *StoreResult) {
			result.BatchRejection.Code = opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_UNSPECIFIED
		}},
		{name: "unknown code", mutate: func(result *StoreResult) {
			result.BatchRejection.Code = opensplunk.BatchRejectionCode(255)
		}},
		{name: "too many violations", mutate: func(result *StoreResult) {
			result.BatchRejection.Violations = make(
				[]*opensplunk.FieldViolation,
				int(HardMaxBatchEvents)+2,
			)
			for index := range result.BatchRejection.Violations {
				result.BatchRejection.Violations[index] = &opensplunk.FieldViolation{
					FieldPath: "events", Code: "invalid", Message: "invalid event",
				}
			}
		}},
		{name: "not durably bounded", mutate: func(result *StoreResult) {
			result.BatchRejection.Message = strings.Repeat("x", int(HardMaxDurableMetadataBytes))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rejection := proto.Clone(validRejection).(*opensplunk.BatchReject)
			result := StoreResult{BatchRejection: rejection}
			test.mutate(&result)
			response, responseErr := service.responseForStoredBatch(batch, result, nil)
			if response != nil || responseErr == nil {
				t.Fatalf("responseForStoredBatch = (%#v, %v), want nil/error", response, responseErr)
			}
		})
	}
}

func TestBatchRejectTruncatesDiagnosticsWithinTransportLimit(t *testing.T) {
	t.Parallel()
	violations := make([]*opensplunk.FieldViolation, HardMaxBatchEvents)
	for index := range violations {
		violations[index] = &opensplunk.FieldViolation{
			FieldPath: strings.Repeat("nested.", int(HardMaxNestingDepth)) + strings.Repeat("x", 4<<10),
			Code:      "invalid_field",
			Message:   "field is invalid",
		}
	}
	rejection := &opensplunk.BatchReject{
		BatchId: "large-terminal-rejection", BatchSequence: 1,
		Code:    opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
		Message: "batch contains no valid events", Violations: violations,
	}
	if uint64(proto.Size(&opensplunk.CollectResponse{
		Payload: &opensplunk.CollectResponse_BatchReject{BatchReject: rejection},
	})) <= HardMaxCollectResponseBytes {
		t.Fatal("test rejection does not exceed the hard response limit")
	}

	response := responseWithBatchReject(rejection)
	response.StreamSequence = math.MaxUint64
	response.SentAt = timestamppb.New(time.Unix(253402300799, 999999999).UTC())
	if size := uint64(proto.Size(response)); size > HardMaxCollectResponseBytes {
		t.Fatalf("bounded response size = %d, limit = %d", size, HardMaxCollectResponseBytes)
	}
	got := response.GetBatchReject().GetViolations()
	if len(got) == 0 {
		t.Fatal("bounded response omitted its truncation marker")
	}
	if len(got) >= len(violations) || got[len(got)-1].GetCode() != "truncated" {
		t.Fatalf("bounded violations = %d with final %#v", len(got), got[len(got)-1])
	}
	if len(rejection.GetViolations()) != len(violations) {
		t.Fatal("response bounding mutated the source rejection")
	}
}

func TestBatchRejectNeverReflectsOversizedUnvalidatedScalars(t *testing.T) {
	t.Parallel()
	rejection := &opensplunk.BatchReject{
		BatchId:       strings.Repeat("b", 3<<20),
		BatchSequence: math.MaxUint64,
		Code:          opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_COLLECTOR_ID_MISMATCH,
		Message:       strings.Repeat("m", 3<<20),
		Violations: []*opensplunk.FieldViolation{{
			FieldPath: strings.Repeat("p", 3<<20),
			Code:      strings.Repeat("c", 3<<20),
			Message:   strings.Repeat("v", 3<<20),
		}},
	}
	response := responseWithBatchReject(rejection)
	response.StreamSequence = math.MaxUint64
	response.SentAt = timestamppb.New(time.Unix(253402300799, 999999999).UTC())
	if size := uint64(proto.Size(response)); size > HardMaxCollectResponseBytes {
		t.Fatalf("bounded response size = %d, limit = %d", size, HardMaxCollectResponseBytes)
	}
	if got := response.GetBatchReject().GetBatchId(); got != "" {
		t.Fatalf("reflected invalid batch ID length = %d, want omitted", len(got))
	}
	if len(response.GetBatchReject().GetMessage()) > 8<<10 {
		t.Fatal("batch rejection message was not bounded")
	}
	if rejection.GetBatchId() == "" || len(rejection.GetMessage()) != 3<<20 {
		t.Fatal("response bounding mutated the source rejection")
	}
}

func TestCollectorMismatchBeforeBatchIDValidationStillReturnsBoundedRejection(t *testing.T) {
	t.Parallel()
	config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
	service, err := NewService(config, staticTestAuthorizer(), acceptingStore())
	if err != nil {
		t.Fatal(err)
	}
	batch := validTestBatch(
		"different-collector",
		strings.Repeat("b", 3<<20),
		1,
		validTestEvent("event-a", "main"),
	)
	response, err := service.processBatch(
		context.Background(), batch, testBatchStreamState(service), service.config.Clock().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetBatchReject().GetCode() != opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_COLLECTOR_ID_MISMATCH {
		t.Fatalf("rejection = %#v", response.GetBatchReject())
	}
	if response.GetBatchReject().GetBatchId() != "" {
		t.Fatal("response reflected the unvalidated oversized batch ID")
	}
	response.StreamSequence = math.MaxUint64
	response.SentAt = timestamppb.New(time.Unix(253402300799, 999999999).UTC())
	if size := uint64(proto.Size(response)); size > HardMaxCollectResponseBytes {
		t.Fatalf("bounded response size = %d, limit = %d", size, HardMaxCollectResponseBytes)
	}
}
