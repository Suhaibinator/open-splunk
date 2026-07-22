package ingest

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBatchRejectTruncatesDiagnosticsWithinTransportLimit(t *testing.T) {
	t.Parallel()
	violations := make([]*opensplunkv1.FieldViolation, HardMaxBatchEvents)
	for index := range violations {
		violations[index] = &opensplunkv1.FieldViolation{
			FieldPath: strings.Repeat("nested.", int(HardMaxNestingDepth)) + strings.Repeat("x", 4<<10),
			Code:      "invalid_field",
			Message:   "field is invalid",
		}
	}
	rejection := &opensplunkv1.BatchReject{
		BatchId: "large-terminal-rejection", BatchSequence: 1,
		Code:    opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
		Message: "batch contains no valid events", Violations: violations,
	}
	if uint64(proto.Size(&opensplunkv1.CollectResponse{
		Payload: &opensplunkv1.CollectResponse_BatchReject{BatchReject: rejection},
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
	rejection := &opensplunkv1.BatchReject{
		BatchId:       strings.Repeat("b", 3<<20),
		BatchSequence: math.MaxUint64,
		Code:          opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_COLLECTOR_ID_MISMATCH,
		Message:       strings.Repeat("m", 3<<20),
		Violations: []*opensplunkv1.FieldViolation{{
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
	service, err := NewService(testServiceConfig(), staticTestAuthorizer(), acceptingStore())
	if err != nil {
		t.Fatal(err)
	}
	batch := validTestBatch(
		"different-collector",
		strings.Repeat("b", 3<<20),
		1,
		validTestEvent("event-a", "main"),
	)
	response, err := service.processBatch(context.Background(), batch, testBatchStreamState())
	if err != nil {
		t.Fatal(err)
	}
	if response.GetBatchReject().GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_COLLECTOR_ID_MISMATCH {
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
