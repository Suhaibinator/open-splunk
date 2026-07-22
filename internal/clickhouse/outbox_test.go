package clickhouse

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/proto"
)

func TestStoreOutboxRoundTripPreservesExactNormalizedBlock(t *testing.T) {
	t.Parallel()
	batch := validStoreBatch()
	batch.OriginalEventCount = 2
	batch.ReceivedAt = time.Date(2026, 7, 21, 1, 2, 3, 456789123, time.FixedZone("source", -7*60*60))
	batch.Events[0].IndexTime = batch.ReceivedAt
	encoded, err := encodeStoreOutbox(batch)
	if err != nil {
		t.Fatalf("encodeStoreOutbox: %v", err)
	}
	decoded, err := decodeStoreOutbox(encoded)
	if err != nil {
		t.Fatalf("decodeStoreOutbox: %v", err)
	}
	if decoded.TenantID != batch.TenantID || decoded.CollectorID != batch.CollectorID ||
		decoded.BatchID != batch.BatchID || decoded.BatchSequence != batch.BatchSequence ||
		decoded.SourceBatchSHA256 != batch.SourceBatchSHA256 || decoded.OriginalEventCount != batch.OriginalEventCount {
		t.Fatalf("decoded identity = %+v, want %+v", decoded, batch)
	}
	wantTime := batch.ReceivedAt.UTC().Truncate(time.Millisecond)
	if !decoded.ReceivedAt.Equal(wantTime) || !decoded.Events[0].IndexTime.Equal(wantTime) {
		t.Fatalf("decoded index time = %v/%v, want %v", decoded.ReceivedAt, decoded.Events[0].IndexTime, wantTime)
	}
	if len(decoded.Events) != len(batch.Events) || !proto.Equal(decoded.Events[0].Event, batch.Events[0].Event) {
		t.Fatalf("decoded events = %#v, want exact protobuf block", decoded.Events)
	}
	if decoded.Events[0].Event == batch.Events[0].Event {
		t.Fatal("decoded outbox aliases the caller's event")
	}
}

func TestStoreOutboxRejectsCorruptionAndInconsistentDisposition(t *testing.T) {
	t.Parallel()
	batch := validStoreBatch()
	batch.OriginalEventCount = 1
	encoded, err := encodeStoreOutbox(batch)
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		nil,
		encoded[:len(storeOutboxHeader)-1],
		append(slices.Clone(encoded), 0),
	}
	corrupted := slices.Clone(encoded)
	corrupted[4]++
	cases = append(cases, corrupted)
	corrupted = slices.Clone(encoded)
	corrupted[len(corrupted)-1] ^= 0xff
	cases = append(cases, corrupted)
	for index, value := range cases {
		if _, err := decodeStoreOutbox(value); err == nil {
			t.Errorf("corrupt case %d was accepted", index)
		}
	}

	batch.OriginalEventCount = 1
	batch.RejectedEvents = []*opensplunkv1.EventRejection{{EventIndex: 0}}
	if _, err := encodeReservationMetadata([][]any{make([]any, len(eventInsertColumns))}, batch); err == nil {
		t.Fatal("inconsistent accepted/rejected disposition was accepted")
	}
}

func TestDurableDecodersRejectHostileCountsBeforeAllocation(t *testing.T) {
	t.Parallel()

	var outboxBody bytes.Buffer
	writeOutboxBytes(&outboxBody, []byte("tenant"))
	writeOutboxBytes(&outboxBody, []byte("collector"))
	writeOutboxBytes(&outboxBody, []byte("batch"))
	writeOutboxUint64(&outboxBody, 1)
	sourceDigest := sha256.Sum256([]byte("source"))
	_, _ = outboxBody.Write(sourceDigest[:])
	writeOutboxUint64(&outboxBody, uint64(time.Now().UnixMilli()))
	writeOutboxUint32(&outboxBody, 1)
	writeOutboxUint32(&outboxBody, ^uint32(0))
	outboxChecksum := sha256.Sum256(outboxBody.Bytes())
	hostileOutbox := append(slices.Clone(storeOutboxHeader[:]), outboxChecksum[:]...)
	hostileOutbox = append(hostileOutbox, outboxBody.Bytes()...)
	if _, err := decodeStoreOutbox(hostileOutbox); err == nil {
		t.Fatal("outbox with hostile event count was accepted")
	}

	var metadataPayload bytes.Buffer
	_, _ = metadataPayload.Write([]byte{'O', 'S', 'V', 'M', 3})
	writeOutboxUint64(&metadataPayload, 0) // indexes
	writeOutboxUint64(&metadataPayload, 1) // batch sequence
	writeOutboxUint32(&metadataPayload, 1) // original events
	writeOutboxUint32(&metadataPayload, ^uint32(0))
	metadataChecksum := sha256.Sum256(metadataPayload.Bytes())
	hostileMetadata := append(slices.Clone(metadataPayload.Bytes()), metadataChecksum[:]...)
	if _, err := decodeReservationMetadata(hostileMetadata); err == nil {
		t.Fatal("metadata with hostile rejection count was accepted")
	}
}

func TestReservationMetadataRoundTripPreservesOriginalRejections(t *testing.T) {
	t.Parallel()
	indexTime := time.Date(2026, 7, 21, 1, 2, 3, 0, time.UTC)
	row := make([]any, len(eventInsertColumns))
	row[2] = "main"
	row[4] = indexTime
	row[23] = indexTime.Add(72 * time.Hour)
	rejection := &opensplunkv1.EventRejection{
		EventIndex: 1,
		EventId:    "rejected",
		Code:       opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX,
		Message:    "original policy decision",
	}
	batch := ingest.StoreBatch{
		BatchSequence:      41,
		OriginalEventCount: 2,
		RejectedEvents:     []*opensplunkv1.EventRejection{rejection},
	}
	encoded, err := encodeReservationMetadata([][]any{row}, batch)
	if err != nil {
		t.Fatalf("encodeReservationMetadata: %v", err)
	}
	decoded, err := decodeReservationMetadata(encoded)
	if err != nil {
		t.Fatalf("decodeReservationMetadata: %v", err)
	}
	if decoded.BatchSequence != 41 || decoded.OriginalEventCount != 2 ||
		decoded.RetentionByIndex["main"] != 72*time.Hour || len(decoded.RejectedEvents) != 1 ||
		!proto.Equal(decoded.RejectedEvents[0], rejection) {
		t.Fatalf("decoded metadata = %+v", decoded)
	}
	if decoded.RejectedEvents[0] == rejection {
		t.Fatal("decoded rejection aliases caller")
	}
	corrupted := slices.Clone(encoded)
	corrupted[len(corrupted)-1] ^= 0xff
	if _, err := decodeReservationMetadata(corrupted); err == nil {
		t.Fatal("metadata checksum corruption was accepted")
	}
}
