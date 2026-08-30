package alertwebhook

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type countedJSONValue struct {
	calls *atomic.Int32
}

func (value countedJSONValue) MarshalJSON() ([]byte, error) {
	value.calls.Add(1)
	return []byte(`"counted"`), nil
}

func TestBuildSignedPayloadSignsExactTrimmedBody(t *testing.T) {
	t.Parallel()
	deliveryAt := time.Date(2026, time.August, 29, 12, 30, 0, 0, time.FixedZone("offset", 3600))
	rows := make([]map[string]any, MaximumSampleRows)
	for index := range rows {
		rows[index] = map[string]any{"_raw": strings.Repeat("x", 8*1024), "index": index}
	}
	secret := make([]byte, SecretBytes)
	for index := range secret {
		secret[index] = byte(index)
	}
	signed, err := BuildSignedPayload(Payload{
		AlertID: "alert-1", AlertRunID: "run-1", SearchJobID: "job-1",
		AlertName: "errors", Application: "search", DeliveryAt: deliveryAt,
		Operator: ConditionGreaterThan, Threshold: 0, ResultCount: 12,
		ResultCountExact: true, SampleRows: rows, ResultsURL: "https://splunk.example.com/search/?searchJobId=job-1",
	}, "delivery-1", secret)
	if err != nil {
		t.Fatalf("BuildSignedPayload() error = %v", err)
	}
	if len(signed.Body) > MaximumPayloadBytes {
		t.Fatalf("body size = %d", len(signed.Body))
	}
	var payload Payload
	if err := json.Unmarshal(signed.Body, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !payload.SampleTruncated || len(payload.SampleRows) >= len(rows) {
		t.Fatalf("trimmed payload has %d rows and sample_truncated=%v", len(payload.SampleRows), payload.SampleTruncated)
	}
	if payload.EventType != EventTriggered || payload.SchemaVersion != PayloadSchemaVersion {
		t.Fatalf("event identity = %q version %d", payload.EventType, payload.SchemaVersion)
	}
	if signed.Timestamp != "1788003000" {
		t.Fatalf("timestamp = %q", signed.Timestamp)
	}
	encoded := strings.TrimPrefix(signed.Headers[HeaderSignature], SignatureVersion+"=")
	if !VerifySignature(signed.Timestamp, signed.Body, secret, encoded) {
		t.Fatal("signature does not authenticate the returned body")
	}
	mutated := append([]byte(nil), signed.Body...)
	mutated[len(mutated)-1] ^= 1
	if VerifySignature(signed.Timestamp, mutated, secret, encoded) {
		t.Fatal("signature authenticated a mutated body")
	}
}

func TestBuildSignedPayloadRejectsUntrimmableMetadata(t *testing.T) {
	t.Parallel()
	_, err := BuildSignedPayload(Payload{
		AlertID: "alert-1", AlertRunID: "run-1", SearchJobID: "job-1",
		AlertName: strings.Repeat("x", MaximumPayloadBytes), Application: "search", DeliveryAt: time.Now(),
		Operator: ConditionEqual, ResultsURL: "https://splunk.example.com/search/?searchJobId=job-1",
	}, "delivery-1", make([]byte, SecretBytes))
	if err == nil {
		t.Fatal("BuildSignedPayload() accepted oversized metadata")
	}
}

func TestBuildSignedPayloadEncodesEachSampleRowOnce(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	rows := make([]map[string]any, MaximumSampleRows)
	for index := range rows {
		rows[index] = map[string]any{
			"counted": countedJSONValue{calls: &calls},
			"padding": strings.Repeat("x", 8*1024),
		}
	}
	signed, err := BuildSignedPayload(validPayload(rows), "delivery-once", make([]byte, SecretBytes))
	if err != nil {
		t.Fatalf("BuildSignedPayload() error = %v", err)
	}
	if calls.Load() != int32(len(rows)) {
		t.Fatalf("sample marshal calls = %d, want %d", calls.Load(), len(rows))
	}
	var payload Payload
	if err := json.Unmarshal(signed.Body, &payload); err != nil {
		t.Fatal(err)
	}
	if !payload.SampleTruncated || len(payload.SampleRows) >= len(rows) {
		t.Fatalf("trimmed rows = %d, sample_truncated = %t", len(payload.SampleRows), payload.SampleTruncated)
	}
}

func TestBuildSignedPayloadUsesExactPayloadLimit(t *testing.T) {
	t.Parallel()
	secret := make([]byte, SecretBytes)
	empty, err := BuildSignedPayload(validPayload(nil), "delivery-boundary", secret)
	if err != nil {
		t.Fatal(err)
	}
	rowEnvelopeBytes := len(`{"value":""}`)
	paddingBytes := MaximumPayloadBytes - len(empty.Body) - rowEnvelopeBytes
	if paddingBytes <= 0 {
		t.Fatalf("metadata unexpectedly consumes %d bytes", len(empty.Body))
	}
	payload := validPayload([]map[string]any{{"value": strings.Repeat("x", paddingBytes)}})
	signed, err := BuildSignedPayload(payload, "delivery-boundary", secret)
	if err != nil {
		t.Fatal(err)
	}
	if len(signed.Body) != MaximumPayloadBytes {
		t.Fatalf("body size = %d, want exact limit %d", len(signed.Body), MaximumPayloadBytes)
	}
	var decoded Payload
	if err := json.Unmarshal(signed.Body, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.SampleTruncated || len(decoded.SampleRows) != 1 {
		t.Fatalf("boundary payload rows = %d, truncated = %t", len(decoded.SampleRows), decoded.SampleTruncated)
	}
}

func TestBuildSignedPayloadPreservesStableJSONAndUpstreamTruncation(t *testing.T) {
	t.Parallel()
	first := validPayload([]map[string]any{{"z": 1, "a": 2}})
	first.SampleTruncated = true
	second := validPayload([]map[string]any{{"a": 2, "z": 1}})
	second.SampleTruncated = true
	secret := make([]byte, SecretBytes)
	left, err := BuildSignedPayload(first, "stable-delivery", secret)
	if err != nil {
		t.Fatal(err)
	}
	right, err := BuildSignedPayload(second, "stable-delivery", secret)
	if err != nil {
		t.Fatal(err)
	}
	if string(left.Body) != string(right.Body) {
		t.Fatalf("stable payloads differ:\n%s\n%s", left.Body, right.Body)
	}
	var decoded Payload
	if err := json.Unmarshal(left.Body, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.SampleTruncated || len(decoded.SampleRows) != 1 {
		t.Fatalf("upstream truncation was not preserved: %#v", decoded)
	}
}

func TestBuildSignedPayloadDoesNotMutateRows(t *testing.T) {
	t.Parallel()
	rows := []map[string]any{{"message": "original"}}
	_, err := BuildSignedPayload(Payload{
		AlertID: "alert-1", AlertRunID: "run-1", SearchJobID: "job-1",
		AlertName: "errors", Application: "search", DeliveryAt: time.Now(), SampleRows: rows,
		Operator: ConditionEqual, ResultsURL: "https://splunk.example.com/search/?searchJobId=job-1",
	}, "delivery-1", make([]byte, SecretBytes))
	if err != nil {
		t.Fatalf("BuildSignedPayload() error = %v", err)
	}
	if rows[0]["message"] != "original" || len(rows) != 1 {
		t.Fatalf("input rows changed: %#v", rows)
	}
}

func TestBuildSignedPayloadRejectsInvalidOperator(t *testing.T) {
	t.Parallel()
	_, err := BuildSignedPayload(Payload{
		AlertID: "alert-1", AlertRunID: "run-1", SearchJobID: "job-1",
		AlertName: "errors", Application: "search", DeliveryAt: time.Now(),
		Operator: "UNKNOWN", ResultsURL: "https://splunk.example.com/search/?searchJobId=job-1",
	}, "delivery-1", make([]byte, SecretBytes))
	if err == nil {
		t.Fatal("BuildSignedPayload() accepted an invalid operator")
	}
}

func validPayload(rows []map[string]any) Payload {
	return Payload{
		AlertID: "alert-1", AlertRunID: "run-1", SearchJobID: "job-1",
		AlertName: "errors", Application: "search",
		DeliveryAt: time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC),
		Operator:   ConditionEqual, SampleRows: rows,
		ResultsURL: "https://splunk.example.com/search/?searchJobId=job-1",
	}
}
