package alertwebhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// BuildSignedPayload serializes a stable payload, removes only trailing sample
// rows to meet the body limit, and signs the exact bytes returned in Body.
func BuildSignedPayload(payload Payload, deliveryID string, secret []byte) (SignedPayload, error) {
	if len(secret) != SecretBytes {
		return SignedPayload{}, fmt.Errorf("%w: signing secret must be 32 bytes", ErrInvalidArgument)
	}
	if payload.AlertID == "" || deliveryID == "" {
		return SignedPayload{}, fmt.Errorf("%w: alert and delivery IDs are required", ErrInvalidArgument)
	}
	if payload.AlertRunID == "" || payload.SearchJobID == "" || payload.AlertName == "" || payload.Application == "" || payload.DeliveryAt.IsZero() {
		return SignedPayload{}, fmt.Errorf("%w: payload identity and delivery time are required", ErrInvalidArgument)
	}
	resultsURL, err := url.Parse(payload.ResultsURL)
	if err != nil || resultsURL == nil || resultsURL.Hostname() == "" || (resultsURL.Scheme != "http" && resultsURL.Scheme != "https") {
		return SignedPayload{}, fmt.Errorf("%w: absolute results URL is required", ErrInvalidArgument)
	}
	if !validOperator(payload.Operator) {
		return SignedPayload{}, fmt.Errorf("%w: unsupported condition operator", ErrInvalidArgument)
	}
	if len(payload.SampleRows) > MaximumSampleRows {
		return SignedPayload{}, fmt.Errorf("%w: at most %d sample rows are permitted", ErrInvalidArgument, MaximumSampleRows)
	}
	if payload.EventType == "" {
		payload.EventType = EventTriggered
	}
	if payload.EventType != EventTriggered && payload.EventType != EventTest {
		return SignedPayload{}, fmt.Errorf("%w: webhook event type is invalid", ErrInvalidArgument)
	}
	payload.SchemaVersion = PayloadSchemaVersion
	payload.ScheduledAt = payload.ScheduledAt.UTC()
	payload.StartedAt = payload.StartedAt.UTC()
	payload.FinishedAt = payload.FinishedAt.UTC()
	payload.DeliveryAt = payload.DeliveryAt.UTC()
	payload.ResultSchema = append([]ResultField{}, payload.ResultSchema...)
	rows := make([]json.RawMessage, len(payload.SampleRows))
	for index, row := range payload.SampleRows {
		encoded, marshalErr := json.Marshal(row)
		if marshalErr != nil {
			return SignedPayload{}, errors.New("alert webhook: serialize sample row")
		}
		rows[index] = encoded
	}

	encoded := encodedPayloadFrom(payload, nil)
	metadataBody, err := json.Marshal(encoded)
	if err != nil {
		return SignedPayload{}, errors.New("alert webhook: serialize payload metadata")
	}
	metadataBytes := len(metadataBody) - len("[]")
	kept := fittingSamplePrefix(rows, metadataBytes)
	if kept < len(rows) {
		encoded.SampleTruncated = true
		metadataBody, err = json.Marshal(encoded)
		if err != nil {
			return SignedPayload{}, errors.New("alert webhook: serialize trimmed payload metadata")
		}
		metadataBytes = len(metadataBody) - len("[]")
		kept = fittingSamplePrefix(rows, metadataBytes)
	}
	if metadataBytes+sampleArrayBytes(rows[:kept]) > MaximumPayloadBytes {
		return SignedPayload{}, errors.New("alert webhook: payload metadata exceeds 64 KiB")
	}
	encoded.SampleRows = rows[:kept]
	body, err := json.Marshal(encoded)
	if err != nil {
		return SignedPayload{}, errors.New("alert webhook: serialize payload")
	}
	if len(body) > MaximumPayloadBytes {
		return SignedPayload{}, errors.New("alert webhook: payload exceeds 64 KiB")
	}
	timestamp := strconv.FormatInt(payload.DeliveryAt.UTC().Unix(), 10)
	signature := Sign(timestamp, body, secret)
	return SignedPayload{
		Body: body,
		Headers: map[string]string{
			HeaderAlertID:    payload.AlertID,
			HeaderDeliveryID: deliveryID,
			HeaderTimestamp:  timestamp,
			HeaderSignature:  SignatureVersion + "=" + signature,
		},
		Timestamp: timestamp,
	}, nil
}

type encodedPayload struct {
	EventType             EventType         `json:"event_type"`
	SchemaVersion         int               `json:"schema_version"`
	AlertID               string            `json:"alert_id"`
	AlertRunID            string            `json:"alert_run_id"`
	SearchJobID           string            `json:"search_job_id"`
	AlertName             string            `json:"alert_name"`
	Application           string            `json:"application"`
	ScheduledAt           time.Time         `json:"scheduled_at"`
	StartedAt             time.Time         `json:"started_at"`
	FinishedAt            time.Time         `json:"finished_at"`
	DeliveryAt            time.Time         `json:"delivery_at"`
	MissedOccurrenceCount uint64            `json:"missed_occurrence_count"`
	Operator              ConditionOperator `json:"operator"`
	Threshold             uint64            `json:"threshold"`
	ResultCount           uint64            `json:"result_count"`
	ResultCountExact      bool              `json:"result_count_exact"`
	ResultSchema          []ResultField     `json:"result_schema"`
	SampleRows            []json.RawMessage `json:"sample_rows"`
	SearchTruncated       bool              `json:"search_truncated"`
	SampleTruncated       bool              `json:"sample_truncated"`
	ResultsURL            string            `json:"results_url"`
}

func encodedPayloadFrom(payload Payload, rows []json.RawMessage) encodedPayload {
	if rows == nil {
		rows = make([]json.RawMessage, 0)
	}
	return encodedPayload{
		EventType: payload.EventType, SchemaVersion: payload.SchemaVersion,
		AlertID: payload.AlertID, AlertRunID: payload.AlertRunID, SearchJobID: payload.SearchJobID,
		AlertName: payload.AlertName, Application: payload.Application,
		ScheduledAt: payload.ScheduledAt, StartedAt: payload.StartedAt,
		FinishedAt: payload.FinishedAt, DeliveryAt: payload.DeliveryAt,
		MissedOccurrenceCount: payload.MissedOccurrenceCount, Operator: payload.Operator,
		Threshold: payload.Threshold, ResultCount: payload.ResultCount,
		ResultCountExact: payload.ResultCountExact, ResultSchema: payload.ResultSchema,
		SampleRows: rows, SearchTruncated: payload.SearchTruncated,
		SampleTruncated: payload.SampleTruncated, ResultsURL: payload.ResultsURL,
	}
}

func fittingSamplePrefix(rows []json.RawMessage, metadataBytes int) int {
	used := metadataBytes + len("[]")
	for index, row := range rows {
		increment := len(row)
		if index != 0 {
			increment++
		}
		if increment > MaximumPayloadBytes-used {
			return index
		}
		used += increment
	}
	return len(rows)
}

func sampleArrayBytes(rows []json.RawMessage) int {
	size := len("[]")
	for index, row := range rows {
		size += len(row)
		if index != 0 {
			size++
		}
	}
	return size
}

func Sign(timestamp string, body, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func VerifySignature(timestamp string, body, secret []byte, encoded string) bool {
	want, err := hex.DecodeString(encoded)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return hmac.Equal(want, mac.Sum(nil))
}

func validOperator(operator ConditionOperator) bool {
	switch operator {
	case ConditionGreaterThan, ConditionLessThan, ConditionEqual, ConditionNotEqual:
		return true
	default:
		return false
	}
}
