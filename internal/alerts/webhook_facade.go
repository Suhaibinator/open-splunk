package alerts

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/alertwebhook"
)

const (
	DefaultDeliveryTimeout   = alertwebhook.DefaultDeliveryTimeout
	DefaultResponseReadLimit = alertwebhook.DefaultResponseReadLimit
	MaximumPayloadBytes      = alertwebhook.MaximumPayloadBytes
	MaximumResponseReadLimit = alertwebhook.MaximumResponseReadLimit
	PayloadSchemaVersion     = alertwebhook.PayloadSchemaVersion
	SignatureVersion         = alertwebhook.SignatureVersion

	HeaderAlertID    = alertwebhook.HeaderAlertID
	HeaderDeliveryID = alertwebhook.HeaderDeliveryID
	HeaderSignature  = alertwebhook.HeaderSignature
	HeaderTimestamp  = alertwebhook.HeaderTimestamp

	WebhookEventTriggered = alertwebhook.EventTriggered
	WebhookEventTest      = alertwebhook.EventTest

	DeliverySucceeded           = alertwebhook.DeliverySucceeded
	DeliveryDestinationRejected = alertwebhook.DeliveryDestinationRejected
	DeliveryDNSFailure          = alertwebhook.DeliveryDNSFailure
	DeliveryCanceled            = alertwebhook.DeliveryCanceled
	DeliveryTimeout             = alertwebhook.DeliveryTimeout
	DeliveryTLSFailure          = alertwebhook.DeliveryTLSFailure
	DeliveryTransportFailure    = alertwebhook.DeliveryTransportFailure
	DeliveryHTTPFailure         = alertwebhook.DeliveryHTTPFailure
)

type Resolver = alertwebhook.Resolver
type DestinationPolicy = alertwebhook.DestinationPolicy
type ParsedDestination = alertwebhook.ParsedDestination
type ResolvedDestination = alertwebhook.ResolvedDestination
type Dialer = alertwebhook.Dialer
type HTTPDoer = alertwebhook.HTTPDoer
type ClientFactory = alertwebhook.ClientFactory
type DeliveryOptions = alertwebhook.DeliveryOptions
type Deliverer = alertwebhook.Deliverer
type DeliveryCategory = alertwebhook.DeliveryCategory
type DeliveryResult = alertwebhook.DeliveryResult
type DeliveryError = alertwebhook.DeliveryError
type WebhookEventType = alertwebhook.EventType
type ResultField = alertwebhook.ResultField
type SignedPayload = alertwebhook.SignedPayload

type WebhookPayload struct {
	EventType             WebhookEventType  `json:"event_type"`
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
	SampleRows            []map[string]any  `json:"sample_rows"`
	SearchTruncated       bool              `json:"search_truncated"`
	SampleTruncated       bool              `json:"sample_truncated"`
	ResultsURL            string            `json:"results_url"`
}

func NewDeliverer(options DeliveryOptions) (*Deliverer, error) {
	deliverer, err := alertwebhook.NewDeliverer(options)
	return deliverer, compatibilityWebhookError(err)
}

func ParseDestination(rawURL string) (ParsedDestination, error) {
	destination, err := alertwebhook.ParseDestination(rawURL)
	return destination, compatibilityWebhookError(err)
}

func ResolveDestination(
	ctx context.Context,
	resolver Resolver,
	rawURL string,
	policy DestinationPolicy,
) (ResolvedDestination, error) {
	destination, err := alertwebhook.ResolveDestination(ctx, resolver, rawURL, policy)
	return destination, compatibilityWebhookError(err)
}

func ValidateDestinationPolicy(policy DestinationPolicy) error {
	return compatibilityWebhookError(alertwebhook.ValidateDestinationPolicy(policy))
}

func BuildSignedPayload(payload WebhookPayload, deliveryID string, secret []byte) (SignedPayload, error) {
	signed, err := alertwebhook.BuildSignedPayload(alertwebhook.Payload{
		EventType: payload.EventType, SchemaVersion: payload.SchemaVersion,
		AlertID: payload.AlertID, AlertRunID: payload.AlertRunID, SearchJobID: payload.SearchJobID,
		AlertName: payload.AlertName, Application: payload.Application,
		ScheduledAt: payload.ScheduledAt, StartedAt: payload.StartedAt,
		FinishedAt: payload.FinishedAt, DeliveryAt: payload.DeliveryAt,
		MissedOccurrenceCount: payload.MissedOccurrenceCount,
		Operator:              alertwebhook.ConditionOperator(payload.Operator), Threshold: payload.Threshold,
		ResultCount: payload.ResultCount, ResultCountExact: payload.ResultCountExact,
		ResultSchema: payload.ResultSchema, SampleRows: payload.SampleRows,
		SearchTruncated: payload.SearchTruncated, SampleTruncated: payload.SampleTruncated,
		ResultsURL: payload.ResultsURL,
	}, deliveryID, secret)
	return signed, compatibilityWebhookError(err)
}

func Sign(timestamp string, body, secret []byte) string {
	return alertwebhook.Sign(timestamp, body, secret)
}

func VerifySignature(timestamp string, body, secret []byte, encoded string) bool {
	return alertwebhook.VerifySignature(timestamp, body, secret, encoded)
}

func compatibilityWebhookError(err error) error {
	if err == nil || !errors.Is(err, alertwebhook.ErrInvalidArgument) {
		return err
	}
	return errors.Join(ErrInvalidArgument, err)
}
