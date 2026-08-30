// Package alertwebhook validates, signs, and delivers alert webhook payloads.
// It deliberately has no dependency on the alert lifecycle package so network
// security policy can be tested and reused independently of scheduling and
// persistence.
package alertwebhook

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"
)

const (
	DefaultDeliveryTimeout   = 10 * time.Second
	DefaultResponseReadLimit = 4 * 1024
	MaximumPayloadBytes      = 64 * 1024
	MaximumResponseReadLimit = 64 * 1024
	MaximumSampleRows        = 10
	PayloadSchemaVersion     = 1
	SecretBytes              = 32
	SignatureVersion         = "v1"

	HeaderAlertID    = "Open-Splunk-Alert-Id"
	HeaderDeliveryID = "Open-Splunk-Delivery-Id"
	HeaderSignature  = "Open-Splunk-Signature"
	HeaderTimestamp  = "Open-Splunk-Timestamp"
)

var ErrInvalidArgument = errors.New("alert webhook: invalid argument")

type EventType string

const (
	EventTriggered EventType = "alert.triggered"
	EventTest      EventType = "alert.test"
)

type ConditionOperator string

const (
	ConditionGreaterThan ConditionOperator = "GREATER_THAN"
	ConditionLessThan    ConditionOperator = "LESS_THAN"
	ConditionEqual       ConditionOperator = "EQUAL"
	ConditionNotEqual    ConditionOperator = "NOT_EQUAL"
)

type DeliveryCategory string

const (
	DeliverySucceeded           DeliveryCategory = "SUCCEEDED"
	DeliveryDestinationRejected DeliveryCategory = "DESTINATION_REJECTED"
	DeliveryDNSFailure          DeliveryCategory = "DNS_FAILURE"
	DeliveryCanceled            DeliveryCategory = "CANCELED"
	DeliveryTimeout             DeliveryCategory = "TIMEOUT"
	DeliveryTLSFailure          DeliveryCategory = "TLS_FAILURE"
	DeliveryTransportFailure    DeliveryCategory = "TRANSPORT_FAILURE"
	DeliveryHTTPFailure         DeliveryCategory = "HTTP_FAILURE"
)

type DeliveryResult struct {
	Category    DeliveryCategory
	StatusCode  int
	Delivered   bool
	AttemptedAt time.Time
}

type DeliveryError struct {
	Category DeliveryCategory
}

func (deliveryError *DeliveryError) Error() string {
	return "alert webhook delivery failed: " + string(deliveryError.Category)
}

type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type Dialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type ClientFactory func(http.RoundTripper, time.Duration) HTTPDoer

type DestinationPolicy struct {
	PrivateHostAllowlist []string
}

type ParsedDestination struct {
	URL      *url.URL
	Hostname string
	Port     string
}

type ResolvedDestination struct {
	ParsedDestination
	Address netip.Addr
}

type DeliveryOptions struct {
	Resolver          Resolver
	Dialer            Dialer
	ClientFactory     ClientFactory
	Clock             func() time.Time
	Timeout           time.Duration
	ResponseReadLimit int64
	TLSRootCAs        *x509.CertPool
	DestinationPolicy DestinationPolicy
}

type ResultField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Payload struct {
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
	SampleRows            []map[string]any  `json:"sample_rows"`
	SearchTruncated       bool              `json:"search_truncated"`
	SampleTruncated       bool              `json:"sample_truncated"`
	ResultsURL            string            `json:"results_url"`
}

type SignedPayload struct {
	Body      []byte
	Headers   map[string]string
	Timestamp string
}
