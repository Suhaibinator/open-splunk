// Package alerts contains the alert domain, lifecycle orchestration, secret
// handling, and compatibility facades. Persistence, condition evaluation,
// webhook delivery, and HTTP/protobuf mapping live at explicit boundaries.
package alerts

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/alertevaluation"
)

const (
	MaximumAlertsPerOwner = 64
	MaximumRunHistory     = 100
	MaximumSampleRows     = 10
	DefaultSampleRows     = 5
)

var (
	ErrActiveRun            = errors.New("alerts: an active run prevents this operation")
	ErrAlreadyExists        = errors.New("alerts: alert already exists")
	ErrCapacity             = errors.New("alerts: owner alert capacity exhausted")
	ErrClosed               = errors.New("alerts: coordinator is closed")
	ErrDeliveryAttempted    = errors.New("alerts: webhook delivery was already attempted")
	ErrDeliveryIDGeneration = errors.New("alerts: webhook delivery ID generation failed")
	ErrIdempotencyConflict  = errors.New("alerts: client request identity was reused")
	ErrInvalidArgument      = errors.New("alerts: invalid argument")
	ErrNotFound             = errors.New("alerts: alert not found")
	ErrSecretRotated        = errors.New("alerts: webhook secret generation rotated")
	ErrVersionConflict      = errors.New("alerts: version conflict")
)

type ConditionOperator = alertevaluation.ConditionOperator

const (
	ConditionGreaterThan = alertevaluation.ConditionGreaterThan
	ConditionLessThan    = alertevaluation.ConditionLessThan
	ConditionEqual       = alertevaluation.ConditionEqual
	ConditionNotEqual    = alertevaluation.ConditionNotEqual
)

type EvaluationCertainty = alertevaluation.EvaluationCertainty

const (
	EvaluationTrue          = alertevaluation.EvaluationTrue
	EvaluationFalse         = alertevaluation.EvaluationFalse
	EvaluationIndeterminate = alertevaluation.EvaluationIndeterminate
)

type AlertState string

const (
	AlertDisabled AlertState = "DISABLED"
	AlertEnabled  AlertState = "ENABLED"
)

type ResultTab uint8

const (
	ResultTabUnspecified ResultTab = iota
	ResultTabEvents
	ResultTabStatistics
	ResultTabVisualization
)

type RunOutcome string

const (
	RunClaimed         RunOutcome = "CLAIMED"
	RunSearching       RunOutcome = "SEARCHING"
	RunSearchFailed    RunOutcome = "SEARCH_FAILED"
	RunSearchCanceled  RunOutcome = "SEARCH_CANCELED"
	RunSearchExpired   RunOutcome = "SEARCH_EXPIRED"
	RunNotTriggered    RunOutcome = "NOT_TRIGGERED"
	RunIndeterminate   RunOutcome = "INDETERMINATE"
	RunDelivering      RunOutcome = "DELIVERING"
	RunDelivered       RunOutcome = "DELIVERED"
	RunDeliveryFailed  RunOutcome = "DELIVERY_FAILED"
	RunDeliveryUnknown RunOutcome = "DELIVERY_UNKNOWN"
	RunDeliverySkipped RunOutcome = "DELIVERY_SKIPPED_SECRET_ROTATED"
	RunOverlapSkipped  RunOutcome = "SKIPPED_OVERLAP"
	RunInterrupted     RunOutcome = "INTERRUPTED"
)

// FailureCategory is the bounded internal reason recorded for alert-run
// failures. Search and delivery adapters may also supply validated categories,
// so RunSummary keeps its persisted representation as a string.
type FailureCategory string

const (
	FailureAdmission                 FailureCategory = "ADMISSION_FAILED"
	FailureAttach                    FailureCategory = "ATTACH_FAILED"
	FailureCanceled                  FailureCategory = "CANCELED"
	FailureDelivery                  FailureCategory = "DELIVERY_FAILED"
	FailureDeliveryAlreadyAuthorized FailureCategory = "DELIVERY_ALREADY_AUTHORIZED"
	FailureDeliveryAuthorization     FailureCategory = "DELIVERY_AUTHORIZATION_FAILED"
	FailureDeliveryID                FailureCategory = "DELIVERY_ID_FAILED"
	FailureEvaluation                FailureCategory = "EVALUATION_FAILED"
	FailureInvalidJobState           FailureCategory = "INVALID_JOB_STATE"
	FailureJobIDMismatch             FailureCategory = "JOB_ID_MISMATCH"
	FailureJobRead                   FailureCategory = "JOB_READ_FAILED"
	FailureNonterminalJob            FailureCategory = "NONTERMINAL_JOB"
	FailurePayloadBuild              FailureCategory = "PAYLOAD_BUILD_FAILED"
	FailureProcessRestart            FailureCategory = "PROCESS_RESTART"
	FailurePublicBaseURL             FailureCategory = "PUBLIC_BASE_URL_UNAVAILABLE"
	FailureResultSample              FailureCategory = "RESULT_SAMPLE_FAILED"
	FailureRetentionExtension        FailureCategory = "RETENTION_EXTENSION_FAILED"
	FailureSearch                    FailureCategory = "SEARCH_FAILED"
	FailureSearchCanceled            FailureCategory = "SEARCH_CANCELED"
	FailureSearchExpired             FailureCategory = "SEARCH_EXPIRED"
	FailureSearchInterrupted         FailureCategory = "SEARCH_INTERRUPTED"
	FailureSecretRotated             FailureCategory = "SECRET_ROTATED"
)

type Condition = alertevaluation.Condition

type Definition struct {
	Name               string
	Description        string
	Application        string
	SPL                string
	IndexScope         []string
	Earliest           string
	Latest             string
	SearchTimezone     string
	Cron               string
	Timezone           string
	Condition          Condition
	SampleRows         int
	DispatchTTL        string
	WebhookTTL         string
	Visualization      []byte
	SelectedFields     []string
	PreferredResultTab ResultTab
}

// EncryptedValue is an authenticated encrypted value. Ciphertext includes the
// GCM authentication tag; Nonce is stored separately for nonce-uniqueness
// auditing. Neither field is safe to expose through a read API.
type EncryptedValue struct {
	Nonce      []byte
	Ciphertext []byte
}

type SecretGeneration struct {
	Generation uint64
	Encrypted  EncryptedValue
	CreatedAt  time.Time
}

type Alert struct {
	ID                 string
	OwnerID            string
	Version            uint64
	State              AlertState
	Definition         Definition
	Endpoint           EncryptedValue
	EndpointGeneration uint64
	WebhookHostname    string
	SecretGeneration   SecretGeneration
	NextRunAt          *time.Time
	LastOutcome        RunOutcome
	LastEvaluatedAt    *time.Time
	LastDeliveredAt    *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type RunSnapshot struct {
	AlertID               string
	AlertRunID            string
	AlertVersion          uint64
	OwnerID               string
	TenantID              string
	Definition            Definition
	Endpoint              EncryptedValue
	EndpointGeneration    uint64
	SecretGeneration      SecretGeneration
	ScheduledAt           time.Time
	ClaimedAt             time.Time
	NextScheduledAt       time.Time
	MissedOccurrenceCount uint32
	DispatchRetention     time.Duration
	TriggeredRetention    time.Duration
}

type RunSummary struct {
	AlertID               string
	AlertRunID            string
	AlertVersion          uint64
	SearchJobID           string
	SearchJobExpiresAt    time.Time
	DeliveryID            string
	FailureCategory       string
	Outcome               RunOutcome
	ScheduledAt           time.Time
	StartedAt             time.Time
	FinishedAt            time.Time
	MissedOccurrenceCount uint32
	Evaluation            EvaluationCertainty
	ResultCount           uint64
	ResultCountExact      bool
	Delivery              DeliveryResult
}

// Repository is the persistence boundary. Implementations must scope every
// operation by OwnerID, enforce optimistic versions and owner capacity, and
// make each mutation atomic. Secret-bearing inputs must never be logged.
type Repository interface {
	FindCreateReplay(context.Context, string, string, [32]byte) (Alert, bool, error)
	Create(context.Context, CreateRecord) (CreateResult, error)
	GetSecretBearing(context.Context, string, string) (Alert, error)
	GetSummary(context.Context, string, string) (AlertSummary, error)
	List(context.Context, string, int) ([]AlertSummary, error)
	Update(context.Context, UpdateRecord) (Alert, error)
	SetState(context.Context, SetStateRecord) (Alert, error)
	RotateSecret(context.Context, RotateSecretRecord) (Alert, error)
	AuthorizeDelivery(context.Context, AuthorizeDeliveryRecord) (DeliveryAuthorization, error)
	DeleteIfIdle(context.Context, DeleteRecord) error
	ListRuns(context.Context, string, string, int) ([]RunSummary, error)
}

// AlertSummary is the only list projection. It deliberately cannot carry
// endpoint or signing-secret ciphertext into API presentation code.
type AlertSummary struct {
	ID               string
	OwnerID          string
	Version          uint64
	State            AlertState
	Definition       Definition
	WebhookHostname  string
	SecretGeneration uint64
	SecretRotatedAt  time.Time
	NextRunAt        *time.Time
	LastOutcome      RunOutcome
	LastEvaluatedAt  *time.Time
	LastDeliveredAt  *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type DeliveryAuthorization string

const (
	DeliveryAuthorized       DeliveryAuthorization = "AUTHORIZED"
	DeliverySecretRotated    DeliveryAuthorization = "SECRET_ROTATED"
	DeliveryAlreadyAttempted DeliveryAuthorization = "ALREADY_ATTEMPTED"
)

// AuthorizeDelivery must atomically compare the current secret generation and
// record the delivery ID as the run's sole attempt. Persisting authorization
// before network I/O gives rotation and crash recovery a linearization point.
type AuthorizeDeliveryRecord struct {
	AlertID          string
	AlertRunID       string
	OwnerID          string
	DeliveryID       string
	SecretGeneration uint64
	AuthorizedAt     time.Time
}

type CreateRecord struct {
	ID                 string
	OwnerID            string
	ClientRequestID    string
	RequestFingerprint [32]byte
	State              AlertState
	Definition         Definition
	Endpoint           EncryptedValue
	EndpointGeneration uint64
	WebhookHostname    string
	SecretGeneration   SecretGeneration
	CreatedAt          time.Time
}

type CreateDisposition string

const (
	CreateCommitted CreateDisposition = "COMMITTED"
	CreateReplayed  CreateDisposition = "REPLAYED"
)

type CreateResult struct {
	Alert       Alert
	Disposition CreateDisposition
}

type UpdateRecord struct {
	ID                 string
	OwnerID            string
	ExpectedVersion    uint64
	Definition         Definition
	Endpoint           EncryptedValue
	EndpointGeneration uint64
	WebhookHostname    string
	UpdatedAt          time.Time
}

type SetStateRecord struct {
	ID              string
	OwnerID         string
	ExpectedVersion uint64
	State           AlertState
	UpdatedAt       time.Time
}

type RotateSecretRecord struct {
	ID                 string
	OwnerID            string
	ExpectedVersion    uint64
	ExpectedGeneration uint64
	SecretGeneration   SecretGeneration
	UpdatedAt          time.Time
}

type DeleteRecord struct {
	ID              string
	OwnerID         string
	ExpectedVersion uint64
}

// RunRepository is kept separate from lifecycle persistence so the scheduler
// can use transactional claim semantics without depending on CRUD services.
type RunRepository interface {
	ClaimDue(context.Context, time.Time, int) ([]RunSnapshot, error)
	ClaimRunNow(context.Context, string, string, time.Time) (RunSnapshot, bool, error)
	RecordOverlap(context.Context, RunSummary) error
	AttachSearchJob(context.Context, string, string, string, time.Time) error
	CompleteRun(context.Context, RunSummary) error
	InterruptUnfinished(context.Context, time.Time) (int64, error)
}

type SearchRequest struct {
	OwnerID     string
	TenantID    string
	Application string
	SPL         string
	Earliest    string
	Latest      string
	Timezone    string
	IndexScope  []string
	AlertID     string
	AlertRunID  string
	ScheduledAt time.Time
	Retention   time.Duration
}

type SearchAdmission interface {
	AdmitAlertSearch(context.Context, SearchRequest) (string, error)
}

type SearchJobState string

const (
	SearchJobQueued      SearchJobState = "QUEUED"
	SearchJobRunning     SearchJobState = "RUNNING"
	SearchJobCompleted   SearchJobState = "COMPLETED"
	SearchJobFailed      SearchJobState = "FAILED"
	SearchJobCanceled    SearchJobState = "CANCELED"
	SearchJobExpired     SearchJobState = "EXPIRED"
	SearchJobInterrupted SearchJobState = "INTERRUPTED"
)

// SearchJobSnapshot is the bounded job metadata needed to evaluate an alert.
// ResultCount is exact unless ResultsTruncated is true, in which case it is a
// lower bound supplied by the retained result snapshot.
type SearchJobSnapshot struct {
	ID               string
	State            SearchJobState
	StartedAt        time.Time
	FinishedAt       time.Time
	ExpiresAt        time.Time
	ResultCount      uint64
	ResultsTruncated bool
	FailureCategory  string
}

type SearchJobReader interface {
	ReadAlertSearchJob(context.Context, string, string) (SearchJobSnapshot, error)
}

type SearchJobPoller interface {
	WaitForTerminal(context.Context, string, string, SearchJobReader) (SearchJobSnapshot, error)
}

type SearchResults struct {
	Schema []ResultField
	Rows   []map[string]any
	More   bool
}

type SearchResultReader interface {
	ReadAlertSearchResults(context.Context, string, string, int) (SearchResults, error)
}

type DurableRetentionUpdater interface {
	ExtendAlertSearchJob(context.Context, string, string, time.Duration) (time.Time, error)
}

type DeliveryAuthorizer interface {
	AuthorizeAndOpenDelivery(context.Context, RunSnapshot, func() (string, error)) (OpenedDeliverySecrets, error)
}

type WebhookDeliverer interface {
	Deliver(context.Context, string, SignedPayload) (DeliveryResult, error)
}
