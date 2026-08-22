package hechttp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

const (
	compatibilityFormatVersion = uint32(1)
	compatibilityTenantID      = "tenant-hec-compatibility"
	compatibilityRequestID     = "0123456789abcdef0123456789abcdef"
)

var (
	compatibilityCorpusFiles = []string{
		"acknowledgment.json",
		"auth-transport.json",
		"health-capacity.json",
		"json-event.json",
		"raw.json",
	}
	compatibilityCaseIDPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{2,127}$`)
	compatibilityAliasPattern   = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	compatibilitySintPattern    = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)
	compatibilityUintPattern    = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
	compatibilityDecimalPattern = regexp.MustCompile(
		`^-?(0|[1-9][0-9]*)(\.[0-9]+([eE][+-]?(0|[1-9][0-9]*))?|[eE][+-]?(0|[1-9][0-9]*))$`,
	)
	compatibilityNanosPattern = regexp.MustCompile(`^-?[0-9]+$`)
)

type compatibilityDocument struct {
	FormatVersion uint32              `json:"format_version"`
	Cases         []compatibilityCase `json:"cases"`
}

type compatibilityCase struct {
	ID      string               `json:"id"`
	Rule    string               `json:"rule"`
	Request compatibilityRequest `json:"request"`
	Setup   compatibilitySetup   `json:"setup"`
	Expect  compatibilityExpect  `json:"expect"`
	file    string
}

type compatibilityRequest struct {
	Method  string                `json:"method"`
	Path    string                `json:"path"`
	Headers []compatibilityHeader `json:"headers"`
	Query   []compatibilityQuery  `json:"query"`
	Body    compatibilityBody     `json:"body"`
}

type compatibilityHeader struct {
	Name  string  `json:"name"`
	Value *string `json:"value"`
}

type compatibilityQuery struct {
	Name  *string `json:"name"`
	Value *string `json:"value"`
}

type compatibilityBody struct {
	Kind    string    `json:"kind"`
	Value   *string   `json:"value,omitempty"`
	Members *[]string `json:"members,omitempty"`
	Unit    *string   `json:"unit,omitempty"`
	Count   *int      `json:"count,omitempty"`
}

type compatibilitySetup struct {
	ReceivedAt        string                        `json:"received_at"`
	Token             *compatibilityToken           `json:"token,omitempty"`
	Indexes           []compatibilityIndex          `json:"indexes"`
	AckAllocations    []compatibilityAckAllocation  `json:"ack_allocations,omitempty"`
	Acknowledgments   []compatibilityAcknowledgment `json:"ack_rows,omitempty"`
	Conditions        []string                      `json:"conditions"`
	RetryAfterSeconds *int                          `json:"retry_after_seconds,omitempty"`
}

type compatibilityToken struct {
	Alias          string                 `json:"alias"`
	Purpose        string                 `json:"purpose"`
	State          string                 `json:"state"`
	Acknowledgment *bool                  `json:"ack_enabled"`
	AllowedIndexes []string               `json:"allowed_indexes"`
	Defaults       *compatibilityDefaults `json:"defaults"`
}

type compatibilityDefaults struct {
	Index      *string `json:"index,omitempty"`
	Host       *string `json:"host,omitempty"`
	Source     *string `json:"source,omitempty"`
	Sourcetype *string `json:"sourcetype,omitempty"`
}

type compatibilityIndex struct {
	Name              string  `json:"name"`
	State             string  `json:"state"`
	IngestionEnabled  *bool   `json:"ingestion_enabled"`
	DefaultSourcetype *string `json:"default_sourcetype,omitempty"`
}

type compatibilityAckAllocation struct {
	Channel string `json:"channel"`
	ID      string `json:"id"`
}

type compatibilityAcknowledgment struct {
	Channel    string  `json:"channel"`
	ID         string  `json:"id"`
	State      string  `json:"state"`
	TerminalAt *string `json:"terminal_at,omitempty"`
}

type compatibilityExpect struct {
	HTTP    compatibilityHTTP    `json:"http"`
	Durable compatibilityDurable `json:"durable"`
	Events  []compatibilityEvent `json:"events"`
	SPL     *compatibilitySPL    `json:"spl,omitempty"`
}

type compatibilityHTTP struct {
	Status   int               `json:"status"`
	Headers  map[string]string `json:"headers"`
	BodyUTF8 *string           `json:"body_utf8"`
}

type compatibilityDurable struct {
	Quota      string `json:"quota"`
	Request    string `json:"request"`
	Ack        string `json:"ack"`
	Outbox     string `json:"outbox"`
	Visibility string `json:"visibility"`
}

type compatibilityEvent struct {
	Ordinal       int                  `json:"ordinal"`
	Index         string               `json:"index"`
	TimeUnixNanos string               `json:"time_unix_nanos"`
	TimeSource    string               `json:"time_source"`
	Host          *string              `json:"host"`
	Source        *string              `json:"source"`
	Sourcetype    *string              `json:"sourcetype"`
	Raw           *string              `json:"raw"`
	Message       *string              `json:"message,omitempty"`
	Fields        []compatibilityField `json:"fields"`
}

type compatibilityField struct {
	Name  string             `json:"name"`
	Value compatibilityValue `json:"value"`
}

type compatibilityValue struct {
	Kind  string                `json:"kind"`
	Value json.RawMessage       `json:"value,omitempty"`
	Items *[]compatibilityValue `json:"items,omitempty"`
}

type compatibilitySPL struct {
	Query string                       `json:"query"`
	Rows  []map[string]json.RawMessage `json:"rows"`
}

func TestHandlerHECCompatibilityCorpus(t *testing.T) {
	cases, err := loadCompatibilityCorpus()
	if err != nil {
		t.Fatalf("load compatibility corpus: %v", err)
	}
	executed := make(map[string]bool, len(cases))
	for ordinal := range cases {
		fixture := cases[ordinal]
		t.Run(fixture.ID, func(t *testing.T) {
			environment, err := newCompatibilityEnvironment(fixture, ordinal)
			if err != nil {
				t.Fatalf("configure fixture %s: %v", fixture.file, err)
			}
			request, err := environment.request(fixture.Request)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			response := httptest.NewRecorder()
			environment.handler.ServeHTTP(response, request)
			executed[fixture.ID] = true

			assertCompatibilityHTTP(t, response, fixture.Expect.HTTP)
			if calls := environment.next.callCount(); calls != 0 {
				t.Errorf("request escaped HEC namespace to next handler %d time(s)", calls)
			}
			if got := environment.durableDisposition(); !reflect.DeepEqual(got, fixture.Expect.Durable) {
				t.Errorf("durable disposition = %#v, want %#v", got, fixture.Expect.Durable)
			}
			assertCompatibilityEvents(t, environment.committedEvents(), fixture.Expect.Events)
			// SPL projections are downstream of reconciliation and are closed-shape
			// validated by the loader; this HTTP-boundary test stops at the complete
			// staged event projection requested by the fixture contract.
			environment.assertBoundedAndConsumed(t)
		})
	}
	if len(executed) != len(cases) {
		missing := make([]string, 0, len(cases)-len(executed))
		for _, fixture := range cases {
			if !executed[fixture.ID] {
				missing = append(missing, fixture.ID)
			}
		}
		t.Fatalf("compatibility cases were not executed: %v", missing)
	}
}

func loadCompatibilityCorpus() ([]compatibilityCase, error) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		return nil, errors.New("locate compatibility test source")
	}
	directory := filepath.Join(filepath.Dir(sourceFile), "..", "hec", "testdata", "compatibility")
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	wanted := make(map[string]bool, len(compatibilityCorpusFiles))
	for _, name := range compatibilityCorpusFiles {
		wanted[name] = false
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".json" || name == "fixture.schema.json" {
			continue
		}
		if _, known := wanted[name]; !known {
			return nil, fmt.Errorf("unexecuted corpus file %q", name)
		}
		wanted[name] = true
	}
	for name, found := range wanted {
		if !found {
			return nil, fmt.Errorf("required corpus file %q is missing", name)
		}
	}

	all := make([]compatibilityCase, 0, 64)
	identifiers := make(map[string]string, 64)
	for _, name := range compatibilityCorpusFiles {
		path := filepath.Join(directory, name)
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(file)
		decoder.DisallowUnknownFields()
		var document compatibilityDocument
		decodeErr := decoder.Decode(&document)
		if decodeErr == nil {
			var trailing json.RawMessage
			if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
				decodeErr = fmt.Errorf("trailing JSON: %w", err)
			}
		}
		closeErr := file.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("%s: %w", name, decodeErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if document.FormatVersion != compatibilityFormatVersion {
			return nil, fmt.Errorf("%s: unknown format_version %d", name, document.FormatVersion)
		}
		if len(document.Cases) == 0 {
			return nil, fmt.Errorf("%s: cases must not be empty", name)
		}
		for index := range document.Cases {
			fixture := &document.Cases[index]
			fixture.file = name
			if err := validateCompatibilityCase(*fixture); err != nil {
				return nil, fmt.Errorf("%s case %d: %w", name, index, err)
			}
			if previous, duplicate := identifiers[fixture.ID]; duplicate {
				return nil, fmt.Errorf("duplicate case id %q in %s and %s", fixture.ID, previous, name)
			}
			identifiers[fixture.ID] = name
		}
		all = append(all, document.Cases...)
	}
	return all, nil
}

type compatibilityEnvironment struct {
	handler         *Handler
	authenticator   *compatibilityAuthenticator
	admission       *compatibilityAdmission
	acknowledgments *compatibilityAcknowledgmentReader
	health          *compatibilityHealth
	next            *compatibilityNext
	credentials     map[string]string
	conditions      map[string]bool
	shutdown        bool
}

func newCompatibilityEnvironment(fixture compatibilityCase, ordinal int) (*compatibilityEnvironment, error) {
	receivedAt, err := time.Parse(time.RFC3339Nano, fixture.Setup.ReceivedAt)
	if err != nil {
		return nil, err
	}
	conditions := make(map[string]bool, len(fixture.Setup.Conditions))
	for _, condition := range fixture.Setup.Conditions {
		conditions[condition] = true
	}
	credentials := make(map[string]string, 1)
	credential := ""
	var authentication auth.Authentication
	var authenticationErr error
	if fixture.Setup.Token != nil {
		credential = fmt.Sprintf("fixture-%03d-%s-credential", ordinal, fixture.Setup.Token.Alias)
		credentials[fixture.Setup.Token.Alias] = credential
		authentication, authenticationErr = compatibilityAuthentication(fixture.Setup)
	}
	authenticator := &compatibilityAuthenticator{
		credential:     credential,
		authentication: authentication,
		err:            authenticationErr,
	}
	admission := &compatibilityAdmission{
		setup:      fixture.Setup,
		conditions: conditions,
		durable:    absentCompatibilityDurable(),
		consumed:   make(map[string]bool),
	}
	acknowledgments := &compatibilityAcknowledgmentReader{rows: fixture.Setup.Acknowledgments}
	health := &compatibilityHealth{conditions: conditions, consumed: make(map[string]bool)}
	next := &compatibilityNext{}
	handler, err := New(Config{
		Next:                              next,
		Authenticator:                     authenticator,
		Admission:                         admission,
		Acknowledgments:                   acknowledgments,
		Health:                            health,
		TenantID:                          compatibilityTenantID,
		MaximumConcurrentRequests:         4,
		MaximumConcurrentRequestsPerToken: 2,
		Now:                               func() time.Time { return receivedAt },
		NewRequestID:                      func() (string, error) { return compatibilityRequestID, nil },
	})
	if err != nil {
		return nil, err
	}
	environment := &compatibilityEnvironment{
		handler:         handler,
		authenticator:   authenticator,
		admission:       admission,
		acknowledgments: acknowledgments,
		health:          health,
		next:            next,
		credentials:     credentials,
		conditions:      conditions,
	}
	if conditions["shutdown"] {
		handler.BeginShutdown()
		environment.shutdown = true
	}
	return environment, nil
}

func compatibilityAuthentication(setup compatibilitySetup) (auth.Authentication, error) {
	token := setup.Token
	if token == nil {
		return auth.Authentication{}, auth.ErrUnauthorized
	}
	allowed := make(map[string]struct{}, len(token.AllowedIndexes))
	for _, name := range token.AllowedIndexes {
		allowed[name] = struct{}{}
	}
	policies := make([]auth.AuthorizedIndexPolicy, 0, len(token.AllowedIndexes))
	for _, index := range setup.Indexes {
		_, isAllowed := allowed[index.Name]
		if !isAllowed || index.State != "active" || index.IngestionEnabled == nil || !*index.IngestionEnabled {
			continue
		}
		policy := auth.AuthorizedIndexPolicy{Name: index.Name, Version: 1}
		if index.DefaultSourcetype != nil {
			policy.DefaultSourcetype = *index.DefaultSourcetype
		}
		policies = append(policies, policy)
	}
	profile := auth.HECTokenProfile{IndexerAcknowledgment: *token.Acknowledgment}
	if token.Defaults.Index != nil {
		profile.DefaultIndexName = *token.Defaults.Index
	}
	if token.Defaults.Host != nil {
		profile.DefaultHost = *token.Defaults.Host
	}
	if token.Defaults.Source != nil {
		profile.DefaultSource = *token.Defaults.Source
	}
	if token.Defaults.Sourcetype != nil {
		profile.DefaultSourcetype = *token.Defaults.Sourcetype
	}
	authentication := auth.Authentication{
		TokenID:           "compatibility-token-record-" + token.Alias,
		TokenVersion:      1,
		Purpose:           auth.IngestionTokenPurpose(token.Purpose),
		HECProfile:        profile,
		AuthorizedIndexes: policies,
	}
	switch {
	case token.State == "disabled":
		return auth.Authentication{}, auth.ErrInactiveToken
	case token.State != "active", token.Purpose != string(auth.IngestionTokenPurposeHEC):
		return auth.Authentication{}, auth.ErrUnauthorized
	case len(policies) == 0:
		return auth.Authentication{}, auth.ErrNoActiveIndexAuthority
	default:
		return authentication, nil
	}
}

func (environment *compatibilityEnvironment) request(fixture compatibilityRequest) (*http.Request, error) {
	body, err := compatibilityBodyBytes(fixture.Body)
	if err != nil {
		return nil, err
	}
	var query strings.Builder
	for index, item := range fixture.Query {
		if index != 0 {
			query.WriteByte('&')
		}
		query.WriteString(url.QueryEscape(*item.Name))
		query.WriteByte('=')
		query.WriteString(url.QueryEscape(*item.Value))
	}
	target := fixture.Path
	if query.Len() != 0 {
		target += "?" + query.String()
	}
	request, err := http.NewRequestWithContext(
		context.Background(),
		fixture.Method,
		"http://hec.invalid"+target,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	request.RequestURI = target
	for _, header := range fixture.Headers {
		value, err := interpolateCompatibilityTokens(*header.Value, environment.credentials)
		if err != nil {
			return nil, fmt.Errorf("header %s: %w", header.Name, err)
		}
		if strings.EqualFold(header.Name, "Content-Length") {
			length, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Content-Length: %w", err)
			}
			request.ContentLength = length
			continue
		}
		request.Header.Add(header.Name, value)
	}
	return request, nil
}

func interpolateCompatibilityTokens(value string, credentials map[string]string) (string, error) {
	for alias, credential := range credentials {
		value = strings.ReplaceAll(value, "{{token:"+alias+"}}", credential)
	}
	value = strings.ReplaceAll(value, "{{token:unknown}}", "fixture-unknown-well-formed-credential")
	if strings.Contains(value, "{{") || strings.Contains(value, "}}") {
		return "", errors.New("unknown fixture interpolation")
	}
	return value, nil
}

func compatibilityBodyBytes(body compatibilityBody) ([]byte, error) {
	switch body.Kind {
	case "utf8":
		return []byte(*body.Value), nil
	case "base64":
		return base64.StdEncoding.Strict().DecodeString(*body.Value)
	case "gzip_utf8":
		return deterministicCompatibilityGZIP(*body.Value)
	case "concatenated_gzip_utf8":
		var result []byte
		for _, member := range *body.Members {
			encoded, err := deterministicCompatibilityGZIP(member)
			if err != nil {
				return nil, err
			}
			result = append(result, encoded...)
		}
		return result, nil
	case "repeat_utf8":
		if *body.Count > math.MaxInt/len(*body.Unit) {
			return nil, errors.New("repeat body overflows address space")
		}
		return bytes.Repeat([]byte(*body.Unit), *body.Count), nil
	default:
		return nil, fmt.Errorf("unknown body generator %q", body.Kind)
	}
}

func deterministicCompatibilityGZIP(value string) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	writer.ModTime = time.Time{}
	writer.Name = ""
	writer.Comment = ""
	writer.OS = 255
	if _, err := writer.Write([]byte(value)); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

type compatibilityAuthenticator struct {
	mu             sync.Mutex
	calls          int
	overLimit      bool
	credential     string
	authentication auth.Authentication
	err            error
}

func (fake *compatibilityAuthenticator) AuthenticateHEC(_ context.Context, credential string) (auth.Authentication, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls++
	if fake.calls > 1 {
		fake.overLimit = true
		return auth.Authentication{}, errors.New("compatibility authenticator call bound exceeded")
	}
	if fake.credential == "" || credential != fake.credential {
		return auth.Authentication{}, auth.ErrUnauthorized
	}
	return fake.authentication, fake.err
}

func (fake *compatibilityAuthenticator) snapshot() (int, bool) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls, fake.overLimit
}

type compatibilityAdmission struct {
	mu         sync.Mutex
	setup      compatibilitySetup
	conditions map[string]bool
	consumed   map[string]bool
	calls      int
	overLimit  bool
	durable    compatibilityDurable
	committed  *ingest.AdmissionRequest
}

func (fake *compatibilityAdmission) Stage(_ context.Context, request ingest.AdmissionRequest) (ingest.StageResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls++
	if fake.calls > 1 || len(request.Events) > 1_000 {
		fake.overLimit = true
		return ingest.StageResult{}, errors.New("compatibility admission bound exceeded")
	}
	authorized := make(map[string]struct{}, len(request.Authorization.AuthorizedIndexes))
	for _, index := range request.Authorization.AuthorizedIndexes {
		authorized[index.Name] = struct{}{}
	}
	for ordinal, admitted := range request.Events {
		if admitted.Event == nil {
			return ingest.StageResult{}, errors.New("compatibility admission received nil event")
		}
		if _, ok := authorized[admitted.Event.GetIndexName()]; !ok {
			return ingest.StageResult{}, &ingest.AdmissionFailure{
				EventIndex: uint32(ordinal),
				Failure: &ingest.EventError{
					Code: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX,
				},
			}
		}
	}
	switch {
	case fake.conditions["ack_capacity"]:
		fake.consumed["ack_capacity"] = true
		return ingest.StageResult{}, visibility.ErrHECAcknowledgmentCapacity
	case fake.conditions["outbox_capacity"]:
		fake.consumed["outbox_capacity"] = true
		return ingest.StageResult{}, visibility.ErrPendingCapacity
	case fake.conditions["quota_limited"]:
		fake.consumed["quota_limited"] = true
		fake.durable.Quota = "unchanged"
		return ingest.StageResult{}, &ingest.TransientStoreError{
			Err:        errors.New("compatibility quota limited"),
			Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED,
			RetryAfter: time.Duration(*fake.setup.RetryAfterSeconds) * time.Second,
		}
	case fake.conditions["staging_busy"]:
		fake.consumed["staging_busy"] = true
		return ingest.StageResult{}, &ingest.TransientStoreError{Err: errors.New("compatibility staging busy")}
	case fake.conditions["staging_internal"]:
		fake.consumed["staging_internal"] = true
		return ingest.StageResult{}, errors.New("compatibility staging internal failure")
	}

	copyRequest := request
	fake.committed = &copyRequest
	fake.durable = compatibilityDurable{
		Quota:      "charged",
		Request:    "staged",
		Ack:        "absent",
		Outbox:     "pending",
		Visibility: "reserved",
	}
	var acknowledgmentID uint64
	if request.HECAdmission != nil && request.HECAdmission.AcknowledgmentEnabled {
		fake.durable.Ack = "pending"
		for _, allocation := range fake.setup.AckAllocations {
			if allocation.Channel != request.HECAdmission.Channel {
				continue
			}
			allocated, err := strconv.ParseUint(allocation.ID, 10, 64)
			if err != nil || allocated == 0 || allocated > 1<<53-1 {
				return ingest.StageResult{}, visibility.ErrHECAcknowledgmentCapacity
			}
			acknowledgmentID = allocated
			break
		}
	}
	var uncompressed uint64
	for _, event := range request.Events {
		uncompressed += event.UncompressedBytes
	}
	return ingest.StageResult{
		VisibilitySequence:  1,
		State:               ingest.StoredBatchPending,
		AcceptedEvents:      uint32(len(request.Events)),
		UncompressedBytes:   uncompressed,
		HECRequestSequence:  1,
		HECAcknowledgmentID: acknowledgmentID,
	}, nil
}

func (fake *compatibilityAdmission) snapshot() (int, bool, compatibilityDurable, []ingest.AdmissionEvent, map[string]bool) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	var events []ingest.AdmissionEvent
	if fake.committed != nil {
		events = append(events, fake.committed.Events...)
	}
	consumed := make(map[string]bool, len(fake.consumed))
	maps.Copy(consumed, fake.consumed)
	return fake.calls, fake.overLimit, fake.durable, events, consumed
}

type compatibilityAcknowledgmentReader struct {
	mu        sync.Mutex
	rows      []compatibilityAcknowledgment
	calls     int
	overLimit bool
}

func (fake *compatibilityAcknowledgmentReader) LookupHECAcknowledgments(
	_ context.Context,
	_, _, channel string,
	ids []uint64,
) (map[uint64]bool, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls++
	if fake.calls > 1 || len(ids) > 1_000 {
		fake.overLimit = true
		return nil, errors.New("compatibility acknowledgment lookup bound exceeded")
	}
	indexed := make(map[uint64]bool, len(fake.rows))
	for _, row := range fake.rows {
		if row.Channel != channel || row.State != "indexed" {
			continue
		}
		id, err := strconv.ParseUint(row.ID, 10, 64)
		if err != nil {
			return nil, err
		}
		indexed[id] = true
	}
	result := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		result[id] = indexed[id]
	}
	return result, nil
}

func (fake *compatibilityAcknowledgmentReader) snapshot() (int, bool) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls, fake.overLimit
}

type compatibilityHealth struct {
	mu         sync.Mutex
	conditions map[string]bool
	consumed   map[string]bool
	calls      int
	overLimit  bool
}

func (fake *compatibilityHealth) HECHealth(context.Context) (HealthSnapshot, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls++
	if fake.calls > 1 {
		fake.overLimit = true
		return HealthSnapshot{}, errors.New("compatibility health call bound exceeded")
	}
	if fake.conditions["queue_unhealthy"] {
		fake.consumed["queue_unhealthy"] = true
	}
	if fake.conditions["ack_unhealthy"] {
		fake.consumed["ack_unhealthy"] = true
	}
	return HealthSnapshot{
		QueueAvailable:          !fake.conditions["queue_unhealthy"],
		AcknowledgmentAvailable: !fake.conditions["ack_unhealthy"],
	}, nil
}

func (fake *compatibilityHealth) snapshot() (int, bool, map[string]bool) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	consumed := make(map[string]bool, len(fake.consumed))
	maps.Copy(consumed, fake.consumed)
	return fake.calls, fake.overLimit, consumed
}

type compatibilityNext struct {
	mu    sync.Mutex
	calls int
}

func (fake *compatibilityNext) ServeHTTP(response http.ResponseWriter, _ *http.Request) {
	fake.mu.Lock()
	fake.calls++
	fake.mu.Unlock()
	http.Error(response, "unexpected non-HEC request", http.StatusInternalServerError)
}

func (fake *compatibilityNext) callCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls
}

func absentCompatibilityDurable() compatibilityDurable {
	return compatibilityDurable{
		Quota: "absent", Request: "absent", Ack: "absent", Outbox: "absent", Visibility: "absent",
	}
}

func unchangedCompatibilityDurable() compatibilityDurable {
	return compatibilityDurable{
		Quota: "unchanged", Request: "unchanged", Ack: "unchanged", Outbox: "unchanged", Visibility: "unchanged",
	}
}

func (environment *compatibilityEnvironment) durableDisposition() compatibilityDurable {
	ackCalls, _ := environment.acknowledgments.snapshot()
	if ackCalls != 0 {
		return unchangedCompatibilityDurable()
	}
	_, _, durable, _, _ := environment.admission.snapshot()
	return durable
}

func (environment *compatibilityEnvironment) committedEvents() []ingest.AdmissionEvent {
	_, _, _, events, _ := environment.admission.snapshot()
	return events
}

func (environment *compatibilityEnvironment) assertBoundedAndConsumed(t *testing.T) {
	t.Helper()
	if calls, exceeded := environment.authenticator.snapshot(); exceeded || calls > 1 {
		t.Errorf("authenticator calls = %d, bounded maximum is 1", calls)
	}
	stageCalls, stageExceeded, _, _, stageConsumed := environment.admission.snapshot()
	if stageExceeded || stageCalls > 1 {
		t.Errorf("admission calls = %d, bounded maximum is 1", stageCalls)
	}
	if calls, exceeded := environment.acknowledgments.snapshot(); exceeded || calls > 1 {
		t.Errorf("acknowledgment calls = %d, bounded maximum is 1", calls)
	}
	healthCalls, healthExceeded, healthConsumed := environment.health.snapshot()
	if healthExceeded || healthCalls > 1 {
		t.Errorf("health calls = %d, bounded maximum is 1", healthCalls)
	}
	for condition := range environment.conditions {
		consumed := stageConsumed[condition] || healthConsumed[condition]
		if condition == "shutdown" {
			consumed = environment.shutdown
		}
		if !consumed {
			t.Errorf("fixture condition %q was not exercised", condition)
		}
	}
}

func assertCompatibilityHTTP(t *testing.T, response *httptest.ResponseRecorder, want compatibilityHTTP) {
	t.Helper()
	if response.Code != want.Status {
		t.Errorf("HTTP status = %d, want %d", response.Code, want.Status)
	}
	if got := response.Body.String(); got != *want.BodyUTF8 {
		t.Errorf("HTTP body = %q, want %q", got, *want.BodyUTF8)
	}
	for name, value := range want.Headers {
		values := response.Header().Values(name)
		if len(values) != 1 || values[0] != value {
			t.Errorf("HTTP header %q = %#v, want exactly [%q]", name, values, value)
		}
	}
	for _, conditional := range []string{"Allow", "Retry-After"} {
		if _, declared := want.Headers[conditional]; !declared && len(response.Header().Values(conditional)) != 0 {
			t.Errorf("HTTP header %q unexpectedly present: %#v", conditional, response.Header().Values(conditional))
		}
	}
}

func assertCompatibilityEvents(t *testing.T, got []ingest.AdmissionEvent, want []compatibilityEvent) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("durable events = %d, want %d", len(got), len(want))
	}
	for ordinal := range want {
		expected := want[ordinal]
		if expected.Ordinal != ordinal {
			t.Fatalf("fixture event ordinal %d appears at projection position %d", expected.Ordinal, ordinal)
		}
		event := got[ordinal].Event
		if event == nil {
			t.Errorf("event %d is nil", ordinal)
			continue
		}
		if event.GetIndexName() != expected.Index || event.GetHost() != *expected.Host ||
			event.GetSource() != *expected.Source || event.GetSourcetype() != *expected.Sourcetype ||
			string(event.GetRaw()) != *expected.Raw {
			t.Errorf("event %d metadata/raw = index:%q host:%q source:%q sourcetype:%q raw:%q; want index:%q host:%q source:%q sourcetype:%q raw:%q",
				ordinal, event.GetIndexName(), event.GetHost(), event.GetSource(), event.GetSourcetype(), string(event.GetRaw()),
				expected.Index, *expected.Host, *expected.Source, *expected.Sourcetype, *expected.Raw)
		}
		if event.Message == nil && expected.Message != nil || event.Message != nil && expected.Message == nil ||
			event.Message != nil && expected.Message != nil && *event.Message != *expected.Message {
			t.Errorf("event %d message = %#v, want %#v", ordinal, event.Message, expected.Message)
		}
		if gotNanos := compatibilityTimestampNanos(event.GetEventTime()); gotNanos != expected.TimeUnixNanos {
			t.Errorf("event %d time_unix_nanos = %q, want %q", ordinal, gotNanos, expected.TimeUnixNanos)
		}
		gotTimeSource := ""
		switch event.GetEventTimeSource() {
		case opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED:
			gotTimeSource = "explicit"
		case opensplunk.EventTimeSource_EVENT_TIME_SOURCE_RECEIVED_AT_FALLBACK:
			gotTimeSource = "received_at_fallback"
		default:
			gotTimeSource = event.GetEventTimeSource().String()
		}
		if gotTimeSource != expected.TimeSource {
			t.Errorf("event %d time_source = %q, want %q", ordinal, gotTimeSource, expected.TimeSource)
		}
		fields := event.GetFields().GetFields()
		if len(fields) != len(expected.Fields) {
			t.Errorf("event %d fields = %d, want %d", ordinal, len(fields), len(expected.Fields))
			continue
		}
		for fieldIndex, expectedField := range expected.Fields {
			field := fields[fieldIndex]
			if field == nil || field.Name != expectedField.Name {
				t.Errorf("event %d field %d name = %#v, want %q", ordinal, fieldIndex, field, expectedField.Name)
				continue
			}
			assertCompatibilityValue(t, fmt.Sprintf("event %d field %q", ordinal, field.Name), field.Value, expectedField.Value)
		}
	}
}

func compatibilityTimestampNanos(timestamp *timestamppb.Timestamp) string {
	if timestamp == nil {
		return "<nil>"
	}
	var result big.Int
	result.SetInt64(timestamp.GetSeconds())
	result.Mul(&result, big.NewInt(1_000_000_000))
	result.Add(&result, big.NewInt(int64(timestamp.GetNanos())))
	return result.String()
}

func assertCompatibilityValue(t *testing.T, location string, got *opensplunk.TypedValue, want compatibilityValue) {
	t.Helper()
	if got == nil {
		t.Errorf("%s value is nil", location)
		return
	}
	switch want.Kind {
	case "null":
		if _, ok := got.Kind.(*opensplunk.TypedValue_NullValue); !ok {
			t.Errorf("%s kind = %T, want null", location, got.Kind)
		}
	case "string":
		var value string
		if err := json.Unmarshal(want.Value, &value); err != nil {
			t.Fatalf("%s decode expected string: %v", location, err)
		}
		if _, ok := got.Kind.(*opensplunk.TypedValue_StringValue); !ok || got.GetStringValue() != value {
			t.Errorf("%s value = %#v, want string %q", location, got.Kind, value)
		}
	case "sint64":
		var authored string
		if err := json.Unmarshal(want.Value, &authored); err != nil {
			t.Fatalf("%s decode expected sint64: %v", location, err)
		}
		value, err := strconv.ParseInt(authored, 10, 64)
		if err != nil {
			t.Fatalf("%s expected sint64 %q: %v", location, authored, err)
		}
		if _, ok := got.Kind.(*opensplunk.TypedValue_Sint64Value); !ok || got.GetSint64Value() != value {
			t.Errorf("%s value = %#v, want sint64 %d", location, got.Kind, value)
		}
	case "uint64":
		var authored string
		if err := json.Unmarshal(want.Value, &authored); err != nil {
			t.Fatalf("%s decode expected uint64: %v", location, err)
		}
		value, err := strconv.ParseUint(authored, 10, 64)
		if err != nil {
			t.Fatalf("%s expected uint64 %q: %v", location, authored, err)
		}
		if _, ok := got.Kind.(*opensplunk.TypedValue_Uint64Value); !ok || got.GetUint64Value() != value {
			t.Errorf("%s value = %#v, want uint64 %d", location, got.Kind, value)
		}
	case "decimal":
		var value string
		if err := json.Unmarshal(want.Value, &value); err != nil {
			t.Fatalf("%s decode expected decimal: %v", location, err)
		}
		if _, ok := got.Kind.(*opensplunk.TypedValue_DecimalValue); !ok || got.GetDecimalValue().GetValue() != value {
			t.Errorf("%s value = %#v, want decimal %q", location, got.Kind, value)
		}
	case "bool":
		var value bool
		if err := json.Unmarshal(want.Value, &value); err != nil {
			t.Fatalf("%s decode expected bool: %v", location, err)
		}
		if _, ok := got.Kind.(*opensplunk.TypedValue_BoolValue); !ok || got.GetBoolValue() != value {
			t.Errorf("%s value = %#v, want bool %t", location, got.Kind, value)
		}
	case "list":
		list, ok := got.Kind.(*opensplunk.TypedValue_ListValue)
		if !ok || list.ListValue == nil {
			t.Errorf("%s kind = %T, want list", location, got.Kind)
			return
		}
		values := list.ListValue.Values
		if len(values) != len(*want.Items) {
			t.Errorf("%s list length = %d, want %d", location, len(values), len(*want.Items))
			return
		}
		for index, item := range *want.Items {
			assertCompatibilityValue(t, fmt.Sprintf("%s[%d]", location, index), values[index], item)
		}
	default:
		t.Fatalf("%s unknown expected value kind %q", location, want.Kind)
	}
}

func validateCompatibilityCase(fixture compatibilityCase) error {
	if !compatibilityCaseIDPattern.MatchString(fixture.ID) {
		return fmt.Errorf("invalid or missing id %q", fixture.ID)
	}
	if len(fixture.Rule) == 0 || len(fixture.Rule) > 512 {
		return errors.New("rule is missing or exceeds 512 bytes")
	}
	request := fixture.Request
	if len(request.Method) == 0 || len(request.Method) > 16 {
		return fmt.Errorf("request method %q is invalid", request.Method)
	}
	if !strings.HasPrefix(request.Path, "/") || len(request.Path) > 8_192 {
		return fmt.Errorf("request path %q is invalid", request.Path)
	}
	if request.Headers == nil || len(request.Headers) > 32 {
		return errors.New("request headers are missing or exceed 32")
	}
	for index, header := range request.Headers {
		if len(header.Name) == 0 || len(header.Name) > 128 || header.Value == nil || len(*header.Value) > 16_384 {
			return fmt.Errorf("request header %d is invalid", index)
		}
	}
	if request.Query == nil || len(request.Query) > 16 {
		return errors.New("request query is missing or exceeds 16")
	}
	for index, item := range request.Query {
		if item.Name == nil || len(*item.Name) > 256 || item.Value == nil || len(*item.Value) > 8_192 {
			return fmt.Errorf("request query item %d is invalid", index)
		}
	}
	if err := validateCompatibilityBody(request.Body); err != nil {
		return fmt.Errorf("request body: %w", err)
	}

	setup := fixture.Setup
	receivedAt, err := time.Parse(time.RFC3339Nano, setup.ReceivedAt)
	if err != nil || setup.ReceivedAt == "" || receivedAt.Location() == nil {
		return fmt.Errorf("setup received_at %q is invalid", setup.ReceivedAt)
	}
	if setup.Indexes == nil || len(setup.Indexes) > 32 {
		return errors.New("setup indexes are missing or exceed 32")
	}
	if setup.Token != nil {
		if err := validateCompatibilityToken(*setup.Token); err != nil {
			return fmt.Errorf("setup token: %w", err)
		}
	}
	indexNames := make(map[string]struct{}, len(setup.Indexes))
	for index, configured := range setup.Indexes {
		if len(configured.Name) == 0 || len(configured.Name) > 255 {
			return fmt.Errorf("setup index %d has invalid name", index)
		}
		if _, duplicate := indexNames[configured.Name]; duplicate {
			return fmt.Errorf("setup index name %q is duplicated", configured.Name)
		}
		indexNames[configured.Name] = struct{}{}
		if configured.State != "active" && configured.State != "disabled" && configured.State != "archived" {
			return fmt.Errorf("setup index %q has unknown state %q", configured.Name, configured.State)
		}
		if configured.IngestionEnabled == nil {
			return fmt.Errorf("setup index %q omits ingestion_enabled", configured.Name)
		}
		if configured.DefaultSourcetype != nil && (len(*configured.DefaultSourcetype) == 0 || len(*configured.DefaultSourcetype) > 255) {
			return fmt.Errorf("setup index %q has invalid default_sourcetype", configured.Name)
		}
	}
	if len(setup.AckAllocations) > 256 {
		return errors.New("setup ack_allocations exceed 256")
	}
	allocations := make(map[string]struct{}, len(setup.AckAllocations))
	for index, allocation := range setup.AckAllocations {
		if len(allocation.Channel) == 0 || len(allocation.Channel) > 128 {
			return fmt.Errorf("ACK allocation %d has invalid channel", index)
		}
		if _, duplicate := allocations[allocation.Channel]; duplicate {
			return fmt.Errorf("ACK allocation channel %q is duplicated", allocation.Channel)
		}
		allocations[allocation.Channel] = struct{}{}
		value, err := strconv.ParseUint(allocation.ID, 10, 53)
		if err != nil || value == 0 || strconv.FormatUint(value, 10) != allocation.ID {
			return fmt.Errorf("ACK allocation channel %q has invalid id %q", allocation.Channel, allocation.ID)
		}
	}
	if len(setup.Acknowledgments) > 1_000 {
		return errors.New("setup ack_rows exceed 1000")
	}
	ackKeys := make(map[string]struct{}, len(setup.Acknowledgments))
	for index, row := range setup.Acknowledgments {
		if len(row.Channel) == 0 || len(row.Channel) > 128 {
			return fmt.Errorf("ack row %d has invalid channel", index)
		}
		id, err := strconv.ParseUint(row.ID, 10, 53)
		if err != nil || id == 0 || strconv.FormatUint(id, 10) != row.ID {
			return fmt.Errorf("ack row %d has invalid id %q", index, row.ID)
		}
		key := row.Channel + "\x00" + row.ID
		if _, duplicate := ackKeys[key]; duplicate {
			return fmt.Errorf("ack row %s/%s is duplicated", row.Channel, row.ID)
		}
		ackKeys[key] = struct{}{}
		switch row.State {
		case "pending":
			if row.TerminalAt != nil {
				return fmt.Errorf("pending ack row %s/%s has terminal_at", row.Channel, row.ID)
			}
		case "indexed", "terminal_failure", "expired":
			if row.TerminalAt == nil {
				return fmt.Errorf("terminal ack row %s/%s omits terminal_at", row.Channel, row.ID)
			}
			if _, err := time.Parse(time.RFC3339Nano, *row.TerminalAt); err != nil {
				return fmt.Errorf("ack row %s/%s terminal_at: %w", row.Channel, row.ID, err)
			}
		default:
			return fmt.Errorf("ack row %d has unknown state %q", index, row.State)
		}
	}
	if setup.Conditions == nil {
		return errors.New("setup conditions are missing")
	}
	conditionSet := make(map[string]struct{}, len(setup.Conditions))
	for _, condition := range setup.Conditions {
		switch condition {
		case "quota_limited", "staging_busy", "staging_internal", "shutdown", "outbox_capacity", "ack_capacity", "queue_unhealthy", "ack_unhealthy":
		default:
			return fmt.Errorf("unknown setup condition %q", condition)
		}
		if _, duplicate := conditionSet[condition]; duplicate {
			return fmt.Errorf("setup condition %q is duplicated", condition)
		}
		conditionSet[condition] = struct{}{}
	}
	_, quotaLimited := conditionSet["quota_limited"]
	if quotaLimited != (setup.RetryAfterSeconds != nil) {
		return errors.New("retry_after_seconds must appear exactly with quota_limited")
	}
	if setup.RetryAfterSeconds != nil && (*setup.RetryAfterSeconds < 1 || *setup.RetryAfterSeconds > 3_600) {
		return errors.New("retry_after_seconds is outside 1..3600")
	}

	if err := validateCompatibilityExpectation(fixture.Expect); err != nil {
		return fmt.Errorf("expect: %w", err)
	}
	return nil
}

func validateCompatibilityBody(body compatibilityBody) error {
	valuePresent := body.Value != nil
	membersPresent := body.Members != nil
	unitPresent := body.Unit != nil
	countPresent := body.Count != nil
	switch body.Kind {
	case "utf8", "base64", "gzip_utf8":
		if !valuePresent || membersPresent || unitPresent || countPresent {
			return fmt.Errorf("generator %q has the wrong members", body.Kind)
		}
		if body.Kind == "base64" {
			if _, err := base64.StdEncoding.Strict().DecodeString(*body.Value); err != nil {
				return fmt.Errorf("base64 value: %w", err)
			}
		}
	case "concatenated_gzip_utf8":
		if valuePresent || !membersPresent || unitPresent || countPresent || len(*body.Members) < 2 || len(*body.Members) > 8 {
			return errors.New("concatenated_gzip_utf8 has the wrong members")
		}
	case "repeat_utf8":
		if valuePresent || membersPresent || !unitPresent || !countPresent || len(*body.Unit) < 1 || len(*body.Unit) > 16 || *body.Count < 1 || *body.Count > 33_554_432 {
			return errors.New("repeat_utf8 has the wrong members")
		}
	default:
		return fmt.Errorf("unknown generator kind %q", body.Kind)
	}
	return nil
}

func validateCompatibilityToken(token compatibilityToken) error {
	if !compatibilityAliasPattern.MatchString(token.Alias) {
		return fmt.Errorf("invalid alias %q", token.Alias)
	}
	if token.Purpose != "hec" && token.Purpose != "native_collector" {
		return fmt.Errorf("unknown purpose %q", token.Purpose)
	}
	switch token.State {
	case "active", "disabled", "expired", "revoked":
	default:
		return fmt.Errorf("unknown state %q", token.State)
	}
	if token.Acknowledgment == nil {
		return errors.New("ack_enabled is missing")
	}
	if len(token.AllowedIndexes) < 1 || len(token.AllowedIndexes) > 64 {
		return errors.New("allowed_indexes are missing or exceed 64")
	}
	seen := make(map[string]struct{}, len(token.AllowedIndexes))
	for _, index := range token.AllowedIndexes {
		if len(index) == 0 || len(index) > 255 {
			return fmt.Errorf("invalid allowed index %q", index)
		}
		if _, duplicate := seen[index]; duplicate {
			return fmt.Errorf("allowed index %q is duplicated", index)
		}
		seen[index] = struct{}{}
	}
	if token.Defaults == nil {
		return errors.New("defaults are missing")
	}
	for name, value := range map[string]*string{
		"index": token.Defaults.Index, "host": token.Defaults.Host,
		"source": token.Defaults.Source, "sourcetype": token.Defaults.Sourcetype,
	} {
		if value != nil && (len(*value) == 0 || len(*value) > 255) {
			return fmt.Errorf("default %s is invalid", name)
		}
	}
	return nil
}

func validateCompatibilityExpectation(expect compatibilityExpect) error {
	if expect.HTTP.Status < 100 || expect.HTTP.Status > 599 || expect.HTTP.Headers == nil || expect.HTTP.BodyUTF8 == nil {
		return errors.New("http status, headers, or body_utf8 are invalid or missing")
	}
	if len(*expect.HTTP.BodyUTF8) > 1_048_576 {
		return errors.New("http body_utf8 exceeds 1 MiB")
	}
	for name, value := range expect.HTTP.Headers {
		if len(name) == 0 || len(name) > 128 || len(value) > 1_024 {
			return fmt.Errorf("http header %q is invalid", name)
		}
	}
	for name, value := range map[string]string{
		"quota": expect.Durable.Quota, "request": expect.Durable.Request,
		"ack": expect.Durable.Ack, "outbox": expect.Durable.Outbox, "visibility": expect.Durable.Visibility,
	} {
		allowed := false
		switch name {
		case "quota":
			allowed = value == "absent" || value == "unchanged" || value == "charged"
		case "request":
			allowed = value == "absent" || value == "unchanged" || value == "staged"
		case "ack", "outbox":
			allowed = value == "absent" || value == "unchanged" || value == "pending"
		case "visibility":
			allowed = value == "absent" || value == "unchanged" || value == "reserved"
		}
		if !allowed {
			return fmt.Errorf("durable %s has unknown disposition %q", name, value)
		}
	}
	if expect.Events == nil || len(expect.Events) > 1_000 {
		return errors.New("events are missing or exceed 1000")
	}
	for index, event := range expect.Events {
		if event.Ordinal != index || len(event.Index) == 0 || len(event.Index) > 255 {
			return fmt.Errorf("event %d has invalid ordinal or index", index)
		}
		if event.TimeSource != "explicit" && event.TimeSource != "received_at_fallback" {
			return fmt.Errorf("event %d has unknown time_source %q", index, event.TimeSource)
		}
		if !compatibilityNanosPattern.MatchString(event.TimeUnixNanos) {
			return fmt.Errorf("event %d has invalid time_unix_nanos %q", index, event.TimeUnixNanos)
		}
		for name, value := range map[string]*string{
			"host": event.Host, "source": event.Source, "sourcetype": event.Sourcetype, "raw": event.Raw,
		} {
			if value == nil {
				return fmt.Errorf("event %d omits %s", index, name)
			}
		}
		if len(*event.Host) > 255 || len(*event.Source) > 255 || len(*event.Sourcetype) > 255 || len(*event.Raw) > 1_048_576 ||
			event.Message != nil && len(*event.Message) > 1_048_576 {
			return fmt.Errorf("event %d projection exceeds its bound", index)
		}
		if event.Fields == nil || len(event.Fields) > 1_024 {
			return fmt.Errorf("event %d fields are missing or exceed 1024", index)
		}
		for fieldIndex, field := range event.Fields {
			if len(field.Name) == 0 || len(field.Name) > 256 {
				return fmt.Errorf("event %d field %d has invalid name", index, fieldIndex)
			}
			if err := validateCompatibilityValue(field.Value, true); err != nil {
				return fmt.Errorf("event %d field %q: %w", index, field.Name, err)
			}
		}
	}
	if expect.SPL != nil {
		if len(expect.SPL.Query) == 0 || len(expect.SPL.Query) > 4_096 || expect.SPL.Rows == nil || len(expect.SPL.Rows) > 1_000 {
			return errors.New("spl query or rows are invalid")
		}
		for index, row := range expect.SPL.Rows {
			if row == nil {
				return fmt.Errorf("spl row %d is not an object", index)
			}
		}
	}
	return nil
}

func validateCompatibilityValue(value compatibilityValue, allowList bool) error {
	hasValue := len(value.Value) != 0
	hasItems := value.Items != nil
	switch value.Kind {
	case "null":
		if hasValue || hasItems {
			return errors.New("null value has unexpected members")
		}
	case "string", "sint64", "uint64", "decimal", "bool":
		if !hasValue || hasItems {
			return fmt.Errorf("%s value has the wrong members", value.Kind)
		}
		if err := validateCompatibilityScalarValue(value); err != nil {
			return err
		}
	case "list":
		if !allowList || hasValue || !hasItems || len(*value.Items) > 1_024 {
			return errors.New("list value has the wrong members")
		}
		for index, item := range *value.Items {
			if err := validateCompatibilityValue(item, false); err != nil {
				return fmt.Errorf("list item %d: %w", index, err)
			}
		}
	default:
		return fmt.Errorf("unknown value kind %q", value.Kind)
	}
	return nil
}

func validateCompatibilityScalarValue(value compatibilityValue) error {
	var authored string
	switch value.Kind {
	case "string":
		return json.Unmarshal(value.Value, &authored)
	case "sint64":
		if err := json.Unmarshal(value.Value, &authored); err != nil {
			return err
		}
		if !compatibilitySintPattern.MatchString(authored) {
			return fmt.Errorf("invalid sint64 string %q", authored)
		}
		if _, err := strconv.ParseInt(authored, 10, 64); err != nil {
			return fmt.Errorf("invalid sint64 string %q", authored)
		}
	case "uint64":
		if err := json.Unmarshal(value.Value, &authored); err != nil {
			return err
		}
		if !compatibilityUintPattern.MatchString(authored) {
			return fmt.Errorf("invalid uint64 string %q", authored)
		}
		if _, err := strconv.ParseUint(authored, 10, 64); err != nil {
			return fmt.Errorf("invalid uint64 string %q", authored)
		}
	case "decimal":
		if err := json.Unmarshal(value.Value, &authored); err != nil {
			return err
		}
		if len(authored) > 128 || !compatibilityDecimalPattern.MatchString(authored) {
			return fmt.Errorf("invalid decimal string %q", authored)
		}
	case "bool":
		var boolean bool
		if err := json.Unmarshal(value.Value, &boolean); err != nil {
			return err
		}
	}
	return nil
}
