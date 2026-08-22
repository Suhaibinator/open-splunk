// Package hechttp exposes the bounded HEC HTTP compatibility surface.
// It adapts protocol requests to shared ingestion admission and never writes
// directly to ClickHouse or chooses authorization policy itself.
package hechttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"fortio.org/safecast"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/hec"
	"github.com/Suhaibinator/open-splunk/internal/hecadapter"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

const (
	DefaultMaximumConcurrentRequests         = 128
	DefaultMaximumConcurrentRequestsPerToken = 16
	defaultMaximumConcurrentHealthRequests   = 8
	maximumRetryAfterSeconds                 = 3_600
)

// Authenticator resolves a plaintext HEC credential into a safe, current
// policy snapshot and records successful token use.
type Authenticator interface {
	AuthenticateHEC(context.Context, string) (auth.Authentication, error)
}

// AdmissionStager owns request-atomic normalization, authorization, quota,
// outbox, visibility, and optional acknowledgment persistence.
type AdmissionStager interface {
	Stage(context.Context, ingest.AdmissionRequest) (ingest.StageResult, error)
}

// HealthSnapshot is deliberately aggregate. It contains no queue counts,
// token/index identities, storage addresses, or failure details.
type HealthSnapshot struct {
	QueueAvailable          bool
	AcknowledgmentAvailable bool
}

// HealthChecker provides one bounded aggregate health observation.
type HealthChecker interface {
	HECHealth(context.Context) (HealthSnapshot, error)
}

// HealthCheckerFunc adapts a function to HealthChecker.
type HealthCheckerFunc func(context.Context) (HealthSnapshot, error)

func (function HealthCheckerFunc) HECHealth(ctx context.Context) (HealthSnapshot, error) {
	return function(ctx)
}

// RequestIDGenerator returns a fresh canonical server request ID. It must not
// derive the ID from credential, channel, query, or body material.
type RequestIDGenerator func() (string, error)

// Config requires the complete HEC dependency set. Construction fails rather
// than registering a partial protocol surface.
type Config struct {
	Next                              http.Handler
	Authenticator                     Authenticator
	Admission                         AdmissionStager
	Acknowledgments                   visibility.HECAcknowledgmentReader
	Health                            HealthChecker
	Metrics                           *Metrics
	Limits                            hec.Limits
	TenantID                          string
	MaximumConcurrentRequests         int
	MaximumConcurrentRequestsPerToken int
	Now                               func() time.Time
	NewRequestID                      RequestIDGenerator
}

// Handler is a complete HEC route namespace wrapper. Requests outside the
// namespace are delegated unchanged to Next.
type Handler struct {
	next            http.Handler
	authenticator   Authenticator
	admission       AdmissionStager
	acknowledgments visibility.HECAcknowledgmentReader
	health          HealthChecker
	metrics         *Metrics
	limits          hec.Limits
	tenantID        string
	now             func() time.Time
	newRequestID    RequestIDGenerator
	globalSlots     chan struct{}
	healthSlots     chan struct{}
	perTokenLimit   int
	tokenSlots      tokenSemaphores
	lifecycle       lifecycleGate
}

// New constructs the complete namespace. Callers should omit construction
// entirely when HEC is disabled.
func New(config Config) (*Handler, error) {
	if config.Next == nil || config.Authenticator == nil || config.Admission == nil ||
		config.Acknowledgments == nil || config.Health == nil {
		return nil, errors.New("complete HEC dependencies are required")
	}
	if config.Limits == (hec.Limits{}) {
		config.Limits = hec.DefaultLimits()
	}
	if err := config.Limits.Validate(); err != nil {
		return nil, err
	}
	if config.TenantID == "" || strings.TrimSpace(config.TenantID) != config.TenantID ||
		strings.IndexByte(config.TenantID, 0) >= 0 || len(config.TenantID) > 255 {
		return nil, errors.New("HEC tenant ID is invalid")
	}
	if config.MaximumConcurrentRequests == 0 {
		config.MaximumConcurrentRequests = DefaultMaximumConcurrentRequests
	}
	if config.MaximumConcurrentRequestsPerToken == 0 {
		config.MaximumConcurrentRequestsPerToken = DefaultMaximumConcurrentRequestsPerToken
	}
	if config.MaximumConcurrentRequests < 1 || config.MaximumConcurrentRequests > 65_536 ||
		config.MaximumConcurrentRequestsPerToken < 1 ||
		config.MaximumConcurrentRequestsPerToken > config.MaximumConcurrentRequests {
		return nil, errors.New("HEC concurrency limits are invalid")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.NewRequestID == nil {
		config.NewRequestID = randomRequestID
	}
	if config.Metrics == nil {
		config.Metrics = NewMetrics()
	}
	handler := &Handler{
		next:            config.Next,
		authenticator:   config.Authenticator,
		admission:       config.Admission,
		acknowledgments: config.Acknowledgments,
		health:          config.Health,
		metrics:         config.Metrics,
		limits:          config.Limits,
		tenantID:        config.TenantID,
		now:             config.Now,
		newRequestID:    config.NewRequestID,
		globalSlots:     make(chan struct{}, config.MaximumConcurrentRequests),
		healthSlots:     make(chan struct{}, min(defaultMaximumConcurrentHealthRequests, config.MaximumConcurrentRequests)),
		perTokenLimit:   config.MaximumConcurrentRequestsPerToken,
	}
	handler.lifecycle.initialize()
	return handler, nil
}

func randomRequestID() (string, error) {
	var source [16]byte
	if _, err := io.ReadFull(rand.Reader, source[:]); err != nil {
		return "", fmt.Errorf("generate HEC request ID: %w", err)
	}
	return hex.EncodeToString(source[:]), nil
}

// Metrics returns the handler's bounded aggregate metric owner.
func (handler *Handler) Metrics() *Metrics { return handler.metrics }

// BeginShutdown closes the admission/query lifecycle gate. It is idempotent.
// Health and closed HEC route errors remain available while new protected
// requests receive the documented shutdown result.
func (handler *Handler) BeginShutdown() { handler.lifecycle.close() }

// Shutdown closes admission and waits for already-started protected requests
// to leave the handler or for ctx to expire.
func (handler *Handler) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("HEC shutdown context is required")
	}
	handler.BeginShutdown()
	if err := handler.lifecycle.wait(ctx); err != nil {
		// The graceful budget is also the request-admission budget. Cancel
		// every protected request context before the runtime performs its final
		// ownership wait; SQLite and ClickHouse boundaries must honor it.
		handler.lifecycle.cancelActive()
		return err
	}
	return nil
}

func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	route := classifyRequestRoute(request)
	if !route.HECNamespace {
		handler.next.ServeHTTP(response, request)
		return
	}
	handler.metrics.observeRequest()
	if err := route.ProtocolError(); err != nil {
		if route.Allow != "" {
			response.Header().Set("Allow", route.Allow)
		}
		handler.writeError(response, err)
		return
	}
	if route.Endpoint == hec.EndpointHealth {
		handler.serveHealth(response, request)
		return
	}
	if request.ContentLength > handler.limits.MaximumCompressedBodyBytes {
		handler.writeError(response, hec.NewProtocolError(hec.ErrorCompressedBodyTooLarge, nil))
		return
	}
	if len(request.RequestURI) > handler.limits.MaximumRequestTargetBytes {
		handler.writeError(response, hec.NewProtocolError(hec.ErrorInvalidDataFormat, nil))
		return
	}
	encoding, err := handler.validateFraming(request, route.Endpoint)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	if queryAuthorizationPresent(request.URL.RawQuery) {
		handler.writeError(response, hec.NewProtocolError(hec.ErrorQueryAuthorizationDisabled, nil))
		return
	}
	releaseGlobal, err := handler.beginGlobal()
	if err != nil {
		handler.writeError(response, err)
		return
	}
	defer releaseGlobal()
	authentication, err := handler.authenticate(request)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	query, err := parseEndpointQuery(request.URL.RawQuery, route.Endpoint, handler.limits)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	requiredChannel := authentication.HECProfile.IndexerAcknowledgment ||
		route.Endpoint == hec.EndpointRaw || route.Endpoint == hec.EndpointAcknowledgment
	channel, _, err := hec.ParseRequestChannel(
		request.Header.Values("X-Splunk-Request-Channel"),
		query.channelValues(),
		requiredChannel,
		handler.limits.MaximumChannelBytes,
	)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	if route.Endpoint == hec.EndpointAcknowledgment &&
		!authentication.HECProfile.IndexerAcknowledgment {
		handler.writeError(response, hec.NewProtocolError(hec.ErrorAcknowledgmentDisabled, nil))
		return
	}
	protectedContext, release, err := handler.beginToken(
		request.Context(),
		authentication.TokenID,
	)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	defer release()
	request = request.WithContext(protectedContext)

	switch route.Endpoint {
	case hec.EndpointEvent:
		receivedAt := handler.now().Round(0).UTC()
		handler.serveJSON(response, request, authentication, channel, encoding, receivedAt)
	case hec.EndpointRaw:
		receivedAt := handler.now().Round(0).UTC()
		handler.serveRaw(response, request, authentication, channel, encoding, query.raw, receivedAt)
	case hec.EndpointAcknowledgment:
		handler.serveAcknowledgment(response, request, authentication, channel, encoding)
	default:
		handler.writeError(response, hec.NewProtocolError(hec.ErrorInternal, nil))
	}
}

func (handler *Handler) validateFraming(request *http.Request, endpoint hec.Endpoint) (string, error) {
	if err := hec.ValidateConsumedHeaderBytes(
		handler.limits.MaximumHeaderBytes,
		request.Header.Values("Authorization"),
		request.Header.Values("Content-Type"),
		request.Header.Values("Content-Encoding"),
		request.Header.Values("X-Splunk-Request-Channel"),
	); err != nil {
		return "", err
	}
	if err := hec.ParseContentType(endpoint, request.Header.Values("Content-Type")); err != nil {
		return "", err
	}
	return hec.ParseContentEncoding(request.Header.Values("Content-Encoding"))
}

func (handler *Handler) authenticate(request *http.Request) (auth.Authentication, error) {
	values := request.Header.Values("Authorization")
	plaintext, err := hec.ParseAuthorization(
		values,
		handler.limits.MaximumHeaderBytes,
	)
	request.Header.Del("Authorization")
	if err != nil {
		handler.metrics.observeAuthenticationFailure()
		return auth.Authentication{}, err
	}
	authentication, err := handler.authenticator.AuthenticateHEC(request.Context(), plaintext)
	if err == nil {
		return authentication, nil
	}
	handler.metrics.observeAuthenticationFailure()
	if errors.Is(err, auth.ErrInactiveToken) {
		return auth.Authentication{}, hec.NewProtocolError(hec.ErrorTokenDisabled, err)
	}
	if errors.Is(err, auth.ErrUnauthorized) || errors.Is(err, auth.ErrNoActiveIndexAuthority) ||
		errors.Is(err, auth.ErrInvalidIndexAuthority) || errors.Is(err, auth.ErrInvalidEventAuthority) {
		return auth.Authentication{}, hec.NewProtocolError(hec.ErrorInvalidToken, err)
	}
	return auth.Authentication{}, hec.NewProtocolError(hec.ErrorInternal, err)
}

func (handler *Handler) beginGlobal() (func(), error) {
	select {
	case handler.globalSlots <- struct{}{}:
	default:
		return nil, hec.NewProtocolError(hec.ErrorServerBusy, nil)
	}
	return func() { <-handler.globalSlots }, nil
}

func (handler *Handler) beginToken(
	parent context.Context,
	tokenID string,
) (context.Context, func(), error) {
	requestContext, lifecycleID, accepted := handler.lifecycle.begin(parent)
	if !accepted {
		handler.metrics.observeShutdownRejection()
		return nil, nil, hec.NewProtocolError(hec.ErrorShuttingDown, nil)
	}
	if !handler.tokenSlots.acquire(tokenID, handler.perTokenLimit) {
		handler.lifecycle.end(lifecycleID)
		return nil, nil, hec.NewProtocolError(hec.ErrorServerBusy, nil)
	}
	return requestContext, func() {
		handler.tokenSlots.release(tokenID)
		handler.lifecycle.end(lifecycleID)
	}, nil
}

func (handler *Handler) beginHealth() (func(), bool) {
	select {
	case handler.healthSlots <- struct{}{}:
		return func() { <-handler.healthSlots }, true
	default:
		return nil, false
	}
}

func classifyRequestRoute(request *http.Request) hec.Route {
	escaped := request.URL.EscapedPath()
	route := hec.ClassifyRoute(request.Method, escaped)
	if route.HECNamespace {
		return route
	}
	// If URL parsing decoded an escaped alias into the HEC namespace, retain
	// ownership of the request but reject it as an unknown path. This prevents
	// proxy/WAF raw-path policy from diverging from application routing.
	decoded := hec.ClassifyRoute(request.Method, request.URL.Path)
	if decoded.HECNamespace {
		return hec.Route{
			Endpoint:      hec.EndpointUnknown,
			HECNamespace:  true,
			KnownPath:     false,
			Method:        request.Method,
			MethodAllowed: false,
		}
	}
	return route
}

func (handler *Handler) requestContext(
	authentication auth.Authentication,
	channel hec.Channel,
	receivedAt time.Time,
) (hecadapter.RequestContext, error) {
	requestID, err := handler.newRequestID()
	if err != nil || requestID == "" {
		return hecadapter.RequestContext{}, hec.NewProtocolError(hec.ErrorInternal, err)
	}
	return hecadapter.RequestContext{
		TenantID:       handler.tenantID,
		Authentication: authentication,
		RequestID:      requestID,
		ReceivedAt:     receivedAt,
		Channel:        channel,
	}, nil
}

func (handler *Handler) serveJSON(
	response http.ResponseWriter,
	request *http.Request,
	authentication auth.Authentication,
	channel hec.Channel,
	encoding string,
	receivedAt time.Time,
) {
	body, err := hec.NewBodyReader(request.Body, encoding, handler.limits)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	defer body.Close()
	decoder, err := hec.NewEnvelopeDecoder(body, handler.limits)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	envelopes := make([]hec.Envelope, 0, 1)
	for {
		envelope, decodeErr := decoder.Next()
		if errors.Is(decodeErr, io.EOF) {
			break
		}
		if decodeErr != nil {
			handler.metrics.observeDecodeFailure()
			handler.writeError(response, decodeErr)
			return
		}
		envelopes = append(envelopes, envelope)
	}
	context, err := handler.requestContext(authentication, channel, receivedAt)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	admission, err := hecadapter.JSON(context, envelopes)
	if err != nil {
		handler.metrics.observeDecodeFailure()
		handler.writeError(response, err)
		return
	}
	handler.stage(response, request, admission, authentication.HECProfile.IndexerAcknowledgment)
}

func (handler *Handler) serveRaw(
	response http.ResponseWriter,
	request *http.Request,
	authentication auth.Authentication,
	channel hec.Channel,
	encoding string,
	query hec.RawQuery,
	receivedAt time.Time,
) {
	body, err := hec.NewBodyReader(request.Body, encoding, handler.limits)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	defer body.Close()
	decoder, err := hec.NewRawDecoder(body, handler.limits)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	lines := make([][]byte, 0, 1)
	for {
		line, decodeErr := decoder.Next()
		if errors.Is(decodeErr, io.EOF) {
			break
		}
		if decodeErr != nil {
			handler.metrics.observeDecodeFailure()
			handler.writeError(response, decodeErr)
			return
		}
		lines = append(lines, line)
	}
	context, err := handler.requestContext(authentication, channel, receivedAt)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	admission, err := hecadapter.Raw(context, query, lines)
	if err != nil {
		handler.metrics.observeDecodeFailure()
		handler.writeError(response, err)
		return
	}
	handler.stage(response, request, admission, authentication.HECProfile.IndexerAcknowledgment)
}

func (handler *Handler) stage(
	response http.ResponseWriter,
	request *http.Request,
	admission ingest.AdmissionRequest,
	acknowledgment bool,
) {
	started := time.Now()
	result, err := handler.admission.Stage(request.Context(), admission)
	handler.metrics.observeStagingLatency(time.Since(started))
	if err != nil {
		handler.metrics.observeStagingFailure()
		if _, ok := errors.AsType[*ingest.AdmissionFailure](err); ok {
			handler.metrics.observeEventPolicyFailure()
		}
		var transient *ingest.TransientStoreError
		if errors.As(err, &transient) &&
			transient.Reason == opensplunk.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED {
			handler.metrics.observeRateLimitedRequest()
		}
		status, retryAfter, failure := mapStageError(err)
		handler.writeErrorWithStatus(response, failure, status, retryAfter)
		return
	}
	if result.HECRequestSequence == 0 ||
		result.State != ingest.StoredBatchPending && result.State != ingest.StoredBatchCommitted {
		handler.writeError(response, hec.NewProtocolError(hec.ErrorInternal, nil))
		return
	}
	public := hec.NewResponse(hec.ResultSuccess)
	if acknowledgment {
		if result.HECAcknowledgmentID == 0 ||
			result.HECAcknowledgmentID > uint64(hec.MaximumEmittedAcknowledgmentID) {
			handler.writeError(response, hec.NewProtocolError(hec.ErrorInternal, nil))
			return
		}
		value := int64(result.HECAcknowledgmentID)
		public.AckID = &value
	}
	handler.metrics.observeAccepted(uint64(result.AcceptedEvents), result.UncompressedBytes)
	handler.writeResponse(response, public, 0, 0)
}

func (handler *Handler) serveAcknowledgment(
	response http.ResponseWriter,
	request *http.Request,
	authentication auth.Authentication,
	channel hec.Channel,
	encoding string,
) {
	body, err := hec.NewBodyReader(request.Body, encoding, handler.limits)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	defer body.Close()
	decoded, err := hec.DecodeAcknowledgmentRequest(body, handler.limits)
	if err != nil {
		handler.metrics.observeDecodeFailure()
		handler.writeError(response, err)
		return
	}
	ids := make([]uint64, len(decoded.IDs))
	for index, id := range decoded.IDs {
		ids[index] = safecast.MustConv[uint64](id)
	}
	statuses, err := handler.acknowledgments.LookupHECAcknowledgments(
		request.Context(),
		handler.tenantID,
		authentication.TokenID,
		string(channel),
		ids,
	)
	if err != nil {
		status, retryAfter, failure := mapStageError(err)
		handler.writeErrorWithStatus(response, failure, status, retryAfter)
		return
	}
	results := make([]hec.AcknowledgmentResult, len(decoded.IDs))
	var misses uint64
	for index, id := range decoded.IDs {
		indexed := statuses[ids[index]]
		results[index] = hec.AcknowledgmentResult{ID: id, Indexed: indexed}
		if !indexed {
			misses++
		}
	}
	encoded, err := hec.MarshalAcknowledgments(results, handler.limits)
	if err != nil {
		handler.writeError(response, hec.NewProtocolError(hec.ErrorInternal, err))
		return
	}
	handler.metrics.observeAcknowledgmentQuery(uint64(len(results)), misses)
	writeJSON(response, http.StatusOK, encoded, 0)
}

func mapStageError(err error) (int, time.Duration, error) {
	if err == nil {
		return 0, 0, hec.NewProtocolError(hec.ErrorInternal, nil)
	}
	if admissionFailure, ok := errors.AsType[*ingest.AdmissionFailure](err); ok {
		kind := hec.ErrorInvalidDataFormat
		if admissionFailure.Failure != nil {
			switch admissionFailure.Failure.Code {
			case opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_INDEX,
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX:
				kind = hec.ErrorIncorrectIndex
			case opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE:
				kind = hec.ErrorEventTooLarge
			default:
				for _, violation := range admissionFailure.Failure.Violations {
					if violation != nil && (violation.GetFieldPath() == "fields" ||
						strings.HasPrefix(violation.GetFieldPath(), "fields.")) {
						kind = hec.ErrorIndexedFields
						break
					}
				}
			}
		}
		return 0, 0, hec.NewEventError(kind, int(admissionFailure.EventIndex), err)
	}
	if errors.Is(err, visibility.ErrHECAcknowledgmentCapacity) {
		return 0, 0, hec.NewProtocolError(hec.ErrorAcknowledgmentCapacity, err)
	}
	if errors.Is(err, visibility.ErrPendingCapacity) || errors.Is(err, visibility.ErrHECRequestCapacity) ||
		errors.Is(err, visibility.ErrExhausted) {
		return 0, 0, hec.NewProtocolError(hec.ErrorQueueCapacity, err)
	}
	if errors.Is(err, visibility.ErrHECAdmissionStale) {
		return 0, 0, hec.NewProtocolError(hec.ErrorInvalidToken, err)
	}
	if errors.Is(err, ingest.ErrAdmissionRequestTooLarge) {
		return 0, 0, hec.NewProtocolError(hec.ErrorNormalizedBodyTooLarge, err)
	}
	if transient, ok := errors.AsType[*ingest.TransientStoreError](err); ok {
		if transient.Reason == opensplunk.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED {
			return http.StatusTooManyRequests, transient.RetryAfter, hec.NewProtocolError(hec.ErrorServerBusy, err)
		}
		return 0, transient.RetryAfter, hec.NewProtocolError(hec.ErrorServerBusy, err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, visibility.ErrClosed) {
		return 0, 0, hec.NewProtocolError(hec.ErrorServerBusy, err)
	}
	return 0, 0, hec.NewProtocolError(hec.ErrorInternal, err)
}

func (handler *Handler) writeError(response http.ResponseWriter, err error) {
	handler.writeErrorWithStatus(response, err, 0, 0)
}

func (handler *Handler) writeErrorWithStatus(
	response http.ResponseWriter,
	err error,
	status int,
	retryAfter time.Duration,
) {
	var failure *hec.ProtocolError
	if !errors.As(err, &failure) || failure == nil {
		failure = hec.NewProtocolError(hec.ErrorInternal, err)
	}
	if status == 0 {
		status = failure.HTTPStatus()
	}
	handler.writeResponse(response, failure.Response(), status, retryAfter)
}

func (handler *Handler) writeResponse(
	response http.ResponseWriter,
	public hec.Response,
	status int,
	retryAfter time.Duration,
) {
	encoded, err := hec.MarshalResponse(public, handler.limits.MaximumResponseBytes)
	if err != nil {
		public = hec.NewResponse(hec.ResultInternalServerError)
		encoded, _ = hec.MarshalResponse(public, hec.HardMaximumResponseBytes)
		status = http.StatusInternalServerError
	}
	if status == 0 {
		status = public.HTTPStatus()
	}
	handler.metrics.observeFailure(public.Code)
	writeJSON(response, status, encoded, retryAfter)
}

func writeJSON(response http.ResponseWriter, status int, body []byte, retryAfter time.Duration) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Pragma", "no-cache")
	if retryAfter > 0 {
		seconds := min(max(int64(math.Ceil(retryAfter.Seconds())), 1), maximumRetryAfterSeconds)
		response.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	}
	response.WriteHeader(status)
	var encoded bytes.Buffer
	if err := json.NewEncoder(&encoded).Encode(json.RawMessage(body)); err != nil {
		return
	}
	_, _ = io.Copy(response, bytes.NewReader(bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})))
}

type endpointQuery struct {
	raw                       hec.RawQuery
	channel                   []string
	acknowledgmentHealthCheck bool
}

func (query endpointQuery) channelValues() []string {
	if query.raw.ChannelPresent {
		return query.raw.ChannelValues()
	}
	return query.channel
}

func parseEndpointQuery(raw string, endpoint hec.Endpoint, limits hec.Limits) (endpointQuery, error) {
	if endpoint == hec.EndpointRaw {
		query, err := hec.ParseRawQuery(raw, limits)
		return endpointQuery{raw: query}, err
	}
	if len(raw) > limits.MaximumRequestTargetBytes {
		return endpointQuery{}, hec.NewProtocolError(hec.ErrorInvalidDataFormat, nil)
	}
	if raw != "" && (raw[0] == '&' || raw[len(raw)-1] == '&' || strings.Contains(raw, "&&")) {
		return endpointQuery{}, hec.NewProtocolError(hec.ErrorInvalidDataFormat, nil)
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return endpointQuery{}, hec.NewProtocolError(hec.ErrorInvalidDataFormat, err)
	}
	if _, exists := values["token"]; exists {
		return endpointQuery{}, hec.NewProtocolError(hec.ErrorQueryAuthorizationDisabled, nil)
	}
	if endpoint == hec.EndpointHealth {
		if len(values) == 0 {
			return endpointQuery{}, nil
		}
		items, exists := values["ack"]
		if !exists || len(values) != 1 || len(items) != 1 ||
			(items[0] != "true" && items[0] != "1") {
			return endpointQuery{}, hec.NewProtocolError(hec.ErrorInvalidDataFormat, nil)
		}
		return endpointQuery{acknowledgmentHealthCheck: true}, nil
	}
	for name, items := range values {
		if name != "channel" || name == "" {
			return endpointQuery{}, hec.NewProtocolError(hec.ErrorInvalidDataFormat, nil)
		}
		if len(items) != 1 {
			return endpointQuery{}, hec.NewProtocolError(hec.ErrorChannelInvalid, nil)
		}
	}
	return endpointQuery{channel: values["channel"]}, nil
}

func queryAuthorizationPresent(raw string) bool {
	for segment := range strings.SplitSeq(raw, "&") {
		name := segment
		if separator := strings.IndexByte(name, '='); separator >= 0 {
			name = name[:separator]
		}
		decoded, err := url.QueryUnescape(name)
		if err == nil && decoded == "token" {
			return true
		}
	}
	return false
}

type tokenSemaphores struct {
	mu     sync.Mutex
	active map[string]int
}

func (semaphores *tokenSemaphores) acquire(tokenID string, limit int) bool {
	semaphores.mu.Lock()
	defer semaphores.mu.Unlock()
	if semaphores.active == nil {
		semaphores.active = make(map[string]int)
	}
	if semaphores.active[tokenID] >= limit {
		return false
	}
	semaphores.active[tokenID]++
	return true
}

func (semaphores *tokenSemaphores) release(tokenID string) {
	semaphores.mu.Lock()
	defer semaphores.mu.Unlock()
	count := semaphores.active[tokenID]
	if count <= 1 {
		delete(semaphores.active, tokenID)
		return
	}
	semaphores.active[tokenID] = count - 1
}

type lifecycleGate struct {
	mu        sync.Mutex
	accepting bool
	nextID    uint64
	active    map[uint64]context.CancelFunc
	drained   chan struct{}
}

func (gate *lifecycleGate) initialize() {
	gate.accepting = true
	gate.active = make(map[uint64]context.CancelFunc)
	gate.drained = make(chan struct{})
}

func (gate *lifecycleGate) begin(parent context.Context) (context.Context, uint64, bool) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if !gate.accepting {
		return nil, 0, false
	}
	gate.nextID++
	if gate.nextID == 0 {
		return nil, 0, false
	}
	requestContext, cancel := context.WithCancel(parent)
	gate.active[gate.nextID] = cancel
	return requestContext, gate.nextID, true
}

func (gate *lifecycleGate) end(id uint64) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if cancel, exists := gate.active[id]; exists {
		cancel()
		delete(gate.active, id)
	}
	if !gate.accepting && len(gate.active) == 0 && gate.drained != nil {
		close(gate.drained)
		gate.drained = nil
	}
}

func (gate *lifecycleGate) close() {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if !gate.accepting {
		return
	}
	gate.accepting = false
	if len(gate.active) == 0 && gate.drained != nil {
		close(gate.drained)
		gate.drained = nil
	}
}

func (gate *lifecycleGate) cancelActive() {
	gate.mu.Lock()
	cancellations := make([]context.CancelFunc, 0, len(gate.active))
	for _, cancel := range gate.active {
		cancellations = append(cancellations, cancel)
	}
	gate.mu.Unlock()
	for _, cancel := range cancellations {
		cancel()
	}
}

func (gate *lifecycleGate) wait(ctx context.Context) error {
	gate.mu.Lock()
	if len(gate.active) == 0 {
		gate.mu.Unlock()
		return nil
	}
	drained := gate.drained
	gate.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
