// Package loggen produces deterministic log fixtures for correctness and load
// testing. The output intentionally covers structured, unstructured, nested,
// null, array, and high-cardinality values used by the ingestion test suite.
package loggen

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// Format selects the shape of each generated line.
type Format string

const (
	FormatZapJSON         Format = "zap-json"
	FormatNestedJSON      Format = "nested-json"
	FormatRaw             Format = "raw"
	FormatCardinalityJSON Format = "cardinality-json"
	FormatMixed           Format = "mixed"
)

// Config controls deterministic fixture generation.
type Config struct {
	Format      Format
	Seed        int64
	Start       time.Time
	Interval    time.Duration
	Service     string
	Environment string
	Host        string
	Cardinality uint64
}

// DefaultConfig returns reproducible defaults suitable for local fixtures.
func DefaultConfig() Config {
	return Config{
		Format:      FormatMixed,
		Seed:        1,
		Start:       time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		Interval:    100 * time.Millisecond,
		Service:     "open-splunk-fixture",
		Environment: "test",
		Host:        "fixture-host",
		Cardinality: 100_000,
	}
}

// Generator creates one newline-free event at a time. A generator is not safe
// for concurrent use; independent generators with equal configuration emit
// byte-identical sequences.
type Generator struct {
	cfg     Config
	ordinal uint64
}

// New validates cfg and creates a deterministic generator.
func New(cfg Config) (*Generator, error) {
	switch cfg.Format {
	case FormatZapJSON, FormatNestedJSON, FormatRaw, FormatCardinalityJSON, FormatMixed:
	default:
		return nil, fmt.Errorf("unsupported log format %q", cfg.Format)
	}
	if cfg.Start.IsZero() {
		return nil, errors.New("start time is required")
	}
	if cfg.Interval < 0 {
		return nil, errors.New("event interval cannot be negative")
	}
	if cfg.Cardinality == 0 {
		return nil, errors.New("cardinality must be greater than zero")
	}
	for name, value := range map[string]string{
		"service": cfg.Service, "environment": cfg.Environment, "host": cfg.Host,
	} {
		if value == "" {
			return nil, fmt.Errorf("%s is required", name)
		}
		if strings.ContainsAny(value, "\r\n") {
			return nil, fmt.Errorf("%s cannot contain a newline", name)
		}
	}
	cfg.Start = cfg.Start.UTC()
	return &Generator{cfg: cfg}, nil
}

// Next returns the next event without a trailing newline.
func (g *Generator) Next() ([]byte, error) {
	ordinal := g.ordinal
	format := g.cfg.Format
	if format == FormatMixed {
		formats := [...]Format{FormatZapJSON, FormatNestedJSON, FormatRaw, FormatCardinalityJSON}
		format = formats[ordinal%uint64(len(formats))]
	}

	shape := g.shape(ordinal)
	var (
		line []byte
		err  error
	)
	switch format {
	case FormatZapJSON:
		line, err = json.Marshal(g.zapEvent(shape))
	case FormatNestedJSON:
		line, err = json.Marshal(g.nestedEvent(shape))
	case FormatRaw:
		line = g.rawEvent(shape)
	case FormatCardinalityJSON:
		line, err = json.Marshal(g.cardinalityEvent(shape))
	default:
		panic("validated format became invalid")
	}
	if err != nil {
		return nil, fmt.Errorf("encode generated event %d: %w", ordinal, err)
	}
	g.ordinal++
	return line, nil
}

// Generate writes exactly count newline-delimited events unless ctx is
// canceled or the writer fails.
func Generate(ctx context.Context, w io.Writer, cfg Config, count uint64) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if w == nil {
		return errors.New("writer is required")
	}
	generator, err := New(cfg)
	if err != nil {
		return err
	}
	for i := uint64(0); i < count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := generator.Next()
		if err != nil {
			return err
		}
		line = append(line, '\n')
		n, err := w.Write(line)
		if err != nil {
			return fmt.Errorf("write event %d: %w", i, err)
		}
		if n != len(line) {
			return fmt.Errorf("write event %d: %w", i, io.ErrShortWrite)
		}
	}
	return nil
}

// DetectFormat classifies output produced by Generator. It is useful in
// fixture checks and deliberately returns the empty string for unknown input.
func DetectFormat(line []byte) Format {
	if !json.Valid(line) {
		if bytesContainWord(line, "service=") && bytesContainWord(line, "request_id=") {
			return FormatRaw
		}
		return ""
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(line, &keys); err != nil {
		return ""
	}
	if _, ok := keys["session_id"]; ok {
		return FormatCardinalityJSON
	}
	if _, ok := keys["http"]; ok {
		return FormatNestedJSON
	}
	if _, ok := keys["logger"]; ok {
		return FormatZapJSON
	}
	return ""
}

type eventShape struct {
	ordinal     uint64
	timestamp   time.Time
	level       string
	message     string
	method      string
	path        string
	status      int
	duration    time.Duration
	bytes       uint64
	ip          string
	traceID     string
	spanID      string
	requestID   string
	sessionID   string
	userID      string
	dimensionID string
}

func (g *Generator) shape(ordinal uint64) eventShape {
	paths := [...]string{"/api/v1/search/jobs/create", "/api/v1/search/jobs/get", "/api/v1/search/jobs/results", "/healthz", "/api/v1/indexes/list"}
	methods := [...]string{"POST", "POST", "POST", "GET", "POST"}
	pathIndex := mix(uint64(g.cfg.Seed), ordinal, 1) % uint64(len(paths))
	status := 200
	level := "INFO"
	message := "request completed"
	switch {
	case ordinal%29 == 0:
		status, level, message = 500, "ERROR", "request failed"
	case ordinal%11 == 0:
		status, level, message = 429, "WARN", "request throttled"
	case ordinal%5 == 0:
		level = "DEBUG"
	}
	durationMicros := 100 + mix(uint64(g.cfg.Seed), ordinal, 2)%500_000
	if status >= 500 {
		durationMicros += 500_000
	}

	return eventShape{
		ordinal:     ordinal,
		timestamp:   g.cfg.Start.Add(time.Duration(ordinal) * g.cfg.Interval),
		level:       level,
		message:     message,
		method:      methods[pathIndex],
		path:        paths[pathIndex],
		status:      status,
		duration:    time.Duration(durationMicros) * time.Microsecond,
		bytes:       20 + mix(uint64(g.cfg.Seed), ordinal, 3)%32_768,
		ip:          fmt.Sprintf("192.0.2.%d", 1+mix(uint64(g.cfg.Seed), ordinal, 4)%254),
		traceID:     deterministicID(g.cfg.Seed, ordinal, 1, 16),
		spanID:      deterministicID(g.cfg.Seed, ordinal, 2, 8),
		requestID:   deterministicID(g.cfg.Seed, ordinal, 3, 16),
		sessionID:   deterministicID(g.cfg.Seed, ordinal, 4, 16),
		userID:      fmt.Sprintf("user-%d", mix(uint64(g.cfg.Seed), ordinal, 5)%g.cfg.Cardinality),
		dimensionID: fmt.Sprintf("dimension-%016x", mix(uint64(g.cfg.Seed), ordinal, 6)),
	}
}

type zapFixture struct {
	Level       string  `json:"level"`
	Timestamp   string  `json:"timestamp"`
	Logger      string  `json:"logger"`
	Caller      string  `json:"caller"`
	Message     string  `json:"message"`
	Layer       string  `json:"layer"`
	Service     string  `json:"service"`
	Environment string  `json:"environment"`
	Host        string  `json:"host"`
	Method      string  `json:"method"`
	Path        string  `json:"path"`
	Status      int     `json:"status"`
	Duration    string  `json:"duration"`
	DurationMS  float64 `json:"duration_ms"`
	Bytes       uint64  `json:"bytes"`
	IP          string  `json:"ip"`
	TraceID     string  `json:"trace_id"`
	SpanID      string  `json:"span_id"`
	RequestID   string  `json:"request_id"`
}

func (g *Generator) zapEvent(shape eventShape) zapFixture {
	return zapFixture{
		Level:       shape.level,
		Timestamp:   shape.timestamp.Format(time.RFC3339Nano),
		Logger:      "api_handler.SRouter",
		Caller:      "router/router.go:735",
		Message:     shape.message,
		Layer:       "api",
		Service:     g.cfg.Service,
		Environment: g.cfg.Environment,
		Host:        g.cfg.Host,
		Method:      shape.method,
		Path:        shape.path,
		Status:      shape.status,
		Duration:    shape.duration.String(),
		DurationMS:  float64(shape.duration) / float64(time.Millisecond),
		Bytes:       shape.bytes,
		IP:          shape.ip,
		TraceID:     shape.traceID,
		SpanID:      shape.spanID,
		RequestID:   shape.requestID,
	}
}

type nestedFixture struct {
	Timestamp string         `json:"timestamp"`
	Severity  string         `json:"severity_text"`
	Body      string         `json:"body"`
	Resource  nestedResource `json:"resource"`
	HTTP      nestedHTTP     `json:"http"`
	Trace     nestedTrace    `json:"trace"`
	Actor     *nestedActor   `json:"actor"`
	Error     *nestedError   `json:"error"`
	Tags      []string       `json:"tags"`
}

type nestedResource struct {
	Service     string `json:"service"`
	Environment string `json:"environment"`
	Host        string `json:"host"`
}

type nestedHTTP struct {
	Request  nestedRequest  `json:"request"`
	Response nestedResponse `json:"response"`
	Duration float64        `json:"duration_ms"`
}

type nestedRequest struct {
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers"`
}

type nestedResponse struct {
	StatusCode int    `json:"status_code"`
	Bytes      uint64 `json:"bytes"`
}

type nestedTrace struct {
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
}

type nestedActor struct {
	UserID string   `json:"user_id"`
	Roles  []string `json:"roles"`
}

type nestedError struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

func (g *Generator) nestedEvent(shape eventShape) nestedFixture {
	var failure *nestedError
	if shape.status >= 500 {
		failure = &nestedError{Kind: "internal", Message: "fixture backend failure"}
	}
	return nestedFixture{
		Timestamp: shape.timestamp.Format(time.RFC3339Nano),
		Severity:  shape.level,
		Body:      shape.message,
		Resource: nestedResource{
			Service: g.cfg.Service, Environment: g.cfg.Environment, Host: g.cfg.Host,
		},
		HTTP: nestedHTTP{
			Request: nestedRequest{
				Method: shape.method, Path: shape.path,
				Headers: map[string][]string{"accept": {"application/x-protobuf"}, "x-fixture": {"open-splunk"}},
			},
			Response: nestedResponse{StatusCode: shape.status, Bytes: shape.bytes},
			Duration: float64(shape.duration) / float64(time.Millisecond),
		},
		Trace: nestedTrace{TraceID: shape.traceID, SpanID: shape.spanID},
		Actor: &nestedActor{UserID: shape.userID, Roles: []string{"developer", "fixture"}},
		Error: failure,
		Tags:  []string{"generated", "nested", g.cfg.Environment},
	}
}

type cardinalityFixture struct {
	Timestamp   string            `json:"timestamp"`
	Message     string            `json:"message"`
	Service     string            `json:"service"`
	Environment string            `json:"environment"`
	Host        string            `json:"host"`
	RequestID   string            `json:"request_id"`
	SessionID   string            `json:"session_id"`
	UserID      string            `json:"user_id"`
	DimensionID string            `json:"dimension_id"`
	Ordinal     uint64            `json:"ordinal"`
	Attributes  map[string]string `json:"attributes"`
}

func (g *Generator) cardinalityEvent(shape eventShape) cardinalityFixture {
	return cardinalityFixture{
		Timestamp:   shape.timestamp.Format(time.RFC3339Nano),
		Message:     shape.message,
		Service:     g.cfg.Service,
		Environment: g.cfg.Environment,
		Host:        g.cfg.Host,
		RequestID:   shape.requestID,
		SessionID:   shape.sessionID,
		UserID:      shape.userID,
		DimensionID: shape.dimensionID,
		Ordinal:     shape.ordinal,
		Attributes: map[string]string{
			"route": shape.path,
			"trace": shape.traceID,
		},
	}
}

func (g *Generator) rawEvent(shape eventShape) []byte {
	return fmt.Appendf(nil,
		"%s %s service=%s environment=%s host=%s method=%s path=%s status=%d duration=%s request_id=%s trace_id=%s message=%s",
		shape.timestamp.Format(time.RFC3339Nano), shape.level, g.cfg.Service, g.cfg.Environment,
		g.cfg.Host, shape.method, shape.path, shape.status, shape.duration,
		shape.requestID, shape.traceID, strconv.Quote(shape.message),
	)
}

func deterministicID(seed int64, ordinal, stream uint64, size int) string {
	var input [24]byte
	binary.LittleEndian.PutUint64(input[0:8], uint64(seed))
	binary.LittleEndian.PutUint64(input[8:16], ordinal)
	binary.LittleEndian.PutUint64(input[16:24], stream)
	digest := sha256.Sum256(input[:])
	return fmt.Sprintf("%x", digest[:size])
}

func mix(seed, ordinal, stream uint64) uint64 {
	z := seed + 0x9e3779b97f4a7c15*(ordinal+1) + 0xbf58476d1ce4e5b9*(stream+1)
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func bytesContainWord(line []byte, word string) bool {
	return strings.Contains(string(line), word)
}
