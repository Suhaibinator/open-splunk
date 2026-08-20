package collector

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"time"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collectorlimits"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/jsonnumber"
	"github.com/Suhaibinator/open-splunk/internal/sha256hex"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultMaxLineBytes  = 1 << 20
	defaultMaxJSONDepth  = 32
	defaultMaxJSONFields = 1_024
)

// InputFormat describes the framing payload decoded by a file input.
type InputFormat string

const (
	InputFormatNDJSON InputFormat = "ndjson"
	InputFormatRaw    InputFormat = "raw"
)

// DecodeConfig contains trusted metadata attached to every event from an
// input. Payload fields cannot override these values.
type DecodeConfig struct {
	Format         InputFormat
	InputID        string
	IndexName      string
	Source         string
	Sourcetype     string
	Host           string
	Service        string
	ConstantFields *opensplunk.TypedObject
	MaxLineBytes   int
	MaxJSONDepth   int
	MaxJSONFields  int
}

// SourcePosition is the durable origin of one framed event. Both line fields
// are zero when a legacy checkpoint cannot supply an exact physical cursor.
type SourcePosition struct {
	FileIdentity               string
	SourcePath                 string
	FileFingerprintLength      uint32
	CheckpointGuardFingerprint string
	CheckpointGuardLength      uint32
	StartOffset                uint64
	EndOffset                  uint64
	LineNumber                 uint64
	NextLineNumber             uint64
}

// Decoder converts framed source bytes to canonical protobuf events.
// Decoder is immutable and safe for concurrent use.
type Decoder struct {
	cfg           DecodeConfig
	constants     []*opensplunk.TypedObjectField
	constantNames map[string]struct{}
}

// NewDecoder validates and takes an independent copy of cfg.
func NewDecoder(cfg DecodeConfig) (*Decoder, error) {
	switch cfg.Format {
	case InputFormatNDJSON, InputFormatRaw:
	default:
		return nil, fmt.Errorf("unsupported input format %q", cfg.Format)
	}
	for name, value := range map[string]string{
		"input ID": cfg.InputID, "index name": cfg.IndexName, "source": cfg.Source,
		"sourcetype": cfg.Sourcetype, "host": cfg.Host,
	} {
		if value == "" {
			return nil, fmt.Errorf("%s is required", name)
		}
		if !utf8.ValidString(value) || strings.ContainsAny(value, "\r\n") {
			return nil, fmt.Errorf("%s contains invalid text", name)
		}
	}
	if !utf8.ValidString(cfg.Service) || strings.ContainsAny(cfg.Service, "\r\n") {
		return nil, errors.New("service contains invalid text")
	}
	if cfg.MaxLineBytes == 0 {
		cfg.MaxLineBytes = defaultMaxLineBytes
	}
	if cfg.MaxJSONDepth == 0 {
		cfg.MaxJSONDepth = defaultMaxJSONDepth
	}
	if cfg.MaxJSONFields == 0 {
		cfg.MaxJSONFields = defaultMaxJSONFields
	}
	if cfg.MaxLineBytes < 1 || cfg.MaxJSONDepth < 1 || cfg.MaxJSONFields < 1 {
		return nil, errors.New("decoder limits must be positive")
	}

	constants, err := cloneAndValidateConstants(cfg.ConstantFields)
	if err != nil {
		return nil, fmt.Errorf("constant fields: %w", err)
	}
	constantNames := make(map[string]struct{}, len(constants))
	for _, field := range constants {
		constantNames[field.GetName()] = struct{}{}
	}
	cfg.ConstantFields = nil
	return &Decoder{cfg: cfg, constants: constants, constantNames: constantNames}, nil
}

// Decode converts raw to an independent event. raw must not contain the file
// delimiter; it is otherwise preserved byte-for-byte.
func (d *Decoder) Decode(raw []byte, position SourcePosition, collectedAt time.Time) (*opensplunk.LogEvent, error) {
	if len(raw) > d.cfg.MaxLineBytes {
		return nil, fmt.Errorf("event has %d bytes, limit is %d", len(raw), d.cfg.MaxLineBytes)
	}
	if position.EndOffset < position.StartOffset {
		return nil, errors.New("source end offset precedes start offset")
	}
	if (position.CheckpointGuardFingerprint == "") != (position.CheckpointGuardLength == 0) {
		return nil, errors.New("source checkpoint guard fingerprint and length must be present together")
	}
	if position.CheckpointGuardLength > 0 {
		if position.CheckpointGuardLength > collectorlimits.MaximumCheckpointGuardBytes {
			return nil, errors.New("source checkpoint guard exceeds the absolute length limit")
		}
		if uint64(position.CheckpointGuardLength) > position.EndOffset {
			return nil, errors.New("source checkpoint guard extends before offset zero")
		}
		if !sha256hex.Valid(position.CheckpointGuardFingerprint) {
			return nil, errors.New("source checkpoint guard fingerprint is not canonical SHA-256 hex")
		}
	}
	if collectedAt.IsZero() {
		return nil, errors.New("collection time is required")
	}
	collectedAt = collectedAt.UTC()
	collectedTimestamp := timestamppb.New(collectedAt)
	if err := collectedTimestamp.CheckValid(); err != nil {
		return nil, fmt.Errorf("invalid collection time: %w", err)
	}

	event := &opensplunk.LogEvent{
		EventId:         stableEventID(d.cfg.InputID, position, raw),
		IndexName:       d.cfg.IndexName,
		CollectedAt:     collectedTimestamp,
		EventTime:       timestamppb.New(collectedAt),
		EventTimeSource: opensplunk.EventTimeSource_EVENT_TIME_SOURCE_COLLECTED_AT_FALLBACK,
		Host:            d.cfg.Host,
		Source:          d.cfg.Source,
		Sourcetype:      d.cfg.Sourcetype,
		Raw:             bytes.Clone(raw),
		RawEncoding:     opensplunk.RawEncoding_RAW_ENCODING_BINARY,
		Fields:          &opensplunk.TypedObject{},
		Origin:          sourceOrigin(d.cfg.InputID, position),
	}
	if d.cfg.Service != "" {
		event.Service = new(d.cfg.Service)
	}
	if utf8.Valid(raw) {
		event.RawEncoding = opensplunk.RawEncoding_RAW_ENCODING_UTF8
	}

	if d.cfg.Format == InputFormatRaw {
		if event.RawEncoding == opensplunk.RawEncoding_RAW_ENCODING_UTF8 {
			event.Message = new(string(raw))
		}
		event.Fields = d.mergeConstants(nil)
		return event, nil
	}

	parsed, err := parseJSONObject(raw, d.cfg.MaxJSONDepth, d.cfg.MaxJSONFields)
	if err != nil {
		return nil, fmt.Errorf("decode NDJSON event: %w", err)
	}
	if err := d.extractCanonical(event, parsed, collectedAt); err != nil {
		return nil, err
	}
	dynamic, err := dynamicFields(parsed)
	if err != nil {
		return nil, fmt.Errorf("convert JSON fields: %w", err)
	}
	event.Fields = d.mergeConstants(dynamic)
	return event, nil
}

func (d *Decoder) extractCanonical(event *opensplunk.LogEvent, object jsonObject, fallback time.Time) error {
	if value, found, err := oneCanonical(object, "timestamp", "ts", "time", "@timestamp"); err != nil {
		return err
	} else if found && value != nil {
		parsed, err := parseEventTime(value)
		if err != nil {
			return fmt.Errorf("invalid event timestamp: %w", err)
		}
		event.EventTime = timestamppb.New(parsed)
		if err := event.EventTime.CheckValid(); err != nil {
			return fmt.Errorf("invalid event timestamp: %w", err)
		}
		event.EventTimeSource = opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED
	} else {
		event.EventTime = timestamppb.New(fallback)
	}

	if value, found, err := oneCanonical(object, "level", "severity", "severity_text"); err != nil {
		return err
	} else if found && value != nil {
		level, ok := value.(string)
		if !ok {
			return errors.New("level must be a string or null")
		}
		event.Level = new(level)
		event.Severity = severityForLevel(level)
	}
	if value, found, err := oneCanonical(object, "message", "msg", "body"); err != nil {
		return err
	} else if found && value != nil {
		message, ok := value.(string)
		if !ok {
			return errors.New("message must be a string or null")
		}
		event.Message = new(message)
	}
	if value, found, err := oneCanonical(object, "trace_id", "traceId"); err != nil {
		return err
	} else if found && value != nil {
		traceID, ok := value.(string)
		if !ok {
			return errors.New("trace_id must be a string or null")
		}
		event.TraceId = new(traceID)
	}
	if value, found, err := oneCanonical(object, "span_id", "spanId"); err != nil {
		return err
	} else if found && value != nil {
		spanID, ok := value.(string)
		if !ok {
			return errors.New("span_id must be a string or null")
		}
		event.SpanId = new(spanID)
	}
	return nil
}

func (d *Decoder) mergeConstants(dynamic []*opensplunk.TypedObjectField) *opensplunk.TypedObject {
	fields := make([]*opensplunk.TypedObjectField, 0, len(dynamic)+len(d.constants))
	positions := make(map[string]int, len(dynamic)+len(d.constants))
	for _, field := range dynamic {
		positions[field.GetName()] = len(fields)
		fields = append(fields, field)
	}
	for _, constant := range d.constants {
		cloned := proto.Clone(constant).(*opensplunk.TypedObjectField)
		if index, exists := positions[constant.GetName()]; exists {
			fields[index] = cloned
			continue
		}
		positions[constant.GetName()] = len(fields)
		fields = append(fields, cloned)
	}
	return &opensplunk.TypedObject{Fields: fields}
}

type jsonObject []jsonField

type jsonField struct {
	name  string
	value any
}

type jsonArray []any

type jsonParser struct {
	decoder   *json.Decoder
	maxDepth  int
	maxFields int
	fields    int
}

func parseJSONObject(raw []byte, maxDepth, maxFields int) (jsonObject, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	parser := jsonParser{decoder: decoder, maxDepth: maxDepth, maxFields: maxFields}
	value, err := parser.value(1)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple JSON values are not allowed")
		}
		return nil, fmt.Errorf("read trailing JSON data: %w", err)
	}
	object, ok := value.(jsonObject)
	if !ok {
		return nil, errors.New("top-level JSON value must be an object")
	}
	return object, nil
}

func (p *jsonParser) value(depth int) (any, error) {
	if depth > p.maxDepth {
		return nil, fmt.Errorf("JSON nesting exceeds limit %d", p.maxDepth)
	}
	token, err := p.decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		switch token.(type) {
		case nil, bool, string, json.Number:
			return token, nil
		default:
			return nil, fmt.Errorf("unsupported JSON token %T", token)
		}
	}
	switch delimiter {
	case '{':
		object := make(jsonObject, 0)
		seen := make(map[string]struct{})
		for p.decoder.More() {
			nameToken, err := p.decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := nameToken.(string)
			if !ok {
				return nil, errors.New("JSON object field name is not a string")
			}
			if _, duplicate := seen[name]; duplicate {
				return nil, fmt.Errorf("duplicate JSON field %q", name)
			}
			seen[name] = struct{}{}
			p.fields++
			if p.fields > p.maxFields {
				return nil, fmt.Errorf("JSON fields exceed limit %d", p.maxFields)
			}
			value, err := p.value(depth + 1)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", name, err)
			}
			object = append(object, jsonField{name: name, value: value})
		}
		if token, err := p.decoder.Token(); err != nil || token != json.Delim('}') {
			if err != nil {
				return nil, err
			}
			return nil, errors.New("JSON object was not terminated")
		}
		return object, nil
	case '[':
		array := make(jsonArray, 0)
		for p.decoder.More() {
			value, err := p.value(depth + 1)
			if err != nil {
				return nil, fmt.Errorf("array element %d: %w", len(array), err)
			}
			array = append(array, value)
		}
		if token, err := p.decoder.Token(); err != nil || token != json.Delim(']') {
			if err != nil {
				return nil, err
			}
			return nil, errors.New("JSON array was not terminated")
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func dynamicFields(object jsonObject) ([]*opensplunk.TypedObjectField, error) {
	fields := make([]*opensplunk.TypedObjectField, 0, len(object))
	for _, field := range object {
		if eventfields.IsCollectorReservedRoot(field.name) {
			continue
		}
		value, err := typedJSONValue(field.value)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.name, err)
		}
		fields = append(fields, &opensplunk.TypedObjectField{Name: field.name, Value: value})
	}
	return fields, nil
}

func typedJSONValue(value any) (*opensplunk.TypedValue, error) {
	switch value := value.(type) {
	case nil:
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_NullValue{
			NullValue: opensplunk.NullValue_NULL_VALUE_NULL,
		}}, nil
	case string:
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: value}}, nil
	case bool:
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_BoolValue{BoolValue: value}}, nil
	case json.Number:
		return typedJSONNumber(value)
	case jsonArray:
		values := make([]*opensplunk.TypedValue, 0, len(value))
		for i, item := range value {
			converted, err := typedJSONValue(item)
			if err != nil {
				return nil, fmt.Errorf("array element %d: %w", i, err)
			}
			values = append(values, converted)
		}
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_ListValue{
			ListValue: &opensplunk.TypedValueList{Values: values},
		}}, nil
	case jsonObject:
		fields := make([]*opensplunk.TypedObjectField, 0, len(value))
		for _, field := range value {
			converted, err := typedJSONValue(field.value)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", field.name, err)
			}
			fields = append(fields, &opensplunk.TypedObjectField{Name: field.name, Value: converted})
		}
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_ObjectValue{
			ObjectValue: &opensplunk.TypedObject{Fields: fields},
		}}, nil
	default:
		return nil, fmt.Errorf("unsupported decoded JSON value %T", value)
	}
}

func typedJSONNumber(number json.Number) (*opensplunk.TypedValue, error) {
	classified := jsonnumber.Classify(number.String())
	switch classified.Kind {
	case jsonnumber.KindSint64:
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_Sint64Value{
			Sint64Value: classified.Sint64,
		}}, nil
	case jsonnumber.KindUint64:
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_Uint64Value{
			Uint64Value: classified.Uint64,
		}}, nil
	case jsonnumber.KindFloat64:
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_DoubleValue{
			DoubleValue: classified.Float64,
		}}, nil
	case jsonnumber.KindDecimal:
		return decimalTypedValue(classified.Decimal), nil
	default:
		return nil, fmt.Errorf("classify JSON number: invalid kind %d", classified.Kind)
	}
}

func decimalTypedValue(value string) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_DecimalValue{
		DecimalValue: &opensplunk.DecimalValue{Value: value},
	}}
}

// parseEventTime converts a canonical timestamp value to a time.Time. Every
// error it returns is deliberately VALUE-FREE (no payload bytes): the offending
// timestamp string/number is never embedded, because these errors flow into
// recordDecodeFailure logs and a source field may carry secret material. In
// particular the string branch does not wrap time.Parse's error (which embeds
// the input value), and the numeric branch does not propagate ParseDecimalRat's
// %q-bearing error.
func parseEventTime(value any) (time.Time, error) {
	switch value := value.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return time.Time{}, errors.New("timestamp is not RFC3339")
		}
		return parsed.UTC(), nil
	case json.Number:
		if len(value.String()) > 128 {
			return time.Time{}, errors.New("numeric timestamp is too long")
		}
		rat, err := jsonnumber.ParseDecimalRat(value.String())
		if err != nil {
			return time.Time{}, errors.New("numeric timestamp is invalid")
		}
		nanoseconds := new(big.Rat).Mul(rat, big.NewRat(int64(time.Second), 1))
		if !nanoseconds.IsInt() {
			return time.Time{}, errors.New("numeric timestamp must have nanosecond precision")
		}
		seconds, remainder := new(big.Int), new(big.Int)
		seconds.QuoRem(nanoseconds.Num(), big.NewInt(int64(time.Second)), remainder)
		if !seconds.IsInt64() {
			return time.Time{}, errors.New("numeric timestamp seconds exceed int64")
		}
		return time.Unix(seconds.Int64(), remainder.Int64()).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("timestamp must be an RFC3339 string or Unix-seconds number, got %T", value)
	}
}

func oneCanonical(object jsonObject, names ...string) (any, bool, error) {
	var (
		value any
		found string
	)
	for _, field := range object {
		for _, name := range names {
			if field.name != name {
				continue
			}
			if found != "" {
				return nil, false, fmt.Errorf("ambiguous canonical fields %q and %q", found, name)
			}
			found, value = name, field.value
		}
	}
	return value, found != "", nil
}

func severityForLevel(level string) opensplunk.LogSeverity {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "trace":
		return opensplunk.LogSeverity_LOG_SEVERITY_TRACE
	case "debug":
		return opensplunk.LogSeverity_LOG_SEVERITY_DEBUG
	case "info", "information", "notice":
		return opensplunk.LogSeverity_LOG_SEVERITY_INFO
	case "warn", "warning":
		return opensplunk.LogSeverity_LOG_SEVERITY_WARN
	case "error", "dpanic":
		return opensplunk.LogSeverity_LOG_SEVERITY_ERROR
	case "fatal", "panic", "critical":
		return opensplunk.LogSeverity_LOG_SEVERITY_FATAL
	default:
		return opensplunk.LogSeverity_LOG_SEVERITY_UNSPECIFIED
	}
}

func sourceOrigin(inputID string, position SourcePosition) *opensplunk.EventOrigin {
	origin := &opensplunk.EventOrigin{InputId: inputID}
	if position.FileIdentity != "" {
		origin.FileIdentity = new(position.FileIdentity)
		origin.FileFingerprintLength = new(position.FileFingerprintLength)
	}
	if position.SourcePath != "" {
		// Unix paths are arbitrary byte strings, while protobuf string fields must
		// be valid UTF-8. Keep the raw path inside the input manager for file I/O,
		// but normalize this diagnostic wire copy so one unusual filename cannot
		// make WAL marshaling fail and stop the collector.
		path := sanitizeCollectorBoundaryText(
			position.SourcePath,
			collectorlimits.MaximumSourceBytes,
		)
		origin.SourcePath = new(path)
	}
	origin.StartOffset = new(position.StartOffset)
	origin.EndOffset = new(position.EndOffset)
	if position.LineNumber != 0 {
		origin.LineNumber = new(position.LineNumber)
	}
	if position.NextLineNumber != 0 {
		origin.NextLineNumber = new(position.NextLineNumber)
	}
	if position.CheckpointGuardLength != 0 {
		origin.CheckpointGuardFingerprint = new(position.CheckpointGuardFingerprint)
		origin.CheckpointGuardLength = new(position.CheckpointGuardLength)
	}
	return origin
}

func stableEventID(inputID string, position SourcePosition, raw []byte) string {
	hash := sha256.New()
	writeHashString(hash, inputID)
	writeHashString(hash, position.FileIdentity)
	// LineNumber is deliberately excluded. Framers reconstruct line counts on
	// restart, while byte coordinates and the persisted file-generation identity
	// remain stable. Including it would turn a crash replay into a new event ID.
	var integers [16]byte
	binary.BigEndian.PutUint64(integers[0:8], position.StartOffset)
	binary.BigEndian.PutUint64(integers[8:16], position.EndOffset)
	_, _ = hash.Write(integers[:])
	writeHashBytes(hash, raw)
	return hex.EncodeToString(hash.Sum(nil))
}

type byteWriter interface {
	Write([]byte) (int, error)
}

func writeHashString(hash byteWriter, value string) { writeHashBytes(hash, []byte(value)) }

func writeHashBytes(hash byteWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write(value)
}

func cloneAndValidateConstants(object *opensplunk.TypedObject) ([]*opensplunk.TypedObjectField, error) {
	if object == nil {
		return nil, nil
	}
	result := make([]*opensplunk.TypedObjectField, 0, len(object.GetFields()))
	seen := make(map[string]struct{}, len(object.GetFields()))
	for i, field := range object.GetFields() {
		if field == nil || field.GetName() == "" || field.GetValue() == nil || field.GetValue().GetKind() == nil {
			return nil, fmt.Errorf("field %d must have a name and typed value", i)
		}
		if !utf8.ValidString(field.GetName()) || strings.TrimSpace(field.GetName()) != field.GetName() {
			return nil, fmt.Errorf("field %d has an invalid name", i)
		}
		if eventfields.IsCollectorReservedRoot(field.GetName()) {
			return nil, fmt.Errorf("field %q is reserved canonical metadata", field.GetName())
		}
		if _, duplicate := seen[field.GetName()]; duplicate {
			return nil, fmt.Errorf("duplicate field %q", field.GetName())
		}
		seen[field.GetName()] = struct{}{}
		result = append(result, proto.Clone(field).(*opensplunk.TypedObjectField))
	}
	return result, nil
}
