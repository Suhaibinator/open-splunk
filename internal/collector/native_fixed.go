package collector

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (d *Decoder) decodeDocker(event *opensplunk.LogEvent, raw []byte) error {
	if !utf8.Valid(raw) {
		return errors.New("docker envelope is not UTF-8")
	}
	object, err := parseJSONObject(raw, d.cfg.MaxJSONDepth, d.cfg.MaxJSONFields)
	if err != nil {
		// JSON token errors can contain source values and field names.
		return errors.New("invalid Docker JSON envelope")
	}
	if !nativeJSONSurrogatesValid(raw) {
		return errors.New("docker JSON contains an unpaired Unicode surrogate")
	}
	if !nativeFixedNamesValid(object) {
		return errors.New("invalid Docker field name")
	}
	var message, timestamp, stream string
	var hasMessage, hasTime, hasStream bool
	fields := make([]*opensplunk.TypedObjectField, 0, len(object))
	for _, field := range object {
		switch field.name {
		case "log":
			message, hasMessage = field.value.(string)
		case "time":
			timestamp, hasTime = field.value.(string)
		case "stream":
			stream, hasStream = field.value.(string)
		default:
			if strings.EqualFold(field.name, "docker_stream") {
				return errors.New("docker stream projection collides with an extra field")
			}
			if eventfields.IsCollectorReservedRoot(field.name) {
				continue
			}
			value, err := typedJSONValue(field.value)
			if err != nil {
				return errors.New("invalid Docker extra field value")
			}
			fields = append(fields, &opensplunk.TypedObjectField{Name: field.name, Value: value})
		}
	}
	if !hasMessage || !hasTime || !hasStream || (stream != "stdout" && stream != "stderr") {
		return errors.New("docker envelope requires log, time, and stdout/stderr stream strings")
	}
	parsed, err := nativeFixedDockerTime(timestamp)
	if err != nil {
		return err
	}
	event.Message = &message
	event.EventTime = parsed
	event.EventTimeSource = opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED
	fields = append(fields, &opensplunk.TypedObjectField{Name: "docker_stream", Value: nativeFixedText([]byte(stream))})
	event.Fields = &opensplunk.TypedObject{Fields: fields}
	return nil
}

// encoding/json replaces lone UTF-16 surrogates with U+FFFD. Reject those
// inputs instead of silently changing the producer's message. This scan runs
// only after JSON syntax validation, so every Unicode escape has four hex digits.
func nativeJSONSurrogatesValid(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if raw[i] != 'u' {
			continue
		}
		code := nativeFixedCodeUnit(raw[i+1 : i+5])
		i += 4
		if code >= 0xdc00 && code <= 0xdfff {
			return false
		}
		if code < 0xd800 || code > 0xdbff {
			continue
		}
		if len(raw)-i < 7 || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low := nativeFixedCodeUnit(raw[i+3 : i+7])
		if low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}

func nativeFixedCodeUnit(raw []byte) uint16 {
	var code uint16
	for _, c := range raw {
		nibble, _ := nativeFixedHex(c)
		code = code<<4 | uint16(nibble)
	}
	return code
}

func nativeFixedNamesValid(value any) bool {
	switch value := value.(type) {
	case jsonObject:
		for _, field := range value {
			name := field.name
			if name == "" || len(name) > eventfields.MaximumDynamicPathSegmentBytes || !utf8.ValidString(name) || strings.TrimSpace(name) != name {
				return false
			}
			for _, r := range name {
				if unicode.IsControl(r) {
					return false
				}
			}
			if !nativeFixedNamesValid(field.value) {
				return false
			}
		}
	case jsonArray:
		for _, item := range value {
			if !nativeFixedNamesValid(item) {
				return false
			}
		}
	}
	return true
}

func nativeFixedDockerTime(value string) (*timestamppb.Timestamp, error) {
	// time.Parse accepts commas, single-digit hours, excess fractional digits,
	// and out-of-range offset components. Docker emits strict RFC3339Nano.
	if len(value) < 20 || value[4] != '-' || value[7] != '-' || value[10] != 'T' || value[13] != ':' || value[16] != ':' {
		return nil, errors.New("invalid Docker timestamp")
	}
	end := 19
	if value[end] == '.' {
		end++
		start := end
		for end < len(value) && value[end] >= '0' && value[end] <= '9' {
			end++
		}
		if end == start || end-start > 9 {
			return nil, errors.New("invalid Docker timestamp precision")
		}
	}
	zone := value[end:]
	if zone != "Z" && (len(zone) != 6 || (zone[0] != '+' && zone[0] != '-') || zone[3] != ':' || !nativeFixedOffset(zone[1:3], zone[4:6])) {
		return nil, errors.New("invalid Docker timestamp offset")
	}
	parsed, err := parseEventTime(value)
	if err != nil {
		return nil, errors.New("invalid Docker timestamp")
	}
	result := timestamppb.New(parsed)
	if result.CheckValid() != nil {
		return nil, errors.New("docker timestamp exceeds supported range")
	}
	return result, nil
}

func nativeFixedOffset(hour, minute string) bool {
	return len(hour) == 2 && len(minute) == 2 && hour[0] >= '0' && hour[0] <= '2' && hour[1] >= '0' && hour[1] <= '9' && hour <= "23" && minute[0] >= '0' && minute[0] <= '5' && minute[1] >= '0' && minute[1] <= '9'
}

func (d *Decoder) decodeAccess(event *opensplunk.LogEvent, raw []byte) error {
	scanner := nativeFixedScanner{raw: raw, apache: d.cfg.Format != "nginx-combined"}
	address, ok := scanner.value(false)
	if !ok || !scanner.space() {
		return errors.New("invalid access client address")
	}
	ident, ok := scanner.value(false)
	if !ok || !scanner.space() {
		return errors.New("invalid access ident")
	}
	var user []byte
	if scanner.apache && len(raw)-scanner.pos >= 2 && raw[scanner.pos] == '"' && raw[scanner.pos+1] == '"' {
		// Apache %u represents an authenticated empty user as literal "".
		// Recognize this sentinel only here; other bare fields remain strict.
		user = raw[scanner.pos:scanner.pos]
		scanner.pos += 2
		ok = true
	} else {
		user, ok = scanner.value(false)
	}
	if !ok || !scanner.space() {
		return errors.New("invalid access user")
	}
	// Fixed presets have exactly this 26-byte date, enclosed by brackets.
	if len(raw)-scanner.pos < 28 || raw[scanner.pos] != '[' || raw[scanner.pos+27] != ']' {
		return errors.New("invalid access timestamp envelope")
	}
	stamp := string(raw[scanner.pos+1 : scanner.pos+27])
	if !nativeFixedOffset(stamp[22:24], stamp[24:26]) {
		return errors.New("invalid access timestamp offset")
	}
	parsed, err := time.Parse("02/Jan/2006:15:04:05 -0700", stamp)
	if err != nil || parsed.Format("02/Jan/2006:15:04:05") != stamp[:20] {
		return errors.New("invalid access timestamp")
	}
	eventTime := timestamppb.New(parsed.UTC())
	if eventTime.CheckValid() != nil {
		return errors.New("access timestamp exceeds supported range")
	}
	scanner.pos += 28
	if !scanner.space() {
		return errors.New("missing access request separator")
	}
	request, ok := scanner.value(true)
	if !ok || !scanner.space() {
		return errors.New("invalid access request envelope")
	}
	status, ok := scanner.value(false)
	if !ok || len(status) != 3 || !scanner.space() {
		return errors.New("invalid access status")
	}
	statusValue, ok := nativeFixedUint(status)
	if !ok {
		return errors.New("invalid access status")
	}
	response, ok := scanner.value(false)
	if !ok {
		return errors.New("invalid access response bytes")
	}
	var responseValue *opensplunk.TypedValue
	if bytes.Equal(response, []byte("-")) {
		if scanner.apache {
			responseValue = nativeFixedInteger(0)
		}
	} else {
		var valid bool
		responseValue, valid = nativeFixedUint(response)
		if !valid {
			return errors.New("invalid access response byte count")
		}
	}
	var referrer, agent []byte
	if d.cfg.Format != "apache-common" {
		if !scanner.space() {
			return errors.New("missing access referrer separator")
		}
		referrer, ok = scanner.value(true)
		if !ok || !scanner.space() {
			return errors.New("invalid access referrer")
		}
		agent, ok = scanner.value(true)
		if !ok {
			return errors.New("invalid access user agent")
		}
	}
	if scanner.pos != len(raw) {
		return errors.New("trailing access data")
	}
	fields := make([]*opensplunk.TypedObjectField, 0, 11)
	appendText := func(name string, value []byte, omitDash bool) {
		if omitDash && bytes.Equal(value, []byte("-")) {
			return
		}
		fields = append(fields, &opensplunk.TypedObjectField{Name: name, Value: nativeFixedText(value)})
	}
	appendText("client_address", address, true)
	appendText("ident", ident, true)
	appendText("user", user, true)
	appendText("request", request, false)
	if method, target, protocol, valid := nativeFixedRequest(request); valid {
		appendText("method", method, false)
		appendText("request_target", target, false)
		appendText("protocol", protocol, false)
	}
	fields = append(fields, &opensplunk.TypedObjectField{Name: "status", Value: statusValue})
	if responseValue != nil {
		fields = append(fields, &opensplunk.TypedObjectField{Name: "response_bytes", Value: responseValue})
	}
	if d.cfg.Format != "apache-common" {
		appendText("referrer", referrer, true)
		appendText("user_agent", agent, true)
	}
	if utf8.Valid(request) {
		event.Message = new(string(request))
	}
	event.EventTime = eventTime
	event.EventTimeSource = opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED
	event.Fields = &opensplunk.TypedObject{Fields: fields}
	return nil
}

// The scanner aliases raw only during decoding. Projection always takes an
// independent string or byte copy, so decoders need no shared scratch state.
type nativeFixedScanner struct {
	raw    []byte
	pos    int
	apache bool
}

func (s *nativeFixedScanner) space() bool {
	start := s.pos
	for s.pos < len(s.raw) && (s.raw[s.pos] == ' ' || s.raw[s.pos] == '\t') {
		s.pos++
	}
	return s.pos > start
}

func (s *nativeFixedScanner) value(quoted bool) ([]byte, bool) {
	if quoted {
		if s.pos >= len(s.raw) || s.raw[s.pos] != '"' {
			return nil, false
		}
		s.pos++
	}
	start := s.pos
	var decoded []byte
	for s.pos < len(s.raw) {
		c := s.raw[s.pos]
		if (quoted && c == '"') || (!quoted && (c == ' ' || c == '\t')) {
			value := s.raw[start:s.pos]
			if decoded != nil {
				value = decoded
			}
			if quoted {
				s.pos++
			}
			return value, quoted || s.pos > start
		}
		if c < ' ' || c == 0x7f || (!quoted && c == '"') {
			return nil, false
		}
		if c == '\\' {
			if decoded == nil {
				decoded = make([]byte, 0, s.pos-start+16)
				decoded = append(decoded, s.raw[start:s.pos]...)
			}
			value, ok := s.escape()
			if !ok {
				return nil, false
			}
			decoded = append(decoded, value)
			continue
		}
		if decoded != nil {
			decoded = append(decoded, c)
		}
		s.pos++
	}
	if quoted || s.pos == start {
		return nil, false
	}
	if decoded != nil {
		return decoded, true
	}
	return s.raw[start:s.pos], true
}

func (s *nativeFixedScanner) escape() (byte, bool) {
	if len(s.raw)-s.pos < 2 {
		return 0, false
	}
	c := s.raw[s.pos+1]
	if c == 'x' {
		if len(s.raw)-s.pos < 4 {
			return 0, false
		}
		hi, highOK := nativeFixedHex(s.raw[s.pos+2])
		lo, lowOK := nativeFixedHex(s.raw[s.pos+3])
		s.pos += 4
		return hi<<4 | lo, highOK && lowOK
	}
	if !s.apache {
		return 0, false
	}
	s.pos += 2
	switch c {
	case '"', '\\':
		return c, true
	case 'b':
		return '\b', true
	case 'n':
		return '\n', true
	case 'r':
		return '\r', true
	case 't':
		return '\t', true
	case 'v':
		return '\v', true
	default:
		return 0, false
	}
}

func nativeFixedHex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

func nativeFixedUint(raw []byte) (*opensplunk.TypedValue, bool) {
	if len(raw) == 0 || len(raw) > 20 {
		return nil, false
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return nil, false
		}
	}
	value, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return nil, false
	}
	return nativeFixedInteger(value), true
}

func nativeFixedInteger(value uint64) *opensplunk.TypedValue {
	if value <= math.MaxInt64 {
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_Sint64Value{Sint64Value: int64(value)}}
	}
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_Uint64Value{Uint64Value: value}}
}

func nativeFixedText(value []byte) *opensplunk.TypedValue {
	if utf8.Valid(value) {
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: string(value)}}
	}
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_BytesValue{BytesValue: bytes.Clone(value)}}
}

func nativeFixedRequest(value []byte) (method, target, protocol []byte, ok bool) {
	first := bytes.IndexByte(value, ' ')
	if first < 1 {
		return nil, nil, nil, false
	}
	second := bytes.IndexByte(value[first+1:], ' ')
	if second < 1 {
		return nil, nil, nil, false
	}
	second += first + 1
	method, target, protocol = value[:first], value[first+1:second], value[second+1:]
	if len(protocol) != 8 || !bytes.Equal(protocol[:5], []byte("HTTP/")) || protocol[5] < '0' || protocol[5] > '9' || protocol[6] != '.' || protocol[7] < '0' || protocol[7] > '9' {
		return nil, nil, nil, false
	}
	for _, c := range method {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && !strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c)) {
			return nil, nil, nil, false
		}
	}
	for _, c := range target {
		if c <= ' ' || c == 0x7f {
			return nil, nil, nil, false
		}
	}
	return method, target, protocol, true
}
