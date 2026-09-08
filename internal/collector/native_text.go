package collector

import (
	"encoding/json"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/parserconfig"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/jsonnumber"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func nativeValidName(name string) bool {
	if len(name) == 0 || len(name) > eventfields.MaximumDynamicPathSegmentBytes || !utf8.ValidString(name) || strings.TrimSpace(name) != name {
		return false
	}
	return !strings.ContainsFunc(name, unicode.IsControl)
}

func nativeHorizontal(c byte) bool { return c == ' ' || c == '\t' }

// decodeLogfmt scans once, checking token and field bounds before conversion.
func (d *Decoder) decodeLogfmt(event *opensplunk.LogEvent, raw []byte) error {
	if !utf8.Valid(raw) {
		return errors.New("logfmt requires UTF-8")
	}
	seen := make(map[string]struct{})
	fields := make([]*opensplunk.TypedObjectField, 0, 8)
	for at := 0; at < len(raw); {
		for at < len(raw) && nativeHorizontal(raw[at]) {
			at++
		}
		if at == len(raw) {
			break
		}
		start := at
		for at < len(raw) && raw[at] != '=' && !nativeHorizontal(raw[at]) {
			at++
		}
		if at == len(raw) || raw[at] != '=' {
			return errors.New("logfmt token requires equals")
		}
		name := string(raw[start:at])
		if !nativeValidName(name) || strings.ContainsAny(name, "\"\\") {
			return errors.New("invalid logfmt field name")
		}
		if _, exists := seen[name]; exists {
			return errors.New("duplicate logfmt field")
		}
		seen[name] = struct{}{}
		if len(seen) > d.cfg.MaxJSONFields {
			return errors.New("logfmt field count exceeds limit")
		}
		at++
		start = at
		var value any
		if at < len(raw) && raw[at] == '"' {
			at++
			closed := false
			escaped := false
			for at < len(raw) {
				c := raw[at]
				at++
				if c == '\\' {
					escaped = true
					if at < len(raw) {
						at++
					}
					continue
				}
				if c == '"' {
					closed = true
					break
				}
			}
			if !closed || (at < len(raw) && !nativeHorizontal(raw[at])) {
				return errors.New("invalid quoted logfmt value")
			}
			var decoded string
			if escaped {
				if err := json.Unmarshal(raw[start:at], &decoded); err != nil {
					return errors.New("invalid logfmt string escape")
				}
				if !nativeJSONSurrogatesValid(raw[start:at]) {
					return errors.New("logfmt string contains an unpaired Unicode surrogate")
				}
			} else {
				for _, c := range raw[start+1 : at-1] {
					if c < ' ' {
						return errors.New("invalid logfmt string escape")
					}
				}
				decoded = string(raw[start+1 : at-1])
			}
			value = decoded
		} else {
			for at < len(raw) && !nativeHorizontal(raw[at]) {
				if raw[at] < ' ' || raw[at] == 0x7f || raw[at] == '"' {
					return errors.New("invalid unquoted logfmt value")
				}
				at++
			}
			token := raw[start:at]
			text := string(token)
			switch {
			case text == "true":
				value = true
			case text == "false":
				value = false
			case len(token) > 0 && (token[0] == '-' || (token[0] >= '0' && token[0] <= '9')) && jsonnumber.Valid(text):
				value = json.Number(text)
			default:
				value = text
			}
		}
		role := d.parser.CanonicalField(name)
		if role != "" {
			if err := d.nativeCanonical(event, role, value); err != nil {
				return err
			}
			continue
		}
		if eventfields.IsCollectorReservedRoot(name) {
			continue
		}
		converted, err := typedJSONValue(value)
		if err != nil {
			return errors.New("invalid logfmt typed value")
		}
		fields = append(fields, &opensplunk.TypedObjectField{Name: name, Value: converted})
	}
	if len(seen) == 0 {
		return errors.New("logfmt requires a field")
	}
	event.Fields.Fields = fields
	return nil
}

func (d *Decoder) decodePattern(event *opensplunk.LogEvent, raw []byte) error {
	var local [8]parserconfig.Capture
	captures, err := d.parser.CaptureInto(raw, local[:0])
	if err != nil {
		return err
	}
	if len(captures) > d.cfg.MaxJSONFields {
		return errors.New("pattern capture count exceeds limit")
	}
	fields := make([]*opensplunk.TypedObjectField, 0, len(captures))
	for _, capture := range captures {
		switch capture.Name {
		case "timestamp", "message", "level", "trace_id", "span_id":
			if err := d.nativeCanonical(event, capture.Name, capture.Value); err != nil {
				return err
			}
		default:
			fields = append(fields, &opensplunk.TypedObjectField{Name: capture.Name, Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: capture.Value}}})
		}
	}
	event.Fields.Fields = fields
	return nil
}

func (d *Decoder) nativeCanonical(event *opensplunk.LogEvent, role string, value any) error {
	if role == "timestamp" {
		var timestamp *timestamppb.Timestamp
		if text, ok := value.(string); ok {
			parsed, err := d.parser.ParseTime(text)
			if err != nil {
				return err
			}
			timestamp = timestamppb.New(parsed)
		} else {
			if d.parser.HasTimestampLayout() {
				return errors.New("configured timestamp requires text")
			}
			parsed, err := parseEventTime(value)
			if err != nil {
				return err
			}
			timestamp = timestamppb.New(parsed)
		}
		if timestamp.CheckValid() != nil {
			return errors.New("timestamp outside event range")
		}
		event.EventTime = timestamp
		event.EventTimeSource = opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return errors.New("canonical text field requires a string")
	}
	switch role {
	case "message":
		event.Message = new(text)
	case "level":
		event.Level = new(text)
		event.Severity = severityForLevel(text)
	case "trace_id":
		event.TraceId = new(text)
	case "span_id":
		event.SpanId = new(text)
	}
	return nil
}
