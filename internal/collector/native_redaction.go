package collector

import (
	"bytes"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// beforeNativePipeline extends the existing lexical sanitizer with native
// projection provenance. Only inputs with an explicit redaction policy reparse;
// NDJSON and raw retain their established path and constant lineage semantics.
func (redactor *durableRedactor) beforeNativePipeline(event *opensplunk.LogEvent, decoder *Decoder) *opensplunk.LogEvent {
	if redactor == nil || event == nil {
		return event
	}
	if decoder.cfg.Format == InputFormatNDJSON || decoder.cfg.Format == InputFormatRaw || len(redactor.configured) == 0 {
		return redactor.beforePipeline(event, decoder.constantNames)
	}
	// The transient projection must precede static-field overwrites: direct
	// confidentiality policies also protect shadowed source bytes. Rename aliases,
	// in contrast, are resolved against the actual post-merge event below.
	sourceDecoder := *decoder
	sourceDecoder.constants = nil
	sourceDecoder.constantNames = nil
	source, err := sourceDecoder.decodeNative(&opensplunk.LogEvent{Fields: &opensplunk.TypedObject{}}, event.Raw)
	if err != nil {
		// A previously decoded record cannot fail immutable reparsing. Fail closed
		// if a future parser violates this invariant, retaining trusted metadata.
		event.Fields = decoder.mergeConstants(nil)
		nativeMaskCanonical(event, "level", ingest.DefaultRedactionReplacement)
		nativeMaskCanonical(event, "trace_id", ingest.DefaultRedactionReplacement)
		nativeMaskCanonical(event, "span_id", ingest.DefaultRedactionReplacement)
		nativeMaskCanonical(event, "timestamp", ingest.DefaultRedactionReplacement)
		nativeMaskText(event, ingest.DefaultRedactionReplacement)
		return redactor.beforePipeline(event, decoder.constantNames)
	}
	origins := nativeRedactionOrigins(source, decoder)
	embeddedChanged := redactor.nativeEmbeddedRedaction(source, event, decoder)
	// Probe the compiled validator to use its exact normalization and replacement
	// precedence rather than maintaining a second definition of sensitive names.
	probe := &opensplunk.LogEvent{Fields: &opensplunk.TypedObject{}}
	for _, origin := range origins {
		probe.Fields.Fields = append(probe.Fields.Fields, &opensplunk.TypedObjectField{Name: origin.name, Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_BoolValue{BoolValue: true}}})
	}
	for _, configured := range redactor.configured {
		configured.RedactEventInPlace(probe)
	}
	aliases := make(map[string]string)
	for _, alias := range redactor.activeAliases(event, decoder.constantNames) {
		if !alias.StructuredOnly {
			aliases[alias.Field] = alias.Replacement
		}
	}
	var fieldReplacements map[string]string
	maskField := func(name, replacement string) {
		if fieldReplacements == nil {
			fieldReplacements = make(map[string]string)
		}
		fieldReplacements[name] = replacement
	}
	var replacement string
	maskText := false
	for i, origin := range origins {
		marker, sensitive := probe.Fields.Fields[i].Value.Kind.(*opensplunk.TypedValue_StringValue)
		current := ""
		if sensitive {
			current = marker.StringValue
		}
		if !sensitive && origin.role == "" {
			current, sensitive = aliases[origin.output]
		}
		if !sensitive {
			continue
		}
		if !maskText {
			replacement = current
			maskText = true
		}
		if origin.role != "" {
			nativeMaskCanonical(event, origin.role, current)
		} else {
			maskField(origin.output, current)
		}
		if nativeAccessRequestOrigin(decoder.cfg.Format, origin.output) {
			for _, name := range []string{"request", "method", "request_target", "protocol"} {
				maskField(name, current)
			}
		}
	}
	for _, field := range event.Fields.GetFields() {
		if _, constant := decoder.constantNames[field.Name]; constant {
			continue
		}
		if value, sensitive := fieldReplacements[field.Name]; sensitive {
			field.Value = &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: value}}
		}
	}
	if maskText {
		nativeMaskText(event, replacement)
	} else if embeddedChanged {
		nativeMaskText(event, ingest.DefaultRedactionReplacement)
	}
	return redactor.beforePipeline(event, decoder.constantNames)
}

type nativeRedactionOrigin struct{ name, output, role string }

func nativeRedactionOrigins(source *opensplunk.LogEvent, decoder *Decoder) []nativeRedactionOrigin {
	origins := make([]nativeRedactionOrigin, 0, len(source.Fields.Fields)+12)
	for _, field := range source.Fields.Fields {
		origins = append(origins, nativeRedactionOrigin{name: field.Name, output: field.Name})
	}
	if decoder.cfg.Format == "docker-json-file" {
		origins = append(origins, nativeRedactionOrigin{name: "stream", output: "docker_stream"})
	}
	for _, role := range []string{"timestamp", "message", "level", "trace_id", "span_id"} {
		present := false
		switch role {
		case "timestamp":
			present = source.EventTime != nil
		case "message":
			present = source.Message != nil
		case "level":
			present = source.Level != nil
		case "trace_id":
			present = source.TraceId != nil
		case "span_id":
			present = source.SpanId != nil
		}
		if !present {
			continue
		}
		name := role
		switch decoder.cfg.Format {
		case "logfmt":
			name = decoder.parser.SourceField(role)
		case "docker-json-file":
			if role == "message" {
				name = "log"
			}
			if role == "timestamp" {
				name = "time"
			}
		}
		origins = append(origins, nativeRedactionOrigin{name: name, output: role, role: role})
		if name != role {
			origins = append(origins, nativeRedactionOrigin{name: role, output: role, role: role})
		}
	}
	return origins
}

func nativeAccessRequestOrigin(format InputFormat, name string) bool {
	if format != "nginx-combined" && format != "apache-common" && format != "apache-combined" {
		return false
	}
	switch name {
	case "request", "method", "request_target", "protocol", "message":
		return true
	}
	return false
}

func nativeMaskCanonical(event *opensplunk.LogEvent, role, replacement string) {
	switch role {
	case "message":
		event.Message = new(replacement)
	case "level":
		event.Level = new(replacement)
		event.Severity = 0
	case "trace_id":
		event.TraceId = nil
	case "span_id":
		event.SpanId = nil
	case "timestamp":
		if event.CollectedAt != nil {
			event.EventTime = proto.Clone(event.CollectedAt).(*timestamppb.Timestamp)
		}
		event.EventTimeSource = opensplunk.EventTimeSource_EVENT_TIME_SOURCE_COLLECTED_AT_FALLBACK
	}
}

// Positional captures and derived values need not occur literally in raw (for
// example access-log hex escapes). Replacing complete text is conservative and
// avoids unsafe substring substitution. The first sensitive origin in stable
// parser projection order determines the text marker; individual fields retain
// their policy's marker. The stable event ID is deliberately untouched.
func nativeMaskText(event *opensplunk.LogEvent, replacement string) {
	event.Raw = []byte(replacement)
	event.RawEncoding = opensplunk.RawEncoding_RAW_ENCODING_UTF8
	if event.Message != nil {
		event.Message = new(replacement)
	}
}

// Decoded strings may reveal assignment syntax hidden by producer escapes.
// Sanitize the payload projection before constants so shadowed source text is
// protected, then copy sanitized source fields only where provenance survives.
func (redactor *durableRedactor) nativeEmbeddedRedaction(source, event *opensplunk.LogEvent, decoder *Decoder) bool {
	before := proto.Clone(source).(*opensplunk.LogEvent)
	for _, configured := range redactor.configured {
		configured.RedactEventInPlace(source)
	}
	for _, field := range source.Fields.Fields {
		redactor.nativeEmbeddedBytes(field.Value)
	}
	changed := !proto.Equal(before, source)
	if changed {
		sourceFields := make(map[string]*opensplunk.TypedValue, len(source.Fields.Fields))
		for _, field := range source.Fields.Fields {
			sourceFields[field.Name] = field.Value
		}
		for _, field := range event.Fields.GetFields() {
			if _, constant := decoder.constantNames[field.Name]; constant {
				continue
			}
			if value, exists := sourceFields[field.Name]; exists {
				field.Value = value
			}
		}
		if source.Message != nil {
			event.Message = source.Message
		}
	}
	// Canonical level and correlation IDs are not dynamic fields, so the common
	// validator deliberately leaves them alone. Their native source text still
	// must not carry an escaped configured credential across the WAL boundary.
	for _, canonical := range []struct {
		role  string
		value *string
	}{
		{"level", source.Level}, {"trace_id", source.TraceId}, {"span_id", source.SpanId},
	} {
		if canonical.value == nil {
			continue
		}
		probe := &opensplunk.LogEvent{Message: canonical.value}
		for _, configured := range redactor.configured {
			configured.RedactEventInPlace(probe)
		}
		if probe.GetMessage() != *canonical.value {
			changed = true
			nativeMaskCanonical(event, canonical.role, probe.GetMessage())
		}
	}
	return changed
}

// Access escape sequences can produce non-UTF-8 typed bytes. The existing text
// redactor is byte-aware at the raw boundary; reuse that path for these values.
func (redactor *durableRedactor) nativeEmbeddedBytes(value *opensplunk.TypedValue) {
	switch kind := value.GetKind().(type) {
	case *opensplunk.TypedValue_BytesValue:
		probe := &opensplunk.LogEvent{Raw: kind.BytesValue, RawEncoding: opensplunk.RawEncoding_RAW_ENCODING_BINARY}
		for _, configured := range redactor.configured {
			configured.RedactEventInPlace(probe)
		}
		if !bytes.Equal(probe.Raw, kind.BytesValue) {
			kind.BytesValue = probe.Raw
		}
	case *opensplunk.TypedValue_ObjectValue:
		for _, field := range kind.ObjectValue.GetFields() {
			redactor.nativeEmbeddedBytes(field.Value)
		}
	case *opensplunk.TypedValue_ListValue:
		for _, item := range kind.ListValue.GetValues() {
			redactor.nativeEmbeddedBytes(item)
		}
	}
}
