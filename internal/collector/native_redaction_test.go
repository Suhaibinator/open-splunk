package collector

import (
	"bytes"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/config"
	"github.com/Suhaibinator/open-splunk/internal/collector/parserconfig"
	"google.golang.org/protobuf/proto"
)

func TestNativeRedactionProjections(t *testing.T) {
	for _, tc := range []struct {
		name, format, raw, sensitive, alias string
		options                             *parserconfig.Options
		constants                           bool
	}{
		{name: "access escaped alias", format: "nginx-combined", raw: `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET / HTTP/1.1" 200 1 "-" "native-secret-\x76alue"`, sensitive: "token", alias: "user_agent"},
		{name: "access derived request", format: "apache-common", raw: `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET /native-secret-value HTTP/1.1" 200 1`, sensitive: "request_target"},
		{name: "Java positional alias", format: "log4j2-pattern", raw: "native-secret-value|safe", sensitive: "token", alias: "credential", options: &parserconfig.Options{Pattern: "%{credential}|%{message}"}},
		{name: "Java direct overwritten", format: "logback-pattern", raw: "native-secret-value|safe", sensitive: "credential", options: &parserconfig.Options{Pattern: "%{credential}|%{message}"}, constants: true},
		{name: "Java canonical message", format: "logback-pattern", raw: "native-secret-value", sensitive: "message", options: &parserconfig.Options{Pattern: "%{message}"}},
		{name: "logfmt mapped message", format: "logfmt", raw: `credential="native-secret-\u0076alue"`, sensitive: "credential", options: &parserconfig.Options{Fields: map[string]string{"message": "credential"}}},
		{name: "logfmt mapped trace", format: "logfmt", raw: `credential=native-secret-value`, sensitive: "credential", options: &parserconfig.Options{Fields: map[string]string{"trace_id": "credential"}}},
		{name: "logfmt mapped level", format: "logfmt", raw: `credential=native-secret-value`, sensitive: "credential", options: &parserconfig.Options{Fields: map[string]string{"level": "credential"}}},
		{name: "Docker message source", format: "docker-json-file", raw: `{"log":"native-secret-value","time":"2026-09-07T12:34:56Z","stream":"stdout"}`, sensitive: "log"},
		{name: "Docker message destination", format: "docker-json-file", raw: `{"log":"native-secret-value","time":"2026-09-07T12:34:56Z","stream":"stdout"}`, sensitive: "message"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DecodeConfig{Format: InputFormat(tc.format), Parser: tc.options, InputID: "test", IndexName: "main", Source: "trusted-source", Sourcetype: tc.format, Host: "trusted-host"}
			if tc.constants {
				cfg.ConstantFields = &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{Name: "credential", Value: stringValue("trusted-constant")}}}
			}
			decoder, err := NewDecoder(cfg)
			if err != nil {
				t.Fatal(err)
			}
			event, err := decoder.Decode([]byte(tc.raw), SourcePosition{}, time.Date(2026, 9, 7, 12, 34, 57, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			originalID := event.EventId
			processors := []config.ProcessorConfig{}
			if tc.alias != "" {
				processors = append(processors, config.ProcessorConfig{Type: "rename", From: tc.alias, To: tc.sensitive})
			}
			processors = append(processors, config.ProcessorConfig{Type: "redact", Fields: []string{tc.sensitive}, Replacement: "[MASKED]"})
			pipeline, redactor, err := buildProcessorRuntime(processors)
			if err != nil {
				t.Fatal(err)
			}
			event = redactor.beforeNativePipeline(event, decoder)
			event, err = pipeline.Process(event)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := proto.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(wire, []byte("native-secret")) {
				t.Fatalf("sensitive native projection leaked: %s", event)
			}
			if event.EventId != originalID || event.Host != decoder.cfg.Host || event.Source != decoder.cfg.Source {
				t.Fatal("trusted metadata or stable ID changed")
			}
		})
	}
}

func TestNativeRedactionPreservesStaticAndDiscardedAliasProvenance(t *testing.T) {
	for _, tc := range []struct {
		name      string
		constants *opensplunk.TypedObject
		prefix    []config.ProcessorConfig
		maskText  bool
	}{
		{name: "constant alias", constants: &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{Name: "credential", Value: stringValue("trusted-static")}}}},
		{name: "denied source", prefix: []config.ProcessorConfig{{Type: "deny", Fields: []string{"credential"}}}},
		{name: "overwritten rename source", maskText: true, prefix: []config.ProcessorConfig{{Type: "rename", From: "safe", To: "credential"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder, err := NewDecoder(DecodeConfig{Format: "log4j2-pattern", InputID: "test", IndexName: "main", Source: "trusted", Sourcetype: "java", Host: "trusted", ConstantFields: tc.constants, Parser: &parserconfig.Options{Pattern: "%{credential}|%{safe}|%{message}"}})
			if err != nil {
				t.Fatal(err)
			}
			raw := []byte("source-only-secret|public|safe message")
			event, err := decoder.Decode(raw, SourcePosition{}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			processors := append(tc.prefix, config.ProcessorConfig{Type: "rename", From: "credential", To: "token"}, config.ProcessorConfig{Type: "redact", Fields: []string{"token"}, Replacement: "MASKED"})
			_, redactor, err := buildProcessorRuntime(processors)
			if err != nil {
				t.Fatal(err)
			}
			event = redactor.beforeNativePipeline(event, decoder)
			if tc.maskText {
				if string(event.Raw) != "MASKED" || fieldValue(event, "credential").GetStringValue() != "source-only-secret" {
					t.Fatalf("rename overwrite provenance changed: %s", event)
				}
				return
			}
			if !bytes.Equal(event.Raw, raw) || event.GetMessage() != "safe message" {
				t.Fatalf("unrelated source provenance was redacted: %s", event)
			}
		})
	}
}

func TestNativeRedactionDockerStreamProjection(t *testing.T) {
	for _, target := range []string{"stream", "docker_stream"} {
		t.Run(target, func(t *testing.T) {
			decoder, err := NewDecoder(DecodeConfig{Format: "docker-json-file", InputID: "test", IndexName: "main", Source: "trusted", Sourcetype: "docker", Host: "trusted"})
			if err != nil {
				t.Fatal(err)
			}
			event, err := decoder.Decode([]byte(`{"log":"safe","stream":"stdout","time":"2026-09-07T12:34:56Z"}`), SourcePosition{}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			_, redactor, err := buildProcessorRuntime([]config.ProcessorConfig{{Type: "redact", Fields: []string{target}, Replacement: "MASKED"}})
			if err != nil {
				t.Fatal(err)
			}
			event = redactor.beforeNativePipeline(event, decoder)
			if fieldValue(event, "docker_stream").GetStringValue() != "MASKED" || string(event.Raw) != "MASKED" {
				t.Fatalf("stream projection leaked: %s", event)
			}
		})
	}
}

func TestNativeRedactionTimestampProjection(t *testing.T) {
	decoder, err := NewDecoder(DecodeConfig{Format: "logfmt", InputID: "test", IndexName: "main", Source: "trusted", Sourcetype: "logfmt", Host: "trusted", Parser: &parserconfig.Options{Fields: map[string]string{"timestamp": "credential"}}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 7, 12, 34, 57, 0, time.UTC)
	event, err := decoder.Decode([]byte(`credential=1788784496`), SourcePosition{}, now)
	if err != nil {
		t.Fatal(err)
	}
	_, redactor, err := buildProcessorRuntime([]config.ProcessorConfig{{Type: "redact", Fields: []string{"credential"}, Replacement: "MASKED"}})
	if err != nil {
		t.Fatal(err)
	}
	event = redactor.beforeNativePipeline(event, decoder)
	if !event.EventTime.AsTime().Equal(now) || event.EventTimeSource != opensplunk.EventTimeSource_EVENT_TIME_SOURCE_COLLECTED_AT_FALLBACK {
		t.Fatalf("sensitive timestamp was not replaced with collection fallback: %s", event)
	}
}

func TestNativeRedactionLegacyAndNoPolicyPaths(t *testing.T) {
	for _, format := range []InputFormat{InputFormatNDJSON, InputFormatRaw, "logfmt"} {
		t.Run(string(format), func(t *testing.T) {
			decoder := newTestDecoder(t, DecodeConfig{Format: format})
			raw := []byte(`{"token":"secret","safe":"kept"}`)
			if format == "logfmt" {
				raw = []byte("token=secret safe=kept")
			}
			event, err := decoder.Decode(raw, SourcePosition{}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			_, redactor, err := buildProcessorRuntime(nil)
			if err != nil {
				t.Fatal(err)
			}
			original := proto.Clone(event).(*opensplunk.LogEvent)
			if got := redactor.beforeNativePipeline(event, decoder); got != event || !proto.Equal(got, original) {
				t.Fatal("no-policy path changed event")
			}
			if format == "logfmt" {
				return
			}
			_, redactor, err = buildProcessorRuntime([]config.ProcessorConfig{{Type: "redact", Fields: []string{"token"}, Replacement: "MASKED"}})
			if err != nil {
				t.Fatal(err)
			}
			want := redactor.beforePipeline(original, decoder.constantNames)
			if got := redactor.beforeNativePipeline(event, decoder); !proto.Equal(got, want) {
				t.Fatalf("legacy redaction behavior changed: %s", got)
			}
		})
	}
}

func TestNativeRedactionEmbeddedEscapedAssignments(t *testing.T) {
	for _, tc := range []struct {
		name, format, raw string
		options           *parserconfig.Options
	}{
		{name: "access escaped assignment", format: "nginx-combined", raw: `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET / HTTP/1.1" 200 1 "-" "token\x3dnative-secret-value"`},
		{name: "access binary assignment", format: "apache-combined", raw: `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET / HTTP/1.1" 200 1 "-" "token\x3dnative-secret-value \xff"`},
		{name: "logfmt escaped canonical message", format: "logfmt", raw: `message="token\u003dnative-secret-value"`},
		{name: "logfmt escaped trace", format: "logfmt", raw: `trace_id="token\u003dnative-secret-value"`},
		{name: "Java embedded assignment", format: "logback-pattern", raw: "token=native-secret-value|safe", options: &parserconfig.Options{Pattern: "%{credential}|%{message}"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder, err := NewDecoder(DecodeConfig{Format: InputFormat(tc.format), InputID: "test", IndexName: "main", Source: "trusted", Sourcetype: tc.format, Host: "trusted", Parser: tc.options})
			if err != nil {
				t.Fatal(err)
			}
			event, err := decoder.Decode([]byte(tc.raw), SourcePosition{}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			_, redactor, err := buildProcessorRuntime([]config.ProcessorConfig{{Type: "redact", Fields: []string{"token"}, Replacement: "MASKED"}})
			if err != nil {
				t.Fatal(err)
			}
			event = redactor.beforeNativePipeline(event, decoder)
			wire, err := proto.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(wire, []byte("native-secret-value")) {
				t.Fatalf("encoded embedded assignment leaked: %s", event)
			}
		})
	}
}

func TestNativeRedactionReparseFailureClosesPayloadBoundary(t *testing.T) {
	decoder, err := NewDecoder(DecodeConfig{Format: "logfmt", InputID: "test", IndexName: "main", Source: "trusted", Sourcetype: "logfmt", Host: "trusted", ConstantFields: &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{Name: "environment", Value: stringValue("production")}}}})
	if err != nil {
		t.Fatal(err)
	}
	event, err := decoder.Decode([]byte("credential=secret message=secret trace_id=secret span_id=secret level=secret"), SourcePosition{}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	originalID := event.EventId
	// Simulate a violation of the immutable parse/reparse contract. Retaining the
	// old projected values would leak credentials while merely masking raw.
	event.Raw = []byte("malformed secret")
	_, redactor, err := buildProcessorRuntime([]config.ProcessorConfig{{Type: "redact", Fields: []string{"credential"}, Replacement: "MASKED"}})
	if err != nil {
		t.Fatal(err)
	}
	event = redactor.beforeNativePipeline(event, decoder)
	wire, err := proto.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wire, []byte("secret")) || event.EventId != originalID || event.Host != "trusted" || fieldValue(event, "environment").GetStringValue() != "production" {
		t.Fatalf("reparse failure did not retain only safe metadata: %s", event)
	}
}
