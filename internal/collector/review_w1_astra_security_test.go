package collector

import (
	"bytes"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/config"
	"github.com/Suhaibinator/open-splunk/internal/collector/parserconfig"
	"google.golang.org/protobuf/proto"
)

func TestReviewW1AstraSecurityNoImplicitKeywordPolicy(t *testing.T) {
	for _, tc := range []struct {
		format, raw string
		options     *parserconfig.Options
	}{
		{"ndjson", `{"message":"password=原文 token=secret","password":"原文","token":"secret","secret":"retained"}`, nil},
		{"raw", `password=原文 token=secret`, nil},
		{"docker-json-file", `{"log":"password=原文 token=secret","time":"2026-09-07T12:34:56Z","stream":"stdout","password":"原文","token":"secret","secret":"retained"}`, nil},
		{"nginx-combined", `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET /?password=secret HTTP/1.1" 200 1 "-" "token=原文"`, nil},
		{"apache-common", `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET /?password=secret HTTP/1.1" 200 1`, nil},
		{"apache-combined", `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET /?password=secret HTTP/1.1" 200 1 "-" "token=原文"`, nil},
		{"logfmt", `message="password=原文 token=secret" password="\ud83d\udd10" token=secret secret=retained`, nil},
		{"log4j2-pattern", "原文|secret|retained|password=secret token=secret", &parserconfig.Options{Pattern: "%{password}|%{token}|%{secret}|%{message}"}},
		{"logback-pattern", "原文|secret|retained|password=secret token=secret", &parserconfig.Options{Pattern: "%{password}|%{token}|%{secret}|%{message}"}},
	} {
		t.Run(tc.format, func(t *testing.T) {
			decoder := reviewW1AstraSecurityDecoder(t, DecodeConfig{Format: InputFormat(tc.format), Parser: tc.options})
			event, err := decoder.Decode([]byte(tc.raw), SourcePosition{}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			original := proto.Clone(event).(*opensplunk.LogEvent)
			for _, processors := range [][]config.ProcessorConfig{nil, {{Type: "rename", From: "absent", To: "password"}}} {
				pipeline, redactor, err := buildProcessorRuntime(processors)
				if err != nil {
					t.Fatal(err)
				}
				got := redactor.beforeNativePipeline(event, decoder)
				got, err = pipeline.Process(got)
				if err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(got, original) || !bytes.Equal(got.Raw, []byte(tc.raw)) {
					t.Fatal("content changed without an explicit redaction policy")
				}
			}
		})
	}
}

func TestReviewW1AstraSecurityEscapedNestedPolicyAndLineage(t *testing.T) {
	for _, tc := range []struct {
		format, raw, alias string
		options            *parserconfig.Options
	}{
		{"docker-json-file", `{"log":"safe","stream":"stdout","time":"2026-09-07T12:34:56Z","container":{"items":[{"token":"hidden-secret"},{"value":"token\u003dhidden-secret"}]}}`, "", nil},
		{"logfmt", `credential="hidden-\u0073ecret"`, "credential", nil},
		{"logback-pattern", "hidden-secret|safe", "credential", &parserconfig.Options{Pattern: "%{credential}|%{message}"}},
		{"nginx-combined", `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET / HTTP/1.1" 200 1 "-" "hidden-\x73ecret\xff"`, "user_agent", nil},
	} {
		t.Run(tc.format, func(t *testing.T) {
			decoder := reviewW1AstraSecurityDecoder(t, DecodeConfig{Format: InputFormat(tc.format), Parser: tc.options})
			event, err := decoder.Decode([]byte(tc.raw), SourcePosition{}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			id := event.EventId
			var processors []config.ProcessorConfig
			if tc.alias != "" {
				processors = append(processors, config.ProcessorConfig{Type: "rename", From: tc.alias, To: "intermediate"}, config.ProcessorConfig{Type: "rename", From: "intermediate", To: "token"})
			}
			processors = append(processors, config.ProcessorConfig{Type: "redact", Fields: []string{"token"}, Replacement: "MASKED"}, config.ProcessorConfig{Type: "deny", Fields: []string{"token"}})
			pipeline, redactor, err := buildProcessorRuntime(processors)
			if err != nil {
				t.Fatal(err)
			}
			event = redactor.beforeNativePipeline(event, decoder)
			wire, err := proto.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(wire, []byte("hidden")) {
				t.Fatal("source escaped secret survived pre-WAL sanitizer")
			}
			event, err = pipeline.Process(event)
			if err != nil {
				t.Fatal(err)
			}
			if event.EventId != id {
				t.Fatal("redaction changed stable ID")
			}
		})
	}
}

func TestReviewW1AstraSecurityUnicodeErrorsArePayloadFree(t *testing.T) {
	for _, tc := range []struct {
		format, raw string
		options     *parserconfig.Options
	}{
		{"docker-json-file", `{"log":"SECRET\ud800","stream":"stdout","time":"2026-09-07T12:34:56Z"}`, nil},
		{"docker-json-file", `{"log":"safe","stream":"stdout","time":"SECRET"}`, nil},
		{"logfmt", `message="SECRET\udfff"`, nil},
		{"logfmt", "SECRET=one SECRET=two", nil},
		{"log4j2-pattern", "SECRET", &parserconfig.Options{Pattern: "%{credential}|%{message}"}},
	} {
		t.Run(tc.format+tc.raw, func(t *testing.T) {
			decoder := reviewW1AstraSecurityDecoder(t, DecodeConfig{Format: InputFormat(tc.format), Parser: tc.options})
			_, err := decoder.Decode([]byte(tc.raw), SourcePosition{}, time.Now())
			if err == nil {
				t.Fatal("malformed record accepted")
			}
			if strings.Contains(err.Error(), "SECRET") {
				t.Fatal("error contains payload")
			}
		})
	}
}

func reviewW1AstraSecurityDecoder(t *testing.T, cfg DecodeConfig) *Decoder {
	t.Helper()
	cfg.InputID = "review"
	cfg.IndexName = "main"
	cfg.Source = "trusted-source"
	cfg.Sourcetype = "review"
	cfg.Host = "trusted-host"
	decoder, err := NewDecoder(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return decoder
}

func TestReviewW1AstraSecurityTrustedMetadataAndStaticPrecedence(t *testing.T) {
	for _, tc := range []struct{ format, raw string }{
		{"docker-json-file", `{"log":"safe","stream":"stdout","time":"2026-09-07T12:34:56Z","host":"spoof","source":"spoof","index_name":"spoof","service":"spoof","environment":"spoof"}`},
		{"logfmt", `message=safe host=spoof source=spoof index_name=spoof service=spoof environment=spoof`},
	} {
		t.Run(tc.format, func(t *testing.T) {
			decoder := reviewW1AstraSecurityDecoder(t, DecodeConfig{Format: InputFormat(tc.format), Service: "trusted-service", ConstantFields: &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{Name: "environment", Value: stringValue("trusted-environment")}}}})
			event, err := decoder.Decode([]byte(tc.raw), SourcePosition{}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if event.Host != "trusted-host" || event.Source != "trusted-source" || event.IndexName != "main" || event.GetService() != "trusted-service" || fieldValue(event, "environment").GetStringValue() != "trusted-environment" {
				t.Fatal("payload overrode trusted metadata")
			}
			for _, name := range []string{"host", "source", "index_name", "service"} {
				if fieldValue(event, name) != nil {
					t.Fatalf("reserved field %s survived as dynamic field", name)
				}
			}
		})
	}
}
