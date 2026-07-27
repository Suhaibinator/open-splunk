package ingest

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

func BenchmarkCompositeSupplementalRedactor(b *testing.B) {
	type benchmarkFixture struct {
		name string
		raw  []byte
	}
	for _, policyCount := range []int{1, 8, 32} {
		policies := make([]RedactionPolicy, policyCount)
		sequential := make([]*Validator, policyCount)
		for index := range policyCount {
			policies[index] = RedactionPolicy{
				AdditionalSensitiveFields: []string{fmt.Sprintf("secret_%02d", index)},
				Replacement:               fmt.Sprintf("<MASK_%02d>", index),
			}
			var err error
			sequential[index], err = NewSupplementalRedactor(DefaultLimits(), policies[index])
			if err != nil {
				b.Fatal(err)
			}
		}
		composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), policies)
		if err != nil {
			b.Fatal(err)
		}

		fixtures := []benchmarkFixture{
			{
				name: "safe_text_4KiB",
				raw:  bytes.Repeat([]byte("safe=value "), (4<<10)/len("safe=value ")),
			},
			{
				name: "safe_text_64KiB",
				raw:  bytes.Repeat([]byte("safe=value "), (64<<10)/len("safe=value ")),
			},
			{
				name: "plain_KV_all_policy_hits_4KiB",
				raw:  compositeBenchmarkText(policyCount, 4<<10),
			},
			{
				name: "plain_KV_all_policy_hits_64KiB",
				raw:  compositeBenchmarkText(policyCount, 64<<10),
			},
			{
				name: "valid_JSON_hits_64KiB",
				raw:  compositeBenchmarkJSON(policyCount, 64<<10),
			},
		}
		if policyCount > 1 {
			fixtures = append(fixtures, benchmarkFixture{
				name: "plain_KV_reverse_policy_hits_4KiB",
				raw:  compositeBenchmarkTextReverse(policyCount, 4<<10),
			})
		}
		for _, fixture := range fixtures {
			fixture := fixture
			want := supplementalBenchmarkResult(fixture.raw, func(event *opensplunkv1.LogEvent) {
				for _, redactor := range sequential {
					redactor.RedactEventInPlace(event)
				}
			})
			got := supplementalBenchmarkResult(fixture.raw, func(event *opensplunkv1.LogEvent) {
				composite.RedactEventInPlace(event)
			})
			if !proto.Equal(got, want) {
				b.Fatalf("fixture %q composite output differs from sequential output", fixture.name)
			}
			b.Run(fmt.Sprintf("policies=%d/%s/sequential", policyCount, fixture.name), func(b *testing.B) {
				benchmarkSupplementalRedaction(b, fixture.raw, func(event *opensplunkv1.LogEvent) {
					for _, redactor := range sequential {
						redactor.RedactEventInPlace(event)
					}
				})
			})
			b.Run(fmt.Sprintf("policies=%d/%s/composite", policyCount, fixture.name), func(b *testing.B) {
				benchmarkSupplementalRedaction(b, fixture.raw, func(event *opensplunkv1.LogEvent) {
					composite.RedactEventInPlace(event)
				})
			})
		}
	}
}

func BenchmarkTopLevelAliasRedaction(b *testing.B) {
	for _, policyCount := range []int{1, 8, 32} {
		policies := make([]TopLevelAliasRedaction, policyCount)
		for index := range policyCount {
			policies[index] = TopLevelAliasRedaction{
				Field:       fmt.Sprintf("secret_%02d", index),
				Replacement: fmt.Sprintf("<MASK_%02d>", index),
			}
		}
		for _, fixture := range []struct {
			name        string
			raw         []byte
			withMessage bool
		}{
			{
				name: "valid_JSON_raw_only_4KiB",
				raw:  compositeBenchmarkJSON(policyCount, 4<<10),
			},
			{
				name:        "plain_KV_raw_and_message_4KiB",
				raw:         compositeBenchmarkText(policyCount, 4<<10),
				withMessage: true,
			},
		} {
			fixture := fixture
			want := aliasBenchmarkResult(b, fixture.raw, fixture.withMessage, func(event *opensplunkv1.LogEvent) {
				legacyTopLevelAliasRedaction(b, event, policies)
			})
			got := aliasBenchmarkResult(b, fixture.raw, fixture.withMessage, func(event *opensplunkv1.LogEvent) {
				RedactTopLevelAliasesInPlace(event, policies)
			})
			if !proto.Equal(got, want) {
				b.Fatalf("fixture %q composite alias output differs from sequential output", fixture.name)
			}
			b.Run(fmt.Sprintf("policies=%d/%s/sequential", policyCount, fixture.name), func(b *testing.B) {
				benchmarkAliasRedaction(b, fixture.raw, fixture.withMessage, func(event *opensplunkv1.LogEvent) {
					legacyTopLevelAliasRedaction(b, event, policies)
				})
			})
			b.Run(fmt.Sprintf("policies=%d/%s/composite", policyCount, fixture.name), func(b *testing.B) {
				benchmarkAliasRedaction(b, fixture.raw, fixture.withMessage, func(event *opensplunkv1.LogEvent) {
					RedactTopLevelAliasesInPlace(event, policies)
				})
			})
		}
	}
}

func supplementalBenchmarkResult(
	raw []byte,
	redact func(event *opensplunkv1.LogEvent),
) *opensplunkv1.LogEvent {
	message := string(raw)
	event := &opensplunkv1.LogEvent{
		Raw:         raw,
		RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		Message:     &message,
	}
	redact(event)
	return event
}

func benchmarkSupplementalRedaction(
	b *testing.B,
	raw []byte,
	redact func(event *opensplunkv1.LogEvent),
) {
	b.Helper()
	event := &opensplunkv1.LogEvent{
		RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
	}
	message := string(raw)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw) * 2))
	b.ResetTimer()
	for range b.N {
		event.Raw = raw
		event.Message = &message
		redact(event)
	}
}

func aliasBenchmarkResult(
	b testing.TB,
	raw []byte,
	withMessage bool,
	redact func(event *opensplunkv1.LogEvent),
) *opensplunkv1.LogEvent {
	b.Helper()
	event := &opensplunkv1.LogEvent{
		Raw:         raw,
		RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
	}
	if withMessage {
		message := string(raw)
		event.Message = &message
	}
	redact(event)
	return event
}

func benchmarkAliasRedaction(
	b *testing.B,
	raw []byte,
	withMessage bool,
	redact func(event *opensplunkv1.LogEvent),
) {
	b.Helper()
	event := &opensplunkv1.LogEvent{
		RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
	}
	message := string(raw)
	b.ReportAllocs()
	byteCount := len(raw)
	if withMessage {
		byteCount *= 2
	}
	b.SetBytes(int64(byteCount))
	b.ResetTimer()
	for range b.N {
		event.Raw = raw
		event.Message = nil
		if withMessage {
			messageCopy := message
			event.Message = &messageCopy
		}
		redact(event)
	}
}

func compositeBenchmarkText(policyCount, targetBytes int) []byte {
	var raw bytes.Buffer
	raw.Grow(targetBytes + 128)
	for index := range policyCount {
		fmt.Fprintf(&raw, "secret_%02d=value-%02d ", index, index)
	}
	for raw.Len() < targetBytes-len("safe=value ") {
		raw.WriteString("safe=value ")
	}
	return raw.Bytes()
}

func compositeBenchmarkTextReverse(policyCount, targetBytes int) []byte {
	var raw bytes.Buffer
	raw.Grow(targetBytes + 128)
	for index := policyCount - 1; index >= 0; index-- {
		fmt.Fprintf(&raw, "secret_%02d=value-%02d ", index, index)
	}
	for raw.Len() < targetBytes-len("safe=value ") {
		raw.WriteString("safe=value ")
	}
	return raw.Bytes()
}

func compositeBenchmarkJSON(policyCount, targetBytes int) []byte {
	var raw bytes.Buffer
	raw.Grow(targetBytes + 128)
	raw.WriteByte('{')
	for index := range policyCount {
		if index > 0 {
			raw.WriteByte(',')
		}
		raw.WriteString(strconv.Quote(fmt.Sprintf("secret_%02d", index)))
		raw.WriteString(`:"secret-value"`)
	}
	raw.WriteString(`,"safe":"`)
	for raw.Len() < targetBytes-2 {
		raw.WriteByte('x')
	}
	raw.WriteString(`"}`)
	return raw.Bytes()
}
