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

func BenchmarkCompositeSupplementalRedactorSyntaxMarkerSafeMiss(b *testing.B) {
	safe := compositeSafeMissPayload()
	for _, syntaxBearing := range []bool{false, true} {
		policies := compositeSafeMissPolicies(syntaxBearing)
		sequential := make([]*Validator, len(policies))
		for index := range policies {
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
		name := "opaque_markers"
		if syntaxBearing {
			name = "first_marker_contains_colon"
		}
		want := supplementalBenchmarkResult(safe, func(event *opensplunkv1.LogEvent) {
			for _, redactor := range sequential {
				redactor.RedactEventInPlace(event)
			}
		})
		got := supplementalBenchmarkResult(safe, func(event *opensplunkv1.LogEvent) {
			composite.RedactEventInPlace(event)
		})
		if !proto.Equal(got, want) {
			b.Fatalf("%s safe-miss output differs from sequential output", name)
		}
		b.Run(name+"/sequential", func(b *testing.B) {
			benchmarkSupplementalRedaction(b, safe, func(event *opensplunkv1.LogEvent) {
				for _, redactor := range sequential {
					redactor.RedactEventInPlace(event)
				}
			})
		})
		b.Run(name+"/composite", func(b *testing.B) {
			benchmarkSupplementalRedaction(b, safe, func(event *opensplunkv1.LogEvent) {
				composite.RedactEventInPlace(event)
			})
		})
	}
}

func BenchmarkCompositeSupplementalRedactorSyntaxMarkerDuplicateJSONSafeMiss(b *testing.B) {
	safe := []byte(`{"safe":"first","safe":"last"}`)
	for _, policyCount := range []int{2, 8, compositeSafeMissPolicyCount} {
		policies := compositeSafeMissPolicies(true)[:policyCount]
		sequential := make([]*Validator, len(policies))
		for index := range policies {
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
		want := supplementalBenchmarkResult(safe, func(event *opensplunkv1.LogEvent) {
			for _, redactor := range sequential {
				redactor.RedactEventInPlace(event)
			}
		})
		got := supplementalBenchmarkResult(safe, func(event *opensplunkv1.LogEvent) {
			composite.RedactEventInPlace(event)
		})
		if !proto.Equal(got, want) {
			b.Fatalf("policies=%d duplicate JSON output differs from sequential output", policyCount)
		}
		b.Run(fmt.Sprintf("policies=%d/sequential", policyCount), func(b *testing.B) {
			benchmarkSupplementalRedaction(b, safe, func(event *opensplunkv1.LogEvent) {
				for _, redactor := range sequential {
					redactor.RedactEventInPlace(event)
				}
			})
		})
		b.Run(fmt.Sprintf("policies=%d/composite", policyCount), func(b *testing.B) {
			benchmarkSupplementalRedaction(b, safe, func(event *opensplunkv1.LogEvent) {
				composite.RedactEventInPlace(event)
			})
		})
	}
}

func BenchmarkCompositeSupplementalRedactorSyntaxMarkerHitOnly(b *testing.B) {
	for _, policyCount := range []int{2, 8, compositeSafeMissPolicyCount} {
		policies := compositeSafeMissPolicies(false)[:policyCount]
		policies[0] = RedactionPolicy{
			AdditionalSensitiveFields: []string{"alpha"},
			Replacement:               "beta=generated",
		}
		policies[1] = RedactionPolicy{
			AdditionalSensitiveFields: []string{"beta"},
			Replacement:               "FINAL",
		}
		sequential := make([]*Validator, len(policies))
		for index, policy := range policies {
			var err error
			sequential[index], err = NewSupplementalRedactor(DefaultLimits(), policy)
			if err != nil {
				b.Fatal(err)
			}
		}
		composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), policies)
		if err != nil {
			b.Fatal(err)
		}
		for _, fixture := range []struct {
			name     string
			newEvent func() *opensplunkv1.LogEvent
		}{
			{
				name: "raw_text",
				newEvent: func() *opensplunkv1.LogEvent {
					return &opensplunkv1.LogEvent{
						Raw:         []byte("alpha=raw-secret"),
						RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
					}
				},
			},
			{
				name: "message_text",
				newEvent: func() *opensplunkv1.LogEvent {
					message := "alpha=message-secret"
					return &opensplunkv1.LogEvent{Message: &message}
				},
			},
			{
				name: "valid_JSON",
				newEvent: func() *opensplunkv1.LogEvent {
					return &opensplunkv1.LogEvent{
						Raw:         []byte(`{"alpha":"raw-secret"}`),
						RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
					}
				},
			},
		} {
			fixture := fixture
			want := fixture.newEvent()
			for _, redactor := range sequential {
				redactor.RedactEventInPlace(want)
			}
			got := composite.RedactEventInPlace(fixture.newEvent())
			if !proto.Equal(got, want) {
				b.Fatalf(
					"policies=%d %s output differs from sequential output",
					policyCount,
					fixture.name,
				)
			}
			b.Run(
				fmt.Sprintf("policies=%d/%s/sequential", policyCount, fixture.name),
				func(b *testing.B) {
					benchmarkSupplementalEvent(b, fixture.newEvent, func(event *opensplunkv1.LogEvent) {
						for _, redactor := range sequential {
							redactor.RedactEventInPlace(event)
						}
					})
				},
			)
			b.Run(
				fmt.Sprintf("policies=%d/%s/composite", policyCount, fixture.name),
				func(b *testing.B) {
					benchmarkSupplementalEvent(b, fixture.newEvent, func(event *opensplunkv1.LogEvent) {
						composite.RedactEventInPlace(event)
					})
				},
			)
		}
	}
}

func BenchmarkCompositeSupplementalRedactorSyntaxMarkerSparseHit(b *testing.B) {
	policies := compositeSafeMissPolicies(false)
	policies[0] = RedactionPolicy{
		AdditionalSensitiveFields: []string{"alpha"},
		Replacement:               "beta=generated",
	}
	policies[1] = RedactionPolicy{
		AdditionalSensitiveFields: []string{"beta"},
		Replacement:               "FINAL",
	}
	sequential := make([]*Validator, len(policies))
	for index, policy := range policies {
		var err error
		sequential[index], err = NewSupplementalRedactor(DefaultLimits(), policy)
		if err != nil {
			b.Fatal(err)
		}
	}
	composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), policies)
	if err != nil {
		b.Fatal(err)
	}

	safe := bytes.Repeat([]byte("x=y "), (64<<10)/len("x=y "))
	safeMessage := string(safe)
	largeTypedValue := string(bytes.Repeat([]byte("x=y "), (1<<20)/len("x=y ")))
	for _, fixture := range []struct {
		name     string
		newEvent func() *opensplunkv1.LogEvent
	}{
		{
			name: "raw_hit_with_64KiB_safe_message",
			newEvent: func() *opensplunkv1.LogEvent {
				message := safeMessage
				return &opensplunkv1.LogEvent{
					Raw:         []byte("alpha=raw-secret"),
					RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
					Message:     &message,
				}
			},
		},
		{
			name: "message_hit_with_64KiB_safe_raw",
			newEvent: func() *opensplunkv1.LogEvent {
				message := "alpha=message-secret"
				return &opensplunkv1.LogEvent{
					Raw:         safe,
					RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
					Message:     &message,
				}
			},
		},
		{
			name: "nested_JSON_message_hit_with_64KiB_safe_raw",
			newEvent: func() *opensplunkv1.LogEvent {
				message := `{"note":"alpha=message-secret","safe":"kept"}`
				return &opensplunkv1.LogEvent{
					Raw:         safe,
					RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
					Message:     &message,
				}
			},
		},
		{
			name: "typed_hit_with_128KiB_safe_text",
			newEvent: func() *opensplunkv1.LogEvent {
				message := safeMessage
				return &opensplunkv1.LogEvent{
					Raw:         safe,
					RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
					Message:     &message,
					Fields:      object(stringField("alpha", "beta=generated")),
				}
			},
		},
		{
			name: "last_policy_typed_hit_with_1MiB_value",
			newEvent: func() *opensplunkv1.LogEvent {
				return &opensplunkv1.LogEvent{
					Fields: object(stringField("secret_31", largeTypedValue)),
				}
			},
		},
	} {
		fixture := fixture
		want := fixture.newEvent()
		for _, redactor := range sequential {
			redactor.RedactEventInPlace(want)
		}
		got := composite.RedactEventInPlace(fixture.newEvent())
		if !proto.Equal(got, want) {
			b.Fatalf("%s output differs from sequential output", fixture.name)
		}
		b.Run(fixture.name+"/sequential", func(b *testing.B) {
			benchmarkSupplementalEvent(b, fixture.newEvent, func(event *opensplunkv1.LogEvent) {
				for _, redactor := range sequential {
					redactor.RedactEventInPlace(event)
				}
			})
		})
		b.Run(fixture.name+"/composite", func(b *testing.B) {
			benchmarkSupplementalEvent(b, fixture.newEvent, func(event *opensplunkv1.LogEvent) {
				composite.RedactEventInPlace(event)
			})
		})
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

func BenchmarkTopLevelAliasRedactionSyntaxMarkerSafeMiss(b *testing.B) {
	safe := compositeSafeMissPayload()
	for _, syntaxBearing := range []bool{false, true} {
		policies := compositeSafeMissAliasPolicies(syntaxBearing)
		name := "opaque_markers"
		if syntaxBearing {
			name = "first_marker_contains_colon"
		}
		want := aliasBenchmarkResult(b, safe, true, func(event *opensplunkv1.LogEvent) {
			legacyTopLevelAliasRedaction(b, event, policies)
		})
		got := aliasBenchmarkResult(b, safe, true, func(event *opensplunkv1.LogEvent) {
			RedactTopLevelAliasesInPlace(event, policies)
		})
		if !proto.Equal(got, want) {
			b.Fatalf("%s safe-miss output differs from sequential output", name)
		}
		b.Run(name+"/sequential", func(b *testing.B) {
			benchmarkAliasRedaction(b, safe, true, func(event *opensplunkv1.LogEvent) {
				legacyTopLevelAliasRedaction(b, event, policies)
			})
		})
		b.Run(name+"/composite", func(b *testing.B) {
			benchmarkAliasRedaction(b, safe, true, func(event *opensplunkv1.LogEvent) {
				RedactTopLevelAliasesInPlace(event, policies)
			})
		})
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

func benchmarkSupplementalEvent(
	b *testing.B,
	newEvent func() *opensplunkv1.LogEvent,
	redact func(event *opensplunkv1.LogEvent),
) {
	b.Helper()
	sample := newEvent()
	byteCount := proto.Size(sample)
	b.ReportAllocs()
	b.SetBytes(int64(byteCount))
	b.ResetTimer()
	for range b.N {
		redact(newEvent())
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
