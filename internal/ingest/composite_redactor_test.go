package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

type supplementalRedactionCase struct {
	name     string
	policies []RedactionPolicy
	event    func(t *testing.T) *opensplunkv1.LogEvent
}

func TestSequentialSupplementalRedactionGolden(t *testing.T) {
	t.Parallel()

	hash := sha256.New()
	var size [8]byte
	for _, test := range supplementalRedactionCases() {
		test := test
		t.Run(test.name, func(t *testing.T) {
			event := applySequentialSupplementalRedaction(t, test.policies, test.event(t))
			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(event)
			if err != nil {
				t.Fatalf("marshal legacy result: %v", err)
			}
			binary.BigEndian.PutUint64(size[:], uint64(len(encoded)))
			hash.Write(size[:])
			hash.Write(encoded)
		})
	}

	const want = "0d16bdbb495dc5d054975f9a46431c4df97fb0a7d7c6309146f6ca127b7effa1"
	if got := hex.EncodeToString(hash.Sum(nil)); got != want {
		t.Fatalf("sequential supplemental redaction corpus SHA-256 = %s, want %s", got, want)
	}
}

func TestCompositeSupplementalRedactorMatchesSequentialPolicies(t *testing.T) {
	t.Parallel()

	for _, test := range supplementalRedactionCases() {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), test.policies)
			if err != nil {
				t.Fatalf("construct composite: %v", err)
			}
			input := test.event(t)
			want := applySequentialSupplementalRedaction(
				t,
				test.policies,
				proto.Clone(input).(*opensplunkv1.LogEvent),
			)
			got := composite.RedactEventInPlace(proto.Clone(input).(*opensplunkv1.LogEvent))
			wantBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(want)
			if err != nil {
				t.Fatalf("marshal sequential result: %v", err)
			}
			gotBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(got)
			if err != nil {
				t.Fatalf("marshal composite result: %v", err)
			}
			if !bytes.Equal(gotBytes, wantBytes) {
				t.Fatalf(
					"composite result differs from sequential oracle:\n got: %+v\nwant: %+v",
					got,
					want,
				)
			}
		})
	}
}

func TestSequentialSupplementalRedactionOrderingRegressions(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		policies []RedactionPolicy
		raw      string
		want     string
	}{
		{
			name: "earlier outer assignment consumes later inner assignment",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "<FIRST>"},
				{AdditionalSensitiveFields: []string{"beta"}, Replacement: "[SECOND]"},
			},
			raw:  `alpha=" beta=0"`,
			want: `alpha="<FIRST>"`,
		},
		{
			name: "policy order beats text order for embedded JSON",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "<FIRST>"},
				{AdditionalSensitiveFields: []string{"beta"}, Replacement: "[SECOND]"},
			},
			raw:  `first="{\"beta\":\"b\"}" second="{\"alpha\":\"a\"}"`,
			want: `<FIRST>`,
		},
		{
			name: "policy order beats text order for ambiguous encoded values",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "<FIRST>"},
				{AdditionalSensitiveFields: []string{"beta"}, Replacement: "[SECOND]"},
			},
			raw:  `beta=\"b\" alpha=\"a\"`,
			want: `<FIRST>`,
		},
		{
			name: "generated marker can become a later quoted key",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "beta"},
				{AdditionalSensitiveFields: []string{"beta"}, Replacement: "FINAL"},
			},
			raw:  `alpha="old"=original-secret`,
			want: `alpha="beta"="FINAL"`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			event := validTestEvent("supplemental-ordering-regression", "main")
			event.Raw = []byte(test.raw)
			event.Message = nil
			event.Fields = nil
			got := applySequentialSupplementalRedaction(t, test.policies, event)
			if string(got.GetRaw()) != test.want {
				t.Fatalf("sequential raw = %q, want %q", got.GetRaw(), test.want)
			}
		})
	}
}

func TestCompositeSupplementalRedactorDoesNotHideEarlierFailClosedBoundary(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		laterField string
		raw        string
	}{
		{
			name:       "generic composite value",
			laterField: "beta",
			raw:        `beta={alpha:\"secret\"}`,
		},
		{
			name:       "cookie folded line",
			laterField: "cookie",
			raw:        `cookie=safe alpha=\"secret\"`,
		},
		{
			name:       "authorization folded line",
			laterField: "authorization",
			raw:        `authorization=Custom alpha=\"secret\"`,
		},
		{
			name:       "private key physical line",
			laterField: "private_key",
			raw:        `private_key=prefix alpha=\"secret\"`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			policies := []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "<FIRST>"},
				{AdditionalSensitiveFields: []string{test.laterField}, Replacement: "[SECOND]"},
			}
			composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), policies)
			if err != nil {
				t.Fatal(err)
			}
			input := validTestEvent("composite-hidden-boundary", "main")
			input.Raw = []byte(test.raw)
			input.Message = nil
			input.Fields = nil
			got := composite.RedactEventInPlace(proto.Clone(input).(*opensplunkv1.LogEvent))
			want := applySequentialSupplementalRedaction(t, policies, input)
			if string(got.GetRaw()) != "<FIRST>" {
				t.Fatalf("composite raw = %q, want earlier fail-closed marker", got.GetRaw())
			}
			if !proto.Equal(got, want) {
				t.Fatalf("composite result differs from sequential result:\n got: %+v\nwant: %+v", got, want)
			}
		})
	}
}

func TestCompositeSupplementalRedactorPreservesPEMExtentMarkerInteraction(t *testing.T) {
	t.Parallel()

	policies := []RedactionPolicy{
		{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "-----BEGIN PRIVATE KEY-----"},
		{AdditionalSensitiveFields: []string{"private_key"}, Replacement: "[SECOND]"},
	}
	composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), policies)
	if err != nil {
		t.Fatal(err)
	}
	raw := "private_key=preamble alpha=planted-secret\n" +
		"key-material-must-not-survive\n" +
		"-----END PRIVATE KEY-----\n" +
		"safe=value"
	input := validTestEvent("composite-pem-marker-interaction", "main")
	input.Raw = []byte(raw)
	input.Message = nil
	input.Fields = nil
	got := composite.RedactEventInPlace(proto.Clone(input).(*opensplunkv1.LogEvent))
	want := applySequentialSupplementalRedaction(t, policies, input)
	if !proto.Equal(got, want) {
		t.Fatalf("composite PEM result differs from sequential result:\n got: %+v\nwant: %+v", got, want)
	}
	if bytes.Contains(got.GetRaw(), []byte("planted-secret")) ||
		bytes.Contains(got.GetRaw(), []byte("key-material-must-not-survive")) {
		t.Fatalf("composite PEM result leaked planted secret: %q", got.GetRaw())
	}
}

func TestCompositeSupplementalRedactorReplaysEarlierMatchThatCanExposeLaterKey(t *testing.T) {
	t.Parallel()

	policies := []RedactionPolicy{
		{AdditionalSensitiveFields: []string{"beta"}, Replacement: "[SECOND]"},
		{AdditionalSensitiveFields: []string{"gamma"}, Replacement: "THIRD"},
	}
	composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), policies)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		raw      []byte
		encoding opensplunkv1.RawEncoding
	}{
		{
			name:     "UTF8 text",
			raw:      []byte(`beta={"x":"y"}gamma"=]SECRET333Z}`),
			encoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		},
		{
			name: "binary text",
			raw: append(
				[]byte{0xff, ' '},
				[]byte(`beta={"x":"y"}gamma"=]SECRET333Z}`)...,
			),
			encoding: opensplunkv1.RawEncoding_RAW_ENCODING_BINARY,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			input := validTestEvent("composite-generated-boundary", "main")
			input.Raw = bytes.Clone(test.raw)
			input.RawEncoding = test.encoding
			input.Message = nil
			input.Fields = nil
			got := composite.RedactEventInPlace(proto.Clone(input).(*opensplunkv1.LogEvent))
			want := applySequentialSupplementalRedaction(t, policies, input)
			if !proto.Equal(got, want) {
				t.Fatalf("composite result differs from sequential result:\n got: %+v\nwant: %+v", got, want)
			}
			if bytes.Contains(got.GetRaw(), []byte("SECRET333Z")) {
				t.Fatalf("composite result leaked planted suffix: %q", got.GetRaw())
			}
		})
	}
}

func TestCompositeSupplementalRedactorHonorsLaterDepthPolicyAfterDirectKeyMatch(t *testing.T) {
	t.Parallel()

	policies := []RedactionPolicy{
		{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "FIRST_DEPTH"},
		{AdditionalSensitiveFields: []string{"beta"}, Replacement: "LAST_DEPTH"},
	}
	composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), policies)
	if err != nil {
		t.Fatal(err)
	}
	deep := `{"alpha":"depth-secret"}`
	for range maxEmbeddedJSONRedactionDepth {
		encoded, marshalErr := json.Marshal(deep)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		deep = string(encoded)
	}
	input := validTestEvent("composite-direct-key-depth-bound", "main")
	input.Raw = []byte(deep)
	input.Message = &deep
	input.Fields = object(stringField("deep", deep))
	got := composite.RedactEventInPlace(proto.Clone(input).(*opensplunkv1.LogEvent))
	want := applySequentialSupplementalRedaction(t, policies, input)
	if !proto.Equal(got, want) {
		t.Fatalf("composite depth result differs from sequential result:\n got: %+v\nwant: %+v", got, want)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wire, []byte("depth-secret")) ||
		bytes.Contains(wire, []byte("FIRST_DEPTH")) ||
		!bytes.Contains(wire, []byte("LAST_DEPTH")) {
		t.Fatalf("depth-bound result did not retain the final policy marker: %+v", got)
	}
}

func TestCompositeSupplementalRedactorChoosesCompatibilityFallbackOnlyWhenNeeded(t *testing.T) {
	t.Parallel()

	ordinary, err := NewCompositeSupplementalRedactor(DefaultLimits(), []RedactionPolicy{
		{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "<FIRST>"},
		{AdditionalSensitiveFields: []string{"beta"}, Replacement: "[SECOND]"},
		{AdditionalSensitiveFields: []string{"third"}, Replacement: "***"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ordinary.sequential) != 0 || len(ordinary.ordered) != 3 {
		t.Fatalf(
			"ordinary composite path = sequential:%d ordered:%d, want 0/3",
			len(ordinary.sequential),
			len(ordinary.ordered),
		)
	}
	finalSyntax, err := NewCompositeSupplementalRedactor(DefaultLimits(), []RedactionPolicy{
		{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "<FIRST>"},
		{AdditionalSensitiveFields: []string{"beta"}, Replacement: "MASK:2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(finalSyntax.sequential) != 0 {
		t.Fatalf("final syntax-bearing marker selected compatibility fallback")
	}

	for _, test := range []struct {
		name     string
		policies []RedactionPolicy
	}{
		{
			name: "syntax-bearing generated marker",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "beta=generated"},
				{AdditionalSensitiveFields: []string{"beta"}, Replacement: "FINAL"},
			},
		},
		{
			name: "repeated exact field",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"repeat"}, Replacement: "OLD"},
				{AdditionalSensitiveFields: []string{"repeat"}, Replacement: "NEW"},
			},
		},
		{
			name: "generated marker becomes later exact field",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "beta"},
				{AdditionalSensitiveFields: []string{"beta"}, Replacement: "FINAL"},
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), test.policies)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(composite.sequential); got != len(test.policies) {
				t.Fatalf("compatibility fallback validators = %d, want %d", got, len(test.policies))
			}
		})
	}
}

func TestNewCompositeSupplementalRedactorValidatesEveryPolicy(t *testing.T) {
	t.Parallel()

	if _, err := NewCompositeSupplementalRedactor(DefaultLimits(), nil); err == nil {
		t.Fatal("empty composite policy list was accepted")
	}
	if _, err := NewCompositeSupplementalRedactor(DefaultLimits(), []RedactionPolicy{{
		Replacement: "MASKED",
	}}); err == nil {
		t.Fatal("composite policy without fields was accepted")
	}
	if _, err := NewCompositeSupplementalRedactor(DefaultLimits(), []RedactionPolicy{{
		AdditionalSensitiveFields: []string{"alpha"},
		Replacement:               string([]byte{0xff}),
	}}); err == nil {
		t.Fatal("composite policy with invalid UTF-8 replacement was accepted")
	}
	limits := DefaultLimits()
	limits.MaxFieldNameBytes = 0
	if _, err := NewCompositeSupplementalRedactor(limits, []RedactionPolicy{{
		AdditionalSensitiveFields: []string{"alpha"},
		Replacement:               "MASKED",
	}}); err == nil {
		t.Fatal("composite policy with invalid limits was accepted")
	}
}

func TestCompositeSupplementalRedactorUsesPerFieldValueExtent(t *testing.T) {
	t.Parallel()

	composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), []RedactionPolicy{
		{AdditionalSensitiveFields: []string{"authorization"}, Replacement: "<AUTH>"},
		{AdditionalSensitiveFields: []string{"cookie"}, Replacement: "<COOKIE>"},
		{AdditionalSensitiveFields: []string{"private_key"}, Replacement: "<KEY>"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "authorization scheme and credential",
			raw:  "authorization=Bearer abc.def trailing=safe",
			want: `authorization="<AUTH>" trailing=safe`,
		},
		{
			name: "cookie consumes folded header line",
			raw:  "cookie=session=secret; theme=dark\nsafe=value",
			want: "cookie=\"<COOKIE>\"\nsafe=value",
		},
		{
			name: "private key consumes complete PEM block",
			raw: "private_key=-----BEGIN PRIVATE KEY-----\n" +
				"planted-private-key-secret\n" +
				"-----END PRIVATE KEY-----\n" +
				"safe=value",
			want: "private_key=\"<KEY>\"\nsafe=value",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := string(composite.redactKeyValueText([]byte(test.raw))); got != test.want {
				t.Fatalf("redacted raw = %q, want %q", got, test.want)
			}
		})
	}
}

func TestCompositeSupplementalRedactorReevaluatesBinaryUTF8BetweenPolicies(t *testing.T) {
	t.Parallel()

	policies := []RedactionPolicy{
		{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "<FIRST>"},
		{AdditionalSensitiveFields: []string{"beta"}, Replacement: "[SECOND]"},
	}
	composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), policies)
	if err != nil {
		t.Fatal(err)
	}
	input := validTestEvent("composite-binary-becomes-utf8", "main")
	input.RawEncoding = opensplunkv1.RawEncoding_RAW_ENCODING_BINARY
	input.Raw = []byte{'a', 'l', 'p', 'h', 'a', '=', 0x95, ' ', 'b', 'e', 't', 'a', '=', '0', '"'}
	input.Message = nil
	input.Fields = nil

	got := composite.RedactEventInPlace(proto.Clone(input).(*opensplunkv1.LogEvent))
	if string(got.GetRaw()) != "[SECOND]" {
		t.Fatalf("composite raw = %q, want policy-ordered [SECOND]", got.GetRaw())
	}
	want := applySequentialSupplementalRedaction(t, policies, input)
	if !proto.Equal(got, want) {
		t.Fatalf("composite result differs from sequential result:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestCompositeSupplementalRedactorIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	policies := []RedactionPolicy{
		{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "<FIRST>"},
		{AdditionalSensitiveFields: []string{"beta"}, Replacement: "[SECOND]"},
		{AdditionalSensitiveFields: []string{"third"}, Replacement: "***"},
	}
	composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), policies)
	if err != nil {
		t.Fatal(err)
	}
	input := validTestEvent("composite-concurrent", "main")
	input.Raw = []byte(`{"alpha":"raw-alpha","nested":{"beta":"raw-beta"},"safe":"kept"}`)
	message := "alpha=message-alpha beta=message-beta safe=value"
	input.Message = &message
	input.Fields = object(
		stringField("alpha", "typed-alpha"),
		objectField("nested", object(stringField("beta", "typed-beta"))),
	)
	want := applySequentialSupplementalRedaction(
		t,
		policies,
		proto.Clone(input).(*opensplunkv1.LogEvent),
	)
	wantBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 32
	start := make(chan struct{})
	errs := make(chan error, goroutines)
	var workers sync.WaitGroup
	workers.Add(goroutines)
	for range goroutines {
		go func() {
			defer workers.Done()
			<-start
			for range 20 {
				got := composite.RedactEventInPlace(proto.Clone(input).(*opensplunkv1.LogEvent))
				gotBytes, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(got)
				if marshalErr != nil {
					errs <- marshalErr
					return
				}
				if !bytes.Equal(gotBytes, wantBytes) {
					errs <- fmt.Errorf("concurrent composite result differs from sequential oracle")
					return
				}
			}
		}()
	}
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestTopLevelAliasCompositeMatchesSequentialTextGroups(t *testing.T) {
	t.Parallel()

	policies := []TopLevelAliasRedaction{
		{Field: "alpha", Replacement: "<FIRST>"},
		{Field: "beta", Replacement: "[SECOND]"},
		{Field: "constant", Replacement: "***", StructuredOnly: true},
	}
	for _, test := range []struct {
		name string
		raw  string
	}{
		{
			name: "plain text",
			raw:  `alpha=raw-alpha beta='raw-beta' constant=raw-safe safe=value`,
		},
		{
			name: "valid root JSON",
			raw:  `{"alpha":"raw-alpha","beta":"raw-beta","constant":"raw-safe","nested":{"alpha":"nested-safe"}}`,
		},
		{
			name: "earlier marker exposes later key boundary",
			raw:  `alpha={"x":"y"}beta"=]alias-secret-must-not-survive}`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			message := `alpha=message-alpha beta='message-beta' constant=message-safe safe=value`
			input := &opensplunkv1.LogEvent{
				Message:     &message,
				Raw:         []byte(test.raw),
				RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
				Fields: object(
					stringField("alpha", "typed-alpha"),
					stringField("beta", "typed-beta"),
					stringField("constant", "typed-constant"),
					objectField("nested", object(stringField("alpha", "nested-safe"))),
				),
			}
			want := legacyTopLevelAliasRedaction(
				t,
				proto.Clone(input).(*opensplunkv1.LogEvent),
				policies,
			)
			got := RedactTopLevelAliasesInPlace(
				proto.Clone(input).(*opensplunkv1.LogEvent),
				policies,
			)
			wantBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			gotBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(gotBytes, wantBytes) {
				t.Fatalf("composite alias result differs:\n got: %+v\nwant: %+v", got, want)
			}
			if bytes.Contains(got.GetRaw(), []byte("alias-secret-must-not-survive")) {
				t.Fatalf("composite alias result leaked generated-boundary suffix: %q", got.GetRaw())
			}
		})
	}
}

func TestTopLevelAliasCompatibilityFallbackMatchesSequentialTextGroups(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		policies []TopLevelAliasRedaction
		raw      string
		wantRaw  string
	}{
		{
			name: "syntax-bearing generated marker",
			policies: []TopLevelAliasRedaction{
				{Field: "alpha", Replacement: "beta=generated"},
				{Field: "beta", Replacement: "FINAL"},
			},
			raw:     "alpha=raw-secret",
			wantRaw: `alpha="beta="FINAL"`,
		},
		{
			name: "literal empty replacement",
			policies: []TopLevelAliasRedaction{
				{Field: "alpha", Replacement: ""},
				{Field: "beta", Replacement: "FINAL"},
			},
			raw:     "alpha=raw-secret safe=value",
			wantRaw: `alpha="" safe=value`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			message := test.raw
			input := &opensplunkv1.LogEvent{
				Message:     &message,
				Raw:         []byte(test.raw),
				RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
				Fields: object(
					stringField("alpha", "typed-alpha"),
					stringField("beta", "typed-beta"),
				),
			}
			want := legacyTopLevelAliasRedaction(
				t,
				proto.Clone(input).(*opensplunkv1.LogEvent),
				test.policies,
			)
			got := RedactTopLevelAliasesInPlace(
				proto.Clone(input).(*opensplunkv1.LogEvent),
				test.policies,
			)
			wantBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			gotBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(gotBytes, wantBytes) {
				t.Fatalf("composite alias compatibility result differs:\n got: %+v\nwant: %+v", got, want)
			}
			if string(got.GetRaw()) != test.wantRaw {
				t.Fatalf("compatibility raw = %q, want %q", got.GetRaw(), test.wantRaw)
			}
		})
	}
}

func legacyTopLevelAliasRedaction(
	t testing.TB,
	event *opensplunkv1.LogEvent,
	policies []TopLevelAliasRedaction,
) *opensplunkv1.LogEvent {
	t.Helper()
	structured := make(map[string]string, len(policies))
	raw := make(map[string]string, len(policies))
	var redactors []*Validator
	groupIndexes := make(map[string]int)
	groupFields := make([][]string, 0)
	for _, policy := range policies {
		structured[policy.Field] = policy.Replacement
		if policy.StructuredOnly {
			continue
		}
		raw[policy.Field] = policy.Replacement
		index, exists := groupIndexes[policy.Replacement]
		if !exists {
			index = len(groupFields)
			groupIndexes[policy.Replacement] = index
			groupFields = append(groupFields, nil)
		}
		groupFields[index] = append(groupFields[index], policy.Field)
	}
	for index, fields := range groupFields {
		var replacement string
		for marker, markerIndex := range groupIndexes {
			if markerIndex == index {
				replacement = marker
				break
			}
		}
		fieldSet := make(map[string]struct{}, len(fields))
		for _, field := range fields {
			fieldSet[field] = struct{}{}
		}
		redactors = append(redactors, &Validator{
			limits:      DefaultLimits(),
			replacement: replacement,
			sensitive:   fieldSet,
			exact:       true,
		})
	}

	redactTopLevelObjectWithReplacements(event.GetFields(), structured)
	if len(raw) == 0 {
		return event
	}
	if event.GetRawEncoding() == opensplunkv1.RawEncoding_RAW_ENCODING_UTF8 ||
		utf8.Valid(event.GetRaw()) {
		if redacted, parsed := redactTopLevelJSONWithReplacements(event.GetRaw(), raw); parsed {
			event.Raw = redacted
		} else {
			for _, redactor := range redactors {
				event.Raw = redactor.redactKeyValueText(event.GetRaw())
			}
		}
	} else {
		for _, redactor := range redactors {
			event.Raw = redactor.redactKeyValueText(event.GetRaw())
		}
	}
	if event.Message != nil {
		message := []byte(event.GetMessage())
		for _, redactor := range redactors {
			message = redactor.redactText(message)
		}
		text := string(message)
		event.Message = &text
	}
	return event
}

func applySequentialSupplementalRedaction(
	t *testing.T,
	policies []RedactionPolicy,
	event *opensplunkv1.LogEvent,
) *opensplunkv1.LogEvent {
	t.Helper()
	for index, policy := range policies {
		redactor, err := NewSupplementalRedactor(DefaultLimits(), policy)
		if err != nil {
			t.Fatalf("construct sequential policy %d: %v", index, err)
		}
		event = redactor.RedactEventInPlace(event)
	}
	return event
}

func supplementalRedactionCases() []supplementalRedactionCase {
	ordinary := []RedactionPolicy{
		{
			AdditionalSensitiveFields: []string{"alpha", "customerCredential", "a:b", "api key"},
			Replacement:               "<FIRST>",
		},
		{
			AdditionalSensitiveFields: []string{"beta", "customer_credential"},
			Replacement:               "[SECOND]",
		},
		{
			AdditionalSensitiveFields: []string{"third"},
			Replacement:               "THIRD",
		},
	}
	return []supplementalRedactionCase{
		{
			name:     "safe JSON remains byte exact",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				event := validTestEvent("composite-safe-json", "main")
				event.Raw = []byte("{ \n  \"message\": \"safe\", \"status\": 200\n}\n")
				message := "ordinary safe message"
				event.Message = &message
				event.Fields = object(stringField("safe", "kept"))
				return event
			},
		},
		{
			name:     "valid JSON nested typed fields and exact lookalikes",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				event := validTestEvent("composite-valid-json", "main")
				event.Raw = []byte(
					`{"alpha":"raw-alpha","nested":{"customer_credential":"raw-snake"},` +
						`"CustomerCredential":"case-safe","customer-credential":"separator-safe",` +
						`"a:b":{"secret":"raw-punctuation"},` +
						`"note":"alpha=string-alpha beta='string-beta' safe=value",` +
						`"n":9007199254740993}`,
				)
				message := `alpha=message-alpha customer_credential="message-snake" safe=value`
				event.Message = &message
				event.Fields = object(
					stringField("alpha", "typed-alpha"),
					objectField("nested", object(
						stringField("customer_credential", "typed-snake"),
						stringField("CustomerCredential", "typed-case-safe"),
					)),
					stringField("note", `a:b=typed-string-punctuation safe=value`),
				)
				return event
			},
		},
		{
			name:     "duplicate JSON keys canonicalize with last member semantics",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				event := validTestEvent("composite-duplicate-json", "main")
				event.Raw = []byte(
					`{"alpha":"shadow-alpha","alpha":"<FIRST>",` +
						`"outer":{"customer_credential":"shadow-snake"},"outer":"safe",` +
						`"visible":9007199254740993}`,
				)
				message := "safe"
				event.Message = &message
				event.Fields = object(stringField("safe", "kept"))
				return event
			},
		},
		{
			name:     "plain key value text uses each exact marker",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				event := validTestEvent("composite-plain-text", "main")
				event.Raw = []byte(`alpha=plain-alpha beta='plain-beta' api key=plain-space safe=value`)
				message := `customerCredential:"message-camel" a:b=message-punctuation safe=value`
				event.Message = &message
				event.Fields = object(
					stringField("note", `alpha=typed-alpha beta=typed-beta safe=value`),
				)
				return event
			},
		},
		{
			name:     "later assignment text inside earlier quoted value stays consumed",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				raw := `alpha=" beta=0"`
				event := validTestEvent("composite-quoted-contained-assignment", "main")
				event.Raw = []byte(raw)
				event.Message = &raw
				event.Fields = object(stringField("note", raw))
				return event
			},
		},
		{
			name:     "policy order beats text order for embedded JSON",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				raw := `first="{\"beta\":\"b\"}" second="{\"alpha\":\"a\"}"`
				event := validTestEvent("composite-embedded-policy-order", "main")
				event.Raw = []byte(raw)
				event.Message = &raw
				event.Fields = object(stringField("note", raw))
				return event
			},
		},
		{
			name:     "policy order beats text order for ambiguous encoded values",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				raw := `beta=\"b\" alpha=\"a\"`
				event := validTestEvent("composite-ambiguous-policy-order", "main")
				event.Raw = []byte(raw)
				event.Message = &raw
				event.Fields = object(stringField("note", raw))
				return event
			},
		},
		{
			name:     "prose wrapped embedded JSON keeps the outer safe sibling",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				note := `failed payload="{\"customer_credential\":\"embedded-snake\"}"`
				raw, err := json.Marshal(map[string]string{"note": note, "safe": "yes"})
				if err != nil {
					t.Fatal(err)
				}
				event := validTestEvent("composite-embedded-json", "main")
				event.Raw = raw
				event.Message = &note
				event.Fields = object(stringField("note", note))
				return event
			},
		},
		{
			name:     "ambiguous encoded value fails closed with matching marker",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				raw := `customer_credential=\"ambiguous-snake\" safe=value`
				event := validTestEvent("composite-ambiguous-value", "main")
				event.Raw = []byte(raw)
				event.Message = &raw
				event.Fields = object(stringField("note", raw))
				return event
			},
		},
		{
			name:     "encoded JSON depth bound preserves ordered fallback",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				deep := `{"alpha":"depth-secret"}`
				for range maxEmbeddedJSONRedactionDepth + 1 {
					encoded, err := json.Marshal(deep)
					if err != nil {
						t.Fatal(err)
					}
					deep = string(encoded)
				}
				event := validTestEvent("composite-depth-bound", "main")
				event.Raw = []byte(deep)
				event.Message = &deep
				event.Fields = object(stringField("deep", deep))
				return event
			},
		},
		{
			name:     "invalid UTF8 raw retains unrelated binary bytes",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				event := validTestEvent("composite-binary", "main")
				event.RawEncoding = opensplunkv1.RawEncoding_RAW_ENCODING_BINARY
				event.Raw = append([]byte{0xff, 0x00, ' '}, []byte(`a:b=binary-secret safe=value`)...)
				event.Message = nil
				event.Fields = object(stringField("safe", "kept"))
				return event
			},
		},
		{
			name:     "binary payload becomes UTF8 between ordered policies",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				event := validTestEvent("composite-binary-becomes-utf8", "main")
				event.RawEncoding = opensplunkv1.RawEncoding_RAW_ENCODING_BINARY
				event.Raw = []byte{
					'a', 'l', 'p', 'h', 'a', '=', 0x95, ' ',
					'b', 'e', 't', 'a', '=', '0', '"',
				}
				event.Message = nil
				event.Fields = object(stringField("safe", "kept"))
				return event
			},
		},
		{
			name:     "typed lists and duplicate fields redact recursively",
			policies: ordinary,
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				event := validTestEvent("composite-typed-list", "main")
				event.Raw = []byte(`{"safe":"kept"}`)
				message := "safe"
				event.Message = &message
				event.Fields = object(
					stringField("alpha", "first-duplicate"),
					stringField("alpha", "second-duplicate"),
					&opensplunkv1.TypedObjectField{
						Name: "items",
						Value: &opensplunkv1.TypedValue{
							Kind: &opensplunkv1.TypedValue_ListValue{
								ListValue: &opensplunkv1.TypedValueList{Values: []*opensplunkv1.TypedValue{
									{
										Kind: &opensplunkv1.TypedValue_ObjectValue{
											ObjectValue: object(stringField("customer_credential", "list-snake")),
										},
									},
									{
										Kind: &opensplunkv1.TypedValue_StringValue{
											StringValue: `beta=list-string-secret safe=value`,
										},
									},
								}},
							},
						},
					},
				)
				return event
			},
		},
		{
			name: "later policy rewrites an earlier generated marker",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "beta=generated-secret"},
				{AdditionalSensitiveFields: []string{"beta"}, Replacement: "FINAL"},
			},
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				event := validTestEvent("composite-marker-cascade", "main")
				event.Raw = []byte(`{"alpha":"raw-alpha","safe":"kept"}`)
				message := "alpha=message-alpha safe=value"
				event.Message = &message
				event.Fields = object(
					stringField("alpha", "typed-alpha"),
					stringField("note", "alpha=typed-string-alpha safe=value"),
				)
				return event
			},
		},
		{
			name: "plain generated marker becomes a later quoted key",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "beta"},
				{AdditionalSensitiveFields: []string{"beta"}, Replacement: "FINAL"},
			},
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				raw := `alpha="old"=original-secret`
				event := validTestEvent("composite-contextual-marker-cascade", "main")
				event.Raw = []byte(raw)
				event.Message = &raw
				event.Fields = object(stringField("note", raw))
				return event
			},
		},
		{
			name: "last sequential policy wins for a repeated exact field",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"repeat"}, Replacement: "OLD"},
				{AdditionalSensitiveFields: []string{"repeat"}, Replacement: "NEW"},
			},
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				event := validTestEvent("composite-repeated-field", "main")
				event.Raw = []byte(`{"repeat":"raw-repeat","safe":"kept"}`)
				message := "repeat=message-repeat safe=value"
				event.Message = &message
				event.Fields = object(stringField("repeat", "typed-repeat"))
				return event
			},
		},
		{
			name: "large ambiguous encoded key uses first ordered fallback",
			policies: []RedactionPolicy{
				{AdditionalSensitiveFields: []string{"alpha"}, Replacement: "FIRST"},
				{AdditionalSensitiveFields: []string{"beta"}, Replacement: "SECOND"},
			},
			event: func(t *testing.T) *opensplunkv1.LogEvent {
				t.Helper()
				ambiguous := fmt.Sprintf(
					`prefix {\"%s\":\"ambiguous-key-secret\"} safe=value`,
					bytes.Repeat([]byte{'a'}, int(DefaultLimits().MaxFieldNameBytes)+1),
				)
				event := validTestEvent("composite-ambiguous-key", "main")
				event.Raw = []byte(ambiguous)
				event.Message = &ambiguous
				event.Fields = object(stringField("note", ambiguous))
				return event
			},
		},
	}
}

func FuzzTopLevelAliasCompositeMatchesSequentialTextGroups(f *testing.F) {
	policies := []TopLevelAliasRedaction{
		{Field: "alpha", Replacement: "<FIRST>"},
		{Field: "beta", Replacement: "[SECOND]"},
		{Field: "constant", Replacement: "***", StructuredOnly: true},
	}
	for _, seed := range [][]byte{
		[]byte(`{"alpha":"one","beta":"two","constant":"raw-safe","safe":"kept"}`),
		[]byte(`alpha=one beta='two' constant=raw-safe safe=value`),
		[]byte(`failed payload="{\"beta\":\"secret\"}"`),
		{'{', '"', 'b', 'e', 't', 'a', '"', ':', '"', '"', ',', '"', 0xda, '"', ':', '"', '"', '}'},
		append([]byte{0xff, 0x00, ' '}, []byte(`alpha=binary-secret safe=value`)...),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		input := &opensplunkv1.LogEvent{
			Raw:         bytes.Clone(raw),
			RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_BINARY,
			Fields: object(
				stringField("alpha", "typed-alpha"),
				stringField("beta", "typed-beta"),
				stringField("constant", "typed-constant"),
				objectField("nested", object(stringField("alpha", "nested-safe"))),
			),
		}
		if utf8.Valid(raw) {
			message := string(raw)
			input.Message = &message
		}
		want := legacyTopLevelAliasRedaction(
			t,
			proto.Clone(input).(*opensplunkv1.LogEvent),
			policies,
		)
		got := RedactTopLevelAliasesInPlace(
			proto.Clone(input).(*opensplunkv1.LogEvent),
			policies,
		)
		wantBytes, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(want)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		gotBytes, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(got)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if !bytes.Equal(gotBytes, wantBytes) {
			t.Fatalf("composite aliases differ from sequential oracle for raw %x", raw)
		}
	})
}

func FuzzCompositeSupplementalRedactorMatchesSequentialPolicies(f *testing.F) {
	policies := []RedactionPolicy{
		{AdditionalSensitiveFields: []string{"alpha", "a:b"}, Replacement: "<FIRST>"},
		{AdditionalSensitiveFields: []string{"beta", "customer_credential"}, Replacement: "[SECOND]"},
		{AdditionalSensitiveFields: []string{"third"}, Replacement: "***"},
	}
	composite, err := NewCompositeSupplementalRedactor(DefaultLimits(), policies)
	if err != nil {
		f.Fatal(err)
	}
	sequential := make([]*Validator, len(policies))
	for index, policy := range policies {
		sequential[index], err = NewSupplementalRedactor(DefaultLimits(), policy)
		if err != nil {
			f.Fatal(err)
		}
	}
	for _, seed := range [][]byte{
		[]byte(`{"alpha":"one","nested":{"beta":"two"},"safe":"kept"}`),
		[]byte(`alpha=one beta='two' a:b=three safe=value`),
		[]byte(`failed payload="{\"customer_credential\":\"secret\"}"`),
		[]byte(`alpha={"x":"y"}beta"=]fuzz-secret}`),
		append([]byte{0xff, 0x00, ' '}, []byte(`third=binary-secret safe=value`)...),
		{'a', 'l', 'p', 'h', 'a', '=', 0x95, ' ', 'b', 'e', 't', 'a', '=', '0', '"'},
		bytes.Repeat([]byte{'"', '\\', ':', '='}, 128),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		input := &opensplunkv1.LogEvent{
			Raw:         bytes.Clone(raw),
			RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_BINARY,
			Fields:      object(stringField("alpha", "typed-alpha"), stringField("beta", "typed-beta")),
		}
		if utf8.Valid(raw) {
			message := string(raw)
			input.Message = &message
			input.Fields.Fields = append(input.Fields.Fields, stringField("note", message))
		}
		want := proto.Clone(input).(*opensplunkv1.LogEvent)
		for _, redactor := range sequential {
			want = redactor.RedactEventInPlace(want)
		}
		got := composite.RedactEventInPlace(proto.Clone(input).(*opensplunkv1.LogEvent))
		wantBytes, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(want)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		gotBytes, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(got)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if !bytes.Equal(gotBytes, wantBytes) {
			t.Fatalf("composite differs from sequential oracle for raw %x", raw)
		}
	})
}
