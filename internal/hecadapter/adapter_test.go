package hecadapter

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/hec"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"google.golang.org/protobuf/proto"
)

const adapterTestChannel = "123e4567-e89b-42d3-a456-426614174000"

func TestJSONConvertsConcatenatedEnvelopeEventKindsExactly(t *testing.T) {
	t.Parallel()
	receivedAt := time.Date(2026, time.August, 10, 18, 19, 20, 987654321, time.FixedZone("test", -7*60*60))
	context := adapterTestContext(receivedAt, false)
	body := strings.Join([]string{
		`{"time":1700000000.123456789,"event":"line\none"}`,
		`{"event":1.2300e+4}`,
		`{"event":true}`,
		`{"event": {"z": 1.00, "a": ["x", false]}}`,
		`{"event": [1, {"k": "v"}, null]}`,
	}, " \n\t")

	request, err := JSON(context, decodeAdapterEnvelopes(t, body))
	if err != nil {
		t.Fatalf("JSON() error = %v", err)
	}
	if got, want := len(request.Events), 5; got != want {
		t.Fatalf("len(Events) = %d, want %d", got, want)
	}
	raw := []string{
		"line\none",
		"1.2300e+4",
		"true",
		`{"z":1.00,"a":["x",false]}`,
		`[1,{"k":"v"},null]`,
	}
	explicitTime := time.Unix(1700000000, 123456789).UTC()
	for ordinal, candidate := range request.Events {
		event := candidate.Event
		if event == nil {
			t.Fatalf("Events[%d].Event is nil", ordinal)
		}
		if got, want := event.GetEventId(), context.RequestID+"-"+strconv.Itoa(ordinal); got != want {
			t.Errorf("Events[%d].EventId = %q, want %q", ordinal, got, want)
		}
		if got, want := string(event.GetRaw()), raw[ordinal]; got != want {
			t.Errorf("Events[%d].Raw = %q, want %q", ordinal, got, want)
		}
		if event.GetRawEncoding() != opensplunk.RawEncoding_RAW_ENCODING_UTF8 {
			t.Errorf("Events[%d].RawEncoding = %v", ordinal, event.GetRawEncoding())
		}
		if event.GetIndexName() != "main" || event.GetHost() != "token-host" ||
			event.GetSource() != "token-source" || event.GetSourcetype() != "token-type" {
			t.Errorf("Events[%d] metadata = index %q host %q source %q sourcetype %q", ordinal,
				event.GetIndexName(), event.GetHost(), event.GetSource(), event.GetSourcetype())
		}
		if got := event.GetCollectedAt().AsTime(); !got.Equal(receivedAt) {
			t.Errorf("Events[%d].CollectedAt = %s, want %s", ordinal, got, receivedAt)
		}
		wantTime := receivedAt.UTC()
		wantSource := opensplunk.EventTimeSource_EVENT_TIME_SOURCE_RECEIVED_AT_FALLBACK
		if ordinal == 0 {
			wantTime = explicitTime
			wantSource = opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED
		}
		if got := event.GetEventTime().AsTime(); !got.Equal(wantTime) {
			t.Errorf("Events[%d].EventTime = %s, want %s", ordinal, got, wantTime)
		}
		if got := event.GetEventTimeSource(); got != wantSource {
			t.Errorf("Events[%d].EventTimeSource = %v, want %v", ordinal, got, wantSource)
		}
		if ordinal == 0 {
			if event.Message == nil || event.GetMessage() != raw[ordinal] {
				t.Errorf("string event Message = %#v, want %q", event.Message, raw[ordinal])
			}
		} else if event.Message != nil {
			t.Errorf("non-string Events[%d].Message = %#v, want nil", ordinal, event.Message)
		}
		if event.Fields != nil {
			t.Errorf("Events[%d].Fields = %#v, want nil", ordinal, event.Fields)
		}
		if got, want := candidate.UncompressedBytes, uint64(proto.Size(event)); got != want {
			t.Errorf("Events[%d].UncompressedBytes = %d, want %d", ordinal, got, want)
		}
	}

	decoder, err := hec.NewEnvelopeDecoder(strings.NewReader(`{"event":null}`), hec.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, err = decoder.Next()
	assertAdapterProtocolError(t, err, hec.ErrorEventBlank, new(0))
}

func TestJSONResolvesMetadataPrecedenceAndFallbacks(t *testing.T) {
	t.Parallel()
	receivedAt := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		profile  auth.HECTokenProfile
		policies []auth.AuthorizedIndexPolicy
		body     string
		want     [4]string
	}{
		{
			name: "event wins over token index and fixed defaults",
			profile: auth.HECTokenProfile{
				DefaultIndexName: "main", DefaultHost: "token-host", DefaultSource: "token-source",
				DefaultSourcetype: "token-type",
			},
			policies: adapterTestPolicies(),
			body: `{"index":"audit","host":"event-host","source":"event-source",` +
				`"sourcetype":"event-type","event":"ok"}`,
			want: [4]string{"audit", "event-host", "event-source", "event-type"},
		},
		{
			name: "token wins over index and fixed defaults",
			profile: auth.HECTokenProfile{
				DefaultIndexName: "main", DefaultHost: "token-host", DefaultSource: "token-source",
				DefaultSourcetype: "token-type",
			},
			policies: adapterTestPolicies(),
			body:     `{"event":"ok"}`,
			want:     [4]string{"main", "token-host", "token-source", "token-type"},
		},
		{
			name:     "selected index supplies sourcetype before fixed fallback",
			profile:  auth.HECTokenProfile{DefaultIndexName: "audit"},
			policies: adapterTestPolicies(),
			body:     `{"event":"ok"}`,
			want:     [4]string{"audit", "hec", "http:hec", "audit-index-type"},
		},
		{
			name:    "fixed metadata fills the final gaps",
			profile: auth.HECTokenProfile{DefaultIndexName: "fallback"},
			policies: []auth.AuthorizedIndexPolicy{{
				Name: "fallback", Version: 1,
			}},
			body: `{"event":"ok"}`,
			want: [4]string{"fallback", "hec", "http:hec", "httpevent"},
		},
		{
			name:     "event index selects that index sourcetype",
			profile:  auth.HECTokenProfile{DefaultIndexName: "main"},
			policies: adapterTestPolicies(),
			body:     `{"index":"audit","event":"ok"}`,
			want:     [4]string{"audit", "hec", "http:hec", "audit-index-type"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authentication := adapterTestAuthentication(test.profile, test.policies)
			context := adapterTestContext(receivedAt, false)
			context.Authentication = authentication
			request, err := JSON(context, decodeAdapterEnvelopes(t, test.body))
			if err != nil {
				t.Fatalf("JSON() error = %v", err)
			}
			event := request.Events[0].Event
			got := [4]string{event.GetIndexName(), event.GetHost(), event.GetSource(), event.GetSourcetype()}
			if got != test.want {
				t.Fatalf("resolved metadata = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestJSONConvertsIndexedFieldsWithoutNumericPrecisionLoss(t *testing.T) {
	t.Parallel()
	context := adapterTestContext(time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC), false)
	body := `{
		"event":"typed",
		"fields":{
			"nil":null,
			"text":"exact",
			"negative":-9223372036854775808,
			"negative_zero":-0,
			"unsigned":18446744073709551615,
			"decimal":1.2300E+004,
			"tiny":-0.0e-001,
			"flag":true,
			"list":[null,"s",-1,2,3.1400,false]
		}
	}`
	request, err := JSON(context, decodeAdapterEnvelopes(t, body))
	if err != nil {
		t.Fatalf("JSON() error = %v", err)
	}
	fields := request.Events[0].Event.GetFields()
	want := &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{
		{Name: "nil", Value: adapterNullValue()},
		{Name: "text", Value: adapterStringValue("exact")},
		{Name: "negative", Value: adapterSint64Value(math.MinInt64)},
		{Name: "negative_zero", Value: adapterSint64Value(0)},
		{Name: "unsigned", Value: adapterUint64Value(math.MaxUint64)},
		{Name: "decimal", Value: adapterDecimalValue("1.2300e4")},
		{Name: "tiny", Value: adapterDecimalValue("-0.0e-1")},
		{Name: "flag", Value: adapterBoolValue(true)},
		{Name: "list", Value: adapterListValue(
			adapterNullValue(),
			adapterStringValue("s"),
			adapterSint64Value(-1),
			adapterUint64Value(2),
			adapterDecimalValue("3.1400"),
			adapterBoolValue(false),
		)},
	}}
	if !proto.Equal(fields, want) {
		t.Fatalf("typed fields mismatch\n got: %v\nwant: %v", fields, want)
	}
}

func TestJSONBuildsHECSourceAndAcknowledgmentAdmissionIdentity(t *testing.T) {
	t.Parallel()
	receivedAt := time.Date(2026, time.August, 10, 23, 59, 58, 765432100, time.UTC)
	context := adapterTestContext(receivedAt, true)
	context.Authentication.TokenRateLimits = ingestquota.Limits{
		MaxEventsPerSecond:            12,
		MaxUncompressedBytesPerSecond: 4096,
	}
	context.Authentication.AllowedHostRegexes = []string{"^token-host$"}
	context.Authentication.AllowedSourceRegexes = []string{"^token-source$"}
	request, err := JSON(context, decodeAdapterEnvelopes(t, `{"event":"identity"}`))
	if err != nil {
		t.Fatalf("JSON() error = %v", err)
	}

	if request.Authorization.SubjectID != context.Authentication.TokenID ||
		request.Authorization.TenantID != context.TenantID || request.Authorization.CollectorID != "" {
		t.Errorf("Authorization identity = %#v", request.Authorization)
	}
	if request.Authorization.TokenRateLimits != context.Authentication.TokenRateLimits ||
		!reflect.DeepEqual(request.Authorization.AuthorizedIndexes, context.Authentication.AuthorizedIndexes) ||
		!reflect.DeepEqual(request.Authorization.AllowedHostRegexes, context.Authentication.AllowedHostRegexes) ||
		!reflect.DeepEqual(request.Authorization.AllowedSourceRegexes, context.Authentication.AllowedSourceRegexes) {
		t.Errorf("Authorization snapshot did not round-trip: %#v", request.Authorization)
	}
	wantSource := ingest.HECSource(context.Authentication.TokenID)
	if request.Source != wantSource || request.CollectorID != "" {
		t.Errorf("source = %#v collector = %q, want %#v and empty", request.Source, request.CollectorID, wantSource)
	}
	if request.BatchID != context.RequestID || request.BatchSequence != 1 ||
		!request.ReceivedAt.Equal(receivedAt) || !request.QuotaEvaluatedAt.Equal(receivedAt) {
		t.Errorf("batch identity/times = %#v", request)
	}
	wantAdmission := &ingest.HECStageAdmission{
		TokenID:               context.Authentication.TokenID,
		TokenVersion:          context.Authentication.TokenVersion,
		RequestID:             context.RequestID,
		AcknowledgmentEnabled: true,
		Channel:               adapterTestChannel,
		CreatedAt:             receivedAt,
	}
	if !reflect.DeepEqual(request.HECAdmission, wantAdmission) {
		t.Errorf("HECAdmission = %#v, want %#v", request.HECAdmission, wantAdmission)
	}
	wantDigest := adapterExpectedSemanticDigest(t, true, adapterTestChannel, request.Events)
	if request.SourceBatchSHA256 != wantDigest {
		t.Errorf("SourceBatchSHA256 = %x, want %x", request.SourceBatchSHA256, wantDigest)
	}
	if request.SourceBatchSHA256 == ([sha256.Size]byte{}) {
		t.Error("SourceBatchSHA256 is zero")
	}

	context.Authentication.AuthorizedIndexes[0].Name = "mutated"
	context.Authentication.AllowedHostRegexes[0] = "mutated"
	context.Authentication.AllowedSourceRegexes[0] = "mutated"
	if request.Authorization.AuthorizedIndexes[0].Name != "main" ||
		request.Authorization.AllowedHostRegexes[0] != "^token-host$" ||
		request.Authorization.AllowedSourceRegexes[0] != "^token-source$" {
		t.Error("admission authorization aliases caller-owned authentication slices")
	}
}

func TestSemanticDigestIsDeterministicAndSeparatesMeaningfulChanges(t *testing.T) {
	t.Parallel()
	receivedAt := time.Date(2026, time.August, 10, 12, 34, 56, 123000000, time.UTC)
	context := adapterTestContext(receivedAt, true)
	first, err := JSON(context, decodeAdapterEnvelopes(t,
		`{"event": {"a": 1, "b": [true, "x"]}} {"event":"tail"}`))
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := JSON(context, decodeAdapterEnvelopes(t,
		"{\n\t\"event\" : { \"a\" : 1 , \"b\" : [ true , \"x\" ] }\n}\n"+
			`{"event" : "tail"}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceBatchSHA256 != repeated.SourceBatchSHA256 {
		t.Errorf("equivalent decoded requests produced different digests: %x != %x",
			first.SourceBatchSHA256, repeated.SourceBatchSHA256)
	}
	again, err := JSON(context, decodeAdapterEnvelopes(t,
		`{"event":{"a":1,"b":[true,"x"]}} {"event":"tail"}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceBatchSHA256 != again.SourceBatchSHA256 {
		t.Error("repeated conversion is not deterministic")
	}

	changedEvent, err := JSON(context, decodeAdapterEnvelopes(t,
		`{"event":{"a":2,"b":[true,"x"]}} {"event":"tail"}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceBatchSHA256 == changedEvent.SourceBatchSHA256 {
		t.Error("event semantic change did not change digest")
	}
	changedOrder, err := JSON(context, decodeAdapterEnvelopes(t,
		`{"event":"tail"} {"event":{"a":1,"b":[true,"x"]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceBatchSHA256 == changedOrder.SourceBatchSHA256 {
		t.Error("event order change did not change digest")
	}
	otherChannel := context
	otherChannel.Channel = hec.Channel("123e4567-e89b-42d3-a456-426614174001")
	changedChannel, err := JSON(otherChannel, decodeAdapterEnvelopes(t,
		`{"event":{"a":1,"b":[true,"x"]}} {"event":"tail"}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceBatchSHA256 == changedChannel.SourceBatchSHA256 {
		t.Error("acknowledgment channel change did not change digest")
	}
}

func TestRawConvertsExactMessagesMetadataAndTime(t *testing.T) {
	t.Parallel()
	receivedAt := time.Date(2026, time.August, 10, 7, 6, 5, 400, time.UTC)
	context := adapterTestContext(receivedAt, true)
	rawQuery := url.Values{
		"time":       {"1700000000.000000001"},
		"host":       {"query-host"},
		"source":     {"query-source"},
		"sourcetype": {"query-type"},
		"index":      {"audit"},
		"channel":    {adapterTestChannel},
	}.Encode()
	query, err := hec.ParseRawQuery(rawQuery, hec.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	request, err := Raw(context, query, [][]byte{[]byte("first line"), []byte("second \u03b2 line")})
	if err != nil {
		t.Fatalf("Raw() error = %v", err)
	}
	if got, want := len(request.Events), 2; got != want {
		t.Fatalf("len(Events) = %d, want %d", got, want)
	}
	wantTime := time.Unix(1700000000, 1).UTC()
	for ordinal, wantRaw := range []string{"first line", "second \u03b2 line"} {
		event := request.Events[ordinal].Event
		if got := string(event.GetRaw()); got != wantRaw {
			t.Errorf("Events[%d].Raw = %q, want %q", ordinal, got, wantRaw)
		}
		if event.Message == nil || event.GetMessage() != wantRaw {
			t.Errorf("Events[%d].Message = %#v, want %q", ordinal, event.Message, wantRaw)
		}
		if event.GetIndexName() != "audit" || event.GetHost() != "query-host" ||
			event.GetSource() != "query-source" || event.GetSourcetype() != "query-type" {
			t.Errorf("Events[%d] metadata = %#v", ordinal, event)
		}
		if got := event.GetEventTime().AsTime(); !got.Equal(wantTime) {
			t.Errorf("Events[%d].EventTime = %s, want %s", ordinal, got, wantTime)
		}
		if event.GetEventTimeSource() != opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED {
			t.Errorf("Events[%d].EventTimeSource = %v", ordinal, event.GetEventTimeSource())
		}
		if got, want := event.GetEventId(), context.RequestID+"-"+strconv.Itoa(ordinal); got != want {
			t.Errorf("Events[%d].EventId = %q, want %q", ordinal, got, want)
		}
		if event.Fields != nil {
			t.Errorf("Events[%d].Fields = %#v, want nil", ordinal, event.Fields)
		}
	}
	if request.HECAdmission == nil || request.HECAdmission.Channel != adapterTestChannel {
		t.Errorf("HECAdmission = %#v", request.HECAdmission)
	}

	defaultContext := adapterTestContext(receivedAt, false)
	defaultQuery, err := hec.ParseRawQuery("", hec.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defaults, err := Raw(defaultContext, defaultQuery, [][]byte{[]byte("fallback")})
	if err != nil {
		t.Fatal(err)
	}
	event := defaults.Events[0].Event
	if got := event.GetEventTime().AsTime(); !got.Equal(receivedAt) ||
		event.GetEventTimeSource() != opensplunk.EventTimeSource_EVENT_TIME_SOURCE_RECEIVED_AT_FALLBACK {
		t.Errorf("raw fallback time = %s/%v, want %s/received fallback",
			got, event.GetEventTimeSource(), receivedAt)
	}
	if defaults.HECAdmission == nil || defaults.HECAdmission.AcknowledgmentEnabled ||
		defaults.HECAdmission.Channel != "" {
		t.Errorf("non-ack HECAdmission = %#v", defaults.HECAdmission)
	}
}

func TestAdapterRejectsIncompleteOrNonHECRequestContext(t *testing.T) {
	t.Parallel()
	receivedAt := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	envelopes := decodeAdapterEnvelopes(t, `{"event":"ok"}`)
	tests := []struct {
		name string
		edit func(*RequestContext)
		kind hec.ErrorKind
	}{
		{"tenant missing", func(value *RequestContext) { value.TenantID = "" }, hec.ErrorInternal},
		{"request ID missing", func(value *RequestContext) { value.RequestID = "" }, hec.ErrorInternal},
		{"receive time missing", func(value *RequestContext) { value.ReceivedAt = time.Time{} }, hec.ErrorInternal},
		{"token ID missing", func(value *RequestContext) { value.Authentication.TokenID = "" }, hec.ErrorInternal},
		{"token version missing", func(value *RequestContext) { value.Authentication.TokenVersion = 0 }, hec.ErrorInternal},
		{"native purpose", func(value *RequestContext) {
			value.Authentication.Purpose = auth.IngestionTokenPurposeNativeCollector
		}, hec.ErrorInvalidToken},
		{"collector binding present", func(value *RequestContext) {
			value.Authentication.BoundCollectorID = "collector-a"
		}, hec.ErrorInvalidToken},
		{"index authority missing", func(value *RequestContext) {
			value.Authentication.AuthorizedIndexes = nil
		}, hec.ErrorInvalidToken},
		{"ack channel missing", func(value *RequestContext) {
			value.Authentication.HECProfile.IndexerAcknowledgment = true
			value.Channel = ""
		}, hec.ErrorChannelMissing},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context := adapterTestContext(receivedAt, false)
			test.edit(&context)
			request, err := JSON(context, envelopes)
			assertAdapterProtocolError(t, err, test.kind, nil)
			assertZeroAdapterAdmission(t, request)
		})
	}

	context := adapterTestContext(receivedAt, false)
	request, err := JSON(context, nil)
	assertAdapterProtocolError(t, err, hec.ErrorNoData, nil)
	assertZeroAdapterAdmission(t, request)
	request, err = Raw(context, hec.RawQuery{}, nil)
	assertAdapterProtocolError(t, err, hec.ErrorNoData, nil)
	assertZeroAdapterAdmission(t, request)
}

func TestAdapterFailuresAreRequestAtomicAndCarryLowestEventOrdinal(t *testing.T) {
	t.Parallel()
	context := adapterTestContext(time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC), false)
	context.Authentication.AuthorizedIndexes = context.Authentication.AuthorizedIndexes[:1]
	request, err := JSON(context, decodeAdapterEnvelopes(t,
		`{"event":"accepted-first"} {"index":"audit","event":"unauthorized-second"}`))
	assertAdapterProtocolError(t, err, hec.ErrorIncorrectIndex, new(1))
	assertZeroAdapterAdmission(t, request)

	missingIndexContext := context
	missingIndexContext.Authentication.HECProfile.DefaultIndexName = ""
	request, err = JSON(missingIndexContext, decodeAdapterEnvelopes(t, `{"event":"missing-index"}`))
	assertAdapterProtocolError(t, err, hec.ErrorIncorrectIndex, new(0))
	assertZeroAdapterAdmission(t, request)

	request, err = Raw(context, hec.RawQuery{}, [][]byte{[]byte("accepted-first"), {}})
	assertAdapterProtocolError(t, err, hec.ErrorInvalidDataFormat, new(1))
	assertZeroAdapterAdmission(t, request)

	request, err = Raw(context, hec.RawQuery{}, [][]byte{[]byte("accepted-first"), []byte("bad\x00event")})
	assertAdapterProtocolError(t, err, hec.ErrorInvalidDataFormat, new(1))
	assertZeroAdapterAdmission(t, request)

	unauthorized := hec.RawQuery{Metadata: hec.MetadataValues{
		Index: hec.OptionalString{Present: true, Value: "audit"},
	}}
	request, err = Raw(context, unauthorized, [][]byte{[]byte("body")})
	assertAdapterProtocolError(t, err, hec.ErrorIncorrectIndex, new(0))
	assertZeroAdapterAdmission(t, request)

	invalidTime := hec.RawQuery{Time: hec.OptionalString{Present: true, Value: "1.0000000001"}}
	request, err = Raw(context, invalidTime, [][]byte{[]byte("body")})
	assertAdapterProtocolError(t, err, hec.ErrorInvalidDataFormat, nil)
	assertZeroAdapterAdmission(t, request)
}

func adapterTestContext(receivedAt time.Time, acknowledgment bool) RequestContext {
	profile := auth.HECTokenProfile{
		DefaultIndexName:      "main",
		DefaultHost:           "token-host",
		DefaultSource:         "token-source",
		DefaultSourcetype:     "token-type",
		IndexerAcknowledgment: acknowledgment,
	}
	context := RequestContext{
		TenantID:       "tenant-a",
		Authentication: adapterTestAuthentication(profile, adapterTestPolicies()),
		RequestID:      "0123456789abcdef0123456789abcdef",
		ReceivedAt:     receivedAt,
	}
	if acknowledgment {
		context.Channel = hec.Channel(adapterTestChannel)
	}
	return context
}

func adapterTestAuthentication(
	profile auth.HECTokenProfile,
	policies []auth.AuthorizedIndexPolicy,
) auth.Authentication {
	return auth.Authentication{
		TokenID:           "hec-token-record",
		TokenVersion:      7,
		TokenName:         "safe-display-name",
		Purpose:           auth.IngestionTokenPurposeHEC,
		HECProfile:        profile,
		AuthorizedIndexes: append([]auth.AuthorizedIndexPolicy(nil), policies...),
	}
}

func adapterTestPolicies() []auth.AuthorizedIndexPolicy {
	return []auth.AuthorizedIndexPolicy{
		{
			Name:              "main",
			Version:           4,
			RetentionPeriod:   24 * time.Hour,
			DefaultSourcetype: "main-index-type",
		},
		{
			Name:              "audit",
			Version:           9,
			RetentionPeriod:   48 * time.Hour,
			DefaultSourcetype: "audit-index-type",
		},
	}
}

func decodeAdapterEnvelopes(t *testing.T, body string) []hec.Envelope {
	t.Helper()
	decoder, err := hec.NewEnvelopeDecoder(strings.NewReader(body), hec.DefaultLimits())
	if err != nil {
		t.Fatalf("NewEnvelopeDecoder() error = %v", err)
	}
	var envelopes []hec.Envelope
	for {
		envelope, nextErr := decoder.Next()
		if errors.Is(nextErr, io.EOF) {
			return envelopes
		}
		if nextErr != nil {
			t.Fatalf("EnvelopeDecoder.Next() error = %v", nextErr)
		}
		envelopes = append(envelopes, envelope)
	}
}

func assertAdapterProtocolError(t *testing.T, err error, kind hec.ErrorKind, ordinal *int) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %v", kind)
	}
	if !hec.IsErrorKind(err, kind) {
		t.Fatalf("error = %v, want kind %v", err, kind)
	}
	var failure *hec.ProtocolError
	if !errors.As(err, &failure) {
		t.Fatalf("error type = %T, want *hec.ProtocolError", err)
	}
	if !reflect.DeepEqual(failure.InvalidEventNumber, ordinal) {
		t.Fatalf("InvalidEventNumber = %#v, want %#v", failure.InvalidEventNumber, ordinal)
	}
}

func assertZeroAdapterAdmission(t *testing.T, request ingest.AdmissionRequest) {
	t.Helper()
	if !reflect.DeepEqual(request, ingest.AdmissionRequest{}) {
		t.Fatalf("request = %#v, want zero admission", request)
	}
}

func adapterExpectedSemanticDigest(
	t *testing.T,
	acknowledgment bool,
	channel string,
	events []ingest.AdmissionEvent,
) [sha256.Size]byte {
	t.Helper()
	hash := sha256.New()
	_, _ = hash.Write([]byte("open-splunk-hec-semantics-v1\x00"))
	if acknowledgment {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	adapterWriteDigestPart(hash, []byte(channel))
	marshal := proto.MarshalOptions{Deterministic: true}
	for _, candidate := range events {
		encoded, err := marshal.Marshal(candidate.Event)
		if err != nil {
			t.Fatalf("marshal expected semantic event: %v", err)
		}
		adapterWriteDigestPart(hash, encoded)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func adapterWriteDigestPart(destination io.Writer, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = destination.Write(length[:])
	_, _ = destination.Write(value)
}

func adapterNullValue() *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_NullValue{
		NullValue: opensplunk.NullValue_NULL_VALUE_NULL,
	}}
}

func adapterStringValue(value string) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: value}}
}

func adapterSint64Value(value int64) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_Sint64Value{Sint64Value: value}}
}

func adapterUint64Value(value uint64) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_Uint64Value{Uint64Value: value}}
}

func adapterDecimalValue(value string) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_DecimalValue{
		DecimalValue: &opensplunk.DecimalValue{Value: value},
	}}
}

func adapterBoolValue(value bool) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_BoolValue{BoolValue: value}}
}

func adapterListValue(values ...*opensplunk.TypedValue) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_ListValue{
		ListValue: &opensplunk.TypedValueList{Values: values},
	}}
}
