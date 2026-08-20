package gradethiscorpus

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector"
)

func TestFixtureIsDeterministicSanitizedAndPinned(t *testing.T) {
	t.Parallel()

	first := Fixture()
	second := Fixture()
	if err := Validate(first); err != nil {
		t.Fatalf("Validate(Fixture()): %v", err)
	}
	if !bytes.Equal(first.NDJSON, second.NDJSON) {
		t.Fatal("Fixture() did not return byte-identical NDJSON")
	}
	sum := sha256.Sum256(first.NDJSON)
	const wantSHA256 = "8ed38ecb866342a19a924e8635619bbfb067623b2dc1eea602e54c81b68e65f9"
	if got := hex.EncodeToString(sum[:]); got != wantSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", got, wantSHA256)
	}

	levels, messages := map[string]int{}, map[string]int{}
	for _, event := range first.Events {
		levels[event.Level]++
		messages[event.Message]++
	}
	if levels["INFO"] != 11 || levels["ERROR"] != 6 || levels["WARN"] != 3 {
		t.Fatalf("level counts = %#v", levels)
	}
	if messages["Request metrics"] != 10 || messages["Heartbeat"] != 4 ||
		messages["Dependency retry scheduled"] != 3 ||
		messages["Database request failed"] != 2 || messages["Request started"] != 1 {
		t.Fatalf("message counts = %#v", messages)
	}
}

func TestSearchManifestRendersExactTenQueries(t *testing.T) {
	t.Parallel()

	fixture := Fixture()
	searches := Searches()
	want := []Search{
		{
			ID: SearchFollowTrace, Name: "follow one request",
			Template: `index=gradethis trace_id="<trace-id>"
| sort _time
| table _time level layer logger message`,
		},
		{
			ID: SearchErrorsAndWarnings, Name: "inspect errors and warnings",
			Template: `index=gradethis (level=ERROR OR level=WARN)
| sort -_time`,
		},
		{
			ID: SearchRawErrorFragment, Name: "find a known error fragment",
			Template: `index=gradethis "connection refused"
| table _time level logger message trace_id`,
		},
		{
			ID: SearchSeverityCounts, Name: "count events by severity",
			Template: `index=gradethis
| stats count by level
| sort -count`,
		},
		{
			ID: SearchFrequentErrors, Name: "find the most frequent errors",
			Template: `index=gradethis level=ERROR
| stats count by logger, message
| sort -count
| head 20`,
		},
		{
			ID: SearchVolumeBySeverity, Name: "chart event volume by severity",
			Template: `index=gradethis
| timechart span=5m count by level`,
		},
		{
			ID: SearchServerErrors, Name: "chart server errors by route",
			Template: `index=gradethis message="Request metrics" status>=500
| timechart span=5m count by path`,
		},
		{
			ID: SearchResponses, Name: "count HTTP responses by route and status",
			Template: `index=gradethis message="Request metrics"
| stats count by path, status
| sort -count`,
		},
		{
			ID: SearchSlowRoutes, Name: "find slow routes",
			Template: `index=gradethis message="Request metrics"
| eval duration_ms=tonumber(replace(duration, "ms$", ""))
| stats count p95(duration_ms) as p95_ms by path
| where p95_ms > 500`,
		},
		{
			ID: SearchTopMessages, Name: "inspect the most common messages",
			Template: `index=gradethis
| top limit=20 message`,
		},
	}
	if len(searches) != len(want) {
		t.Fatalf("searches = %d, want %d", len(searches), len(want))
	}
	seen := make(map[SearchID]struct{}, len(searches))
	for index, search := range searches {
		if search != want[index] {
			t.Fatalf("search %d = %#v, want %#v", index, search, want[index])
		}
		if _, duplicate := seen[search.ID]; duplicate {
			t.Fatalf("duplicate search ID %q", search.ID)
		}
		seen[search.ID] = struct{}{}
		source, err := search.Render(fixture.TraceID)
		if err != nil {
			t.Fatalf("Render(%q): %v", search.ID, err)
		}
		if source == "" || strings.Contains(source, tracePlaceholder) {
			t.Fatalf("rendered %q = %q", search.ID, source)
		}
	}
	if _, err := searches[0].Render(`bad" | head 0`); err == nil {
		t.Fatal("Render accepted an unsafe trace identifier")
	}
	searches[0].Template = "mutated"
	if Searches()[0] != want[0] {
		t.Fatal("Searches returned storage aliased to its caller")
	}
}

func TestFixtureDecodesThroughCollectorWithTypedRequestFields(t *testing.T) {
	t.Parallel()

	fixture := Fixture()
	decoder, err := collector.NewDecoder(collector.DecodeConfig{
		Format: collector.InputFormatNDJSON, InputID: "gradethis-corpus",
		IndexName: IndexName, Source: Source, Sourcetype: Sourcetype,
		Host: Host, Service: Service,
	})
	if err != nil {
		t.Fatal(err)
	}
	offset := uint64(0)
	for index, expected := range fixture.Events {
		end := offset + uint64(len(expected.RawLine))
		event, decodeErr := decoder.Decode(expected.RawLine, collector.SourcePosition{
			FileIdentity: "gradethis-corpus-file", SourcePath: Source,
			FileFingerprintLength: 4096, StartOffset: offset, EndOffset: end,
			LineNumber: uint64(index + 1),
		}, fixture.IndexTime)
		if decodeErr != nil {
			t.Fatalf("Decode(%q): %v", expected.ID, decodeErr)
		}
		if got := event.GetEventTime().AsTime(); !got.Equal(fixture.BaseTime.Add(expected.Offset)) {
			t.Fatalf("%q event time = %v", expected.ID, got)
		}
		if event.GetLevel() != expected.Level || event.GetMessage() != expected.Message ||
			event.GetTraceId() != expected.TraceID || event.GetSpanId() != expected.SpanID {
			t.Fatalf("%q canonical event = %#v", expected.ID, event)
		}
		if event.GetHost() != Host || event.GetSource() != Source ||
			event.GetSourcetype() != Sourcetype || event.GetService() != Service ||
			!bytes.Equal(event.GetRaw(), expected.RawLine) {
			t.Fatalf("%q trusted metadata/raw = %#v", expected.ID, event)
		}
		if (event.TraceId != nil) != (expected.TraceID != "") ||
			(event.SpanId != nil) != (expected.SpanID != "") {
			t.Fatalf("%q trace/span presence = %#v", expected.ID, event)
		}

		wantFields := map[string]any{
			"caller":      expected.Caller,
			"environment": "test",
			"layer":       expected.Layer,
			"logger":      expected.Logger,
		}
		if expected.Request {
			wantFields["method"] = expected.Method
			wantFields["path"] = expected.Path
			wantFields["status"] = expected.Status
			wantFields["duration"] = expected.Duration
			wantFields["bytes"] = int64(expected.Bytes)
			wantFields["ip"] = expected.IP
			wantFields["user_agent"] = expected.UserAgent
		}
		if expected.Error != "" {
			wantFields["error"] = expected.Error
		}
		if expected.ExplicitNull {
			wantFields["optional_note"] = nil
		}
		gotFields := make(map[string]any, len(event.GetFields().GetFields()))
		for _, field := range event.GetFields().GetFields() {
			if _, duplicate := gotFields[field.GetName()]; duplicate {
				t.Fatalf("%q decoded duplicate field %q", expected.ID, field.GetName())
			}
			gotFields[field.GetName()] = decodedFixtureScalar(t, field.GetValue())
		}
		if !reflect.DeepEqual(gotFields, wantFields) {
			t.Fatalf("%q decoded fields = %#v, want %#v", expected.ID, gotFields, wantFields)
		}
		offset = end + 1
	}
}

func TestInspectionLoadCohortsPinIndependentPruningDimensions(t *testing.T) {
	t.Parallel()

	profile := Fixture()
	cohorts, err := InspectionLoadCohorts("tenant")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateInspectionLoad(profile, "tenant", cohorts); err != nil {
		t.Fatal(err)
	}
	if len(cohorts) != 4 {
		t.Fatalf("inspection load cohorts = %d, want 4", len(cohorts))
	}
	var rows uint64
	seen := make(map[InspectionLoadCohortID]struct{}, len(cohorts))
	for _, cohort := range cohorts {
		rows += cohort.Rows
		if cohort.Rows != InspectionLoadRowsPerCohort {
			t.Fatalf("cohort %q rows = %d", cohort.ID, cohort.Rows)
		}
		if _, duplicate := seen[cohort.ID]; duplicate {
			t.Fatalf("duplicate cohort %q", cohort.ID)
		}
		seen[cohort.ID] = struct{}{}
	}
	if rows != InspectionLoadTotalRows {
		t.Fatalf("inspection load rows = %d, want %d", rows, InspectionLoadTotalRows)
	}

	byID := make(map[InspectionLoadCohortID]InspectionLoadCohort, len(cohorts))
	for _, cohort := range cohorts {
		byID[cohort.ID] = cohort
	}
	latest := profile.BaseTime.Add(15 * time.Minute)
	outOfTime := byID[InspectionLoadSameScopeOutOfTime]
	if outOfTime.TenantID != "tenant" ||
		outOfTime.IndexName != IndexName ||
		!outOfTime.EventTime.Equal(profile.BaseTime.Add(30*time.Minute)) ||
		outOfTime.EventTime.Before(latest) {
		t.Fatalf("same-scope out-of-time cohort = %#v", outOfTime)
	}
	foreignTenant := byID[InspectionLoadForeignTenant]
	if foreignTenant.TenantID == "tenant" ||
		foreignTenant.IndexName != IndexName ||
		foreignTenant.EventTime.Before(profile.BaseTime) ||
		!foreignTenant.EventTime.Before(latest) {
		t.Fatalf("foreign-tenant cohort = %#v", foreignTenant)
	}
	foreignIndex := byID[InspectionLoadForeignIndex]
	if foreignIndex.TenantID != "tenant" ||
		foreignIndex.IndexName == IndexName ||
		foreignIndex.EventTime.Before(profile.BaseTime) ||
		!foreignIndex.EventTime.Before(latest) {
		t.Fatalf("foreign-index cohort = %#v", foreignIndex)
	}
	adjacent := byID[InspectionLoadAdjacentPartition]
	if adjacent.TenantID != "tenant" ||
		adjacent.IndexName != IndexName ||
		!adjacent.EventTime.Before(profile.BaseTime) ||
		adjacent.EventTime.Month() == profile.BaseTime.Month() {
		t.Fatalf("adjacent-partition cohort = %#v", adjacent)
	}

	cohorts[0].TenantID = "mutated"
	fresh, err := InspectionLoadCohorts("tenant")
	if err != nil {
		t.Fatal(err)
	}
	if fresh[0].TenantID == "mutated" {
		t.Fatal("InspectionLoadCohorts returned aliased storage")
	}
}

func TestFixtureScannerRejectsSensitiveAndNonSyntheticData(t *testing.T) {
	t.Parallel()

	tests := [][]byte{
		[]byte(`{"token":"secret"}`),
		[]byte(`{"user_id":"person-1"}`),
		[]byte(`{"message":"contact alice@example.com"}`),
		[]byte(`{"stacktrace":"frame"}`),
		[]byte(`{"caller":"/Users/alice/code/main.go:1"}`),
		[]byte(`{"accessToken":"secret"}`),
		[]byte(`{"nested":{"clientSecret":"secret"}}`),
		[]byte(`{"dbPassword":"secret"}`),
		[]byte(`{"passwordHash":"digest"}`),
		[]byte(`{"authorizationHeader":"value"}`),
		[]byte(`{"prod_api_token":"secret"}`),
		[]byte(`{"dbpassword":"secret"}`),
		[]byte(`{"passwordhash":"digest"}`),
		[]byte(`{"authorizationheader":"value"}`),
		[]byte(`{"prodtokenvalue":"secret"}`),
		[]byte(`{"accountUserId":"person-1"}`),
		[]byte(`{"account_user_id":"person-1"}`),
		[]byte(`{"customerSessionId":"session-1"}`),
		[]byte(`{"prod_api_key":"secret"}`),
		[]byte(`{"db_private_key":"secret"}`),
		[]byte(`{"alice@example.com":"value"}`),
		[]byte(`{"message":"SELECT * FROM production"}`),
		[]byte(`{"message":"goroutine 12 [running]"}`),
		[]byte(`{"message":"wrote /srv/acme/prod.log"}`),
		[]byte(`{"message":"caller=/Users/alice/prod.go"}`),
		[]byte(`{"message":"path=C:\\Users\\alice\\prod.log"}`),
		[]byte(`{"message":"authorization=Bearer live-token"}`),
		[]byte(`{"error":"password=live-secret"}`),
		[]byte(`{"message":"Bearer live-token"}`),
		[]byte(`{"message":"see https://logs.acme.internal/events"}`),
		[]byte(`{"ip":"10.0.0.1"}`),
		[]byte(`{"ip":"2001:4860:4860::8888"}`),
		[]byte(`{"message":"remote=[2001:4860:4860::8888]:443"}`),
		[]byte(`{"message":"ok","message":"duplicate"}`),
	}
	for _, line := range tests {
		payload := append(bytes.Clone(line), '\n')
		if err := ScanNDJSON(payload); err == nil {
			t.Errorf("ScanNDJSON(%s) unexpectedly succeeded", line)
		}
	}
	if err := ScanNDJSON([]byte(
		"{\"ip\":\"198.51.100.7\",\"ip6\":\"2001:db8::7\",\"remote\":\"[2001:db8::7]:443\",\"path\":\"/api/assessments\",\"user_agent\":\"synthetic-client/1.0\"," +
			"\"message\":\"see https://logs.example.test/events\",\"duration\":\"800ms\"}\n",
	)); err != nil {
		t.Fatalf("ScanNDJSON(documentation data): %v", err)
	}
	for _, test := range []struct {
		payload []byte
		secret  string
	}{
		{[]byte("{\"alice@example.com\":\"value\"}\n"), "alice@example.com"},
		{[]byte("{\"customer-reference\":\"one\",\"customer-reference\":\"two\"}\n"), "customer-reference"},
	} {
		err := ScanNDJSON(test.payload)
		if err == nil {
			t.Fatalf("ScanNDJSON(%s) unexpectedly succeeded", test.payload)
		}
		if strings.Contains(err.Error(), test.secret) {
			t.Fatalf("scanner error echoed rejected key %q: %v", test.secret, err)
		}
	}
	if err := ScanNDJSON([]byte(`{"message":"missing final newline"}`)); err == nil {
		t.Fatal("ScanNDJSON accepted a fixture without a final newline")
	}
}

func decodedFixtureScalar(t *testing.T, value *opensplunk.TypedValue) any {
	t.Helper()
	if value == nil {
		t.Fatal("decoded fixture value is nil")
	}
	switch kind := value.GetKind().(type) {
	case *opensplunk.TypedValue_NullValue:
		return nil
	case *opensplunk.TypedValue_StringValue:
		return kind.StringValue
	case *opensplunk.TypedValue_Sint64Value:
		return kind.Sint64Value
	case *opensplunk.TypedValue_Uint64Value:
		return kind.Uint64Value
	case *opensplunk.TypedValue_DoubleValue:
		return kind.DoubleValue
	case *opensplunk.TypedValue_BoolValue:
		return kind.BoolValue
	default:
		t.Fatalf("decoded fixture value has unsupported kind %T", value.GetKind())
		return nil
	}
}
