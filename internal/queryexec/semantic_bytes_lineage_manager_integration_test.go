package queryexec

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

const semanticBytesLineageIndex = "semantic-bytes-lineage"

type semanticBytesLineageEvent struct {
	id       string
	at       time.Time
	raw      []byte
	encoding opensplunkv1.RawEncoding
	host     string
	source   string
	service  *string
}

type semanticBytesLineageExpected struct {
	kind    searchjobs.ValueKind
	payload []byte
}

// TestSemanticBytesV02ManagerAgainstClickHouse pins the semantic String/Bytes
// lineage that belongs to the v0.2 candidate independently of the later v0.3
// command additions.
func TestSemanticBytesV02ManagerAgainstClickHouse(t *testing.T) {
	testSemanticBytesLineageManagerAgainstClickHouse(t, false)
}

// TestSemanticBytesLineageManagerAgainstClickHouse extends the v0.2 baseline
// through the v0.3 fillnull and strcat commands at the public manager boundary.
func TestSemanticBytesLineageManagerAgainstClickHouse(t *testing.T) {
	testSemanticBytesLineageManagerAgainstClickHouse(t, true)
}

func testSemanticBytesLineageManagerAgainstClickHouse(t *testing.T, includeV03 bool) {
	t.Helper()
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}

	earliest := time.Date(2026, time.August, 12, 20, 0, 0, 0, time.UTC)
	latest := earliest.Add(time.Hour)
	indexTime := latest.Add(time.Hour)
	invalid := []byte{0xff, 'b', 'i', 'n', 0xfe}
	events := []semanticBytesLineageEvent{
		{
			id: "semantic-01-utf8", at: earliest.Add(time.Minute),
			raw: []byte("utf8-界"), encoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			source: "lineage",
		},
		{
			id: "semantic-02-binary-valid", at: earliest.Add(2 * time.Minute),
			raw: []byte("binary-valid-界"), encoding: opensplunkv1.RawEncoding_RAW_ENCODING_BINARY,
			source: "lineage",
		},
		{
			id: "semantic-03-binary-invalid", at: earliest.Add(3 * time.Minute),
			raw: invalid, encoding: opensplunkv1.RawEncoding_RAW_ENCODING_BINARY,
			source: "lineage",
		},
		{
			id: "semantic-04-null", at: earliest.Add(4 * time.Minute),
			raw: []byte("null-control"), encoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			source: "lineage",
		},
	}
	ctx, executor := semanticBytesLineageStartClickHouse(t, indexTime, events)

	base := map[string]semanticBytesLineageExpected{
		"semantic-01-utf8": {
			kind: searchjobs.ValueKindString, payload: []byte("utf8-界"),
		},
		"semantic-02-binary-valid": {
			kind: searchjobs.ValueKindBytes, payload: []byte("binary-valid-界"),
		},
		"semantic-03-binary-invalid": {
			kind: searchjobs.ValueKindBytes, payload: invalid,
		},
		"semantic-04-null": {
			kind: searchjobs.ValueKindString, payload: []byte("null-control"),
		},
	}
	identity := func(
		_ string,
		value semanticBytesLineageExpected,
	) semanticBytesLineageExpected {
		return value
	}
	nullControl := func(
		id string,
		value semanticBytesLineageExpected,
	) semanticBytesLineageExpected {
		if id == "semantic-04-null" {
			return semanticBytesLineageExpected{kind: searchjobs.ValueKindNull}
		}
		return value
	}
	wrapped := func(
		_ string,
		value semanticBytesLineageExpected,
	) semanticBytesLineageExpected {
		payload := make([]byte, 0, len(value.payload)+2)
		payload = append(payload, '<')
		payload = append(payload, value.payload...)
		payload = append(payload, '>')
		return semanticBytesLineageExpected{kind: value.kind, payload: payload}
	}
	rawPlusMaybe := func(
		id string,
		value semanticBytesLineageExpected,
	) semanticBytesLineageExpected {
		if id == "semantic-04-null" {
			return semanticBytesLineageExpected{kind: searchjobs.ValueKindNull}
		}
		payload := make([]byte, 0, len(value.payload)+1)
		payload = append(payload, value.payload...)
		payload = append(payload, 'x')
		return semanticBytesLineageExpected{kind: value.kind, payload: payload}
	}
	filled := func(
		id string,
		value semanticBytesLineageExpected,
	) semanticBytesLineageExpected {
		if id == "semantic-04-null" {
			return semanticBytesLineageExpected{
				kind: searchjobs.ValueKindString, payload: []byte("fallback"),
			}
		}
		return value
	}

	for _, test := range []struct {
		name     string
		pipeline string
		expected func(string, semanticBytesLineageExpected) semanticBytesLineageExpected
	}{
		{
			name: "eval copy", pipeline: ` | eval output=_raw`, expected: identity,
		},
		{
			name: "rename", pipeline: ` | rename _raw AS output`, expected: identity,
		},
		{
			name: "tostring", pipeline: ` | eval output=tostring(_raw)`, expected: identity,
		},
		{
			name: "concatenation", pipeline: ` | eval output="<" . _raw . ">"`, expected: wrapped,
		},
		{
			name: "nullable mixed concatenation",
			pipeline: ` | eval maybe=if(event_id="semantic-04-null",null,"x")` +
				` | eval output=_raw . maybe`,
			expected: rawPlusMaybe,
		},
		{
			name: "if", pipeline: ` | eval output=if(event_id="semantic-04-null",null,_raw)`, expected: nullControl,
		},
		{
			name: "case", pipeline: ` | eval output=case(event_id="semantic-04-null",null,event_id=event_id,_raw)`, expected: nullControl,
		},
		{
			name: "coalesce", pipeline: ` | eval output=coalesce(null,if(event_id="semantic-04-null",null,_raw))`, expected: nullControl,
		},
		{
			name: "fillnull",
			pipeline: ` | eval output=if(event_id="semantic-04-null",null,_raw)` +
				` | fillnull value="fallback" output`,
			expected: filled,
		},
		{
			name: "strcat", pipeline: ` | strcat "<" _raw ">" output`, expected: wrapped,
		},
	} {
		if !includeV03 && (test.name == "fillnull" || test.name == "strcat") {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			source := `index=` + semanticBytesLineageIndex + ` source="lineage"` +
				test.pipeline + ` | table event_id output | sort 0 +event_id`
			page := semanticBytesLineageRunManagerSearch(
				t,
				ctx,
				executor,
				indexTime,
				"semantic-lineage-"+strings.ReplaceAll(test.name, " ", "-"),
				source,
				earliest,
				latest,
			)
			semanticBytesLineageRequireSchema(
				t,
				page,
				[]string{"event_id", "output"},
			)
			if page.Schema.Columns[1].Kind != searchjobs.ValueKindMixed {
				t.Fatalf("output schema = %#v, want sealed Mixed String/Bytes column", page.Schema.Columns[1])
			}
			if test.name == "nullable mixed concatenation" &&
				!page.Schema.Columns[1].Nullable {
				t.Fatalf(
					"nullable mixed concatenation schema = %#v, want nullable Mixed",
					page.Schema.Columns[1],
				)
			}
			if len(page.Rows) != len(base) || !page.Complete || page.TotalRows != uint64(len(base)) {
				t.Fatalf(
					"page = rows %d total %d complete=%t, want %d complete rows",
					len(page.Rows), page.TotalRows, page.Complete, len(base),
				)
			}

			seen := make(map[string]struct{}, len(page.Rows))
			for _, row := range page.Rows {
				id, ok := row.Values[0].String()
				if !ok {
					t.Fatalf("event_id = %#v, want String", row.Values[0])
				}
				value, found := base[id]
				if !found {
					t.Fatalf("unexpected event_id %q", id)
				}
				if _, duplicate := seen[id]; duplicate {
					t.Fatalf("duplicate event_id %q", id)
				}
				seen[id] = struct{}{}
				semanticBytesLineageRequireValue(t, row.Values[1], test.expected(id, value))
			}
		})
	}
}

// TestSemanticBytesModeManagerAgainstClickHouse requires mode to preserve the
// winning scalar's semantic identity instead of reclassifying it from UTF-8
// validity. Unique winners cover every semantic class; equal-count ties prove
// the provisional bytewise-low policy is payload-first and then String-before-
// Bytes when both semantic kinds carry the same payload.
func TestSemanticBytesModeManagerAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}

	earliest := time.Date(2026, time.August, 12, 22, 0, 0, 0, time.UTC)
	latest := earliest.Add(time.Hour)
	indexTime := latest.Add(time.Hour)
	invalid := []byte{0xff, 'm', 'o', 'd', 'e', 0xfe}
	utf8Service := "utf8"
	binaryValidService := "binary-valid"
	binaryInvalidService := "binary-invalid"
	payloadTieService := "tie-payload"
	kindTieService := "tie-kind"
	events := make([]semanticBytesLineageEvent, 0, 16)
	appendModeGroup := func(
		service string,
		winner []byte,
		encoding opensplunkv1.RawEncoding,
	) {
		serviceCopy := service
		for repeat := 0; repeat < 3; repeat++ {
			events = append(events, semanticBytesLineageEvent{
				id:  fmt.Sprintf("mode-%s-winner-%d", service, repeat),
				at:  earliest.Add(time.Duration(len(events)+1) * time.Minute),
				raw: winner, encoding: encoding, source: "mode", service: &serviceCopy,
			})
		}
		events = append(events, semanticBytesLineageEvent{
			id:       fmt.Sprintf("mode-%s-loser", service),
			at:       earliest.Add(time.Duration(len(events)+1) * time.Minute),
			raw:      []byte("ordinary-loser"),
			encoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			source:   "mode", service: &serviceCopy,
		})
	}
	appendModeGroup(utf8Service, []byte("modal-utf8-界"), opensplunkv1.RawEncoding_RAW_ENCODING_UTF8)
	appendModeGroup(binaryValidService, []byte("modal-binary-valid"), opensplunkv1.RawEncoding_RAW_ENCODING_BINARY)
	appendModeGroup(binaryInvalidService, invalid, opensplunkv1.RawEncoding_RAW_ENCODING_BINARY)
	appendModeTie := func(
		service string,
		firstRaw []byte,
		firstEncoding opensplunkv1.RawEncoding,
		secondRaw []byte,
		secondEncoding opensplunkv1.RawEncoding,
	) {
		serviceCopy := service
		for ordinal, value := range []struct {
			raw      []byte
			encoding opensplunkv1.RawEncoding
		}{
			{raw: firstRaw, encoding: firstEncoding},
			{raw: secondRaw, encoding: secondEncoding},
		} {
			events = append(events, semanticBytesLineageEvent{
				id:  fmt.Sprintf("mode-%s-tie-%d", service, ordinal),
				at:  earliest.Add(time.Duration(len(events)+1) * time.Minute),
				raw: value.raw, encoding: value.encoding, source: "mode", service: &serviceCopy,
			})
		}
	}
	// Bytewise payload order wins before semantic kind: binary "a" precedes
	// ordinary String "z" even though its semantic bit sorts later.
	appendModeTie(
		payloadTieService,
		[]byte("a"),
		opensplunkv1.RawEncoding_RAW_ENCODING_BINARY,
		[]byte("z"),
		opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
	)
	// Identical payloads remain distinct modal values. Their equal-count tie is
	// resolved String before Bytes by the local deterministic policy.
	appendModeTie(
		kindTieService,
		[]byte("same"),
		opensplunkv1.RawEncoding_RAW_ENCODING_BINARY,
		[]byte("same"),
		opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
	)
	ctx, executor := semanticBytesLineageStartClickHouse(t, indexTime, events)

	page := semanticBytesLineageRunManagerSearch(
		t,
		ctx,
		executor,
		indexTime,
		"semantic-mode",
		`index=`+semanticBytesLineageIndex+` source="mode"`+
			` | stats mode(_raw) AS modal BY service`+
			` | eval lowered=lower(modal)`+
			` | table service modal lowered | sort 0 +service`,
		earliest,
		latest,
	)
	semanticBytesLineageRequireSchema(t, page, []string{"service", "modal", "lowered"})
	if page.Schema.Columns[1] != (searchjobs.Column{
		Name: "modal", Kind: searchjobs.ValueKindMixed, Nullable: true,
	}) {
		t.Fatalf("mode schema = %#v, want nullable sealed Mixed output", page.Schema.Columns[1])
	}
	if page.Schema.Columns[2] != (searchjobs.Column{
		Name: "lowered", Kind: searchjobs.ValueKindString, Nullable: true,
	}) {
		t.Fatalf("mode lower schema = %#v, want nullable String", page.Schema.Columns[2])
	}
	if len(page.Rows) != 5 || !page.Complete || page.TotalRows != 5 {
		t.Fatalf("mode page = rows %d total %d complete=%t", len(page.Rows), page.TotalRows, page.Complete)
	}
	want := map[string]semanticBytesLineageExpected{
		utf8Service: {
			kind: searchjobs.ValueKindString, payload: []byte("modal-utf8-界"),
		},
		binaryValidService: {
			kind: searchjobs.ValueKindBytes, payload: []byte("modal-binary-valid"),
		},
		binaryInvalidService: {
			kind: searchjobs.ValueKindBytes, payload: invalid,
		},
		payloadTieService: {
			kind: searchjobs.ValueKindBytes, payload: []byte("a"),
		},
		kindTieService: {
			kind: searchjobs.ValueKindString, payload: []byte("same"),
		},
	}
	seen := make(map[string]struct{}, len(page.Rows))
	for _, row := range page.Rows {
		service, ok := row.Values[0].String()
		if !ok {
			t.Fatalf("mode service = %#v, want String", row.Values[0])
		}
		expected, found := want[service]
		if !found {
			t.Fatalf("unexpected mode service %q", service)
		}
		if _, duplicate := seen[service]; duplicate {
			t.Fatalf("duplicate mode service %q", service)
		}
		seen[service] = struct{}{}
		semanticBytesLineageRequireValue(t, row.Values[1], expected)
		if expected.kind == searchjobs.ValueKindBytes {
			if !row.Values[2].IsNull() {
				t.Fatalf(
					"mode service %q lower(Bytes winner) = %#v, want null",
					service,
					row.Values[2],
				)
			}
			continue
		}
		lowered, ok := row.Values[2].String()
		if !ok || lowered != strings.ToLower(string(expected.payload)) {
			t.Fatalf(
				"mode service %q lower(String winner) = %q (%t), want %q",
				service,
				lowered,
				ok,
				strings.ToLower(string(expected.payload)),
			)
		}
	}

	regrouped := semanticBytesLineageRunManagerSearch(
		t,
		ctx,
		executor,
		indexTime,
		"semantic-mode-regroup",
		`index=`+semanticBytesLineageIndex+` source="mode"`+
			` | stats mode(_raw) AS modal BY service`+
			` | stats count BY modal | table modal count | sort 0 +modal`,
		earliest,
		latest,
	)
	semanticBytesLineageRequireSchema(t, regrouped, []string{"modal", "count"})
	if regrouped.Schema.Columns[0] != (searchjobs.Column{
		Name: "modal", Kind: searchjobs.ValueKindMixed, Nullable: true,
	}) || regrouped.Schema.Columns[1] != (searchjobs.Column{
		Name: "count", Kind: searchjobs.ValueKindUnsigned,
	}) {
		t.Fatalf("mode re-group schema = %#v", regrouped.Schema)
	}
	regroupedWant := make([]semanticBytesLineageExpected, 0, len(want))
	for _, expected := range want {
		regroupedWant = append(regroupedWant, expected)
	}
	if len(regrouped.Rows) != len(regroupedWant) || !regrouped.Complete ||
		regrouped.TotalRows != uint64(len(regroupedWant)) {
		t.Fatalf(
			"mode re-group page = rows %d total %d complete=%t, want %d",
			len(regrouped.Rows),
			regrouped.TotalRows,
			regrouped.Complete,
			len(regroupedWant),
		)
	}
	for _, row := range regrouped.Rows {
		matched := -1
		for index, expected := range regroupedWant {
			if semanticBytesLineageValueMatches(row.Values[0], expected) {
				matched = index
				break
			}
		}
		if matched < 0 {
			t.Fatalf("unexpected mode re-group value %#v", row.Values[0])
		}
		count, ok := row.Values[1].Unsigned()
		if !ok || count != 1 {
			t.Fatalf("mode re-group count = %#v, want unsigned 1", row.Values[1])
		}
		regroupedWant = append(regroupedWant[:matched], regroupedWant[matched+1:]...)
	}
	if len(regroupedWant) != 0 {
		t.Fatalf("missing mode re-group values: %#v", regroupedWant)
	}

	projected := semanticBytesLineageRunManagerSearch(
		t,
		ctx,
		executor,
		indexTime,
		"semantic-mode-projection-composition",
		`index=`+semanticBytesLineageIndex+` source="mode"`+
			` | stats mode(_raw) AS modal BY service`+
			` | eval selected=if(service=service,modal,null)`+
			` | table service selected`+
			` | eval lowered=lower(selected)`+
			` | table service selected lowered | sort 0 +service`,
		earliest,
		latest,
	)
	semanticBytesLineageRequireSchema(
		t,
		projected,
		[]string{"service", "selected", "lowered"},
	)
	if projected.Schema.Columns[1] != (searchjobs.Column{
		Name: "selected", Kind: searchjobs.ValueKindMixed, Nullable: true,
	}) || projected.Schema.Columns[2] != (searchjobs.Column{
		Name: "lowered", Kind: searchjobs.ValueKindString, Nullable: true,
	}) {
		t.Fatalf("mode projection-composition schema = %#v", projected.Schema)
	}
	if len(projected.Rows) != len(want) || !projected.Complete ||
		projected.TotalRows != uint64(len(want)) {
		t.Fatalf(
			"mode projection-composition page = rows %d total %d complete=%t, want %d",
			len(projected.Rows),
			projected.TotalRows,
			projected.Complete,
			len(want),
		)
	}
	projectedSeen := make(map[string]struct{}, len(projected.Rows))
	for _, row := range projected.Rows {
		service, ok := row.Values[0].String()
		if !ok {
			t.Fatalf("mode projection-composition service = %#v, want String", row.Values[0])
		}
		expected, found := want[service]
		if !found {
			t.Fatalf("unexpected mode projection-composition service %q", service)
		}
		if _, duplicate := projectedSeen[service]; duplicate {
			t.Fatalf("duplicate mode projection-composition service %q", service)
		}
		projectedSeen[service] = struct{}{}
		semanticBytesLineageRequireValue(t, row.Values[1], expected)
		if expected.kind == searchjobs.ValueKindBytes {
			if !row.Values[2].IsNull() {
				t.Fatalf(
					"mode projection-composition service %q lower(Bytes) = %#v, want null",
					service,
					row.Values[2],
				)
			}
			continue
		}
		lowered, ok := row.Values[2].String()
		if !ok || lowered != strings.ToLower(string(expected.payload)) {
			t.Fatalf(
				"mode projection-composition service %q lower(String) = %q (%t), want %q",
				service,
				lowered,
				ok,
				strings.ToLower(string(expected.payload)),
			)
		}
	}
}

// TestSparklineFeedsStatsByThroughManagerAgainstClickHouse covers the fixed
// Array(String) producer alongside the generic String/Bytes result sidecar.
// Downstream stats BY expands both generated count members and extrema members
// containing invalid UTF-8 without leaking private transport columns.
func TestSparklineFeedsStatsByThroughManagerAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}

	earliest := time.Date(2026, time.August, 13, 0, 0, 0, 0, time.UTC)
	latest := earliest.Add(4 * time.Hour)
	indexTime := latest.Add(time.Hour)
	invalid := []byte{0xff, 's', 'p', 'a', 'r', 'k', 0xfe}
	hosts := [][]string{
		{"alpha"},
		{"beta", string(invalid)},
		{"delta-0", "delta-1", "gamma"},
		{"a", "b", "c", "omega"},
	}
	events := make([]semanticBytesLineageEvent, 0, 10)
	for hour := 0; hour < 4; hour++ {
		for ordinal := 0; ordinal <= hour; ordinal++ {
			events = append(events, semanticBytesLineageEvent{
				id:       fmt.Sprintf("sparkline-%d-%d", hour, ordinal),
				at:       earliest.Add(time.Duration(hour)*time.Hour + time.Duration(ordinal+1)*time.Minute),
				raw:      []byte("sparkline"),
				encoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
				host:     hosts[hour][ordinal],
				source:   "sparkline",
			})
		}
	}
	ctx, executor := semanticBytesLineageStartClickHouse(t, indexTime, events)

	t.Run("generated count members remain strings", func(t *testing.T) {
		page := semanticBytesLineageRunManagerSearch(
			t,
			ctx,
			executor,
			indexTime,
			"semantic-sparkline-count-stats-by",
			`index=`+semanticBytesLineageIndex+` source="sparkline"`+
				` | stats sparkline(count,1h) AS trend`+
				` | stats count BY trend | table trend count | sort 0 +trend`,
			earliest,
			latest,
		)
		semanticBytesLineageRequireSchema(t, page, []string{"trend", "count"})
		if page.Schema.Columns[0] != (searchjobs.Column{
			Name: "trend", Kind: searchjobs.ValueKindString,
		}) || page.Schema.Columns[1] != (searchjobs.Column{
			Name: "count", Kind: searchjobs.ValueKindUnsigned,
		}) {
			t.Fatalf("count sparkline stats-BY schema = %#v", page.Schema)
		}
		want := map[string]uint64{
			"##__SPARKLINE__##": 1,
			"1":                 1,
			"2":                 1,
			"3":                 1,
			"4":                 1,
		}
		semanticBytesLineageRequireStringCounts(t, page, want)
	})

	t.Run("extrema invalid UTF8 member remains bytes", func(t *testing.T) {
		page := semanticBytesLineageRunManagerSearch(
			t,
			ctx,
			executor,
			indexTime,
			"semantic-sparkline-extrema-stats-by",
			`index=`+semanticBytesLineageIndex+` source="sparkline"`+
				` | stats sparkline(max(host),1h) AS trend`+
				` | stats count BY trend | table trend count | sort 0 +trend`,
			earliest,
			latest,
		)
		semanticBytesLineageRequireSchema(t, page, []string{"trend", "count"})
		if page.Schema.Columns[0] != (searchjobs.Column{
			Name: "trend", Kind: searchjobs.ValueKindMixed, Nullable: true,
		}) || page.Schema.Columns[1] != (searchjobs.Column{
			Name: "count", Kind: searchjobs.ValueKindUnsigned,
		}) {
			t.Fatalf("extrema sparkline stats-BY schema = %#v", page.Schema)
		}
		want := []semanticBytesLineageExpected{
			{kind: searchjobs.ValueKindString, payload: []byte("##__SPARKLINE__##")},
			{kind: searchjobs.ValueKindString, payload: []byte("alpha")},
			{kind: searchjobs.ValueKindBytes, payload: invalid},
			{kind: searchjobs.ValueKindString, payload: []byte("gamma")},
			{kind: searchjobs.ValueKindString, payload: []byte("omega")},
		}
		if len(page.Rows) != len(want) || !page.Complete || page.TotalRows != uint64(len(want)) {
			t.Fatalf(
				"extrema sparkline stats-BY page = rows %d total %d complete=%t, want %d",
				len(page.Rows), page.TotalRows, page.Complete, len(want),
			)
		}
		for _, row := range page.Rows {
			matched := -1
			for index, expected := range want {
				if semanticBytesLineageValueMatches(row.Values[0], expected) {
					matched = index
					break
				}
			}
			if matched < 0 {
				t.Fatalf("unexpected extrema sparkline member %#v", row.Values[0])
			}
			count, ok := row.Values[1].Unsigned()
			if !ok || count != 1 {
				t.Fatalf("extrema sparkline count = %#v, want unsigned 1", row.Values[1])
			}
			want = append(want[:matched], want[matched+1:]...)
		}
		if len(want) != 0 {
			t.Fatalf("missing extrema sparkline members: %#v", want)
		}
	})
}

func semanticBytesLineageStartClickHouse(
	t *testing.T,
	indexTime time.Time,
	events []semanticBytesLineageEvent,
) (context.Context, *Executor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse image: %s", container.Image)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupCtx); closeErr != nil {
			t.Errorf("close semantic String/Bytes container: %v", closeErr)
		}
	})
	queryIntegrationMigrate(t, ctx, container.Name, container.Password)

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := connection.PrepareBatch(ctx, `
		INSERT INTO open_splunk.events
		(
			event_id, tenant_id, index_name, event_time, index_time,
			collected_at, event_time_source, host, source, sourcetype,
			service, severity, level, body, raw, raw_encoding, trace_id,
			span_id, fields, field_names, collector_id, batch_id,
			batch_sequence, expires_at, visibility_seq
		)`)
	if err != nil {
		t.Fatalf("prepare semantic String/Bytes fixtures: %v", err)
	}
	for index, event := range events {
		host := event.host
		if host == "" {
			host = "semantic-bytes"
		}
		if err := batch.Append(
			event.id,
			"tenant",
			semanticBytesLineageIndex,
			event.at,
			indexTime,
			nil,
			uint8(1),
			host,
			event.source,
			"test",
			event.service,
			uint8(1),
			nil,
			nil,
			event.raw,
			uint8(event.encoding),
			nil,
			nil,
			clickhousedriver.NewJSON(),
			[]string{},
			"semantic-bytes",
			"batch",
			uint64(index+1),
			clickhouse.MaximumSearchTime(),
			uint64(1),
		); err != nil {
			_ = batch.Abort()
			t.Fatalf("append semantic String/Bytes fixture %q: %v", event.id, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send semantic String/Bytes fixtures: %v", err)
	}

	executor, err := New(connection, Config{ReadAdmission: indexread.UnfencedAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, executor
}

func semanticBytesLineageRunManagerSearch(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	indexTime time.Time,
	id string,
	source string,
	earliest time.Time,
	latest time.Time,
) searchjobs.ResultPage {
	t.Helper()
	job, page := queryIntegrationRunSearchRangeForIndex(
		t,
		ctx,
		executor,
		indexTime,
		id,
		source,
		earliest,
		latest,
		semanticBytesLineageIndex,
	)
	if job.State != searchjobs.StateCompleted {
		t.Fatalf("search %q state = %v, failure=%#v", source, job.State, job.Failure)
	}
	return page
}

func semanticBytesLineageRequireSchema(
	t *testing.T,
	page searchjobs.ResultPage,
	want []string,
) {
	t.Helper()
	if len(page.Schema.Columns) != len(want) {
		t.Fatalf("schema = %#v, want columns %v", page.Schema, want)
	}
	for index, name := range want {
		column := page.Schema.Columns[index]
		if column.Name != name {
			t.Fatalf("schema column %d = %#v, want name %q", index, column, name)
		}
		if strings.HasPrefix(column.Name, "__os_") {
			t.Fatalf("private semantic sidecar leaked through public schema: %#v", page.Schema)
		}
	}
}

func semanticBytesLineageRequireValue(
	t *testing.T,
	got searchjobs.Value,
	want semanticBytesLineageExpected,
) {
	t.Helper()
	if got.Kind() != want.kind {
		t.Fatalf("value kind = %v, want %v; value=%#v", got.Kind(), want.kind, got)
	}
	switch want.kind {
	case searchjobs.ValueKindNull:
		if !got.IsNull() {
			t.Fatalf("value = %#v, want null", got)
		}
	case searchjobs.ValueKindString:
		value, ok := got.String()
		if !ok || value != string(want.payload) {
			t.Fatalf("String value = %q (%t), want %q", value, ok, string(want.payload))
		}
	case searchjobs.ValueKindBytes:
		value, ok := got.Bytes()
		if !ok || !bytes.Equal(value, want.payload) {
			t.Fatalf("Bytes value = %v (%t), want %v", value, ok, want.payload)
		}
	default:
		t.Fatalf("test expected unsupported value kind %v", want.kind)
	}
}

func semanticBytesLineageValueMatches(
	got searchjobs.Value,
	want semanticBytesLineageExpected,
) bool {
	if got.Kind() != want.kind {
		return false
	}
	switch want.kind {
	case searchjobs.ValueKindNull:
		return got.IsNull()
	case searchjobs.ValueKindString:
		value, ok := got.String()
		return ok && value == string(want.payload)
	case searchjobs.ValueKindBytes:
		value, ok := got.Bytes()
		return ok && bytes.Equal(value, want.payload)
	default:
		return false
	}
}

func semanticBytesLineageRequireStringCounts(
	t *testing.T,
	page searchjobs.ResultPage,
	want map[string]uint64,
) {
	t.Helper()
	if len(page.Rows) != len(want) || !page.Complete || page.TotalRows != uint64(len(want)) {
		t.Fatalf(
			"sparkline stats-BY page = rows %d total %d complete=%t, want %d",
			len(page.Rows), page.TotalRows, page.Complete, len(want),
		)
	}
	for _, row := range page.Rows {
		member, ok := row.Values[0].String()
		if !ok {
			t.Fatalf("sparkline stats-BY member = %#v, want String", row.Values[0])
		}
		expected, found := want[member]
		if !found {
			t.Fatalf("unexpected sparkline stats-BY member %q", member)
		}
		count, ok := row.Values[1].Unsigned()
		if !ok || count != expected {
			t.Fatalf("sparkline member %q count = %#v, want %d", member, row.Values[1], expected)
		}
		delete(want, member)
	}
	if len(want) != 0 {
		t.Fatalf("missing sparkline stats-BY members: %#v", want)
	}
}
