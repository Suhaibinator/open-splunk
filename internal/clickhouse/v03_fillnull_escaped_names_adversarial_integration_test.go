package clickhouse

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const v03EscapedFillNullSource = "v03-fillnull-escaped-names"

func TestCompileV03FillNullEscapedPhysicalNameCompositions(t *testing.T) {
	t.Parallel()

	sources := make([]string, 0, 32)
	for _, name := range []string{
		`literal\.dot`, `slash\\leaf`, `mixed\\slash\.dot`, `question?mark`,
		`dollar$1`, `brace{name}`, `semi:semicolon`, `κλειδί界💥`,
	} {
		sources = append(sources,
			`index=gradethis | eval `+name+`="fixed"`+
				` | fillnull value="fallback" `+name+` | table event_id `+name,
		)
	}
	for _, name := range []string{
		`number\.dot`, `number\\slash`, `number?mark`, `number$1`,
		`number{brace}`, `数値界`,
	} {
		sources = append(sources,
			`index=gradethis | eval `+name+`=7`+
				` | fillnull value="fallback" `+name+` | table event_id `+name,
		)
	}
	for _, name := range []string{
		`dynamic\.dot`, `dynamic\\slash`, `dynamic?mark`, `dynamic$1`,
		`dynamic{brace}`, `動的界`,
	} {
		sources = append(sources,
			`index=gradethis | fillnull value="fallback" `+name+
				` | table event_id `+name,
		)
	}
	for _, barrier := range []string{
		``,
		` | fields event_id calculated\\slash\.dot`,
		` | table event_id calculated\\slash\.dot`,
	} {
		sources = append(sources,
			`index=gradethis | eval calculated\\slash\.dot=coalesce(host,source)`+
				barrier+` | fillnull value="fallback" calculated\\slash\.dot`+
				` | table event_id calculated\\slash\.dot`,
		)
	}
	sources = append(sources,
		`index=gradethis | eval literal\.dot="kept" | fillnull literal\.dot`+
			` | search literal\.dot="kept" | table event_id literal\.dot`,
		`index=gradethis | eval dollar$1="kept" | fillnull dollar$1`+
			` | stats count BY dollar$1`,
		`index=gradethis | eval brace{name}="kept" | fillnull brace{name}`+
			` | chart count OVER brace{name} BY event_id`,
		`index=gradethis | eval multi\\slash?mark="a,b"`+
			` | makemv delim="," multi\\slash?mark`+
			` | fillnull multi\\slash?mark | mvexpand multi\\slash?mark`+
			` | table event_id multi\\slash?mark`,
	)

	for _, source := range sources {
		compiled := compileSPL(t, source)
		if !compiled.HasValidExecutionSeal() {
			t.Fatalf("escaped-name composition lacks a valid execution seal: %s", source)
		}
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("placeholder/argument count for %q = %d/%d\nSQL: %s",
				source, got, want, compiled.SQL)
		}
	}
}

func TestV03FillNullWeirdFieldAdmissionBoundary(t *testing.T) {
	t.Parallel()

	admitted := []string{
		`literal\.dot`,
		`slash\\leaf`,
		`mixed\\slash\.dot`,
		`question?mark`,
		`dollar$1`,
		`brace{name}`,
		`semi:semicolon`,
		`κλειδί界💥`,
	}
	for _, name := range admitted {
		if !spl.IsExactUnquotedFieldName(name) {
			t.Errorf("admitted weird field %q is not one exact unquoted token", name)
			continue
		}
		if _, err := plan.ResolveField(name, spl.Range{}); err != nil {
			t.Errorf("resolve admitted weird field %q: %v", name, err)
		}
		if _, err := spl.Parse(`index=main | fillnull value="fallback" ` + name); err != nil {
			t.Errorf("parse admitted weird fillnull field %q: %v", name, err)
		}
	}

	// v0.3 command fields are deliberately exact and unquoted. Quotes are not
	// part of this surface, so they must fail before any backend identifier is
	// generated rather than being smuggled through hexadecimal SQL escaping.
	for _, name := range []string{
		`'single quoted'`,
		`"double quoted"`,
		"`backtick quoted`",
		`embedded'quote`,
		`embedded"quote`,
		"embedded`quote",
	} {
		if spl.IsExactUnquotedFieldName(name) {
			t.Errorf("quoted field %q was admitted as exact unquoted syntax", name)
		}
		if _, err := spl.Parse(`index=main | fillnull value="fallback" ` + name); err == nil {
			t.Errorf("quoted fillnull field %q parsed successfully", name)
		}
	}

	// Control bytes are never valid path segments even when a legacy lexical
	// boundary could otherwise consume them as part of one token.
	for _, name := range []string{
		"control\x00nul",
		"control\nnewline",
		"control\t tab",
		"control\x7fdel",
		"control\u0080c1",
	} {
		if _, err := plan.ResolveField(name, spl.Range{}); err == nil {
			t.Errorf("control-bearing field %q resolved successfully", name)
		}
	}

	// A stats alias may be an opaque literal result name even when it is not a
	// canonical path. That does not make the same spelling referenceable by a
	// later command. The quoted spelling is rejected lexically; the legacy
	// unquoted spelling reaches planning and is rejected at that boundary.
	quoted := `index=main | stats count AS "parent..child" | table 'parent..child'`
	_, quotedErr := spl.Parse(quoted)
	var diagnostic *spl.Diagnostic
	if !errors.As(quotedErr, &diagnostic) || diagnostic.Code != "SPL_INVALID_FIELD" {
		t.Errorf("parse invalid quoted literal-path reference error = %#v, want SPL_INVALID_FIELD", quotedErr)
	}
	unquoted := `index=main | stats count AS "parent..child" | table parent..child`
	parsed, parseErr := spl.Parse(unquoted)
	if parseErr != nil {
		t.Fatalf("parse legacy unquoted literal-path reference: %v", parseErr)
	}
	visibility := uint64(1)
	boundary := time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC)
	_, buildErr := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		Earliest:          boundary.Add(-time.Hour),
		Latest:            boundary.Add(time.Hour),
		SearchStart:       boundary,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   boundary,
		VisibilityCutoff:  &visibility,
	})
	var planDiagnostic *plan.Diagnostic
	if !errors.As(buildErr, &planDiagnostic) || planDiagnostic.Code != "SPL_INVALID_FIELD" {
		t.Errorf("plan invalid unquoted literal-path reference error = %#v, want SPL_INVALID_FIELD", buildErr)
	}
}

// TestV03FillNullEscapedPhysicalNamesAgainstClickHouse pins the boundary
// between normalized SPL field spellings and binder-safe ClickHouse quoted
// identifiers. In particular, an eval-created physical column is filled under
// the exact same escaped name instead of being mistaken for the hexadecimal
// SQL spelling emitted by quoteIdentifier.
func TestV03FillNullEscapedPhysicalNamesAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	eventAnchor := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	newEvent := func(
		id string,
		ordinal int,
		fields ...*opensplunkv1.TypedObjectField,
	) *ingest.StoredEvent {
		event := testStoredEvent(id, "spl-v03-fillnull-escaped", indexTime)
		event.Event.Source = v03EscapedFillNullSource
		event.Event.EventTime = timestamppb.New(eventAnchor.Add(time.Duration(ordinal) * time.Second))
		event.Event.CollectedAt = timestamppb.New(event.Event.EventTime.AsTime().Add(-time.Second))
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}
	fixtures := []*ingest.StoredEvent{
		newEvent(
			"weird-1",
			1,
			typedField("dynamic_scalar", typedString("dynamic-keep-界")),
			typedField("dynamic_list", typedList(typedString("a"), typedString("β"))),
		),
		newEvent(
			"weird-2",
			2,
			typedField("dynamic_scalar", typedNull()),
			typedField("dynamic_list", typedNull()),
		),
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"spl-v03-fillnull-escaped",
		"v03-fillnull-escaped-names",
		1,
		fixtures...,
	)
	base := `index=spl-v03-fillnull-escaped source="` +
		v03EscapedFillNullSource + `" | sort 0 +event_id`
	assertRows := func(source string, fields []string, want [][]string) {
		t.Helper()
		compiled := compile(source)
		if !compiled.HasValidExecutionSeal() {
			t.Fatalf("weird-name query lacks a valid execution seal: %s", source)
		}
		v03AssertJSONRows(t, queryContext, connection, compiled, fields, want)
	}
	constantRows := func(value string) [][]string {
		return [][]string{{"weird-1", value}, {"weird-2", value}}
	}

	for _, name := range []string{
		`literal\.dot`,
		`slash\\leaf`,
		`mixed\\slash\.dot`,
		`question?mark`,
		`dollar$1`,
		`brace{name}`,
		`semi:semicolon`,
		`κλειδί界💥`,
	} {
		name := name
		t.Run("fixed String "+name, func(_ *testing.T) {
			assertRows(
				base+` | eval `+name+`="fixed-keep-界"`+
					` | fillnull value="fixed-fallback" `+name+
					` | table event_id `+name,
				[]string{"event_id", name},
				constantRows("fixed-keep-界"),
			)
		})
	}

	for _, barrier := range []struct {
		name, spl string
	}{
		{name: "direct"},
		{name: "fields", spl: ` | fields event_id literal\.dot`},
		{name: "table", spl: ` | table event_id literal\.dot`},
	} {
		barrier := barrier
		t.Run("fixed String escaped dot "+barrier.name, func(_ *testing.T) {
			assertRows(
				base+` | eval literal\.dot="barrier-keep"`+barrier.spl+
					` | fillnull value="barrier-fallback" literal\.dot`+
					` | table event_id literal\.dot`,
				[]string{"event_id", `literal\.dot`},
				constantRows("barrier-keep"),
			)
		})
	}

	for _, name := range []string{
		`number\.dot`,
		`number\\slash`,
		`number?mark`,
		`number$1`,
		`number{brace}`,
		`数値界`,
	} {
		name := name
		t.Run("fixed Number "+name, func(_ *testing.T) {
			assertRows(
				base+` | eval `+name+`=7`+
					` | fillnull value="number-fallback" `+name+
					` | table event_id `+name,
				[]string{"event_id", name},
				constantRows("7"),
			)
		})
	}

	for _, name := range []string{
		`dynamic\.dot`,
		`dynamic\\slash`,
		`dynamic?mark`,
		`dynamic$1`,
		`dynamic{brace}`,
		`動的界`,
	} {
		name := name
		t.Run("direct Dynamic "+name, func(_ *testing.T) {
			assertRows(
				base+` | eval `+name+`=dynamic_scalar`+
					` | fillnull value="dynamic-fallback" `+name+
					` | table event_id `+name,
				[]string{"event_id", name},
				[][]string{
					{"weird-1", "dynamic-keep-界"},
					{"weird-2", "dynamic-fallback"},
				},
			)
		})
	}

	for _, barrier := range []struct {
		name, spl string
	}{
		{name: "direct"},
		{name: "fields", spl: ` | fields event_id calculated\\slash\.dot`},
		{name: "table", spl: ` | table event_id calculated\\slash\.dot`},
	} {
		barrier := barrier
		t.Run("calculated Dynamic "+barrier.name, func(_ *testing.T) {
			assertRows(
				base+` | eval calculated\\slash\.dot=dynamic_scalar`+
					barrier.spl+
					` | fillnull value="calculated-fallback" calculated\\slash\.dot`+
					` | table event_id calculated\\slash\.dot`,
				[]string{"event_id", `calculated\\slash\.dot`},
				[][]string{
					{"weird-1", "dynamic-keep-界"},
					{"weird-2", "calculated-fallback"},
				},
			)
		})
	}

	t.Run("downstream search", func(_ *testing.T) {
		assertRows(
			base+` | eval literal\.dot="search-keep"`+
				` | fillnull value="search-fallback" literal\.dot`+
				` | search literal\.dot="search-keep" | table event_id literal\.dot`,
			[]string{"event_id", `literal\.dot`},
			constantRows("search-keep"),
		)
	})
	t.Run("downstream stats", func(_ *testing.T) {
		assertRows(
			base+` | eval dollar$1="stats-keep"`+
				` | fillnull value="stats-fallback" dollar$1`+
				` | stats count BY dollar$1 | table dollar$1 count`,
			[]string{`dollar$1`, "count"},
			[][]string{{"stats-keep", "2"}},
		)
	})
	t.Run("downstream chart", func(t *testing.T) {
		compiled := compile(
			base + ` | eval brace{name}="chart-keep"` +
				` | fillnull value="chart-fallback" brace{name}` +
				` | chart count OVER brace{name} BY event_id`,
		)
		want := []chartEdgeTransportRow{{
			ordinal: 0,
			row:     "chart-keep",
			names:   "0:weird-1|0:weird-2",
			counts:  "1|1",
		}}
		if got := chartEdgeTransport(t, queryContext, connection, compiled); !reflect.DeepEqual(got, want) {
			t.Fatalf("weird-name chart = %#v, want %#v", got, want)
		}
	})
	t.Run("downstream mvexpand", func(_ *testing.T) {
		assertRows(
			base+` | eval multi\\slash?mark=dynamic_list`+
				` | fillnull value="mv-fallback" multi\\slash?mark`+
				` | mvexpand multi\\slash?mark | table event_id multi\\slash?mark`,
			[]string{"event_id", `multi\\slash?mark`},
			[][]string{
				{"weird-1", "a"},
				{"weird-1", "β"},
				{"weird-2", "mv-fallback"},
			},
		)
	})
}
