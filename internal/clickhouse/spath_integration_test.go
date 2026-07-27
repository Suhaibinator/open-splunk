package clickhouse

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	spathIntegrationIndex  = "spath-edge"
	spathLimitIndex        = "spath-limit"
	spathCorruptIndex      = "spath-corrupt"
	spathIntegrationTenant = "tenant"
)

// TestCompileSequentialSpathArgumentAccounting guards the placeholder ordering
// required before a sequential pipeline can be sent through the native driver.
// It is intentionally not opt-in: this part of the integration boundary is a
// pure compiler invariant and should fail in the ordinary unit suite.
func TestCompileSequentialSpathArgumentAccounting(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name:   "one stage",
			source: `index=gradethis | spath output=first path=payload.first`,
		},
		{
			name:   "two independent stages",
			source: `index=gradethis | spath output=first path=payload.first | spath output=second path=payload.second`,
		},
		{
			name:   "second stage consumes first",
			source: `index=gradethis | spath output=first path=payload.first | spath input=first output=second path=nested.value`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			compiled := compileSPL(t, test.source)
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("%q placeholder count = %d, args = %d",
					test.source, got, want)
			}
		})
	}
}

// TestSpathAgainstClickHouse executes the bounded JSON-path contract against
// the repository's pinned ClickHouse release. It owns one disposable server
// and one small fixture so it can run independently of the other opt-in suites.
func TestSpathAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	connection, store := spathStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)
	visibilityCutoff := spathStoreFixture(t, ctx, store, indexTime)
	compile := func(t *testing.T, source string) CompiledQuery {
		t.Helper()
		return spathCompile(t, source, indexTime.Add(time.Minute), visibilityCutoff)
	}

	t.Run("typed scalar leaves and zero based array indexes", func(t *testing.T) {
		for _, test := range []struct {
			name string
			path string
			want string
		}{
			{name: "string", path: "payload.text", want: "String/hello"},
			{name: "empty string", path: "payload.empty", want: "String/"},
			{name: "Boolean", path: "payload.flag", want: "Bool/true"},
			{name: "false Boolean", path: "payload.false", want: "Bool/false"},
			{name: "null", path: "payload.nothing", want: "None/<none>"},
			{name: "zero", path: "payload.zero", want: "Int64/0"},
			{name: "negative zero", path: "payload.negative_zero", want: "Int64/0"},
			{name: "signed integer", path: "payload.signed", want: "Int64/-9"},
			{
				name: "minimum signed integer",
				path: "payload.signed_min",
				want: "Int64/-9223372036854775808",
			},
			{
				name: "maximum signed integer",
				path: "payload.signed_max",
				want: "Int64/9223372036854775807",
			},
			{
				name: "first unsigned integer",
				path: "payload.unsigned_min",
				want: "UInt64/9223372036854775808",
			},
			{
				name: "unsigned integer",
				path: "payload.unsigned",
				want: "UInt64/18446744073709551615",
			},
			{name: "zero array index", path: "payload.items{0}.name", want: "String/zero"},
			{name: "one array index", path: "payload.items{1}.name", want: "String/one"},
			{name: "first duplicate member", path: "payload.duplicate", want: "String/first"},
		} {
			t.Run(test.name, func(t *testing.T) {
				compiled := compile(t, `index=spath-edge event_id=s-scalars
| spath output=selected path=`+test.path+`
| table selected`)
				got := spathRows(t, ctx, connection,
					`SELECT concat(dynamicType(selected), '/',
						if(dynamicType(selected) = 'None', '<none>', toString(selected)))
					FROM (`+compiled.SQL+`)`,
					compiled.Args, 1)
				if !reflect.DeepEqual(got, [][]string{{test.want}}) {
					t.Fatalf("spath %s value = %#v, want %q", test.name, got, test.want)
				}
			})
		}
	})

	t.Run("miss malformed and non string inputs preserve destinations", func(t *testing.T) {
		compiled := compile(t, `index=spath-edge
| spath output=prior path=payload.text
| table event_id prior`)
		got := spathRows(t, ctx, connection,
			`SELECT event_id, concat(dynamicType(prior), '/',
				if(dynamicType(prior) = 'None', '<none>', toString(prior)))
			FROM (`+compiled.SQL+`) ORDER BY event_id`,
			compiled.Args, 2)
		want := [][]string{
			{"s-binary", "None/<none>"},
			{"s-malformed", "Bool/false"},
			{"s-miss", "Int64/17"},
			{"s-scalars", "String/hello"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("sparse spath destination values = %#v, want %#v", got, want)
		}

		nonString := compile(t, `index=spath-edge event_id=s-scalars
| spath input=numeric_source output=prior path=value
| table prior`)
		nonStringRows := spathRows(t, ctx, connection,
			`SELECT concat(dynamicType(prior), '/', toString(prior))
			FROM (`+nonString.SQL+`)`,
			nonString.Args, 1)
		if !reflect.DeepEqual(nonStringRows, [][]string{{"String/old-scalar"}}) {
			t.Fatalf("non-string source destination = %#v, want old String value", nonStringRows)
		}
	})

	t.Run("valid UTF8 bytes marked binary stay ineligible through copies and misses", func(t *testing.T) {
		for _, source := range []string{
			`index=spath-edge event_id=s-binary
| spath output=selected path=payload.text
| table selected`,
			`index=spath-edge event_id=s-binary
| eval copied=_raw
| spath input=copied output=selected path=payload.text
| table selected`,
			`index=spath-edge event_id=s-binary
| eval copied=if(isnull(absent), _raw, _raw)
| spath input=copied output=selected path=payload.text
| table selected`,
			`index=spath-edge event_id=s-binary
| rename _raw AS copied
| spath input=copied output=selected path=payload.text
| table selected`,
			`index=spath-edge event_id=s-binary
| spath output=_raw path=missing
| spath output=selected path=payload.text
| table selected`,
			`index=spath-edge event_id=s-binary
| rex field=_raw "(?<_raw>never)"
| spath output=selected path=payload.text
| table selected`,
		} {
			compiled := compile(t, source)
			got := spathRows(t, ctx, connection,
				`SELECT dynamicType(selected) FROM (`+compiled.SQL+`)`,
				compiled.Args, 1)
			if !reflect.DeepEqual(got, [][]string{{"None"}}) {
				t.Fatalf("binary provenance pipeline returned %#v for %q, want missing", got, source)
			}
		}
	})

	t.Run("input equal to output reads the pre command value", func(t *testing.T) {
		compiled := compile(t, `index=spath-edge event_id=s-scalars
| spath input=json_source output=json_source path=value
| table json_source`)
		got := spathRows(t, ctx, connection,
			`SELECT concat(dynamicType(json_source), '/', toString(json_source))
			FROM (`+compiled.SQL+`)`,
			compiled.Args, 1)
		if !reflect.DeepEqual(got, [][]string{{"String/replaced"}}) {
			t.Fatalf("same-field spath value = %#v, want String/replaced", got)
		}
	})

	t.Run("sequential stages consume typed outputs and preserve argument order", func(t *testing.T) {
		chained := compile(t, `index=spath-edge event_id=s-scalars
| spath output=nested path=payload.nested_json
| spath input=nested output=selected path=deep.value
| table selected`)
		got := spathRows(t, ctx, connection,
			`SELECT concat(dynamicType(selected), '/', toString(selected))
			FROM (`+chained.SQL+`)`,
			chained.Args, 1)
		if !reflect.DeepEqual(got, [][]string{{"String/stage-two"}}) {
			t.Fatalf("chained spath output = %#v, want String/stage-two", got)
		}

		independent := compile(t, `index=spath-edge event_id=s-scalars
| spath output=first path=payload.text
| spath output=second path=payload.false
| table first second`)
		got = spathRows(t, ctx, connection,
			`SELECT concat(toString(first), '/', toString(second))
			FROM (`+independent.SQL+`)`,
			independent.Args, 1)
		if !reflect.DeepEqual(got, [][]string{{"hello/false"}}) {
			t.Fatalf("independent spath outputs = %#v, want hello/false", got)
		}
	})

	t.Run("unsupported selected values fail with the stable marker", func(t *testing.T) {
		for _, test := range []struct {
			name string
			path string
		}{
			{name: "fraction", path: "payload.fraction"},
			{name: "exponent", path: "payload.exponent"},
			{name: "fractional exponent", path: "payload.fractional_exponent"},
			{name: "rounded fractional spelling", path: "payload.rounded_fraction"},
			{name: "object", path: "payload.container"},
			{name: "array", path: "payload.items"},
		} {
			t.Run(test.name, func(t *testing.T) {
				compiled := compile(t, `index=spath-edge event_id=s-scalars
| spath output=selected path=`+test.path+`
| table selected`)
				err := spathQueryError(ctx, connection,
					`SELECT toString(selected) FROM (`+compiled.SQL+`)`,
					compiled.Args)
				if err == nil || !strings.Contains(err.Error(), UnsupportedSpathValueMarker) {
					t.Fatalf("unsupported %s error = %v, want stable spath marker", test.name, err)
				}
			})
		}
	})

	t.Run("array selectors reject objects and out of range positions as misses", func(t *testing.T) {
		for _, path := range []string{
			"payload.container{0}.leaf",
			"payload.items{2}.name",
		} {
			compiled := compile(t, `index=spath-edge event_id=s-scalars
| spath output=selected path=`+path+`
| table selected`)
			got := spathRows(t, ctx, connection,
				`SELECT dynamicType(selected) FROM (`+compiled.SQL+`)`,
				compiled.Args, 1)
			if !reflect.DeepEqual(got, [][]string{{"None"}}) {
				t.Fatalf("spath wrong-container path %q = %#v, want missing", path, got)
			}
		}
	})

	t.Run("input byte ceiling is exact and dead destinations may be pruned", func(t *testing.T) {
		replacement := strings.Repeat("a", 1024)
		sourceFor := func(eventID, suffix string) string {
			return `index=spath-limit event_id=` + eventID + `
| eval amplified=replace(_raw,"x","` + replacement + `")
| spath input=amplified output=selected path=value
| ` + suffix
		}

		exact := compile(t, sourceFor("s-limit-exact", "table selected"))
		got := spathRows(t, ctx, connection,
			`SELECT dynamicType(selected) FROM (`+exact.SQL+`)`,
			exact.Args, 1)
		if !reflect.DeepEqual(got, [][]string{{"None"}}) {
			t.Fatalf("exactly 1 MiB spath input = %#v, want bounded miss", got)
		}

		over := compile(t, sourceFor("s-limit-over", "table selected"))
		err := spathQueryError(ctx, connection,
			`SELECT toString(selected) FROM (`+over.SQL+`)`,
			over.Args)
		if err == nil || !strings.Contains(err.Error(), SpathInputLimitMarker) {
			t.Fatalf("1 MiB + 1 spath input error = %v, want stable limit marker", err)
		}

		dead := compile(t, sourceFor("s-limit-over", "table event_id"))
		var count uint64
		if err := connection.QueryRow(
			ctx,
			`SELECT count() FROM (`+dead.SQL+`)`,
			dead.Args...,
		).Scan(&count); err != nil {
			t.Fatalf("execute dead oversized spath destination: %v\nSQL: %s", err, dead.SQL)
		}
		if count != 1 {
			t.Fatalf("dead oversized spath count = %d, want 1", count)
		}
	})

	t.Run("unauthorized index poison is never evaluated", func(t *testing.T) {
		compiled := compile(t, `index=spath-edge
| spath output=selected path=payload.poison
| table event_id selected`)
		var rows, missing uint64
		if err := connection.QueryRow(
			ctx,
			`SELECT count(), countIf(dynamicType(selected) = 'None')
			FROM (`+compiled.SQL+`)`,
			compiled.Args...,
		).Scan(&rows, &missing); err != nil {
			t.Fatalf("execute authorized-scope spath: %v\nSQL: %s", err, compiled.SQL)
		}
		if rows != 4 || missing != 4 {
			t.Fatalf("authorized-scope spath returned rows=%d missing=%d, want 4/4", rows, missing)
		}
	})

	t.Run("downstream predicate and stats use the extracted value", func(t *testing.T) {
		predicate := compile(t, `index=spath-edge
| spath output=signed_value path=payload.signed
| where signed_value=-9
| stats count`)
		if got := strings.Count(predicate.SQL, " AS MATERIALIZED ("); got != 1 {
			t.Fatalf("spath predicate materializations = %d, want one:\n%s", got, predicate.SQL)
		}
		bounded := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(clickhousedriver.Settings{
			"max_execution_time":                uint64(15),
			"timeout_overflow_mode":             "throw",
			"max_memory_usage":                  uint64(256 << 20),
			"max_threads":                       uint64(1),
			"max_subquery_depth":                uint64(100),
			"max_query_size":                    uint64(1 << 20),
			"enable_materialized_cte":           uint8(1),
			"short_circuit_function_evaluation": "enable",
		}))
		var count uint64
		if err := connection.QueryRow(
			bounded,
			`SELECT toUInt64(count) FROM (`+predicate.SQL+`)`,
			predicate.Args...,
		).Scan(&count); err != nil {
			t.Fatalf("execute spath predicate: %v\nSQL: %s", err, predicate.SQL)
		}
		if count != 1 {
			t.Fatalf("spath predicate count = %d, want 1", count)
		}

		grouped := compile(t, `index=spath-edge
| spath output=flag_value path=payload.flag
| stats count BY flag_value`)
		got := spathRows(t, bounded, connection,
			`SELECT toString(flag_value), toString(count)
			FROM (`+grouped.SQL+`)`,
			grouped.Args, 2)
		if !reflect.DeepEqual(got, [][]string{{"true", "1"}}) {
			t.Fatalf("stats by spath output = %#v, want true count 1", got)
		}
	})

	t.Run("field catalog consumes spath presence and type metadata", func(t *testing.T) {
		bounded := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(clickhousedriver.Settings{
			"max_execution_time":                uint64(15),
			"timeout_overflow_mode":             "throw",
			"max_memory_usage":                  uint64(512 << 20),
			"max_threads":                       uint64(1),
			"max_subquery_depth":                uint64(100),
			"max_query_size":                    uint64(1 << 20),
			"enable_materialized_cte":           uint8(1),
			"short_circuit_function_evaluation": "enable",
		}))
		for _, test := range []struct {
			name        string
			source      string
			field       string
			wantTypes   []uint8
			events      uint64
			nulls       uint64
			missing     uint64
			totalEvents uint64
		}{
			{
				name:   "Boolean",
				source: `index=spath-edge | spath output=selected path=payload.flag | table selected`,
				field:  "selected", wantTypes: []uint8{uint8(eventfields.StoredValueTypeBool)},
				events: 1, missing: 3, totalEvents: 4,
			},
			{
				name:   "explicit null",
				source: `index=spath-edge | spath output=selected path=payload.nothing | table selected`,
				field:  "selected", wantTypes: []uint8{uint8(eventfields.StoredValueTypeNull)},
				events: 1, nulls: 1, missing: 3, totalEvents: 4,
			},
			{
				name:   "miss preserves raw binary type",
				source: `index=spath-edge | spath output=_raw path=missing | table _raw`,
				field:  "_raw",
				wantTypes: []uint8{
					uint8(eventfields.StoredValueTypeString),
					uint8(eventfields.StoredValueTypeBytes),
				},
				events: 4, totalEvents: 4,
			},
			{
				name:   "invalid UTF8 declaration fails closed as bytes",
				source: `index=spath-corrupt | table _raw`,
				field:  "_raw",
				wantTypes: []uint8{
					uint8(eventfields.StoredValueTypeBytes),
				},
				events: 1, totalEvents: 1,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				logical := spathBuildPlan(
					t,
					test.source,
					indexTime.Add(time.Minute),
					visibilityCutoff,
				)
				catalog, err := (Compiler{}).CompileFieldCatalog(
					logical,
					FieldCatalogSpec{MaximumFields: 16},
				)
				if err != nil {
					t.Fatalf("compile spath field catalog: %v", err)
				}
				q := quoteIdentifier
				match := q(FieldCatalogRowKindColumn) + ` = 1 AND ` +
					q(FieldCatalogNameColumn) + ` = '` + test.field + `'`
				query := `SELECT
						toUInt64(countIf(` + match + `)),
						arraySort(arrayFlatten(groupArrayIf(` + q(FieldCatalogObservedTypesColumn) + `, ` + match + `))),
						toUInt64(sumIf(` + q(FieldCatalogEventCountColumn) + `, ` + match + `)),
						toUInt64(sumIf(` + q(FieldCatalogNullCountColumn) + `, ` + match + `)),
						toUInt64(sumIf(` + q(FieldCatalogMissingCountColumn) + `, ` + match + `)),
						toUInt64(max(` + q(FieldCatalogTotalEventsColumn) + `)),
						toUInt8(max(` + q(FieldCatalogInvalidColumn) + `))
					FROM (` + catalog.SQL + `)`
				var profileRows, eventCount, nullCount, missingCount, totalEvents uint64
				var observedTypes []uint8
				var invalid uint8
				if err := connection.QueryRow(bounded, query, catalog.Args...).Scan(
					&profileRows,
					&observedTypes,
					&eventCount,
					&nullCount,
					&missingCount,
					&totalEvents,
					&invalid,
				); err != nil {
					t.Fatalf("execute spath field catalog: %v\nSQL: %s", err, catalog.SQL)
				}
				if profileRows != 1 || !reflect.DeepEqual(observedTypes, test.wantTypes) ||
					eventCount != test.events || nullCount != test.nulls ||
					missingCount != test.missing || totalEvents != test.totalEvents || invalid != 0 {
					t.Fatalf(
						"spath field catalog = profiles:%d types:%v events:%d nulls:%d missing:%d total:%d invalid:%d",
						profileRows,
						observedTypes,
						eventCount,
						nullCount,
						missingCount,
						totalEvents,
						invalid,
					)
				}
			})
		}
	})

	t.Run("field summary and timeline consume the final spath relation", func(t *testing.T) {
		logical := spathBuildPlan(
			t,
			`index=spath-edge | spath output=selected path=payload.flag`,
			indexTime.Add(time.Minute),
			visibilityCutoff,
		)
		summary, err := (Compiler{}).CompileFieldSummary(logical, FieldSummarySpec{
			FieldName:             "selected",
			MaximumValues:         10,
			MaximumDistinctValues: 100,
			MaximumValueBytes:     4_096,
		})
		if err != nil {
			t.Fatalf("compile spath field summary: %v", err)
		}
		var invalid, unsupported uint8
		var summarized uint64
		if err := connection.QueryRow(
			ctx,
			`SELECT
				toUInt8(max(`+quoteIdentifier(FieldSummaryMetadataInvalidColumn)+`)),
				toUInt8(max(`+quoteIdentifier(FieldSummaryUnsupportedColumn)+`)),
				toUInt64(sumIf(`+quoteIdentifier(FieldSummaryValueCountColumn)+`,
					`+quoteIdentifier(FieldSummaryRowKindColumn)+` = 1))
			FROM (`+summary.SQL+`)`,
			summary.Args...,
		).Scan(&invalid, &unsupported, &summarized); err != nil {
			t.Fatalf("execute spath field summary: %v\nSQL: %s", err, summary.SQL)
		}
		if invalid != 0 || unsupported != 0 || summarized != 1 {
			t.Fatalf(
				"spath field summary = invalid:%d unsupported:%d values:%d, want 0/0/1",
				invalid,
				unsupported,
				summarized,
			)
		}

		timeline, err := (Compiler{}).CompileTimeline(logical, TimelineSpec{
			FirstBucket: time.Date(2026, time.July, 25, 0, 0, 0, 0, time.UTC),
			SpanSeconds: 3_600,
			BucketCount: 24,
			Earliest:    time.Date(2026, time.July, 25, 0, 0, 0, 0, time.UTC),
			Latest:      time.Date(2026, time.July, 26, 0, 0, 0, 0, time.UTC),
		})
		if err != nil {
			t.Fatalf("compile spath timeline: %v", err)
		}
		var buckets, events uint64
		if err := connection.QueryRow(
			ctx,
			`SELECT count(), sum(`+quoteIdentifier(TimelineCountColumn)+`)
			FROM (`+timeline.SQL+`)`,
			timeline.Args...,
		).Scan(&buckets, &events); err != nil {
			t.Fatalf("execute spath timeline: %v\nSQL: %s", err, timeline.SQL)
		}
		if buckets != 24 || events != 4 {
			t.Fatalf("spath timeline = buckets:%d events:%d, want 24/4", buckets, events)
		}
	})

	t.Run("physical actions perform one raw extraction and bounded type checks", func(t *testing.T) {
		compiled := compile(t, `index=spath-edge
| spath output=selected path=payload.text
| table event_id selected`)
		actions := explainCompiledQuery(t, ctx, connection, "EXPLAIN actions=1 ", compiled)
		if got := strings.Count(actions, "FUNCTION JSONExtractRaw("); got != 1 {
			t.Fatalf("physical plan has %d raw JSON function actions, want one:\n%s", got, actions)
		}
		if got := strings.Count(actions, "FUNCTION JSONType("); got != 1 {
			t.Fatalf("physical plan has %d JSON type actions, want one for a non-indexed path:\n%s", got, actions)
		}
		planText := explainCompiledQuery(t, ctx, connection, "EXPLAIN ", compiled)
		for _, blockingStep := range []string{"Aggregating", "Join", "Window", "MergingAggregated"} {
			if strings.Contains(planText, blockingStep) {
				t.Fatalf("streaming spath pipeline introduced a %s step:\n%s", blockingStep, planText)
			}
		}
	})
}

func spathStoreFixture(t *testing.T, ctx context.Context, store *Store, indexTime time.Time) uint64 {
	t.Helper()
	scalarJSON := `{"payload":{"text":"hello","empty":"","flag":true,"nothing":null,` +
		`"false":false,"zero":0,"negative_zero":-0,` +
		`"signed":-9,"signed_min":-9223372036854775808,"signed_max":9223372036854775807,` +
		`"unsigned_min":9223372036854775808,"unsigned":18446744073709551615,` +
		`"fraction":1.25,"exponent":1e0,"fractional_exponent":1.5e1,` +
		`"rounded_fraction":9007199254740993.0,` +
		`"container":{"leaf":"value"},"items":[{"name":"zero"},{"name":"one"}],` +
		`"nested_json":"{\"deep\":{\"value\":\"stage-two\"}}",` +
		`"duplicate":"first","duplicate":"second"}}`
	events := []*ingest.StoredEvent{
		spathEvent(
			"s-scalars",
			[]byte(scalarJSON),
			opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			0,
			typedField("prior", typedString("old-scalar")),
			typedField("json_source", typedString(`{"value":"replaced"}`)),
			typedField("numeric_source", typedUint(99)),
		),
		spathEvent(
			"s-miss",
			[]byte(`{"payload":{"other":"value"}}`),
			opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			1,
			typedField("prior", typedSint(17)),
		),
		spathEvent(
			"s-malformed",
			[]byte(`{"payload":`),
			opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			2,
			typedField("prior", typedBool(false)),
		),
		spathEvent(
			"s-binary",
			[]byte(`{"payload":{"text":"must-not-parse"}}`),
			opensplunkv1.RawEncoding_RAW_ENCODING_BINARY,
			3,
			typedField("prior", typedNull()),
		),
	}
	limitExact := spathEvent(
		"s-limit-exact",
		bytes.Repeat([]byte{'x'}, 1024),
		opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		5,
	)
	limitExact.Event.IndexName = spathLimitIndex
	limitOver := spathEvent(
		"s-limit-over",
		append(bytes.Repeat([]byte{'x'}, 1024), 'y'),
		opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		6,
	)
	limitOver.Event.IndexName = spathLimitIndex
	corrupt := spathEvent(
		"s-corrupt",
		[]byte{0xff, '{', '}'},
		opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		7,
	)
	corrupt.Event.IndexName = spathCorruptIndex
	poison := spathEvent(
		"s-poison",
		[]byte(`{"payload":{"poison":{"must":"not execute"}}}`),
		opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		4,
	)
	poison.Event.IndexName = "spath-poison"
	events = append(events, poison, limitExact, limitOver, corrupt)
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:          spathIntegrationTenant,
		CollectorID:       "collector",
		BatchID:           "spath-integration-batch",
		BatchSequence:     1,
		SourceBatchSHA256: testSourceBatchDigest("spath-integration-batch"),
		ReceivedAt:        indexTime,
		Events:            events,
	}); err != nil {
		t.Fatalf("store spath fixture: %v", err)
	}
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture spath visibility cutoff: %v", err)
	}
	return cutoff
}

func spathEvent(
	id string,
	raw []byte,
	rawEncoding opensplunkv1.RawEncoding,
	second int,
	fields ...*opensplunkv1.TypedObjectField,
) *ingest.StoredEvent {
	eventTime := time.Date(2026, time.July, 25, 11, 59, second, 0, time.UTC)
	return &ingest.StoredEvent{
		TenantID:    spathIntegrationTenant,
		CollectorID: "collector",
		BatchID:     "spath-integration-batch",
		IndexTime:   time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC),
		Event: &opensplunkv1.LogEvent{
			EventId:         id,
			IndexName:       spathIntegrationIndex,
			EventTime:       timestamppb.New(eventTime),
			CollectedAt:     timestamppb.New(eventTime),
			EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            "api",
			Source:          "app.log",
			Sourcetype:      "json",
			Severity:        opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
			Raw:             raw,
			RawEncoding:     rawEncoding,
			Message:         stringPointer("spath integration fixture"),
			Fields:          typedObjectValue(fields...),
		},
	}
}

func spathCompile(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) CompiledQuery {
	t.Helper()
	logical := spathBuildPlan(t, source, cutoff, visibilityCutoff)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile spath SPL %q: %v", source, err)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d for %q", got, want, source)
	}
	return compiled
}

func spathBuildPlan(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) *plan.Query {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse spath SPL %q: %v", source, err)
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          spathIntegrationTenant,
		AuthorizedIndexes: []string{spathIntegrationIndex, spathLimitIndex, spathCorruptIndex},
		Earliest:          time.Date(2026, time.July, 25, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, time.July, 26, 0, 0, 0, 0, time.UTC),
		SearchStart:       cutoff.Add(-time.Second),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   cutoff,
		VisibilityCutoff:  uint64PointerForIntegration(visibilityCutoff),
	})
	if err != nil {
		t.Fatalf("build spath SPL %q: %v", source, err)
	}
	return logical
}

func spathRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	query string,
	args []any,
	width int,
) [][]string {
	t.Helper()
	rows, err := connection.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("execute spath query: %v\nSQL: %s\nargs: %#v", err, query, args)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
			t.Errorf("close spath rows: %v", closeErr)
		}
	}()
	var collected [][]string
	for rows.Next() {
		values := make([]string, width)
		targets := make([]any, width)
		for index := range values {
			targets[index] = &values[index]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatalf("scan spath row: %v\nSQL: %s", err, query)
		}
		collected = append(collected, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate spath rows: %v\nSQL: %s", err, query)
	}
	return collected
}

func spathQueryError(
	ctx context.Context,
	connection clickhousedriver.Conn,
	query string,
	args []any,
) error {
	rows, err := connection.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var ignored string
		if err := rows.Scan(&ignored); err != nil {
			return err
		}
	}
	return rows.Err()
}

func explainCompiledQuery(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	prefix string,
	compiled CompiledQuery,
) string {
	t.Helper()
	rows, err := connection.Query(ctx, prefix+compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatalf("explain compiled query: %v\nSQL: %s", err, compiled.SQL)
	}
	defer func() { _ = rows.Close() }()
	var explain strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan query explain: %v", err)
		}
		explain.WriteString(line)
		explain.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate query explain: %v", err)
	}
	return explain.String()
}

func spathStartClickHouse(t *testing.T, ctx context.Context) (clickhousedriver.Conn, *Store) {
	t.Helper()
	container, err := testsupport.StartClickHouse(ctx, os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"))
	if err != nil {
		t.Fatalf("start pinned ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := container.Close(cleanupCtx); err != nil {
			t.Errorf("close spath ClickHouse container: %v", err)
		}
	})

	migrationPaths, err := filepath.Glob(
		filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"),
	)
	if err != nil || len(migrationPaths) == 0 {
		t.Fatalf("discover migrations: paths=%v err=%v", migrationPaths, err)
	}
	var migrations bytes.Buffer
	for _, migrationPath := range migrationPaths {
		migration, readErr := os.ReadFile(migrationPath)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", migrationPath, readErr)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	command := exec.CommandContext(
		ctx,
		"docker", "exec", "--interactive", container.Name, "clickhouse-client",
		"--user", container.Username, "--password", container.Password, "--multiquery",
	)
	command.Stdin = bytes.NewReader(migrations.Bytes())
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("apply spath ClickHouse migrations: %v\n%s", err, output)
	}

	config := DefaultConfig()
	config.Addresses = []string{container.Address}
	config.Username = container.Username
	config.Password = container.Password
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open spath visibility control database: %v", err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatalf("create spath visibility sequencer: %v", err)
	}
	store, err := Open(config, fixedRetention(30*24*time.Hour), sequencer)
	if err != nil {
		t.Fatalf("open spath store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("ping spath store: %v", err)
	}
	options, _, err := config.clickHouseOptions()
	if err != nil {
		t.Fatalf("resolve spath ClickHouse options: %v", err)
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatalf("open spath query connection: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, store
}
