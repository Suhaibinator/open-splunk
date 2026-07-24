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
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	binEdgeIndex  = "binedge"
	binEdgeTenant = "tenant"
)

// binEdgeEventTime is the canonical clock this fixture uses. Each event is one
// minute apart so the default event order, dedup retention, and timechart
// bucketing are all deterministic.
func binEdgeEventTime(minute int) time.Time {
	return time.Date(2026, time.July, 21, 3, minute, 0, 0, time.UTC)
}

// TestBinEdgePipelineAgainstClickHouse executes difficult Dynamic-bin
// pipelines against the pinned ClickHouse server. It starts its own container
// and writes its own fixture through the store writer so it can run beside the
// other opt-in integration tests.
func TestBinEdgePipelineAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	connection, store := binEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.July, 21, 4, 0, 0, 0, time.UTC)
	cutoff := binEdgeStoreFixture(t, ctx, store, indexTime)
	compile := func(source string) CompiledQuery {
		t.Helper()
		return binEdgeCompile(t, source, indexTime.Add(time.Minute), cutoff)
	}

	t.Run("default replace-in-place form", func(t *testing.T) {
		compiled := compile(`index=binedge | bin metric span=10 | table event_id metric`)
		if !reflect.DeepEqual(compiled.OutputFields, []string{"event_id", "metric"}) {
			t.Fatalf("output fields = %v", compiled.OutputFields)
		}
		got := binEdgeRows(t, ctx, connection,
			`SELECT event_id, dynamicType(metric),
				if(dynamicType(metric) = 'None', '<none>', toString(metric))
			FROM (`+compiled.SQL+`) ORDER BY event_id`, compiled.Args, 3)
		want := [][]string{
			{"b-float", "Float64", "-20"},
			{"b-int", "Int64", "-20"},
			{"b-missing", "None", "<none>"},
			{"b-null", "None", "<none>"},
			{"b-text", "String", "not-a-number"},
			{"b-uint", "UInt64", "20"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("replace-in-place buckets = %#v, want %#v", got, want)
		}

		// The immutable convenience payload is suppressed because the replaced
		// field would otherwise be visible twice with contradictory values.
		open := compile(`index=binedge | bin metric span=10`)
		for _, field := range open.OutputFields {
			if field == "fields" {
				t.Fatalf("replace-in-place bin exposed a stale fields payload: %v", open.OutputFields)
			}
		}
		var rows uint64
		if err := connection.QueryRow(ctx, "SELECT count() FROM ("+open.SQL+")", open.Args...).Scan(&rows); err != nil {
			t.Fatalf("execute open-schema replace-in-place bin: %v\nSQL: %s", err, open.SQL)
		}
		if rows != 6 {
			t.Fatalf("open-schema replace-in-place bin returned %d rows, want 6", rows)
		}
	})

	t.Run("fixed and runtime typed paths agree", func(t *testing.T) {
		// severity is a promoted fixed UInt8 column and sev_probe stores the
		// same logical value as a runtime-typed event field.
		compiled := compile(
			`index=binedge | bin severity span=4 AS fixed_band | bin sev_probe span=4 AS dynamic_band` +
				` | table event_id fixed_band dynamic_band`)
		got := binEdgeRows(t, ctx, connection,
			`SELECT event_id, toString(fixed_band), toString(dynamic_band) FROM (`+compiled.SQL+`) ORDER BY event_id`,
			compiled.Args, 3)
		want := [][]string{
			{"b-float", "0", "0"},
			{"b-int", "4", "4"},
			{"b-missing", "0", "0"},
			{"b-null", "0", "0"},
			{"b-text", "0", "0"},
			{"b-uint", "4", "4"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fixed and runtime buckets = %#v, want %#v", got, want)
		}

		// The signed floor definition must also converge across the two paths.
		signed := compile(
			`index=binedge event_id=b-int | eval fixed_metric=tonumber("-11")` +
				` | bin fixed_metric span=10 AS fixed_band | bin metric span=10 AS dynamic_band` +
				` | table fixed_band dynamic_band`)
		signedRows := binEdgeRows(t, ctx, connection,
			`SELECT toString(fixed_band), toString(dynamic_band) FROM (`+signed.SQL+`)`, signed.Args, 2)
		if !reflect.DeepEqual(signedRows, [][]string{{"-20", "-20"}}) {
			t.Fatalf("signed fixed/runtime buckets = %#v, want [[-20 -20]]", signedRows)
		}
	})

	t.Run("numeric predicates filter bucketed numeric text", func(t *testing.T) {
		// text_metric spells 25, -11, and 21.5, so its buckets are 20, -20,
		// and 20 and must remain visible to ordered numeric comparison.
		for source, want := range map[string]uint64{
			`index=binedge | bin text_metric span=10 AS band | where band>=20`:  2,
			`index=binedge | bin text_metric span=10 AS band | where band>0`:    2,
			`index=binedge | bin text_metric span=10 AS band | where band>20`:   0,
			`index=binedge | bin text_metric span=10 AS band | where band<=20`:  3,
			`index=binedge | bin text_metric span=10 AS band | where band<=-20`: 1,
			`index=binedge | bin text_metric span=10 AS band | where band<20`:   1,
			`index=binedge | bin text_metric span=10 AS band | where band=20`:   2,
			`index=binedge | bin text_metric span=10 AS band | where band=-20`:  1,
		} {
			compiled := compile(source)
			var count uint64
			if err := connection.QueryRow(ctx, "SELECT count() FROM ("+compiled.SQL+")", compiled.Args...).Scan(&count); err != nil {
				t.Fatalf("execute %q: %v\nSQL: %s", source, err, compiled.SQL)
			}
			if count != want {
				t.Fatalf("%q matched %d rows, want %d", source, count, want)
			}
		}

		// Text and its numeric twin converge on one lexical group key.
		grouped := compile(`index=binedge | bin text_metric span=10 AS band | where band>=20 | stats count BY band`)
		got := binEdgeRows(t, ctx, connection,
			"SELECT "+quoteIdentifier("band")+", toString(count) FROM ("+grouped.SQL+")", grouped.Args, 2)
		if !reflect.DeepEqual(got, [][]string{{"20", "2"}}) {
			t.Fatalf("bucketed numeric text group = %#v, want [[20 2]]", got)
		}
	})

	t.Run("long chained pipeline", func(t *testing.T) {
		compiled := compile(`index=binedge
| rex field=_raw "latency=(?<latency_text>[0-9]+)"
| eval doubled=tonumber(latency_text)
| bin latency_text span=100 AS latency_band
| where latency_band>=0
| stats count BY latency_band
| sort -count
| head 3`)
		if !reflect.DeepEqual(compiled.OutputFields, []string{"latency_band", "count"}) {
			t.Fatalf("chained output fields = %v", compiled.OutputFields)
		}
		got := binEdgeRows(t, ctx, connection,
			"SELECT "+quoteIdentifier("latency_band")+", toString(count) FROM ("+compiled.SQL+")",
			compiled.Args, 2)
		want := [][]string{{"0", "3"}, {"100", "1"}, {"200", "1"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("chained pipeline = %#v, want %#v", got, want)
		}
	})

	t.Run("dedup over a binned field", func(t *testing.T) {
		compiled := compile(`index=binedge | bin metric span=10 AS band | dedup band | table event_id band`)
		got := binEdgeRows(t, ctx, connection,
			"SELECT event_id, toString(band) FROM ("+compiled.SQL+")", compiled.Args, 2)
		// Missing and explicit-null buckets are not dedup keys, and the two
		// -20 buckets collapse to the newest event that produced one.
		want := [][]string{{"b-text", "not-a-number"}, {"b-float", "-20"}, {"b-uint", "20"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("dedup over buckets = %#v, want %#v", got, want)
		}
	})

	t.Run("top and rare over binned output", func(t *testing.T) {
		// Both commands order by count and break ties on the lexical group key
		// in descending order, so the runtime bucket text decides the tie.
		top := compile(`index=binedge | bin metric span=10 AS band | top band`)
		gotTop := binEdgeRows(t, ctx, connection,
			"SELECT toString(band), toString(count), toString(percent) FROM ("+top.SQL+")", top.Args, 3)
		wantTop := [][]string{{"-20", "2", "50"}, {"not-a-number", "1", "25"}, {"20", "1", "25"}}
		if !reflect.DeepEqual(gotTop, wantTop) {
			t.Fatalf("top over buckets = %#v, want %#v", gotTop, wantTop)
		}

		rare := compile(`index=binedge | bin metric span=10 AS band | rare band`)
		gotRare := binEdgeRows(t, ctx, connection,
			"SELECT toString(band), toString(count), toString(percent) FROM ("+rare.SQL+")", rare.Args, 3)
		wantRare := [][]string{{"not-a-number", "1", "25"}, {"20", "1", "25"}, {"-20", "2", "50"}}
		if !reflect.DeepEqual(gotRare, wantRare) {
			t.Fatalf("rare over buckets = %#v, want %#v", gotRare, wantRare)
		}
	})

	// DEFECT: the documented contract states that downstream sorts consume the
	// bucketed value and that no value can fail an otherwise successful search.
	// Sorting by a runtime-typed bin destination instead fails every search
	// with the sanitized unsupported-value marker, even when the scope holds a
	// single plainly supported Int64 event. The identical pipeline over the
	// same field without the bin stage sorts successfully.
	t.Run("sort over a binned field", func(t *testing.T) {
		control := compile(`index=binedge | sort metric | table event_id`)
		if got := len(binEdgeRows(t, ctx, connection,
			"SELECT event_id FROM ("+control.SQL+")", control.Args, 1)); got != 6 {
			t.Fatalf("sorting the unbinned runtime-typed field returned %d rows, want 6", got)
		}

		// The smallest reproduction: one event whose bucket is a plainly
		// supported Int64. Nothing in this scope is an unsupported value.
		single := compile(`index=binedge event_id=b-int | bin metric span=10 AS band | sort band | table event_id band`)
		var singleRows uint64
		if err := connection.QueryRow(ctx, "SELECT count() FROM ("+single.SQL+")",
			single.Args...).Scan(&singleRows); err != nil {
			t.Fatalf("sorting one supported Int64 bucket: %v\nSQL: %s", err, single.SQL)
		}
		if singleRows != 1 {
			t.Fatalf("sorting one supported Int64 bucket returned %d rows, want 1", singleRows)
		}

		compiled := compile(`index=binedge | bin metric span=10 AS band | sort band | table event_id`)
		got := binEdgeRows(t, ctx, connection, "SELECT event_id FROM ("+compiled.SQL+")", compiled.Args, 1)
		// Numeric buckets sort by value, non-numeric text follows, and missing
		// or explicit-null destinations sort last.
		want := [][]string{{"b-int"}, {"b-float"}, {"b-uint"}, {"b-text"}, {"b-null"}, {"b-missing"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("sort over buckets = %#v, want %#v", got, want)
		}

		// A pass-through String bucket has no numeric branch at all and still
		// fails, so the defect is not limited to numeric bucketing.
		passThrough := compile(`index=binedge | bin label span=10 AS label_band | sort label_band | table event_id`)
		if got := len(binEdgeRows(t, ctx, connection,
			"SELECT event_id FROM ("+passThrough.SQL+")", passThrough.Args, 1)); got != 6 {
			t.Fatalf("sorting a pass-through String bucket returned %d rows, want 6", got)
		}
	})

	// DEFECT: `search <field> <relational operator> <literal>` over a binned
	// destination fails the same way, while the equivalent `where` predicate
	// over the identical bucket succeeds. The two surfaces must agree.
	t.Run("search comparison over a binned field", func(t *testing.T) {
		for _, test := range []struct {
			name, search, where string
			want                uint64
		}{
			{
				name:   "at least",
				search: `index=binedge | bin metric span=10 AS band | search band>=20`,
				where:  `index=binedge | bin metric span=10 AS band | where band>=20`,
				want:   1,
			},
			{
				name:   "less than",
				search: `index=binedge | bin metric span=10 AS band | search band<0`,
				where:  `index=binedge | bin metric span=10 AS band | where band<0`,
				want:   2,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				whereCompiled := compile(test.where)
				var whereCount uint64
				if err := connection.QueryRow(ctx, "SELECT count() FROM ("+whereCompiled.SQL+")",
					whereCompiled.Args...).Scan(&whereCount); err != nil {
					t.Fatalf("execute %q: %v", test.where, err)
				}
				if whereCount != test.want {
					t.Fatalf("%q matched %d rows, want %d", test.where, whereCount, test.want)
				}
				searchCompiled := compile(test.search)
				var searchCount uint64
				if err := connection.QueryRow(ctx, "SELECT count() FROM ("+searchCompiled.SQL+")",
					searchCompiled.Args...).Scan(&searchCount); err != nil {
					t.Fatalf("execute %q: %v\nSQL: %s", test.search, err, searchCompiled.SQL)
				}
				if searchCount != whereCount {
					t.Fatalf("%q matched %d rows but the equivalent where matched %d",
						test.search, searchCount, whereCount)
				}
			})
		}
	})

	t.Run("timechart is unchanged by a numeric bin", func(t *testing.T) {
		plainNames, plainCounts, plainInvalid := binEdgeTimechart(t, ctx, connection,
			compile(`index=binedge | timechart span=5m count BY label`))
		binnedNames, binnedCounts, binnedInvalid := binEdgeTimechart(t, ctx, connection,
			compile(`index=binedge | bin metric span=10 AS band | timechart span=5m count BY label`))
		if plainNames != binnedNames || plainCounts != binnedCounts || plainInvalid != binnedInvalid {
			t.Fatalf(
				"a numeric bin changed the timechart series: %q/%q/%d vs %q/%q/%d",
				plainNames, plainCounts, plainInvalid, binnedNames, binnedCounts, binnedInvalid,
			)
		}
		if plainInvalid != 0 || plainNames != "0:alpha|0:beta|0:gamma" || plainCounts != "2|2|2" {
			t.Fatalf("timechart series = %q/%q/%d", plainNames, plainCounts, plainInvalid)
		}

		// bin discretizes numbers and leaves other scalars alone, so binning a
		// non-numeric string split field must not disturb its series either.
		passNames, passCounts, passInvalid := binEdgeTimechart(t, ctx, connection,
			compile(`index=binedge | bin label span=10 AS label_band | timechart span=5m count BY label_band`))
		if passNames != plainNames || passCounts != plainCounts || passInvalid != plainInvalid {
			t.Fatalf(
				"binning a non-numeric split field changed its series: %q/%q/%d",
				passNames, passCounts, passInvalid,
			)
		}

		// A numeric split value is an unsupported timechart label whether or
		// not a bin produced it.
		_, _, rawNumericInvalid := binEdgeTimechart(t, ctx, connection,
			compile(`index=binedge | timechart span=5m count BY metric`))
		_, _, binnedNumericInvalid := binEdgeTimechart(t, ctx, connection,
			compile(`index=binedge | bin metric span=10 AS band | timechart span=5m count BY band`))
		if rawNumericInvalid != 1 || binnedNumericInvalid != rawNumericInvalid {
			t.Fatalf(
				"numeric split classification changed across bin: raw=%d binned=%d",
				rawNumericInvalid, binnedNumericInvalid,
			)
		}
	})

	t.Run("bin then rex overwriting the same field", func(t *testing.T) {
		compiled := compile(
			`index=binedge | bin metric span=10 AS band | rex field=_raw "status=(?<band>[0-9]+)"` +
				` | table event_id band`)
		got := binEdgeRows(t, ctx, connection,
			`SELECT event_id, dynamicType(band),
				if(dynamicType(band) = 'None', '<none>', toString(band))
			FROM (`+compiled.SQL+`) ORDER BY event_id`, compiled.Args, 3)
		want := [][]string{
			// No match preserves the bucket the bin stage published, with its
			// runtime type intact.
			{"b-float", "Float64", "-20"},
			{"b-int", "String", "503"},
			{"b-missing", "String", "500"},
			{"b-null", "String", "204"},
			{"b-text", "String", "not-a-number"},
			{"b-uint", "String", "200"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("bin then rex = %#v, want %#v", got, want)
		}
	})

	t.Run("numeric bin written to canonical time", func(t *testing.T) {
		compiled := compile(`index=binedge | bin metric span=10 AS _time | table event_id _time metric`)
		got := binEdgeRows(t, ctx, connection,
			`SELECT event_id, dynamicType(`+quoteIdentifier("_time")+`),
				if(dynamicType(`+quoteIdentifier("_time")+`) = 'None', '<none>', toString(`+quoteIdentifier("_time")+`)),
				if(dynamicType(metric) = 'None', '<none>', toString(metric))
			FROM (`+compiled.SQL+`) ORDER BY event_id`, compiled.Args, 4)
		want := [][]string{
			{"b-float", "Float64", "-20", "-11.5"},
			{"b-int", "Int64", "-20", "-11"},
			// An event without the source keeps the prior canonical value.
			{"b-missing", "DateTime64(9, 'UTC')", "2026-07-21 03:04:00.000000000", "<none>"},
			{"b-null", "None", "<none>", "<none>"},
			{"b-text", "String", "not-a-number", "not-a-number"},
			{"b-uint", "UInt64", "20", "25"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("numeric bin AS _time = %#v, want %#v", got, want)
		}
	})

	t.Run("consecutive bins with distinct spans", func(t *testing.T) {
		chained := compile(`index=binedge event_id=b-int
| bin metric span=10 AS first
| bin first span=6 AS second
| bin sev_probe span=4 AS third
| table first second third`)
		got := binEdgeRows(t, ctx, connection,
			`SELECT concat(dynamicType(first), '/', toString(first)),
				concat(dynamicType(second), '/', toString(second)),
				concat(dynamicType(third), '/', toString(third))
			FROM (`+chained.SQL+`)`, chained.Args, 3)
		want := [][]string{{"Int64/-20", "Int64/-24", "UInt64/4"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("chained bins = %#v, want %#v", got, want)
		}

		replaced := compile(`index=binedge event_id=b-int | bin metric span=10 | bin metric span=6 | table metric`)
		gotReplaced := binEdgeRows(t, ctx, connection,
			`SELECT concat(dynamicType(metric), '/', toString(metric)) FROM (`+replaced.SQL+`)`, replaced.Args, 1)
		if !reflect.DeepEqual(gotReplaced, [][]string{{"Int64/-24"}}) {
			t.Fatalf("chained replace-in-place bins = %#v, want [[Int64/-24]]", gotReplaced)
		}
	})

	t.Run("prior stored destination survives a missing source", func(t *testing.T) {
		compiled := compile(`index=binedge | bin metric span=10 AS prior_band | table event_id prior_band`)
		got := binEdgeRows(t, ctx, connection,
			`SELECT event_id, dynamicType(prior_band),
				if(dynamicType(prior_band) = 'None', '<none>', toString(prior_band))
			FROM (`+compiled.SQL+`) ORDER BY event_id`, compiled.Args, 3)
		want := [][]string{
			{"b-float", "Float64", "-20"},
			{"b-int", "Int64", "-20"},
			// Only this event lacks the source, so only it keeps the value,
			// type, and presence its destination already had.
			{"b-missing", "String", "prior"},
			{"b-null", "None", "<none>"},
			{"b-text", "String", "not-a-number"},
			{"b-uint", "UInt64", "20"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("prior stored destination = %#v, want %#v", got, want)
		}
	})

	t.Run("bin whose destination is projected away", func(t *testing.T) {
		compiled := compile(`index=binedge | bin metric span=10 AS band | table event_id`)
		var rows uint64
		if err := connection.QueryRow(ctx, "SELECT count() FROM ("+compiled.SQL+")", compiled.Args...).Scan(&rows); err != nil {
			t.Fatalf("execute pruned Dynamic bin: %v\nSQL: %s", err, compiled.SQL)
		}
		if rows != 6 {
			t.Fatalf("pruned Dynamic bin returned %d rows, want 6", rows)
		}
	})

	t.Run("streaming physical plan", func(t *testing.T) {
		for _, test := range []struct {
			name              string
			source            string
			wantMetadataScans int
		}{
			{
				name:              "one stage",
				source:            `index=binedge | bin metric span=10 AS band | table event_id band`,
				wantMetadataScans: 1,
			},
			{
				name:              "chained stages",
				source:            `index=binedge | bin metric span=10 | bin metric span=7 | table event_id metric`,
				wantMetadataScans: 1,
			},
			{
				name:              "distinct sources",
				source:            `index=binedge | bin metric span=10 AS a | bin sev_probe span=4 AS b | table a b`,
				wantMetadataScans: 2,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				compiled := compile(test.source)
				actions := binEdgeExplain(t, ctx, connection, "EXPLAIN actions=1 ", compiled)
				if got := strings.Count(actions, "FUNCTION arrayFirstIndex("); got != test.wantMetadataScans {
					t.Fatalf(
						"physical plan performs %d aligned metadata lookups, want %d:\n%s",
						got, test.wantMetadataScans, actions,
					)
				}
				planText := binEdgeExplain(t, ctx, connection, "EXPLAIN ", compiled)
				for _, fence := range []string{"Aggregating", "Join", "Window", "MergingAggregated"} {
					if strings.Contains(planText, fence) {
						t.Fatalf("streaming bin pipeline introduced a %s step:\n%s", fence, planText)
					}
				}
			})
		}
	})
}

func binEdgeStoreFixture(t *testing.T, ctx context.Context, store *Store, indexTime time.Time) uint64 {
	t.Helper()
	events := []*ingest.StoredEvent{
		binEdgeEvent("b-int", "latency=137 status=503", 0, opensplunkv1.LogSeverity_LOG_SEVERITY_ERROR,
			typedField("metric", typedSint(-11)),
			typedField("sev_probe", typedUint(5)),
			typedField("text_metric", typedString("25")),
			typedField("prior_band", typedString("prior")),
			typedField("label", typedString("alpha")),
		),
		binEdgeEvent("b-uint", "latency=245 status=200", 1, opensplunkv1.LogSeverity_LOG_SEVERITY_WARN,
			typedField("metric", typedUint(25)),
			typedField("sev_probe", typedUint(4)),
			typedField("text_metric", typedString("-11")),
			typedField("label", typedString("alpha")),
		),
		// This event carries no status= capture so a later rex stage cannot
		// match, which proves a published numeric bucket survives a no match.
		binEdgeEvent("b-float", "latency=99", 2, opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
			typedField("metric", typedDouble(-11.5)),
			typedField("sev_probe", typedUint(3)),
			typedField("text_metric", typedString("21.5")),
			typedField("label", typedString("beta")),
		),
		binEdgeEvent("b-text", "no numbers here", 3, opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
			typedField("metric", typedString("not-a-number")),
			typedField("sev_probe", typedUint(3)),
			typedField("label", typedString("beta")),
		),
		binEdgeEvent("b-missing", "latency=7 status=500", 4, opensplunkv1.LogSeverity_LOG_SEVERITY_DEBUG,
			typedField("sev_probe", typedUint(2)),
			typedField("prior_band", typedString("prior")),
			typedField("label", typedString("gamma")),
		),
		binEdgeEvent("b-null", "latency=61 status=204", 5, opensplunkv1.LogSeverity_LOG_SEVERITY_TRACE,
			typedField("metric", typedNull()),
			typedField("sev_probe", typedUint(1)),
			typedField("label", typedString("gamma")),
		),
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID: binEdgeTenant, CollectorID: "collector", BatchID: "bin-edge-batch", BatchSequence: 1,
		SourceBatchSHA256: testSourceBatchDigest("bin-edge-batch"),
		ReceivedAt:        indexTime,
		Events:            events,
	}); err != nil {
		t.Fatalf("store bin-edge fixture: %v", err)
	}
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture bin-edge visibility cutoff: %v", err)
	}
	return cutoff
}

func binEdgeEvent(
	id, raw string,
	minute int,
	severity opensplunkv1.LogSeverity,
	fields ...*opensplunkv1.TypedObjectField,
) *ingest.StoredEvent {
	eventTime := binEdgeEventTime(minute)
	return &ingest.StoredEvent{
		TenantID:    binEdgeTenant,
		CollectorID: "collector",
		BatchID:     "bin-edge-batch",
		IndexTime:   time.Date(2026, time.July, 21, 4, 0, 0, 0, time.UTC),
		Event: &opensplunkv1.LogEvent{
			EventId:         id,
			IndexName:       binEdgeIndex,
			EventTime:       timestamppb.New(eventTime),
			CollectedAt:     timestamppb.New(eventTime),
			EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            "api",
			Source:          "app.log",
			Sourcetype:      "go:zap:json",
			Severity:        severity,
			Raw:             []byte(raw),
			RawEncoding:     opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			Message:         stringPointer("Request metrics"),
			Fields:          typedObjectValue(fields...),
		},
	}
}

func binEdgeCompile(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse bin-edge SPL %q: %v", source, err)
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID: binEdgeTenant, AuthorizedIndexes: []string{binEdgeIndex},
		Earliest:         time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC),
		Latest:           time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC),
		IndexTimeCutoff:  cutoff,
		VisibilityCutoff: uint64PointerForIntegration(visibilityCutoff),
	})
	if err != nil {
		t.Fatalf("build bin-edge SPL %q: %v", source, err)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile bin-edge SPL %q: %v", source, err)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d for %q", got, want, source)
	}
	return compiled
}

// binEdgeRows collects a bounded string projection so an exact expectation can
// be compared without per-query scan plumbing.
func binEdgeRows(
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
		t.Fatalf("execute bin-edge query: %v\nSQL: %s\nargs: %#v", err, query, args)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
			t.Errorf("close bin-edge rows: %v", closeErr)
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
			t.Fatalf("scan bin-edge row: %v\nSQL: %s", err, query)
		}
		collected = append(collected, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate bin-edge rows: %v\nSQL: %s", err, query)
	}
	return collected
}

// binEdgeTimechart reduces a bounded timechart result to its series names,
// element-wise totals, and validity flag so two pipelines can be compared.
func binEdgeTimechart(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) (string, string, uint8) {
	t.Helper()
	if compiled.Timechart == nil {
		t.Fatalf("compiled query is not a timechart: %#v", compiled)
	}
	query := "SELECT arrayStringConcat(any(" + quoteIdentifier(TimechartNamesColumn) + "), '|'), " +
		"arrayStringConcat(arrayMap(value -> toString(value), sumForEach(" +
		quoteIdentifier(TimechartCountsColumn) + ")), '|'), " +
		"max(" + quoteIdentifier(TimechartInvalidColumn) + ") FROM (" + compiled.SQL + ")"
	var names, counts string
	var invalid uint8
	if err := connection.QueryRow(ctx, query, compiled.Args...).Scan(&names, &counts, &invalid); err != nil {
		t.Fatalf("execute bin-edge timechart: %v\nSQL: %s", err, query)
	}
	return names, counts, invalid
}

func binEdgeExplain(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	prefix string,
	compiled CompiledQuery,
) string {
	t.Helper()
	rows, err := connection.Query(ctx, prefix+compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatalf("explain bin-edge query: %v\nSQL: %s", err, compiled.SQL)
	}
	var explain strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			_ = rows.Close()
			t.Fatalf("scan bin-edge explain: %v", err)
		}
		explain.WriteString(line)
		explain.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate bin-edge explain: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close bin-edge explain: %v", err)
	}
	return explain.String()
}

// binEdgeStartClickHouse starts an isolated pinned server, applies the
// repository migrations, and returns a query connection plus a store writer.
func binEdgeStartClickHouse(t *testing.T, ctx context.Context) (clickhousedriver.Conn, *Store) {
	t.Helper()
	container := "open-splunk-bin-edge-" + integrationRandomHex(t, 6)
	password := integrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = storeIntegrationImage
	}
	integrationDocker(t, ctx, nil,
		"run", "--detach", "--rm", "--name", container,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB=open_splunk",
		"--env", "CLICKHOUSE_USER=open_splunk",
		"--env", "CLICKHOUSE_PASSWORD="+password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		image,
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", container).Run()
	})
	integrationWaitForClickHouse(t, ctx, container, password)

	migrationPaths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"))
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
	integrationDocker(t, ctx, bytes.NewReader(migrations.Bytes()),
		"exec", "--interactive", container, "clickhouse-client",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)

	config := DefaultConfig()
	config.Addresses = []string{integrationNativeAddress(t, ctx, container)}
	config.Username = "open_splunk"
	config.Password = password
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open bin-edge visibility control database: %v", err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatalf("create bin-edge visibility sequencer: %v", err)
	}
	store, err := Open(config, fixedRetention(30*24*time.Hour), sequencer)
	if err != nil {
		t.Fatalf("open bin-edge store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("ping bin-edge store: %v", err)
	}
	options, _, err := config.clickHouseOptions()
	if err != nil {
		t.Fatalf("resolve bin-edge ClickHouse options: %v", err)
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatalf("open bin-edge query connection: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, store
}
