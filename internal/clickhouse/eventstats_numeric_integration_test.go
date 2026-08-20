package clickhouse

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

// testEventStatsNumericAggregatesAgainstClickHouse pins numeric normalization, nullable sum
// and average semantics, row preservation, grouping, downstream composition,
// and the existing 10,000-row eventstats fence to the production ClickHouse
// image. The average assertions deliberately reuse the sum fixture so both
// functions prove the same immediate-member normalization without duplicating
// a second ingestion corpus.
func testEventStatsNumericAggregatesAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	newEvent := func(
		id, source string,
		fields ...*opensplunk.TypedObjectField,
	) *ingest.StoredEvent {
		event := compilerIntegrationEvent(
			id,
			"eventstats-sum-host",
			"eventstats sum fixture",
			indexTime,
			fields...,
		)
		event.BatchID = "eventstats-sum-batch"
		event.Event.Source = source
		return event
	}
	const fixtureSource = "eventstats-sum-fixture"
	events := []*ingest.StoredEvent{
		newEvent(
			"eventstats-sum-01-int",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("A")),
			typedField("eventstats_sum_value", typedSint(10)),
			typedField("eventstats_avg_overflow", typedDouble(math.MaxFloat64)),
			typedField("eventstats_sum_ineligible", typedBool(true)),
		),
		newEvent(
			"eventstats-sum-02-float",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("A")),
			typedField("eventstats_sum_value", typedDouble(0.5)),
			typedField("eventstats_avg_overflow", typedDouble(math.MaxFloat64)),
			typedField("eventstats_sum_ineligible", typedString("not-a-number")),
		),
		newEvent(
			"eventstats-sum-03-string",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("A")),
			typedField("eventstats_sum_value", typedString("20.5")),
			typedField("eventstats_sum_ineligible", typedNull()),
		),
		newEvent(
			"eventstats-sum-04-multivalue",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("A")),
			typedField(
				"eventstats_sum_value",
				typedList(
					typedSint(1),
					typedString("2.5"),
					typedString("bad"),
					typedNull(),
				),
			),
			typedField(
				"eventstats_sum_ineligible",
				typedObject(typedField("child", typedSint(99))),
			),
		),
		newEvent(
			"eventstats-sum-05-missing",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("A")),
			typedField("eventstats_sum_decimal", typedDecimal("3.25")),
		),
		newEvent(
			"eventstats-sum-06-nonnumeric",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("B")),
			typedField("eventstats_sum_value", typedString("bad")),
		),
		newEvent(
			"eventstats-sum-07-null",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("B")),
			typedField("eventstats_sum_value", typedNull()),
		),
		newEvent(
			"eventstats-sum-08-missing-group",
			fixtureSource,
			typedField("eventstats_sum_value", typedSint(7)),
		),
		newEvent(
			"eventstats-sum-09-null-group",
			fixtureSource,
			typedField("eventstats_sum_group", typedNull()),
			typedField("eventstats_sum_value", typedSint(8)),
		),
		newEvent(
			"eventstats-sum-same-tenant-poison",
			"eventstats-sum-poison",
			typedField("eventstats_sum_group", typedString("A")),
			typedField("eventstats_sum_value", typedSint(2_000)),
		),
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        "collector",
		BatchID:            "eventstats-sum-batch",
		BatchSequence:      82,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  testSourceBatchDigest("eventstats-sum-batch"),
		ReceivedAt:         indexTime,
		Events:             events,
	}); err != nil {
		t.Fatalf("store eventstats sum fixtures: %v", err)
	}

	foreign := newEvent(
		"eventstats-sum-foreign-poison",
		fixtureSource,
		typedField("eventstats_sum_group", typedString("A")),
		typedField("eventstats_sum_value", typedSint(1_000)),
	)
	foreign.TenantID = "other-tenant"
	foreign.BatchID = "eventstats-sum-foreign-batch"
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "other-tenant",
		CollectorID:        "collector",
		BatchID:            "eventstats-sum-foreign-batch",
		BatchSequence:      82,
		OriginalEventCount: 1,
		SourceBatchSHA256: testSourceBatchDigest(
			"eventstats-sum-foreign-batch",
		),
		ReceivedAt: indexTime,
		Events:     []*ingest.StoredEvent{foreign},
	}); err != nil {
		t.Fatalf("store foreign eventstats sum poison: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture eventstats sum visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
	}
	base := `index=compiler source="` + fixtureSource + `"`

	collect := func(name string, query CompiledQuery) []eventStatsNullableFloatRow {
		t.Helper()
		return collectEventStatsNullableFloatRows(
			t,
			ctx,
			connection,
			name,
			query,
		)
	}
	collectSingleValue := func(name string, query CompiledQuery) *float64 {
		t.Helper()
		return collectEventStatsNullableFloat(
			t,
			ctx,
			connection,
			name,
			query,
		)
	}

	ids := []string{
		"eventstats-sum-01-int",
		"eventstats-sum-02-float",
		"eventstats-sum-03-string",
		"eventstats-sum-04-multivalue",
		"eventstats-sum-05-missing",
		"eventstats-sum-06-nonnumeric",
		"eventstats-sum-07-null",
		"eventstats-sum-08-missing-group",
		"eventstats-sum-09-null-group",
	}
	groupANumericIDs := []string{
		"eventstats-sum-01-int",
		"eventstats-sum-02-float",
		"eventstats-sum-03-string",
		"eventstats-sum-04-multivalue",
		"eventstats-sum-05-missing",
	}
	assertGroupedSummary := func(name, source, field string) {
		t.Helper()

		logical := buildIntegrationPlan(
			t,
			base+source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
		got := collectEventStatsFieldPresence(
			t,
			ctx,
			connection,
			logical,
			field,
		)
		want := eventStatsFieldPresence{present: 7, nulls: 2, missing: 2, total: 9}
		if got != want {
			t.Fatalf(
				"%s presence = %#v, want %#v",
				name,
				got,
				want,
			)
		}
	}
	numericRows := func(
		value float64,
		presentIDs ...string,
	) []eventStatsNullableFloatRow {
		present := make(map[string]struct{}, len(presentIDs))
		for _, id := range presentIDs {
			present[id] = struct{}{}
		}
		rows := make([]eventStatsNullableFloatRow, 0, len(ids))
		for _, id := range ids {
			row := eventStatsNullableFloatRow{id: id}
			if _, ok := present[id]; ok {
				row.value = &value
			}
			rows = append(rows, row)
		}
		return rows
	}
	type numericAggregateCase struct {
		name        string
		function    string
		output      string
		globalValue float64
		groupValue  float64
	}
	aggregates := []numericAggregateCase{
		{
			name:        "sum",
			function:    "sum",
			output:      "total",
			globalValue: 49.5,
			groupValue:  34.5,
		},
		{
			name:        "avg",
			function:    "avg",
			output:      "mean",
			globalValue: 49.5 / 7,
			groupValue:  34.5 / 5,
		},
	}
	for _, aggregate := range aggregates {
		globalWant := numericRows(aggregate.globalValue, ids...)
		globalGot := collect(
			"scoped global eventstats "+aggregate.name,
			compile(
				base+` | eventstats `+aggregate.function+`(eventstats_sum_value) AS `+aggregate.output+
					` | sort event_id | table event_id `+aggregate.output,
			),
		)
		if !reflect.DeepEqual(globalGot, globalWant) {
			t.Fatalf("global eventstats %s = %#v, want %#v", aggregate.name, globalGot, globalWant)
		}

		decimalWant := numericRows(3.25, ids...)
		decimalGot := collect(
			"tagged Decimal eventstats "+aggregate.name,
			compile(
				base+` | eventstats `+aggregate.function+`(eventstats_sum_decimal) AS `+aggregate.output+
					` | sort event_id | table event_id `+aggregate.output,
			),
		)
		if !reflect.DeepEqual(decimalGot, decimalWant) {
			t.Fatalf("Decimal eventstats %s = %#v, want %#v", aggregate.name, decimalGot, decimalWant)
		}

		fixed := collectSingleValue(
			"fixed multivalue eventstats "+aggregate.name,
			compile(
				base+` | stats values(eventstats_sum_value) AS fixed_values | eventstats `+
					aggregate.function+`(fixed_values) AS `+aggregate.output+` | table `+aggregate.output,
			),
		)
		if fixed == nil || *fixed != aggregate.globalValue {
			t.Fatalf("fixed multivalue eventstats %s = %v, want %g", aggregate.name, fixed, aggregate.globalValue)
		}

		groupedWant := numericRows(aggregate.groupValue, groupANumericIDs...)
		groupedGot := collect(
			"grouped eventstats "+aggregate.name,
			compile(
				base+` | eventstats `+aggregate.function+`(eventstats_sum_value) AS `+aggregate.output+
					` BY eventstats_sum_group | sort event_id | table event_id `+aggregate.output,
			),
		)
		if !reflect.DeepEqual(groupedGot, groupedWant) {
			t.Fatalf("grouped eventstats %s = %#v, want %#v", aggregate.name, groupedGot, groupedWant)
		}
		assertGroupedSummary(
			"grouped eventstats "+aggregate.name,
			` | eventstats `+aggregate.function+`(eventstats_sum_value) AS `+aggregate.output+
				` BY eventstats_sum_group`,
			aggregate.output,
		)

		allIneligible := collect(
			"all-ineligible eventstats "+aggregate.name,
			compile(
				base+` | eventstats `+aggregate.function+`(eventstats_sum_ineligible) AS `+aggregate.output+
					` | sort event_id | table event_id `+aggregate.output,
			),
		)
		if len(allIneligible) != len(ids) {
			t.Fatalf("all-ineligible eventstats %s rows = %d, want %d", aggregate.name, len(allIneligible), len(ids))
		}
		for index, row := range allIneligible {
			if row.id != ids[index] || row.value != nil {
				t.Fatalf(
					"all-ineligible eventstats %s row %d = %#v, want id %q with null %s",
					aggregate.name,
					index,
					row,
					ids[index],
					aggregate.output,
				)
			}
		}

		downstreamWant := []eventStatsNullableFloatRow{
			{id: ids[0], value: &aggregate.globalValue},
			{id: ids[3], value: &aggregate.globalValue},
		}
		downstreamGot := collect(
			"downstream-filtered eventstats "+aggregate.name,
			compile(
				base+` | eventstats `+aggregate.function+`(eventstats_sum_value) AS `+aggregate.output+
					` | where event_id="`+ids[3]+`" OR event_id="`+ids[0]+`" | sort event_id | table event_id `+aggregate.output,
			),
		)
		if !reflect.DeepEqual(downstreamGot, downstreamWant) {
			t.Fatalf("downstream eventstats %s = %#v, want %#v", aggregate.name, downstreamGot, downstreamWant)
		}

		projected := collect(
			"projected-away eventstats "+aggregate.name,
			compile(
				base+` | fields event_id | eventstats `+aggregate.function+`(eventstats_sum_value) AS `+aggregate.output+
					` | sort event_id | head 1 | table event_id `+aggregate.output,
			),
		)
		if want := []eventStatsNullableFloatRow{{id: ids[0]}}; !reflect.DeepEqual(projected, want) {
			t.Fatalf("projected eventstats %s = %#v, want %#v", aggregate.name, projected, want)
		}

		overflow := compileIntegrationSPLForIndex(
			t,
			`index=eventstats-boundary source="eventstats-boundary" | eventstats `+
				aggregate.function+`(eventstats_sum_missing) AS `+aggregate.output+
				` | search event_id="not-present" | table `+aggregate.output,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
			"eventstats-boundary",
		)
		overflowErr := executeCompiledExpectingNoRows(ctx, connection, overflow)
		if overflowErr == nil || !strings.Contains(overflowErr.Error(), EventStatsInputLimitMarker) {
			t.Fatalf("10,001-row eventstats %s error = %v, want atomic limit failure", aggregate.name, overflowErr)
		}
	}

	globalMeanWant := numericRows(49.5/7, ids...)
	aliasedMeanGot := collect(
		"aliased eventstats avg",
		compile(
			base+` | eventstats avg(eventstats_sum_value) AS eventstats_sum_value | sort event_id | table event_id eventstats_sum_value`,
		),
	)
	if !reflect.DeepEqual(aliasedMeanGot, globalMeanWant) {
		t.Fatalf(
			"aliased eventstats avg = %#v, want %#v; input must resolve before output replacement",
			aliasedMeanGot,
			globalMeanWant,
		)
	}

	nonFiniteMean := collectSingleValue(
		"computed non-finite eventstats avg",
		compile(base+` | eventstats avg(eventstats_avg_overflow) AS mean | head 1 | table mean`),
	)
	if nonFiniteMean == nil || !math.IsInf(*nonFiniteMean, 1) {
		t.Fatalf("computed non-finite eventstats avg = %v, want +Inf", nonFiniteMean)
	}

	canonicalTimeMean := collectSingleValue(
		"canonical-time eventstats avg",
		compile(base+` | eventstats avg(_time) AS mean_time | head 1 | table mean_time`),
	)
	wantCanonicalTime := float64(time.Date(
		2026,
		7,
		21,
		3,
		4,
		5,
		123456789,
		time.FixedZone("event-offset", 5*60*60),
	).UnixNano()) / 1e9
	if canonicalTimeMean == nil ||
		math.Abs(*canonicalTimeMean-wantCanonicalTime) > 1e-6 {
		t.Fatalf("canonical-time eventstats avg = %v, want %g", canonicalTimeMean, wantCanonicalTime)
	}
}
