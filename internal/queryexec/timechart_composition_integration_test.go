package queryexec

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type compositionSink struct {
	fakeSink
	bounds []searchjobs.TimeBucketBounds
}

func (sink *compositionSink) AddRowWithTimeBucket(values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	sink.bounds = append(sink.bounds, bounds)
	return sink.AddRow(values)
}

func TestTimechartCompositionAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1")
	}
	earliest := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	latest := earliest.Add(time.Hour)
	indexTime := latest.Add(time.Hour)
	ctx, executor := semanticBytesLineageStartClickHouse(t, indexTime, []semanticBytesLineageEvent{
		{id: "compose-a", at: earliest.Add(100 * time.Millisecond), host: "west coast", raw: []byte("a")},
		{id: "compose-b", at: earliest.Add(900 * time.Millisecond), host: "east", raw: []byte("b")},
		{id: "compose-c", at: earliest.Add(2100 * time.Millisecond), host: "west coast", raw: []byte("c")},
	})
	for _, test := range []struct {
		name, source string
		rows         int
		bounds       bool
	}{
		{"deferred lookup", `timechart span=1s count BY host | eval service="api" | lookup catalog service_id AS service OUTPUT owner | where owner="platform" | head 1`, 1, true},
		{"observed lookup", `timechart span=250ms fixedrange=false count | eval service="api" | lookup catalog service_id AS service OUTPUT owner | where owner="platform" | head 1`, 1, true},
		{"renamed timestamp", `timechart span=1s count | eval saved=_time | fields - _time | rename saved AS _time | timechart span=5s count | head 1`, 1, true},
		{"minimum timestamp", `timechart span=1s count | stats min(_time) AS _time | timechart span=5s count | head 1`, 1, true},
		{"rex rename dedup", `timechart span=1s count BY host | eval label="api-123" | rex field=label "(?<service>[a-z]+)-" | rename service AS kind | dedup kind | table kind`, 1, false},
		{"eventstats suffix", `timechart span=1s count BY host | eventstats sum(east) AS total | where total>0 | head 1`, 1, true},
		{"static count", `timechart span=1s count | where count>0 | head 1`, 1, true},
		{"static value", `eval metric=2 | timechart span=1s sum(metric) AS total | where total>0 | head 1`, 1, true},
		{"dynamic literal", `timechart span=1s count BY host | where 'west coast'>0 | table _time 'west coast'`, 2, true},
		{"dynamic reaggregate", `timechart span=1s count BY host | stats sum(*)`, 1, false},
		{"dynamic repeated", `timechart span=1s count BY host | timechart span=5s sum('west coast') AS total | head 1`, 1, true},
		{"static observed", `timechart span=250ms fixedrange=false cont=false count | head 1`, 1, true},
		{"split observed", `eval metric=2 | timechart span=250ms fixedrange=false cont=false sum(metric) BY host | where 'west coast'>0`, 2, true},
		{"observed empty count", `search host=absent | timechart span=250ms fixedrange=false count`, 0, false},
		{"observed empty avg", `search host=absent | timechart span=250ms fixedrange=false avg(missing_metric)`, 0, false},
		{"observed auto", `timechart fixedrange=false cont=false count | head 1`, 1, true},
		{"removed time", `timechart span=1s count BY host | table east`, 3600, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			query, err := spl.Parse(fmt.Sprintf("index=%s | %s", semanticBytesLineageIndex, test.source))
			if err != nil {
				t.Fatal(err)
			}
			visibility := uint64(1)
			logical, err := plan.Build(query, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{semanticBytesLineageIndex}, Earliest: earliest, Latest: latest, SearchStart: indexTime, IndexTimeCutoff: indexTime, VisibilityCutoff: &visibility, SearchTimezone: "UTC"})
			if err != nil {
				t.Fatal(err)
			}
			compiler := clickhouse.Compiler{}
			if strings.Contains(test.source, "lookup catalog") {
				var contract plan.Lookup
				for i, operator := range logical.Operators {
					if lookup, ok := operator.(*plan.Lookup); ok {
						contract = *lookup
						break
					}
					if continuation, ok := logical.TimechartContinuationAt(i); ok {
						contracts, contractErr := continuation.LookupContracts()
						if contractErr != nil {
							t.Fatal(contractErr)
						}
						if len(contracts) != 0 {
							contract = contracts[0]
							break
						}
					}
				}
				resolution, resolutionErr := clickhouse.NewLookupResolution("tenant", "catalog", "catalog-asset", 1, 40, sha256.Sum256([]byte("catalog-1")), []string{"service_id", "owner"}, [][]string{{"api", "platform"}})
				if resolutionErr != nil {
					t.Fatal(resolutionErr)
				}
				resolution, resolutionErr = resolution.WithLogicalContract(contract, "catalog-logical", 1)
				if resolutionErr != nil {
					t.Fatal(resolutionErr)
				}
				if strings.Contains(test.source, "BY host") {
					compiler, err = compiler.WithDeferredLookupResolutionsContext(ctx, []clickhouse.LookupResolution{resolution})
				} else {
					compiler, err = compiler.WithLookupResolutionsContext(ctx, []clickhouse.LookupResolution{resolution})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			compiled, err := compiler.Compile(logical)
			if err != nil {
				t.Fatal(err)
			}
			if !compiled.RequiresTimechartInputDiscovery() {
				settings, err := executor.settingsForContext(ctx, compiled)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := executor.connection.Query(clickhousedriver.Context(ctx, clickhousedriver.WithSettings(settings)), "EXPLAIN PLAN json=1,description=0,indexes=1,actions=0,header=1 "+compiled.SQL, compiled.Args...)
				if err != nil {
					t.Fatal(err)
				}
				var parts []string
				for rows.Next() {
					var line string
					if err := rows.Scan(&line); err != nil {
						_ = rows.Close()
						t.Fatal(err)
					}
					parts = append(parts, line)
				}
				if err := rows.Err(); err != nil {
					_ = rows.Close()
					t.Fatal(err)
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
				physical, err := parseExplainPlanText(ctx, strings.Join(parts, "\n"))
				if err != nil {
					t.Fatal(err)
				}
				if len(physical.Reads) != 1 {
					t.Fatalf("physical MergeTree reads=%d want=1", len(physical.Reads))
				}
			}
			sink := &compositionSink{}
			operationContext, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if err := executor.Execute(operationContext, compiled, sink); err != nil {
				t.Fatal(err)
			}
			if len(sink.rows) != test.rows {
				t.Fatalf("rows=%d want=%d", len(sink.rows), test.rows)
			}
			if test.bounds && len(sink.bounds) != test.rows {
				t.Fatalf("bounds=%d rows=%d", len(sink.bounds), len(sink.rows))
			}
			if !test.bounds && len(sink.bounds) != 0 {
				t.Fatal("replaced time retained bounds")
			}
		})
	}
	for _, source := range []string{
		`eval metric=1e308 | timechart span=1s sum(metric) AS total | where total<0 | head 1`,
		`eval metric=1e308 | timechart span=1s sum(metric) AS total BY source | head 1`,
		fmt.Sprintf(`eval host="%s" | timechart span=1s count BY host | head 1`, strings.Repeat("x", 257)),
	} {
		t.Run("validation "+source, func(t *testing.T) {
			query, err := spl.Parse(fmt.Sprintf("index=%s | %s", semanticBytesLineageIndex, source))
			if err != nil {
				t.Fatal(err)
			}
			visibility := uint64(1)
			logical, err := plan.Build(query, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{semanticBytesLineageIndex}, Earliest: earliest, Latest: latest, SearchStart: indexTime, IndexTimeCutoff: indexTime, VisibilityCutoff: &visibility, SearchTimezone: "UTC"})
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := (clickhouse.Compiler{}).Compile(logical)
			if err != nil {
				t.Fatal(err)
			}
			sink := &compositionSink{}
			if err := executor.Execute(ctx, compiled, sink); err == nil {
				t.Fatal("invalid upstream accepted")
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatal("invalid upstream published partial results")
			}
		})
	}
}

func TestTimechartContinuationCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&Executor{}).executeTimechartStages(ctx, clickhouse.CompiledQuery{}, &fakeSink{}); err == nil {
		t.Fatal("canceled stage accepted")
	}
}

func TestObservedTimechartInputExceedsPreviewRowsAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1")
	}
	earliest := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	latest := earliest.Add(time.Hour)
	indexTime := latest.Add(time.Hour)
	events := make([]semanticBytesLineageEvent, 10005)
	for i := range events {
		events[i] = semanticBytesLineageEvent{id: fmt.Sprintf("bulk-%d", i), at: earliest.Add(time.Millisecond), host: "bulk", raw: []byte("bulk")}
	}
	ctx, executor := semanticBytesLineageStartClickHouse(t, indexTime, events)
	parsed, err := spl.Parse(fmt.Sprintf("index=%s | timechart span=1s fixedrange=false count", semanticBytesLineageIndex))
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{semanticBytesLineageIndex}, Earliest: earliest, Latest: latest, SearchStart: indexTime, IndexTimeCutoff: indexTime, VisibilityCutoff: &visibility, SearchTimezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	sink := &compositionSink{}
	if err := executor.Execute(ctx, compiled, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.rows) != 1 {
		t.Fatalf("rows=%d", len(sink.rows))
	}
	count, ok := sink.rows[0][1].Unsigned()
	if !ok || count != 10005 {
		t.Fatalf("count=%d", count)
	}
}
