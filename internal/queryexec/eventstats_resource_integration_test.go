package queryexec

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type eventStatsResourceEnvelope struct {
	maximumDuration time.Duration
	maximumMemory   uint64
	maximumRows     uint64
	maximumBytes    uint64
	maximumResults  uint64
	resultBytes     uint64
	maximumGroups   uint64
	maximumThreads  uint64
}

// queryIntegrationTestEventStatsProductionEnvelope executes the smallest
// validating Dynamic extrema graph through every production query path. This
// is deliberately a topology/resource test; the store integration corpus owns
// multirow extrema direction and cross-group semantics. Here, a compact fixture
// makes query-log resource measurements reproducible while still proving the
// published value and analysis products exactly.
func queryIntegrationTestEventStatsProductionEnvelope(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	executor, err := New(connection, Config{
		ReadAdmission: indexread.UnfencedAdmission{},
	})
	if err != nil {
		t.Fatalf("create production eventstats executor: %v", err)
	}

	prefix := "open-splunk-eventstats-envelope-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-"
	operations := []string{"search", "catalog", "summary", "suggestions"}
	issued := 0
	executor.newQueryID = func() (string, error) {
		if issued >= len(operations) {
			return "", fmt.Errorf("eventstats resource test requested too many query IDs")
		}
		queryID := prefix + operations[issued]
		issued++
		return queryID, nil
	}

	const (
		analysis                = `index=main source="source" | eventstats min(status) AS minimum_status BY path`
		fieldAnalysisDeadline   = 15 * time.Second
		fieldSuggestionDeadline = 10 * time.Second
	)
	search := queryIntegrationCompileSearchRange(
		t,
		analysis+` | table event_id minimum_status`,
		indexTime,
		indexTime.Add(-time.Hour),
		indexTime.Add(time.Hour),
	)
	sink := &fakeSink{}
	searchContext, cancelSearch := context.WithTimeout(ctx, defaultMaxExecutionTime)
	if err := executor.Execute(searchContext, search, sink); err != nil {
		cancelSearch()
		t.Fatalf("execute production eventstats search: %v", err)
	}
	cancelSearch()
	wantColumns := []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "minimum_status", Kind: searchjobs.ValueKindMixed, Nullable: true},
	}
	if !slices.Equal(sink.schema.Columns, wantColumns) || len(sink.rows) != 1 {
		t.Fatalf(
			"production eventstats search schema = %#v, rows = %d, want %#v and 1 row",
			sink.schema.Columns,
			len(sink.rows),
			wantColumns,
		)
	}
	if eventID, ok := sink.rows[0][0].String(); !ok || eventID != "queryexec-event" {
		t.Fatalf("production eventstats event_id = %q, string = %v", eventID, ok)
	}
	if minimum, ok := sink.rows[0][1].Double(); !ok || minimum != 200 {
		t.Fatalf("production eventstats minimum_status = %v, double = %v", minimum, ok)
	}

	logical := queryIntegrationFieldPlan(t, "main", indexTime, analysis)
	compiledCatalog, err := (clickhouse.Compiler{}).CompileFieldCatalog(
		logical,
		clickhouse.FieldCatalogSpec{
			MaximumFields: clickhouse.MaximumFieldCatalogFields,
		},
	)
	if err != nil {
		t.Fatalf("compile production eventstats field catalog: %v", err)
	}
	catalogContext, cancelCatalog := context.WithTimeout(ctx, fieldAnalysisDeadline)
	catalog, err := executor.ExecuteFieldCatalog(
		catalogContext,
		compiledCatalog,
	)
	cancelCatalog()
	if err != nil {
		t.Fatalf("execute production eventstats field catalog: %v", err)
	}
	if catalog.TotalEvents != 1 {
		t.Fatalf("production eventstats field catalog = %#v", catalog)
	}
	assertFieldCatalogProfile(t, catalog, FieldProfileRow{
		FieldName:     "minimum_status",
		ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeDouble},
		EventCount:    1,
	})

	compiledSummary := queryIntegrationCompileFieldSummary(
		t,
		"main",
		indexTime,
		analysis,
		"minimum_status",
	)
	summaryContext, cancelSummary := context.WithTimeout(ctx, fieldAnalysisDeadline)
	summary, err := executor.ExecuteFieldSummary(
		summaryContext,
		compiledSummary,
	)
	cancelSummary()
	if err != nil {
		t.Fatalf("execute production eventstats field summary: %v", err)
	}
	if summary.FieldName != "minimum_status" ||
		!slices.Equal(
			summary.ObservedTypes,
			[]eventfields.StoredValueType{eventfields.StoredValueTypeDouble},
		) || summary.EventCount != 1 || summary.NullCount != 0 ||
		summary.MissingCount != 0 || summary.DistinctCount != 1 ||
		len(summary.TopValues) != 1 || summary.TopValues[0].Count != 1 {
		t.Fatalf("production eventstats field summary = %#v", summary)
	}
	if minimum, ok := summary.TopValues[0].Value.Double(); !ok || minimum != 200 {
		t.Fatalf(
			"production eventstats summary minimum_status = %v, double = %v",
			minimum,
			ok,
		)
	}

	compiledSuggestions := queryIntegrationCompileFieldSuggestions(
		t,
		"main",
		indexTime,
		analysis,
		"minimum_",
		clickhouse.MaximumFieldSuggestions,
	)
	suggestionContext, cancelSuggestions := context.WithTimeout(
		ctx,
		fieldSuggestionDeadline,
	)
	suggestions, err := executor.ExecuteFieldSuggestions(
		suggestionContext,
		compiledSuggestions,
	)
	cancelSuggestions()
	if err != nil {
		t.Fatalf("execute production eventstats field suggestions: %v", err)
	}
	if !slices.Equal(suggestions.FieldNames, []string{"minimum_status"}) ||
		suggestions.Truncated {
		t.Fatalf("production eventstats field suggestions = %#v", suggestions)
	}
	if issued != len(operations) {
		t.Fatalf("production eventstats query IDs = %d, want %d", issued, len(operations))
	}

	if err := connection.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush eventstats production query log: %v", err)
	}
	// system.query_log.Settings records deviations from the active profile,
	// so a cap equal to ClickHouse's host-derived default is omitted. Resolve
	// that omission through the effective setting instead of treating it as 0.
	rows, err := connection.Query(
		ctx,
		`SELECT query_id, query_duration_ms, memory_usage,
			toUInt64OrZero(Settings['max_execution_time']),
			toUInt64OrZero(Settings['max_memory_usage']),
			toUInt64OrZero(Settings['max_rows_to_read']),
			toUInt64OrZero(Settings['max_bytes_to_read']),
			toUInt64OrZero(Settings['max_result_rows']),
			toUInt64OrZero(Settings['max_result_bytes']),
			toUInt64OrZero(Settings['max_rows_to_group_by']),
			if(
				mapContains(Settings, 'max_threads'),
				toUInt64OrZero(Settings['max_threads']),
				toUInt64(getSetting('max_threads'))
			),
			toUInt64OrZero(Settings['use_query_cache'])
		FROM system.query_log
		WHERE type = 'QueryFinish' AND startsWith(query_id, ?)`,
		prefix,
	)
	if err != nil {
		t.Fatalf("read eventstats production query log: %v", err)
	}
	defer rows.Close()

	envelopes := map[string]eventStatsResourceEnvelope{
		prefix + "search": {
			maximumDuration: defaultMaxExecutionTime,
			maximumMemory:   defaultMaxMemoryBytes,
			maximumRows:     defaultMaxRowsToRead,
			maximumBytes:    defaultMaxBytesToRead,
			maximumResults:  defaultMaxResultRows,
			resultBytes:     defaultMaxResultBytes,
			maximumGroups:   defaultMaxResultRows,
			maximumThreads:  defaultMaxThreads,
		},
		prefix + "catalog": {
			maximumDuration: fieldAnalysisDeadline,
			maximumMemory:   maximumFieldCatalogMemoryBytes,
			maximumRows:     maximumFieldCatalogRowsToRead,
			maximumBytes:    maximumFieldCatalogBytesToRead,
			maximumResults:  defaultMaxResultRows,
			resultBytes:     maximumFieldCatalogBytes,
			maximumGroups:   uint64(clickhouse.MaximumFieldCatalogFields) + 1,
			maximumThreads:  maximumFieldCatalogThreads,
		},
		prefix + "summary": {
			maximumDuration: fieldAnalysisDeadline,
			maximumMemory:   defaultMaxMemoryBytes,
			maximumRows:     defaultMaxRowsToRead,
			maximumBytes:    defaultMaxBytesToRead,
			maximumResults:  defaultMaxResultRows,
			resultBytes:     maximumFieldSummaryResultBytes,
			maximumGroups:   uint64(clickhouse.MaximumFieldSummaryDistinctValues),
			maximumThreads:  defaultMaxThreads,
		},
		prefix + "suggestions": {
			maximumDuration: fieldSuggestionDeadline,
			maximumMemory:   maximumFieldSuggestionMemoryBytes,
			maximumRows:     maximumFieldSuggestionRowsToRead,
			maximumBytes:    maximumFieldSuggestionBytesToRead,
			maximumResults:  uint64(clickhouse.MaximumFieldSuggestions) + 2,
			resultBytes:     maximumFieldSuggestionResultBytes,
			maximumGroups:   maximumFieldSuggestionGroups,
			maximumThreads:  maximumFieldSuggestionThreads,
		},
	}
	seen := make(map[string]struct{}, len(envelopes))
	for rows.Next() {
		var queryID string
		var durationMS, memory, executionSetting, memorySetting uint64
		var rowsToRead, bytesToRead, resultRows, resultBytes uint64
		var rowsToGroupBy, threads, queryCache uint64
		if err := rows.Scan(
			&queryID,
			&durationMS,
			&memory,
			&executionSetting,
			&memorySetting,
			&rowsToRead,
			&bytesToRead,
			&resultRows,
			&resultBytes,
			&rowsToGroupBy,
			&threads,
			&queryCache,
		); err != nil {
			t.Fatalf("scan eventstats production query log: %v", err)
		}
		envelope, ok := envelopes[queryID]
		if !ok {
			t.Fatalf("unexpected eventstats production query log row %q", queryID)
		}
		if _, duplicate := seen[queryID]; duplicate {
			t.Fatalf("duplicate eventstats production query log row %q", queryID)
		}
		seen[queryID] = struct{}{}
		t.Logf(
			"eventstats production query %q: duration=%s memory=%d settings=[execution=%d memory=%d rows=%d bytes=%d result_rows=%d result_bytes=%d groups=%d threads=%d cache=%d]",
			queryID,
			time.Duration(durationMS)*time.Millisecond,
			memory,
			executionSetting,
			memorySetting,
			rowsToRead,
			bytesToRead,
			resultRows,
			resultBytes,
			rowsToGroupBy,
			threads,
			queryCache,
		)
		if duration := time.Duration(durationMS) * time.Millisecond; duration >= envelope.maximumDuration {
			t.Fatalf(
				"eventstats production query %q duration = %v, want below %v",
				queryID,
				duration,
				envelope.maximumDuration,
			)
		}
		if memory >= envelope.maximumMemory {
			t.Fatalf(
				"eventstats production query %q memory = %d, want below %d",
				queryID,
				memory,
				envelope.maximumMemory,
			)
		}
		// clickhouse-go derives the protocol setting from the remaining context
		// deadline and adds five seconds. The client deadline is still the outer
		// bound above; this range proves the server received a finite companion
		// timeout rather than an unlimited/default setting.
		minimumExecutionSetting := uint64(envelope.maximumDuration / time.Second)
		if executionSetting < minimumExecutionSetting ||
			executionSetting > minimumExecutionSetting+5 {
			t.Fatalf(
				"eventstats production query %q max_execution_time = %d, want %d through %d",
				queryID,
				executionSetting,
				minimumExecutionSetting,
				minimumExecutionSetting+5,
			)
		}
		if memorySetting != envelope.maximumMemory ||
			rowsToRead != envelope.maximumRows ||
			bytesToRead != envelope.maximumBytes ||
			resultRows != envelope.maximumResults ||
			resultBytes != envelope.resultBytes ||
			rowsToGroupBy != envelope.maximumGroups ||
			threads != envelope.maximumThreads || queryCache != 0 {
			t.Fatalf(
				"eventstats production query %q settings = [memory=%d rows=%d bytes=%d result_rows=%d result_bytes=%d groups=%d threads=%d cache=%d], want %#v",
				queryID,
				memorySetting,
				rowsToRead,
				bytesToRead,
				resultRows,
				resultBytes,
				rowsToGroupBy,
				threads,
				queryCache,
				envelope,
			)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate eventstats production query log: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close eventstats production query log: %v", err)
	}
	if len(seen) != len(envelopes) {
		t.Fatalf("eventstats production query log rows = %d, want %d", len(seen), len(envelopes))
	}
}
