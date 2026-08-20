package queryexec

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestExplainerBuffersExactPlanAndPreservesParameters(t *testing.T) {
	t.Parallel()

	planText := strings.Replace(
		validStructuredExplainPlan,
		"private description",
		"Unicode \u2502 plan node",
		1,
	)
	rows := explainFakeRows(planText)
	connection := &fakeQueryConnection{rows: rows}
	explainer := mustExplainer(t, connection)
	query := sealedExplainQuery(t)
	replaced := false
	for index, argument := range query.Args {
		if argument == "needle" {
			query.Args[index] = "bound-secret"
			replaced = true
			break
		}
	}
	if !replaced {
		t.Fatalf("Compiler args have no search literal: %#v", query.Args)
	}

	got, err := explainer.Explain(context.Background(), query)
	if err != nil {
		t.Fatalf("Explain() error = %v", err)
	}
	if got != (ExplainResult{
		Text:    planText,
		QueryID: "open-splunk-explain-test",
	}) {
		t.Fatalf("Explain() = %#v", got)
	}
	wantQuery := explainQueryPrefix + query.SQL + explainQuerySettingsPrefix +
		strconv.FormatUint(
			uint64(maximumExplainExecutionTime/time.Second),
			10,
		) + explainQuerySettingsSuffix
	const wantStructuredPrefix = "EXPLAIN PLAN json = 1, description = 0, indexes = 1, actions = 0, header = 1 SELECT * FROM ("
	const wantStructuredSettings = ") AS __os_explain_input SETTINGS max_execution_time = 10, use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0, enable_full_text_index = 1"
	if explainQueryPrefix != wantStructuredPrefix ||
		explainQuerySettingsPrefix+"10"+explainQuerySettingsSuffix != wantStructuredSettings {
		t.Fatalf(
			"fixed structured PLAN shape drifted: prefix=%q settings=%q",
			explainQueryPrefix,
			explainQuerySettingsPrefix+"10"+explainQuerySettingsSuffix,
		)
	}
	if connection.query != wantQuery ||
		!reflect.DeepEqual(connection.args, query.Args) {
		t.Fatalf(
			"query/args = %q %#v, want %q %#v",
			connection.query,
			connection.args,
			wantQuery,
			query.Args,
		)
	}
	if strings.Contains(got.Text, "bound-secret") {
		t.Fatalf("Explain() serialized a bind argument: %q", got.Text)
	}
	if !rows.closed {
		t.Fatal("EXPLAIN rows were not closed")
	}
	resultType := reflect.TypeFor[ExplainResult]()
	if resultType.NumField() != 2 ||
		resultType.Field(0).Name != "Text" ||
		resultType.Field(1).Name != "QueryID" {
		t.Fatalf("ExplainResult unexpectedly exposes more state: %v", resultType)
	}
}

func TestProductionExplainerRejectsTamperedNonReadArgumentBeforeLaneOrConnection(t *testing.T) {
	t.Parallel()

	query := sealedExplainQueryFromSPL(t, `index=main status="non-read-explain" | table status`)
	query.Args = slices.Clone(query.Args)
	mutated := false
	for index, argument := range query.Args {
		if value, ok := argument.(string); ok && value == "non-read-explain" {
			query.Args[index] = "tampered-filter"
			mutated = true
			break
		}
	}
	if !mutated {
		t.Fatal("compiled fixture did not expose the filter bind argument")
	}
	if _, _, ok := query.ReadScope(); !ok {
		t.Fatal("non-read mutation unexpectedly invalidated the older SQL/read-scope seal")
	}
	connection := &fakeQueryConnection{err: errors.New("connection must not be called")}
	explainer := mustExplainer(t, connection)
	explainer.requireExecutionSeal = true
	lanesBefore := len(explainer.lanes)
	result, err := explainer.Explain(context.Background(), query)
	if !errors.Is(err, searchjobs.ErrInvalidResult) || !reflect.DeepEqual(result, ExplainResult{}) {
		t.Fatalf("Explain(tampered non-read argument) = (%#v, %v)", result, err)
	}
	if connection.query != "" || len(explainer.lanes) != lanesBefore {
		t.Fatalf("tampered query reached connection/lane: query=%q lanes=%d/%d", connection.query, len(explainer.lanes), lanesBefore)
	}
}

func TestValidateExplainResult(t *testing.T) {
	t.Parallel()

	const validQueryID = "open-splunk-explain-validator"
	exactAggregateLines := make(
		[]string,
		int(maximumExplainResultBytes/maximumExplainLineBytes),
	)
	exactAggregateLines[0] = strings.Repeat("x", int(maximumExplainLineBytes))
	for index := 1; index < len(exactAggregateLines); index++ {
		exactAggregateLines[index] = strings.Repeat(
			"x",
			int(maximumExplainLineBytes)-1,
		)
	}
	exactAggregate := strings.Join(exactAggregateLines, "\n")
	if uint64(len(exactAggregate)) != maximumExplainResultBytes {
		t.Fatalf(
			"exact aggregate fixture = %d bytes, want %d",
			len(exactAggregate),
			maximumExplainResultBytes,
		)
	}
	exactRows := make([]string, int(maximumExplainResultRows))
	for index := range exactRows {
		exactRows[index] = "x"
	}
	overRows := append(slices.Clone(exactRows), "x")
	maximumQueryID := explainQueryIDPrefix + strings.Repeat(
		"x",
		maximumExplainQueryIDBytes-len(explainQueryIDPrefix),
	)

	tests := []struct {
		name    string
		result  ExplainResult
		wantErr error
	}{
		{
			name: "valid multiline Unicode",
			result: ExplainResult{
				Text:    "Projection\n  Unicode \u2502 node",
				QueryID: validQueryID,
			},
		},
		{
			name: "maximum query ID",
			result: ExplainResult{
				Text:    "Projection",
				QueryID: maximumQueryID,
			},
		},
		{
			name: "maximum line",
			result: ExplainResult{
				Text:    strings.Repeat("x", int(maximumExplainLineBytes)),
				QueryID: validQueryID,
			},
		},
		{
			name: "maximum aggregate",
			result: ExplainResult{
				Text:    exactAggregate,
				QueryID: validQueryID,
			},
		},
		{
			name: "maximum rows",
			result: ExplainResult{
				Text:    strings.Join(exactRows, "\n"),
				QueryID: validQueryID,
			},
		},
		{
			name: "empty text",
			result: ExplainResult{
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "invalid UTF-8",
			result: ExplainResult{
				Text:    string([]byte{'p', 'r', 'i', 'v', 'a', 't', 'e', 0xff}),
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "control character",
			result: ExplainResult{
				Text:    "private-result\x00",
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "tab control",
			result: ExplainResult{
				Text:    "private\tresult",
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "C1 control",
			result: ExplainResult{
				Text:    "private\u0085result",
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "leading empty line",
			result: ExplainResult{
				Text:    "\nprivate-result",
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "trailing empty line",
			result: ExplainResult{
				Text:    "private-result\n",
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "interior empty line",
			result: ExplainResult{
				Text:    "private-result\n\nnode",
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "line one byte over",
			result: ExplainResult{
				Text:    strings.Repeat("x", int(maximumExplainLineBytes)+1),
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrExecutionLimit,
		},
		{
			name: "aggregate one byte over",
			result: ExplainResult{
				Text:    exactAggregate + "x",
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrExecutionLimit,
		},
		{
			name: "one row over",
			result: ExplainResult{
				Text:    strings.Join(overRows, "\n"),
				QueryID: validQueryID,
			},
			wantErr: searchjobs.ErrExecutionLimit,
		},
		{
			name: "empty query ID",
			result: ExplainResult{
				Text: "private-result",
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "wrong query ID prefix",
			result: ExplainResult{
				Text:    "private-result",
				QueryID: "private-query-id",
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "empty query ID suffix",
			result: ExplainResult{
				Text:    "private-result",
				QueryID: explainQueryIDPrefix,
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "query ID control",
			result: ExplainResult{
				Text:    "private-result",
				QueryID: explainQueryIDPrefix + "private\nid",
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "oversized query ID",
			result: ExplainResult{
				Text: "private-result",
				QueryID: explainQueryIDPrefix + strings.Repeat(
					"x",
					maximumExplainQueryIDBytes-len(explainQueryIDPrefix)+1,
				),
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateExplainResult(test.result)
			if test.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateExplainResult() error = %v", err)
				}
				return
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf(
					"ValidateExplainResult() error = %v, want %v",
					err,
					test.wantErr,
				)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatalf("ValidateExplainResult() leaked result content: %v", err)
			}
		})
	}
}

func TestExplainerAcceptsExactCompilerArgumentInventory(t *testing.T) {
	t.Parallel()

	query := sealedExplainQueryFromSPL(
		t,
		`index=main | eval signed=-7,`+
			`unsigned=18446744073709551615,ratio=1.25,ok=true,text="x",`+
			`rounded=round(1.25, 2) | head 1`,
	)
	inventory := make(map[reflect.Type]struct{})
	for _, argument := range query.Args {
		inventory[reflect.TypeOf(argument)] = struct{}{}
	}
	want := []reflect.Type{
		reflect.TypeFor[string](),
		reflect.TypeFor[bool](),
		reflect.TypeFor[int64](),
		reflect.TypeFor[uint64](),
		reflect.TypeFor[float64](),
		reflect.TypeFor[uint8](),
	}
	if len(inventory) != len(want) {
		t.Fatalf("Compiler argument types = %v, want exactly %v", inventory, want)
	}
	for _, argumentType := range want {
		if _, ok := inventory[argumentType]; !ok {
			t.Fatalf("Compiler argument types = %v, missing %v", inventory, argumentType)
		}
	}

	connection := &fakeQueryConnection{rows: explainStructuredRows()}
	if _, err := mustExplainer(t, connection).Explain(
		context.Background(),
		query,
	); err != nil {
		t.Fatalf("Explain() rejected Compiler argument inventory: %v", err)
	}
	if !reflect.DeepEqual(connection.args, query.Args) {
		t.Fatalf("detached args = %#v, want %#v", connection.args, query.Args)
	}
}

func TestExplainerAcceptsBoundedCompilerArrayArguments(t *testing.T) {
	t.Parallel()

	for _, argument := range []any{
		[]string(nil),
		[]string{},
		[]string{"audit", "main"},
		[]uint8(nil),
		[]uint8{},
		[]uint8{0, 1, 255},
	} {
		query := sealedExplainQuery(t)
		query.Args[0] = argument
		connection := &fakeQueryConnection{rows: explainStructuredRows()}
		if _, err := mustExplainer(t, connection).Explain(
			context.Background(),
			query,
		); err != nil {
			t.Fatalf("Explain() rejected compiler argument %T: %v", argument, err)
		}
		if !reflect.DeepEqual(connection.args[0], argument) {
			t.Fatalf("detached %T = %#v, want %#v", argument, connection.args[0], argument)
		}
	}

	over := sealedExplainQuery(t)
	over.Args[0] = make([]string, maximumExplainArrayElements+1)
	connection := &fakeQueryConnection{rows: explainStructuredRows()}
	if result, err := mustExplainer(t, connection).Explain(
		context.Background(),
		over,
	); !errors.Is(err, searchjobs.ErrExecutionLimit) ||
		result != (ExplainResult{}) || connection.query != "" {
		t.Fatalf("Explain(over-array) = (%#v, %v), query=%q", result, err, connection.query)
	}
}

func TestExplainerRejectsDriverUnsafeArgumentsBeforeQuery(t *testing.T) {
	t.Parallel()

	var typedNil *string
	value := 7
	tests := []struct {
		name     string
		argument any
	}{
		{name: "nil", argument: nil},
		{name: "typed nil", argument: typedNil},
		{name: "pointer", argument: &value},
		{name: "map", argument: map[string]string{"secret": "value"}},
		{name: "other slice", argument: []int64{7}},
		{name: "named scalar", argument: explainNamedString("secret")},
		{name: "panic formatter", argument: panicExplainFormatter{}},
		{name: "panic stringer", argument: panicExplainStringer{}},
		{name: "panic Valuer", argument: panicExplainValuer{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query := sealedExplainQuery(t)
			query.Args[0] = test.argument
			connection := &fakeQueryConnection{rows: explainStructuredRows()}
			got, err := mustExplainer(t, connection).Explain(
				context.Background(),
				query,
			)
			if !errors.Is(err, searchjobs.ErrInvalidResult) ||
				got != (ExplainResult{}) ||
				connection.query != "" {
				t.Fatalf(
					"Explain() = (%#v, %v), query=%q",
					got,
					err,
					connection.query,
				)
			}
		})
	}
}

func TestExplainerRequiresExactCompilerPlaceholderCardinality(t *testing.T) {
	t.Parallel()

	valid := sealedExplainQuery(t)
	if got, want := compilerPlaceholderCount(valid.SQL), len(valid.Args); got != want {
		t.Fatalf("Compiler placeholders = %d, args = %d", got, want)
	}
	tests := []struct {
		name string
		args []any
	}{
		{
			name: "missing",
			args: slices.Clone(valid.Args[:len(valid.Args)-1]),
		},
		{
			name: "extra",
			args: append(slices.Clone(valid.Args), "ignored-by-clickhouse-go"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query := valid
			query.Args = test.args
			connection := &fakeQueryConnection{rows: explainStructuredRows()}
			got, err := mustExplainer(t, connection).Explain(
				context.Background(),
				query,
			)
			if !errors.Is(err, searchjobs.ErrInvalidResult) ||
				!strings.Contains(err.Error(), "compiler placeholders") ||
				got != (ExplainResult{}) ||
				connection.query != "" {
				t.Fatalf(
					"Explain() = (%#v, %v), query=%q",
					got,
					err,
					connection.query,
				)
			}
		})
	}
}

func TestExplainerBoundsRenderedArgumentsBeforeDriverBinding(t *testing.T) {
	t.Parallel()

	query := sealedExplainQuery(t)
	query.Args[0] = strings.Repeat(`\\'`, 300<<10)
	connection := &fakeQueryConnection{rows: explainStructuredRows()}
	got, err := mustExplainer(t, connection).Explain(
		context.Background(),
		query,
	)
	if !errors.Is(err, searchjobs.ErrExecutionLimit) ||
		got != (ExplainResult{}) ||
		connection.query != "" {
		t.Fatalf(
			"Explain() = (%#v, %v), query bytes=%d",
			got,
			err,
			len(connection.query),
		)
	}
}

func TestExplainerDetachesArgumentsBeforeQueryAdmission(t *testing.T) {
	query := sealedExplainQuery(t)
	arrayArgument := []string{"before-admission"}
	query.Args[0] = arrayArgument
	wantArgs := slices.Clone(query.Args)
	wantArgs[0] = slices.Clone(arrayArgument)
	connection := &fakeQueryConnection{rows: explainStructuredRows()}
	explainer := mustExplainer(t, connection)
	detached := make(chan struct{})
	release := make(chan struct{})
	explainer.newQueryID = func() (string, error) {
		close(detached)
		<-release
		return "open-splunk-explain-detached", nil
	}

	type explainCallResult struct {
		result ExplainResult
		err    error
	}
	completed := make(chan explainCallResult, 1)
	go func() {
		result, err := explainer.Explain(context.Background(), query)
		completed <- explainCallResult{result: result, err: err}
	}()
	select {
	case <-detached:
	case <-time.After(time.Second):
		t.Fatal("Explain() did not reach post-detachment admission")
	}
	// The query-ID hook is after detachment and before Query. Mutating the
	// caller-owned slices here is synchronized and therefore race-detector safe.
	arrayArgument[0] = "mutated-after-admission"
	query.Args[0] = strings.Repeat("after-admission", 64<<10)
	close(release)

	select {
	case call := <-completed:
		if call.err != nil {
			t.Fatalf("Explain() error = %v", call.err)
		}
		if !reflect.DeepEqual(connection.args, wantArgs) {
			t.Fatalf("Query args = %#v, want detached %#v", connection.args, wantArgs)
		}
	case <-time.After(time.Second):
		t.Fatal("Explain() did not complete")
	}
}

func TestSettingsForExplainClonesAndTightensEveryResourceCap(t *testing.T) {
	t.Parallel()

	base, err := querySettings(Config{
		MaxExecutionTime: 2 * maximumExplainExecutionTime,
		MaxMemoryBytes:   2 * maximumExplainMemoryBytes,
		MaxRowsToRead:    2 * maximumExplainRowsToRead,
		MaxBytesToRead:   2 * maximumExplainBytesToRead,
		MaxResultRows:    2 * maximumExplainResultRows,
		MaxResultBytes:   2 * maximumExplainResultBytes,
		MaxRowsToGroupBy: 2 * maximumExplainGroups,
		MaxThreads:       4,
	})
	if err != nil {
		t.Fatal(err)
	}
	base["use_query_cache"] = uint8(1)
	base["max_query_size"] = 2 * defaultMaxQueryBytes
	before := maps.Clone(base)

	got, err := settingsForExplain(mustValidatedSettings(t, base))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"readonly":             uint8(2),
		"max_execution_time":   uint64(maximumExplainExecutionTime / time.Second),
		"max_memory_usage":     maximumExplainMemoryBytes,
		"max_rows_to_read":     maximumExplainRowsToRead,
		"max_bytes_to_read":    maximumExplainBytesToRead,
		"max_result_rows":      maximumExplainResultRows,
		"max_result_bytes":     maximumExplainResultBytes,
		"max_rows_to_group_by": maximumExplainGroups,
		"max_threads":          maximumExplainThreads,
		"max_query_size":       defaultMaxQueryBytes,
		"use_query_cache":      uint8(0),
	}
	for _, name := range requiredTextIndexSettingNames {
		want[name] = uint8(1)
	}
	for name, wantValue := range want {
		if got[name] != wantValue {
			t.Errorf("setting %s = %#v, want %#v", name, got[name], wantValue)
		}
	}
	if !reflect.DeepEqual(base, before) {
		t.Fatalf("base settings were mutated: got %#v, want %#v", base, before)
	}
	if got["max_query_size"].(uint64) <=
		maximumExplainQueryBytes+uint64(len(explainQueryPrefix))+(256<<10) {
		t.Fatalf(
			"max_query_size %d leaves no bounded driver-parameter headroom above raw SQL",
			got["max_query_size"],
		)
	}

	stricter := maps.Clone(base)
	stricter["max_execution_time"] = uint64(3)
	stricter["max_memory_usage"] = uint64(1 << 20)
	stricter["max_result_rows"] = uint64(7)
	stricter["max_result_bytes"] = uint64(1_024)
	stricter["max_query_size"] = uint64(2_048)
	strict, err := settingsForExplain(mustValidatedSettings(t, stricter))
	if err != nil {
		t.Fatal(err)
	}
	for name, wantValue := range map[string]any{
		"max_execution_time": uint64(3),
		"max_memory_usage":   uint64(1 << 20),
		"max_result_rows":    uint64(7),
		"max_result_bytes":   uint64(1_024),
		"max_query_size":     uint64(2_048),
	} {
		if strict[name] != wantValue {
			t.Errorf("stricter setting %s = %#v, want %#v", name, strict[name], wantValue)
		}
	}
}

func TestExplainerPreservesExactTimeoutAndPinsCeiledServerSetting(t *testing.T) {
	t.Parallel()

	connection := &deadlineExplainConnection{rows: explainStructuredRows()}
	explainer := mustExplainer(t, connection)
	explainer.settings["max_execution_time"] = uint64(2)
	explainer.executionTimeout = 1500 * time.Millisecond
	query := sealedExplainQuery(t)
	started := time.Now()
	if _, err := explainer.Explain(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	want := explainQueryPrefix + query.SQL + explainQuerySettingsPrefix + "2" +
		explainQuerySettingsSuffix
	if connection.query != want {
		t.Fatalf("Query() SQL = %q, want %q", connection.query, want)
	}
	if !connection.hasDeadline {
		t.Fatal("stricter timeout was not exposed as a transport deadline")
	}
	remaining := connection.deadline.Sub(started)
	if remaining < 1400*time.Millisecond || remaining > 1600*time.Millisecond {
		t.Fatalf("exact execution timeout = %v, want about 1.5s", remaining)
	}
}

func TestExplainerRejectsInvalidStateQueryAndQueryIDBeforeExecution(t *testing.T) {
	var typedNilConnection *fakeQueryConnection
	validQuery := sealedExplainQuery(t)
	tests := []struct {
		name     string
		executor func(*fakeQueryConnection) *Explainer
		ctx      context.Context
		query    clickhouse.CompiledQuery
	}{
		{
			name: "nil receiver",
			executor: func(*fakeQueryConnection) *Explainer {
				return nil
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "nil connection",
			executor: func(*fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, typedNilConnection)
				for _, lane := range executor.allLanes {
					lane.connection = nil
				}
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "typed nil connection",
			executor: func(*fakeQueryConnection) *Explainer {
				return mustExplainer(t, typedNilConnection)
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "nil transport discard",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				for _, lane := range executor.allLanes {
					lane.discard = nil
				}
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "nil query ID generator",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				executor.newQueryID = nil
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "nil gate",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				executor.lanes = nil
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "wrong gate capacity",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				executor.lanes = make(chan *explainLane, 1)
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "nil settings",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				executor.settings = nil
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "nil context",
			executor: func(connection *fakeQueryConnection) *Explainer {
				return mustExplainer(t, connection)
			},
			query: validQuery,
		},
		{
			name: "blank SQL",
			executor: func(connection *fakeQueryConnection) *Explainer {
				return mustExplainer(t, connection)
			},
			ctx: context.Background(), query: clickhouse.CompiledQuery{SQL: " \n\t"},
		},
		{
			name: "oversized SQL",
			executor: func(connection *fakeQueryConnection) *Explainer {
				return mustExplainer(t, connection)
			},
			ctx: context.Background(), query: clickhouse.CompiledQuery{
				SQL: strings.Repeat("x", 256<<10+1),
			},
		},
		{
			name: "invalid UTF-8 SQL",
			executor: func(connection *fakeQueryConnection) *Explainer {
				return mustExplainer(t, connection)
			},
			ctx: context.Background(), query: clickhouse.CompiledQuery{
				SQL: string([]byte{'S', 'E', 'L', 'E', 'C', 'T', ' ', 0xff}),
			},
		},
		{
			name: "NUL SQL",
			executor: func(connection *fakeQueryConnection) *Explainer {
				return mustExplainer(t, connection)
			},
			ctx: context.Background(), query: clickhouse.CompiledQuery{SQL: "SELECT\x00 1"},
		},
		{
			name: "control SQL",
			executor: func(connection *fakeQueryConnection) *Explainer {
				return mustExplainer(t, connection)
			},
			ctx: context.Background(), query: clickhouse.CompiledQuery{SQL: "SELECT\v1"},
		},
		{
			name: "query ID generation failure",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				executor.newQueryID = func() (string, error) {
					return "", errors.New("secret random source detail")
				}
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "empty query ID",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				executor.newQueryID = func() (string, error) { return "", nil }
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "query ID control",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				executor.newQueryID = func() (string, error) {
					return "explain\nsecret", nil
				}
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "query ID wrong prefix",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				executor.newQueryID = func() (string, error) {
					return "open-splunk-search-deadbeef", nil
				}
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "query ID empty suffix",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				executor.newQueryID = func() (string, error) {
					return explainQueryIDPrefix, nil
				}
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
		{
			name: "oversized query ID",
			executor: func(connection *fakeQueryConnection) *Explainer {
				executor := mustExplainer(t, connection)
				executor.newQueryID = func() (string, error) {
					return explainQueryIDPrefix +
						strings.Repeat(
							"x",
							maximumExplainQueryIDBytes-len(explainQueryIDPrefix)+1,
						), nil
				}
				return executor
			},
			ctx: context.Background(), query: validQuery,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &fakeQueryConnection{rows: explainStructuredRows()}
			executor := test.executor(connection)
			got, err := executor.Explain(test.ctx, test.query)
			if err == nil || got != (ExplainResult{}) {
				t.Fatalf("Explain() = (%#v, %v), want zero and error", got, err)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("Explain() leaked private error detail: %v", err)
			}
			if connection.query != "" {
				t.Fatalf("invalid request issued query %q", connection.query)
			}
		})
	}

	t.Run("canceled context", func(t *testing.T) {
		connection := &fakeQueryConnection{rows: explainStructuredRows()}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		got, err := mustExplainer(t, connection).Explain(ctx, validQuery)
		if !errors.Is(err, context.Canceled) || got != (ExplainResult{}) ||
			connection.query != "" {
			t.Fatalf("Explain() = (%#v, %v), query=%q", got, err, connection.query)
		}
	})
}

func TestExplainerRequiresUnchangedCompilerSQLBeforeQuery(t *testing.T) {
	sealed := sealedExplainQuery(t)
	forged := clickhouse.CompiledQuery{
		SQL:          sealed.SQL,
		Args:         sealed.Args,
		OutputFields: sealed.OutputFields,
		Timechart:    sealed.Timechart,
		Chart:        sealed.Chart,
		SparseFields: sealed.SparseFields,
	}
	appendedSettings := sealed
	appendedSettings.SQL += " SETTINGS max_execution_time = 0, readonly = 0"
	multiStatement := sealed
	multiStatement.SQL += "; SELECT currentUser()"

	for _, test := range []struct {
		name  string
		query clickhouse.CompiledQuery
	}{
		{name: "forged public fields", query: forged},
		{name: "appended settings", query: appendedSettings},
		{name: "multiple statements", query: multiStatement},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := &fakeQueryConnection{rows: explainStructuredRows()}
			got, err := mustExplainer(t, connection).Explain(
				context.Background(),
				test.query,
			)
			if !errors.Is(err, searchjobs.ErrInvalidResult) ||
				got != (ExplainResult{}) ||
				connection.query != "" {
				t.Fatalf(
					"Explain() = (%#v, %v), query=%q",
					got,
					err,
					connection.query,
				)
			}
			if !strings.Contains(err.Error(), "unchanged Compiler result") {
				t.Fatalf("provenance error = %v", err)
			}
		})
	}
}

func TestValidateExplainQueryChecksRawLengthBeforeContentAndSeal(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantMessage string
	}{
		{
			name: "oversized invalid blank SQL is length-first",
			sql: strings.Repeat(" ", 256<<10) +
				string([]byte{0xff}),
			wantMessage: "raw byte limit",
		},
		{
			name:        "blank SQL",
			sql:         " \n\t",
			wantMessage: "is empty",
		},
		{
			name: "invalid UTF-8",
			sql: string([]byte{
				'S', 'E', 'L', 'E', 'C', 'T', ' ', 0xff,
			}),
			wantMessage: "valid UTF-8",
		},
		{
			name:        "NUL",
			sql:         "SELECT\x00 1",
			wantMessage: "contains NUL",
		},
		{
			name:        "unsupported control",
			sql:         "SELECT\v1",
			wantMessage: "unsupported controls",
		},
		{
			name:        "exact raw byte maximum reaches provenance check",
			sql:         strings.Repeat("x", 256<<10),
			wantMessage: "unchanged Compiler result",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateExplainQuery(clickhouse.CompiledQuery{SQL: test.sql})
			if !errors.Is(err, searchjobs.ErrInvalidResult) ||
				!strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf("validateExplainQuery() error = %v", err)
			}
		})
	}
}

func TestExplainerRejectsMalformedSchemaAndEmptyResults(t *testing.T) {
	validQuery := sealedExplainQuery(t)
	var typedNilType *fakeColumnType
	tests := []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{
			name: "wrong column",
			mutate: func(rows *fakeRows) {
				rows.columns[0] = "plan"
			},
		},
		{
			name: "extra column",
			mutate: func(rows *fakeRows) {
				rows.columns = append(rows.columns, "extra")
			},
		},
		{
			name: "missing type",
			mutate: func(rows *fakeRows) {
				rows.types = nil
			},
		},
		{
			name: "typed nil type",
			mutate: func(rows *fakeRows) {
				rows.types[0] = typedNilType
			},
		},
		{
			name: "wrong type name",
			mutate: func(rows *fakeRows) {
				rows.types[0] = fakeColumnType{
					name: "explain", databaseType: "LowCardinality(String)",
					scanType: reflect.TypeFor[string](),
				}
			},
		},
		{
			name: "nullable type",
			mutate: func(rows *fakeRows) {
				rows.types[0] = fakeColumnType{
					name: "explain", databaseType: "String",
					scanType: reflect.TypeFor[string](), nullable: true,
				}
			},
		},
		{
			name: "wrong scan type",
			mutate: func(rows *fakeRows) {
				rows.types[0] = fakeColumnType{
					name: "explain", databaseType: "String",
					scanType: reflect.TypeFor[[]byte](),
				}
			},
		},
		{
			name: "empty result",
			mutate: func(rows *fakeRows) {
				rows.data = nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := explainStructuredRows()
			test.mutate(rows)
			got, err := mustExplainer(
				t,
				&fakeQueryConnection{rows: rows},
			).Explain(context.Background(), validQuery)
			if !errors.Is(err, searchjobs.ErrInvalidResult) ||
				got != (ExplainResult{}) ||
				!rows.closed {
				t.Fatalf("Explain() = (%#v, %v), rows closed=%v", got, err, rows.closed)
			}
		})
	}

	t.Run("typed nil rows", func(t *testing.T) {
		var rows *fakeRows
		got, err := mustExplainer(
			t,
			&fakeQueryConnection{rows: rows},
		).Explain(context.Background(), validQuery)
		if !errors.Is(err, searchjobs.ErrInvalidResult) || got != (ExplainResult{}) {
			t.Fatalf("Explain() = (%#v, %v)", got, err)
		}
	})
}

func TestExplainerEnforcesTextRowsLineAndByteContracts(t *testing.T) {
	validQuery := sealedExplainQuery(t)
	tooManyRows := make([]string, 4_097)
	for index := range tooManyRows {
		tooManyRows[index] = "x"
	}
	exactBytes := make(
		[]string,
		int(maximumExplainResultBytes/maximumExplainLineBytes),
	)
	exactBytes[0] = strings.Repeat("x", int(maximumExplainLineBytes))
	for index := 1; index < len(exactBytes); index++ {
		exactBytes[index] = strings.Repeat("x", int(maximumExplainLineBytes)-1)
	}
	oneByteOver := slices.Clone(exactBytes)
	oneByteOver[1] += "x"
	tests := []struct {
		name    string
		lines   []string
		wantErr error
	}{
		{name: "empty line", lines: []string{""}, wantErr: searchjobs.ErrInvalidResult},
		{
			name: "invalid UTF-8",
			lines: []string{
				string([]byte{'p', 'l', 'a', 'n', 0xff}),
			},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{name: "NUL", lines: []string{"plan\x00node"}, wantErr: searchjobs.ErrInvalidResult},
		{name: "tab", lines: []string{"plan\tnode"}, wantErr: searchjobs.ErrInvalidResult},
		{
			name: "C1 control", lines: []string{"plan\u0085node"},
			wantErr: searchjobs.ErrInvalidResult,
		},
		{
			name: "oversized line", lines: []string{
				strings.Repeat("x", int(maximumExplainLineBytes)+1),
			},
			wantErr: searchjobs.ErrExecutionLimit,
		},
		{name: "too many rows", lines: tooManyRows, wantErr: searchjobs.ErrExecutionLimit},
		{
			name: "one byte over total including newlines", lines: oneByteOver,
			wantErr: searchjobs.ErrExecutionLimit,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := explainFakeRows(strings.Join(test.lines, "\n"))
			got, err := mustExplainer(
				t,
				&fakeQueryConnection{rows: rows},
			).Explain(context.Background(), validQuery)
			if !errors.Is(err, test.wantErr) || got != (ExplainResult{}) || !rows.closed {
				t.Fatalf("Explain() = (%#v, %v), rows closed=%v", got, err, rows.closed)
			}
		})
	}

	t.Run("multiple server rows are rejected atomically", func(t *testing.T) {
		rows := explainFakeRows(
			validStructuredExplainPlan,
			validStructuredExplainPlan,
		)
		got, err := mustExplainer(
			t,
			&fakeQueryConnection{rows: rows},
		).Explain(context.Background(), validQuery)
		if !errors.Is(err, searchjobs.ErrInvalidResult) ||
			got != (ExplainResult{}) ||
			!rows.closed {
			t.Fatalf("Explain() = (%#v, %v), rows closed=%v", got, err, rows.closed)
		}
	})

	t.Run("maximum normalized lines are accepted atomically", func(t *testing.T) {
		lines := make([]string, 4_096)
		for index := range lines {
			lines[index] = "x"
		}
		var text strings.Builder
		var lineCount, resultBytes uint64
		err := appendExplainRow(
			&text,
			strings.Join(lines, "\n"),
			&lineCount,
			&resultBytes,
		)
		if err != nil {
			t.Fatal(err)
		}
		if text.String() != strings.Join(lines, "\n") ||
			lineCount != maximumExplainResultRows {
			t.Fatalf(
				"maximum-line result = bytes %d lines %d",
				text.Len(),
				lineCount,
			)
		}
	})

	t.Run("maximum normalized line is accepted", func(t *testing.T) {
		line := strings.Repeat("x", int(maximumExplainLineBytes))
		var text strings.Builder
		var lineCount, resultBytes uint64
		err := appendExplainRow(
			&text,
			line,
			&lineCount,
			&resultBytes,
		)
		if err != nil || text.String() != line ||
			lineCount != 1 ||
			resultBytes != maximumExplainLineBytes {
			t.Fatalf(
				"appendExplainRow() = bytes %d lines %d accounted %d error %v",
				text.Len(),
				lineCount,
				resultBytes,
				err,
			)
		}
	})

	t.Run("one structured JSON driver row is normalized into bounded lines", func(t *testing.T) {
		row := validStructuredExplainPlan
		got, err := mustExplainer(
			t,
			&fakeQueryConnection{rows: explainFakeRows(row)},
		).Explain(context.Background(), validQuery)
		if err != nil || got.Text != row {
			t.Fatalf("Explain() = (%q, %v), want exact structured row", got.Text, err)
		}
	})

	t.Run("one structured driver row cannot exceed the line count", func(t *testing.T) {
		lines := make([]string, int(maximumExplainResultRows)+1)
		for index := range lines {
			lines[index] = "x"
		}
		rows := explainFakeRows(strings.Join(lines, "\n"))
		got, err := mustExplainer(
			t,
			&fakeQueryConnection{rows: rows},
		).Explain(context.Background(), validQuery)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) ||
			got != (ExplainResult{}) ||
			!rows.closed {
			t.Fatalf("Explain() = (%#v, %v), rows closed=%v", got, err, rows.closed)
		}
	})

	t.Run("structured driver rows retain per-line control rejection", func(t *testing.T) {
		rows := explainFakeRows("{\n  \"Plan\": \"private\tvalue\"\n}")
		got, err := mustExplainer(
			t,
			&fakeQueryConnection{rows: rows},
		).Explain(context.Background(), validQuery)
		if !errors.Is(err, searchjobs.ErrInvalidResult) ||
			got != (ExplainResult{}) ||
			!rows.closed {
			t.Fatalf("Explain() = (%#v, %v), rows closed=%v", got, err, rows.closed)
		}
	})

	t.Run("exact total byte maximum including newlines is accepted", func(t *testing.T) {
		var text strings.Builder
		var lineCount, resultBytes uint64
		err := appendExplainRow(
			&text,
			strings.Join(exactBytes, "\n"),
			&lineCount,
			&resultBytes,
		)
		if err != nil {
			t.Fatal(err)
		}
		if text.Len() != 1<<20 ||
			text.String() != strings.Join(exactBytes, "\n") ||
			resultBytes != maximumExplainResultBytes {
			t.Fatalf(
				"exact-boundary result bytes = %d accounted %d, want %d",
				text.Len(),
				resultBytes,
				1<<20,
			)
		}
	})
}

func TestExplainerSanitizesAndClassifiesDriverFailures(t *testing.T) {
	const leak = "password=hunter2 generated SQL SELECT private"
	validQuery := sealedExplainQuery(t)
	validQuery.Args[0] = leak
	tests := []struct {
		name       string
		connection queryConnection
		wantErr    error
	}{
		{
			name: "query resource limit",
			connection: &fakeQueryConnection{err: &clickhousedriver.Exception{
				Code: 241, Name: "MEMORY_LIMIT_EXCEEDED", Message: leak,
			}},
			wantErr: searchjobs.ErrExecutionLimit,
		},
		{
			name: "query storage failure",
			connection: &fakeQueryConnection{
				err: fmt.Errorf("%s: %w", leak, io.ErrUnexpectedEOF),
			},
			wantErr: searchjobs.ErrStorageUnavailable,
		},
		{
			name:       "query unknown failure",
			connection: &fakeQueryConnection{err: errors.New(leak)},
			wantErr:    errExplainQueryFailed,
		},
		{
			name: "scan storage failure",
			connection: &fakeQueryConnection{rows: &controlledExplainRows{
				fakeRows: explainStructuredRows(),
				scanErr:  fmt.Errorf("%s: %w", leak, io.ErrUnexpectedEOF),
			}},
			wantErr: searchjobs.ErrStorageUnavailable,
		},
		{
			name: "scan unknown failure",
			connection: &fakeQueryConnection{rows: &controlledExplainRows{
				fakeRows: explainStructuredRows(),
				scanErr:  errors.New(leak),
			}},
			wantErr: errExplainQueryFailed,
		},
		{
			name: "iteration failure",
			connection: &fakeQueryConnection{rows: func() *fakeRows {
				rows := explainStructuredRows()
				rows.err = fmt.Errorf("%s: %w", leak, io.ErrUnexpectedEOF)
				return rows
			}()},
			wantErr: searchjobs.ErrStorageUnavailable,
		},
		{
			name: "close failure",
			connection: &fakeQueryConnection{rows: func() *fakeRows {
				rows := explainStructuredRows()
				rows.closeErr = fmt.Errorf("%s: %w", leak, io.ErrUnexpectedEOF)
				return rows
			}()},
			wantErr: searchjobs.ErrStorageUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := mustExplainer(t, test.connection).Explain(
				context.Background(),
				validQuery,
			)
			if !errors.Is(err, test.wantErr) || got != (ExplainResult{}) {
				t.Fatalf("Explain() = (%#v, %v), want %v", got, err, test.wantErr)
			}
			if strings.Contains(err.Error(), leak) {
				t.Fatalf("Explain() leaked driver or query detail: %v", err)
			}
		})
	}

	t.Run("query failure discards the lane transport", func(t *testing.T) {
		for _, test := range []struct {
			name       string
			discardErr error
		}{
			{name: "successful discard"},
			{
				name:       "sanitized discard failure",
				discardErr: errors.New(leak),
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				var discards atomic.Int32
				explainer := mustExplainer(
					t,
					&fakeQueryConnection{err: errors.New(leak)},
				)
				for _, lane := range explainer.allLanes {
					lane.discard = func() error {
						discards.Add(1)
						return test.discardErr
					}
				}
				got, err := explainer.Explain(
					context.Background(),
					validQuery,
				)
				if !errors.Is(err, errExplainQueryFailed) ||
					got != (ExplainResult{}) ||
					discards.Load() != 1 ||
					strings.Contains(err.Error(), leak) {
					t.Fatalf(
						"Explain() = (%#v, %v), discards = %d",
						got,
						err,
						discards.Load(),
					)
				}
			})
		}
	})

	t.Run("primary validation error wins over close failure", func(t *testing.T) {
		rows := explainStructuredRows()
		rows.columns[0] = "wrong"
		rows.closeErr = errors.New(leak)
		got, err := mustExplainer(
			t,
			&fakeQueryConnection{rows: rows},
		).Explain(context.Background(), validQuery)
		if !errors.Is(err, searchjobs.ErrInvalidResult) ||
			got != (ExplainResult{}) ||
			strings.Contains(err.Error(), leak) {
			t.Fatalf("Explain() = (%#v, %v)", got, err)
		}
	})
}

func TestExplainerCancellationIsAtomicAndBoundsLaneWait(t *testing.T) {
	validQuery := sealedExplainQuery(t)
	t.Run("cancellation during scan wins over driver detail", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		rows := &controlledExplainRows{
			fakeRows: explainStructuredRows(),
			beforeScan: func() {
				cancel()
			},
			scanErr: errors.New("secret scan detail"),
		}
		got, err := mustExplainer(
			t,
			&fakeQueryConnection{rows: rows},
		).Explain(ctx, validQuery)
		if !errors.Is(err, context.Canceled) ||
			got != (ExplainResult{}) ||
			strings.Contains(err.Error(), "secret") ||
			!rows.closed {
			t.Fatalf("Explain() = (%#v, %v), rows closed=%v", got, err, rows.closed)
		}
	})

	t.Run("full gate fails fast without query work", func(t *testing.T) {
		connection := &fakeQueryConnection{rows: explainStructuredRows()}
		explainer := mustExplainer(t, connection)
		firstLane := <-explainer.lanes
		secondLane := <-explainer.lanes
		defer func() {
			explainer.lanes <- firstLane
			explainer.lanes <- secondLane
		}()
		start := time.Now()
		got, err := explainer.Explain(
			context.Background(),
			clickhouse.CompiledQuery{SQL: "unsealed work must not be validated"},
		)
		if !errors.Is(err, searchjobs.ErrCapacity) ||
			got != (ExplainResult{}) ||
			connection.query != "" {
			t.Fatalf("Explain() = (%#v, %v), query=%q", got, err, connection.query)
		}
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Fatalf("full gate returned after %v, want fail-fast capacity", elapsed)
		}
	})

	t.Run("long caller deadline remains driver-visible and internally bounded", func(t *testing.T) {
		connection := &deadlineExplainConnection{rows: explainStructuredRows()}
		explainer := mustExplainer(t, connection)
		var discards atomic.Int32
		for _, lane := range explainer.allLanes {
			lane.discard = func() error {
				discards.Add(1)
				return nil
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		start := time.Now()
		if _, err := explainer.Explain(
			ctx,
			validQuery,
		); err != nil {
			t.Fatal(err)
		}
		if discards.Load() != 0 {
			t.Fatalf(
				"successful query discarded %d transports",
				discards.Load(),
			)
		}
		if !connection.hasDeadline {
			t.Fatal("driver context hid the transport deadline")
		}
		remainingAtAdmission := connection.deadline.Sub(start)
		if remainingAtAdmission <= 9*time.Second ||
			remainingAtAdmission > maximumExplainExecutionTime+time.Second {
			t.Fatalf(
				"driver deadline remaining = %v, want the real bounded timeout",
				remainingAtAdmission,
			)
		}
		if connection.context == nil {
			t.Fatal("driver context was not captured")
		}
		select {
		case <-connection.context.Done():
		default:
			t.Fatal("driver context did not inherit timer cancellation")
		}
		if !errors.Is(connection.context.Err(), context.Canceled) {
			t.Fatalf("driver context error = %v, want cancellation", connection.context.Err())
		}
	})

	t.Run("sub-second caller deadline stays visible and cancels driver wait", func(t *testing.T) {
		connection := &cancelWaitingExplainConnection{}
		explainer := mustExplainer(t, connection)
		var discards atomic.Int32
		for _, lane := range explainer.allLanes {
			lane.discard = func() error {
				discards.Add(1)
				return errors.New("secret discard detail")
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		got, err := explainer.Explain(ctx, validQuery)
		if !errors.Is(err, context.DeadlineExceeded) ||
			got != (ExplainResult{}) ||
			!connection.hasDeadline ||
			!errors.Is(connection.observedErr, context.DeadlineExceeded) ||
			discards.Load() != 1 ||
			strings.Contains(err.Error(), "secret") {
			t.Fatalf(
				"Explain() = (%#v, %v), driver deadline=%v error=%v discards=%d",
				got,
				err,
				connection.hasDeadline,
				connection.observedErr,
				discards.Load(),
			)
		}
	})
}

func TestSanitizeExplainQueryErrorHandlesSocketContextDeadlineRace(
	t *testing.T,
) {
	t.Parallel()

	timeoutErr := explainTestTimeoutError{detail: "secret socket detail"}
	publishedLate := explainTestDeadlineContext{
		Context:  context.Background(),
		deadline: time.Now().Add(-time.Second),
	}
	if err := sanitizeExplainQueryError(publishedLate, timeoutErr); !errors.Is(
		err,
		context.DeadlineExceeded,
	) || strings.Contains(err.Error(), timeoutErr.detail) {
		t.Fatalf("expired socket timeout classification = %v", err)
	}

	earlierDriverTimeout := explainTestDeadlineContext{
		Context:  context.Background(),
		deadline: time.Now().Add(time.Hour),
	}
	if err := sanitizeExplainQueryError(
		earlierDriverTimeout,
		timeoutErr,
	); !errors.Is(err, searchjobs.ErrStorageUnavailable) ||
		strings.Contains(err.Error(), timeoutErr.detail) {
		t.Fatalf("early socket timeout classification = %v", err)
	}

	if err := sanitizeExplainQueryError(
		publishedLate,
		errors.New("secret non-timeout detail"),
	); !errors.Is(err, errExplainQueryFailed) ||
		strings.Contains(err.Error(), "secret") {
		t.Fatalf("expired non-timeout classification = %v", err)
	}
}

func TestExplainerHasTwoIsolatedRequestLanes(t *testing.T) {
	connection := &blockingExplainConnection{
		entered: make(chan struct{}, 3),
		release: make(chan struct{}),
	}
	explainer := mustExplainer(t, connection)
	validQuery := sealedExplainQuery(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	results := make(chan error, 3)
	var wait sync.WaitGroup
	wait.Add(3)
	for range 3 {
		go func() {
			defer wait.Done()
			_, err := explainer.Explain(ctx, validQuery)
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-connection.entered:
		case <-ctx.Done():
			t.Fatal("two EXPLAIN requests did not enter the independent gate")
		}
	}
	select {
	case <-connection.entered:
		t.Fatal("third EXPLAIN entered while two requests held the gate")
	case <-time.After(75 * time.Millisecond):
	}
	close(connection.release)
	wait.Wait()
	close(results)
	var successful, capacityRejected int
	for err := range results {
		switch {
		case err == nil:
			successful++
		case errors.Is(err, searchjobs.ErrCapacity):
			capacityRejected++
		default:
			t.Errorf("Explain() error = %v", err)
		}
	}
	if successful != maximumConcurrentExplains || capacityRejected != 1 {
		t.Fatalf(
			"EXPLAIN results: successful=%d capacity=%d",
			successful,
			capacityRejected,
		)
	}
	if got := connection.maximum.Load(); got != maximumConcurrentExplains {
		t.Fatalf("maximum concurrent EXPLAIN queries = %d, want %d", got, maximumConcurrentExplains)
	}
}

func TestExplainerCloseCancelsJoinsAndClosesEveryLane(t *testing.T) {
	connection := &closeWaitingExplainConnection{
		entered: make(chan struct{}),
	}
	explainer := mustExplainer(t, connection)
	var closed atomic.Int32
	for _, lane := range explainer.allLanes {
		lane.close = func() error {
			closed.Add(1)
			return nil
		}
	}
	query := sealedExplainQuery(t)

	explainResult := make(chan error, 1)
	go func() {
		_, err := explainer.Explain(context.Background(), query)
		explainResult <- err
	}()
	select {
	case <-connection.entered:
	case <-time.After(time.Second):
		t.Fatal("Explain() did not enter the active transport")
	}

	closeResults := make(chan error, 3)
	for range 3 {
		go func() { closeResults <- explainer.Close() }()
	}
	for range 3 {
		select {
		case err := <-closeResults:
			if err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close() did not join the active Explain call")
		}
	}
	select {
	case err := <-explainResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Explain() error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active Explain call did not return")
	}
	if got := closed.Load(); got != maximumConcurrentExplains {
		t.Fatalf(
			"closed lanes = %d, want %d",
			got,
			maximumConcurrentExplains,
		)
	}

	got, err := explainer.Explain(
		context.Background(),
		sealedExplainQuery(t),
	)
	if err == nil || got != (ExplainResult{}) || connection.calls.Load() != 1 {
		t.Fatalf(
			"Explain() after Close = (%#v, %v), transport calls=%d",
			got,
			err,
			connection.calls.Load(),
		)
	}
}

func TestExplainerCloseSanitizesLaneFailuresAndRejectsNilReceiver(t *testing.T) {
	explainer := mustExplainer(
		t,
		&fakeQueryConnection{rows: explainStructuredRows()},
	)
	const secret = "private close detail"
	for _, lane := range explainer.allLanes {
		lane.close = func() error { return errors.New(secret) }
	}
	err := explainer.Close()
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("Close() error = %v", err)
	}
	if second := explainer.Close(); second == nil || second.Error() != err.Error() {
		t.Fatalf("second Close() error = %v, want stable %v", second, err)
	}

	var nilExplainer *Explainer
	if err := nilExplainer.Close(); err == nil {
		t.Fatal("nil Explainer.Close() unexpectedly succeeded")
	}
}

func TestExplainerClosedStateWinsOverFullLaneCapacity(t *testing.T) {
	t.Parallel()

	explainer := mustExplainer(
		t,
		&fakeQueryConnection{rows: explainStructuredRows()},
	)
	firstLane := <-explainer.lanes
	secondLane := <-explainer.lanes
	defer func() {
		explainer.lanes <- firstLane
		explainer.lanes <- secondLane
	}()
	if err := explainer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	got, err := explainer.Explain(
		context.Background(),
		sealedExplainQuery(t),
	)
	if !errors.Is(err, errExplainerClosed) ||
		errors.Is(err, searchjobs.ErrCapacity) ||
		got != (ExplainResult{}) {
		t.Fatalf("Explain(closed full lanes) = (%#v, %v)", got, err)
	}
}

func TestRandomExplainQueryIDIsBoundedDistinctAndDiagnostic(t *testing.T) {
	t.Parallel()

	seen := make(map[string]struct{}, 128)
	for range 128 {
		queryID, err := randomExplainQueryID()
		if err != nil {
			t.Fatal(err)
		}
		if !validExplainQueryID(queryID) ||
			!strings.HasPrefix(queryID, explainQueryIDPrefix) {
			t.Fatalf("invalid EXPLAIN query ID %q", queryID)
		}
		if _, duplicate := seen[queryID]; duplicate {
			t.Fatalf("duplicate EXPLAIN query ID %q", queryID)
		}
		seen[queryID] = struct{}{}
	}
}

func mustExplainer(t *testing.T, connection queryConnection) *Explainer {
	t.Helper()
	baseSettings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	settings, err := settingsForExplain(mustValidatedSettings(t, baseSettings))
	if err != nil {
		t.Fatal(err)
	}
	lanes := make(chan *explainLane, maximumConcurrentExplains)
	allLanes := make([]*explainLane, 0, maximumConcurrentExplains)
	for range maximumConcurrentExplains {
		lane := &explainLane{
			connection: connection,
			activateContext: func(context.Context) (func() error, error) {
				return func() error { return nil }, nil
			},
			discard: func() error { return nil },
		}
		lanes <- lane
		allLanes = append(allLanes, lane)
	}
	return &Explainer{
		settings:         settings,
		executionTimeout: maximumExplainExecutionTime,
		lanes:            lanes,
		allLanes:         allLanes,
		newQueryID:       func() (string, error) { return "open-splunk-explain-test", nil },
	}
}

func sealedExplainQuery(t *testing.T) clickhouse.CompiledQuery {
	t.Helper()
	return sealedExplainQueryFromSPL(t, `index=main "needle" | head 1`)
}

func sealedExplainQueryFromSPL(
	t *testing.T,
	source string,
) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	searchStart := time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC)
	visibility := uint64(7)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		Earliest:          searchStart.Add(-time.Hour),
		Latest:            searchStart,
		SearchStart:       searchStart,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   searchStart,
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.HasValidSQLSeal() {
		t.Fatal("test compiler returned an unsealed query")
	}
	return compiled
}

func explainFakeRows(lines ...string) *fakeRows {
	data := make([][]any, len(lines))
	for index, line := range lines {
		data[index] = []any{line}
	}
	return &fakeRows{
		columns: []string{"explain"},
		types: []driver.ColumnType{fakeColumnType{
			name: "explain", databaseType: "String", scanType: reflect.TypeFor[string](),
		}},
		data: data,
	}
}

func explainStructuredRows() *fakeRows {
	return explainFakeRows(validStructuredExplainPlan)
}

type explainTestDeadlineContext struct {
	context.Context

	deadline time.Time
}

func (ctx explainTestDeadlineContext) Deadline() (time.Time, bool) {
	return ctx.deadline, true
}

type explainTestTimeoutError struct {
	detail string
}

func (err explainTestTimeoutError) Error() string {
	return err.detail
}

func (explainTestTimeoutError) Timeout() bool {
	return true
}

func (explainTestTimeoutError) Temporary() bool {
	return true
}

type controlledExplainRows struct {
	*fakeRows
	beforeScan func()
	scanErr    error
}

type explainNamedString string

type panicExplainFormatter struct{}

func (panicExplainFormatter) Format(fmt.State, rune) {
	panic("formatter must never run")
}

type panicExplainStringer struct{}

func (panicExplainStringer) String() string {
	panic("String must never run")
}

type panicExplainValuer struct{}

func (panicExplainValuer) Value() (sqldriver.Value, error) {
	panic("Value must never run")
}

func (rows *controlledExplainRows) Scan(destinations ...any) error {
	if rows.beforeScan != nil {
		rows.beforeScan()
	}
	if rows.scanErr != nil {
		return rows.scanErr
	}
	return rows.fakeRows.Scan(destinations...)
}

type deadlineExplainConnection struct {
	rows        driver.Rows
	context     context.Context
	deadline    time.Time
	hasDeadline bool
	query       string
}

func (connection *deadlineExplainConnection) Query(
	ctx context.Context,
	query string,
	_ ...any,
) (driver.Rows, error) {
	connection.deadline, connection.hasDeadline = ctx.Deadline()
	connection.context = ctx
	connection.query = query
	return connection.rows, nil
}

type cancelWaitingExplainConnection struct {
	hasDeadline bool
	observedErr error
}

func (connection *cancelWaitingExplainConnection) Query(
	ctx context.Context,
	_ string,
	_ ...any,
) (driver.Rows, error) {
	_, connection.hasDeadline = ctx.Deadline()
	<-ctx.Done()
	connection.observedErr = ctx.Err()
	return nil, fmt.Errorf("private driver detail: %w", ctx.Err())
}

type blockingExplainConnection struct {
	entered chan struct{}
	release chan struct{}
	current atomic.Int32
	maximum atomic.Int32
}

func (connection *blockingExplainConnection) Query(
	ctx context.Context,
	_ string,
	_ ...any,
) (driver.Rows, error) {
	current := connection.current.Add(1)
	for {
		maximum := connection.maximum.Load()
		if current <= maximum || connection.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	select {
	case connection.entered <- struct{}{}:
	case <-ctx.Done():
		connection.current.Add(-1)
		return nil, ctx.Err()
	}
	select {
	case <-connection.release:
	case <-ctx.Done():
		connection.current.Add(-1)
		return nil, ctx.Err()
	}
	connection.current.Add(-1)
	return explainStructuredRows(), nil
}

type closeWaitingExplainConnection struct {
	entered chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (connection *closeWaitingExplainConnection) Query(
	ctx context.Context,
	_ string,
	_ ...any,
) (driver.Rows, error) {
	connection.calls.Add(1)
	connection.once.Do(func() { close(connection.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestExplainTestFixturesImplementDriverContracts(t *testing.T) {
	t.Parallel()

	var _ driver.Rows = (*controlledExplainRows)(nil)
	var _ queryConnection = (*deadlineExplainConnection)(nil)
	var _ queryConnection = (*cancelWaitingExplainConnection)(nil)
	var _ queryConnection = (*blockingExplainConnection)(nil)
	var _ queryConnection = (*closeWaitingExplainConnection)(nil)
	if !slices.Equal(explainFakeRows("x").columns, []string{"explain"}) {
		t.Fatal("invalid EXPLAIN fixture")
	}
}
