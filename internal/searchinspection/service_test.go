package searchinspection

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

// Keep compiler selection a concrete production boundary. If Config.Compiler
// regresses to an interface, this declaration intentionally stops compiling.
var _ clickhouse.Compiler = Config{}.Compiler

func TestInspectBuildsProjectsCompilesOnceAndExplainsExactQuery(t *testing.T) {
	snapshot := validInspectionSnapshot()
	logicalFixture, fixtureErr := searchsnapshot.BuildExecutionPlan(snapshot)
	if fixtureErr != nil {
		t.Fatalf("build fixture: %v", fixtureErr)
	}
	if _, fixtureErr = projectLogicalPlan(
		context.Background(),
		logicalFixture,
		snapshot.SPL,
	); fixtureErr != nil {
		t.Fatalf("project fixture: %v", fixtureErr)
	}
	compiledFixture, fixtureErr := (clickhouse.Compiler{}).Compile(logicalFixture)
	if fixtureErr != nil {
		t.Fatalf("compile fixture: %v", fixtureErr)
	}
	if !validGeneratedSQL(compiledFixture) {
		t.Fatal("compiled fixture failed SQL validation")
	}
	searches := &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}}
	compiler := &inspectionCompiler{}
	explainer := &inspectionExplainer{
		result: inspectionExplainResult("open-splunk-explain-service-test"),
	}
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches: searches, Compiler: compiler, Explainer: explainer,
	})

	result, err := service.Inspect(
		context.Background(),
		searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID},
		Request{SearchJobID: snapshot.ID},
	)
	if err != nil {
		t.Fatalf(
			"Inspect() error = %v; calls snapshots=%d compiler=%d explainer=%d",
			err,
			searches.callCount(),
			compiler.callCount(),
			explainer.callCount(),
		)
	}
	if searches.callCount() != 2 {
		t.Fatalf("snapshot calls = %d, want preflight and postflight", searches.callCount())
	}
	lookupAccesses, lookupIDs := searches.lookupCalls()
	wantAccess := searchjobs.AccessScope{
		TenantID: snapshot.TenantID,
		OwnerID:  snapshot.OwnerID,
	}
	if !reflect.DeepEqual(
		lookupAccesses,
		[]searchjobs.AccessScope{wantAccess, wantAccess},
	) || !slices.Equal(lookupIDs, []string{snapshot.ID, snapshot.ID}) {
		t.Fatalf(
			"snapshot lookup scope/IDs = %#v/%#v, want exact request twice",
			lookupAccesses,
			lookupIDs,
		)
	}
	if compiler.callCount() != 1 || explainer.callCount() != 1 {
		t.Fatalf("compile/explain calls = %d/%d, want 1/1", compiler.callCount(), explainer.callCount())
	}
	compiled := compiler.lastQuery()
	explained := explainer.lastQuery()
	if !compiled.HasValidExecutionSeal() || !explained.HasValidExecutionSeal() ||
		!explained.EqualForExecution(compiled) {
		t.Fatalf("Explainer input changed:\ncompiled=%#v\nexplained=%#v", compiled, explained)
	}
	if len(compiled.Args) == 0 {
		t.Fatal("fixture compiled without bound arguments")
	}
	if &compiled.Args[0] == &explained.Args[0] {
		t.Fatal("service passed the compiler-owned argument slice to Explainer")
	}
	if result.GeneratedSQL != compiled.SQL ||
		result.ExplainText != explainer.result.Text ||
		result.DiagnosticQueryID != explainer.result.QueryID {
		t.Fatal("inspection result did not preserve compiler and Explainer diagnostics")
	}
	if result.KnowledgeSnapshot != nil {
		t.Fatalf("legacy inspection invented knowledge authority: %#v", result.KnowledgeSnapshot)
	}
	if !strings.Contains(
		result.ExplainText,
		"sensitive-literal-7f2c",
	) {
		t.Fatal("raw EXPLAIN fixture did not exercise literal-bearing metadata")
	}
	if !slices.Equal(
		result.PhysicalPlan.NodeTypes,
		[]string{"Expression", "ReadFromMergeTree"},
	) || len(result.PhysicalPlan.Reads) != 1 {
		t.Fatalf("physical plan = %#v", result.PhysicalPlan)
	}
	if got := stageOperators(result.Plan); !slices.Equal(
		got,
		[]string{"Scan", "Filter", "Aggregate", "Sort", "Limit"},
	) {
		t.Fatalf("operators = %v", got)
	}
	if result.Plan.Output.Kind != OutputKindStatic ||
		!slices.Equal(result.Plan.Output.Fields, []string{"host", "events"}) {
		t.Fatalf("logical output = %#v", result.Plan.Output)
	}
	for _, stage := range result.Plan.Stages {
		if stage.SourceRange.End.ByteOffset > uint64(len(snapshot.SPL)) {
			t.Fatalf("stage range exceeds source: %#v", stage)
		}
	}

	rendered := fmt.Sprintf(
		"%#v %#v %s",
		result.Plan,
		result.PhysicalPlan,
		result.GeneratedSQL,
	)
	for _, secret := range []string{
		snapshot.TenantID,
		snapshot.OwnerID,
		snapshot.EffectiveIndexes[0],
		"sensitive-literal-7f2c",
	} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("inspection result leaked compiler argument %q", secret)
		}
	}
	result.Plan.Stages[0].Operator = "mutated"
	result.Plan.Stages[1].InputFields[0] = "mutated"
	result.Plan.Stages[2].OutputFields[0] = "mutated"
	result.Plan.Output.Fields[0] = "mutated"
	result.Plan.ReferencedFields[0] = "mutated"
	result.PhysicalPlan.NodeTypes[0] = "mutated"
	result.PhysicalPlan.Reads[0].Columns[0] = "mutated"
	result.PhysicalPlan.Reads[0].Indexes[0].Keys[0] = "mutated"
	again, err := service.Inspect(
		context.Background(),
		searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID},
		Request{SearchJobID: snapshot.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if again.Plan.Stages[0].Operator != "Scan" ||
		again.Plan.Stages[1].InputFields[0] != "index" ||
		again.Plan.Stages[2].OutputFields[0] != "events" ||
		again.Plan.Output.Fields[0] != "host" ||
		again.Plan.ReferencedFields[0] == "mutated" ||
		again.PhysicalPlan.NodeTypes[0] != "Expression" ||
		again.PhysicalPlan.Reads[0].Columns[0] != "event_time" ||
		again.PhysicalPlan.Reads[0].Indexes[0].Keys[0] != "event_time" {
		t.Fatalf(
			"result aliases retained state: logical=%#v physical=%#v",
			again.Plan,
			again.PhysicalPlan,
		)
	}
}

func TestInspectRejectsGenuineSealedCompilerScopeSubstitutionBeforeExplain(t *testing.T) {
	base := validInspectionSnapshot()
	for _, test := range []struct {
		name   string
		change func(*searchjobs.ExecutionSnapshot)
	}{
		{name: "tenant", change: func(value *searchjobs.ExecutionSnapshot) { value.TenantID = "other-tenant" }},
		{name: "index", change: func(value *searchjobs.ExecutionSnapshot) {
			value.SPL = strings.ReplaceAll(value.SPL, "sensitive-index-7f2c", "other-index")
			value.EffectiveIndexes = []string{"other-index"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			alternate := base
			alternate.EffectiveIndexes = slices.Clone(base.EffectiveIndexes)
			test.change(&alternate)
			logical, err := searchsnapshot.BuildExecutionPlan(alternate)
			if err != nil {
				t.Fatalf("BuildExecutionPlan(alternate): %v", err)
			}
			compiled, err := (clickhouse.Compiler{}).Compile(logical)
			if err != nil || !compiled.HasValidExecutionSeal() {
				t.Fatalf("Compile(alternate) = valid:%t err:%v", compiled.HasValidExecutionSeal(), err)
			}
			searches := &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{base, base}}
			explainer := &inspectionExplainer{result: inspectionExplainResult("scope-substitution")}
			service := newInspectionTestService(t, inspectionTestConfig{
				Searches:  searches,
				Compiler:  &inspectionCompiler{override: &compiled},
				Explainer: explainer,
			})
			result, err := service.Inspect(
				context.Background(),
				searchjobs.AccessScope{TenantID: base.TenantID, OwnerID: base.OwnerID},
				Request{SearchJobID: base.ID},
			)
			if !errors.Is(err, ErrInspectionFailed) || !reflect.DeepEqual(result, Result{}) {
				t.Fatalf("Inspect(scope substitution) = (%#v, %v)", result, err)
			}
			if explainer.callCount() != 0 {
				t.Fatalf("scope-substituted query reached Explainer %d times", explainer.callCount())
			}
		})
	}
}

func TestInspectRejectsInvalidRequestsBeforeDependencyWork(t *testing.T) {
	snapshot := validInspectionSnapshot()
	validAccess := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	tests := []struct {
		name    string
		access  searchjobs.AccessScope
		request Request
	}{
		{name: "empty ID", access: validAccess},
		{name: "padded ID", access: validAccess, request: Request{SearchJobID: " " + snapshot.ID}},
		{name: "oversized ID", access: validAccess, request: Request{SearchJobID: strings.Repeat("j", searchjobs.MaximumJobIDBytes+1)}},
		{name: "invalid UTF-8 ID", access: validAccess, request: Request{SearchJobID: string([]byte{0xff})}},
		{name: "control ID", access: validAccess, request: Request{SearchJobID: "job\nid"}},
		{name: "empty tenant", access: searchjobs.AccessScope{OwnerID: snapshot.OwnerID}, request: Request{SearchJobID: snapshot.ID}},
		{name: "padded tenant", access: searchjobs.AccessScope{TenantID: " tenant", OwnerID: snapshot.OwnerID}, request: Request{SearchJobID: snapshot.ID}},
		{name: "empty owner", access: searchjobs.AccessScope{TenantID: snapshot.TenantID}, request: Request{SearchJobID: snapshot.ID}},
		{name: "oversized owner", access: searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: strings.Repeat("o", maximumAccessIdentityBytes+1)}, request: Request{SearchJobID: snapshot.ID}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			searches := &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}}
			compiler := &inspectionCompiler{}
			explainer := &inspectionExplainer{}
			service := newInspectionTestService(t, inspectionTestConfig{
				Searches: searches, Compiler: compiler, Explainer: explainer,
			})
			result, err := service.Inspect(context.Background(), test.access, test.request)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("error = %v, want ErrInvalidRequest", err)
			}
			assertZeroInspection(t, result)
			if searches.callCount()+compiler.callCount()+explainer.callCount() != 0 {
				t.Fatal("invalid request performed dependency work")
			}
		})
	}
}

func TestInspectHonorsCallerCancellationAtEveryBoundary(t *testing.T) {
	snapshot := validInspectionSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	request := Request{SearchJobID: snapshot.ID}

	t.Run("before admission", func(t *testing.T) {
		searches := &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}}
		service := newInspectionTestService(t, inspectionTestConfig{
			Searches: searches, Compiler: &inspectionCompiler{}, Explainer: &inspectionExplainer{},
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := service.Inspect(ctx, access, request)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		assertZeroInspection(t, result)
		if searches.callCount() != 0 {
			t.Fatal("canceled request performed snapshot work")
		}
	})

	t.Run("after compilation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		compiler := &inspectionCompiler{afterCompile: cancel}
		explainer := &inspectionExplainer{}
		service := newInspectionTestService(t, inspectionTestConfig{
			Searches:  &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}},
			Compiler:  compiler,
			Explainer: explainer,
		})
		result, err := service.Inspect(ctx, access, request)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		assertZeroInspection(t, result)
		if explainer.callCount() != 0 {
			t.Fatal("canceled compiled request entered Explainer")
		}
	})

	t.Run("inside explain", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		explainer := &inspectionExplainer{
			started: make(chan struct{}),
			release: make(chan struct{}),
		}
		service := newInspectionTestService(t, inspectionTestConfig{
			Searches:  &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}},
			Compiler:  &inspectionCompiler{},
			Explainer: explainer,
		})
		done := make(chan inspectionCallResult, 1)
		go func() {
			result, err := service.Inspect(ctx, access, request)
			done <- inspectionCallResult{result: result, err: err}
		}()
		<-explainer.started
		cancel()
		call := <-done
		assertZeroInspection(t, call.result)
		if !errors.Is(call.err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", call.err)
		}
	})
}

func TestInspectEnforcesExactServiceRuntime(t *testing.T) {
	snapshot := validInspectionSnapshot()
	explainer := &inspectionExplainer{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches:      &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}},
		Compiler:      &inspectionCompiler{},
		Explainer:     explainer,
		MaxConcurrent: 1,
		MaxRuntime:    100 * time.Millisecond,
	})
	result, err := service.Inspect(
		context.Background(),
		searchjobs.AccessScope{
			TenantID: snapshot.TenantID,
			OwnerID:  snapshot.OwnerID,
		},
		Request{SearchJobID: snapshot.ID},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want DeadlineExceeded", err)
	}
	assertZeroInspection(t, result)
	if explainer.callCount() != 1 {
		t.Fatalf("Explainer calls = %d, want 1", explainer.callCount())
	}
}

func TestInspectAdmissionIsFailFastBeforeSnapshotAndCompilation(t *testing.T) {
	snapshot := validInspectionSnapshot()
	searches := &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}}
	compiler := &inspectionCompiler{}
	explainer := &inspectionExplainer{
		result:  inspectionExplainResult("open-splunk-explain-capacity"),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches: searches, Compiler: compiler, Explainer: explainer, MaxConcurrent: 1,
	})
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	request := Request{SearchJobID: snapshot.ID}
	first := make(chan inspectionCallResult, 1)
	go func() {
		result, err := service.Inspect(
			context.Background(),
			access,
			request,
		)
		first <- inspectionCallResult{result: result, err: err}
	}()
	<-explainer.started

	result, err := service.Inspect(context.Background(), access, request)
	if !errors.Is(err, searchjobs.ErrCapacity) {
		t.Fatalf("second error = %v, want ErrCapacity", err)
	}
	assertZeroInspection(t, result)
	if searches.callCount() != 1 || compiler.callCount() != 1 || explainer.callCount() != 1 {
		t.Fatalf(
			"work after saturated admission = snapshots %d compiler %d explainer %d",
			searches.callCount(), compiler.callCount(), explainer.callCount(),
		)
	}
	close(explainer.release)
	firstCall := <-first
	if firstCall.err != nil {
		t.Fatalf("first inspection failed: %v", firstCall.err)
	}
	if reflect.DeepEqual(firstCall.result, Result{}) {
		t.Fatal("first inspection returned no result")
	}
	if searches.callCount() != 2 {
		t.Fatalf("snapshot calls after postflight = %d, want 2", searches.callCount())
	}
}

func TestInspectUsesMetadataOnlyWhenResultLeaseCapacityIsSaturated(t *testing.T) {
	now := time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: inspectionManagerExecutorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink searchjobs.ResultSink,
		) error {
			if !slices.Equal(query.OutputFields, []string{"message"}) {
				return searchjobs.ErrInvalidResult
			}
			if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
				Name: "message", Kind: searchjobs.ValueKindString,
			}}}); err != nil {
				return err
			}
			return sink.AddRow([]searchjobs.Value{
				searchjobs.StringValue("retained"),
			})
		}),
		Snapshotter: inspectionSnapshotterFunc(func(context.Context) (uint64, error) {
			return 77, nil
		}),
		MaxResultLeases:       1,
		MaxResultLeasesPerJob: 1,
		RetentionTTL:          time.Hour,
		CleanupInterval:       -1,
		Now:                   func() time.Time { return now },
		NewID:                 func() string { return "inspection-manager-job" },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close search manager: %v", err)
		}
	})
	resolvedRange, err := searchtime.NewAbsoluteRange(
		now.Add(-2*time.Hour),
		now.Add(-time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(context.Background(), searchjobs.CreateRequest{
		SPL:               "index=main | table message",
		OwnerID:           "owner",
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         resolvedRange,
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := waitForInspectionJob(t, manager, created.ID)
	if completed.State != searchjobs.StateCompleted {
		t.Fatalf("job state = %v, want completed", completed.State)
	}
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	lease, err := manager.AcquireResultsFor(
		context.Background(),
		access,
		completed.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Close(); err != nil {
			t.Errorf("close result lease: %v", err)
		}
	})
	if second, acquireErr := manager.AcquireResultsFor(
		context.Background(),
		access,
		completed.ID,
	); !errors.Is(acquireErr, searchjobs.ErrCapacity) || second != nil {
		t.Fatalf(
			"second result lease = (%v, %v), want nil/ErrCapacity",
			second,
			acquireErr,
		)
	}

	service := newInspectionTestService(t, inspectionTestConfig{
		Searches: manager,
		Compiler: clickhouse.Compiler{},
		Explainer: &inspectionExplainer{
			result: inspectionExplainResult(
				"open-splunk-explain-metadata-only",
			),
		},
	})
	result, err := service.Inspect(
		context.Background(),
		access,
		Request{SearchJobID: completed.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.GeneratedSQL == "" || len(result.Plan.Stages) == 0 {
		t.Fatalf("inspection result = %#v", result)
	}
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("preexisting lease after inspection = (%#v, %t, %v)", row, ok, err)
	}
	message, ok := row.Values[0].String()
	if !ok || message != "retained" {
		t.Fatalf("leased result after inspection = %#v", row)
	}
}

func TestInspectPostflightPreventsStaleOrChangedPublication(t *testing.T) {
	snapshot := validInspectionSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	request := Request{SearchJobID: snapshot.ID}
	tests := []struct {
		name      string
		second    searchjobs.ExecutionSnapshot
		secondErr error
		want      error
	}{
		{
			name:      "expired during explain",
			second:    snapshot,
			secondErr: fmt.Errorf("%w: storage tombstone detail", searchjobs.ErrExpired),
			want:      searchjobs.ErrExpired,
		},
		{
			name:      "cleaned during explain",
			second:    snapshot,
			secondErr: fmt.Errorf("%w: private identity", searchjobs.ErrNotFound),
			want:      searchjobs.ErrNotFound,
		},
		{
			name: "snapshot changed",
			second: func() searchjobs.ExecutionSnapshot {
				changed := snapshot
				changed.VisibilityCutoff++
				return changed
			}(),
			want: ErrInspectionFailed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newInspectionTestService(t, inspectionTestConfig{
				Searches: &inspectionSearches{
					snapshots: []searchjobs.ExecutionSnapshot{snapshot, test.second},
					errors:    []error{nil, test.secondErr},
				},
				Compiler: &inspectionCompiler{},
				Explainer: &inspectionExplainer{
					result: inspectionExplainResult(
						"open-splunk-explain-postflight",
					),
				},
			})
			result, err := service.Inspect(context.Background(), access, request)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			assertZeroInspection(t, result)
			if err != nil && strings.Contains(err.Error(), "storage tombstone detail") {
				t.Fatalf("error leaked dependency detail: %v", err)
			}
		})
	}
}

func TestInspectRejectsChangedSnapshotIdentityBeforePlanning(t *testing.T) {
	snapshot := validInspectionSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	tests := []struct {
		name   string
		change func(*searchjobs.ExecutionSnapshot)
	}{
		{name: "ID", change: func(value *searchjobs.ExecutionSnapshot) { value.ID += "-changed" }},
		{name: "tenant", change: func(value *searchjobs.ExecutionSnapshot) { value.TenantID += "-changed" }},
		{name: "owner", change: func(value *searchjobs.ExecutionSnapshot) { value.OwnerID += "-changed" }},
		{name: "zero expiry", change: func(value *searchjobs.ExecutionSnapshot) { value.ExpiresAt = time.Time{} }},
		{name: "zero finish", change: func(value *searchjobs.ExecutionSnapshot) { value.FinishedAt = time.Time{} }},
		{name: "expiry before finish", change: func(value *searchjobs.ExecutionSnapshot) { value.ExpiresAt = value.FinishedAt.Add(-time.Second) }},
		{name: "oversized SPL", change: func(value *searchjobs.ExecutionSnapshot) {
			value.SPL = strings.Repeat("x", maximumProjectionSourceBytes+1)
		}},
		{name: "too many indexes", change: func(value *searchjobs.ExecutionSnapshot) {
			value.EffectiveIndexes = make(
				[]string,
				searchjobs.MaximumScopeIndexes+1,
			)
			for index := range value.EffectiveIndexes {
				value.EffectiveIndexes[index] = fmt.Sprintf("index-%03d", index)
			}
		}},
		{name: "invalid index", change: func(value *searchjobs.ExecutionSnapshot) {
			value.EffectiveIndexes = []string{"bad\nindex"}
		}},
		{name: "invalid timezone", change: func(value *searchjobs.ExecutionSnapshot) {
			value.SearchTimezone = " UTC"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := snapshot
			test.change(&changed)
			compiler := &inspectionCompiler{}
			explainer := &inspectionExplainer{}
			service := newInspectionTestService(t, inspectionTestConfig{
				Searches:  &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{changed}},
				Compiler:  compiler,
				Explainer: explainer,
			})
			result, err := service.Inspect(
				context.Background(), access, Request{SearchJobID: snapshot.ID},
			)
			if !errors.Is(err, ErrInspectionFailed) {
				t.Fatalf("error = %v, want ErrInspectionFailed", err)
			}
			assertZeroInspection(t, result)
			if compiler.callCount()+explainer.callCount() != 0 {
				t.Fatal("invalid snapshot reached compiler or Explainer")
			}
		})
	}
}

func TestInspectSanitizesEveryDependencyError(t *testing.T) {
	snapshot := validInspectionSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	request := Request{SearchJobID: snapshot.ID}
	const secret = "SELECT private_password FROM internal_host"
	safeErrors := []error{
		searchjobs.ErrNotFound,
		searchjobs.ErrExpired,
		searchjobs.ErrResultsNotReady,
		searchjobs.ErrResultsUnavailable,
		searchjobs.ErrCapacity,
		searchjobs.ErrClosed,
		searchjobs.ErrExecutionLimit,
		searchjobs.ErrStorageUnavailable,
		searchjobs.ErrUnsupportedValue,
		searchjobs.ErrInvalidResult,
	}
	for _, dependency := range []string{"snapshot", "compiler", "explainer"} {
		for _, safe := range safeErrors {
			t.Run(dependency+"/"+safe.Error(), func(t *testing.T) {
				searches := &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}}
				compiler := &inspectionCompiler{}
				explainer := &inspectionExplainer{
					result: inspectionExplainResult(
						"open-splunk-explain-error",
					),
				}
				wrapped := fmt.Errorf("%s: %w", secret, safe)
				switch dependency {
				case "snapshot":
					searches.errors = []error{wrapped}
				case "compiler":
					compiler.err = wrapped
				case "explainer":
					explainer.err = wrapped
				}
				service := newInspectionTestService(t, inspectionTestConfig{
					Searches: searches, Compiler: compiler, Explainer: explainer,
				})
				result, err := service.Inspect(context.Background(), access, request)
				if !errors.Is(err, safe) {
					t.Fatalf("error = %v, want exact %v", err, safe)
				}
				assertZeroInspection(t, result)
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error leaked dependency detail: %v", err)
				}
			})
		}
	}

	for _, dependency := range []string{"snapshot", "compiler", "explainer"} {
		for _, unknown := range []error{
			context.Canceled,
			context.DeadlineExceeded,
			errors.New(secret),
		} {
			t.Run(dependency+"/unknown/"+unknown.Error(), func(t *testing.T) {
				searches := &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}}
				compiler := &inspectionCompiler{}
				explainer := &inspectionExplainer{
					result: inspectionExplainResult(
						"open-splunk-explain-unknown",
					),
				}
				switch dependency {
				case "snapshot":
					searches.errors = []error{unknown}
				case "compiler":
					compiler.err = unknown
				case "explainer":
					explainer.err = unknown
				}
				service := newInspectionTestService(t, inspectionTestConfig{
					Searches: searches, Compiler: compiler, Explainer: explainer,
				})
				result, err := service.Inspect(context.Background(), access, request)
				if !errors.Is(err, ErrInspectionFailed) {
					t.Fatalf("error = %v, want ErrInspectionFailed", err)
				}
				assertZeroInspection(t, result)
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error leaked dependency detail: %v", err)
				}
			})
		}
	}
}

func TestInspectClassifiesComplexityAndRejectsUnsealedCompilerOutput(t *testing.T) {
	snapshot := validInspectionSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	request := Request{SearchJobID: snapshot.ID}

	t.Run("complexity", func(t *testing.T) {
		service := newInspectionTestService(t, inspectionTestConfig{
			Searches: &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}},
			Compiler: &inspectionCompiler{err: &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX", Message: "contains sensitive source",
			}},
			Explainer: &inspectionExplainer{},
		})
		result, err := service.Inspect(context.Background(), access, request)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) {
			t.Fatalf("error = %v, want ErrExecutionLimit", err)
		}
		assertZeroInspection(t, result)
	})

	t.Run("unsealed", func(t *testing.T) {
		explainer := &inspectionExplainer{}
		service := newInspectionTestService(t, inspectionTestConfig{
			Searches: &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}},
			Compiler: &inspectionCompiler{override: &clickhouse.CompiledQuery{
				SQL: "SELECT private_password FROM events",
			}},
			Explainer: explainer,
		})
		result, err := service.Inspect(context.Background(), access, request)
		if !errors.Is(err, ErrInspectionFailed) {
			t.Fatalf("error = %v, want ErrInspectionFailed", err)
		}
		assertZeroInspection(t, result)
		if explainer.callCount() != 0 {
			t.Fatal("unsealed SQL reached Explainer")
		}
	})
}

func TestInspectRevalidatesExplainOutputBeforeAtomicPublication(t *testing.T) {
	snapshot := validInspectionSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	tests := []struct {
		name   string
		result queryexec.ExplainResult
	}{
		{name: "empty text", result: queryexec.ExplainResult{QueryID: "open-splunk-explain-valid"}},
		{name: "oversized text", result: queryexec.ExplainResult{
			Text: strings.Repeat("x", (2<<20)+1), QueryID: "open-splunk-explain-valid",
		}},
		{name: "invalid text UTF-8", result: queryexec.ExplainResult{
			Text: string([]byte{0xff}), QueryID: "open-splunk-explain-valid",
		}},
		{name: "text control", result: queryexec.ExplainResult{
			Text: "line\x00secret", QueryID: "open-splunk-explain-valid",
		}},
		{name: "empty query ID", result: queryexec.ExplainResult{Text: "plan"}},
		{name: "wrong query ID prefix", result: queryexec.ExplainResult{
			Text: "plan", QueryID: "other-query-id",
		}},
		{name: "query ID control", result: queryexec.ExplainResult{
			Text: "plan", QueryID: "open-splunk-explain-bad\nid",
		}},
		{name: "oversized query ID", result: queryexec.ExplainResult{
			Text: "plan", QueryID: "open-splunk-explain-" + strings.Repeat("x", 256),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newInspectionTestService(t, inspectionTestConfig{
				Searches:  &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}},
				Compiler:  &inspectionCompiler{},
				Explainer: &inspectionExplainer{result: test.result},
			})
			result, err := service.Inspect(
				context.Background(), access, Request{SearchJobID: snapshot.ID},
			)
			if !errors.Is(err, ErrInspectionFailed) {
				t.Fatalf("error = %v, want ErrInspectionFailed", err)
			}
			assertZeroInspection(t, result)
		})
	}
}

func TestInspectCloseCancelsAndJoinsWithoutClosingSharedDependencies(t *testing.T) {
	snapshot := validInspectionSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	explainer := &inspectionExplainer{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches:  &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}},
		Compiler:  &inspectionCompiler{},
		Explainer: explainer,
	})
	inspectionDone := make(chan inspectionCallResult, 1)
	go func() {
		result, err := service.Inspect(
			context.Background(), access, Request{SearchJobID: snapshot.ID},
		)
		inspectionDone <- inspectionCallResult{result: result, err: err}
	}()
	<-explainer.started

	const closeCalls = 8
	closeErrors := make(chan error, closeCalls)
	for range closeCalls {
		go func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			closeErrors <- service.Close(closeCtx)
		}()
	}
	for range closeCalls {
		if err := <-closeErrors; err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}
	activeCall := <-inspectionDone
	assertZeroInspection(t, activeCall.result)
	if !errors.Is(activeCall.err, searchjobs.ErrClosed) {
		t.Fatalf(
			"active inspection error = %v, want ErrClosed",
			activeCall.err,
		)
	}
	result, err := service.Inspect(
		context.Background(), access, Request{SearchJobID: snapshot.ID},
	)
	if !errors.Is(err, searchjobs.ErrClosed) {
		t.Fatalf("post-close error = %v, want ErrClosed", err)
	}
	assertZeroInspection(t, result)
}

func TestCloseSynchronouslyCancelsAdmittedOperationBeforeDependency(t *testing.T) {
	snapshot := validInspectionSnapshot()
	searches := &inspectionSearches{
		snapshots: []searchjobs.ExecutionSnapshot{snapshot},
	}
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches:  searches,
		Compiler:  &inspectionCompiler{},
		Explainer: &inspectionExplainer{},
	})
	token, operationContext, cancelOperation, err :=
		service.beginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	operationEnded := false
	defer func() {
		if operationEnded {
			return
		}
		cancelOperation()
		service.unregisterOperation(token)
		service.releaseOperation()
	}()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- service.Close(context.Background())
	}()
	select {
	case <-operationContext.Done():
	case <-time.After(time.Second):
		t.Fatal("Close did not synchronously cancel the admitted operation")
	}
	if !errors.Is(operationContext.Err(), context.Canceled) {
		t.Fatalf(
			"operation context error = %v, want context.Canceled",
			operationContext.Err(),
		)
	}
	_, snapshotErr := service.completedSnapshotFor(
		operationContext,
		searchjobs.AccessScope{
			TenantID: snapshot.TenantID,
			OwnerID:  snapshot.OwnerID,
		},
		snapshot.ID,
	)
	if !errors.Is(snapshotErr, context.Canceled) {
		t.Fatalf(
			"post-shutdown snapshot guard error = %v, want context.Canceled",
			snapshotErr,
		)
	}
	if searches.callCount() != 0 {
		t.Fatalf(
			"snapshot calls after shutdown = %d, want 0",
			searches.callCount(),
		)
	}

	closed := service.unregisterOperation(token)
	if !closed {
		t.Fatal("operation unregistered without observing shutdown")
	}
	if completionErr := finalInspectionError(
		context.Background(),
		operationContext,
		closed,
		nil,
	); !errors.Is(completionErr, searchjobs.ErrClosed) {
		t.Fatalf(
			"completion error = %v, want ErrClosed",
			completionErr,
		)
	}
	cancelOperation()
	service.releaseOperation()
	operationEnded = true
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestInspectCloseCanTimeOutAndRetry(t *testing.T) {
	snapshot := validInspectionSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	explainer := &inspectionExplainer{
		result:        inspectionExplainResult("open-splunk-explain-close-retry"),
		started:       make(chan struct{}),
		release:       make(chan struct{}),
		ignoreContext: true,
	}
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches:  &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}},
		Compiler:  &inspectionCompiler{},
		Explainer: explainer,
	})
	inspectionDone := make(chan inspectionCallResult, 1)
	go func() {
		result, err := service.Inspect(
			context.Background(), access, Request{SearchJobID: snapshot.ID},
		)
		inspectionDone <- inspectionCallResult{result: result, err: err}
	}()
	<-explainer.started
	closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelClose()
	if err := service.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close() error = %v, want DeadlineExceeded", err)
	}
	close(explainer.release)
	activeCall := <-inspectionDone
	assertZeroInspection(t, activeCall.result)
	if !errors.Is(activeCall.err, searchjobs.ErrClosed) {
		t.Fatalf("inspection error = %v, want ErrClosed", activeCall.err)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatalf("retry Close() error = %v", err)
	}
}

func TestNewRejectsInvalidDependenciesAndBounds(t *testing.T) {
	snapshot := validInspectionSnapshot()
	valid := Config{
		Searches:  &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}},
		Compiler:  clickhouse.Compiler{},
		Explainer: &inspectionExplainer{},
	}
	var nilSearches *inspectionSearches
	var nilExplainer *inspectionExplainer
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "nil searches", mutate: func(config *Config) { config.Searches = nil }},
		{name: "typed nil searches", mutate: func(config *Config) { config.Searches = nilSearches }},
		{name: "nil Explainer", mutate: func(config *Config) { config.Explainer = nil }},
		{name: "typed nil Explainer", mutate: func(config *Config) { config.Explainer = nilExplainer }},
		{name: "negative concurrency", mutate: func(config *Config) { config.MaxConcurrent = -1 }},
		{name: "excess concurrency", mutate: func(config *Config) { config.MaxConcurrent = maximumConcurrentInspections + 1 }},
		{name: "negative runtime", mutate: func(config *Config) { config.MaxRuntime = -1 }},
		{name: "excess runtime", mutate: func(config *Config) { config.MaxRuntime = maximumInspectionRuntime + time.Nanosecond }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.mutate(&config)
			if service, err := New(config); err == nil || service != nil {
				t.Fatalf("New() = (%#v, %v), want nil error", service, err)
			}
		})
	}
	service, err := New(valid)
	if err != nil {
		t.Fatal(err)
	}
	if cap(service.gate) != defaultConcurrentInspections ||
		service.maxRuntime != defaultInspectionRuntime {
		t.Fatalf("default bounds = concurrency %d runtime %s", cap(service.gate), service.maxRuntime)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestInternalConstructorRejectsNilCompiler(t *testing.T) {
	snapshot := validInspectionSnapshot()
	searches := &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}}
	explainer := &inspectionExplainer{}
	var typedNil *inspectionCompiler

	for name, compiler := range map[string]queryCompiler{
		"nil":       nil,
		"typed nil": typedNil,
	} {
		t.Run(name, func(t *testing.T) {
			service, err := newService(searches, compiler, explainer, 0, 0)
			if err == nil || service != nil {
				t.Fatalf("newService() = (%#v, %v), want nil error", service, err)
			}
		})
	}
}

func TestPublicConfigRejectsSameScopeCompilerSubstitutionByType(t *testing.T) {
	field, ok := reflect.TypeOf(Config{}).FieldByName("Compiler")
	trustedType := reflect.TypeOf(clickhouse.Compiler{})
	if !ok || field.Type != trustedType {
		t.Fatalf(
			"Config.Compiler type = %v, want exact %v",
			field.Type,
			trustedType,
		)
	}

	base := validInspectionSnapshot()
	alternate := base
	alternate.SPL = strings.Replace(
		base.SPL,
		`status="sensitive-literal-7f2c"`,
		`status="same-scope-substitution"`,
		1,
	)
	if alternate.SPL == base.SPL {
		t.Fatal("same-scope substitution fixture did not change the logical source")
	}
	logical, err := searchsnapshot.BuildExecutionPlan(alternate)
	if err != nil {
		t.Fatalf("BuildExecutionPlan(same-scope alternate): %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil || !compiled.HasValidExecutionSeal() {
		t.Fatalf(
			"Compile(same-scope alternate) = valid:%t err:%v",
			compiled.HasValidExecutionSeal(),
			err,
		)
	}
	compiledTenant, compiledIndexes, ok := compiled.ReadScope()
	if !ok || compiledTenant != base.TenantID ||
		!slices.Equal(compiledIndexes, base.EffectiveIndexes) {
		t.Fatalf(
			"same-scope alternate read scope = (%q, %v, %t), want (%q, %v, true)",
			compiledTenant,
			compiledIndexes,
			ok,
			base.TenantID,
			base.EffectiveIndexes,
		)
	}
	fake := &sameScopeSubstitutionCompiler{compiled: compiled}
	fakeType := reflect.TypeOf(fake)
	compilerInterface := reflect.TypeOf((*queryCompiler)(nil)).Elem()
	if !fakeType.Implements(compilerInterface) {
		t.Fatalf("same-scope fake %v does not implement %v", fakeType, compilerInterface)
	}
	if fakeType.AssignableTo(field.Type) || fakeType.ConvertibleTo(field.Type) {
		t.Fatalf(
			"same-scope compiler fake %v can enter public Config.Compiler %v",
			fakeType,
			field.Type,
		)
	}
}

func TestInspectNilReceiverAndContext(t *testing.T) {
	var service *Service
	result, err := service.Inspect(
		context.Background(),
		searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
		Request{SearchJobID: "job"},
	)
	if err == nil {
		t.Fatal("nil service succeeded")
	}
	assertZeroInspection(t, result)

	snapshot := validInspectionSnapshot()
	service = newInspectionTestService(t, inspectionTestConfig{
		Searches:  &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{snapshot}},
		Compiler:  &inspectionCompiler{},
		Explainer: &inspectionExplainer{},
	})
	//nolint:staticcheck // This case explicitly verifies the nil-context guard.
	result, err = service.Inspect(
		nil,
		searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID},
		Request{SearchJobID: snapshot.ID},
	)
	if err == nil {
		t.Fatal("nil context succeeded")
	}
	assertZeroInspection(t, result)
	//nolint:staticcheck // This case explicitly verifies the nil-context guard.
	if err := service.Close(nil); err == nil {
		t.Fatal("Close(nil) succeeded")
	}
}

type inspectionSearches struct {
	mu        sync.Mutex
	snapshots []searchjobs.ExecutionSnapshot
	errors    []error
	calls     int
	accesses  []searchjobs.AccessScope
	ids       []string
}

type inspectionCallResult struct {
	result Result
	err    error
}

type inspectionManagerExecutorFunc func(
	context.Context,
	clickhouse.CompiledQuery,
	searchjobs.ResultSink,
) error

func (execute inspectionManagerExecutorFunc) Execute(
	ctx context.Context,
	query clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	return execute(ctx, query, sink)
}

type inspectionSnapshotterFunc func(context.Context) (uint64, error)

func (snapshot inspectionSnapshotterFunc) VisibilityCutoff(
	ctx context.Context,
) (uint64, error) {
	return snapshot(ctx)
}

func (searches *inspectionSearches) CompletedExecutionSnapshotFor(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (searchjobs.ExecutionSnapshot, error) {
	return searches.nextExecutionSnapshot(ctx, access, id)
}

func (searches *inspectionSearches) AcquireExecutionFor(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (searchjobs.ResultLease, searchjobs.ExecutionSnapshot, error) {
	snapshot, err := searches.nextExecutionSnapshot(ctx, access, id)
	if err != nil {
		return nil, searchjobs.ExecutionSnapshot{}, err
	}
	return inspectionResultLease{}, snapshot, nil
}

func (searches *inspectionSearches) nextExecutionSnapshot(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (searchjobs.ExecutionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.ExecutionSnapshot{}, err
	}
	searches.mu.Lock()
	defer searches.mu.Unlock()
	index := searches.calls
	searches.calls++
	searches.accesses = append(searches.accesses, access)
	searches.ids = append(searches.ids, id)
	if index < len(searches.errors) && searches.errors[index] != nil {
		return searchjobs.ExecutionSnapshot{}, searches.errors[index]
	}
	if len(searches.snapshots) == 0 {
		return searchjobs.ExecutionSnapshot{}, nil
	}
	if index >= len(searches.snapshots) {
		index = len(searches.snapshots) - 1
	}
	return searches.snapshots[index], nil
}

type inspectionResultLease struct{}

func (inspectionResultLease) Schema() searchjobs.Schema {
	return searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "message", Kind: searchjobs.ValueKindString,
	}}}
}
func (inspectionResultLease) RowCount() uint64       { return 0 }
func (inspectionResultLease) RowCountExact() bool    { return true }
func (inspectionResultLease) ResultsTruncated() bool { return false }
func (inspectionResultLease) Generation() uint64     { return 1 }
func (inspectionResultLease) Close() error           { return nil }
func (inspectionResultLease) Next(context.Context) (searchjobs.ResultRow, bool, error) {
	return searchjobs.ResultRow{}, false, nil
}

func (searches *inspectionSearches) callCount() int {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	return searches.calls
}

func (searches *inspectionSearches) lookupCalls() (
	[]searchjobs.AccessScope,
	[]string,
) {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	return slices.Clone(searches.accesses), slices.Clone(searches.ids)
}

type inspectionCompiler struct {
	mu           sync.Mutex
	base         clickhouse.Compiler
	err          error
	override     *clickhouse.CompiledQuery
	afterCompile func()
	calls        int
	last         clickhouse.CompiledQuery
}

// sameScopeSubstitutionCompiler models the legacy attack that motivated the
// concrete public compiler boundary: it ignores the supplied logical plan and
// returns a genuine compiler-sealed query for a different plan with the same
// tenant and effective indexes.
type sameScopeSubstitutionCompiler struct {
	compiled clickhouse.CompiledQuery
}

func (compiler *sameScopeSubstitutionCompiler) Compile(
	*plan.Query,
) (clickhouse.CompiledQuery, error) {
	return compiler.compiled, nil
}

func (compiler *inspectionCompiler) Compile(logical *plan.Query) (clickhouse.CompiledQuery, error) {
	compiler.mu.Lock()
	compiler.calls++
	err := compiler.err
	override := compiler.override
	afterCompile := compiler.afterCompile
	compiler.mu.Unlock()
	if err != nil {
		return clickhouse.CompiledQuery{}, err
	}
	var compiled clickhouse.CompiledQuery
	if override != nil {
		compiled = *override
	} else {
		var compileErr error
		compiled, compileErr = compiler.base.Compile(logical)
		if compileErr != nil {
			return clickhouse.CompiledQuery{}, compileErr
		}
	}
	compiler.mu.Lock()
	compiler.last = compiled
	compiler.mu.Unlock()
	if afterCompile != nil {
		afterCompile()
	}
	return compiled, nil
}

func (compiler *inspectionCompiler) callCount() int {
	compiler.mu.Lock()
	defer compiler.mu.Unlock()
	return compiler.calls
}

func (compiler *inspectionCompiler) lastQuery() clickhouse.CompiledQuery {
	compiler.mu.Lock()
	defer compiler.mu.Unlock()
	return compiler.last
}

type inspectionExplainer struct {
	mu            sync.Mutex
	result        queryexec.ExplainResult
	err           error
	started       chan struct{}
	release       chan struct{}
	ignoreContext bool
	startOnce     sync.Once
	calls         int
	queries       []clickhouse.CompiledQuery
}

func inspectionExplainResult(queryID string) queryexec.ExplainResult {
	const text = `[
  {
    "Plan": {
      "Node Type": "Expression",
      "Plans": [
        {
          "Node Type": "ReadFromMergeTree",
          "Header": [
            {"Name": "event_time", "Type": "DateTime64(9, 'UTC')"},
            {
              "Name": "equals(tenant_id, 'sensitive-literal-7f2c')",
              "Type": "UInt8"
            },
            {"Name": "sensitive_literal_7f2c", "Type": "String"}
          ],
          "Indexes": [
            {
              "Type": "PrimaryKey",
              "Keys": [
                "event_time",
                "equals(trace_id, 'sensitive-literal-7f2c')",
                "sensitive_literal_7f2c"
              ],
              "Initial Parts": 2,
              "Selected Parts": 1,
              "Initial Granules": 4,
              "Selected Granules": 1
            }
          ]
        }
      ]
    }
  }
]`
	return queryexec.ExplainResult{
		Text:    text,
		QueryID: queryID,
	}
}

func (explainer *inspectionExplainer) Explain(
	ctx context.Context,
	query clickhouse.CompiledQuery,
) (queryexec.ExplainResult, error) {
	explainer.mu.Lock()
	explainer.calls++
	explainer.queries = append(explainer.queries, query)
	started := explainer.started
	release := explainer.release
	ignoreContext := explainer.ignoreContext
	result := explainer.result
	err := explainer.err
	explainer.mu.Unlock()
	if started != nil {
		explainer.startOnce.Do(func() { close(started) })
	}
	if release != nil {
		if ignoreContext {
			<-release
		} else {
			select {
			case <-release:
			case <-ctx.Done():
				return queryexec.ExplainResult{}, ctx.Err()
			}
		}
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return queryexec.ExplainResult{}, contextErr
	}
	return result, err
}

func (explainer *inspectionExplainer) callCount() int {
	explainer.mu.Lock()
	defer explainer.mu.Unlock()
	return explainer.calls
}

func (explainer *inspectionExplainer) lastQuery() clickhouse.CompiledQuery {
	explainer.mu.Lock()
	defer explainer.mu.Unlock()
	if len(explainer.queries) == 0 {
		return clickhouse.CompiledQuery{}
	}
	return explainer.queries[len(explainer.queries)-1]
}

func validInspectionSnapshot() searchjobs.ExecutionSnapshot {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	const (
		jobID    = "019fa490-d629-7203-899e-3fcd0fa18cd1"
		ownerID  = "sensitive-owner-7f2c"
		tenantID = "sensitive-tenant-7f2c"
		index    = "sensitive-index-7f2c"
		source   = `index=sensitive-index-7f2c status="sensitive-literal-7f2c" | stats count AS events BY host | sort -events | head 10`
	)
	managerNow := now.Add(-30 * time.Minute)
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: inspectionManagerExecutorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink searchjobs.ResultSink,
		) error {
			columns := make([]searchjobs.Column, len(query.OutputFields))
			for position, field := range query.OutputFields {
				columns[position] = searchjobs.Column{Name: field, Kind: searchjobs.ValueKindString}
			}
			return sink.SetSchema(searchjobs.Schema{Columns: columns})
		}),
		Snapshotter:     inspectionSnapshotterFunc(func(context.Context) (uint64, error) { return 42, nil }),
		RetentionTTL:    90 * time.Minute,
		CleanupInterval: -1,
		Now:             func() time.Time { return managerNow },
		NewID:           func() string { return jobID },
		CursorKey:       []byte("inspection-legacy-fixture-cursor-key-at-least-32-bytes"),
	})
	if err != nil {
		panic(fmt.Sprintf("create sealed inspection fixture manager: %v", err))
	}
	defer func() { _ = manager.Close() }()
	resolved, err := searchtime.NewAbsoluteRange(now.Add(-2*time.Hour), now.Add(-time.Hour))
	if err != nil {
		panic(fmt.Sprintf("create sealed inspection fixture range: %v", err))
	}
	created, err := manager.Create(context.Background(), searchjobs.CreateRequest{
		SPL:               source,
		OwnerID:           ownerID,
		TenantID:          tenantID,
		AuthorizedIndexes: []string{index},
		RequestedIndexes:  []string{index},
		TimeRange:         resolved,
	})
	if err != nil {
		panic(fmt.Sprintf("create sealed inspection fixture: %v", err))
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, getErr := manager.GetFor(searchjobs.AccessScope{TenantID: tenantID, OwnerID: ownerID}, created.ID)
		if getErr != nil {
			panic(fmt.Sprintf("read sealed inspection fixture: %v", getErr))
		}
		if job.State.Terminal() {
			if job.State != searchjobs.StateCompleted {
				panic(fmt.Sprintf("sealed inspection fixture state: %s", job.State))
			}
			snapshot, snapshotErr := manager.CompletedExecutionSnapshotFor(
				context.Background(),
				searchjobs.AccessScope{TenantID: tenantID, OwnerID: ownerID},
				created.ID,
			)
			if snapshotErr != nil {
				panic(fmt.Sprintf("seal inspection fixture snapshot: %v", snapshotErr))
			}
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	panic("sealed inspection fixture did not complete")
}

func waitForInspectionJob(
	t *testing.T,
	manager *searchjobs.Manager,
	id string,
) searchjobs.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if job.State.Terminal() {
			return job
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("search job did not reach a terminal state")
	return searchjobs.Job{}
}

type inspectionTestConfig struct {
	Searches  completedSearches
	Compiler  queryCompiler
	Explainer queryExplainer

	MaxConcurrent int
	MaxRuntime    time.Duration
}

func newInspectionTestService(
	t *testing.T,
	config inspectionTestConfig,
) *Service {
	t.Helper()
	service, err := newService(
		config.Searches,
		config.Compiler,
		config.Explainer,
		config.MaxConcurrent,
		config.MaxRuntime,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(closeCtx); err != nil {
			t.Errorf("close inspection service: %v", err)
		}
	})
	return service
}

func stageOperators(logical LogicalPlan) []string {
	operators := make([]string, len(logical.Stages))
	for index, stage := range logical.Stages {
		operators[index] = stage.Operator
	}
	return operators
}

func assertZeroInspection(t *testing.T, result Result) {
	t.Helper()
	if !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("partial inspection published: %#v", result)
	}
}

func TestInspectionFixtureIsValidUTF8(t *testing.T) {
	snapshot := validInspectionSnapshot()
	if !utf8.ValidString(snapshot.SPL) {
		t.Fatal("test snapshot SPL is invalid UTF-8")
	}
}
