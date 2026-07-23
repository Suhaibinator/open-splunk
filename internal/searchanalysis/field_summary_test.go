package searchanalysis

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestFieldServiceSummaryBuildsAtomicExactResultAndCachesMaximumPrefix(t *testing.T) {
	snapshot := fieldTestSnapshot("summary-cache")
	searches := &fakeFieldSearches{snapshot: snapshot}
	compiler := &fakeFieldSummaryCompiler{fieldKnown: true}
	decimal := mustFieldSummaryDecimal(t, "1.25")
	executor := &fakeFieldSummaryExecutor{result: queryexec.FieldSummaryResult{
		FieldName: "value",
		ObservedTypes: []eventfields.StoredValueType{
			eventfields.StoredValueTypeNull,
			eventfields.StoredValueTypeString,
			eventfields.StoredValueTypeSint64,
			eventfields.StoredValueTypeBool,
			eventfields.StoredValueTypeDecimal,
		},
		EventCount: 11, NullCount: 1, MissingCount: 2, DistinctCount: 4,
		TopValues: []queryexec.FieldValueCountRow{
			{Value: searchjobs.StringValue("alpha"), Count: 4},
			{Value: searchjobs.SignedValue(2), Count: 3},
			{Value: searchjobs.BoolValue(false), Count: 2},
			{Value: decimal, Count: 1},
		},
	}}
	service := newFieldSummaryTestService(t, snapshot, FieldConfig{
		Searches: searches, Compiler: compiler, Executor: executor,
		DefaultSummaryValues: 2, MaximumSummaryValues: 4,
	})
	access := fieldAccess(snapshot)

	first, err := service.GetFieldSummary(context.Background(), access, GetFieldSummaryRequest{
		SearchJobID: snapshot.ID, FieldName: "value",
	})
	if err != nil {
		t.Fatalf("GetFieldSummary(first) error = %v", err)
	}
	distinct := uint64(4)
	wantProfile := FieldProfile{
		FieldName: "value", DisplayName: "value", ValueKind: searchjobs.ValueKindMixed,
		ObservedValueKinds: []searchjobs.ValueKind{
			searchjobs.ValueKindNull,
			searchjobs.ValueKindString,
			searchjobs.ValueKindSigned,
			searchjobs.ValueKindBool,
			searchjobs.ValueKindDecimal,
		},
		EventCount: 11, NullCount: 1, MissingCount: 2, DistinctCount: &distinct,
		Interesting: true,
	}
	if !reflect.DeepEqual(first.Profile, wantProfile) {
		t.Fatalf("profile = %#v, want %#v", first.Profile, wantProfile)
	}
	if len(first.TopValues) != 2 ||
		first.TopValues[0].Count != 4 ||
		first.TopValues[1].Count != 3 ||
		first.TopValues[0].CountIsApproximate ||
		first.TopValuesAreApproximate {
		t.Fatalf("default summary = %#v", first)
	}

	// Mutating the detached response cannot affect the maximum-size cached
	// summary or a later prefix.
	first.Profile.ObservedValueKinds[0] = searchjobs.ValueKindBool
	first.TopValues[0].Count = 99
	second, err := service.GetFieldSummary(context.Background(), access, GetFieldSummaryRequest{
		SearchJobID: snapshot.ID, FieldName: "value", MaxValues: fieldUint32Pointer(1),
	})
	if err != nil {
		t.Fatalf("GetFieldSummary(second) error = %v", err)
	}
	if len(second.TopValues) != 1 ||
		second.TopValues[0].Count != 4 ||
		second.Profile.ObservedValueKinds[0] != searchjobs.ValueKindNull {
		t.Fatalf("cached prefix was not detached: %#v", second)
	}
	if searches.Calls() != 2 {
		t.Fatalf("snapshot calls = %d, want a fresh lookup per request", searches.Calls())
	}
	if compiler.Calls() != 1 || executor.Calls() != 1 {
		t.Fatalf("compiler/executor calls = %d/%d, want 1/1", compiler.Calls(), executor.Calls())
	}
	spec := compiler.Spec()
	if spec != (clickhouse.FieldSummarySpec{
		FieldName:             "value",
		MaximumValues:         4,
		MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues,
		MaximumValueBytes:     clickhouse.MaximumFieldSummaryValueBytes,
	}) {
		t.Fatalf("compiled spec = %+v", spec)
	}
	query := compiler.Query()
	if query == nil {
		t.Fatal("compiler did not receive the rebuilt completed plan")
	}
	scan, ok := query.Operators[0].(*plan.Scan)
	if !ok ||
		scan.VisibilityCutoff != snapshot.VisibilityCutoff ||
		!reflect.DeepEqual(scan.Indexes, snapshot.EffectiveIndexes) {
		t.Fatalf("rebuilt scan = %#v", query.Operators[0])
	}
}

func TestFieldServiceSummaryValidatesRequestBeforeLookupAndPreservesExactFieldName(t *testing.T) {
	snapshot := fieldTestSnapshot("summary-validation")
	searches := &fakeFieldSearches{snapshot: snapshot}
	compiler := &fakeFieldSummaryCompiler{fieldKnown: true}
	executor := &fakeFieldSummaryExecutor{execute: func(
		_ context.Context,
		query clickhouse.CompiledFieldSummary,
	) (queryexec.FieldSummaryResult, error) {
		return zeroFieldSummaryResult(query.Spec.FieldName), nil
	}}
	service := newFieldSummaryTestService(t, snapshot, FieldConfig{
		Searches: searches, Compiler: compiler, Executor: executor,
	})
	access := fieldAccess(snapshot)
	invalidRequests := []GetFieldSummaryRequest{
		{},
		{SearchJobID: snapshot.ID},
		{SearchJobID: snapshot.ID, FieldName: string([]byte{0xff})},
		{SearchJobID: snapshot.ID, FieldName: strings.Repeat("f", maximumFieldProfileNameBytes+1)},
		{SearchJobID: snapshot.ID, FieldName: "a..b"},
		{SearchJobID: snapshot.ID, FieldName: "wild*"},
		{SearchJobID: snapshot.ID, FieldName: "__os_private"},
		{SearchJobID: snapshot.ID, FieldName: "host", MaxValues: fieldUint32Pointer(0)},
		{
			SearchJobID: snapshot.ID, FieldName: "host",
			MaxValues: fieldUint32Pointer(service.MaximumSummaryValues() + 1),
		},
	}
	for _, request := range invalidRequests {
		if _, err := service.GetFieldSummary(context.Background(), access, request); !errors.Is(err, ErrInvalidFieldRequest) {
			t.Errorf("GetFieldSummary(%+v) error = %v, want ErrInvalidFieldRequest", request, err)
		}
	}
	for _, invalidAccess := range []searchjobs.AccessScope{
		{},
		{TenantID: "tenant"},
		{TenantID: string([]byte{0xff}), OwnerID: "owner"},
		{TenantID: "tenant", OwnerID: strings.Repeat("o", maximumFieldAccessIdentityLen+1)},
	} {
		if _, err := service.GetFieldSummary(
			context.Background(),
			invalidAccess,
			GetFieldSummaryRequest{SearchJobID: snapshot.ID, FieldName: "host"},
		); !errors.Is(err, ErrInvalidFieldRequest) {
			t.Errorf("GetFieldSummary(access %+v) error = %v, want ErrInvalidFieldRequest", invalidAccess, err)
		}
	}
	if searches.Calls() != 0 {
		t.Fatalf("snapshot calls for invalid requests = %d, want 0", searches.Calls())
	}

	const exactName = " host "
	summary, err := service.GetFieldSummary(context.Background(), access, GetFieldSummaryRequest{
		SearchJobID: "  " + snapshot.ID + "  ",
		FieldName:   exactName,
	})
	if err != nil {
		t.Fatalf("GetFieldSummary(exact name) error = %v", err)
	}
	if summary.Profile.FieldName != exactName || compiler.Spec().FieldName != exactName {
		t.Fatalf("exact field spelling was changed: profile %q spec %q", summary.Profile.FieldName, compiler.Spec().FieldName)
	}
	if _, err := service.GetFieldSummary(nil, access, GetFieldSummaryRequest{
		SearchJobID: snapshot.ID, FieldName: "host",
	}); err == nil {
		t.Fatal("GetFieldSummary(nil context) error = nil")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.GetFieldSummary(canceled, access, GetFieldSummaryRequest{
		SearchJobID: snapshot.ID, FieldName: "host",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetFieldSummary(canceled) error = %v", err)
	}
	var nilService *FieldService
	if _, err := nilService.GetFieldSummary(context.Background(), access, GetFieldSummaryRequest{
		SearchJobID: snapshot.ID, FieldName: "host",
	}); err == nil {
		t.Fatal("nil FieldService.GetFieldSummary() error = nil")
	}
}

func TestFieldServiceSummaryRechecksScopeIdentityExpiryAndFingerprint(t *testing.T) {
	snapshot := fieldTestSnapshot("summary-lifecycle")
	clock := &fieldTestClock{now: snapshot.FinishedAt.Add(time.Minute)}
	var stateMu sync.Mutex
	current := snapshot
	var lookupErr error
	searches := &fakeFieldSearches{lookup: func(
		_ context.Context,
		access searchjobs.AccessScope,
		id string,
	) (searchjobs.ExecutionSnapshot, error) {
		stateMu.Lock()
		defer stateMu.Unlock()
		if access != fieldAccess(snapshot) || id != snapshot.ID {
			t.Fatalf("lookup = (%+v, %q)", access, id)
		}
		return current, lookupErr
	}}
	compiler := &fakeFieldSummaryCompiler{fieldKnown: true}
	executor := &fakeFieldSummaryExecutor{execute: func(
		_ context.Context,
		query clickhouse.CompiledFieldSummary,
	) (queryexec.FieldSummaryResult, error) {
		return zeroFieldSummaryResult(query.Spec.FieldName), nil
	}}
	service := newFieldSummaryTestService(t, snapshot, FieldConfig{
		Searches: searches, Compiler: compiler, Executor: executor, Clock: clock.Now,
	})
	request := GetFieldSummaryRequest{SearchJobID: snapshot.ID, FieldName: "projected"}
	access := fieldAccess(snapshot)

	if _, err := service.GetFieldSummary(context.Background(), access, request); err != nil {
		t.Fatalf("GetFieldSummary(first) error = %v", err)
	}
	stateMu.Lock()
	current.VisibilityCutoff++
	stateMu.Unlock()
	if _, err := service.GetFieldSummary(context.Background(), access, request); err != nil {
		t.Fatalf("GetFieldSummary(changed fingerprint) error = %v", err)
	}
	if compiler.Calls() != 2 || executor.Calls() != 2 {
		t.Fatalf("changed snapshot reused summary: compiler/executor %d/%d", compiler.Calls(), executor.Calls())
	}

	stateMu.Lock()
	current.OwnerID = "another-owner"
	stateMu.Unlock()
	if _, err := service.GetFieldSummary(context.Background(), access, request); !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("GetFieldSummary(identity mismatch) error = %v, want ErrInvalidResult", err)
	}
	if compiler.Calls() != 2 {
		t.Fatalf("identity mismatch reached compiler: %d calls", compiler.Calls())
	}

	stateMu.Lock()
	current = snapshot
	lookupErr = searchjobs.ErrExpired
	stateMu.Unlock()
	if _, err := service.GetFieldSummary(context.Background(), access, request); !errors.Is(err, searchjobs.ErrExpired) {
		t.Fatalf("GetFieldSummary(provider expiry) error = %v", err)
	}
	if compiler.Calls() != 2 {
		t.Fatalf("provider expiry used cache or compiler: %d calls", compiler.Calls())
	}

	stateMu.Lock()
	lookupErr = nil
	current = snapshot
	stateMu.Unlock()
	clock.Advance(snapshot.ExpiresAt.Sub(clock.Now()))
	if _, err := service.GetFieldSummary(context.Background(), access, request); !errors.Is(err, searchjobs.ErrExpired) {
		t.Fatalf("GetFieldSummary(local expiry) error = %v", err)
	}
}

func TestFieldServiceSummaryNotFoundAndZeroOrNullFields(t *testing.T) {
	snapshot := fieldTestSnapshot("summary-empty")
	access := fieldAccess(snapshot)
	tests := []struct {
		name       string
		compiler   *fakeFieldSummaryCompiler
		executor   *fakeFieldSummaryExecutor
		want       error
		assertions func(*testing.T, FieldSummary)
	}{
		{
			name: "compiler not found",
			compiler: &fakeFieldSummaryCompiler{
				fieldKnown: true,
				err:        errors.Join(errors.New("projected away"), clickhouse.ErrFieldSummaryNotFound),
			},
			executor: &fakeFieldSummaryExecutor{},
			want:     ErrFieldNotFound,
		},
		{
			name:     "executor not found",
			compiler: &fakeFieldSummaryCompiler{},
			executor: &fakeFieldSummaryExecutor{err: clickhouse.ErrFieldSummaryNotFound},
			want:     ErrFieldNotFound,
		},
		{
			name:     "open schema zero means absent",
			compiler: &fakeFieldSummaryCompiler{fieldKnown: false},
			executor: &fakeFieldSummaryExecutor{result: zeroFieldSummaryResult("field")},
			want:     ErrFieldNotFound,
		},
		{
			name:     "known zero event field",
			compiler: &fakeFieldSummaryCompiler{fieldKnown: true},
			executor: &fakeFieldSummaryExecutor{result: zeroFieldSummaryResult("field")},
			assertions: func(t *testing.T, summary FieldSummary) {
				t.Helper()
				if summary.Profile.ValueKind != searchjobs.ValueKindNull ||
					summary.Profile.EventCount != 0 ||
					summary.Profile.DistinctCount == nil ||
					*summary.Profile.DistinctCount != 0 ||
					len(summary.TopValues) != 0 {
					t.Fatalf("zero-event summary = %#v", summary)
				}
			},
		},
		{
			name:     "known all null field",
			compiler: &fakeFieldSummaryCompiler{fieldKnown: true},
			executor: &fakeFieldSummaryExecutor{result: queryexec.FieldSummaryResult{
				FieldName: "field", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeNull},
				EventCount: 3, NullCount: 3, MissingCount: 2,
			}},
			assertions: func(t *testing.T, summary FieldSummary) {
				t.Helper()
				if summary.Profile.ValueKind != searchjobs.ValueKindNull ||
					!reflect.DeepEqual(summary.Profile.ObservedValueKinds, []searchjobs.ValueKind{searchjobs.ValueKindNull}) ||
					summary.Profile.DistinctCount == nil ||
					*summary.Profile.DistinctCount != 0 ||
					len(summary.TopValues) != 0 {
					t.Fatalf("all-null summary = %#v", summary)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newFieldSummaryTestService(t, snapshot, FieldConfig{
				Searches: &fakeFieldSearches{snapshot: snapshot},
				Compiler: test.compiler,
				Executor: test.executor,
			})
			summary, err := service.GetFieldSummary(context.Background(), access, GetFieldSummaryRequest{
				SearchJobID: snapshot.ID, FieldName: "field",
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("GetFieldSummary() error = %v, want %v", err, test.want)
			}
			if test.want != nil {
				return
			}
			test.assertions(t, summary)
		})
	}
}

func TestNormalizeFieldSummaryAcceptsCanonicalScalarsOrderingAndFarFutureTime(t *testing.T) {
	t.Parallel()

	spec := fieldSummaryTestSpec("field", 4)
	key := fieldSummaryTestKey("field")
	decimal := mustFieldSummaryDecimal(t, "1.25")
	future := time.Date(2500, 3, 4, 5, 6, 7, 123_000_000, time.FixedZone("offset", 2*60*60))
	result := queryexec.FieldSummaryResult{
		FieldName: "field",
		ObservedTypes: []eventfields.StoredValueType{
			eventfields.StoredValueTypeString,
			eventfields.StoredValueTypeSint64,
			eventfields.StoredValueTypeTimestamp,
			eventfields.StoredValueTypeDecimal,
		},
		EventCount: 4, DistinctCount: 4,
		// Equal counts sort by display before kind. Therefore signed display
		// "1" precedes string display "z", despite the inverse kind order.
		TopValues: []queryexec.FieldValueCountRow{
			{Value: searchjobs.SignedValue(1), Count: 1},
			{Value: decimal, Count: 1},
			{Value: searchjobs.TimeValue(future), Count: 1},
			{Value: searchjobs.StringValue("z"), Count: 1},
		},
	}
	summary, _, err := normalizeFieldSummaryResult(result, spec, key)
	if err != nil {
		t.Fatalf("normalizeFieldSummaryResult() error = %v", err)
	}
	gotFuture, ok := summary.TopValues[2].Value.Time()
	if !ok || !gotFuture.Equal(future) || gotFuture.Location() != time.UTC {
		t.Fatalf("far-future timestamp = (%v, %v), want exact UTC %v", gotFuture, ok, future.UTC())
	}

	// Equal display and count use kind as the next deterministic tie-breaker.
	result.ObservedTypes = []eventfields.StoredValueType{
		eventfields.StoredValueTypeString,
		eventfields.StoredValueTypeSint64,
	}
	result.EventCount = 2
	result.DistinctCount = 2
	result.TopValues = []queryexec.FieldValueCountRow{
		{Value: searchjobs.StringValue("1"), Count: 1},
		{Value: searchjobs.SignedValue(1), Count: 1},
	}
	if _, _, err := normalizeFieldSummaryResult(result, spec, key); err != nil {
		t.Fatalf("equal-display kind ordering error = %v", err)
	}
}

func TestNormalizeFieldSummaryRejectsMalformedExecutorResults(t *testing.T) {
	t.Parallel()

	spec := fieldSummaryTestSpec("field", 2)
	key := fieldSummaryTestKey("field")
	baseline := func() queryexec.FieldSummaryResult {
		return queryexec.FieldSummaryResult{
			FieldName: "field", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString},
			EventCount: 2, DistinctCount: 1,
			TopValues: []queryexec.FieldValueCountRow{{Value: searchjobs.StringValue("a"), Count: 2}},
		}
	}
	tests := []struct {
		name   string
		mutate func(*queryexec.FieldSummaryResult)
	}{
		{name: "field name", mutate: func(result *queryexec.FieldSummaryResult) { result.FieldName = "other" }},
		{name: "count overflow", mutate: func(result *queryexec.FieldSummaryResult) {
			result.EventCount = ^uint64(0)
			result.MissingCount = 1
		}},
		{name: "null exceeds event", mutate: func(result *queryexec.FieldSummaryResult) { result.NullCount = 3 }},
		{name: "distinct exceeds present", mutate: func(result *queryexec.FieldSummaryResult) { result.DistinctCount = 3 }},
		{name: "incomplete prefix", mutate: func(result *queryexec.FieldSummaryResult) {
			result.DistinctCount = 2
		}},
		{name: "zero count", mutate: func(result *queryexec.FieldSummaryResult) { result.TopValues[0].Count = 0 }},
		{name: "count exceeds present", mutate: func(result *queryexec.FieldSummaryResult) { result.TopValues[0].Count = 3 }},
		{name: "unobserved type", mutate: func(result *queryexec.FieldSummaryResult) {
			result.TopValues[0].Value = searchjobs.BoolValue(true)
		}},
		{name: "observed scalar without exact group", mutate: func(result *queryexec.FieldSummaryResult) {
			result.ObservedTypes = []eventfields.StoredValueType{
				eventfields.StoredValueTypeString,
				eventfields.StoredValueTypeSint64,
			}
		}},
		{name: "observed container", mutate: func(result *queryexec.FieldSummaryResult) {
			result.ObservedTypes = []eventfields.StoredValueType{
				eventfields.StoredValueTypeString,
				eventfields.StoredValueTypeList,
			}
			result.DistinctCount = 2
			result.TopValues = []queryexec.FieldValueCountRow{
				{Value: searchjobs.StringValue("a"), Count: 1},
				{Value: searchjobs.StringValue("b"), Count: 1},
			}
		}},
		{name: "invalid kind", mutate: func(result *queryexec.FieldSummaryResult) {
			result.TopValues[0].Value = searchjobs.Value{}
		}},
		{name: "null value", mutate: func(result *queryexec.FieldSummaryResult) {
			result.ObservedTypes = []eventfields.StoredValueType{eventfields.StoredValueTypeNull}
			result.NullCount = 1
			result.TopValues[0].Value = searchjobs.NullValue()
		}},
		{name: "noncanonical decimal", mutate: func(result *queryexec.FieldSummaryResult) {
			result.ObservedTypes = []eventfields.StoredValueType{eventfields.StoredValueTypeDecimal}
			result.TopValues[0].Value = mustFieldSummaryDecimal(t, "1.0")
		}},
		{name: "nonfinite double", mutate: func(result *queryexec.FieldSummaryResult) {
			result.ObservedTypes = []eventfields.StoredValueType{eventfields.StoredValueTypeDouble}
			result.TopValues[0].Value = searchjobs.DoubleValue(math.Inf(1))
		}},
		{name: "oversized value", mutate: func(result *queryexec.FieldSummaryResult) {
			result.TopValues[0].Value = searchjobs.StringValue(strings.Repeat("x", int(spec.MaximumValueBytes)+1))
		}},
		{name: "duplicate", mutate: func(result *queryexec.FieldSummaryResult) {
			result.DistinctCount = 2
			result.TopValues = []queryexec.FieldValueCountRow{
				{Value: searchjobs.StringValue("a"), Count: 1},
				{Value: searchjobs.StringValue("a"), Count: 1},
			}
		}},
		{name: "count order", mutate: func(result *queryexec.FieldSummaryResult) {
			result.EventCount = 3
			result.DistinctCount = 2
			result.TopValues = []queryexec.FieldValueCountRow{
				{Value: searchjobs.StringValue("a"), Count: 1},
				{Value: searchjobs.StringValue("b"), Count: 2},
			}
		}},
		{name: "display tie order", mutate: func(result *queryexec.FieldSummaryResult) {
			result.ObservedTypes = []eventfields.StoredValueType{
				eventfields.StoredValueTypeString,
				eventfields.StoredValueTypeSint64,
			}
			result.DistinctCount = 2
			result.TopValues = []queryexec.FieldValueCountRow{
				{Value: searchjobs.StringValue("z"), Count: 1},
				{Value: searchjobs.SignedValue(1), Count: 1},
			}
		}},
		{name: "complete count gap", mutate: func(result *queryexec.FieldSummaryResult) {
			result.EventCount = 3
		}},
		{name: "truncated covers all", mutate: func(result *queryexec.FieldSummaryResult) {
			result.EventCount = 2
			result.DistinctCount = 3
			result.TopValues = []queryexec.FieldValueCountRow{
				{Value: searchjobs.StringValue("a"), Count: 1},
				{Value: searchjobs.StringValue("b"), Count: 1},
			}
		}},
		{name: "truncated cannot cover omitted values", mutate: func(result *queryexec.FieldSummaryResult) {
			result.EventCount = 4
			result.DistinctCount = 4
			result.TopValues = []queryexec.FieldValueCountRow{
				{Value: searchjobs.StringValue("a"), Count: 2},
				{Value: searchjobs.StringValue("b"), Count: 1},
			}
		}},
		{name: "omitted value would outrank prefix", mutate: func(result *queryexec.FieldSummaryResult) {
			result.EventCount = 10
			result.DistinctCount = 3
			result.TopValues = []queryexec.FieldValueCountRow{
				{Value: searchjobs.StringValue("a"), Count: 1},
				{Value: searchjobs.StringValue("b"), Count: 1},
			}
		}},
		{name: "omitted values cannot represent observed kinds", mutate: func(result *queryexec.FieldSummaryResult) {
			result.ObservedTypes = []eventfields.StoredValueType{
				eventfields.StoredValueTypeString,
				eventfields.StoredValueTypeSint64,
				eventfields.StoredValueTypeBool,
			}
			result.EventCount = 4
			result.DistinctCount = 3
			result.TopValues = []queryexec.FieldValueCountRow{
				{Value: searchjobs.StringValue("a"), Count: 2},
				{Value: searchjobs.StringValue("b"), Count: 1},
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := baseline()
			test.mutate(&result)
			if _, _, err := normalizeFieldSummaryResult(result, spec, key); !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("normalizeFieldSummaryResult() error = %v, want ErrInvalidResult", err)
			}
		})
	}

	encodedByteSpec := fieldSummaryTestSpec("field", 1)
	encodedByteSpec.MaximumValueBytes = 4
	encodedByteResult := queryexec.FieldSummaryResult{
		FieldName: "field",
		ObservedTypes: []eventfields.StoredValueType{
			eventfields.StoredValueTypeBytes,
		},
		EventCount: 1, DistinctCount: 1,
		TopValues: []queryexec.FieldValueCountRow{{
			Value: searchjobs.BytesValue([]byte{1, 2, 3, 4}), Count: 1,
		}},
	}
	if _, _, err := normalizeFieldSummaryResult(
		encodedByteResult,
		encodedByteSpec,
		key,
	); !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("encoded byte limit error = %v, want ErrInvalidResult", err)
	}
}

func TestFieldServiceSummaryMapsCompilerAndExecutorErrors(t *testing.T) {
	t.Parallel()

	snapshot := fieldTestSnapshot("summary-errors")
	diagnostic := &plan.Diagnostic{Code: "SPL_QUERY_TOO_COMPLEX", Message: "too complex"}
	tests := []struct {
		name        string
		compileErr  error
		executeErr  error
		want        error
		executorHit bool
	}{
		{name: "compile execution limit", compileErr: diagnostic, want: searchjobs.ErrExecutionLimit},
		{
			name: "metadata unavailable", executeErr: queryexec.ErrFieldMetadataUnavailable,
			want: ErrFieldAnalysisUnsupported, executorHit: true,
		},
		{
			name: "unsupported scalar", executeErr: searchjobs.ErrUnsupportedValue,
			want: searchjobs.ErrUnsupportedValue, executorHit: true,
		},
		{
			name: "execution limit", executeErr: searchjobs.ErrExecutionLimit,
			want: searchjobs.ErrExecutionLimit, executorHit: true,
		},
		{
			name: "invalid executor result", executeErr: searchjobs.ErrInvalidResult,
			want: searchjobs.ErrInvalidResult, executorHit: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compiler := &fakeFieldSummaryCompiler{fieldKnown: true, err: test.compileErr}
			executor := &fakeFieldSummaryExecutor{err: test.executeErr}
			service := newFieldSummaryTestService(t, snapshot, FieldConfig{
				Searches: &fakeFieldSearches{snapshot: snapshot},
				Compiler: compiler,
				Executor: executor,
			})
			_, err := service.GetFieldSummary(
				context.Background(),
				fieldAccess(snapshot),
				GetFieldSummaryRequest{SearchJobID: snapshot.ID, FieldName: "field"},
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("GetFieldSummary() error = %v, want %v", err, test.want)
			}
			wantCalls := 0
			if test.executorHit {
				wantCalls = 1
			}
			if executor.Calls() != wantCalls {
				t.Fatalf("executor calls = %d, want %d", executor.Calls(), wantCalls)
			}
		})
	}
}

func TestFieldServiceSummaryCoalescesAndSharesAdmissionGate(t *testing.T) {
	snapshot := fieldTestSnapshot("summary-coalesce")
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	executor := &fakeFieldSummaryExecutor{execute: func(
		ctx context.Context,
		query clickhouse.CompiledFieldSummary,
	) (queryexec.FieldSummaryResult, error) {
		startedOnce.Do(func() { close(started) })
		select {
		case <-release:
			return zeroFieldSummaryResult(query.Spec.FieldName), nil
		case <-ctx.Done():
			return queryexec.FieldSummaryResult{}, ctx.Err()
		}
	}}
	service := newFieldSummaryTestService(t, snapshot, FieldConfig{
		Searches:      &fakeFieldSearches{snapshot: snapshot},
		Compiler:      &fakeFieldSummaryCompiler{fieldKnown: true},
		Executor:      executor,
		MaxConcurrent: 1,
	})
	access := fieldAccess(snapshot)
	request := GetFieldSummaryRequest{SearchJobID: snapshot.ID, FieldName: "field"}
	const callers = 8
	errs := make(chan error, callers)
	for range callers {
		go func() {
			_, err := service.GetFieldSummary(context.Background(), access, request)
			errs <- err
		}()
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("summary executor did not start")
	}
	waitForFieldCondition(t, "all summary callers to coalesce", func() bool {
		service.mu.Lock()
		defer service.mu.Unlock()
		for _, flight := range service.summaryFlights {
			return flight.waiters == callers
		}
		return false
	})

	if _, err := service.GetFieldSummary(context.Background(), access, GetFieldSummaryRequest{
		SearchJobID: snapshot.ID, FieldName: "different",
	}); !errors.Is(err, ErrFieldAnalysisCapacity) {
		t.Fatalf("different field while gate full error = %v, want capacity", err)
	}
	close(release)
	for range callers {
		if err := <-errs; err != nil {
			t.Fatalf("coalesced GetFieldSummary() error = %v", err)
		}
	}
	if executor.Calls() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.Calls())
	}
}

func TestFieldServiceSummaryCancellationAndCloseAbandonWorkers(t *testing.T) {
	t.Run("last waiter cancellation permits retry", func(t *testing.T) {
		snapshot := fieldTestSnapshot("summary-cancel")
		var attempts int
		var attemptsMu sync.Mutex
		firstCanceled := make(chan struct{})
		executor := &fakeFieldSummaryExecutor{execute: func(
			ctx context.Context,
			query clickhouse.CompiledFieldSummary,
		) (queryexec.FieldSummaryResult, error) {
			attemptsMu.Lock()
			attempts++
			attempt := attempts
			attemptsMu.Unlock()
			if attempt == 1 {
				<-ctx.Done()
				close(firstCanceled)
				return queryexec.FieldSummaryResult{}, ctx.Err()
			}
			return zeroFieldSummaryResult(query.Spec.FieldName), nil
		}}
		service := newFieldSummaryTestService(t, snapshot, FieldConfig{
			Searches: &fakeFieldSearches{snapshot: snapshot},
			Compiler: &fakeFieldSummaryCompiler{fieldKnown: true},
			Executor: executor,
		})
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := service.GetFieldSummary(ctx, fieldAccess(snapshot), GetFieldSummaryRequest{
				SearchJobID: snapshot.ID, FieldName: "field",
			})
			result <- err
		}()
		waitForFieldCondition(t, "first summary execution", func() bool { return executor.Calls() == 1 })
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled caller error = %v", err)
		}
		select {
		case <-firstCanceled:
		case <-time.After(2 * time.Second):
			t.Fatal("abandoned summary worker was not canceled")
		}
		if _, err := service.GetFieldSummary(
			context.Background(),
			fieldAccess(snapshot),
			GetFieldSummaryRequest{SearchJobID: snapshot.ID, FieldName: "field"},
		); err != nil {
			t.Fatalf("retry error = %v", err)
		}
		if executor.Calls() != 2 {
			t.Fatalf("executor calls = %d, want abandoned work retried", executor.Calls())
		}
	})

	t.Run("close cancels and waits", func(t *testing.T) {
		snapshot := fieldTestSnapshot("summary-close")
		executor := &fakeFieldSummaryExecutor{execute: func(
			ctx context.Context,
			_ clickhouse.CompiledFieldSummary,
		) (queryexec.FieldSummaryResult, error) {
			<-ctx.Done()
			return queryexec.FieldSummaryResult{}, ctx.Err()
		}}
		service := newFieldTestService(t, FieldConfig{
			Searches: &fakeFieldSearches{snapshot: snapshot},
			Compiler: &fakeFieldSummaryCompiler{fieldKnown: true},
			Executor: executor,
		})
		result := make(chan error, 1)
		go func() {
			_, err := service.GetFieldSummary(
				context.Background(),
				fieldAccess(snapshot),
				GetFieldSummaryRequest{SearchJobID: snapshot.ID, FieldName: "field"},
			)
			result <- err
		}()
		waitForFieldCondition(t, "summary execution before close", func() bool { return executor.Calls() == 1 })
		closeContext, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelClose()
		if err := service.Close(closeContext); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if err := <-result; !errors.Is(err, searchjobs.ErrClosed) {
			t.Fatalf("in-flight request error = %v, want ErrClosed", err)
		}
		if _, err := service.GetFieldSummary(
			context.Background(),
			fieldAccess(snapshot),
			GetFieldSummaryRequest{SearchJobID: snapshot.ID, FieldName: "field"},
		); !errors.Is(err, searchjobs.ErrClosed) {
			t.Fatalf("post-close request error = %v, want ErrClosed", err)
		}
	})
}

func TestFieldServiceSummaryCacheTTLExpiryAndByteLimit(t *testing.T) {
	t.Run("TTL and job expiry", func(t *testing.T) {
		snapshot := fieldTestSnapshot("summary-ttl")
		clock := &fieldTestClock{now: snapshot.FinishedAt.Add(time.Minute)}
		executor := &fakeFieldSummaryExecutor{execute: func(
			_ context.Context,
			query clickhouse.CompiledFieldSummary,
		) (queryexec.FieldSummaryResult, error) {
			return zeroFieldSummaryResult(query.Spec.FieldName), nil
		}}
		service := newFieldSummaryTestService(t, snapshot, FieldConfig{
			Searches: &fakeFieldSearches{snapshot: snapshot},
			Compiler: &fakeFieldSummaryCompiler{fieldKnown: true},
			Executor: executor,
			Clock:    clock.Now, SummaryCacheTTL: time.Minute,
		})
		request := GetFieldSummaryRequest{SearchJobID: snapshot.ID, FieldName: "field"}
		for range 2 {
			if _, err := service.GetFieldSummary(context.Background(), fieldAccess(snapshot), request); err != nil {
				t.Fatalf("cached GetFieldSummary() error = %v", err)
			}
		}
		if executor.Calls() != 1 {
			t.Fatalf("executor calls before TTL = %d, want 1", executor.Calls())
		}
		clock.Advance(time.Minute)
		if _, err := service.GetFieldSummary(context.Background(), fieldAccess(snapshot), request); err != nil {
			t.Fatalf("post-TTL GetFieldSummary() error = %v", err)
		}
		if executor.Calls() != 2 {
			t.Fatalf("executor calls after TTL = %d, want 2", executor.Calls())
		}
		clock.Advance(snapshot.ExpiresAt.Sub(clock.Now()))
		if _, err := service.GetFieldSummary(
			context.Background(),
			fieldAccess(snapshot),
			request,
		); !errors.Is(err, searchjobs.ErrExpired) {
			t.Fatalf("job-expired GetFieldSummary() error = %v", err)
		}
	})

	t.Run("entry larger than cache is never retained", func(t *testing.T) {
		snapshot := fieldTestSnapshot("summary-bytes")
		executor := &fakeFieldSummaryExecutor{result: zeroFieldSummaryResult("field")}
		service := newFieldSummaryTestService(t, snapshot, FieldConfig{
			Searches:             &fakeFieldSearches{snapshot: snapshot},
			Compiler:             &fakeFieldSummaryCompiler{fieldKnown: true},
			Executor:             executor,
			MaxSummaryCacheBytes: 1,
		})
		for attempt := range 2 {
			if _, err := service.GetFieldSummary(
				context.Background(),
				fieldAccess(snapshot),
				GetFieldSummaryRequest{SearchJobID: snapshot.ID, FieldName: "field"},
			); !errors.Is(err, ErrFieldAnalysisCapacity) {
				t.Fatalf("attempt %d error = %v, want capacity", attempt, err)
			}
		}
		if executor.Calls() != 2 {
			t.Fatalf("oversized executor calls = %d, want retry without cache", executor.Calls())
		}
	})
}

type fakeFieldSummaryCompiler struct {
	fakeFieldCompiler

	summaryMu  sync.Mutex
	query      *plan.Query
	spec       clickhouse.FieldSummarySpec
	fieldKnown bool
	err        error
	compile    func(*plan.Query, clickhouse.FieldSummarySpec) (clickhouse.CompiledFieldSummary, error)
	calls      int
}

func (compiler *fakeFieldSummaryCompiler) CompileFieldSummary(
	query *plan.Query,
	spec clickhouse.FieldSummarySpec,
) (clickhouse.CompiledFieldSummary, error) {
	compiler.summaryMu.Lock()
	compiler.calls++
	compiler.query = query
	compiler.spec = spec
	compile := compiler.compile
	err := compiler.err
	fieldKnown := compiler.fieldKnown
	compiler.summaryMu.Unlock()
	if compile != nil {
		return compile(query, spec)
	}
	return clickhouse.CompiledFieldSummary{
		SQL: "SELECT field summary", Spec: spec, FieldKnown: fieldKnown,
	}, err
}

func (compiler *fakeFieldSummaryCompiler) Calls() int {
	compiler.summaryMu.Lock()
	defer compiler.summaryMu.Unlock()
	return compiler.calls
}

func (compiler *fakeFieldSummaryCompiler) Query() *plan.Query {
	compiler.summaryMu.Lock()
	defer compiler.summaryMu.Unlock()
	return compiler.query
}

func (compiler *fakeFieldSummaryCompiler) Spec() clickhouse.FieldSummarySpec {
	compiler.summaryMu.Lock()
	defer compiler.summaryMu.Unlock()
	return compiler.spec
}

type fakeFieldSummaryExecutor struct {
	fakeFieldExecutor

	summaryMu sync.Mutex
	result    queryexec.FieldSummaryResult
	err       error
	execute   func(context.Context, clickhouse.CompiledFieldSummary) (queryexec.FieldSummaryResult, error)
	calls     int
}

func (executor *fakeFieldSummaryExecutor) ExecuteFieldSummary(
	ctx context.Context,
	query clickhouse.CompiledFieldSummary,
) (queryexec.FieldSummaryResult, error) {
	executor.summaryMu.Lock()
	executor.calls++
	execute := executor.execute
	result := executor.result
	err := executor.err
	executor.summaryMu.Unlock()
	if execute != nil {
		return execute(ctx, query)
	}
	return result, err
}

func (executor *fakeFieldSummaryExecutor) Calls() int {
	executor.summaryMu.Lock()
	defer executor.summaryMu.Unlock()
	return executor.calls
}

func newFieldSummaryTestService(
	t *testing.T,
	snapshot searchjobs.ExecutionSnapshot,
	config FieldConfig,
) *FieldService {
	t.Helper()
	if config.Clock == nil {
		now := snapshot.FinishedAt.Add(time.Minute)
		config.Clock = func() time.Time { return now }
	}
	service := newFieldTestService(t, config)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("Close() cleanup error = %v", err)
		}
	})
	return service
}

func zeroFieldSummaryResult(fieldName string) queryexec.FieldSummaryResult {
	return queryexec.FieldSummaryResult{FieldName: fieldName}
}

func fieldSummaryTestSpec(fieldName string, maximumValues uint32) clickhouse.FieldSummarySpec {
	return clickhouse.FieldSummarySpec{
		FieldName:             fieldName,
		MaximumValues:         maximumValues,
		MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues,
		MaximumValueBytes:     clickhouse.MaximumFieldSummaryValueBytes,
	}
}

func fieldSummaryTestKey(fieldName string) fieldSummaryCacheKey {
	return fieldSummaryCacheKey{
		fieldCacheKey: fieldCacheKey{tenantID: "tenant", ownerID: "owner", jobID: "job"},
		fieldName:     fieldName,
	}
}

func mustFieldSummaryDecimal(t *testing.T, value string) searchjobs.Value {
	t.Helper()
	result, err := searchjobs.DecimalValue(value)
	if err != nil {
		t.Fatalf("DecimalValue(%q) error = %v", value, err)
	}
	return result
}
