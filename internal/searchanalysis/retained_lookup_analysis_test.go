package searchanalysis

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type searchAnalysisLookupResolverFunc func(
	context.Context,
	searchjobs.LookupAdmissionResolutionScope,
) (searchjobs.LookupAdmissionResolution, error)

func (resolve searchAnalysisLookupResolverFunc) ResolveLookupAdmission(
	ctx context.Context,
	scope searchjobs.LookupAdmissionResolutionScope,
) (searchjobs.LookupAdmissionResolution, error) {
	return resolve(ctx, scope)
}

type retainedLookupFieldExecutor struct {
	catalogSealed bool
	summarySealed bool
}

func (executor *retainedLookupFieldExecutor) ExecuteFieldCatalog(
	_ context.Context,
	compiled clickhouse.CompiledFieldCatalog,
) (queryexec.FieldCatalogResult, error) {
	executor.catalogSealed = compiled.HasValidExecutionSeal()
	return queryexec.FieldCatalogResult{
		TotalEvents: 1,
		Fields: []queryexec.FieldProfileRow{{
			FieldName:     "service_owner",
			ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString},
			EventCount:    1,
		}},
	}, nil
}

func (executor *retainedLookupFieldExecutor) ExecuteFieldSummary(
	_ context.Context,
	compiled clickhouse.CompiledFieldSummary,
) (queryexec.FieldSummaryResult, error) {
	executor.summarySealed = compiled.HasValidExecutionSeal()
	return queryexec.FieldSummaryResult{
		FieldName:     compiled.Spec.FieldName,
		ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString},
		EventCount:    1,
		DistinctCount: 1,
		TopValues: []queryexec.FieldValueCountRow{{
			Value: searchjobs.StringValue("platform"),
			Count: 1,
		}},
	}, nil
}

type retainedLookupTimelineExecutor struct {
	sealed bool
}

func (executor *retainedLookupTimelineExecutor) ExecuteTimeline(
	_ context.Context,
	compiled clickhouse.CompiledTimeline,
) ([]queryexec.TimelineBucket, error) {
	executor.sealed = compiled.HasValidExecutionSeal()
	return timelineRows(compiled.Spec), nil
}

func TestCompletedSearchAnalysesRestoreSealedLookupAuthority(t *testing.T) {
	template := fieldTestSnapshot("retained-lookup-analysis")
	template.AppID = searchAnalysisAuthorityAppID
	template.SPL = `index=main | lookup service_catalog service_id AS service OUTPUT owner AS service_owner`
	fixture := newSearchAnalysisKnowledgeFixture(t, template)
	resolution := retainedSearchAnalysisLookupResolution(t, template.TenantID)
	lookupResolver := searchAnalysisLookupResolverFunc(func(
		ctx context.Context,
		scope searchjobs.LookupAdmissionResolutionScope,
	) (searchjobs.LookupAdmissionResolution, error) {
		if err := ctx.Err(); err != nil {
			return searchjobs.LookupAdmissionResolution{}, err
		}
		if scope.TenantID != template.TenantID || scope.AppID != template.AppID ||
			len(scope.Names) != 1 || scope.Names[0] != "service_catalog" {
			return searchjobs.LookupAdmissionResolution{}, errors.New("unexpected lookup resolution scope")
		}
		return searchjobs.LookupAdmissionResolution{
			Explicit: []clickhouse.LookupResolution{resolution},
		}, nil
	})
	snapshot, err := sealSearchAnalysisSnapshotWithAuthorities(
		template,
		fixture.resolver,
		lookupResolver,
		clickhouse.Compiler{},
	)
	if err != nil {
		t.Fatalf("seal lookup-bearing snapshot: %v", err)
	}
	retained, err := snapshot.OpenRetainedKnowledgeExecution()
	if err != nil || retained == nil || !retained.CompiledQuery.HasLookupAuthority() {
		t.Fatalf("retained lookup authority = (%#v, %v)", retained, err)
	}
	access := fieldAccess(snapshot)

	fieldExecutor := &retainedLookupFieldExecutor{}
	fieldService := newFieldTestService(t, FieldConfig{
		Searches: &rawSearchAnalysisSnapshots{
			snapshots: []searchjobs.ExecutionSnapshot{snapshot},
		},
		Compiler:  clickhouse.Compiler{},
		Executor:  fieldExecutor,
		CursorKey: fieldTestCursorKey,
		Clock: func() time.Time {
			return snapshot.FinishedAt.Add(time.Second)
		},
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if closeErr := fieldService.Close(ctx); closeErr != nil {
			t.Errorf("FieldService.Close(): %v", closeErr)
		}
	})
	page, err := fieldService.ListFields(
		context.Background(),
		access,
		ListFieldsRequest{SearchJobID: snapshot.ID},
	)
	if err != nil || len(page.Fields) != 1 ||
		page.Fields[0].FieldName != "service_owner" || !fieldExecutor.catalogSealed {
		t.Fatalf("ListFields(retained lookup) = (%#v, %v), sealed %v", page, err, fieldExecutor.catalogSealed)
	}
	summary, err := fieldService.GetFieldSummary(
		context.Background(),
		access,
		GetFieldSummaryRequest{
			SearchJobID: snapshot.ID,
			FieldName:   "service_owner",
		},
	)
	topValue, topValueOK := "", false
	if len(summary.TopValues) == 1 {
		topValue, topValueOK = summary.TopValues[0].Value.String()
	}
	if err != nil || summary.Profile.FieldName != "service_owner" ||
		!topValueOK || topValue != "platform" || !fieldExecutor.summarySealed {
		t.Fatalf("GetFieldSummary(retained lookup) = (%#v, %v), sealed %v", summary, err, fieldExecutor.summarySealed)
	}

	timelineExecutor := &retainedLookupTimelineExecutor{}
	timelineService := newTimelineTestService(t, Config{
		Searches: &rawSearchAnalysisSnapshots{
			snapshots: []searchjobs.ExecutionSnapshot{snapshot},
		},
		Compiler: clickhouse.Compiler{},
		Executor: timelineExecutor,
	})
	result, err := timelineService.Get(
		context.Background(),
		access,
		Request{SearchJobID: snapshot.ID},
	)
	if err != nil || !result.Complete || !timelineExecutor.sealed {
		t.Fatalf("Get timeline(retained lookup) = (%#v, %v), sealed %v", result, err, timelineExecutor.sealed)
	}
}

func retainedSearchAnalysisLookupResolution(
	t *testing.T,
	tenantID string,
) clickhouse.LookupResolution {
	t.Helper()
	key, err := plan.ResolveField("service", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(service): %v", err)
	}
	output, err := plan.ResolveField("service_owner", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(service_owner): %v", err)
	}
	contract := plan.Lookup{
		DefinitionName: "service_catalog",
		Keys: []plan.LookupKey{{
			LookupField: "service_id",
			EventField:  key,
		}},
		Outputs: []plan.LookupOutput{{
			LookupField: "owner",
			EventField:  output,
		}},
		WriteMode: plan.LookupWriteModeOverwrite,
	}
	resolution, err := clickhouse.NewLookupResolutionWithContract(
		contract,
		"lookup-service-catalog",
		1,
		tenantID,
		"asset-service-catalog",
		1,
		uint64(len("service_id,owner\napi,platform\n")),
		sha256.Sum256([]byte("retained-search-analysis-lookup")),
		[]string{"service_id", "owner"},
		[][]string{{"api", "platform"}},
	)
	if err != nil {
		t.Fatalf("NewLookupResolutionWithContract(): %v", err)
	}
	return resolution
}
