package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchinspection"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/gradethiscorpus"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"github.com/Suhaibinator/open-splunk/migrations"
	"google.golang.org/protobuf/proto"
)

const gradeThisInspectionRouteBearerToken = "open-splunk-gradethis-inspection-route-token-0123456789"

// TestSearchInspectionRouteGradeThisAgainstClickHouse proves that the
// administrator-only HTTP projection preserves every field returned by the
// real inspection service for all ten canonical GradeThis searches.
func TestSearchInspectionRouteGradeThisAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip(
			"set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test",
		)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	container, err := testsupport.StartClickHouse(
		ctx,
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close GradeThis inspection route fixture: %v", closeErr)
		}
	})

	options := &clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf(
				"close GradeThis inspection route query connection: %v",
				closeErr,
			)
		}
	})
	if err := connection.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ApplyClickHouseMigrations(
		ctx,
		connection,
		migrations.ClickHouse(),
	); err != nil {
		t.Fatal(err)
	}
	if err := gradethiscorpus.StopInspectionMerges(ctx, connection); err != nil {
		t.Fatal(err)
	}

	storeConnection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	controlDB, err := control.Open(
		ctx,
		filepath.Join(t.TempDir(), "visibility.sqlite"),
	)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := controlDB.Close(); closeErr != nil {
			t.Errorf(
				"close GradeThis inspection route control DB: %v",
				closeErr,
			)
		}
	})
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := sequencer.Close(); closeErr != nil {
			t.Errorf(
				"close GradeThis inspection route visibility sequencer: %v",
				closeErr,
			)
		}
	})
	store, err := clickhouse.NewStore(
		storeConnection,
		clickhouse.RetentionProviderFunc(
			func(context.Context, string, string) (time.Duration, error) {
				return 100 * 365 * 24 * time.Hour, nil
			},
		),
		sequencer,
	)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close GradeThis inspection route store: %v", closeErr)
		}
	})

	const (
		tenantID = "tenant"
		ownerID  = "owner"
	)
	stored, err := gradethiscorpus.StoreCanonical(ctx, store, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	profile := stored.Profile
	if err := gradethiscorpus.StoreInspectionLoad(
		ctx,
		connection,
		profile,
		tenantID,
	); err != nil {
		t.Fatal(err)
	}
	layout, err := gradethiscorpus.ReadInspectionPartLayout(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	if err := gradethiscorpus.ValidateInspectionPartLayout(
		profile,
		layout,
	); err != nil {
		t.Fatal(err)
	}

	cutoff, err := sequencer.Cutoff(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cutoff != 1 {
		t.Fatalf("GradeThis inspection visibility cutoff = %d, want 1", cutoff)
	}
	executor, err := queryexec.New(connection, queryexec.Config{})
	if err != nil {
		t.Fatal(err)
	}
	resolvedRange, err := searchtime.NewAbsoluteRange(
		profile.BaseTime,
		profile.BaseTime.Add(15*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	var nextID atomic.Uint64
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: executor,
		Snapshotter: gradeThisInspectionRouteSnapshotter(
			cutoff,
		),
		Compiler:        clickhouse.Compiler{},
		MaxConcurrent:   1,
		MaxQueued:       1,
		CleanupInterval: -1,
		Now: func() time.Time {
			return profile.IndexTime.Add(500 * time.Microsecond)
		},
		NewID: func() string {
			return fmt.Sprintf(
				"gradethis-inspection-route-%02d",
				nextID.Add(1),
			)
		},
		CursorKey: []byte(
			"0123456789abcdef0123456789abcdef",
		),
		CursorScope: "gradethis-inspection-route-corpus-v0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := manager.Close(); closeErr != nil {
			t.Errorf("close GradeThis inspection route manager: %v", closeErr)
		}
	})

	explainer, err := queryexec.NewExplainer(options, queryexec.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := explainer.Close(); closeErr != nil {
			t.Errorf(
				"close GradeThis inspection route Explainer: %v",
				closeErr,
			)
		}
	})

	corpus := gradethiscorpus.Searches()
	if len(corpus) != 10 {
		t.Fatalf("GradeThis inspection corpus size = %d, want 10", len(corpus))
	}
	snapshots := &gradeThisInspectionRouteCompletedSearches{
		manager: manager,
		calls:   make(map[string]int, len(corpus)),
	}
	service, err := searchinspection.New(searchinspection.Config{
		Searches:  snapshots,
		Compiler:  clickhouse.Compiler{},
		Explainer: explainer,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer closeCancel()
		if closeErr := service.Close(closeContext); closeErr != nil {
			t.Errorf(
				"close GradeThis inspection route service: %v",
				closeErr,
			)
		}
	})
	recordingService := &gradeThisInspectionRouteRecorder{
		service: service,
		results: make(
			map[string]searchinspection.Result,
			len(corpus),
		),
	}
	browserAuthenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(gradeThisInspectionRouteBearerToken),
		tenantID,
		ownerID,
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, Config{
		SearchJobs:           manager,
		SearchInspections:    recordingService,
		Indexes:              fakeIndexCatalog{},
		BrowserAuthenticator: browserAuthenticator,
		WebUI:                testUI(),
		TenantID:             tenantID,
		OwnerID:              ownerID,
	})

	queryIDs := make(map[string]struct{}, len(corpus))
	for _, search := range corpus {
		source, renderErr := search.Render(profile.TraceID)
		if renderErr != nil {
			t.Fatal(renderErr)
		}
		created, createErr := manager.Create(
			ctx,
			searchjobs.CreateRequest{
				SPL:      source,
				OwnerID:  ownerID,
				TenantID: tenantID,
				AuthorizedIndexes: []string{
					gradethiscorpus.IndexName,
				},
				RequestedIndexes: []string{
					gradethiscorpus.IndexName,
				},
				TimeRange: resolvedRange,
			},
		)
		if createErr != nil {
			t.Fatal(createErr)
		}
		terminal := waitForGradeThisInspectionRouteJob(
			t,
			ctx,
			manager,
			created.ID,
		)
		if terminal.State != searchjobs.StateCompleted ||
			terminal.Failure != nil {
			t.Fatalf(
				"GradeThis inspection route source job %q = state %v failure %#v",
				search.ID,
				terminal.State,
				terminal.Failure,
			)
		}

		response := postGradeThisInspectionRoute(
			t,
			ctx,
			handler,
			created.ID,
		)
		if response.Code != http.StatusOK {
			t.Fatalf(
				"GradeThis inspection route %q status = %d, body = %s",
				search.ID,
				response.Code,
				response.Body.String(),
			)
		}
		var decoded opensplunkv1.InspectSearchJobResponse
		if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
			t.Fatalf(
				"decode GradeThis inspection route %q response: %v",
				search.ID,
				err,
			)
		}
		recorded, ok := recordingService.result(created.ID)
		if !ok {
			t.Fatalf(
				"GradeThis inspection route %q did not record its exact result",
				search.ID,
			)
		}
		assertGradeThisInspectionRouteResponse(
			t,
			&decoded,
			created.ID,
			recorded,
		)
		if snapshots.callCount(created.ID) != 2 {
			t.Fatalf(
				"GradeThis inspection route %q snapshot reads = %d, want 2",
				search.ID,
				snapshots.callCount(created.ID),
			)
		}
		if _, duplicate := queryIDs[decoded.GetDiagnosticQueryId()]; duplicate {
			t.Fatalf(
				"GradeThis inspection route reused diagnostic query ID %q",
				decoded.GetDiagnosticQueryId(),
			)
		}
		queryIDs[decoded.GetDiagnosticQueryId()] = struct{}{}
	}

	if len(queryIDs) != len(corpus) {
		t.Fatalf(
			"GradeThis inspection route unique query IDs = %d, want %d",
			len(queryIDs),
			len(corpus),
		)
	}
	if snapshots.totalCalls() != 2*len(corpus) {
		t.Fatalf(
			"GradeThis inspection route total snapshot reads = %d, want %d",
			snapshots.totalCalls(),
			2*len(corpus),
		)
	}
	if recordingService.callCount() != len(corpus) {
		t.Fatalf(
			"GradeThis inspection route calls = %d, want %d",
			recordingService.callCount(),
			len(corpus),
		)
	}
}

type gradeThisInspectionRouteSnapshotter uint64

func (snapshotter gradeThisInspectionRouteSnapshotter) VisibilityCutoff(
	ctx context.Context,
) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return uint64(snapshotter), nil
}

type gradeThisInspectionRouteCompletedSearches struct {
	manager *searchjobs.Manager
	mu      sync.Mutex
	calls   map[string]int
	total   int
}

func (searches *gradeThisInspectionRouteCompletedSearches) CompletedExecutionSnapshotFor(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (searchjobs.ExecutionSnapshot, error) {
	snapshot, err := searches.manager.CompletedExecutionSnapshotFor(
		ctx,
		access,
		id,
	)
	if err != nil {
		return searchjobs.ExecutionSnapshot{}, err
	}
	searches.mu.Lock()
	searches.calls[id]++
	searches.total++
	searches.mu.Unlock()
	return snapshot, nil
}

func (searches *gradeThisInspectionRouteCompletedSearches) callCount(
	id string,
) int {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	return searches.calls[id]
}

func (searches *gradeThisInspectionRouteCompletedSearches) totalCalls() int {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	return searches.total
}

type gradeThisInspectionRouteRecorder struct {
	service *searchinspection.Service
	mu      sync.Mutex
	calls   int
	results map[string]searchinspection.Result
}

func TestGradeThisInspectionRouteResultCloneIsIndependent(t *testing.T) {
	t.Parallel()

	original := searchinspection.Result{
		Plan: searchinspection.LogicalPlan{
			Stages: []searchinspection.PlanStage{
				{
					Index:        0,
					Operator:     "Scan",
					InputFields:  []string{"input"},
					OutputFields: []string{"output"},
					SourceRange: searchinspection.SourceRange{
						Start: searchinspection.SourcePosition{
							ByteOffset: 0,
							Line:       1,
							Column:     1,
						},
						End: searchinspection.SourcePosition{
							ByteOffset: 4,
							Line:       1,
							Column:     5,
						},
					},
				},
			},
			ReferencedFields: []string{"referenced"},
			Output: searchinspection.OutputShape{
				Kind:             searchinspection.OutputKindDynamic,
				Fields:           []string{"fixed"},
				MaxDynamicFields: 8,
			},
		},
		PhysicalPlan: queryexec.ExplainPlan{
			NodeTypes: []string{"ReadFromMergeTree"},
			Reads: []queryexec.ExplainRead{
				{
					Columns: []string{"body"},
					Indexes: []queryexec.ExplainIndex{
						{
							Type:             "Skip",
							Name:             "idx_raw_text",
							Keys:             []string{"raw"},
							InitialParts:     5,
							SelectedParts:    1,
							InitialGranules:  10,
							SelectedGranules: 2,
						},
					},
				},
			},
		},
		GeneratedSQL:      "SELECT body",
		ExplainText:       `{"Plan":{"Node Type":"ReadFromMergeTree"}}`,
		DiagnosticQueryID: "open-splunk-explain-clone",
	}
	cloned := cloneGradeThisInspectionRouteResult(original)

	original.Plan.Stages[0].Index = 9
	original.Plan.Stages[0].Operator = "Filter"
	original.Plan.Stages[0].InputFields[0] = "mutated-input"
	original.Plan.Stages[0].OutputFields[0] = "mutated-output"
	original.Plan.Stages[0].SourceRange.End.ByteOffset = 99
	original.Plan.ReferencedFields[0] = "mutated-reference"
	original.Plan.Output.Fields[0] = "mutated-fixed"
	original.PhysicalPlan.NodeTypes[0] = "Filter"
	original.PhysicalPlan.Reads[0].Columns[0] = "raw"
	original.PhysicalPlan.Reads[0].Indexes[0].Name = "idx_trace_id"
	original.PhysicalPlan.Reads[0].Indexes[0].Keys[0] = "trace_id"
	original.PhysicalPlan.Reads[0].Indexes[0].InitialParts = 99
	original.GeneratedSQL = "mutated SQL"
	original.ExplainText = "mutated EXPLAIN"
	original.DiagnosticQueryID = "mutated-query-id"

	stage := cloned.Plan.Stages[0]
	physicalIndex := cloned.PhysicalPlan.Reads[0].Indexes[0]
	if stage.Index != 0 ||
		stage.Operator != "Scan" ||
		!slices.Equal(stage.InputFields, []string{"input"}) ||
		!slices.Equal(stage.OutputFields, []string{"output"}) ||
		stage.SourceRange.End.ByteOffset != 4 ||
		!slices.Equal(
			cloned.Plan.ReferencedFields,
			[]string{"referenced"},
		) ||
		!slices.Equal(cloned.Plan.Output.Fields, []string{"fixed"}) ||
		!slices.Equal(
			cloned.PhysicalPlan.NodeTypes,
			[]string{"ReadFromMergeTree"},
		) ||
		!slices.Equal(
			cloned.PhysicalPlan.Reads[0].Columns,
			[]string{"body"},
		) ||
		physicalIndex.Name != "idx_raw_text" ||
		!slices.Equal(physicalIndex.Keys, []string{"raw"}) ||
		physicalIndex.InitialParts != 5 ||
		cloned.GeneratedSQL != "SELECT body" ||
		cloned.ExplainText !=
			`{"Plan":{"Node Type":"ReadFromMergeTree"}}` ||
		cloned.DiagnosticQueryID != "open-splunk-explain-clone" {
		t.Fatalf("detached GradeThis inspection result changed: %#v", cloned)
	}
}

func (recorder *gradeThisInspectionRouteRecorder) Inspect(
	ctx context.Context,
	access searchjobs.AccessScope,
	request searchinspection.Request,
) (searchinspection.Result, error) {
	result, err := recorder.service.Inspect(ctx, access, request)
	if err != nil {
		return searchinspection.Result{}, err
	}
	recorder.mu.Lock()
	recorder.calls++
	recorder.results[request.SearchJobID] =
		cloneGradeThisInspectionRouteResult(result)
	recorder.mu.Unlock()
	return result, nil
}

func (recorder *gradeThisInspectionRouteRecorder) result(
	jobID string,
) (searchinspection.Result, bool) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	result, ok := recorder.results[jobID]
	return result, ok
}

func (recorder *gradeThisInspectionRouteRecorder) callCount() int {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return recorder.calls
}

func cloneGradeThisInspectionRouteResult(
	result searchinspection.Result,
) searchinspection.Result {
	cloned := searchinspection.Result{
		Plan: searchinspection.LogicalPlan{
			Stages: cloneGradeThisInspectionRouteStages(
				result.Plan.Stages,
			),
			ReferencedFields: cloneGradeThisInspectionRouteStrings(
				result.Plan.ReferencedFields,
			),
			Output: searchinspection.OutputShape{
				Kind: result.Plan.Output.Kind,
				Fields: cloneGradeThisInspectionRouteStrings(
					result.Plan.Output.Fields,
				),
				MaxDynamicFields: result.Plan.Output.MaxDynamicFields,
			},
		},
		PhysicalPlan: queryexec.ExplainPlan{
			NodeTypes: cloneGradeThisInspectionRouteStrings(
				result.PhysicalPlan.NodeTypes,
			),
			Reads: cloneGradeThisInspectionRouteReads(
				result.PhysicalPlan.Reads,
			),
		},
		GeneratedSQL:      strings.Clone(result.GeneratedSQL),
		ExplainText:       strings.Clone(result.ExplainText),
		DiagnosticQueryID: strings.Clone(result.DiagnosticQueryID),
	}
	return cloned
}

func cloneGradeThisInspectionRouteStages(
	stages []searchinspection.PlanStage,
) []searchinspection.PlanStage {
	if stages == nil {
		return nil
	}
	cloned := make([]searchinspection.PlanStage, len(stages))
	for index, stage := range stages {
		cloned[index] = stage
		cloned[index].Operator = strings.Clone(stage.Operator)
		cloned[index].InputFields =
			cloneGradeThisInspectionRouteStrings(stage.InputFields)
		cloned[index].OutputFields =
			cloneGradeThisInspectionRouteStrings(stage.OutputFields)
	}
	return cloned
}

func cloneGradeThisInspectionRouteReads(
	reads []queryexec.ExplainRead,
) []queryexec.ExplainRead {
	if reads == nil {
		return nil
	}
	cloned := make([]queryexec.ExplainRead, len(reads))
	for readIndex, read := range reads {
		cloned[readIndex].Columns =
			cloneGradeThisInspectionRouteStrings(read.Columns)
		if read.Indexes == nil {
			continue
		}
		cloned[readIndex].Indexes = make(
			[]queryexec.ExplainIndex,
			len(read.Indexes),
		)
		for index, physicalIndex := range read.Indexes {
			cloned[readIndex].Indexes[index] = physicalIndex
			cloned[readIndex].Indexes[index].Type =
				strings.Clone(physicalIndex.Type)
			cloned[readIndex].Indexes[index].Name =
				strings.Clone(physicalIndex.Name)
			cloned[readIndex].Indexes[index].Keys =
				cloneGradeThisInspectionRouteStrings(
					physicalIndex.Keys,
				)
		}
	}
	return cloned
}

func cloneGradeThisInspectionRouteStrings(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	for index, value := range values {
		cloned[index] = strings.Clone(value)
	}
	return cloned
}

func waitForGradeThisInspectionRouteJob(
	t *testing.T,
	ctx context.Context,
	manager *searchjobs.Manager,
	id string,
) searchjobs.Job {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if job.State.Terminal() {
			return job
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatal("GradeThis inspection route source job did not terminate")
		case <-ticker.C:
		}
	}
}

func postGradeThisInspectionRoute(
	t *testing.T,
	ctx context.Context,
	handler http.Handler,
	jobID string,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(&opensplunkv1.InspectSearchJobRequest{
		SearchJobId: jobID,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"/api/v1/search/jobs/inspect",
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set(
		"Authorization",
		"Bearer "+gradeThisInspectionRouteBearerToken,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertGradeThisInspectionRouteResponse(
	t *testing.T,
	actual *opensplunkv1.InspectSearchJobResponse,
	jobID string,
	expected searchinspection.Result,
) {
	t.Helper()
	want := &opensplunkv1.InspectSearchJobResponse{
		SearchJobId:       jobID,
		LogicalPlan:       gradeThisInspectionRouteLogicalPlan(expected.Plan),
		PhysicalPlan:      gradeThisInspectionRoutePhysicalPlan(expected.PhysicalPlan),
		GeneratedSql:      expected.GeneratedSQL,
		ExplainText:       expected.ExplainText,
		DiagnosticQueryId: expected.DiagnosticQueryID,
	}
	if !proto.Equal(actual, want) {
		t.Fatalf(
			"GradeThis inspection route projection differs from the exact service result:\nactual: %s\nwant: %s",
			actual,
			want,
		)
	}
}

func gradeThisInspectionRouteLogicalPlan(
	logical searchinspection.LogicalPlan,
) *opensplunkv1.SearchInspectionLogicalPlan {
	stages := make(
		[]*opensplunkv1.SearchInspectionLogicalStage,
		len(logical.Stages),
	)
	for index, stage := range logical.Stages {
		stages[index] = &opensplunkv1.SearchInspectionLogicalStage{
			StageIndex:   stage.Index,
			Operator:     stage.Operator,
			InputFields:  slices.Clone(stage.InputFields),
			OutputFields: slices.Clone(stage.OutputFields),
			SourceRange: &opensplunkv1.SourceRange{
				Start: &opensplunkv1.SourcePosition{
					ByteOffset: stage.SourceRange.Start.ByteOffset,
					Line:       stage.SourceRange.Start.Line,
					Column:     stage.SourceRange.Start.Column,
				},
				End: &opensplunkv1.SourcePosition{
					ByteOffset: stage.SourceRange.End.ByteOffset,
					Line:       stage.SourceRange.End.Line,
					Column:     stage.SourceRange.End.Column,
				},
			},
		}
	}
	return &opensplunkv1.SearchInspectionLogicalPlan{
		Stages:           stages,
		ReferencedFields: slices.Clone(logical.ReferencedFields),
		Output: &opensplunkv1.SearchInspectionOutputShape{
			Kind:   gradeThisInspectionRouteOutputKind(logical.Output.Kind),
			Fields: slices.Clone(logical.Output.Fields),
			MaxDynamicFields: uint32(
				logical.Output.MaxDynamicFields,
			),
		},
	}
}

func gradeThisInspectionRouteOutputKind(
	kind searchinspection.OutputKind,
) opensplunkv1.SearchInspectionOutputKind {
	switch kind {
	case searchinspection.OutputKindOpen:
		return opensplunkv1.SearchInspectionOutputKind_SEARCH_INSPECTION_OUTPUT_KIND_OPEN
	case searchinspection.OutputKindStatic:
		return opensplunkv1.SearchInspectionOutputKind_SEARCH_INSPECTION_OUTPUT_KIND_STATIC
	case searchinspection.OutputKindDynamic:
		return opensplunkv1.SearchInspectionOutputKind_SEARCH_INSPECTION_OUTPUT_KIND_DYNAMIC
	default:
		return opensplunkv1.SearchInspectionOutputKind_SEARCH_INSPECTION_OUTPUT_KIND_UNSPECIFIED
	}
}

func gradeThisInspectionRoutePhysicalPlan(
	physical queryexec.ExplainPlan,
) *opensplunkv1.SearchInspectionPhysicalPlan {
	reads := make(
		[]*opensplunkv1.SearchInspectionPhysicalRead,
		len(physical.Reads),
	)
	for readIndex, read := range physical.Reads {
		indexes := make(
			[]*opensplunkv1.SearchInspectionPhysicalIndex,
			len(read.Indexes),
		)
		for index, physicalIndex := range read.Indexes {
			indexes[index] = &opensplunkv1.SearchInspectionPhysicalIndex{
				Type:             physicalIndex.Type,
				Name:             physicalIndex.Name,
				Keys:             slices.Clone(physicalIndex.Keys),
				InitialParts:     physicalIndex.InitialParts,
				SelectedParts:    physicalIndex.SelectedParts,
				InitialGranules:  physicalIndex.InitialGranules,
				SelectedGranules: physicalIndex.SelectedGranules,
			}
		}
		reads[readIndex] = &opensplunkv1.SearchInspectionPhysicalRead{
			Columns: slices.Clone(read.Columns),
			Indexes: indexes,
		}
	}
	return &opensplunkv1.SearchInspectionPhysicalPlan{
		NodeTypes: slices.Clone(physical.NodeTypes),
		Reads:     reads,
	}
}
