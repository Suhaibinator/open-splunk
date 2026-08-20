package searchinspection

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/gradethiscorpus"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

type gradeThisInspectionIndexCounts struct {
	initialParts     uint64
	selectedParts    uint64
	initialGranules  uint64
	selectedGranules uint64
}

type gradeThisInspectionSkipEvidence struct {
	name   string
	keys   []string
	counts gradeThisInspectionIndexCounts
}

type gradeThisInspectionPlanSummary struct {
	columns    []string
	minMax     gradeThisInspectionIndexCounts
	partition  gradeThisInspectionIndexCounts
	primaryKey gradeThisInspectionIndexCounts
	skips      []gradeThisInspectionSkipEvidence
}

var (
	errGradeThisInspectionReadCount = errors.New(
		"GradeThis inspection plan must contain exactly one MergeTree read",
	)
	errGradeThisInspectionColumns = errors.New(
		"GradeThis inspection plan has unexpected physical columns",
	)
	errGradeThisInspectionMinMax = errors.New(
		"GradeThis inspection plan has invalid MinMax evidence",
	)
	errGradeThisInspectionPartition = errors.New(
		"GradeThis inspection plan has invalid partition evidence",
	)
	errGradeThisInspectionPrimaryKey = errors.New(
		"GradeThis inspection plan has invalid primary-key evidence",
	)
	errGradeThisInspectionSkip = errors.New(
		"GradeThis inspection plan has unexpected skip-index evidence",
	)
	errGradeThisInspectionIndex = errors.New(
		"GradeThis inspection plan has unexpected index evidence",
	)
	errGradeThisInspectionSearch = errors.New(
		"GradeThis inspection plan has no corpus contract",
	)
)

var gradeThisInspectionExpectedColumns = map[gradethiscorpus.SearchID][]string{
	gradethiscorpus.SearchFollowTrace: {
		"_part_offset",
		"_part_starting_offset",
		"batch_id",
		"batch_sequence",
		"collector_id",
		"event_id",
		"event_time",
		"index_name",
		"visibility_seq",
	},
	gradethiscorpus.SearchErrorsAndWarnings: {
		"_part_offset",
		"_part_starting_offset",
		"batch_id",
		"batch_sequence",
		"collector_id",
		"event_id",
		"event_time",
		"index_name",
		"index_time",
		"level",
		"visibility_seq",
	},
	gradethiscorpus.SearchRawErrorFragment: {
		"batch_id",
		"batch_sequence",
		"body",
		"collector_id",
		"event_id",
		"event_time",
		"fields",
		"index_name",
		"level",
		"trace_id",
		"visibility_seq",
	},
	gradethiscorpus.SearchSeverityCounts: {
		"level",
	},
	gradethiscorpus.SearchFrequentErrors: {
		"body",
		"field_names",
		"fields",
	},
	gradethiscorpus.SearchVolumeBySeverity: {
		"event_time",
		"level",
	},
	gradethiscorpus.SearchServerErrors: {
		"event_time",
		"field_names",
		"fields",
	},
	gradethiscorpus.SearchResponses: {
		"field_names",
		"fields",
	},
	gradethiscorpus.SearchSlowRoutes: {
		"field_names",
		"fields",
	},
	gradethiscorpus.SearchTopMessages: {
		"body",
	},
}

// TestGradeThisInspectionServiceAgainstClickHouse proves that pipeline commands exact
// compatibility snapshots traverse the real internal Service, compiler, and
// Explainer against the deterministic five-part load fixture.
func TestGradeThisInspectionServiceAgainstClickHouse(t *testing.T) {
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
		if err := container.Close(cleanupContext); err != nil {
			t.Errorf("close GradeThis inspection fixture: %v", err)
		}
	})
	migrateInspectionClickHouse(t, ctx, container)

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
		if err := connection.Close(); err != nil {
			t.Errorf("close GradeThis inspection connection: %v", err)
		}
	})
	if err := connection.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	if err := gradethiscorpus.StopInspectionMerges(
		ctx,
		connection,
	); err != nil {
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
		if err := controlDB.Close(); err != nil {
			t.Errorf("close GradeThis inspection control DB: %v", err)
		}
	})
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sequencer.Close(); err != nil {
			t.Errorf("close GradeThis inspection visibility sequencer: %v", err)
		}
	})
	store, err := clickhouse.NewStore(
		storeConnection,
		clickhouse.RetentionProviderFunc(
			func(
				context.Context,
				string,
				string,
			) (time.Duration, error) {
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
		if err := store.Close(); err != nil {
			t.Errorf("close GradeThis inspection store: %v", err)
		}
	})
	stored, err := gradethiscorpus.StoreCanonical(ctx, store, "tenant")
	if err != nil {
		t.Fatal(err)
	}
	profile := stored.Profile
	if err := gradethiscorpus.StoreInspectionLoad(
		ctx,
		connection,
		profile,
		"tenant",
	); err != nil {
		t.Fatal(err)
	}
	layout, err := gradethiscorpus.ReadInspectionPartLayout(
		ctx,
		connection,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := gradethiscorpus.ValidateInspectionPartLayout(
		profile,
		layout,
	); err != nil {
		t.Fatal(err)
	}
	t.Logf("GradeThis inspection part layout: %#v", layout)

	cutoff, err := sequencer.Cutoff(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cutoff != 1 {
		t.Fatal("GradeThis inspection visibility cutoff changed")
	}
	executor, err := queryexec.New(connection, queryexec.Config{
		ReadAdmission: indexread.UnfencedAdmission{},
	})
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
		Snapshotter: gradeThisInspectionSnapshotter(
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
				"gradethis-inspection-%02d",
				nextID.Add(1),
			)
		},
		CursorKey: []byte(
			"0123456789abcdef0123456789abcdef",
		),
		CursorScope: "gradethis-inspection-corpus",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close GradeThis inspection manager: %v", err)
		}
	})

	explainer, err := queryexec.NewExplainer(options, queryexec.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := explainer.Close(); err != nil {
			t.Errorf("close GradeThis inspection Explainer: %v", err)
		}
	})

	corpus := gradethiscorpus.Searches()
	if len(corpus) != 10 {
		t.Fatal("GradeThis inspection corpus must contain ten searches")
	}
	searches := &gradeThisInspectionCompletedSearches{
		manager: manager,
		calls:   make(map[string]int, len(corpus)),
	}
	compiler := &inspectionCompiler{}
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches:  searches,
		Compiler:  compiler,
		Explainer: explainer,
	})

	evidence := make(
		map[gradethiscorpus.SearchID]gradeThisInspectionPlanSummary,
		len(corpus),
	)
	queryIDs := make(map[string]struct{}, len(corpus))
	for _, search := range corpus {
		t.Run(string(search.ID), func(t *testing.T) {
			source, err := search.Render(profile.TraceID)
			if err != nil {
				t.Fatal(err)
			}
			created, err := manager.Create(
				ctx,
				searchjobs.CreateRequest{
					SPL:      source,
					OwnerID:  "owner",
					TenantID: "tenant",
					AuthorizedIndexes: []string{
						gradethiscorpus.IndexName,
					},
					RequestedIndexes: []string{
						gradethiscorpus.IndexName,
					},
					TimeRange: resolvedRange,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			terminal := waitForGradeThisInspectionJob(
				t,
				manager,
				created.ID,
			)
			if terminal.State != searchjobs.StateCompleted ||
				terminal.Failure != nil {
				t.Fatal("GradeThis inspection source job did not complete")
			}
			jobID := created.ID
			result, err := service.Inspect(
				ctx,
				searchjobs.AccessScope{
					TenantID: "tenant",
					OwnerID:  "owner",
				},
				Request{SearchJobID: jobID},
			)
			if err != nil {
				t.Fatal(err)
			}
			if searches.callCount(jobID) != 2 {
				t.Fatal("GradeThis inspection did not perform preflight and postflight snapshot reads")
			}
			if result.GeneratedSQL == "" ||
				!strings.Contains(
					result.GeneratedSQL,
					`"open_splunk"."events"`,
				) {
				t.Fatal("GradeThis inspection did not publish generated storage SQL")
			}
			for _, private := range []string{
				profile.TraceID,
				"connection refused",
				"Request metrics",
				gradethiscorpus.IndexName,
			} {
				if strings.Contains(result.GeneratedSQL, private) {
					t.Fatal("GradeThis generated SQL retained a bound value")
				}
			}
			if !strings.HasPrefix(
				result.DiagnosticQueryID,
				"open-splunk-explain-",
			) {
				t.Fatal("GradeThis inspection returned an invalid diagnostic query ID")
			}
			if _, duplicate := queryIDs[result.DiagnosticQueryID]; duplicate {
				t.Fatal("GradeThis inspection reused a diagnostic query ID")
			}
			queryIDs[result.DiagnosticQueryID] = struct{}{}

			reparsed, err := queryexec.ParseExplainPlan(
				queryexec.ExplainResult{
					Text:    result.ExplainText,
					QueryID: result.DiagnosticQueryID,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.PhysicalPlan, reparsed) {
				t.Fatal("GradeThis service physical plan changed after independent parsing")
			}
			summary, err := gradeThisSummarizeInspectionPlan(
				result.PhysicalPlan,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := gradeThisValidateInspectionSummary(
				search.ID,
				summary,
			); err != nil {
				t.Fatal(err)
			}
			if _, duplicate := evidence[search.ID]; duplicate {
				t.Fatal("GradeThis inspection produced duplicate corpus evidence")
			}
			evidence[search.ID] = summary

			if len(result.Plan.Stages) == 0 {
				t.Fatal("GradeThis inspection published an empty logical plan")
			}
			for _, stage := range result.Plan.Stages {
				if stage.SourceRange.End.ByteOffset >
					uint64(len(source)) {
					t.Fatal("GradeThis logical plan source range exceeded the exact SPL")
				}
			}
			safeProjection := fmt.Sprintf(
				"%#v %#v",
				result.Plan,
				result.PhysicalPlan,
			)
			for _, private := range []string{
				profile.TraceID,
				"connection refused",
				"Request metrics",
				gradethiscorpus.IndexName,
			} {
				if strings.Contains(safeProjection, private) {
					t.Fatal("GradeThis safe inspection projection retained a bound value")
				}
			}
			skipNames := make([]string, len(summary.skips))
			for index, skip := range summary.skips {
				skipNames[index] = skip.name
			}
			t.Logf(
				"GradeThis inspection evidence: search=%s "+
					"columns=%s skips=%s "+
					"minmax_parts=%d->%d minmax_granules=%d->%d "+
					"partition_parts=%d->%d partition_granules=%d->%d "+
					"primary_key_parts=%d->%d primary_key_granules=%d->%d",
				search.ID,
				strings.Join(summary.columns, ","),
				strings.Join(skipNames, ","),
				summary.minMax.initialParts,
				summary.minMax.selectedParts,
				summary.minMax.initialGranules,
				summary.minMax.selectedGranules,
				summary.partition.initialParts,
				summary.partition.selectedParts,
				summary.partition.initialGranules,
				summary.partition.selectedGranules,
				summary.primaryKey.initialParts,
				summary.primaryKey.selectedParts,
				summary.primaryKey.initialGranules,
				summary.primaryKey.selectedGranules,
			)
		})
	}
	if len(evidence) != len(corpus) ||
		len(queryIDs) != len(corpus) ||
		searches.totalCalls() != 2*len(corpus) ||
		compiler.callCount() != len(corpus) {
		t.Fatal("GradeThis inspection did not cover the exact corpus once")
	}
	for _, search := range corpus {
		if _, ok := evidence[search.ID]; !ok {
			t.Fatal("GradeThis inspection corpus evidence is incomplete")
		}
	}
}

type gradeThisInspectionSnapshotter uint64

func (snapshotter gradeThisInspectionSnapshotter) VisibilityCutoff(
	ctx context.Context,
) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return uint64(snapshotter), nil
}

type gradeThisInspectionCompletedSearches struct {
	manager *searchjobs.Manager
	mu      sync.Mutex
	calls   map[string]int
	total   int
}

func (searches *gradeThisInspectionCompletedSearches) AcquireExecutionFor(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (searchjobs.ResultLease, searchjobs.ExecutionSnapshot, error) {
	lease, snapshot, err := searches.manager.AcquireExecutionFor(ctx, access, id)
	if err != nil {
		return nil, searchjobs.ExecutionSnapshot{}, err
	}
	searches.mu.Lock()
	searches.calls[id]++
	searches.total++
	searches.mu.Unlock()
	return lease, snapshot, nil
}

func (searches *gradeThisInspectionCompletedSearches) CompletedExecutionSnapshotFor(
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

func (searches *gradeThisInspectionCompletedSearches) callCount(
	id string,
) int {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	return searches.calls[id]
}

func (searches *gradeThisInspectionCompletedSearches) totalCalls() int {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	return searches.total
}

func waitForGradeThisInspectionJob(
	t *testing.T,
	manager *searchjobs.Manager,
	id string,
) searchjobs.Job {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
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
	t.Fatal("GradeThis inspection source job did not terminate")
	return searchjobs.Job{}
}

func gradeThisSummarizeInspectionPlan(
	physical queryexec.ExplainPlan,
) (gradeThisInspectionPlanSummary, error) {
	if len(physical.Reads) != 1 {
		return gradeThisInspectionPlanSummary{},
			errGradeThisInspectionReadCount
	}

	columns := slices.Clone(physical.Reads[0].Columns)
	for _, column := range columns {
		if !gradeThisInspectionColumnAllowed(column) {
			return gradeThisInspectionPlanSummary{},
				errGradeThisInspectionColumns
		}
	}
	slices.Sort(columns)

	var (
		summary = gradeThisInspectionPlanSummary{
			columns: columns,
		}
		minMaxCount, partitionCount, primaryCount int
		skipNames                                 = make(map[string]struct{}, 2)
		visibilitySkipSeen                        bool
	)
	for _, index := range physical.Reads[0].Indexes {
		switch index.Type {
		case "MinMax":
			minMaxCount++
			if index.Name != "" ||
				!slices.Equal(index.Keys, []string{"event_time"}) ||
				index.InitialParts != 5 ||
				index.SelectedParts != 3 ||
				index.InitialGranules != 5 ||
				index.SelectedGranules != 3 {
				return gradeThisInspectionPlanSummary{},
					errGradeThisInspectionMinMax
			}
			summary.minMax = gradeThisInspectionCounts(index)
		case "Partition":
			partitionCount++
			if index.Name != "" ||
				!slices.Equal(
					index.Keys,
					[]string{"toYYYYMM(event_time)"},
				) ||
				index.InitialParts != 3 ||
				index.SelectedParts != 3 ||
				index.InitialGranules != 3 ||
				index.SelectedGranules != 3 {
				return gradeThisInspectionPlanSummary{},
					errGradeThisInspectionPartition
			}
			summary.partition = gradeThisInspectionCounts(index)
		case "PrimaryKey":
			primaryCount++
			if index.Name != "" ||
				!slices.Equal(
					index.Keys,
					[]string{"tenant_id", "index_name", "event_time"},
				) ||
				index.InitialParts != 3 ||
				index.SelectedParts != 1 ||
				index.InitialGranules != 3 ||
				index.SelectedGranules != 1 {
				return gradeThisInspectionPlanSummary{},
					errGradeThisInspectionPrimaryKey
			}
			summary.primaryKey = gradeThisInspectionCounts(index)
		case "Skip":
			if (index.Name != "idx_visibility_seq" &&
				index.Name != "idx_field_names") ||
				len(index.Keys) != 0 ||
				index.InitialParts != 1 ||
				index.SelectedParts != 1 ||
				index.InitialGranules != 1 ||
				index.SelectedGranules != 1 {
				return gradeThisInspectionPlanSummary{},
					errGradeThisInspectionSkip
			}
			if _, duplicate := skipNames[index.Name]; duplicate {
				return gradeThisInspectionPlanSummary{},
					errGradeThisInspectionSkip
			}
			skipNames[index.Name] = struct{}{}
			if index.Name == "idx_visibility_seq" {
				visibilitySkipSeen = true
			}
			summary.skips = append(
				summary.skips,
				gradeThisInspectionSkipEvidence{
					name:   index.Name,
					keys:   slices.Clone(index.Keys),
					counts: gradeThisInspectionCounts(index),
				},
			)
		default:
			return gradeThisInspectionPlanSummary{},
				errGradeThisInspectionIndex
		}
	}
	if minMaxCount != 1 {
		return gradeThisInspectionPlanSummary{},
			errGradeThisInspectionMinMax
	}
	if partitionCount != 1 {
		return gradeThisInspectionPlanSummary{},
			errGradeThisInspectionPartition
	}
	if primaryCount != 1 {
		return gradeThisInspectionPlanSummary{},
			errGradeThisInspectionPrimaryKey
	}
	if !visibilitySkipSeen {
		return gradeThisInspectionPlanSummary{},
			errGradeThisInspectionSkip
	}
	slices.SortFunc(
		summary.skips,
		func(left, right gradeThisInspectionSkipEvidence) int {
			return strings.Compare(left.name, right.name)
		},
	)
	return summary, nil
}

func gradeThisValidateInspectionSummary(
	searchID gradethiscorpus.SearchID,
	summary gradeThisInspectionPlanSummary,
) error {
	expectedColumns, ok := gradeThisInspectionExpectedColumns[searchID]
	if !ok {
		return errGradeThisInspectionSearch
	}
	if !slices.Equal(summary.columns, expectedColumns) {
		return errGradeThisInspectionColumns
	}
	if summary.minMax != gradeThisInspectionMinMaxCounts() {
		return errGradeThisInspectionMinMax
	}
	if summary.partition != gradeThisInspectionPartitionCounts() {
		return errGradeThisInspectionPartition
	}
	if summary.primaryKey != gradeThisInspectionPrimaryKeyCounts() {
		return errGradeThisInspectionPrimaryKey
	}

	expectedSkips := []gradeThisInspectionSkipEvidence{
		gradeThisInspectionSkip("idx_visibility_seq"),
	}
	if searchID == gradethiscorpus.SearchServerErrors {
		expectedSkips = append(
			[]gradeThisInspectionSkipEvidence{
				gradeThisInspectionSkip("idx_field_names"),
			},
			expectedSkips...,
		)
	}
	if !gradeThisInspectionSkipsEqual(summary.skips, expectedSkips) {
		return errGradeThisInspectionSkip
	}
	return nil
}

func gradeThisInspectionColumnAllowed(column string) bool {
	for _, expected := range gradeThisInspectionExpectedColumns {
		if slices.Contains(expected, column) {
			return true
		}
	}
	return false
}

func gradeThisInspectionSkipsEqual(
	left, right []gradeThisInspectionSkipEvidence,
) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].name != right[index].name ||
			!slices.Equal(left[index].keys, right[index].keys) ||
			left[index].counts != right[index].counts {
			return false
		}
	}
	return true
}

func gradeThisInspectionSkip(name string) gradeThisInspectionSkipEvidence {
	return gradeThisInspectionSkipEvidence{
		name:   name,
		counts: gradeThisInspectionSkipCounts(),
	}
}

func gradeThisInspectionMinMaxCounts() gradeThisInspectionIndexCounts {
	return gradeThisInspectionIndexCounts{
		initialParts:     5,
		selectedParts:    3,
		initialGranules:  5,
		selectedGranules: 3,
	}
}

func gradeThisInspectionPartitionCounts() gradeThisInspectionIndexCounts {
	return gradeThisInspectionIndexCounts{
		initialParts:     3,
		selectedParts:    3,
		initialGranules:  3,
		selectedGranules: 3,
	}
}

func gradeThisInspectionPrimaryKeyCounts() gradeThisInspectionIndexCounts {
	return gradeThisInspectionIndexCounts{
		initialParts:     3,
		selectedParts:    1,
		initialGranules:  3,
		selectedGranules: 1,
	}
}

func gradeThisInspectionSkipCounts() gradeThisInspectionIndexCounts {
	return gradeThisInspectionIndexCounts{
		initialParts:     1,
		selectedParts:    1,
		initialGranules:  1,
		selectedGranules: 1,
	}
}

func gradeThisInspectionCounts(
	index queryexec.ExplainIndex,
) gradeThisInspectionIndexCounts {
	return gradeThisInspectionIndexCounts{
		initialParts:     index.InitialParts,
		selectedParts:    index.SelectedParts,
		initialGranules:  index.InitialGranules,
		selectedGranules: index.SelectedGranules,
	}
}
