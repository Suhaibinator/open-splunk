package searchws

import (
	"math"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestProjectSearchMapsEveryLifecycleState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		state    searchjobs.State
		want     opensplunkv1.SearchJobState
		phase    opensplunkv1.SearchExecutionPhase
		terminal bool
	}{
		{name: "queued", state: searchjobs.StateQueued, want: opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED, phase: opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_WAITING_FOR_SLOT},
		{name: "parsing", state: searchjobs.StateParsing, want: opensplunkv1.SearchJobState_SEARCH_JOB_STATE_PARSING, phase: opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_PARSING},
		{name: "planning", state: searchjobs.StatePlanning, want: opensplunkv1.SearchJobState_SEARCH_JOB_STATE_PLANNING, phase: opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_OPTIMIZING},
		{name: "running", state: searchjobs.StateRunning, want: opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING, phase: opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_EXECUTING},
		{name: "completed", state: searchjobs.StateCompleted, want: opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, phase: opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE, terminal: true},
		{name: "failed", state: searchjobs.StateFailed, want: opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED, phase: opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE, terminal: true},
		{name: "canceled", state: searchjobs.StateCanceled, want: opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED, phase: opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE, terminal: true},
		{name: "expired", state: searchjobs.StateExpired, want: opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED, phase: opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE, terminal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := validSearchProjectionJob()
			job.State = test.state

			projection, err := projectSearch(job, job.CreatedAt.Add(5*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if projection.version != job.Version || !projection.incarnation.Equal(job.CreatedAt) || projection.terminal != test.terminal {
				t.Fatalf("projection metadata = {version:%d incarnation:%s terminal:%t}", projection.version, projection.incarnation, projection.terminal)
			}
			wantEvents := 2
			if test.terminal {
				wantEvents++
			}
			if len(projection.events) != wantEvents {
				t.Fatalf("event count = %d, want %d", len(projection.events), wantEvents)
			}
			state := projection.events[0].GetSearchStateChanged()
			if state == nil || state.GetSearchJobId() != job.ID || state.GetState() != test.want || state.GetStateVersion() != job.Version {
				t.Fatalf("state event = %+v", state)
			}
			progress := projection.events[1].GetSearchProgress()
			if progress == nil || progress.GetPhase() != test.phase {
				t.Fatalf("progress event = %+v, want phase %v", progress, test.phase)
			}
			if test.terminal {
				terminal := projection.events[2].GetSearchTerminal()
				if terminal == nil || terminal.GetSearchJobId() != job.ID || terminal.GetState() != test.want || terminal.GetStateVersion() != job.Version || terminal.GetFinalProgress() == nil {
					t.Fatalf("terminal event = %+v", terminal)
				}
			}
		})
	}
}

func TestProjectSearchProgressUsesAuthoritativeTimingAndCounters(t *testing.T) {
	t.Parallel()

	job := validSearchProjectionJob()
	job.State = searchjobs.StateRunning
	job.StartedAt = job.CreatedAt.Add(2 * time.Second)
	job.RowCount = 17
	job.ResultBytes = 4097
	job.ScannedRows = 170
	job.ScannedBytes = 40_970
	now := job.CreatedAt.Add(11*time.Second + 321*time.Millisecond)

	projection, err := projectSearch(job, now)
	if err != nil {
		t.Fatal(err)
	}
	progress := projection.events[1].GetSearchProgress()
	if progress.GetScannedRows() != job.ScannedRows || progress.GetScannedBytes() != job.ScannedBytes ||
		progress.GetProducedRows() != job.RowCount || progress.GetResultBytes() != job.ResultBytes || progress.GetCountersAreEstimates() {
		t.Fatalf("counters = %+v", progress)
	}
	if got := progress.GetElapsed().AsDuration(); got != now.Sub(job.StartedAt) {
		t.Fatalf("elapsed = %s, want %s", got, now.Sub(job.StartedAt))
	}
	if got := progress.GetQueueWait().AsDuration(); got != job.StartedAt.Sub(job.CreatedAt) {
		t.Fatalf("queue wait = %s, want %s", got, job.StartedAt.Sub(job.CreatedAt))
	}
	if got := progress.GetUpdatedAt().AsTime(); !got.Equal(now) || got.Location() != time.UTC {
		t.Fatalf("updated at = %s, want canonical %s", got, now.UTC())
	}

	job.State = searchjobs.StateCompleted
	job.FinishedAt = job.StartedAt.Add(4 * time.Second)
	projection, err = projectSearch(job, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	progress = projection.events[1].GetSearchProgress()
	if got := progress.GetElapsed().AsDuration(); got != job.FinishedAt.Sub(job.StartedAt) {
		t.Fatalf("terminal elapsed = %s, want %s", got, job.FinishedAt.Sub(job.StartedAt))
	}
	if got := progress.GetUpdatedAt().AsTime(); !got.Equal(job.FinishedAt) {
		t.Fatalf("terminal updated at = %s, want %s", got, job.FinishedAt)
	}
	if terminal := projection.events[len(projection.events)-1].GetSearchTerminal(); terminal.GetFinalProgress() == nil ||
		terminal.GetFinalProgress().GetScannedRows() != job.ScannedRows || terminal.GetFinalProgress().GetScannedBytes() != job.ScannedBytes ||
		terminal.GetFinalProgress().GetProducedRows() != job.RowCount {
		t.Fatalf("terminal progress = %+v", terminal.GetFinalProgress())
	}
}

func TestProjectSearchClampsNegativeDurationsAndIncludesResultExpiry(t *testing.T) {
	t.Parallel()

	job := validSearchProjectionJob()
	job.State = searchjobs.StateCompleted
	job.StartedAt = job.CreatedAt.Add(-time.Second)
	job.FinishedAt = job.StartedAt.Add(-time.Second)
	job.ExpiresAt = job.CreatedAt.Add(time.Hour)

	projection, err := projectSearch(job, job.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	progress := projection.events[1].GetSearchProgress()
	if progress.GetElapsed().AsDuration() != 0 || progress.GetQueueWait().AsDuration() != 0 {
		t.Fatalf("negative durations were not clamped: %+v", progress)
	}
	terminal := projection.events[len(projection.events)-1].GetSearchTerminal()
	if got := terminal.GetResultsExpireAt().AsTime(); !got.Equal(job.ExpiresAt) {
		t.Fatalf("results expire at = %s, want %s", got, job.ExpiresAt)
	}
}

func TestProjectSearchRejectsInvalidSnapshots(t *testing.T) {
	t.Parallel()

	outsideProtoRange := time.Date(10_000, time.January, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*searchjobs.Job, *time.Time)
	}{
		{name: "missing id", mutate: func(job *searchjobs.Job, _ *time.Time) { job.ID = "" }},
		{name: "missing version", mutate: func(job *searchjobs.Job, _ *time.Time) { job.Version = 0 }},
		{name: "missing creation time", mutate: func(job *searchjobs.Job, _ *time.Time) { job.CreatedAt = time.Time{} }},
		{name: "invalid state", mutate: func(job *searchjobs.Job, _ *time.Time) { job.State = searchjobs.StateInvalid }},
		{name: "unknown state", mutate: func(job *searchjobs.Job, _ *time.Time) { job.State = searchjobs.State(255) }},
		{name: "progress timestamp outside protobuf range", mutate: func(_ *searchjobs.Job, now *time.Time) { *now = outsideProtoRange }},
		{name: "finished timestamp outside protobuf range", mutate: func(job *searchjobs.Job, _ *time.Time) {
			job.State = searchjobs.StateCompleted
			job.FinishedAt = outsideProtoRange
		}},
		{name: "result expiry outside protobuf range", mutate: func(job *searchjobs.Job, _ *time.Time) {
			job.State = searchjobs.StateCompleted
			job.ExpiresAt = outsideProtoRange
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := validSearchProjectionJob()
			now := job.CreatedAt.Add(time.Second)
			test.mutate(&job, &now)
			if _, err := projectSearch(job, now); err == nil {
				t.Fatal("projectSearch() succeeded")
			}
		})
	}
}

func TestProjectExportMapsEveryLifecycleState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		state    exportjobs.State
		want     opensplunkv1.ExportJobState
		terminal bool
	}{
		{name: "queued", state: exportjobs.StateQueued, want: opensplunkv1.ExportJobState_EXPORT_JOB_STATE_QUEUED},
		{name: "running", state: exportjobs.StateRunning, want: opensplunkv1.ExportJobState_EXPORT_JOB_STATE_RUNNING},
		{name: "completed", state: exportjobs.StateCompleted, want: opensplunkv1.ExportJobState_EXPORT_JOB_STATE_COMPLETED, terminal: true},
		{name: "failed", state: exportjobs.StateFailed, want: opensplunkv1.ExportJobState_EXPORT_JOB_STATE_FAILED, terminal: true},
		{name: "canceled", state: exportjobs.StateCanceled, want: opensplunkv1.ExportJobState_EXPORT_JOB_STATE_CANCELED, terminal: true},
		{name: "expired", state: exportjobs.StateExpired, want: opensplunkv1.ExportJobState_EXPORT_JOB_STATE_EXPIRED, terminal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := validExportProjectionJob()
			job.State = test.state

			projection, err := projectExport(job, job.CreatedAt.Add(5*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if projection.version != job.Version || !projection.incarnation.Equal(job.CreatedAt) || projection.terminal != test.terminal {
				t.Fatalf("projection metadata = {version:%d incarnation:%s terminal:%t}", projection.version, projection.incarnation, projection.terminal)
			}
			wantEvents := 2
			if test.terminal {
				wantEvents++
			}
			if len(projection.events) != wantEvents {
				t.Fatalf("event count = %d, want %d", len(projection.events), wantEvents)
			}
			state := projection.events[0].GetExportStateChanged()
			if state == nil || state.GetExportJobId() != job.ID || state.GetState() != test.want || state.GetStateVersion() != job.Version {
				t.Fatalf("state event = %+v", state)
			}
			if projection.events[1].GetExportProgress() == nil {
				t.Fatal("progress event is missing")
			}
			if test.terminal {
				terminal := projection.events[2].GetExportTerminal()
				if terminal == nil || terminal.GetExportJobId() != job.ID || terminal.GetState() != test.want || terminal.GetStateVersion() != job.Version || terminal.GetFinalProgress() == nil {
					t.Fatalf("terminal event = %+v", terminal)
				}
			}
		})
	}
}

func TestProjectExportProgressAndTerminalArtifact(t *testing.T) {
	t.Parallel()

	job := validExportProjectionJob()
	job.State = exportjobs.StateCompleted
	job.StartedAt = job.CreatedAt.Add(2 * time.Second)
	job.FinishedAt = job.StartedAt.Add(7 * time.Second)
	job.Progress = exportjobs.Progress{RowsWritten: 23, BytesWritten: 8193, UpdatedAt: job.FinishedAt.Add(-time.Millisecond)}
	job.Artifact = &exportjobs.Artifact{
		FileName: "results.jsonl", MediaType: "application/x-ndjson", SizeBytes: 8193, RowCount: 23,
		ExpiresAt: job.FinishedAt.Add(time.Hour),
	}

	projection, err := projectExport(job, job.FinishedAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	progress := projection.events[1].GetExportProgress()
	if progress.GetRowsWritten() != 23 || progress.GetBytesWritten() != 8193 || progress.GetElapsed().AsDuration() != 7*time.Second {
		t.Fatalf("progress = %+v", progress)
	}
	if got := progress.GetUpdatedAt().AsTime(); !got.Equal(job.Progress.UpdatedAt) {
		t.Fatalf("updated at = %s, want %s", got, job.Progress.UpdatedAt)
	}
	terminal := projection.events[2].GetExportTerminal()
	artifact := terminal.GetArtifact()
	if artifact.GetFileName() != job.Artifact.FileName || artifact.GetMediaType() != job.Artifact.MediaType || artifact.GetSizeBytes() != job.Artifact.SizeBytes || artifact.GetRowCount() != job.Artifact.RowCount {
		t.Fatalf("artifact = %+v", artifact)
	}
	if got := artifact.GetExpiresAt().AsTime(); !got.Equal(job.Artifact.ExpiresAt) {
		t.Fatalf("artifact expiry = %s, want %s", got, job.Artifact.ExpiresAt)
	}
}

func TestProjectExportProgressFallbackAndNegativeElapsed(t *testing.T) {
	t.Parallel()

	job := validExportProjectionJob()
	job.State = exportjobs.StateRunning
	job.StartedAt = job.CreatedAt.Add(time.Hour)
	job.Progress.UpdatedAt = time.Time{}
	projection, err := projectExport(job, job.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	progress := projection.events[1].GetExportProgress()
	if progress.GetElapsed().AsDuration() != 0 {
		t.Fatalf("elapsed = %s, want zero", progress.GetElapsed().AsDuration())
	}
	if got := progress.GetUpdatedAt().AsTime(); !got.Equal(job.CreatedAt) {
		t.Fatalf("updated at fallback = %s, want %s", got, job.CreatedAt)
	}
}

func TestProjectExportRejectsInvalidSnapshots(t *testing.T) {
	t.Parallel()

	outsideProtoRange := time.Date(10_000, time.January, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*exportjobs.Job)
	}{
		{name: "missing id", mutate: func(job *exportjobs.Job) { job.ID = "" }},
		{name: "missing version", mutate: func(job *exportjobs.Job) { job.Version = 0 }},
		{name: "missing creation time", mutate: func(job *exportjobs.Job) { job.CreatedAt = time.Time{} }},
		{name: "invalid state", mutate: func(job *exportjobs.Job) { job.State = exportjobs.StateInvalid }},
		{name: "unknown state", mutate: func(job *exportjobs.Job) { job.State = exportjobs.State(255) }},
		{name: "progress timestamp outside protobuf range", mutate: func(job *exportjobs.Job) { job.Progress.UpdatedAt = outsideProtoRange }},
		{name: "artifact expiry outside protobuf range", mutate: func(job *exportjobs.Job) {
			job.State = exportjobs.StateCompleted
			job.Artifact = &exportjobs.Artifact{FileName: "results.csv", MediaType: "text/csv", ExpiresAt: outsideProtoRange}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := validExportProjectionJob()
			test.mutate(&job)
			if _, err := projectExport(job, job.CreatedAt.Add(time.Second)); err == nil {
				t.Fatal("projectExport() succeeded")
			}
		})
	}
}

func TestProjectSearchSchemaMatchesHTTPResultClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spl  string
		want opensplunkv1.ResultSetKind
	}{
		{name: "events", spl: "index=main", want: opensplunkv1.ResultSetKind_RESULT_SET_KIND_EVENTS},
		{name: "rex events", spl: `index=main | rex "(?<request_id>request_id=\w+)"`, want: opensplunkv1.ResultSetKind_RESULT_SET_KIND_EVENTS},
		{name: "table", spl: "index=main | table level count", want: opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS},
		{name: "stats", spl: "index=main | stats count by level", want: opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS},
		{name: "top", spl: "index=main | top limit=20 message", want: opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS},
		{name: "rare", spl: "index=main | rare limit=20 message", want: opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS},
		{name: "timechart wins after table", spl: "index=main | table _time level | timechart span=5m count by level", want: opensplunkv1.ResultSetKind_RESULT_SET_KIND_TIME_SERIES},
		{name: "invalid SPL", spl: "index=main | unsupported", want: opensplunkv1.ResultSetKind_RESULT_SET_KIND_UNSPECIFIED},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := validSearchProjectionJob()
			job.SPL = test.spl
			job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{
				{Name: "_time", Kind: searchjobs.ValueKindTime},
				{Name: "count", Kind: searchjobs.ValueKindUnsigned, Nullable: true, Multivalue: true},
			}}

			projection, err := projectSearch(job, job.CreatedAt)
			if err != nil {
				t.Fatal(err)
			}
			available := projection.events[2].GetResultSchemaAvailable()
			if available == nil || available.GetSearchJobId() != job.ID {
				t.Fatalf("schema event = %+v", available)
			}
			schema := available.GetSchema()
			if schema.GetSchemaId() != job.ID || schema.GetRevision() != 1 || schema.GetResultKind() != test.want || len(schema.GetColumns()) != 2 {
				t.Fatalf("schema = %+v, want result kind %v", schema, test.want)
			}
			if schema.Columns[0].GetSemanticType() != opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_EVENT_TIME {
				t.Fatalf("_time semantic = %v", schema.Columns[0].GetSemanticType())
			}
			wantSecondSemantic := opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_UNSPECIFIED
			if test.want == opensplunkv1.ResultSetKind_RESULT_SET_KIND_TIME_SERIES {
				wantSecondSemantic = opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_METRIC
			}
			second := schema.Columns[1]
			if second.GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_UINT64 || second.GetSemanticType() != wantSecondSemantic || !second.GetNullable() || !second.GetMultivalue() {
				t.Fatalf("second column = %+v", second)
			}
		})
	}
}

func TestProjectSearchSchemaMapsEveryValueKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind searchjobs.ValueKind
		want opensplunkv1.ValueType
	}{
		{name: "null", kind: searchjobs.ValueKindNull, want: opensplunkv1.ValueType_VALUE_TYPE_NULL},
		{name: "string", kind: searchjobs.ValueKindString, want: opensplunkv1.ValueType_VALUE_TYPE_STRING},
		{name: "signed", kind: searchjobs.ValueKindSigned, want: opensplunkv1.ValueType_VALUE_TYPE_SINT64},
		{name: "unsigned", kind: searchjobs.ValueKindUnsigned, want: opensplunkv1.ValueType_VALUE_TYPE_UINT64},
		{name: "double", kind: searchjobs.ValueKindDouble, want: opensplunkv1.ValueType_VALUE_TYPE_DOUBLE},
		{name: "bool", kind: searchjobs.ValueKindBool, want: opensplunkv1.ValueType_VALUE_TYPE_BOOL},
		{name: "bytes", kind: searchjobs.ValueKindBytes, want: opensplunkv1.ValueType_VALUE_TYPE_BYTES},
		{name: "time", kind: searchjobs.ValueKindTime, want: opensplunkv1.ValueType_VALUE_TYPE_TIMESTAMP},
		{name: "duration", kind: searchjobs.ValueKindDuration, want: opensplunkv1.ValueType_VALUE_TYPE_DURATION},
		{name: "list", kind: searchjobs.ValueKindList, want: opensplunkv1.ValueType_VALUE_TYPE_LIST},
		{name: "object", kind: searchjobs.ValueKindObject, want: opensplunkv1.ValueType_VALUE_TYPE_OBJECT},
		{name: "decimal", kind: searchjobs.ValueKindDecimal, want: opensplunkv1.ValueType_VALUE_TYPE_DECIMAL},
		{name: "mixed", kind: searchjobs.ValueKindMixed, want: opensplunkv1.ValueType_VALUE_TYPE_MIXED},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := validSearchProjectionJob()
			job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "field", Kind: test.kind}}}
			projection, err := projectSearch(job, job.CreatedAt)
			if err != nil {
				t.Fatal(err)
			}
			column := projection.events[2].GetResultSchemaAvailable().GetSchema().GetColumns()[0]
			if column.GetFieldName() != "field" || column.GetDisplayName() != "field" || column.GetValueType() != test.want {
				t.Fatalf("column = %+v, want type %v", column, test.want)
			}
		})
	}
}

func TestProjectSearchSchemaMapsPublicFieldSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		field string
		want  opensplunkv1.ColumnSemanticType
	}{
		{field: "_time", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_EVENT_TIME},
		{field: "_indextime", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_INDEX_TIME},
		{field: "_raw", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_RAW},
		{field: "index", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_INDEX},
		{field: "host", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_HOST},
		{field: "source", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_SOURCE},
		{field: "sourcetype", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_SOURCETYPE},
		{field: "level", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_LEVEL},
		{field: "message", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_MESSAGE},
		{field: "body", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_MESSAGE},
		{field: "trace_id", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_TRACE_ID},
		{field: "span_id", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_SPAN_ID},
		{field: "custom", want: opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_UNSPECIFIED},
	}
	columns := make([]searchjobs.Column, len(tests))
	for index, test := range tests {
		columns[index] = searchjobs.Column{Name: test.field, Kind: searchjobs.ValueKindString}
	}
	job := validSearchProjectionJob()
	job.Schema = &searchjobs.Schema{Columns: columns}
	projection, err := projectSearch(job, job.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	converted := projection.events[2].GetResultSchemaAvailable().GetSchema().GetColumns()
	for index, test := range tests {
		if got := converted[index].GetSemanticType(); got != test.want {
			t.Errorf("semantic type for %q = %v, want %v", test.field, got, test.want)
		}
	}
}

func TestProjectSearchRejectsInvalidSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		schema searchjobs.Schema
	}{
		{name: "empty column", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Kind: searchjobs.ValueKindString}}}},
		{name: "non UTF-8 column", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: string([]byte{0xff}), Kind: searchjobs.ValueKindString}}}},
		{name: "duplicate column", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "field", Kind: searchjobs.ValueKindString}, {Name: "field", Kind: searchjobs.ValueKindUnsigned}}}},
		{name: "invalid value kind", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "field", Kind: searchjobs.ValueKindInvalid}}}},
		{name: "unknown value kind", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "field", Kind: searchjobs.ValueKind(255)}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := validSearchProjectionJob()
			job.Schema = &test.schema
			if _, err := projectSearch(job, job.CreatedAt); err == nil {
				t.Fatal("projectSearch() succeeded")
			}
		})
	}
}

func TestProjectSearchMapsEveryFailureCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code searchjobs.FailureCode
		want opensplunkv1.SearchFailureCode
	}{
		{name: "invalid SPL", code: searchjobs.FailureInvalidSPL, want: opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_SPL},
		{name: "unsupported SPL", code: searchjobs.FailureUnsupportedSPL, want: opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_UNSUPPORTED_SPL},
		{name: "invalid time range", code: searchjobs.FailureInvalidTimeRange, want: opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_TIME_RANGE},
		{name: "index forbidden", code: searchjobs.FailureIndexForbidden, want: opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INDEX_FORBIDDEN},
		{name: "resource limit", code: searchjobs.FailureResourceLimit, want: opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_RESOURCE_LIMIT},
		{name: "timeout", code: searchjobs.FailureTimeout, want: opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_TIMEOUT},
		{name: "storage unavailable", code: searchjobs.FailureStorageUnavailable, want: opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_STORAGE_UNAVAILABLE},
		{name: "execution", code: searchjobs.FailureExecution, want: opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_EXECUTION},
		{name: "internal", code: searchjobs.FailureInternal, want: opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := validSearchProjectionJob()
			job.State = searchjobs.StateFailed
			job.Failure = &searchjobs.Failure{Code: test.code, Message: "safe failure", Retryable: true}
			projection, err := projectSearch(job, job.CreatedAt)
			if err != nil {
				t.Fatal(err)
			}
			failure := projection.events[2].GetSearchTerminal().GetFailure()
			if failure.GetCode() != test.want || failure.GetMessage() != "safe failure" || !failure.GetRetryable() {
				t.Fatalf("failure = %+v, want code %v", failure, test.want)
			}
		})
	}
}

func TestProjectSearchMapsDiagnosticsAndDetachesSuggestions(t *testing.T) {
	t.Parallel()

	suggestions := []string{"quote the value", "use search"}
	job := validSearchProjectionJob()
	job.State = searchjobs.StateFailed
	job.Failure = &searchjobs.Failure{
		Code: searchjobs.FailureInvalidSPL,
		Diagnostics: []searchjobs.Diagnostic{
			{Code: "SPL001", Message: "unexpected token", Line: 2, Column: 3, EndLine: 2, EndColumn: 9, Suggestions: suggestions},
			{Code: "SPL002", Message: "no source position"},
			{Code: "SPL003", Message: "bounded source position", Line: -1, Column: -2, EndLine: math.MaxInt, EndColumn: math.MaxInt},
		},
	}

	projection, err := projectSearch(job, job.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	diagnostics := projection.events[2].GetSearchTerminal().GetFailure().GetDiagnostics()
	if len(diagnostics) != 3 {
		t.Fatalf("diagnostic count = %d", len(diagnostics))
	}
	first := diagnostics[0]
	if first.GetCode() != "SPL001" || first.GetSeverity() != opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR || first.GetMessage() != "unexpected token" {
		t.Fatalf("first diagnostic = %+v", first)
	}
	if first.GetSourceRange().GetStart().GetLine() != 2 || first.GetSourceRange().GetStart().GetColumn() != 3 || first.GetSourceRange().GetEnd().GetLine() != 2 || first.GetSourceRange().GetEnd().GetColumn() != 9 {
		t.Fatalf("source range = %+v", first.GetSourceRange())
	}
	if diagnostics[1].GetSourceRange() != nil {
		t.Fatalf("positionless diagnostic has range %+v", diagnostics[1].GetSourceRange())
	}
	if diagnostics[2].GetSourceRange().GetStart().GetLine() != 0 || diagnostics[2].GetSourceRange().GetStart().GetColumn() != 0 || diagnostics[2].GetSourceRange().GetEnd().GetLine() != math.MaxUint32 || diagnostics[2].GetSourceRange().GetEnd().GetColumn() != math.MaxUint32 {
		t.Fatalf("bounded source range = %+v", diagnostics[2].GetSourceRange())
	}

	suggestions[0] = "mutated"
	job.Failure.Diagnostics[0].Suggestions[1] = "also mutated"
	if got := first.GetSuggestions(); len(got) != 2 || got[0] != "quote the value" || got[1] != "use search" {
		t.Fatalf("projected suggestions aliased source: %q", got)
	}
}

func TestProjectSearchEmitsTruncationWarningWithAuthoritativeTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		terminal bool
	}{
		{name: "live"},
		{name: "terminal", terminal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := validSearchProjectionJob()
			job.State = searchjobs.StateRunning
			job.ResultsTruncated = true
			now := job.CreatedAt.Add(3 * time.Second)
			wantOccurredAt := now
			if test.terminal {
				job.State = searchjobs.StateCompleted
				job.FinishedAt = job.CreatedAt.Add(2 * time.Second)
				wantOccurredAt = job.FinishedAt
			}
			projection, err := projectSearch(job, now)
			if err != nil {
				t.Fatal(err)
			}
			warning := projection.events[2].GetWarning().GetWarning()
			if warning.GetCode() != "RESULTS_TRUNCATED" || warning.GetMessage() == "" || !warning.GetOccurredAt().AsTime().Equal(wantOccurredAt) {
				t.Fatalf("warning = %+v, want occurred at %s", warning, wantOccurredAt)
			}
			if test.terminal && projection.events[3].GetSearchTerminal() == nil {
				t.Fatal("warning did not precede terminal event")
			}
		})
	}
}

func TestProjectSearchDetachesSchemaFromSnapshot(t *testing.T) {
	t.Parallel()

	columns := []searchjobs.Column{{Name: "level", Kind: searchjobs.ValueKindString, Nullable: true}}
	job := validSearchProjectionJob()
	job.Schema = &searchjobs.Schema{Columns: columns}
	projection, err := projectSearch(job, job.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	columns[0] = searchjobs.Column{Name: "mutated", Kind: searchjobs.ValueKindInvalid}
	converted := projection.events[2].GetResultSchemaAvailable().GetSchema().GetColumns()[0]
	if converted.GetFieldName() != "level" || converted.GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_STRING || !converted.GetNullable() {
		t.Fatalf("projected schema aliased source: %+v", converted)
	}
}

func TestProjectExportMapsEveryFailureCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code exportjobs.FailureCode
		want opensplunkv1.ExportFailureCode
	}{
		{name: "row limit", code: exportjobs.FailureRowLimit, want: opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_ROW_LIMIT},
		{name: "byte limit", code: exportjobs.FailureByteLimit, want: opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_BYTE_LIMIT},
		{name: "source unavailable", code: exportjobs.FailureSourceUnavailable, want: opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_SEARCH_UNAVAILABLE},
		{name: "storage unavailable", code: exportjobs.FailureStorageUnavailable, want: opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_STORAGE_UNAVAILABLE},
		{name: "internal", code: exportjobs.FailureInternal, want: opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_INTERNAL},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := validExportProjectionJob()
			job.State = exportjobs.StateFailed
			job.Failure = &exportjobs.Failure{Code: test.code, Message: "safe export failure", Retryable: true}
			projection, err := projectExport(job, job.CreatedAt)
			if err != nil {
				t.Fatal(err)
			}
			failure := projection.events[2].GetExportTerminal().GetFailure()
			if failure.GetCode() != test.want || failure.GetMessage() != "safe export failure" || !failure.GetRetryable() {
				t.Fatalf("failure = %+v, want code %v", failure, test.want)
			}
		})
	}
}

func TestProjectExportDetachesTerminalMetadata(t *testing.T) {
	t.Parallel()

	artifact := &exportjobs.Artifact{FileName: "results.csv", MediaType: "text/csv", SizeBytes: 42, RowCount: 3, ExpiresAt: validExportProjectionJob().CreatedAt.Add(time.Hour)}
	failure := &exportjobs.Failure{Code: exportjobs.FailureStorageUnavailable, Message: "safe failure", Retryable: true}
	job := validExportProjectionJob()
	job.State = exportjobs.StateFailed
	job.Artifact = artifact
	job.Failure = failure
	projection, err := projectExport(job, job.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	artifact.FileName = "mutated"
	artifact.SizeBytes = 999
	failure.Message = "mutated"
	failure.Retryable = false
	terminal := projection.events[2].GetExportTerminal()
	if terminal.GetArtifact().GetFileName() != "results.csv" || terminal.GetArtifact().GetSizeBytes() != 42 {
		t.Fatalf("projected artifact aliased source: %+v", terminal.GetArtifact())
	}
	if terminal.GetFailure().GetMessage() != "safe failure" || !terminal.GetFailure().GetRetryable() {
		t.Fatalf("projected failure aliased source: %+v", terminal.GetFailure())
	}
}

func validSearchProjectionJob() searchjobs.Job {
	return searchjobs.Job{
		ID:        "search-1",
		Version:   7,
		SPL:       "index=main",
		State:     searchjobs.StateQueued,
		CreatedAt: time.Date(2026, time.July, 22, 9, 30, 0, 123_456_789, time.FixedZone("test", -7*60*60)),
	}
}

func validExportProjectionJob() exportjobs.Job {
	return exportjobs.Job{
		ID:        "export-1",
		Version:   9,
		State:     exportjobs.StateQueued,
		CreatedAt: time.Date(2026, time.July, 22, 10, 15, 0, 987_654_321, time.FixedZone("test", -7*60*60)),
	}
}
