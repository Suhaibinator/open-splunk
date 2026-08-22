package export_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	openexport "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"github.com/Suhaibinator/open-splunk/migrations"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	pipelineLiveVerticalTenant = "tenant-pipeline-live-vertical"
	pipelineLiveVerticalOwner  = "owner-pipeline-live-vertical"
	pipelineLiveVerticalIndex  = "pipeline-live-vertical"
)

type pipelineLiveVerticalSnapshotter uint64

func (snapshot pipelineLiveVerticalSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return uint64(snapshot), nil
}

func TestPipelinePinnedClickHousePublishesNullableListsThroughPagingAndExport(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"))
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close pipeline live vertical ClickHouse: %v", closeErr)
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
	queryConnection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := queryConnection.Close(); closeErr != nil {
			t.Errorf("close pipeline live query connection: %v", closeErr)
		}
	})
	if err := queryConnection.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := server.ApplyClickHouseMigrations(ctx, queryConnection, migrations.ClickHouse()); err != nil {
		t.Fatal(err)
	}

	storeConnection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "visibility.sqlite"))
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := controlDB.Close(); closeErr != nil {
			t.Errorf("close pipeline live control DB: %v", closeErr)
		}
	})
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := sequencer.Close(); closeErr != nil {
			t.Errorf("close pipeline live sequencer: %v", closeErr)
		}
	})
	store, err := clickhouse.NewStore(
		storeConnection,
		clickhouse.RetentionProviderFunc(func(context.Context, string, string) (time.Duration, error) {
			return 100 * 365 * 24 * time.Hour, nil
		}),
		sequencer,
	)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close pipeline live store: %v", closeErr)
		}
	})

	eventAnchor := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	indexTime := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	binaryEvent := pipelineLiveVerticalEvent("event-04", eventAnchor.Add(4*time.Second), indexTime,
		pipelineLiveVerticalField("tags", pipelineLiveVerticalNull()),
		pipelineLiveVerticalField("n", pipelineLiveVerticalDouble(4)))
	binaryEvent.Event.Raw = []byte("binary-valid-utf8")
	binaryEvent.Event.RawEncoding = opensplunk.RawEncoding_RAW_ENCODING_BINARY
	events := []*ingest.StoredEvent{
		pipelineLiveVerticalEvent("event-01", eventAnchor.Add(time.Second), indexTime,
			pipelineLiveVerticalField("tags", pipelineLiveVerticalString("a,b")),
			pipelineLiveVerticalField("n", pipelineLiveVerticalDouble(1))),
		pipelineLiveVerticalEvent("event-02", eventAnchor.Add(2*time.Second), indexTime,
			pipelineLiveVerticalField("tags", pipelineLiveVerticalString("")),
			pipelineLiveVerticalField("n", pipelineLiveVerticalDouble(2))),
		pipelineLiveVerticalEvent("event-03", eventAnchor.Add(3*time.Second), indexTime,
			pipelineLiveVerticalField("n", pipelineLiveVerticalDouble(3))),
		binaryEvent,
	}
	digest := sha256.Sum256([]byte("pipeline-live-nullable-list-vertical"))
	stored, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           pipelineLiveVerticalTenant,
		CollectorID:        "collector-pipeline-live",
		BatchID:            "batch-pipeline-live",
		BatchSequence:      1,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  digest,
		ReceivedAt:         indexTime,
		Events:             events,
	})
	if err != nil || stored.Accepted != uint32(len(events)) {
		t.Fatalf("store pipeline live fixture = (%+v, %v)", stored, err)
	}
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatal(err)
	}

	executor, err := queryexec.New(queryConnection, queryexec.Config{
		ReadAdmission: indexread.UnfencedAdmission{},
	})
	if err != nil {
		t.Fatal(err)
	}
	var nextJobID atomic.Uint64
	searchManager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     pipelineLiveVerticalSnapshotter(cutoff),
		Compiler:        clickhouse.Compiler{},
		MaxConcurrent:   1,
		MaxQueued:       2,
		DefaultPageSize: 2,
		MaxPageSize:     2,
		CleanupInterval: -1,
		Now:             func() time.Time { return indexTime.Add(time.Second) },
		NewID: func() string {
			return fmt.Sprintf("pipeline-live-vertical-%02d", nextJobID.Add(1))
		},
		CursorKey:   []byte("0123456789abcdef0123456789abcdef"),
		CursorScope: "pipeline-live-nullable-list-vertical",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := searchManager.Close(); closeErr != nil {
			t.Errorf("close pipeline live search manager: %v", closeErr)
		}
	})
	resolvedRange, err := searchtime.NewAbsoluteRange(eventAnchor, eventAnchor.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	access := searchjobs.AccessScope{TenantID: pipelineLiveVerticalTenant, OwnerID: pipelineLiveVerticalOwner}
	create := func(source string) searchjobs.Job {
		t.Helper()
		job, createErr := searchManager.Create(ctx, searchjobs.CreateRequest{
			SPL:               source,
			OwnerID:           access.OwnerID,
			TenantID:          access.TenantID,
			AuthorizedIndexes: []string{pipelineLiveVerticalIndex},
			RequestedIndexes:  []string{pipelineLiveVerticalIndex},
			TimeRange:         resolvedRange,
		})
		if createErr != nil {
			t.Fatalf("create pipeline live search: %v", createErr)
		}
		return pipelineWaitSearch(t, searchManager, access, job.ID)
	}

	unexpanded := create(`index=` + pipelineLiveVerticalIndex +
		` | sort 0 +event_id | makemv delim="," tags | table event_id tags`)
	unexpandedPages := pipelineCollectPages(t, searchManager, access, unexpanded.ID)
	if len(unexpandedPages) != 2 {
		t.Fatalf("nullable-list result pages = %d, want 2", len(unexpandedPages))
	}
	wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "tags", Kind: searchjobs.ValueKindList, Nullable: true, Multivalue: true},
	}}
	for index, page := range unexpandedPages {
		if !slices.Equal(page.Schema.Columns, wantSchema.Columns) {
			t.Fatalf("nullable-list page %d schema = %#v, want %#v", index, page.Schema, wantSchema)
		}
	}
	unexpandedRows := append(slices.Clone(unexpandedPages[0].Rows), unexpandedPages[1].Rows...)
	pipelineAssertNullableListRows(t, unexpandedRows)

	artifactDir := t.TempDir()
	exportManager, err := openexport.New(openexport.Config{
		Source:          searchManager,
		ArtifactDir:     artifactDir,
		MaxWorkers:      1,
		CleanupInterval: -1,
		NewID:           func() string { return "pipeline-live-list-export" },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := exportManager.Close(); closeErr != nil {
			t.Errorf("close pipeline live export manager: %v", closeErr)
		}
	})
	exportJob, err := exportManager.Create(ctx, access, openexport.CreateRequest{
		SearchJobID: unexpanded.ID,
		Format:      openexport.FormatJSONLines,
		Columns:     []string{"event_id", "tags"},
	})
	if err != nil {
		t.Fatal(err)
	}
	completedExport := pipelineWaitExport(t, exportManager, access, exportJob.ID)
	if completedExport.Artifact == nil || completedExport.Artifact.RowCount != 4 {
		t.Fatalf("completed nullable-list export = %#v", completedExport)
	}
	grant, err := exportManager.CreateDownloadGrant(ctx, access, completedExport.ID)
	if err != nil {
		t.Fatal(err)
	}
	download, err := exportManager.RedeemDownload(ctx, grant.Token)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := io.ReadAll(download)
	closeErr := download.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	pipelineAssertNullableListJSONLines(t, encoded)

	expanded := create(`index=` + pipelineLiveVerticalIndex +
		` | sort 0 +event_id | makemv delim="," tags | mvexpand tags limit=0 | table event_id tags`)
	expandedPages := pipelineCollectPages(t, searchManager, access, expanded.ID)
	if len(expandedPages) != 2 {
		t.Fatalf("expanded result pages = %d, want 2", len(expandedPages))
	}
	wantExpandedColumns := []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "tags", Kind: searchjobs.ValueKindString, Nullable: true},
	}
	for pageIndex, page := range expandedPages {
		if !slices.Equal(page.Schema.Columns, wantExpandedColumns) {
			t.Fatalf(
				"expanded page %d schema = %#v, want %#v",
				pageIndex,
				page.Schema.Columns,
				wantExpandedColumns,
			)
		}
	}
	expandedRows := append(slices.Clone(expandedPages[0].Rows), expandedPages[1].Rows...)
	wantExpanded := []string{"event-01=a", "event-01=b", "event-03=<null>", "event-04=<null>"}
	gotExpanded := make([]string, len(expandedRows))
	for index, row := range expandedRows {
		id, idOK := row.Values[0].String()
		if !idOK {
			t.Fatalf("expanded row %d id = %#v", index, row.Values[0])
		}
		value := "<null>"
		if !row.Values[1].IsNull() {
			var valueOK bool
			value, valueOK = row.Values[1].String()
			if !valueOK {
				t.Fatalf("expanded row %d value = %#v", index, row.Values[1])
			}
		}
		gotExpanded[index] = id + "=" + value
	}
	if !slices.Equal(gotExpanded, wantExpanded) {
		t.Fatalf("expanded limit=0 rows = %v, want %v", gotExpanded, wantExpanded)
	}

	assertBinaryResult := func(source string, want []byte) {
		t.Helper()
		job := create(source)
		pages := pipelineCollectPages(t, searchManager, access, job.ID)
		if len(pages) != 1 || len(pages[0].Schema.Columns) != 1 ||
			pages[0].Schema.Columns[0].Name != "copied" ||
			pages[0].Schema.Columns[0].Kind != searchjobs.ValueKindMixed ||
			len(pages[0].Rows) != 1 {
			t.Fatalf("binary pipeline result shape = %#v", pages)
		}
		got, ok := pages[0].Rows[0].Values[0].Bytes()
		if !ok || !slices.Equal(got, want) {
			t.Fatalf("binary pipeline value = %#v / Bytes %t, want %v", pages[0].Rows[0].Values[0], ok, want)
		}
	}
	binaryRaw := []byte("binary-valid-utf8")
	assertBinaryResult(
		`index=`+pipelineLiveVerticalIndex+` event_id="event-04"`+
			` | eval copied=_raw | fillnull value="safe" copied | table copied`,
		binaryRaw,
	)
	assertBinaryResult(
		`index=`+pipelineLiveVerticalIndex+` event_id="event-04"`+
			` | strcat _raw ":" copied | table copied`,
		append(append([]byte(nil), binaryRaw...), ':'),
	)
	assertBinaryResult(
		`index=`+pipelineLiveVerticalIndex+` event_id="event-04"`+
			` | eval copied=_raw`+
			` | strcat allrequired=true missing ":" copied | table copied`,
		binaryRaw,
	)

	// This is the release-plan public paging vertical: unlike the manager-only
	// fixture, every row here is produced by the pinned ClickHouse compiler and
	// consumed by the real executor before the manager assigns cursor ordinals.
	pipelineCommands := create(`index=` + pipelineLiveVerticalIndex +
		` | regex message!="reject"` +
		` | sort 0 +event_id | reverse` +
		` | accum n AS running` +
		` | strcat host "/" source endpoint` +
		` | addinfo` +
		` | fillnull value="unknown" optional` +
		` | addtotals fieldname=total n running` +
		` | delta running AS step p=1` +
		` | makemv delim="," tags | mvexpand tags | reverse` +
		` | table event_id tags running endpoint optional total step info_sid`)
	pipelineCommandsPages := pipelineCollectPages(t, searchManager, access, pipelineCommands.ID)
	if len(pipelineCommandsPages) != 2 {
		t.Fatalf("pipeline-command result pages = %d, want 2", len(pipelineCommandsPages))
	}
	wantPipelineCommandsFields := []string{
		"event_id", "tags", "running", "endpoint", "optional", "total", "step", "info_sid",
	}
	for pageIndex, page := range pipelineCommandsPages {
		gotFields := make([]string, len(page.Schema.Columns))
		for index, column := range page.Schema.Columns {
			gotFields[index] = column.Name
			if strings.HasPrefix(strings.ToLower(column.Name), "__os_") {
				t.Fatalf("pipeline-command page %d leaked private column %q", pageIndex, column.Name)
			}
		}
		if !slices.Equal(gotFields, wantPipelineCommandsFields) {
			t.Fatalf("pipeline-command page %d fields = %v, want %v", pageIndex, gotFields, wantPipelineCommandsFields)
		}
	}
	pipelineCommandsRows := append(slices.Clone(pipelineCommandsPages[0].Rows), pipelineCommandsPages[1].Rows...)
	wantPipelineCommands := []struct {
		id, tag, endpoint, optional, sid string
		running, total, step             float64
		stepNull                         bool
	}{
		{id: "event-01", tag: "b", running: 10, endpoint: "v03-live-host/v03-live-source", optional: "unknown", total: 11, step: 1, sid: pipelineCommands.ID},
		{id: "event-01", tag: "a", running: 10, endpoint: "v03-live-host/v03-live-source", optional: "unknown", total: 11, step: 1, sid: pipelineCommands.ID},
		{id: "event-03", running: 7, endpoint: "v03-live-host/v03-live-source", optional: "unknown", total: 10, step: 3, sid: pipelineCommands.ID},
		{id: "event-04", running: 4, endpoint: "v03-live-host/v03-live-source", optional: "unknown", total: 8, stepNull: true, sid: pipelineCommands.ID},
	}
	if len(pipelineCommandsRows) != len(wantPipelineCommands) {
		t.Fatalf("pipeline-command rows = %d, want %d", len(pipelineCommandsRows), len(wantPipelineCommands))
	}
	for index, want := range wantPipelineCommands {
		row := pipelineCommandsRows[index]
		if row.Ordinal != uint64(index) {
			t.Fatalf("pipeline-command row %d ordinal = %d", index, row.Ordinal)
		}
		id, idOK := row.Values[0].String()
		tag := ""
		tagNull := row.Values[1].IsNull()
		if !tagNull {
			var tagOK bool
			tag, tagOK = row.Values[1].String()
			if !tagOK {
				t.Fatalf("pipeline-command row %d tag = %#v", index, row.Values[1])
			}
		}
		running, runningOK := row.Values[2].Double()
		endpoint, endpointOK := row.Values[3].String()
		optional, optionalOK := row.Values[4].String()
		total, totalOK := row.Values[5].Double()
		step, stepOK := row.Values[6].Double()
		sid, sidOK := row.Values[7].String()
		if !idOK || id != want.id || tag != want.tag || tagNull != (want.tag == "") ||
			!runningOK || running != want.running || !endpointOK || endpoint != want.endpoint ||
			!optionalOK || optional != want.optional || !totalOK || total != want.total ||
			stepOK == want.stepNull || (!want.stepNull && step != want.step) ||
			!sidOK || sid != want.sid {
			t.Fatalf("pipeline-command row %d = %#v, want %+v", index, row, want)
		}
	}
}

func pipelineLiveVerticalEvent(
	id string,
	eventTime time.Time,
	indexTime time.Time,
	fields ...*opensplunk.TypedObjectField,
) *ingest.StoredEvent {
	message := "v0.3 live vertical " + id
	return &ingest.StoredEvent{
		TenantID:    pipelineLiveVerticalTenant,
		CollectorID: "collector-pipeline-live",
		BatchID:     "batch-pipeline-live",
		IndexTime:   indexTime,
		Event: &opensplunk.LogEvent{
			EventId:         id,
			IndexName:       pipelineLiveVerticalIndex,
			EventTime:       timestamppb.New(eventTime),
			CollectedAt:     timestamppb.New(eventTime.Add(-time.Second)),
			EventTimeSource: opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            "v03-live-host",
			Source:          "v03-live-source",
			Sourcetype:      "v03:live",
			Raw:             []byte(message),
			RawEncoding:     opensplunk.RawEncoding_RAW_ENCODING_UTF8,
			Message:         &message,
			Fields:          &opensplunk.TypedObject{Fields: fields},
		},
	}
}

func pipelineLiveVerticalField(name string, value *opensplunk.TypedValue) *opensplunk.TypedObjectField {
	return &opensplunk.TypedObjectField{Name: name, Value: value}
}

func pipelineLiveVerticalString(value string) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: value}}
}

func pipelineLiveVerticalDouble(value float64) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_DoubleValue{DoubleValue: value}}
}

func pipelineLiveVerticalNull() *opensplunk.TypedValue {
	return &opensplunk.TypedValue{
		Kind: &opensplunk.TypedValue_NullValue{NullValue: opensplunk.NullValue_NULL_VALUE_NULL},
	}
}

func pipelineWaitSearch(
	t *testing.T,
	manager *searchjobs.Manager,
	access searchjobs.AccessScope,
	id string,
) searchjobs.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.GetFor(access, id)
		if err != nil {
			t.Fatalf("GetFor(%q): %v", id, err)
		}
		switch job.State {
		case searchjobs.StateCompleted:
			return job
		case searchjobs.StateFailed, searchjobs.StateCanceled, searchjobs.StateExpired:
			t.Fatalf("search %q reached %v: %#v", id, job.State, job.Failure)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("search %q did not complete", id)
	return searchjobs.Job{}
}

func pipelineCollectPages(
	t *testing.T,
	manager *searchjobs.Manager,
	access searchjobs.AccessScope,
	id string,
) []searchjobs.ResultPage {
	t.Helper()
	var pages []searchjobs.ResultPage
	cursor := ""
	for {
		page, err := manager.ResultsFor(access, id, searchjobs.PageRequest{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("ResultsFor(%q, page %d): %v", id, len(pages), err)
		}
		pages = append(pages, page)
		if page.Complete {
			if page.NextCursor != "" {
				t.Fatalf("complete result page retained cursor %q", page.NextCursor)
			}
			return pages
		}
		if page.NextCursor == "" || page.NextCursor == cursor {
			t.Fatalf("result page %d has invalid next cursor %q", len(pages)-1, page.NextCursor)
		}
		cursor = page.NextCursor
	}
}

func pipelineAssertNullableListRows(t *testing.T, rows []searchjobs.ResultRow) {
	t.Helper()
	if len(rows) != 4 {
		t.Fatalf("nullable-list rows = %d, want 4", len(rows))
	}
	wantIDs := []string{"event-01", "event-02", "event-03", "event-04"}
	for index, row := range rows {
		if row.Ordinal != uint64(index) {
			t.Fatalf("nullable-list row %d ordinal = %d", index, row.Ordinal)
		}
		id, ok := row.Values[0].String()
		if !ok || id != wantIDs[index] {
			t.Fatalf("nullable-list row %d id = %#v, want %q", index, row.Values[0], wantIDs[index])
		}
	}
	members, ok := rows[0].Values[1].List()
	if !ok || len(members) != 2 {
		t.Fatalf("member list = %#v, want [a b]", rows[0].Values[1])
	}
	first, firstOK := members[0].String()
	second, secondOK := members[1].String()
	if !firstOK || !secondOK || first != "a" || second != "b" {
		t.Fatalf("member list = %#v, want [a b]", members)
	}
	empty, ok := rows[1].Values[1].List()
	if !ok || len(empty) != 0 {
		t.Fatalf("present empty list = %#v", rows[1].Values[1])
	}
	if !rows[2].Values[1].IsMissing() || !rows[3].Values[1].IsNull() {
		t.Fatalf("missing/null list rows = %#v / %#v", rows[2].Values[1], rows[3].Values[1])
	}
}

func pipelineWaitExport(
	t *testing.T,
	manager *openexport.Manager,
	access searchjobs.AccessScope,
	id string,
) openexport.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.Get(context.Background(), access, id)
		if err != nil {
			t.Fatalf("export Get(%q): %v", id, err)
		}
		switch job.State {
		case openexport.StateCompleted:
			return job
		case openexport.StateFailed, openexport.StateCanceled, openexport.StateExpired:
			t.Fatalf("export %q reached %v: %#v", id, job.State, job.Failure)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("export %q did not complete", id)
	return openexport.Job{}
}

func pipelineAssertNullableListJSONLines(t *testing.T, encoded []byte) {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(string(encoded), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("nullable-list JSONL rows = %d, want 4: %q", len(lines), encoded)
	}
	wantIDs := []string{"event-01", "event-02", "event-03", "event-04"}
	for index, line := range lines {
		var object map[string]any
		if err := json.Unmarshal([]byte(line), &object); err != nil {
			t.Fatalf("decode JSONL row %d: %v", index, err)
		}
		if len(object) != 2 || object["event_id"] != wantIDs[index] {
			t.Fatalf("JSONL row %d = %#v", index, object)
		}
		for name := range object {
			if strings.HasPrefix(strings.ToLower(name), "__os_") {
				t.Fatalf("JSONL row %d leaked private field %q", index, name)
			}
		}
		switch index {
		case 0:
			members, ok := object["tags"].([]any)
			if !ok || !slices.Equal(members, []any{"a", "b"}) {
				t.Fatalf("JSONL member list = %#v, want [a b]", object["tags"])
			}
		case 1:
			members, ok := object["tags"].([]any)
			if !ok || len(members) != 0 {
				t.Fatalf("JSONL present empty list = %#v", object["tags"])
			}
		default:
			if object["tags"] != nil {
				t.Fatalf("JSONL null list row %d = %#v", index, object["tags"])
			}
		}
	}
}
