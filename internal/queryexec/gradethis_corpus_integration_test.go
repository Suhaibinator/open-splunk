package queryexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/gradethiscorpus"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

const gradeThisCorpusPageSize = 3

// TestGradeThisCorpusAgainstClickHouse is the exact executable acceptance
// contract for the current GradeThis searches. One synthetic NDJSON profile flows through the collector
// decoder, production ClickHouse Store, SPL compiler, query executor, search
// job manager, and owner/tenant-scoped signed-cursor paging.
func TestGradeThisCorpusAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	container, err := testsupport.StartClickHouse(ctx, os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupCtx); closeErr != nil {
			t.Errorf("close ClickHouse corpus container: %v", closeErr)
		}
	})
	queryIntegrationMigrate(t, ctx, container.Name, container.Password)

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
	t.Cleanup(func() { _ = queryConnection.Close() })
	if err := queryConnection.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	gradeThisStopInspectionMerges(t, ctx, queryConnection)
	executor, err := New(queryConnection, Config{
		ReadAdmission: indexread.UnfencedAdmission{},
	})
	if err != nil {
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
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sequencer.Close(); err != nil {
			t.Errorf("close GradeThis corpus visibility sequencer: %v", err)
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
			t.Errorf("close GradeThis corpus store: %v", closeErr)
		}
	})

	fixture := gradeThisStoreCorpus(t, ctx, store)
	gradeThisStoreInspectionLoad(
		t,
		ctx,
		queryConnection,
		fixture.profile,
	)
	gradeThisAssertInspectionPartLayout(t, ctx, queryConnection)
	cutoff, err := sequencer.Cutoff(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cutoff != 1 {
		t.Fatalf("visibility cutoff = %d, want 1", cutoff)
	}

	var nextID atomic.Uint64
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     queryIntegrationSnapshotter(cutoff),
		Compiler:        clickhouse.Compiler{},
		MaxConcurrent:   1,
		MaxQueued:       1,
		CleanupInterval: -1,
		Now:             func() time.Time { return fixture.profile.IndexTime.Add(500 * time.Microsecond) },
		NewID: func() string {
			return fmt.Sprintf("gradethis-corpus-%02d", nextID.Add(1))
		},
		CursorKey:   []byte("0123456789abcdef0123456789abcdef"),
		CursorScope: "gradethis-corpus",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	expectations := gradeThisExpectations(t, fixture)
	searches := gradethiscorpus.Searches()
	if len(expectations) != len(searches) {
		t.Fatalf("expectations = %d, searches = %d", len(expectations), len(searches))
	}
	evidence := make(
		map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
		len(searches),
	)
	for _, search := range searches {
		t.Run(string(search.ID), func(t *testing.T) {
			expectation, ok := expectations[search.ID]
			if !ok {
				t.Fatalf("no expectation for search %q", search.ID)
			}
			source, err := search.Render(fixture.profile.TraceID)
			if err != nil {
				t.Fatal(err)
			}
			created, err := manager.Create(ctx, searchjobs.CreateRequest{
				SPL: source, OwnerID: "owner", TenantID: "tenant",
				AuthorizedIndexes: []string{gradethiscorpus.IndexName},
				RequestedIndexes:  []string{gradethiscorpus.IndexName},
				TimeRange: queryIntegrationTimeRange(
					t, fixture.profile.BaseTime, fixture.profile.BaseTime.Add(15*time.Minute),
				),
			})
			if err != nil {
				t.Fatal(err)
			}
			terminal := queryIntegrationWaitForTerminal(t, manager, created.ID)
			if terminal.State != searchjobs.StateCompleted || terminal.Failure != nil {
				t.Fatalf("terminal job = state %v failure %#v", terminal.State, terminal.Failure)
			}
			if terminal.RowCount != uint64(len(expectation.rows)) || terminal.ResultsTruncated ||
				terminal.Schema == nil || !reflect.DeepEqual(*terminal.Schema, expectation.schema) {
				t.Fatalf(
					"terminal result contract = rows %d truncated %v schema %#v, want rows %d schema %#v",
					terminal.RowCount, terminal.ResultsTruncated, terminal.Schema,
					len(expectation.rows), expectation.schema,
				)
			}
			gradeThisAssertResultScope(t, manager, created.ID)
			gotRows, pageCount := gradeThisReadAllPages(
				t, manager, created.ID, expectation.schema, len(expectation.rows),
			)
			if len(expectation.rows) > gradeThisCorpusPageSize && pageCount < 2 {
				t.Fatalf("rows=%d were not exercised through multiple pages", len(expectation.rows))
			}
			gradeThisAssertRows(t, gotRows, expectation.rows)
			evidence[search.ID] = gradeThisCaptureInspectionEvidence(
				t,
				search.ID,
				created.ID,
				terminal,
			)
		})
	}
	gradeThisAssertInspectionCorpusEvidence(t, searches, evidence)
}

type gradeThisDecodedFixture struct {
	profile gradethiscorpus.Profile
	byID    map[string]*opensplunk.LogEvent
}

func gradeThisStoreCorpus(t *testing.T, ctx context.Context, store *clickhouse.Store) gradeThisDecodedFixture {
	t.Helper()
	stored, err := gradethiscorpus.StoreCanonical(
		ctx,
		store,
		"tenant",
	)
	if err != nil {
		t.Fatal(err)
	}
	return gradeThisDecodedFixture{
		profile: stored.Profile,
		byID:    stored.EventsByID,
	}
}

type gradeThisExpectation struct {
	schema searchjobs.Schema
	rows   [][]searchjobs.Value
}

func gradeThisExpectations(t *testing.T, fixture gradeThisDecodedFixture) map[gradethiscorpus.SearchID]gradeThisExpectation {
	t.Helper()
	column := func(name string, kind searchjobs.ValueKind, nullable ...bool) searchjobs.Column {
		return searchjobs.Column{
			Name: name, Kind: kind, Nullable: len(nullable) != 0 && nullable[0],
		}
	}
	schema := func(columns ...searchjobs.Column) searchjobs.Schema {
		return searchjobs.Schema{Columns: columns}
	}
	row := func(values ...searchjobs.Value) []searchjobs.Value { return values }
	event := func(id string) *opensplunk.LogEvent {
		result := fixture.byID[id]
		if result == nil {
			t.Fatalf("fixture has no event %q", id)
		}
		return result
	}
	tableTraceRow := func(id string) []searchjobs.Value {
		value := event(id)
		return row(
			searchjobs.TimeValue(value.GetEventTime().AsTime()),
			searchjobs.StringValue(value.GetLevel()),
			gradeThisDynamicField(t, value, "layer"),
			gradeThisDynamicField(t, value, "logger"),
			searchjobs.StringValue(value.GetMessage()),
		)
	}
	tableErrorRow := func(id string) []searchjobs.Value {
		value := event(id)
		return row(
			searchjobs.TimeValue(value.GetEventTime().AsTime()),
			searchjobs.StringValue(value.GetLevel()),
			gradeThisDynamicField(t, value, "logger"),
			searchjobs.StringValue(value.GetMessage()),
			gradeThisOptionalString(value.TraceId),
		)
	}

	base := fixture.profile.BaseTime
	return map[gradethiscorpus.SearchID]gradeThisExpectation{
		gradethiscorpus.SearchFollowTrace: {
			schema: schema(
				column("_time", searchjobs.ValueKindTime),
				column("level", searchjobs.ValueKindString, true),
				column("layer", searchjobs.ValueKindMixed, true),
				column("logger", searchjobs.ValueKindMixed, true),
				column("message", searchjobs.ValueKindString, true),
			),
			rows: [][]searchjobs.Value{
				tableTraceRow("trace-start"),
				tableTraceRow("trace-database-error"),
			},
		},
		gradethiscorpus.SearchErrorsAndWarnings: {
			schema: gradeThisEventSchema(),
			rows: gradeThisEventRows(t, fixture, []string{
				"submissions-500",
				"dependency-warning-c",
				"deadline-database-error",
				"assessments-503-c",
				"dependency-warning-b",
				"assessments-503-b",
				"dependency-warning-a",
				"assessments-503-a",
				"trace-database-error",
			}),
		},
		gradethiscorpus.SearchRawErrorFragment: {
			schema: schema(
				column("_time", searchjobs.ValueKindTime),
				column("level", searchjobs.ValueKindString, true),
				column("logger", searchjobs.ValueKindMixed, true),
				column("message", searchjobs.ValueKindString, true),
				column("trace_id", searchjobs.ValueKindString, true),
			),
			rows: [][]searchjobs.Value{tableErrorRow("trace-database-error")},
		},
		gradethiscorpus.SearchSeverityCounts: {
			schema: schema(
				column("level", searchjobs.ValueKindString, true),
				column("count", searchjobs.ValueKindUnsigned),
			),
			rows: [][]searchjobs.Value{
				row(searchjobs.StringValue("INFO"), searchjobs.UnsignedValue(11)),
				row(searchjobs.StringValue("ERROR"), searchjobs.UnsignedValue(6)),
				row(searchjobs.StringValue("WARN"), searchjobs.UnsignedValue(3)),
			},
		},
		gradethiscorpus.SearchFrequentErrors: {
			schema: schema(
				column("logger", searchjobs.ValueKindString),
				column("message", searchjobs.ValueKindString, true),
				column("count", searchjobs.ValueKindUnsigned),
			),
			rows: [][]searchjobs.Value{
				row(searchjobs.StringValue("api_handler.SRouter"), searchjobs.StringValue("Request metrics"), searchjobs.UnsignedValue(4)),
				row(searchjobs.StringValue("persistence_handler"), searchjobs.StringValue("Database request failed"), searchjobs.UnsignedValue(2)),
			},
		},
		gradethiscorpus.SearchVolumeBySeverity: {
			schema: schema(
				column("_time", searchjobs.ValueKindTime),
				column("ERROR", searchjobs.ValueKindUnsigned),
				column("INFO", searchjobs.ValueKindUnsigned),
				column("WARN", searchjobs.ValueKindUnsigned),
			),
			rows: [][]searchjobs.Value{
				row(searchjobs.TimeValue(base), searchjobs.UnsignedValue(2), searchjobs.UnsignedValue(3), searchjobs.UnsignedValue(1)),
				row(searchjobs.TimeValue(base.Add(5*time.Minute)), searchjobs.UnsignedValue(2), searchjobs.UnsignedValue(4), searchjobs.UnsignedValue(1)),
				row(searchjobs.TimeValue(base.Add(10*time.Minute)), searchjobs.UnsignedValue(2), searchjobs.UnsignedValue(4), searchjobs.UnsignedValue(1)),
			},
		},
		gradethiscorpus.SearchServerErrors: {
			schema: schema(
				column("_time", searchjobs.ValueKindTime),
				column("/api/assessments", searchjobs.ValueKindUnsigned),
				column("/api/submissions", searchjobs.ValueKindUnsigned),
			),
			rows: [][]searchjobs.Value{
				row(searchjobs.TimeValue(base), searchjobs.UnsignedValue(1), searchjobs.UnsignedValue(0)),
				row(searchjobs.TimeValue(base.Add(5*time.Minute)), searchjobs.UnsignedValue(2), searchjobs.UnsignedValue(0)),
				row(searchjobs.TimeValue(base.Add(10*time.Minute)), searchjobs.UnsignedValue(0), searchjobs.UnsignedValue(1)),
			},
		},
		gradethiscorpus.SearchResponses: {
			schema: schema(
				column("path", searchjobs.ValueKindString),
				column("status", searchjobs.ValueKindString),
				column("count", searchjobs.ValueKindUnsigned),
			),
			rows: [][]searchjobs.Value{
				row(searchjobs.StringValue("/api/assessments"), searchjobs.StringValue("200"), searchjobs.UnsignedValue(4)),
				row(searchjobs.StringValue("/api/assessments"), searchjobs.StringValue("503"), searchjobs.UnsignedValue(3)),
				row(searchjobs.StringValue("/api/submissions"), searchjobs.StringValue("200"), searchjobs.UnsignedValue(2)),
				row(searchjobs.StringValue("/api/submissions"), searchjobs.StringValue("500"), searchjobs.UnsignedValue(1)),
			},
		},
		gradethiscorpus.SearchSlowRoutes: {
			schema: schema(
				column("path", searchjobs.ValueKindString),
				column("count", searchjobs.ValueKindUnsigned),
				column("p95_ms", searchjobs.ValueKindDouble, true),
			),
			rows: [][]searchjobs.Value{
				row(searchjobs.StringValue("/api/assessments"), searchjobs.UnsignedValue(7), searchjobs.DoubleValue(800)),
			},
		},
		gradethiscorpus.SearchTopMessages: {
			schema: schema(
				column("message", searchjobs.ValueKindString, true),
				column("count", searchjobs.ValueKindUnsigned),
				column("percent", searchjobs.ValueKindDouble),
			),
			rows: [][]searchjobs.Value{
				row(searchjobs.StringValue("Request metrics"), searchjobs.UnsignedValue(10), searchjobs.DoubleValue(50)),
				row(searchjobs.StringValue("Heartbeat"), searchjobs.UnsignedValue(4), searchjobs.DoubleValue(20)),
				row(searchjobs.StringValue("Dependency retry scheduled"), searchjobs.UnsignedValue(3), searchjobs.DoubleValue(15)),
				row(searchjobs.StringValue("Database request failed"), searchjobs.UnsignedValue(2), searchjobs.DoubleValue(10)),
				row(searchjobs.StringValue("Request started"), searchjobs.UnsignedValue(1), searchjobs.DoubleValue(5)),
			},
		},
	}
}

func gradeThisEventSchema() searchjobs.Schema {
	return searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "_raw", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "index", Kind: searchjobs.ValueKindString},
		{Name: "host", Kind: searchjobs.ValueKindString},
		{Name: "source", Kind: searchjobs.ValueKindString},
		{Name: "sourcetype", Kind: searchjobs.ValueKindString},
		{Name: "service", Kind: searchjobs.ValueKindString, Nullable: true},
		{Name: "level", Kind: searchjobs.ValueKindString, Nullable: true},
		{Name: "message", Kind: searchjobs.ValueKindString, Nullable: true},
		{Name: "trace_id", Kind: searchjobs.ValueKindString, Nullable: true},
		{Name: "span_id", Kind: searchjobs.ValueKindString, Nullable: true},
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "_indextime", Kind: searchjobs.ValueKindTime},
		{Name: "fields", Kind: searchjobs.ValueKindObject},
	}}
}

func gradeThisEventRows(t *testing.T, fixture gradeThisDecodedFixture, ids []string) [][]searchjobs.Value {
	t.Helper()
	result := make([][]searchjobs.Value, 0, len(ids))
	for _, id := range ids {
		event := fixture.byID[id]
		if event == nil {
			t.Fatalf("fixture has no event %q", id)
		}
		result = append(result, []searchjobs.Value{
			searchjobs.TimeValue(event.GetEventTime().AsTime()),
			searchjobs.StringValue(string(event.GetRaw())),
			searchjobs.StringValue(event.GetIndexName()),
			searchjobs.StringValue(event.GetHost()),
			searchjobs.StringValue(event.GetSource()),
			searchjobs.StringValue(event.GetSourcetype()),
			gradeThisOptionalString(event.Service),
			gradeThisOptionalString(event.Level),
			gradeThisOptionalString(event.Message),
			gradeThisOptionalString(event.TraceId),
			gradeThisOptionalString(event.SpanId),
			searchjobs.StringValue(event.GetEventId()),
			searchjobs.TimeValue(fixture.profile.IndexTime.Truncate(time.Millisecond)),
			gradeThisObjectValue(t, event.GetFields()),
		})
	}
	return result
}

func gradeThisDynamicField(t *testing.T, event *opensplunk.LogEvent, name string) searchjobs.Value {
	t.Helper()
	for _, field := range event.GetFields().GetFields() {
		if field.GetName() == name {
			return gradeThisTypedValue(t, field.GetValue())
		}
	}
	t.Fatalf("event %q has no dynamic field %q", event.GetEventId(), name)
	return searchjobs.Value{}
}

func gradeThisOptionalString(value *string) searchjobs.Value {
	if value == nil {
		return searchjobs.NullValue()
	}
	return searchjobs.StringValue(*value)
}

func gradeThisObjectValue(t *testing.T, object *opensplunk.TypedObject) searchjobs.Value {
	t.Helper()
	fields := slices.Clone(object.GetFields())
	slices.SortFunc(fields, func(left, right *opensplunk.TypedObjectField) int {
		return strings.Compare(left.GetName(), right.GetName())
	})
	result := make([]searchjobs.ObjectField, 0, len(fields))
	for _, field := range fields {
		result = append(result, searchjobs.ObjectField{
			Name: field.GetName(), Value: gradeThisTypedValue(t, field.GetValue()),
		})
	}
	value, err := searchjobs.ObjectValue(result...)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func gradeThisTypedValue(t *testing.T, value *opensplunk.TypedValue) searchjobs.Value {
	t.Helper()
	if value == nil {
		t.Fatal("fixture typed value is nil")
	}
	switch kind := value.GetKind().(type) {
	case *opensplunk.TypedValue_NullValue:
		return searchjobs.NullValue()
	case *opensplunk.TypedValue_StringValue:
		return searchjobs.StringValue(kind.StringValue)
	case *opensplunk.TypedValue_Sint64Value:
		return searchjobs.SignedValue(kind.Sint64Value)
	case *opensplunk.TypedValue_Uint64Value:
		return searchjobs.UnsignedValue(kind.Uint64Value)
	case *opensplunk.TypedValue_DoubleValue:
		return searchjobs.DoubleValue(kind.DoubleValue)
	case *opensplunk.TypedValue_BoolValue:
		return searchjobs.BoolValue(kind.BoolValue)
	case *opensplunk.TypedValue_BytesValue:
		return searchjobs.BytesValue(kind.BytesValue)
	case *opensplunk.TypedValue_TimestampValue:
		return searchjobs.TimeValue(kind.TimestampValue.AsTime())
	case *opensplunk.TypedValue_DurationValue:
		return searchjobs.DurationValue(kind.DurationValue.AsDuration())
	case *opensplunk.TypedValue_ListValue:
		children := make([]searchjobs.Value, 0, len(kind.ListValue.GetValues()))
		for _, child := range kind.ListValue.GetValues() {
			children = append(children, gradeThisTypedValue(t, child))
		}
		return searchjobs.ListValue(children...)
	case *opensplunk.TypedValue_ObjectValue:
		return gradeThisObjectValue(t, kind.ObjectValue)
	case *opensplunk.TypedValue_DecimalValue:
		result, err := searchjobs.DecimalValue(kind.DecimalValue.GetValue())
		if err != nil {
			t.Fatal(err)
		}
		return result
	default:
		t.Fatalf("unsupported fixture typed value %T", value.GetKind())
		return searchjobs.Value{}
	}
}

func gradeThisReadAllPages(
	t *testing.T,
	manager *searchjobs.Manager,
	jobID string,
	wantSchema searchjobs.Schema,
	wantRows int,
) ([]searchjobs.ResultRow, int) {
	t.Helper()
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	var (
		cursor      string
		rows        []searchjobs.ResultRow
		total       uint64
		maxPages    uint64
		pageCount   int
		tamperCheck bool
		seenCursor  = map[string]struct{}{}
	)
	for {
		page, err := manager.ResultsFor(access, jobID, searchjobs.PageRequest{
			Limit: gradeThisCorpusPageSize, Cursor: cursor,
		})
		if err != nil {
			t.Fatal(err)
		}
		pageCount++
		if !reflect.DeepEqual(page.Schema, wantSchema) {
			t.Fatalf("page %d schema = %#v, want %#v", pageCount, page.Schema, wantSchema)
		}
		if pageCount == 1 {
			total = page.TotalRows
			if total != uint64(wantRows) {
				t.Fatalf("page total rows = %d, want %d", total, wantRows)
			}
			// MaxPageBytes may split a request below its row limit. Requiring
			// every nonterminal page to contain a row still makes TotalRows the
			// strict worst-case page ceiling.
			maxPages = total
			if maxPages == 0 {
				maxPages = 1
			}
		} else if page.TotalRows != total {
			t.Fatalf("page %d total rows = %d, want stable %d", pageCount, page.TotalRows, total)
		}
		if uint64(pageCount) > maxPages {
			t.Fatalf("page count = %d, maximum from total rows = %d", pageCount, maxPages)
		}
		if len(page.Rows) > gradeThisCorpusPageSize {
			t.Fatalf("page %d rows = %d, limit %d", pageCount, len(page.Rows), gradeThisCorpusPageSize)
		}
		for _, row := range page.Rows {
			if row.Ordinal != uint64(len(rows)) {
				t.Fatalf("row ordinal = %d, want %d", row.Ordinal, len(rows))
			}
			rows = append(rows, row)
		}
		if page.Complete {
			if page.NextCursor != "" {
				t.Fatalf("terminal page cursor = %q, want empty", page.NextCursor)
			}
			break
		}
		if len(page.Rows) == 0 {
			t.Fatalf("nonterminal page %d is empty", pageCount)
		}
		if !tamperCheck {
			tamperCheck = true
			if _, tamperErr := manager.ResultsFor(access, jobID, searchjobs.PageRequest{
				Limit: gradeThisCorpusPageSize, Cursor: "A" + page.NextCursor,
			}); !errors.Is(tamperErr, searchjobs.ErrInvalidCursor) {
				t.Fatalf("tampered cursor error = %v, want ErrInvalidCursor", tamperErr)
			}
		}
		if uint64(pageCount) >= maxPages {
			t.Fatalf("page %d remained nonterminal at the derived maximum %d", pageCount, maxPages)
		}
		if page.NextCursor == "" || page.NextCursor == cursor {
			t.Fatalf("nonterminal page %d cursor did not advance", pageCount)
		}
		if _, duplicate := seenCursor[page.NextCursor]; duplicate {
			t.Fatalf("page %d repeated cursor", pageCount)
		}
		seenCursor[page.NextCursor] = struct{}{}
		cursor = page.NextCursor
	}
	if uint64(len(rows)) != total {
		t.Fatalf("assembled rows = %d, total = %d", len(rows), total)
	}
	if wantRows > gradeThisCorpusPageSize && !tamperCheck {
		t.Fatal("multi-page result did not exercise signed-cursor tamper rejection")
	}
	return rows, pageCount
}

func gradeThisAssertResultScope(t *testing.T, manager *searchjobs.Manager, jobID string) {
	t.Helper()
	for name, access := range map[string]searchjobs.AccessScope{
		"other owner":  {TenantID: "tenant", OwnerID: "other-owner"},
		"other tenant": {TenantID: "other-tenant", OwnerID: "owner"},
	} {
		if _, err := manager.ResultsFor(access, jobID, searchjobs.PageRequest{
			Limit: gradeThisCorpusPageSize,
		}); !errors.Is(err, searchjobs.ErrNotFound) {
			t.Fatalf("%s result access error = %v, want ErrNotFound", name, err)
		}
	}
}

func gradeThisAssertRows(t *testing.T, got []searchjobs.ResultRow, want [][]searchjobs.Value) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rows = %d, want %d", len(got), len(want))
	}
	for rowIndex := range want {
		if len(got[rowIndex].Values) != len(want[rowIndex]) {
			t.Fatalf("row %d cells = %d, want %d", rowIndex, len(got[rowIndex].Values), len(want[rowIndex]))
		}
		for columnIndex := range want[rowIndex] {
			if !gradeThisValuesEqual(got[rowIndex].Values[columnIndex], want[rowIndex][columnIndex]) {
				t.Fatalf(
					"row %d column %d = %s, want %s",
					rowIndex, columnIndex,
					gradeThisDescribeValue(got[rowIndex].Values[columnIndex]),
					gradeThisDescribeValue(want[rowIndex][columnIndex]),
				)
			}
		}
	}
}

func gradeThisValuesEqual(left, right searchjobs.Value) bool {
	if left.Kind() != right.Kind() {
		return false
	}
	switch left.Kind() {
	case searchjobs.ValueKindNull:
		return true
	case searchjobs.ValueKindString:
		leftValue, leftOK := left.String()
		rightValue, rightOK := right.String()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindSigned:
		leftValue, leftOK := left.Signed()
		rightValue, rightOK := right.Signed()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindUnsigned:
		leftValue, leftOK := left.Unsigned()
		rightValue, rightOK := right.Unsigned()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindDouble:
		leftValue, leftOK := left.Double()
		rightValue, rightOK := right.Double()
		return leftOK && rightOK && math.Float64bits(leftValue) == math.Float64bits(rightValue)
	case searchjobs.ValueKindBool:
		leftValue, leftOK := left.Bool()
		rightValue, rightOK := right.Bool()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindBytes:
		leftValue, leftOK := left.Bytes()
		rightValue, rightOK := right.Bytes()
		return leftOK && rightOK && bytes.Equal(leftValue, rightValue)
	case searchjobs.ValueKindTime:
		leftValue, leftOK := left.Time()
		rightValue, rightOK := right.Time()
		return leftOK && rightOK && leftValue.Equal(rightValue)
	case searchjobs.ValueKindDuration:
		leftValue, leftOK := left.Duration()
		rightValue, rightOK := right.Duration()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindDecimal:
		leftValue, leftOK := left.Decimal()
		rightValue, rightOK := right.Decimal()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindList:
		leftValues, leftOK := left.List()
		rightValues, rightOK := right.List()
		if !leftOK || !rightOK || len(leftValues) != len(rightValues) {
			return false
		}
		for index := range leftValues {
			if !gradeThisValuesEqual(leftValues[index], rightValues[index]) {
				return false
			}
		}
		return true
	case searchjobs.ValueKindObject:
		leftFields, leftOK := left.Object()
		rightFields, rightOK := right.Object()
		if !leftOK || !rightOK || len(leftFields) != len(rightFields) {
			return false
		}
		for index := range leftFields {
			if leftFields[index].Name != rightFields[index].Name ||
				!gradeThisValuesEqual(leftFields[index].Value, rightFields[index].Value) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func gradeThisDescribeValue(value searchjobs.Value) string {
	switch value.Kind() {
	case searchjobs.ValueKindNull:
		return "null"
	case searchjobs.ValueKindString:
		result, _ := value.String()
		return fmt.Sprintf("string(%q)", result)
	case searchjobs.ValueKindSigned:
		result, _ := value.Signed()
		return fmt.Sprintf("signed(%d)", result)
	case searchjobs.ValueKindUnsigned:
		result, _ := value.Unsigned()
		return fmt.Sprintf("unsigned(%d)", result)
	case searchjobs.ValueKindDouble:
		result, _ := value.Double()
		return fmt.Sprintf("double(%g)", result)
	case searchjobs.ValueKindBool:
		result, _ := value.Bool()
		return fmt.Sprintf("bool(%v)", result)
	case searchjobs.ValueKindBytes:
		result, _ := value.Bytes()
		return fmt.Sprintf("bytes(%x)", result)
	case searchjobs.ValueKindTime:
		result, _ := value.Time()
		return fmt.Sprintf("time(%s)", result.Format(time.RFC3339Nano))
	case searchjobs.ValueKindDuration:
		result, _ := value.Duration()
		return fmt.Sprintf("duration(%s)", result)
	case searchjobs.ValueKindDecimal:
		result, _ := value.Decimal()
		return fmt.Sprintf("decimal(%s)", result)
	case searchjobs.ValueKindList:
		result, _ := value.List()
		return fmt.Sprintf("list(%v)", result)
	case searchjobs.ValueKindObject:
		result, _ := value.Object()
		return fmt.Sprintf("object(%v)", result)
	default:
		return fmt.Sprintf("kind(%d)", value.Kind())
	}
}
