package queryexec

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestExpressionV02ExecutorManagerAgainstClickHouse is the transport-level
// v0.2 acceptance gate. Trusted typed events pass through the production Store;
// parser, planner, compiler, Executor, Manager, paging, and field inspection
// then operate on the same immutable visibility snapshot.
func TestExpressionV02ExecutorManagerAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse image: %s", container.Image)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupCtx); closeErr != nil {
			t.Errorf("close ClickHouse expression v0.2 container: %v", closeErr)
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
	executor, err := New(queryConnection, Config{ReadAdmission: indexread.UnfencedAdmission{}})
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
		if closeErr := sequencer.Close(); closeErr != nil {
			t.Errorf("close expression v0.2 visibility sequencer: %v", closeErr)
		}
	})
	store, err := clickhouse.NewStore(
		storeConnection,
		clickhouse.RetentionProviderFunc(func(context.Context, string, string) (time.Duration, error) {
			return 24 * time.Hour, nil
		}),
		sequencer,
	)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close expression v0.2 store: %v", closeErr)
		}
	})

	indexTime := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	expressionV02StoreFixture(t, ctx, store, indexTime)
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cutoff != 1 {
		t.Fatalf("visibility cutoff = %d, want 1", cutoff)
	}

	var nextID atomic.Uint64
	executionErrors := make(chan error, 1)
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     queryIntegrationSnapshotter(cutoff),
		Compiler:        clickhouse.Compiler{},
		MaxConcurrent:   1,
		MaxQueued:       1,
		CleanupInterval: -1,
		Now:             func() time.Time { return indexTime.Add(time.Second) },
		NewID: func() string {
			return fmt.Sprintf("expression-v02-vertical-%02d", nextID.Add(1))
		},
		CursorKey:   []byte("0123456789abcdef0123456789abcdef"),
		CursorScope: "expression-v0.2-vertical",
		OnExecutionError: func(_ string, _ searchjobs.FailureCode, cause error) {
			select {
			case executionErrors <- cause:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	run := func(source string) (searchjobs.Job, searchjobs.ResultPage) {
		t.Helper()
		created, createErr := manager.Create(ctx, searchjobs.CreateRequest{
			SPL: source, OwnerID: "owner", TenantID: "tenant",
			AuthorizedIndexes: []string{"expression-v02"},
			RequestedIndexes:  []string{"expression-v02"},
			TimeRange: queryIntegrationTimeRange(
				t, indexTime.Add(-time.Hour), indexTime.Add(time.Hour),
			),
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		terminal := queryIntegrationWaitForTerminal(t, manager, created.ID)
		if terminal.State != searchjobs.StateCompleted || terminal.Failure != nil {
			t.Fatalf("terminal job = state %v failure %#v", terminal.State, terminal.Failure)
		}
		page, resultErr := manager.Results(created.ID, searchjobs.PageRequest{Limit: 2})
		if resultErr != nil {
			t.Fatal(resultErr)
		}
		return terminal, page
	}

	compile := func(source string) clickhouse.CompiledQuery {
		t.Helper()
		logical := expressionV02VerticalPlan(t, indexTime, cutoff, source)
		compiled, compileErr := (clickhouse.Compiler{}).Compile(logical)
		if compileErr != nil {
			t.Fatalf("compile v0.2 vertical SPL %q: %v", source, compileErr)
		}
		if !compiled.RequiresAtomicResult() {
			t.Fatalf("v0.2 maximum shape did not require atomic execution: %q", source)
		}
		return compiled
	}

	t.Run("typed arithmetic sparse null and signed zero through paging", func(t *testing.T) {
		terminal, first := run(
			`index=expression-v02 source="sdet-v02" | eval sum=dyn_int+1,string_half=dyn_string/2,decimal_sum=dyn_decimal+0.75,zero=1/0,negative_zero=0.0/-2.0 | sort event_id | table event_id,sum,string_half,decimal_sum,zero,negative_zero`,
		)
		if terminal.RowCount != 4 || terminal.ResultsTruncated {
			t.Fatalf("terminal result = rows %d truncated %v", terminal.RowCount, terminal.ResultsTruncated)
		}
		queryIntegrationAssertColumns(t, first, []string{
			"event_id", "sum", "string_half", "decimal_sum", "zero", "negative_zero",
		})
		if len(first.Rows) != 2 || first.NextCursor == "" {
			t.Fatalf("first page = rows %d cursor %q", len(first.Rows), first.NextCursor)
		}
		second, pageErr := manager.Results(terminal.ID, searchjobs.PageRequest{Limit: 2, Cursor: first.NextCursor})
		if pageErr != nil {
			t.Fatal(pageErr)
		}
		if len(second.Rows) != 2 || second.NextCursor != "" {
			t.Fatalf("second page = rows %d cursor %q", len(second.Rows), second.NextCursor)
		}
		rows := append(append([]searchjobs.ResultRow(nil), first.Rows...), second.Rows...)
		ids := []string{"a-finite", "b-explicit-null", "c-missing", "d-finite"}
		for rowIndex, id := range ids {
			gotID, ok := rows[rowIndex].Values[0].String()
			if !ok || gotID != id {
				t.Fatalf("row %d ID = %q/%v, want %q", rowIndex, gotID, ok, id)
			}
			if !rows[rowIndex].Values[4].IsNull() {
				t.Fatalf("row %d zero division = %#v, want null", rowIndex, rows[rowIndex].Values[4])
			}
			negativeZero, ok := rows[rowIndex].Values[5].Double()
			if !ok || negativeZero != 0 || !isNegativeZero(negativeZero) {
				t.Fatalf("row %d negative zero = %v/%v", rowIndex, negativeZero, ok)
			}
		}
		expressionV02AssertDouble(t, rows[0].Values[1], 6, "a sum")
		expressionV02AssertDouble(t, rows[0].Values[2], 6.25, "a numeric String")
		expressionV02AssertDouble(t, rows[0].Values[3], 4, "a Decimal")
		if !rows[1].Values[1].IsNull() || !rows[2].Values[1].IsNull() {
			t.Fatalf("explicit-null/missing arithmetic = %#v/%#v, want null/null", rows[1].Values[1], rows[2].Values[1])
		}
		expressionV02AssertDouble(t, rows[3].Values[1], 8, "d sum")
	})

	t.Run("membership truth table through manager", func(t *testing.T) {
		_, page := run(
			`index=expression-v02 source="sdet-v02" | where status IN ("A", "C", null) | sort event_id | table event_id`,
		)
		if len(page.Rows) != 2 {
			t.Fatalf("membership rows = %d, want 2", len(page.Rows))
		}
		for index, want := range []string{"a-finite", "d-finite"} {
			got, ok := page.Rows[index].Values[0].String()
			if !ok || got != want {
				t.Fatalf("membership row %d = %q/%v, want %q", index, got, ok, want)
			}
		}
	})

	t.Run("maximum arithmetic cancellation is prompt and publishes nothing", func(t *testing.T) {
		compiled := compile(expressionV02MaximumArithmeticSource())
		cancelContext, cancelQuery := context.WithCancel(ctx)
		defer cancelQuery()
		sink := &expressionV02CancelOnProgressSink{cancel: cancelQuery}
		started := time.Now()
		executeErr := executor.Execute(cancelContext, compiled, sink)
		elapsed := time.Since(started)
		if !errors.Is(executeErr, context.Canceled) {
			t.Fatalf("maximum arithmetic cancellation error = %v, want context.Canceled", executeErr)
		}
		progressCalls, scannedRows, canceledAt := sink.progressSnapshot()
		if progressCalls == 0 || scannedRows == 0 || canceledAt.IsZero() {
			t.Fatalf(
				"maximum arithmetic cancellation progress = calls %d rows %d at %v",
				progressCalls,
				scannedRows,
				canceledAt,
			)
		}
		prompt := time.Since(canceledAt)
		if prompt > 2*time.Second {
			t.Fatalf("maximum arithmetic cancellation returned after %v, want at most 2s", prompt)
		}
		if sink.setCalls != 0 || len(sink.rows) != 0 {
			t.Fatalf(
				"canceled maximum arithmetic published schema=%d rows=%d",
				sink.setCalls,
				len(sink.rows),
			)
		}
		t.Logf(
			"maximum arithmetic native cancellation returned in %v (%v total; %d scanned rows)",
			prompt,
			elapsed,
			scannedRows,
		)
	})

	t.Run("maximum membership obeys the native read limit atomically", func(t *testing.T) {
		compiled := compile(expressionV02MaximumMembershipSource())
		bounded, boundedErr := New(queryConnection, Config{
			MaxRowsToRead: 1,
			ReadAdmission: indexread.UnfencedAdmission{},
		})
		if boundedErr != nil {
			t.Fatal(boundedErr)
		}
		sink := &fakeSink{}
		started := time.Now()
		executeErr := bounded.Execute(ctx, compiled, sink)
		elapsed := time.Since(started)
		if !errors.Is(executeErr, searchjobs.ErrExecutionLimit) {
			t.Fatalf("maximum membership read-limit error = %v, want ErrExecutionLimit", executeErr)
		}
		if sink.setCalls != 0 || len(sink.rows) != 0 {
			t.Fatalf(
				"read-limited maximum membership published schema=%d rows=%d",
				sink.setCalls,
				len(sink.rows),
			)
		}
		t.Logf("maximum membership native read limit returned in %v", elapsed)
	})

	t.Run("field catalog and summary preserve missing versus null", func(t *testing.T) {
		logical := expressionV02VerticalPlan(t, indexTime, cutoff,
			`index=expression-v02 source="sdet-v02" | eval adjusted=dyn_int+1`)
		catalogQuery, compileErr := (clickhouse.Compiler{}).CompileFieldCatalog(
			logical, clickhouse.FieldCatalogSpec{MaximumFields: 64},
		)
		if compileErr != nil {
			t.Fatal(compileErr)
		}
		catalog, executeErr := executor.ExecuteFieldCatalog(ctx, catalogQuery)
		if executeErr != nil {
			t.Fatal(executeErr)
		}
		if catalog.TotalEvents != 4 {
			t.Fatalf("catalog total events = %d, want 4", catalog.TotalEvents)
		}
		var profile, adjustedProfile *FieldProfileRow
		for index := range catalog.Fields {
			if catalog.Fields[index].FieldName == "dyn_int" {
				profile = &catalog.Fields[index]
			}
			if catalog.Fields[index].FieldName == "adjusted" {
				adjustedProfile = &catalog.Fields[index]
			}
		}
		if profile == nil || profile.EventCount != 3 || profile.NullCount != 1 || profile.MissingCount != 1 {
			t.Fatalf("dyn_int catalog profile = %#v, want events/null/missing 3/1/1", profile)
		}
		if adjustedProfile == nil || adjustedProfile.EventCount != 4 ||
			adjustedProfile.NullCount != 2 || adjustedProfile.MissingCount != 0 {
			t.Fatalf("adjusted catalog profile = %#v, want events/null/missing 4/2/0", adjustedProfile)
		}

		summaryQuery, compileErr := (clickhouse.Compiler{}).CompileFieldSummary(logical, clickhouse.FieldSummarySpec{
			FieldName:             "adjusted",
			MaximumValues:         10,
			MaximumDistinctValues: 100,
			MaximumValueBytes:     4_096,
		})
		if compileErr != nil {
			t.Fatal(compileErr)
		}
		summary, executeErr := executor.ExecuteFieldSummary(ctx, summaryQuery)
		if executeErr != nil {
			t.Fatal(executeErr)
		}
		if summary.FieldName != "adjusted" || summary.EventCount != 4 || summary.NullCount != 2 ||
			summary.MissingCount != 0 || summary.DistinctCount != 2 || len(summary.TopValues) != 2 {
			t.Fatalf("adjusted summary = %#v", summary)
		}
		got := make(map[float64]uint64, 2)
		for _, value := range summary.TopValues {
			double, ok := value.Value.Double()
			if !ok {
				t.Fatalf("summary value = %#v, want Double", value.Value)
			}
			got[double] = value.Count
		}
		if !reflect.DeepEqual(got, map[float64]uint64{6: 1, 8: 1}) {
			t.Fatalf("summary values = %#v", got)
		}
	})

	t.Run("malformed semantic tag fails atomically with no visible prefix", func(t *testing.T) {
		expressionV02InsertMalformedDecimal(t, ctx, queryConnection, indexTime, cutoff)
		compiledPlan := expressionV02VerticalPlan(t, indexTime, cutoff,
			`index=expression-v02 source="sdet-v02" | eval result=malformed+1 | sort event_id | table event_id,result`)
		compiled, compileErr := (clickhouse.Compiler{}).Compile(compiledPlan)
		if compileErr != nil {
			t.Fatal(compileErr)
		}
		if !compiled.RequiresAtomicResult() {
			t.Fatal("malformed-tag arithmetic query did not retain atomic-result evidence")
		}

		created, createErr := manager.Create(ctx, searchjobs.CreateRequest{
			SPL:     `index=expression-v02 source="sdet-v02" | eval result=malformed+1 | sort event_id | table event_id,result`,
			OwnerID: "owner", TenantID: "tenant",
			AuthorizedIndexes: []string{"expression-v02"}, RequestedIndexes: []string{"expression-v02"},
			TimeRange: queryIntegrationTimeRange(t, indexTime.Add(-time.Hour), indexTime.Add(time.Hour)),
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		terminal := queryIntegrationWaitForTerminal(t, manager, created.ID)
		if terminal.State != searchjobs.StateFailed || terminal.RowCount != 0 || terminal.Schema != nil ||
			terminal.ResultsTruncated || terminal.Failure == nil || terminal.Failure.Code != searchjobs.FailureExecution {
			t.Fatalf("atomic failure terminal = %#v", terminal)
		}
		if _, resultErr := manager.Results(created.ID, searchjobs.PageRequest{Limit: 2}); !errors.Is(resultErr, searchjobs.ErrResultsUnavailable) {
			t.Fatalf("failed result access = %v, want ErrResultsUnavailable", resultErr)
		}
		if terminal.Failure.Message == "" || strings.Contains(terminal.Failure.Message, "malformed-secret-1e") {
			t.Fatalf("unsafe public failure = %#v", terminal.Failure)
		}
		select {
		case cause := <-executionErrors:
			if cause == nil || !strings.Contains(cause.Error(), clickhouse.UnsupportedExpressionValueMarker) ||
				strings.Contains(cause.Error(), "malformed-secret-1e") {
				t.Fatalf("unsafe or unclassified internal execution error = %v", cause)
			}
		default:
			t.Fatal("manager did not report the malformed-tag execution cause")
		}
	})
}

func expressionV02StoreFixture(t *testing.T, ctx context.Context, store *clickhouse.Store, indexTime time.Time) {
	t.Helper()
	events := []*ingest.StoredEvent{
		expressionV02VerticalEvent(indexTime, "a-finite",
			expressionV02VerticalField("dyn_int", expressionV02VerticalSint(5)),
			expressionV02VerticalField("dyn_string", expressionV02VerticalString("12.5")),
			expressionV02VerticalField("dyn_decimal", expressionV02VerticalDecimal("3.25")),
			expressionV02VerticalField("status", expressionV02VerticalString("A")),
		),
		expressionV02VerticalEvent(indexTime, "b-explicit-null",
			expressionV02VerticalField("dyn_int", expressionV02VerticalNull()),
			expressionV02VerticalField("status", expressionV02VerticalString("B")),
		),
		expressionV02VerticalEvent(indexTime, "c-missing",
			expressionV02VerticalField("status", expressionV02VerticalNull()),
		),
		expressionV02VerticalEvent(indexTime, "d-finite",
			expressionV02VerticalField("dyn_int", expressionV02VerticalSint(7)),
			expressionV02VerticalField("status", expressionV02VerticalString("C")),
		),
	}
	digest := sha256.Sum256([]byte("expression-v0.2-vertical-fixture"))
	result, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        "expression-v02-collector",
		BatchID:            "expression-v02-batch",
		BatchSequence:      1,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  digest,
		ReceivedAt:         indexTime,
		Events:             events,
		RetentionByIndex:   map[string]time.Duration{"expression-v02": 24 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted != uint32(len(events)) || result.Duplicate != 0 {
		t.Fatalf("store result = %#v", result)
	}
}

func expressionV02VerticalEvent(
	indexTime time.Time,
	eventID string,
	fields ...*opensplunkv1.TypedObjectField,
) *ingest.StoredEvent {
	eventTime := indexTime.Add(-time.Minute)
	message := "expression v0.2 SDET fixture"
	return &ingest.StoredEvent{
		TenantID: "tenant", CollectorID: "expression-v02-collector",
		BatchID: "expression-v02-batch", IndexTime: indexTime,
		Event: &opensplunkv1.LogEvent{
			EventId: eventID, IndexName: "expression-v02", EventTime: timestamppb.New(eventTime),
			CollectedAt:     timestamppb.New(eventTime.Add(time.Second)),
			EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            "sdet-host", Source: "sdet-v02", Sourcetype: "open-splunk:sdet",
			Severity: opensplunkv1.LogSeverity_LOG_SEVERITY_INFO, Message: &message,
			Raw: []byte(message), RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			Fields: &opensplunkv1.TypedObject{Fields: fields},
		},
	}
}

func expressionV02VerticalField(name string, value *opensplunkv1.TypedValue) *opensplunkv1.TypedObjectField {
	return &opensplunkv1.TypedObjectField{Name: name, Value: value}
}

func expressionV02VerticalNull() *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_NullValue{
		NullValue: opensplunkv1.NullValue_NULL_VALUE_NULL,
	}}
}

func expressionV02VerticalString(value string) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_StringValue{StringValue: value}}
}

func expressionV02VerticalSint(value int64) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_Sint64Value{Sint64Value: value}}
}

func expressionV02VerticalDecimal(value string) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_DecimalValue{
		DecimalValue: &opensplunkv1.DecimalValue{Value: value},
	}}
}

func expressionV02VerticalPlan(
	t *testing.T,
	indexTime time.Time,
	visibilityCutoff uint64,
	source string,
) *plan.Query {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse v0.2 vertical SPL %q: %v", source, err)
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID: "tenant", AuthorizedIndexes: []string{"expression-v02"},
		RequestedIndexes: []string{"expression-v02"},
		Earliest:         indexTime.Add(-time.Hour), Latest: indexTime.Add(time.Hour),
		SearchStart: indexTime.Add(time.Second), SearchTimezone: "UTC",
		IndexTimeCutoff: indexTime.Add(time.Second), VisibilityCutoff: &visibilityCutoff,
	})
	if err != nil {
		t.Fatalf("build v0.2 vertical SPL %q: %v", source, err)
	}
	return logical
}

func expressionV02InsertMalformedDecimal(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	indexTime time.Time,
	visibilityCutoff uint64,
) {
	t.Helper()
	const insert = `INSERT INTO open_splunk.events (` +
		`event_id, tenant_id, index_name, event_time, index_time, collected_at, event_time_source, ` +
		`host, source, sourcetype, service, severity, level, body, raw, raw_encoding, trace_id, span_id, ` +
		`fields, field_names, collector_id, ingest_source_kind, ingest_source_id, batch_id, batch_sequence, ` +
		`expires_at, visibility_seq, field_types, field_metadata_version)`
	batch, err := connection.PrepareBatch(ctx, insert)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = batch.Close() }()
	document := clickhousedriver.NewJSON()
	document.SetValueAtPath("malformed", clickhousedriver.NewDynamicWithType(map[string]string{
		"\x00open_splunk_type": "decimal/v1", "\x00open_splunk_value": "malformed-secret-1e",
	}, "Map(String, String)"))
	if err := batch.Append(
		"z-malformed", "tenant", "expression-v02", indexTime.Add(-time.Minute), indexTime,
		nil, uint8(opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED), "sdet-host", "sdet-v02", "open-splunk:sdet",
		nil, uint8(opensplunkv1.LogSeverity_LOG_SEVERITY_INFO), nil, nil, []byte("redacted poison fixture"),
		uint8(opensplunkv1.RawEncoding_RAW_ENCODING_UTF8), nil, nil, document, []string{"malformed"},
		"expression-v02-collector", uint8(ingest.IngestionSourceKindNativeCollector), "expression-v02-collector",
		"expression-v02-poison", uint64(2), indexTime.Add(24*time.Hour), visibilityCutoff,
		[]uint8{uint8(eventfields.StoredValueTypeDecimal)}, eventfields.CurrentFieldMetadataVersion,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func expressionV02AssertDouble(t *testing.T, value searchjobs.Value, want float64, label string) {
	t.Helper()
	got, ok := value.Double()
	if !ok || got != want {
		t.Fatalf("%s = %v/%v, want %v", label, got, ok, want)
	}
}

func isNegativeZero(value float64) bool { return 1/value < 0 }

func expressionV02MaximumArithmeticSource() string {
	return `index=expression-v02 source="sdet-v02" | eval result=dyn_int` +
		strings.Repeat(`+0`, spl.MaximumArithmeticOperatorsPerQuery) +
		` | table result`
}

func expressionV02MaximumMembershipSource() string {
	candidates := make([]string, spl.MaximumMembershipCandidates)
	for index := range candidates {
		candidates[index] = fmt.Sprintf("%d", index)
	}
	return `index=expression-v02 source="sdet-v02" | where dyn_int IN (` +
		strings.Join(candidates, ",") + `) | table event_id`
}

type expressionV02CancelOnProgressSink struct {
	fakeSink

	mu            sync.Mutex
	cancel        context.CancelFunc
	progressCalls int
	scannedRows   uint64
	canceledAt    time.Time
}

func (sink *expressionV02CancelOnProgressSink) ReportProgress(
	delta searchjobs.ExecutionProgressDelta,
) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.progressCalls++
	sink.scannedRows += delta.ScannedRows
	if sink.canceledAt.IsZero() {
		sink.canceledAt = time.Now()
		sink.cancel()
	}
	return nil
}

func (sink *expressionV02CancelOnProgressSink) progressSnapshot() (
	int,
	uint64,
	time.Time,
) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.progressCalls, sink.scannedRows, sink.canceledAt
}
