package queryexec

import (
	"context"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func queryIntegrationTestLogicalRetention(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	executor *Executor,
) {
	t.Helper()

	const indexName = "logical-retention"
	const searchSource = `index=logical-retention | table message retention_marker`
	eventTime := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
	indexTime := eventTime.Add(time.Second)
	cutoff := time.Date(2099, time.January, 2, 3, 4, 5, 123_456_789, time.UTC)
	queryIntegrationInsertRetentionEvents(t, ctx, connection, indexName, eventTime, indexTime, cutoff)
	queryIntegrationAssertRetentionRowsRemainPhysical(t, ctx, connection, indexName, cutoff)

	t.Run("logical retention excludes physically present expired rows", func(t *testing.T) {
		compiler := clickhouse.Compiler{}

		searchPlan := queryIntegrationRetentionPlan(
			t,
			searchSource,
			indexName,
			eventTime,
			cutoff,
		)
		compiledSearch, err := compiler.Compile(searchPlan)
		if err != nil {
			t.Fatalf("Compile(search): %v", err)
		}
		searchSink := &fakeSink{}
		if err := executor.Execute(ctx, compiledSearch, searchSink); err != nil {
			t.Fatalf("Execute(search): %v\nSQL: %s\nargs: %#v", err, compiledSearch.SQL, compiledSearch.Args)
		}
		if len(searchSink.rows) != 1 || len(searchSink.rows[0]) != 2 {
			t.Fatalf("retained search rows = %#v, want one visible row", searchSink.rows)
		}
		queryIntegrationAssertRetentionValues(t, searchSink.rows[0])

		access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "logical-retention-owner"}
		manager, err := searchjobs.New(searchjobs.Config{
			Executor:        executor,
			Snapshotter:     queryIntegrationSnapshotter(1),
			Compiler:        compiler,
			MaxConcurrent:   1,
			MaxQueued:       1,
			CleanupInterval: -1,
			Now:             func() time.Time { return cutoff },
			NewID:           func() string { return "logical-retention-search" },
			CursorKey:       []byte("0123456789abcdef0123456789abcdef"),
		})
		if err != nil {
			t.Fatalf("searchjobs.New: %v", err)
		}
		defer func() {
			if err := manager.Close(); err != nil {
				t.Errorf("close logical-retention search manager: %v", err)
			}
		}()
		created, err := manager.Create(ctx, searchjobs.CreateRequest{
			SPL:               searchSource,
			OwnerID:           access.OwnerID,
			TenantID:          access.TenantID,
			AuthorizedIndexes: []string{indexName},
			RequestedIndexes:  []string{indexName},
			TimeRange:         queryIntegrationTimeRange(t, eventTime, eventTime.Add(time.Second)),
		})
		if err != nil {
			t.Fatalf("create logical-retention search: %v", err)
		}
		completed := queryIntegrationWaitForTerminal(t, manager, created.ID)
		if completed.State != searchjobs.StateCompleted || !completed.IndexTimeCutoff.Equal(cutoff) {
			t.Fatalf("logical-retention search = %+v, want completed at cutoff %v", completed, cutoff)
		}
		page, err := manager.ResultsFor(access, created.ID, searchjobs.PageRequest{Limit: 10})
		if err != nil {
			t.Fatalf("logical-retention ResultsFor: %v", err)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("logical-retention results = %#v, want one row", page.Rows)
		}
		queryIntegrationAssertRetentionValues(t, page.Rows[0].Values)
		preview, err := manager.PreviewFor(access, created.ID, 10)
		if err != nil {
			t.Fatalf("logical-retention PreviewFor: %v", err)
		}
		if len(preview.Rows) != 1 || preview.Truncated {
			t.Fatalf("logical-retention preview = %+v, want one complete row", preview)
		}
		queryIntegrationAssertRetentionValues(t, preview.Rows[0].Values)

		reexecution, err := exportjobs.NewReexecutionSource(exportjobs.ReexecutionSourceConfig{
			Searches: manager,
			Executor: executor,
			Compiler: compiler,
		})
		if err != nil {
			t.Fatalf("NewReexecutionSource: %v", err)
		}
		exportLease, err := reexecution.AcquireResultsFor(ctx, access, created.ID)
		if err != nil {
			t.Fatalf("AcquireResultsFor(export): %v", err)
		}
		defer func() {
			if err := exportLease.Close(); err != nil {
				t.Errorf("close logical-retention export lease: %v", err)
			}
		}()
		exportRow, ok, err := exportLease.Next(ctx)
		if err != nil || !ok {
			t.Fatalf("logical-retention export first row = (%+v, %t, %v)", exportRow, ok, err)
		}
		queryIntegrationAssertRetentionValues(t, exportRow.Values)
		if extra, ok, err := exportLease.Next(ctx); err != nil || ok {
			t.Fatalf("logical-retention export extra row = (%+v, %t, %v)", extra, ok, err)
		}

		analysisPlan := queryIntegrationRetentionPlan(
			t,
			`index=logical-retention`,
			indexName,
			eventTime,
			cutoff,
		)
		timelineSpec := clickhouse.TimelineSpec{
			FirstBucket: eventTime,
			SpanSeconds: 1,
			BucketCount: 1,
			Earliest:    eventTime,
			Latest:      eventTime.Add(time.Second),
		}
		compiledTimeline, err := compiler.CompileTimeline(analysisPlan, timelineSpec)
		if err != nil {
			t.Fatalf("CompileTimeline: %v", err)
		}
		buckets, err := executor.ExecuteTimeline(ctx, compiledTimeline)
		if err != nil {
			t.Fatalf("ExecuteTimeline: %v\nSQL: %s\nargs: %#v", err, compiledTimeline.SQL, compiledTimeline.Args)
		}
		if len(buckets) != 1 || buckets[0].Count != 1 || !buckets[0].AlignedStart.Equal(eventTime) {
			t.Fatalf("retained timeline = %#v, want one visible event", buckets)
		}

		compiledCatalog, err := compiler.CompileFieldCatalog(
			analysisPlan,
			clickhouse.FieldCatalogSpec{MaximumFields: 100},
		)
		if err != nil {
			t.Fatalf("CompileFieldCatalog: %v", err)
		}
		catalog, err := executor.ExecuteFieldCatalog(ctx, compiledCatalog)
		if err != nil {
			t.Fatalf("ExecuteFieldCatalog: %v\nSQL: %s\nargs: %#v", err, compiledCatalog.SQL, compiledCatalog.Args)
		}
		if catalog.TotalEvents != 1 {
			t.Fatalf("retained field catalog total = %d, want 1", catalog.TotalEvents)
		}
		sawMarker := false
		for _, profile := range catalog.Fields {
			switch profile.FieldName {
			case "expired_only":
				t.Fatalf("retained field catalog exposed expired-only profile: %#v", profile)
			case "retention_marker":
				sawMarker = profile.EventCount == 1 && profile.MissingCount == 0
			}
		}
		if !sawMarker {
			t.Fatalf("retained field catalog lacks visible marker profile: %#v", catalog)
		}

		compiledSummary, err := compiler.CompileFieldSummary(
			analysisPlan,
			clickhouse.FieldSummarySpec{
				FieldName:             "retention_marker",
				MaximumValues:         10,
				MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues,
				MaximumValueBytes:     clickhouse.MaximumFieldSummaryValueBytes,
			},
		)
		if err != nil {
			t.Fatalf("CompileFieldSummary: %v", err)
		}
		summary, err := executor.ExecuteFieldSummary(ctx, compiledSummary)
		if err != nil {
			t.Fatalf("ExecuteFieldSummary: %v\nSQL: %s\nargs: %#v", err, compiledSummary.SQL, compiledSummary.Args)
		}
		if summary.EventCount != 1 || summary.MissingCount != 0 || summary.DistinctCount != 1 ||
			len(summary.TopValues) != 1 || summary.TopValues[0].Count != 1 {
			t.Fatalf("retained field summary = %#v", summary)
		}
		if value, ok := summary.TopValues[0].Value.String(); !ok || value != "visible-after" {
			t.Fatalf("retained field summary value = %q, %t; summary=%#v", value, ok, summary)
		}
	})

	queryIntegrationAssertRetentionRowsRemainPhysical(t, ctx, connection, indexName, cutoff)
}

func queryIntegrationAssertRetentionValues(t *testing.T, values []searchjobs.Value) {
	t.Helper()
	if len(values) != 2 {
		t.Fatalf("logical-retention values = %#v, want two cells", values)
	}
	message, messageOK := values[0].String()
	marker, markerOK := values[1].String()
	if !messageOK || !markerOK || message != "visible-after" || marker != "visible-after" {
		t.Fatalf(
			"logical-retention values = (%q, %t, %q, %t), want visible-after",
			message,
			messageOK,
			marker,
			markerOK,
		)
	}
}

func queryIntegrationInsertRetentionEvents(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	indexName string,
	eventTime time.Time,
	indexTime time.Time,
	cutoff time.Time,
) {
	t.Helper()

	query := "INSERT INTO open_splunk.events (" +
		"event_id, tenant_id, index_name, event_time, index_time, body, raw, raw_encoding, " +
		"fields, field_names, field_types, field_metadata_version, collector_id, batch_id, " +
		"batch_sequence, expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatalf("prepare logical-retention batch: %v", err)
	}
	storedBoundary := cutoff.Truncate(time.Millisecond)
	fixtures := []struct {
		id        string
		expiresAt time.Time
	}{
		{id: "expired-before", expiresAt: storedBoundary.Add(-time.Millisecond)},
		{id: "expired-equal", expiresAt: storedBoundary},
		{id: "visible-after", expiresAt: storedBoundary.Add(time.Millisecond)},
	}
	for index, fixture := range fixtures {
		document := clickhousedriver.NewJSON()
		document.SetValueAtPath("retention_marker", clickhousedriver.NewDynamic(fixture.id))
		fieldNames := []string{"retention_marker"}
		fieldTypes := []uint8{uint8(eventfields.StoredValueTypeString)}
		if !fixture.expiresAt.After(storedBoundary) {
			document.SetValueAtPath("expired_only", clickhousedriver.NewDynamic(true))
			fieldNames = []string{"expired_only", "retention_marker"}
			fieldTypes = []uint8{
				uint8(eventfields.StoredValueTypeBool),
				uint8(eventfields.StoredValueTypeString),
			}
		}
		message := fixture.id
		if err := batch.Append(
			"logical-retention-"+fixture.id,
			"tenant",
			indexName,
			eventTime.Add(time.Duration(index)*time.Millisecond),
			indexTime,
			&message,
			[]byte(message),
			uint8(1),
			document,
			fieldNames,
			fieldTypes,
			eventfields.CurrentFieldMetadataVersion,
			"collector",
			"logical-retention-batch",
			uint64(index+1),
			fixture.expiresAt,
			uint64(1),
		); err != nil {
			t.Fatalf("append logical-retention row %q: %v", fixture.id, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send logical-retention batch: %v", err)
	}
}

func queryIntegrationAssertRetentionRowsRemainPhysical(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	indexName string,
	cutoff time.Time,
) {
	t.Helper()

	var physicalRows, logicallyExpiredRows uint64
	if err := connection.QueryRow(
		ctx,
		"SELECT count(), countIf(expires_at <= parseDateTime64BestEffort(?, 3, 'UTC')) "+
			"FROM open_splunk.events WHERE tenant_id = ? AND index_name = ?",
		cutoff.UTC().Format("2006-01-02 15:04:05.000"),
		"tenant",
		indexName,
	).Scan(&physicalRows, &logicallyExpiredRows); err != nil {
		t.Fatalf("read physical logical-retention rows: %v", err)
	}
	if physicalRows != 3 || logicallyExpiredRows != 2 {
		t.Fatalf(
			"physical logical-retention rows = %d, expired at cutoff = %d; want 3/2",
			physicalRows,
			logicallyExpiredRows,
		)
	}
}

func queryIntegrationRetentionPlan(
	t *testing.T,
	source string,
	indexName string,
	eventTime time.Time,
	cutoff time.Time,
) *plan.Query {
	t.Helper()

	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse logical-retention SPL: %v", err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{indexName},
		RequestedIndexes:  []string{indexName},
		Earliest:          eventTime,
		Latest:            eventTime.Add(time.Second),
		IndexTimeCutoff:   cutoff,
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatalf("build logical-retention plan: %v", err)
	}
	return logical
}
