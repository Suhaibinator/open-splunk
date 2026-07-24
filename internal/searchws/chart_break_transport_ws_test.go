package searchws

import (
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const chartBreakTransportPivotSPL = "index=main | chart count OVER path BY series"

// chartBreakTransportRuntimeNames are legal chart column labels: the pivot
// names every column after the first from a field value, so an export or a
// subscriber sees delimiters, quotes, and non-ASCII text in column positions.
var chartBreakTransportRuntimeNames = []string{"a,b", "a\"b", "VALUE_audit", "naïve"}

// chartBreakTransportPivotJob builds a completed buffered-pivot search whose
// public schema is one plan-time row column followed by runtime-named counts.
func chartBreakTransportPivotJob(id string, names ...string) searchjobs.Job {
	job := scopedSearchJob(id)
	job.SPL = chartBreakTransportPivotSPL
	job.Version = 9
	job.State = searchjobs.StateCompleted
	job.FinishedAt = job.StartedAt.Add(time.Second)
	columns := make([]searchjobs.Column, 0, len(names)+1)
	columns = append(columns, searchjobs.Column{Name: "path", Kind: searchjobs.ValueKindString})
	for _, name := range names {
		columns = append(columns, searchjobs.Column{Name: name, Kind: searchjobs.ValueKindUnsigned})
	}
	job.Schema = &searchjobs.Schema{Columns: columns}
	return job
}

func chartBreakTransportPivotRows(labels []string, counts [][]uint64) []searchjobs.ResultRow {
	rows := make([]searchjobs.ResultRow, len(labels))
	for index, label := range labels {
		values := make([]searchjobs.Value, 0, len(counts[index])+1)
		values = append(values, searchjobs.StringValue(label))
		for _, count := range counts[index] {
			values = append(values, searchjobs.UnsignedValue(count))
		}
		rows[index] = searchjobs.ResultRow{Ordinal: uint64(index), Values: values}
	}
	return rows
}

// TestChartBreakTransportWebSocketReplaysTheWholePivotAfterReconnect answers
// what a preview subscriber sees for a buffered terminal chart: the pivot only
// ever exists as one complete unit, so a subscriber that reconnects and
// replays from the same sequence must receive byte-identical runtime column
// names, cells, and sequence numbers.
func TestChartBreakTransportWebSocketReplaysTheWholePivotAfterReconnect(t *testing.T) {
	job := chartBreakTransportPivotJob("search-chart-replay", chartBreakTransportRuntimeNames...)
	labels := []string{"/a", "/b"}
	counts := [][]uint64{{1, 2, 3, 4}, {0, 0, 5, 0}}
	reader := newPreviewSearchSnapshots(previewSnapshot(job, chartBreakTransportPivotRows(labels, counts)...))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 4
	})

	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("pivot", "pivot", job.ID, 0, 4))
	observation := readPreviewObservations(t, client, "pivot")["pivot"]

	schema := observation.schema.GetResultSchemaAvailable().GetSchema()
	if schema.GetResultKind() != opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS {
		t.Fatalf("pivot result kind = %v, want statistics", schema.GetResultKind())
	}
	assertChartBreakTransportPivotSchema(t, schema, job)
	assertChartBreakTransportPivotPreview(t, observation.preview.GetResultPreview(), labels, counts)

	// Reconnect as a brand-new connection and replay from before the schema.
	// The chart's runtime-named columns must survive the replay path exactly.
	reconnected := fixture.dial()
	writeCommand(t, reconnected, subscribeWithPreview("resume", "resume", job.ID, 0, 4))
	replayed := readPreviewObservations(t, reconnected, "resume")["resume"]
	if replayed.schema.GetSequence() != observation.schema.GetSequence() ||
		replayed.preview.GetSequence() != observation.preview.GetSequence() {
		t.Fatalf("replayed sequences = schema %d preview %d, want %d and %d",
			replayed.schema.GetSequence(), replayed.preview.GetSequence(),
			observation.schema.GetSequence(), observation.preview.GetSequence())
	}
	assertChartBreakTransportPivotSchema(t, replayed.schema.GetResultSchemaAvailable().GetSchema(), job)
	assertChartBreakTransportPivotPreview(t, replayed.preview.GetResultPreview(), labels, counts)
}

// TestChartBreakTransportWebSocketBoundedPreviewOfPivotIsAPrefix proves a
// subscriber whose row budget is smaller than the buffered pivot receives a
// truncated prefix of the row axis with the complete column axis, never a
// partial row.
func TestChartBreakTransportWebSocketBoundedPreviewOfPivotIsAPrefix(t *testing.T) {
	job := chartBreakTransportPivotJob("search-chart-bounded", chartBreakTransportRuntimeNames...)
	labels := []string{"/a", "/b", "/c"}
	counts := [][]uint64{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}}
	reader := newPreviewSearchSnapshots(previewSnapshot(job, chartBreakTransportPivotRows(labels, counts)...))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 3
	})

	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("bounded", "bounded", job.ID, 0, 1))
	observation := readPreviewObservations(t, client, "bounded")["bounded"]

	assertChartBreakTransportPivotSchema(t, observation.schema.GetResultSchemaAvailable().GetSchema(), job)
	preview := observation.preview.GetResultPreview()
	if !preview.GetTruncated() {
		t.Fatalf("bounded pivot preview = %+v, want a truncated prefix", preview)
	}
	assertChartBreakTransportPivotPreview(t, preview, labels[:1], counts[:1])
}

// TestChartBreakTransportWebSocketPivotCountColumnsAreNotEventMetadata pins
// the semantic contract of a runtime-named count column. Every column after
// the first is an unsigned occurrence count whose name happens to be a field
// value, so it must not be advertised to clients as event metadata such as a
// host or a level. The time-series projection already forces METRIC on its
// runtime-named columns for exactly this reason.
func TestChartBreakTransportWebSocketPivotCountColumnsAreNotEventMetadata(t *testing.T) {
	// Ordinary values of a split field that collide with event metadata names.
	names := []string{"host", "level", "source", "message"}
	job := chartBreakTransportPivotJob("search-chart-semantics", names...)
	reader := newPreviewSearchSnapshots(previewSnapshot(job,
		chartBreakTransportPivotRows([]string{"/a"}, [][]uint64{{1, 2, 3, 4}})...))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 1
	})

	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("semantics", "semantics", job.ID, 0, 1))
	observation := readPreviewObservations(t, client, "semantics")["semantics"]

	columns := observation.schema.GetResultSchemaAvailable().GetSchema().GetColumns()
	if len(columns) != len(names)+1 {
		t.Fatalf("pivot schema published %d columns, want %d", len(columns), len(names)+1)
	}
	for index, column := range columns[1:] {
		if column.GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_UINT64 {
			t.Fatalf("count column %q value type = %v", column.GetFieldName(), column.GetValueType())
		}
		semantic := column.GetSemanticType()
		if semantic != opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_UNSPECIFIED &&
			semantic != opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_METRIC {
			t.Fatalf("chart count column %d (%q) is advertised as %v, but it is an unsigned occurrence "+
				"count whose name is a runtime field value, not event metadata",
				index+1, column.GetFieldName(), semantic)
		}
	}
}

func assertChartBreakTransportPivotSchema(t *testing.T, schema *opensplunkv1.ResultSchema, job searchjobs.Job) {
	t.Helper()
	columns := schema.GetColumns()
	if len(columns) != len(job.Schema.Columns) {
		t.Fatalf("schema published %d columns, want %d", len(columns), len(job.Schema.Columns))
	}
	for index, want := range job.Schema.Columns {
		got := columns[index]
		if got.GetFieldName() != want.Name || got.GetDisplayName() != want.Name {
			t.Fatalf("column %d = %q/%q, want %q", index, got.GetFieldName(), got.GetDisplayName(), want.Name)
		}
		wantType := opensplunkv1.ValueType_VALUE_TYPE_UINT64
		if index == 0 {
			wantType = opensplunkv1.ValueType_VALUE_TYPE_STRING
		}
		if got.GetValueType() != wantType || got.GetNullable() || got.GetMultivalue() {
			t.Fatalf("column %d = %+v, want %v and no nullable/multivalue flags", index, got, wantType)
		}
	}
}

func assertChartBreakTransportPivotPreview(
	t *testing.T,
	preview *opensplunkv1.ResultPreview,
	labels []string,
	counts [][]uint64,
) {
	t.Helper()
	if preview.GetUpdateMode() != opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET {
		t.Fatalf("pivot preview update mode = %v, want reset", preview.GetUpdateMode())
	}
	rows := preview.GetRows()
	if len(rows) != len(labels) {
		t.Fatalf("pivot preview rows = %d, want %d", len(rows), len(labels))
	}
	for index, row := range rows {
		if row.GetOrdinal() != uint64(index) {
			t.Fatalf("preview row %d ordinal = %d", index, row.GetOrdinal())
		}
		cells := row.GetCells()
		if len(cells) != len(counts[index])+1 {
			t.Fatalf("preview row %d has %d cells, want %d", index, len(cells), len(counts[index])+1)
		}
		if cells[0].GetStringValue() != labels[index] {
			t.Fatalf("preview row %d label = %q, want %q", index, cells[0].GetStringValue(), labels[index])
		}
		for cell, want := range counts[index] {
			if cells[cell+1].GetUint64Value() != want {
				t.Fatalf("preview row %d cell %d = %d, want %d", index, cell, cells[cell+1].GetUint64Value(), want)
			}
		}
	}
}
