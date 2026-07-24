package export

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// chartBreakTransportPivotSchema builds the public shape a chart publishes:
// one plan-time row column followed by unsigned count columns whose names came
// from field values. Every name below is legal under the chart label contract
// (non-empty, valid UTF-8, at most 256 bytes, not NULL/OTHER, no leading
// underscore after VALUE normalization, distinct from the row column).
func chartBreakTransportPivotSchema(names ...string) searchjobs.Schema {
	columns := make([]searchjobs.Column, 0, len(names)+1)
	columns = append(columns, searchjobs.Column{Name: "path", Kind: searchjobs.ValueKindString})
	for _, name := range names {
		columns = append(columns, searchjobs.Column{Name: name, Kind: searchjobs.ValueKindUnsigned})
	}
	return searchjobs.Schema{Columns: columns}
}

func chartBreakTransportPivotRow(label string, counts ...uint64) searchjobs.ResultRow {
	values := make([]searchjobs.Value, 0, len(counts)+1)
	values = append(values, searchjobs.StringValue(label))
	for _, count := range counts {
		values = append(values, searchjobs.UnsignedValue(count))
	}
	return searchjobs.ResultRow{Values: values}
}

// chartBreakTransportHostileNames are column names a chart can legitimately
// publish because they are ordinary field values.
var chartBreakTransportHostileNames = []string{
	"a,b",  // the CSV delimiter
	"a\"b", // the CSV quote
	"a\nb", // an embedded record separator
	// A name containing a bare CRLF is deliberately absent: the serializer
	// writes it verbatim, but conforming CSV readers (including encoding/csv)
	// normalize CRLF to LF inside a quoted field, which is a property of the
	// format rather than of this exporter.
	"\ufeffbom",          // a byte-order mark inside the payload
	"VALUE_audit",        // the normalized form of the _audit split value
	"  padded  ",         // leading and trailing spaces
	"na\u00efve\u2028ls", // non-ASCII plus a Unicode line separator
}

// TestChartBreakTransportCSVRoundTripsDataDerivedPivotColumns proves a chart
// export survives a real CSV reader: every runtime column name and every cell
// must come back byte-identical, and the header must stay aligned with the
// cells beneath it.
func TestChartBreakTransportCSVRoundTripsDataDerivedPivotColumns(t *testing.T) {
	t.Parallel()

	schema := chartBreakTransportPivotSchema(chartBreakTransportHostileNames...)
	selection, err := selectColumns(schema, nil)
	if err != nil {
		t.Fatalf("selectColumns: %v", err)
	}
	var output bytes.Buffer
	serializer, err := newCSVSerializer(&output, selection, CSVOptions{})
	if err != nil {
		t.Fatalf("newCSVSerializer: %v", err)
	}
	rows := []searchjobs.ResultRow{
		chartBreakTransportPivotRow("/a", 1, 2, 3, 4, 5, 6, 7),
		chartBreakTransportPivotRow("with,\"quotes\"\nand newline", 0, 0, 0, 0, 0, 0, 8),
	}
	for _, row := range rows {
		if err := serializer.WriteRow(row); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := serializer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader := csv.NewReader(strings.NewReader(output.String()))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("re-read exported CSV: %v", err)
	}
	if len(records) != len(rows)+1 {
		t.Fatalf("exported %d CSV records, want %d", len(records), len(rows)+1)
	}
	header := records[0]
	if len(header) != len(schema.Columns) {
		t.Fatalf("header has %d cells for %d columns: %q", len(header), len(schema.Columns), header)
	}
	seen := make(map[string]int, len(header))
	for index, column := range schema.Columns {
		// encoding/csv normalizes a bare \r\n inside a quoted field to \n, so a
		// name carrying one is compared after the same normalization.
		want := strings.ReplaceAll(column.Name, "\r\n", "\n")
		if header[index] != want {
			t.Fatalf("header cell %d = %q, want %q", index, header[index], want)
		}
		if prior, exists := seen[header[index]]; exists {
			t.Fatalf("header cells %d and %d are both %q: a CSV consumer cannot tell the columns apart",
				prior, index, header[index])
		}
		seen[header[index]] = index
	}
	for rowIndex, record := range records[1:] {
		if len(record) != len(schema.Columns) {
			t.Fatalf("row %d has %d cells for %d columns", rowIndex, len(record), len(schema.Columns))
		}
		wantLabel, _ := rows[rowIndex].Values[0].String()
		wantLabel = strings.ReplaceAll(wantLabel, "\r\n", "\n")
		if record[0] != wantLabel {
			t.Fatalf("row %d label = %q, want %q", rowIndex, record[0], wantLabel)
		}
		for cell := 1; cell < len(record); cell++ {
			want, _ := rows[rowIndex].Values[cell].Unsigned()
			if record[cell] != formatUnsignedForTest(want) {
				t.Fatalf("row %d cell %d = %q, want %d", rowIndex, cell, record[cell], want)
			}
		}
	}
}

// TestChartBreakTransportCSVHeaderCollapsesFormulaProtectedPivotColumns pins
// the one place where two distinct, legal chart column names stop being
// distinguishable after encoding. Spreadsheet-injection protection prefixes an
// apostrophe to a header beginning with = + - @ or whitespace, but leaves a
// header that already begins with an apostrophe alone, so "-a" and "'-a"
// collapse onto the same header cell while their columns keep different data.
func TestChartBreakTransportCSVHeaderCollapsesFormulaProtectedPivotColumns(t *testing.T) {
	t.Parallel()

	// Published in the pivot's own UTF-8 ascending column order: 0x27 < 0x2d.
	schema := chartBreakTransportPivotSchema("'-a", "-a")
	selection, err := selectColumns(schema, nil)
	if err != nil {
		t.Fatalf("selectColumns: %v", err)
	}
	var output bytes.Buffer
	serializer, err := newCSVSerializer(&output, selection, CSVOptions{})
	if err != nil {
		t.Fatalf("newCSVSerializer: %v", err)
	}
	if err := serializer.WriteRow(chartBreakTransportPivotRow("/a", 1, 2)); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := serializer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	records, err := csv.NewReader(strings.NewReader(output.String())).ReadAll()
	if err != nil {
		t.Fatalf("re-read exported CSV: %v", err)
	}
	header := records[0]
	if header[1] == header[2] {
		t.Fatalf("distinct chart columns %q and %q both export as the header cell %q, "+
			"so the exported pivot has two indistinguishable count columns (full header %q)",
			schema.Columns[1].Name, schema.Columns[2].Name, header[1], header)
	}
}

// TestChartBreakTransportJSONLinesKeysSurviveDataDerivedPivotColumns proves the
// JSON Lines export escapes runtime column names into keys a standard JSON
// reader recovers exactly, with one object key per published column.
func TestChartBreakTransportJSONLinesKeysSurviveDataDerivedPivotColumns(t *testing.T) {
	t.Parallel()

	names := append([]string{}, chartBreakTransportHostileNames...)
	names = append(names, "back\\slash", "tab\tseparated", "bell")
	schema := chartBreakTransportPivotSchema(names...)
	selection, err := selectColumns(schema, nil)
	if err != nil {
		t.Fatalf("selectColumns: %v", err)
	}
	var output bytes.Buffer
	serializer, err := newJSONLinesSerializer(&output, selection, JSONLinesOptions{})
	if err != nil {
		t.Fatalf("newJSONLinesSerializer: %v", err)
	}
	counts := make([]uint64, len(names))
	for index := range counts {
		counts[index] = uint64(index + 1)
	}
	if err := serializer.WriteRow(chartBreakTransportPivotRow("/a\"b\n", counts...)); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := serializer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	line := strings.TrimRight(output.String(), "\n")
	if strings.Contains(line, "\n") {
		t.Fatalf("a runtime column name split the JSON Lines record: %q", output.String())
	}
	decoder := json.NewDecoder(strings.NewReader(line))
	decoder.UseNumber()
	var decoded map[string]json.RawMessage
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode exported JSON line: %v (line %q)", err, line)
	}
	if _, err := decoder.Token(); err != io.EOF {
		t.Fatalf("exported JSON line carried trailing content: %v", err)
	}
	if len(decoded) != len(schema.Columns) {
		t.Fatalf("decoded %d keys for %d columns: %v", len(decoded), len(schema.Columns), decoded)
	}
	for index, column := range schema.Columns {
		raw, ok := decoded[column.Name]
		if !ok {
			t.Fatalf("column %q is missing from the exported object: %v", column.Name, decoded)
		}
		if index == 0 {
			if string(raw) != `"/a\"b\n"` {
				t.Fatalf("row label encoded as %s", raw)
			}
			continue
		}
		if string(raw) != formatUnsignedForTest(counts[index-1]) {
			t.Fatalf("column %q encoded as %s, want %d", column.Name, raw, counts[index-1])
		}
	}
}

// TestChartBreakTransportExportsMixedRawPivotRowColumn covers the pivot's one
// nullable, Mixed row column. `chart count OVER _raw BY level` publishes the
// same column `stats count BY _raw` does, so a row label may be non-UTF-8
// bytes; neither serializer may reject or silently corrupt the export.
func TestChartBreakTransportExportsMixedRawPivotRowColumn(t *testing.T) {
	t.Parallel()

	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_raw", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "INFO", Kind: searchjobs.ValueKindUnsigned},
	}}
	selection, err := selectColumns(schema, nil)
	if err != nil {
		t.Fatalf("selectColumns: %v", err)
	}
	rows := []searchjobs.ResultRow{
		{Values: []searchjobs.Value{searchjobs.StringValue("plain"), searchjobs.UnsignedValue(2)}},
		{Values: []searchjobs.Value{searchjobs.BytesValue([]byte{0x61, 0xff, 0xfe, 0x62}), searchjobs.UnsignedValue(3)}},
	}

	var csvOutput bytes.Buffer
	csvSerializer, err := newCSVSerializer(&csvOutput, selection, CSVOptions{})
	if err != nil {
		t.Fatalf("newCSVSerializer: %v", err)
	}
	for _, row := range rows {
		if err := csvSerializer.WriteRow(row); err != nil {
			t.Fatalf("CSV WriteRow: %v", err)
		}
	}
	if err := csvSerializer.Close(); err != nil {
		t.Fatalf("CSV Close: %v", err)
	}
	records, err := csv.NewReader(strings.NewReader(csvOutput.String())).ReadAll()
	if err != nil {
		t.Fatalf("re-read exported CSV: %v", err)
	}
	if len(records) != 3 || records[0][0] != "_raw" {
		t.Fatalf("CSV records = %q", records)
	}
	if records[1][0] != "plain" || records[2][0] != "Yf/+Yg==" {
		t.Fatalf("CSV row labels = %q and %q", records[1][0], records[2][0])
	}

	var jsonOutput bytes.Buffer
	jsonSerializer, err := newJSONLinesSerializer(&jsonOutput, selection, JSONLinesOptions{})
	if err != nil {
		t.Fatalf("newJSONLinesSerializer: %v", err)
	}
	for _, row := range rows {
		if err := jsonSerializer.WriteRow(row); err != nil {
			t.Fatalf("JSON WriteRow: %v", err)
		}
	}
	if err := jsonSerializer.Close(); err != nil {
		t.Fatalf("JSON Close: %v", err)
	}
	lines := strings.Split(strings.TrimRight(jsonOutput.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("JSON Lines export = %q", jsonOutput.String())
	}
	for index, line := range lines {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("decode JSON line %d: %v (%q)", index, err, line)
		}
		if _, ok := decoded["_raw"]; !ok {
			t.Fatalf("JSON line %d lost the row column: %q", index, line)
		}
	}
}

func formatUnsignedForTest(value uint64) string {
	digits := make([]byte, 0, 20)
	if value == 0 {
		return "0"
	}
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}

// TestChartBreakTransportExportRejectsCollidingPivotColumns proves the export
// column resolver refuses a pivot whose row column name is also taken by a
// runtime-named count column. The chart operator rejects such a label before
// publication; this pins the second, independent gate that keeps a positional
// export from silently binding two columns to one name.
func TestChartBreakTransportExportRejectsCollidingPivotColumns(t *testing.T) {
	t.Parallel()

	colliding := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "path", Kind: searchjobs.ValueKindString},
		{Name: "path", Kind: searchjobs.ValueKindUnsigned},
	}}
	if _, err := selectColumns(colliding, nil); err == nil {
		t.Fatal("selectColumns accepted a pivot whose count column took the row column's name")
	}
	if _, err := selectColumns(chartBreakTransportPivotSchema("INFO"), []string{"INFO", "INFO"}); err == nil {
		t.Fatal("selectColumns accepted the same runtime column twice")
	}
	if _, err := selectColumns(chartBreakTransportPivotSchema("INFO"), []string{"ERROR"}); err == nil {
		t.Fatal("selectColumns accepted a column the pivot never published")
	}
}

// TestChartBreakTransportExportSelectsRuntimeColumnsByName proves a caller can
// project a subset of the pivot's data-derived columns, in its own order, and
// that the CSV cells follow the requested order rather than the pivot's.
func TestChartBreakTransportExportSelectsRuntimeColumnsByName(t *testing.T) {
	t.Parallel()

	schema := chartBreakTransportPivotSchema("a,b", "NULLish", "OTHERish")
	selection, err := selectColumns(schema, []string{"OTHERish", "a,b"})
	if err != nil {
		t.Fatalf("selectColumns: %v", err)
	}
	var output bytes.Buffer
	serializer, err := newCSVSerializer(&output, selection, CSVOptions{})
	if err != nil {
		t.Fatalf("newCSVSerializer: %v", err)
	}
	if err := serializer.WriteRow(chartBreakTransportPivotRow("/a", 1, 2, 3)); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := serializer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	records, err := csv.NewReader(strings.NewReader(output.String())).ReadAll()
	if err != nil {
		t.Fatalf("re-read exported CSV: %v", err)
	}
	want := [][]string{{"OTHERish", "a,b"}, {"3", "1"}}
	if len(records) != len(want) {
		t.Fatalf("exported %d records, want %d", len(records), len(want))
	}
	for index, record := range records {
		if len(record) != 2 || record[0] != want[index][0] || record[1] != want[index][1] {
			t.Fatalf("record %d = %q, want %q", index, record, want[index])
		}
	}
}
