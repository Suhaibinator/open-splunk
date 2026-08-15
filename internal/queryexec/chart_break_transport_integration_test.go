package queryexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// TestChartBreakTransportRowCeilingAgainstClickHouse pins the buffered pivot's
// 10,000-row contract against the real backend on both sides of the boundary.
// The ceiling is enforced by a server-side guard inside the compiled SQL, so
// only a real ClickHouse can prove that crossing it fails atomically, that the
// failure reaches a search job as a redacted resource limit, and that landing
// exactly on it still publishes the whole pivot.
//
// It is opt-in because it starts an ephemeral Docker container and may pull the
// pinned ClickHouse image.
func TestChartBreakTransportRowCeilingAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	container := "open-splunk-chart-transport-" + queryIntegrationRandomHex(t, 6)
	password := queryIntegrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = queryExecutorIntegrationImage
	}
	queryIntegrationDocker(t, ctx, nil,
		"run", "--detach", "--rm", "--name", container,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB=open_splunk",
		"--env", "CLICKHOUSE_USER=open_splunk",
		"--env", "CLICKHOUSE_PASSWORD="+password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		image,
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", "--volumes", container).Run()
	})
	queryIntegrationWait(t, ctx, container, password)
	queryIntegrationMigrate(t, ctx, container, password)

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{queryIntegrationNativeAddress(t, ctx, container)},
		Auth: clickhousedriver.Auth{
			Database: "open_splunk",
			Username: "open_splunk",
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	executor, err := New(connection, Config{
		ReadAdmission: indexread.UnfencedAdmission{},
	})
	if err != nil {
		t.Fatal(err)
	}

	const ceiling = 10_000
	base, indexTime := chartBreakTransportInsertWideRowAxis(t, ctx, connection, ceiling+1)
	source := `index=main source="chart-row-ceiling" | chart count OVER path BY level`
	// event i sits at base+i seconds and the scope filter is [earliest, latest),
	// so the exclusive upper bound selects exactly its own offset in rows.
	rangeFor := func(rows int) (time.Time, time.Time) {
		return base, base.Add(time.Duration(rows) * time.Second)
	}

	t.Run("exactly the row ceiling publishes the whole pivot", func(t *testing.T) {
		earliest, latest := rangeFor(ceiling)
		job, page := queryIntegrationRunSearchRange(
			t, ctx, executor, indexTime, "chart-transport-ceiling-fit", source, earliest, latest)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("boundary chart state = %v, failure=%+v", job.State, job.Failure)
		}
		if job.RowCount != ceiling || job.ResultsTruncated {
			t.Fatalf("boundary chart rows = %d truncated=%v, want exactly %d retained rows",
				job.RowCount, job.ResultsTruncated, ceiling)
		}
		queryIntegrationAssertColumns(t, page, []string{"path", "INFO"})
		if page.TotalRows != ceiling || len(page.Rows) != 10 || page.Complete {
			t.Fatalf("boundary chart page = total %d rows %d complete %v",
				page.TotalRows, len(page.Rows), page.Complete)
		}
		// The row axis is the ascending order stats count BY path publishes.
		for index, row := range page.Rows {
			label, ok := row.Values[0].String()
			if !ok || label != chartBreakTransportRowLabel(index) {
				t.Fatalf("boundary chart row %d = %q (%v), want %q",
					index, label, ok, chartBreakTransportRowLabel(index))
			}
			count, ok := row.Values[1].Unsigned()
			if !ok || count != 1 {
				t.Fatalf("boundary chart row %d count = %d (%v), want 1", index, count, ok)
			}
		}
	})

	t.Run("one row past the ceiling fails atomically and redacted", func(t *testing.T) {
		earliest, latest := rangeFor(ceiling + 1)
		compiled := queryIntegrationCompileSearchRange(t, source, indexTime, earliest, latest)
		if compiled.Chart == nil || compiled.Chart.RowLimit != ceiling {
			t.Fatalf("compiled chart contract = %#v", compiled.Chart)
		}
		sink := &fakeSink{}
		err := executor.Execute(ctx, compiled, sink)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) {
			t.Fatalf("over-ceiling chart error = %v, want ErrExecutionLimit", err)
		}
		// The whole pivot is buffered before publication precisely so the
		// server-side guard can suppress every row, not truncate the axis.
		if sink.setCalls != 0 || len(sink.rows) != 0 {
			t.Fatalf("over-ceiling chart published schema=%d rows=%d, want nothing", sink.setCalls, len(sink.rows))
		}
		if strings.Contains(err.Error(), clickhouse.ChartRowLimitMarker) ||
			strings.Contains(err.Error(), "SELECT") || strings.Contains(err.Error(), "path") {
			t.Fatalf("over-ceiling chart error leaked backend detail: %v", err)
		}

		job, _ := queryIntegrationRunSearchRange(
			t, ctx, executor, indexTime, "chart-transport-ceiling-over", source, earliest, latest)
		if job.State != searchjobs.StateFailed || job.Failure == nil ||
			job.Failure.Code != searchjobs.FailureResourceLimit {
			t.Fatalf("over-ceiling chart job = %v failure=%+v", job.State, job.Failure)
		}
		if job.RowCount != 0 || job.Schema != nil || job.ResultsTruncated {
			t.Fatalf("over-ceiling chart retained rows=%d schema=%#v truncated=%v",
				job.RowCount, job.Schema, job.ResultsTruncated)
		}
		if strings.Contains(job.Failure.Message, clickhouse.ChartRowLimitMarker) ||
			strings.Contains(job.Failure.Message, "SELECT") ||
			strings.Contains(job.Failure.Message, "path") ||
			strings.Contains(job.Failure.Message, "chart-row") {
			t.Fatalf("over-ceiling chart failure leaked backend detail: %q", job.Failure.Message)
		}
	})
}

func chartBreakTransportRowLabel(index int) string {
	return fmt.Sprintf("/p%06d", index)
}

// chartBreakTransportInsertWideRowAxis writes one event per distinct row value
// so a scoped search selects an exact number of chart rows: event i carries
// path /pNNNNNN and sits at base+i seconds.
func chartBreakTransportInsertWideRowAxis(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	rows int,
) (time.Time, time.Time) {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Minute)
	indexTime := time.Now().UTC().Truncate(time.Millisecond)
	level := "INFO"
	for index := range rows {
		label := chartBreakTransportRowLabel(index)
		document := clickhousedriver.NewJSON()
		document.SetValueAtPath("path", clickhousedriver.NewDynamic(label))
		message := "chart row ceiling " + label
		if err := batch.Append(
			fmt.Sprintf("chart-row-ceiling-%06d", index), "tenant", "main",
			base.Add(time.Duration(index)*time.Second), indexTime,
			nil, uint8(1), "host", "chart-row-ceiling", "test", nil, uint8(1), &level, &message, []byte(message),
			uint8(1), nil, nil, document, []string{"path"}, "collector", "chart-ceiling-batch", uint64(index+1),
			indexTime.Add(24*time.Hour), uint64(1),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return base, indexTime
}

// TestChartBreakTransportCeilingLabelDomainAgainstClickHouse attacks the
// published column domain at the row ceiling, where the pivot is widest and the
// domain is assembled by a window aggregate that every row reads. The domain is
// deduplicated inside that aggregate's state, so a defect there is invisible at
// small scale and shows up only as a wrong, reordered, or missing column once
// thousands of rows share one domain.
//
// The fixture is deliberately hostile to the assembly rather than to the count:
// twelve ordinary labels exactly fill the series limit, one of them needs
// Splunk's leading-underscore normalization to sort into its published
// position, three further labels must merge into a single OTHER column, and a
// disjoint tail of rows contributes the NULL column. The executor rejects any
// domain that is misordered, duplicated, or convergent after normalization, so
// the exact column list below is a real contract and not a restatement of the
// implementation.
func TestChartBreakTransportCeilingLabelDomainAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	container := "open-splunk-chart-domain-" + queryIntegrationRandomHex(t, 6)
	password := queryIntegrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = queryExecutorIntegrationImage
	}
	queryIntegrationDocker(t, ctx, nil,
		"run", "--detach", "--rm", "--name", container,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB=open_splunk",
		"--env", "CLICKHOUSE_USER=open_splunk",
		"--env", "CLICKHOUSE_PASSWORD="+password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		image,
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", "--volumes", container).Run()
	})
	queryIntegrationWait(t, ctx, container, password)
	queryIntegrationMigrate(t, ctx, container, password)

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{queryIntegrationNativeAddress(t, ctx, container)},
		Auth: clickhousedriver.Auth{
			Database: "open_splunk",
			Username: "open_splunk",
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	executor, err := New(connection, Config{
		ReadAdmission: indexread.UnfencedAdmission{},
	})
	if err != nil {
		t.Fatal(err)
	}

	const (
		ceiling      = 10_000
		ownCellCount = 1
	)
	base, indexTime := chartBreakTransportInsertLabeledRowAxis(t, ctx, connection, ceiling)
	source := `index=main source="chart-ceiling-domain" | chart count OVER path BY level`
	compiled := queryIntegrationCompileSearchRange(
		t, source, indexTime, base, base.Add(time.Duration(ceiling)*time.Second))
	sink := &fakeSink{}
	if err := executor.Execute(ctx, compiled, sink); err != nil {
		t.Fatalf("execute ceiling domain chart: %v", err)
	}

	// VALUE_audit precedes the a-prefixed labels because the published domain is
	// ordered by the normalized name, and the executor independently rejects any
	// other ordering.
	wantColumns := []string{"path", "VALUE_audit"}
	for index := 1; index <= 9; index++ {
		wantColumns = append(wantColumns, fmt.Sprintf("a%02d", index))
	}
	wantColumns = append(wantColumns, "NULL", "OTHER")
	if len(sink.schema.Columns) != len(wantColumns) {
		t.Fatalf("ceiling domain schema = %#v, want %d columns %v",
			sink.schema, len(wantColumns), wantColumns)
	}
	for index, name := range wantColumns {
		if sink.schema.Columns[index].Name != name {
			t.Fatalf("ceiling domain column %d = %q, want %q (schema %#v)",
				index, sink.schema.Columns[index].Name, name, sink.schema)
		}
	}
	if len(sink.rows) != ceiling {
		t.Fatalf("ceiling domain rows = %d, want %d", len(sink.rows), ceiling)
	}

	nullColumn := len(wantColumns) - 2
	otherColumn := len(wantColumns) - 1
	for index, row := range sink.rows {
		if len(row) != len(wantColumns) {
			t.Fatalf("ceiling domain row %d has %d values, want %d", index, len(row), len(wantColumns))
		}
		path, ok := row[0].String()
		if !ok || path != chartBreakTransportDomainRowLabel(index) {
			t.Fatalf("ceiling domain row %d path = %q (%v), want %q",
				index, path, ok, chartBreakTransportDomainRowLabel(index))
		}
		// Column 0 is the row axis, so the owning label's cell sits one past the
		// index of its label in the published series order.
		ownColumn := 1 + chartBreakTransportDomainLabelOrdinal(index)
		wantNull := uint64(0)
		if index >= ceiling-chartBreakTransportNullRows {
			wantNull = chartBreakTransportNullPerRow
		}
		wantOther := uint64(0)
		if index >= ceiling-chartBreakTransportOtherRows {
			wantOther = chartBreakTransportOtherPerRow
		}
		for column := 1; column < len(wantColumns); column++ {
			want := uint64(0)
			switch column {
			case ownColumn:
				want = ownCellCount
			case nullColumn:
				want = wantNull
			case otherColumn:
				want = wantOther
			}
			got, ok := row[column].Unsigned()
			if !ok || got != want {
				t.Fatalf("ceiling domain row %d column %q = %d (%v), want %d",
					index, wantColumns[column], got, ok, want)
			}
		}
	}
}

// The rare-label and absent-label tails are disjoint suffixes of the row axis,
// so a row can own an ordinary label, an OTHER cell, and a NULL cell at once
// without any of them being derivable from the others.
const (
	chartBreakTransportOtherRows   = 100
	chartBreakTransportNullRows    = 50
	chartBreakTransportOtherPerRow = 3
	chartBreakTransportNullPerRow  = 2
)

func chartBreakTransportDomainRowLabel(index int) string {
	return fmt.Sprintf("/q%06d", index)
}

// chartBreakTransportDomainLabels are the ten ordinary labels that exactly fill
// the series limit; NULL and OTHER occupy the two remaining transport columns.
// "_audit" is published as VALUE_audit and therefore sorts ahead of every
// a-prefixed label.
var chartBreakTransportDomainLabels = []string{
	"_audit", "a01", "a02", "a03", "a04",
	"a05", "a06", "a07", "a08", "a09",
}

// chartBreakTransportDomainLabelOrdinal returns the published series position of
// the label owned by a row. "_audit" is assigned to every twelfth row and is
// published first, and the a-prefixed labels follow in their natural order.
func chartBreakTransportDomainLabelOrdinal(index int) int {
	return index % len(chartBreakTransportDomainLabels)
}

// chartBreakTransportInsertLabeledRowAxis writes one primary event per distinct
// row value, round-robin across the twelve ordinary labels, then appends the
// rare labels that must collapse into OTHER and the absent labels that must
// become NULL to disjoint tails of the row axis. Every event a row owns shares
// that row's timestamp, so a scope of exactly `rows` seconds selects the whole
// fixture.
func chartBreakTransportInsertLabeledRowAxis(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	rows int,
) (time.Time, time.Time) {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Minute)
	indexTime := time.Now().UTC().Truncate(time.Millisecond)
	sequence := uint64(0)
	appendEvent := func(index int, level *string) {
		t.Helper()
		sequence++
		path := chartBreakTransportDomainRowLabel(index)
		document := clickhousedriver.NewJSON()
		document.SetValueAtPath("path", clickhousedriver.NewDynamic(path))
		message := "chart ceiling domain " + path
		if err := batch.Append(
			fmt.Sprintf("chart-ceiling-domain-%08d", sequence), "tenant", "main",
			base.Add(time.Duration(index)*time.Second), indexTime,
			nil, uint8(1), "host", "chart-ceiling-domain", "test", nil, uint8(1), level, &message, []byte(message),
			uint8(1), nil, nil, document, []string{"path"}, "collector", "chart-domain-batch", sequence,
			indexTime.Add(24*time.Hour), uint64(1),
		); err != nil {
			t.Fatal(err)
		}
	}
	for index := range rows {
		level := chartBreakTransportDomainLabels[chartBreakTransportDomainLabelOrdinal(index)]
		appendEvent(index, &level)
	}
	// The rare labels stay far below every ordinary label's score, so the series
	// limit must push all three into one OTHER column rather than displacing an
	// ordinary label.
	rareLabels := []string{"z01", "z02", "z03"}
	if len(rareLabels) != chartBreakTransportOtherPerRow {
		t.Fatalf("rare label count = %d, want %d", len(rareLabels), chartBreakTransportOtherPerRow)
	}
	for index := rows - chartBreakTransportOtherRows; index < rows; index++ {
		for _, rare := range rareLabels {
			appendEvent(index, &rare)
		}
	}
	for index := rows - chartBreakTransportNullRows; index < rows; index++ {
		for range chartBreakTransportNullPerRow {
			appendEvent(index, nil)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return base, indexTime
}
