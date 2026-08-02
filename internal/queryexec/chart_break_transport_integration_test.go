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
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", container).Run()
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
