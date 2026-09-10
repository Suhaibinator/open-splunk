package queryexec

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func TestStagedTimechartUsesExactAdmittedRows(t *testing.T) {
	first := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	for _, admitted := range []bool{false, true} {
		for _, liveLimit := range []uint64{1, 3, 100} {
			if !admitted && liveLimit != 3 {
				continue
			}
			for _, count := range []int{3, 4} {
				t.Run(fmt.Sprintf("admitted_%t_live_%d_rows_%d", admitted, liveLimit, count), func(t *testing.T) {
					compiled := compileTerminalTimechartFixture(t, first, first.Add(time.Duration(count)*time.Second))
					counts := make([]uint64, count)
					counts[0] = 1
					connection := &terminalTimechartQueueConnection{rows: []driver.Rows{
						timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}}), fixedTimechartOrdinalRows(counts),
					}}
					executor := mustExecutor(t, connection)
					if err := executor.Reconfigure(Config{MaxResultRows: liveLimit}); err != nil {
						t.Fatal(err)
					}
					ctx := context.Background()
					if admitted {
						policy := searchlimits.Default()
						policy.MaxResultRows = 3
						ctx = searchlimits.WithPolicy(ctx, policy)
					}
					settings, err := executor.settingsForContext(ctx, compiled)
					if err != nil {
						t.Fatal(err)
					}
					wantNative := uint64(3)
					if admitted {
						wantNative = 4
					}
					if settings["max_result_rows"] != wantNative {
						t.Fatalf("native rows=%v, want%d", settings["max_result_rows"], wantNative)
					}
					sink := &terminalTimechartSink{}
					err = executor.Execute(ctx, compiled, sink)
					if count == 4 {
						if !errors.Is(err, searchjobs.ErrExecutionLimit) {
							t.Fatalf("overflow error=%v", err)
						}
						if sink.compiledCalls != 0 || sink.setCalls != 0 || len(sink.rows) != 0 || len(sink.bounds) != 0 {
							t.Fatalf("overflow published descriptor=%d schema=%d rows=%d bounds=%d", sink.compiledCalls, sink.setCalls, len(sink.rows), len(sink.bounds))
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						if sink.compiledCalls != 1 || sink.setCalls != 1 || len(sink.rows) != count || len(sink.bounds) != count {
							t.Fatalf("complete result descriptor=%d schema=%d rows=%d bounds=%d", sink.compiledCalls, sink.setCalls, len(sink.rows), len(sink.bounds))
						}
					}
					if connection.calls != 2 {
						t.Fatalf("queries=%d,want2", connection.calls)
					}
				})
			}
		}
	}
}
