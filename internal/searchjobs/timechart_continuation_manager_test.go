package searchjobs

import (
	"context"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestManagerAuthenticatesContinuationDescriptor(t *testing.T) {
	for _, mode := range []string{"valid", "tampered", "unrelated", "missing", "repeated"} {
		t.Run(mode, func(t *testing.T) {
			manager := newTestManager(t, Config{
				Executor: executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
					final, err := query.ContinueContext(ctx, []clickhouse.RelationColumn{
						{Name: "_time", Type: "DateTime64(9, 'UTC')"},
						{Name: "count", Type: "UInt64"},
					}, nil)
					if err != nil {
						return err
					}
					switch mode {
					case "tampered":
						final.OutputFields = []string{"forged"}
					case "unrelated":
						final = query
					}
					if mode != "missing" {
						// Deliberately swallow callback errors: the manager must
						// remember authority failures and prevent publication.
						_ = sink.(CompiledResultSink).SetCompiledQuery(final)
						if mode == "repeated" {
							_ = sink.(CompiledResultSink).SetCompiledQuery(final)
						}
					}
					_ = sink.SetSchema(Schema{Columns: []Column{{Name: "count", Kind: ValueKindUnsigned}}})
					_ = sink.AddRow([]Value{UnsignedValue(7)})
					return nil
				}),
				CleanupInterval: -1,
				NewID:           sequenceIDs("continuation-" + mode),
			})
			created, err := manager.Create(context.Background(), withSPL(validRequest(),
				"index=main | timechart span=5m count by host | table count"))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "valid" {
				job := waitForState(t, manager, created.ID, StateCompleted)
				if job.RowCount != 1 || job.Schema == nil || len(job.Schema.Columns) != 1 || job.Schema.Columns[0].Name != "count" {
					t.Fatalf("resolved result = %#v", job)
				}
				return
			}
			job := waitForState(t, manager, created.ID, StateFailed)
			if job.Schema != nil || job.RowCount != 0 {
				t.Fatalf("invalid continuation published results: %#v", job)
			}
		})
	}
}
