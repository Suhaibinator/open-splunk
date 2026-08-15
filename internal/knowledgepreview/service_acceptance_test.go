package knowledgepreview

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// This package-level test uses the production Preview adapter and a
// Manager-minted execution snapshot.
func TestProductionPreviewAdapterUsesRetainedScopeAndBoundedPairedResults(t *testing.T) {
	for _, test := range []struct {
		name        string
		maximumRows *uint32
		wantRows    int
	}{
		{name: "absent defaults to one hundred", wantRows: int(DefaultMaximumRows)},
		{name: "one", maximumRows: new(uint32(1)), wantRows: 1},
		{name: "exact maximum", maximumRows: new(MaximumRows), wantRows: int(MaximumRows)},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPreviewFixture(t)
			var executions, attemptedRows atomic.Int64
			executor := executorFunc(func(
				ctx context.Context,
				_ clickhouse.CompiledQuery,
				sink searchjobs.ResultSink,
			) error {
				executions.Add(1)
				if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
					Name: "preview_value", Kind: searchjobs.ValueKindString,
				}}}); err != nil {
					return err
				}
				for index := uint32(0); index <= MaximumRows; index++ {
					if err := ctx.Err(); err != nil {
						return err
					}
					attemptedRows.Add(1)
					if err := sink.AddRow([]searchjobs.Value{
						searchjobs.StringValue("retained-event"),
					}); err != nil {
						return err
					}
				}
				return nil
			})
			service, err := NewService(Config{
				Searches: fixture.manager,
				Writer:   fixture.writer,
				Compiler: ProductionCompilerAdapter{Compiler: clickhouse.Compiler{
					Database: "open_splunk", Table: "events",
				}},
				Executor: executor,
			})
			if err != nil {
				t.Fatal(err)
			}
			request := validAliasPreviewRequest()
			request.MaximumRows = test.maximumRows
			sealed, err := service.Preview(
				context.Background(), fixture.access, fixture.scope, request,
			)
			if err != nil {
				t.Fatal(err)
			}
			response, err := sealed.Proto(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !response.GetValidation().GetValid() || !response.GetTruncated() ||
				len(response.GetBeforeRows()) != test.wantRows ||
				len(response.GetAfterRows()) != test.wantRows {
				t.Fatalf("Preview() bounded response = valid %v truncated %v before %d after %d",
					response.GetValidation().GetValid(), response.GetTruncated(),
					len(response.GetBeforeRows()), len(response.GetAfterRows()))
			}
			if response.GetBeforeSchema().GetSchemaId() != previewTestJob ||
				response.GetAfterSchema().GetSchemaId() != previewTestJob ||
				response.GetBeforeSchema().GetRevision() != 1 ||
				response.GetAfterSchema().GetRevision() != 1 {
				t.Fatalf("Preview() did not preserve retained result identity: %#v %#v",
					response.GetBeforeSchema(), response.GetAfterSchema())
			}
			if executions.Load() != 2 {
				t.Fatalf("executor calls = %d, want paired before/after", executions.Load())
			}
			wantAttempts := int64(2 * (test.wantRows + 1))
			if attemptedRows.Load() != wantAttempts {
				t.Fatalf("row attempts = %d, want exact N+1 %d", attemptedRows.Load(), wantAttempts)
			}
		})
	}
}

func TestProductionPreviewCancellationDuringAfterProjectionIsAtomic(t *testing.T) {
	fixture := newPreviewFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var executions atomic.Int64
	executor := executorFunc(func(
		_ context.Context,
		_ clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		call := executions.Add(1)
		if call == 2 {
			cancel()
		}
		if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
			Name: "preview_value", Kind: searchjobs.ValueKindString,
		}}}); err != nil {
			return err
		}
		return sink.AddRow([]searchjobs.Value{searchjobs.StringValue("retained-event")})
	})
	service, err := NewService(Config{
		Searches: fixture.manager,
		Writer:   fixture.writer,
		Compiler: ProductionCompilerAdapter{Compiler: clickhouse.Compiler{
			Database: "open_splunk", Table: "events",
		}},
		Executor: executor,
	})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := service.Preview(ctx, fixture.access, fixture.scope, validAliasPreviewRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Preview(canceled after projection) error = %v, want context.Canceled", err)
	}
	if len(sealed.DeterministicBytes()) != 0 || executions.Load() != 2 {
		t.Fatalf("canceled Preview leaked response or skipped pair: bytes=%d executions=%d",
			len(sealed.DeterministicBytes()), executions.Load())
	}
}
