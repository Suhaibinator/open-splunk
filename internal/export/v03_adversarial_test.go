package export

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

func TestV03ExpandedPublicRowsExportWithoutContainerOrPrivateArtifacts(t *testing.T) {
	t.Parallel()

	const (
		searchJobID = "v03-export-search"
		source      = `index=main` +
			` | regex message!="reject"` +
			` | sort 0 +event_id` +
			` | accum value AS running` +
			` | strcat host "/" route endpoint` +
			` | addinfo` +
			` | fillnull value="0" optional` +
			` | addtotals fieldname=total value running` +
			` | delta running AS step p=2` +
			` | makemv delim="," allowempty=true tags` +
			` | mvexpand tags limit=2` +
			` | reverse` +
			` | table event_id tags info_sid`
	)
	access := searchjobs.AccessScope{TenantID: "tenant-v03-export", OwnerID: "owner-v03-export"}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "tags", Kind: searchjobs.ValueKindString, Nullable: true},
		{Name: "info_sid", Kind: searchjobs.ValueKindString},
	}}
	rows := [][]searchjobs.Value{
		// An expanded scalar String may legitimately contain JSON-looking
		// punctuation. It must remain a String, not be mistaken for a leaked
		// multivalue container representation.
		{searchjobs.StringValue("event-2"), searchjobs.StringValue("[literal]"), searchjobs.StringValue(searchJobID)},
		{searchjobs.StringValue("event-1"), searchjobs.NullValue(), searchjobs.StringValue(searchJobID)},
		{searchjobs.StringValue("event-1"), searchjobs.StringValue(""), searchjobs.StringValue(searchJobID)},
	}
	searchManager, err := searchjobs.New(searchjobs.Config{
		Executor: integrationSearchExecutor(func(_ context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
			if !slices.Equal(query.OutputFields, []string{"event_id", "tags", "info_sid"}) ||
				!query.HasValidExecutionSeal() || !query.RequiresAtomicResult() {
				return fmt.Errorf("v0.3 export compile = fields %v sealed %t atomic %t", query.OutputFields, query.HasValidExecutionSeal(), query.RequiresAtomicResult())
			}
			for _, field := range query.OutputFields {
				if strings.HasPrefix(strings.ToLower(field), "__os_") {
					return fmt.Errorf("v0.3 export compile leaked private field %q", field)
				}
			}
			if schema.Columns[1].Multivalue || schema.Columns[1].HasFlatMultivalueDelimiter {
				return fmt.Errorf("expanded tag column retained container presentation: %#v", schema.Columns[1])
			}
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			for _, row := range rows {
				if err := sink.AddRow(row); err != nil {
					return err
				}
			}
			return nil
		}),
		Snapshotter:     integrationSnapshotter(func(context.Context) (uint64, error) { return 19, nil }),
		CleanupInterval: -1,
		NewID:           func() string { return searchJobID },
		CursorKey:       []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("searchjobs.New() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := searchManager.Close(); closeErr != nil {
			t.Errorf("close search manager: %v", closeErr)
		}
	})
	resolvedRange, err := searchtime.NewAbsoluteRange(
		time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 12, 2, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	searchJob, err := searchManager.Create(context.Background(), searchjobs.CreateRequest{
		SPL:               source,
		OwnerID:           access.OwnerID,
		TenantID:          access.TenantID,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         resolvedRange,
	})
	if err != nil {
		t.Fatalf("create v0.3 search: %v", err)
	}
	waitForIntegrationSearchState(t, searchManager, access, searchJob.ID, searchjobs.StateCompleted)

	exportManager, err := New(Config{
		Source:          searchManager,
		ArtifactDir:     t.TempDir(),
		MaxWorkers:      1,
		CleanupInterval: -1,
		NewID:           func() string { return "v03-export-artifact" },
	})
	if err != nil {
		t.Fatalf("export.New() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := exportManager.Close(); closeErr != nil {
			t.Errorf("close export manager: %v", closeErr)
		}
	})
	created, err := exportManager.Create(context.Background(), access, CreateRequest{
		SearchJobID: searchJob.ID,
		Format:      FormatJSONLines,
		Columns:     []string{"tags", "event_id", "info_sid"},
	})
	if err != nil {
		t.Fatalf("create v0.3 export: %v", err)
	}
	completed := waitForIntegrationExportState(t, exportManager, access, created.ID, StateCompleted)
	if completed.Artifact == nil || completed.Artifact.RowCount != uint64(len(rows)) ||
		!slices.Equal(completed.Columns, []string{"tags", "event_id", "info_sid"}) {
		t.Fatalf("completed v0.3 export = %#v", completed)
	}
	contents, err := os.ReadFile(filepath.Join(exportManager.artifactDir, completed.Artifact.FileName))
	if err != nil {
		t.Fatalf("read v0.3 artifact: %v", err)
	}
	want := "{\"tags\":\"[literal]\",\"event_id\":\"event-2\",\"info_sid\":\"v03-export-search\"}\n" +
		"{\"tags\":null,\"event_id\":\"event-1\",\"info_sid\":\"v03-export-search\"}\n" +
		"{\"tags\":\"\",\"event_id\":\"event-1\",\"info_sid\":\"v03-export-search\"}\n"
	if string(contents) != want {
		t.Fatalf("v0.3 JSONL = %q, want %q", contents, want)
	}
	for lineIndex, line := range strings.Split(strings.TrimSuffix(string(contents), "\n"), "\n") {
		var object map[string]any
		if err := json.Unmarshal([]byte(line), &object); err != nil {
			t.Fatalf("decode JSONL row %d: %v", lineIndex, err)
		}
		if len(object) != 3 {
			t.Fatalf("JSONL row %d fields = %#v, want exactly requested public fields", lineIndex, object)
		}
		for key := range object {
			if strings.HasPrefix(strings.ToLower(key), "__os_") {
				t.Fatalf("JSONL row %d exposed private key %q", lineIndex, key)
			}
		}
		if tag := object["tags"]; tag != nil {
			if _, ok := tag.(string); !ok {
				t.Fatalf("JSONL row %d expanded tags = %T %#v, want scalar String or null", lineIndex, tag, tag)
			}
		}
	}
}
