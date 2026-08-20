package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestPipelineCommandsRemainStableAndPrivateFreeAcrossPublicPaging(t *testing.T) {
	t.Parallel()

	const jobID = "pipeline-public-paging-1"
	wantFields := []string{"event_id", "tags", "running", "info_sid"}
	schema := Schema{Columns: []Column{
		{Name: "event_id", Kind: ValueKindString},
		{Name: "tags", Kind: ValueKindString, Nullable: true},
		{Name: "running", Kind: ValueKindDouble, Nullable: true},
		{Name: "info_sid", Kind: ValueKindString},
	}}
	wantRows := [][]Value{
		{StringValue("event-3"), StringValue("界"), DoubleValue(6), StringValue(jobID)},
		{StringValue("event-2"), StringValue(""), DoubleValue(5), StringValue(jobID)},
		{StringValue("event-2"), StringValue("b"), DoubleValue(5), StringValue(jobID)},
		{StringValue("event-1"), NullValue(), DoubleValue(2), StringValue(jobID)},
		{StringValue("event-1"), StringValue("a"), DoubleValue(2), StringValue(jobID)},
	}

	executor := executorFunc(func(_ context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if !reflect.DeepEqual(query.OutputFields, wantFields) {
			return fmt.Errorf("pipeline public output fields = %v, want %v", query.OutputFields, wantFields)
		}
		for _, field := range query.OutputFields {
			if strings.HasPrefix(strings.ToLower(field), "__os_") {
				return fmt.Errorf("compiler-private ordering field leaked into public output: %q", field)
			}
		}
		if !query.RequiresAtomicResult() {
			return errors.New("mvexpand query lacks atomic-result evidence")
		}
		jobIDArguments := 0
		for _, argument := range query.Args {
			if value, ok := argument.(string); ok && value == jobID {
				jobIDArguments++
			}
		}
		if jobIDArguments != 1 {
			return fmt.Errorf("immutable addinfo SID occurs %d times in bound arguments, want once", jobIDArguments)
		}
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		for _, row := range wantRows {
			if err := sink.AddRow(row); err != nil {
				return err
			}
		}
		return nil
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		DefaultPageSize: 2,
		MaxPageSize:     2,
		CleanupInterval: -1,
		NewID:           sequenceIDs("pipeline-public-paging"),
	})
	request := validRequest()
	request.SPL = `index=main` +
		` | regex message!="reject"` +
		` | sort 0 +event_id` +
		` | accum value AS running` +
		` | strcat host "/" route endpoint` +
		` | addinfo` +
		` | fillnull value="0" optional` +
		` | addtotals fieldname=total value running` +
		` | delta running AS step p=2` +
		` | makemv delim="," allowempty=true tags` +
		` | mvexpand tags` +
		` | reverse` +
		` | table event_id tags running info_sid`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create pipeline paging job: %v", err)
	}
	if created.ID != jobID {
		t.Fatalf("job id = %q, want %q", created.ID, jobID)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.ResultsTruncated || completed.RowCount != uint64(len(wantRows)) {
		t.Fatalf("completed result metadata = rows %d truncated %v", completed.RowCount, completed.ResultsTruncated)
	}

	access := AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}
	var (
		cursor   string
		gotRows  []ResultRow
		pageSeen int
	)
	for {
		page, pageErr := manager.ResultsFor(access, created.ID, PageRequest{Limit: 2, Cursor: cursor})
		if pageErr != nil {
			t.Fatalf("ResultsFor page %d: %v", pageSeen, pageErr)
		}
		if !reflect.DeepEqual(page.Schema, schema) {
			t.Fatalf("page %d schema = %#v, want %#v", pageSeen, page.Schema, schema)
		}
		if page.TotalRows != uint64(len(wantRows)) {
			t.Fatalf("page %d total rows = %d, want %d", pageSeen, page.TotalRows, len(wantRows))
		}
		gotRows = append(gotRows, page.Rows...)
		pageSeen++
		if page.Complete {
			if page.NextCursor != "" {
				t.Fatalf("complete page retained cursor %q", page.NextCursor)
			}
			break
		}
		if page.NextCursor == "" || page.NextCursor == cursor {
			t.Fatalf("incomplete page %d has invalid next cursor %q", pageSeen-1, page.NextCursor)
		}
		cursor = page.NextCursor
	}
	if pageSeen != 3 || len(gotRows) != len(wantRows) {
		t.Fatalf("paging returned %d pages and %d rows, want 3/%d", pageSeen, len(gotRows), len(wantRows))
	}
	for index, row := range gotRows {
		if row.Ordinal != uint64(index) {
			t.Fatalf("paged row %d ordinal = %d", index, row.Ordinal)
		}
		if !pipelinePublicValuesEqual(row.Values, wantRows[index]) {
			t.Fatalf("paged row %d = %#v, want %#v", index, row.Values, wantRows[index])
		}
	}
	if schema.Columns[1].Multivalue || schema.Columns[1].HasFlatMultivalueDelimiter {
		t.Fatalf("expanded field retained multivalue presentation metadata: %#v", schema.Columns[1])
	}
}

func TestPipelineAtomicExpansionFailurePublishesNoPublicPrefix(t *testing.T) {
	t.Parallel()

	executor := executorFunc(func(_ context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if !query.RequiresAtomicResult() {
			return errors.New("mvexpand query lacks atomic-result evidence")
		}
		if err := sink.SetSchema(Schema{Columns: []Column{
			{Name: "event_id", Kind: ValueKindString},
			{Name: "tags", Kind: ValueKindString, Nullable: true},
		}}); err != nil {
			return err
		}
		// Simulate a backend that discovers the complete-stage expansion limit
		// only after handing the sink one candidate row. Terminal failure must
		// discard that retained prefix instead of turning it into truncation.
		if err := sink.AddRow([]Value{StringValue("candidate"), StringValue("first")}); err != nil {
			return err
		}
		return ErrExecutionLimit
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
		NewID:           sequenceIDs("pipeline-atomic-expansion"),
	})
	request := validRequest()
	request.SPL = `index=main | makemv delim="," tags | mvexpand tags | head 1 | table event_id tags`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	failed := waitForState(t, manager, created.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureResourceLimit ||
		failed.RowCount != 0 || failed.ResultBytes != 0 || failed.ResultsTruncated {
		t.Fatalf("atomic expansion failure = %#v", failed)
	}
	if _, resultErr := manager.ResultsFor(
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		created.ID,
		PageRequest{Limit: 1},
	); !errors.Is(resultErr, ErrResultsUnavailable) {
		t.Fatalf("ResultsFor failed expansion = %v, want ErrResultsUnavailable", resultErr)
	}
}

func TestPipelineAtomicResultRowLimitStagesNoPreviewAndPublishesNothing(t *testing.T) {
	t.Parallel()

	firstRowStaged := make(chan struct{})
	releaseOverflow := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if !query.RequiresAtomicResult() {
			return errors.New("mvexpand query lacks atomic-result evidence")
		}
		if err := sink.SetSchema(Schema{Columns: []Column{
			{Name: "event_id", Kind: ValueKindString},
			{Name: "tags", Kind: ValueKindString, Nullable: true},
		}}); err != nil {
			return err
		}
		if err := sink.AddRow([]Value{StringValue("first"), StringValue("a")}); err != nil {
			return err
		}
		close(firstRowStaged)
		select {
		case <-releaseOverflow:
		case <-ctx.Done():
			return ctx.Err()
		}
		return sink.AddRow([]Value{StringValue("second"), StringValue("b")})
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		MaxRows:         1,
		MaxBytes:        1 << 20,
		CleanupInterval: -1,
		NewID:           sequenceIDs("pipeline-atomic-row-limit"),
	})
	request := validRequest()
	request.SPL = `index=main | makemv delim="," tags | mvexpand tags | table event_id tags`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstRowStaged:
	case <-time.After(3 * time.Second):
		t.Fatal("atomic executor did not stage its first row")
	}
	access := AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}
	if _, previewErr := manager.PreviewFor(access, created.ID, 1); !errors.Is(previewErr, ErrResultsNotReady) {
		t.Fatalf("PreviewFor(staged atomic prefix) = %v, want ErrResultsNotReady", previewErr)
	}
	close(releaseOverflow)
	failed := waitForState(t, manager, created.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureResourceLimit ||
		failed.Schema != nil || failed.RowCount != 0 || failed.ResultBytes != 0 ||
		failed.ResultsTruncated {
		t.Fatalf("atomic row-limit failure = %#v", failed)
	}
	if _, resultErr := manager.ResultsFor(access, created.ID, PageRequest{Limit: 1}); !errors.Is(resultErr, ErrResultsUnavailable) {
		t.Fatalf("ResultsFor(atomic row-limit failure) = %v", resultErr)
	}
	if _, previewErr := manager.PreviewFor(access, created.ID, 1); !errors.Is(previewErr, ErrResultsUnavailable) {
		t.Fatalf("PreviewFor(atomic row-limit failure) = %v", previewErr)
	}
}

func pipelinePublicValuesEqual(got, want []Value) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range want {
		if got[index].Kind() != want[index].Kind() {
			return false
		}
		switch want[index].Kind() {
		case ValueKindNull:
			continue
		case ValueKindString:
			gotText, gotOK := got[index].String()
			wantText, wantOK := want[index].String()
			if !gotOK || !wantOK || gotText != wantText {
				return false
			}
		case ValueKindDouble:
			gotNumber, gotOK := got[index].Double()
			wantNumber, wantOK := want[index].Double()
			if !gotOK || !wantOK || gotNumber != wantNumber {
				return false
			}
		default:
			if !reflect.DeepEqual(got[index], want[index]) {
				return false
			}
		}
	}
	return true
}

// This test pins Manager behavior after a caller has already reconstructed the
// SPL and attached provenance. Persistence-layer saved-search loading and
// history reconstruction require separate product-surface tests.
func TestPipelineManagerExecutesCommandsWithSavedAndHistoryOriginMetadata(t *testing.T) {
	t.Parallel()

	const source = `index=main` +
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

	for _, test := range []struct {
		name   string
		origin JobOrigin
		object string
		jobID  string
	}{
		{name: "saved search", origin: JobOriginSavedSearch, object: "saved-pipeline", jobID: "pipeline-saved-origin-1"},
		{name: "history rerun", origin: JobOriginHistoryRerun, object: "history-pipeline", jobID: "pipeline-history-origin-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			executor := executorFunc(func(_ context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
				wantFields := []string{"event_id", "tags", "info_sid"}
				if !reflect.DeepEqual(query.OutputFields, wantFields) || !query.HasValidExecutionSeal() || !query.RequiresAtomicResult() {
					return fmt.Errorf("origin compile = fields %v sealed %t atomic %t", query.OutputFields, query.HasValidExecutionSeal(), query.RequiresAtomicResult())
				}
				for _, field := range query.OutputFields {
					if strings.HasPrefix(strings.ToLower(field), "__os_") {
						return fmt.Errorf("origin compile leaked private field %q", field)
					}
				}
				boundSID := 0
				for _, argument := range query.Args {
					if argument == test.jobID {
						boundSID++
					}
				}
				if boundSID != 1 {
					return fmt.Errorf("origin compile bound job ID %d times", boundSID)
				}
				if err := sink.SetSchema(Schema{Columns: []Column{
					{Name: "event_id", Kind: ValueKindString},
					{Name: "tags", Kind: ValueKindString, Nullable: true},
					{Name: "info_sid", Kind: ValueKindString},
				}}); err != nil {
					return err
				}
				return sink.AddRow([]Value{StringValue(test.object), StringValue("界"), StringValue(test.jobID)})
			})
			manager := newTestManager(t, Config{
				Executor:        executor,
				CleanupInterval: -1,
				NewID:           sequenceIDs(strings.TrimSuffix(test.jobID, "-1")),
			})
			request := validRequest()
			request.SPL = source
			request.Source = JobSource{Origin: test.origin, ObjectID: test.object}
			created, err := manager.Create(context.Background(), request)
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if created.ID != test.jobID || created.SPL != source || created.Source != request.Source {
				t.Fatalf("created origin snapshot = %#v", created)
			}
			completed := waitForState(t, manager, created.ID, StateCompleted)
			if completed.Source != request.Source || completed.SPL != source || completed.RowCount != 1 {
				t.Fatalf("completed origin snapshot = %#v", completed)
			}
			page, err := manager.ResultsFor(
				AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
				created.ID,
				PageRequest{Limit: 1},
			)
			gotObject := ""
			if err == nil && len(page.Rows) == 1 {
				gotObject, _ = page.Rows[0].Values[0].String()
			}
			if err != nil || len(page.Rows) != 1 || gotObject != test.object {
				t.Fatalf("origin results = %#v, error = %v", page, err)
			}
		})
	}
}
