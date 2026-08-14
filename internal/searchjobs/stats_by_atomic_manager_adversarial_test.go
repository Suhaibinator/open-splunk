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

// TestStatsByAtomicManagerHidesStagedPrefixAndClearsItOnFailure proves the
// compiler's stats-BY atomic bit is honored at the public lifecycle boundary.
// A schema and row staged before the runtime guard fails are visible through
// neither preview nor paging, and terminal failure erases the private prefix.
func TestStatsByAtomicManagerHidesStagedPrefixAndClearsItOnFailure(t *testing.T) {
	t.Parallel()

	firstRowStaged := make(chan struct{})
	releaseFailure := make(chan struct{})
	backendSecret := "sdet-secret-storage-detail"
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if !reflect.DeepEqual(query.OutputFields, []string{"grouping", "count"}) {
			return fmt.Errorf("stats BY outputs = %v", query.OutputFields)
		}
		if !query.HasValidExecutionSeal() || !query.RequiresAtomicResult() {
			return fmt.Errorf(
				"stats BY query seal=%t atomic=%t",
				query.HasValidExecutionSeal(),
				query.RequiresAtomicResult(),
			)
		}
		if err := sink.SetSchema(Schema{Columns: []Column{
			{Name: "grouping", Kind: ValueKindString},
			{Name: "count", Kind: ValueKindUnsigned},
		}}); err != nil {
			return err
		}
		if err := sink.AddRow([]Value{StringValue("apparently-valid"), UnsignedValue(1)}); err != nil {
			return err
		}
		close(firstRowStaged)
		select {
		case <-releaseFailure:
		case <-ctx.Done():
			return ctx.Err()
		}
		return fmt.Errorf("%w: %s", ErrUnsupportedValue, backendSecret)
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
		NewID:           sequenceIDs("stats-by-atomic-manager"),
	})
	request := validRequest()
	request.SPL = `index=main | stats count BY grouping | search count>=1`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create stats BY job: %v", err)
	}
	select {
	case <-firstRowStaged:
	case <-time.After(3 * time.Second):
		t.Fatal("stats BY executor did not stage its prefix")
	}

	access := AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}
	if _, previewErr := manager.PreviewFor(access, created.ID, 10); !errors.Is(previewErr, ErrResultsNotReady) {
		t.Fatalf("PreviewFor(staged stats BY prefix) = %v, want ErrResultsNotReady", previewErr)
	}
	if running, getErr := manager.GetFor(access, created.ID); getErr != nil {
		t.Fatalf("GetFor(staged stats BY prefix): %v", getErr)
	} else if running.Schema != nil || running.RowCount != 0 || running.ResultBytes != 0 {
		t.Fatalf("running stats BY prefix became public: %#v", running)
	}

	close(releaseFailure)
	failed := waitForState(t, manager, created.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureUnsupportedSPL ||
		failed.Failure.Message != "search command does not support one or more field values" ||
		failed.Schema != nil || failed.RowCount != 0 || failed.ResultBytes != 0 ||
		failed.ResultsTruncated {
		t.Fatalf("failed stats BY snapshot = %#v", failed)
	}
	if strings.Contains(failed.Failure.Message, backendSecret) {
		t.Fatalf("failed stats BY snapshot leaked backend detail: %#v", failed.Failure)
	}
	if _, previewErr := manager.PreviewFor(access, created.ID, 10); !errors.Is(previewErr, ErrResultsUnavailable) {
		t.Fatalf("PreviewFor(failed stats BY) = %v, want ErrResultsUnavailable", previewErr)
	}
	if _, resultErr := manager.ResultsFor(access, created.ID, PageRequest{Limit: 10}); !errors.Is(resultErr, ErrResultsUnavailable) {
		t.Fatalf("ResultsFor(failed stats BY) = %v, want ErrResultsUnavailable", resultErr)
	}
}

// TestStatsByExpansionLimitManagerHidesStagedPrefixAndClearsItOnFailure proves
// the fixed-Array(String) Cartesian-product guard receives the same public
// transaction boundary as Dynamic stats-BY validation.
func TestStatsByExpansionLimitManagerHidesStagedPrefixAndClearsItOnFailure(t *testing.T) {
	t.Parallel()

	firstRowStaged := make(chan struct{})
	releaseFailure := make(chan struct{})
	backendSecret := "sdet-secret-expansion-detail"
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if !reflect.DeepEqual(query.OutputFields, []string{"tags", "zones", "count"}) {
			return fmt.Errorf("fixed-multivalue stats BY outputs = %v", query.OutputFields)
		}
		if !query.HasValidExecutionSeal() || !query.RequiresAtomicResult() {
			return fmt.Errorf(
				"fixed-multivalue stats BY query seal=%t atomic=%t",
				query.HasValidExecutionSeal(),
				query.RequiresAtomicResult(),
			)
		}
		if err := sink.SetSchema(Schema{Columns: []Column{
			{Name: "tags", Kind: ValueKindMixed},
			{Name: "zones", Kind: ValueKindMixed},
			{Name: "count", Kind: ValueKindUnsigned},
		}}); err != nil {
			return err
		}
		if err := sink.AddRow([]Value{
			StringValue("apparently-valid-tag"),
			StringValue("apparently-valid-zone"),
			UnsignedValue(1),
		}); err != nil {
			return err
		}
		close(firstRowStaged)
		select {
		case <-releaseFailure:
		case <-ctx.Done():
			return ctx.Err()
		}
		return fmt.Errorf("%w: %s", ErrExecutionLimit, backendSecret)
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
		NewID:           sequenceIDs("stats-by-expansion-atomic-manager"),
	})
	request := validRequest()
	request.SPL = `index=main | stats values(tag) AS tags values(zone) AS zones | stats count BY tags zones | head 1`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create fixed-multivalue stats BY job: %v", err)
	}
	select {
	case <-firstRowStaged:
	case <-time.After(3 * time.Second):
		t.Fatal("fixed-multivalue stats BY executor did not stage its prefix")
	}

	access := AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}
	if _, previewErr := manager.PreviewFor(access, created.ID, 10); !errors.Is(previewErr, ErrResultsNotReady) {
		t.Fatalf("PreviewFor(staged expansion prefix) = %v, want ErrResultsNotReady", previewErr)
	}
	if running, getErr := manager.GetFor(access, created.ID); getErr != nil {
		t.Fatalf("GetFor(staged expansion prefix): %v", getErr)
	} else if running.Schema != nil || running.RowCount != 0 || running.ResultBytes != 0 {
		t.Fatalf("running expansion prefix became public: %#v", running)
	}

	close(releaseFailure)
	failed := waitForState(t, manager, created.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureResourceLimit ||
		failed.Failure.Message != "search exceeded a configured execution resource limit" ||
		failed.Schema != nil || failed.RowCount != 0 || failed.ResultBytes != 0 ||
		failed.ResultsTruncated {
		t.Fatalf("failed fixed-multivalue stats BY snapshot = %#v", failed)
	}
	if strings.Contains(failed.Failure.Message, backendSecret) {
		t.Fatalf("failed expansion snapshot leaked backend detail: %#v", failed.Failure)
	}
	if _, previewErr := manager.PreviewFor(access, created.ID, 10); !errors.Is(previewErr, ErrResultsUnavailable) {
		t.Fatalf("PreviewFor(failed expansion) = %v, want ErrResultsUnavailable", previewErr)
	}
	if _, resultErr := manager.ResultsFor(access, created.ID, PageRequest{Limit: 10}); !errors.Is(resultErr, ErrResultsUnavailable) {
		t.Fatalf("ResultsFor(failed expansion) = %v, want ErrResultsUnavailable", resultErr)
	}
}
