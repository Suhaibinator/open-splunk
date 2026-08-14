package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"testing"
)

type cancelAfterLookupChecks struct {
	context.Context
	cancelAt int
	calls    int
}

func (ctx *cancelAfterLookupChecks) Err() error {
	ctx.calls++
	if ctx.calls >= ctx.cancelAt {
		return context.Canceled
	}
	return nil
}

func TestLookupCompilationContextCancelsBeforeAndDuringAssetValidation(t *testing.T) {
	logical := buildPlan(
		t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner`,
	)
	rows := make([][]string, 4*lookupContextCheckRows)
	for index := range rows {
		rows[index] = []string{strconv.Itoa(index), "owner"}
	}
	resolution := bindTestLookupResolution(
		t,
		testLookupResolution(t, "tenant-1", rows),
		logical,
	)
	configured := Compiler{lookupResolutions: []LookupResolution{resolution}}

	preCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := configured.CompileContext(preCanceled, logical); !errors.Is(err, context.Canceled) {
		t.Fatalf("CompileContext(pre-canceled) error = %v, want context.Canceled", err)
	}

	// CompileContext performs five bounded context checks before reaching the
	// second 1,024-row resolution checkpoint. Cancel there so the regression is
	// deterministic and proves the scan does not run to the asset boundary.
	midCanceled := &cancelAfterLookupChecks{
		Context:  context.Background(),
		cancelAt: 6,
	}
	if _, err := configured.CompileContext(midCanceled, logical); !errors.Is(err, context.Canceled) {
		t.Fatalf("CompileContext(mid-validation) error = %v, want context.Canceled", err)
	}
	if midCanceled.calls != midCanceled.cancelAt {
		t.Fatalf("mid-validation context checks = %d, want %d", midCanceled.calls, midCanceled.cancelAt)
	}
}

func TestLookupExternalTableHashCancelsMidColumn(t *testing.T) {
	logical := buildPlan(
		t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner`,
	)
	rows := make([][]string, 4*lookupContextCheckRows)
	for index := range rows {
		rows[index] = []string{strconv.Itoa(index), "owner"}
	}
	resolution := bindTestLookupResolution(
		t,
		testLookupResolution(t, "tenant-1", rows),
		logical,
	)
	compiled, err := (Compiler{
		lookupResolutions: []LookupResolution{resolution},
	}).CompileContext(context.Background(), logical)
	if err != nil {
		t.Fatalf("CompileContext(): %v", err)
	}
	preCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	if cloned, ok, cloneErr := compiled.CloneForExecutionContext(preCanceled); ok ||
		!errors.Is(cloneErr, context.Canceled) || cloned.SQL != "" {
		t.Fatalf("CloneForExecutionContext(pre-canceled) = (%#v, %v, %v)", cloned, ok, cloneErr)
	}

	// Two 4,096-row columns consume ten validation checks (start, table, and
	// four row checkpoints per column). The writer then checks the table and
	// rows; cancel on its second row checkpoint, after hashing 1,024 cells.
	midHash := &cancelAfterLookupChecks{
		Context:  context.Background(),
		cancelAt: 13,
	}
	written, hashErr := writeCompiledLookupExternalTablesContext(
		midHash,
		sha256.New(),
		compiled.lookupTables,
	)
	if written || !errors.Is(hashErr, context.Canceled) {
		t.Fatalf("write lookup tables = (%v, %v), want canceled", written, hashErr)
	}
	if midHash.calls != midHash.cancelAt {
		t.Fatalf("mid-hash context checks = %d, want %d", midHash.calls, midHash.cancelAt)
	}
}
