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

func TestLookupResolutionValidationReusesImmutableBackingMetrics(t *testing.T) {
	rows := make([][]string, 4*lookupContextCheckRows)
	for index := range rows {
		rows[index] = []string{strconv.Itoa(index), "owner"}
	}
	resolution := testLookupResolution(t, "tenant-1", rows)

	cached := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: 100}
	if err := validateLookupResolutionContext(cached, resolution); err != nil {
		t.Fatalf("validate cached resolution: %v", err)
	}
	uncachedResolution := resolution
	uncachedResolution.backing = nil
	uncached := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: 100}
	if err := validateLookupResolutionContext(uncached, uncachedResolution); err != nil {
		t.Fatalf("validate uncached resolution: %v", err)
	}
	if cached.calls >= uncached.calls || cached.calls > 2 {
		t.Fatalf(
			"resolution validation checks cached=%d uncached=%d, want scalar cached validation",
			cached.calls,
			uncached.calls,
		)
	}
}

func TestLookupExternalBackingAuthenticationAndMaterializationAreCancellable(t *testing.T) {
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

	// Authentication is the one full selected-cell validation and commitment
	// scan. Cancel during the second column to prove the immutable cache is not
	// established from a partial payload.
	midAuthentication := &cancelAfterLookupChecks{
		Context:  context.Background(),
		cancelAt: 6,
	}
	backing, authenticationErr := authenticateCompiledLookupExternalBackingContext(
		midAuthentication,
		compiled.lookupTables[0].backing.values,
	)
	if backing != nil || !errors.Is(authenticationErr, context.Canceled) {
		t.Fatalf("authenticate lookup backing = (%v, %v), want canceled", backing, authenticationErr)
	}
	if midAuthentication.calls != midAuthentication.cancelAt {
		t.Fatalf(
			"mid-authentication context checks = %d, want %d",
			midAuthentication.calls,
			midAuthentication.cancelAt,
		)
	}

	// Seal hashing consumes the authenticated commitment rather than scanning
	// cells again, but fresh driver blocks still walk every row and retain
	// bounded cancellation checkpoints.
	midMaterialization := &cancelAfterLookupChecks{
		Context:  context.Background(),
		cancelAt: 5,
	}
	if tables, materializationErr := materializeCompiledLookupExternalTables(
		midMaterialization,
		compiled.lookupTables,
	); tables != nil || !errors.Is(materializationErr, context.Canceled) {
		t.Fatalf(
			"materialize lookup backing = (%v, %v), want canceled",
			tables,
			materializationErr,
		)
	}
	if midMaterialization.calls != midMaterialization.cancelAt {
		t.Fatalf(
			"mid-materialization context checks = %d, want %d",
			midMaterialization.calls,
			midMaterialization.cancelAt,
		)
	}
}

func TestLookupExternalSealHashUsesAuthenticatedBackingWithoutCellRescan(t *testing.T) {
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

	checks := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: 100}
	written, err := writeCompiledLookupExternalTablesContext(
		checks,
		sha256.New(),
		compiled.lookupTables,
	)
	if err != nil || !written {
		t.Fatalf("write authenticated lookup backing = (%v, %v)", written, err)
	}
	if checks.calls > 8 {
		t.Fatalf(
			"seal hashing performed row-proportional checks: %d checks for %d rows",
			checks.calls,
			len(rows),
		)
	}
}
