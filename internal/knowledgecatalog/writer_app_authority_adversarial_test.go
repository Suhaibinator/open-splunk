package knowledgecatalog

import (
	"errors"
	"testing"
)

func TestWriterDefinitionAppLifecycleWidthIsPreflightedBeforeHydration(t *testing.T) {
	harness := newWriterFaultHarness(t)
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open app-lifecycle corruption connection: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
			t.Errorf("restore app lifecycle checks during cleanup: %v", err)
		}
		if err := connection.Close(); err != nil {
			t.Errorf("close app-lifecycle corruption connection: %v", err)
		}
	}()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable app lifecycle checks: %v", err)
	}
	const hostileStateBytes = 4 << 20
	result, err := connection.ExecContext(t.Context(), `
		UPDATE app_workspaces
		SET state = 'WIDE-' || CAST(zeroblob(?) AS TEXT)
		WHERE tenant_id = ? AND app_id = ?`,
		hostileStateBytes,
		writerFaultTenant,
		writerFaultApp,
	)
	if err != nil {
		t.Fatalf("inject oversized app lifecycle: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("oversized app lifecycle rows = %d, %v; want 1", affected, err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatalf("restore app lifecycle checks: %v", err)
	}

	stable := readWriterFaultSnapshot(t, harness.database)
	response, err := harness.writer.Create(
		harness.actorContext,
		harness.scope,
		writerFaultCreateRequest("oversized-app-state", "oversized-app-state-request-0001"),
	)
	if response != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Create() with oversized authorized app state = (%v, %v), want nil/ErrCorrupt", response, err)
	}
	if harness.idCalls.Load() != 0 || harness.clockCalls.Load() != 0 {
		t.Fatalf("generators called before app-state rejection: IDs=%d clocks=%d", harness.idCalls.Load(), harness.clockCalls.Load())
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), stable)

	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable app checks for restoration: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `
		UPDATE app_workspaces SET state = 'active'
		WHERE tenant_id = ? AND app_id = ?`, writerFaultTenant, writerFaultApp); err != nil {
		t.Fatalf("restore app lifecycle: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatalf("restore app checks after restoration: %v", err)
	}
	assertWriterFaultIntegrity(t, harness.database)
}
