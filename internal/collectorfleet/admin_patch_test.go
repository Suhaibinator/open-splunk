package collectorfleet

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestAdministrativePatchAPIsPreserveIndependentFieldsAndClearDisplay(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 29, 6, 0, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}

	initial, err := store.GetAdministration(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatalf("GetAdministration(initial): %v", err)
	}
	if initial.Version != collector.Version ||
		initial.DisplayName != nil ||
		initial.AdministrativeState != AdministrativeStateEnabled {
		t.Fatalf("initial administration = %#v", initial)
	}

	requestedDisplayName := "  Production collector  "
	displayUpdated, err := store.UpdateDisplayName(
		ctx,
		lease.Scope,
		lease.CollectorID,
		initial.Version,
		&requestedDisplayName,
		connectedAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("UpdateDisplayName(set): %v", err)
	}
	if displayUpdated.Version != initial.Version+1 ||
		displayUpdated.DisplayName == nil ||
		*displayUpdated.DisplayName != "Production collector" ||
		displayUpdated.AdministrativeState != AdministrativeStateEnabled {
		t.Fatalf("display update = %#v", displayUpdated)
	}

	// Neither caller-owned input nor a returned snapshot may alias persisted state.
	requestedDisplayName = "mutated input"
	*displayUpdated.DisplayName = "mutated result"
	afterDisplay, err := store.GetAdministration(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if afterDisplay.DisplayName == nil || *afterDisplay.DisplayName != "Production collector" {
		t.Fatalf("persisted display aliased caller memory: %#v", afterDisplay)
	}
	var displayUpdatedAt int64
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT updated_at_unix_micro
		FROM collector_fleet
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	).Scan(&displayUpdatedAt); err != nil {
		t.Fatalf("read display update time: %v", err)
	}

	disabled, err := store.SetAdministrativeState(
		ctx,
		lease.Scope,
		lease.CollectorID,
		afterDisplay.Version,
		AdministrativeStateDisabled,
		connectedAt.Add(30*time.Second),
	)
	if err != nil {
		t.Fatalf("SetAdministrativeState(disable): %v", err)
	}
	if disabled.Version != afterDisplay.Version+1 ||
		disabled.DisplayName == nil ||
		*disabled.DisplayName != "Production collector" ||
		disabled.AdministrativeState != AdministrativeStateDisabled {
		t.Fatalf("state update replaced display metadata: %#v", disabled)
	}
	var stateUpdatedAt int64
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT updated_at_unix_micro
		FROM collector_fleet
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	).Scan(&stateUpdatedAt); err != nil {
		t.Fatalf("read state update time: %v", err)
	}
	if stateUpdatedAt != displayUpdatedAt {
		t.Fatalf(
			"backwards receive time moved updated_at from %d to %d",
			displayUpdatedAt,
			stateUpdatedAt,
		)
	}

	cleared, err := store.UpdateDisplayName(
		ctx,
		lease.Scope,
		lease.CollectorID,
		disabled.Version,
		nil,
		connectedAt.Add(3*time.Minute),
	)
	if err != nil {
		t.Fatalf("UpdateDisplayName(clear): %v", err)
	}
	if cleared.Version != disabled.Version+1 ||
		cleared.DisplayName != nil ||
		cleared.AdministrativeState != AdministrativeStateDisabled {
		t.Fatalf("display clear replaced administrative state: %#v", cleared)
	}

	persisted, err := store.GetAdministration(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted != cleared {
		t.Fatalf("persisted administration = %#v, want %#v", persisted, cleared)
	}
}

func TestAdministrativeEnabledStateCASPreservesActiveRuntime(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 29, 6, 10, 0, 0, time.UTC)
	before, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}

	updated, err := store.SetAdministrativeState(
		ctx,
		lease.Scope,
		lease.CollectorID,
		before.Version,
		AdministrativeStateEnabled,
		connectedAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("SetAdministrativeState(enabled active collector): %v", err)
	}
	if updated.Version != before.Version+1 ||
		updated.AdministrativeState != AdministrativeStateEnabled {
		t.Fatalf("enabled-to-enabled update = %#v", updated)
	}

	after, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Version != updated.Version ||
		after.TelemetryRevision != before.TelemetryRevision ||
		after.LeaseGeneration != before.LeaseGeneration ||
		after.ActiveLease == nil ||
		after.ActiveLease.BootEpoch != lease.BootEpoch ||
		after.ActiveLease.StreamID != lease.StreamID ||
		after.ActiveLease.Generation != lease.Generation ||
		after.DisconnectedAt != nil {
		t.Fatalf("same-state enable changed or rejected active runtime: before=%#v after=%#v", before, after)
	}
}

func TestAdministrativePatchAPIsValidateInputsAndHideTenantExistence(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 29, 6, 15, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	validDisplayName := "valid"

	assertInvalid := func(operation string, err error) {
		t.Helper()
		if !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("%s error = %v, want ErrInvalidArgument", operation, err)
		}
	}

	//nolint:staticcheck // This public boundary must reject a nil context without panicking.
	_, err = store.GetAdministration(nil, lease.Scope, lease.CollectorID)
	assertInvalid("GetAdministration(nil context)", err)
	_, err = store.GetAdministration(ctx, Scope{TenantID: " tenant-a"}, lease.CollectorID)
	assertInvalid("GetAdministration(padded tenant)", err)
	_, err = store.GetAdministration(ctx, lease.Scope, "-invalid")
	assertInvalid("GetAdministration(invalid collector ID)", err)

	//nolint:staticcheck // This public boundary must reject a nil context without panicking.
	_, err = store.UpdateDisplayName(
		nil,
		lease.Scope,
		lease.CollectorID,
		collector.Version,
		&validDisplayName,
		connectedAt.Add(time.Minute),
	)
	assertInvalid("UpdateDisplayName(nil context)", err)
	_, err = store.UpdateDisplayName(
		ctx,
		lease.Scope,
		lease.CollectorID,
		0,
		&validDisplayName,
		connectedAt.Add(time.Minute),
	)
	assertInvalid("UpdateDisplayName(zero version)", err)
	_, err = store.UpdateDisplayName(
		ctx,
		lease.Scope,
		lease.CollectorID,
		uint64(math.MaxInt64)+1,
		&validDisplayName,
		connectedAt.Add(time.Minute),
	)
	assertInvalid("UpdateDisplayName(oversized version)", err)
	_, err = store.UpdateDisplayName(
		ctx,
		lease.Scope,
		lease.CollectorID,
		collector.Version,
		&validDisplayName,
		time.Time{},
	)
	assertInvalid("UpdateDisplayName(zero receive time)", err)

	for name, displayName := range map[string]string{
		"blank":        " \t ",
		"too long":     strings.Repeat("x", maximumDisplayNameBytes+1),
		"NUL":          "invalid\x00name",
		"control rune": "invalid\nname",
	} {
		_, updateErr := store.UpdateDisplayName(
			ctx,
			lease.Scope,
			lease.CollectorID,
			collector.Version,
			&displayName,
			connectedAt.Add(time.Minute),
		)
		assertInvalid("UpdateDisplayName("+name+")", updateErr)
	}

	_, err = store.SetAdministrativeState(
		ctx,
		lease.Scope,
		lease.CollectorID,
		0,
		AdministrativeStateDisabled,
		connectedAt.Add(time.Minute),
	)
	assertInvalid("SetAdministrativeState(zero version)", err)
	_, err = store.SetAdministrativeState(
		ctx,
		lease.Scope,
		lease.CollectorID,
		collector.Version,
		AdministrativeState("paused"),
		connectedAt.Add(time.Minute),
	)
	assertInvalid("SetAdministrativeState(invalid state)", err)
	_, err = store.SetAdministrativeState(
		ctx,
		lease.Scope,
		lease.CollectorID,
		collector.Version,
		AdministrativeStateDisabled,
		time.Time{},
	)
	assertInvalid("SetAdministrativeState(zero receive time)", err)

	missingTargets := []struct {
		name  string
		scope Scope
		id    string
	}{
		{
			name:  "cross tenant",
			scope: Scope{TenantID: "tenant-b"},
			id:    lease.CollectorID,
		},
		{
			name:  "absent collector",
			scope: lease.Scope,
			id:    "123e4567-e89b-12d3-a456-426614174999",
		},
	}
	for _, target := range missingTargets {
		_, getErr := store.GetAdministration(ctx, target.scope, target.id)
		if !errors.Is(getErr, control.ErrNotFound) {
			t.Fatalf("GetAdministration(%s) error = %v, want ErrNotFound", target.name, getErr)
		}
		_, displayErr := store.UpdateDisplayName(
			ctx,
			target.scope,
			target.id,
			collector.Version,
			&validDisplayName,
			connectedAt.Add(time.Minute),
		)
		if !errors.Is(displayErr, control.ErrNotFound) {
			t.Fatalf("UpdateDisplayName(%s) error = %v, want ErrNotFound", target.name, displayErr)
		}
		_, stateErr := store.SetAdministrativeState(
			ctx,
			target.scope,
			target.id,
			collector.Version,
			AdministrativeStateDisabled,
			connectedAt.Add(time.Minute),
		)
		if !errors.Is(stateErr, control.ErrNotFound) {
			t.Fatalf(
				"SetAdministrativeState(%s) error = %v, want ErrNotFound",
				target.name,
				stateErr,
			)
		}
	}

	persisted, err := store.GetAdministration(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Version != collector.Version ||
		persisted.DisplayName != nil ||
		persisted.AdministrativeState != AdministrativeStateEnabled {
		t.Fatalf("rejected administrative patches mutated state: %#v", persisted)
	}
}

func TestAdministrativePatchCASHasExactlyOneWinner(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 29, 6, 30, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	baselineDisplayName := "baseline"
	baseline, err := store.UpdateDisplayName(
		ctx,
		lease.Scope,
		lease.CollectorID,
		collector.Version,
		&baselineDisplayName,
		connectedAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}

	type patchResult struct {
		operation string
		snapshot  AdministrationSnapshot
		err       error
	}
	start := make(chan struct{})
	results := make(chan patchResult, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		displayName := "display contender"
		<-start
		snapshot, updateErr := store.UpdateDisplayName(
			ctx,
			lease.Scope,
			lease.CollectorID,
			baseline.Version,
			&displayName,
			connectedAt.Add(2*time.Minute),
		)
		results <- patchResult{operation: "display", snapshot: snapshot, err: updateErr}
	}()
	go func() {
		defer workers.Done()
		<-start
		snapshot, updateErr := store.SetAdministrativeState(
			ctx,
			lease.Scope,
			lease.CollectorID,
			baseline.Version,
			AdministrativeStateDisabled,
			connectedAt.Add(2*time.Minute),
		)
		results <- patchResult{operation: "state", snapshot: snapshot, err: updateErr}
	}()
	close(start)
	workers.Wait()
	close(results)

	var winner patchResult
	winners := 0
	conflicts := 0
	for result := range results {
		switch {
		case result.err == nil:
			winner = result
			winners++
		case errors.Is(result.err, control.ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("%s patch error = %v", result.operation, result.err)
		}
	}
	if winners != 1 || conflicts != 1 {
		t.Fatalf("concurrent patches produced winners=%d conflicts=%d", winners, conflicts)
	}

	persisted, err := store.GetAdministration(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Version != baseline.Version+1 ||
		persisted.Version != winner.snapshot.Version ||
		persisted.AdministrativeState != winner.snapshot.AdministrativeState ||
		!adminPatchEqualStrings(persisted.DisplayName, winner.snapshot.DisplayName) {
		t.Fatalf("persisted administration = %#v, winner = %#v", persisted, winner)
	}
	switch winner.operation {
	case "display":
		if persisted.DisplayName == nil ||
			*persisted.DisplayName != "display contender" ||
			persisted.AdministrativeState != AdministrativeStateEnabled {
			t.Fatalf("display winner overwrote state: %#v", persisted)
		}
	case "state":
		if persisted.DisplayName == nil ||
			*persisted.DisplayName != baselineDisplayName ||
			persisted.AdministrativeState != AdministrativeStateDisabled {
			t.Fatalf("state winner overwrote display: %#v", persisted)
		}
	default:
		t.Fatalf("unexpected winning operation %q", winner.operation)
	}
}

func TestAdministrativePatchTerminalCapacitySemantics(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 29, 6, 45, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	preservedDisplayName := "preserved at capacity"
	withDisplay, err := store.UpdateDisplayName(
		ctx,
		lease.Scope,
		lease.CollectorID,
		collector.Version,
		&preservedDisplayName,
		connectedAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_fleet
		SET admin_version = ?
		WHERE tenant_id = ? AND collector_id = ?`,
		int64(math.MaxInt64),
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatalf("saturate administrator version: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_runtime
		SET telemetry_revision = ?
		WHERE tenant_id = ? AND collector_id = ?`,
		int64(math.MaxInt64),
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatalf("saturate telemetry revision: %v", err)
	}

	type terminalResult struct {
		snapshot AdministrationSnapshot
		err      error
	}
	start := make(chan struct{})
	results := make(chan terminalResult, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			snapshot, updateErr := store.SetAdministrativeState(
				ctx,
				lease.Scope,
				lease.CollectorID,
				uint64(math.MaxInt64),
				AdministrativeStateDisabled,
				connectedAt.Add(2*time.Minute),
			)
			results <- terminalResult{snapshot: snapshot, err: updateErr}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	var disabled AdministrationSnapshot
	successes := 0
	rejections := 0
	for result := range results {
		switch {
		case result.err == nil:
			disabled = result.snapshot
			successes++
		case errors.Is(result.err, control.ErrCapacityExceeded),
			errors.Is(result.err, control.ErrVersionConflict):
			rejections++
		default:
			t.Fatalf("concurrent terminal disable error = %v", result.err)
		}
	}
	if successes != 1 || rejections != 1 {
		t.Fatalf(
			"concurrent terminal disables produced successes=%d rejections=%d",
			successes,
			rejections,
		)
	}
	if disabled.Version != uint64(math.MaxInt64) ||
		disabled.DisplayName == nil ||
		*disabled.DisplayName != preservedDisplayName ||
		disabled.AdministrativeState != AdministrativeStateDisabled {
		t.Fatalf("terminal disable = %#v, pre-capacity snapshot %#v", disabled, withDisplay)
	}

	replacementDisplayName := "must be rejected"
	capacityOperations := []struct {
		name string
		run  func() error
	}{
		{
			name: "display update",
			run: func() error {
				_, updateErr := store.UpdateDisplayName(
					ctx,
					lease.Scope,
					lease.CollectorID,
					uint64(math.MaxInt64),
					&replacementDisplayName,
					connectedAt.Add(3*time.Minute),
				)
				return updateErr
			},
		},
		{
			name: "re-enable",
			run: func() error {
				_, updateErr := store.SetAdministrativeState(
					ctx,
					lease.Scope,
					lease.CollectorID,
					uint64(math.MaxInt64),
					AdministrativeStateEnabled,
					connectedAt.Add(3*time.Minute),
				)
				return updateErr
			},
		},
		{
			name: "second disable",
			run: func() error {
				_, updateErr := store.SetAdministrativeState(
					ctx,
					lease.Scope,
					lease.CollectorID,
					uint64(math.MaxInt64),
					AdministrativeStateDisabled,
					connectedAt.Add(3*time.Minute),
				)
				return updateErr
			},
		},
	}
	for _, operation := range capacityOperations {
		if operationErr := operation.run(); !errors.Is(operationErr, control.ErrCapacityExceeded) {
			t.Fatalf("%s error = %v, want ErrCapacityExceeded", operation.name, operationErr)
		}
	}

	persisted, err := store.GetAdministration(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Version != uint64(math.MaxInt64) ||
		persisted.DisplayName == nil ||
		*persisted.DisplayName != preservedDisplayName ||
		persisted.AdministrativeState != AdministrativeStateDisabled {
		t.Fatalf("rejected capacity operations mutated state: %#v", persisted)
	}
}

func TestAdministrativePatchEnableRequiresValidInactiveRuntime(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 29, 7, 0, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}

	disabled, err := store.SetAdministrativeState(
		ctx,
		lease.Scope,
		lease.CollectorID,
		collector.Version,
		AdministrativeStateDisabled,
		connectedAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("SetAdministrativeState(disable): %v", err)
	}
	inactive, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if inactive.ActiveLease != nil || inactive.DisconnectedAt == nil {
		t.Fatalf("disable did not create a valid inactive runtime: %#v", inactive)
	}

	enabled, err := store.SetAdministrativeState(
		ctx,
		lease.Scope,
		lease.CollectorID,
		disabled.Version,
		AdministrativeStateEnabled,
		connectedAt.Add(2*time.Minute),
	)
	if err != nil {
		t.Fatalf("SetAdministrativeState(enable valid inactive runtime): %v", err)
	}
	if enabled.Version != disabled.Version+1 ||
		enabled.AdministrativeState != AdministrativeStateEnabled {
		t.Fatalf("enabled snapshot = %#v", enabled)
	}
	disabledAgain, err := store.SetAdministrativeState(
		ctx,
		lease.Scope,
		lease.CollectorID,
		enabled.Version,
		AdministrativeStateDisabled,
		connectedAt.Add(3*time.Minute),
	)
	if err != nil {
		t.Fatalf("SetAdministrativeState(disable inactive runtime): %v", err)
	}

	adminPatchInstallCorruption(
		t,
		database,
		ctx,
		`UPDATE collector_runtime
		 SET observed_at_unix_micro = 1
		 WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	)
	if _, err := store.SetAdministrativeState(
		ctx,
		lease.Scope,
		lease.CollectorID,
		disabledAgain.Version,
		AdministrativeStateEnabled,
		connectedAt.Add(4*time.Minute),
	); err == nil {
		t.Fatal("SetAdministrativeState enabled a corrupt inactive runtime")
	}
	persisted, err := store.GetAdministration(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Version != disabledAgain.Version ||
		persisted.AdministrativeState != AdministrativeStateDisabled {
		t.Fatalf("rejected re-enable mutated administration: %#v", persisted)
	}
	if _, err := store.Get(ctx, lease.Scope, lease.CollectorID); err == nil {
		t.Fatal("Get accepted corrupt inactive runtime")
	}
}

func TestAdministrativeProjectionAndDisableSurviveUnrelatedCorruption(t *testing.T) {
	t.Parallel()

	corruptions := []struct {
		name          string
		statement     string
		wantFullError string
	}{
		{
			name: "runtime telemetry",
			statement: `UPDATE collector_runtime
				    SET queued_events = -1
				    WHERE tenant_id = ? AND collector_id = ?`,
			wantFullError: "negative collector telemetry",
		},
		{
			name: "child capability",
			statement: `UPDATE collector_capabilities
				    SET capability = 0
				    WHERE tenant_id = ? AND collector_id = ? AND capability = 1`,
			wantFullError: "invalid collector capability",
		},
	}
	for index, corruption := range corruptions {
		corruption := corruption
		t.Run(corruption.name, func(t *testing.T) {
			t.Parallel()

			database, store := openTestStore(t)
			ctx := context.Background()
			connectedAt := time.Date(2026, 7, 29, 7, 15+index*15, 0, 0, time.UTC)
			collector, lease, err := store.Claim(ctx, testClaim(connectedAt))
			if err != nil {
				t.Fatal(err)
			}
			displayName := "administration remains readable"
			administration, err := store.UpdateDisplayName(
				ctx,
				lease.Scope,
				lease.CollectorID,
				collector.Version,
				&displayName,
				connectedAt.Add(time.Minute),
			)
			if err != nil {
				t.Fatal(err)
			}
			adminPatchInstallCorruption(
				t,
				database,
				ctx,
				corruption.statement,
				lease.TenantID,
				lease.CollectorID,
			)

			if _, err := store.Get(ctx, lease.Scope, lease.CollectorID); err == nil ||
				!strings.Contains(err.Error(), corruption.wantFullError) {
				t.Fatalf("Get(corrupt %s) error = %v", corruption.name, err)
			}
			readable, err := store.GetAdministration(ctx, lease.Scope, lease.CollectorID)
			if err != nil {
				t.Fatalf("GetAdministration(corrupt %s): %v", corruption.name, err)
			}
			if readable.Version != administration.Version ||
				readable.DisplayName == nil ||
				*readable.DisplayName != displayName ||
				readable.AdministrativeState != AdministrativeStateEnabled {
				t.Fatalf("administration over corrupt %s = %#v", corruption.name, readable)
			}

			disabled, err := store.SetAdministrativeState(
				ctx,
				lease.Scope,
				lease.CollectorID,
				readable.Version,
				AdministrativeStateDisabled,
				connectedAt.Add(2*time.Minute),
			)
			if err != nil {
				t.Fatalf("SetAdministrativeState(disable corrupt %s): %v", corruption.name, err)
			}
			if disabled.Version != readable.Version+1 ||
				disabled.DisplayName == nil ||
				*disabled.DisplayName != displayName ||
				disabled.AdministrativeState != AdministrativeStateDisabled {
				t.Fatalf("disable over corrupt %s = %#v", corruption.name, disabled)
			}
			afterDisable, err := store.GetAdministration(ctx, lease.Scope, lease.CollectorID)
			if err != nil {
				t.Fatalf("GetAdministration(disabled corrupt %s): %v", corruption.name, err)
			}
			if afterDisable.Version != disabled.Version ||
				afterDisable.DisplayName == nil ||
				*afterDisable.DisplayName != displayName ||
				afterDisable.AdministrativeState != AdministrativeStateDisabled {
				t.Fatalf("persisted disable over corrupt %s = %#v", corruption.name, afterDisable)
			}
			if _, err := store.Get(ctx, lease.Scope, lease.CollectorID); err == nil {
				t.Fatalf("Get accepted disabled corrupt %s", corruption.name)
			}
		})
	}
}

func adminPatchEqualStrings(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func adminPatchInstallCorruption(
	t *testing.T,
	database *control.DB,
	ctx context.Context,
	statement string,
	arguments ...any,
) {
	t.Helper()
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	defer connection.Close()
	checksIgnored := false
	defer func() {
		if checksIgnored {
			_, _ = connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`)
		}
	}()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable check constraints for corruption fixture: %v", err)
	}
	checksIgnored = true
	result, err := connection.ExecContext(ctx, statement, arguments...)
	if err != nil {
		t.Fatalf("install corruption fixture: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("read corruption fixture rows affected: %v", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("corruption fixture updated %d rows, want 1", rowsAffected)
	}
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatalf("restore check constraints after corruption fixture: %v", err)
	}
	checksIgnored = false
}
