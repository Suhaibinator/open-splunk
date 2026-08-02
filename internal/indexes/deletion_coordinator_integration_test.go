package indexes_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexes"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"github.com/Suhaibinator/open-splunk/migrations"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestIndexDataDeletionCoordinatorAgainstClickHouse composes the persistent
// SQLite control plane, visibility outbox, native ClickHouse Store, and
// coordinator. It is opt-in because it starts an ephemeral Docker container
// and may pull the repository's digest-pinned ClickHouse image.
func TestIndexDataDeletionCoordinatorAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip(
			"set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test",
		)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}

	image := strings.TrimSpace(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if image == "" {
		image = testsupport.DefaultClickHouseImage
	}
	if !strings.Contains(image, "@sha256:") {
		t.Fatalf(
			"OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE must be digest-pinned, got %q",
			image,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close coordinator ClickHouse fixture: %v", closeErr)
		}
	})

	options := &clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
	}
	queryConnection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := queryConnection.Close(); closeErr != nil {
			t.Errorf("close coordinator query connection: %v", closeErr)
		}
	})
	if err := queryConnection.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := server.ApplyClickHouseMigrations(
		ctx,
		queryConnection,
		migrations.ClickHouse(),
	); err != nil {
		t.Fatal(err)
	}

	controlPath := filepath.Join(t.TempDir(), "control.sqlite")
	storeConfig := clickhouse.DefaultConfig()
	storeConfig.Addresses = []string{container.Address}
	storeConfig.Database = container.Database
	storeConfig.Username = container.Username
	storeConfig.Password = container.Password
	// A long reconciler delay gives each restart schedule a deterministic
	// window in which only the coordinator's frozen drain can claim the outbox.
	storeConfig.RetryAfter = 30 * time.Second
	t.Run("shared registry rejects reads before native deletion", func(t *testing.T) {
		testCoordinatorSharedReadRetirement(
			t,
			ctx,
			queryConnection,
			storeConfig,
		)
	})

	const (
		targetTenant  = "coordinator-tenant"
		targetIndex   = "coordinator-delete"
		foreignTenant = "coordinator-foreign-tenant"
		neighborIndex = "coordinator-neighbor"
	)
	trace := newCoordinatorIntegrationTrace()
	firstGate := newCoordinatorSequencerGate()
	firstProcess := openCoordinatorIntegrationProcess(
		t,
		ctx,
		controlPath,
		storeConfig,
		firstGate.wrap,
	)
	waitCoordinatorIntegrationSignal(
		t,
		ctx,
		firstGate.delegated,
		"initial outbox reconciliation",
	)
	if err := firstProcess.store.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	july := time.Date(2026, time.July, 21, 3, 4, 5, 0, time.UTC)
	fixtures := []coordinatorEventFixture{
		{
			eventID:   "coordinator-delete-july",
			tenantID:  targetTenant,
			indexName: targetIndex,
			eventTime: july,
		},
		{
			eventID:   "coordinator-delete-august",
			tenantID:  targetTenant,
			indexName: targetIndex,
			eventTime: july.AddDate(0, 1, 0),
		},
		{
			eventID:   "coordinator-delete-foreign-tenant",
			tenantID:  foreignTenant,
			indexName: targetIndex,
			eventTime: july,
		},
		{
			eventID:   "coordinator-delete-neighbor-index",
			tenantID:  targetTenant,
			indexName: neighborIndex,
			eventTime: july,
		},
	}
	for position, fixture := range fixtures {
		storeCoordinatorEvent(
			t,
			ctx,
			firstProcess.store,
			uint64(position+1),
			fixture,
		)
	}
	assertCoordinatorDeletionRows(
		t,
		ctx,
		queryConnection,
		targetTenant,
		targetIndex,
		foreignTenant,
		neighborIndex,
		2,
		2,
		1,
		1,
	)

	var tableUUID string
	if err := queryConnection.QueryRow(
		ctx,
		`SELECT toString(uuid)
		 FROM system.tables
		 WHERE database = ? AND name = ?`,
		container.Database,
		"events",
	).Scan(&tableUUID); err != nil {
		t.Fatalf("read ClickHouse events table UUID: %v", err)
	}

	index, err := firstProcess.control.CreateIndex(ctx, control.IndexDefinition{
		Name:             targetIndex,
		DisplayName:      "Coordinator physical deletion",
		IngestionEnabled: true,
		SearchEnabled:    true,
	})
	if err != nil {
		t.Fatalf("create coordinator index: %v", err)
	}
	index, err = firstProcess.control.SetIndexState(
		ctx,
		index.ID,
		index.Version,
		control.IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive coordinator index: %v", err)
	}
	operation, err := firstProcess.control.BeginIndexDataDeletion(
		ctx,
		control.IndexDataDeletionScope{TenantID: targetTenant},
		index.ID,
		index.Version,
		index.Definition.Name,
	)
	if err != nil {
		t.Fatalf("begin coordinator deletion: %v", err)
	}
	if operation.TenantID != targetTenant {
		t.Fatalf(
			"admitted operation tenant = %q, want %q",
			operation.TenantID,
			targetTenant,
		)
	}
	if _, err := firstProcess.control.GetIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"mutation attempt before coordinator error = %v, want ErrNotFound",
			err,
		)
	}

	// MarkSending durably flips the reservation to ambiguous, then reports a
	// lost SQLite result. The native batch is aborted before Send, and the gate
	// prevents the store-owned reconciler from claiming the durable replay.
	ambiguousFixture := coordinatorEventFixture{
		eventID:   "coordinator-delete-ambiguous-replay",
		tenantID:  targetTenant,
		indexName: targetIndex,
		eventTime: july.AddDate(0, 2, 0),
	}
	firstGate.armAmbiguousFailure()
	ambiguousBatch := coordinatorStoreBatch(5, ambiguousFixture)
	result, storeErr := firstProcess.store.Store(ctx, ambiguousBatch)
	if !errors.Is(storeErr, io.ErrUnexpectedEOF) ||
		result.Accepted != 0 ||
		result.Duplicate != 0 ||
		result.AcknowledgedThrough != nil ||
		!result.CommittedAt.IsZero() ||
		result.OriginalEventCount != 0 ||
		len(result.RejectedEvents) != 0 {
		t.Fatalf(
			"ambiguous fixture Store = (%#v, %v), want zero result and EOF",
			result,
			storeErr,
		)
	}
	waitCoordinatorIntegrationSignal(
		t,
		ctx,
		firstGate.markFailed,
		"durable ambiguous MarkSending result loss",
	)
	waitCoordinatorIntegrationSignal(
		t,
		ctx,
		firstGate.blocked,
		"background reconciler outbox hold",
	)
	if usage, err := firstGate.PendingUsage(ctx); err != nil ||
		usage.Reservations != 1 || usage.OutboxBytes == 0 {
		t.Fatalf(
			"ambiguous outbox usage = %#v, error=%v, want one durable reservation",
			usage,
			err,
		)
	}
	if cutoff, err := firstProcess.store.VisibilityCutoff(ctx); err != nil ||
		cutoff != 4 {
		t.Fatalf(
			"visibility cutoff before restart = %d, error=%v, want 4",
			cutoff,
			err,
		)
	}
	assertCoordinatorEventCount(
		t,
		ctx,
		queryConnection,
		ambiguousFixture.eventID,
		0,
	)
	if err := firstProcess.close(); err != nil {
		t.Fatalf("close ambiguous pre-coordinator process: %v", err)
	}

	// On the first coordinator restart, keep the regular reconciler held until
	// the write-freeze callback enters DrainPending. The wrapped frozen
	// capability releases the hold, verifies the replayed target row is
	// physical before Advance, and records the call order.
	replayGate := newCoordinatorSequencerGate()
	replayGate.hold.Store(true)
	secondProcess := openCoordinatorIntegrationProcess(
		t,
		ctx,
		controlPath,
		storeConfig,
		replayGate.wrap,
	)
	waitCoordinatorIntegrationSignal(
		t,
		ctx,
		replayGate.blocked,
		"restarted background reconciler outbox hold",
	)
	restartedOperation, err := secondProcess.control.GetIndexDeletionOperation(
		ctx,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("read deletion operation after first restart: %v", err)
	}
	if restartedOperation != operation ||
		restartedOperation.TenantID != targetTenant {
		t.Fatalf(
			"first-restart operation = %#v, want %#v with tenant %q",
			restartedOperation,
			operation,
			targetTenant,
		)
	}

	// A process restarted under a different deployment tenant must fail closed
	// from the admission snapshot before polling or freezing ClickHouse. The
	// original tenant can then recover the same oldest operation.
	wrongTenantStore := &countingCoordinatorDeletionStore{}
	wrongTenantErrors := make(chan error, 1)
	wrongTenantCoordinator := startCoordinatorIntegration(
		t,
		secondProcess.control,
		wrongTenantStore,
		indexes.IndexDataDeletionCoordinatorConfig{
			TenantID:         foreignTenant,
			ReadRetirement:   indexread.NewRegistry(),
			PollInterval:     time.Hour,
			RecoveryInterval: time.Hour,
			RetryInitial:     time.Hour,
			RetryMaximum:     time.Hour,
			StepTimeout:      30 * time.Second,
			OnError: func(err error) {
				select {
				case wrongTenantErrors <- err:
				default:
				}
			},
		},
	)
	select {
	case wrongTenantErr := <-wrongTenantErrors:
		if wrongTenantErr == nil ||
			!strings.Contains(wrongTenantErr.Error(), operation.ID) ||
			!strings.Contains(wrongTenantErr.Error(), "operation tenant") ||
			!strings.Contains(wrongTenantErr.Error(), targetTenant) ||
			!strings.Contains(wrongTenantErr.Error(), foreignTenant) {
			t.Fatalf(
				"wrong-tenant coordinator error = %v, want operation %q and tenant drift",
				wrongTenantErr,
				operation.ID,
			)
		}
	case <-ctx.Done():
		t.Fatalf(
			"wait for wrong-tenant coordinator failure: %v",
			ctx.Err(),
		)
	case <-time.After(time.Minute):
		t.Fatal("timed out waiting for wrong-tenant coordinator failure")
	}
	closeCoordinatorIntegration(t, wrongTenantCoordinator)
	if statusCalls, freezeCalls := wrongTenantStore.calls(); statusCalls != 0 || freezeCalls != 0 {
		t.Fatalf(
			"wrong-tenant native calls = status %d, freeze %d; want 0/0",
			statusCalls,
			freezeCalls,
		)
	}
	if _, err := secondProcess.control.GetIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"wrong-tenant mutation attempt error = %v, want ErrNotFound",
			err,
		)
	}
	unchangedOperation, err := secondProcess.control.GetIndexDeletionOperation(
		ctx,
		operation.ID,
	)
	if err != nil || unchangedOperation != operation {
		t.Fatalf(
			"operation after wrong-tenant restart = %#v, error=%v; want %#v",
			unchangedOperation,
			err,
			operation,
		)
	}
	if usage, err := replayGate.PendingUsage(ctx); err != nil ||
		usage.Reservations != 1 || usage.OutboxBytes == 0 {
		t.Fatalf(
			"outbox after wrong-tenant restart = %#v, error=%v; want one durable reservation",
			usage,
			err,
		)
	}
	assertCoordinatorEventCount(
		t,
		ctx,
		queryConnection,
		ambiguousFixture.eventID,
		0,
	)

	firstDeletionStore := &recordingCoordinatorDeletionStore{
		delegate:      secondProcess.store,
		query:         queryConnection,
		replayGate:    replayGate,
		replayEventID: ambiguousFixture.eventID,
		phase:         "first",
		trace:         trace,
	}
	precommitControl := &precommitErrorDeletionControl{
		DB:           secondProcess.control,
		trace:        trace,
		failed:       make(chan struct{}),
		auditMissing: make(chan struct{}),
	}
	firstCoordinator := startCoordinatorIntegration(
		t,
		precommitControl,
		firstDeletionStore,
		indexes.IndexDataDeletionCoordinatorConfig{
			TenantID:         targetTenant,
			ReadRetirement:   indexread.NewRegistry(),
			PollInterval:     25 * time.Millisecond,
			RecoveryInterval: time.Hour,
			RetryInitial:     time.Hour,
			RetryMaximum:     time.Hour,
			StepTimeout:      30 * time.Second,
		},
	)
	waitCoordinatorIntegrationSignal(
		t,
		ctx,
		precommitControl.failed,
		"precommit completion failure",
	)
	waitCoordinatorIntegrationSignal(
		t,
		ctx,
		precommitControl.auditMissing,
		"missing completion audit after precommit failure",
	)
	closeCoordinatorIntegration(t, firstCoordinator)
	if calls := precommitControl.completeCalls.Load(); calls != 1 {
		t.Fatalf("precommit completion calls = %d, want 1", calls)
	}
	if usage, err := replayGate.PendingUsage(ctx); err != nil ||
		usage != (visibility.PendingUsage{}) {
		t.Fatalf(
			"outbox after frozen replay = %#v, error=%v, want empty",
			usage,
			err,
		)
	}
	if cutoff, err := secondProcess.store.VisibilityCutoff(ctx); err != nil ||
		cutoff != 5 {
		t.Fatalf(
			"visibility cutoff after frozen replay = %d, error=%v, want 5",
			cutoff,
			err,
		)
	}
	attempt, err := secondProcess.control.GetIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("read durable mutation attempt after precommit error: %v", err)
	}
	if _, err := secondProcess.control.GetIndexDataDeletionCompletion(
		ctx,
		operation.ID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"completion after precommit error = %v, want ErrNotFound",
			err,
		)
	}
	assertCoordinatorDeletionRows(
		t,
		ctx,
		queryConnection,
		targetTenant,
		targetIndex,
		foreignTenant,
		neighborIndex,
		0,
		0,
		1,
		1,
	)
	if err := secondProcess.close(); err != nil {
		t.Fatalf("close precommit-error process: %v", err)
	}

	// The second coordinator restart recovers the durable attempt. Because the
	// prior zero proof was not terminally committed, it must freeze, drain, and
	// Advance again. Its completion commit succeeds but reports EOF, forcing an
	// immutable-audit read before the worker may discover the next operation.
	thirdProcess := openCoordinatorIntegrationProcess(
		t,
		ctx,
		controlPath,
		storeConfig,
		nil,
	)
	restartedOperation, err = thirdProcess.control.GetIndexDeletionOperation(
		ctx,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("read deletion operation after second restart: %v", err)
	}
	if restartedOperation != operation ||
		restartedOperation.TenantID != targetTenant {
		t.Fatalf(
			"second-restart operation = %#v, want %#v with tenant %q",
			restartedOperation,
			operation,
			targetTenant,
		)
	}
	secondDeletionStore := &recordingCoordinatorDeletionStore{
		delegate: thirdProcess.store,
		phase:    "second",
		trace:    trace,
	}
	controlWithAmbiguousCompletion := &commitThenErrorDeletionControl{
		DB:       thirdProcess.control,
		trace:    trace,
		resolved: make(chan struct{}),
		advanced: make(chan struct{}),
	}
	secondCoordinator := startCoordinatorIntegration(
		t,
		controlWithAmbiguousCompletion,
		secondDeletionStore,
		indexes.IndexDataDeletionCoordinatorConfig{
			TenantID:         targetTenant,
			ReadRetirement:   indexread.NewRegistry(),
			PollInterval:     25 * time.Millisecond,
			RecoveryInterval: 250 * time.Millisecond,
			RetryInitial:     25 * time.Millisecond,
			RetryMaximum:     250 * time.Millisecond,
			StepTimeout:      30 * time.Second,
		},
	)
	waitCoordinatorIntegrationSignal(
		t,
		ctx,
		controlWithAmbiguousCompletion.resolved,
		"committed completion recovery",
	)
	waitCoordinatorIntegrationSignal(
		t,
		ctx,
		controlWithAmbiguousCompletion.advanced,
		"post-recovery operation discovery",
	)
	closeCoordinatorIntegration(t, secondCoordinator)
	if calls := controlWithAmbiguousCompletion.completeCalls.Load(); calls != 1 {
		t.Fatalf(
			"commit-then-error completion calls = %d, want 1",
			calls,
		)
	}

	secondAdvances := trace.advancesFor("second")
	if len(secondAdvances) != 1 ||
		secondAdvances[0].State !=
			clickhouse.IndexDataDeletionPhysicallyEmpty ||
		secondAdvances[0].SubmissionAttempted {
		t.Fatalf(
			"second-restart advances = %#v, want one read-only zero proof",
			secondAdvances,
		)
	}
	assertCoordinatorIntegrationSubsequence(
		t,
		trace.snapshot(),
		[]string{
			"first.drain.done",
			"first.replay.visible",
			"first.advance",
			"first.complete.precommit-error",
			"first.completion.missing",
			"second.drain.done",
			"second.advance",
			"second.complete.commit-error",
			"second.completion.resolved",
			"second.next.empty",
		},
	)

	completion, err := thirdProcess.control.GetIndexDataDeletionCompletion(
		ctx,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("read coordinator deletion completion: %v", err)
	}
	wantTarget := control.IndexDeletionMutationTarget{
		TenantID:  targetTenant,
		Database:  container.Database,
		Table:     "events",
		TableUUID: tableUUID,
	}
	if completion.DeletionOperationID != operation.ID ||
		completion.IndexID != operation.IndexID ||
		completion.IndexName != operation.IndexName ||
		completion.ArchivedVersion != operation.ArchivedVersion ||
		completion.DeletedVersion != operation.DeletingVersion ||
		completion.Target != wantTarget ||
		completion.Target.TenantID != operation.TenantID ||
		completion.ProtocolVersion !=
			control.IndexDeletionMutationProtocolVersion ||
		!completion.OperationCreatedAt.Equal(operation.CreatedAt) ||
		completion.CorrelationID == "" ||
		completion.MutationCreatedAt.Before(operation.CreatedAt) ||
		completion.CompletedAt.Before(completion.MutationCreatedAt) {
		t.Fatalf(
			"completion = %#v, operation = %#v, target = %#v",
			completion,
			operation,
			wantTarget,
		)
	}
	assertOneCoordinatorDeletionMutation(
		t,
		ctx,
		thirdProcess.store,
		attempt,
	)
	assertCoordinatorTerminalControlState(
		t,
		ctx,
		thirdProcess.control,
		operation,
		completion,
	)
	if err := thirdProcess.close(); err != nil {
		t.Fatalf("close terminal process: %v", err)
	}

	restartedControl, err := control.Open(ctx, controlPath)
	if err != nil {
		t.Fatalf("reopen terminal control plane: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := restartedControl.Close(); closeErr != nil {
			t.Errorf("close restarted terminal control plane: %v", closeErr)
		}
	})
	restartedCompletion, err := restartedControl.GetIndexDataDeletionCompletion(
		ctx,
		operation.ID,
	)
	if err != nil {
		t.Fatalf("recover terminal deletion completion: %v", err)
	}
	if !sameIndexDataDeletionCompletion(restartedCompletion, completion) {
		t.Fatalf(
			"restarted completion = %#v, want %#v",
			restartedCompletion,
			completion,
		)
	}
	if restartedCompletion.Target.TenantID != operation.TenantID {
		t.Fatalf(
			"restarted completion tenant = %q, want admitted tenant %q",
			restartedCompletion.Target.TenantID,
			operation.TenantID,
		)
	}
	assertCoordinatorTerminalControlState(
		t,
		ctx,
		restartedControl,
		operation,
		restartedCompletion,
	)
}

func testCoordinatorSharedReadRetirement(
	t *testing.T,
	ctx context.Context,
	queryConnection driver.Conn,
	storeConfig clickhouse.Config,
) {
	t.Helper()

	const (
		tenantID  = "coordinator-read-fence-tenant"
		indexName = "coordinator-read-fence"
		eventID   = "coordinator-read-fence-event"
	)
	eventTime := time.Date(2026, time.June, 2, 3, 4, 5, 0, time.UTC)
	process := openCoordinatorIntegrationProcess(
		t,
		ctx,
		filepath.Join(t.TempDir(), "read-fence-control.sqlite"),
		storeConfig,
		nil,
	)
	storeCoordinatorEvent(t, ctx, process.store, 1, coordinatorEventFixture{
		eventID:   eventID,
		tenantID:  tenantID,
		indexName: indexName,
		eventTime: eventTime,
	})

	sharedRegistry := indexread.NewRegistry()
	countingConnection := &coordinatorReadFenceConnection{
		Conn: queryConnection,
	}
	executor, err := queryexec.New(
		countingConnection,
		queryexec.Config{ReadAdmission: sharedRegistry},
	)
	if err != nil {
		t.Fatalf("create read-fenced ClickHouse executor: %v", err)
	}
	compiled := compileCoordinatorReadFenceQuery(
		t,
		tenantID,
		indexName,
		eventTime,
	)
	preflightSink := &coordinatorReadFenceSink{}
	if err := executor.Execute(ctx, compiled, preflightSink); err != nil {
		t.Fatalf("execute pre-retirement query: %v", err)
	}
	if schemaCalls, rowCalls := preflightSink.calls(); schemaCalls != 1 || rowCalls != 1 {
		t.Fatalf(
			"pre-retirement publications = schema %d, rows %d; want 1/1",
			schemaCalls,
			rowCalls,
		)
	}

	index, err := process.control.CreateIndex(ctx, control.IndexDefinition{
		Name:             indexName,
		DisplayName:      "Coordinator read retirement",
		IngestionEnabled: true,
		SearchEnabled:    true,
	})
	if err != nil {
		t.Fatalf("create read-retirement index: %v", err)
	}
	index, err = process.control.SetIndexState(
		ctx,
		index.ID,
		index.Version,
		control.IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive read-retirement index: %v", err)
	}
	operation, err := process.control.BeginIndexDataDeletion(
		ctx,
		control.IndexDataDeletionScope{TenantID: tenantID},
		index.ID,
		index.Version,
		index.Definition.Name,
	)
	if err != nil {
		t.Fatalf("begin read-retirement deletion: %v", err)
	}

	blockedStore := newCoordinatorReadFenceDeletionStore(process.store)
	coordinatorErrors := make(chan error, 1)
	coordinator := startCoordinatorIntegration(
		t,
		process.control,
		blockedStore,
		indexes.IndexDataDeletionCoordinatorConfig{
			TenantID:         tenantID,
			ReadRetirement:   sharedRegistry,
			PollInterval:     25 * time.Millisecond,
			RecoveryInterval: time.Hour,
			RetryInitial:     25 * time.Millisecond,
			RetryMaximum:     250 * time.Millisecond,
			StepTimeout:      30 * time.Second,
			OnError: func(err error) {
				select {
				case coordinatorErrors <- err:
				default:
				}
			},
		},
	)
	waitCoordinatorIntegrationSignal(
		t,
		ctx,
		blockedStore.entered,
		"read retirement before native deletion",
	)
	assertCoordinatorEventCount(t, ctx, queryConnection, eventID, 1)

	queryCallsBefore := countingConnection.queryCalls.Load()
	retiredSink := &coordinatorReadFenceSink{}
	err = executor.Execute(ctx, compiled, retiredSink)
	if !errors.Is(err, indexread.ErrUnavailable) ||
		!errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf(
			"post-retirement Execute() error = %v, want ErrUnavailable and ErrStorageUnavailable",
			err,
		)
	}
	if queryCallsAfter := countingConnection.queryCalls.Load(); queryCallsAfter != queryCallsBefore {
		t.Fatalf(
			"post-retirement native query calls = %d, want unchanged %d",
			queryCallsAfter,
			queryCallsBefore,
		)
	}
	if schemaCalls, rowCalls := retiredSink.calls(); schemaCalls != 0 || rowCalls != 0 {
		t.Fatalf(
			"post-retirement publications = schema %d, rows %d; want 0/0",
			schemaCalls,
			rowCalls,
		)
	}

	close(blockedStore.release)
	waitCoordinatorReadFenceCompletion(
		t,
		ctx,
		process.control,
		operation.ID,
		coordinatorErrors,
	)
	closeCoordinatorIntegration(t, coordinator)
	assertCoordinatorEventCount(t, ctx, queryConnection, eventID, 0)
}

type coordinatorEventFixture struct {
	eventID   string
	tenantID  string
	indexName string
	eventTime time.Time
}

type coordinatorReadFenceConnection struct {
	driver.Conn
	queryCalls atomic.Uint32
}

func (connection *coordinatorReadFenceConnection) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	connection.queryCalls.Add(1)
	return connection.Conn.Query(ctx, query, args...)
}

type coordinatorReadFenceSink struct {
	schemaCalls atomic.Uint32
	rowCalls    atomic.Uint32
}

func (sink *coordinatorReadFenceSink) SetSchema(searchjobs.Schema) error {
	sink.schemaCalls.Add(1)
	return nil
}

func (sink *coordinatorReadFenceSink) AddRow([]searchjobs.Value) error {
	sink.rowCalls.Add(1)
	return nil
}

func (sink *coordinatorReadFenceSink) calls() (uint32, uint32) {
	return sink.schemaCalls.Load(), sink.rowCalls.Load()
}

type coordinatorReadFenceDeletionStore struct {
	delegate *clickhouse.Store
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func newCoordinatorReadFenceDeletionStore(
	delegate *clickhouse.Store,
) *coordinatorReadFenceDeletionStore {
	return &coordinatorReadFenceDeletionStore{
		delegate: delegate,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (store *coordinatorReadFenceDeletionStore) IndexDataDeletionStatus(
	ctx context.Context,
	request clickhouse.IndexDataDeletionRequest,
) (clickhouse.IndexDataDeletionProgress, error) {
	return store.delegate.IndexDataDeletionStatus(ctx, request)
}

func (store *coordinatorReadFenceDeletionStore) WithWritesFrozen(
	ctx context.Context,
	callback func(context.Context, clickhouse.FrozenWrites) error,
) error {
	store.once.Do(func() { close(store.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-store.release:
	}
	return store.delegate.WithWritesFrozen(ctx, callback)
}

func compileCoordinatorReadFenceQuery(
	t *testing.T,
	tenantID string,
	indexName string,
	eventTime time.Time,
) clickhouse.CompiledQuery {
	t.Helper()

	parsed, err := spl.Parse("index=" + indexName + " | stats count")
	if err != nil {
		t.Fatalf("parse read-fence SPL: %v", err)
	}
	visibilityCutoff := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          tenantID,
		AuthorizedIndexes: []string{indexName},
		RequestedIndexes:  []string{indexName},
		Earliest:          eventTime.Add(-time.Hour),
		Latest:            eventTime.Add(time.Hour),
		SearchStart:       eventTime.Add(2 * time.Hour),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   eventTime.Add(2 * time.Hour),
		VisibilityCutoff:  &visibilityCutoff,
	})
	if err != nil {
		t.Fatalf("build read-fence plan: %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile read-fence plan: %v", err)
	}
	return compiled
}

func waitCoordinatorReadFenceCompletion(
	t *testing.T,
	ctx context.Context,
	controlPlane *control.DB,
	operationID string,
	coordinatorErrors <-chan error,
) {
	t.Helper()

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	for {
		if _, err := controlPlane.GetIndexDataDeletionCompletion(
			ctx,
			operationID,
		); err == nil {
			return
		} else if !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("read read-fence deletion completion: %v", err)
		}
		select {
		case err := <-coordinatorErrors:
			t.Fatalf("reconcile read-fence deletion: %v", err)
		case <-ctx.Done():
			t.Fatalf("wait for read-fence deletion completion: %v", ctx.Err())
		case <-timer.C:
			t.Fatal("timed out waiting for read-fence deletion completion")
		case <-ticker.C:
		}
	}
}

func storeCoordinatorEvent(
	t *testing.T,
	ctx context.Context,
	store *clickhouse.Store,
	sequence uint64,
	fixture coordinatorEventFixture,
) {
	t.Helper()

	result, err := store.Store(
		ctx,
		coordinatorStoreBatch(sequence, fixture),
	)
	if err != nil {
		t.Fatalf("store coordinator fixture %q: %v", fixture.eventID, err)
	}
	if result.Accepted != 1 || result.Duplicate != 0 {
		t.Fatalf(
			"store coordinator fixture %q result = %#v",
			fixture.eventID,
			result,
		)
	}
}

func coordinatorStoreBatch(
	sequence uint64,
	fixture coordinatorEventFixture,
) ingest.StoreBatch {
	collectorID := "coordinator-collector"
	batchID := "coordinator-batch-" + fixture.eventID
	indexTime := fixture.eventTime.Add(time.Minute)
	event := &ingest.StoredEvent{
		TenantID:    fixture.tenantID,
		CollectorID: collectorID,
		BatchID:     batchID,
		IndexTime:   indexTime,
		Event: &opensplunkv1.LogEvent{
			EventId:         fixture.eventID,
			IndexName:       fixture.indexName,
			EventTime:       timestamppb.New(fixture.eventTime),
			CollectedAt:     timestamppb.New(fixture.eventTime.Add(-time.Second)),
			EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            "coordinator-host",
			Source:          "coordinator.log",
			Sourcetype:      "open_splunk:coordinator",
			Severity:        opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
			Raw:             []byte(`{"message":"coordinator deletion fixture"}`),
			RawEncoding:     opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			Fields:          &opensplunkv1.TypedObject{},
		},
	}
	return ingest.StoreBatch{
		TenantID:           fixture.tenantID,
		CollectorID:        collectorID,
		BatchID:            batchID,
		BatchSequence:      sequence,
		OriginalEventCount: 1,
		SourceBatchSHA256: sha256.Sum256(
			[]byte("coordinator-source-batch:" + batchID),
		),
		ReceivedAt: indexTime,
		Events:     []*ingest.StoredEvent{event},
	}
}

func assertCoordinatorEventCount(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	eventID string,
	want uint64,
) {
	t.Helper()

	var count uint64
	if err := connection.QueryRow(
		ctx,
		"SELECT count() FROM open_splunk.events WHERE event_id = ?",
		eventID,
	).Scan(&count); err != nil {
		t.Fatalf("query coordinator event %q: %v", eventID, err)
	}
	if count != want {
		t.Fatalf(
			"coordinator event %q rows = %d, want %d",
			eventID,
			count,
			want,
		)
	}
}

func assertCoordinatorDeletionRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	targetTenant string,
	targetIndex string,
	foreignTenant string,
	neighborIndex string,
	wantTargetRows uint64,
	wantTargetPartitions uint64,
	wantForeignTenantRows uint64,
	wantNeighborRows uint64,
) {
	t.Helper()

	var (
		targetRows       uint64
		targetPartitions uint64
		foreignRows      uint64
		neighborRows     uint64
	)
	if err := connection.QueryRow(
		ctx,
		`SELECT
		     countIf(tenant_id = ? AND index_name = ?),
		     uniqExactIf(
		       toYYYYMM(event_time),
		       tenant_id = ? AND index_name = ?
		     ),
		     countIf(tenant_id = ? AND index_name = ?),
		     countIf(tenant_id = ? AND index_name = ?)
		 FROM open_splunk.events`,
		targetTenant,
		targetIndex,
		targetTenant,
		targetIndex,
		foreignTenant,
		targetIndex,
		targetTenant,
		neighborIndex,
	).Scan(
		&targetRows,
		&targetPartitions,
		&foreignRows,
		&neighborRows,
	); err != nil {
		t.Fatalf("query coordinator deletion scope: %v", err)
	}
	if targetRows != wantTargetRows ||
		targetPartitions != wantTargetPartitions ||
		foreignRows != wantForeignTenantRows ||
		neighborRows != wantNeighborRows {
		t.Fatalf(
			"deletion scope = target %d/%d partitions, foreign tenant %d, neighbor %d; want %d/%d/%d/%d",
			targetRows,
			targetPartitions,
			foreignRows,
			neighborRows,
			wantTargetRows,
			wantTargetPartitions,
			wantForeignTenantRows,
			wantNeighborRows,
		)
	}
}

func assertCoordinatorTerminalControlState(
	t *testing.T,
	ctx context.Context,
	db *control.DB,
	operation control.IndexDeletionOperation,
	completion control.IndexDataDeletionCompletion,
) {
	t.Helper()

	if _, err := db.GetIndex(ctx, operation.IndexID); !errors.Is(
		err,
		control.ErrNotFound,
	) {
		t.Fatalf("GetIndex(terminal) error = %v, want ErrNotFound", err)
	}
	if _, err := db.GetIndexByName(
		ctx,
		operation.IndexName,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"GetIndexByName(terminal) error = %v, want ErrNotFound",
			err,
		)
	}
	if _, err := db.GetIndexDeletionOperation(
		ctx,
		operation.ID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"GetIndexDeletionOperation(terminal) error = %v, want ErrNotFound",
			err,
		)
	}
	if _, err := db.GetIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"GetIndexDeletionMutationAttempt(terminal) error = %v, want ErrNotFound",
			err,
		)
	}
	if _, err := db.NextIndexDeletionOperation(ctx); !errors.Is(
		err,
		control.ErrNotFound,
	) {
		t.Fatalf(
			"NextIndexDeletionOperation(terminal) error = %v, want ErrNotFound",
			err,
		)
	}
	if _, err := db.CreateIndex(ctx, control.IndexDefinition{
		Name:             operation.IndexName,
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf(
			"CreateIndex(reserved terminal name) error = %v, want ErrAlreadyExists",
			err,
		)
	}

	var (
		state             string
		version           uint64
		tombstoneName     string
		tombstoneVersion  uint64
		tombstoneUnixTime int64
	)
	if err := db.SQLDB().QueryRowContext(
		ctx,
		`SELECT
		     indexes.state,
		     indexes.version,
		     index_deletion_tombstones.name,
		     index_deletion_tombstones.deleted_version,
		     index_deletion_tombstones.deleted_at_unix_micro
		 FROM indexes
		 JOIN index_deletion_tombstones
		   ON index_deletion_tombstones.index_id = indexes.index_id
		 WHERE indexes.index_id = ?`,
		operation.IndexID,
	).Scan(
		&state,
		&version,
		&tombstoneName,
		&tombstoneVersion,
		&tombstoneUnixTime,
	); err != nil {
		t.Fatalf("read terminal retained index and tombstone: %v", err)
	}
	if state != string(control.IndexStateDeleting) ||
		version != operation.DeletingVersion ||
		tombstoneName != operation.IndexName ||
		tombstoneVersion != operation.DeletingVersion ||
		tombstoneUnixTime != completion.CompletedAt.UnixMicro() {
		t.Fatalf(
			"terminal row = state %q version %d tombstone %q/%d/%d; operation=%#v completion=%#v",
			state,
			version,
			tombstoneName,
			tombstoneVersion,
			tombstoneUnixTime,
			operation,
			completion,
		)
	}
}

type coordinatorIntegrationProcess struct {
	control   *control.DB
	sequencer *visibility.SQLiteSequencer
	store     *clickhouse.Store
	closeOnce sync.Once
	closeErr  error
}

func openCoordinatorIntegrationProcess(
	t *testing.T,
	ctx context.Context,
	controlPath string,
	storeConfig clickhouse.Config,
	wrapSequencer func(visibility.Sequencer) visibility.Sequencer,
) *coordinatorIntegrationProcess {
	t.Helper()

	controlDB, err := control.Open(ctx, controlPath)
	if err != nil {
		t.Fatalf("open coordinator control plane: %v", err)
	}
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		_ = controlDB.Close()
		t.Fatalf("open coordinator visibility sequencer: %v", err)
	}
	var storeSequencer visibility.Sequencer = sequencer
	if wrapSequencer != nil {
		storeSequencer = wrapSequencer(sequencer)
	}
	store, err := clickhouse.Open(
		storeConfig,
		clickhouse.RetentionProviderFunc(
			func(context.Context, string, string) (time.Duration, error) {
				return 20 * 365 * 24 * time.Hour, nil
			},
		),
		storeSequencer,
	)
	if err != nil {
		_ = sequencer.Close()
		_ = controlDB.Close()
		t.Fatalf("open coordinator ClickHouse store: %v", err)
	}
	process := &coordinatorIntegrationProcess{
		control:   controlDB,
		sequencer: sequencer,
		store:     store,
	}
	t.Cleanup(func() {
		if closeErr := process.close(); closeErr != nil {
			t.Errorf("close coordinator integration process: %v", closeErr)
		}
	})
	return process
}

func (process *coordinatorIntegrationProcess) close() error {
	if process == nil {
		return nil
	}
	process.closeOnce.Do(func() {
		process.closeErr = errors.Join(
			process.store.Close(),
			process.sequencer.Close(),
			process.control.Close(),
		)
	})
	return process.closeErr
}

var (
	errCoordinatorOutboxHeld          = errors.New("coordinator test outbox replay held")
	errCoordinatorCompletionPrecommit = errors.New("coordinator test completion precommit failure")
)

type coordinatorSequencerGate struct {
	visibility.Sequencer
	hold           atomic.Bool
	failMark       atomic.Bool
	delegated      chan struct{}
	delegatedOnce  sync.Once
	blocked        chan struct{}
	blockedOnce    sync.Once
	markFailed     chan struct{}
	markFailedOnce sync.Once
}

func newCoordinatorSequencerGate() *coordinatorSequencerGate {
	return &coordinatorSequencerGate{
		delegated:  make(chan struct{}),
		blocked:    make(chan struct{}),
		markFailed: make(chan struct{}),
	}
}

func (gate *coordinatorSequencerGate) wrap(
	delegate visibility.Sequencer,
) visibility.Sequencer {
	gate.Sequencer = delegate
	return gate
}

func (gate *coordinatorSequencerGate) armAmbiguousFailure() {
	gate.hold.Store(true)
	gate.failMark.Store(true)
}

func (gate *coordinatorSequencerGate) AcquirePending(
	ctx context.Context,
	attemptID string,
) (visibility.Reservation, bool, error) {
	if gate.hold.Load() {
		gate.blockedOnce.Do(func() { close(gate.blocked) })
		return visibility.Reservation{}, false, errCoordinatorOutboxHeld
	}
	reservation, found, err := gate.Sequencer.AcquirePending(ctx, attemptID)
	gate.delegatedOnce.Do(func() { close(gate.delegated) })
	return reservation, found, err
}

func (gate *coordinatorSequencerGate) MarkSending(
	ctx context.Context,
	sequence uint64,
	attemptID string,
) error {
	if err := gate.Sequencer.MarkSending(ctx, sequence, attemptID); err != nil {
		return err
	}
	if gate.failMark.CompareAndSwap(true, false) {
		gate.markFailedOnce.Do(func() { close(gate.markFailed) })
		return io.ErrUnexpectedEOF
	}
	return nil
}

type coordinatorIntegrationTrace struct {
	mutex    sync.Mutex
	events   []string
	advances map[string][]clickhouse.IndexDataDeletionProgress
}

func newCoordinatorIntegrationTrace() *coordinatorIntegrationTrace {
	return &coordinatorIntegrationTrace{
		advances: make(map[string][]clickhouse.IndexDataDeletionProgress),
	}
}

func (trace *coordinatorIntegrationTrace) record(event string) {
	trace.mutex.Lock()
	trace.events = append(trace.events, event)
	trace.mutex.Unlock()
}

func (trace *coordinatorIntegrationTrace) recordAdvance(
	phase string,
	progress clickhouse.IndexDataDeletionProgress,
) {
	trace.mutex.Lock()
	trace.advances[phase] = append(trace.advances[phase], progress)
	trace.mutex.Unlock()
}

func (trace *coordinatorIntegrationTrace) snapshot() []string {
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	return append([]string(nil), trace.events...)
}

func (trace *coordinatorIntegrationTrace) advancesFor(
	phase string,
) []clickhouse.IndexDataDeletionProgress {
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	return append(
		[]clickhouse.IndexDataDeletionProgress(nil),
		trace.advances[phase]...,
	)
}

type recordingCoordinatorDeletionStore struct {
	delegate      *clickhouse.Store
	query         clickhousedriver.Conn
	replayGate    *coordinatorSequencerGate
	replayEventID string
	phase         string
	trace         *coordinatorIntegrationTrace
	replayChecked atomic.Bool
}

type countingCoordinatorDeletionStore struct {
	statusCalls atomic.Uint32
	freezeCalls atomic.Uint32
}

func (store *countingCoordinatorDeletionStore) IndexDataDeletionStatus(
	_ context.Context,
	_ clickhouse.IndexDataDeletionRequest,
) (clickhouse.IndexDataDeletionProgress, error) {
	store.statusCalls.Add(1)
	return clickhouse.IndexDataDeletionProgress{}, errors.New(
		"wrong-tenant coordinator unexpectedly polled ClickHouse",
	)
}

func (store *countingCoordinatorDeletionStore) WithWritesFrozen(
	_ context.Context,
	_ func(context.Context, clickhouse.FrozenWrites) error,
) error {
	store.freezeCalls.Add(1)
	return errors.New(
		"wrong-tenant coordinator unexpectedly froze ClickHouse writes",
	)
}

func (store *countingCoordinatorDeletionStore) calls() (uint32, uint32) {
	return store.statusCalls.Load(), store.freezeCalls.Load()
}

func (store *recordingCoordinatorDeletionStore) IndexDataDeletionStatus(
	ctx context.Context,
	request clickhouse.IndexDataDeletionRequest,
) (clickhouse.IndexDataDeletionProgress, error) {
	store.trace.record(store.phase + ".status")
	return store.delegate.IndexDataDeletionStatus(ctx, request)
}

func (store *recordingCoordinatorDeletionStore) WithWritesFrozen(
	ctx context.Context,
	callback func(context.Context, clickhouse.FrozenWrites) error,
) error {
	store.trace.record(store.phase + ".freeze.enter")
	err := store.delegate.WithWritesFrozen(
		ctx,
		func(ctx context.Context, frozen clickhouse.FrozenWrites) error {
			return callback(ctx, &recordingCoordinatorFrozenWrites{
				store:    store,
				delegate: frozen,
			})
		},
	)
	store.trace.record(store.phase + ".freeze.exit")
	return err
}

type recordingCoordinatorFrozenWrites struct {
	store    *recordingCoordinatorDeletionStore
	delegate clickhouse.FrozenWrites
}

func (frozen *recordingCoordinatorFrozenWrites) DrainPending(
	ctx context.Context,
) error {
	frozen.store.trace.record(frozen.store.phase + ".drain.start")
	if frozen.store.replayGate != nil {
		frozen.store.replayGate.hold.Store(false)
	}
	if err := frozen.delegate.DrainPending(ctx); err != nil {
		return err
	}
	frozen.store.trace.record(frozen.store.phase + ".drain.done")
	if frozen.store.replayEventID != "" &&
		frozen.store.replayChecked.CompareAndSwap(false, true) {
		var count uint64
		if err := frozen.store.query.QueryRow(
			ctx,
			"SELECT count() FROM open_splunk.events WHERE event_id = ?",
			frozen.store.replayEventID,
		).Scan(&count); err != nil {
			return fmt.Errorf("query replayed outbox row: %w", err)
		}
		if count != 1 {
			return fmt.Errorf(
				"replayed outbox rows before deletion advance = %d, want 1",
				count,
			)
		}
		frozen.store.trace.record(frozen.store.phase + ".replay.visible")
	}
	return nil
}

func (frozen *recordingCoordinatorFrozenWrites) IndexDataDeletionTarget(
	ctx context.Context,
) (clickhouse.IndexDataDeletionTarget, error) {
	frozen.store.trace.record(frozen.store.phase + ".target")
	return frozen.delegate.IndexDataDeletionTarget(ctx)
}

func (frozen *recordingCoordinatorFrozenWrites) AdvanceIndexDataDeletion(
	ctx context.Context,
	request clickhouse.IndexDataDeletionRequest,
) (clickhouse.IndexDataDeletionProgress, error) {
	frozen.store.trace.record(frozen.store.phase + ".advance")
	progress, err := frozen.delegate.AdvanceIndexDataDeletion(ctx, request)
	if err == nil {
		frozen.store.trace.recordAdvance(frozen.store.phase, progress)
	}
	return progress, err
}

type precommitErrorDeletionControl struct {
	*control.DB
	trace         *coordinatorIntegrationTrace
	completeCalls atomic.Uint32
	failed        chan struct{}
	failOnce      sync.Once
	auditMissing  chan struct{}
	auditOnce     sync.Once
}

func (controlPlane *precommitErrorDeletionControl) CompleteIndexDataDeletion(
	context.Context,
	control.IndexDeletionMutationAttempt,
) (control.IndexDataDeletionCompletion, error) {
	controlPlane.completeCalls.Add(1)
	controlPlane.trace.record("first.complete.precommit-error")
	controlPlane.failOnce.Do(func() { close(controlPlane.failed) })
	return control.IndexDataDeletionCompletion{},
		errCoordinatorCompletionPrecommit
}

func (controlPlane *precommitErrorDeletionControl) GetIndexDataDeletionCompletion(
	ctx context.Context,
	operationID string,
) (control.IndexDataDeletionCompletion, error) {
	completion, err := controlPlane.DB.GetIndexDataDeletionCompletion(
		ctx,
		operationID,
	)
	if errors.Is(err, control.ErrNotFound) {
		controlPlane.trace.record("first.completion.missing")
		controlPlane.auditOnce.Do(func() { close(controlPlane.auditMissing) })
	}
	return completion, err
}

func startCoordinatorIntegration(
	t *testing.T,
	controlPlane indexes.DeletionControl,
	store indexes.DeletionStore,
	config indexes.IndexDataDeletionCoordinatorConfig,
) *indexes.IndexDataDeletionCoordinator {
	t.Helper()

	coordinator, err := indexes.NewIndexDataDeletionCoordinator(
		controlPlane,
		store,
		config,
	)
	if err != nil {
		t.Fatalf("create index data deletion coordinator: %v", err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		if closeErr := coordinator.Close(closeContext); closeErr != nil {
			t.Errorf("close index data deletion coordinator: %v", closeErr)
		}
	})
	return coordinator
}

func closeCoordinatorIntegration(
	t *testing.T,
	coordinator *indexes.IndexDataDeletionCoordinator,
) {
	t.Helper()

	closeContext, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()
	if err := coordinator.Close(closeContext); err != nil {
		t.Fatalf("close index data deletion coordinator: %v", err)
	}
}

func waitCoordinatorIntegrationSignal(
	t *testing.T,
	ctx context.Context,
	signal <-chan struct{},
	description string,
) {
	t.Helper()

	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("wait for %s: %v", description, ctx.Err())
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

func assertCoordinatorIntegrationSubsequence(
	t *testing.T,
	events []string,
	want []string,
) {
	t.Helper()

	position := 0
	for _, event := range events {
		if position < len(want) && event == want[position] {
			position++
		}
	}
	if position != len(want) {
		t.Fatalf("calls %v do not contain ordered subsequence %v", events, want)
	}
}

func assertOneCoordinatorDeletionMutation(
	t *testing.T,
	ctx context.Context,
	store *clickhouse.Store,
	attempt control.IndexDeletionMutationAttempt,
) {
	t.Helper()

	progress, err := store.IndexDataDeletionStatus(
		ctx,
		clickhouse.IndexDataDeletionRequest{
			OperationID:     attempt.DeletionOperationID,
			CorrelationID:   attempt.CorrelationID,
			TenantID:        attempt.Target.TenantID,
			IndexName:       attempt.IndexName,
			Database:        attempt.Target.Database,
			Table:           attempt.Target.Table,
			TableUUID:       attempt.Target.TableUUID,
			ProtocolVersion: attempt.ProtocolVersion,
		},
	)
	if err != nil {
		t.Fatalf("inspect correlated deletion mutation: %v", err)
	}
	if progress.State != clickhouse.IndexDataDeletionReady ||
		progress.MatchingMutations != 1 ||
		progress.PendingMutations != 0 {
		t.Fatalf(
			"correlated deletion mutation = %#v, want ready with exactly one matching mutation",
			progress,
		)
	}
}

func sameIndexDataDeletionCompletion(
	left control.IndexDataDeletionCompletion,
	right control.IndexDataDeletionCompletion,
) bool {
	return left.DeletionOperationID == right.DeletionOperationID &&
		left.CorrelationID == right.CorrelationID &&
		left.IndexID == right.IndexID &&
		left.IndexName == right.IndexName &&
		left.ArchivedVersion == right.ArchivedVersion &&
		left.DeletedVersion == right.DeletedVersion &&
		left.Target == right.Target &&
		left.ProtocolVersion == right.ProtocolVersion &&
		left.OperationCreatedAt.Equal(right.OperationCreatedAt) &&
		left.MutationCreatedAt.Equal(right.MutationCreatedAt) &&
		left.CompletedAt.Equal(right.CompletedAt)
}

// commitThenErrorDeletionControl simulates the process losing the successful
// SQLite commit result. The coordinator must resolve the immutable completion
// instead of submitting or proving physical deletion a second time.
type commitThenErrorDeletionControl struct {
	*control.DB
	injected      atomic.Bool
	completeCalls atomic.Uint32
	trace         *coordinatorIntegrationTrace
	resolved      chan struct{}
	resolveOnce   sync.Once
	advanced      chan struct{}
	advanceOnce   sync.Once
}

func (controlPlane *commitThenErrorDeletionControl) NextIndexDeletionOperation(
	ctx context.Context,
) (control.IndexDeletionOperation, error) {
	operation, err := controlPlane.DB.NextIndexDeletionOperation(ctx)
	if errors.Is(err, control.ErrNotFound) &&
		controlPlane.injected.Load() {
		controlPlane.trace.record("second.next.empty")
		controlPlane.advanceOnce.Do(func() {
			close(controlPlane.advanced)
		})
	}
	return operation, err
}

func (controlPlane *commitThenErrorDeletionControl) CompleteIndexDataDeletion(
	ctx context.Context,
	expected control.IndexDeletionMutationAttempt,
) (control.IndexDataDeletionCompletion, error) {
	controlPlane.completeCalls.Add(1)
	completion, err := controlPlane.DB.CompleteIndexDataDeletion(ctx, expected)
	if err != nil {
		return control.IndexDataDeletionCompletion{}, err
	}
	if controlPlane.injected.CompareAndSwap(false, true) {
		controlPlane.trace.record("second.complete.commit-error")
		return control.IndexDataDeletionCompletion{}, io.ErrUnexpectedEOF
	}
	return completion, nil
}

func (controlPlane *commitThenErrorDeletionControl) GetIndexDataDeletionCompletion(
	ctx context.Context,
	deletionOperationID string,
) (control.IndexDataDeletionCompletion, error) {
	completion, err := controlPlane.DB.GetIndexDataDeletionCompletion(
		ctx,
		deletionOperationID,
	)
	if err == nil && controlPlane.injected.Load() {
		controlPlane.trace.record("second.completion.resolved")
		controlPlane.resolveOnce.Do(func() {
			close(controlPlane.resolved)
		})
	}
	return completion, err
}
