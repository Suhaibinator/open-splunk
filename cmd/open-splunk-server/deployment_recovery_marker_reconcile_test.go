package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	clickhouserow "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

const (
	markerReconcileRecoverySetID       = "0123456789abcdef0123456789abcdef"
	markerReconcileBackupOperationUUID = "66666666-6666-4666-8666-666666666666"
)

func TestDeploymentRecoveryMarkerReconcileFlagParserRequiresExactSingleInputs(t *testing.T) {
	fixture := newDeploymentRecoveryCommandFixture(t)
	valid := []string{
		"-recovery-set-id", markerReconcileRecoverySetID,
		"-confirm-recovery-set-id", markerReconcileRecoverySetID,
		"-backup-operation-uuid", markerReconcileBackupOperationUUID,
		"-confirm-backup-operation-uuid", markerReconcileBackupOperationUUID,
		"-address", "clickhouse:9440",
		"-password-file", fixture.passwordFile,
		"-ca-cert", fixture.caCertFile,
		"-server-name", "clickhouse",
	}
	options, err := parseDeploymentRecoveryMarkerReconcileOptions(valid)
	if err != nil {
		t.Fatal(err)
	}
	want := deploymentRecoveryMarkerReconcileOptions{
		RecoverySetID:                markerReconcileRecoverySetID,
		ConfirmedRecoverySetID:       markerReconcileRecoverySetID,
		BackupOperationUUID:          markerReconcileBackupOperationUUID,
		ConfirmedBackupOperationUUID: markerReconcileBackupOperationUUID,
		Address:                      "clickhouse:9440",
		PasswordFile:                 fixture.passwordFile,
		CACertFile:                   fixture.caCertFile,
		ServerName:                   "clickhouse",
	}
	if !reflect.DeepEqual(options, want) {
		t.Fatalf("parsed marker reconcile options = %#v, want %#v", options, want)
	}

	for index := 0; index < len(valid); index += 2 {
		name := valid[index]
		t.Run("duplicate "+name, func(t *testing.T) {
			arguments := append(append([]string(nil), valid...), name, valid[index+1])
			if _, err := parseDeploymentRecoveryMarkerReconcileOptions(arguments); err == nil ||
				!strings.Contains(err.Error(), "exactly once") {
				t.Fatalf("duplicate %s error = %v", name, err)
			}
		})
	}
	for name, arguments := range map[string][]string{
		"missing required flag": valid[:len(valid)-2],
		"positional":            append(append([]string(nil), valid...), "unexpected"),
		"mismatched recovery ID": func() []string {
			value := append([]string(nil), valid...)
			value[3] = strings.Repeat("f", 32)
			return value
		}(),
		"mismatched operation UUID": func() []string {
			value := append([]string(nil), valid...)
			value[7] = "77777777-7777-4777-8777-777777777777"
			return value
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseDeploymentRecoveryMarkerReconcileOptions(arguments); err == nil {
				t.Fatal("invalid marker reconcile arguments succeeded")
			}
		})
	}
}

func TestDeploymentRecoveryMarkerReconcileRejectsIncompleteDependenciesBeforeLock(t *testing.T) {
	mutations := map[string]func(*deploymentRecoveryMarkerReconcileDependencies){
		"migration files":       func(value *deploymentRecoveryMarkerReconcileDependencies) { value.migrationFiles = nil },
		"host lock":             func(value *deploymentRecoveryMarkerReconcileDependencies) { value.acquireHostLock = nil },
		"open":                  func(value *deploymentRecoveryMarkerReconcileDependencies) { value.open = nil },
		"backup privileges":     func(value *deploymentRecoveryMarkerReconcileDependencies) { value.validateBackupPrivileges = nil },
		"backup source":         func(value *deploymentRecoveryMarkerReconcileDependencies) { value.validateBackupSource = nil },
		"require marker":        func(value *deploymentRecoveryMarkerReconcileDependencies) { value.requireMarker = nil },
		"clear marker":          func(value *deploymentRecoveryMarkerReconcileDependencies) { value.clearMarker = nil },
		"require marker absent": func(value *deploymentRecoveryMarkerReconcileDependencies) { value.requireMarkerAbsent = nil },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			dependencies := defaultDeploymentRecoveryMarkerReconcileDependencies()
			lockCalls := 0
			dependencies.acquireHostLock = func() (*serverLock, error) {
				lockCalls++
				return nil, errors.New("unexpected lock")
			}
			mutate(&dependencies)
			err := runDeploymentRecoveryMarkerReconcile(
				t.Context(),
				validDeploymentRecoveryMarkerReconcileOptions(),
				dependencies,
			)
			if err == nil || !strings.Contains(err.Error(), "incomplete dependencies") {
				t.Fatalf("incomplete dependency error = %v", err)
			}
			if lockCalls != 0 {
				t.Fatalf("incomplete dependency lock calls = %d", lockCalls)
			}
		})
	}
}

func TestDeploymentRecoveryMarkerReconcileExactOperationOrdering(t *testing.T) {
	options, password := newDeploymentRecoveryMarkerReconcileTestOptions(t)
	lockPath := filepath.Join(t.TempDir(), "host.server.lock")
	operations := make([]string, 0, 10)
	session := &deploymentRecoveryMarkerReconcileTestSession{
		rowCount:      1,
		slot:          1,
		recoverySetID: markerReconcileRecoverySetID,
		operationUUID: markerReconcileBackupOperationUUID,
		operations:    &operations,
	}
	assertLockHeld := func(where string) {
		t.Helper()
		contender, err := acquireHostServerLockAt(lockPath)
		if contender != nil {
			_ = contender.Close()
		}
		if !errors.Is(err, errServerAlreadyRunning) {
			t.Fatalf("%s: server lock contender error = %v", where, err)
		}
	}
	session.onClose = func() { assertLockHeld("close session") }
	dependencies := defaultDeploymentRecoveryMarkerReconcileDependencies()
	dependencies.acquireHostLock = func() (*serverLock, error) {
		operations = append(operations, "lock")
		return acquireHostServerLockAt(lockPath)
	}
	dependencies.open = func(connectionOptions *clickhousedriver.Options) (deploymentRecoverySession, error) {
		operations = append(operations, "open")
		assertLockHeld("open session")
		if connectionOptions.Auth.Username != deploymentRecoveryBackupUsername ||
			connectionOptions.Auth.Password != password ||
			connectionOptions.Auth.Database != "default" {
			t.Fatalf("backup principal options = %#v", connectionOptions.Auth)
		}
		return session, nil
	}
	dependencies.validateBackupPrivileges = func(
		_ context.Context,
		connection server.ClickHousePrivilegeConnection,
	) error {
		operations = append(operations, "privileges")
		assertLockHeld("validate privileges")
		if connection != session {
			t.Fatal("privilege validation received a different session")
		}
		return nil
	}
	dependencies.validateBackupSource = func(
		_ context.Context,
		connection server.ClickHouseRecoveryValidationConnection,
		migrationFiles fs.FS,
	) (server.ClickHouseRecoveryDatabaseInspection, error) {
		operations = append(operations, "source")
		assertLockHeld("validate source")
		if connection != session || migrationFiles == nil {
			t.Fatal("canonical source validation dependencies are incomplete")
		}
		return server.ClickHouseRecoveryDatabaseInspection{
			DatabaseName: deploymentRecoveryDatabase,
		}, nil
	}

	if err := runDeploymentRecoveryMarkerReconcile(t.Context(), options, dependencies); err != nil {
		t.Fatal(err)
	}
	wantOperations := []string{
		"lock", "open", "ping", "privileges", "source",
		"marker-read", "marker-read", "marker-truncate", "marker-read", "close",
	}
	if !reflect.DeepEqual(operations, wantOperations) {
		t.Fatalf("operations = %v, want %v", operations, wantOperations)
	}
	if session.rowCount != 0 || session.closeCalls != 1 {
		t.Fatalf("completed session = rows:%d closes:%d", session.rowCount, session.closeCalls)
	}
	assertDeploymentRecoveryMarkerReconcileLockReleased(t, lockPath)
}

func TestDeploymentRecoveryMarkerReconcileRequiresStoppedServerBeforeCredentialsOrNetwork(
	t *testing.T,
) {
	lockPath := filepath.Join(t.TempDir(), "host.server.lock")
	held, err := acquireHostServerLockAt(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	options := validDeploymentRecoveryMarkerReconcileOptions()
	options.PasswordFile = filepath.Join(t.TempDir(), "must-not-be-read.password")
	options.CACertFile = filepath.Join(t.TempDir(), "must-not-be-read.pem")
	dependencies := defaultDeploymentRecoveryMarkerReconcileDependencies()
	dependencies.acquireHostLock = func() (*serverLock, error) {
		return acquireHostServerLockAt(lockPath)
	}
	dependencies.open = func(*clickhousedriver.Options) (deploymentRecoverySession, error) {
		t.Fatal("busy-lock reconciliation opened a network session")
		return nil, nil
	}

	err = runDeploymentRecoveryMarkerReconcile(t.Context(), options, dependencies)
	if !errors.Is(err, errServerAlreadyRunning) || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("busy-lock error = %v", err)
	}
}

func TestDeploymentRecoveryMarkerReconcileValidatesExactConfirmationsBeforeLock(t *testing.T) {
	valid := validDeploymentRecoveryMarkerReconcileOptions()
	tests := map[string]deploymentRecoveryMarkerReconcileOptions{
		"invalid recovery set ID": func() deploymentRecoveryMarkerReconcileOptions {
			value := valid
			value.RecoverySetID = strings.ToUpper(value.RecoverySetID)
			value.ConfirmedRecoverySetID = value.RecoverySetID
			return value
		}(),
		"different recovery set confirmation": func() deploymentRecoveryMarkerReconcileOptions {
			value := valid
			value.ConfirmedRecoverySetID = strings.Repeat("f", 32)
			return value
		}(),
		"invalid backup operation UUID": func() deploymentRecoveryMarkerReconcileOptions {
			value := valid
			value.BackupOperationUUID = strings.ToUpper(
				"abcdefab-cdef-4abc-8def-abcdefabcdef",
			)
			value.ConfirmedBackupOperationUUID = value.BackupOperationUUID
			return value
		}(),
		"nil backup operation UUID": func() deploymentRecoveryMarkerReconcileOptions {
			value := valid
			value.BackupOperationUUID = "00000000-0000-0000-0000-000000000000"
			value.ConfirmedBackupOperationUUID = value.BackupOperationUUID
			return value
		}(),
		"different backup operation confirmation": func() deploymentRecoveryMarkerReconcileOptions {
			value := valid
			value.ConfirmedBackupOperationUUID = "77777777-7777-4777-8777-777777777777"
			return value
		}(),
	}
	for name, options := range tests {
		t.Run(name, func(t *testing.T) {
			lockCalls := 0
			dependencies := defaultDeploymentRecoveryMarkerReconcileDependencies()
			dependencies.acquireHostLock = func() (*serverLock, error) {
				lockCalls++
				return nil, errors.New("unexpected lock call")
			}
			if err := runDeploymentRecoveryMarkerReconcile(
				t.Context(),
				options,
				dependencies,
			); err == nil {
				t.Fatal("invalid confirmation succeeded")
			}
			if lockCalls != 0 {
				t.Fatalf("invalid confirmation lock calls = %d", lockCalls)
			}
		})
	}
}

func TestDeploymentRecoveryMarkerReconcileAbsentIsIdempotent(t *testing.T) {
	options, _ := newDeploymentRecoveryMarkerReconcileTestOptions(t)
	lockPath := filepath.Join(t.TempDir(), "host.server.lock")
	operations := make([]string, 0, 8)
	session := &deploymentRecoveryMarkerReconcileTestSession{operations: &operations}
	dependencies := deploymentRecoveryMarkerReconcileTestDependencies(
		t,
		lockPath,
		session,
	)

	if err := runDeploymentRecoveryMarkerReconcile(t.Context(), options, dependencies); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(operations, ","), "marker-truncate") {
		t.Fatalf("absent marker operations = %v", operations)
	}
	wantTail := []string{"marker-read", "marker-read", "close"}
	if len(operations) < len(wantTail) ||
		!reflect.DeepEqual(operations[len(operations)-len(wantTail):], wantTail) {
		t.Fatalf("absent marker operations = %v", operations)
	}
	assertDeploymentRecoveryMarkerReconcileLockReleased(t, lockPath)
}

func TestDeploymentRecoveryMarkerReconcileFailsClosedBeforeClear(t *testing.T) {
	tests := map[string]struct {
		mutate func(*deploymentRecoveryMarkerReconcileDependencies, error)
	}{
		"privilege validation": {
			mutate: func(dependencies *deploymentRecoveryMarkerReconcileDependencies, failure error) {
				dependencies.validateBackupPrivileges = func(
					context.Context,
					server.ClickHousePrivilegeConnection,
				) error {
					return failure
				}
			},
		},
		"source validation": {
			mutate: func(dependencies *deploymentRecoveryMarkerReconcileDependencies, failure error) {
				dependencies.validateBackupSource = func(
					context.Context,
					server.ClickHouseRecoveryValidationConnection,
					fs.FS,
				) (server.ClickHouseRecoveryDatabaseInspection, error) {
					return server.ClickHouseRecoveryDatabaseInspection{}, failure
				}
			},
		},
		"synchronous clear": {
			mutate: func(dependencies *deploymentRecoveryMarkerReconcileDependencies, failure error) {
				dependencies.clearMarker = func(
					context.Context,
					server.ClickHouseRecoveryArchiveMarkerConnection,
					string,
					string,
					string,
				) error {
					return failure
				}
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			options, _ := newDeploymentRecoveryMarkerReconcileTestOptions(t)
			lockPath := filepath.Join(t.TempDir(), "host.server.lock")
			session := &deploymentRecoveryMarkerReconcileTestSession{
				rowCount:      1,
				slot:          1,
				recoverySetID: markerReconcileRecoverySetID,
				operationUUID: markerReconcileBackupOperationUUID,
			}
			dependencies := deploymentRecoveryMarkerReconcileTestDependencies(
				t,
				lockPath,
				session,
			)
			failure := errors.New("injected " + name + " failure")
			test.mutate(&dependencies, failure)

			err := runDeploymentRecoveryMarkerReconcile(t.Context(), options, dependencies)
			if !errors.Is(err, failure) {
				t.Fatalf("%s error = %v", name, err)
			}
			if session.rowCount != 1 || session.closeCalls != 1 {
				t.Fatalf("%s state = rows:%d closes:%d", name, session.rowCount, session.closeCalls)
			}
			assertDeploymentRecoveryMarkerReconcileLockReleased(t, lockPath)
		})
	}
}

func TestDeploymentRecoveryMarkerReconcileDoesNotTreatReadFailureAsAbsence(t *testing.T) {
	options, _ := newDeploymentRecoveryMarkerReconcileTestOptions(t)
	lockPath := filepath.Join(t.TempDir(), "host.server.lock")
	readFailure := errors.New("read marker transport failure")
	session := &deploymentRecoveryMarkerReconcileTestSession{
		readErr: func(reads int) error {
			if reads == 1 {
				return readFailure
			}
			return nil
		},
	}
	dependencies := deploymentRecoveryMarkerReconcileTestDependencies(
		t,
		lockPath,
		session,
	)

	err := runDeploymentRecoveryMarkerReconcile(t.Context(), options, dependencies)
	if !errors.Is(err, readFailure) {
		t.Fatalf("marker read failure = %v", err)
	}
	if session.readCalls != 1 || session.closeCalls != 1 {
		t.Fatalf("marker read failure calls = reads:%d closes:%d", session.readCalls, session.closeCalls)
	}
	assertDeploymentRecoveryMarkerReconcileLockReleased(t, lockPath)
}

func TestDeploymentRecoveryMarkerReconcileRejectsUnknownMarkerWithoutMutation(t *testing.T) {
	tests := map[string]struct {
		rowCount      uint64
		slot          uint8
		recoverySetID string
		operationUUID string
	}{
		"wrong recovery set": {
			rowCount: 1, slot: 1,
			recoverySetID: strings.Repeat("f", 32),
			operationUUID: markerReconcileBackupOperationUUID,
		},
		"wrong operation": {
			rowCount: 1, slot: 1,
			recoverySetID: markerReconcileRecoverySetID,
			operationUUID: "77777777-7777-4777-8777-777777777777",
		},
		"duplicate rows": {
			rowCount: 2, slot: 1,
			recoverySetID: markerReconcileRecoverySetID,
			operationUUID: markerReconcileBackupOperationUUID,
		},
	}
	for name, marker := range tests {
		t.Run(name, func(t *testing.T) {
			options, _ := newDeploymentRecoveryMarkerReconcileTestOptions(t)
			lockPath := filepath.Join(t.TempDir(), "host.server.lock")
			operations := make([]string, 0, 8)
			session := &deploymentRecoveryMarkerReconcileTestSession{
				rowCount:      marker.rowCount,
				slot:          marker.slot,
				recoverySetID: marker.recoverySetID,
				operationUUID: marker.operationUUID,
				operations:    &operations,
			}
			dependencies := deploymentRecoveryMarkerReconcileTestDependencies(
				t,
				lockPath,
				session,
			)

			err := runDeploymentRecoveryMarkerReconcile(t.Context(), options, dependencies)
			if !errors.Is(err, server.ErrClickHouseRecoveryArchiveMarkerMismatch) {
				t.Fatalf("unknown marker error = %v", err)
			}
			if session.rowCount != marker.rowCount ||
				session.recoverySetID != marker.recoverySetID ||
				session.operationUUID != marker.operationUUID {
				t.Fatalf("unknown marker mutated to %#v", session)
			}
			if strings.Contains(strings.Join(operations, ","), "marker-truncate") {
				t.Fatalf("unknown marker operations = %v", operations)
			}
			if session.closeCalls != 1 {
				t.Fatalf("unknown marker close calls = %d", session.closeCalls)
			}
			assertDeploymentRecoveryMarkerReconcileLockReleased(t, lockPath)
		})
	}
}

func TestDeploymentRecoveryMarkerReconcileCancellationLeavesExactMarker(t *testing.T) {
	t.Run("before lock", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		lockCalls := 0
		dependencies := defaultDeploymentRecoveryMarkerReconcileDependencies()
		dependencies.acquireHostLock = func() (*serverLock, error) {
			lockCalls++
			return nil, errors.New("unexpected lock call")
		}
		err := runDeploymentRecoveryMarkerReconcile(
			ctx,
			validDeploymentRecoveryMarkerReconcileOptions(),
			dependencies,
		)
		if !errors.Is(err, context.Canceled) || lockCalls != 0 {
			t.Fatalf("pre-lock cancellation = error:%v locks:%d", err, lockCalls)
		}
	})

	t.Run("after exact read", func(t *testing.T) {
		options, _ := newDeploymentRecoveryMarkerReconcileTestOptions(t)
		lockPath := filepath.Join(t.TempDir(), "host.server.lock")
		ctx, cancel := context.WithCancel(t.Context())
		operations := make([]string, 0, 8)
		session := &deploymentRecoveryMarkerReconcileTestSession{
			rowCount:      1,
			slot:          1,
			recoverySetID: markerReconcileRecoverySetID,
			operationUUID: markerReconcileBackupOperationUUID,
			operations:    &operations,
			onRead: func(reads int) {
				if reads == 1 {
					cancel()
				}
			},
		}
		dependencies := deploymentRecoveryMarkerReconcileTestDependencies(
			t,
			lockPath,
			session,
		)

		err := runDeploymentRecoveryMarkerReconcile(ctx, options, dependencies)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("post-read cancellation error = %v", err)
		}
		if session.rowCount != 1 || session.closeCalls != 1 ||
			strings.Contains(strings.Join(operations, ","), "marker-truncate") {
			t.Fatalf("post-read cancellation state = rows:%d closes:%d operations:%v", session.rowCount, session.closeCalls, operations)
		}
		assertDeploymentRecoveryMarkerReconcileLockReleased(t, lockPath)
	})
}

func TestDeploymentRecoveryMarkerReconcilePropagatesSessionCloseFailureAndReleasesLock(
	t *testing.T,
) {
	options, _ := newDeploymentRecoveryMarkerReconcileTestOptions(t)
	lockPath := filepath.Join(t.TempDir(), "host.server.lock")
	closeFailure := errors.New("close marker reconcile session")
	session := &deploymentRecoveryMarkerReconcileTestSession{
		operations: nil,
		closeErr:   closeFailure,
	}
	dependencies := deploymentRecoveryMarkerReconcileTestDependencies(
		t,
		lockPath,
		session,
	)

	err := runDeploymentRecoveryMarkerReconcile(t.Context(), options, dependencies)
	if !errors.Is(err, closeFailure) || session.closeCalls != 1 {
		t.Fatalf("session close failure = error:%v closes:%d", err, session.closeCalls)
	}
	assertDeploymentRecoveryMarkerReconcileLockReleased(t, lockPath)
}

func validDeploymentRecoveryMarkerReconcileOptions() deploymentRecoveryMarkerReconcileOptions {
	return deploymentRecoveryMarkerReconcileOptions{
		RecoverySetID:                markerReconcileRecoverySetID,
		ConfirmedRecoverySetID:       markerReconcileRecoverySetID,
		BackupOperationUUID:          markerReconcileBackupOperationUUID,
		ConfirmedBackupOperationUUID: markerReconcileBackupOperationUUID,
		Address:                      "clickhouse:9440",
		ServerName:                   "clickhouse",
	}
}

func newDeploymentRecoveryMarkerReconcileTestOptions(
	t *testing.T,
) (deploymentRecoveryMarkerReconcileOptions, string) {
	t.Helper()
	fixture := newDeploymentRecoveryCommandFixture(t)
	options := validDeploymentRecoveryMarkerReconcileOptions()
	options.PasswordFile = fixture.passwordFile
	options.CACertFile = fixture.caCertFile
	return options, fixture.password
}

func deploymentRecoveryMarkerReconcileTestDependencies(
	t *testing.T,
	lockPath string,
	session *deploymentRecoveryMarkerReconcileTestSession,
) deploymentRecoveryMarkerReconcileDependencies {
	t.Helper()
	dependencies := defaultDeploymentRecoveryMarkerReconcileDependencies()
	dependencies.acquireHostLock = func() (*serverLock, error) {
		return acquireHostServerLockAt(lockPath)
	}
	dependencies.open = func(*clickhousedriver.Options) (deploymentRecoverySession, error) {
		return session, nil
	}
	dependencies.validateBackupPrivileges = func(
		context.Context,
		server.ClickHousePrivilegeConnection,
	) error {
		return nil
	}
	dependencies.validateBackupSource = func(
		context.Context,
		server.ClickHouseRecoveryValidationConnection,
		fs.FS,
	) (server.ClickHouseRecoveryDatabaseInspection, error) {
		return server.ClickHouseRecoveryDatabaseInspection{
			DatabaseName: deploymentRecoveryDatabase,
		}, nil
	}
	return dependencies
}

func assertDeploymentRecoveryMarkerReconcileLockReleased(t *testing.T, lockPath string) {
	t.Helper()
	lock, err := acquireHostServerLockAt(lockPath)
	if err != nil {
		t.Fatalf("reacquire released marker reconcile lock: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("close reacquired marker reconcile lock: %v", err)
	}
}

type deploymentRecoveryMarkerReconcileTestSession struct {
	clickhousedriver.Conn
	rowCount      uint64
	slot          uint8
	recoverySetID string
	operationUUID string
	operations    *[]string
	readCalls     int
	closeCalls    int
	closeErr      error
	readErr       func(int) error
	onRead        func(int)
	onClose       func()
}

func (session *deploymentRecoveryMarkerReconcileTestSession) record(operation string) {
	if session.operations != nil {
		*session.operations = append(*session.operations, operation)
	}
}

func (session *deploymentRecoveryMarkerReconcileTestSession) Ping(ctx context.Context) error {
	session.record("ping")
	return ctx.Err()
}

func (session *deploymentRecoveryMarkerReconcileTestSession) QueryRow(
	_ context.Context,
	query string,
	_ ...any,
) clickhouserow.Row {
	if !strings.Contains(query, "FROM open_splunk.recovery_archive_markers") {
		return deploymentRecoveryMarkerReconcileTestRow{
			err: fmt.Errorf("unexpected marker reconciliation query %q", query),
		}
	}
	session.record("marker-read")
	session.readCalls++
	if session.readErr != nil {
		if err := session.readErr(session.readCalls); err != nil {
			return deploymentRecoveryMarkerReconcileTestRow{err: err}
		}
	}
	if session.onRead != nil {
		session.onRead(session.readCalls)
	}
	return deploymentRecoveryMarkerReconcileTestRow{values: []any{
		session.rowCount,
		session.slot,
		session.recoverySetID,
		session.operationUUID,
	}}
}

func (session *deploymentRecoveryMarkerReconcileTestSession) Exec(
	ctx context.Context,
	query string,
	args ...any,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	const truncate = "TRUNCATE TABLE open_splunk.recovery_archive_markers SYNC"
	if query != truncate || len(args) != 0 {
		return fmt.Errorf("unexpected marker reconciliation exec %q with %#v", query, args)
	}
	session.record("marker-truncate")
	session.rowCount = 0
	session.slot = 0
	session.recoverySetID = ""
	session.operationUUID = ""
	return nil
}

func (session *deploymentRecoveryMarkerReconcileTestSession) Close() error {
	session.record("close")
	session.closeCalls++
	if session.onClose != nil {
		session.onClose()
	}
	return session.closeErr
}

type deploymentRecoveryMarkerReconcileTestRow struct {
	values []any
	err    error
}

func (row deploymentRecoveryMarkerReconcileTestRow) Err() error { return row.err }

func (row deploymentRecoveryMarkerReconcileTestRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return fmt.Errorf("marker reconcile scan destinations = %d, want %d", len(destinations), len(row.values))
	}
	for index, value := range row.values {
		switch destination := destinations[index].(type) {
		case *uint64:
			converted, ok := value.(uint64)
			if !ok {
				return fmt.Errorf("marker reconcile row value %d is %T, want uint64", index, value)
			}
			*destination = converted
		case *uint8:
			converted, ok := value.(uint8)
			if !ok {
				return fmt.Errorf("marker reconcile row value %d is %T, want uint8", index, value)
			}
			*destination = converted
		case *string:
			converted, ok := value.(string)
			if !ok {
				return fmt.Errorf("marker reconcile row value %d is %T, want string", index, value)
			}
			*destination = converted
		default:
			return fmt.Errorf("unsupported marker reconcile row destination %T", destinations[index])
		}
	}
	return nil
}

func (deploymentRecoveryMarkerReconcileTestRow) ScanStruct(any) error {
	return errors.New("marker reconcile test row does not support ScanStruct")
}
