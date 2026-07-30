package server_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"github.com/Suhaibinator/open-splunk/migrations"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestClickHouseServicePrincipalLifecycle is opt-in because it starts the
// digest-pinned ClickHouse image. It proves the clean migration identity,
// validates both long-lived explicit grant surfaces and implicit-version pin,
// and executes Store ingestion plus ALTER DELETE through distinct pools.
func TestClickHouseServicePrincipalLifecycle(t *testing.T) {
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
	container, err := testsupport.StartClickHouseWithServicePrincipals(
		ctx,
		image,
	)
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
			t.Errorf("close service-principal ClickHouse fixture: %v", closeErr)
		}
	})

	migrationConnection := openPrincipalConnection(
		t,
		container,
		"default",
		container.MigrationUsername,
		container.MigrationPassword,
	)
	if err := migrationConnection.Ping(ctx); err != nil {
		t.Fatalf("ping migration principal: %v", err)
	}
	if err := server.ValidateClickHouseMigrationPrivileges(
		ctx,
		migrationConnection,
	); err != nil {
		t.Fatalf("validate migration principal before DDL: %v", err)
	}
	if err := server.ApplyClickHouseMigrations(
		ctx,
		migrationConnection,
		migrations.ClickHouse(),
	); err != nil {
		t.Fatalf("apply clean migrations as migration principal: %v", err)
	}
	if err := server.ValidateClickHouseRuntimePrivileges(
		ctx,
		migrationConnection,
	); !errors.Is(err, server.ErrClickHousePrivilegeProhibited) {
		t.Fatalf(
			"migration principal runtime validation error = %v, want prohibited privilege",
			err,
		)
	}
	expectClickHousePrivilegeDenied(
		t,
		ctx,
		"migration principal event read",
		func() error {
			var count uint64
			return migrationConnection.QueryRow(
				ctx,
				"SELECT count() FROM open_splunk.events",
			).Scan(&count)
		},
	)
	if err := migrationConnection.Close(); err != nil {
		t.Fatalf("close migration principal before runtime opens: %v", err)
	}

	runtimeConnection := openPrincipalConnection(
		t,
		container,
		container.Database,
		container.RuntimeUsername,
		container.RuntimePassword,
	)
	deletionConnection := openPrincipalConnection(
		t,
		container,
		container.Database,
		container.DeletionUsername,
		container.DeletionPassword,
	)
	storeOwnsConnections := false
	t.Cleanup(func() {
		if storeOwnsConnections {
			return
		}
		_ = deletionConnection.Close()
		_ = runtimeConnection.Close()
	})
	if err := runtimeConnection.Ping(ctx); err != nil {
		t.Fatalf("ping runtime principal: %v", err)
	}
	if err := deletionConnection.Ping(ctx); err != nil {
		t.Fatalf("ping deletion principal: %v", err)
	}
	if err := server.ValidateClickHouseRuntimePrivileges(
		ctx,
		runtimeConnection,
	); err != nil {
		t.Fatalf("validate runtime principal: %v", err)
	}
	if err := server.ValidateClickHouseDeletionWorkerPrivileges(
		ctx,
		deletionConnection,
	); err != nil {
		t.Fatalf("validate deletion principal: %v", err)
	}
	var implicitMovePartitionGrant uint8
	if err := deletionConnection.QueryRow(
		ctx,
		"CHECK GRANT ALTER MOVE PARTITION ON open_splunk.events",
	).Scan(&implicitMovePartitionGrant); err != nil {
		t.Fatalf("check deletion principal's implicit partition privilege: %v", err)
	}
	if implicitMovePartitionGrant != 0 {
		t.Fatalf(
			"implicit ALTER MOVE PARTITION grant = %d, want 0",
			implicitMovePartitionGrant,
		)
	}
	if err := container.ExecuteBootstrapSQLForTest(
		ctx,
		"GRANT ALTER UPDATE ON open_splunk.events TO open_splunk_runtime",
	); err != nil {
		t.Fatalf("grant adversarial runtime privilege: %v", err)
	}
	if err := server.ValidateClickHouseRuntimePrivileges(
		ctx,
		runtimeConnection,
	); !errors.Is(err, server.ErrClickHousePrivilegeProhibited) {
		t.Fatalf(
			"overprivileged runtime validation error = %v, want prohibited privilege",
			err,
		)
	}
	if err := container.ExecuteBootstrapSQLForTest(
		ctx,
		"REVOKE ALTER UPDATE ON open_splunk.events FROM open_splunk_runtime",
	); err != nil {
		t.Fatalf("revoke adversarial runtime privilege: %v", err)
	}
	if err := server.ValidateClickHouseRuntimePrivileges(
		ctx,
		runtimeConnection,
	); err != nil {
		t.Fatalf("validate runtime principal after adversarial revoke: %v", err)
	}
	expectClickHousePrivilegeDenied(
		t,
		ctx,
		"runtime mutation",
		func() error {
			return runtimeConnection.Exec(
				ctx,
				"ALTER TABLE open_splunk.events DELETE WHERE 0",
			)
		},
	)
	expectClickHousePrivilegeDenied(
		t,
		ctx,
		"runtime schema alteration",
		func() error {
			return runtimeConnection.Exec(
				ctx,
				"ALTER TABLE open_splunk.events ADD COLUMN forbidden_runtime UInt8",
			)
		},
	)
	expectClickHousePrivilegeDenied(
		t,
		ctx,
		"deletion ingestion",
		func() error {
			return deletionConnection.Exec(
				ctx,
				"INSERT INTO open_splunk.events (event_id) VALUES ('forbidden')",
			)
		},
	)
	expectClickHousePrivilegeDenied(
		t,
		ctx,
		"deletion schema alteration",
		func() error {
			return deletionConnection.Exec(
				ctx,
				"ALTER TABLE open_splunk.events ADD COLUMN forbidden_deletion UInt8",
			)
		},
	)

	controlDB, err := control.Open(
		ctx,
		filepath.Join(t.TempDir(), "control.sqlite"),
	)
	if err != nil {
		t.Fatalf("open service-principal control plane: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := controlDB.Close(); closeErr != nil {
			t.Errorf("close service-principal control plane: %v", closeErr)
		}
	})
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatalf("open service-principal visibility sequencer: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := sequencer.Close(); closeErr != nil {
			t.Errorf("close service-principal visibility sequencer: %v", closeErr)
		}
	})
	store, err := internalclickhouse.NewStoreWithDeletionConnection(
		runtimeConnection,
		deletionConnection,
		internalclickhouse.RetentionProviderFunc(
			func(context.Context, string, string) (time.Duration, error) {
				return 24 * time.Hour, nil
			},
		),
		sequencer,
	)
	if err != nil {
		t.Fatalf("construct split-principal Store: %v", err)
	}
	storeOwnsConnections = true
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close split-principal Store: %v", closeErr)
		}
	})
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("ping split-principal Store: %v", err)
	}

	const (
		tenantID  = "principal-tenant"
		indexName = "principal-index"
		eventID   = "principal-event"
	)
	indexTime := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	batchID := "principal-batch"
	sourceDigest := sha256.Sum256([]byte("principal-source-batch"))
	stored := &ingest.StoredEvent{
		TenantID:    tenantID,
		CollectorID: "principal-collector",
		BatchID:     batchID,
		IndexTime:   indexTime,
		Event: &opensplunkv1.LogEvent{
			EventId:         eventID,
			IndexName:       indexName,
			EventTime:       timestamppb.New(indexTime.Add(-time.Second)),
			EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            "principal-host",
			Source:          "principal.log",
			Sourcetype:      "open_splunk:principal",
			Severity:        opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
			Raw:             []byte(`{"message":"principal lifecycle"}`),
			RawEncoding:     opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			Fields:          &opensplunkv1.TypedObject{},
		},
	}
	result, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           tenantID,
		CollectorID:        stored.CollectorID,
		BatchID:            batchID,
		BatchSequence:      1,
		OriginalEventCount: 1,
		SourceBatchSHA256:  sourceDigest,
		ReceivedAt:         indexTime,
		Events:             []*ingest.StoredEvent{stored},
	})
	if err != nil {
		t.Fatalf("ingest through runtime principal: %v", err)
	}
	if result.Accepted != 1 || result.Duplicate != 0 {
		t.Fatalf("runtime-principal Store result = %#v", result)
	}

	var target internalclickhouse.IndexDataDeletionTarget
	var progress internalclickhouse.IndexDataDeletionProgress
	for attempt := 0; attempt < 100; attempt++ {
		err = store.WithWritesFrozen(
			ctx,
			func(
				frozenContext context.Context,
				frozen internalclickhouse.FrozenWrites,
			) error {
				if drainErr := frozen.DrainPending(frozenContext); drainErr != nil {
					return drainErr
				}
				if target.TableUUID == "" {
					target, err = frozen.IndexDataDeletionTarget(frozenContext)
					if err != nil {
						return err
					}
				}
				progress, err = frozen.AdvanceIndexDataDeletion(
					frozenContext,
					internalclickhouse.IndexDataDeletionRequest{
						OperationID:     "idxdel_principal_lifecycle",
						CorrelationID:   "idxmut_principal_lifecycle",
						TenantID:        tenantID,
						IndexName:       indexName,
						Database:        target.Database,
						Table:           target.Table,
						TableUUID:       target.TableUUID,
						ProtocolVersion: 1,
					},
				)
				return err
			},
		)
		if err != nil {
			t.Fatalf("advance through deletion principal: %v", err)
		}
		if progress.State ==
			internalclickhouse.IndexDataDeletionPhysicallyEmpty {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if progress.State != internalclickhouse.IndexDataDeletionPhysicallyEmpty {
		t.Fatalf("deletion progress = %#v, want physically empty", progress)
	}
	var remaining uint64
	if err := runtimeConnection.QueryRow(
		ctx,
		`SELECT count()
		 FROM open_splunk.events
		 WHERE tenant_id = ? AND index_name = ?`,
		tenantID,
		indexName,
	).Scan(&remaining); err != nil {
		t.Fatalf("verify deletion through runtime read principal: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining principal-lifecycle rows = %d, want 0", remaining)
	}
}

func openPrincipalConnection(
	t *testing.T,
	container *testsupport.ClickHouseContainer,
	database string,
	username string,
	password string,
) clickhousedriver.Conn {
	t.Helper()
	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		MaxOpenConns: 8,
		MaxIdleConns: 1,
	})
	if err != nil {
		t.Fatalf("open ClickHouse principal %q: %v", username, err)
	}
	return connection
}

func expectClickHousePrivilegeDenied(
	t *testing.T,
	_ context.Context,
	operation string,
	call func() error,
) {
	t.Helper()
	err := call()
	if err == nil {
		t.Fatalf("%s unexpectedly succeeded", operation)
	}
	message := strings.ToLower(err.Error())
	if !strings.Contains(message, "not enough privileges") &&
		!strings.Contains(message, "necessary to have grant") {
		t.Fatalf("%s error = %v, want privilege denial", operation, err)
	}
	t.Logf("%s denied as expected: %s", operation, fmt.Sprint(err))
}
