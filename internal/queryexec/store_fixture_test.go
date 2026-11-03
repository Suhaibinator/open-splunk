package queryexec

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func startQueryExecutorStoreFixture(
	t *testing.T,
	ctx context.Context,
) (clickhousedriver.Conn, *Executor, *clickhouse.Store) {
	t.Helper()
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse image: %s", container.Image)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupCtx); closeErr != nil {
			t.Errorf("close ClickHouse query executor fixture container: %v", closeErr)
		}
	})
	queryIntegrationMigrate(t, ctx, container.Name, container.Password)

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
	t.Cleanup(func() { _ = queryConnection.Close() })
	if err := queryConnection.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	executor, err := New(queryConnection, Config{ReadAdmission: indexread.UnfencedAdmission{}})
	if err != nil {
		t.Fatal(err)
	}

	storeConnection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "visibility.sqlite"))
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := sequencer.Close(); closeErr != nil {
			t.Errorf("close query executor fixture visibility sequencer: %v", closeErr)
		}
	})
	store, err := clickhouse.NewStore(
		storeConnection,
		clickhouse.RetentionProviderFunc(func(context.Context, string, string) (time.Duration, error) {
			return 24 * time.Hour, nil
		}),
		sequencer,
	)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close query executor fixture store: %v", closeErr)
		}
	})

	return queryConnection, executor, store
}
