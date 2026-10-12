package testsupport

import (
	"context"
	"os"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func TestPinnedClickHouseFixtureStartsFinalDaemon(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	container, err := StartClickHouse(ctx, DefaultClickHouseImage)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := container.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr:         []string{container.Address},
		Auth:         clickhousedriver.Auth{Database: container.Database, Username: container.Username, Password: container.Password},
		DialTimeout:  5 * time.Second,
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	var value uint8
	if err := connection.QueryRow(ctx, "SELECT toUInt8(1)").Scan(&value); err != nil || value != 1 {
		t.Fatalf("query final daemon: value=%d error=%v", value, err)
	}
}
