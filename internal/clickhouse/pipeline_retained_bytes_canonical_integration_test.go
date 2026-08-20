package clickhouse

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

func TestPipelinePinnedClickHouseRetainedTupleHasCanonicalBytes(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close retained-byte ClickHouse: %v", closeErr)
		}
	})

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("close retained-byte connection: %v", closeErr)
		}
	})

	state := compileState{
		publicOrder: []string{"absent", "empty", "slash", "unicode", "wide", "decimal"},
		visible: map[string]fieldState{
			"absent": {
				valueSQL:                     "CAST([], 'Array(String)')",
				optionalMultivaluePresentSQL: "toUInt8(0)",
				kind:                         fieldKindStringArray,
			},
			"empty": {
				valueSQL:                     "CAST([], 'Array(String)')",
				optionalMultivaluePresentSQL: "toUInt8(1)",
				kind:                         fieldKindStringArray,
			},
			"slash":   {valueSQL: "'ignored'", kind: fieldKindString},
			"unicode": {valueSQL: "'界'", kind: fieldKindString},
			"wide": {
				valueSQL: "toUInt64(9007199254740993)",
				kind:     fieldKindNumber,
			},
			"decimal": {
				valueSQL: "toDecimal64('123.40', 2)",
				kind:     fieldKindNumber,
			},
		},
	}
	retainedTuple := publicRetainedTupleSQL(state, "slash", "'a/b'")
	query := "SELECT toJSONString(" + retainedTuple + ")" +
		materializedValidationSettingsSQL
	var encoded string
	if err := connection.QueryRow(ctx, query).Scan(&encoded); err != nil {
		t.Fatalf("execute retained-byte canonicalization: %v\nSQL: %s", err, query)
	}
	const want = `{"absent":null,"empty":[],"slash":"a/b","unicode":"界","wide":9007199254740993,"decimal":123.4}`
	if encoded != want {
		t.Fatalf("canonical retained tuple = %q, want %q", encoded, want)
	}
}
