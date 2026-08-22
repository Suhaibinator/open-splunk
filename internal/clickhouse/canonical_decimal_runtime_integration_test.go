package clickhouse

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestCanonicalDecimalPayloadTextSQLAgainstClickHouse pins the lexical
// arbitrary-exponent implementation against the repository ClickHouse image
// and the public Go canonicalizer. It is table-free so failures isolate the SQL
// primitive from Store and migration behavior.
func TestCanonicalDecimalPayloadTextSQLAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	probeContext, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer probeCancel()
	if output, err := exec.CommandContext(
		probeContext,
		"docker", "info", "--format", "{{.ServerVersion}}",
	).CombinedOutput(); err != nil {
		t.Skipf("docker daemon is unavailable: %v (%s)", err, output)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatalf("start pinned ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close canonical decimal ClickHouse fixture: %v", closeErr)
		}
	})
	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("open canonical decimal ClickHouse connection: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("close canonical decimal ClickHouse connection: %v", closeErr)
		}
	})
	queryContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithSettings(clickhousedriver.Settings{
			"short_circuit_function_evaluation": "enable",
		}),
	)
	if err := connection.Ping(queryContext); err != nil {
		t.Fatalf("ping canonical decimal ClickHouse fixture: %v", err)
	}

	hugePositive := "10e" + strings.Repeat("9", MaximumExactNumericBinTextBytes-len("10e"))
	hugeNegative := "10e-" + strings.Repeat("9", MaximumExactNumericBinTextBytes-len("10e-"))
	hugePositiveDigits := strings.TrimPrefix(hugePositive, "10e")
	hugeNegativeDigits := strings.TrimPrefix(hugeNegative, "10e-")
	tests := []struct {
		source string
		want   string
	}{
		{source: "0", want: "0"},
		{source: "-0.000e99", want: "0"},
		{source: "+001.2300", want: "1.23"},
		{source: "123e-2", want: "1.23"},
		{source: "0.000001", want: "0.000001"},
		{source: "0.0000001", want: "1e-7"},
		{source: "100000000000000000000", want: "100000000000000000000"},
		{source: "1000000000000000000000", want: "1e21"},
		{source: "-12.3400e+7", want: "-123400000"},
		{source: "10e20", want: "1e21"},
		{source: "10e-1", want: "1"},
		{source: "100e-2", want: "1"},
		{source: "0.00100e2", want: "0.1"},
		{source: "123e999", want: "1.23e1001"},
		{source: "123E-999", want: "1.23e-997"},
		{source: "1e000000000000000000007", want: "10000000"},
		{source: hugePositive, want: "1e1" + strings.Repeat("0", len(hugePositiveDigits))},
		{source: hugeNegative, want: "1e-" + hugeNegativeDigits[:len(hugeNegativeDigits)-1] + "8"},
	}
	query := "SELECT " + canonicalDecimalPayloadTextSQL("CAST(? AS String)")
	for _, test := range tests {
		var got string
		if queryErr := connection.QueryRow(queryContext, query, test.source).Scan(&got); queryErr != nil {
			t.Fatalf("execute canonical decimal for input length %d: %v", len(test.source), queryErr)
		}
		if got != test.want {
			t.Fatalf("canonical decimal for %q = %q, want %q", test.source, got, test.want)
		}
	}

	for _, source := range []string{
		"",
		".1",
		"1.",
		"1e",
		strings.Repeat("1", MaximumExactNumericBinTextBytes+1),
	} {
		var got string
		if queryErr := connection.QueryRow(queryContext, query, source).Scan(&got); queryErr != nil {
			t.Fatalf("execute rejected canonical decimal for input length %d: %v", len(source), queryErr)
		}
		if got != "" {
			t.Fatalf("rejected canonical decimal for input length %d = %q, want empty", len(source), got)
		}
	}

	// Native multivalue consumers invoke the canonicalizer inside a Dynamic
	// lambda even for non-Decimal members. Pin that type-inference context: it
	// must remain valid on the release engine, not merely as a String scalar.
	dynamicValues := "arrayConcat([CAST('text' AS Dynamic)], [CAST(1 AS Dynamic)], " +
		"[CAST(1.0 AS Dynamic)], [CAST(true AS Dynamic)], " + nullNativeMVSQL() + ")"
	dynamicQuery := "SELECT arrayMap(member -> " +
		nativeMVCanonicalTextSQL("member") + ", " + dynamicValues + ")"
	var dynamicText []string
	if queryErr := connection.QueryRow(queryContext, dynamicQuery).Scan(&dynamicText); queryErr != nil {
		t.Fatalf("execute canonical text in Dynamic lambda: %v", queryErr)
	}
	wantDynamic := []string{"text", "1", "1", "true", "null"}
	if !slices.Equal(dynamicText, wantDynamic) {
		t.Fatalf("canonical mixed Dynamic values = %q, want %q", dynamicText, wantDynamic)
	}
}
