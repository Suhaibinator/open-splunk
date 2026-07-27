package clickhouse

import (
	"context"
	"strconv"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func testNowAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	event := testStoredEvent("now-scalar", "now", indexTime)
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"now",
		"now-batch",
		114,
		event,
	)
	// The shared integration planner deliberately keeps search admission one
	// second before the storage cutoff so this test detects accidental
	// coupling between now() and IndexTimeCutoff.
	want := indexTime.Add(9 * time.Second).Unix()

	composed := compile(
		`index=now event_id="now-scalar"` +
			` | eval first=now(), second=now(), rendered=tostring(now())` +
			` | where first=second AND now()=first` +
			` | table first,second,rendered`,
	)
	var first, second int64
	var rendered string
	if err := connection.QueryRow(
		queryContext,
		composed.SQL,
		composed.Args...,
	).Scan(&first, &second, &rendered); err != nil {
		t.Fatalf(
			"execute composed now: %v\nSQL: %s\nargs: %#v",
			err,
			composed.SQL,
			composed.Args,
		)
	}
	if first != want || second != want || rendered != strconv.FormatInt(want, 10) {
		t.Fatalf(
			"composed now = %d/%d/%q, want %d/%d/%q",
			first,
			second,
			rendered,
			want,
			want,
			strconv.FormatInt(want, 10),
		)
	}

	transformed := compile(
		`index=now event_id="now-scalar"` +
			` | eval first=now()` +
			` | table first` +
			` | stats count BY first` +
			` | eval second=now()` +
			` | table first,second,count`,
	)
	var transformedFirst, transformedSecond int64
	var count uint64
	if err := connection.QueryRow(
		queryContext,
		transformed.SQL,
		transformed.Args...,
	).Scan(&transformedFirst, &transformedSecond, &count); err != nil {
		t.Fatalf(
			"execute transformed now: %v\nSQL: %s\nargs: %#v",
			err,
			transformed.SQL,
			transformed.Args,
		)
	}
	if transformedFirst != want || transformedSecond != want || count != 1 {
		t.Fatalf(
			"transformed now = %d/%d/%d, want %d/%d/1",
			transformedFirst,
			transformedSecond,
			count,
			want,
			want,
		)
	}
}
