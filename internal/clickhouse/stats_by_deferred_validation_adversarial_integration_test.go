package clickhouse

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestStatsByDeferredValidationAdversarialAgainstClickHouse attacks the
// whole-input stats BY guard with result-eliminating downstream operators.
// String multivalues are a supported grouping domain, while objects and
// multivalues containing containers must fail before any public row escapes.
func TestStatsByDeferredValidationAdversarialAgainstClickHouse(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 12, 18, 0, 0, 0, time.UTC)
	newEvent := func(
		id, source string,
		fields ...*opensplunk.TypedObjectField,
	) *ingest.StoredEvent {
		event := testStoredEvent(id, "stats-by-deferred", indexTime)
		event.Event.Source = source
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}

	events := []*ingest.StoredEvent{
		newEvent("valid-scalar", "valid-scalar",
			typedField("grouping", typedString("scalar")),
		),
		newEvent("valid-list", "valid-list",
			typedField("grouping", typedList(
				typedString("a"),
				typedString("b"),
				typedString("b"),
			)),
		),
		newEvent("valid-empty-list", "valid-empty-list",
			typedField("grouping", typedList()),
		),
		newEvent("valid-null-members", "valid-null-members",
			typedField("grouping", typedList(
				typedNull(),
				typedString("present"),
				typedNull(),
			)),
		),
		newEvent("valid-whole-null", "valid-whole-null",
			typedField("grouping", typedNull()),
		),
		newEvent("valid-missing", "valid-missing"),
		newEvent("invalid-object", "invalid-object",
			typedField("grouping", typedObject(
				typedField("member", typedString("sdet-secret-object-value")),
			)),
			typedField("chronological_measure", typedString("valid-measure")),
		),
		newEvent("invalid-nested-list", "invalid-nested-list",
			typedField("grouping", typedList(typedObject(
				typedField("member", typedString("sdet-secret-nested-value")),
			))),
		),
		newEvent("invalid-mixed-list", "invalid-mixed-list",
			typedField("grouping", typedList(
				typedString("ordinary"),
				typedObject(typedField("member", typedString("sdet-secret-mixed-value"))),
			)),
		),
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"stats-by-deferred",
		"stats-by-deferred-batch",
		731,
		events...,
	)

	readGroups := func(source string) map[string]uint64 {
		t.Helper()
		compiled := compile(source)
		if !compiled.HasValidExecutionSeal() {
			t.Fatalf("compiled stats BY query is not sealed: %q", source)
		}
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute valid stats BY %q: %v\nSQL: %s", source, queryErr, compiled.SQL)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close valid stats BY %q: %v", source, closeErr)
			}
		}()
		groups := make(map[string]uint64)
		for rows.Next() {
			var group string
			var count uint64
			if scanErr := rows.Scan(&group, &count); scanErr != nil {
				t.Fatalf("scan valid stats BY %q: %v", source, scanErr)
			}
			groups[group] = count
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate valid stats BY %q: %v", source, rowsErr)
		}
		return groups
	}

	for _, test := range []struct {
		name   string
		source string
		want   map[string]uint64
	}{
		{
			name:   "scalar",
			source: `index=stats-by-deferred source="valid-scalar" | stats count BY grouping`,
			want:   map[string]uint64{"scalar": 1},
		},
		{
			name:   "Array String preserves duplicate split values",
			source: `index=stats-by-deferred source="valid-list" | stats count BY grouping`,
			want:   map[string]uint64{"a": 1, "b": 2},
		},
		{
			name:   "Array String dedup_splitvals",
			source: `index=stats-by-deferred source="valid-list" | stats count BY grouping dedup_splitvals=true`,
			want:   map[string]uint64{"a": 1, "b": 1},
		},
		{
			name:   "empty Array String has no groups",
			source: `index=stats-by-deferred source="valid-empty-list" | stats count BY grouping`,
			want:   map[string]uint64{},
		},
		{
			name:   "null members are omitted",
			source: `index=stats-by-deferred source="valid-null-members" | stats count BY grouping`,
			want:   map[string]uint64{"present": 1},
		},
		{
			name:   "whole-cell null has no groups",
			source: `index=stats-by-deferred source="valid-whole-null" | stats count BY grouping`,
			want:   map[string]uint64{},
		},
		{
			name:   "missing field has no groups",
			source: `index=stats-by-deferred source="valid-missing" | stats count BY grouping`,
			want:   map[string]uint64{},
		},
	} {
		t.Run("valid/"+test.name, func(t *testing.T) {
			if got := readGroups(test.source); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("stats BY groups = %#v, want %#v", got, test.want)
			}
		})
	}

	downstream := []struct {
		name   string
		suffix string
	}{
		{name: "direct"},
		{name: "search eliminates every group", suffix: ` | search count>100 | head 1`},
		{name: "where eliminates every group", suffix: ` | where count>100 | head 1`},
		{name: "fields drops grouping then eliminates", suffix: ` | fields count | search count>100 | head 1`},
		{name: "table drops grouping then eliminates", suffix: ` | table count | search count>100 | head 1`},
		{name: "second stats consumes an empty first result", suffix: ` | stats sum(count) AS total | search total>100`},
	}
	invalidSources := []struct {
		name   string
		source string
		secret string
	}{
		{name: "object", source: "invalid-object", secret: "sdet-secret-object-value"},
		{name: "nested object member", source: "invalid-nested-list", secret: "sdet-secret-nested-value"},
		{name: "mixed scalar and object members", source: "invalid-mixed-list", secret: "sdet-secret-mixed-value"},
	}

	assertRejected := func(t *testing.T, source, secret string) {
		t.Helper()
		compiled := compile(source)
		if !compiled.HasValidExecutionSeal() {
			t.Fatalf("compiled hostile stats BY query is not sealed: %q", source)
		}
		rowsSeen := 0
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr == nil {
			for rows.Next() {
				rowsSeen++
			}
			queryErr = rows.Err()
			if closeErr := rows.Close(); queryErr == nil && closeErr != nil {
				queryErr = closeErr
			}
		}
		if rowsSeen != 0 {
			t.Fatalf("unsupported stats BY published %d rows before failing", rowsSeen)
		}
		var exception *clickhousedriver.Exception
		if !errors.As(queryErr, &exception) || exception.Code != 395 {
			t.Fatalf("unsupported stats BY error = %v, want ClickHouse code 395", queryErr)
		}
		if !strings.Contains(exception.Message, UnsupportedStatsByValueMarker) ||
			strings.Contains(exception.Message, UnsupportedStatsMeasureValueMarker) {
			t.Fatalf(
				"unsupported stats BY marker = %q, want only %q",
				exception.Message,
				UnsupportedStatsByValueMarker,
			)
		}
		if strings.Contains(exception.Message, secret) {
			t.Fatalf("unsupported stats BY exception leaked event value %q: %q", secret, exception.Message)
		}
	}

	for _, invalid := range invalidSources {
		for _, consumer := range downstream {
			t.Run("unsupported/"+invalid.name+"/"+consumer.name, func(t *testing.T) {
				assertRejected(
					t,
					`index=stats-by-deferred source="`+invalid.source+`" | stats count BY grouping`+consumer.suffix,
					invalid.secret,
				)
			})
		}
	}

	// This query composes two distinct chronological validation kinds. The
	// eventstats input is valid, so the later unsupported BY value must retain
	// its own marker instead of being mislabeled by the ordinary measure guard.
	t.Run("unsupported/multiple validation kinds retain stats BY marker", func(t *testing.T) {
		assertRejected(
			t,
			`index=stats-by-deferred source="invalid-object"`+
				` | eventstats latest(chronological_measure) AS latest_measure`+
				` | stats count BY grouping | search count>100 | head 1`,
			"sdet-secret-object-value",
		)
	})
}
