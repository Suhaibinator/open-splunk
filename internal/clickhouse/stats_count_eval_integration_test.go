package clickhouse

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

func testStatsCountEvalAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	const (
		requestBatchID     = "stats-count-eval-request-batch"
		requestCollectorID = "stats-count-eval-request-collector"
		requestSource      = "sanitized-api-requests"
	)
	newRequest := func(
		id string,
		host string,
		raw string,
		fields ...*opensplunkv1.TypedObjectField,
	) *ingest.StoredEvent {
		t.Helper()
		event := compilerIntegrationEvent(
			id,
			host,
			raw,
			indexTime,
			fields...,
		)
		event.CollectorID = requestCollectorID
		event.BatchID = requestBatchID
		event.Event.Source = requestSource
		return event
	}
	requestEvents := []*ingest.StoredEvent{
		newRequest(
			"request-auth-get-ok",
			"auth",
			`{"method":"GET","path":"/v1/session","status":200}`,
			typedField("method", typedString("GET")),
			typedField("status", typedSint(200)),
		),
		newRequest(
			"request-auth-post-created",
			"auth",
			`{"method":"POST","path":"/v1/session","status":201}`,
			typedField("method", typedString("POST")),
			typedField("status", typedSint(201)),
		),
		newRequest(
			"request-auth-get-error",
			"auth",
			`{"method":"GET","path":"/v1/session","status":503}`,
			typedField("method", typedString("GET")),
			typedField("status", typedSint(503)),
		),
		newRequest(
			"request-auth-post-status-missing",
			"auth",
			`{"method":"POST","path":"/v1/token"}`,
			typedField("method", typedString("POST")),
		),
		newRequest(
			"request-auth-get-status-null",
			"auth",
			`{"method":"GET","path":"/v1/token","status":null}`,
			typedField("method", typedString("GET")),
			typedField("status", typedNull()),
		),
		newRequest(
			"request-auth-method-null-literal-status",
			"auth",
			`{"method":null,"path":"/v1/token","status":"5*"}`,
			typedField("method", typedNull()),
			typedField("status", typedString("5*")),
		),
		newRequest(
			"request-billing-post-ok",
			"billing",
			`{"method":"POST","path":"/v1/invoices","status":200}`,
			typedField("method", typedString("POST")),
			typedField("status", typedSint(200)),
		),
		newRequest(
			"request-billing-get-ok",
			"billing",
			`{"method":"GET","path":"/v1/invoices","status":204}`,
			typedField("method", typedString("GET")),
			typedField("status", typedSint(204)),
		),
		newRequest(
			"request-billing-post-error",
			"billing",
			`{"method":"POST","path":"/v1/invoices","status":500}`,
			typedField("method", typedString("POST")),
			typedField("status", typedSint(500)),
		),
		newRequest(
			"request-billing-post-gateway-error",
			"billing",
			`{"method":"POST","path":"/v1/payments","status":502}`,
			typedField("method", typedString("POST")),
			typedField("status", typedSint(502)),
		),
		newRequest(
			"request-billing-get-client-error",
			"billing",
			`{"method":"GET","path":"/v1/payments","status":404}`,
			typedField("method", typedString("GET")),
			typedField("status", typedSint(404)),
		),
		newRequest(
			"request-billing-method-and-status-missing",
			"billing",
			`{"path":"/v1/payments","message":"request metadata unavailable"}`,
		),
	}
	storeResult, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:          "tenant",
		CollectorID:       requestCollectorID,
		BatchID:           requestBatchID,
		BatchSequence:     1,
		SourceBatchSHA256: testSourceBatchDigest(requestBatchID),
		ReceivedAt:        indexTime,
		Events:            requestEvents,
	})
	if err != nil {
		t.Fatalf("store sanitized request fixtures: %v", err)
	}
	if storeResult.Accepted != uint32(len(requestEvents)) || storeResult.Duplicate != 0 {
		t.Fatalf(
			"store sanitized request fixtures result = %+v, want %d accepted",
			storeResult,
			len(requestEvents),
		)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture conditional-count visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
	}
	base := `index=compiler source="null-predicate"`

	t.Run("sanitized requests preserve grouped conditional counts", func(t *testing.T) {
		compiled := compile(
			`index=compiler source="` + requestSource + `"` +
				` | stats count AS total` +
				` count(eval(method="GET")) AS get_count` +
				` count(eval(method="POST")) AS post_count` +
				` count(eval(status>=500)) AS error_count` +
				` count(eval(isnull(method))) AS unknown_method_count` +
				` count(eval(isnull(status))) AS unknown_status_count` +
				` count(eval(isnotnull(status))) AS known_status_count` +
				` count(eval(status="5*")) AS literal_wildcard_status_count BY host` +
				` | sort 0 +host`,
		)
		wantFields := []string{
			"host",
			"total",
			"get_count",
			"post_count",
			"error_count",
			"unknown_method_count",
			"unknown_status_count",
			"known_status_count",
			"literal_wildcard_status_count",
		}
		if !slices.Equal(compiled.OutputFields, wantFields) {
			t.Fatalf(
				"sanitized request output fields = %#v, want %#v",
				compiled.OutputFields,
				wantFields,
			)
		}
		if scans := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); scans != 1 {
			t.Fatalf(
				"sanitized request conditional counts use %d event scans, want 1:\n%s",
				scans,
				compiled.SQL,
			)
		}
		if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
			t.Fatalf("sanitized request conditional counts expand rows:\n%s", compiled.SQL)
		}
		rows, queryErr := connection.Query(ctx, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf(
				"execute sanitized request conditional counts: %v\nSQL: %s\nargs: %#v",
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close sanitized request conditional-count rows: %v", closeErr)
			}
		}()
		type requestStats struct {
			service       string
			requests      uint64
			gets          uint64
			posts         uint64
			errors        uint64
			unknownMethod uint64
			unknownStatus uint64
			knownStatus   uint64
			literalStatus uint64
		}
		if columns := rows.Columns(); !slices.Equal(columns, wantFields) {
			t.Fatalf(
				"sanitized request result columns = %#v, want %#v",
				columns,
				wantFields,
			)
		}
		var results []requestStats
		for rows.Next() {
			var row requestStats
			if scanErr := rows.Scan(
				&row.service,
				&row.requests,
				&row.gets,
				&row.posts,
				&row.errors,
				&row.unknownMethod,
				&row.unknownStatus,
				&row.knownStatus,
				&row.literalStatus,
			); scanErr != nil {
				t.Fatalf("scan sanitized request conditional-count row: %v", scanErr)
			}
			results = append(results, row)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate sanitized request conditional-count rows: %v", rowsErr)
		}
		if len(results) != 2 ||
			results[0] != (requestStats{
				service: "auth", requests: 6, gets: 3, posts: 2, errors: 1,
				unknownMethod: 1, unknownStatus: 2, knownStatus: 4, literalStatus: 1,
			}) ||
			results[1] != (requestStats{
				service: "billing", requests: 6, gets: 2, posts: 3, errors: 2,
				unknownMethod: 1, unknownStatus: 1, knownStatus: 5,
			}) {
			t.Fatalf("sanitized request conditional counts = %#v", results)
		}

		actions := explainCompiledQuery(
			t,
			ctx,
			connection,
			"EXPLAIN actions=1 ",
			compiled,
		)
		if strings.Contains(actions, "ArrayJoin") {
			t.Fatalf("sanitized request conditional counts expand event rows:\n%s", actions)
		}
	})

	t.Run("true only including null and missing", func(t *testing.T) {
		compiled := compile(
			base + ` | stats count AS rows` +
				` count(eval(true=true)) AS true_count` +
				` count(eval(true=false)) AS false_count` +
				` count(eval(null=true)) AS null_count` +
				` count(eval(absent=1)) AS missing_count`,
		)
		var rows, trueCount, falseCount, nullCount, missingCount uint64
		if queryErr := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(
			&rows,
			&trueCount,
			&falseCount,
			&nullCount,
			&missingCount,
		); queryErr != nil {
			t.Fatalf(
				"execute conditional truth table: %v\nSQL: %s\nargs: %#v",
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		if rows != 9 || trueCount != 9 || falseCount != 0 ||
			nullCount != 0 || missingCount != 0 {
			t.Fatalf(
				"conditional truth table = (%d,%d,%d,%d,%d), want (9,9,0,0,0)",
				rows,
				trueCount,
				falseCount,
				nullCount,
				missingCount,
			)
		}
	})

	t.Run("canonical and dynamic comparisons", func(t *testing.T) {
		compiled := compile(
			base + ` | stats` +
				` count(eval(event_id="null-zero")) AS canonical_match` +
				` count(eval(event_id="not-an-event")) AS canonical_miss` +
				` count(eval(probe=0)) AS dynamic_match` +
				` count(eval(probe=1)) AS dynamic_miss`,
		)
		var canonicalMatch, canonicalMiss, dynamicMatch, dynamicMiss uint64
		if queryErr := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(
			&canonicalMatch,
			&canonicalMiss,
			&dynamicMatch,
			&dynamicMiss,
		); queryErr != nil {
			t.Fatalf(
				"execute conditional comparisons: %v\nSQL: %s\nargs: %#v",
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		if canonicalMatch != 1 || canonicalMiss != 0 ||
			dynamicMatch != 1 || dynamicMiss != 0 {
			t.Fatalf(
				"conditional comparison counts = (%d,%d,%d,%d), want (1,0,1,0)",
				canonicalMatch,
				canonicalMiss,
				dynamicMatch,
				dynamicMiss,
			)
		}
	})

	t.Run("three valued Boolean composition", func(t *testing.T) {
		compiled := compile(
			base + ` | stats` +
				` count(eval(NOT isnull(probe))) AS nonnull` +
				` count(eval(isnull(probe) OR isnotnull(probe))) AS union_count` +
				` count(eval(isnull(probe) AND isnotnull(probe))) AS intersection_count` +
				` count(eval(NOT absent=1)) AS negated_missing`,
		)
		var nonnull, unionCount, intersectionCount, negatedMissing uint64
		if queryErr := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(
			&nonnull,
			&unionCount,
			&intersectionCount,
			&negatedMissing,
		); queryErr != nil {
			t.Fatalf(
				"execute conditional Boolean composition: %v\nSQL: %s\nargs: %#v",
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		if nonnull != 7 || unionCount != 9 ||
			intersectionCount != 0 || negatedMissing != 0 {
			t.Fatalf(
				"conditional Boolean counts = (%d,%d,%d,%d), want (7,9,0,0)",
				nonnull,
				unionCount,
				intersectionCount,
				negatedMissing,
			)
		}
	})

	t.Run("nested Boolean if and nullable result", func(t *testing.T) {
		compiled := compile(
			base + ` | stats` +
				` count(eval(if(isnull(probe), if(isnull(absent), true, false), false))) AS nested` +
				` count(eval(if(isnull(probe), null, true)=true)) AS nullable_branch` +
				` count(eval(if(absent=1, true, false))) AS null_condition`,
		)
		var nested, nullableBranch, nullCondition uint64
		if queryErr := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(
			&nested,
			&nullableBranch,
			&nullCondition,
		); queryErr != nil {
			t.Fatalf(
				"execute conditional nested if: %v\nSQL: %s\nargs: %#v",
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		if nested != 2 || nullableBranch != 7 || nullCondition != 0 {
			t.Fatalf(
				"conditional nested if counts = (%d,%d,%d), want (2,7,0)",
				nested,
				nullableBranch,
				nullCondition,
			)
		}
	})

	t.Run("grouped sibling measures retain every row", func(t *testing.T) {
		compiled := compile(
			base + ` | eval bucket=if(isnull(probe), "null", "value")` +
				` | stats count AS rows` +
				` count(eval(isnull(probe))) AS nulls` +
				` count(eval(isnotnull(probe))) AS present BY bucket` +
				` | sort bucket`,
		)
		rows, queryErr := connection.Query(ctx, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf(
				"execute grouped conditional counts: %v\nSQL: %s\nargs: %#v",
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close grouped conditional-count rows: %v", closeErr)
			}
		}()
		type result struct {
			bucket        string
			rows          uint64
			nulls         uint64
			presentValues uint64
		}
		var results []result
		for rows.Next() {
			var row result
			if scanErr := rows.Scan(
				&row.bucket,
				&row.rows,
				&row.nulls,
				&row.presentValues,
			); scanErr != nil {
				t.Fatalf("scan grouped conditional-count row: %v", scanErr)
			}
			results = append(results, row)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate grouped conditional-count rows: %v", rowsErr)
		}
		if len(results) != 2 ||
			results[0] != (result{bucket: "null", rows: 2, nulls: 2}) ||
			results[1] != (result{bucket: "value", rows: 7, presentValues: 7}) {
			t.Fatalf("grouped conditional counts = %#v", results)
		}
	})

	t.Run("projected and calculated fields", func(t *testing.T) {
		projected := compile(
			base + ` | fields event_id | stats count AS rows` +
				` count(eval(isnull(probe))) AS missing` +
				` count(eval(isnotnull(probe))) AS present`,
		)
		var rows, missing, present uint64
		if queryErr := connection.QueryRow(ctx, projected.SQL, projected.Args...).Scan(
			&rows,
			&missing,
			&present,
		); queryErr != nil {
			t.Fatalf(
				"execute projected conditional count: %v\nSQL: %s\nargs: %#v",
				queryErr,
				projected.SQL,
				projected.Args,
			)
		}
		if rows != 9 || missing != 9 || present != 0 {
			t.Fatalf(
				"projected conditional counts = (%d,%d,%d), want (9,9,0)",
				rows,
				missing,
				present,
			)
		}

		calculated := compile(
			base + ` | spath input=_raw output=selected path=value` +
				` | stats count AS rows count(eval(isnull(selected))) AS missing`,
		)
		if queryErr := connection.QueryRow(
			ctx,
			calculated.SQL,
			calculated.Args...,
		).Scan(&rows, &missing); queryErr != nil {
			t.Fatalf(
				"execute materialized conditional count: %v\nSQL: %s\nargs: %#v",
				queryErr,
				calculated.SQL,
				calculated.Args,
			)
		}
		if rows != 9 || missing != 9 {
			t.Fatalf(
				"materialized conditional counts = (%d,%d), want (9,9)",
				rows,
				missing,
			)
		}
	})

	t.Run("global empty input returns zero", func(t *testing.T) {
		compiled := compile(
			`index=compiler source="conditional-count-empty"` +
				` | stats count AS rows count(eval(isnull(probe))) AS matches`,
		)
		var rows, matches uint64
		if queryErr := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(
			&rows,
			&matches,
		); queryErr != nil {
			t.Fatalf(
				"execute empty conditional count: %v\nSQL: %s\nargs: %#v",
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		if rows != 0 || matches != 0 {
			t.Fatalf("empty conditional counts = (%d,%d), want (0,0)", rows, matches)
		}
	})

	t.Run("ordinary physical plan has no row expansion", func(t *testing.T) {
		compiled := compile(
			base + ` | stats count AS rows` +
				` count(eval(isnull(probe))) AS missing` +
				` count(eval(isnotnull(probe))) AS present`,
		)
		actions := explainCompiledQuery(
			t,
			ctx,
			connection,
			"EXPLAIN actions=1 ",
			compiled,
		)
		if strings.Contains(actions, "ArrayJoin") {
			t.Fatalf("ordinary conditional count expands event rows:\n%s", actions)
		}
	})
}
