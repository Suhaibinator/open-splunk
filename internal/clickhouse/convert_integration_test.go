package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func TestConvertScalarRecognizersAgainstClickHouse(t *testing.T) {
	ctx, connection, _ := convertProbeConnection(t)
	for _, test := range []struct{ input, want string }{
		{"0", "0"}, {"-0.5", "-0.5"}, {"1e2", "100"},
		{"000000000000000000021.5", "21.5"},
		{"18446744073709551615", "18446744073709551615"},
		{"9223372036854775808", "9223372036854775808"},
		{"-9223372036854775808", "-9223372036854775808"},
		{"1,234.5", "1234.5"}, {"9,007,199,254,740,993", "9007199254740993"}, {"18,446,744,073,709,551,615", "18446744073709551615"}, {"-9,007,199,254,740,993", "-9007199254740993"}, {"12,34", "null"},
		{"00:10:15", "615"}, {"2+03:04:05.25", "183845.25"},
		{"12:05.125", "725.125"}, {"01:60:00", "null"},
		{"2k", "2"}, {"1.5m", "1536"}, {"2g", "2097152"},
		{"0k", "null"}, {"-2m", "null"},
		{"1.2.3", "null"}, {"192.0.2.1", "null"},
		{"123abc", "null"}, {"212 sec", "null"},
		{" 123", "null"}, {"123 ", "null"}, {"NaN", "null"}, {"1e999", "null"},
	} {
		t.Run(test.input, func(t *testing.T) {
			sql, args := compileConvertNumericCandidate("CAST(input AS Dynamic)", "toUInt8(2)", "input", true)
			var got string
			if err := connection.QueryRow(ctx, "WITH CAST(? AS String) AS input SELECT ifNull(toJSONString("+sql+"), 'null')", append([]any{test.input}, args...)...).Scan(&got); err != nil {
				t.Fatalf("recognize %q: %v", test.input, err)
			}
			if got != test.want {
				t.Fatalf("recognize %q = %s, want %s", test.input, got, test.want)
			}
		})
	}
	for _, test := range []struct{ source, inputSQL, want string }{
		{`index=gradethis | convert auto(x)`, `SELECT '1' AS x, 1 AS ordinal UNION ALL SELECT 'bad' AS x, 2 AS ordinal`, `"1"`},
		{`index=gradethis | convert auto(x)`, `SELECT '1' AS x, 1 AS ordinal UNION ALL SELECT '2' AS x, 2 AS ordinal`, `1`},
		{`index=gradethis | convert auto(x)`, `SELECT '1' AS x, 1 AS ordinal UNION ALL SELECT CAST(NULL AS Nullable(String)) AS x, 2 AS ordinal`, `1`},
		{`index=gradethis | convert num(x)`, `SELECT 'bad' AS x, 1 AS ordinal`, `null`},
		{`index=gradethis | convert num(x)`, `SELECT ' 1 ' AS x, 1 AS ordinal`, `1`},
		{`index=gradethis | convert rmunit(x)`, `SELECT ' 212 sec ' AS x, 1 AS ordinal`, `212`},
		{`index=gradethis | convert rmunit(x)`, `SELECT '212 sec' AS x, 1 AS ordinal`, `212`},
		{`index=gradethis | convert timeformat="%F %T" ctime(x)`, `SELECT '0' AS x, 1 AS ordinal`, `"1970-01-01 00:00:00"`},
		{`index=gradethis | convert timeformat=" %Y-%m-%d" mktime(x)`, `SELECT ' 2026-07-01' AS x, 1 AS ordinal`, `1782864000`},
		{`index=gradethis | convert timeformat="%F %T" mktime(x)`, `SELECT '2026-07-01 00:00:00' AS x, 1 AS ordinal`, `1782864000`},
	} {
		t.Run(test.source+test.want, func(t *testing.T) {
			logical := buildPlan(t, test.source)
			_, state, _, err := compileScan("open_splunk", "events", logical.Operators[0].(*plan.Scan), logical.SearchStart, logical.SearchTimezone)
			if err != nil {
				t.Fatal(err)
			}
			state.visible["x"] = fieldState{valueSQL: quoteIdentifier("x"), kind: fieldKindString, existsSQL: "1"}
			relation, _, args, err := compileConvert(newScanRelation(test.inputSQL, logical.Operators[len(logical.Operators)-1].SourceRange()), nil, logical.Operators[len(logical.Operators)-1].(*plan.Convert), state, 1)
			if err != nil {
				t.Fatal(err)
			}
			sql := applyMaterializedValidationSettings("SELECT ifNull(toJSONString(x), 'null') FROM (" + relation.sql + ") WHERE ordinal=1")
			var got string
			if err := connection.QueryRow(ctx, sql, args...).Scan(&got); err != nil {
				t.Fatalf("execute convert: %v\nSQL: %s\nargs: %#v", err, sql, args)
			}
			if got != test.want {
				t.Fatalf("convert = %s, want %s", got, test.want)
			}
		})
	}
}

func convertProbeConnection(t *testing.T) (context.Context, clickhousedriver.Conn, *testsupport.ClickHouseContainer) {
	t.Helper()
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	container, err := testsupport.StartClickHouse(ctx, os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"))
	if err != nil {
		t.Fatalf("start pinned ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancelCleanup := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancelCleanup()
		if err := container.Close(cleanup); err != nil {
			t.Errorf("close convert probe ClickHouse: %v", err)
		}
	})
	connection, err := clickhousedriver.Open(&clickhousedriver.Options{Protocol: clickhousedriver.Native, Addr: []string{container.Address},
		Auth:        clickhousedriver.Auth{Database: container.Database, Username: container.Username, Password: container.Password},
		DialTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return ctx, connection, container
}

func testConvertAgainstClickHouse(ctx context.Context, t *testing.T, store *Store, connection clickhousedriver.Conn, indexTime time.Time) {
	t.Helper()
	valid := testStoredEvent("convert-valid", "convert", indexTime)
	valid.Event.Fields = typedObjectValue(typedField("epoch", typedString("0")), typedField("delay", typedString("00:10:15")),
		typedField("bytes", typedString("1,234.5")), typedField("duration", typedString("212 sec")), typedField("mixed", typedString("1")),
		typedField("object", typedObject(typedField("child", typedString("kept")), typedField("literal.dot", typedString("dot")), typedField("literal%dot", typedString("percent")), typedField("number", typedDouble(1)))), typedField("nothing", typedNull()))
	invalid := testStoredEvent("convert-invalid", "convert", indexTime)
	invalid.Event.Fields = typedObjectValue(typedField("epoch", typedString("bad")), typedField("delay", typedString("bad")),
		typedField("bytes", typedString("bad")), typedField("duration", typedString("bad")), typedField("mixed", typedString("bad")),
		typedField("object", typedObject(typedField("child", typedString("kept")), typedField("literal.dot", typedString("dot")), typedField("literal%dot", typedString("percent")), typedField("number", typedDouble(1)))))
	valid.Event.Fields.Fields = append(valid.Event.Fields.Fields,
		typedField("decimal", typedDecimal("123456789012345678901234567890.125")),
		typedField("untouched", typedString("keep raw")),
		typedField("nested", typedObject(typedField("value", typedString("3")))),
		typedField("literal.dot", typedString("2")),
		typedField("oversized", typedString(strings.Repeat("0", MaximumConvertInputBytes)+"1")))
	for _, event := range []*ingest.StoredEvent{valid, invalid} {
		event.Event.Fields.Fields = append(event.Event.Fields.Fields,
			typedField("section_assessment_id", typedSint(17)), typedField("section_assessment_submission_id", typedString("s1")),
			typedField("upload_batch_id", typedString("u1")), typedField("source_file_id", typedString("f1")),
			typedField("workflow_type", typedString("w1")), typedField("event_name", typedString("e1")),
			typedField("timestamp_text", typedString("2026-07-01 00:00:00")), typedField("decimal_epoch", typedDecimal("0.125")))
	}
	for index := range 70 {
		valid.Event.Fields.Fields = append(valid.Event.Fields.Fields, typedField(fmt.Sprintf("f%02d", index), typedString(fmt.Sprint(index))))
		invalid.Event.Fields.Fields = append(invalid.Event.Fields.Fields, typedField(fmt.Sprintf("f%02d", index), typedString(fmt.Sprint(index))))
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(ctx, t, store, indexTime, "convert", "convert-batch", 149, valid, invalid)
	compiled := compile(`index=convert | convert timeformat="%F %T" ctime(epoch) AS rendered dur2sec(delay) AS seconds num(bytes) AS number rmunit(duration) AS units auto(mixed) | sort 0 +event_id | table event_id epoch rendered seconds number units mixed`)
	pipelineAssertJSONRows(t, queryContext, connection, compiled, []string{"event_id", "epoch", "rendered", "seconds", "number", "units", "mixed"}, [][]string{
		{"convert-invalid", "bad", "bad", "bad", "<null>", "bad", "bad"},
		{"convert-valid", "0", "1970-01-01 00:00:00", "615", "1234.5", "212", "1"},
	})
	preserved := compile(`index=convert | convert ctime(object) AS copied num(nothing) AS neutral | table event_id copied.child object.child neutral`)
	rows := pipelineJSONRows(t, queryContext, connection, preserved, []string{"copied.child", "object.child"})
	if len(rows) != 2 {
		t.Fatalf("convert container row count = %d", len(rows))
	}
	for _, row := range rows {
		if pipelineJSONText(row[0]) != "kept" || pipelineJSONText(row[1]) != "kept" {
			t.Fatalf("convert lost container descendants: %#v", rows)
		}
	}
	descendants := compile(`index=convert event_id="convert-valid" | convert ctime(object) AS copied | eval second=copied | convert num(second.number) | table copied.literal\.dot second.literal%dot second.number`)
	pipelineAssertJSONRows(t, queryContext, connection, descendants, []string{`copied.literal\.dot`, "second.literal%dot", "second.number"}, [][]string{{"dot", "percent", "1"}})
	filtered := compile(`index=convert | convert auto(mixed) | search event_id="convert-valid" | table mixed`)
	pipelineAssertJSONRows(t, queryContext, connection, filtered, []string{"mixed"}, [][]string{{"1"}})
	raw := compile(`index=convert event_id="convert-valid" | convert num(epoch)`)
	if !raw.SparseFieldsSubset {
		t.Fatal("convert dropped sparse raw-field transport")
	}
	payload := pipelineJSONRows(t, queryContext, connection, raw, []string{"fields", "epoch"})
	if len(payload) != 1 {
		t.Fatalf("raw convert rows = %#v", payload)
	}
	if pipelineJSONText(payload[0][1]) != "0" {
		t.Fatalf("raw epoch = %#v", payload[0][1])
	}
	if !strings.Contains(fmt.Sprint(payload[0][0]), "keep raw") {
		t.Fatalf("unrelated raw field was lost: %#v", payload[0][0])
	}
	decimal := compile(`index=convert event_id="convert-valid" | convert num(decimal) | table event_id decimal`)
	pipelineAssertTaggedScalar(t, queryContext, connection, decimal, "decimal", "decimal/v1", "123456789012345678901234567890.125")
	oversized := compile(`index=convert event_id="convert-valid" | convert ctime(oversized) AS retained num(oversized) AS removed | table retained removed`)
	pipelineAssertJSONRows(t, queryContext, connection, oversized, []string{"retained", "removed"}, [][]string{{strings.Repeat("0", MaximumConvertInputBytes) + "1", "<null>"}})
	upstream := compile(`index=convert section_assessment_id=17 | stats latest(_time) as last_seen`)
	var upstreamPhysical, upstreamDynamic, upstreamJSON string
	if err := connection.QueryRow(queryContext, "SELECT toTypeName(last_seen), dynamicType(last_seen), ifNull(toJSONString(last_seen), 'null') FROM ("+upstream.SQL+")", upstream.Args...).Scan(&upstreamPhysical, &upstreamDynamic, &upstreamJSON); err != nil {
		t.Fatal(err)
	}
	wantEpoch := fmt.Sprintf("%d.%09d", valid.Event.EventTime.AsTime().Unix(), valid.Event.EventTime.AsTime().Nanosecond())
	if upstreamPhysical != "Dynamic" || upstreamDynamic != "String" || upstreamJSON != strconv.Quote(wantEpoch) {
		t.Fatalf("latest(_time) epoch transport = %s/%s/%s, want %s", upstreamPhysical, upstreamDynamic, upstreamJSON, wantEpoch)
	}
	for _, source := range []string{
		`index=convert | stats earliest(_time) AS first_seen latest(_time) AS last_seen | table first_seen last_seen`,
		`index=convert | eval copied=_time | rename copied AS time_copy | stats earliest(time_copy) AS first_seen latest(time_copy) AS last_seen | table first_seen last_seen`,
	} {
		pipelineAssertJSONRows(t, queryContext, connection, compile(source), []string{"first_seen", "last_seen"}, [][]string{{wantEpoch, wantEpoch}})
	}
	issue := compile(`index=convert section_assessment_id=17 | stats min(_time) as first_seen latest(_time) as last_seen values(upload_batch_id) as upload_batch_id values(source_file_id) as source_file_id values(workflow_type) as workflow_types values(event_name) as events by section_assessment_submission_id | sort - last_seen | convert ctime(first_seen) ctime(last_seen) | head 20`)
	wantTime := valid.Event.EventTime.AsTime().UTC().Format("01/02/2006 15:04:05")
	pipelineAssertJSONRows(t, queryContext, connection, issue, []string{"section_assessment_submission_id", "first_seen", "last_seen", "upload_batch_id", "source_file_id", "workflow_types", "events"}, [][]string{{"s1", wantTime, wantTime, "[u1]", "[f1]", "[w1]", "[e1]"}})
	preciseChronological := compile(`index=convert | stats latest(_time) AS last_seen | convert timeformat="%s.%9N" ctime(last_seen) | table last_seen`)
	pipelineAssertJSONRows(t, queryContext, connection, preciseChronological, []string{"last_seen"}, [][]string{{wantEpoch}})
	decimalTime := compile(`index=convert event_id="convert-valid" | convert timeformat="%s.%3N" ctime(decimal_epoch) | table decimal_epoch`)
	pipelineAssertJSONRows(t, queryContext, connection, decimalTime, []string{"decimal_epoch"}, [][]string{{"0.125"}})
	visibilityCutoff := mustVisibilityCutoff(t, ctx, store)
	scope := plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{"convert"}, Earliest: time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC), Latest: time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC), SearchStart: indexTime.Add(9 * time.Second), SearchTimezone: "UTC", IndexTimeCutoff: indexTime.Add(10 * time.Second), VisibilityCutoff: &visibilityCutoff}
	localized, err := spl.Parse(`index=convert event_id="convert-valid" | convert timeformat="%F %T" ctime(epoch) AS localized mktime(timestamp_text) AS parsed | table localized parsed`)
	if err != nil {
		t.Fatal(err)
	}
	localScope := scope
	localScope.SearchTimezone = "America/Los_Angeles"
	localPlan, err := plan.Build(localized, localScope)
	if err != nil {
		t.Fatal(err)
	}
	localCompiled, err := (Compiler{}).Compile(localPlan)
	if err != nil {
		t.Fatal(err)
	}
	pipelineAssertJSONRows(t, queryContext, connection, localCompiled, []string{"localized", "parsed"}, [][]string{{"1969-12-31 16:00:00", "1782889200"}})
	for _, test := range []struct {
		source string
		fields []string
		want   [][]string
	}{
		{`index=convert | convert num(f*) none(f*) | table event_id f00`, []string{"f00"}, [][]string{{"0"}, {"0"}}},
		{`index=convert event_id="convert-valid" | convert num(epoch) AS newepoch num(new*) | table newepoch`, []string{"newepoch"}, [][]string{{"0"}}},
		{`index=convert event_id="convert-valid" | convert ctime(epoch*) | convert num(f60*) | table epoch f60`, []string{"epoch", "f60"}, [][]string{{"01/01/1970 00:00:00", "60"}}},
		{`index=convert | convert num(f60*) | stats sum(f60*)`, []string{"sum(f60)"}, [][]string{{"120"}}},
		{`index=convert | stats sum(f60*) AS v* | convert num(v*)`, []string{"v"}, [][]string{{"120"}}},
		{`index=convert | convert auto(mix*) | search event_id="convert-valid" | head 1 | table mixed`, []string{"mixed"}, [][]string{{"1"}}},
		{`index=convert | convert num(never_matches*) | convert num(f60*) | table f60`, []string{"f60"}, [][]string{{"60"}, {"60"}}},
		{`index=convert event_id="convert-valid" | convert num(nested.*) | table nested.value`, []string{"nested.value"}, [][]string{{"3"}}},
	} {
		t.Run(test.source, func(t *testing.T) {
			compiled := compileConvertWithRuntimeInventory(t, queryContext, connection, test.source, scope)
			pipelineAssertJSONRows(t, queryContext, connection, compiled, test.fields, test.want)
		})
	}
	for _, source := range []string{
		`index=convert | eventstats earliest(mixed) AS first_seen | convert auto(mixed) | where 1=0 | head 1 | table mixed`,
		`index=convert | streamstats earliest(mixed) AS first_seen | convert auto(mixed) | where 1=0 | table mixed`,
	} {
		compiled := compile(source)
		if rows := pipelineJSONRows(t, queryContext, connection, compiled, []string{"mixed"}); len(rows) != 0 {
			t.Fatalf("empty chronological convert result = %#v", rows)
		}
	}
	// A query-wide auto failure must retain the original String even after a
	// later predicate removes the invalid row.
	preservedType := pipelineJSONRows(t, queryContext, connection, filtered, []string{"mixed"})
	if len(preservedType) != 1 {
		t.Fatal("missing auto row")
	}
	if value, ok := preservedType[0][0].(chcol.Dynamic); !ok || value.Type() != "String" {
		t.Fatalf("auto filtered result changed type: %#v", preservedType)
	}

}

func TestConvertCommandsAgainstClickHouse(t *testing.T) {
	ctx, connection, container := convertProbeConnection(t)
	migrationPaths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"))
	if err != nil || len(migrationPaths) == 0 {
		t.Fatalf("discover migrations: %v", err)
	}
	var migrations bytes.Buffer
	for _, path := range migrationPaths {
		migration, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	integrationDocker(t, ctx, bytes.NewReader(migrations.Bytes()), "exec", "--interactive", container.Name, "clickhouse-client", "--user", container.Username, "--password", container.Password, "--multiquery")
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "convert-control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	store, err := NewStore(connection, fixedRetention(storeIntegrationFixtureRetention), sequencer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	testConvertAgainstClickHouse(ctx, t, store, connection, time.Date(2026, 7, 21, 3, 4, 6, 987654321, time.UTC))
}

func compileConvertWithRuntimeInventory(t *testing.T, ctx context.Context, connection clickhousedriver.Conn, source string, scope plan.Scope) CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := plan.PrepareStatsWildcard(parsed, scope)
	if err != nil {
		t.Fatalf("prepare runtime wildcard: %v", err)
	}
	for rounds := 0; preparation.FullPlan() == nil; rounds++ {
		if rounds >= 8 {
			t.Fatal("wildcard discovery did not finish")
		}
		request := preparation.Request()
		inventory, err := (Compiler{}).CompileStatsWildcardInventory(preparation.Prefix(), request)
		if err != nil {
			t.Fatalf("compile runtime inventory: %v", err)
		}
		rows, err := connection.Query(ctx, inventory.SQL, inventory.Args...)
		if err != nil {
			t.Fatalf("execute runtime inventory: %v\nSQL: %s\nargs: %#v", err, inventory.SQL, inventory.Args)
		}
		var matches []plan.StatsWildcardInventoryMatch
		header := false
		for rows.Next() {
			var ordinal, invalid uint8
			var field string
			if err := rows.Scan(&ordinal, &field, &invalid); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			if !header {
				if ordinal != 0 || field != "" || invalid != 0 {
					_ = rows.Close()
					t.Fatalf("invalid inventory header: %d/%q/%d", ordinal, field, invalid)
				}
				header = true
				continue
			}
			if invalid != 0 {
				_ = rows.Close()
				t.Fatal("poisoned inventory row")
			}
			matches = append(matches, plan.StatsWildcardInventoryMatch{Ordinal: ordinal, Field: field})
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !header {
			t.Fatal("inventory omitted header")
		}
		expansion, err := plan.ValidateStatsWildcardInventory(request, matches)
		if err != nil {
			t.Fatalf("validate runtime inventory: %v", err)
		}
		preparation, err = plan.PrepareNextStatsWildcard(parsed, scope, expansion)
		if err != nil {
			t.Fatalf("resume runtime inventory: %v", err)
		}
	}
	compiled, err := (Compiler{}).Compile(preparation.FullPlan())
	if err != nil {
		t.Fatalf("compile expanded query: %v", err)
	}
	return compiled
}
