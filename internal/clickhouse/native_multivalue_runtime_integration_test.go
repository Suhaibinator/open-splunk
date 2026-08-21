package clickhouse

import (
	"context"
	"math"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

const nativeMultivalueRuntimeIndex = "native-multivalue-runtime"

// TestNativeMultivalueAgainstClickHouse pins the native Array(Dynamic)
// lowering against the repository's ClickHouse release. The fixture is kept
// deliberately small: the contract under test is the value shape and path
// traversal, not scan volume.
func TestNativeMultivalueAgainstClickHouse(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
	event := testStoredEvent("native-multivalue", nativeMultivalueRuntimeIndex, indexTime)
	event.Event.Source = "native-multivalue-runtime"
	event.Event.Raw = []byte(`{
		"users":["alice","bob","alice",null],
		"groups":[
			{"users":[{"name":"anna"},{"name":null}]},
			{"users":[]},
			{"users":[{"name":"bob"}]},
			{"users":"not-an-array"},
			{}
		],
		"empty":[]
	}`)
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		nativeMultivalueRuntimeIndex,
		"native-multivalue-runtime-batch",
		501,
		event,
	)
	base := `index=native-multivalue-runtime event_id="native-multivalue"`

	t.Run("empty delimiter splits Unicode characters", func(t *testing.T) {
		compiled := compile(base + ` | eval chars=split("A⏰😀β", "") | table chars`)
		row := nativeMultivalueRuntimeQueryRow(t, queryContext, connection, compiled)
		got, ok := nativeMultivalueRuntimeValue(row["chars"]).([]string)
		want := []string{"A", "⏰", "😀", "β"}
		if !ok || !slices.Equal(got, want) {
			t.Fatalf("Unicode split = %#v (%T), want %#v", row["chars"], row["chars"], want)
		}
		nativeMultivalueRequirePresent(t, compiled, row, "chars")
	})

	t.Run("mixed members retain types null and stable first occurrence", func(t *testing.T) {
		compiled := compile(base +
			` | eval values=mvdedup(mvappend("alpha", 1, 1.0, true, null, "alpha", 1.0, null, true, 1))` +
			` | table values`)
		row := nativeMultivalueRuntimeQueryRow(t, queryContext, connection, compiled)
		members := nativeMultivalueRuntimeDynamicMembers(t, row["values"])
		got := make([]string, len(members))
		for index, member := range members {
			got[index] = nativeMultivalueRuntimeDynamicSignature(member)
		}
		want := []string{
			"String/alpha",
			"Int64/1",
			"Float64/1",
			"Bool/true",
			"None/<null>",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("stable mixed mvdedup = %v, want %v", got, want)
		}
		nativeMultivalueRequirePresent(t, compiled, row, "values")
	})

	t.Run("dedup treats signed floating zero as equal and retains first", func(t *testing.T) {
		compiled := compile(base +
			` | eval values=mvdedup(mvappend(0.0 / -1.0, 0.0, 0.0 / -1.0)) | table values`)
		row := nativeMultivalueRuntimeQueryRow(t, queryContext, connection, compiled)
		members := nativeMultivalueRuntimeDynamicMembers(t, row["values"])
		if len(members) != 1 {
			t.Fatalf("signed-zero mvdedup length = %d, want 1", len(members))
		}
		value, ok := members[0].Any().(float64)
		if !ok || !math.Signbit(value) {
			t.Fatalf("signed-zero mvdedup first member = %#v (%T), want retained -0.0", members[0].Any(), members[0].Any())
		}
		nativeMultivalueRequirePresent(t, compiled, row, "values")
	})

	t.Run("signed indexes and inclusive ranges", func(t *testing.T) {
		compiled := compile(base +
			` | eval source_values=split("a,b,c,d", ",")` +
			` | eval single=mvindex(source_values, -1), window=mvindex(source_values, -3, -1)` +
			` | table single window`)
		row := nativeMultivalueRuntimeQueryRow(t, queryContext, connection, compiled)
		single, ok := nativeMultivalueRuntimeValue(row["single"]).(clickhousedriver.Dynamic)
		if !ok || nativeMultivalueRuntimeDynamicSignature(single) != "String/d" {
			t.Fatalf("mvindex(..., -1) = %#v (%T), want String/d", row["single"], row["single"])
		}
		window := nativeMultivalueRuntimeDynamicMembers(t, row["window"])
		gotWindow := make([]string, len(window))
		for index, member := range window {
			gotWindow[index] = nativeMultivalueRuntimeDynamicSignature(member)
		}
		wantWindow := []string{"String/b", "String/c", "String/d"}
		if !slices.Equal(gotWindow, wantWindow) {
			t.Fatalf("inclusive negative mvindex = %v, want %v", gotWindow, wantWindow)
		}
		nativeMultivalueRequirePresent(t, compiled, row, "window")
	})

	t.Run("nested wildcard traversal preserves null and source order", func(t *testing.T) {
		compiled := compile(base + ` | spath path=groups{}.users{}.name output=names | table names`)
		row := nativeMultivalueRuntimeQueryRow(t, queryContext, connection, compiled)
		members := nativeMultivalueRuntimeDynamicMembers(t, row["names"])
		got := make([]string, len(members))
		for index, member := range members {
			got[index] = nativeMultivalueRuntimeDynamicSignature(member)
		}
		want := []string{"String/anna", "None/<null>", "String/bob"}
		if !slices.Equal(got, want) {
			t.Fatalf("nested wildcard spath = %v, want %v", got, want)
		}
		nativeMultivalueRequirePresent(t, compiled, row, "names")
	})

	t.Run("terminal empty wildcard is present empty", func(t *testing.T) {
		compiled := compile(base + ` | spath path=empty{} output=values | table values`)
		row := nativeMultivalueRuntimeQueryRow(t, queryContext, connection, compiled)
		if members := nativeMultivalueRuntimeDynamicMembers(t, row["values"]); len(members) != 0 {
			t.Fatalf("terminal empty wildcard = %#v, want present empty", members)
		}
		nativeMultivalueRequirePresent(t, compiled, row, "values")
	})

	t.Run("supplied spath eval mvexpand stats pipeline", func(t *testing.T) {
		compiled := compile(base +
			` | spath path=users{} output=users` +
			` | eval users=mvdedup(users), first_user=mvindex(users, 0), user_list=mvjoin(users, ",")` +
			` | mvexpand users` +
			` | stats count BY users` +
			` | sort users`)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute native multivalue pipeline: %v\nSQL: %s\nargs: %#v", queryErr, compiled.SQL, compiled.Args)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
				t.Errorf("close native multivalue pipeline rows: %v", closeErr)
			}
		}()
		var got []string
		for rows.Next() {
			var user string
			var count uint64
			if scanErr := rows.Scan(&user, &count); scanErr != nil {
				t.Fatalf("scan native multivalue pipeline: %v", scanErr)
			}
			got = append(got, user+"/"+pipelineJSONText(count))
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate native multivalue pipeline: %v", rowsErr)
		}
		want := []string{"alice/1", "bob/1"}
		if !slices.Equal(got, want) {
			t.Fatalf("native multivalue pipeline rows = %v, want %v", got, want)
		}
	})
}

func nativeMultivalueRuntimeQueryRow(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) map[string]any {
	t.Helper()
	rows, err := connection.Query(ctx, compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatalf("execute native multivalue query: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
			t.Errorf("close native multivalue rows: %v", closeErr)
		}
	}()
	columns := rows.Columns()
	columnTypes := rows.ColumnTypes()
	if len(columns) != len(columnTypes) {
		t.Fatalf("native multivalue columns/types = %d/%d", len(columns), len(columnTypes))
	}
	if !rows.Next() {
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("read native multivalue row: %v", rowsErr)
		}
		t.Fatal("native multivalue query returned no rows")
	}
	targets := make([]any, len(columnTypes))
	for index, columnType := range columnTypes {
		scanType := columnType.ScanType()
		if scanType == nil {
			t.Fatalf("native multivalue column %q has no scan type", columns[index])
		}
		targets[index] = reflect.New(scanType).Interface()
	}
	if scanErr := rows.Scan(targets...); scanErr != nil {
		t.Fatalf("scan native multivalue row: %v", scanErr)
	}
	result := make(map[string]any, len(columns))
	for index, column := range columns {
		result[column] = reflect.ValueOf(targets[index]).Elem().Interface()
	}
	if rows.Next() {
		t.Fatal("native multivalue scalar query returned more than one row")
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		t.Fatalf("iterate native multivalue row: %v", rowsErr)
	}
	return result
}

func nativeMultivalueRuntimeValue(value any) any {
	reflected := reflect.ValueOf(value)
	for reflected.IsValid() && reflected.Kind() == reflect.Pointer {
		if reflected.IsNil() {
			return nil
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() {
		return nil
	}
	return reflected.Interface()
}

func nativeMultivalueRuntimeDynamicMembers(
	t *testing.T,
	value any,
) []clickhousedriver.Dynamic {
	t.Helper()
	members, ok := nativeMultivalueRuntimeValue(value).([]clickhousedriver.Dynamic)
	if !ok {
		t.Fatalf("native multivalue = %#v (%T), want []Dynamic", value, value)
	}
	return members
}

func nativeMultivalueRuntimeDynamicSignature(value clickhousedriver.Dynamic) string {
	if value.Nil() {
		return "None/<null>"
	}
	return value.Type() + "/" + pipelineJSONText(value.Any())
}

func nativeMultivalueRequirePresent(
	t *testing.T,
	compiled CompiledQuery,
	row map[string]any,
	field string,
) {
	t.Helper()
	outputIndex := -1
	for index, output := range compiled.OutputFields {
		if output == field {
			outputIndex = index
			break
		}
	}
	if outputIndex < 0 {
		t.Fatalf("compiled outputs %v do not contain %q", compiled.OutputFields, field)
	}
	for _, output := range compiled.OptionalMultivalueOutputs {
		if int(output.OutputIndex) != outputIndex {
			continue
		}
		present, ok := nativeMultivalueRuntimeValue(row[output.PresentColumn()]).(uint8)
		if !ok || present != 1 {
			t.Fatalf(
				"%s presence sidecar %q = %#v (%T), want UInt8(1)",
				field,
				output.PresentColumn(),
				row[output.PresentColumn()],
				row[output.PresentColumn()],
			)
		}
		return
	}
	t.Fatalf("compiled output %q has no optional multivalue descriptor", field)
}
