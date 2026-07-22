package queryexec

import (
	"context"
	"errors"
	"io"
	"math/big"
	"net"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func TestExecutorStreamsTypedRowsAndExactSchema(t *testing.T) {
	t.Parallel()
	timestamp := time.Date(2026, time.July, 21, 3, 4, 5, 123000000, time.UTC)
	rows := &fakeRows{
		columns: []string{"_time", "message", "status", "_raw"},
		types: []driver.ColumnType{
			fakeColumnType{name: "_time", databaseType: "DateTime64(9, 'UTC')", scanType: reflect.TypeOf(time.Time{})},
			fakeColumnType{name: "message", databaseType: "Nullable(String)", scanType: reflect.TypeOf((*string)(nil)), nullable: true},
			fakeColumnType{name: "status", databaseType: "Dynamic", scanType: reflect.TypeOf(chcol.Dynamic{})},
			fakeColumnType{name: "_raw", databaseType: "String", scanType: reflect.TypeOf("")},
		},
		data: [][]any{
			{timestamp, "hello", chcol.NewDynamicWithType(int64(500), "Int64"), "valid"},
			{timestamp.Add(time.Second), nil, chcol.NewDynamicWithType("500", "String"), string([]byte{0xff, 0x00})},
		},
	}
	connection := &fakeQueryConnection{rows: rows}
	executor := mustExecutor(t, connection)
	sink := &fakeSink{}
	query := clickhouse.CompiledQuery{
		SQL: "SELECT scoped", Args: []any{"tenant", uint64(7)},
		OutputFields: []string{"_time", "message", "status", "_raw"},
	}
	if err := executor.Execute(context.Background(), query, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if connection.query != query.SQL || !reflect.DeepEqual(connection.args, query.Args) {
		t.Fatalf("query/args = %q %#v", connection.query, connection.args)
	}
	wantKinds := []searchjobs.ValueKind{
		searchjobs.ValueKindTime, searchjobs.ValueKindString, searchjobs.ValueKindMixed, searchjobs.ValueKindMixed,
	}
	for index, want := range wantKinds {
		if sink.schema.Columns[index].Kind != want {
			t.Errorf("schema column %d kind = %v, want %v", index, sink.schema.Columns[index].Kind, want)
		}
	}
	if !sink.schema.Columns[1].Nullable || !sink.schema.Columns[2].Nullable || !sink.schema.Columns[3].Nullable {
		t.Fatalf("nullable schema = %+v", sink.schema.Columns)
	}
	if len(sink.rows) != 2 || !rows.closed {
		t.Fatalf("rows=%d closed=%v", len(sink.rows), rows.closed)
	}
	if value, ok := sink.rows[0][1].String(); !ok || value != "hello" {
		t.Fatalf("message = %#v", sink.rows[0][1])
	}
	if !sink.rows[1][1].IsNull() {
		t.Fatalf("nullable message = %#v", sink.rows[1][1])
	}
	if value, ok := sink.rows[0][2].Signed(); !ok || value != 500 {
		t.Fatalf("integer Dynamic = %#v", sink.rows[0][2])
	}
	if value, ok := sink.rows[1][2].String(); !ok || value != "500" {
		t.Fatalf("string Dynamic = %#v", sink.rows[1][2])
	}
	if value, ok := sink.rows[1][3].Bytes(); !ok || !slices.Equal(value, []byte{0xff, 0}) {
		t.Fatalf("binary raw = %#v", sink.rows[1][3])
	}
}

func TestConvertJSONRestoresLogicalPathsAndNestedTypes(t *testing.T) {
	t.Parallel()
	document := chcol.NewJSON()
	document.SetValueAtPath("literal%2Edot", chcol.NewDynamicWithType("value", "String"))
	document.SetValueAtPath("percent%252Ekey", chcol.NewDynamicWithType(uint64(9), "UInt64"))
	document.SetValueAtPath("nested.ok", chcol.NewDynamicWithType(true, "Bool"))
	document.SetValueAtPath("nested.missing", chcol.NewDynamicWithType(nil, ""))
	value, err := convertValue(document)
	if err != nil {
		t.Fatalf("convertValue(JSON): %v", err)
	}
	fields, ok := value.Object()
	if !ok || len(fields) != 3 {
		t.Fatalf("root object = %#v", value)
	}
	if fields[0].Name != "literal.dot" || fields[1].Name != "nested" || fields[2].Name != "percent%2Ekey" {
		t.Fatalf("logical fields = %#v", fields)
	}
	nested, ok := fields[1].Value.Object()
	if !ok || len(nested) != 2 || nested[0].Name != "missing" || !nested[0].Value.IsNull() || nested[1].Name != "ok" {
		t.Fatalf("nested object = %#v", fields[1].Value)
	}
}

func TestExecutorConvertsDriverDecimalAndDurationScanTypes(t *testing.T) {
	t.Parallel()

	amount := decimal.RequireFromString("-12345678901234567890.00100")
	elapsed := 3*time.Hour + 4*time.Minute + 5*time.Second + 600*time.Microsecond
	rows := &fakeRows{
		columns: []string{"amount", "elapsed"},
		types: []driver.ColumnType{
			fakeColumnType{name: "amount", databaseType: "Decimal(38, 5)", scanType: reflect.TypeOf(decimal.Decimal{})},
			fakeColumnType{name: "elapsed", databaseType: "Time64(6)", scanType: reflect.TypeOf(time.Duration(0))},
		},
		data: [][]any{{amount, elapsed}},
	}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	sink := &fakeSink{}
	err := executor.Execute(context.Background(), clickhouse.CompiledQuery{
		SQL:          "SELECT amount, elapsed",
		OutputFields: []string{"amount", "elapsed"},
	}, sink)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := []searchjobs.ValueKind{sink.schema.Columns[0].Kind, sink.schema.Columns[1].Kind}; !reflect.DeepEqual(got, []searchjobs.ValueKind{searchjobs.ValueKindDecimal, searchjobs.ValueKindDuration}) {
		t.Fatalf("schema kinds = %v", got)
	}
	if got, ok := sink.rows[0][0].Decimal(); !ok || got != "-12345678901234567890.001" {
		t.Fatalf("decimal = %q, %v", got, ok)
	}
	if got, ok := sink.rows[0][1].Duration(); !ok || got != elapsed {
		t.Fatalf("duration = %v, %v", got, ok)
	}
}

func TestConvertAdditionalClickHouseDriverScanTypes(t *testing.T) {
	t.Parallel()

	variant, err := convertValue(chcol.NewVariantWithType(uint64(17), "UInt64"))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := variant.Unsigned(); !ok || got != 17 {
		t.Fatalf("Variant = %#v", variant)
	}

	wide := new(big.Int)
	wide.SetString("340282366920938463463374607431768211455", 10)
	wideValue, err := convertValue(wide)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := wideValue.Decimal(); !ok || got != wide.String() {
		t.Fatalf("big.Int = %q, %v", got, ok)
	}

	id := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	idValue, err := convertValue(id)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := idValue.String(); !ok || got != id.String() {
		t.Fatalf("UUID = %q, %v", got, ok)
	}

	ip := net.ParseIP("192.0.2.1").To4()
	ipValue, err := convertValue(ip)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := ipValue.String(); !ok || got != "192.0.2.1" {
		t.Fatalf("IP = %q, %v", got, ok)
	}
}

func TestSchemaKindUnwrapsClickHouseWrappers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		field    string
		database string
		kind     searchjobs.ValueKind
		multi    bool
	}{
		{"message", "LowCardinality(Nullable(String))", searchjobs.ValueKindString, false},
		{"count", "UInt64", searchjobs.ValueKindUnsigned, false},
		{"ratio", "Float64", searchjobs.ValueKindDouble, false},
		{"amount", "Decimal(38, 9)", searchjobs.ValueKindDecimal, false},
		{"clock", "Time", searchjobs.ValueKindDuration, false},
		{"precise_clock", "Time64(9)", searchjobs.ValueKindDuration, false},
		{"wide_signed", "Int256", searchjobs.ValueKindDecimal, false},
		{"wide_unsigned", "UInt128", searchjobs.ValueKindDecimal, false},
		{"id", "UUID", searchjobs.ValueKindString, false},
		{"ip", "IPv6", searchjobs.ValueKindString, false},
		{"values", "Array(Dynamic)", searchjobs.ValueKindList, true},
		{"named_tuple", "Tuple(name String, count UInt64)", searchjobs.ValueKindMixed, false},
		{"plain_tuple", "Tuple(String, UInt64)", searchjobs.ValueKindMixed, false},
		{"fields", "JSON(max_dynamic_paths=256)", searchjobs.ValueKindObject, false},
		{"anything", "Dynamic", searchjobs.ValueKindMixed, false},
		{"choice", "Variant(String, UInt64)", searchjobs.ValueKindMixed, false},
		{"_raw", "String", searchjobs.ValueKindMixed, false},
	}
	for _, test := range tests {
		kind, multi := schemaKind(test.field, test.database)
		if kind != test.kind || multi != test.multi {
			t.Errorf("schemaKind(%q, %q) = %v/%v, want %v/%v", test.field, test.database, kind, multi, test.kind, test.multi)
		}
	}
}

func TestExecutorRejectsColumnDriftAndPropagatesSinkLimit(t *testing.T) {
	t.Parallel()
	rows := &fakeRows{
		columns: []string{"wrong"},
		types:   []driver.ColumnType{fakeColumnType{name: "wrong", databaseType: "String", scanType: reflect.TypeOf("")}},
	}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	err := executor.Execute(context.Background(), clickhouse.CompiledQuery{SQL: "SELECT 1", OutputFields: []string{"expected"}}, &fakeSink{})
	if !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("column drift error = %v", err)
	}

	rows = &fakeRows{
		columns: []string{"message"},
		types:   []driver.ColumnType{fakeColumnType{name: "message", databaseType: "String", scanType: reflect.TypeOf("")}},
		data:    [][]any{{"one"}, {"two"}},
	}
	sink := &fakeSink{addErr: searchjobs.ErrRowLimit}
	executor = mustExecutor(t, &fakeQueryConnection{rows: rows})
	err = executor.Execute(context.Background(), clickhouse.CompiledQuery{SQL: "SELECT message", OutputFields: []string{"message"}}, sink)
	if !errors.Is(err, searchjobs.ErrRowLimit) || rows.nextCalls != 1 || !rows.closed {
		t.Fatalf("sink error=%v next=%d closed=%v", err, rows.nextCalls, rows.closed)
	}
}

func TestQuerySettingsAreReadOnlyAndBounded(t *testing.T) {
	t.Parallel()
	settings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"readonly", "max_execution_time", "max_memory_usage", "max_rows_to_read", "max_bytes_to_read",
		"max_result_rows", "max_result_bytes", "max_threads",
	} {
		if _, exists := settings[name]; !exists {
			t.Errorf("missing query setting %q", name)
		}
	}
	if settings["readonly"] != uint8(2) || settings["result_overflow_mode"] != "throw" || settings["read_overflow_mode"] != "throw" {
		t.Fatalf("unsafe settings = %#v", settings)
	}
	if _, err := querySettings(Config{MaxExecutionTime: -time.Second}); err == nil {
		t.Fatal("negative execution time accepted")
	}
}

func TestClassifyQueryErrorsRedactsIntoStableCategories(t *testing.T) {
	t.Parallel()
	if err := classifyQueryError(context.Background(), io.ErrUnexpectedEOF); !errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("network error = %v", err)
	}
	resource := &clickhousedriver.Exception{Code: 241, Name: "MEMORY_LIMIT_EXCEEDED"}
	if err := classifyQueryError(context.Background(), resource); !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("resource error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := classifyQueryError(ctx, errors.New("contains secret")); !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("canceled error = %v", err)
	}
}

func mustExecutor(t *testing.T, connection queryConnection) *Executor {
	t.Helper()
	settings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	return &Executor{
		connection: connection,
		settings:   settings,
		newQueryID: func() (string, error) { return "open-splunk-search-test", nil },
	}
}

type fakeQueryConnection struct {
	rows  driver.Rows
	err   error
	query string
	args  []any
}

func (connection *fakeQueryConnection) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	connection.query = query
	connection.args = slices.Clone(args)
	return connection.rows, connection.err
}

type fakeColumnType struct {
	name, databaseType string
	scanType           reflect.Type
	nullable           bool
}

func (column fakeColumnType) Name() string             { return column.name }
func (column fakeColumnType) Nullable() bool           { return column.nullable }
func (column fakeColumnType) ScanType() reflect.Type   { return column.scanType }
func (column fakeColumnType) DatabaseTypeName() string { return column.databaseType }

type fakeRows struct {
	columns   []string
	types     []driver.ColumnType
	data      [][]any
	index     int
	nextCalls int
	err       error
	closeErr  error
	closed    bool
}

func (rows *fakeRows) Next() bool {
	if rows.index >= len(rows.data) {
		return false
	}
	rows.nextCalls++
	rows.index++
	return true
}
func (rows *fakeRows) Scan(destinations ...any) error {
	if rows.index == 0 || rows.index > len(rows.data) {
		return errors.New("scan outside active row")
	}
	values := rows.data[rows.index-1]
	if len(values) != len(destinations) {
		return errors.New("destination count mismatch")
	}
	for index, destination := range destinations {
		if err := assignFakeScan(destination, values[index]); err != nil {
			return err
		}
	}
	return nil
}
func (rows *fakeRows) ScanStruct(any) error             { return errors.New("unused") }
func (rows *fakeRows) ColumnTypes() []driver.ColumnType { return rows.types }
func (rows *fakeRows) Totals(...any) error              { return nil }
func (rows *fakeRows) Columns() []string                { return rows.columns }
func (rows *fakeRows) Close() error                     { rows.closed = true; return rows.closeErr }
func (rows *fakeRows) Err() error                       { return rows.err }
func (rows *fakeRows) HasData() bool                    { return len(rows.data) != 0 }

func assignFakeScan(destination, source any) error {
	target := reflect.ValueOf(destination)
	if target.Kind() != reflect.Pointer || target.IsNil() {
		return errors.New("fake scan destination is not a pointer")
	}
	return assignFakeValue(target.Elem(), source)
}

func assignFakeValue(target reflect.Value, source any) error {
	if source == nil {
		target.SetZero()
		return nil
	}
	value := reflect.ValueOf(source)
	if value.Type().AssignableTo(target.Type()) {
		target.Set(value)
		return nil
	}
	if target.Kind() == reflect.Pointer {
		pointer := reflect.New(target.Type().Elem())
		if err := assignFakeValue(pointer.Elem(), source); err != nil {
			return err
		}
		target.Set(pointer)
		return nil
	}
	return errors.New("fake scan type mismatch")
}

type fakeSink struct {
	schema searchjobs.Schema
	rows   [][]searchjobs.Value
	setErr error
	addErr error
}

func (sink *fakeSink) SetSchema(schema searchjobs.Schema) error {
	if sink.setErr != nil {
		return sink.setErr
	}
	sink.schema = schema
	return nil
}
func (sink *fakeSink) AddRow(values []searchjobs.Value) error {
	if sink.addErr != nil {
		return sink.addErr
	}
	sink.rows = append(sink.rows, slices.Clone(values))
	return nil
}
