package queryexec

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestV03RealMakeMVCompilerTransportPublishesNullEmptyAndOrderedMembers(t *testing.T) {
	t.Parallel()

	compiled := v03CompileRealMakeMVTransport(t)
	descriptors, valid := compiled.ValidatedResultOptionalMultivalueOutputs()
	if !valid || len(descriptors) != 1 || descriptors[0].OutputIndex != 1 ||
		!compiled.HasValidExecutionSeal() || !compiled.RequiresAtomicResult() {
		t.Fatalf("real makemv transport = descriptors %#v valid %t sealed %t atomic %t", descriptors, valid, compiled.HasValidExecutionSeal(), compiled.RequiresAtomicResult())
	}
	descriptor := descriptors[0]
	rows := &fakeRows{
		columns: []string{"event_id", "tags", descriptor.PresentColumn()},
		types:   v03OptionalMultivalueColumnTypes(descriptor),
		data: [][]any{
			{"missing", []string{}, uint8(0)},
			{"empty", []string{}, uint8(1)},
			{"members", []string{"a", "", "界", "a"}, uint8(1)},
		},
	}
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(context.Background(), compiled, sink); err != nil {
		t.Fatalf("Execute(real makemv transport) error = %v", err)
	}
	if len(sink.schema.Columns) != 2 || sink.schema.Columns[1].Kind != searchjobs.ValueKindList ||
		!sink.schema.Columns[1].Nullable || !sink.schema.Columns[1].Multivalue {
		t.Fatalf("public makemv schema = %#v", sink.schema)
	}
	if len(sink.rows) != 3 || !sink.rows[0][1].IsNull() {
		t.Fatalf("public makemv rows = %#v", sink.rows)
	}
	empty, emptyOK := sink.rows[1][1].List()
	members, membersOK := sink.rows[2][1].List()
	if !emptyOK || len(empty) != 0 || !membersOK || len(members) != 4 {
		t.Fatalf("public makemv values = empty %#v members %#v", sink.rows[1][1], sink.rows[2][1])
	}
	want := []string{"a", "", "界", "a"}
	got := make([]string, len(members))
	for index, member := range members {
		got[index], _ = member.String()
	}
	if !slices.Equal(got, want) {
		t.Fatalf("public makemv member order = %v, want %v", got, want)
	}
}

func TestV03OptionalMultivalueTransportDistinguishesNullEmptyAndMembers(t *testing.T) {
	t.Parallel()

	descriptor := clickhouse.ResultOptionalMultivalueOutput{OutputIndex: 1}
	query := clickhouse.CompiledQuery{
		OutputFields:              []string{"event_id", "tags"},
		OptionalMultivalueOutputs: []clickhouse.ResultOptionalMultivalueOutput{descriptor},
	}
	columns := []string{"event_id", "tags", descriptor.PresentColumn()}
	types := v03OptionalMultivalueColumnTypes(descriptor)
	containers, optional, err := validateOrdinaryResultColumns(query, columns, types, -1)
	if err != nil {
		t.Fatalf("validate canonical optional multivalue transport: %v", err)
	}
	if len(containers) != 2 || len(optional) != 2 || !optional[1].valid || optional[1].presentColumn != 2 {
		t.Fatalf("validated transports = containers %#v optional %#v", containers, optional)
	}

	tests := []struct {
		name    string
		raw     []string
		present uint8
		want    []string
		null    bool
	}{
		{name: "missing or explicit null", raw: []string{}, present: 0, null: true},
		{name: "present typed empty", raw: []string{}, present: 1, want: []string{}},
		{name: "ordered Unicode and empty members", raw: []string{"a", "", "界", "a"}, present: 1, want: []string{"a", "", "界", "a"}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			raw := slices.Clone(test.raw)
			present := test.present
			value, convertErr := convertOptionalMultivalueOutput(
				[]any{new(string), &raw, &present},
				1,
				optional[1],
			)
			if convertErr != nil {
				t.Fatalf("convert optional multivalue: %v", convertErr)
			}
			if test.null {
				if !value.IsNull() {
					t.Fatalf("absent value = %#v, want explicit public null", value)
				}
				return
			}
			members, ok := value.List()
			if !ok || len(members) != len(test.want) {
				t.Fatalf("converted value = %#v, want list %v", value, test.want)
			}
			got := make([]string, len(members))
			for index, member := range members {
				text, textOK := member.String()
				if !textOK {
					t.Fatalf("member %d = %#v, want String", index, member)
				}
				got[index] = text
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("members = %v, want %v", got, test.want)
			}
		})
	}
}

func TestV03OptionalMultivalueTransportRejectsForgedNativeValues(t *testing.T) {
	t.Parallel()

	transport := resultOptionalMultivalueTransport{valid: true, presentColumn: 1}
	empty := []string{}
	payload := []string{"secret-prefix"}
	zero := uint8(0)
	one := uint8(1)
	two := uint8(2)
	wrongPresence := uint16(1)
	wrongValue := []uint8{1}
	invalidUTF8 := []string{string([]byte{0xff})}
	for _, test := range []struct {
		name         string
		destinations []any
	}{
		{name: "absent retains payload", destinations: []any{&payload, &zero}},
		{name: "presence outside Boolean domain", destinations: []any{&empty, &two}},
		{name: "presence native type", destinations: []any{&empty, &wrongPresence}},
		{name: "value native type", destinations: []any{&wrongValue, &one}},
		{name: "invalid UTF-8 member", destinations: []any{&invalidUTF8, &one}},
		{name: "nil value", destinations: []any{(*[]string)(nil), &one}},
		{name: "nil presence", destinations: []any{&empty, (*uint8)(nil)}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value, err := convertOptionalMultivalueOutput(test.destinations, 0, transport)
			if err == nil || !reflect.DeepEqual(value, searchjobs.Value{}) {
				t.Fatalf("convert forged row = (%#v, %v), want zero value and error", value, err)
			}
		})
	}
}

func TestV03OptionalMultivalueTransportRejectsLateForgedRowAtomically(t *testing.T) {
	t.Parallel()

	compiled := v03CompileRealMakeMVTransport(t)
	descriptors, valid := compiled.ValidatedResultOptionalMultivalueOutputs()
	if !valid || len(descriptors) != 1 {
		t.Fatalf("real makemv descriptors = %#v, valid %t", descriptors, valid)
	}
	descriptor := descriptors[0]
	rows := &fakeRows{
		columns: []string{"event_id", "tags", descriptor.PresentColumn()},
		types:   v03OptionalMultivalueColumnTypes(descriptor),
		data: [][]any{
			{"would-leak-without-barrier", []string{"visible-prefix"}, uint8(1)},
			{"malformed-late-row", []string{}, uint8(2)},
		},
	}
	sink := &fakeSink{}
	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		compiled,
		sink,
	)
	if !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("Execute(late malformed optional MV) error = %v, want ErrInvalidResult", err)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 || len(sink.events) != 0 {
		t.Fatalf("atomic optional-MV failure published schema=%d rows=%d events=%v", sink.setCalls, len(sink.rows), sink.events)
	}
	if !rows.closed {
		t.Fatal("atomic optional-MV failure left backend rows open")
	}
}

func TestV03OptionalMultivalueTransportRejectsForgedHeadersTypesAndDescriptors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*clickhouse.CompiledQuery, *[]string, *[]driver.ColumnType, *int)
	}{
		{name: "descriptor out of range", mutate: func(query *clickhouse.CompiledQuery, _ *[]string, _ *[]driver.ColumnType, _ *int) {
			query.OptionalMultivalueOutputs[0].OutputIndex = 2
		}},
		{name: "duplicate descriptor", mutate: func(query *clickhouse.CompiledQuery, _ *[]string, _ *[]driver.ColumnType, _ *int) {
			query.OptionalMultivalueOutputs = append(query.OptionalMultivalueOutputs, query.OptionalMultivalueOutputs[0])
		}},
		{name: "container descriptor overlap", mutate: func(query *clickhouse.CompiledQuery, columns *[]string, types *[]driver.ColumnType, _ *int) {
			container := clickhouse.ResultContainerOutput{OutputIndex: 1}
			query.ContainerOutputs = []clickhouse.ResultContainerOutput{container}
			*columns = []string{"event_id", "tags", container.NamesColumn(), container.TypesColumn(), container.MetadataVersionColumn(), query.OptionalMultivalueOutputs[0].PresentColumn()}
			*types = append(containerOutputColumnTypes(container), (*types)[2])
		}},
		{name: "private column omitted", mutate: func(_ *clickhouse.CompiledQuery, columns *[]string, types *[]driver.ColumnType, _ *int) {
			*columns = (*columns)[:2]
			*types = (*types)[:2]
		}},
		{name: "private column reordered", mutate: func(_ *clickhouse.CompiledQuery, columns *[]string, _ *[]driver.ColumnType, _ *int) {
			(*columns)[1], (*columns)[2] = (*columns)[2], (*columns)[1]
		}},
		{name: "array is nullable", mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType, _ *int) {
			value := (*types)[1].(fakeColumnType)
			value.nullable = true
			(*types)[1] = value
		}},
		{name: "array hides wrapper", mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType, _ *int) {
			value := (*types)[1].(fakeColumnType)
			value.databaseType = "LowCardinality(Array(String))"
			(*types)[1] = value
		}},
		{name: "array scan type", mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType, _ *int) {
			value := (*types)[1].(fakeColumnType)
			value.scanType = reflect.TypeOf([]any{})
			(*types)[1] = value
		}},
		{name: "presence nullable", mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType, _ *int) {
			value := (*types)[2].(fakeColumnType)
			value.nullable = true
			(*types)[2] = value
		}},
		{name: "presence wrong type", mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType, _ *int) {
			value := (*types)[2].(fakeColumnType)
			value.databaseType = "Bool"
			(*types)[2] = value
		}},
		{name: "sparse fields payload overlap", mutate: func(query *clickhouse.CompiledQuery, columns *[]string, types *[]driver.ColumnType, sparse *int) {
			query.OutputFields[1] = "fields"
			query.SparseFields = true
			(*columns)[1] = "fields"
			*columns = append((*columns)[:2], append([]string{clickhouse.SparseEventFieldNamesColumn}, (*columns)[2:]...)...)
			jsonType := fakeColumnType{name: "fields", databaseType: "JSON", scanType: reflect.TypeOf((*any)(nil)).Elem()}
			*types = append((*types)[:2], append([]driver.ColumnType{jsonType}, (*types)[2:]...)...)
			*sparse = 1
		}},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			descriptor := clickhouse.ResultOptionalMultivalueOutput{OutputIndex: 1}
			query := clickhouse.CompiledQuery{
				OutputFields:              []string{"event_id", "tags"},
				OptionalMultivalueOutputs: []clickhouse.ResultOptionalMultivalueOutput{descriptor},
			}
			columns := []string{"event_id", "tags", descriptor.PresentColumn()}
			types := v03OptionalMultivalueColumnTypes(descriptor)
			sparse := -1
			test.mutate(&query, &columns, &types, &sparse)
			if _, _, err := validateOrdinaryResultColumns(query, columns, types, sparse); !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("validate forged transport error = %v, want ErrInvalidResult", err)
			}
		})
	}
}

func v03OptionalMultivalueColumnTypes(
	descriptor clickhouse.ResultOptionalMultivalueOutput,
) []driver.ColumnType {
	return []driver.ColumnType{
		fakeColumnType{name: "event_id", databaseType: "String", scanType: reflect.TypeOf("")},
		fakeColumnType{name: "tags", databaseType: "Array(String)", scanType: reflect.TypeOf([]string{})},
		fakeColumnType{name: descriptor.PresentColumn(), databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0))},
	}
}

func v03CompileRealMakeMVTransport(t *testing.T) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(`index=main | makemv delim="," allowempty=true tags | table event_id tags`)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	visibility := uint64(7)
	earliest := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)
	latest := earliest.Add(time.Hour)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant-v03-transport",
		AuthorizedIndexes: []string{"main"},
		Earliest:          earliest,
		Latest:            latest,
		SearchStart:       latest,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   latest.Add(time.Second),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return compiled
}
