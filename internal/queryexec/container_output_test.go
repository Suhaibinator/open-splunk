package queryexec

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestConvertContainerOutputReconstructsV1NestedValues(t *testing.T) {
	t.Parallel()

	names, types := containerOutputMetadata(
		containerOutputMetadataField{"bytes", eventfields.StoredValueTypeBytes},
		containerOutputMetadataField{"list", eventfields.StoredValueTypeList},
		containerOutputMetadataField{`literal\.dot`, eventfields.StoredValueTypeString},
		containerOutputMetadataField{`nested.slash\\key`, eventfields.StoredValueTypeString},
		containerOutputMetadataField{"nothing", eventfields.StoredValueTypeNull},
	)
	raw := map[string]any{
		"bytes": map[string]string{
			extendedTypeKey:  "bytes/v1",
			extendedValueKey: "AP8",
		},
		"list": []any{
			int64(7),
			nil,
			"x",
			map[string]any{"50%": "percent", "a%2Eb": "verbatim"},
		},
		eventfields.EncodePhysicalPathSegment("literal.dot"): "escaped",
		"nested": map[string]any{
			eventfields.EncodePhysicalPathSegment(`slash\key`): "nested-value",
		},
		// ClickHouse JSON extraction omits the explicit-null member. Its
		// authoritative relative name and v1 type restore it below.
	}

	got, err := convertContainerOutput(
		chcol.NewDynamicWithType(raw, "Map(String, Dynamic)"),
		names,
		types,
		eventfields.CurrentFieldMetadataVersion,
	)
	if err != nil {
		t.Fatalf("convertContainerOutput(v1): %v", err)
	}
	root := containerOutputObject(t, got)
	if value, ok := root["literal.dot"].String(); !ok || value != "escaped" {
		t.Fatalf("escaped literal-dot field = %#v", root["literal.dot"])
	}
	if !root["nothing"].IsNull() {
		t.Fatalf("explicit null = %#v", root["nothing"])
	}
	if value, ok := root["bytes"].Bytes(); !ok || !slices.Equal(value, []byte{0, 0xff}) {
		t.Fatalf("bytes field = %#v", root["bytes"])
	}
	list, ok := root["list"].List()
	if !ok || len(list) != 4 {
		t.Fatalf("list field = %#v", root["list"])
	}
	if value, valueOK := list[0].Signed(); !valueOK || value != 7 ||
		!list[1].IsNull() {
		t.Fatalf("list contents = %#v", list)
	}
	if value, valueOK := list[2].String(); !valueOK || value != "x" {
		t.Fatalf("list string = %#v", list[2])
	}
	listObject := containerOutputObject(t, list[3])
	if value, valueOK := listObject["50%"].String(); !valueOK || value != "percent" {
		t.Fatalf("list percent key = %#v", listObject)
	}
	if value, valueOK := listObject["a%2Eb"].String(); !valueOK || value != "verbatim" {
		t.Fatalf("list encoded-looking key = %#v", listObject)
	}
	nested := containerOutputObject(t, root["nested"])
	if value, ok := nested[`slash\key`].String(); !ok || value != "nested-value" {
		t.Fatalf("escaped nested field = %#v", nested[`slash\key`])
	}
}

func TestConvertContainerOutputReconstructsDynamicJSON(t *testing.T) {
	t.Parallel()

	document := chcol.NewJSON()
	document.SetValueAtPath("child", chcol.NewDynamic("value"))
	document.SetValueAtPath("nested.count", chcol.NewDynamic(int64(7)))
	value := chcol.NewDynamicWithType(document, "JSON")
	names, types := containerOutputMetadata(
		containerOutputMetadataField{"child", eventfields.StoredValueTypeString},
		containerOutputMetadataField{"nested.count", eventfields.StoredValueTypeSint64},
		containerOutputMetadataField{"nothing", eventfields.StoredValueTypeNull},
	)

	got, err := convertContainerOutput(
		value,
		names,
		types,
		eventfields.CurrentFieldMetadataVersion,
	)
	if err != nil {
		t.Fatalf("convertContainerOutput(Dynamic(JSON)): %v", err)
	}
	root := containerOutputObject(t, got)
	if child, ok := root["child"].String(); !ok || child != "value" {
		t.Fatalf("Dynamic(JSON) child = %#v", root["child"])
	}
	nested := containerOutputObject(t, root["nested"])
	if count, ok := nested["count"].Signed(); !ok || count != 7 {
		t.Fatalf("Dynamic(JSON) nested count = %#v", nested["count"])
	}
	if !root["nothing"].IsNull() {
		t.Fatalf("Dynamic(JSON) explicit null = %#v", root["nothing"])
	}
}

func TestConvertContainerOutputSupportsLegacyV0Names(t *testing.T) {
	t.Parallel()

	got, err := convertContainerOutput(
		map[string]any{"legacy": "value"},
		[]string{"legacy", "nothing"},
		[]uint8{},
		0,
	)
	if err != nil {
		t.Fatalf("convertContainerOutput(v0): %v", err)
	}
	root := containerOutputObject(t, got)
	if value, ok := root["legacy"].String(); !ok || value != "value" {
		t.Fatalf("legacy field = %#v", root["legacy"])
	}
	if !root["nothing"].IsNull() {
		t.Fatalf("legacy absent native member = %#v, want explicit null", root["nothing"])
	}
}

func TestConvertContainerOutputIgnoresSharedNullOnlyPaths(t *testing.T) {
	t.Parallel()
	nullJSON := chcol.NewJSON()
	nullJSON.SetValueAtPath("child", chcol.NewDynamic(nil))

	got, err := convertContainerOutput(
		map[string]any{
			"kept":    "value",
			"phantom": chcol.NewDynamicWithType(nil, ""),
			"nested_phantom": chcol.NewDynamicWithType(
				map[string]chcol.Dynamic{
					"child": chcol.NewDynamicWithType(nil, ""),
				},
				"Map(String, Dynamic)",
			),
			"json_phantom": chcol.NewDynamicWithType(nullJSON, "JSON"),
		},
		[]string{"kept"},
		[]uint8{uint8(eventfields.StoredValueTypeString)},
		eventfields.CurrentFieldMetadataVersion,
	)
	if err != nil {
		t.Fatalf("convertContainerOutput(shared null paths): %v", err)
	}
	root := containerOutputObject(t, got)
	if len(root) != 1 {
		t.Fatalf("shared null-only paths leaked: %#v", root)
	}
	if value, ok := root["kept"].String(); !ok || value != "value" {
		t.Fatalf("kept value = %#v", root["kept"])
	}
}

func TestConvertContainerOutputRejectsInvalidMetadataAndValues(t *testing.T) {
	t.Parallel()

	stringType := uint8(eventfields.StoredValueTypeString)
	signedType := uint8(eventfields.StoredValueTypeSint64)
	tests := []struct {
		name    string
		value   any
		names   []string
		types   []uint8
		version uint8
	}{
		{
			name: "v1 length mismatch", value: map[string]any{"a": "value"},
			names: []string{"a"}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			name: "legacy carries types", value: map[string]any{"a": "value"},
			names: []string{"a"}, types: []uint8{stringType}, version: 0,
		},
		{
			name: "future version", value: map[string]any{"a": "value"},
			names: []string{"a"}, types: []uint8{stringType}, version: 2,
		},
		{
			name: "invalid type code", value: map[string]any{"a": "value"},
			names: []string{"a"}, types: []uint8{0}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			name: "invalid path escape", value: map[string]any{"a": "value"},
			names: []string{`a\q`}, types: []uint8{stringType}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			name: "unsorted paths", value: map[string]any{"a": "a", "b": "b"},
			names: []string{"b", "a"}, types: []uint8{stringType, stringType}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			name: "path collision", value: map[string]any{"a": map[string]any{"b": "value"}},
			names: []string{"a", "a.b"}, types: []uint8{stringType, stringType}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			name: "missing non-null value", value: map[string]any{},
			names: []string{"a"}, types: []uint8{stringType}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			name: "stored type mismatch", value: map[string]any{"a": "value"},
			names: []string{"a"}, types: []uint8{signedType}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			name: "extra native value", value: map[string]any{"a": "value", "extra": "poison"},
			names: []string{"a"}, types: []uint8{stringType}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			name: "non-object container", value: "value",
			names: []string{"a"}, types: []uint8{stringType}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			name:  "decoded key collision",
			value: map[string]any{"a.b": "one", "a%2Eb": "two"},
			names: []string{`a\.b`}, types: []uint8{stringType}, version: eventfields.CurrentFieldMetadataVersion,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := convertContainerOutput(
				test.value,
				test.names,
				test.types,
				test.version,
			); err == nil {
				t.Fatal("convertContainerOutput unexpectedly succeeded")
			}
		})
	}
}

func TestExecutorContainerOutputStripsHiddenColumns(t *testing.T) {
	t.Parallel()

	descriptor := testResultContainerOutput(1)
	names, types := containerOutputMetadata(
		containerOutputMetadataField{"nothing", eventfields.StoredValueTypeNull},
		containerOutputMetadataField{"value", eventfields.StoredValueTypeString},
	)
	rows := &fakeRows{
		columns: []string{
			"event_id",
			"payload",
			descriptor.NamesColumn(),
			descriptor.TypesColumn(),
			descriptor.MetadataVersionColumn(),
		},
		types: containerOutputColumnTypes(descriptor),
		data: [][]any{{
			"event-1",
			chcol.NewDynamicWithType(
				map[string]any{"value": "visible"},
				"Map(String, Dynamic)",
			),
			names,
			types,
			eventfields.CurrentFieldMetadataVersion,
		}},
	}
	sink := &fakeSink{}
	query := clickhouse.CompiledQuery{
		SQL:              "SELECT container output",
		OutputFields:     []string{"event_id", "payload"},
		ContainerOutputs: []clickhouse.ResultContainerOutput{descriptor},
	}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute(container output): %v", err)
	}
	if sink.setCalls != 1 || len(sink.schema.Columns) != 2 ||
		sink.schema.Columns[0].Name != "event_id" ||
		sink.schema.Columns[1].Name != "payload" {
		t.Fatalf("public schema = %#v, calls=%d", sink.schema, sink.setCalls)
	}
	if len(sink.rows) != 1 || len(sink.rows[0]) != 2 {
		t.Fatalf("public rows = %#v", sink.rows)
	}
	if value, ok := sink.rows[0][0].String(); !ok || value != "event-1" {
		t.Fatalf("event id = %#v", sink.rows[0][0])
	}
	payload := containerOutputObject(t, sink.rows[0][1])
	if value, ok := payload["value"].String(); !ok || value != "visible" ||
		!payload["nothing"].IsNull() {
		t.Fatalf("container payload = %#v", payload)
	}
}

func TestExecutorContainerOutputCoexistsWithSparseFields(t *testing.T) {
	t.Parallel()

	descriptor := testResultContainerOutput(1)
	document := chcol.NewJSON()
	document.SetValueAtPath("visible", chcol.NewDynamicWithType("raw", "String"))
	containerNames, containerTypes := containerOutputMetadata(
		containerOutputMetadataField{"child", eventfields.StoredValueTypeString},
	)
	rows := &fakeRows{
		columns: []string{
			"fields",
			"payload",
			clickhouse.SparseEventFieldNamesColumn,
			descriptor.NamesColumn(),
			descriptor.TypesColumn(),
			descriptor.MetadataVersionColumn(),
		},
		types: []driver.ColumnType{
			fakeColumnType{
				name: "fields", databaseType: "JSON(max_dynamic_paths=256)",
				scanType: reflect.TypeOf((*chcol.JSON)(nil)),
			},
			fakeColumnType{
				name: "payload", databaseType: "Dynamic",
				scanType: reflect.TypeOf((*any)(nil)).Elem(),
			},
			fakeColumnType{
				name:         clickhouse.SparseEventFieldNamesColumn,
				databaseType: "Array(String)", scanType: reflect.TypeOf([]string{}),
			},
			fakeColumnType{
				name:         descriptor.NamesColumn(),
				databaseType: "Array(String)", scanType: reflect.TypeOf([]string{}),
			},
			fakeColumnType{
				name:         descriptor.TypesColumn(),
				databaseType: "Array(UInt8)", scanType: reflect.TypeOf([]uint8{}),
			},
			fakeColumnType{
				name:         descriptor.MetadataVersionColumn(),
				databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0)),
			},
		},
		data: [][]any{{
			document,
			chcol.NewDynamicWithType(map[string]any{"child": "container"}, "Map(String, Dynamic)"),
			[]string{"visible"},
			containerNames,
			containerTypes,
			eventfields.CurrentFieldMetadataVersion,
		}},
	}
	sink := &fakeSink{}
	query := clickhouse.CompiledQuery{
		SQL:              "SELECT sparse and container output",
		OutputFields:     []string{"fields", "payload"},
		SparseFields:     true,
		ContainerOutputs: []clickhouse.ResultContainerOutput{descriptor},
	}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute(sparse and container): %v", err)
	}
	if len(sink.schema.Columns) != 2 || len(sink.rows) != 1 || len(sink.rows[0]) != 2 {
		t.Fatalf("public sparse/container result = schema %#v rows %#v", sink.schema, sink.rows)
	}
	fields := containerOutputObject(t, sink.rows[0][0])
	payload := containerOutputObject(t, sink.rows[0][1])
	if value, ok := fields["visible"].String(); !ok || value != "raw" {
		t.Fatalf("sparse fields = %#v", fields)
	}
	if value, ok := payload["child"].String(); !ok || value != "container" {
		t.Fatalf("container payload = %#v", payload)
	}
}

func TestExecutorContainerOutputRejectsInvalidHeadersAndTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*clickhouse.CompiledQuery, *[]string, *[]driver.ColumnType)
	}{
		{
			name: "missing hidden column",
			mutate: func(_ *clickhouse.CompiledQuery, columns *[]string, types *[]driver.ColumnType) {
				*columns = (*columns)[:len(*columns)-1]
				*types = (*types)[:len(*types)-1]
			},
		},
		{
			name: "extra hidden column",
			mutate: func(_ *clickhouse.CompiledQuery, columns *[]string, types *[]driver.ColumnType) {
				*columns = append(*columns, "__os_result_container_extra")
				*types = append(*types, fakeColumnType{
					name: "__os_result_container_extra", databaseType: "UInt8",
					scanType: reflect.TypeOf(uint8(0)),
				})
			},
		},
		{
			name: "reordered hidden columns",
			mutate: func(_ *clickhouse.CompiledQuery, columns *[]string, types *[]driver.ColumnType) {
				(*columns)[2], (*columns)[3] = (*columns)[3], (*columns)[2]
				(*types)[2], (*types)[3] = (*types)[3], (*types)[2]
			},
		},
		{
			name: "public value is not Dynamic",
			mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType) {
				(*types)[1] = fakeColumnType{
					name: "payload", databaseType: "String", scanType: reflect.TypeOf(""),
				}
			},
		},
		{
			name: "public value has forged Dynamic prefix",
			mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType) {
				(*types)[1] = fakeColumnType{
					name: "payload", databaseType: "DynamicPoison", scanType: reflect.TypeOf((*any)(nil)).Elem(),
				}
			},
		},
		{
			name: "public value hides nullable Dynamic wrapper",
			mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType) {
				(*types)[1] = fakeColumnType{
					name: "payload", databaseType: "Nullable(Dynamic)", scanType: reflect.TypeOf((*any)(nil)).Elem(),
				}
			},
		},
		{
			name: "names database type",
			mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType) {
				(*types)[2] = fakeColumnType{
					name: "names", databaseType: "Array(UInt8)", scanType: reflect.TypeOf([]string{}),
				}
			},
		},
		{
			name: "types scan type",
			mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType) {
				(*types)[3] = fakeColumnType{
					name: "types", databaseType: "Array(UInt8)", scanType: reflect.TypeOf([]uint16{}),
				}
			},
		},
		{
			name: "metadata version nullable",
			mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType) {
				(*types)[4] = fakeColumnType{
					name: "version", databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0)), nullable: true,
				}
			},
		},
		{
			name: "metadata version database type is nullable",
			mutate: func(_ *clickhouse.CompiledQuery, _ *[]string, types *[]driver.ColumnType) {
				(*types)[4] = fakeColumnType{
					name: "version", databaseType: "LowCardinality(Nullable(UInt8))",
					scanType: reflect.TypeOf(uint8(0)),
				}
			},
		},
		{
			name: "duplicate descriptor",
			mutate: func(query *clickhouse.CompiledQuery, _ *[]string, _ *[]driver.ColumnType) {
				query.ContainerOutputs = append(query.ContainerOutputs, query.ContainerOutputs[0])
			},
		},
		{
			name: "descriptor output out of range",
			mutate: func(query *clickhouse.CompiledQuery, _ *[]string, _ *[]driver.ColumnType) {
				query.ContainerOutputs[0].OutputIndex = 2
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			descriptor := testResultContainerOutput(1)
			query := clickhouse.CompiledQuery{
				OutputFields:     []string{"event_id", "payload"},
				ContainerOutputs: []clickhouse.ResultContainerOutput{descriptor},
			}
			columns := []string{
				"event_id", "payload", descriptor.NamesColumn(),
				descriptor.TypesColumn(), descriptor.MetadataVersionColumn(),
			}
			columnTypes := containerOutputColumnTypes(descriptor)
			test.mutate(&query, &columns, &columnTypes)
			if _, err := validateOrdinaryResultColumns(
				query,
				columns,
				columnTypes,
				-1,
			); !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("validateOrdinaryResultColumns() error = %v, want ErrInvalidResult", err)
			}
		})
	}
}

func TestExecutorContainerOutputRejectsInvalidNativeMetadata(t *testing.T) {
	t.Parallel()

	names := []string{"value"}
	types := []uint8{uint8(eventfields.StoredValueTypeString)}
	version := eventfields.CurrentFieldMetadataVersion
	valid := []any{&names, &types, &version}
	transport := resultContainerTransport{
		valid: true, namesColumn: 0, typesColumn: 1, metadataVersionColumn: 2,
	}
	if gotNames, gotTypes, gotVersion, err := scannedContainerMetadata(
		valid,
		transport,
	); err != nil || !slices.Equal(gotNames, names) || !slices.Equal(gotTypes, types) ||
		gotVersion != version {
		t.Fatalf(
			"scannedContainerMetadata(valid) = %#v/%#v/%d, %v",
			gotNames,
			gotTypes,
			gotVersion,
			err,
		)
	}

	badNames := []uint8{1}
	badTypes := []string{"string"}
	badVersion := uint16(1)
	for _, destinations := range [][]any{
		{&badNames, &types, &version},
		{&names, &badTypes, &version},
		{&names, &types, &badVersion},
	} {
		if _, _, _, err := scannedContainerMetadata(destinations, transport); err == nil {
			t.Fatal("scannedContainerMetadata accepted invalid native metadata")
		}
	}
}

type containerOutputMetadataField struct {
	name       string
	storedType eventfields.StoredValueType
}

func containerOutputMetadata(
	fields ...containerOutputMetadataField,
) ([]string, []uint8) {
	sort.Slice(fields, func(left, right int) bool {
		return fields[left].name < fields[right].name
	})
	names := make([]string, len(fields))
	types := make([]uint8, len(fields))
	for index, field := range fields {
		names[index] = field.name
		types[index] = uint8(field.storedType)
	}
	return names, types
}

func testResultContainerOutput(index uint16) clickhouse.ResultContainerOutput {
	return clickhouse.ResultContainerOutput{OutputIndex: index}
}

func containerOutputColumnTypes(
	descriptor clickhouse.ResultContainerOutput,
) []driver.ColumnType {
	return []driver.ColumnType{
		fakeColumnType{name: "event_id", databaseType: "String", scanType: reflect.TypeOf("")},
		fakeColumnType{
			name: "payload", databaseType: "Dynamic", scanType: reflect.TypeOf((*any)(nil)).Elem(),
		},
		fakeColumnType{
			name: descriptor.NamesColumn(), databaseType: "Array(String)", scanType: reflect.TypeOf([]string{}),
		},
		fakeColumnType{
			name: descriptor.TypesColumn(), databaseType: "Array(UInt8)", scanType: reflect.TypeOf([]uint8{}),
		},
		fakeColumnType{
			name: descriptor.MetadataVersionColumn(), databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0)),
		},
	}
}

func containerOutputObject(
	t *testing.T,
	value searchjobs.Value,
) map[string]searchjobs.Value {
	t.Helper()
	fields, ok := value.Object()
	if !ok {
		t.Fatalf("container value = %#v, want object", value)
	}
	result := make(map[string]searchjobs.Value, len(fields))
	for _, field := range fields {
		result[field.Name] = field.Value
	}
	return result
}
