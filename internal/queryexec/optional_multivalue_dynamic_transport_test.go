package queryexec

import (
	"context"
	"errors"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestOptionalDynamicMultivalueTransportPublishesNativeScalarMembers(t *testing.T) {
	t.Parallel()

	descriptor := clickhouse.ResultOptionalMultivalueOutput{OutputIndex: 0}
	query := clickhouse.CompiledQuery{
		OutputFields:              []string{"members"},
		OptionalMultivalueOutputs: []clickhouse.ResultOptionalMultivalueOutput{descriptor},
	}
	types := []driver.ColumnType{
		fakeColumnType{
			name:         "members",
			databaseType: "Array(Dynamic)",
			scanType:     reflect.TypeFor[[]chcol.Dynamic](),
		},
		fakeColumnType{
			name:         descriptor.PresentColumn(),
			databaseType: "UInt8",
			scanType:     reflect.TypeFor[uint8](),
		},
	}
	_, transports, err := validateOrdinaryResultColumns(
		query,
		[]string{"members", descriptor.PresentColumn()},
		types,
		-1,
	)
	if err != nil {
		t.Fatalf("validate Array(Dynamic) transport: %v", err)
	}
	if len(transports) != 1 || !transports[0].valid || !transports[0].dynamic ||
		transports[0].presentColumn != 1 {
		t.Fatalf("dynamic optional transport = %#v", transports)
	}

	wide := new(big.Int)
	wide.SetString("170141183460469231731687303715884105728", 10)
	members := []chcol.Dynamic{
		chcol.NewDynamicWithType("alpha", "String"),
		chcol.NewDynamicWithType(int64(-7), "Int64"),
		chcol.NewDynamicWithType(uint64(9), "UInt64"),
		chcol.NewDynamicWithType(float64(1.25), "Float64"),
		chcol.NewDynamicWithType(true, "Bool"),
		chcol.NewDynamicWithType(nil, ""),
		chcol.NewDynamicWithType(map[string]string{
			extendedTypeKey:  "decimal/v1",
			extendedValueKey: "-123.4500e+2",
		}, "Map(String, String)"),
		chcol.NewDynamicWithType(wide, "Int256"),
	}
	present := uint8(1)
	converted, err := convertOptionalMultivalueOutput(
		[]any{&members, &present},
		0,
		transports[0],
	)
	if err != nil {
		t.Fatalf("convert Array(Dynamic) transport: %v", err)
	}
	items, ok := converted.List()
	if !ok || len(items) != len(members) {
		t.Fatalf("converted dynamic multivalue = %#v", converted)
	}
	if value, valueOK := items[0].String(); !valueOK || value != "alpha" {
		t.Fatalf("String member = %#v", items[0])
	}
	if value, valueOK := items[1].Signed(); !valueOK || value != -7 {
		t.Fatalf("signed member = %#v", items[1])
	}
	if value, valueOK := items[2].Unsigned(); !valueOK || value != 9 {
		t.Fatalf("unsigned member = %#v", items[2])
	}
	if value, valueOK := items[3].Double(); !valueOK || value != 1.25 {
		t.Fatalf("double member = %#v", items[3])
	}
	if value, valueOK := items[4].Bool(); !valueOK || !value {
		t.Fatalf("Boolean member = %#v", items[4])
	}
	if !items[5].IsNull() {
		t.Fatalf("null member = %#v", items[5])
	}
	if value, valueOK := items[6].Decimal(); !valueOK || value != "-123.4500e+2" {
		t.Fatalf("tagged Decimal member = %#v", items[6])
	}
	if value, valueOK := items[7].Decimal(); !valueOK || value != wide.String() {
		t.Fatalf("wide Decimal member = %#v", items[7])
	}
}

func TestExecutorPublishesOptionalDynamicMultivalueWithNullEmptyDistinction(t *testing.T) {
	t.Parallel()

	descriptor := clickhouse.ResultOptionalMultivalueOutput{OutputIndex: 0}
	query := clickhouse.CompiledQuery{
		SQL:                       "SELECT members, present",
		OutputFields:              []string{"members"},
		OptionalMultivalueOutputs: []clickhouse.ResultOptionalMultivalueOutput{descriptor},
	}
	rows := &fakeRows{
		columns: []string{"members", descriptor.PresentColumn()},
		types: []driver.ColumnType{
			fakeColumnType{name: "members", databaseType: "Array(Dynamic)", scanType: reflect.TypeFor[[]chcol.Dynamic]()},
			fakeColumnType{name: descriptor.PresentColumn(), databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
		},
		data: [][]any{
			{[]chcol.Dynamic{}, uint8(0)},
			{[]chcol.Dynamic{}, uint8(2)},
			{[]chcol.Dynamic{}, uint8(1)},
			{[]chcol.Dynamic{
				chcol.NewDynamicWithType("alice", "String"),
				chcol.NewDynamicWithType(false, "Bool"),
				chcol.NewDynamicWithType(nil, ""),
			}, uint8(1)},
		},
	}
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute optional Array(Dynamic): %v", err)
	}
	if len(sink.schema.Columns) != 1 || sink.schema.Columns[0] != (searchjobs.Column{
		Name: "members", Kind: searchjobs.ValueKindList, Nullable: true, Multivalue: true,
	}) {
		t.Fatalf("optional dynamic MV schema = %#v", sink.schema)
	}
	if len(sink.rows) != 4 || !sink.rows[0][0].IsMissing() || !sink.rows[1][0].IsNull() {
		t.Fatalf("optional dynamic MV rows = %#v", sink.rows)
	}
	empty, emptyOK := sink.rows[2][0].List()
	native, nativeOK := sink.rows[3][0].List()
	if !emptyOK || len(empty) != 0 || !nativeOK || len(native) != 3 {
		t.Fatalf("optional dynamic MV values = %#v", sink.rows)
	}
}

func TestOptionalDynamicMultivalueTransportRejectsUnsupportedMembers(t *testing.T) {
	t.Parallel()

	invalidUTF8 := string([]byte{0xff})
	for _, test := range []struct {
		name   string
		member chcol.Dynamic
	}{
		{name: "semantic Bytes", member: chcol.NewDynamicWithType(invalidUTF8, "String")},
		{name: "non-finite NaN", member: chcol.NewDynamicWithType(math.NaN(), "Float64")},
		{name: "non-finite infinity", member: chcol.NewDynamicWithType(math.Inf(1), "Float64")},
		{name: "timestamp", member: chcol.NewDynamicWithType(time.Unix(1, 0), "DateTime64(9, 'UTC')")},
		{name: "nested list", member: chcol.NewDynamicWithType([]string{"nested"}, "Array(String)")},
		{name: "object", member: chcol.NewDynamicWithType(map[string]any{"nested": "value"}, "Map(String, Dynamic)")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value, err := convertOptionalDynamicMultivalue([]chcol.Dynamic{test.member})
			if err == nil || !reflect.DeepEqual(value, searchjobs.Value{}) {
				t.Fatalf("unsupported dynamic MV member = (%#v, %v)", value, err)
			}
		})
	}
}

func TestOptionalMultivalueTransportEnforcesSharedPerRowBounds(t *testing.T) {
	t.Parallel()

	t.Run("dynamic member count", func(t *testing.T) {
		raw := make([]chcol.Dynamic, maximumOptionalMultivalueMembers+1)
		for index := range raw {
			raw[index] = chcol.NewDynamicWithType(nil, "")
		}
		if _, err := convertOptionalDynamicMultivalue(raw); err == nil {
			t.Fatal("over-limit dynamic member count was accepted")
		}
	})

	t.Run("dynamic canonical payload", func(t *testing.T) {
		exact := []chcol.Dynamic{chcol.NewDynamicWithType(
			strings.Repeat("x", maximumOptionalMultivaluePayloadBytes),
			"String",
		)}
		if _, err := convertOptionalDynamicMultivalue(exact); err != nil {
			t.Fatalf("exact dynamic payload boundary: %v", err)
		}
		over := []chcol.Dynamic{chcol.NewDynamicWithType(
			strings.Repeat("x", maximumOptionalMultivaluePayloadBytes+1),
			"String",
		)}
		if _, err := convertOptionalDynamicMultivalue(over); err == nil {
			t.Fatal("over-limit dynamic payload was accepted")
		}
	})

	t.Run("String array bounds", func(t *testing.T) {
		present := uint8(1)
		transport := resultOptionalMultivalueTransport{valid: true, presentColumn: 1}
		tooMany := make([]string, maximumOptionalMultivalueMembers+1)
		if _, err := convertOptionalMultivalueOutput(
			[]any{&tooMany, &present}, 0, transport,
		); err == nil {
			t.Fatal("over-limit String member count was accepted")
		}
		overBytes := []string{strings.Repeat("x", maximumOptionalMultivaluePayloadBytes+1)}
		if _, err := convertOptionalMultivalueOutput(
			[]any{&overBytes, &present}, 0, transport,
		); err == nil {
			t.Fatal("over-limit String payload was accepted")
		}
	})
}

func TestOptionalDynamicMultivalueTransportRejectsForgedHeadersAndRows(t *testing.T) {
	t.Parallel()

	descriptor := clickhouse.ResultOptionalMultivalueOutput{OutputIndex: 0}
	query := clickhouse.CompiledQuery{
		OutputFields:              []string{"members"},
		OptionalMultivalueOutputs: []clickhouse.ResultOptionalMultivalueOutput{descriptor},
	}
	columns := []string{"members", descriptor.PresentColumn()}
	canonical := []driver.ColumnType{
		fakeColumnType{name: "members", databaseType: "Array(Dynamic)", scanType: reflect.TypeFor[[]chcol.Dynamic]()},
		fakeColumnType{name: descriptor.PresentColumn(), databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
	}
	for _, test := range []struct {
		name   string
		mutate func([]driver.ColumnType)
	}{
		{name: "generic slice scan type", mutate: func(types []driver.ColumnType) {
			value := types[0].(fakeColumnType)
			value.scanType = reflect.TypeFor[[]any]()
			types[0] = value
		}},
		{name: "hidden wrapper", mutate: func(types []driver.ColumnType) {
			value := types[0].(fakeColumnType)
			value.databaseType = "LowCardinality(Array(Dynamic))"
			types[0] = value
		}},
		{name: "nullable", mutate: func(types []driver.ColumnType) {
			value := types[0].(fakeColumnType)
			value.nullable = true
			types[0] = value
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			types := append([]driver.ColumnType(nil), canonical...)
			test.mutate(types)
			if _, _, err := validateOrdinaryResultColumns(query, columns, types, -1); !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("forged dynamic MV header error = %v, want ErrInvalidResult", err)
			}
		})
	}

	emptyStrings := []string{}
	present := uint8(1)
	if _, err := convertOptionalMultivalueOutput(
		[]any{&emptyStrings, &present},
		0,
		resultOptionalMultivalueTransport{valid: true, presentColumn: 1, dynamic: true},
	); err == nil {
		t.Fatal("dynamic transport accepted a []string native row")
	}
}
