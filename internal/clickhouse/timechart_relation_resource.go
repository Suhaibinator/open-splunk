package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	driverproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func relationInputMaximumBytes(ctx context.Context) uint64 {
	policy := searchlimits.Default()
	if admitted, ok := searchlimits.FromContext(ctx); ok {
		policy = admitted
	}
	maximum := min(policy.MaxResultBytes, policy.MaxMemoryBytes)
	if remaining, ok := searchlimits.RemainingExecutionBytes(ctx); ok {
		maximum = min(maximum, remaining)
	}
	return maximum
}

type relationInputPreflight struct {
	retainedBytes      uint64
	nativeBytes        uint64
	dynamicColumnCount int
}

// preflightRelationInput validates and sizes every retained and native backing
// before newRelationInput allocates cell payload.
func preflightRelationInput(
	ctx context.Context,
	columns []RelationColumn,
	rows [][]any,
	ends []time.Time,
	maxRows uint64,
	maximum uint64,
) (relationInputPreflight, error) {
	if ctx == nil {
		return relationInputPreflight{}, errors.New("materialize timechart: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return relationInputPreflight{}, err
	}
	if ends != nil && (len(ends) != len(rows) || len(columns) == 0 || columns[0].Name != "_time") {
		return relationInputPreflight{}, errors.New("continue timechart: invalid bucket bounds")
	}
	walk := relationTraversal{ctx: ctx}
	if len(columns) == 0 {
		return relationInputPreflight{}, errors.New("materialize timechart: empty schema")
	}
	if uint64(len(rows)) > maxRows {
		return relationInputPreflight{}, fmt.Errorf("%w: materialize timechart row limit exceeded", ErrTimechartResourceLimit)
	}
	retained := uint64(unsafe.Sizeof(compiledRelationInput{}))
	chargeRetained := func(value uint64) bool {
		if retained > maximum || value > maximum-retained {
			return false
		}
		retained += value
		return true
	}
	if !chargeProduct(uint64(len(columns)), uint64(unsafe.Sizeof(RelationColumn{})), chargeRetained) ||
		!chargeProduct(uint64(len(rows)), uint64(unsafe.Sizeof([]any{})), chargeRetained) {
		return relationInputPreflight{}, fmt.Errorf("%w: materialize timechart schema capacity exceeds byte limit", ErrTimechartResourceLimit)
	}
	if ends != nil && !chargeProduct(uint64(len(ends)), uint64(unsafe.Sizeof(time.Time{})), chargeRetained) {
		return relationInputPreflight{}, fmt.Errorf("%w: continue timechart bucket bounds exceed byte limit", ErrTimechartResourceLimit)
	}

	native := uint64(unsafe.Sizeof(ext.Table{})) +
		uint64(unsafe.Sizeof(driverproto.Block{})) +
		uint64(unsafe.Sizeof(column.ServerContext{}))
	addNative := func(value uint64) bool {
		var ok bool
		native, ok = retainedAdd(native, value)
		return ok
	}
	physicalColumns := len(columns)
	if ends != nil {
		physicalColumns++
	}
	perColumn := 2*uint64(unsafe.Sizeof("")) +
		3*uint64(unsafe.Sizeof(any(nil))) +
		uint64(unsafe.Sizeof((func(*ext.Table) error)(nil)))
	if !chargeProduct(uint64(physicalColumns), perColumn, addNative) {
		return relationInputPreflight{}, errors.New("materialize timechart: native relation is invalid")
	}
	dynamicColumns := 0
	for _, column := range columns {
		if !walk.step() {
			return relationInputPreflight{}, walk.err
		}
		if column.Name == "" || !utf8.ValidString(column.Name) {
			return relationInputPreflight{}, errors.New("materialize timechart: invalid schema name")
		}
		if !relationColumnTypeSupported(column.Type) {
			return relationInputPreflight{}, errors.New("materialize timechart: unsupported scalar type")
		}
		if !chargeRetained(uint64(len(column.Name))) || !chargeRetained(uint64(len(column.Type))) {
			return relationInputPreflight{}, fmt.Errorf("%w: materialize timechart schema exceeds byte limit", ErrTimechartResourceLimit)
		}
		if !addNative(nativeColumnDescriptorBytes(column)) ||
			!addNative(nativeColumnInitialCapacityBytes(column.Type, len(rows))) {
			return relationInputPreflight{}, errors.New("materialize timechart: native relation is invalid")
		}
		if column.Type == "Dynamic" {
			dynamicColumns++
		}
	}
	if ends != nil && (!addNative(nativeColumnDescriptorBytes(
		RelationColumn{Name: ResultTimeBucketEndColumn, Type: "DateTime64(9, 'UTC')"},
	)) || !addNative(nativeColumnInitialCapacityBytes("DateTime64(9, 'UTC')", len(rows)))) {
		return relationInputPreflight{}, errors.New("materialize timechart: native relation is invalid")
	}
	if !chargeProduct(uint64(dynamicColumns), uint64(unsafe.Sizeof(int(0))), addNative) {
		return relationInputPreflight{}, errors.New("materialize timechart: native relation is invalid")
	}
	for rowIndex, row := range rows {
		if err := ctx.Err(); err != nil {
			return relationInputPreflight{}, err
		}
		if len(row) != len(columns) {
			return relationInputPreflight{}, errors.New("materialize timechart: row width is invalid")
		}
		if ends != nil {
			start, ok := row[0].(time.Time)
			if !ok || !start.Before(ends[rowIndex]) {
				return relationInputPreflight{}, errors.New("continue timechart: invalid bucket interval")
			}
			if !addNative(2 * uint64(unsafe.Sizeof(time.Time{}))) {
				return relationInputPreflight{}, errors.New("materialize timechart: native relation is invalid")
			}
		}
		if !chargeProduct(uint64(len(row)), uint64(unsafe.Sizeof(any(nil))), chargeRetained) {
			return relationInputPreflight{}, fmt.Errorf("%w: materialize timechart cell capacity exceeds byte limit", ErrTimechartResourceLimit)
		}
		for index, column := range columns {
			retainedCell, nativeCell, ok := walk.preflightTypedValue(column.Type, row[index])
			if walk.err != nil {
				return relationInputPreflight{}, walk.err
			}
			if !ok {
				return relationInputPreflight{}, errors.New("materialize timechart: cell type is invalid")
			}
			if !chargeRetained(retainedCell) {
				return relationInputPreflight{}, fmt.Errorf("%w: materialize timechart cells exceed byte limit", ErrTimechartResourceLimit)
			}
			if !addNative(nativeCell) {
				return relationInputPreflight{}, errors.New("materialize timechart: native relation is invalid")
			}
		}
	}
	names := make([]string, len(columns))
	for index, column := range columns {
		if !walk.step() {
			return relationInputPreflight{}, walk.err
		}
		names[index] = column.Name
	}
	slices.Sort(names)
	for index := 1; index < len(names); index++ {
		if names[index] == names[index-1] {
			return relationInputPreflight{}, errors.New("materialize timechart: invalid schema name")
		}
	}
	if err := ctx.Err(); err != nil {
		return relationInputPreflight{}, err
	}
	return relationInputPreflight{
		retainedBytes:      retained,
		nativeBytes:        native,
		dynamicColumnCount: dynamicColumns,
	}, nil
}

// relationInputNativeMaterializationBytes returns the additional modeled heap
// needed while the immutable relation remains live and clickhouse-go builds
// its external-table block. Slice-backed driver columns are charged at twice
// their populated length, covering append capacity without multiplying the
// already-retained relation or the stage's other three reserved shares.
func relationInputNativeMaterializationBytes(
	ctx context.Context,
	input *compiledRelationInput,
) (uint64, bool, error) {
	if ctx == nil || input == nil || len(input.columns) == 0 || input.nativeBytes == 0 ||
		input.dynamicColumnCount < 0 || input.dynamicColumnCount > len(input.columns) {
		return 0, false, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	return input.nativeBytes, true, nil
}

func nativeColumnInitialCapacityBytes(kind string, rows int) uint64 {
	if rows == 0 {
		return 0
	}
	var minimum uint64
	switch relationBaseType(kind) {
	case "Bool":
		// The first append to byte/bool storage reserves at least eight bytes.
		minimum = 8
	case "String":
		// Even an empty string appends one start/end position.
		minimum = uint64(unsafe.Sizeof(chproto.Position{}))
	}
	if strings.HasPrefix(kind, "Nullable(") {
		// Nullable always appends one byte to its null map and one value to its
		// base column, including nil rows. Both first appends need a backing.
		minimum += 8
		switch relationBaseType(kind) {
		case "UInt64", "Int64", "Float64", "DateTime64(9, 'UTC')", "Dynamic":
			minimum += 8
		}
	}
	return minimum
}

func nativeColumnDescriptorBytes(descriptor RelationColumn) uint64 {
	// Physical relation names consist of this fixed prefix and one decimal
	// ordinal. Twenty digits cover every possible machine-sized index.
	physicalNameBytes := uint64(len("__os_relation_") + 20)
	if descriptor.Name == ResultTimeBucketEndColumn {
		physicalNameBytes = uint64(len(ResultTimeBucketEndColumn))
	}
	base := uint64(unsafe.Sizeof(column.Dynamic{}))
	switch relationBaseType(descriptor.Type) {
	case "String":
		base = uint64(unsafe.Sizeof(column.String{}))
	case "Bool":
		base = uint64(unsafe.Sizeof(column.Bool{}))
	case "DateTime64(9, 'UTC')":
		base = uint64(unsafe.Sizeof(column.DateTime64{}))
	}
	if strings.HasPrefix(descriptor.Type, "Nullable(") {
		base += uint64(unsafe.Sizeof(column.Nullable{}))
	}
	return base + physicalNameBytes + uint64(len(descriptor.Type))
}

func (walk *relationTraversal) preflightTypedValue(kind string, value any) (uint64, uint64, bool) {
	if kind == "Dynamic" {
		return walk.preflightDynamicValue(value, 0, true)
	}
	if !walk.step() {
		return 0, 0, false
	}
	if !relationScalarValueValid(kind, value) {
		return 0, 0, false
	}
	retained, ok := retainedCompiledArgument(value)
	if !ok {
		return 0, 0, false
	}
	native, ok := nativeScalarCellBytes(kind, value)
	return retained, native, ok
}

func nativeScalarCellBytes(kind string, value any) (uint64, bool) {
	nullable := strings.HasPrefix(kind, "Nullable(")
	if value == nil {
		if relationBaseType(kind) == "Dynamic" {
			if nullable {
				// Nullable records the null bit, then Dynamic appends its null
				// discriminator. No temporary chcol.Dynamic wrapper is built.
				return 4*uint64(unsafe.Sizeof(int(0))) + 2, true
			}
			return 0, false
		}
		if !nullable {
			return 0, false
		}
		// clickhouse-go's Nullable.AppendRow always appends nil to the base
		// column after recording the null bit. Model that zero value with the
		// same append-capacity charge as a present scalar.
		switch relationBaseType(kind) {
		case "UInt64", "Int64", "Float64", "DateTime64(9, 'UTC')":
			return 2*uint64(unsafe.Sizeof(uint64(0))) + 2, true
		case "String":
			return 2*uint64(unsafe.Sizeof(chproto.Position{})) + 2, true
		case "Bool":
			return 4, true
		default:
			return 0, false
		}
	}
	var bytes uint64
	switch relationBaseType(kind) {
	case "UInt64", "Int64", "Float64":
		bytes = 2 * uint64(unsafe.Sizeof(uint64(0)))
	case "Bool":
		bytes = 2
	case "DateTime64(9, 'UTC')":
		bytes = 2 * uint64(unsafe.Sizeof(int64(0)))
	case "String":
		text, ok := value.(string)
		positionBytes := 2 * uint64(unsafe.Sizeof(chproto.Position{}))
		if !ok || uint64(len(text)) > (math.MaxUint64-positionBytes)/2 {
			return 0, false
		}
		bytes = 2*uint64(len(text)) + positionBytes
	default:
		return 0, false
	}
	if nullable {
		bytes += 2
	}
	return bytes, true
}

func (walk *relationTraversal) preflightDynamicValue(value any, depth int, root bool) (uint64, uint64, bool) {
	if !walk.step() {
		return 0, 0, false
	}
	if depth > 17 {
		return 0, 0, false
	}
	var native uint64
	if root {
		native = uint64(unsafe.Sizeof(chcol.Dynamic{}))
	}
	// Both discriminator and offset columns grow by append. Charge twice the
	// populated width for each, hence four machine integers per Dynamic node.
	var ok bool
	native, ok = retainedAdd(native, 4*uint64(unsafe.Sizeof(int(0))))
	if !ok {
		return 0, 0, false
	}
	if items, list := value.([]any); list {
		retained := uint64(unsafe.Sizeof([]any{}))
		if uint64(len(items)) > math.MaxUint64/uint64(unsafe.Sizeof(any(nil))) {
			return 0, 0, false
		}
		retained, ok = retainedAdd(retained, uint64(len(items))*uint64(unsafe.Sizeof(any(nil))))
		if !ok {
			return 0, 0, false
		}
		// nativeRelationDynamic materializes one exact []Dynamic backing. The
		// driver Array additionally retains an offset with append headroom.
		if uint64(len(items)) > math.MaxUint64/uint64(unsafe.Sizeof(chcol.Dynamic{})) {
			return 0, 0, false
		}
		native, ok = retainedAdd(native, uint64(len(items))*uint64(unsafe.Sizeof(chcol.Dynamic{})))
		if !ok {
			return 0, 0, false
		}
		native, ok = retainedAdd(native, 2*uint64(unsafe.Sizeof(uint64(0))))
		if !ok {
			return 0, 0, false
		}
		for _, item := range items {
			childRetained, childNative, childOK := walk.preflightDynamicValue(item, depth+1, false)
			if !childOK {
				return 0, 0, false
			}
			retained, ok = retainedAdd(retained, childRetained)
			if !ok {
				return 0, 0, false
			}
			native, ok = retainedAdd(native, childNative)
			if !ok {
				return 0, 0, false
			}
		}
		return retained, native, true
	}
	switch value.(type) {
	case nil, string, int64, uint64, float64, bool, time.Time:
	default:
		return 0, 0, false
	}
	storage, supported := retainedCompiledArgument(value)
	if !supported {
		return 0, 0, false
	}
	// Primitive driver columns use append-backed storage. String payload and
	// offsets are both covered by doubling the retained scalar representation.
	if storage > math.MaxUint64/2 {
		return 0, 0, false
	}
	native, ok = retainedAdd(native, 2*storage)
	return storage, native, ok
}

func relationColumnTypeSupported(kind string) bool {
	switch relationBaseType(kind) {
	case "UInt64", "Int64", "Float64", "String", "Dynamic", "Bool", "DateTime64(9, 'UTC')":
		return true
	default:
		return false
	}
}

func chargeProduct(count, size uint64, charge func(uint64) bool) bool {
	if size != 0 && count > math.MaxUint64/size {
		return false
	}
	return charge(count * size)
}
