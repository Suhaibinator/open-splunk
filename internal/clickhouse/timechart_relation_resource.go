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

// relationInputRetainedBytes validates a closed timechart relation and returns
// the complete backing allocated by newRelationInput. It allocates no cell
// payload and rejects an over-budget shape before building its fixed-width
// name index, so an oversized nested value is never deep-copied.
func relationInputRetainedBytes(
	ctx context.Context,
	columns []RelationColumn,
	rows [][]any,
	maxRows uint64,
	maximum uint64,
) (uint64, error) {
	if ctx == nil {
		return 0, errors.New("materialize timechart: context is nil")
	}
	if len(columns) == 0 {
		return 0, errors.New("materialize timechart: empty schema")
	}
	if uint64(len(rows)) > maxRows {
		return 0, fmt.Errorf("%w: materialize timechart row limit exceeded", ErrTimechartResourceLimit)
	}
	retained := uint64(unsafe.Sizeof(compiledRelationInput{}))
	charge := func(value uint64) bool {
		if retained > maximum || value > maximum-retained {
			return false
		}
		retained += value
		return true
	}
	if !chargeProduct(uint64(len(columns)), uint64(unsafe.Sizeof(RelationColumn{})), charge) ||
		!chargeProduct(uint64(len(rows)), uint64(unsafe.Sizeof([]any{})), charge) {
		return 0, fmt.Errorf("%w: materialize timechart schema capacity exceeds byte limit", ErrTimechartResourceLimit)
	}
	for _, column := range columns {
		if column.Name == "" || !utf8.ValidString(column.Name) {
			return 0, errors.New("materialize timechart: invalid schema name")
		}
		if !relationColumnTypeSupported(column.Type) {
			return 0, errors.New("materialize timechart: unsupported scalar type")
		}
		if !charge(uint64(len(column.Name))) || !charge(uint64(len(column.Type))) {
			return 0, fmt.Errorf("%w: materialize timechart schema exceeds byte limit", ErrTimechartResourceLimit)
		}
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if len(row) != len(columns) ||
			!chargeProduct(uint64(len(row)), uint64(unsafe.Sizeof(any(nil))), charge) {
			return 0, fmt.Errorf("%w: materialize timechart cell capacity exceeds byte limit", ErrTimechartResourceLimit)
		}
		for index, value := range row {
			if !relationValueValid(columns[index].Type, value) {
				return 0, errors.New("materialize timechart: cell type is invalid")
			}
			size, ok := retainedRelationValue(value, 0)
			if !ok || !charge(size) {
				return 0, fmt.Errorf("%w: materialize timechart cells exceed byte limit", ErrTimechartResourceLimit)
			}
		}
	}
	names := make([]string, len(columns))
	for index, column := range columns {
		names[index] = column.Name
	}
	slices.Sort(names)
	for index := 1; index < len(names); index++ {
		if names[index] == names[index-1] {
			return 0, errors.New("materialize timechart: invalid schema name")
		}
	}
	return retained, nil
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
	if ctx == nil || input == nil || len(input.columns) == 0 {
		return 0, false, nil
	}
	columnCount := len(input.columns)
	if input.bucketEnds != nil {
		if len(input.bucketEnds) != len(input.rows) {
			return 0, false, nil
		}
		columnCount++
	}
	total := uint64(unsafe.Sizeof(ext.Table{})) +
		uint64(unsafe.Sizeof(driverproto.Block{})) +
		uint64(unsafe.Sizeof(column.ServerContext{}))
	add := func(value uint64) bool {
		var ok bool
		total, ok = retainedAdd(total, value)
		return ok
	}
	// NewTable retains its column names and interfaces. The definitions slice
	// and one row conversion coexist with that block during construction.
	if !chargeProduct(uint64(columnCount), 2*uint64(unsafe.Sizeof("")), add) ||
		!chargeProduct(uint64(columnCount), 2*uint64(unsafe.Sizeof(any(nil))), add) ||
		!chargeProduct(uint64(columnCount), uint64(unsafe.Sizeof((func(*ext.Table) error)(nil))), add) {
		return 0, false, nil
	}
	for _, descriptor := range input.columns {
		if !relationColumnTypeSupported(descriptor.Type) ||
			!add(nativeColumnDescriptorBytes(descriptor)) {
			return 0, false, nil
		}
	}
	if input.bucketEnds != nil && !add(nativeColumnDescriptorBytes(
		RelationColumn{Name: ResultTimeBucketEndColumn, Type: "DateTime64(9, 'UTC')"},
	)) {
		return 0, false, nil
	}
	rowWidth := uint64(len(input.columns))
	rowScratch := uint64(0)
	rowScratchOK := chargeProduct(rowWidth, uint64(unsafe.Sizeof(any(nil))), func(value uint64) bool {
		rowScratch = value
		return true
	})
	if input.bucketEnds != nil {
		// materializeRelationInput currently clones the row, then clones it a
		// second time while appending the private bucket end.
		if rowWidth == math.MaxUint64 || !rowScratchOK {
			return 0, false, nil
		}
		rowScratchOK = chargeProduct(rowWidth+1, uint64(unsafe.Sizeof(any(nil))), func(value uint64) bool {
			var ok bool
			rowScratch, ok = retainedAdd(rowScratch, value)
			return ok
		})
	}
	if !rowScratchOK || !add(rowScratch) {
		return 0, false, nil
	}
	for _, row := range input.rows {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		if len(row) != len(input.columns) {
			return 0, false, nil
		}
		for columnIndex, value := range row {
			bytes, ok := nativeRelationCellBytes(input.columns[columnIndex].Type, value)
			if !ok || !add(bytes) {
				return 0, false, nil
			}
		}
		if input.bucketEnds != nil && !add(2*uint64(unsafe.Sizeof(time.Time{}))) {
			return 0, false, nil
		}
	}
	return total, true, nil
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

func nativeRelationCellBytes(kind string, value any) (uint64, bool) {
	nullable := strings.HasPrefix(kind, "Nullable(")
	if value == nil {
		if nullable {
			return 2, true
		}
		if relationBaseType(kind) == "Dynamic" {
			return uint64(unsafe.Sizeof(chcol.Dynamic{})) + 4*uint64(unsafe.Sizeof(int(0))), true
		}
		return 0, false
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
		if !ok || uint64(len(text)) > math.MaxUint64/2-2*uint64(unsafe.Sizeof(uint64(0))) {
			return 0, false
		}
		bytes = 2*uint64(len(text)) + 2*uint64(unsafe.Sizeof(uint64(0)))
	case "Dynamic":
		return nativeRelationDynamicBytes(value, true)
	default:
		return 0, false
	}
	if nullable {
		bytes += 2
	}
	return bytes, true
}

func nativeRelationDynamicBytes(value any, root bool) (uint64, bool) {
	var total uint64
	if root {
		total = uint64(unsafe.Sizeof(chcol.Dynamic{}))
	}
	// Both discriminator and offset columns grow by append. Charge twice the
	// populated width for each, hence four machine integers per Dynamic node.
	var ok bool
	total, ok = retainedAdd(total, 4*uint64(unsafe.Sizeof(int(0))))
	if !ok {
		return 0, false
	}
	if items, list := value.([]any); list {
		// nativeRelationDynamic materializes one exact []Dynamic backing. The
		// driver Array additionally retains an offset with append headroom.
		if uint64(len(items)) > math.MaxUint64/uint64(unsafe.Sizeof(chcol.Dynamic{})) {
			return 0, false
		}
		total, ok = retainedAdd(total, uint64(len(items))*uint64(unsafe.Sizeof(chcol.Dynamic{})))
		if !ok {
			return 0, false
		}
		total, ok = retainedAdd(total, 2*uint64(unsafe.Sizeof(uint64(0))))
		if !ok {
			return 0, false
		}
		for _, item := range items {
			child, childOK := nativeRelationDynamicBytes(item, false)
			if !childOK {
				return 0, false
			}
			total, ok = retainedAdd(total, child)
			if !ok {
				return 0, false
			}
		}
		return total, true
	}
	storage, supported := retainedCompiledArgument(value)
	if !supported {
		return 0, false
	}
	// Primitive driver columns use append-backed storage. String payload and
	// offsets are both covered by doubling the retained scalar representation.
	if storage > math.MaxUint64/2 {
		return 0, false
	}
	total, ok = retainedAdd(total, 2*storage)
	return total, ok
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
