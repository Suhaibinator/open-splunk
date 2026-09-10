package clickhouse

import (
	"context"
	"errors"
	"math"
	"unsafe"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	driverproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func externalTableNativeMaximumBytes(
	ctx context.Context,
	hasRelation bool,
) (uint64, bool) {
	if !hasRelation {
		if _, ok := searchlimits.RemainingExecutionBytes(ctx); !ok {
			return 0, false
		}
	}
	return relationInputMaximumBytes(ctx), true
}

// lookupExternalTablesNativeMaterializationBytes returns the additional heap
// retained by fresh clickhouse-go external-table blocks while the immutable
// lookup authority remains live. Each append-grown driver backing is modeled
// separately; the two-times populated width covers Go slice capacity without
// multiplying the already-retained lookup cells or unrelated stage state.
func lookupExternalTablesNativeMaterializationBytes(
	ctx context.Context,
	tables []compiledLookupExternalTable,
) (uint64, bool, error) {
	if ctx == nil {
		return 0, false, errors.New("measure ClickHouse lookup tables: context is nil")
	}
	if err := validateCompiledLookupExternalTablesContext(ctx, tables); err != nil {
		return 0, false, err
	}
	total := uint64(len(tables)) * uint64(unsafe.Sizeof((*ext.Table)(nil)))
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		bytes, ok := measureCompiledLookupExternalTable(table)
		if !ok {
			return 0, false, nil
		}
		total, ok = retainedAdd(total, bytes)
		if !ok {
			return 0, false, nil
		}
	}
	return total, true, nil
}

func measureCompiledLookupExternalTable(
	table compiledLookupExternalTable,
) (uint64, bool) {
	columnCount := uint64(len(table.columns)) + 1
	total := uint64(unsafe.Sizeof(ext.Table{})) +
		uint64(unsafe.Sizeof(driverproto.Block{})) +
		uint64(unsafe.Sizeof(column.ServerContext{}))
	add := func(value uint64) bool {
		var ok bool
		total, ok = retainedAdd(total, value)
		return ok
	}
	// NewTable consumes one definition closure per column. Charge the function
	// slice slot, closure code pointer, captured name/type string headers, and
	// one word of small-allocation padding; they coexist with the finished block.
	functionWord := uint64(unsafe.Sizeof((func(*ext.Table) error)(nil)))
	definitionBytes := 3*functionWord + 2*uint64(unsafe.Sizeof(""))
	if !chargeProduct(columnCount, definitionBytes, add) ||
		// Block.AddColumn grows both the retained name and interface slices.
		!chargeProduct(columnCount, 2*uint64(unsafe.Sizeof("")), add) ||
		!chargeProduct(columnCount, 2*uint64(unsafe.Sizeof(any(nil))), add) ||
		!add(uint64(unsafe.Sizeof(column.UInt8{}))) ||
		!chargeProduct(uint64(len(table.columns)), uint64(unsafe.Sizeof(column.String{})), add) ||
		// One reusable row of interface headers is live during all appends.
		!chargeProduct(columnCount, uint64(unsafe.Sizeof(any(nil))), add) {
		return 0, false
	}
	rows := uint64(table.backing.rowCount)
	if rows > 0 {
		// The matched marker is one append-grown byte per row. Eight bytes covers
		// the allocator's first backing when only a few rows are present.
		if rows > math.MaxUint64/2 || !add(max(uint64(8), 2*rows)) {
			return 0, false
		}
	}
	// Every String append records a Position even for an empty value. Payload
	// bytes use a distinct append-grown buffer per column; an eight-byte floor
	// per column covers the allocator rounding of small non-empty buffers.
	positionBytes := uint64(unsafe.Sizeof(chproto.Position{}))
	if rows > math.MaxUint64/(2*positionBytes) {
		return 0, false
	}
	if rows > 0 {
		positionBacking := max(positionBytes, 2*rows*positionBytes)
		if !chargeProduct(uint64(len(table.columns)), positionBacking, add) {
			return 0, false
		}
	}
	if table.backing.payloadBytes > 0 {
		if !chargeProduct(uint64(len(table.columns)), 8, add) ||
			table.backing.payloadBytes > math.MaxUint64/2 ||
			!add(2*table.backing.payloadBytes) {
			return 0, false
		}
	}
	return total, true
}

func externalTablesNativeMaterializationBytes(
	ctx context.Context,
	tables []compiledLookupExternalTable,
	input *compiledRelationInput,
) (uint64, bool, error) {
	lookupBytes, ok, err := lookupExternalTablesNativeMaterializationBytes(ctx, tables)
	if err != nil || !ok {
		return 0, ok, err
	}
	if input == nil {
		return lookupBytes, true, nil
	}
	relationBytes, ok, err := relationInputNativeMaterializationBytes(ctx, input)
	if err != nil || !ok {
		return 0, ok, err
	}
	// The combined materializer allocates one exact result slice. Replace the
	// lookup-only result backing with its one additional relation pointer.
	total, ok := retainedAdd(
		lookupBytes,
		uint64(unsafe.Sizeof((*ext.Table)(nil))),
	)
	if !ok {
		return 0, false, nil
	}
	total, ok = retainedAdd(total, relationBytes)
	return total, ok, nil
}
