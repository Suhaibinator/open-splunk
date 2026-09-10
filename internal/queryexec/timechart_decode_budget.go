package queryexec

import (
	"context"
	"reflect"
	"time"
	"unsafe"

	"fortio.org/safecast"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// preflightDecodedRow walks the native graph without constructing Value lists.
// Native primitive arrays can be much smaller than their decoded Value slots.
// Object ancestors reserve their existing defensive child copies as well.
func preflightDecodedRow(ctx context.Context, destinations []any, maximum uint64) error {
	charge := func(bytes uint64) error {
		if bytes > maximum {
			return searchjobs.ErrExecutionLimit
		}
		maximum -= bytes
		return nil
	}
	var visit func(any, int, uint64) error
	var visitJSON func(*chcol.JSON, int, uint64) error
	visit = func(value any, depth int, copies uint64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if depth > 32 {
			return searchjobs.ErrInvalidResult
		}
		switch value := value.(type) {
		case chcol.Dynamic:
			return visit(value.Any(), depth, copies)
		case *chcol.Dynamic:
			if value == nil {
				return charge(uint64(unsafe.Sizeof(searchjobs.Value{})) * copies)
			}
			return visit(value.Any(), depth, copies)
		case chcol.JSON:
			return visitJSON(&value, depth, copies)
		case *chcol.JSON:
			return visitJSON(value, depth, copies)
		}
		if err := charge(uint64(unsafe.Sizeof(searchjobs.Value{})) * copies); err != nil {
			return err
		}
		if value == nil {
			return nil
		}
		if _, ok := value.(time.Time); ok {
			return nil
		}
		reflected := reflect.ValueOf(value)
		for reflected.IsValid() && (reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface) {
			if reflected.IsNil() {
				return nil
			}
			reflected = reflected.Elem()
		}
		if !reflected.IsValid() {
			return nil
		}
		switch reflected.Kind() {
		case reflect.String:
			if safecast.MustConv[uint64](reflected.Len()) > maximum/copies {
				return searchjobs.ErrExecutionLimit
			}
			return charge(safecast.MustConv[uint64](reflected.Len()) * copies)
		case reflect.Slice, reflect.Array:
			if reflected.Type().Elem().Kind() == reflect.Uint8 {
				if safecast.MustConv[uint64](reflected.Len()) > maximum/(2*copies) {
					return searchjobs.ErrExecutionLimit
				}
				return charge(safecast.MustConv[uint64](reflected.Len()) * 2 * copies)
			}
			// Check the entire slot backing before visiting a single child.
			if safecast.MustConv[uint64](reflected.Len()) > maximum/(uint64(unsafe.Sizeof(searchjobs.Value{}))*copies) {
				return searchjobs.ErrExecutionLimit
			}
			for index := range reflected.Len() {
				if err := visit(reflected.Index(index).Interface(), depth+1, copies); err != nil {
					return err
				}
			}
		case reflect.Map:
			if reflected.Type().Key().Kind() != reflect.String {
				return searchjobs.ErrInvalidResult
			}
			copies++
			structural := uint64(unsafe.Sizeof(searchjobs.ObjectField{})) + uint64(unsafe.Sizeof(reflect.Value{}))
			if safecast.MustConv[uint64](reflected.Len()) > maximum/(structural*copies) {
				return searchjobs.ErrExecutionLimit
			}
			if err := charge(safecast.MustConv[uint64](reflected.Len()) * structural * copies); err != nil {
				return err
			}
			entries := reflected.MapRange()
			for entries.Next() {
				if err := visit(entries.Key().String(), depth+1, copies); err != nil {
					return err
				}
				if err := visit(entries.Value().Interface(), depth+1, copies); err != nil {
					return err
				}
			}
		}
		return nil
	}
	visitJSON = func(document *chcol.JSON, depth int, copies uint64) error {
		if document == nil {
			return charge(uint64(unsafe.Sizeof(searchjobs.Value{})) * copies)
		}
		// Account expanded path ancestors by their actual parsed depth,
		// including temporary maps/path indexes and defensive object copies.
		for path, cell := range document.ValuesByPath() {
			if uint64(len(path)) > maximum/(4*copies) {
				return searchjobs.ErrExecutionLimit
			}
			if err := charge(uint64(len(path)) * 4 * copies); err != nil {
				return err
			}
			normalized, err := eventfields.NormalizePhysicalDynamicPath(path)
			if err != nil {
				return searchjobs.ErrInvalidResult
			}
			segments, err := eventfields.ParseNormalizedDynamicPath(normalized)
			if err != nil || depth+len(segments) > 32 {
				return searchjobs.ErrInvalidResult
			}
			levels := uint64(len(segments))
			structure := uint64(unsafe.Sizeof(searchjobs.ObjectField{})) + uint64(unsafe.Sizeof(searchjobs.Value{})) + uint64(unsafe.Sizeof(reflect.Value{}))
			if levels > maximum/(structure*(copies+levels)) {
				return searchjobs.ErrExecutionLimit
			}
			if err := charge(levels * structure * (copies + levels)); err != nil {
				return err
			}
			if err := visit(cell, depth+len(segments), copies+levels); err != nil {
				return err
			}
		}
		return nil
	}
	for _, destination := range destinations {
		if err := visit(scannedValue(destination), 0, 1); err != nil {
			return err
		}
	}
	return nil
}
