package patterns

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func measureInputRow(ctx context.Context, row searchjobs.ResultRow, maximum uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if maximum < 128 {
		return 0, ErrLimit
	}
	bytes := uint64(128)
	for _, value := range row.Values {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		valueBytes, exceeded, err := value.ProtoSizeLowerBound(maximum)
		if err != nil {
			return 0, ErrUnsupported
		}
		if exceeded || valueBytes > maximum {
			return 0, ErrLimit
		}
		retainedBytes, err := value.RetainedSizeBytesContext(ctx)
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return 0, contextErr
			}
			return 0, ErrUnsupported
		}
		if retainedBytes > maximum || maximum < 64 || valueBytes > maximum-64 {
			return 0, ErrLimit
		}
		increment := valueBytes + 64
		if retainedBytes > maximum-increment {
			return 0, ErrLimit
		}
		if err := addBounded(&bytes, increment+retainedBytes, maximum); err != nil {
			return 0, err
		}
	}
	if row.TimeBucket != nil {
		if err := addBounded(&bytes, uint64(len(row.TimeBucket.Earliest)+len(row.TimeBucket.Latest)+64), maximum); err != nil {
			return 0, err
		}
	}
	return bytes, nil
}

func selectedColumns(schema searchjobs.Schema, requested []string) ([]int, searchjobs.Schema, error) {
	if len(requested) == 0 {
		indexes := make([]int, len(schema.Columns))
		for index := range indexes {
			indexes[index] = index
		}
		return indexes, schema, nil
	}
	byName := make(map[string]int, len(schema.Columns))
	for index, column := range schema.Columns {
		if _, duplicate := byName[column.Name]; duplicate {
			return nil, searchjobs.Schema{}, ErrUnsupported
		}
		byName[column.Name] = index
	}
	indexes := make([]int, len(requested))
	columns := make([]searchjobs.Column, len(requested))
	seen := make(map[string]struct{}, len(requested))
	for index, name := range requested {
		columnIndex, exists := byName[name]
		if name == "" || !exists {
			return nil, searchjobs.Schema{}, ErrInvalidRequest
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, searchjobs.Schema{}, ErrInvalidRequest
		}
		seen[name] = struct{}{}
		indexes[index] = columnIndex
		columns[index] = schema.Columns[columnIndex]
	}
	return indexes, searchjobs.Schema{Columns: columns}, nil
}

func projectRow(row searchjobs.ResultRow, projection []int) searchjobs.ResultRow {
	values := make([]searchjobs.Value, len(projection))
	for index, sourceIndex := range projection {
		values[index] = row.Values[sourceIndex]
	}
	result := searchjobs.ResultRow{Ordinal: row.Ordinal, Values: values}
	if row.TimeBucket != nil {
		bounds := *row.TimeBucket
		result.TimeBucket = &bounds
	}
	return result
}

func projectionDigest(columns []string) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte("open-splunk/pattern-projection/v1\x00"))
	for _, column := range columns {
		_, _ = digest.Write([]byte(column))
		_, _ = digest.Write([]byte{'\x00'})
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func measureResponseRow(ctx context.Context, row searchjobs.ResultRow) (uint64, error) {
	// RetainedSizeBytesContext counts every nested immutable Value and container
	// slot without cloning byte payloads. The protobuf lower bound plus the fixed
	// envelope conservatively accounts conversion objects before the final 3x
	// serialization reservation covers conversion and marshal copies.
	bytes := uint64(256)
	for _, value := range row.Values {
		retained, err := value.RetainedSizeBytesContext(ctx)
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return 0, contextErr
			}
			return 0, ErrUnsupported
		}
		protoBytes, exceeded, err := value.ProtoSizeLowerBound(MaximumMemberResponseBytes)
		if err != nil {
			return 0, ErrUnsupported
		}
		if exceeded {
			return 0, ErrLimit
		}
		if err := addBounded(&bytes, retained, MaximumMemberResponseBytes); err != nil {
			return 0, err
		}
		if err := addBounded(&bytes, protoBytes+256, MaximumMemberResponseBytes); err != nil {
			return 0, err
		}
	}
	if row.TimeBucket != nil {
		if err := addBounded(&bytes, uint64(len(row.TimeBucket.Earliest)+len(row.TimeBucket.Latest)+64), MaximumMemberResponseBytes); err != nil {
			return 0, err
		}
	}
	return bytes, nil
}

func measureResponseSchema(schema searchjobs.Schema) (uint64, error) {
	bytes := uint64(512)
	for _, column := range schema.Columns {
		increment := uint64(len(column.Name) + len(column.FlatMultivalueDelimiter) + 512)
		if err := addBounded(&bytes, increment, MaximumMemberResponseBytes); err != nil {
			return 0, err
		}
	}
	return bytes, nil
}

func responseReservationBytes(wireUpperBound uint64) (uint64, error) {
	const multiplier = uint64(3)
	if wireUpperBound > ^uint64(0)/multiplier {
		return 0, ErrLimit
	}
	return wireUpperBound * multiplier, nil
}

func cloneRow(row searchjobs.ResultRow) searchjobs.ResultRow {
	result := row
	result.Values = slices.Clone(row.Values)
	if row.TimeBucket != nil {
		bounds := *row.TimeBucket
		result.TimeBucket = &bounds
	}
	return result
}
