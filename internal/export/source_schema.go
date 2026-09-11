package export

import (
	"context"
	"errors"
	"unsafe"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// maximumTrustedSourceSchemaBytes bounds metadata already retained by a
// completed, manager-attested result. It is separate from maximumColumns,
// which limits the columns selected into one export. Wide source schemas are
// scanned without cloning or building an index proportional to this ceiling.
const maximumTrustedSourceSchemaBytes = uint64(defaultMaxTotalMetadata)

type trustedResolvedSchemaLease interface {
	// The byte count attests an already measured immutable schema. Selection
	// may reuse it for size admission while still validating every source name.
	trustedResolvedSchema() (searchjobs.Schema, uint64, bool)
}

func trustedSchemaForSelection(
	lease searchjobs.ResultLease,
) (searchjobs.Schema, bool) {
	trusted, ok := lease.(trustedResolvedSchemaLease)
	if !ok {
		return lease.Schema(), false
	}
	schema, retainedBytes, valid := trusted.trustedResolvedSchema()
	if !valid || retainedBytes == 0 ||
		retainedBytes > maximumTrustedSourceSchemaBytes ||
		len(schema.Columns) == 0 {
		return searchjobs.Schema{}, false
	}
	return schema, true
}

func measureTrustedSourceSchema(
	ctx context.Context,
	schema searchjobs.Schema,
) (uint64, bool, error) {
	if ctx == nil {
		return 0, false, errors.New("measure export source schema: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if len(schema.Columns) == 0 {
		return 0, false, nil
	}
	columnBytes := uint64(unsafe.Sizeof(searchjobs.Column{}))
	columnCount := safecast.MustConv[uint64](len(schema.Columns))
	if columnCount > maximumTrustedSourceSchemaBytes/columnBytes {
		return 0, false, nil
	}
	total := uint64(unsafe.Sizeof(searchjobs.Schema{})) + columnCount*columnBytes
	if total > maximumTrustedSourceSchemaBytes {
		return 0, false, nil
	}
	for index, column := range schema.Columns {
		if index%1_024 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, false, err
			}
		}
		nameBytes := safecast.MustConv[uint64](len(column.Name))
		delimiterBytes := safecast.MustConv[uint64](len(column.FlatMultivalueDelimiter))
		if nameBytes > maximumTrustedSourceSchemaBytes-total {
			return 0, false, nil
		}
		total += nameBytes
		if delimiterBytes > maximumTrustedSourceSchemaBytes-total {
			return 0, false, nil
		}
		total += delimiterBytes
	}
	return total, true, nil
}

func validTrustedSourceSchema(schema searchjobs.Schema) bool {
	_, valid, err := measureTrustedSourceSchema(context.Background(), schema)
	return err == nil && valid
}
