package searchjobproto

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Schema projects an executor result schema into its shared HTTP/WebSocket
// representation. Column order is preserved because result cells are
// positional.
func Schema(schemaID string, schema searchjobs.Schema, resultKind opensplunkv1.ResultSetKind) (*opensplunkv1.ResultSchema, error) {
	columns := make([]*opensplunkv1.ResultColumn, len(schema.Columns))
	seen := make(map[string]struct{}, len(schema.Columns))
	for index, column := range schema.Columns {
		if column.Name == "" || !utf8.ValidString(column.Name) {
			return nil, errors.New("search result schema contains an invalid column name")
		}
		if _, exists := seen[column.Name]; exists {
			return nil, errors.New("search result schema contains duplicate columns")
		}
		seen[column.Name] = struct{}{}
		valueType, err := ValueKind(column.Kind)
		if err != nil {
			return nil, err
		}
		semantic := semanticType(column.Name)
		if resultKind == opensplunkv1.ResultSetKind_RESULT_SET_KIND_TIME_SERIES && index > 0 {
			semantic = opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_METRIC
		}
		columns[index] = &opensplunkv1.ResultColumn{
			FieldName:    column.Name,
			DisplayName:  column.Name,
			ValueType:    valueType,
			SemanticType: semantic,
			Nullable:     column.Nullable,
			Multivalue:   column.Multivalue,
		}
	}
	return &opensplunkv1.ResultSchema{
		SchemaId:   schemaID,
		Revision:   1,
		ResultKind: resultKind,
		Columns:    columns,
	}, nil
}

// Rows projects no more than maximumRows detached rows. The explicit limit
// keeps disposable previews bounded independently from the source snapshot.
// Passing a negative limit is invalid; zero permits only an empty row set.
func Rows(
	ctx context.Context,
	jobID string,
	schema searchjobs.Schema,
	rows []searchjobs.ResultRow,
	maximumRows int,
) ([]*opensplunkv1.ResultRow, error) {
	if ctx == nil {
		return nil, errors.New("search result conversion context is required")
	}
	if maximumRows < 0 {
		return nil, errors.New("search result row limit is invalid")
	}
	if len(rows) > maximumRows {
		return nil, errors.New("search result rows exceed conversion limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make([]*opensplunkv1.ResultRow, len(rows))
	for rowIndex, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(row.Values) != len(schema.Columns) {
			return nil, errors.New("search result row does not match schema")
		}
		cells := make([]*opensplunkv1.TypedValue, len(row.Values))
		for cellIndex, value := range row.Values {
			converted, err := Value(ctx, value)
			if err != nil {
				return nil, err
			}
			cells[cellIndex] = converted
		}
		result[rowIndex] = &opensplunkv1.ResultRow{
			RowId:   fmt.Sprintf("%s:%d", jobID, row.Ordinal),
			Ordinal: row.Ordinal,
			Cells:   cells,
		}
	}
	return result, nil
}

// ResultPage projects one already-bounded retained result page. Callers that
// project a subset of a live snapshot should use Rows with their configured
// preview limit instead.
func ResultPage(
	ctx context.Context,
	jobID string,
	page searchjobs.ResultPage,
	resultKind opensplunkv1.ResultSetKind,
	includeTotal bool,
	resultsTruncated bool,
) (*opensplunkv1.ResultPage, error) {
	schema, err := Schema(jobID, page.Schema, resultKind)
	if err != nil {
		return nil, err
	}
	rows, err := Rows(ctx, jobID, page.Schema, page.Rows, len(page.Rows))
	if err != nil {
		return nil, err
	}
	pageResponse := &opensplunkv1.PageResponse{TotalSizeExact: includeTotal && !resultsTruncated}
	if page.NextCursor != "" {
		pageResponse.NextPageToken = stringPointer(page.NextCursor)
	}
	if includeTotal {
		pageResponse.TotalSize = uint64Pointer(page.TotalRows)
	}
	return &opensplunkv1.ResultPage{
		Schema: schema,
		Rows:   rows,
		Page:   pageResponse,
		// The retained generation is immutable, but a truncation boundary means
		// this preview is not the complete SPL result set.
		SnapshotComplete: !resultsTruncated,
	}, nil
}

type protoValueFrame struct {
	list         *opensplunkv1.TypedValueList
	object       *opensplunkv1.TypedObject
	pendingField string
	expected     int
}

// Value converts one immutable typed result value without recursive Go calls.
// The context is checked for every detached traversal token so cancellation
// remains responsive for deeply nested or wide structural values.
func Value(ctx context.Context, value searchjobs.Value) (*opensplunkv1.TypedValue, error) {
	if ctx == nil {
		return nil, errors.New("search result conversion context is required")
	}
	var root *opensplunkv1.TypedValue
	frames := make([]protoValueFrame, 0, 8)
	attach := func(converted *opensplunkv1.TypedValue) error {
		if converted == nil {
			return errors.New("search result value conversion produced an empty value")
		}
		if len(frames) == 0 {
			if root != nil {
				return errors.New("search result value contains multiple roots")
			}
			root = converted
			return nil
		}
		frame := &frames[len(frames)-1]
		if frame.list != nil {
			if len(frame.list.Values) >= frame.expected {
				return errors.New("search result list contains too many values")
			}
			frame.list.Values = append(frame.list.Values, converted)
			return nil
		}
		if frame.object == nil || frame.pendingField == "" {
			return errors.New("search result object value is missing a field name")
		}
		if len(frame.object.Fields) >= frame.expected {
			return errors.New("search result object contains too many fields")
		}
		frame.object.Fields = append(frame.object.Fields, &opensplunkv1.TypedObjectField{
			Name:  frame.pendingField,
			Value: converted,
		})
		frame.pendingField = ""
		return nil
	}

	err := value.VisitDetached(func(token searchjobs.ValueVisitToken) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var converted *opensplunkv1.TypedValue
		switch token.Kind {
		case searchjobs.ValueVisitNull:
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_NullValue{NullValue: opensplunkv1.NullValue_NULL_VALUE_NULL}}
		case searchjobs.ValueVisitString:
			if !utf8.ValidString(token.StringValue) {
				return errors.New("search result string is not valid UTF-8")
			}
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_StringValue{StringValue: token.StringValue}}
		case searchjobs.ValueVisitSigned:
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_Sint64Value{Sint64Value: token.SignedValue}}
		case searchjobs.ValueVisitUnsigned:
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_Uint64Value{Uint64Value: token.UnsignedValue}}
		case searchjobs.ValueVisitDouble:
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_DoubleValue{DoubleValue: token.DoubleValue}}
		case searchjobs.ValueVisitBool:
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_BoolValue{BoolValue: token.BoolValue}}
		case searchjobs.ValueVisitBytes:
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_BytesValue{BytesValue: token.BytesValue}}
		case searchjobs.ValueVisitTime:
			timestamp, err := timestampToProto(token.TimeValue)
			if err != nil {
				return err
			}
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_TimestampValue{TimestampValue: timestamp}}
		case searchjobs.ValueVisitDuration:
			duration := durationpb.New(token.DurationValue)
			if err := duration.CheckValid(); err != nil {
				return errors.New("search result duration is outside protobuf range")
			}
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_DurationValue{DurationValue: duration}}
		case searchjobs.ValueVisitDecimal:
			canonical, err := searchjobs.CanonicalDecimal(token.StringValue)
			if err != nil {
				return err
			}
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_DecimalValue{DecimalValue: &opensplunkv1.DecimalValue{Value: canonical}}}
		case searchjobs.ValueVisitListBegin:
			list := &opensplunkv1.TypedValueList{Values: make([]*opensplunkv1.TypedValue, 0, token.Length)}
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_ListValue{ListValue: list}}
			if err := attach(converted); err != nil {
				return err
			}
			frames = append(frames, protoValueFrame{list: list, expected: token.Length})
			return nil
		case searchjobs.ValueVisitListEnd:
			if len(frames) == 0 || frames[len(frames)-1].list == nil {
				return errors.New("search result list boundary is invalid")
			}
			frame := frames[len(frames)-1]
			if len(frame.list.Values) != frame.expected {
				return errors.New("search result list length is invalid")
			}
			frames = frames[:len(frames)-1]
			return nil
		case searchjobs.ValueVisitObjectBegin:
			object := &opensplunkv1.TypedObject{Fields: make([]*opensplunkv1.TypedObjectField, 0, token.Length)}
			converted = &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_ObjectValue{ObjectValue: object}}
			if err := attach(converted); err != nil {
				return err
			}
			frames = append(frames, protoValueFrame{object: object, expected: token.Length})
			return nil
		case searchjobs.ValueVisitObjectField:
			if len(frames) == 0 || frames[len(frames)-1].object == nil || frames[len(frames)-1].pendingField != "" {
				return errors.New("search result object field boundary is invalid")
			}
			if token.StringValue == "" || !utf8.ValidString(token.StringValue) {
				return errors.New("search result object field name is not valid UTF-8")
			}
			frames[len(frames)-1].pendingField = token.StringValue
			return nil
		case searchjobs.ValueVisitObjectEnd:
			if len(frames) == 0 || frames[len(frames)-1].object == nil {
				return errors.New("search result object boundary is invalid")
			}
			frame := frames[len(frames)-1]
			if frame.pendingField != "" || len(frame.object.Fields) != frame.expected {
				return errors.New("search result object length is invalid")
			}
			frames = frames[:len(frames)-1]
			return nil
		default:
			return errors.New("search result value token is invalid")
		}
		return attach(converted)
	})
	if err != nil {
		return nil, err
	}
	if len(frames) != 0 || root == nil {
		return nil, errors.New("search result value traversal is incomplete")
	}
	return root, nil
}

// ValueKind maps a retained schema kind to its protobuf value type.
func ValueKind(kind searchjobs.ValueKind) (opensplunkv1.ValueType, error) {
	switch kind {
	case searchjobs.ValueKindNull:
		return opensplunkv1.ValueType_VALUE_TYPE_NULL, nil
	case searchjobs.ValueKindString:
		return opensplunkv1.ValueType_VALUE_TYPE_STRING, nil
	case searchjobs.ValueKindSigned:
		return opensplunkv1.ValueType_VALUE_TYPE_SINT64, nil
	case searchjobs.ValueKindUnsigned:
		return opensplunkv1.ValueType_VALUE_TYPE_UINT64, nil
	case searchjobs.ValueKindDouble:
		return opensplunkv1.ValueType_VALUE_TYPE_DOUBLE, nil
	case searchjobs.ValueKindBool:
		return opensplunkv1.ValueType_VALUE_TYPE_BOOL, nil
	case searchjobs.ValueKindBytes:
		return opensplunkv1.ValueType_VALUE_TYPE_BYTES, nil
	case searchjobs.ValueKindTime:
		return opensplunkv1.ValueType_VALUE_TYPE_TIMESTAMP, nil
	case searchjobs.ValueKindDuration:
		return opensplunkv1.ValueType_VALUE_TYPE_DURATION, nil
	case searchjobs.ValueKindList:
		return opensplunkv1.ValueType_VALUE_TYPE_LIST, nil
	case searchjobs.ValueKindObject:
		return opensplunkv1.ValueType_VALUE_TYPE_OBJECT, nil
	case searchjobs.ValueKindDecimal:
		return opensplunkv1.ValueType_VALUE_TYPE_DECIMAL, nil
	case searchjobs.ValueKindMixed:
		return opensplunkv1.ValueType_VALUE_TYPE_MIXED, nil
	default:
		return opensplunkv1.ValueType_VALUE_TYPE_UNSPECIFIED, errors.New("search result schema type is invalid")
	}
}

func semanticType(field string) opensplunkv1.ColumnSemanticType {
	switch field {
	case "_time":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_EVENT_TIME
	case "_indextime":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_INDEX_TIME
	case "_raw":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_RAW
	case "index":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_INDEX
	case "host":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_HOST
	case "source":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_SOURCE
	case "sourcetype":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_SOURCETYPE
	case "level":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_LEVEL
	case "message", "body":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_MESSAGE
	case "trace_id":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_TRACE_ID
	case "span_id":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_SPAN_ID
	default:
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_UNSPECIFIED
	}
}

// timestampToProto accepts the protobuf minimum instant. In Go that instant is
// time.Time's zero value, which is valid for a typed result cell.
func timestampToProto(input time.Time) (*timestamppb.Timestamp, error) {
	result := timestamppb.New(input.Round(0).UTC())
	if err := result.CheckValid(); err != nil {
		return nil, errors.New("timestamp is outside protobuf range")
	}
	return result, nil
}

func uint64Pointer(value uint64) *uint64 { return &value }
