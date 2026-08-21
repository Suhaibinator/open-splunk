package knowledgepreview

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

// ResponseInput is accepted only from the service after both executions have
// completed. SealResponse clones every protobuf projection before retaining
// it and binds it to the original job ID and selected row limit.
type ResponseInput struct {
	Validation   knowledgevalidation.SealedValidateResponse
	BeforeSchema *opensplunk.ResultSchema
	AfterSchema  *opensplunk.ResultSchema
	BeforeRows   []*opensplunk.ResultRow
	AfterRows    []*opensplunk.ResultRow
	Truncated    bool
	JobID        string
	MaximumRows  uint32
}

// SealedResponse retains the exact deterministic wire projection. Its zero
// value is invalid and every accessor rechecks the independent validation seal
// plus all row/schema invariants before detaching.
type SealedResponse struct {
	validation  knowledgevalidation.SealedValidateResponse
	response    *opensplunk.PreviewKnowledgeObjectResponse
	wire        []byte
	jobID       string
	maximumRows uint32
}

func SealResponse(ctx context.Context, input ResponseInput) (SealedResponse, error) {
	if ctx == nil || input.JobID == "" || input.MaximumRows == 0 ||
		input.MaximumRows > MaximumRows {
		return SealedResponse{}, ErrInvariant
	}
	if err := ctx.Err(); err != nil {
		return SealedResponse{}, err
	}
	validation, err := input.Validation.Proto(ctx)
	if err != nil || validation == nil || validation.GetResult() == nil {
		return SealedResponse{}, ErrInvariant
	}
	transient := opensplunk.PreviewKnowledgeObjectResponse{
		Validation:            validation.GetResult(),
		BeforeSchema:          input.BeforeSchema,
		AfterSchema:           input.AfterSchema,
		BeforeRows:            input.BeforeRows,
		AfterRows:             input.AfterRows,
		Truncated:             input.Truncated,
		TenantCatalogRevision: validation.GetTenantCatalogRevision(),
	}
	// Size the complete caller graph before cloning any schema, row, or cell.
	// proto.Size does not materialize the wire representation, so an oversized
	// combined response cannot multiply its retained scalar or recursive data.
	if proto.Size(&transient) > MaximumResponseBytes {
		return SealedResponse{}, ErrResponseTooLarge
	}
	if err := ctx.Err(); err != nil {
		return SealedResponse{}, err
	}
	response := &opensplunk.PreviewKnowledgeObjectResponse{
		Validation:            validation.GetResult(),
		BeforeSchema:          cloneSchema(input.BeforeSchema),
		AfterSchema:           cloneSchema(input.AfterSchema),
		BeforeRows:            cloneRows(input.BeforeRows),
		AfterRows:             cloneRows(input.AfterRows),
		Truncated:             input.Truncated,
		TenantCatalogRevision: validation.GetTenantCatalogRevision(),
	}
	if err := validateResponse(ctx, response, validation, input.JobID, input.MaximumRows); err != nil {
		return SealedResponse{}, err
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return SealedResponse{}, ErrInvariant
	}
	if len(wire) == 0 || len(wire) > MaximumResponseBytes {
		return SealedResponse{}, ErrResponseTooLarge
	}
	if err := ctx.Err(); err != nil {
		return SealedResponse{}, err
	}
	return SealedResponse{
		validation:  input.Validation,
		response:    response,
		wire:        wire,
		jobID:       strings.Clone(input.JobID),
		maximumRows: input.MaximumRows,
	}, nil
}

// Proto returns a fully detached response after proving that neither the
// retained protobuf nor its validation authority changed after sealing.
func (sealed SealedResponse) Proto(ctx context.Context) (*opensplunk.PreviewKnowledgeObjectResponse, error) {
	if ctx == nil || sealed.response == nil || len(sealed.wire) == 0 ||
		sealed.jobID == "" || sealed.maximumRows == 0 ||
		sealed.maximumRows > MaximumRows {
		return nil, ErrInvariant
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	validation, err := sealed.validation.Proto(ctx)
	if err != nil || validation == nil {
		return nil, ErrInvariant
	}
	if err := validateResponse(
		ctx,
		sealed.response,
		validation,
		sealed.jobID,
		sealed.maximumRows,
	); err != nil {
		return nil, err
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(sealed.response)
	if err != nil || len(wire) == 0 || len(wire) > MaximumResponseBytes ||
		!bytes.Equal(wire, sealed.wire) {
		return nil, ErrInvariant
	}
	cloned, ok := proto.Clone(sealed.response).(*opensplunk.PreviewKnowledgeObjectResponse)
	if !ok || cloned == nil {
		return nil, ErrInvariant
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cloned, nil
}

// DeterministicBytes returns a detached copy of the exact checked encoding.
func (sealed SealedResponse) DeterministicBytes() []byte {
	return bytes.Clone(sealed.wire)
}

func validateResponse(
	ctx context.Context,
	response *opensplunk.PreviewKnowledgeObjectResponse,
	validation *opensplunk.ValidateKnowledgeObjectResponse,
	jobID string,
	maximumRows uint32,
) error {
	if response == nil || validation == nil || validation.GetResult() == nil ||
		response.GetValidation() == nil ||
		!proto.Equal(response.GetValidation(), validation.GetResult()) ||
		response.GetTenantCatalogRevision() != validation.GetTenantCatalogRevision() ||
		response.GetTenantCatalogRevision() > math.MaxInt64 ||
		len(response.ProtoReflect().GetUnknown()) != 0 {
		return ErrInvariant
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !response.GetValidation().GetValid() {
		if response.GetBeforeSchema() != nil || response.GetAfterSchema() != nil ||
			len(response.GetBeforeRows()) != 0 || len(response.GetAfterRows()) != 0 ||
			response.GetTruncated() {
			return ErrInvariant
		}
		return nil
	}
	if err := validateSchema(response.GetBeforeSchema(), jobID); err != nil {
		return err
	}
	if err := validateSchema(response.GetAfterSchema(), jobID); err != nil {
		return err
	}
	if response.GetBeforeSchema().GetResultKind() != response.GetAfterSchema().GetResultKind() {
		return ErrInvariant
	}
	if err := validateRows(ctx, response.GetBeforeRows(), response.GetBeforeSchema(), jobID, maximumRows); err != nil {
		return err
	}
	return validateRows(ctx, response.GetAfterRows(), response.GetAfterSchema(), jobID, maximumRows)
}

func validateSchema(schema *opensplunk.ResultSchema, jobID string) error {
	if schema == nil || len(schema.ProtoReflect().GetUnknown()) != 0 ||
		schema.GetSchemaId() != jobID || schema.GetRevision() != 1 ||
		!validResultKind(schema.GetResultKind()) || len(schema.GetColumns()) == 0 ||
		len(schema.GetColumns()) > MaximumColumns {
		return ErrInvariant
	}
	seen := make(map[string]struct{}, len(schema.GetColumns()))
	for _, column := range schema.GetColumns() {
		if column == nil || len(column.ProtoReflect().GetUnknown()) != 0 ||
			column.GetFieldName() == "" || !utf8.ValidString(column.GetFieldName()) ||
			column.GetDisplayName() != column.GetFieldName() ||
			!validColumnValueType(column.GetValueType()) ||
			!validSemanticType(column.GetSemanticType()) || column.GetHiddenByDefault() {
			return ErrInvariant
		}
		if _, duplicate := seen[column.GetFieldName()]; duplicate {
			return ErrInvariant
		}
		seen[column.GetFieldName()] = struct{}{}
	}
	return nil
}

func validateRows(
	ctx context.Context,
	rows []*opensplunk.ResultRow,
	schema *opensplunk.ResultSchema,
	jobID string,
	maximumRows uint32,
) error {
	if uint64(len(rows)) > uint64(maximumRows) {
		return ErrInvariant
	}
	for index, row := range rows {
		if index%32 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		ordinal := uint64(index)
		if row == nil || len(row.ProtoReflect().GetUnknown()) != 0 ||
			row.GetOrdinal() != ordinal ||
			row.GetRowId() != fmt.Sprintf("%s:%d", jobID, ordinal) ||
			len(row.GetCells()) != len(schema.GetColumns()) {
			return ErrInvariant
		}
		for _, cell := range row.GetCells() {
			if err := validateTypedValue(cell, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateTypedValue(value *opensplunk.TypedValue, depth int) error {
	if value == nil || depth > 32 || len(value.ProtoReflect().GetUnknown()) != 0 {
		return ErrInvariant
	}
	switch selected := value.GetKind().(type) {
	case *opensplunk.TypedValue_NullValue:
		if selected.NullValue != opensplunk.NullValue_NULL_VALUE_NULL {
			return ErrInvariant
		}
	case *opensplunk.TypedValue_MissingValue:
		if selected.MissingValue != opensplunk.MissingValue_MISSING_VALUE_MISSING {
			return ErrInvariant
		}
	case *opensplunk.TypedValue_StringValue:
		if !utf8.ValidString(selected.StringValue) {
			return ErrInvariant
		}
	case *opensplunk.TypedValue_Sint64Value,
		*opensplunk.TypedValue_Uint64Value,
		*opensplunk.TypedValue_DoubleValue,
		*opensplunk.TypedValue_BoolValue,
		*opensplunk.TypedValue_BytesValue:
		return nil
	case *opensplunk.TypedValue_TimestampValue:
		if selected.TimestampValue == nil ||
			len(selected.TimestampValue.ProtoReflect().GetUnknown()) != 0 ||
			selected.TimestampValue.CheckValid() != nil {
			return ErrInvariant
		}
	case *opensplunk.TypedValue_DurationValue:
		if selected.DurationValue == nil ||
			len(selected.DurationValue.ProtoReflect().GetUnknown()) != 0 ||
			selected.DurationValue.CheckValid() != nil {
			return ErrInvariant
		}
	case *opensplunk.TypedValue_DecimalValue:
		if selected.DecimalValue == nil ||
			len(selected.DecimalValue.ProtoReflect().GetUnknown()) != 0 {
			return ErrInvariant
		}
		if _, err := searchjobs.DecimalValue(selected.DecimalValue.GetValue()); err != nil {
			return ErrInvariant
		}
	case *opensplunk.TypedValue_ListValue:
		if selected.ListValue == nil ||
			len(selected.ListValue.ProtoReflect().GetUnknown()) != 0 {
			return ErrInvariant
		}
		for _, child := range selected.ListValue.GetValues() {
			if err := validateTypedValue(child, depth+1); err != nil {
				return err
			}
		}
	case *opensplunk.TypedValue_ObjectValue:
		if selected.ObjectValue == nil ||
			len(selected.ObjectValue.ProtoReflect().GetUnknown()) != 0 {
			return ErrInvariant
		}
		seen := make(map[string]struct{}, len(selected.ObjectValue.GetFields()))
		for _, field := range selected.ObjectValue.GetFields() {
			if field == nil || len(field.ProtoReflect().GetUnknown()) != 0 ||
				field.GetName() == "" || !utf8.ValidString(field.GetName()) {
				return ErrInvariant
			}
			if _, duplicate := seen[field.GetName()]; duplicate {
				return ErrInvariant
			}
			seen[field.GetName()] = struct{}{}
			if err := validateTypedValue(field.GetValue(), depth+1); err != nil {
				return err
			}
		}
	default:
		return ErrInvariant
	}
	return nil
}

func validResultKind(value opensplunk.ResultSetKind) bool {
	switch value {
	case opensplunk.ResultSetKind_RESULT_SET_KIND_EVENTS,
		opensplunk.ResultSetKind_RESULT_SET_KIND_STATISTICS,
		opensplunk.ResultSetKind_RESULT_SET_KIND_TIME_SERIES:
		return true
	default:
		return false
	}
}

func validColumnValueType(value opensplunk.ValueType) bool {
	return value >= opensplunk.ValueType_VALUE_TYPE_NULL &&
		value <= opensplunk.ValueType_VALUE_TYPE_MISSING
}

func validSemanticType(value opensplunk.ColumnSemanticType) bool {
	return value >= opensplunk.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_UNSPECIFIED &&
		value <= opensplunk.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_DIMENSION
}

func cloneSchema(input *opensplunk.ResultSchema) *opensplunk.ResultSchema {
	if input == nil {
		return nil
	}
	cloned, _ := proto.Clone(input).(*opensplunk.ResultSchema)
	return cloned
}

func cloneRows(input []*opensplunk.ResultRow) []*opensplunk.ResultRow {
	if input == nil {
		return nil
	}
	result := make([]*opensplunk.ResultRow, len(input))
	for index, row := range input {
		result[index], _ = proto.Clone(row).(*opensplunk.ResultRow)
	}
	return slices.Clone(result)
}
