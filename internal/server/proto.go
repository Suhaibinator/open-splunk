package server

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func searchJobToProto(job searchjobs.Job, now time.Time) (*opensplunkv1.SearchJob, error) {
	resultKind := resultKindForSPL(job.SPL)
	earliest, err := validTimestamp(job.Earliest)
	if err != nil {
		return nil, err
	}
	latest, err := validTimestamp(job.Latest)
	if err != nil {
		return nil, err
	}
	indexTimeCutoff, err := validTimestamp(job.IndexTimeCutoff)
	if err != nil {
		return nil, err
	}
	createdAt, err := validTimestamp(job.CreatedAt)
	if err != nil {
		return nil, err
	}
	result := &opensplunkv1.SearchJob{
		SearchJobId:  job.ID,
		StateVersion: job.Version,
		Definition: &opensplunkv1.SearchDefinition{
			Spl:        job.SPL,
			TimeRange:  absoluteTimeRangeToProto(job.Earliest, job.Latest),
			IndexScope: slices.Clone(job.RequestedIndexes),
		},
		NormalizedSpl:       optionalString(job.NormalizedSPL),
		EffectiveIndexScope: slices.Clone(job.EffectiveIndexes),
		ResolvedTimeRange: &opensplunkv1.ResolvedTimeRange{
			Earliest: earliest,
			Latest:   latest,
			Timezone: "UTC",
		},
		IndexTimeCutoff: indexTimeCutoff,
		State:           searchStateToProto(job.State),
		ResultKind:      resultKind,
		Progress:        searchProgressToProto(job, now),
		CreatedAt:       createdAt,
	}
	if job.Schema != nil {
		result.ResultSchema, err = schemaToProto(job.ID, *job.Schema, resultKind)
		if err != nil {
			return nil, err
		}
	}
	if job.Failure != nil {
		result.Failure = failureToProto(*job.Failure)
		result.Diagnostics = diagnosticsToProto(job.Failure.Diagnostics)
	}
	if !job.StartedAt.IsZero() {
		result.StartedAt, err = validTimestamp(job.StartedAt)
		if err != nil {
			return nil, err
		}
	}
	if !job.FinishedAt.IsZero() {
		result.FinishedAt, err = validTimestamp(job.FinishedAt)
		if err != nil {
			return nil, err
		}
	}
	if !job.ExpiresAt.IsZero() {
		result.ExpiresAt, err = validTimestamp(job.ExpiresAt)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func searchProgressToProto(job searchjobs.Job, now time.Time) *opensplunkv1.SearchProgress {
	end := now.Round(0).UTC()
	if !job.FinishedAt.IsZero() {
		end = job.FinishedAt
	}
	elapsed := time.Duration(0)
	if !job.StartedAt.IsZero() && end.After(job.StartedAt) {
		elapsed = end.Sub(job.StartedAt)
	}
	return &opensplunkv1.SearchProgress{
		Phase:        searchPhaseToProto(job.State),
		ProducedRows: job.RowCount,
		ResultBytes:  job.ResultBytes,
		Elapsed:      durationpb.New(elapsed),
		UpdatedAt:    timestamppb.New(end),
	}
}

func resultPageToProto(ctx context.Context, jobID string, page searchjobs.ResultPage, resultKind opensplunkv1.ResultSetKind, includeTotal bool) (*opensplunkv1.ResultPage, error) {
	if ctx == nil {
		return nil, errors.New("search result conversion context is required")
	}
	schema, err := schemaToProto(jobID, page.Schema, resultKind)
	if err != nil {
		return nil, err
	}
	rows := make([]*opensplunkv1.ResultRow, len(page.Rows))
	for rowIndex, row := range page.Rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(row.Values) != len(page.Schema.Columns) {
			return nil, errors.New("search result row does not match schema")
		}
		cells := make([]*opensplunkv1.TypedValue, len(row.Values))
		for cellIndex, value := range row.Values {
			cells[cellIndex], err = valueToProto(ctx, value)
			if err != nil {
				return nil, err
			}
		}
		rows[rowIndex] = &opensplunkv1.ResultRow{
			RowId:   fmt.Sprintf("%s:%d", jobID, row.Ordinal),
			Ordinal: row.Ordinal,
			Cells:   cells,
		}
	}
	pageResponse := &opensplunkv1.PageResponse{TotalSizeExact: includeTotal}
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
		// ResultsFor is available only after the manager has frozen the result
		// snapshot. Complete denotes the end of pagination, not snapshot
		// mutability, so every successfully returned page is complete.
		SnapshotComplete: true,
	}, nil
}

func schemaToProto(schemaID string, schema searchjobs.Schema, resultKind opensplunkv1.ResultSetKind) (*opensplunkv1.ResultSchema, error) {
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
		valueType, err := valueKindToProto(column.Kind)
		if err != nil {
			return nil, err
		}
		columns[index] = &opensplunkv1.ResultColumn{
			FieldName:    column.Name,
			DisplayName:  column.Name,
			ValueType:    valueType,
			SemanticType: semanticType(column.Name),
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

func resultKindForSPL(source string) opensplunkv1.ResultSetKind {
	query, err := spl.Parse(source)
	if err != nil {
		return opensplunkv1.ResultSetKind_RESULT_SET_KIND_UNSPECIFIED
	}
	for _, command := range query.Commands {
		switch command.(type) {
		case *spl.TableCommand, *spl.StatsCommand, *spl.TopCommand:
			return opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS
		}
	}
	return opensplunkv1.ResultSetKind_RESULT_SET_KIND_EVENTS
}

type protoValueFrame struct {
	list         *opensplunkv1.TypedValueList
	object       *opensplunkv1.TypedObject
	pendingField string
	expected     int
}

func valueToProto(ctx context.Context, value searchjobs.Value) (*opensplunkv1.TypedValue, error) {
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
			canonical, err := canonicalTransportDecimal(token.StringValue)
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

func canonicalTransportDecimal(source string) (string, error) {
	lower := strings.ToLower(source)
	mantissa, exponent, hasExponent := strings.Cut(lower, "e")
	negative := false
	if mantissa != "" {
		switch mantissa[0] {
		case '-':
			negative = true
			mantissa = mantissa[1:]
		case '+':
			mantissa = mantissa[1:]
		}
	}
	integer, fraction, hasFraction := strings.Cut(mantissa, ".")
	if integer == "" || !decimalDigits(integer) || (hasFraction && (fraction == "" || !decimalDigits(fraction))) {
		return "", errors.New("search result decimal is invalid")
	}
	explicitExponent := new(big.Int)
	if hasExponent {
		exponentNegative := false
		if exponent != "" {
			switch exponent[0] {
			case '-':
				exponentNegative = true
				exponent = exponent[1:]
			case '+':
				exponent = exponent[1:]
			}
		}
		if exponent == "" || !decimalDigits(exponent) {
			return "", errors.New("search result decimal is invalid")
		}
		exponent = strings.TrimLeft(exponent, "0")
		if exponent == "" {
			exponent = "0"
		}
		if _, ok := explicitExponent.SetString(exponent, 10); !ok {
			return "", errors.New("search result decimal is invalid")
		}
		if exponentNegative {
			explicitExponent.Neg(explicitExponent)
		}
	}

	digits := integer + fraction
	firstNonzero := strings.IndexFunc(digits, func(character rune) bool { return character != '0' })
	if firstNonzero < 0 {
		return "0", nil
	}
	lastNonzero := len(digits) - 1
	for digits[lastNonzero] == '0' {
		lastNonzero--
	}
	coefficient := digits[firstNonzero : lastNonzero+1]
	scientificExponent := new(big.Int).Set(explicitExponent)
	scientificExponent.Add(scientificExponent, big.NewInt(int64(len(integer)-firstNonzero-1)))

	sign := ""
	if negative {
		sign = "-"
	}
	// Use a deterministic plain form for ordinary magnitudes and normalized
	// scientific notation outside that range. This keeps wire values compact
	// even for attacker-controlled exponents while ensuring numerically
	// equivalent lexical forms have one representation.
	if scientificExponent.IsInt64() {
		exponentValue := scientificExponent.Int64()
		if exponentValue >= -6 && exponentValue < 21 {
			decimalPoint := int(exponentValue) + 1
			switch {
			case decimalPoint <= 0:
				return sign + "0." + strings.Repeat("0", -decimalPoint) + coefficient, nil
			case decimalPoint >= len(coefficient):
				return sign + coefficient + strings.Repeat("0", decimalPoint-len(coefficient)), nil
			default:
				return sign + coefficient[:decimalPoint] + "." + coefficient[decimalPoint:], nil
			}
		}
	}
	if len(coefficient) == 1 {
		return sign + coefficient + "e" + scientificExponent.String(), nil
	}
	return sign + coefficient[:1] + "." + coefficient[1:] + "e" + scientificExponent.String(), nil
}

func decimalDigits(value string) bool {
	for index := range len(value) {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func valueKindToProto(kind searchjobs.ValueKind) (opensplunkv1.ValueType, error) {
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

func searchStateToProto(state searchjobs.State) opensplunkv1.SearchJobState {
	switch state {
	case searchjobs.StateQueued:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED
	case searchjobs.StateParsing:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_PARSING
	case searchjobs.StatePlanning:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_PLANNING
	case searchjobs.StateRunning:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING
	case searchjobs.StateCompleted:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED
	case searchjobs.StateFailed:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED
	case searchjobs.StateCanceled:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED
	case searchjobs.StateExpired:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED
	default:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED
	}
}

func searchPhaseToProto(state searchjobs.State) opensplunkv1.SearchExecutionPhase {
	switch state {
	case searchjobs.StateQueued:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_WAITING_FOR_SLOT
	case searchjobs.StateParsing:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_PARSING
	case searchjobs.StatePlanning:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_OPTIMIZING
	case searchjobs.StateRunning:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_EXECUTING
	case searchjobs.StateCompleted, searchjobs.StateFailed, searchjobs.StateCanceled, searchjobs.StateExpired:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE
	default:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_UNSPECIFIED
	}
}

func failureToProto(failure searchjobs.Failure) *opensplunkv1.SearchFailure {
	return &opensplunkv1.SearchFailure{
		Code:        failureCodeToProto(failure.Code),
		Message:     failure.Message,
		Retryable:   failure.Retryable,
		Diagnostics: diagnosticsToProto(failure.Diagnostics),
	}
}

func failureCodeToProto(code searchjobs.FailureCode) opensplunkv1.SearchFailureCode {
	switch code {
	case searchjobs.FailureInvalidSPL:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_SPL
	case searchjobs.FailureUnsupportedSPL:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_UNSUPPORTED_SPL
	case searchjobs.FailureInvalidTimeRange:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_TIME_RANGE
	case searchjobs.FailureIndexForbidden:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INDEX_FORBIDDEN
	case searchjobs.FailureResourceLimit:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_RESOURCE_LIMIT
	case searchjobs.FailureTimeout:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_TIMEOUT
	case searchjobs.FailureStorageUnavailable:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_STORAGE_UNAVAILABLE
	case searchjobs.FailureExecution:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_EXECUTION
	case searchjobs.FailureInternal:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL
	default:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_UNSPECIFIED
	}
}

func diagnosticsToProto(diagnostics []searchjobs.Diagnostic) []*opensplunkv1.Diagnostic {
	result := make([]*opensplunkv1.Diagnostic, len(diagnostics))
	for index, diagnostic := range diagnostics {
		converted := &opensplunkv1.Diagnostic{
			Code:        diagnostic.Code,
			Severity:    opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
			Message:     diagnostic.Message,
			Suggestions: slices.Clone(diagnostic.Suggestions),
		}
		if diagnostic.Line > 0 || diagnostic.Column > 0 || diagnostic.EndLine > 0 || diagnostic.EndColumn > 0 {
			converted.SourceRange = &opensplunkv1.SourceRange{
				Start: &opensplunkv1.SourcePosition{Line: nonnegativeUint32(diagnostic.Line), Column: nonnegativeUint32(diagnostic.Column)},
				End:   &opensplunkv1.SourcePosition{Line: nonnegativeUint32(diagnostic.EndLine), Column: nonnegativeUint32(diagnostic.EndColumn)},
			}
		}
		result[index] = converted
	}
	return result
}

func indexSummaryToProto(index control.Index) *opensplunkv1.IndexSummary {
	return &opensplunkv1.IndexSummary{
		IndexId:         index.ID,
		Name:            index.Definition.Name,
		DisplayName:     index.Definition.DisplayName,
		State:           indexStateToProto(index.State),
		IngestionAccess: accessState(index.Definition.IngestionEnabled),
		SearchAccess:    accessState(index.Definition.SearchEnabled),
	}
}

func indexStateToProto(state control.IndexState) opensplunkv1.IndexState {
	switch state {
	case control.IndexStateActive:
		return opensplunkv1.IndexState_INDEX_STATE_ACTIVE
	case control.IndexStateArchived:
		return opensplunkv1.IndexState_INDEX_STATE_ARCHIVED
	case control.IndexStateDeleting:
		return opensplunkv1.IndexState_INDEX_STATE_DELETING
	default:
		return opensplunkv1.IndexState_INDEX_STATE_UNSPECIFIED
	}
}

func accessState(enabled bool) opensplunkv1.IndexAccessState {
	if enabled {
		return opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED
	}
	return opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_DISABLED
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

func validTimestamp(input time.Time) (*timestamppb.Timestamp, error) {
	if input.IsZero() {
		return nil, errors.New("required timestamp is zero")
	}
	return timestampToProto(input)
}

// timestampToProto accepts the protobuf minimum instant. In Go that instant is
// time.Time's zero value, which is invalid only for required metadata fields,
// not for a typed search-result cell.
func timestampToProto(input time.Time) (*timestamppb.Timestamp, error) {
	result := timestamppb.New(input.Round(0).UTC())
	if err := result.CheckValid(); err != nil {
		return nil, errors.New("timestamp is outside protobuf range")
	}
	return result, nil
}

func absoluteTimeRangeToProto(earliest, latest time.Time) *opensplunkv1.TimeRangeSpec {
	earliestText := earliest.UTC().Format(time.RFC3339Nano)
	latestText := latest.UTC().Format(time.RFC3339Nano)
	timezone := "UTC"
	return &opensplunkv1.TimeRangeSpec{Earliest: &earliestText, Latest: &latestText, Timezone: &timezone}
}

func cloneApps(input []*opensplunkv1.AppSummary) []*opensplunkv1.AppSummary {
	result := make([]*opensplunkv1.AppSummary, len(input))
	for index, app := range input {
		result[index] = proto.Clone(app).(*opensplunkv1.AppSummary)
	}
	return result
}

func appExists(apps []*opensplunkv1.AppSummary, id string) bool {
	for _, app := range apps {
		if app.GetAppId() == id {
			return true
		}
	}
	return false
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return stringPointer(value)
}

func stringPointer(value string) *string { return &value }

func uint64Pointer(value uint64) *uint64 { return &value }

func nonnegativeUint32(value int) uint32 {
	if value <= 0 {
		return 0
	}
	return uint32(value)
}

func identitySanitizer[T proto.Message](request T) (T, error) { return request, nil }
