package plan

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func convertValue(literal spl.Literal) (Value, error) {
	switch literal.Kind {
	case spl.LiteralKindString:
		return Value{Kind: ValueKindString, String: literal.Text, Quoted: literal.Quoted, SourceText: literal.Text}, nil
	case spl.LiteralKindInteger:
		if strings.HasPrefix(literal.Text, "-") {
			value, err := strconv.ParseInt(literal.Text, 10, 64)
			if err != nil {
				return Value{}, &Diagnostic{Code: "SPL_NUMBER_OUT_OF_RANGE", Message: "signed integer literal is outside the supported 64-bit range", Range: literal.Range}
			}
			return Value{Kind: ValueKindInt64, Int64: value, SourceText: literal.Text}, nil
		}
		value, err := strconv.ParseUint(strings.TrimPrefix(literal.Text, "+"), 10, 64)
		if err != nil {
			return Value{}, &Diagnostic{Code: "SPL_NUMBER_OUT_OF_RANGE", Message: "unsigned integer literal is outside the supported 64-bit range", Range: literal.Range}
		}
		if value <= math.MaxInt64 {
			return Value{Kind: ValueKindInt64, Int64: int64(value), SourceText: literal.Text}, nil
		}
		return Value{Kind: ValueKindUint64, Uint64: value, SourceText: literal.Text}, nil
	case spl.LiteralKindFloat:
		value, err := strconv.ParseFloat(literal.Text, 64)
		if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
			return Value{}, &Diagnostic{Code: "SPL_NUMBER_OUT_OF_RANGE", Message: "floating-point literal is not finite", Range: literal.Range}
		}
		return Value{Kind: ValueKindFloat64, Float64: value, SourceText: literal.Text}, nil
	case spl.LiteralKindBool:
		return Value{Kind: ValueKindBool, Bool: strings.EqualFold(literal.Text, "true"), SourceText: literal.Text}, nil
	case spl.LiteralKindNull:
		return Value{Kind: ValueKindNull, SourceText: literal.Text}, nil
	default:
		return Value{}, &Diagnostic{Code: "SPL_INVALID_LITERAL", Message: "invalid comparison literal", Range: literal.Range}
	}
}

func convertFields(names []string, sourceRange spl.Range) ([]FieldRef, error) {
	fields := make([]FieldRef, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, ok := seen[name]; ok {
			return nil, &Diagnostic{Code: "SPL_DUPLICATE_FIELD", Message: fmt.Sprintf("field %q is repeated", name), Range: sourceRange}
		}
		seen[name] = struct{}{}
		field, err := ResolveField(name, sourceRange)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func convertFieldsCommand(command *spl.FieldsCommand) ([]FieldRef, []ProjectFieldPattern, error) {
	if command == nil || len(command.Fields) == 0 ||
		len(command.Fields) > spl.MaximumExplicitProjectionFields {
		return nil, nil, &Diagnostic{
			Code: "SPL_INVALID_QUERY", Message: "fields command metadata is invalid",
		}
	}
	metadataPresent := len(command.QuotedFields) != 0 ||
		len(command.WildcardFields) != 0 || len(command.FieldRanges) != 0
	if metadataPresent && (len(command.QuotedFields) != len(command.Fields) ||
		len(command.WildcardFields) != len(command.Fields) ||
		len(command.FieldRanges) != len(command.Fields)) {
		return nil, nil, &Diagnostic{
			Code: "SPL_INVALID_FIELD", Message: "fields selector metadata is inconsistent", Range: command.Range,
		}
	}
	fields := make([]FieldRef, 0, len(command.Fields))
	patterns := make([]ProjectFieldPattern, 0, len(command.Fields))
	seen := make(map[string]struct{}, len(command.Fields))
	for index, name := range command.Fields {
		sourceRange := command.Range
		quoted := false
		wildcard := spl.IsFieldsFieldGlob(name)
		if metadataPresent {
			sourceRange = command.FieldRanges[index]
			quoted = command.QuotedFields[index]
			wildcard = command.WildcardFields[index]
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, nil, &Diagnostic{
				Code: "SPL_DUPLICATE_FIELD", Message: fmt.Sprintf("field selector %q is repeated", name), Range: sourceRange,
			}
		}
		seen[name] = struct{}{}
		if wildcard {
			if !spl.IsFieldsFieldGlob(name) {
				return nil, nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_FIELD_PATTERN", Message: "fields wildcard selector is invalid", Range: sourceRange,
				}
			}
			patterns = append(patterns, ProjectFieldPattern{Pattern: name, Range: sourceRange})
			continue
		}
		if strings.Contains(name, "*") {
			return nil, nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_FIELD_PATTERN", Message: "fields selector has inconsistent wildcard metadata", Range: sourceRange,
			}
		}
		var (
			field FieldRef
			err   error
		)
		if quoted {
			field, err = ResolveQuotedField(name, sourceRange)
		} else {
			field, err = ResolveField(name, sourceRange)
		}
		if err != nil {
			return nil, nil, err
		}
		fields = append(fields, field)
	}
	return fields, patterns, nil
}

// convertSortFields resolves authored sort keys for sort and dedup sortby.
// frequencyPlan is the logical lowering of one top or rare command.
type frequencyPlan struct {
	operators    []Operator
	outputFields []string
}

// buildFrequencyOperators lowers top and rare to a bounded count aggregate.
// Without BY the plan is Aggregate → Window(percent of total) → Sort(limit).
// With BY the aggregate groups by the BY tuple followed by the counted fields,
// the percentage is partitioned per BY tuple, the sort places each BY group's
// most (or least) frequent tuples first, and a Deduplicate keyed on the BY
// tuple keeps the first limit rows of every group. showcount=false and
// showperc=false drop the generated column after it has served the ordering.
func buildFrequencyOperators(command spl.Command) (frequencyPlan, error) {
	var (
		commandName     string
		frequencyFields []spl.FrequencyField
		byFields        []spl.FrequencyField
		commandRange    spl.Range
		limit           uint64
		countName       = "count"
		percentName     = "percent"
		hideCount       bool
		hidePercent     bool
		leastFrequent   bool
	)
	switch command := command.(type) {
	case *spl.TopCommand:
		if command == nil {
			return frequencyPlan{}, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "top command is nil"}
		}
		commandName = command.Name()
		frequencyFields, byFields, commandRange, limit = command.Fields, command.By, command.Range, command.Limit
		hideCount, hidePercent = command.HideCount, command.HidePercent
		if command.CountField != "" {
			countName = command.CountField
		}
		if command.PercentField != "" {
			percentName = command.PercentField
		}
	case *spl.RareCommand:
		if command == nil {
			return frequencyPlan{}, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "rare command is nil"}
		}
		commandName = command.Name()
		frequencyFields, byFields, commandRange, limit = command.Fields, command.By, command.Range, command.Limit
		hideCount, hidePercent = command.HideCount, command.HidePercent
		if command.CountField != "" {
			countName = command.CountField
		}
		if command.PercentField != "" {
			percentName = command.PercentField
		}
		leastFrequent = true
	default:
		return frequencyPlan{}, fmt.Errorf("build frequency plan: unexpected command %T", command)
	}
	if len(frequencyFields) == 0 {
		return frequencyPlan{}, &Diagnostic{
			Code:    "SPL_EXPECTED_FIELD",
			Message: commandName + " requires at least one field",
			Range:   commandRange,
		}
	}
	if len(frequencyFields) > spl.MaximumFrequencyFields || len(byFields) > spl.MaximumFrequencyFields {
		return frequencyPlan{}, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("%s contains more than %d fields", commandName, spl.MaximumFrequencyFields),
			Range:   commandRange,
		}
	}
	if countName == percentName {
		return frequencyPlan{}, &Diagnostic{
			Code:    "SPL_DUPLICATE_FIELD",
			Message: fmt.Sprintf("%s countfield and percentfield both name %q", commandName, countName),
			Range:   commandRange,
		}
	}
	// Splunk lists the BY tuple first, then the counted tuple, then the
	// generated columns.
	grouped := make([]spl.FrequencyField, 0, len(byFields)+len(frequencyFields))
	grouped = append(grouped, byFields...)
	grouped = append(grouped, frequencyFields...)
	outputFields := make([]string, 0, len(grouped)+2)
	for _, field := range grouped {
		if field.Name == countName || field.Name == percentName {
			return frequencyPlan{}, &Diagnostic{
				Code:    "SPL_DUPLICATE_FIELD",
				Message: fmt.Sprintf("%s field %q collides with a generated output field", commandName, field.Name),
				Range:   field.Range,
			}
		}
		outputFields = append(outputFields, field.Name)
	}
	groupBy, fieldErr := convertStatsGroupFields(commandName, grouped)
	if fieldErr != nil {
		return frequencyPlan{}, fieldErr
	}
	partitionBy := groupBy[:len(byFields):len(byFields)]
	countedFields := groupBy[len(byFields):]
	countField, countErr := ResolveField(countName, commandRange)
	if countErr != nil {
		return frequencyPlan{}, countErr
	}
	if _, percentErr := ResolveField(percentName, commandRange); percentErr != nil {
		return frequencyPlan{}, percentErr
	}

	sortKeys := make([]SortKey, 0, len(groupBy)+1)
	for _, field := range partitionBy {
		sortKeys = append(sortKeys, SortKey{Field: field, Mode: SortValueModeLexical})
	}
	sortKeys = append(sortKeys, SortKey{Field: countField, Descending: !leastFrequent})
	for _, field := range countedFields {
		sortKeys = append(sortKeys, SortKey{Field: field, Descending: true, Mode: SortValueModeLexical})
	}

	operators := []Operator{&Aggregate{
		GroupBy: groupBy,
		Measures: []AggregateMeasure{{
			Function: AggregateFunctionCountRows,
			Output:   countName,
		}},
		Range: commandRange,
	}}
	if !hidePercent {
		operators = append(operators, &Window{
			Function:    WindowFunctionPercentOfTotal,
			Input:       countField,
			Output:      percentName,
			PartitionBy: partitionBy,
			Range:       commandRange,
		})
	}
	if len(partitionBy) == 0 {
		operators = append(operators, &Sort{Keys: sortKeys, Limit: limit, Range: commandRange})
	} else {
		// Sort{Limit: 0} is unbounded: every group must be ordered before the
		// per-group retention below, and limit=0 keeps every tuple of every group.
		operators = append(operators, &Sort{Keys: sortKeys, Range: commandRange})
		if limit > 0 {
			operators = append(operators, &Deduplicate{
				Count: limit,
				Keys:  partitionBy,
				Range: commandRange,
			})
		}
	}
	if !hideCount {
		outputFields = append(outputFields, countName)
	}
	if !hidePercent {
		outputFields = append(outputFields, percentName)
	}
	if hideCount {
		operators = append(operators, &Project{
			Mode:   ProjectModeExclude,
			Fields: []FieldRef{countField},
			Range:  commandRange,
		})
	}
	return frequencyPlan{operators: operators, outputFields: outputFields}, nil
}

func convertSortFields(fields []spl.SortField) ([]SortKey, error) {
	keys := make([]SortKey, 0, len(fields))
	for _, field := range fields {
		fieldRange := field.FieldRange
		if fieldRange == (spl.Range{}) {
			fieldRange = field.Range
		}
		var (
			ref      FieldRef
			fieldErr error
		)
		if field.Quoted {
			ref, fieldErr = ResolveQuotedField(field.Field, fieldRange)
		} else {
			ref, fieldErr = ResolveField(field.Field, fieldRange)
		}
		if fieldErr != nil {
			return nil, fieldErr
		}
		mode := SortValueModeAuto
		switch field.Mode {
		case spl.SortValueModeAuto:
		case spl.SortValueModeString:
			mode = SortValueModeLexical
		case spl.SortValueModeNumber:
			mode = SortValueModeNumeric
		case spl.SortValueModeIP:
			mode = SortValueModeIP
		default:
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_QUERY",
				Message: "sort field has an invalid value mode",
				Range:   field.Range,
			}
		}
		keys = append(keys, SortKey{Field: ref, Descending: field.Descending, Mode: mode})
	}
	return keys, nil
}

func convertTableFields(command *spl.TableCommand) ([]FieldRef, error) {
	if command == nil {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "table command is nil"}
	}
	if len(command.QuotedFields) == 0 && len(command.FieldRanges) == 0 {
		return convertFields(command.Fields, command.Range)
	}
	if len(command.QuotedFields) != len(command.Fields) ||
		len(command.FieldRanges) != len(command.Fields) {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_FIELD",
			Message: "table field quote and source-range metadata is inconsistent",
			Range:   command.Range,
		}
	}
	fields := make([]FieldRef, 0, len(command.Fields))
	seen := make(map[string]struct{}, len(command.Fields))
	for index, name := range command.Fields {
		if _, duplicate := seen[name]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_FIELD",
				Message: fmt.Sprintf("field %q is repeated", name),
				Range:   command.FieldRanges[index],
			}
		}
		seen[name] = struct{}{}
		var (
			field FieldRef
			err   error
		)
		if command.QuotedFields[index] {
			field, err = ResolveQuotedField(name, command.FieldRanges[index])
		} else {
			field, err = ResolveField(name, command.FieldRanges[index])
		}
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

// ResolveField parses deterministic dotted dynamic access. A backslash escapes
// a literal dot or backslash within one path segment.
func ResolveField(name string, sourceRange spl.Range) (FieldRef, error) {
	if eventfields.IsCanonicalSPLField(name) {
		return FieldRef{Name: name, Canonical: true, Range: sourceRange}, nil
	}
	if name == "" || !utf8.ValidString(name) {
		return FieldRef{}, &Diagnostic{Code: "SPL_INVALID_FIELD", Message: "field name must be non-empty UTF-8", Range: sourceRange}
	}
	if len(name) > maxFieldNameBytes {
		return FieldRef{}, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("field name exceeds %d UTF-8 bytes", maxFieldNameBytes),
			Range:   sourceRange,
		}
	}
	if strings.HasPrefix(strings.ToLower(name), "__os_") {
		return FieldRef{}, &Diagnostic{Code: "SPL_RESERVED_FIELD", Message: "field name uses the compiler-private __os_ namespace", Range: sourceRange}
	}
	if strings.Contains(name, "*") {
		return FieldRef{}, &Diagnostic{Code: "SPL_UNSUPPORTED_FIELD_PATTERN", Message: "wildcard field-name patterns are not supported", Range: sourceRange}
	}
	path, err := splitFieldPath(name)
	if err != nil {
		return FieldRef{}, &Diagnostic{Code: "SPL_INVALID_FIELD", Message: err.Error(), Range: sourceRange}
	}
	if len(path) > maxFieldPathSegments {
		return FieldRef{}, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("field path contains more than %d segments", maxFieldPathSegments),
			Range:   sourceRange,
		}
	}
	for _, segment := range path {
		if len(segment) > maxFieldPathSegmentBytes {
			return FieldRef{}, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("field path segment exceeds %d UTF-8 bytes", maxFieldPathSegmentBytes),
				Range:   sourceRange,
			}
		}
	}
	return FieldRef{Name: name, Path: path, Range: sourceRange}, nil
}

// ResolveQuotedField resolves repository-standard single-quoted exact-field
// syntax. Canonical event paths retain their ordinary metadata. A safe stats
// literal name that cannot be a canonical path (for example ".com") receives
// one exact logical segment so downstream stages can bind it by visible Name
// without minting storage-path authority.
func ResolveQuotedField(name string, sourceRange spl.Range) (FieldRef, error) {
	if spl.IsExactQuotedFieldName(name) {
		return ResolveField(name, sourceRange)
	}
	if !spl.IsStatsLiteralFieldReference(name) {
		return FieldRef{}, &Diagnostic{
			Code:    "SPL_INVALID_FIELD",
			Message: "single-quoted field is not a safe exact field reference",
			Range:   sourceRange,
		}
	}
	return FieldRef{Name: name, Path: []string{name}, Range: sourceRange}, nil
}

func resolveStatsInputField(
	name string,
	sourceRange spl.Range,
	quoted bool,
) (FieldRef, error) {
	if quoted {
		return ResolveQuotedField(name, sourceRange)
	}
	return ResolveField(name, sourceRange)
}

func splitFieldPath(name string) ([]string, error) {
	var path []string
	var segment strings.Builder
	escaped := false
	for _, r := range name {
		if escaped {
			if r != '.' && r != '\\' {
				return nil, fmt.Errorf("field %q contains unsupported escape \\%c", name, r)
			}
			segment.WriteRune(r)
			escaped = false
			continue
		}
		switch r {
		case '\\':
			escaped = true
		case '.':
			if segment.Len() == 0 {
				return nil, fmt.Errorf("field %q contains an empty path segment", name)
			}
			path = append(path, segment.String())
			segment.Reset()
		default:
			if unicode.IsControl(r) {
				return nil, fmt.Errorf("field %q contains a control character", name)
			}
			segment.WriteRune(r)
		}
	}
	if escaped {
		return nil, fmt.Errorf("field %q ends with an incomplete escape", name)
	}
	if segment.Len() == 0 {
		return nil, fmt.Errorf("field %q contains an empty path segment", name)
	}
	path = append(path, segment.String())
	return path, nil
}
