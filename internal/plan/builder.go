package plan

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// Scope is the server-resolved security and snapshot boundary for a search.
// AuthorizedIndexes must come from trusted control-plane state, never SPL.
type Scope struct {
	TenantID          string
	AuthorizedIndexes []string
	RequestedIndexes  []string
	Earliest          time.Time
	Latest            time.Time
	IndexTimeCutoff   time.Time
	// VisibilityCutoff must be resolved by the storage writer when the search
	// job starts. A pointer distinguishes an empty-table cutoff of zero from a
	// caller that forgot to establish an immutable snapshot.
	VisibilityCutoff *uint64
}

var canonicalFields = map[string]struct{}{
	"event_id":     {},
	"index":        {},
	"_time":        {},
	"_indextime":   {},
	"host":         {},
	"source":       {},
	"sourcetype":   {},
	"service":      {},
	"severity":     {},
	"level":        {},
	"message":      {},
	"_raw":         {},
	"trace_id":     {},
	"span_id":      {},
	"collector_id": {},
	"batch_id":     {},
}

// Build performs semantic analysis and emits a security-constrained plan.
func Build(query *spl.Query, scope Scope) (*Query, error) {
	if query == nil {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "query is nil"}
	}
	indexes, err := resolveIndexes(scope, query)
	if err != nil {
		return nil, err
	}
	if scope.TenantID == "" {
		return nil, &Diagnostic{Code: "SPL_INVALID_SCOPE", Message: "tenant scope is empty", Range: query.Range}
	}
	earliest := scope.Earliest.UTC()
	latest := scope.Latest.UTC()
	cutoff := scope.IndexTimeCutoff.UTC()
	if earliest.IsZero() || latest.IsZero() || !earliest.Before(latest) {
		return nil, &Diagnostic{Code: "SPL_INVALID_TIME_RANGE", Message: "time range must be a non-empty half-open interval", Range: query.Range}
	}
	if cutoff.IsZero() {
		return nil, &Diagnostic{Code: "SPL_INVALID_TIME_RANGE", Message: "index-time cutoff is required", Range: query.Range}
	}
	if scope.VisibilityCutoff == nil {
		return nil, &Diagnostic{Code: "SPL_INVALID_SNAPSHOT", Message: "storage visibility cutoff is required", Range: query.Range}
	}

	result := &Query{EffectiveIndexes: indexes}
	result.Operators = append(result.Operators, &Scan{
		TenantID:         scope.TenantID,
		Indexes:          append([]string(nil), indexes...),
		Earliest:         earliest,
		Latest:           latest,
		IndexTimeCutoff:  cutoff,
		VisibilityCutoff: *scope.VisibilityCutoff,
		Range:            query.Range,
	})
	if query.Search != nil {
		expression, convertErr := convertExpression(query.Search)
		if convertErr != nil {
			return nil, convertErr
		}
		result.Operators = append(result.Operators, &Filter{Expression: expression, Range: query.Search.SourceRange()})
	}

	outputSchemaKnown := false
	for _, command := range query.Commands {
		switch command := command.(type) {
		case *spl.SearchCommand:
			expression, convertErr := convertExpression(command.Expression)
			if convertErr != nil {
				return nil, convertErr
			}
			result.Operators = append(result.Operators, &Filter{Expression: expression, Range: command.Range})
		case *spl.FieldsCommand:
			fields, fieldErr := convertFields(command.Fields, command.Range)
			if fieldErr != nil {
				return nil, fieldErr
			}
			mode := ProjectModeInclude
			if command.Exclude {
				mode = ProjectModeExclude
			}
			result.Operators = append(result.Operators, &Project{Mode: mode, Fields: fields, Range: command.Range})
			if outputSchemaKnown {
				result.OutputFields = projectKnownOutputFields(result.OutputFields, command.Fields, command.Exclude)
				if len(result.OutputFields) == 0 {
					return nil, &Diagnostic{
						Code:        "SPL_EMPTY_PROJECTION",
						Message:     "fields removes every column from the transforming result",
						Range:       command.Range,
						Suggestions: []string{"retain at least one stats or table output field"},
					}
				}
			}
		case *spl.TableCommand:
			fields, fieldErr := convertFields(command.Fields, command.Range)
			if fieldErr != nil {
				return nil, fieldErr
			}
			result.OutputFields = append([]string(nil), command.Fields...)
			outputSchemaKnown = true
			result.Operators = append(result.Operators, &Project{Mode: ProjectModeTable, Fields: fields, Range: command.Range})
		case *spl.SortCommand:
			keys := make([]SortKey, 0, len(command.Fields))
			for _, field := range command.Fields {
				ref, fieldErr := ResolveField(field.Field, field.Range)
				if fieldErr != nil {
					return nil, fieldErr
				}
				keys = append(keys, SortKey{Field: ref, Descending: field.Descending})
			}
			limit := command.Limit
			if !command.LimitSpecified {
				limit = 10_000
			}
			result.Operators = append(result.Operators, &Sort{Keys: keys, Limit: limit, Range: command.Range})
		case *spl.LimitCommand:
			result.Operators = append(result.Operators, &Limit{Count: command.Count, FromEnd: command.Name() == "tail", Range: command.Range})
		case *spl.StatsCommand:
			if command.Aggregate.Function != spl.AggregateFunctionCount {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
					Message: "only the argument-free count aggregate is supported",
					Range:   command.Aggregate.Range,
				}
			}
			groupBy, groupErr := convertStatsGroupFields(command.GroupBy)
			if groupErr != nil {
				return nil, groupErr
			}
			if _, aliasErr := ResolveField(command.Aggregate.Alias, command.Aggregate.AliasRange); aliasErr != nil {
				return nil, aliasErr
			}
			for _, group := range groupBy {
				if group.Name == command.Aggregate.Alias {
					return nil, &Diagnostic{
						Code:    "SPL_DUPLICATE_FIELD",
						Message: fmt.Sprintf("aggregate output field %q duplicates a grouping field", command.Aggregate.Alias),
						Range:   command.Aggregate.AliasRange,
					}
				}
			}
			outputFields := make([]string, 0, len(groupBy)+1)
			for _, group := range groupBy {
				outputFields = append(outputFields, group.Name)
			}
			outputFields = append(outputFields, command.Aggregate.Alias)
			result.OutputFields = outputFields
			outputSchemaKnown = true
			result.Operators = append(result.Operators, &Aggregate{
				GroupBy: groupBy,
				Measures: []AggregateMeasure{{
					Function: AggregateFunctionCountRows,
					Output:   command.Aggregate.Alias,
				}},
				Range: command.Range,
			})
		case *spl.TopCommand:
			field, fieldErr := ResolveField(command.Field, command.FieldRange)
			if fieldErr != nil {
				return nil, fieldErr
			}
			if command.Field == "count" || command.Field == "percent" {
				return nil, &Diagnostic{
					Code:    "SPL_DUPLICATE_FIELD",
					Message: fmt.Sprintf("top field %q collides with a generated output field", command.Field),
					Range:   command.FieldRange,
				}
			}
			countField, countErr := ResolveField("count", command.Range)
			if countErr != nil {
				return nil, countErr
			}
			result.OutputFields = []string{command.Field, "count", "percent"}
			outputSchemaKnown = true
			result.Operators = append(result.Operators,
				&Aggregate{
					GroupBy: []FieldRef{field},
					Measures: []AggregateMeasure{{
						Function: AggregateFunctionCountRows,
						Output:   "count",
					}},
					Range: command.Range,
				},
				&Window{
					Function: WindowFunctionPercentOfTotal,
					Input:    countField,
					Output:   "percent",
					Range:    command.Range,
				},
				&Sort{
					Keys: []SortKey{
						{Field: countField, Descending: true},
						{Field: field, Descending: true, Mode: SortValueModeLexical},
					},
					Limit: command.Limit,
					Range: command.Range,
				},
			)
		default:
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_COMMAND",
				Message: fmt.Sprintf("unsupported command %q", command.Name()),
				Range:   command.SourceRange(),
			}
		}
	}
	return result, nil
}

func projectKnownOutputFields(current, requested []string, exclude bool) []string {
	requestedSet := make(map[string]struct{}, len(requested))
	for _, name := range requested {
		requestedSet[name] = struct{}{}
	}
	if exclude {
		result := make([]string, 0, len(current))
		for _, name := range current {
			if _, remove := requestedSet[name]; !remove {
				result = append(result, name)
			}
		}
		return result
	}

	available := make(map[string]struct{}, len(current))
	for _, name := range current {
		available[name] = struct{}{}
	}
	result := make([]string, 0, len(requested)+2)
	for _, name := range requested {
		if _, ok := available[name]; ok {
			result = append(result, name)
		}
	}
	for _, implicit := range []string{"_time", "_raw"} {
		if _, ok := available[implicit]; ok && !slices.Contains(result, implicit) {
			result = append(result, implicit)
		}
	}
	return result
}

func convertStatsGroupFields(fields []spl.StatsGroupField) ([]FieldRef, error) {
	result := make([]FieldRef, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if _, duplicate := seen[field.Name]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_FIELD",
				Message: fmt.Sprintf("stats grouping field %q is repeated", field.Name),
				Range:   field.Range,
			}
		}
		seen[field.Name] = struct{}{}
		resolved, err := ResolveField(field.Name, field.Range)
		if err != nil {
			return nil, err
		}
		result = append(result, resolved)
	}
	return result, nil
}

func resolveIndexes(scope Scope, query *spl.Query) ([]string, error) {
	authorized := normalizedSet(scope.AuthorizedIndexes)
	if len(authorized) == 0 {
		return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: "search is not authorized for any index", Range: query.Range}
	}

	effective := authorized
	if len(scope.RequestedIndexes) > 0 {
		effective = make(map[string]struct{}, len(scope.RequestedIndexes))
		for _, requested := range scope.RequestedIndexes {
			name := strings.TrimSpace(requested)
			if _, ok := authorized[name]; !ok {
				return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the authorized scope", name), Range: query.Range}
			}
			effective[name] = struct{}{}
		}
	}

	for _, reference := range positiveIndexReferences(query) {
		if strings.Contains(reference.value, "*") {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_INDEX_SELECTOR",
				Message: "wildcard index selectors are not supported in compatibility version 0.1",
				Range:   reference.range_,
			}
		}
		if _, ok := authorized[reference.value]; !ok {
			return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the authorized scope", reference.value), Range: reference.range_}
		}
		if _, ok := effective[reference.value]; !ok {
			return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the requested scope", reference.value), Range: reference.range_}
		}
	}

	indexes := make([]string, 0, len(effective))
	for index := range effective {
		indexes = append(indexes, index)
	}
	sort.Strings(indexes)
	return indexes, nil
}

type indexReference struct {
	value  string
	range_ spl.Range
}

func positiveIndexReferences(query *spl.Query) []indexReference {
	var references []indexReference
	collect := func(expression spl.Expr) {
		collectPositiveIndexReferences(expression, false, &references)
	}
	if query.Search != nil {
		collect(query.Search)
	}
	for _, command := range query.Commands {
		switch command.(type) {
		case *spl.StatsCommand, *spl.TopCommand:
			return references
		}
		if search, ok := command.(*spl.SearchCommand); ok {
			collect(search.Expression)
		}
	}
	return references
}

func collectPositiveIndexReferences(expression spl.Expr, negated bool, destination *[]indexReference) {
	switch expression := expression.(type) {
	case *spl.BinaryExpr:
		collectPositiveIndexReferences(expression.Left, negated, destination)
		collectPositiveIndexReferences(expression.Right, negated, destination)
	case *spl.NotExpr:
		collectPositiveIndexReferences(expression.Operand, !negated, destination)
	case *spl.ComparisonExpr:
		if !negated && expression.Field == "index" && expression.Op == spl.CompareOpEqual {
			*destination = append(*destination, indexReference{value: expression.Value.Text, range_: expression.Range})
		}
	}
}

func normalizedSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result[value] = struct{}{}
		}
	}
	return result
}

func convertExpression(expression spl.Expr) (Expression, error) {
	switch expression := expression.(type) {
	case *spl.BinaryExpr:
		left, err := convertExpression(expression.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertExpression(expression.Right)
		if err != nil {
			return nil, err
		}
		op := BooleanOpAnd
		if expression.Op == spl.BoolOpOr {
			op = BooleanOpOr
		}
		return &BooleanExpression{Op: op, Left: left, Right: right, Range: expression.Range}, nil
	case *spl.NotExpr:
		operand, err := convertExpression(expression.Operand)
		if err != nil {
			return nil, err
		}
		return &NotExpression{Operand: operand, Range: expression.Range}, nil
	case *spl.TermExpr:
		return &TextExpression{Value: expression.Value, Quoted: expression.Quoted, Wildcard: strings.Contains(expression.Value, "*"), Range: expression.Range}, nil
	case *spl.ComparisonExpr:
		field, err := ResolveField(expression.Field, expression.Range)
		if err != nil {
			return nil, err
		}
		value, err := convertValue(expression.Value)
		if err != nil {
			return nil, err
		}
		return &ComparisonExpression{Field: field, Op: convertComparisonOp(expression.Op), Value: value, Range: expression.Range}, nil
	default:
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_EXPRESSION", Message: fmt.Sprintf("unsupported expression type %T", expression), Range: expression.SourceRange()}
	}
}

func convertComparisonOp(op spl.CompareOp) ComparisonOp {
	switch op {
	case spl.CompareOpEqual:
		return ComparisonOpEqual
	case spl.CompareOpNotEqual:
		return ComparisonOpNotEqual
	case spl.CompareOpLess:
		return ComparisonOpLess
	case spl.CompareOpLessEqual:
		return ComparisonOpLessEqual
	case spl.CompareOpGreater:
		return ComparisonOpGreater
	case spl.CompareOpGreaterEqual:
		return ComparisonOpGreaterEqual
	default:
		return ComparisonOpInvalid
	}
}

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

// ResolveField parses deterministic dotted dynamic access. A backslash escapes
// a literal dot or backslash within one path segment.
func ResolveField(name string, sourceRange spl.Range) (FieldRef, error) {
	if _, ok := canonicalFields[name]; ok {
		return FieldRef{Name: name, Canonical: true}, nil
	}
	if name == "" || !utf8.ValidString(name) {
		return FieldRef{}, &Diagnostic{Code: "SPL_INVALID_FIELD", Message: "field name must be non-empty UTF-8", Range: sourceRange}
	}
	if strings.HasPrefix(strings.ToLower(name), "__os_") {
		return FieldRef{}, &Diagnostic{Code: "SPL_RESERVED_FIELD", Message: "field name uses the compiler-private __os_ namespace", Range: sourceRange}
	}
	if strings.Contains(name, "*") {
		return FieldRef{}, &Diagnostic{Code: "SPL_UNSUPPORTED_FIELD_PATTERN", Message: "wildcard field-name patterns are not supported in compatibility version 0.1", Range: sourceRange}
	}
	path, err := splitFieldPath(name)
	if err != nil {
		return FieldRef{}, &Diagnostic{Code: "SPL_INVALID_FIELD", Message: err.Error(), Range: sourceRange}
	}
	return FieldRef{Name: name, Path: path}, nil
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
			if r < 0x20 || r == 0x7f {
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
