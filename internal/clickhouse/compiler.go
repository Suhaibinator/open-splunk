package clickhouse

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

const (
	internalFieldsColumn     = "__os_fields"
	internalFieldNamesColumn = "__os_field_names"
	internalSortTimeColumn   = "__os_sort_time"
	internalSortIDColumn     = "__os_sort_event_id"
)

var physicalIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Compiler lowers backend-neutral logical plans to parameterized ClickHouse
// SQL. Database and table are trusted configuration and still pass a strict
// identifier allowlist; all user-authored values are query parameters.
type Compiler struct {
	Database string
	Table    string
}

// CompiledQuery is executable SQL plus ordered bind arguments and public
// result fields. Internal helper columns never appear in OutputFields.
type CompiledQuery struct {
	SQL          string
	Args         []any
	OutputFields []string
}

// Compile compiles one plan without mutating it.
func (c Compiler) Compile(query *plan.Query) (CompiledQuery, error) {
	if query == nil || len(query.Operators) == 0 {
		return CompiledQuery{}, errors.New("compile ClickHouse query: logical plan is empty")
	}
	database := c.Database
	if database == "" {
		database = "open_splunk"
	}
	table := c.Table
	if table == "" {
		table = "events"
	}
	if !physicalIdentifier.MatchString(database) || !physicalIdentifier.MatchString(table) {
		return CompiledQuery{}, errors.New("compile ClickHouse query: database and table must be simple identifiers")
	}
	scan, ok := query.Operators[0].(*plan.Scan)
	if !ok {
		return CompiledQuery{}, errors.New("compile ClickHouse query: first operator must be Scan")
	}
	fragment, state, args, err := compileScan(database, table, scan)
	if err != nil {
		return CompiledQuery{}, err
	}

	aliasSequence := 0
	for _, operator := range query.Operators[1:] {
		aliasSequence++
		alias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
		switch operator := operator.(type) {
		case *plan.Filter:
			predicate, predicateArgs, compileErr := compileExpression(operator.Expression, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = "SELECT * FROM (" + fragment + ") AS " + alias + " WHERE " + predicate
			args = append(args, predicateArgs...)
		case *plan.Project:
			projection, nextState, projectionArgs, compileErr := compileProjection(operator, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = "SELECT " + strings.Join(projection, ", ") + " FROM (" + fragment + ") AS " + alias
			args = append(args, projectionArgs...)
			state = nextState
		case *plan.Sort:
			order, orderArgs, compileErr := compileOrder(operator.Keys, state, false)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = "SELECT * FROM (" + fragment + ") AS " + alias + " ORDER BY " + order
			args = append(args, orderArgs...)
			if operator.Limit > 0 {
				fragment += " LIMIT ?"
				args = append(args, operator.Limit)
			}
			state.order = append([]plan.SortKey(nil), operator.Keys...)
		case *plan.Limit:
			keys := state.order
			if len(keys) == 0 {
				keys = stableSortKeys()
			}
			if operator.FromEnd {
				reversed, reversedArgs, compileErr := compileOrder(keys, state, true)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				innerAlias := quoteIdentifier(fmt.Sprintf("_tail_%d", aliasSequence))
				fragment = "SELECT * FROM (SELECT * FROM (" + fragment + ") AS " + alias + " ORDER BY " + reversed + " LIMIT ?) AS " + innerAlias
				args = append(args, reversedArgs...)
				args = append(args, operator.Count)
				forward, forwardArgs, compileErr := compileOrder(keys, state, false)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				fragment += " ORDER BY " + forward
				args = append(args, forwardArgs...)
			} else {
				if len(state.order) == 0 {
					order, orderArgs, compileErr := compileOrder(keys, state, false)
					if compileErr != nil {
						return CompiledQuery{}, compileErr
					}
					fragment = "SELECT * FROM (" + fragment + ") AS " + alias + " ORDER BY " + order + " LIMIT ?"
					args = append(args, orderArgs...)
				} else {
					fragment = "SELECT * FROM (" + fragment + ") AS " + alias + " LIMIT ?"
				}
				args = append(args, operator.Count)
			}
		default:
			return CompiledQuery{}, fmt.Errorf("compile ClickHouse query: unsupported logical operator %T", operator)
		}
	}

	if len(state.order) == 0 && !hasLimit(query.Operators) {
		order, orderArgs, compileErr := compileOrder(stableSortKeys(), state, false)
		if compileErr != nil {
			return CompiledQuery{}, compileErr
		}
		aliasSequence++
		fragment = "SELECT * FROM (" + fragment + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence)) + " ORDER BY " + order
		args = append(args, orderArgs...)
	}

	finalProjection, outputFields, err := finalProjection(state)
	if err != nil {
		return CompiledQuery{}, err
	}
	aliasSequence++
	fragment = "SELECT " + strings.Join(finalProjection, ", ") + " FROM (" + fragment + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	return CompiledQuery{SQL: fragment, Args: args, OutputFields: outputFields}, nil
}

type compileState struct {
	visible      map[string]fieldState
	publicOrder  []string
	allowDynamic bool
	blocked      map[string]struct{}
	order        []plan.SortKey
}

type fieldState struct {
	valueSQL   string
	existsSQL  string
	existsArgs []any
}

func compileScan(database, table string, scan *plan.Scan) (string, compileState, []any, error) {
	if scan.TenantID == "" || len(scan.Indexes) == 0 || scan.Earliest.IsZero() || scan.Latest.IsZero() || !scan.Earliest.Before(scan.Latest) || scan.IndexTimeCutoff.IsZero() {
		return "", compileState{}, nil, errors.New("compile ClickHouse query: Scan has an invalid security or time scope")
	}
	selects := []string{
		aliasPhysical("event_id", "event_id"),
		aliasPhysical("index_name", "index"),
		aliasPhysical("event_time", "_time"),
		aliasPhysical("index_time", "_indextime"),
		aliasPhysical("host", "host"),
		aliasPhysical("source", "source"),
		aliasPhysical("sourcetype", "sourcetype"),
		aliasPhysical("service", "service"),
		aliasPhysical("severity", "severity"),
		aliasPhysical("level", "level"),
		aliasPhysical("body", "message"),
		aliasPhysical("raw", "_raw"),
		aliasPhysical("trace_id", "trace_id"),
		aliasPhysical("span_id", "span_id"),
		aliasPhysical("collector_id", "collector_id"),
		aliasPhysical("batch_id", "batch_id"),
		aliasPhysical("fields", internalFieldsColumn),
		aliasPhysical("field_names", internalFieldNamesColumn),
		aliasPhysical("event_time", internalSortTimeColumn),
		aliasPhysical("event_id", internalSortIDColumn),
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(scan.Indexes)), ", ")
	where := []string{
		quoteIdentifier("tenant_id") + " = ?",
		quoteIdentifier("index_name") + " IN (" + placeholders + ")",
		quoteIdentifier("event_time") + " >= ?",
		quoteIdentifier("event_time") + " < ?",
		quoteIdentifier("index_time") + " <= ?",
	}
	args := make([]any, 0, len(scan.Indexes)+4)
	args = append(args, scan.TenantID)
	for _, index := range scan.Indexes {
		args = append(args, index)
	}
	args = append(args, scan.Earliest.UTC(), scan.Latest.UTC(), scan.IndexTimeCutoff.UTC())

	visible := make(map[string]fieldState, len(canonicalColumnNames))
	for _, field := range canonicalColumnNames {
		visible[field] = canonicalState(field)
	}
	state := compileState{
		visible:      visible,
		publicOrder:  append([]string(nil), defaultPublicFields...),
		allowDynamic: true,
		blocked:      make(map[string]struct{}),
	}
	return "SELECT " + strings.Join(selects, ", ") + " FROM " + quoteIdentifier(database) + "." + quoteIdentifier(table) + " WHERE " + strings.Join(where, " AND "), state, args, nil
}

var canonicalColumnNames = []string{
	"event_id", "index", "_time", "_indextime", "host", "source", "sourcetype",
	"service", "severity", "level", "message", "_raw", "trace_id", "span_id", "collector_id", "batch_id",
}

var defaultPublicFields = []string{
	"_time", "_raw", "index", "host", "source", "sourcetype", "service", "level", "message", "trace_id", "span_id", "event_id", "_indextime", "fields",
}

func canonicalState(field string) fieldState {
	value := quoteIdentifier(field)
	switch field {
	case "service", "level", "message", "trace_id", "span_id":
		return fieldState{valueSQL: value, existsSQL: "isNotNull(" + value + ")"}
	default:
		return fieldState{valueSQL: value, existsSQL: "1"}
	}
}

func compileExpression(expression plan.Expression, state compileState) (string, []any, error) {
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		left, leftArgs, err := compileExpression(expression.Left, state)
		if err != nil {
			return "", nil, err
		}
		right, rightArgs, err := compileExpression(expression.Right, state)
		if err != nil {
			return "", nil, err
		}
		operator := "AND"
		if expression.Op == plan.BooleanOpOr {
			operator = "OR"
		}
		return "(" + left + " " + operator + " " + right + ")", append(leftArgs, rightArgs...), nil
	case *plan.NotExpression:
		operand, args, err := compileExpression(expression.Operand, state)
		if err != nil {
			return "", nil, err
		}
		return "NOT (" + operand + ")", args, nil
	case *plan.TextExpression:
		raw, ok := state.visible["_raw"]
		if !ok {
			return "0", nil, nil
		}
		if expression.Wildcard {
			return "match(toString(" + raw.valueSQL + "), ?)", []any{wildcardRegex(expression.Value, true)}, nil
		}
		return "positionCaseInsensitiveUTF8(toString(" + raw.valueSQL + "), ?) > 0", []any{expression.Value}, nil
	case *plan.ComparisonExpression:
		field, ok, err := resolveCompiledField(expression.Field, state)
		if err != nil {
			return "", nil, err
		}
		if !ok {
			return "0", nil, nil
		}
		return compileComparison(expression, field)
	default:
		return "", nil, fmt.Errorf("compile ClickHouse predicate: unsupported expression %T", expression)
	}
}

func compileComparison(expression *plan.ComparisonExpression, field fieldState) (string, []any, error) {
	exists := field.existsSQL
	args := append([]any(nil), field.existsArgs...)
	if expression.Value.Kind == plan.ValueKindNull {
		equal := "(" + exists + " AND isNull(" + field.valueSQL + "))"
		if expression.Op == plan.ComparisonOpEqual {
			return equal, args, nil
		}
		if expression.Op == plan.ComparisonOpNotEqual {
			return "(" + exists + " AND NOT isNull(" + field.valueSQL + "))", args, nil
		}
		return "", nil, errors.New("compile ClickHouse predicate: null only supports = and !=")
	}

	operator, err := comparisonSQL(expression.Op)
	if err != nil {
		return "", nil, err
	}
	valueSQL := field.valueSQL
	var predicate string
	var argument any
	switch expression.Value.Kind {
	case plan.ValueKindString:
		argument = expression.Value.String
		if strings.ContainsAny(expression.Value.String, "*?") && (expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual) {
			predicate = "match(toString(" + valueSQL + "), ?)"
			argument = wildcardRegex(expression.Value.String, true)
		} else if expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual {
			if expression.Field.Name == "index" {
				predicate = valueSQL + " = ?"
			} else {
				predicate = "lowerUTF8(toString(" + valueSQL + ")) = lowerUTF8(?)"
			}
		} else {
			predicate = "toString(" + valueSQL + ") " + operator + " ?"
		}
	case plan.ValueKindInt64:
		argument = expression.Value.Int64
		predicate = valueSQL + " " + operator + " ?"
	case plan.ValueKindUint64:
		argument = expression.Value.Uint64
		predicate = valueSQL + " " + operator + " ?"
	case plan.ValueKindFloat64:
		argument = expression.Value.Float64
		predicate = valueSQL + " " + operator + " ?"
	case plan.ValueKindBool:
		argument = expression.Value.Bool
		predicate = valueSQL + " " + operator + " ?"
	default:
		return "", nil, errors.New("compile ClickHouse predicate: invalid literal type")
	}
	args = append(args, argument)
	if expression.Op == plan.ComparisonOpNotEqual {
		// SPL field!=value excludes missing fields while treating a present null
		// as unequal to a non-null value. ifNull collapses SQL's UNKNOWN here.
		return "(" + exists + " AND NOT ifNull(" + predicate + ", 0))", args, nil
	}
	return "(" + exists + " AND ifNull(" + predicate + ", 0))", args, nil
}

func comparisonSQL(operator plan.ComparisonOp) (string, error) {
	switch operator {
	case plan.ComparisonOpEqual, plan.ComparisonOpNotEqual:
		return "=", nil
	case plan.ComparisonOpLess:
		return "<", nil
	case plan.ComparisonOpLessEqual:
		return "<=", nil
	case plan.ComparisonOpGreater:
		return ">", nil
	case plan.ComparisonOpGreaterEqual:
		return ">=", nil
	default:
		return "", errors.New("compile ClickHouse predicate: invalid comparison operator")
	}
}

func resolveCompiledField(field plan.FieldRef, state compileState) (fieldState, bool, error) {
	if existing, ok := state.visible[field.Name]; ok {
		return existing, true, nil
	}
	if _, blocked := state.blocked[field.Name]; blocked || field.Canonical || !state.allowDynamic {
		return fieldState{}, false, nil
	}
	if len(field.Path) == 0 {
		return fieldState{}, false, fmt.Errorf("compile ClickHouse field %q: dynamic path is empty", field.Name)
	}
	value := quoteIdentifier(internalFieldsColumn)
	for _, segment := range field.Path {
		if segment == "" {
			return fieldState{}, false, fmt.Errorf("compile ClickHouse field %q: dynamic path has empty segment", field.Name)
		}
		value += "." + quoteIdentifier(segment)
	}
	return fieldState{
		valueSQL:   value,
		existsSQL:  "has(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)",
		existsArgs: []any{normalizedDynamicPath(field.Path)},
	}, true, nil
}

func compileProjection(operator *plan.Project, state compileState) ([]string, compileState, []any, error) {
	next := compileState{
		visible:      make(map[string]fieldState),
		allowDynamic: operator.Mode != plan.ProjectModeTable,
		blocked:      cloneSet(state.blocked),
		order:        append([]plan.SortKey(nil), state.order...),
	}
	var names []string
	switch operator.Mode {
	case plan.ProjectModeInclude, plan.ProjectModeTable:
		for _, field := range operator.Fields {
			names = append(names, field.Name)
		}
		if operator.Mode == plan.ProjectModeInclude {
			for _, implicit := range []string{"_time", "_raw"} {
				if _, exists := state.visible[implicit]; exists && !slices.Contains(names, implicit) {
					names = append(names, implicit)
				}
			}
		}
	case plan.ProjectModeExclude:
		excluded := make(map[string]struct{}, len(operator.Fields))
		for _, field := range operator.Fields {
			excluded[field.Name] = struct{}{}
			next.blocked[field.Name] = struct{}{}
		}
		for _, name := range state.publicOrder {
			if name == "fields" {
				continue // avoid leaking excluded dynamic members in the public object
			}
			if _, remove := excluded[name]; !remove {
				names = append(names, name)
			}
		}
	default:
		return nil, compileState{}, nil, errors.New("compile ClickHouse projection: invalid mode")
	}

	projection := make([]string, 0, len(names)+4)
	args := make([]any, 0)
	for _, name := range names {
		var ref plan.FieldRef
		for _, candidate := range operator.Fields {
			if candidate.Name == name {
				ref = candidate
				break
			}
		}
		if ref.Name == "" {
			ref = plan.FieldRef{Name: name, Canonical: true}
		}
		compiled, ok, err := resolveCompiledField(ref, state)
		if err != nil {
			return nil, compileState{}, nil, err
		}
		if !ok {
			continue
		}
		projection = append(projection, compiled.valueSQL+" AS "+quoteIdentifier(name))
		next.visible[name] = fieldState{valueSQL: quoteIdentifier(name), existsSQL: rewriteExistenceForProjection(compiled, name), existsArgs: append([]any(nil), compiled.existsArgs...)}
		next.publicOrder = append(next.publicOrder, name)
	}
	projection = append(projection,
		quoteIdentifier(internalFieldsColumn),
		quoteIdentifier(internalFieldNamesColumn),
		quoteIdentifier(internalSortTimeColumn),
		quoteIdentifier(internalSortIDColumn),
	)
	if operator.Mode == plan.ProjectModeTable {
		next.allowDynamic = false
	}
	return projection, next, args, nil
}

func rewriteExistenceForProjection(field fieldState, name string) string {
	if field.existsSQL == "1" {
		return "1"
	}
	if strings.HasPrefix(field.existsSQL, "isNotNull(") {
		return "isNotNull(" + quoteIdentifier(name) + ")"
	}
	return field.existsSQL
}

func compileOrder(keys []plan.SortKey, state compileState, reverse bool) (string, []any, error) {
	parts := make([]string, 0, len(keys))
	args := make([]any, 0)
	for _, key := range keys {
		var value string
		if key.Field.Name == internalSortTimeColumn || key.Field.Name == internalSortIDColumn {
			value = quoteIdentifier(key.Field.Name)
		} else {
			field, ok, err := resolveCompiledField(key.Field, state)
			if err != nil {
				return "", nil, err
			}
			if !ok {
				return "", nil, fmt.Errorf("compile ClickHouse sort: field %q is not available", key.Field.Name)
			}
			value = field.valueSQL
		}
		descending := key.Descending
		if reverse {
			descending = !descending
		}
		direction := "ASC"
		if descending {
			direction = "DESC"
		}
		parts = append(parts, value+" "+direction+" NULLS LAST")
	}
	if len(parts) == 0 {
		return "", nil, errors.New("compile ClickHouse sort: no keys")
	}
	return strings.Join(parts, ", "), args, nil
}

func stableSortKeys() []plan.SortKey {
	return []plan.SortKey{
		{Field: plan.FieldRef{Name: internalSortTimeColumn}, Descending: true},
		{Field: plan.FieldRef{Name: internalSortIDColumn}, Descending: true},
	}
}

func finalProjection(state compileState) ([]string, []string, error) {
	projection := make([]string, 0, len(state.publicOrder))
	output := make([]string, 0, len(state.publicOrder))
	seen := make(map[string]struct{}, len(state.publicOrder))
	for _, name := range state.publicOrder {
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		if name == "fields" {
			projection = append(projection, quoteIdentifier(internalFieldsColumn)+" AS "+quoteIdentifier("fields"))
			output = append(output, name)
			continue
		}
		if _, ok := state.visible[name]; !ok {
			continue
		}
		projection = append(projection, quoteIdentifier(name))
		output = append(output, name)
	}
	if len(projection) == 0 {
		return nil, nil, errors.New("compile ClickHouse query: projection has no visible fields")
	}
	return projection, output, nil
}

func hasLimit(operators []plan.Operator) bool {
	for _, operator := range operators {
		if _, ok := operator.(*plan.Limit); ok {
			return true
		}
		if sortOperator, ok := operator.(*plan.Sort); ok && sortOperator.Limit > 0 {
			return true
		}
	}
	return false
}

func aliasPhysical(physical, alias string) string {
	return quoteIdentifier(physical) + " AS " + quoteIdentifier(alias)
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func normalizedDynamicPath(path []string) string {
	parts := make([]string, len(path))
	for i, segment := range path {
		segment = strings.ReplaceAll(segment, `\`, `\\`)
		segment = strings.ReplaceAll(segment, `.`, `\.`)
		parts[i] = segment
	}
	return strings.Join(parts, ".")
}

func wildcardRegex(value string, caseInsensitive bool) string {
	var result strings.Builder
	if caseInsensitive {
		result.WriteString("(?i)")
	}
	result.WriteByte('^')
	for _, r := range value {
		switch r {
		case '*':
			result.WriteString(".*")
		case '?':
			result.WriteByte('.')
		case '.', '+', '(', ')', '[', ']', '{', '}', '^', '$', '|', '\\':
			result.WriteByte('\\')
			result.WriteRune(r)
		default:
			result.WriteRune(r)
		}
	}
	result.WriteByte('$')
	return result.String()
}

func cloneSet(source map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(source))
	for key := range source {
		result[key] = struct{}{}
	}
	return result
}
