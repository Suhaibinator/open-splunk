package clickhouse

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

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
	remainingOperators := query.Operators[1:]
	for operatorIndex, operator := range remainingOperators {
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
		case *plan.Aggregate:
			if operatorIndex+1 != len(remainingOperators) {
				return CompiledQuery{}, errors.New("compile ClickHouse aggregate: Aggregate must be the final logical operator")
			}
			projection, predicates, groups, nextState, aggregateArgs, compileErr := compileAggregate(operator, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = "SELECT " + strings.Join(projection, ", ") + " FROM (" + fragment + ") AS " + alias
			if len(predicates) > 0 {
				fragment += " WHERE " + strings.Join(predicates, " AND ")
			}
			if len(groups) > 0 {
				fragment += " GROUP BY " + strings.Join(groups, ", ")
			}
			args = append(args, aggregateArgs...)
			state = nextState
		case *plan.Sort:
			materialized, sortKeys, order, compileErr := compileSort(operator.Keys, state, aliasSequence)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = "SELECT *, " + strings.Join(materialized, ", ") + " FROM (" + fragment + ") AS " + alias + " ORDER BY " + order
			if operator.Limit > 0 {
				fragment += " LIMIT ?"
				args = append(args, operator.Limit)
			}
			state.order = sortKeys
		case *plan.Limit:
			keys := state.order
			if len(keys) == 0 {
				keys = stableCompiledSortKeys()
			}
			if operator.FromEnd {
				reversed, compileErr := compileMaterializedOrder(keys, true)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				fragment = "SELECT * FROM (" + fragment + ") AS " + alias + " ORDER BY " + reversed + " LIMIT ?"
				args = append(args, operator.Count)
				state.order = reverseCompiledSortKeys(keys)
			} else {
				order, compileErr := compileMaterializedOrder(keys, false)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				fragment = "SELECT * FROM (" + fragment + ") AS " + alias + " ORDER BY " + order + " LIMIT ?"
				args = append(args, operator.Count)
				state.order = append([]compiledSortKey(nil), keys...)
			}
		default:
			return CompiledQuery{}, fmt.Errorf("compile ClickHouse query: unsupported logical operator %T", operator)
		}
	}

	finalProjection, outputFields, err := finalProjection(state)
	if err != nil {
		return CompiledQuery{}, err
	}
	aliasSequence++
	finalOrder := state.order
	if len(finalOrder) == 0 {
		finalOrder = stableCompiledSortKeys()
	}
	order, err := compileMaterializedOrder(finalOrder, false)
	if err != nil {
		return CompiledQuery{}, err
	}
	fragment = "SELECT " + strings.Join(finalProjection, ", ") + " FROM (" + fragment + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence)) + " ORDER BY " + order
	return CompiledQuery{SQL: fragment, Args: args, OutputFields: outputFields}, nil
}

type compileState struct {
	visible      map[string]fieldState
	publicOrder  []string
	allowDynamic bool
	blocked      map[string]struct{}
	order        []compiledSortKey
}

type fieldKind uint8

const (
	fieldKindInvalid fieldKind = iota
	fieldKindDynamic
	fieldKindString
	fieldKindNumber
	fieldKindBool
	fieldKindTime
)

type fieldState struct {
	valueSQL   string
	existsSQL  string
	existsArgs []any
	kind       fieldKind
}

type compiledSortKey struct {
	valueSQL   string
	descending bool
	nullsFirst bool
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
		quoteIdentifier("event_time") + " >= parseDateTime64BestEffort(?, 9, 'UTC')",
		quoteIdentifier("event_time") + " < parseDateTime64BestEffort(?, 9, 'UTC')",
		quoteIdentifier("index_time") + " <= parseDateTime64BestEffort(?, 3, 'UTC')",
		quoteIdentifier("visibility_seq") + " <= ?",
	}
	args := make([]any, 0, len(scan.Indexes)+5)
	args = append(args, scan.TenantID)
	for _, index := range scan.Indexes {
		args = append(args, index)
	}
	// clickhouse-go infers a bare time.Time placeholder as DateTime, which has
	// only second precision. Bind canonical text and parse it explicitly so a
	// DateTime64(3) cutoff cannot exclude rows committed earlier in the same
	// second. Text also avoids UnixNano overflow for supported pre-epoch and
	// upper-bound DateTime64 values.
	args = append(args,
		formatDateTime64Nanoseconds(scan.Earliest),
		formatDateTime64Nanoseconds(scan.Latest),
		formatDateTime64Milliseconds(scan.IndexTimeCutoff),
		scan.VisibilityCutoff,
	)

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

func formatDateTime64Nanoseconds(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000000000")
}

func formatDateTime64Milliseconds(value time.Time) string {
	return value.UTC().Truncate(time.Millisecond).Format("2006-01-02 15:04:05.000")
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
	kind := fieldKindString
	switch field {
	case "severity":
		kind = fieldKindNumber
	case "_time", "_indextime":
		kind = fieldKindTime
	}
	// Canonical columns exist in the event schema even when their value is
	// nullable. This preserves explicit-null comparisons; field=* separately
	// requires a non-null value.
	return fieldState{valueSQL: value, existsSQL: "1", kind: kind}
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
		if expression.Value == "*" {
			return "isNotNull(" + raw.valueSQL + ")", nil, nil
		}
		if expression.Wildcard {
			return "match(toString(" + raw.valueSQL + "), ?)", []any{freeTextRegex(expression.Value, expression.Quoted)}, nil
		}
		if !expression.Quoted {
			return "match(toString(" + raw.valueSQL + "), ?)", []any{freeTextRegex(expression.Value, false)}, nil
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

	text := comparisonSourceText(expression.Value)
	if expression.Value.Kind == plan.ValueKindString && text == "*" &&
		(expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual) {
		if expression.Op == plan.ComparisonOpNotEqual {
			// SPL field!=* excludes missing fields and every present value,
			// including explicit null, so it cannot match an event.
			return "0", nil, nil
		}
		return "(" + exists + " AND isNotNull(" + field.valueSQL + "))", args, nil
	}

	operator, err := comparisonSQL(expression.Op)
	if err != nil {
		return "", nil, err
	}
	var predicate string
	if expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual {
		predicate = equalityPredicate(expression, field, text)
	} else {
		predicate, err = relationalPredicate(expression, field, operator)
		if err != nil {
			return "", nil, err
		}
	}
	argument := any(text)
	if expression.Value.Kind == plan.ValueKindString && strings.Contains(text, "*") &&
		(expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual) {
		argument = wildcardRegex(text, true)
	}
	args = append(args, argument)
	if expression.Op == plan.ComparisonOpNotEqual {
		// SPL field!=value excludes missing fields while treating a present null
		// as unequal to a non-null value. ifNull collapses SQL's UNKNOWN here.
		return "(" + exists + " AND NOT ifNull(" + predicate + ", 0))", args, nil
	}
	return "(" + exists + " AND ifNull(" + predicate + ", 0))", args, nil
}

func equalityPredicate(expression *plan.ComparisonExpression, field fieldState, text string) string {
	valueSQL := field.valueSQL
	if expression.Value.Kind == plan.ValueKindString && strings.Contains(text, "*") {
		return "match(toString(" + valueSQL + "), ?)"
	}
	if expression.Field.Name == "index" {
		return valueSQL + " = ?"
	}
	base := "lowerUTF8(toString(" + valueSQL + ")) = lowerUTF8(?)"
	if field.kind != fieldKindDynamic {
		return base
	}
	guard := dynamicLiteralGuard(valueSQL, expression.Value.Kind)
	if expression.Value.Kind == plan.ValueKindFloat64 {
		base = "toFloat64OrNull(toString(" + valueSQL + ")) = toFloat64OrNull(?)"
	}
	return "(" + guard + " AND " + base + ")"
}

func relationalPredicate(expression *plan.ComparisonExpression, field fieldState, operator string) (string, error) {
	if expression.Value.Kind == plan.ValueKindBool {
		return "", errors.New("compile ClickHouse predicate: booleans do not support ordered comparison")
	}
	if field.kind == fieldKindTime {
		if expression.Value.Kind == plan.ValueKindString {
			return field.valueSQL + " " + operator + " parseDateTime64BestEffortOrNull(?, 9, 'UTC')", nil
		}
		return "(toFloat64(toUnixTimestamp64Nano(" + field.valueSQL + ")) / 1000000000) " + operator + " toFloat64OrNull(?)", nil
	}
	return "toFloat64OrNull(toString(" + field.valueSQL + ")) " + operator + " toFloat64OrNull(?)", nil
}

func dynamicLiteralGuard(valueSQL string, kind plan.ValueKind) string {
	typeSQL := "dynamicType(" + valueSQL + ")"
	switch kind {
	case plan.ValueKindInt64, plan.ValueKindUint64:
		return typeSQL + " IN ('Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt128', 'UInt256')"
	case plan.ValueKindFloat64:
		return "(startsWith(" + typeSQL + ", 'Float') OR startsWith(" + typeSQL + ", 'Decimal'))"
	case plan.ValueKindBool:
		return typeSQL + " = 'Bool'"
	case plan.ValueKindString:
		return typeSQL + " = 'String'"
	default:
		return "0"
	}
}

func comparisonSourceText(value plan.Value) string {
	if value.SourceText != "" {
		return value.SourceText
	}
	switch value.Kind {
	case plan.ValueKindString:
		return value.String
	case plan.ValueKindInt64:
		return fmt.Sprintf("%d", value.Int64)
	case plan.ValueKindUint64:
		return fmt.Sprintf("%d", value.Uint64)
	case plan.ValueKindFloat64:
		return fmt.Sprintf("%g", value.Float64)
	case plan.ValueKindBool:
		if value.Bool {
			return "true"
		}
		return "false"
	case plan.ValueKindNull:
		return "null"
	default:
		return ""
	}
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
		value += "." + quoteIdentifier(encodePhysicalPathSegment(segment))
	}
	return fieldState{
		valueSQL:   value,
		existsSQL:  "has(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)",
		existsArgs: []any{normalizedDynamicPath(field.Path)},
		kind:       fieldKindDynamic,
	}, true, nil
}

func compileProjection(operator *plan.Project, state compileState) ([]string, compileState, []any, error) {
	next := compileState{
		visible:      make(map[string]fieldState),
		allowDynamic: operator.Mode != plan.ProjectModeTable,
		blocked:      cloneSet(state.blocked),
		order:        append([]compiledSortKey(nil), state.order...),
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
		next.visible[name] = fieldState{
			valueSQL: quoteIdentifier(name), existsSQL: rewriteExistenceForProjection(compiled, name),
			existsArgs: append([]any(nil), compiled.existsArgs...), kind: compiled.kind,
		}
		next.publicOrder = append(next.publicOrder, name)
	}
	privateColumns := []string{
		quoteIdentifier(internalFieldsColumn), quoteIdentifier(internalFieldNamesColumn),
		quoteIdentifier(internalSortTimeColumn), quoteIdentifier(internalSortIDColumn),
	}
	for _, key := range state.order {
		privateColumns = append(privateColumns, key.valueSQL)
	}
	for _, column := range privateColumns {
		if !slices.Contains(projection, column) {
			projection = append(projection, column)
		}
	}
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

func compileAggregate(operator *plan.Aggregate, state compileState) (
	projection []string,
	predicates []string,
	groups []string,
	next compileState,
	args []any,
	err error,
) {
	if operator == nil || len(operator.Measures) == 0 {
		return nil, nil, nil, compileState{}, nil, errors.New("compile ClickHouse aggregate: no measures")
	}
	next = compileState{
		visible:      make(map[string]fieldState, len(operator.GroupBy)+len(operator.Measures)),
		allowDynamic: false,
		blocked:      make(map[string]struct{}),
	}
	seen := make(map[string]struct{}, len(operator.GroupBy)+len(operator.Measures))
	for _, group := range operator.GroupBy {
		if _, duplicate := seen[group.Name]; duplicate {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: output field %q is duplicated", group.Name)
		}
		seen[group.Name] = struct{}{}
		field, ok, resolveErr := resolveCompiledField(group, state)
		if resolveErr != nil {
			return nil, nil, nil, compileState{}, nil, resolveErr
		}
		if !ok {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: grouping field %q is not available", group.Name)
		}
		valueSQL := field.valueSQL
		kind := field.kind
		if kind == fieldKindDynamic {
			// SPL fields are compared and grouped by their lexical value. Dynamic
			// scalar storage types therefore intentionally converge on the same
			// UTF-8 group key (for example integer 500 and string "500").
			// Missing and explicit-null values are removed below.
			valueSQL = "toString(" + valueSQL + ")"
			kind = fieldKindString
		}
		projection = append(projection, valueSQL+" AS "+quoteIdentifier(group.Name))
		predicates = append(predicates, "("+field.existsSQL+" AND isNotNull("+field.valueSQL+"))")
		args = append(args, field.existsArgs...)
		groups = append(groups, valueSQL)
		next.visible[group.Name] = fieldState{valueSQL: quoteIdentifier(group.Name), existsSQL: "1", kind: kind}
		next.publicOrder = append(next.publicOrder, group.Name)
		next.order = append(next.order, compiledSortKey{valueSQL: quoteIdentifier(group.Name)})
	}
	for _, measure := range operator.Measures {
		if _, duplicate := seen[measure.Output]; duplicate {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: output field %q is duplicated", measure.Output)
		}
		seen[measure.Output] = struct{}{}
		if measure.Function != plan.AggregateFunctionCountRows {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: unsupported function %d", measure.Function)
		}
		projection = append(projection, "count() AS "+quoteIdentifier(measure.Output))
		next.visible[measure.Output] = fieldState{valueSQL: quoteIdentifier(measure.Output), existsSQL: "1", kind: fieldKindNumber}
		next.publicOrder = append(next.publicOrder, measure.Output)
		if len(next.order) == 0 {
			next.order = append(next.order, compiledSortKey{valueSQL: quoteIdentifier(measure.Output)})
		}
	}
	return projection, predicates, groups, next, args, nil
}

func compileSort(keys []plan.SortKey, state compileState, stage int) ([]string, []compiledSortKey, string, error) {
	if len(keys) == 0 {
		return nil, nil, "", errors.New("compile ClickHouse sort: no keys")
	}
	materialized := make([]string, 0, len(keys)+1)
	compiled := make([]compiledSortKey, 0, len(keys)+1)
	for i, key := range keys {
		field, ok, err := resolveCompiledField(key.Field, state)
		if err != nil {
			return nil, nil, "", err
		}
		if !ok {
			return nil, nil, "", fmt.Errorf("compile ClickHouse sort: field %q is not available", key.Field.Name)
		}
		alias := fmt.Sprintf("__os_order_%d_%d", stage, i)
		sortValue := field.valueSQL
		if field.kind == fieldKindDynamic {
			sortValue = dynamicSortValue(field.valueSQL)
		}
		materialized = append(materialized, sortValue+" AS "+quoteIdentifier(alias))
		compiled = append(compiled, compiledSortKey{valueSQL: quoteIdentifier(alias), descending: key.Descending})
	}
	// A stable event ID tie-breaker makes result paging deterministic without
	// changing the user's primary sort semantics.
	tieAlias := fmt.Sprintf("__os_order_%d_tie", stage)
	materialized = append(materialized, quoteIdentifier(internalSortIDColumn)+" AS "+quoteIdentifier(tieAlias))
	compiled = append(compiled, compiledSortKey{valueSQL: quoteIdentifier(tieAlias), descending: true})
	order, err := compileMaterializedOrder(compiled, false)
	if err != nil {
		return nil, nil, "", err
	}
	return materialized, compiled, order, nil
}

func dynamicSortValue(valueSQL string) string {
	text := "toString(" + valueSQL + ")"
	number := "toFloat64OrNull(" + text + ")"
	// Dynamic itself is intentionally forbidden in ClickHouse ORDER BY. A
	// fixed tuple also gives SPL-like numeric ordering for numeric values and
	// strings, then puts nonnumeric scalars before missing/explicit null.
	return "tuple(" +
		"if(isNull(" + valueSQL + "), toUInt8(2), if(isNotNull(" + number + "), toUInt8(0), toUInt8(1))), " +
		"ifNull(" + number + ", 0.), " +
		"ifNull(" + text + ", '')" +
		")"
}

func compileMaterializedOrder(keys []compiledSortKey, reverse bool) (string, error) {
	if len(keys) == 0 {
		return "", errors.New("compile ClickHouse sort: no keys")
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		descending := key.descending
		nullsFirst := key.nullsFirst
		if reverse {
			descending = !descending
			nullsFirst = !nullsFirst
		}
		direction := "ASC"
		if descending {
			direction = "DESC"
		}
		nulls := "NULLS LAST"
		if nullsFirst {
			nulls = "NULLS FIRST"
		}
		parts = append(parts, key.valueSQL+" "+direction+" "+nulls)
	}
	return strings.Join(parts, ", "), nil
}

func reverseCompiledSortKeys(keys []compiledSortKey) []compiledSortKey {
	result := make([]compiledSortKey, len(keys))
	for i, key := range keys {
		key.descending = !key.descending
		key.nullsFirst = !key.nullsFirst
		result[i] = key
	}
	return result
}

func stableCompiledSortKeys() []compiledSortKey {
	return []compiledSortKey{
		{valueSQL: quoteIdentifier(internalSortTimeColumn), descending: true},
		{valueSQL: quoteIdentifier(internalSortIDColumn), descending: true},
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
		case '.', '+', '?', '(', ')', '[', ']', '{', '}', '^', '$', '|', '\\':
			result.WriteByte('\\')
			result.WriteRune(r)
		default:
			result.WriteRune(r)
		}
	}
	result.WriteByte('$')
	return result.String()
}

func freeTextRegex(value string, quoted bool) string {
	var result strings.Builder
	result.WriteString("(?i)")
	if !quoted {
		result.WriteString("(?:^|[^[:alnum:]_])")
	}
	for _, r := range value {
		if r == '*' {
			if quoted {
				result.WriteString(".*")
			} else {
				result.WriteString("[[:alnum:]_]*")
			}
			continue
		}
		if strings.ContainsRune(`.+?()[]{}^$|\\`, r) {
			result.WriteByte('\\')
		}
		result.WriteRune(r)
	}
	if !quoted {
		result.WriteString("(?:$|[^[:alnum:]_])")
	}
	return result.String()
}

func cloneSet(source map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(source))
	for key := range source {
		result[key] = struct{}{}
	}
	return result
}
