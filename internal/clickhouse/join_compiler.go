package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// Both branches consume the same post-knowledge relation. Its definition owns
// the single set of sealed tenant, index, time, and visibility arguments.
const joinSourceName = `"__os_join_events"`

func prepareJoinSource(operators []plan.Operator, relation compiledRelation, state compileState, args []any) (compiledRelation, compileState, []any, error) {
	var join *plan.Join
	for _, op := range operators {
		if candidate, ok := op.(*plan.Join); ok && candidate != nil {
			if join != nil {
				return compiledRelation{}, compileState{}, nil, &plan.Diagnostic{Code: "SPL_QUERY_TOO_COMPLEX", Message: "only one join subsearch per search is supported", Range: candidate.Range}
			}
			join = candidate
		}
	}
	if join == nil {
		return relation, state, args, nil
	}
	state = cloneCompileState(state)
	state.chronologicalBarriers = append(state.chronologicalBarriers, compiledChronologicalBarrier{
		name: joinSourceName, sql: relation.sql, args: args, fanout: 2, depth: relation.depth, ownerRange: join.Range,
	})
	return relation.selectFrom("SELECT * FROM "+joinSourceName, join.Range), state, nil, nil
}

func joinKeyColumns(keys []plan.FieldRef, state compileState) ([]string, []string, []any, error) {
	var columns, names []string
	var args []any
	for i, key := range keys {
		value, eligible, binds, err := compileExactStringKey(key, state)
		if err != nil {
			return nil, nil, nil, err
		}
		name := quoteIdentifier(fmt.Sprintf("__os_join_key_%d", i))
		columns = append(columns, "if("+eligible+", "+value+", NULL) AS "+name)
		names = append(names, name)
		args = append(args, binds...)
	}
	return columns, names, args, nil
}

func joinFieldSource(ref plan.FieldRef, state compileState) (compiledKnowledgeFieldSource, error) {
	field, known, err := resolveCompiledField(ref, state)
	if err != nil {
		return compiledKnowledgeFieldSource{}, err
	}
	return compileKnowledgeFieldSourceFromField(field, known)
}

func compileJoin(relation compiledRelation, state compileState, source compiledDeferredKnowledgeRelation, args []any, op *plan.Join) (compiledRelation, compileState, []any, error) {
	if len(op.Keys) == 0 || len(op.Keys)+len(op.Outputs) > spl.MaximumExplicitProjectionFields {
		return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse join: invalid projection")
	}
	if err := validateCompiledPredicateComplexity(op.Filter); err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	predicate, filterArgs, err := compileFilterExpression(op.Filter, source.state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	right, keys, rightArgs, err := joinKeyColumns(op.Keys, source.state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	left, _, leftArgs, err := joinKeyColumns(op.Keys, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}

	// Freeze both sets of values before overwriting a public field. Each tuple
	// carries the value, presence, semantic type and nested object metadata.
	next := cloneCompileState(state)
	excluded := slices.Clone(keys)
	var chosen, values, replaced []string
	for i, ref := range op.Outputs {
		prior, err := joinFieldSource(ref, state)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		imported, err := joinFieldSource(ref, source.state)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		leftName := quoteIdentifier(fmt.Sprintf("__os_join_prior_%d", i))
		rightName := quoteIdentifier(fmt.Sprintf("__os_join_import_%d", i))
		selected := quoteIdentifier(fmt.Sprintf("__os_join_selected_%d", i))
		left = append(left, prior.sql+" AS "+leftName)
		leftArgs = append(leftArgs, prior.args...)
		excluded = append(excluded, leftName)
		right = append(right, imported.sql+" AS "+rightName)
		rightArgs = append(rightArgs, imported.args...)
		chosen = append(chosen, "if("+imported.producedSQL(rightName)+", "+rightName+", "+leftName+") AS "+selected)
		public := quoteIdentifier(ref.Name)
		if _, exists := state.visible[ref.Name]; exists {
			replaced = append(replaced, public)
		}
		values = append(values, imported.valueSQL(selected)+" AS "+public)
		names := imported.namesSQL(selected)
		storedType := imported.storedTypeSQL(selected)
		next.visible[ref.Name] = fieldState{
			valueSQL: public, kind: fieldKindDynamic, dynamicTypeSQL: "dynamicType(" + public + ")",
			storedTypeSQL: storedType, textEligibleSQL: storedType + " = toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeString)) + ")",
			existsSQL: imported.producedSQL(selected), descendantSQL: "notEmpty(" + names + ")",
			relativeFieldNamesSQL: names, relativeFieldTypesSQL: imported.typesSQL(selected),
			fieldMetadataVersionSQL: imported.metadataVersionSQL(selected), materializeForPredicate: true,
		}
		next.privateColumns = append(next.privateColumns, selected)
		delete(next.blocked, ref.Name)
		if !slices.Contains(next.publicOrder, ref.Name) {
			next.publicOrder = append(next.publicOrder, ref.Name)
		}
	}
	order, err := compileMaterializedOrder(stableCompiledSortKeys(), false)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	// Apply the subsearch row ceiling before selecting the first row per key.
	// Its original event order includes stable tie breakers for equal timestamps.
	bounded := "SELECT * FROM " + joinSourceName + " WHERE " + predicate + " ORDER BY " + order + " LIMIT 50000"
	rightSQL := "SELECT " + strings.Join(right, ", ") + " FROM (" + bounded + ") ORDER BY " + order + " LIMIT 1 BY " + strings.Join(keys, ", ")
	leftSQL := "SELECT *, " + strings.Join(left, ", ") + " FROM (" + relation.sql + ")"
	kind := "INNER"
	if op.Left {
		kind = "LEFT"
	}
	selectedColumns := "l.* EXCEPT (" + strings.Join(excluded, ", ") + ")"
	if len(chosen) > 0 {
		selectedColumns += ", " + strings.Join(chosen, ", ")
	}
	joined := "SELECT " + selectedColumns + " FROM (" + leftSQL + ") AS l " + kind + " ALL JOIN (" + rightSQL + ") AS r USING (" + strings.Join(keys, ", ") + ")"
	sql := joined
	if len(values) > 0 {
		star := "*"
		if len(replaced) > 0 {
			star += " EXCEPT (" + strings.Join(replaced, ", ") + ")"
		}
		sql = "SELECT " + star + ", " + strings.Join(values, ", ") + " FROM (" + joined + ")"
	}
	allArgs := append(leftArgs, args...)
	allArgs = append(allArgs, rightArgs...)
	allArgs = append(allArgs, filterArgs...)
	depth := relationalNodeDepth(relation.depth+1, source.relation.depth+2)
	if len(values) > 0 {
		depth++
	}
	return compiledRelation{sql: sql, depth: depth, ownerRange: op.Range}, next, allArgs, nil
}
