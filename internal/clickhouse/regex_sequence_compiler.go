package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

// authoredRegexProgramBudget is the compiler-owned authored-suffix accounting
// seam shared by rex/Extract, regex/RegexFilter, and scalar match(). Its
// evidence is combined with the retained knowledge charges during preparation.
// The narrower match-style sub-budget deliberately counts only RegexFilter and
// scalar match; retained and authored extraction programs retain their existing
// independently larger shared profile.
type authoredRegexProgramBudget struct {
	evidence            *authoredKnowledgeCompilation
	matchStyleWorkUnits uint64
	active              map[any]struct{}
	nodes               int
}

func (budget *authoredRegexProgramBudget) chargeShared(
	programWorkUnits int,
) error {
	if budget == nil || budget.evidence == nil || programWorkUnits <= 0 {
		return errors.New("compile ClickHouse regex budget: invalid program charge")
	}
	work := safecast.MustConv[uint64](programWorkUnits)
	budget.evidence.regexPrograms++
	budget.evidence.regexWorkUnits += work
	return nil
}

func (budget *authoredRegexProgramBudget) chargeMatchStyle(
	programWorkUnits int,
	sourceRange spl.Range,
) error {
	if programWorkUnits <= 0 {
		return errors.New("compile ClickHouse match budget: invalid program charge")
	}
	work := safecast.MustConv[uint64](programWorkUnits)
	maximum := uint64(splregex.MaximumMatchQueryProgramWorkUnits)
	if budget == nil || budget.matchStyleWorkUnits > maximum ||
		work > maximum-budget.matchStyleWorkUnits {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search match programs require more than %d work units",
				splregex.MaximumMatchQueryProgramWorkUnits,
			),
			Range: sourceRange,
		}
	}
	budget.matchStyleWorkUnits += work
	return budget.chargeShared(programWorkUnits)
}

func regexFilterPatternRange(operator *plan.RegexFilter) spl.Range {
	if operator != nil && operator.PatternRange != (spl.Range{}) {
		return operator.PatternRange
	}
	if operator != nil {
		return operator.Range
	}
	return spl.Range{}
}

func (budget *authoredRegexProgramBudget) visitOperator(operator plan.Operator) error {
	switch operator := operator.(type) {
	case *plan.Filter:
		if operator != nil {
			return budget.visitExpressionRoot(operator.Expression)
		}
	case *plan.Extend:
		if operator == nil {
			return nil
		}
		for _, assignment := range operator.Assignments {
			if err := budget.visitScalarRoot(assignment.Expression); err != nil {
				return err
			}
		}
	case *plan.Strcat:
		if operator == nil {
			return nil
		}
		for _, operand := range operator.Operands {
			if err := budget.visitScalarRoot(operand); err != nil {
				return err
			}
		}
	case *plan.Aggregate:
		if operator == nil {
			return nil
		}
		// Preserve the compiler's established structural-preflight precedence:
		// an oversized aggregate is rejected before any measure expression is
		// traversed (including a forged cyclic predicate containing match()).
		if err := validateAggregateCardinality(operator); err != nil {
			return err
		}
		for _, measure := range operator.Measures {
			if err := budget.visitMeasure(measure); err != nil {
				return err
			}
		}
	case *plan.EventAggregate:
		if operator != nil {
			return budget.visitMeasure(operator.Measure)
		}
	case *plan.StreamAggregate:
		if operator != nil {
			return budget.visitMeasure(operator.Measure)
		}
	case *plan.Timechart:
		if operator != nil {
			return budget.visitMeasure(operator.Measure)
		}
	case *plan.Chart:
		if operator != nil {
			return budget.visitMeasure(operator.Measure)
		}
	}
	return nil
}

func (budget *authoredRegexProgramBudget) visitMeasure(measure plan.AggregateMeasure) error {
	if measure.InputExpression != nil {
		if err := budget.visitScalarRoot(measure.InputExpression); err != nil {
			return err
		}
	}
	if measure.Predicate != nil {
		return budget.visitExpressionRoot(measure.Predicate)
	}
	return nil
}

func (budget *authoredRegexProgramBudget) visitScalarRoot(
	expression plan.ScalarExpression,
) error {
	budget.active = make(map[any]struct{})
	budget.nodes = 0
	return budget.visitScalar(expression, 1)
}

func (budget *authoredRegexProgramBudget) visitExpressionRoot(
	expression plan.Expression,
) error {
	budget.active = make(map[any]struct{})
	budget.nodes = 0
	return budget.visitExpression(expression, 1)
}

func (budget *authoredRegexProgramBudget) enter(
	node any,
	depth int,
	sourceRange spl.Range,
) error {
	if node == nil {
		return nil
	}
	if depth > maxCompiledPredicateDepth {
		return predicateComplexityError(
			fmt.Sprintf("regex expression nesting exceeds %d levels", maxCompiledPredicateDepth),
			sourceRange,
		)
	}
	if _, cyclic := budget.active[node]; cyclic {
		return predicateComplexityError("regex expression graph contains a cycle", sourceRange)
	}
	budget.nodes++
	if budget.nodes > maxCompiledPredicateNodes {
		return predicateComplexityError(
			fmt.Sprintf("regex expression contains more than %d structural nodes", maxCompiledPredicateNodes),
			sourceRange,
		)
	}
	budget.active[node] = struct{}{}
	return nil
}

func (budget *authoredRegexProgramBudget) visitExpression(
	expression plan.Expression,
	depth int,
) error {
	if nilPlanExpression(expression) {
		return nil
	}
	if err := budget.enter(expression, depth, expression.SourceRange()); err != nil {
		return err
	}
	defer delete(budget.active, expression)
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		if expression == nil {
			return nil
		}
		if err := budget.visitExpression(expression.Left, depth+1); err != nil {
			return err
		}
		return budget.visitExpression(expression.Right, depth+1)
	case *plan.NotExpression:
		if expression != nil {
			return budget.visitExpression(expression.Operand, depth+1)
		}
	case *plan.EvalComparisonExpression:
		if expression == nil {
			return nil
		}
		if err := budget.visitScalar(expression.Left, depth+1); err != nil {
			return err
		}
		return budget.visitScalar(expression.Right, depth+1)
	case *plan.MembershipExpression:
		if expression == nil {
			return nil
		}
		if err := budget.visitScalar(expression.Value, depth+1); err != nil {
			return err
		}
		for _, candidate := range expression.Candidates {
			if err := budget.visitScalar(candidate, depth+1); err != nil {
				return err
			}
		}
	case *plan.ScalarPredicateExpression:
		if expression != nil {
			return budget.visitScalar(expression.Value, depth+1)
		}
	}
	return nil
}

func (budget *authoredRegexProgramBudget) visitScalar(
	expression plan.ScalarExpression,
	depth int,
) error {
	if nilScalarExpression(expression) {
		return nil
	}
	if err := budget.enter(expression, depth, expression.SourceRange()); err != nil {
		return err
	}
	defer delete(budget.active, expression)
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		if expression != nil {
			return budget.visitScalar(expression.Operand, depth+1)
		}
	case *plan.ScalarBinaryExpression:
		if expression == nil {
			return nil
		}
		if err := budget.visitScalar(expression.Left, depth+1); err != nil {
			return err
		}
		return budget.visitScalar(expression.Right, depth+1)
	case *plan.ScalarCallExpression:
		if expression == nil {
			return nil
		}
		for _, argument := range expression.Arguments {
			if err := budget.visitScalar(argument, depth+1); err != nil {
				return err
			}
		}
		if expression.Function != plan.ScalarFunctionMatch || len(expression.Arguments) != 2 {
			return nil
		}
		pattern, ok := scalarQuotedStringLiteral(expression.Arguments[1])
		if !ok {
			return nil
		}
		patternRange := expression.Arguments[1].SourceRange()
		if patternRange == (spl.Range{}) {
			patternRange = expression.Range
		}
		compiled, err := compileMatchPatternForBackend(pattern, patternRange)
		if err != nil {
			return err
		}
		return budget.chargeMatchStyle(compiled.ProgramWorkUnits, patternRange)
	case *plan.ScalarIfExpression:
		if expression == nil {
			return nil
		}
		if err := budget.visitExpression(expression.Condition, depth+1); err != nil {
			return err
		}
		if err := budget.visitScalar(expression.True, depth+1); err != nil {
			return err
		}
		return budget.visitScalar(expression.False, depth+1)
	case *plan.ScalarCaseExpression:
		if expression == nil {
			return nil
		}
		for _, branch := range expression.Branches {
			if err := budget.visitExpression(branch.Condition, depth+1); err != nil {
				return err
			}
			if err := budget.visitScalar(branch.Value, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func compileRegexFilter(
	operator *plan.RegexFilter,
	state compileState,
) (string, []any, error) {
	if operator == nil || operator.Input.Name == "" ||
		operator.Range == (spl.Range{}) {
		return "", nil, errors.New("compile ClickHouse regex: operator is invalid")
	}
	if err := validateCanonicalFieldRef("regex", "input", operator.Input); err != nil {
		return "", nil, err
	}
	input := &plan.ScalarFieldExpression{Field: operator.Input, Range: operator.Input.Range}
	pattern := &plan.ScalarLiteralExpression{
		Value: plan.Value{
			Kind:       plan.ValueKindString,
			String:     operator.Pattern,
			Quoted:     true,
			SourceText: operator.Pattern,
		},
		Range: operator.Range,
	}
	call := &plan.ScalarCallExpression{
		Function:  plan.ScalarFunctionMatch,
		Arguments: []plan.ScalarExpression{input, pattern},
		Range:     operator.Range,
	}
	matched, err := compileMatchScalar(call, state)
	if err != nil {
		return "", nil, err
	}
	if !operator.Negated {
		return "ifNull(" + matched.valueSQL + ", 0)", matched.valueArgs, nil
	}
	// Splunk's regex field!=pattern includes missing and null fields. isnull()
	// shares the existing missing/null presence model, while match shares the
	// normalized regex program and bounded input conversion.
	nullCall := &plan.ScalarCallExpression{
		Function:  plan.ScalarFunctionIsNull,
		Arguments: []plan.ScalarExpression{input},
		Range:     operator.Range,
	}
	missing, err := compileNullTestScalar(nullCall, state)
	if err != nil {
		return "", nil, err
	}
	args := make([]any, 0, len(missing.valueArgs)+len(matched.valueArgs))
	args = append(args, missing.valueArgs...)
	args = append(args, matched.valueArgs...)
	return "(ifNull(" + missing.valueSQL + ", 0) OR NOT ifNull(" +
		matched.valueSQL + ", 0))", args, nil
}

func compileReverse(
	relation compiledRelation,
	operator *plan.Reverse,
	state compileState,
	stage int,
) (compiledRelation, compileState, error) {
	if operator == nil || operator.Range == (spl.Range{}) {
		return compiledRelation{}, compileState{}, errors.New(
			"compile ClickHouse reverse: operator is invalid",
		)
	}
	keys := append([]compiledSortKey(nil), defaultCompiledOrder(state)...)
	if len(keys) == 0 {
		return compiledRelation{}, compileState{}, errors.New(
			"compile ClickHouse reverse: established input order is empty",
		)
	}
	// Materialize the established tuple into private aliases before reversing.
	// This makes the tuple durable through a replacement of any authored field
	// that originally supplied an order expression, and repeated reverse simply
	// flips this same complete tuple (including stable tie breakers).
	materialized := make([]string, 0, len(keys)+len(state.tieBreakers))
	durable := make([]compiledSortKey, 0, len(keys))
	for index, key := range keys {
		name := quoteIdentifier(fmt.Sprintf("__os_reverse_order_%d_%d", stage, index))
		materialized = append(materialized, key.valueSQL+" AS "+name)
		durable = append(durable, compiledSortKey{
			valueSQL:           name,
			descending:         !key.descending,
			nullsFirst:         !key.nullsFirst,
			separatePresence:   key.separatePresence,
			presenceDescending: !key.presenceDescending,
		})
	}
	// Keep the identity-tie lineage distinct from the complete established
	// order, just as sort and streamstats do. Downstream explicit sorts consume
	// this tuple independently; aliasing it to the whole order would duplicate
	// every key at each reverse/accum composition.
	durableTieBreakers := make([]compiledSortKey, 0, len(state.tieBreakers))
	for index, key := range state.tieBreakers {
		name := quoteIdentifier(fmt.Sprintf(
			"__os_reverse_tie_breaker_%d_%d",
			stage,
			index,
		))
		materialized = append(materialized, key.valueSQL+" AS "+name)
		durableTieBreakers = append(durableTieBreakers, compiledSortKey{
			valueSQL:           name,
			descending:         !key.descending,
			nullsFirst:         !key.nullsFirst,
			separatePresence:   key.separatePresence,
			presenceDescending: !key.presenceDescending,
		})
	}
	alias := quoteIdentifier(fmt.Sprintf("_stage_%d_reverse", stage))
	order, err := compileMaterializedOrder(durable, false)
	if err != nil {
		return compiledRelation{}, compileState{}, err
	}
	result := relation.selectFrom(
		"SELECT *, "+strings.Join(materialized, ", ")+" FROM ("+
			relation.sql+") AS "+alias+" ORDER BY "+order,
		operator.Range,
	)
	next := cloneCompileState(state)
	next.order = durable
	next.tieBreakers = durableTieBreakers
	for _, key := range durable {
		next.privateColumns = append(next.privateColumns, key.valueSQL)
	}
	for _, key := range durableTieBreakers {
		next.privateColumns = append(next.privateColumns, key.valueSQL)
	}
	return result, next, nil
}

func compileStrcat(
	relation compiledRelation,
	operator *plan.Strcat,
	state compileState,
	alias string,
	stage int,
) (compiledRelation, compileState, []any, error) {
	if operator == nil || len(operator.Operands) < 2 ||
		len(operator.Operands) > spl.MaximumConcatenationOperands ||
		operator.Destination.Name == "" || operator.Range == (spl.Range{}) {
		return compiledRelation{}, compileState{}, nil, errors.New(
			"compile ClickHouse strcat: operator is invalid",
		)
	}
	if err := validateCanonicalFieldRef(
		"strcat",
		"destination",
		operator.Destination,
	); err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	call := &plan.ScalarCallExpression{
		Function:  plan.ScalarFunctionConcat,
		Arguments: operator.Operands,
		Range:     operator.Range,
	}
	value, err := compileConcatenationScalarWithNullPolicy(
		call,
		state,
		!operator.AllRequired,
	)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if operator.AllRequired {
		// concat's shared conversion returns null for every missing, explicit-null,
		// or unsupported source. The failed-write arm must preserve more than the
		// destination's physical value: sparse presence, semantic type, optional
		// multivalue nullness, and flattened-container descendants are all part of
		// the field. Select between two canonical lossless field tuples so those
		// authorities change together or not at all.
		previous, previousKnown, resolveErr := resolveCompiledField(
			operator.Destination,
			state,
		)
		if resolveErr != nil {
			return compiledRelation{}, compileState{}, nil, resolveErr
		}
		previousSource, sourceErr := compileKnowledgeFieldSourceFromField(
			previous,
			previousKnown,
		)
		if sourceErr != nil {
			return compiledRelation{}, compileState{}, nil, fmt.Errorf(
				"compile ClickHouse strcat prior destination %q: %w",
				operator.Destination.Name,
				sourceErr,
			)
		}

		const (
			candidateVariable = "__os_strcat_value"
			candidateText     = "__os_strcat_text"
			previousVariable  = "__os_strcat_previous"
		)
		candidateTextSQL := value.textEligibleSQL
		if candidateTextSQL == "" {
			candidateTextSQL = "1"
		}
		writtenSource := "tuple(toUInt8(1), CAST(" + candidateVariable +
			" AS Dynamic), toUInt8(if(ifNull(" + candidateText + ", 0), " +
			strconv.Itoa(int(eventfields.StoredValueTypeString)) + ", " +
			strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + ")), " +
			knowledgeEmptyRelativeFieldNamesSQL() + ", " +
			knowledgeEmptyRelativeFieldTypesSQL() + ", toUInt8(0))"
		selectedSource := bindSQLExpressions(
			[]string{candidateVariable, candidateText, previousVariable},
			[]string{value.valueSQL, candidateTextSQL, previousSource.sql},
			"if(isNotNull("+candidateVariable+"), "+writtenSource+", "+
				previousVariable+")",
		)
		sourceAlias := quoteIdentifier(fmt.Sprintf("__os_strcat_source_%d", stage))
		existsAlias := quoteIdentifier(fmt.Sprintf("__os_strcat_exists_%d", stage))
		nativeStringOutput := value.textEligibleSQL == "" && (!previousKnown ||
			(previous.kind == fieldKindString && previous.textEligibleSQL == "" &&
				previous.storedTypeSQL == ""))
		typeAlias := ""
		textAlias := ""
		if nativeStringOutput {
			textAlias = quoteIdentifier(fmt.Sprintf(
				"__os_strcat_text_eligible_%d",
				stage,
			))
		} else {
			typeAlias = quoteIdentifier(fmt.Sprintf("__os_strcat_type_%d", stage))
		}
		namesAlias := ""
		typesAlias := ""
		metadataAlias := ""
		preserveContainer := previousKnown &&
			(!previous.storedPath.isZero() || previous.relativeFieldNamesSQL != "")
		if preserveContainer {
			namesAlias = quoteIdentifier(fmt.Sprintf("__os_strcat_names_%d", stage))
			typesAlias = quoteIdentifier(fmt.Sprintf("__os_strcat_types_%d", stage))
			metadataAlias = quoteIdentifier(fmt.Sprintf(
				"__os_strcat_metadata_version_%d",
				stage,
			))
		}

		outputName := quoteIdentifier(operator.Destination.Name)
		outputValue := previousSource.valueSQL(sourceAlias)
		if nativeStringOutput {
			// The two possible values are String or missing, so retain the existing
			// native nullable-String result schema. The lossless tuple still carries
			// sparse presence and distinguishes preserved Bytes provenance.
			outputValue = "dynamicElement(" + outputValue + ", 'String')"
		} else {
			selectedTypeSQL := previousSource.storedTypeSQL(sourceAlias)
			selectedStringSQL := "dynamicElement(" + outputValue + ", 'String')"
			bytesValueSQL := bytesEnvelopePayloadDynamicSQL(
				rawStdBase64EncodeSQL("assumeNotNull(" + selectedStringSQL + ")"),
			)
			outputValue = "if(toUInt8(" + selectedTypeSQL + ") = toUInt8(" +
				strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "), if(dynamicType(" +
				previousSource.valueSQL(sourceAlias) + ") = 'String', " + bytesValueSQL +
				", " + previousSource.valueSQL(sourceAlias) + "), " +
				previousSource.valueSQL(sourceAlias) + ")"
		}
		projection := "SELECT " + upsertWildcardFieldProjection(
			"*",
			state,
			operator.Destination.Name,
			outputValue,
			alias,
			authoredFieldPhysicallyPublic(state, operator.Destination.Name),
		)
		projection += ", " + previousSource.producedSQL(sourceAlias) +
			" AS " + existsAlias
		if nativeStringOutput {
			projection += ", toUInt8(" + previousSource.storedTypeSQL(sourceAlias) +
				" = toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeString)) +
				")) AS " + textAlias
		} else {
			projection += ", " + previousSource.storedTypeSQL(sourceAlias) +
				" AS " + typeAlias
		}
		if preserveContainer {
			projection += ", " + previousSource.namesSQL(sourceAlias) +
				" AS " + namesAlias + ", " +
				previousSource.typesSQL(sourceAlias) + " AS " + typesAlias +
				", " + previousSource.metadataVersionSQL(sourceAlias) +
				" AS " + metadataAlias
		}
		projection += " FROM (" + relation.sql + ") AS " + alias
		result := relation.selectFrom(
			"WITH arrayJoin(["+selectedSource+"]) AS "+sourceAlias+" "+projection,
			operator.Range,
		)

		next := cloneCompileState(state)
		if exposesRawFieldsPayload(state) && !operator.Destination.Canonical {
			dropRawFieldsPayload(&next)
		}
		delete(next.blocked, operator.Destination.Name)
		if !slices.Contains(next.publicOrder, operator.Destination.Name) {
			next.publicOrder = append(next.publicOrder, operator.Destination.Name)
		}
		outputKind := fieldKindDynamic
		if nativeStringOutput {
			outputKind = fieldKindString
		}
		output := fieldState{
			valueSQL:                outputName,
			textEligibleSQL:         textAlias,
			storedTypeSQL:           typeAlias,
			existsSQL:               existsAlias,
			kind:                    outputKind,
			caseSensitive:           false,
			maxStringBytes:          value.maxStringBytes,
			alwaysNull:              value.alwaysNull && (!previousKnown || previous.alwaysNull),
			materializeForPredicate: value.materializeForPredicate || previous.materializeForPredicate,
		}
		if !nativeStringOutput {
			output.dynamicTypeSQL = "dynamicType(" + outputName + ")"
			output.dynamicDomain = dynamicScalarDomainAny
		}
		if previousKnown {
			output.maxStringBytes = max(
				output.maxStringBytes,
				fieldStateStringByteBound(previous),
			)
		}
		if preserveContainer {
			output.descendantSQL = "notEmpty(" + namesAlias + ")"
			output.relativeFieldNamesSQL = namesAlias
			output.relativeFieldTypesSQL = typesAlias
			output.fieldMetadataVersionSQL = metadataAlias
		}
		next.visible[operator.Destination.Name] = output
		next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
		next.privateColumns = append(next.privateColumns, existsAlias)
		if nativeStringOutput {
			next.privateColumns = append(next.privateColumns, textAlias)
		} else {
			next.privateColumns = append(next.privateColumns, typeAlias)
		}
		if preserveContainer {
			next.privateColumns = append(
				next.privateColumns,
				namesAlias,
				typesAlias,
				metadataAlias,
			)
		}
		prefixArgs := append([]any(nil), value.valueArgs...)
		prefixArgs = append(prefixArgs, previousSource.args...)
		return result, next, prefixArgs, nil
	}
	if value.textEligibleSQL != "" {
		// A physical String may carry semantic Bytes provenance (notably a
		// concatenation derived from binary _raw). Publish those rows through the
		// same sealed bytes/v1 Dynamic envelope used by ingestion so queryexec can
		// preserve Bytes even when their payload happens to be valid UTF-8.
		const publishedValue = "__os_strcat_published_value"
		valueSQL := bindSQLExpressions(
			[]string{publishedValue},
			[]string{value.valueSQL},
			"if(ifNull("+value.textEligibleSQL+", 0), CAST("+
				publishedValue+" AS Dynamic), "+bytesEnvelopePayloadDynamicSQL(
				rawStdBase64EncodeSQL("assumeNotNull("+publishedValue+")"),
			)+")",
		)
		published := value
		published.valueSQL = valueSQL
		published.kind = fieldKindDynamic
		published.dynamicDomain = dynamicScalarDomainAny
		published.textEligibleSQL = ""
		published.storedTypeSQL = ""
		// The bytes/v1 Dynamic envelope is now the complete public type
		// authority. Do not retain the fixed-String sidecar contract on a
		// physical Dynamic result; the ordinary result transport intentionally
		// accepts String-or-Bytes descriptors only for String columns.
		published.semanticBytesSQL = ""
		published.semanticBytesArgs = nil
		published.semanticBytesByUTF8Validity = false
		published.textEligibleBySemanticBytes = false
		published.stringOrBytes = false
		published.stringOrBytesNullable = false
		nextSQL := upsertFieldProjectionSQL(
			relation.sql,
			state,
			operator.Destination.Name,
			valueSQL,
			alias,
		)
		next, extendErr := extendCompileState(
			state,
			operator.Destination,
			published,
			false,
		)
		if extendErr != nil {
			return compiledRelation{}, compileState{}, nil, extendErr
		}
		return relation.selectFrom(nextSQL, operator.Range), next, value.valueArgs, nil
	}
	valueSQL := value.valueSQL
	nextSQL := upsertFieldProjectionSQL(
		relation.sql,
		state,
		operator.Destination.Name,
		valueSQL,
		alias,
	)
	next, err := extendCompileState(state, operator.Destination, value, false)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	return relation.selectFrom(nextSQL, operator.Range), next, value.valueArgs, nil
}
