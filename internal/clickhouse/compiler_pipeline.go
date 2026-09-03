package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func (c Compiler) compileWithFinalizerContext(
	ctx context.Context,
	query *plan.Query,
	finalize queryFinalizer,
	permitTerminalWideOperators bool,
) (CompiledQuery, error) {
	if ctx == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse query: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return CompiledQuery{}, err
	}
	if query == nil || len(query.Operators) == 0 {
		return CompiledQuery{}, errors.New("compile ClickHouse query: logical plan is empty")
	}
	if finalize == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse query: finalizer is required")
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
	if isNilPlanOperator(query.Operators[0]) {
		return CompiledQuery{}, errors.New("compile ClickHouse query: first operator must be a non-nil Scan")
	}
	scan, ok := query.Operators[0].(*plan.Scan)
	if !ok {
		return CompiledQuery{}, errors.New("compile ClickHouse query: first operator must be Scan")
	}
	preparation, err := prepareKnowledgeCompilation(query)
	if err != nil {
		return CompiledQuery{}, err
	}
	lookupPreparation, err := prepareLookupCompilationContext(
		ctx,
		query,
		scan,
		c.lookupResolutions,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	fragment, state, args, err := compileScan(
		database,
		table,
		scan,
		query.SearchStart,
		query.SearchTimezone,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	if state.context == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse query: compile context is unavailable")
	}
	state.context.operationContext = ctx
	relation := newScanRelation(fragment, scan.Range)
	knowledge, err := compileDeferredKnowledgeRelation(
		relation,
		state,
		args,
		preparation,
		lookupPreparation.automatic,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	relation = knowledge.relation
	state = knowledge.state
	args = knowledge.args

	aliasSequence := 0
	lookupStageIndex := 0
	var statsPartitionsMaxThreadsHint uint8
	finishCompiled := func(
		compiled CompiledQuery,
		complexityRange spl.Range,
	) (CompiledQuery, error) {
		compiled.atomicResult = state.context != nil && state.context.atomicResult
		terminalWide := compiled.Chart != nil || compiled.Timechart != nil
		if terminalWide && len(state.chronologicalBarriers) > 0 {
			var wrapErr error
			compiled, wrapErr = wrapCompiledChronologicalValidation(
				compiled,
				state,
				aliasSequence,
			)
			if wrapErr != nil {
				return CompiledQuery{}, wrapErr
			}
		}
		if terminalWide {
			if depthErr := validateCompiledRelationalDepth(compiled); depthErr != nil {
				return CompiledQuery{}, depthErr
			}
		} else if depthErr := validateFinalizedRelationalDepth(relation, compiled); depthErr != nil {
			return CompiledQuery{}, depthErr
		}
		if state.context != nil && state.context.requiresMaterializedValidationSettings {
			compiled.SQL = applyMaterializedValidationSettings(compiled.SQL)
		}
		compiled.statsPartitionsMaxThreadsHint = statsPartitionsMaxThreadsHint
		if state.context != nil {
			compiled.lookupTables = cloneCompiledLookupExternalTables(
				state.context.lookupTables,
			)
		}
		if len(compiled.SQL) > maxCompiledQueryBytes {
			return CompiledQuery{}, &plan.Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("compiled query exceeds %d bytes", maxCompiledQueryBytes),
				Range:   complexityRange,
			}
		}
		return sealFinalCompiledQueryContext(
			ctx,
			compiled,
			query,
			scan,
			preparation,
			knowledge.prelude,
			lookupPreparation,
		)
	}
	remainingStart := 1 + preparation.prefixLength
	if lookupPreparation.automatic != nil {
		if remainingStart >= len(query.Operators) ||
			query.Operators[remainingStart] != lookupPreparation.automatic.operator {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse automatic lookups: prepared group order disagrees with the logical plan",
			)
		}
		remainingStart++
	}
	remainingOperators := query.Operators[remainingStart:]
	for operatorIndex := 0; operatorIndex < len(remainingOperators); operatorIndex++ {
		operator := remainingOperators[operatorIndex]
		if isNilPlanOperator(operator) {
			return CompiledQuery{}, fmt.Errorf(
				"compile ClickHouse query: operator %d is nil",
				operatorIndex+1+preparation.prefixLength,
			)
		}
		aliasSequence++
		alias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
		switch operator := operator.(type) {
		case *plan.Filter:
			if complexityErr := validateCompiledPredicateComplexity(operator.Expression); complexityErr != nil {
				return CompiledQuery{}, complexityErr
			}
			materializedFields := predicateMaterializationFields(operator.Expression, state)
			exactNumericFields := repeatedExactNumericPredicateFields(
				operator.Expression,
				state,
			)
			predicateState := state
			nextState := state
			var excludedColumns, replacements, bindings []string
			if len(materializedFields) > 0 {
				predicateState, nextState, excludedColumns, replacements, bindings = bindMaterializedPredicateFields(
					state,
					materializedFields,
					aliasSequence,
				)
			}
			exactNumericAliases := make([]string, 0, len(exactNumericFields)*2)
			if len(exactNumericFields) > 0 {
				var keyColumns []string
				predicateState, keyColumns, exactNumericAliases, err = bindExactNumericPredicateFields(
					predicateState,
					exactNumericFields,
					aliasSequence,
					"filter",
				)
				if err != nil {
					return CompiledQuery{}, err
				}
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(keyColumns, ", ")+" FROM ("+
						relation.sql+") AS "+alias,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
			}
			predicate, predicateArgs, compileErr := compileFilterExpression(
				operator.Expression,
				predicateState,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			filterProjection := "*"
			if len(exactNumericAliases) > 0 {
				filterProjection = "* EXCEPT (" +
					strings.Join(exactNumericAliases, ", ") + ")"
			}
			filterSQL := "SELECT " + filterProjection + " FROM (" +
				relation.sql + ") AS " + alias + " WHERE " + predicate
			if len(materializedFields) > 0 {
				materialized := quoteIdentifier(fmt.Sprintf("__os_filter_input_%d", aliasSequence))
				privateColumns := append(
					append([]string(nil), excludedColumns...),
					exactNumericAliases...,
				)
				filterSQL = "WITH " + materialized + " AS MATERIALIZED (" + relation.sql + ") " +
					"SELECT * EXCEPT (" + strings.Join(privateColumns, ", ") + ") REPLACE (" +
					strings.Join(replacements, ", ") + ") FROM " + materialized + " AS " +
					alias + " ARRAY JOIN " +
					strings.Join(bindings, ", ") + " WHERE " + predicate +
					materializedCTESettingsSQL
			}
			relation = relation.selectFrom(filterSQL, operator.Range)
			args = append(args, predicateArgs...)
			state = nextState
		case *plan.RegexFilter:
			predicate, predicateArgs, compileErr := compileRegexFilter(operator, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = relation.selectFrom(
				"SELECT * FROM ("+relation.sql+") AS "+alias+" WHERE "+predicate,
				operator.Range,
			)
			args = append(args, predicateArgs...)
		case *plan.Project:
			projection, nextState, projectionArgs, compileErr := compileProjection(operator, state, alias, aliasSequence)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = relation.selectFrom(
				"SELECT "+strings.Join(projection, ", ")+" FROM ("+relation.sql+") AS "+alias,
				operator.Range,
			)
			// Projection expressions precede their nested input relation in SQL,
			// so their bind values precede every already-compiled input argument.
			args = append(projectionArgs, args...)
			state = nextState
		case *plan.Extend:
			if len(operator.Assignments) == 0 {
				return CompiledQuery{}, errors.New("compile ClickHouse extend: no assignments")
			}
			if len(operator.Assignments) > maxCompiledAssignments {
				return CompiledQuery{}, &plan.Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("eval contains more than %d assignments", maxCompiledAssignments),
					Range:   operator.Range,
				}
			}
			for index, assignment := range operator.Assignments {
				if complexityErr := validateCompiledScalarComplexity(assignment.Expression); complexityErr != nil {
					return CompiledQuery{}, complexityErr
				}
				if scalarExpressionMayReturnBooleanFunction(assignment.Expression) {
					return CompiledQuery{}, errors.New(
						"compile ClickHouse extend: eval cannot directly assign a Boolean result",
					)
				}
				value, compileErr := compileScalarValue(assignment.Expression, state)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				// A native-MV guard can be nested under arithmetic, conditionals, or
				// another scalar wrapper that changes the result kind. Keep the
				// assignment's validation fence based on the authored expression tree,
				// not only on the outer compiled scalar's traits.
				if scalarExpressionRequiresNativeMVValidation(assignment.Expression, state) {
					value.requiresRuntimeValidation = true
				}
				prefixArgs := append([]any(nil), value.valueArgs...)
				semanticAlias := ""
				multivalueStateAlias := ""
				nextSQL := ""
				if value.kind == fieldKindString && value.stringOrBytes {
					if value.semanticBytesSQL == "" {
						return CompiledQuery{}, errors.New(
							"compile ClickHouse extend: String-or-Bytes value lacks semantic Bytes provenance",
						)
					}
					semanticAlias = quoteIdentifier(fmt.Sprintf(
						"__os_string_or_bytes_%d_%d",
						aliasSequence,
						index,
					))
					nextSQL = upsertFieldProjectionWithPrivateSQL(
						relation.sql,
						state,
						assignment.Output.Name,
						value.valueSQL,
						"toUInt8(ifNull("+value.semanticBytesSQL+", 0)) AS "+semanticAlias,
						alias,
					)
					prefixArgs = append(prefixArgs, value.semanticBytesArgs...)
					value.semanticBytesSQL = semanticAlias
					value.semanticBytesArgs = nil
				} else if value.optionalMultivaluePresentSQL != "" {
					// Native eval functions can produce a present-empty list, so
					// presence cannot be reconstructed from notEmpty(output). Seal
					// the authored-input predicate beside the calculated array in the
					// same projection, before an assignment that overwrites its input
					// field could make the predicate self-referential.
					multivalueStateAlias = quoteIdentifier(fmt.Sprintf(
						"__os_eval_mv_state_%d_%d",
						aliasSequence,
						index,
					))
					nextSQL = upsertFieldProjectionWithPrivateSQL(
						relation.sql,
						state,
						assignment.Output.Name,
						value.valueSQL,
						"tuple(toUInt8(ifNull("+value.existsSQL+", 0)), "+
							"toUInt8(ifNull("+value.optionalMultivaluePresentSQL+", 0))) AS "+multivalueStateAlias,
						alias,
					)
					prefixArgs = append(prefixArgs, value.existsArgs...)
					prefixArgs = append(prefixArgs, value.existsArgs...)
					value.existsSQL = "tupleElement(" + multivalueStateAlias + ", 1) != 0"
					value.optionalMultivaluePresentSQL = "tupleElement(" + multivalueStateAlias + ", 2) != 0"
					value.existsArgs = nil
				} else {
					nextSQL = upsertFieldProjectionSQL(
						relation.sql,
						state,
						assignment.Output.Name,
						value.valueSQL,
						alias,
					)
				}
				if value.requiresRuntimeValidation {
					validationInput := quoteIdentifier(fmt.Sprintf(
						"__os_eval_mv_validation_%d_%d",
						aliasSequence,
						index,
					))
					validationAlias := quoteIdentifier(fmt.Sprintf(
						"_stage_%d_eval_mv_validation_%d",
						aliasSequence,
						index,
					))
					nextSQL = "WITH " + validationInput + " AS MATERIALIZED (" +
						nextSQL + ") SELECT * FROM " + validationInput + " AS " +
						validationAlias + " WHERE ignore(" +
						quoteIdentifier(assignment.Output.Name) + ") = 0"
					if state.context != nil {
						state.context.atomicResult = true
						state.context.requiresMaterializedValidationSettings = true
					}
				}
				relation = relation.selectFrom(nextSQL, operator.Range)
				if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
					return CompiledQuery{}, err
				}
				// Extend is emitted in an outer SELECT, so its placeholders occur
				// before every placeholder already present in the nested fragment.
				// Sequential assignments add another outer SELECT and therefore
				// prepend in reverse nesting order as well.
				args = prependArguments(prefixArgs, args)
				_, directField := assignment.Expression.(*plan.ScalarFieldExpression)
				extendState := state
				if multivalueStateAlias != "" {
					extendState.privateColumns = append(
						append([]string(nil), state.privateColumns...),
						multivalueStateAlias,
					)
				}
				nextState, stateErr := extendCompileState(
					extendState,
					assignment.Output,
					value,
					directField,
				)
				if stateErr != nil {
					return CompiledQuery{}, stateErr
				}
				state = nextState
				if index+1 < len(operator.Assignments) {
					aliasSequence++
					alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				}
			}
		case *plan.Strcat:
			enriched, nextState, prefixArgs, compileErr := compileStrcat(
				relation,
				operator,
				state,
				alias,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.FillNull:
			enriched, nextState, prefixArgs, compileErr := compileFillNull(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.RowTotal:
			enriched, nextState, prefixArgs, barrier, compileErr := compileRowTotal(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			nextState, args = bindChronologicalBarrier(nextState, barrier, args)
			state = nextState
		case *plan.OrderedDelta:
			enriched, nextState, prefixArgs, barrier, compileErr := compileOrderedDelta(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			nextState, args = bindChronologicalBarrier(nextState, barrier, args)
			state = nextState
		case *plan.MakeMultivalue:
			enriched, nextState, prefixArgs, compileErr := compileMakeMultivalue(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.ExpandMultivalue:
			enriched, nextState, prefixArgs, compileErr := compileExpandMultivalue(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.NoMultivalue:
			presented, nextState, prefixArgs, compileErr := compileNoMultivalue(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = presented
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.TimeBucket:
			bucketed, nextState, prefixArgs, compileErr := compileTimeBucket(
				relation,
				state,
				scan,
				operator,
				alias,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = bucketed
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.NumericBucket:
			bucketed, nextState, prefixArgs, compileErr := compileNumericBucket(
				relation,
				state,
				operator,
				alias,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = bucketed
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.Extract:
			extracted, nextState, prefixArgs, additionalAliases, compileErr := compileExtract(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = extracted
			args = prependArguments(prefixArgs, args)
			state = nextState
			aliasSequence += additionalAliases
		case *plan.ExtractJSON:
			extracted, nextState, prefixArgs, additionalAliases, compileErr := compileExtractJSON(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = extracted
			args = prependArguments(prefixArgs, args)
			state = nextState
			aliasSequence += additionalAliases
		case *plan.Rename:
			if len(operator.Assignments) == 0 {
				return CompiledQuery{}, errors.New("compile ClickHouse rename: no assignments")
			}
			if len(operator.Assignments) > maxCompiledAssignments {
				return CompiledQuery{}, &plan.Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("rename contains more than %d assignments", maxCompiledAssignments),
					Range:   operator.Range,
				}
			}
			seenSources := make(map[string]struct{}, len(operator.Assignments))
			seenDestinations := make(map[string]struct{}, len(operator.Assignments))
			for index, assignment := range operator.Assignments {
				if assignment.Source.Name == assignment.Destination.Name {
					return CompiledQuery{}, errors.New("compile ClickHouse rename: source and destination must differ")
				}
				if _, duplicate := seenSources[assignment.Source.Name]; duplicate {
					return CompiledQuery{}, errors.New("compile ClickHouse rename: source field is repeated")
				}
				if _, duplicate := seenDestinations[assignment.Destination.Name]; duplicate {
					return CompiledQuery{}, errors.New("compile ClickHouse rename: destination field is repeated")
				}
				seenSources[assignment.Source.Name] = struct{}{}
				seenDestinations[assignment.Destination.Name] = struct{}{}
				if index > 0 {
					aliasSequence++
					alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				}
				projection, nextState, changed, compileErr := compileRenameAssignment(assignment, state)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				state = nextState
				if changed {
					relation = relation.selectFrom(
						"SELECT "+strings.Join(projection, ", ")+" FROM ("+relation.sql+") AS "+alias,
						operator.Range,
					)
					if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
						return CompiledQuery{}, err
					}
				}
			}
		case *plan.Lookup:
			if lookupStageIndex >= len(lookupPreparation.stages) ||
				lookupPreparation.stages[lookupStageIndex].sourceOperator != operator ||
				!lookupResolutionContractsEqual(
					*lookupPreparation.stages[lookupStageIndex].operator,
					*operator,
				) {
				return CompiledQuery{}, errors.New(
					"compile ClickHouse lookup: prepared stage order disagrees with the logical plan",
				)
			}
			var additionalAliases int
			relation, state, args, additionalAliases, err = compileLookupStage(
				relation,
				state,
				args,
				lookupPreparation.stages[lookupStageIndex],
				aliasSequence,
			)
			if err != nil {
				return CompiledQuery{}, err
			}
			lookupStageIndex++
			aliasSequence += additionalAliases
		case *plan.Aggregate:
			if cardinalityErr := validateAggregateCardinality(operator); cardinalityErr != nil {
				return CompiledQuery{}, cardinalityErr
			}
			if validateErr := validateAggregatePredicateMeasures(operator, state); validateErr != nil {
				return CompiledQuery{}, validateErr
			}
			// Aggregate is also the internal plan node used by top and rare.
			// Parser-built stats always carries effective StatsOptions, so that
			// pointer is the trust-boundary discriminator for the command-scoped
			// execution hint. Nil legacy/internal aggregates must not serialize
			// unrelated query stages.
			if operator.StatsOptions != nil {
				stageThreadHint, hintErr := effectiveStatsPartitionsMaxThreadsHint(operator)
				if hintErr != nil {
					return CompiledQuery{}, hintErr
				}
				statsPartitionsMaxThreadsHint = mergeStatsPartitionsMaxThreadsHint(
					statsPartitionsMaxThreadsHint,
					stageThreadHint,
				)
			}
			materializedFields := aggregatePredicateMaterializationFields(operator, state)
			if len(materializedFields) > 0 {
				var bindings, boundColumns []string
				state, bindings, boundColumns = bindAggregatePredicateFields(
					state,
					materializedFields,
					aliasSequence,
				)
				materialized := quoteIdentifier(fmt.Sprintf(
					"__os_stats_predicate_input_%d",
					aliasSequence,
				))
				relation = relation.selectFrom(
					"WITH "+materialized+" AS MATERIALIZED ("+relation.sql+") "+
						"SELECT *, "+strings.Join(boundColumns, ", ")+" FROM "+
						materialized+" AS "+alias+" ARRAY JOIN "+
						strings.Join(bindings, ", ")+materializedCTESettingsSQL,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
			}
			projection, predicates, groups, nextState, aggregateArgs, compileErr := compileAggregateValidated(
				operator,
				state,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			if len(nextState.preAggregateValidationColumns) > 0 {
				// Materialize whole-input validation windows before filtering incomplete
				// group tuples. Otherwise a missing sibling key could hide an
				// unsupported container value.
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(nextState.preAggregateValidationColumns, ", ")+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				args = prependArguments(nextState.preAggregateValidationArgs, args)

				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateValidationColumns = nil
				nextState.preAggregateValidationArgs = nil
			}
			if len(predicates) > 0 {
				// Keep validation and missing/null elimination in a distinct
				// pre-aggregation scope after whole-input flags are materialized.
				relation = relation.selectFrom(
					"SELECT * FROM ("+relation.sql+") AS "+alias+" WHERE "+strings.Join(predicates, " AND "),
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
			}
			if len(nextState.preAggregateColumns) > 0 {
				// Materialize grouping keys and numeric measure inputs only after
				// sparse group tuples have been discarded.
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(nextState.preAggregateColumns, ", ")+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				args = prependArguments(nextState.preAggregateArgs, args)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateColumns = nil
				nextState.preAggregateArgs = nil
			}
			if len(nextState.preAggregateGroupExpansions) > 0 {
				if nextState.context == nil {
					return CompiledQuery{}, errors.New(
						"compile ClickHouse aggregate: multivalue validation context is missing",
					)
				}
				// The Cartesian guard is runtime data-dependent for both raw Dynamic
				// and fixed Array(String) inputs. A later backend row can therefore
				// select the expansion marker after earlier groups were produced.
				nextState.context.atomicResult = true
				expansionProduct, guardErr := statsMultivalueByExpansionProductSQL(
					nextState.preAggregateGroupExpansions,
				)
				if guardErr != nil {
					return CompiledQuery{}, guardErr
				}
				productAlias := quoteIdentifier("__os_stats_mv_by_combinations")
				anyOverLimitAlias := quoteIdentifier("__os_stats_mv_by_any_over_limit")
				maximum := statsMultivalueByExpansionMaximumSQL()
				// Freeze both the row-local product and a whole-eligible-input
				// violation bit before the first BY ARRAY JOIN. The window flag
				// makes one violating source event poison every retained row, so
				// downstream LIMIT or optimizer consumption cannot hide it.
				relation = relation.selectFrom(
					"SELECT *, "+expansionProduct+" AS "+productAlias+", "+
						"max(toUInt8(("+expansionProduct+") > "+maximum+")) OVER () AS "+
						anyOverLimitAlias+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				expansionGuard, guardErr := statsMultivalueByExpansionGuardSQL(
					productAlias,
					anyOverLimitAlias,
				)
				if guardErr != nil {
					return CompiledQuery{}, guardErr
				}
				relation = relation.selectFrom(
					"SELECT * EXCEPT ("+productAlias+", "+anyOverLimitAlias+") FROM ("+
						relation.sql+") AS "+alias+" WHERE "+expansionGuard,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				// Expand one BY field per relational stage. ClickHouse's comma form
				// zips arrays by position; staged ARRAY JOINs instead produce the SPL
				// Cartesian product for multiple multivalue grouping fields.
				for _, expansion := range nextState.preAggregateGroupExpansions {
					relation = relation.selectFrom(
						"SELECT *, "+expansion.valueAlias+" FROM ("+relation.sql+") AS "+alias+" ARRAY JOIN "+
							expansion.valuesAlias+" AS "+expansion.valueAlias,
						operator.Range,
					)
					aliasSequence++
					alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				}
				nextState.preAggregateGroupExpansions = nil
			}
			if len(nextState.preAggregateSparklineWindows) > 0 {
				// Sparkline bins partition the already-expanded stats BY domain. Keep
				// their window state in a separate relation before the ordinary outer
				// aggregate so scalar measures and time series can coexist in one row.
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(nextState.preAggregateSparklineWindows, ", ")+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateSparklineWindows = nil
			}
			if len(nextState.preAggregateListWindowColumns) > 0 {
				// Bound list() input bytes before aggregation. Per-input prefix
				// windows establish each value's position and cumulative payload
				// within its BY group without expanding event rows.
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(nextState.preAggregateListWindowColumns, ", ")+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateListWindowColumns = nil
			}
			if len(nextState.preAggregateListCandidateColumns) > 0 {
				// Freeze the already-bounded candidate arrays and their tiny
				// overflow flags before the aggregate. This prevents repeated
				// evaluation and keeps every partial list state byte-bounded.
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(nextState.preAggregateListCandidateColumns, ", ")+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateListCandidateColumns = nil
			}
			aggregateSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql + ") AS " + alias
			if len(groups) > 0 {
				aggregateSQL += " GROUP BY " + strings.Join(groups, ", ")
			}
			relation = relation.selectFrom(aggregateSQL, operator.Range)
			args = append(args, aggregateArgs...)
			if len(nextState.postAggregateSparklines) > 0 {
				var additionalAliases int
				relation, additionalAliases, compileErr = compileStatsSparklineResults(
					relation,
					nextState.postAggregateSparklines,
					operator.Range,
					aliasSequence,
				)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				aliasSequence += additionalAliases
				nextState.postAggregateSparklines = nil
			}
			if len(nextState.postAggregateChronological) > 0 {
				var additionalAliases int
				var barrier *pendingChronologicalBarrier
				relation, additionalAliases, barrier = compileChronologicalResults(
					relation,
					nextState.postAggregateChronological,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				nextState, args = bindChronologicalBarrier(nextState, barrier, args)
				nextState.postAggregateChronological = nil
			}
			if len(nextState.postAggregateScalarExtrema) > 0 {
				var additionalAliases int
				relation, additionalAliases = compileScalarExtremaResults(
					relation,
					nextState.postAggregateScalarExtrema,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				nextState.postAggregateScalarExtrema = nil
			}
			publishedValues := make([]string, 0, len(nextState.postAggregateExactStrings))
			for _, measure := range nextState.postAggregateExactStrings {
				if measure.function == plan.AggregateFunctionValues {
					publishedValues = append(publishedValues, measure.outputColumn)
				}
			}
			if len(nextState.postAggregateExactStrings) > 0 {
				var additionalAliases int
				relation, additionalAliases = compileBoundedExactStringResults(
					relation,
					nextState.postAggregateExactStrings,
					nextState.postAggregateDistinctCounts,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				nextState.postAggregateExactStrings = nil
				nextState.postAggregateDistinctCounts = nil
			} else if len(nextState.postAggregateDistinctCounts) > 0 {
				var additionalAliases int
				relation, additionalAliases = compileBoundedDistinctCountResults(
					relation,
					nextState.postAggregateDistinctCounts,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				nextState.postAggregateDistinctCounts = nil
			}
			if len(nextState.postAggregateOrderedStrings) > 0 {
				var additionalAliases int
				relation, additionalAliases = compileBoundedOrderedStringResults(
					relation,
					nextState.postAggregateOrderedStrings,
					publishedValues,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				nextState.postAggregateOrderedStrings = nil
			}
			state = nextState
		case *plan.EventAggregate:
			if operatorIndex+1 < len(remainingOperators) {
				if adjacent, ok := remainingOperators[operatorIndex+1].(*plan.EventAggregate); ok &&
					canFuseChronologicalEventAggregates(operator, adjacent, state) {
					enriched, nextState, prefixArgs, barrier, compileErr :=
						compileFusedChronologicalEventAggregates(
							relation,
							operator,
							adjacent,
							state,
							aliasSequence,
							aliasSequence+1,
						)
					if compileErr != nil {
						return CompiledQuery{}, compileErr
					}
					relation = enriched
					args = prependArguments(prefixArgs, args)
					nextState, args = bindChronologicalBarrier(nextState, barrier, args)
					state = nextState
					operatorIndex++
					aliasSequence++
					break
				}
			}
			enriched, nextState, prefixArgs, barrier, compileErr := compileEventAggregate(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			if barrier != nil && barrier.prefixArgumentsAfterExisting {
				args = append(args, prefixArgs...)
			} else {
				args = prependArguments(prefixArgs, args)
			}
			nextState, args = bindChronologicalBarrier(nextState, barrier, args)
			state = nextState
		case *plan.StreamAggregate:
			if operatorIndex+1 < len(remainingOperators) {
				if adjacent, ok := remainingOperators[operatorIndex+1].(*plan.StreamAggregate); ok &&
					canFuseChronologicalStreamAggregates(operator, adjacent, state) {
					enriched, nextState, prefixArgs, barrier, compileErr :=
						compileFusedChronologicalStreamAggregates(
							relation,
							operator,
							adjacent,
							state,
							aliasSequence,
							aliasSequence+1,
						)
					if compileErr != nil {
						return CompiledQuery{}, compileErr
					}
					relation = enriched
					args = prependArguments(prefixArgs, args)
					nextState, args = bindChronologicalBarrier(nextState, barrier, args)
					state = nextState
					operatorIndex++
					aliasSequence++
					break
				}
			}
			enriched, nextState, prefixArgs, barrier, compileErr := compileStreamAggregate(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			nextState, args = bindChronologicalBarrier(nextState, barrier, args)
			state = nextState
		case *plan.Timechart:
			if !permitTerminalWideOperators {
				return CompiledQuery{}, errors.New("compile ClickHouse query: timechart is unavailable for event analysis")
			}
			if operatorIndex+1 != len(remainingOperators) {
				return CompiledQuery{}, errors.New("compile ClickHouse timechart: operator must be terminal")
			}
			compiled, compileErr := compileTimechart(
				relation,
				state,
				args,
				operator,
				query.OutputFields,
				query.DynamicOutput,
				scan,
				alias,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			return finishCompiled(compiled, operator.Range)
		case *plan.Chart:
			if !permitTerminalWideOperators {
				return CompiledQuery{}, errors.New("compile ClickHouse query: chart is unavailable for event analysis")
			}
			if operatorIndex+1 != len(remainingOperators) {
				return CompiledQuery{}, errors.New("compile ClickHouse chart: operator must be terminal")
			}
			compiled, compileErr := compileChart(relation, state, args, operator, query.DynamicOutput, alias)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			return finishCompiled(compiled, operator.Range)
		case *plan.Window:
			expression, nextState, compileErr := compileWindow(operator, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = relation.selectFrom(
				"SELECT *, "+expression+" AS "+quoteIdentifier(operator.Output)+" FROM ("+relation.sql+") AS "+alias,
				operator.Range,
			)
			state = nextState
		case *plan.Sort:
			if operatorIndex > 0 {
				previous, ok := remainingOperators[operatorIndex-1].(*plan.Sort)
				if ok && equivalentSortOperators(previous, operator) {
					// Reuse the preceding command's durable comparator instead of
					// expanding its exact Auto expression again. Retain the authored
					// ORDER BY/LIMIT boundary: besides preserving source-range and
					// relational-depth accounting, that boundary is observable to
					// commands which follow this run of identical sorts.
					order, orderErr := compileMaterializedOrder(state.order, false)
					if orderErr != nil {
						return CompiledQuery{}, orderErr
					}
					sortSQL := "SELECT * FROM (" + relation.sql + ") AS " + alias +
						" ORDER BY " + order
					if operator.Limit > 0 {
						sortSQL += " LIMIT ?"
						args = append(args, operator.Limit)
					}
					relation = relation.selectFrom(sortSQL, operator.Range)
					break
				}
			}
			materialized, sortKeys, order, prefixArgs, compileErr := compileSort(operator.Keys, state, aliasSequence)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			sortSQL := "SELECT *, " + strings.Join(materialized, ", ") + " FROM (" + relation.sql + ") AS " + alias + " ORDER BY " + order
			args = prependArguments(prefixArgs, args)
			if operator.Limit > 0 {
				sortSQL += " LIMIT ?"
				args = append(args, operator.Limit)
			}
			relation = relation.selectFrom(sortSQL, operator.Range)
			state.order = sortKeys
		case *plan.Deduplicate:
			deduplicated, prefixArgs, currentOrder, additionalAliases, compileErr := compileDeduplicate(relation, operator, state, aliasSequence)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = deduplicated
			args = prependArguments(prefixArgs, args)
			args = append(args, operator.Count)
			state.order = currentOrder
			aliasSequence += additionalAliases
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
				relation = relation.selectFrom(
					"SELECT * FROM ("+relation.sql+") AS "+alias+" ORDER BY "+reversed+" LIMIT ?",
					operator.Range,
				)
				args = append(args, operator.Count)
				state.order = reverseCompiledSortKeys(keys)
			} else {
				order, compileErr := compileMaterializedOrder(keys, false)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				relation = relation.selectFrom(
					"SELECT * FROM ("+relation.sql+") AS "+alias+" ORDER BY "+order+" LIMIT ?",
					operator.Range,
				)
				args = append(args, operator.Count)
				state.order = append([]compiledSortKey(nil), keys...)
			}
		case *plan.Reverse:
			reversed, nextState, compileErr := compileReverse(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = reversed
			state = nextState
		default:
			return CompiledQuery{}, fmt.Errorf("compile ClickHouse query: unsupported logical operator %T", operator)
		}
		if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
			return CompiledQuery{}, err
		}
	}
	if lookupStageIndex != len(lookupPreparation.stages) {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse lookup: not every prepared stage was lowered",
		)
	}

	compiled, err := finalize(relation, state, args, scan, aliasSequence)
	if err != nil {
		return CompiledQuery{}, err
	}
	return finishCompiled(compiled, scan.Range)
}

type authoredKnowledgeCompilation struct {
	regexPrograms      uint32
	regexWorkUnits     uint64
	extractionOutputs  uint32
	jsonEvaluationWork uint32
}
