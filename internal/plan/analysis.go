package plan

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	maximumAnalysisNodes          = 4_096
	maximumKnowledgeAnalysisNodes = 36_000
	maximumAnalysisDepth          = 1_024
)

// Analysis is bounded metadata derived from a fully accepted logical plan.
// ReferencedFields contains sorted, unique logical row fields read by the
// pipeline. Write-only outputs are omitted unless a later operator reads them.
type Analysis struct {
	ReferencedFields []string
}

// StagedAnalysis contains the whole-query read-set and one aligned read-set
// for every operator in the validated pipeline.
type StagedAnalysis struct {
	ReferencedFields []string
	Stages           []Analysis
}

// AnalyzeStages derives one detached read-set per operator after first proving
// that the complete query is valid. Whole-query validation is essential for
// knowledge operators: an isolated generated operator has no private prelude
// marker and must never be accepted as independent authority.
func AnalyzeStages(query *Query) (StagedAnalysis, error) {
	full, err := analyze(query)
	if err != nil {
		return StagedAnalysis{}, err
	}

	stages := make([]Analysis, len(query.Operators))
	for index, operator := range query.Operators {
		analyzer := queryAnalyzer{fields: make(map[string]struct{})}
		if err := analyzer.visitOperator(operator, 1); err != nil {
			return StagedAnalysis{}, fmt.Errorf(
				"analyze logical query stage %d: %w",
				index,
				err,
			)
		}
		fields := make([]string, 0, len(analyzer.fields))
		for field := range analyzer.fields {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		stages[index] = Analysis{ReferencedFields: fields}
	}
	return StagedAnalysis{
		ReferencedFields: full.referencedFields,
		Stages:           stages,
	}, nil
}

// Analyze derives safe public metadata from query. It fails closed for typed
// nils, unknown future nodes, malformed field references, excessive depth, and
// excessive work so forged plans cannot yield incomplete dependency metadata.
func Analyze(query *Query) (Analysis, error) {
	analysis, err := analyze(query)
	if err != nil {
		return Analysis{}, err
	}
	return Analysis{ReferencedFields: analysis.referencedFields}, nil
}

type internalAnalysis struct {
	referencedFields []string
	scalarPredicates uint32
}

func analyze(query *Query) (internalAnalysis, error) {
	if query == nil {
		return internalAnalysis{}, errors.New("analyze logical query: query is nil")
	}
	if err := ValidateKnowledgePreludeIntegrity(query); err != nil {
		return internalAnalysis{}, fmt.Errorf("analyze logical query: %w", err)
	}
	if err := ValidateAutomaticLookupIntegrity(query); err != nil {
		return internalAnalysis{}, fmt.Errorf("analyze logical query: %w", err)
	}
	analyzer := queryAnalyzer{
		fields: make(map[string]struct{}),
	}
	for _, operator := range query.Operators {
		if err := analyzer.visitOperator(operator, 1); err != nil {
			return internalAnalysis{}, err
		}
	}
	if analyzer.calendarSpan {
		if _, err := ianatimezone.Load(query.SearchTimezone); err != nil {
			return internalAnalysis{}, errors.New(
				"analyze logical query: calendar spans require a valid search timezone",
			)
		}
	}
	fields := make([]string, 0, len(analyzer.fields))
	for field := range analyzer.fields {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return internalAnalysis{
		referencedFields: fields,
		scalarPredicates: analyzer.scalarPredicates,
	}, nil
}

type queryAnalyzer struct {
	fields                map[string]struct{}
	nodes                 int
	knowledgeNodes        int
	scalarPredicates      uint32
	arithmeticOperators   int
	membershipCandidates  int
	calendarSpan          bool
	activeExpressionNodes map[any]struct{}
}

func (analyzer *queryAnalyzer) enter(depth int) error {
	if depth > maximumAnalysisDepth {
		return fmt.Errorf("analyze logical query: tree exceeds depth %d", maximumAnalysisDepth)
	}
	analyzer.nodes++
	if analyzer.nodes > maximumAnalysisNodes {
		return fmt.Errorf("analyze logical query: tree exceeds %d nodes", maximumAnalysisNodes)
	}
	return nil
}

// enterKnowledge keeps the parser-authored analysis ceiling unchanged while
// admitting the separately bounded flattened knowledge inventory. The limit
// covers 32,768 scalar AST occurrences plus generated fields, object inputs,
// selector dimensions, and explicit prelude operators at their shared maxima.
func (analyzer *queryAnalyzer) enterKnowledge(depth int) error {
	if depth > maximumAnalysisDepth {
		return fmt.Errorf("analyze logical query: tree exceeds depth %d", maximumAnalysisDepth)
	}
	analyzer.knowledgeNodes++
	if analyzer.knowledgeNodes > maximumKnowledgeAnalysisNodes {
		return fmt.Errorf(
			"analyze logical query: knowledge prelude exceeds %d nodes",
			maximumKnowledgeAnalysisNodes,
		)
	}
	return nil
}

func (analyzer *queryAnalyzer) enterExpressionNode(node any, depth int) error {
	if node == nil {
		return errors.New("analyze logical query: expression is nil")
	}
	if !reflect.TypeOf(node).Comparable() {
		return fmt.Errorf("analyze logical query: unsupported expression %T", node)
	}
	if err := analyzer.enter(depth); err != nil {
		return err
	}
	if analyzer.activeExpressionNodes == nil {
		analyzer.activeExpressionNodes = make(map[any]struct{})
	}
	if _, cyclic := analyzer.activeExpressionNodes[node]; cyclic {
		return errors.New("analyze logical query: expression graph contains a cycle")
	}
	analyzer.activeExpressionNodes[node] = struct{}{}
	return nil
}

func (analyzer *queryAnalyzer) leaveExpressionNode(node any) {
	delete(analyzer.activeExpressionNodes, node)
}

func (analyzer *queryAnalyzer) chargeArithmeticOperator() error {
	if analyzer.arithmeticOperators >= spl.MaximumArithmeticOperatorsPerQuery {
		return fmt.Errorf(
			"analyze logical query: arithmetic exceeds %d operators per query",
			spl.MaximumArithmeticOperatorsPerQuery,
		)
	}
	analyzer.arithmeticOperators++
	return nil
}

func (analyzer *queryAnalyzer) chargeMembershipCandidates(count int) error {
	if count < 1 || count > spl.MaximumMembershipCandidates {
		return fmt.Errorf(
			"analyze logical query: membership candidate count must be from 1 through %d",
			spl.MaximumMembershipCandidates,
		)
	}
	if analyzer.membershipCandidates >
		spl.MaximumMembershipCandidatesPerQuery-count {
		return fmt.Errorf(
			"analyze logical query: membership exceeds %d candidates per query",
			spl.MaximumMembershipCandidatesPerQuery,
		)
	}
	analyzer.membershipCandidates += count
	return nil
}

func (analyzer *queryAnalyzer) addField(field FieldRef, depth int) error {
	if err := analyzer.validateField(field, depth); err != nil {
		return err
	}
	analyzer.fields[field.Name] = struct{}{}
	return nil
}

func (analyzer *queryAnalyzer) validateField(field FieldRef, depth int) error {
	if err := analyzer.enter(depth); err != nil {
		return err
	}
	if field.Name == "" {
		return errors.New("analyze logical query: field name is empty")
	}
	return nil
}

func (analyzer *queryAnalyzer) validateOutputName(name string, depth int) error {
	if err := analyzer.enter(depth); err != nil {
		return err
	}
	if name == "" {
		return errors.New("analyze logical query: output field name is empty")
	}
	return nil
}

func (analyzer *queryAnalyzer) addFields(fields []FieldRef, depth int) error {
	for _, field := range fields {
		if err := analyzer.addField(field, depth); err != nil {
			return err
		}
	}
	return nil
}

func (analyzer *queryAnalyzer) addChronologicalTimeDependency(
	function AggregateFunction,
	source FieldRef,
	depth int,
) error {
	switch function {
	case AggregateFunctionEarliest,
		AggregateFunctionLatest,
		AggregateFunctionEarliestTime,
		AggregateFunctionLatestTime,
		AggregateFunctionRate:
		// These functions select or derive values using event chronology even
		// when their aggregate input is a calculated scalar expression.
	default:
		return nil
	}
	return analyzer.addField(FieldRef{
		Name:      "_time",
		Canonical: true,
		Range:     source.Range,
	}, depth)
}

func emptyAggregateField(field FieldRef) bool {
	return field.Name == "" && !field.Canonical && field.Path == nil &&
		field.Range == (spl.Range{})
}

func validSparklineSpan(span SparklineSpan) bool {
	switch span.Kind {
	case SparklineSpanKindAutomatic:
		return span.Magnitude == 0 && span.Unit == SparklineSpanUnitInvalid
	case SparklineSpanKindExplicit:
		if span.Magnitude == 0 {
			return false
		}
		var unitsPerSecond uint64
		switch span.Unit {
		case SparklineSpanUnitMicrosecond:
			unitsPerSecond = 1_000_000
		case SparklineSpanUnitMillisecond:
			unitsPerSecond = 1_000
		case SparklineSpanUnitCentisecond:
			unitsPerSecond = 100
		case SparklineSpanUnitDecisecond:
			unitsPerSecond = 10
		case SparklineSpanUnitSecond,
			SparklineSpanUnitMinute,
			SparklineSpanUnitHour,
			SparklineSpanUnitDay,
			SparklineSpanUnitMonth:
			// These are valid positive integral spans. Month remains a
			// calendar unit and is deliberately not converted to a duration.
		default:
			return false
		}
		return unitsPerSecond == 0 ||
			(span.Magnitude < unitsPerSecond &&
				unitsPerSecond%span.Magnitude == 0)
	default:
		return false
	}
}

func sparklineFunctionRequiresInput(function AggregateFunction) (bool, bool) {
	switch function {
	case AggregateFunctionCountRows:
		return false, true
	case AggregateFunctionCountValues,
		AggregateFunctionDistinctCount,
		AggregateFunctionAverage,
		AggregateFunctionStandardDeviationSample,
		AggregateFunctionStandardDeviationPopulation,
		AggregateFunctionVarianceSample,
		AggregateFunctionVariancePopulation,
		AggregateFunctionSum,
		AggregateFunctionSumSquares,
		AggregateFunctionMinimum,
		AggregateFunctionMaximum,
		AggregateFunctionRange:
		return true, true
	default:
		return false, false
	}
}

func (analyzer *queryAnalyzer) visitSparklineMeasure(
	measure *SparklineMeasure,
	depth int,
) error {
	if measure == nil ||
		measure.MaximumPoints != spl.MaximumStatsSparklinePoints ||
		!validSparklineSpan(measure.Span) ||
		!validResolvedEventAggregateField(measure.Time) ||
		measure.Time.Name != "_time" || !measure.Time.Canonical ||
		measure.Time.Path != nil {
		return errors.New("analyze logical query: sparkline metadata is invalid")
	}
	requiresInput, supported := sparklineFunctionRequiresInput(measure.Function)
	if !supported {
		return errors.New("analyze logical query: sparkline function is invalid")
	}
	hasInput := !emptyAggregateField(measure.Input)
	if hasInput != requiresInput ||
		(hasInput && !validResolvedEventAggregateField(measure.Input)) {
		return errors.New("analyze logical query: sparkline input metadata is invalid")
	}
	if err := analyzer.addField(measure.Time, depth); err != nil {
		return err
	}
	if hasInput {
		return analyzer.addField(measure.Input, depth)
	}
	return nil
}

func (analyzer *queryAnalyzer) visitAggregateMeasure(
	measure AggregateMeasure,
	depth int,
) error {
	if err := analyzer.validateOutputName(measure.Output, depth); err != nil {
		return err
	}
	if measure.OutputLiteral {
		if !spl.IsStatsLiteralOutputName(measure.Output) {
			return errors.New("analyze logical query: aggregate literal output is invalid")
		}
	} else if _, err := ResolveField(measure.Output, spl.Range{}); err != nil {
		return fmt.Errorf("analyze logical query: aggregate output is invalid: %w", err)
	}
	if measure.Sparkline != nil {
		if measure.Function != AggregateFunctionInvalid ||
			!emptyAggregateField(measure.Input) || measure.InputExpression != nil ||
			measure.Predicate != nil || measure.Percentile != 0 {
			return errors.New("analyze logical query: sparkline and scalar aggregate metadata overlap")
		}
		return analyzer.visitSparklineMeasure(measure.Sparkline, depth)
	}
	hasInput := !emptyAggregateField(measure.Input)
	hasInputExpression := measure.InputExpression != nil
	hasPredicate := measure.Predicate != nil
	switch measure.Function {
	case AggregateFunctionCountRows:
		if hasInput || hasInputExpression || hasPredicate || measure.Percentile != 0 {
			return errors.New("analyze logical query: row count metadata is invalid")
		}
	case AggregateFunctionCountPredicate:
		if hasInput || hasInputExpression || !hasPredicate || measure.Percentile != 0 ||
			!validEventAggregatePredicate(measure.Predicate) {
			return errors.New("analyze logical query: conditional count metadata is invalid")
		}
	case AggregateFunctionPercentile,
		AggregateFunctionExactPercentile,
		AggregateFunctionUpperPercentile:
		if hasInput == hasInputExpression || hasPredicate ||
			measure.Percentile < 1 || measure.Percentile > 99 {
			return errors.New("analyze logical query: percentile metadata is invalid")
		}
	case AggregateFunctionMedian,
		AggregateFunctionSum, AggregateFunctionAverage,
		AggregateFunctionRange, AggregateFunctionSumSquares,
		AggregateFunctionStandardDeviationSample,
		AggregateFunctionStandardDeviationPopulation,
		AggregateFunctionVarianceSample,
		AggregateFunctionVariancePopulation,
		AggregateFunctionDistinctCount,
		AggregateFunctionEstimatedDistinctCount,
		AggregateFunctionEstimatedDistinctCountError,
		AggregateFunctionValues, AggregateFunctionList,
		AggregateFunctionMinimum, AggregateFunctionMaximum,
		AggregateFunctionMode, AggregateFunctionFirst, AggregateFunctionLast,
		AggregateFunctionEarliest, AggregateFunctionLatest,
		AggregateFunctionEarliestTime, AggregateFunctionLatestTime,
		AggregateFunctionRate:
		if hasInput == hasInputExpression || hasPredicate || measure.Percentile != 0 {
			return errors.New("analyze logical query: field-taking aggregate metadata is invalid")
		}
	case AggregateFunctionCountValues:
		if !hasInput || hasInputExpression || hasPredicate || measure.Percentile != 0 {
			return errors.New("analyze logical query: field occurrence count metadata is invalid")
		}
	default:
		return errors.New("analyze logical query: aggregate function is invalid")
	}
	if hasInput {
		if !validResolvedEventAggregateField(measure.Input) {
			return errors.New("analyze logical query: aggregate input field metadata is invalid")
		}
		if err := analyzer.addField(measure.Input, depth); err != nil {
			return err
		}
	}
	if hasInputExpression {
		if err := analyzer.visitScalarExpressionStrict(
			measure.InputExpression,
			depth,
		); err != nil {
			return err
		}
	}
	if err := analyzer.addChronologicalTimeDependency(
		measure.Function,
		measure.Input,
		depth,
	); err != nil {
		return err
	}
	if hasPredicate {
		return analyzer.visitExpression(measure.Predicate, depth)
	}
	return nil
}

func (analyzer *queryAnalyzer) visitOperator(operator Operator, depth int) error {
	enter := analyzer.enter
	if isKnowledgePreludeOperator(operator) {
		enter = analyzer.enterKnowledge
	}
	if err := enter(depth); err != nil {
		return err
	}
	if nilcheck.IsNil(operator) {
		return fmt.Errorf("analyze logical query: operator %T is nil", operator)
	}
	switch operator := operator.(type) {
	case *Scan:
		return nil
	case *Filter:
		return analyzer.visitExpression(operator.Expression, depth+1)
	case *RegexFilter:
		if operator == nil {
			return errors.New("analyze logical query: regex filter is invalid")
		}
		return analyzer.addField(operator.Input, depth+1)
	case *Project:
		if len(operator.Fields)+len(operator.Patterns) == 0 {
			return errors.New("analyze logical query: project selectors are invalid")
		}
		if operator.Mode == ProjectModeTable && len(operator.Patterns) != 0 {
			return errors.New("analyze logical query: table project cannot contain wildcards")
		}
		for _, pattern := range operator.Patterns {
			if !spl.IsFieldsFieldGlob(pattern.Pattern) {
				return errors.New("analyze logical query: project wildcard is invalid")
			}
		}
		return analyzer.addFields(operator.Fields, depth+1)
	case *Extend:
		for _, assignment := range operator.Assignments {
			if err := analyzer.validateField(assignment.Output, depth+1); err != nil {
				return err
			}
			if err := analyzer.visitScalarExpression(assignment.Expression, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *Strcat:
		if operator == nil || len(operator.Operands) < 2 ||
			len(operator.Operands) > spl.MaximumConcatenationOperands {
			return errors.New("analyze logical query: strcat is invalid")
		}
		if err := analyzer.validateField(operator.Destination, depth+1); err != nil {
			return err
		}
		for _, operand := range operator.Operands {
			if err := analyzer.visitScalarExpression(operand, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *FillNull:
		if operator == nil || len(operator.Fields) < 1 ||
			len(operator.Fields) > spl.MaximumExplicitProjectionFields ||
			!utf8.ValidString(operator.Value) {
			return errors.New("analyze logical query: fillnull is invalid")
		}
		return analyzer.addFields(operator.Fields, depth+1)
	case *RowTotal:
		if operator == nil || len(operator.Inputs) < 1 ||
			len(operator.Inputs) > spl.MaximumExplicitProjectionFields {
			return errors.New("analyze logical query: row total is invalid")
		}
		if err := analyzer.validateOutputName(operator.Output, depth+1); err != nil {
			return err
		}
		for range len(operator.Inputs) - 1 {
			if err := analyzer.chargeArithmeticOperator(); err != nil {
				return err
			}
		}
		return analyzer.addFields(operator.Inputs, depth+1)
	case *OrderedDelta:
		if operator == nil || operator.Previous < 1 ||
			operator.Previous > spl.MaximumStreamStatsWindow ||
			!validOrderedDeltaOutput(operator.Input.Name, operator.Output) {
			return errors.New("analyze logical query: ordered delta is invalid")
		}
		if err := analyzer.validateOutputName(operator.Output, depth+1); err != nil {
			return err
		}
		if err := analyzer.chargeArithmeticOperator(); err != nil {
			return err
		}
		return analyzer.addField(operator.Input, depth+1)
	case *MakeMultivalue:
		if operator == nil || operator.Delimiter == "" ||
			!utf8.ValidString(operator.Delimiter) ||
			len(operator.Delimiter) > spl.MaximumMakeMVDelimiterBytes {
			return errors.New("analyze logical query: makemv is invalid")
		}
		return analyzer.addField(operator.Input, depth+1)
	case *ExpandMultivalue:
		if operator == nil || operator.Limit > spl.MaximumMVExpandLimit ||
			operator.QueryOrdinal < 1 ||
			int(operator.QueryOrdinal) > MaximumMVExpandStages {
			return errors.New("analyze logical query: mvexpand is invalid")
		}
		return analyzer.addField(operator.Input, depth+1)
	case *NoMultivalue:
		if operator == nil || !spl.IsExactUnquotedFieldName(operator.Input.Name) ||
			strings.HasPrefix(operator.Input.Name, "_") {
			return errors.New("analyze logical query: nomv is invalid")
		}
		return analyzer.addField(operator.Input, depth+1)
	case *TimeBucket:
		if !validBucketSpanContract(operator.Span, operator.Calendar) {
			return errors.New("analyze logical query: time bucket span metadata is invalid")
		}
		analyzer.calendarSpan = analyzer.calendarSpan || operator.Calendar != CalendarNone
		if err := analyzer.validateField(operator.Output, depth+1); err != nil {
			return err
		}
		return analyzer.addField(operator.Field, depth+1)
	case *NumericBucket:
		if err := analyzer.validateField(operator.Output, depth+1); err != nil {
			return err
		}
		return analyzer.addField(operator.Input, depth+1)
	case *Extract:
		for _, capture := range operator.Captures {
			if err := analyzer.validateField(capture.Output, depth+1); err != nil {
				return err
			}
		}
		return analyzer.addField(operator.Input, depth+1)
	case *ExtractJSON:
		if err := analyzer.validateField(operator.Output, depth+1); err != nil {
			return err
		}
		return analyzer.addField(operator.Input, depth+1)
	case *Lookup:
		return analyzer.visitLookup(operator, depth+1)
	case *AutomaticLookupGroup:
		return analyzer.visitAutomaticLookupGroup(operator, depth+1)
	case *ConditionalExtract:
		extraction := operator.Extraction()
		if err := analyzer.visitKnowledgeSelector(extraction.Selector(), depth+1); err != nil {
			return err
		}
		captures := extraction.Captures()
		if len(captures) == 0 {
			return errors.New("analyze logical query: knowledge regex has no captures")
		}
		for _, capture := range captures {
			if capture.Group() == 0 {
				return errors.New("analyze logical query: knowledge regex capture is invalid")
			}
			if err := analyzer.validateKnowledgeField(capture.Name(), depth+1); err != nil {
				return err
			}
		}
		return analyzer.addKnowledgeField(extraction.Input(), depth+1)
	case *ConditionalExtractJSON:
		extraction := operator.Extraction()
		if err := analyzer.visitKnowledgeSelector(extraction.Selector(), depth+1); err != nil {
			return err
		}
		if len(extraction.Steps()) == 0 {
			return errors.New("analyze logical query: knowledge JSON path is empty")
		}
		if err := analyzer.validateKnowledgeField(extraction.Output(), depth+1); err != nil {
			return err
		}
		return analyzer.addKnowledgeField(extraction.Input(), depth+1)
	case *CopyFieldAlias:
		assignments := operator.Assignments()
		if len(assignments) == 0 {
			return errors.New("analyze logical query: knowledge alias stage is empty")
		}
		for _, assignment := range assignments {
			if err := analyzer.visitKnowledgeSelector(assignment.Selector(), depth+1); err != nil {
				return err
			}
			if err := analyzer.validateKnowledgeField(assignment.Destination(), depth+1); err != nil {
				return err
			}
			if err := analyzer.addKnowledgeField(assignment.Source(), depth+1); err != nil {
				return err
			}
		}
		return nil
	case *ParallelExtend:
		assignments := operator.Assignments()
		if len(assignments) == 0 {
			return errors.New("analyze logical query: knowledge calculated stage is empty")
		}
		for _, assignment := range assignments {
			if assignment.Expression() == "" || assignment.Nodes() == 0 {
				return errors.New("analyze logical query: knowledge calculated expression is invalid")
			}
			if err := analyzer.visitKnowledgeSelector(assignment.Selector(), depth+1); err != nil {
				return err
			}
			if err := analyzer.validateKnowledgeField(assignment.Destination(), depth+1); err != nil {
				return err
			}
			for _, input := range assignment.InputFields() {
				if err := analyzer.addKnowledgeField(input, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	case *Rename:
		for _, assignment := range operator.Assignments {
			if err := analyzer.validateField(assignment.Destination, depth+1); err != nil {
				return err
			}
			if err := analyzer.addField(assignment.Source, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *EventAggregate:
		if !validEventAggregateContract(operator) {
			return errors.New(
				"analyze logical query: event aggregate is invalid",
			)
		}
		if err := analyzer.visitAggregateMeasure(operator.Measure, depth+1); err != nil {
			return err
		}
		for _, field := range operator.GroupBy {
			if err := analyzer.addField(field, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *StreamAggregate:
		if !validStreamAggregateContract(operator) {
			return errors.New(
				"analyze logical query: stream aggregate is invalid",
			)
		}
		if err := analyzer.visitAggregateMeasure(operator.Measure, depth+1); err != nil {
			return err
		}
		return analyzer.addFields(operator.GroupBy, depth+1)
	case *Aggregate:
		if len(operator.Measures) == 0 ||
			len(operator.Measures) > spl.MaximumStatsMeasures ||
			len(operator.GroupBy) > spl.MaximumStatsGroupFields {
			return errors.New("analyze logical query: aggregate resource bounds are invalid")
		}
		requiresStatsOptions := false
		for _, measure := range operator.Measures {
			requiresStatsOptions = requiresStatsOptions ||
				measure.OutputLiteral || measure.Sparkline != nil
		}
		if requiresStatsOptions && operator.StatsOptions == nil {
			return errors.New("analyze logical query: stats-only aggregate metadata requires stats options")
		}
		if operator.StatsOptions != nil &&
			(operator.StatsOptions.Partitions < 1 ||
				operator.StatsOptions.Partitions > spl.MaximumStatsPartitions ||
				!utf8.ValidString(operator.StatsOptions.Delimiter)) {
			return errors.New("analyze logical query: stats options are invalid")
		}
		seenOutputs := make(map[string]struct{}, len(operator.GroupBy)+len(operator.Measures))
		seenSources := make([]AggregateMeasure, 0, len(operator.Measures))
		for _, field := range operator.GroupBy {
			if !validResolvedEventAggregateField(field) {
				return errors.New("analyze logical query: aggregate group field metadata is invalid")
			}
			if _, duplicate := seenOutputs[field.Name]; duplicate {
				return errors.New("analyze logical query: aggregate group field is duplicated")
			}
			seenOutputs[field.Name] = struct{}{}
			if err := analyzer.addField(field, depth+1); err != nil {
				return err
			}
		}
		for _, measure := range operator.Measures {
			if _, duplicate := seenOutputs[measure.Output]; duplicate {
				return errors.New("analyze logical query: aggregate output field is duplicated")
			}
			seenOutputs[measure.Output] = struct{}{}
			if err := analyzer.visitAggregateMeasure(measure, depth+1); err != nil {
				return err
			}
			for _, source := range seenSources {
				if sameStatsAggregateSource(source, measure) {
					return errors.New("analyze logical query: aggregate source is renamed more than once")
				}
			}
			seenSources = append(seenSources, measure)
		}
		return nil
	case *Timechart:
		if !validBucketSpanContract(operator.Span, operator.Calendar) {
			return errors.New("analyze logical query: timechart span metadata is invalid")
		}
		analyzer.calendarSpan = analyzer.calendarSpan || operator.Calendar != CalendarNone
		if err := analyzer.addField(operator.Time, depth+1); err != nil {
			return err
		}
		if !validTimechartMeasureContract(operator) {
			return errors.New("analyze logical query: timechart measure is invalid")
		}
		if err := analyzer.validateOutputName(operator.Measure.Output, depth+1); err != nil {
			return err
		}
		if operator.Measure.Function != AggregateFunctionCountRows {
			if err := analyzer.addField(operator.Measure.Input, depth+1); err != nil {
				return err
			}
		}
		if operator.Split != nil {
			return analyzer.addField(operator.Split.Field, depth+1)
		}
		return nil
	case *Chart:
		if !validChartContract(operator) {
			return errors.New("analyze logical query: chart contract is invalid")
		}
		if err := analyzer.addField(operator.Over, depth+1); err != nil {
			return err
		}
		if err := analyzer.addField(operator.SplitBy, depth+1); err != nil {
			return err
		}
		if operator.Measure.Function != AggregateFunctionCountRows {
			return analyzer.addField(operator.Measure.Input, depth+1)
		}
		return nil
	case *Window:
		if err := analyzer.validateOutputName(operator.Output, depth+1); err != nil {
			return err
		}
		if err := analyzer.addFields(operator.PartitionBy, depth+1); err != nil {
			return err
		}
		return analyzer.addField(operator.Input, depth+1)
	case *Sort:
		for _, key := range operator.Keys {
			if err := analyzer.addField(key.Field, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *Deduplicate:
		return analyzer.addFields(operator.Keys, depth+1)
	case *Limit:
		return nil
	case *Reverse:
		return nil
	default:
		return fmt.Errorf("analyze logical query: unsupported operator %T", operator)
	}
}

func (analyzer *queryAnalyzer) visitExpression(expression Expression, depth int) error {
	if err := analyzer.enterExpressionNode(expression, depth); err != nil {
		return err
	}
	defer analyzer.leaveExpressionNode(expression)
	switch expression := expression.(type) {
	case *BooleanExpression:
		if expression == nil {
			return errors.New("analyze logical query: boolean expression is nil")
		}
		if err := analyzer.visitExpression(expression.Left, depth+1); err != nil {
			return err
		}
		return analyzer.visitExpression(expression.Right, depth+1)
	case *NotExpression:
		if expression == nil {
			return errors.New("analyze logical query: not expression is nil")
		}
		return analyzer.visitExpression(expression.Operand, depth+1)
	case *TextExpression:
		if expression == nil {
			return errors.New("analyze logical query: text expression is nil")
		}
		return analyzer.addField(FieldRef{Name: "_raw", Canonical: true}, depth+1)
	case *ComparisonExpression:
		if expression == nil {
			return errors.New("analyze logical query: comparison expression is nil")
		}
		return analyzer.addField(expression.Field, depth+1)
	case *EvalComparisonExpression:
		if expression == nil {
			return errors.New("analyze logical query: eval comparison expression is nil")
		}
		analyzer.scalarPredicates++
		if err := analyzer.visitScalarExpression(expression.Left, depth+1); err != nil {
			return err
		}
		return analyzer.visitScalarExpression(expression.Right, depth+1)
	case *ScalarPredicateExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar predicate expression is nil")
		}
		analyzer.scalarPredicates++
		return analyzer.visitScalarExpression(expression.Value, depth+1)
	case *MembershipExpression:
		if expression == nil {
			return errors.New("analyze logical query: membership expression is nil")
		}
		if !validExpressionRangeOrZero(expression.Range) {
			return errors.New("analyze logical query: membership expression range is invalid")
		}
		if err := analyzer.chargeMembershipCandidates(len(expression.Candidates)); err != nil {
			return err
		}
		analyzer.scalarPredicates++
		if err := analyzer.visitScalarExpressionStrict(expression.Value, depth+1); err != nil {
			return err
		}
		for _, candidate := range expression.Candidates {
			if err := analyzer.visitScalarExpressionStrict(candidate, depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("analyze logical query: unsupported expression %T", expression)
	}
}

func (analyzer *queryAnalyzer) visitScalarExpression(expression ScalarExpression, depth int) error {
	return analyzer.visitScalarExpressionWithContext(expression, depth, 0, false)
}

func (analyzer *queryAnalyzer) visitScalarExpressionStrict(
	expression ScalarExpression,
	depth int,
) error {
	return analyzer.visitScalarExpressionWithContext(expression, depth, 0, true)
}

func (analyzer *queryAnalyzer) visitScalarExpressionWithContext(
	expression ScalarExpression,
	depth int,
	unaryChain int,
	strict bool,
) error {
	if err := analyzer.enterExpressionNode(expression, depth); err != nil {
		return err
	}
	defer analyzer.leaveExpressionNode(expression)
	switch expression := expression.(type) {
	case *ScalarUnaryExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar unary expression is nil")
		}
		if !validScalarUnaryOp(expression.Op) {
			return errors.New("analyze logical query: scalar unary operator is invalid")
		}
		if !validExpressionRangeOrZero(expression.Range) {
			return errors.New("analyze logical query: scalar unary expression range is invalid")
		}
		if unaryChain >= spl.MaximumUnaryOperatorChain {
			return fmt.Errorf(
				"analyze logical query: unary operator chain exceeds %d",
				spl.MaximumUnaryOperatorChain,
			)
		}
		if err := analyzer.chargeArithmeticOperator(); err != nil {
			return err
		}
		return analyzer.visitScalarExpressionWithContext(
			expression.Operand,
			depth+1,
			unaryChain+1,
			true,
		)
	case *ScalarBinaryExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar binary expression is nil")
		}
		if !validScalarBinaryOp(expression.Op) {
			return errors.New("analyze logical query: scalar binary operator is invalid")
		}
		if !validExpressionRangeOrZero(expression.Range) {
			return errors.New("analyze logical query: scalar binary expression range is invalid")
		}
		if err := analyzer.chargeArithmeticOperator(); err != nil {
			return err
		}
		if err := analyzer.visitScalarExpressionStrict(expression.Left, depth+1); err != nil {
			return err
		}
		return analyzer.visitScalarExpressionStrict(expression.Right, depth+1)
	case *ScalarFieldExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar field expression is nil")
		}
		if strict && !validResolvedEventAggregateField(expression.Field) {
			return errors.New("analyze logical query: scalar field metadata is invalid")
		}
		return analyzer.addField(expression.Field, depth+1)
	case *ScalarLiteralExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar literal expression is nil")
		}
		if strict && !validEventAggregateLiteralKind(expression.Value.Kind) {
			return errors.New("analyze logical query: scalar literal kind is invalid")
		}
		return nil
	case *ScalarCallExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar call expression is nil")
		}
		if strict && (!validEventAggregateScalarFunction(expression.Function) ||
			!validEventAggregateNativeMVCall(expression)) {
			return errors.New("analyze logical query: scalar function metadata is invalid")
		}
		for _, argument := range expression.Arguments {
			if err := analyzer.visitScalarExpressionWithContext(
				argument,
				depth+1,
				0,
				strict,
			); err != nil {
				return err
			}
		}
		return nil
	case *ScalarIfExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar if expression is nil")
		}
		if err := analyzer.visitExpression(expression.Condition, depth+1); err != nil {
			return err
		}
		if err := analyzer.visitScalarExpressionWithContext(
			expression.True,
			depth+1,
			0,
			strict,
		); err != nil {
			return err
		}
		return analyzer.visitScalarExpressionWithContext(
			expression.False,
			depth+1,
			0,
			strict,
		)
	case *ScalarCaseExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar case expression is nil")
		}
		for _, branch := range expression.Branches {
			if err := analyzer.visitExpression(branch.Condition, depth+1); err != nil {
				return err
			}
			if err := analyzer.visitScalarExpressionWithContext(
				branch.Value,
				depth+1,
				0,
				strict,
			); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("analyze logical query: unsupported scalar expression %T", expression)
	}
}
