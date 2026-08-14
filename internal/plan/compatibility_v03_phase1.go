package plan

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

const maximumSearchJobIDBytes = 256

// RegexFilter is the command-specific filter boundary for regex. It remains
// distinct from eval predicates so parser-owned eval/where predicate evidence
// cannot be minted by an alias command, while the backend still invokes the
// shared match implementation and its aggregate regex budget.
type RegexFilter struct {
	Input        FieldRef
	Pattern      string
	PatternRange spl.Range
	Negated      bool
	Range        spl.Range
}

func (*RegexFilter) operator()                 {}
func (*RegexFilter) LogicalName() string       { return "RegexFilter" }
func (op *RegexFilter) SourceRange() spl.Range { return op.Range }

// Reverse reverses the complete established input ordinal. It changes no
// fields, values, event identities, or row cardinality.
type Reverse struct {
	Range spl.Range
}

func (*Reverse) operator()                 {}
func (*Reverse) LogicalName() string       { return "Reverse" }
func (op *Reverse) SourceRange() spl.Range { return op.Range }

// Strcat concatenates a bounded list of exact fields and String literals into
// Destination. Operands use the existing scalar IR so ClickHouse lowering can
// share exactly the period-concatenation conversion and byte accounting.
type Strcat struct {
	Operands    []ScalarExpression
	Destination FieldRef
	AllRequired bool
	Range       spl.Range
}

func (*Strcat) operator()                 {}
func (*Strcat) LogicalName() string       { return "Strcat" }
func (op *Strcat) SourceRange() spl.Range { return op.Range }

func buildRegexCommand(command *spl.RegexCommand, outputSchemaKnown bool) (*RegexFilter, error) {
	if command == nil || command.Field == "" || command.PatternRange == (spl.Range{}) ||
		command.Range == (spl.Range{}) || !spl.IsExactUnquotedFieldName(command.Field) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_REGEX_SYNTAX",
			Message: "regex requires one exact unquoted field and one quoted literal pattern",
			Range:   safeSPLNodeRange(command),
		}
	}
	if !outputSchemaKnown && command.Field == "fields" {
		return nil, &Diagnostic{
			Code:    "SPL_AMBIGUOUS_REGEX_FIELD",
			Message: "regex cannot read the event result's reserved fields payload without an exact upstream schema",
			Range:   command.FieldRange,
		}
	}
	if _, err := splregex.CompileMatchPattern(command.Pattern); err != nil {
		if splregex.IsMatchComplexityError(err) {
			return nil, &Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"regex regular expression exceeds the %d-byte or %d-work-unit limit",
					splregex.MaximumMatchPatternBytes,
					splregex.MaximumMatchProgramWorkUnits,
				),
				Range: command.PatternRange,
			}
		}
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_REGEX",
			Message: "regex regular expression is outside the supported RE2-compatible subset",
			Range:   command.PatternRange,
		}
	}
	input, err := ResolveField(command.Field, command.FieldRange)
	if err != nil {
		return nil, err
	}
	return &RegexFilter{
		Input:        input,
		Pattern:      strings.Clone(command.Pattern),
		PatternRange: command.PatternRange,
		Negated:      command.Negated,
		Range:        command.Range,
	}, nil
}

func buildAccumCommand(command *spl.AccumCommand, outputSchemaKnown bool) (*StreamAggregate, error) {
	if command == nil || command.Range == (spl.Range{}) ||
		!spl.IsExactUnquotedFieldName(command.Field) ||
		!spl.IsExactUnquotedFieldName(command.Output) ||
		command.FieldRange == (spl.Range{}) || command.OutputRange == (spl.Range{}) ||
		(!command.ExplicitOutput &&
			(command.Output != command.Field || command.OutputRange != command.FieldRange)) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_ACCUM_SYNTAX",
			Message: "accum requires one exact input and its default replacement or one explicit AS output",
			Range:   safeSPLNodeRange(command),
		}
	}
	if !outputSchemaKnown && command.Field == "fields" {
		return nil, &Diagnostic{
			Code:    "SPL_AMBIGUOUS_ACCUM_FIELD",
			Message: "accum cannot read the event result's reserved fields payload without an exact upstream schema",
			Range:   command.FieldRange,
		}
	}
	if !outputSchemaKnown && command.Output == "fields" {
		return nil, &Diagnostic{
			Code:    "SPL_AMBIGUOUS_ACCUM_FIELD",
			Message: "accum cannot replace the event result's reserved fields payload without an exact upstream schema",
			Range:   command.OutputRange,
		}
	}
	input, err := ResolveField(command.Field, command.FieldRange)
	if err != nil {
		return nil, err
	}
	if _, err := ResolveField(command.Output, command.OutputRange); err != nil {
		return nil, err
	}
	return &StreamAggregate{
		Measure: AggregateMeasure{
			Function: AggregateFunctionSum,
			Input:    input,
			Output:   command.Output,
		},
		OutputRange:    command.OutputRange,
		IncludeCurrent: true,
		Global:         true,
		Range:          command.Range,
	}, nil
}

func buildStrcatCommand(
	command *spl.StrcatCommand,
	outputSchemaKnown bool,
	budget *splExpressionResourceBudget,
) (*Strcat, error) {
	if command == nil || command.Range == (spl.Range{}) ||
		len(command.Operands) < 2 || len(command.Operands) > spl.MaximumConcatenationOperands ||
		!spl.IsExactUnquotedFieldName(command.Destination) ||
		command.DestinationRange == (spl.Range{}) ||
		(!command.AllRequiredSpecified &&
			(command.AllRequired || command.AllRequiredRange != (spl.Range{}))) ||
		(command.AllRequiredSpecified && command.AllRequiredRange == (spl.Range{})) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STRCAT_SYNTAX",
			Message: "strcat requires two through 32 exact fields or quoted literals and one exact destination",
			Range:   safeSPLNodeRange(command),
		}
	}
	if budget == nil || budget.concatenationOperands >
		spl.MaximumConcatenationOperandsPerQuery-len(command.Operands) {
		return nil, &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"concatenation contains more than %d operand occurrences per query",
				spl.MaximumConcatenationOperandsPerQuery,
			),
			Range: command.Range,
		}
	}
	budget.concatenationOperands += len(command.Operands)
	if !outputSchemaKnown && command.Destination == "fields" {
		return nil, &Diagnostic{
			Code:    "SPL_AMBIGUOUS_STRCAT_FIELD",
			Message: "strcat cannot replace the event result's reserved fields payload without an exact upstream schema",
			Range:   command.DestinationRange,
		}
	}
	destination, err := ResolveField(command.Destination, command.DestinationRange)
	if err != nil {
		return nil, err
	}
	operands := make([]ScalarExpression, 0, len(command.Operands))
	for _, operand := range command.Operands {
		if operand.Range == (spl.Range{}) ||
			(operand.Field == "") == (operand.Literal == nil) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STRCAT_SYNTAX",
				Message: "each strcat source must be exactly one field or quoted String literal",
				Range:   operand.Range,
			}
		}
		if operand.Literal != nil {
			operands = append(operands, &ScalarLiteralExpression{
				Value: Value{
					Kind:       ValueKindString,
					String:     strings.Clone(*operand.Literal),
					Quoted:     true,
					SourceText: strings.Clone(*operand.Literal),
				},
				Range: operand.Range,
			})
			continue
		}
		if !spl.IsExactUnquotedFieldName(operand.Field) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STRCAT_SYNTAX",
				Message: "strcat source fields must be exact and unquoted",
				Range:   operand.Range,
			}
		}
		if !outputSchemaKnown && operand.Field == "fields" {
			return nil, &Diagnostic{
				Code:    "SPL_AMBIGUOUS_STRCAT_FIELD",
				Message: "strcat cannot read the event result's reserved fields payload without an exact upstream schema",
				Range:   operand.Range,
			}
		}
		field, fieldErr := ResolveField(operand.Field, operand.Range)
		if fieldErr != nil {
			return nil, fieldErr
		}
		operands = append(operands, &ScalarFieldExpression{Field: field, Range: operand.Range})
	}
	return &Strcat{
		Operands:    operands,
		Destination: destination,
		AllRequired: command.AllRequired,
		Range:       command.Range,
	}, nil
}

func buildAddInfoCommand(command *spl.AddInfoCommand, scope Scope) (*Extend, error) {
	if command == nil || command.Range == (spl.Range{}) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_ADDINFO_SYNTAX",
			Message: "addinfo does not accept arguments or options",
			Range:   safeSPLNodeRange(command),
		}
	}
	searchJobID := scope.SearchJobID
	unboundValidation := searchJobID == "" && scope.AllowUnboundSearchJobID
	if !unboundValidation && !validSearchJobID(searchJobID) {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_SEARCH_JOB_ID",
			Message: "addinfo requires an immutable public search-job identifier",
			Range:   command.Range,
		}
	}
	sid := Value{
		Kind:       ValueKindString,
		String:     strings.Clone(searchJobID),
		SourceText: strings.Clone(searchJobID),
	}
	if unboundValidation {
		// Validation and suggestion planning intentionally have no public job
		// identity to publish. Preserve the output shape with an explicit null
		// placeholder instead of fabricating a string that could be mistaken for
		// an executable SID. Every executing path must supply SearchJobID.
		sid = Value{Kind: ValueKindNull, SourceText: "null"}
	}
	values := []struct {
		name  string
		value Value
	}{
		{name: "info_min_time", value: unixSecondsValue(scope.Earliest)},
		{name: "info_max_time", value: unixSecondsValue(scope.Latest)},
		{name: "info_search_time", value: unixSecondsValue(scope.SearchStart)},
		{name: "info_sid", value: sid},
	}
	assignments := make([]ExtendAssignment, 0, len(values))
	for _, entry := range values {
		output, err := ResolveField(entry.name, command.Range)
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, ExtendAssignment{
			Output: output,
			Expression: &ScalarLiteralExpression{
				Value: entry.value,
				Range: command.Range,
			},
			Range: command.Range,
		})
	}
	return &Extend{Assignments: assignments, Range: command.Range}, nil
}

func unixSecondsValue(value time.Time) Value {
	seconds := float64(value.Unix()) + float64(value.Nanosecond())/1e9
	return Value{
		Kind:       ValueKindFloat64,
		Float64:    seconds,
		SourceText: strconv.FormatFloat(seconds, 'g', -1, 64),
	}
}

func validSearchJobID(value string) bool {
	if value == "" || len(value) > maximumSearchJobIDBytes ||
		!utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
