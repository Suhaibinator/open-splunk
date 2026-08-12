package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

const (
	// These strings are stable executor classifiers. They deliberately contain
	// neither object identity nor the rejected event value.
	KnowledgeSelectorInvalidUTF8Marker = "open-splunk: knowledge selector value is not valid UTF-8"
	KnowledgeSelectorValueLimitMarker  = "open-splunk: knowledge selector value exceeds the per-value limit"
	KnowledgeSelectorEventLimitMarker  = "open-splunk: knowledge selector input exceeds the per-event limit"
	KnowledgeSelectorQueryLimitMarker  = "open-splunk: knowledge selector work exceeds the per-query limit"
)

type compiledKnowledgeSelector struct {
	sql  string
	args []any
}

type knowledgeSelectorDimension struct {
	program     knowledge.DimensionRuntimeProgram
	constrained bool
}

// compileKnowledgeSelector lowers the immutable selector authority to one
// row-local tuple: (matched UInt8, inspected bytes UInt128, query units
// UInt128). Later dimensions occur only inside the preceding match branch.
func compileKnowledgeSelector(selector knowledgeprogram.Selector) (compiledKnowledgeSelector, error) {
	dimensions := [knowledge.MaximumSelectorDimensions]knowledgeSelectorDimension{}
	constrained := false
	for dimension := knowledge.DimensionIndex; dimension <= knowledge.DimensionSourcetype; dimension++ {
		program, ok := selector.RuntimeProgram(dimension)
		if !ok {
			continue
		}
		dimensions[dimension-knowledge.DimensionIndex] = knowledgeSelectorDimension{
			program:     program,
			constrained: true,
		}
		constrained = true
	}
	return compileKnowledgeSelectorDimensions(selector.IsUnrestricted(), constrained, dimensions)
}

func compileKnowledgeSelectorDimensions(
	unrestricted bool,
	constrained bool,
	dimensions [knowledge.MaximumSelectorDimensions]knowledgeSelectorDimension,
) (compiledKnowledgeSelector, error) {
	actualConstrained := false
	for _, dimension := range dimensions {
		if !dimension.constrained && !knowledgeSelectorDimensionProgramIsZero(dimension.program) {
			return compiledKnowledgeSelector{}, errors.New(
				"compile ClickHouse knowledge selector: unconstrained dimension retains matcher authority",
			)
		}
		actualConstrained = actualConstrained || dimension.constrained
	}
	if constrained != actualConstrained || unrestricted != !actualConstrained {
		return compiledKnowledgeSelector{}, errors.New("compile ClickHouse knowledge selector: authority is inconsistent")
	}
	result := compiledKnowledgeSelector{sql: knowledgeSelectorUniversalTuple()}
	for dimension := knowledge.DimensionSourcetype; dimension >= knowledge.DimensionIndex; dimension-- {
		entry := dimensions[dimension-knowledge.DimensionIndex]
		if !entry.constrained {
			if dimension == knowledge.DimensionIndex {
				break
			}
			continue
		}
		current, err := compileKnowledgeSelectorDimension(
			quoteIdentifier(dimension.String()),
			entry.program,
		)
		if err != nil {
			return compiledKnowledgeSelector{}, fmt.Errorf(
				"compile ClickHouse knowledge selector %s: %w",
				dimension,
				err,
			)
		}
		result = combineKnowledgeSelectorDimensions(current, result)
		if dimension == knowledge.DimensionIndex {
			break
		}
	}
	return result, nil
}

func compileKnowledgeSelectorDimension(
	valueSQL string,
	program knowledge.DimensionRuntimeProgram,
) (compiledKnowledgeSelector, error) {
	if valueSQL == "" {
		return compiledKnowledgeSelector{}, errors.New("trusted metadata expression is empty")
	}
	exact := slices.Clone(program.ExactLiterals)
	if !slices.IsSorted(exact) {
		return compiledKnowledgeSelector{}, errors.New("exact literals are not canonical")
	}
	for index, literal := range exact {
		if literal == "" || index > 0 && exact[index-1] == literal {
			return compiledKnowledgeSelector{}, errors.New("exact literals are invalid")
		}
	}
	wildcard := strings.Clone(program.WildcardRE2)
	assessment := program.Assessment
	if len(exact) == 0 && wildcard == "" {
		return compiledKnowledgeSelector{}, errors.New("constrained dimension has no matcher")
	}
	if wildcard == "" && assessment != (knowledge.MatcherTransitionAssessment{}) {
		return compiledKnowledgeSelector{}, errors.New("literal-only dimension has wildcard work")
	}
	if wildcard != "" && (assessment.Initial == 0 || assessment.PerInputByte == 0 || assessment.Final == 0) {
		return compiledKnowledgeSelector{}, errors.New("wildcard dimension has no assessed work")
	}

	const (
		valueVariable = "__os_ko_selector_value"
		bytesVariable = "__os_ko_selector_bytes"
		exactVariable = "__os_ko_selector_exact"
	)
	matchSQL := "toUInt8(0)"
	unitsSQL := bytesVariable
	args := make([]any, 0, 2)
	switch {
	case len(exact) > 0 && wildcard != "":
		matchSQL = "toUInt8(if(" + exactVariable + " != 0, 1, match(" +
			valueVariable + ", ?)))"
		unitsSQL = bytesVariable + " + if(" + exactVariable + " != 0, toUInt128(0), " +
			knowledgeSelectorWildcardUnits(bytesVariable, assessment) + ")"
		// bindSQLExpressions writes its lambda body before the bound value, so
		// the wildcard placeholder precedes the exact-literal array placeholder.
		args = append(args, wildcard, exact)
	case len(exact) > 0:
		matchSQL = "toUInt8(" + exactVariable + " != 0)"
		args = append(args, exact)
	case wildcard != "":
		matchSQL = "toUInt8(match(" + valueVariable + ", ?))"
		unitsSQL = bytesVariable + " + " + knowledgeSelectorWildcardUnits(bytesVariable, assessment)
		args = append(args, wildcard)
	}
	valid := "tuple(" + matchSQL + ", " + bytesVariable + ", " + unitsSQL + ")"
	if len(exact) > 0 {
		valid = bindSQLExpressions(
			[]string{exactVariable},
			[]string{"toUInt8(has(CAST(? AS Array(String)), " + valueVariable + "))"},
			valid,
		)
	}
	valid = bindSQLExpressions(
		[]string{bytesVariable},
		[]string{"toUInt128(length(" + valueVariable + "))"},
		valid,
	)
	invalidUTF8 := "NOT isValidUTF8(" + valueVariable + ")"
	overValueLimit := "length(" + valueVariable + ") > " +
		strconv.Itoa(knowledge.MaximumSelectorRuntimeValueBytes)
	guarded := "if(" + overValueLimit + ", " +
		knowledgeSelectorThrowTuple(overValueLimit, KnowledgeSelectorValueLimitMarker) +
		", if(" + invalidUTF8 + ", " +
		knowledgeSelectorThrowTuple(invalidUTF8, KnowledgeSelectorInvalidUTF8Marker) +
		", " + valid + "))"
	guarded = bindSQLExpressions(
		[]string{valueVariable},
		[]string{"assumeNotNull(" + valueSQL + ")"},
		guarded,
	)
	return compiledKnowledgeSelector{
		sql:  "if(isNull(" + valueSQL + "), " + knowledgeSelectorZeroTuple() + ", " + guarded + ")",
		args: args,
	}, nil
}

func knowledgeSelectorDimensionProgramIsZero(program knowledge.DimensionRuntimeProgram) bool {
	return len(program.ExactLiterals) == 0 && program.WildcardRE2 == "" &&
		program.Assessment == (knowledge.MatcherTransitionAssessment{})
}

func combineKnowledgeSelectorDimensions(
	current compiledKnowledgeSelector,
	tail compiledKnowledgeSelector,
) compiledKnowledgeSelector {
	const (
		currentVariable = "__os_ko_selector_current"
		tailVariable    = "__os_ko_selector_tail"
	)
	tailMerge := bindSQLExpressions(
		[]string{tailVariable},
		[]string{tail.sql},
		"tuple(toUInt8(tupleElement("+tailVariable+", 1)), "+
			"toUInt128(tupleElement("+currentVariable+", 2)) + toUInt128(tupleElement("+tailVariable+", 2)), "+
			"toUInt128(tupleElement("+currentVariable+", 3)) + toUInt128(tupleElement("+tailVariable+", 3)))",
	)
	merged := bindSQLExpressions(
		[]string{currentVariable},
		[]string{current.sql},
		"if(tupleElement("+currentVariable+", 1) != 0, "+tailMerge+", "+currentVariable+")",
	)
	// The lazily nested tail is textually inside the outer lambda body, while
	// the current tuple is its bound value. Placeholder order is therefore the
	// reverse of evaluation order even though runtime evaluation remains fixed.
	args := make([]any, 0, len(tail.args)+len(current.args))
	args = append(args, tail.args...)
	args = append(args, current.args...)
	return compiledKnowledgeSelector{sql: merged, args: args}
}

func knowledgeSelectorWildcardUnits(
	bytesSQL string,
	assessment knowledge.MatcherTransitionAssessment,
) string {
	bound := "toUInt128(" + strconv.FormatUint(assessment.Initial, 10) + ") + " +
		"toUInt128(" + bytesSQL + ") * toUInt128(" +
		strconv.FormatUint(assessment.PerInputByte, 10) + ") + toUInt128(" +
		strconv.FormatUint(assessment.Final, 10) + ")"
	return "toUInt128(" + strconv.Itoa(knowledge.SelectorMatcherTransitionUnits) + ") * (" + bound + ")"
}

func knowledgeSelectorThrowTuple(condition, marker string) string {
	return "tuple(toUInt8(throwIf(toUInt8(" + condition + "), '" + marker + "') = 0), " +
		"toUInt128(0), toUInt128(0))"
}

func knowledgeSelectorUniversalTuple() string {
	return "tuple(toUInt8(1), toUInt128(0), toUInt128(0))"
}

func knowledgeSelectorZeroTuple() string {
	return "tuple(toUInt8(0), toUInt128(0), toUInt128(0))"
}
