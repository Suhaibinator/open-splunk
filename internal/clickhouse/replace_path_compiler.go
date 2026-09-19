package clickhouse

import (
	"errors"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

const (
	// MaximumReplacePathInputBytes bounds per-call segment array allocation.
	MaximumReplacePathInputBytes uint64 = 4 << 20
	// MaximumReplacePathQueryInputBytes bounds cumulative per-row segment work.
	MaximumReplacePathQueryInputBytes uint64 = 16 << 20
	maxCompiledReplacePathSQLBytes           = 64 << 10
)

type compiledReplacePathBudget struct {
	inputBytes       uint64
	programWorkUnits int
}

func compileReplacePathSQL(inputSQL string) string {
	// Bind all user expressions once, with parameters in input/pattern/replacement
	// order. Arrays contain at most input-bytes+1 components; no delimiter is lost.
	const value = "__os_replace_path_value"
	const core = "__os_replace_path_core"
	const parts = "__os_replace_path_parts"
	const pattern = "__os_replace_path_pattern"
	const replacement = "__os_replace_path_replacement"
	const part = "__os_replace_path_part"
	const index = "__os_replace_path_index"
	joined := "arrayStringConcat(arrayMap((" + part + ", " + index + ") -> if(" + index + " = 1, " + part + ", replaceRegexpAll(concat('/', " + part + "), " + pattern + ", " + replacement + ")), " + parts + ", arrayEnumerate(" + parts + ")), '')"
	joined = bindSQLExpressions([]string{parts}, []string{"splitByChar('/', " + core + ")"}, joined)
	// BODY cannot consume newline. Removing exactly one final LF implements
	// PCRE's non-multiline dollar without consuming it in the replacement.
	text := "assumeNotNull(" + value + ")"
	finalLF := "endsWith(" + text + ", char(10))"
	joined = bindSQLExpressions([]string{core}, []string{"substring(" + text + ", 1, length(" + text + ") - " + finalLF + ")"}, joined)
	body := "if(isNull(" + value + "), CAST(NULL AS Nullable(String)), concat(" + joined + ", if(" + finalLF + ", char(10), '')))"
	return bindSQLExpressions([]string{value, pattern, replacement}, []string{inputSQL, "CAST(? AS String)", "CAST(? AS String)"}, body)
}

func reserveReplacePath(context *compileContext, pattern splregex.ReplacePattern, inputBytes uint64, sqlBytes int, sourceRange spl.Range) error {
	if context == nil {
		return errors.New("compile ClickHouse replace: query context is required")
	}
	used := context.replacePathBudget
	if inputBytes > MaximumReplacePathInputBytes || used.inputBytes > MaximumReplacePathQueryInputBytes ||
		inputBytes > MaximumReplacePathQueryInputBytes-used.inputBytes ||
		used.programWorkUnits > splregex.MaximumReplacePathQueryProgramWorkUnits ||
		pattern.ProgramWorkUnits > splregex.MaximumReplacePathQueryProgramWorkUnits-used.programWorkUnits ||
		sqlBytes > maxCompiledReplacePathSQLBytes {
		return &plan.Diagnostic{Code: "SPL_QUERY_TOO_COMPLEX", Message: "replace path evaluation exceeds the input, program, or SQL resource limit", Range: sourceRange}
	}
	context.replacePathBudget.inputBytes += inputBytes
	context.replacePathBudget.programWorkUnits += pattern.ProgramWorkUnits
	return nil
}
