package clickhouse

import (
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

type compiledStatsMultivalueGroup struct {
	field           fieldState
	valuesSQL       string
	valuesArgs      []any
	unsupportedSQL  string
	unsupportedArgs []any
}

// compileStatsMultivalueGroup recognizes the two physical inputs that can
// carry more than one SPL value: a fixed String array and a raw Dynamic field.
// Scalars in Dynamic are represented as singleton arrays, so one staged ARRAY
// JOIN path handles rows whose runtime shape varies without zipping multiple
// BY fields together. Unsupported nested containers retain a separate
// whole-input validation bit; they are never silently discarded as nulls.
func compileStatsMultivalueGroup(
	group plan.FieldRef,
	state compileState,
	deduplicate bool,
) (compiledStatsMultivalueGroup, bool, error) {
	field, exists, err := resolveCompiledField(group, state)
	if err != nil {
		return compiledStatsMultivalueGroup{}, false, err
	}
	if !exists {
		return compiledStatsMultivalueGroup{}, false, nil
	}

	emptyStrings := "CAST([], 'Array(String)')"
	switch field.kind {
	case fieldKindStringArray:
		valuesSQL := field.valueSQL
		valuesArgs := append([]any(nil), field.existsArgs...)
		if field.existsSQL != "" && field.existsSQL != "1" {
			valuesSQL = "if(" + field.existsSQL + ", " + valuesSQL + ", " + emptyStrings + ")"
		}
		if deduplicate {
			valuesSQL = "arrayDistinct(" + valuesSQL + ")"
		}
		return compiledStatsMultivalueGroup{
			field:      field,
			valuesSQL:  valuesSQL,
			valuesArgs: valuesArgs,
		}, true, nil
	case fieldKindDynamic:
		typeSQL := dynamicTypeExpression(field)
		scalarSupported, scalarLexical := statsByScalarExpressionsFor(
			field.valueSQL,
			typeSQL,
		)
		memberType := "dynamicType(element)"
		memberSupported, memberLexical := statsByScalarExpressionsFor(
			"element",
			memberType,
		)
		members := "dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)')"
		memberValues := "arrayMap(element -> " + memberLexical +
			", arrayFilter(element -> " + memberType + " != 'None' AND (" +
			memberSupported + "), " + members + "))"
		valuesSQL := "multiIf(" +
			typeSQL + " = 'Array(Dynamic)', " + memberValues + ", " +
			typeSQL + " = 'None', " + emptyStrings + ", " +
			scalarSupported + ", [" + scalarLexical + "], " +
			emptyStrings + ")"
		existsSQL := field.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		valuesSQL = "if(" + existsSQL + ", " + valuesSQL + ", " + emptyStrings + ")"
		if deduplicate {
			valuesSQL = "arrayDistinct(" + valuesSQL + ")"
		}

		arrayUnsupported := "arrayExists(element -> " + memberType +
			" != 'None' AND NOT (" + memberSupported + "), " + members + ")"
		scalarUnsupported := "(" + typeSQL + " != 'None' AND NOT (" +
			scalarSupported + "))"
		unsupportedSQL := "(" + existsSQL + ") AND if(" + typeSQL +
			" = 'Array(Dynamic)', " + arrayUnsupported + ", " + scalarUnsupported + ")"
		unsupportedArgs := append([]any(nil), field.existsArgs...)
		if field.descendantSQL != "" {
			unsupportedSQL = "(" + field.descendantSQL + ") OR (" + unsupportedSQL + ")"
			unsupportedArgs = append(
				append([]any(nil), field.descendantArgs...),
				unsupportedArgs...,
			)
		}
		return compiledStatsMultivalueGroup{
			field:           field,
			valuesSQL:       valuesSQL,
			valuesArgs:      append([]any(nil), field.existsArgs...),
			unsupportedSQL:  unsupportedSQL,
			unsupportedArgs: unsupportedArgs,
		}, true, nil
	default:
		return compiledStatsMultivalueGroup{}, false, nil
	}
}

type compiledStatsGroupExpansion struct {
	valuesAlias string
	valueAlias  string
}
