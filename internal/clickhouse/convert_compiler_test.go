package clickhouse

import (
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileConvertExactFunctionsPreserveSparseMetadata(t *testing.T) {
	t.Parallel()
	for _, source := range []string{
		`index=gradethis | convert ctime(_time) AS rendered | table _time rendered`,
		`index=gradethis | convert timeformat="%F %T" mktime(timestamp) AS epoch | table timestamp epoch`,
		`index=gradethis | convert dur2sec(delay) AS seconds num(bytes) rmunit(duration) | table seconds bytes duration`,
		`index=gradethis | convert ctime(nested) AS copied | table copied.child nested.child`,
		`index=gradethis | eval epoch="0" | convert ctime(epoch) | table epoch`,
		`index=gradethis | fields event_id | convert num(absent) AS number | table event_id number`,
	} {
		t.Run(source, func(t *testing.T) {
			compiled := compileSPL(t, source)
			if !compiled.HasValidExecutionSeal() {
				t.Fatal("convert query has no execution authority")
			}
			if strings.Count(compiled.SQL, "?") != len(compiled.Args) {
				t.Fatalf("placeholder count differs: %d placeholders, %d args", strings.Count(compiled.SQL, "?"), len(compiled.Args))
			}
			for _, required := range []string{"__os_convert_exists_", "__os_convert_type_", "__os_convert_names_", "__os_convert_types_", "__os_convert_metadata_"} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("convert does not retain %s", required)
				}
			}
		})
	}
}

func TestCompileConvertAutoPinsBothSidesOfWholeFieldDecision(t *testing.T) {
	t.Parallel()
	compiled := compileSPL(t, `index=gradethis | convert auto(bytes) | where bytes=1 | head 1 | table bytes`)
	for _, required := range []string{
		`"__os_convert_auto_input_2_0" AS MATERIALIZED`,
		`"__os_convert_auto_output_2_0" AS MATERIALIZED`,
		"min(toUInt8(", "OVER ()", materializedValidationSettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("auto lacks whole-field fence %q", required)
		}
	}
}

func TestCompileConvertChecksForgedFunctionsAndFieldAuthority(t *testing.T) {
	t.Parallel()
	for _, function := range []spl.ConvertFunction{spl.ConvertFunctionInvalid, spl.ConvertFunctionNone, 255} {
		logical := buildPlan(t, `index=gradethis | convert num(bytes)`)
		logical.Operators[len(logical.Operators)-1].(*plan.Convert).Assignments[0].Function = function
		if _, err := (Compiler{}).Compile(logical); err == nil {
			t.Fatalf("forged function %d was accepted", function)
		}
	}
	logical := buildPlan(t, `index=gradethis | convert num(bytes)`)
	logical.Operators[len(logical.Operators)-1].(*plan.Convert).Assignments[0].Input.Path = []string{"other"}
	if _, err := (Compiler{}).Compile(logical); err == nil {
		t.Fatal("forged field path was accepted")
	}
	logical = buildPlan(t, `index=gradethis | convert ctime(_time)`)
	logical.Operators[len(logical.Operators)-1].(*plan.Convert).TimeFormat = "%Z"
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if err == nil || !errors.As(err, &diagnostic) {
		t.Fatalf("forged format error = %v", err)
	}
}

func TestCompileConvertNumericGrammarAndPlaceholderContract(t *testing.T) {
	t.Parallel()
	for _, all := range []bool{false, true} {
		sql, args := compileConvertNumericCandidate("input", "kind", "text", all)
		if strings.Count(sql, "?") != len(args) {
			t.Fatalf("recognizer placeholders = %d, args = %d", strings.Count(sql, "?"), len(args))
		}
		for _, argument := range args {
			if argument == convertNumericPrefixPattern {
				t.Fatal("implicit numeric conversion accepts a numeric prefix")
			}
		}
	}
}
