package clickhouse

import (
	"strings"
	"testing"
)

func TestCompilePipelineRetainedBytesSealsCanonicalJSONSettings(t *testing.T) {
	t.Parallel()

	compiled, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | makemv delim="," tags_csv | table tags_csv`,
	))
	if err != nil {
		t.Fatalf("Compile(makemv): %v", err)
	}
	for _, setting := range []string{
		`enable_named_columns_in_function_tuple = 1`,
		`output_format_json_named_tuples_as_objects = 1`,
		`output_format_json_skip_null_value_in_named_tuples = 0`,
		`output_format_json_map_as_array_of_tuples = 0`,
		`output_format_json_escape_forward_slashes = 0`,
		`output_format_json_quote_64bit_integers = 0`,
		`output_format_json_quote_64bit_floats = 0`,
		`output_format_json_quote_decimals = 0`,
		`output_format_json_quote_denormals = 0`,
	} {
		if !strings.Contains(compiled.SQL, setting) {
			t.Errorf("canonical retained-byte query does not seal %q:\n%s", setting, compiled.SQL)
		}
	}
}

func TestCompilePipelineMakeMVRetainedBytesUsesLogicalNullForAbsentReplacement(t *testing.T) {
	t.Parallel()

	compiled, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | makemv delim="," tags_csv | table tags_csv`,
	))
	if err != nil {
		t.Fatalf("Compile(makemv): %v", err)
	}
	const retainedStart = `sum(toUInt64(length(toJSONString(`
	start := strings.Index(compiled.SQL, retainedStart)
	if start < 0 {
		t.Fatalf("makemv query has no retained-byte expression:\n%s", compiled.SQL)
	}
	end := strings.Index(compiled.SQL[start:], ` AS "__os_makemv_retained_bytes_`)
	if end < 0 {
		t.Fatalf("makemv retained-byte expression has no output alias:\n%s", compiled.SQL)
	}
	retained := compiled.SQL[start : start+end]
	if !strings.Contains(retained, `"__os_makemv_value_present_`) ||
		!strings.Contains(retained, `CAST(NULL AS Dynamic)`) ||
		!strings.Contains(retained, `CAST("__os_makemv_result_`) {
		t.Fatalf(
			"makemv retained bytes charge the physical empty array instead of the public null:\n%s",
			retained,
		)
	}
}

func TestPipelineRetainedTupleUsesLogicalPresenceForPriorOptionalMultivalue(t *testing.T) {
	t.Parallel()

	state := compileState{
		publicOrder: []string{"tags", "message"},
		visible: map[string]fieldState{
			"tags": {
				valueSQL:                     `"tags"`,
				optionalMultivaluePresentSQL: `"__os_tags_present"`,
				kind:                         fieldKindStringArray,
			},
			"message": {valueSQL: `"message"`, kind: fieldKindString},
		},
	}
	retained := publicRetainedTupleSQL(state, "message", `"next_message"`)
	if !strings.Contains(retained, `"__os_tags_present"`) ||
		!strings.Contains(retained, `CAST(NULL AS Dynamic)`) ||
		!strings.Contains(retained, `CAST("tags" AS Dynamic)`) {
		t.Fatalf(
			"retained tuple ignores the public null represented by a prior optional-list sidecar:\n%s",
			retained,
		)
	}
}
