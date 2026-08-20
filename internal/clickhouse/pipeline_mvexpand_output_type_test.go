package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestPipelineMVExpandPreservesFixedScalarOutputTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      fieldState
		wantKind   fieldKind
		wantNumber string
	}{
		{
			name: "String multivalue becomes nullable String",
			input: fieldState{
				valueSQL:                     `"value"`,
				existsSQL:                    `"value_present"`,
				optionalMultivaluePresentSQL: `"value_present"`,
				kind:                         fieldKindStringArray,
			},
			wantKind: fieldKindString,
		},
		{
			name: "String scalar",
			input: fieldState{
				valueSQL: `"value"`, existsSQL: "1", kind: fieldKindString,
				maxStringBytes: 4096,
			},
			wantKind: fieldKindString,
		},
		{
			name: "signed scalar",
			input: fieldState{
				valueSQL: `"value"`, existsSQL: "1", kind: fieldKindNumber,
				numberType: "Int64", numericIntegral: true,
			},
			wantKind: fieldKindNumber, wantNumber: "Int64",
		},
		{
			name: "floating scalar",
			input: fieldState{
				valueSQL: `"value"`, existsSQL: "1", kind: fieldKindNumber,
				numberType: "Float64", ieeeComparison: true,
			},
			wantKind: fieldKindNumber, wantNumber: "Float64",
		},
		{
			name: "Bool scalar",
			input: fieldState{
				valueSQL: `"value"`, existsSQL: "1", kind: fieldKindBool,
			},
			wantKind: fieldKindBool,
		},
		{
			name: "time scalar",
			input: fieldState{
				valueSQL: `"value"`, existsSQL: "1", kind: fieldKindTime,
				numberType: "DateTime64(9, 'UTC')", canonicalTime: true,
			},
			wantKind: fieldKindTime, wantNumber: "DateTime64(9, 'UTC')",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result, next, _, err := compilePipelineMVExpandTypeFixture(t, test.input)
			if err != nil {
				t.Fatalf("compileExpandMultivalue: %v", err)
			}
			output := next.visible["value"]
			if output.kind != test.wantKind || output.numberType != test.wantNumber {
				t.Fatalf(
					"expanded output type = kind %d/%q, want %d/%q",
					output.kind,
					output.numberType,
					test.wantKind,
					test.wantNumber,
				)
			}
			if output.optionalMultivaluePresentSQL != "" {
				t.Fatalf("expanded scalar retained multivalue presence sidecar %q", output.optionalMultivaluePresentSQL)
			}
			if strings.Contains(result.sql, `CAST("__os_mvexpand_input_1" AS Dynamic)`) ||
				strings.Contains(result.sql, `arrayMap(value -> CAST(value AS Dynamic), "__os_mvexpand_input_1")`) {
				t.Fatalf("fixed mvexpand value was widened to Dynamic:\n%s", result.sql)
			}
		})
	}
}

func TestPipelineMVExpandKeepsDynamicOutputDynamic(t *testing.T) {
	t.Parallel()

	result, next, _, err := compilePipelineMVExpandTypeFixture(t, fieldState{
		valueSQL:       `"value"`,
		dynamicTypeSQL: `dynamicType("value")`,
		existsSQL:      "1",
		kind:           fieldKindDynamic,
	})
	if err != nil {
		t.Fatalf("compileExpandMultivalue: %v", err)
	}
	output := next.visible["value"]
	if output.kind != fieldKindDynamic ||
		output.dynamicTypeSQL != `dynamicType("value")` {
		t.Fatalf("expanded Dynamic output = %#v, want Dynamic", output)
	}
	if !strings.Contains(result.sql, `dynamicElement("__os_mvexpand_input_1", 'Array(Dynamic)')`) {
		t.Fatalf("Dynamic mvexpand lost its runtime member dispatch:\n%s", result.sql)
	}
}

func TestPipelineMVExpandPreservesFixedStringSemanticsAndRejectsNonText(t *testing.T) {
	t.Parallel()

	t.Run("scalar", func(t *testing.T) {
		t.Parallel()

		input := fieldState{
			valueSQL:        `"value"`,
			existsSQL:       "1",
			kind:            fieldKindString,
			caseSensitive:   true,
			textEligibleSQL: `"text_eligible"`,
			storedTypeSQL:   `"stored_type"`,
		}
		result, next, _, err := compilePipelineMVExpandTypeFixture(t, input)
		if err != nil {
			t.Fatalf("compileExpandMultivalue: %v", err)
		}
		output := next.visible["value"]
		if !output.caseSensitive || output.textEligibleSQL != input.textEligibleSQL ||
			output.storedTypeSQL != input.storedTypeSQL {
			t.Fatalf("expanded fixed String metadata = %#v, want source text/type/case provenance", output)
		}
		for _, required := range []string{
			`ifNull("text_eligible", 0)`,
			`toUInt8(ifNull("stored_type", 0)) = toUInt8(2)`,
			`isValidUTF8("__os_mvexpand_input_1")`,
			UnsupportedMVExpandValueMarker,
		} {
			if !strings.Contains(result.sql, required) {
				t.Fatalf("fixed String mvexpand lost %q:\n%s", required, result.sql)
			}
		}
	})

	t.Run("String array", func(t *testing.T) {
		t.Parallel()

		input := fieldState{
			valueSQL:                     `"value"`,
			existsSQL:                    `"value_present"`,
			optionalMultivaluePresentSQL: `"value_present"`,
			kind:                         fieldKindStringArray,
			caseSensitive:                true,
			textEligibleSQL:              `"array_text_eligible"`,
			storedTypeSQL:                `"array_stored_type"`,
		}
		result, next, _, err := compilePipelineMVExpandTypeFixture(t, input)
		if err != nil {
			t.Fatalf("compileExpandMultivalue: %v", err)
		}
		output := next.visible["value"]
		if !output.caseSensitive || output.kind != fieldKindString {
			t.Fatalf("expanded fixed String-array metadata = %#v, want case-sensitive String", output)
		}
		if output.textEligibleSQL != `isValidUTF8(ifNull("value", CAST('' AS String)))` {
			t.Fatalf("expanded member text proof = %q", output.textEligibleSQL)
		}
		if output.storedTypeSQL != "" {
			t.Fatalf("expanded member kept source list stored type %q", output.storedTypeSQL)
		}
		for _, required := range []string{
			`ifNull("array_text_eligible", 0)`,
			`toUInt8(ifNull("array_stored_type", 0)) = toUInt8(10)`,
			`arrayExists(value -> NOT isValidUTF8(value), "__os_mvexpand_input_1")`,
			UnsupportedMVExpandValueMarker,
		} {
			if !strings.Contains(result.sql, required) {
				t.Fatalf("fixed String-array mvexpand lost %q:\n%s", required, result.sql)
			}
		}
	})
}

func TestPipelineMVExpandDynamicStringBranchesRejectInvalidUTF8AndBytesProvenance(t *testing.T) {
	t.Parallel()

	result, _, _, err := compilePipelineMVExpandTypeFixture(t, fieldState{
		valueSQL:        `"value"`,
		dynamicTypeSQL:  `dynamicType("value")`,
		existsSQL:       "1",
		kind:            fieldKindDynamic,
		textEligibleSQL: `"text_eligible"`,
		storedTypeSQL:   `"stored_type"`,
	})
	if err != nil {
		t.Fatalf("compileExpandMultivalue: %v", err)
	}
	for _, required := range []string{
		`dynamicType(member) = 'String' AND isValidUTF8(dynamicElement(member, 'String'))`,
		`arrayExists(member -> NOT isValidUTF8(member), dynamicElement("__os_mvexpand_input_1", 'Array(String)'))`,
		`toUInt8(ifNull("stored_type", 0)) = toUInt8(10)`,
		`toUInt8(ifNull("stored_type", 0)) = toUInt8(2)`,
		`ifNull("text_eligible", 0)`,
	} {
		if !strings.Contains(result.sql, required) {
			t.Fatalf("Dynamic mvexpand lost %q:\n%s", required, result.sql)
		}
	}
}

func TestPipelineMakeMVRejectsNonTextAndPublishesTypedProvenance(t *testing.T) {
	t.Parallel()

	input := fieldState{
		valueSQL:        `"value"`,
		existsSQL:       "1",
		kind:            fieldKindString,
		caseSensitive:   true,
		textEligibleSQL: `"text_eligible"`,
		storedTypeSQL:   `"stored_type"`,
	}
	result, next, _, err := compilePipelineMakeMVTypeFixture(t, input)
	if err != nil {
		t.Fatalf("compileMakeMultivalue: %v", err)
	}
	output := next.visible["value"]
	if output.kind != fieldKindStringArray || !output.caseSensitive ||
		output.optionalMultivaluePresentSQL == "" {
		t.Fatalf("makemv output metadata = %#v", output)
	}
	if output.textEligibleSQL != `arrayAll(member -> isValidUTF8(member), "value")` {
		t.Fatalf("makemv output text proof = %q", output.textEligibleSQL)
	}
	if output.storedTypeSQL != "" {
		t.Fatalf("makemv output should use fixed List-kind inference, got stored type %q", output.storedTypeSQL)
	}
	for _, required := range []string{
		`ifNull("text_eligible", 0)`,
		`toUInt8(ifNull("stored_type", 0)) = toUInt8(2)`,
		`isValidUTF8("__os_makemv_input_1")`,
		UnsupportedMakeMVValueMarker,
	} {
		if !strings.Contains(result.sql, required) {
			t.Fatalf("fixed String makemv lost %q:\n%s", required, result.sql)
		}
	}
}

func TestPipelineMVExpandPreservesIndexCaseSensitiveComparison(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | mvexpand index | where index="GRADETHIS" | table index`,
	)
	if !strings.Contains(compiled.SQL, `"index" = ?`) ||
		strings.Contains(compiled.SQL, `lowerUTF8(toString("index"))`) {
		t.Fatalf("mvexpand changed index comparison semantics:\n%s", compiled.SQL)
	}
}

func compilePipelineMVExpandTypeFixture(
	t *testing.T,
	input fieldState,
) (compiledRelation, compileState, []any, error) {
	t.Helper()

	sourceRange := pipelineCompilerRange()
	field := pipelineCompilerField(t, "value", sourceRange)
	state := compileState{
		visible:         map[string]fieldState{"value": input},
		context:         &compileContext{},
		publicOrder:     []string{"value"},
		blocked:         make(map[string]struct{}),
		blockedPrefixes: make(map[string]struct{}),
		order:           []compiledSortKey{{valueSQL: `"sequence"`}},
	}
	return compileExpandMultivalue(
		newScanRelation(`SELECT "value", "value_present", "sequence"`, sourceRange),
		&plan.ExpandMultivalue{
			Input: field, Limit: 1, QueryOrdinal: 1, Range: sourceRange,
		},
		state,
		1,
	)
}

func compilePipelineMakeMVTypeFixture(
	t *testing.T,
	input fieldState,
) (compiledRelation, compileState, []any, error) {
	t.Helper()

	sourceRange := pipelineCompilerRange()
	field := pipelineCompilerField(t, "value", sourceRange)
	state := compileState{
		visible:         map[string]fieldState{"value": input},
		context:         &compileContext{},
		publicOrder:     []string{"value"},
		blocked:         make(map[string]struct{}),
		blockedPrefixes: make(map[string]struct{}),
	}
	return compileMakeMultivalue(
		newScanRelation(`SELECT "value", "text_eligible", "stored_type"`, sourceRange),
		&plan.MakeMultivalue{
			Input: field, Delimiter: ",", Range: sourceRange,
		},
		state,
		1,
	)
}
