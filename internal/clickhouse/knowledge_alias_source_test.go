package clickhouse

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileKnowledgeAliasSourcePinsStoredPathAuthority(t *testing.T) {
	field := knowledgeAliasStoredSourceFieldForTest(t, "payload.nested")
	compiled, err := compileKnowledgeAliasSource(field)
	if err != nil {
		t.Fatalf("compileKnowledgeAliasSource: %v", err)
	}
	if err := validateCompiledKnowledgeAliasSource(compiled); err != nil {
		t.Fatalf("validateCompiledKnowledgeAliasSource: %v", err)
	}

	wantAuthority, err := mintStoredPathAuthority([]string{"payload", "nested"})
	if err != nil {
		t.Fatalf("mint expected stored path: %v", err)
	}
	if !field.storedPath.equal(wantAuthority) ||
		!compiled.proof.storedPath.equal(wantAuthority) {
		t.Fatalf(
			"stored path authority disagrees: field=%#v compiled=%#v want=%#v",
			field.storedPath,
			compiled.proof.storedPath,
			wantAuthority,
		)
	}
	if compiled.proof.sourceValue != wantAuthority.valueSQL() ||
		compiled.proof.exactPresence != knowledgeAliasSourceExactPresenceSQL() ||
		compiled.proof.descendants != knowledgeAliasSourceDescendantPresenceSQL() ||
		compiled.proof.metadataVersion != knowledgeAliasSourceMetadataVersionSQL() {
		t.Fatalf("retained source proof = %#v", compiled.proof)
	}

	exact := strings.Index(compiled.sql, "multiIf(has(")
	descendant := strings.Index(compiled.sql, "arrayExists(field_name -> startsWith")
	missing := strings.LastIndex(
		compiled.sql,
		"tuple(toUInt8(0), CAST(NULL AS Dynamic), toUInt8(0)",
	)
	if exact < 0 || descendant <= exact || missing <= descendant {
		t.Fatalf(
			"source branch order exact=%d descendant=%d missing=%d:\n%s",
			exact,
			descendant,
			missing,
			compiled.sql,
		)
	}
	for _, required := range []string{
		"tuple(toUInt8(1), CAST(" + wantAuthority.valueSQL() + " AS Dynamic)",
		"JSONExtract(ifNull(JSONExtractRaw(toJSONString(" + quoteIdentifier(internalFieldsColumn) + ")",
		"toUInt8(" + fmt.Sprint(eventfields.StoredValueTypeObject) + ")",
		"CAST([], 'Array(String)')",
		"CAST([], 'Array(UInt8)')",
		knowledgeAliasSourceMetadataVersionSQL(),
	} {
		if !strings.Contains(compiled.sql, required) {
			t.Fatalf("source descriptor omits %q:\n%s", required, compiled.sql)
		}
	}
	if got, want := compiled.args, knowledgeAliasSourceExpectedArgsForTest(wantAuthority); !knowledgeAliasSourceArgumentsEqual(got, want) {
		t.Fatalf("source arguments = %#v, want %#v", got, want)
	}
	if got := compiled.producedSQL("source_result"); got != "toUInt8(tupleElement(source_result, 1))" {
		t.Fatalf("produced accessor = %q", got)
	}
	if got := compiled.valueSQL("source_result"); got != "tupleElement(source_result, 2)" {
		t.Fatalf("value accessor = %q", got)
	}
	if got := compiled.storedTypeSQL("source_result"); got != "toUInt8(tupleElement(source_result, 3))" {
		t.Fatalf("stored-type accessor = %q", got)
	}
	if got := compiled.namesSQL("source_result"); got != "tupleElement(source_result, 4)" {
		t.Fatalf("relative-names accessor = %q", got)
	}
	if got := compiled.typesSQL("source_result"); got != "tupleElement(source_result, 5)" {
		t.Fatalf("relative-types accessor = %q", got)
	}
	if got := compiled.metadataVersionSQL("source_result"); got != "toUInt8(tupleElement(source_result, 6))" {
		t.Fatalf("metadata-version accessor = %q", got)
	}
}

func TestCompileKnowledgeAliasSourcePreservesContainerLeaves(t *testing.T) {
	field := knowledgeAliasStoredSourceFieldForTest(t, "payload")
	compiled, err := compileKnowledgeAliasSource(field)
	if err != nil {
		t.Fatalf("compileKnowledgeAliasSource: %v", err)
	}

	// Descendant presence comes only from field_names. This retains an explicit
	// null name even when ClickHouse's JSON parent value has no physical member.
	// The aligned UInt8 sidecar carries every stable leaf type without coercing
	// String, Bytes, numeric, temporal, list, object, or Decimal values.
	for _, required := range []string{
		"arrayMap(field_name -> substring(field_name, length(CAST(? AS String)) + 1)",
		"arrayFilter(field_name -> startsWith(field_name, CAST(? AS String)), " +
			quoteIdentifier(internalFieldNamesColumn) + ")",
		"arrayMap(field_index -> toUInt8(arrayElement(" +
			quoteIdentifier(internalFieldTypesColumn) + ", field_index))",
		"arrayEnumerate(" + quoteIdentifier(internalFieldNamesColumn) + ")",
		knowledgeAliasMaterializedDynamicSQL(
			quoteIdentifier(internalFieldsColumn),
			[]string{"CAST(? AS String)"},
		),
		"toUInt8(" + quoteIdentifier(internalFieldMetadataVersionColumn) + ")",
	} {
		if !strings.Contains(compiled.sql, required) {
			t.Fatalf("container source omits %q:\n%s", required, compiled.sql)
		}
	}
	if strings.Contains(compiled.sql, "arrayZip(") {
		t.Fatalf("relative names can be truncated by a zipped legacy type array:\n%s", compiled.sql)
	}
	if got := strings.Count(compiled.sql, "JSONExtract("); got != 1 {
		t.Fatalf("container materializers = %d, want one:\n%s", got, compiled.sql)
	}
	if strings.Contains(compiled.sql, "toString("+field.valueSQL+")") ||
		strings.Contains(compiled.sql, "isNotNull("+field.valueSQL+")") {
		t.Fatalf("container leaves are coerced or null-filtered:\n%s", compiled.sql)
	}

	alignedTypes := "length(" + quoteIdentifier(internalFieldTypesColumn) + ") = length(" +
		quoteIdentifier(internalFieldNamesColumn) + ")"
	if got := strings.Count(compiled.sql, alignedTypes); got < 2 {
		t.Fatalf(
			"aligned exact/descendant type branches = %d, want both:\n%s",
			got,
			compiled.sql,
		)
	}
	if !strings.Contains(
		compiled.sql,
		"), CAST([], 'Array(UInt8)'))",
	) {
		t.Fatalf("legacy descendants do not retain names with an explicit empty type sidecar:\n%s", compiled.sql)
	}
	// A legacy exact leaf still has deterministic scalar type classification;
	// it must not silently receive the invalid type code zero.
	for _, fallback := range []string{
		"dynamicType(" + field.valueSQL + ")",
		"bytes/v1",
		"timestamp/v1",
		"duration/v1",
		"decimal/v1",
	} {
		if !strings.Contains(compiled.sql, fallback) {
			t.Fatalf("legacy exact type fallback omits %q:\n%s", fallback, compiled.sql)
		}
	}
	if compiled.namesSQL("container") == compiled.typesSQL("container") ||
		compiled.typesSQL("container") == compiled.metadataVersionSQL("container") {
		t.Fatal("container names, types, and metadata version share one tuple element")
	}
	if !strings.Contains(compiled.sql, knowledgeAliasSourceMetadataVersionSQL()) {
		t.Fatal("unknown future metadata versions cannot be validated independently")
	}
}

func TestCompileKnowledgeAliasSourceEscapedPaths(t *testing.T) {
	literalSegments := []string{"a.b", "percent%2Ekey", `slash\key`}
	literalName := eventfields.NormalizeDynamicPath(literalSegments)
	literalField := knowledgeAliasStoredSourceFieldForTest(t, literalName)
	literal, err := compileKnowledgeAliasSource(literalField)
	if err != nil {
		t.Fatalf("compile literal escaped path: %v", err)
	}
	wantPhysical := []string{"a%2Eb", "percent%252Ekey", `slash\key`}
	if !slices.Equal(literal.proof.storedPath.logicalSegments, literalSegments) ||
		!slices.Equal(literal.proof.storedPath.physicalSegments, wantPhysical) ||
		literal.proof.storedPath.normalizedExactPath != literalName ||
		literal.proof.storedPath.normalizedDescendantPrefix != literalName+"." {
		t.Fatalf("escaped stored path proof = %#v", literal.proof.storedPath)
	}
	segments := make([]string, len(wantPhysical))
	for index := range segments {
		segments[index] = "CAST(? AS String)"
	}
	wantMaterializer := knowledgeAliasMaterializedDynamicSQL(
		quoteIdentifier(internalFieldsColumn),
		segments,
	)
	if got := strings.Count(literal.sql, wantMaterializer); got != 1 {
		t.Fatalf(
			"segment-wise JSON materializers = %d, want one %q:\n%s",
			got,
			wantMaterializer,
			literal.sql,
		)
	}
	if got := literal.args[3 : 3+len(wantPhysical)]; !slices.Equal(
		got,
		[]any{wantPhysical[0], wantPhysical[1], wantPhysical[2]},
	) {
		t.Fatalf("segment-wise JSON path arguments = %#v", got)
	}

	nested := knowledgeAliasStoredSourceFieldForTest(t, "a.b")
	literalDot := knowledgeAliasStoredSourceFieldForTest(
		t,
		eventfields.NormalizeDynamicPath([]string{"a.b"}),
	)
	if nested.storedPath.equal(literalDot.storedPath) ||
		nested.storedPath.valueSQL() == literalDot.storedPath.valueSQL() {
		t.Fatalf(
			"nested and literal-dot paths collapsed: nested=%#v literal=%#v",
			nested.storedPath,
			literalDot.storedPath,
		)
	}
	if !slices.Equal(nested.storedPath.physicalSegments, []string{"a", "b"}) ||
		!slices.Equal(literalDot.storedPath.physicalSegments, []string{"a%2Eb"}) {
		t.Fatalf(
			"nested/literal physical segments = %#v / %#v",
			nested.storedPath.physicalSegments,
			literalDot.storedPath.physicalSegments,
		)
	}

	short := knowledgeAliasStoredSourceFieldForTest(t, "a")
	sibling := knowledgeAliasStoredSourceFieldForTest(t, "ab")
	if short.storedPath.normalizedDescendantPrefix != "a." ||
		sibling.storedPath.normalizedDescendantPrefix != "ab." ||
		short.storedPath.normalizedDescendantPrefix ==
			sibling.storedPath.normalizedDescendantPrefix {
		t.Fatalf(
			"boundary-safe descendant prefixes = %q / %q",
			short.storedPath.normalizedDescendantPrefix,
			sibling.storedPath.normalizedDescendantPrefix,
		)
	}
}

func TestCompileKnowledgeAliasSourceRejectsForgeryAndDetaches(t *testing.T) {
	field := knowledgeAliasStoredSourceFieldForTest(t, "payload.child")
	compiled, err := compileKnowledgeAliasSource(field)
	if err != nil {
		t.Fatalf("compileKnowledgeAliasSource: %v", err)
	}

	fieldForgeries := []struct {
		name   string
		mutate func(*fieldState)
	}{
		{name: "kind", mutate: func(got *fieldState) { got.kind = fieldKindString }},
		{name: "value", mutate: func(got *fieldState) { got.valueSQL += "_forged" }},
		{name: "dynamic type", mutate: func(got *fieldState) { got.dynamicTypeSQL = "Dynamic" }},
		{name: "stored type", mutate: func(got *fieldState) { got.storedTypeSQL = "2" }},
		{name: "exact predicate", mutate: func(got *fieldState) { got.existsSQL = "1" }},
		{name: "exact argument", mutate: func(got *fieldState) { got.existsArgs[0] = "forged" }},
		{name: "descendant predicate", mutate: func(got *fieldState) { got.descendantSQL = "1" }},
		{name: "descendant argument", mutate: func(got *fieldState) { got.descendantArgs[0] = "forged." }},
		{name: "logical authority", mutate: func(got *fieldState) { got.storedPath.logicalSegments[0] = "forged" }},
		{name: "exact authority", mutate: func(got *fieldState) { got.storedPath.normalizedExactPath += "x" }},
		{name: "prefix authority", mutate: func(got *fieldState) { got.storedPath.normalizedDescendantPrefix += "x" }},
		{name: "physical authority", mutate: func(got *fieldState) { got.storedPath.physicalSegments[0] = "forged" }},
	}
	for _, test := range fieldForgeries {
		t.Run("field "+test.name, func(t *testing.T) {
			forged := cloneKnowledgeAliasSourceFieldForTest(field)
			test.mutate(&forged)
			if _, err := compileKnowledgeAliasSource(forged); err == nil {
				t.Fatal("forged source field unexpectedly compiled")
			}
		})
	}

	descriptorForgeries := []struct {
		name   string
		mutate func(*compiledKnowledgeAliasSource)
	}{
		{name: "logical authority", mutate: func(got *compiledKnowledgeAliasSource) { got.proof.storedPath.logicalSegments[0] = "forged" }},
		{name: "physical authority", mutate: func(got *compiledKnowledgeAliasSource) { got.proof.storedPath.physicalSegments[0] = "forged" }},
		{name: "source value", mutate: func(got *compiledKnowledgeAliasSource) { got.proof.sourceValue += "x" }},
		{name: "exact predicate", mutate: func(got *compiledKnowledgeAliasSource) { got.proof.exactPresence = "1" }},
		{name: "descendant predicate", mutate: func(got *compiledKnowledgeAliasSource) { got.proof.descendants = "1" }},
		{name: "metadata version", mutate: func(got *compiledKnowledgeAliasSource) { got.proof.metadataVersion = "0" }},
		{name: "SQL", mutate: func(got *compiledKnowledgeAliasSource) { got.sql += " " }},
		{name: "argument", mutate: func(got *compiledKnowledgeAliasSource) { got.args[0] = "forged" }},
		{name: "argument type", mutate: func(got *compiledKnowledgeAliasSource) { got.args[0] = uint8(1) }},
		{name: "argument count", mutate: func(got *compiledKnowledgeAliasSource) { got.args = got.args[:len(got.args)-1] }},
	}
	for _, test := range descriptorForgeries {
		t.Run("descriptor "+test.name, func(t *testing.T) {
			forged := cloneCompiledKnowledgeAliasSourceForTest(compiled)
			test.mutate(&forged)
			if err := validateCompiledKnowledgeAliasSource(forged); err == nil {
				t.Fatal("forged source descriptor unexpectedly validated")
			}
		})
	}

	logical := []string{"detached", "source"}
	authority, err := mintStoredPathAuthority(logical)
	if err != nil {
		t.Fatalf("mint detached authority: %v", err)
	}
	logical[0] = "caller-mutated"
	if authority.logicalSegments[0] != "detached" ||
		validateStoredPathAuthority(authority) != nil {
		t.Fatalf("minted authority aliases caller input: %#v", authority)
	}
	clonedAuthority := authority.clone()
	clonedAuthority.logicalSegments[0] = "clone-mutated"
	clonedAuthority.physicalSegments[0] = "clone-mutated"
	if authority.logicalSegments[0] != "detached" ||
		authority.physicalSegments[0] != "detached" {
		t.Fatalf("stored authority clone aliases its source: %#v", authority)
	}

	callerField := cloneKnowledgeAliasSourceFieldForTest(field)
	callerField.storedPath.logicalSegments[0] = "caller-mutated"
	callerField.storedPath.physicalSegments[0] = "caller-mutated"
	callerField.existsArgs[0] = "caller-mutated"
	callerField.descendantArgs[0] = "caller-mutated."
	if err := validateCompiledKnowledgeAliasSource(compiled); err != nil {
		t.Fatalf("compiled descriptor aliases caller field: %v", err)
	}

	for _, test := range []struct {
		name string
		copy func(compileState) (compileState, error)
	}{
		{
			name: "cloneCompileState",
			copy: func(state compileState) (compileState, error) {
				return cloneCompileState(state), nil
			},
		},
		{
			name: "extendCompileState",
			copy: func(state compileState) (compileState, error) {
				return extendCompileState(
					state,
					plan.FieldRef{Name: "derived"},
					compiledScalar{
						valueSQL: "toInt64(1)", kind: fieldKindNumber,
						numberType: "Int64",
					},
					false,
				)
			},
		},
		{
			name: "compileWindow",
			copy: func(state compileState) (compileState, error) {
				_, next, err := compileWindow(&plan.Window{
					Function: plan.WindowFunctionPercentOfTotal,
					Input:    plan.FieldRef{Name: "metric"},
					Output:   "percent",
				}, state)
				return next, err
			},
		},
	} {
		t.Run(test.name+" detaches stored paths", func(t *testing.T) {
			raw := knowledgeAliasStoredSourceFieldForTest(t, "raw_payload")
			state := compileState{
				visible: map[string]fieldState{
					"metric": {
						valueSQL: "metric", existsSQL: "1", kind: fieldKindNumber,
						numberType: "UInt64",
					},
					"raw_payload": raw,
				},
				publicOrder:     []string{"metric", "raw_payload"},
				blocked:         make(map[string]struct{}),
				blockedPrefixes: make(map[string]struct{}),
			}
			copied, err := test.copy(state)
			if err != nil {
				t.Fatalf("copy compile state: %v", err)
			}
			copiedRaw := copied.visible["raw_payload"]
			copiedRaw.storedPath.logicalSegments[0] = "copy-mutated"
			copiedRaw.storedPath.physicalSegments[0] = "copy-mutated"
			copied.visible["raw_payload"] = copiedRaw
			original := state.visible["raw_payload"].storedPath
			if original.logicalSegments[0] != "raw_payload" ||
				original.physicalSegments[0] != "raw_payload" {
				t.Fatalf("copied state aliases source stored path: %#v", original)
			}
		})
	}
}

func TestCompileKnowledgeAliasSourceBoundsArgumentsAndSQL(t *testing.T) {
	maximumName := strings.Repeat("z", knowledge.MaximumFieldDestinationBytes)
	if _, err := knowledgedefinition.Normalize(
		knowledgeAliasSourceDefinitionForTest(maximumName),
	); err != nil {
		t.Fatalf("normalize maximum alias source: %v", err)
	}
	if _, err := knowledgedefinition.Normalize(
		knowledgeAliasSourceDefinitionForTest(maximumName + "z"),
	); err == nil {
		t.Fatal("alias source above 255 bytes unexpectedly normalized")
	}

	maximumSegments := make([]string, eventfields.MaximumDynamicPathSegments+1)
	for index := range maximumSegments {
		maximumSegments[index] = fmt.Sprintf("s%02d", index)
	}
	maximumPath := eventfields.NormalizeDynamicPath(maximumSegments)
	if _, err := knowledgedefinition.Normalize(
		knowledgeAliasSourceDefinitionForTest(maximumPath),
	); err != nil {
		t.Fatalf("normalize 17-segment alias source: %v", err)
	}
	tooDeep := append(slices.Clone(maximumSegments), "overflow")
	if _, err := knowledgedefinition.Normalize(
		knowledgeAliasSourceDefinitionForTest(eventfields.NormalizeDynamicPath(tooDeep)),
	); err == nil {
		t.Fatal("18-segment alias source unexpectedly normalized")
	}
	if _, err := mintStoredPathAuthority(tooDeep); err == nil {
		t.Fatal("18-segment stored authority unexpectedly minted")
	}

	// Storage permits a 256-byte segment, while the knowledge definition
	// independently caps its complete source spelling at 255 bytes.
	if _, err := mintStoredPathAuthority([]string{
		strings.Repeat("p", eventfields.MaximumDynamicPathSegmentBytes),
	}); err != nil {
		t.Fatalf("mint maximum storage segment: %v", err)
	}

	for _, name := range []string{maximumName, maximumPath} {
		field := knowledgeAliasStoredSourceFieldForTest(t, name)
		compiled, err := compileKnowledgeAliasSource(field)
		if err != nil {
			t.Fatalf("compile boundary source %q: %v", name, err)
		}
		if len(compiled.sql) == 0 ||
			len(compiled.sql) > maxCompiledKnowledgeAliasSourceSQLBytes {
			t.Fatalf(
				"boundary source SQL bytes = %d, limit %d",
				len(compiled.sql),
				maxCompiledKnowledgeAliasSourceSQLBytes,
			)
		}
		if got := strings.Count(compiled.sql, "?"); got != len(compiled.args) {
			t.Fatalf("boundary placeholders = %d, args = %d", got, len(compiled.args))
		}
		wantArgs := knowledgeAliasSourceExpectedArgsForTest(field.storedPath)
		if !knowledgeAliasSourceArgumentsEqual(compiled.args, wantArgs) {
			t.Fatalf("boundary args = %#v, want %#v", compiled.args, wantArgs)
		}
		if len(compiled.args) != len(field.storedPath.physicalSegments)+6 {
			t.Fatalf(
				"boundary args = %d, want %d path-plus-fixed arguments",
				len(compiled.args),
				len(field.storedPath.physicalSegments)+6,
			)
		}
		for index, argument := range compiled.args {
			if _, ok := argument.(string); !ok {
				t.Fatalf("boundary argument %d = %#v (%T), want detached String", index, argument, argument)
			}
		}
		segments := make([]string, len(field.storedPath.physicalSegments))
		for index := range segments {
			segments[index] = "CAST(? AS String)"
		}
		wantMaterializer := knowledgeAliasMaterializedDynamicSQL(
			quoteIdentifier(internalFieldsColumn),
			segments,
		)
		if got := strings.Count(compiled.sql, wantMaterializer); got != 1 {
			t.Fatalf(
				"boundary JSON materializers = %d, want one %q:\n%s",
				got,
				wantMaterializer,
				compiled.sql,
			)
		}
		if err := validateCompiledKnowledgeAliasSource(compiled); err != nil {
			t.Fatalf("validate boundary source: %v", err)
		}
	}
}

func knowledgeAliasStoredSourceFieldForTest(t *testing.T, name string) fieldState {
	t.Helper()
	field, err := plan.ResolveField(name, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(%q): %v", name, err)
	}
	state := compileState{
		visible:         make(map[string]fieldState),
		allowDynamic:    true,
		blocked:         make(map[string]struct{}),
		blockedPrefixes: make(map[string]struct{}),
	}
	resolved, present, err := resolveCompiledField(field, state)
	if err != nil {
		t.Fatalf("resolveCompiledField(%q): %v", name, err)
	}
	if !present {
		t.Fatalf("stored source %q did not resolve", name)
	}
	return resolved
}

func knowledgeAliasSourceExpectedArgsForTest(
	authority storedPathAuthority,
) []any {
	result := make([]any, 0, len(authority.physicalSegments)+6)
	result = append(result,
		authority.normalizedExactPath,
		authority.normalizedExactPath,
		authority.normalizedDescendantPrefix,
	)
	for _, segment := range authority.physicalSegments {
		result = append(result, segment)
	}
	return append(result,
		authority.normalizedDescendantPrefix,
		authority.normalizedDescendantPrefix,
		authority.normalizedDescendantPrefix,
	)
}

func cloneKnowledgeAliasSourceFieldForTest(field fieldState) fieldState {
	result := field
	result.existsArgs = append([]any(nil), field.existsArgs...)
	result.descendantArgs = append([]any(nil), field.descendantArgs...)
	result.storedPath = field.storedPath.clone()
	return result
}

func cloneCompiledKnowledgeAliasSourceForTest(
	compiled compiledKnowledgeAliasSource,
) compiledKnowledgeAliasSource {
	result := compiled
	result.args = append([]any(nil), compiled.args...)
	result.proof.storedPath = compiled.proof.storedPath.clone()
	return result
}

func knowledgeAliasSourceDefinitionForTest(
	source string,
) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        "app",
		Name:         "alias-source-boundary",
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField:       source,
				DestinationField:  "copied_source",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			},
		},
	}
}
