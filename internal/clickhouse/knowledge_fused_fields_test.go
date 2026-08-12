package clickhouse

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

func TestCompileKnowledgeAliasStageBuildsOneFrozenProjection(t *testing.T) {
	program := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "app", Name: "alias-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector: &opensplunkv1.KnowledgeSelector{HostPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "api"}}},
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: "host", DestinationField: "alias_host",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			}},
		},
		{
			AppId: "app", Name: "alias-b", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector: &opensplunkv1.KnowledgeSelector{SourcePatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "service-*"}}},
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: "source", DestinationField: "alias_source",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			}},
		},
	})
	state := knowledgeFusedFieldState()
	compiled, err := compileKnowledgeAliasStage(
		state,
		program.Aliases(),
		7,
		compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compileKnowledgeAliasStage: %v", err)
	}
	if compiled.emittedAssignments != 2 || len(compiled.arrayJoinBindings) != 2 {
		t.Fatalf("assignment emission = %d / %d", compiled.emittedAssignments, len(compiled.arrayJoinBindings))
	}
	if strings.Contains(strings.Join(compiled.projection, ", "), "SELECT ") ||
		strings.Contains(strings.Join(compiled.bindingProjection, ", "), "SELECT ") {
		t.Fatalf("projection helper opened a relation: %s", strings.Join(compiled.projection, ", "))
	}
	if strings.Contains(strings.Join(compiled.bindingProjection, ", "), `AS "alias_host"`) {
		t.Fatalf("binding layer publishes a destination alias: %s", strings.Join(compiled.bindingProjection, ", "))
	}
	for _, required := range []string{
		`"__os_ko_field_binding_7_0"`,
		`"__os_ko_field_binding_7_1"`,
		`"__os_ko_selector_input_bytes_7"`,
		`"__os_ko_selector_query_units_7"`,
		`"__os_ko_alias_copy_bytes_7"`,
		`"__os_ko_alias_copy_units_7"`,
		`__os_ko_field_previous_present`,
	} {
		combined := strings.Join(append(slices.Clone(compiled.projection), compiled.arrayJoinBindings...), " ")
		if !strings.Contains(combined, required) {
			t.Fatalf("fused projection omits %q:\n%s", required, combined)
		}
	}
	if _, ok := compiled.state.visible["alias_host"]; !ok {
		t.Fatal("alias_host is absent from successor state")
	}
	if got := compiled.state.visible["alias_host"]; got.kind != fieldKindDynamic ||
		!got.materializeForPredicate || got.existsSQL == "" || got.storedTypeSQL == "" {
		t.Fatalf("alias_host state = %#v", got)
	}
	if slices.Contains(compiled.state.privateColumns, compiled.selectorCharges.inputBytes) ||
		slices.Contains(compiled.state.privateColumns, compiled.selectorCharges.queryUnits) ||
		slices.Contains(compiled.state.privateColumns, compiled.aliasCopyCharges.eventBytes) ||
		slices.Contains(compiled.state.privateColumns, compiled.aliasCopyCharges.queryUnits) {
		t.Fatalf("runtime charges leaked into generic private columns: %#v", compiled.state.privateColumns)
	}
	projectionSQL := strings.Join(compiled.projection, " ")
	bindingSQL := strings.Join(compiled.arrayJoinBindings, " ")
	for _, fragment := range []string{
		"byteSize(",
		`__os_ko_field_source`,
		`__os_ko_field_payload_bytes`,
	} {
		if !strings.Contains(bindingSQL, fragment) {
			t.Fatalf("alias-copy binding omits %q:\n%s", fragment, bindingSQL)
		}
	}
	for _, fragment := range []string{
		"least(",
		fmt.Sprintf("toUInt128(%d)) AS %s", knowledge.MaximumAliasCopyRuntimeEventBytes+1, compiled.aliasCopyCharges.eventBytes),
		fmt.Sprintf("toUInt128(%d)) AS %s", knowledge.MaximumAliasCopyRuntimeQueryUnits+1, compiled.aliasCopyCharges.queryUnits),
	} {
		if !strings.Contains(projectionSQL, fragment) {
			t.Fatalf("alias-copy projection omits %q:\n%s", fragment, projectionSQL)
		}
	}
	firstSelector := slices.IndexFunc(compiled.suffixArgs, func(value any) bool {
		patterns, ok := value.([]string)
		return ok && slices.Equal(patterns, []string{"api"})
	})
	secondSelector := slices.IndexFunc(compiled.suffixArgs, func(value any) bool {
		pattern, ok := value.(string)
		return ok && strings.Contains(pattern, "service-")
	})
	if firstSelector < 0 || secondSelector <= firstSelector {
		t.Fatalf("selector argument order = %#v", compiled.suffixArgs)
	}
	if got := strings.Count(strings.Join(compiled.arrayJoinBindings, " "), "?"); got != len(compiled.suffixArgs) {
		t.Fatalf("placeholders = %d, args = %d", got, len(compiled.suffixArgs))
	}
	inputSQL := "SELECT ? AS host"
	composedSQL := "SELECT " + strings.Join(compiled.projection, ", ") +
		" FROM (SELECT " + strings.Join(compiled.bindingProjection, ", ") +
		" FROM (" + inputSQL + ") ARRAY JOIN " +
		strings.Join(compiled.arrayJoinBindings, ", ") + ")"
	composedArgs := append([]any{"input-first"}, compiled.suffixArgs...)
	if got := strings.Count(composedSQL, "?"); got != len(composedArgs) {
		t.Fatalf("composed placeholders = %d, args = %d", got, len(composedArgs))
	}
	if composedArgs[0] != "input-first" {
		t.Fatalf("stage arguments preceded the input relation: %#v", composedArgs)
	}
}

func TestCompileKnowledgeCalculatedStageUsesFrozenInputAndExactConversion(t *testing.T) {
	program := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "app", Name: "calculated-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
				DestinationField: "calculated_host", Expression: "lower(host)",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			}},
		},
		{
			AppId: "app", Name: "calculated-b", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
				DestinationField: "calculated_source", Expression: "upper(source)",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			}},
		},
	})
	state := knowledgeFusedFieldState()
	assignments := program.CalculatedFields()
	second, err := compileKnowledgeCalculatedAssignment(assignments[1], state)
	if err != nil {
		t.Fatalf("compile second assignment: %v", err)
	}
	missingPrevious, err := compileKnowledgeFieldSourceFromField(fieldState{}, false)
	if err != nil {
		t.Fatalf("compile missing previous: %v", err)
	}
	secondCandidate, err := compileKnowledgeFieldCandidate(second, missingPrevious)
	if err != nil {
		t.Fatalf("compile second candidate: %v", err)
	}
	if strings.Contains(secondCandidate.sql, "calculated_host") {
		t.Fatalf("second assignment observed same-stage output:\n%s", secondCandidate.sql)
	}
	compiled, err := compileKnowledgeCalculatedStage(
		state,
		assignments,
		11,
		compiledKnowledgeSelectorChargeColumns{},
		compiledKnowledgeAliasCopyChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compileKnowledgeCalculatedStage: %v", err)
	}
	if compiled.emittedAssignments != uint32(len(assignments)) ||
		len(compiled.arrayJoinBindings) != len(assignments) {
		t.Fatalf("calculated emission = %#v", compiled)
	}
	combined := strings.Join(append(slices.Clone(compiled.projection), compiled.arrayJoinBindings...), " ")
	for _, required := range []string{
		`CAST(lowerUTF8("host") AS Dynamic)`, `CAST(upperUTF8("source") AS Dynamic)`,
		`"__os_ko_field_exists_11_0"`, `"__os_ko_field_exists_11_1"`,
	} {
		if !strings.Contains(combined, required) {
			t.Fatalf("calculated projection omits %q:\n%s", required, combined)
		}
	}
}

func TestCompileKnowledgeAliasStageGroupsDisjointDestinationWriters(t *testing.T) {
	program := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "app", Name: "alias-east", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector: &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "east"}}},
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: "host", DestinationField: "shared_destination",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			}},
		},
		{
			AppId: "app", Name: "alias-west", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector: &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "west"}}},
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: "source", DestinationField: "shared_destination",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			}},
		},
	})
	compiled, err := compileKnowledgeAliasStage(
		knowledgeFusedFieldState(),
		program.Aliases(),
		4,
		compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compileKnowledgeAliasStage: %v", err)
	}
	if compiled.emittedAssignments != 2 || len(compiled.arrayJoinBindings) != 1 {
		t.Fatalf("emission = %d assignments / %d bindings", compiled.emittedAssignments, len(compiled.arrayJoinBindings))
	}
	joined := strings.Join(compiled.projection, " ")
	if got := strings.Count(joined, ` AS "shared_destination"`); got != 1 {
		t.Fatalf("destination projections = %d, want one:\n%s", got, joined)
	}
	if !strings.Contains(joined, `"__os_ko_field_binding_4_0"`) ||
		strings.Contains(joined, `"__os_ko_field_binding_4_1"`) {
		t.Fatalf("grouped merge did not publish one destination binding:\n%s", joined)
	}
	bindings := strings.Join(compiled.arrayJoinBindings, " ")
	if strings.Count(bindings, `__os_ko_field_selector`) < 2 {
		t.Fatalf("grouped merge does not evaluate both writers:\n%s", bindings)
	}
	if got := strings.Count(bindings, "byteSize("); got != 3 {
		t.Fatalf("grouped destination charge evaluated %d byte-size terms, want 3 for one compact winner:\n%s", got, bindings)
	}
	if strings.Contains(joined, "byteSize(") ||
		strings.Contains(joined, `tupleElement(tupleElement("__os_ko_field_binding_4_0"`) {
		t.Fatalf("outer projection re-expands a compact destination binding:\n%s", joined)
	}
	wrote := `if(__os_ko_field_wrote != 0, `
	if got := strings.Count(bindings, wrote); got != 3 {
		t.Fatalf("winner selection plus payload/work guards = %d, want 3:\n%s", got, bindings)
	}
}

func TestCompileKnowledgeAliasStageAcceptsProgramMaximumBeyondAuthoredWidth(t *testing.T) {
	definitions := make([]*opensplunkv1.KnowledgeObjectDefinition, 65)
	for index := range definitions {
		name := fmt.Sprintf("alias-%03d", index)
		definitions[index] = &opensplunkv1.KnowledgeObjectDefinition{
			AppId: "app", Name: name, SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: "host", DestinationField: fmt.Sprintf("destination_%03d", index),
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			}},
		}
	}
	program := knowledgeFusedFieldProgram(t, definitions)
	compiled, err := compileKnowledgeAliasStage(
		knowledgeFusedFieldState(),
		program.Aliases(),
		5,
		compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compile 65 aliases: %v", err)
	}
	if compiled.emittedAssignments != 65 {
		t.Fatalf("emitted assignments = %d, want 65", compiled.emittedAssignments)
	}
}

func TestCompileKnowledgeFusedStagesCarryCumulativeSelectorCharges(t *testing.T) {
	aliasProgram := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{{
		AppId: "app", Name: "alias", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
			SourceField: "host", DestinationField: "copied_host",
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		}},
	}})
	first, err := compileKnowledgeAliasStage(
		knowledgeFusedFieldState(),
		aliasProgram.Aliases(),
		8,
		compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compile alias stage: %v", err)
	}
	calculatedProgram := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{{
		AppId: "app", Name: "calculated", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			DestinationField: "lower_host", Expression: "lower(host)",
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		}},
	}})
	second, err := compileKnowledgeCalculatedStage(
		first.state,
		calculatedProgram.CalculatedFields(),
		9,
		first.selectorCharges,
		first.aliasCopyCharges,
	)
	if err != nil {
		t.Fatalf("compile calculated stage: %v", err)
	}
	projection := strings.Join(second.projection, " ")
	if !strings.Contains(projection, "toUInt128("+first.selectorCharges.inputBytes+")") ||
		!strings.Contains(projection, "toUInt128("+first.selectorCharges.queryUnits+")") ||
		!strings.Contains(projection, "toUInt128("+first.aliasCopyCharges.eventBytes+") AS "+second.aliasCopyCharges.eventBytes) ||
		!strings.Contains(projection, "toUInt128("+first.aliasCopyCharges.queryUnits+") AS "+second.aliasCopyCharges.queryUnits) {
		t.Fatalf("second stage dropped prior runtime charges:\n%s", projection)
	}
	bindingProjection := strings.Join(second.bindingProjection, " ")
	if !strings.Contains(bindingProjection, first.selectorCharges.inputBytes) ||
		!strings.Contains(bindingProjection, first.selectorCharges.queryUnits) ||
		!strings.Contains(bindingProjection, first.aliasCopyCharges.eventBytes) ||
		!strings.Contains(bindingProjection, first.aliasCopyCharges.queryUnits) {
		t.Fatalf("second binding layer dropped prior runtime columns:\n%s", bindingProjection)
	}
	if slices.Contains(second.state.privateColumns, first.selectorCharges.inputBytes) ||
		slices.Contains(second.state.privateColumns, first.selectorCharges.queryUnits) {
		t.Fatalf("second stage retained stale charge columns: %#v", second.state.privateColumns)
	}
}

func TestCompileKnowledgeFieldAssignmentKeepsPresentNullSeparate(t *testing.T) {
	program := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{{
		AppId: "app", Name: "calculated-null", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			DestinationField: "calculated_null", Expression: "null",
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		}},
	}})
	compiled, err := compileKnowledgeCalculatedAssignment(
		program.CalculatedFields()[0],
		knowledgeFusedFieldState(),
	)
	if err != nil {
		t.Fatalf("compileKnowledgeCalculatedAssignment: %v", err)
	}
	missingPrevious, err := compileKnowledgeFieldSourceFromField(fieldState{}, false)
	if err != nil {
		t.Fatalf("compile missing previous: %v", err)
	}
	candidate, err := compileKnowledgeFieldCandidate(compiled, missingPrevious)
	if err != nil {
		t.Fatalf("compile present-null candidate: %v", err)
	}
	if !strings.Contains(candidate.sql, "CAST(NULL AS Dynamic)") {
		t.Fatalf("present-null tuple is not explicit:\n%s", candidate.sql)
	}
	if compiled.producedSQL("result") ==
		compiled.source.valueSQL(compiled.sourceResultSQL("result")) {
		t.Fatal("produced presence and Dynamic value use the same tuple element")
	}
}

func TestCompileKnowledgeFieldAssignmentsRetainTypedLoweringProof(t *testing.T) {
	aliasProgram := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{{
		AppId: "app", Name: "alias-proof", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
			SourceField: "host", DestinationField: "alias_host",
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		}},
	}})
	aliasOperation := aliasProgram.Aliases()[0]
	aliasAssignment, err := compileKnowledgeAliasAssignment(aliasOperation, knowledgeFusedFieldState())
	if err != nil {
		t.Fatalf("compile alias assignment: %v", err)
	}
	if aliasAssignment.alias.Origin() != aliasOperation.Origin() ||
		aliasAssignment.alias.Source() != aliasOperation.Source() ||
		aliasAssignment.alias.Destination() != aliasOperation.Destination() ||
		aliasAssignment.calculated.Origin() != (knowledgeprogram.Origin{}) {
		t.Fatalf("alias lowering proof = %#v", aliasAssignment)
	}
	aliasStage, err := compileKnowledgeFusedFieldProjection(
		knowledgeFusedFieldState(),
		[]compiledKnowledgeFieldAssignment{aliasAssignment},
		0,
		"alias",
		compiledKnowledgeSelectorChargeColumns{},
		compiledKnowledgeAliasCopyChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compile alias projection: %v", err)
	}
	if len(aliasStage.aliases) != 1 || aliasStage.aliases[0].Origin() != aliasOperation.Origin() ||
		len(aliasStage.calculated) != 0 {
		t.Fatalf("alias stage lowering proof = %#v", aliasStage)
	}
	otherAliasProgram := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{{
		AppId: "app", Name: "other-alias-proof", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
			SourceField: "source", DestinationField: "other_alias",
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		}},
	}})
	otherAliasAssignment, err := compileKnowledgeAliasAssignment(
		otherAliasProgram.Aliases()[0],
		knowledgeFusedFieldState(),
	)
	if err != nil {
		t.Fatalf("compile other alias assignment: %v", err)
	}
	forgedAliasAssignment := aliasAssignment
	forgedAliasAssignment.alias = otherAliasAssignment.alias
	if _, err := compileKnowledgeFusedFieldProjection(
		knowledgeFusedFieldState(),
		[]compiledKnowledgeFieldAssignment{forgedAliasAssignment},
		0,
		"alias",
		compiledKnowledgeSelectorChargeColumns{},
		compiledKnowledgeAliasCopyChargeColumns{},
	); err == nil {
		t.Fatal("alias projection accepted mismatched retained authority")
	}
	aliasAssignment.alias = knowledgeprogram.Alias{}
	if _, err := compileKnowledgeFusedFieldProjection(
		knowledgeFusedFieldState(),
		[]compiledKnowledgeFieldAssignment{aliasAssignment},
		0,
		"alias",
		compiledKnowledgeSelectorChargeColumns{},
		compiledKnowledgeAliasCopyChargeColumns{},
	); err == nil {
		t.Fatal("alias projection accepted an assignment without lowerer-retained authority")
	}

	calculatedProgram := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{{
		AppId: "app", Name: "calculated-proof", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			DestinationField: "lower_host", Expression: "lower(host)",
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		}},
	}})
	calculatedOperation := calculatedProgram.CalculatedFields()[0]
	calculatedAssignment, err := compileKnowledgeCalculatedAssignment(
		calculatedOperation,
		knowledgeFusedFieldState(),
	)
	if err != nil {
		t.Fatalf("compile calculated assignment: %v", err)
	}
	if calculatedAssignment.calculated.Origin() != calculatedOperation.Origin() ||
		calculatedAssignment.calculated.Expression() != calculatedOperation.Expression() ||
		calculatedAssignment.calculated.Destination() != calculatedOperation.Destination() ||
		calculatedAssignment.alias.Origin() != (knowledgeprogram.Origin{}) {
		t.Fatalf("calculated lowering proof = %#v", calculatedAssignment)
	}
	calculatedStage, err := compileKnowledgeFusedFieldProjection(
		knowledgeFusedFieldState(),
		[]compiledKnowledgeFieldAssignment{calculatedAssignment},
		0,
		"calculated",
		compiledKnowledgeSelectorChargeColumns{},
		compiledKnowledgeAliasCopyChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compile calculated projection: %v", err)
	}
	if len(calculatedStage.calculated) != 1 ||
		calculatedStage.calculated[0].Origin() != calculatedOperation.Origin() ||
		len(calculatedStage.aliases) != 0 {
		t.Fatalf("calculated stage lowering proof = %#v", calculatedStage)
	}
	otherCalculatedProgram := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{{
		AppId: "app", Name: "other-calculated-proof", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			DestinationField: "upper_source", Expression: "upper(source)",
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		}},
	}})
	otherCalculatedAssignment, err := compileKnowledgeCalculatedAssignment(
		otherCalculatedProgram.CalculatedFields()[0],
		knowledgeFusedFieldState(),
	)
	if err != nil {
		t.Fatalf("compile other calculated assignment: %v", err)
	}
	forgedCalculatedAssignment := calculatedAssignment
	forgedCalculatedAssignment.calculated = otherCalculatedAssignment.calculated
	if _, err := compileKnowledgeFusedFieldProjection(
		knowledgeFusedFieldState(),
		[]compiledKnowledgeFieldAssignment{forgedCalculatedAssignment},
		0,
		"calculated",
		compiledKnowledgeSelectorChargeColumns{},
		compiledKnowledgeAliasCopyChargeColumns{},
	); err == nil {
		t.Fatal("calculated projection accepted mismatched retained authority")
	}
	calculatedAssignment.calculated = knowledgeprogram.Calculated{}
	if _, err := compileKnowledgeFusedFieldProjection(
		knowledgeFusedFieldState(),
		[]compiledKnowledgeFieldAssignment{calculatedAssignment},
		0,
		"calculated",
		compiledKnowledgeSelectorChargeColumns{},
		compiledKnowledgeAliasCopyChargeColumns{},
	); err == nil {
		t.Fatal("calculated projection accepted an assignment without lowerer-retained authority")
	}
}

func knowledgeFusedFieldState() compileState {
	return compileState{
		visible: map[string]fieldState{
			"host":       canonicalState("host"),
			"source":     canonicalState("source"),
			"index":      canonicalState("index"),
			"sourcetype": canonicalState("sourcetype"),
		},
		publicOrder:     []string{"host", "source", "index", "sourcetype", "fields"},
		allowDynamic:    true,
		eventRows:       true,
		blocked:         make(map[string]struct{}),
		blockedPrefixes: make(map[string]struct{}),
	}
}

func knowledgeFusedFieldProgram(
	t *testing.T,
	definitions []*opensplunkv1.KnowledgeObjectDefinition,
) knowledgeprogram.Program {
	t.Helper()
	objects := make([]*opensplunkv1.KnowledgeSnapshotObject, len(definitions))
	stageOrdinals := make(map[opensplunkv1.KnowledgeSearchStage]uint32)
	for index, definition := range definitions {
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatalf("Normalize(%d): %v", index, err)
		}
		stage := opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_UNSPECIFIED
		switch normalized.ObjectType {
		case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
			stage = opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS
		case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
			stage = opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
		default:
			t.Fatalf("unexpected object type %v", normalized.ObjectType)
		}
		objects[index] = &opensplunkv1.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index),
			Stage:             stage,
			StageOrdinal:      stageOrdinals[stage],
			KnowledgeObjectId: "fused-" + normalized.Name,
			Version:           1,
			ObjectType:        normalized.ObjectType,
			Name:              normalized.Name,
			AppId:             normalized.AppID,
			OwnerId:           "owner",
			SharingScope:      normalized.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSha256:  slices.Clone(normalized.Digest[:]),
		}
		stageOrdinals[stage]++
	}
	program, err := knowledgeprogram.Prepare(knowledgeprogram.Input{Objects: objects})
	if err != nil {
		t.Fatalf("knowledgeprogram.Prepare: %v", err)
	}
	return program
}
