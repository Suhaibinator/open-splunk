package clickhouse

import (
	"slices"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileKnowledgeSidecarMergeLazilyBindsOneDestination(t *testing.T) {
	program := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeAliasSidecarDefinition(
			"copy-payload",
			"payload",
			"copied_payload",
			"east",
			opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
		),
	})
	input := knowledgeExtractionStageState()
	compiled, err := compileKnowledgeAliasStage(
		input,
		program.Aliases(),
		3,
		compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compile alias sidecar stage: %v", err)
	}
	if compiled.emittedAssignments != 1 || len(compiled.arrayJoinBindings) != 1 {
		t.Fatalf("emission = %d assignments / %d bindings", compiled.emittedAssignments, len(compiled.arrayJoinBindings))
	}
	binding := compiled.arrayJoinBindings[0]
	if strings.Count(binding, `AS "__os_ko_field_binding_3_0"`) != 1 ||
		strings.Count(binding, "JSONExtract(") != 2 ||
		strings.Count(binding, "arrayFirst(") != 1 ||
		!strings.Contains(binding, "__os_ko_field_previous_present") {
		t.Fatalf("lazy source/prior binding shape is invalid:\n%s", binding)
	}
	selectorBranch := strings.LastIndex(binding, "if(tupleElement(__os_ko_field_selector, 1) != 0")
	sourceMaterializer := strings.LastIndex(binding, "JSONExtract(")
	if selectorBranch < 0 || sourceMaterializer <= selectorBranch {
		t.Fatalf("source materializer is not inside selector eligibility:\n%s", binding)
	}
	if got := strings.Count(binding, "?"); got != len(compiled.suffixArgs) {
		t.Fatalf("placeholders = %d, args = %d", got, len(compiled.suffixArgs))
	}

	field := compiled.state.visible["copied_payload"]
	if field.relativeFieldNamesSQL == "" || field.relativeFieldTypesSQL == "" ||
		field.fieldMetadataVersionSQL == "" || field.descendantSQL == "" {
		t.Fatalf("successor sidecar authority = %#v", field)
	}
	for _, column := range []string{
		field.relativeFieldNamesSQL,
		field.relativeFieldTypesSQL,
		field.fieldMetadataVersionSQL,
	} {
		if !slices.Contains(compiled.state.privateColumns, column) {
			t.Fatalf("sidecar %q is not private: %#v", column, compiled.state.privateColumns)
		}
	}
	delete(compiled.state.visible, "host")
	compiled.state.privateColumns[0] = "mutated"
	if _, ok := input.visible["host"]; !ok || slices.Contains(input.privateColumns, "mutated") {
		t.Fatal("returned state aliases the frozen input state")
	}
}

func TestCompileKnowledgeSidecarMergeGroupsDisjointWritersAtomically(t *testing.T) {
	program := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeAliasSidecarDefinition(
			"a-east",
			"payload",
			"shared_copy",
			"east",
			opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
		),
		knowledgeAliasSidecarDefinition(
			"b-west",
			"alternate",
			"shared_copy",
			"west",
			opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		),
	})
	compiled, err := compileKnowledgeAliasStage(
		knowledgeExtractionStageState(),
		program.Aliases(),
		4,
		compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compile disjoint sidecar aliases: %v", err)
	}
	if compiled.emittedAssignments != 2 || len(compiled.arrayJoinBindings) != 1 {
		t.Fatalf("emission = %d assignments / %d destination groups", compiled.emittedAssignments, len(compiled.arrayJoinBindings))
	}
	binding := compiled.arrayJoinBindings[0]
	if strings.Count(binding, "JSONExtract(") != 3 ||
		strings.Count(binding, "__os_ko_field_selector") < 2 ||
		strings.Count(binding, "arrayFirst(") != 1 {
		t.Fatalf("disjoint winner/prior materialization is not grouped:\n%s", binding)
	}
	projection := strings.Join(compiled.projection, " ")
	for _, required := range []string{
		` AS "shared_copy"`,
		`"__os_ko_field_names_4_0"`,
		`"__os_ko_field_types_4_0"`,
		`"__os_ko_field_metadata_version_4_0"`,
	} {
		if !strings.Contains(projection, required) {
			t.Fatalf("atomic destination projection omits %q:\n%s", required, projection)
		}
	}
	if got := strings.Count(binding, "?"); got != len(compiled.suffixArgs) {
		t.Fatalf("group placeholders = %d, args = %d", got, len(compiled.suffixArgs))
	}
}

func TestCompileKnowledgeCalculatedSidecarsRetainOnlyDirectFieldsAndRejectForgery(t *testing.T) {
	aliasProgram := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeAliasSidecarDefinition(
			"alias",
			"payload",
			"aliased_payload",
			"east",
			opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		),
	})
	aliasStage, err := compileKnowledgeAliasStage(
		knowledgeExtractionStageState(),
		aliasProgram.Aliases(),
		0,
		compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compile alias stage: %v", err)
	}
	calculatedProgram := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeCalculatedSidecarDefinition("a-direct", "direct_copy", "aliased_payload"),
		knowledgeCalculatedSidecarDefinition("b-lower", "lower_copy", "lower(aliased_payload)"),
	})
	operations := calculatedProgram.CalculatedFields()
	direct, err := compileKnowledgeCalculatedAssignment(operations[0], aliasStage.state)
	if err != nil {
		t.Fatalf("compile direct calculated field: %v", err)
	}
	transformed, err := compileKnowledgeCalculatedAssignment(operations[1], aliasStage.state)
	if err != nil {
		t.Fatalf("compile transformed calculated field: %v", err)
	}
	aliased := aliasStage.state.visible["aliased_payload"]
	for _, sidecar := range []string{
		aliased.relativeFieldNamesSQL,
		aliased.relativeFieldTypesSQL,
		aliased.fieldMetadataVersionSQL,
	} {
		if sidecar == "" || !strings.Contains(direct.source.sql, sidecar) ||
			strings.Contains(transformed.source.sql, sidecar) {
			t.Fatalf("direct/transformed sidecar propagation disagrees for %q", sidecar)
		}
	}
	directInputAuthority, err := compileKnowledgeFieldInputStateAuthority(
		aliasStage.state,
		operations[0].InputFields(),
	)
	if err != nil {
		t.Fatalf("compile direct input authority: %v", err)
	}
	if !validCompiledKnowledgeFieldSourceAuthority(
		direct.source,
		"expression:"+operations[0].Expression(),
		directInputAuthority,
	) {
		t.Fatal("direct calculated source is not sealed")
	}

	for _, test := range []struct {
		name   string
		mutate func(*compiledKnowledgeFieldAssignment)
	}{
		{name: "source SQL", mutate: func(value *compiledKnowledgeFieldAssignment) { value.source.sql += " " }},
		{name: "source authority", mutate: func(value *compiledKnowledgeFieldAssignment) { value.source.authority += "-other" }},
		{name: "source input authority", mutate: func(value *compiledKnowledgeFieldAssignment) {
			value.source.inputStateAuthority[0] ^= 0xff
		}},
		{name: "source byte bound", mutate: func(value *compiledKnowledgeFieldAssignment) {
			value.source.maxStringBytes++
		}},
		{name: "valid source from another assignment", mutate: func(value *compiledKnowledgeFieldAssignment) {
			value.source = transformed.source
		}},
		{name: "compiled selector", mutate: func(value *compiledKnowledgeFieldAssignment) {
			value.selectorSQL.sql += " "
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			forged := direct
			test.mutate(&forged)
			if _, compileErr := compileKnowledgeFusedFieldProjection(
				aliasStage.state,
				[]compiledKnowledgeFieldAssignment{forged},
				0,
				"calculated",
				compiledKnowledgeSelectorChargeColumns{},
				compiledKnowledgeAliasCopyChargeColumns{},
			); compileErr == nil {
				t.Fatal("forged assignment compiled")
			}
		})
	}
	partial := compiledScalar{
		valueSQL:              "CAST('x' AS Dynamic)",
		existsSQL:             "1",
		kind:                  fieldKindDynamic,
		relativeFieldNamesSQL: quoteIdentifier("partial_names"),
	}
	if _, err := compileKnowledgeFieldSourceFromScalar(partial, true); err == nil {
		t.Fatal("partial sidecar authority compiled")
	}
}

func TestCompileKnowledgeSidecarMergeRejectsReferencedStateReplay(t *testing.T) {
	program := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeAliasSidecarDefinition(
			"copy-host",
			"host",
			"copied_host",
			"east",
			opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		),
	})
	operation := program.Aliases()[0]
	stateA := knowledgeExtractionStageState()
	stateB := cloneCompileState(stateA)
	host := stateB.visible["host"]
	host.maxStringBytes++
	stateB.visible["host"] = host
	replayed, err := compileKnowledgeAliasAssignment(operation, stateB)
	if err != nil {
		t.Fatalf("compile replay source: %v", err)
	}
	if _, err := compileKnowledgeFusedFieldProjection(
		stateA,
		[]compiledKnowledgeFieldAssignment{replayed},
		0,
		"alias",
		compiledKnowledgeSelectorChargeColumns{},
		compiledKnowledgeAliasCopyChargeColumns{},
	); err == nil {
		t.Fatal("source authority replayed across a changed referenced field")
	}

	unrelated := cloneCompileState(stateA)
	source := unrelated.visible["source"]
	source.maxStringBytes++
	unrelated.visible["source"] = source
	semanticallySame, err := compileKnowledgeAliasAssignment(operation, unrelated)
	if err != nil {
		t.Fatalf("compile assignment with unrelated state change: %v", err)
	}
	if _, err := compileKnowledgeFusedFieldProjection(
		stateA,
		[]compiledKnowledgeFieldAssignment{semanticallySame},
		0,
		"alias",
		compiledKnowledgeSelectorChargeColumns{},
		compiledKnowledgeAliasCopyChargeColumns{},
	); err != nil {
		t.Fatalf("unrelated field change invalidated source authority: %v", err)
	}
}

func TestCompileKnowledgeExtractionSidecarsKeepRawPriorLazy(t *testing.T) {
	program := knowledgePreparationProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeRegexStageDefinition(
			"replace-payload",
			`(?P<payload>[a-z]+)`,
			[]string{"payload"},
			"east",
		),
	})
	compiled, err := compileKnowledgeExtractionStage(
		knowledgeExtractionStageState(),
		program,
		6,
		compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compile raw-prior extraction: %v", err)
	}
	bindings := strings.Join(compiled.arrayJoinBindings, " ")
	projection := strings.Join(compiled.projection, " ")
	if strings.Contains(bindings, "JSONExtract(") {
		t.Fatalf("raw prior was materialized in the eager binding layer:\n%s", bindings)
	}
	if strings.Count(projection, "JSONExtract(") != 1 ||
		!strings.Contains(projection, `"__os_ko_extract_previous_authority_6_0"`) {
		t.Fatalf("raw prior is not confined to the no-write projection fallback:\n%s", projection)
	}
	_, previous, _, previousErr := compileKnowledgeExtractionPrevious(
		"payload",
		knowledgeExtractionStageState(),
		6,
		0,
	)
	if previousErr != nil {
		t.Fatalf("compile raw prior authority: %v", previousErr)
	}
	for label, expression := range map[string]string{
		"names": previous.namesSQL,
		"types": previous.typesSQL,
	} {
		if !strings.Contains(expression, "NOT (has(") {
			t.Fatalf("%s sidecar does not preserve exact-leaf precedence: %s", label, expression)
		}
	}
}

func TestCompileKnowledgeExtractionSidecarsPreservePriorAndClearScalarWrites(t *testing.T) {
	program := knowledgePreparationProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeJSONStageDefinition("json", "payload", "payload.value", "east"),
	})
	state := knowledgeExtractionStageState()
	state.visible["payload"] = fieldState{
		valueSQL:                quoteIdentifier("payload"),
		existsSQL:               quoteIdentifier("prior_exists"),
		storedTypeSQL:           quoteIdentifier("prior_type"),
		descendantSQL:           quoteIdentifier("prior_descendant"),
		relativeFieldNamesSQL:   quoteIdentifier("prior_names"),
		relativeFieldTypesSQL:   quoteIdentifier("prior_types"),
		fieldMetadataVersionSQL: quoteIdentifier("prior_metadata_version"),
		kind:                    fieldKindDynamic,
	}
	state.publicOrder = append(state.publicOrder, "payload")
	state.privateColumns = append(
		state.privateColumns,
		quoteIdentifier("prior_exists"),
		quoteIdentifier("prior_type"),
		quoteIdentifier("prior_descendant"),
		quoteIdentifier("prior_names"),
		quoteIdentifier("prior_types"),
		quoteIdentifier("prior_metadata_version"),
	)
	compiled, err := compileKnowledgeExtractionStage(
		state,
		program,
		2,
		compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compile extraction sidecar merge: %v", err)
	}
	projection := strings.Join(compiled.projection, " ")
	for _, required := range []string{
		quoteIdentifier("prior_names"),
		quoteIdentifier("prior_types"),
		quoteIdentifier("prior_metadata_version"),
		knowledgeEmptyRelativeFieldNamesSQL(),
		knowledgeEmptyRelativeFieldTypesSQL(),
		`"__os_ko_extract_names_2_0"`,
		`"__os_ko_extract_types_2_0"`,
		`"__os_ko_extract_metadata_version_2_0"`,
	} {
		if !strings.Contains(projection, required) {
			t.Fatalf("extraction merge omits %q:\n%s", required, projection)
		}
	}
	field := compiled.state.visible["payload"]
	if field.relativeFieldNamesSQL == quoteIdentifier("prior_names") ||
		field.relativeFieldNamesSQL == "" || field.relativeFieldTypesSQL == "" ||
		field.fieldMetadataVersionSQL == "" {
		t.Fatalf("extraction successor authority = %#v", field)
	}
	for _, column := range []string{
		field.relativeFieldNamesSQL,
		field.relativeFieldTypesSQL,
		field.fieldMetadataVersionSQL,
	} {
		if !slices.Contains(compiled.state.privateColumns, column) {
			t.Fatalf("extraction sidecar %q is not private", column)
		}
	}
}

func TestCompileKnowledgeContainerSidecarPathRemainsClosedAtSeal(t *testing.T) {
	program := knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeAliasSidecarDefinition(
			"container-alias",
			"payload",
			"copied_payload",
			"east",
			opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		),
	})
	logical, err := plan.InjectKnowledgePrelude(buildPlan(t, `index=gradethis`), program)
	if err != nil {
		t.Fatalf("inject container knowledge prelude: %v", err)
	}
	capture, _, compileErr := compileCentralKnowledgeCapture(logical)
	requireCentralKnowledgeClosedSeal(t, compileErr)
	_, visible := capture.state.visible["copied_payload"]
	if !capture.called || !visible {
		t.Fatalf("container alias did not reach the closed final seal: %#v", capture)
	}
}

func knowledgeAliasSidecarDefinition(
	name string,
	source string,
	destination string,
	index string,
	overwrite opensplunkv1.KnowledgeOverwriteBehavior,
) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app", Name: name,
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Selector: &opensplunkv1.KnowledgeSelector{
			IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: index}},
		},
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField:       source,
				DestinationField:  destination,
				OverwriteBehavior: overwrite,
			},
		},
	}
}

func knowledgeCalculatedSidecarDefinition(
	name string,
	destination string,
	expression string,
) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app", Name: name,
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
			CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
				DestinationField:  destination,
				Expression:        expression,
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			},
		},
	}
}
