package clickhouse

import (
	"slices"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestAuthoredTableCarriesKnowledgeSidecarsPrivately(t *testing.T) {
	program := authoredSidecarProgram(t)
	baseline := captureAuthoredSidecarState(t, program, `index=gradethis`)
	capture := captureAuthoredSidecarState(
		t,
		program,
		`index=gradethis | table copied_payload`,
	)
	want := baseline.state.visible["copied_payload"]
	got, ok := capture.state.visible["copied_payload"]
	if !ok {
		t.Fatal("table dropped selected knowledge container")
	}
	requireIdentitySidecars(t, got, want)
	requirePrivateSidecars(t, capture, got)
	if !slices.Equal(capture.compiled.OutputFields, []string{"copied_payload"}) {
		t.Fatalf("table outputs = %#v", capture.compiled.OutputFields)
	}
}

func TestAuthoredProjectionPrunesDroppedKnowledgeSidecars(t *testing.T) {
	program := authoredSidecarProgram(t)
	baseline := captureAuthoredSidecarState(t, program, `index=gradethis`)
	want := baseline.state.visible["copied_payload"]
	capture := captureAuthoredSidecarState(
		t,
		program,
		`index=gradethis | table host`,
	)
	if _, ok := capture.state.visible["copied_payload"]; ok {
		t.Fatal("table retained dropped knowledge container")
	}
	for _, column := range knowledgeSidecarColumns(want) {
		if slices.Contains(capture.state.privateColumns, column) {
			t.Fatalf("dropped sidecar %q remains private", column)
		}
	}
	if !slices.Equal(capture.compiled.OutputFields, []string{"host"}) {
		t.Fatalf("table outputs = %#v", capture.compiled.OutputFields)
	}
}

func TestAuthoredRenameCarriesKnowledgeSidecars(t *testing.T) {
	program := authoredSidecarProgram(t)
	baseline := captureAuthoredSidecarState(t, program, `index=gradethis`)
	capture := captureAuthoredSidecarState(
		t,
		program,
		`index=gradethis | rename copied_payload AS moved_payload | table moved_payload`,
	)
	if _, ok := capture.state.visible["copied_payload"]; ok {
		t.Fatal("rename retained the source field")
	}
	got, ok := capture.state.visible["moved_payload"]
	if !ok {
		t.Fatal("rename omitted the destination field")
	}
	requireIdentitySidecars(t, got, baseline.state.visible["copied_payload"])
	if got.valueSQL != quoteIdentifier("moved_payload") {
		t.Fatalf("renamed value SQL = %q", got.valueSQL)
	}
	requirePrivateSidecars(t, capture, got)
	if !slices.Equal(capture.compiled.OutputFields, []string{"moved_payload"}) {
		t.Fatalf("rename outputs = %#v", capture.compiled.OutputFields)
	}
}

func TestAuthoredDirectEvalCarriesKnowledgeSidecars(t *testing.T) {
	program := authoredSidecarProgram(t)
	baseline := captureAuthoredSidecarState(t, program, `index=gradethis`)
	capture := captureAuthoredSidecarState(
		t,
		program,
		`index=gradethis | eval carried_payload=copied_payload | table carried_payload`,
	)
	if _, ok := capture.state.visible["copied_payload"]; ok {
		t.Fatal("table retained the eval source")
	}
	got, ok := capture.state.visible["carried_payload"]
	if !ok {
		t.Fatal("eval omitted the destination field")
	}
	requireIdentitySidecars(t, got, baseline.state.visible["copied_payload"])
	if got.valueSQL != quoteIdentifier("carried_payload") {
		t.Fatalf("eval value SQL = %q", got.valueSQL)
	}
	requirePrivateSidecars(t, capture, got)
	if !slices.Equal(capture.compiled.OutputFields, []string{"carried_payload"}) {
		t.Fatalf("eval outputs = %#v", capture.compiled.OutputFields)
	}
}

func TestAuthoredTransformedEvalClearsKnowledgeSidecars(t *testing.T) {
	program := authoredSidecarProgram(t)
	baseline := captureAuthoredSidecarState(t, program, `index=gradethis`)
	capture := captureAuthoredSidecarState(
		t,
		program,
		`index=gradethis | eval copied_payload=lower(copied_payload) | table copied_payload`,
	)
	got, ok := capture.state.visible["copied_payload"]
	if !ok {
		t.Fatal("transformed eval omitted its destination")
	}
	if got.relativeFieldNamesSQL != "" || got.relativeFieldTypesSQL != "" ||
		got.fieldMetadataVersionSQL != "" || got.descendantSQL != "" ||
		len(got.descendantArgs) != 0 || !got.storedPath.isZero() {
		t.Fatalf("transformed eval retained container authority: %#v", got)
	}
	for _, column := range knowledgeSidecarColumns(
		baseline.state.visible["copied_payload"],
	) {
		if slices.Contains(capture.state.privateColumns, column) {
			t.Fatalf("transformed eval retained private sidecar %q", column)
		}
	}
	if !slices.Equal(capture.compiled.OutputFields, []string{"copied_payload"}) {
		t.Fatalf("transformed eval outputs = %#v", capture.compiled.OutputFields)
	}
}

func TestAuthoredSidecarConsumersRejectPartialAuthority(t *testing.T) {
	state := knowledgeExtractionStageState()
	state.visible["broken"] = fieldState{
		valueSQL:              quoteIdentifier("broken"),
		existsSQL:             "1",
		kind:                  fieldKindDynamic,
		relativeFieldNamesSQL: quoteIdentifier("broken_names"),
	}
	state.publicOrder = append(state.publicOrder, "broken")
	state.privateColumns = append(state.privateColumns, quoteIdentifier("broken_names"))
	if _, _, _, err := compileProjection(&plan.Project{
		Mode:   plan.ProjectModeTable,
		Fields: []plan.FieldRef{{Name: "broken", Canonical: true}},
	}, state); err == nil {
		t.Fatal("table accepted partial sidecar authority")
	}
	if _, _, _, err := compileRenameAssignment(plan.RenameAssignment{
		Source:      plan.FieldRef{Name: "broken", Canonical: true},
		Destination: plan.FieldRef{Name: "renamed", Canonical: true},
	}, state); err == nil {
		t.Fatal("rename accepted partial sidecar authority")
	}
	if _, err := extendCompileState(
		state,
		plan.FieldRef{Name: "extended", Canonical: true},
		compiledScalar{
			valueSQL:              quoteIdentifier("broken"),
			existsSQL:             "1",
			kind:                  fieldKindDynamic,
			relativeFieldNamesSQL: quoteIdentifier("broken_names"),
		},
		true,
	); err == nil {
		t.Fatal("eval accepted partial sidecar authority")
	}
}

func TestAuthoredRexConditionallyMergesKnowledgeSidecars(t *testing.T) {
	program := authoredSidecarProgram(t)
	baseline := captureAuthoredSidecarState(t, program, `index=gradethis`)
	capture := captureAuthoredSidecarState(
		t,
		program,
		`index=gradethis | rex field=_raw "(?<copied_payload>[a-z]+)" | table copied_payload`,
	)
	requireConditionalAuthoredSidecars(
		t,
		capture,
		baseline.state.visible["copied_payload"],
		"copied_payload",
		"__os_rex_matched_",
	)
}

func TestAuthoredSpathConditionallyMergesKnowledgeSidecars(t *testing.T) {
	program := authoredSidecarProgram(t)
	baseline := captureAuthoredSidecarState(t, program, `index=gradethis`)
	capture := captureAuthoredSidecarState(
		t,
		program,
		`index=gradethis | spath input=_raw output=copied_payload path=value | table copied_payload`,
	)
	requireConditionalAuthoredSidecars(
		t,
		capture,
		baseline.state.visible["copied_payload"],
		"copied_payload",
		"__os_spath_matched_",
	)
}

func TestAuthoredDynamicBinConditionallyMergesKnowledgeSidecars(t *testing.T) {
	program := authoredSidecarProgram(t)
	baseline := captureAuthoredSidecarState(t, program, `index=gradethis`)
	capture := captureAuthoredSidecarState(
		t,
		program,
		`index=gradethis | bin metric span=10 AS copied_payload | table copied_payload`,
	)
	got, ok := capture.state.visible["copied_payload"]
	if !ok || got.relativeFieldNamesSQL == "" || got.relativeFieldTypesSQL == "" ||
		got.fieldMetadataVersionSQL == "" {
		t.Fatalf("dynamic bin sidecars are incomplete: %#v", got)
	}
	for _, column := range knowledgeSidecarColumns(
		baseline.state.visible["copied_payload"],
	) {
		if !strings.Contains(capture.relation.sql, column) {
			t.Fatalf("dynamic bin dropped prior sidecar %q", column)
		}
	}
	for _, marker := range []string{
		"__os_numeric_bin_output_names_",
		"__os_numeric_bin_output_types_",
		"__os_numeric_bin_output_metadata_version_",
		knowledgeEmptyRelativeFieldNamesSQL(),
		knowledgeEmptyRelativeFieldTypesSQL(),
	} {
		if !strings.Contains(capture.relation.sql, marker) {
			t.Fatalf("dynamic bin merge omits %q", marker)
		}
	}
	requirePrivateSidecars(t, capture, got)
}

func TestAuthoredDynamicBinInPlaceClearsKnowledgeSidecars(t *testing.T) {
	program := authoredSidecarProgram(t)
	baseline := captureAuthoredSidecarState(t, program, `index=gradethis`)
	capture := captureAuthoredSidecarState(
		t,
		program,
		`index=gradethis | bin copied_payload span=10 | table copied_payload`,
	)
	got := capture.state.visible["copied_payload"]
	if got.relativeFieldNamesSQL != "" || got.relativeFieldTypesSQL != "" ||
		got.fieldMetadataVersionSQL != "" {
		t.Fatalf("in-place bin retained container sidecars: %#v", got)
	}
	for _, column := range knowledgeSidecarColumns(
		baseline.state.visible["copied_payload"],
	) {
		if slices.Contains(capture.state.privateColumns, column) {
			t.Fatalf("in-place bin retained prior sidecar %q", column)
		}
	}
}

func authoredSidecarProgram(t *testing.T) knowledgeprogram.Program {
	t.Helper()
	return knowledgeFusedFieldProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeAliasSidecarDefinition(
			"container-alias",
			"payload",
			"copied_payload",
			"east",
			opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		),
	})
}

func captureAuthoredSidecarState(
	t *testing.T,
	program knowledgeprogram.Program,
	source string,
) centralKnowledgeFinalizerCapture {
	t.Helper()
	logical, err := plan.InjectKnowledgePrelude(buildPlan(t, source), program)
	if err != nil {
		t.Fatalf("inject knowledge prelude: %v", err)
	}
	capture, _, compileErr := compileCentralKnowledgeCapture(logical)
	requireCentralKnowledgeClosedSeal(t, compileErr)
	if !capture.called {
		t.Fatal("authored suffix did not reach the closed final seal")
	}
	return capture
}

func requireIdentitySidecars(t *testing.T, got, want fieldState) {
	t.Helper()
	if got.relativeFieldNamesSQL != want.relativeFieldNamesSQL ||
		got.relativeFieldTypesSQL != want.relativeFieldTypesSQL ||
		got.fieldMetadataVersionSQL != want.fieldMetadataVersionSQL ||
		got.descendantSQL != want.descendantSQL ||
		!got.storedPath.equal(want.storedPath) {
		t.Fatalf("identity sidecars = %#v, want %#v", got, want)
	}
}

func requirePrivateSidecars(
	t *testing.T,
	capture centralKnowledgeFinalizerCapture,
	field fieldState,
) {
	t.Helper()
	for _, column := range knowledgeSidecarColumns(field) {
		if !slices.Contains(capture.state.privateColumns, column) {
			t.Fatalf("sidecar %q is not private: %#v", column, capture.state.privateColumns)
		}
		if _, visible := capture.state.visible[column]; visible ||
			slices.Contains(capture.state.publicOrder, column) ||
			slices.Contains(capture.compiled.OutputFields, column) {
			t.Fatalf("sidecar %q leaked into the public result", column)
		}
	}
}

func requireConditionalAuthoredSidecars(
	t *testing.T,
	capture centralKnowledgeFinalizerCapture,
	previous fieldState,
	output string,
	matchedMarker string,
) {
	t.Helper()
	got, ok := capture.state.visible[output]
	if !ok {
		t.Fatalf("conditional writer omitted %q", output)
	}
	if got.relativeFieldNamesSQL == "" || got.relativeFieldTypesSQL == "" ||
		got.fieldMetadataVersionSQL == "" || got.descendantSQL == "" {
		t.Fatalf("conditional sidecars are incomplete: %#v", got)
	}
	for _, column := range knowledgeSidecarColumns(previous) {
		if !strings.Contains(capture.relation.sql, column) {
			t.Fatalf("conditional writer dropped prior sidecar %q", column)
		}
	}
	if !strings.Contains(capture.relation.sql, matchedMarker) ||
		!strings.Contains(capture.relation.sql, knowledgeEmptyRelativeFieldNamesSQL()) ||
		!strings.Contains(capture.relation.sql, knowledgeEmptyRelativeFieldTypesSQL()) {
		t.Fatalf("conditional merge SQL is incomplete:\n%s", capture.relation.sql)
	}
	requirePrivateSidecars(t, capture, got)
	if !slices.Equal(capture.compiled.OutputFields, []string{output}) {
		t.Fatalf("conditional outputs = %#v", capture.compiled.OutputFields)
	}
}

func knowledgeSidecarColumns(field fieldState) []string {
	return []string{
		field.relativeFieldNamesSQL,
		field.relativeFieldTypesSQL,
		field.fieldMetadataVersionSQL,
	}
}
