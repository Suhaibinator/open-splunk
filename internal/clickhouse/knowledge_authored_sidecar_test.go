package clickhouse

import (
	"slices"
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

func knowledgeSidecarColumns(field fieldState) []string {
	return []string{
		field.relativeFieldNamesSQL,
		field.relativeFieldTypesSQL,
		field.fieldMetadataVersionSQL,
	}
}
