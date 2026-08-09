package clickhouse

import (
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileResultContainerOutputsKeepsHiddenTransportPrivateAndOrdered(t *testing.T) {
	t.Parallel()

	state := compileState{
		visible: map[string]fieldState{
			"plain": {
				valueSQL: quoteIdentifier("plain"),
				kind:     fieldKindString,
			},
			"first":  testResultContainerField("first"),
			"second": testResultContainerField("second"),
		},
		publicOrder: []string{"plain", "first", "second"},
	}
	compiled, err := finalizeOrdinaryQuery(
		compiledRelation{sql: "SELECT source", depth: 1, ownerRange: spl.Range{}},
		state,
		[]any{"scope"},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("finalizeOrdinaryQuery: %v", err)
	}
	wantDescriptors := []ResultContainerOutput{
		canonicalResultContainerOutput(1),
		canonicalResultContainerOutput(2),
	}
	if !slices.Equal(compiled.OutputFields, []string{"plain", "first", "second"}) ||
		!slices.Equal(compiled.ContainerOutputs, wantDescriptors) {
		t.Fatalf("compiled output = %#v / %#v", compiled.OutputFields, compiled.ContainerOutputs)
	}
	positions := []int{
		strings.Index(compiled.SQL, quoteIdentifier("plain")),
		strings.Index(compiled.SQL, quoteIdentifier("first")),
		strings.Index(compiled.SQL, quoteIdentifier("second")),
		strings.Index(compiled.SQL, quoteIdentifier(wantDescriptors[0].NamesColumn())),
		strings.Index(compiled.SQL, quoteIdentifier(wantDescriptors[0].TypesColumn())),
		strings.Index(compiled.SQL, quoteIdentifier(wantDescriptors[0].MetadataVersionColumn())),
		strings.Index(compiled.SQL, quoteIdentifier(wantDescriptors[1].NamesColumn())),
		strings.Index(compiled.SQL, quoteIdentifier(wantDescriptors[1].TypesColumn())),
		strings.Index(compiled.SQL, quoteIdentifier(wantDescriptors[1].MetadataVersionColumn())),
	}
	for index, position := range positions {
		if position < 0 || index > 0 && position <= positions[index-1] {
			t.Fatalf("physical output order = %#v\n%s", positions, compiled.SQL)
		}
	}
	for _, output := range wantDescriptors {
		for _, hidden := range []string{
			output.NamesColumn(),
			output.TypesColumn(),
			output.MetadataVersionColumn(),
		} {
			if slices.Contains(compiled.OutputFields, hidden) {
				t.Fatalf("hidden column %q entered public output", hidden)
			}
		}
	}
}

func TestCompileResultContainerOutputsRejectsIncompleteOrCollidingAuthority(t *testing.T) {
	t.Parallel()

	partial := testResultContainerField("container")
	partial.relativeFieldTypesSQL = ""
	if _, _, err := compileResultContainerOutputs(
		compileState{visible: map[string]fieldState{"container": partial}},
		[]string{"container"},
	); err == nil {
		t.Fatal("partial sidecar triple compiled")
	}

	descriptor := canonicalResultContainerOutput(0)
	state := compileState{visible: map[string]fieldState{
		"container": testResultContainerField("container"),
		descriptor.NamesColumn(): {
			valueSQL: quoteIdentifier(descriptor.NamesColumn()),
			kind:     fieldKindString,
		},
	}}
	if _, _, err := compileResultContainerOutputs(
		state,
		[]string{"container", descriptor.NamesColumn()},
	); err == nil {
		t.Fatal("public/hidden column collision compiled")
	}
}

func TestResultContainerOutputsSurviveChronologicalValidation(t *testing.T) {
	t.Parallel()

	descriptor := canonicalResultContainerOutput(0)
	resultColumns := []string{
		"container",
		descriptor.NamesColumn(),
		descriptor.TypesColumn(),
		descriptor.MetadataVersionColumn(),
	}
	projection := make([]string, len(resultColumns))
	for index, column := range resultColumns {
		projection[index] = quoteIdentifier(column)
	}
	compiled, err := wrapChronologicalValidation(
		`SELECT "container", "`+descriptor.NamesColumn()+`", "`+
			descriptor.TypesColumn()+`", "`+descriptor.MetadataVersionColumn()+`"`,
		1,
		spl.Range{},
		[]compiledChronologicalBarrier{{
			name:              `"container_barrier"`,
			sql:               `SELECT toUInt8(0) AS "invalid"`,
			validationColumns: []string{`"invalid"`},
			depth:             1,
		}},
		projection,
		resultColumns,
		"",
		eventStatsOrdinarySourceFanout,
		CompiledQuery{
			OutputFields:     []string{"container"},
			ContainerOutputs: []ResultContainerOutput{descriptor},
		},
		0,
	)
	if err != nil {
		t.Fatalf("wrapChronologicalValidation: %v", err)
	}
	if !slices.Equal(compiled.ContainerOutputs, []ResultContainerOutput{descriptor}) {
		t.Fatalf("chronological descriptors = %#v", compiled.ContainerOutputs)
	}
	for _, hidden := range resultColumns[1:] {
		if !strings.Contains(compiled.SQL, quoteIdentifier(hidden)) {
			t.Fatalf("chronological output lost %q:\n%s", hidden, compiled.SQL)
		}
	}
}

func TestCompiledQueryExecutionSealBindsContainerOutputs(t *testing.T) {
	t.Parallel()

	base := compileSPL(t, `index=gradethis | table event_id status`)
	baseRetained, ok := base.RetainedBytes()
	if !ok {
		t.Fatal("base query has no retained-byte authority")
	}
	withTransport := base
	withTransport.ContainerOutputs = []ResultContainerOutput{
		canonicalResultContainerOutput(1),
	}
	withTransport.executionSeal = nil
	sealed, err := sealCompiledQueryExecution(withTransport)
	if err != nil {
		t.Fatalf("seal container transport: %v", err)
	}
	cloned, ok := sealed.CloneForExecution()
	retained, retainedOK := sealed.RetainedBytes()
	if !ok || !retainedOK || retained <= baseRetained ||
		!sealed.EqualForExecution(cloned) ||
		&sealed.ContainerOutputs[0] == &cloned.ContainerOutputs[0] {
		t.Fatalf("clone/retained authority = (%t, %d/%d, %t)", ok, retained, baseRetained, retainedOK)
	}
	cloned.ContainerOutputs[0].OutputIndex = 0
	if cloned.HasValidExecutionSeal() || !sealed.HasValidExecutionSeal() {
		t.Fatal("descriptor mutation did not invalidate only the detached clone")
	}

	for _, test := range []struct {
		name    string
		outputs []ResultContainerOutput
	}{
		{"duplicate", []ResultContainerOutput{{OutputIndex: 0}, {OutputIndex: 0}}},
		{"reordered", []ResultContainerOutput{{OutputIndex: 1}, {OutputIndex: 0}}},
		{"out of range", []ResultContainerOutput{{OutputIndex: 2}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			candidate.ContainerOutputs = test.outputs
			candidate.executionSeal = nil
			if _, err := sealCompiledQueryExecution(candidate); err == nil {
				t.Fatal("invalid container descriptor sealed")
			}
		})
	}

	empty := base
	empty.ContainerOutputs = []ResultContainerOutput{}
	empty.executionSeal = nil
	empty, err = sealCompiledQueryExecution(empty)
	if err != nil {
		t.Fatalf("seal non-nil empty descriptor: %v", err)
	}
	if base.EqualForExecution(empty) {
		t.Fatal("nil and non-nil empty descriptor authority compared equal")
	}
}

func testResultContainerField(name string) fieldState {
	return fieldState{
		valueSQL:                  quoteIdentifier(name),
		kind:                      fieldKindDynamic,
		relativeFieldNamesSQL:     quoteIdentifier("__sidecar_" + name + "_names"),
		relativeFieldTypesSQL:     quoteIdentifier("__sidecar_" + name + "_types"),
		fieldMetadataVersionSQL:   quoteIdentifier("__sidecar_" + name + "_version"),
		dynamicNumericEligibleSQL: "0",
	}
}
