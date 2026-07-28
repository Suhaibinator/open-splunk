package clickhouse

import (
	"strings"
	"testing"
)

func TestCompilerSealsEveryMainQueryShapeAgainstSQLMutation(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | head 1`,
		`index=gradethis | timechart span=5m count BY level`,
		`index=gradethis | chart count OVER level BY message`,
	} {
		compiled := compileSPL(t, source)
		if !compiled.HasValidSQLSeal() {
			t.Fatalf("%q main compiler result is not sealed", source)
		}

		copied := compiled
		if !copied.HasValidSQLSeal() {
			t.Fatalf("%q copied compiler result lost its seal", source)
		}
		copied.Args = append(copied.Args, "mutable-driver-argument")
		if !copied.HasValidSQLSeal() {
			t.Fatalf("%q argument mutation invalidated the structural seal", source)
		}

		for _, suffix := range []string{
			" SETTINGS max_execution_time = 0",
			"; SELECT currentUser()",
			" /* post-compile mutation */",
		} {
			mutated := compiled
			mutated.SQL += suffix
			if mutated.HasValidSQLSeal() {
				t.Fatalf("%q accepted SQL suffix %q", source, suffix)
			}
		}
	}
}

func TestCompiledQuerySQLSealCannotBeForgedThroughPublicFields(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | head 1`)
	forged := CompiledQuery{
		SQL:          compiled.SQL,
		Args:         compiled.Args,
		OutputFields: compiled.OutputFields,
		Timechart:    compiled.Timechart,
		Chart:        compiled.Chart,
		SparseFields: compiled.SparseFields,
	}
	if forged.HasValidSQLSeal() {
		t.Fatal("public CompiledQuery fields forged a compiler SQL seal")
	}

	mutated := compiled
	mutated.SQL = strings.Clone(compiled.SQL)
	if !mutated.HasValidSQLSeal() {
		t.Fatal("byte-identical SQL copy unexpectedly invalidated the seal")
	}
}
