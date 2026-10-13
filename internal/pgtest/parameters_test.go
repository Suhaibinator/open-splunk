package pgtest

import "testing"

func TestParametersPreservesQuotedQuestionMarksAndNativeBindings(t *testing.T) {
	template := `UPDATE "table?" SET value=?, note='literal? ''quoted?''', "column?"=? WHERE id=?`
	want := `UPDATE "table?" SET value=$1, note='literal? ''quoted?''', "column?"=$2 WHERE id=$3`
	if got := Parameters(template); got != want {
		t.Fatalf("native template = %q, want %q", got, want)
	}
	native := `SELECT $1, 'question?', "identifier?"`
	if got := Parameters(native); got != native {
		t.Fatalf("native SQL changed to %q", got)
	}
}
