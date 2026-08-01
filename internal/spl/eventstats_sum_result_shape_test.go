package spl

import "testing"

func TestClassifyResultShapeTreatsEventStatsSumAsRowPreserving(t *testing.T) {
	t.Parallel()

	query, err := Parse(
		"index=main | eventstats sum(bytes) AS total_bytes BY level",
	)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := ClassifyResultShape(query); got != (ResultShape{
		Kind: ResultKindEvents,
	}) {
		t.Fatalf("ClassifyResultShape = %#v, want event results", got)
	}
}
