package plan

import "testing"

func TestSpathRemainsEligibleForCompletedJobFieldAnalysis(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | spath output=value path=payload.value | where value>=7`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(logical); err != nil {
		t.Fatalf("ValidateFieldAnalysisEligibility: %v", err)
	}
}

func TestSpathTimelineEligibilityTracksCanonicalTimeOutput(t *testing.T) {
	t.Parallel()

	ordinary, err := Build(
		mustParse(t, `index=gradethis | spath output=value path=payload.value`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build ordinary: %v", err)
	}
	if err := ValidateTimelineEligibility(ordinary); err != nil {
		t.Fatalf("ValidateTimelineEligibility ordinary: %v", err)
	}

	replaced, err := Build(
		mustParse(t, `index=gradethis | spath output=_time path=timestamp`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build replaced _time: %v", err)
	}
	assertDiagnosticCode(t, ValidateTimelineEligibility(replaced), timelineTimeDiagnosticCode)
}
