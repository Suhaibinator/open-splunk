package plan

import (
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestPipelineTimelineAcceptsAccumAndAddInfoAsEventPreservingLowerings(t *testing.T) {
	tests := []string{
		`index=gradethis | accum bytes`,
		`index=gradethis | accum bytes AS running`,
		`index=gradethis | addinfo`,
		`index=gradethis | addinfo | accum bytes AS running | reverse`,
	}
	for _, source := range tests {
		t.Run(source, func(t *testing.T) {
			scope := testScope([]string{"gradethis"}, nil)
			scope.SearchJobID = "pipeline-timeline-job"
			logical, err := Build(mustParse(t, source), scope)
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			if err := ValidateTimelineEligibility(logical); err != nil {
				t.Fatalf("ValidateTimelineEligibility() error = %v", err)
			}
		})
	}
}

func TestPipelineTimelineLocatesAccumCanonicalTimeReplacementAtDestination(t *testing.T) {
	const source = `index=gradethis | accum bytes AS _time`
	parsed := mustParse(t, source)
	command, ok := parsed.Commands[0].(*spl.AccumCommand)
	if !ok {
		t.Fatalf("parsed command = %T, want *spl.AccumCommand", parsed.Commands[0])
	}
	logical, err := Build(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	err = ValidateTimelineEligibility(logical)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != timelineTimeDiagnosticCode {
		t.Fatalf("ValidateTimelineEligibility() error = %v, want %s", err, timelineTimeDiagnosticCode)
	}
	if diagnostic.Range != command.OutputRange {
		t.Fatalf("diagnostic range = %+v, want exact accum destination %+v", diagnostic.Range, command.OutputRange)
	}
	if got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != "_time" {
		t.Fatalf("diagnostic located %q, want _time", got)
	}
}
