package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildRetainsExactParserOwnedEvalPredicateEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		want   uint32
	}{
		{
			name: "eval if and where",
			source: `index=gradethis | eval selected=if(status=200, "yes", "no") ` +
				`| where isnull(optional) OR selected="yes"`,
			want: 3,
		},
		{
			name:   "stats count eval",
			source: `index=gradethis | stats count(eval(status=200 AND isnull(optional))) AS matches`,
			want:   2,
		},
		{
			name:   "eventstats count eval",
			source: `index=gradethis | eventstats count(eval(status=200 OR isnull(optional))) AS matches`,
			want:   2,
		},
		{
			name:   "streamstats count eval",
			source: `index=gradethis | streamstats count(eval(status=200 OR isnull(optional))) AS matches`,
			want:   2,
		},
		{
			name: "case branches",
			source: `index=gradethis | eval selected=case(status=200, "ok", status=500, "error") ` +
				`| where selected="ok"`,
			want: 3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := spl.Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if count, ok := parsed.ParsedEvalPredicateCount(); !ok || count != test.want {
				t.Fatalf("parsed predicate evidence = (%d, %v), want (%d, true)", count, ok, test.want)
			}
			logical, err := Build(parsed, testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if count, ok := logical.AuthoredScalarPredicateCount(); !ok || count != test.want {
				t.Fatalf("logical predicate evidence = (%d, %v), want (%d, true)", count, ok, test.want)
			}
		})
	}
}

func TestAuthoredScalarPredicateCountRejectsDirectAndMutatedPlans(t *testing.T) {
	t.Parallel()

	parsed, err := spl.Parse(`index=gradethis | where status=200`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	logical, err := Build(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	logical.Operators = append(logical.Operators, logical.Operators[len(logical.Operators)-1])
	if count, ok := logical.AuthoredScalarPredicateCount(); ok || count != 0 {
		t.Fatalf("mutated predicate evidence = (%d, %v), want (0, false)", count, ok)
	}
	if count, ok := (&Query{Operators: logical.Operators}).AuthoredScalarPredicateCount(); ok || count != 0 {
		t.Fatalf("direct predicate evidence = (%d, %v), want (0, false)", count, ok)
	}
}
