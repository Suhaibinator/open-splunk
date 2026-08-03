package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func eventStatsListPlanCase() eventStatsStringAggregatePlanCase {
	return eventStatsStringAggregatePlanCase{
		name:        "list",
		call:        "list",
		output:      "recent_users",
		function:    AggregateFunctionList,
		splFunction: spl.AggregateFunctionList,
	}
}

func TestBuildEventStatsListPreservesInputOrdering(t *testing.T) {
	t.Parallel()

	ordered, err := Build(
		mustParse(
			t,
			`index=gradethis | sort 0 +_time`+
				` | eventstats list(user) AS recent_users`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build ordered list: %v", err)
	}
	if len(ordered.Operators) < 2 {
		t.Fatalf("operators = %#v, want Sort before EventAggregate", ordered.Operators)
	}
	if _, ok := ordered.Operators[len(ordered.Operators)-2].(*Sort); !ok {
		t.Fatalf(
			"operator before eventstats = %T, want *Sort",
			ordered.Operators[len(ordered.Operators)-2],
		)
	}
	if _, ok := ordered.Operators[len(ordered.Operators)-1].(*EventAggregate); !ok {
		t.Fatalf(
			"last operator = %T, want *EventAggregate",
			ordered.Operators[len(ordered.Operators)-1],
		)
	}
	if err := ValidateFieldAnalysisEligibility(ordered); err != nil {
		t.Fatalf("ordered field analysis eligibility: %v", err)
	}
	if err := ValidateTimelineEligibility(ordered); err != nil {
		t.Fatalf("ordered timeline eligibility: %v", err)
	}
}
