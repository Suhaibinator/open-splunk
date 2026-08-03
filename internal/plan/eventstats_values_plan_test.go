package plan

import "github.com/Suhaibinator/open-splunk/internal/spl"

func eventStatsValuesPlanCase() eventStatsStringAggregatePlanCase {
	return eventStatsStringAggregatePlanCase{
		name:        "values",
		call:        "values",
		output:      "unique_users",
		function:    AggregateFunctionValues,
		splFunction: spl.AggregateFunctionValues,
	}
}
