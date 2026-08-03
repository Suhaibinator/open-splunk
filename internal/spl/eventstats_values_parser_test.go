package spl

func eventStatsValuesParserCase() eventStatsStringAggregateParserCase {
	return eventStatsStringAggregateParserCase{
		name:            "values",
		call:            "values",
		mixedCaseCall:   "VaLuEs",
		function:        AggregateFunctionValues,
		alias:           "unique_users",
		mixedCaseAlias:  "uniqueUsers",
		suggestionAlias: "distinct_values",
		unsupportedCall: "value",
	}
}
