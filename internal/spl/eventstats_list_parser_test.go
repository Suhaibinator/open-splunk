package spl

func eventStatsListParserCase() eventStatsStringAggregateParserCase {
	return eventStatsStringAggregateParserCase{
		name:            "list",
		call:            "list",
		mixedCaseCall:   "LiSt",
		function:        AggregateFunctionList,
		alias:           "recent_users",
		mixedCaseAlias:  "recentUsers",
		suggestionAlias: "ordered_values",
		unsupportedCall: "collect",
	}
}
