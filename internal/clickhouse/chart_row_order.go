package clickhouse

// chartRowSortSQL orders the published row value exactly as an automatic sort
// over the corresponding stats group column. Mixed rows carry a private byte
// tag; their public text determines order and the tag only breaks exact ties.
func chartRowSortSQL(valueSQL string, kind ChartRowKind) string {
	text := valueSQL
	switch kind {
	case ChartRowKindMixed:
		text = "tupleElement(" + valueSQL + ", 1)"
	case ChartRowKindString:
	default:
		return valueSQL
	}
	return "tuple(" + autoSortOrderingKeySQL(text, text) + ", " + valueSQL + ")"
}
