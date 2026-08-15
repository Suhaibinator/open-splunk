package searchjobs

import "github.com/Suhaibinator/open-splunk/internal/clickhouse"

// ChartRowValueKind maps the compiler's declared row kind onto the public
// result kind. Both axes of a chart are runtime data, so the first column's
// kind is contract metadata rather than something re-derived from ClickHouse.
func ChartRowValueKind(kind clickhouse.ChartRowKind) (ValueKind, bool) {
	switch kind {
	case clickhouse.ChartRowKindString:
		return ValueKindString, true
	case clickhouse.ChartRowKindSigned:
		return ValueKindSigned, true
	case clickhouse.ChartRowKindUnsigned:
		return ValueKindUnsigned, true
	case clickhouse.ChartRowKindDouble:
		return ValueKindDouble, true
	case clickhouse.ChartRowKindBool:
		return ValueKindBool, true
	case clickhouse.ChartRowKindTime:
		return ValueKindTime, true
	case clickhouse.ChartRowKindMixed:
		return ValueKindMixed, true
	default:
		return ValueKindInvalid, false
	}
}
