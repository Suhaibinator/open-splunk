package clickhouse

import "strings"

// Carry domain accounting and complete-source validation through one chain.
// Extra branches over the collapsed relation duplicate its upstream tree
// during ClickHouse analysis, even when the source CTE is materialized. Scalar
// windows let us guard the distinct labels before constructing their array.
func writeTimechartDomainCTEs(
	sql *strings.Builder,
	source, resourceUsage, domain, eligible string,
	bucketCount, cellBytes uint64,
) {
	q := quoteIdentifier
	encoded := q("__os_tc_encoded")
	invalid := q("__os_tc_invalid")
	collision := q("__os_tc_collision")
	member := q("__os_tc_domain_member")
	domainCount := q("__os_tc_domain_count")
	labelBytes := q("__os_tc_label_bytes")
	domainRows := q("__os_timechart_domain_rows")
	guardedRows := q("__os_timechart_guarded_domain_rows")

	// Keep the private empty encoding until the final aggregate: it carries
	// invalid/colliding rows excluded by series options or the visible grid.
	sql.WriteString(domainRows + " AS (SELECT " + encoded)
	sql.WriteString(", toUInt8(maxOrDefault(" + encoded + " != '' AND " + eligible + ")) AS " + member)
	sql.WriteString(", toUInt8(maxOrDefault(" + invalid + " != 0 OR " + collision + " != 0)) AS " + invalid)
	sql.WriteString(" FROM " + source + " GROUP BY " + encoded + "), ")
	sql.WriteString(resourceUsage + " AS (SELECT *, countIf(" + member + " != 0) OVER () AS " + domainCount)
	sql.WriteString(", sumIf(toUInt64(length(" + encoded + ")), " + member + " != 0) OVER () AS " + labelBytes)
	sql.WriteString(" FROM " + domainRows + "), ")
	sql.WriteString(guardedRows + " AS MATERIALIZED (SELECT * FROM " + resourceUsage + " WHERE ")
	sql.WriteString(timechartResourceGuardPredicate(resourceUsage, bucketCount, cellBytes))
	sql.WriteString("), ")

	sql.WriteString(domain)
	sql.WriteString(" AS MATERIALIZED (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArrayIf((multiIf(")
	sql.WriteString(encoded)
	sql.WriteString(" = '1:', toUInt8(1), ")
	sql.WriteString(encoded)
	sql.WriteString(" = '2:', toUInt8(2), toUInt8(0)), if(startsWith(")
	sql.WriteString(encoded)
	sql.WriteString(", '0:'), ")
	sql.WriteString(splunkSeriesLabelSQL("substring(" + encoded + ", 3)"))
	sql.WriteString(", CAST('' AS String)), ")
	sql.WriteString(encoded)
	sql.WriteString("), ")
	sql.WriteString(member)
	sql.WriteString(" != 0))) AS names, maxOrDefault(")
	sql.WriteString(invalid)
	sql.WriteString(") AS ")
	sql.WriteString(invalid)
	sql.WriteString(", maxOrDefault(" + domainCount + ") AS " + domainCount)
	sql.WriteString(", maxOrDefault(" + labelBytes + ") AS " + labelBytes)
	sql.WriteString(" FROM " + guardedRows + "), ")
}
