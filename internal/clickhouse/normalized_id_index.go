package clickhouse

import (
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// The normalized candidate is a necessary condition for positive search
// equality; the unchanged residual still decides presence, null, and text
// semantics. Only immutable promoted ID lineage carries this permission.
func normalizedIDIndexCandidateEligible(expression *plan.ComparisonExpression, field fieldState) bool {
	return field.normalizedIDIndexEligible && field.kind == fieldKindString &&
		!field.caseSensitive && expression.Op == plan.ComparisonOpEqual &&
		expression.Value.Kind == plan.ValueKindString &&
		!strings.Contains(expression.Value.String, "*")
}

// Keep this expression identical to the additive normalized-ID Bloom indexes.
// Mapping null to empty can admit a false positive for an empty search, which
// the residual excludes; it cannot remove a matching non-null String.
func normalizedIDIndexExpressionSQL(valueSQL string) string {
	return "lowerUTF8(ifNull(" + valueSQL + ", ''))"
}
