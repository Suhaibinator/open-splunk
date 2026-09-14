package queryexec

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// Legacy callers use the nil-poller path through the same conversion logic.

func convertSparseEventFieldsWithCache(
	value any,
	fieldNames []string,
	allowSubset bool,
	cache *resultMetadataCache,
	budget *resultMetadataCacheBudget,
) (searchjobs.Value, error) {
	return (*resultValueDecoder)(nil).convertSparseEventFieldsWithCache(value, fieldNames, allowSubset, cache, budget)
}

func convertValue(value any) (searchjobs.Value, error) {
	return (*resultValueDecoder)(nil).convertValue(value)
}

func convertContainerOutputWithCache(
	value any,
	names []string,
	types []uint8,
	metadataVersion uint8,
	cache *resultMetadataCache,
	budget *resultMetadataCacheBudget,
) (searchjobs.Value, error) {
	return (*resultValueDecoder)(nil).convertContainerOutputWithCache(value, names, types, metadataVersion, cache, budget)
}

func convertOptionalDynamicMultivalue(raw []chcol.Dynamic) (searchjobs.Value, error) {
	return (*resultValueDecoder)(nil).convertOptionalDynamicMultivalue(raw)
}

func convertOptionalMultivalueOutput(
	destinations []any,
	valueColumn int,
	transport resultOptionalMultivalueTransport,
) (searchjobs.Value, error) {
	return (*resultValueDecoder)(nil).convertOptionalMultivalueOutput(destinations, valueColumn, transport)
}
