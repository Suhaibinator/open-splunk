package indexread

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/indexname"
)

const (
	maximumTenantIDBytes = 255

	// MaximumIndexesPerScope bounds validation, catalog admission, and live
	// registry work before any caller-controlled scope is retained.
	MaximumIndexesPerScope = 256
)

// NormalizedScope is a detached, canonical read scope. IndexNames is sorted
// and contains no duplicates, so downstream catalog and registry admission
// can share it without repeating normalization work.
type NormalizedScope struct {
	TenantID   string
	IndexNames []string
}

// NormalizeScope validates and defensively clones a bounded read scope. Every
// index name must already be canonical: normalization never silently changes
// the security scope selected by a compiled query.
func NormalizeScope(tenantID string, indexNames []string) (NormalizedScope, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return NormalizedScope{}, err
	}
	if len(indexNames) == 0 {
		return NormalizedScope{}, fmt.Errorf(
			"%w: index scope is empty",
			ErrInvalidArgument,
		)
	}
	if len(indexNames) > MaximumIndexesPerScope {
		return NormalizedScope{}, fmt.Errorf(
			"%w: index scope exceeds %d entries",
			ErrInvalidArgument,
			MaximumIndexesPerScope,
		)
	}

	names := make([]string, 0, len(indexNames))
	seen := make(map[string]struct{}, len(indexNames))
	for position, indexName := range indexNames {
		if !indexname.ValidCanonical(indexName) {
			return NormalizedScope{}, fmt.Errorf(
				"%w: index name at position %d is not canonical",
				ErrInvalidArgument,
				position,
			)
		}
		if _, duplicate := seen[indexName]; duplicate {
			continue
		}
		cloned := strings.Clone(indexName)
		seen[cloned] = struct{}{}
		names = append(names, cloned)
	}
	sort.Strings(names)

	return NormalizedScope{
		TenantID:   strings.Clone(tenantID),
		IndexNames: names,
	}, nil
}

// ValidateTenantID checks the shared bounded tenant identity contract used by
// read admission and by production runtime composition.
func ValidateTenantID(value string) error {
	if value == "" || len(value) > maximumTenantIDBytes ||
		!utf8.ValidString(value) || strings.TrimSpace(value) != value ||
		strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: invalid tenant ID", ErrInvalidArgument)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: invalid tenant ID", ErrInvalidArgument)
		}
	}
	return nil
}
