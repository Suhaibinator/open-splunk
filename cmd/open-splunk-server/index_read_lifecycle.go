package main

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/indexread"
)

// indexReadLifecycle owns the single live registry shared by production read
// admission and physical-deletion retirement. Callers receive the two narrow
// capabilities, but cannot accidentally construct them from different
// registries at this composition boundary.
type indexReadLifecycle struct {
	admission  indexread.Admission
	retirement indexread.Retirement
}

func newIndexReadLifecycle(
	catalog indexReadCatalog,
	tenantID string,
) (*indexReadLifecycle, error) {
	registry := indexread.NewRegistry()
	admission, err := newCatalogIndexReadAdmission(
		catalog,
		registry,
		tenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("create index read lifecycle: %w", err)
	}
	return &indexReadLifecycle{
		admission:  admission,
		retirement: registry,
	}, nil
}
