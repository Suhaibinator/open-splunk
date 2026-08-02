package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
)

// indexReadCatalog is the durable half of search read admission. The live
// registry closes the deletion race; this lookup also rejects deleting and
// terminally tombstoned indexes after a process restart has forgotten its
// in-memory retirement set.
type indexReadCatalog interface {
	GetIndexesByNames(context.Context, []string) ([]control.Index, error)
}

type catalogIndexReadAdmission struct {
	catalog  indexReadCatalog
	live     indexread.Admission
	tenantID string
}

func newCatalogIndexReadAdmission(
	catalog indexReadCatalog,
	live indexread.Admission,
	tenantID string,
) (*catalogIndexReadAdmission, error) {
	if nilRuntimeDependency(catalog) {
		return nil, errors.New("create index read admission: catalog is required")
	}
	if nilRuntimeDependency(live) {
		return nil, errors.New("create index read admission: live admission is required")
	}
	if err := indexread.ValidateTenantID(tenantID); err != nil {
		return nil, errors.New("create index read admission: tenant ID is invalid")
	}
	return &catalogIndexReadAdmission{
		catalog:  catalog,
		live:     live,
		tenantID: strings.Clone(tenantID),
	}, nil
}

func (admission *catalogIndexReadAdmission) Acquire(
	ctx context.Context,
	tenantID string,
	indexNames []string,
) (context.Context, func(), error) {
	if admission == nil || nilRuntimeDependency(admission.catalog) ||
		nilRuntimeDependency(admission.live) {
		return nil, nil, errors.New("acquire index read: admission is unavailable")
	}
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: context is required", indexread.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	scope, err := indexread.NormalizeScope(tenantID, indexNames)
	if err != nil {
		return nil, nil, err
	}
	if scope.TenantID != admission.tenantID {
		return nil, nil, fmt.Errorf("%w: tenant scope is unavailable", indexread.ErrUnavailable)
	}

	records, err := admission.catalog.GetIndexesByNames(ctx, scope.IndexNames)
	switch {
	case errors.Is(err, control.ErrNotFound):
		return nil, nil, fmt.Errorf(
			"%w: one or more indexes are not in the live catalog",
			indexread.ErrUnavailable,
		)
	case err != nil:
		return nil, nil, fmt.Errorf("read indexes for read admission: %w", err)
	case len(records) != len(scope.IndexNames):
		return nil, nil, fmt.Errorf(
			"%w: catalog returned an incomplete index scope",
			indexread.ErrUnavailable,
		)
	}
	for position, name := range scope.IndexNames {
		record := records[position]
		switch {
		case record.Definition.Name != name:
			return nil, nil, fmt.Errorf(
				"%w: index %q identity changed",
				indexread.ErrUnavailable,
				name,
			)
		case record.State != control.IndexStateActive &&
			record.State != control.IndexStateArchived:
			return nil, nil, fmt.Errorf(
				"%w: index %q is not physically readable",
				indexread.ErrUnavailable,
				name,
			)
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
	}

	// Retirement is permanent and precedes physical mutation. Therefore a
	// deletion that races after the durable checks either has already retired
	// this scope (Acquire rejects) or will cancel and join the returned lease.
	return admission.live.Acquire(ctx, scope.TenantID, scope.IndexNames)
}

var _ indexread.Admission = (*catalogIndexReadAdmission)(nil)
