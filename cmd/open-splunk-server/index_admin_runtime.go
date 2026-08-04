package main

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func newRuntimeIndexAdministration(
	database *control.DB,
	tenantID string,
	appender control.IndexMutationAuditAppender,
) (*control.AuditedIndexAdministration, error) {
	administration, err := control.NewAuditedIndexAdministration(
		database,
		control.AuditedIndexAdministrationOptions{
			TenantID: tenantID,
			Appender: appender,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("open audited index administration: %w", err)
	}
	return administration, nil
}
