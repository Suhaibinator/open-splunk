package main

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
)

func newRuntimeIndexAdministration(
	database *control.DB,
	tenantID string,
	appender control.IndexMutationAuditAppender,
) (*control.AuditedIndexAdministration, error) {
	validator, err := knowledgecatalog.NewIndexNameAdmissionValidator(database)
	if err != nil {
		return nil, fmt.Errorf("open knowledge index-name admission validator: %w", err)
	}
	administration, err := control.NewAuditedIndexAdministration(
		database,
		control.AuditedIndexAdministrationOptions{
			TenantID:  tenantID,
			Appender:  appender,
			Validator: validator,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("open audited index administration: %w", err)
	}
	return administration, nil
}
