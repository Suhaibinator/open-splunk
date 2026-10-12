package main

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"gorm.io/gorm"
)

func newRuntimeIndexAdministration(
	database *control.DB,
	tenantID string,
	appender control.IndexMutationAuditAppender,
	binders ...func(context.Context, *gorm.DB, string) error,
) (*control.AuditedIndexAdministration, error) {
	validator, err := knowledgecatalog.NewIndexNameAdmissionValidator(database)
	if err != nil {
		return nil, fmt.Errorf("open knowledge index-name admission validator: %w", err)
	}
	var binder func(context.Context, *gorm.DB, string) error
	if len(binders) > 0 {
		binder = binders[0]
	}
	administration, err := control.NewAuditedIndexAdministration(
		database,
		control.AuditedIndexAdministrationOptions{
			TenantID:         tenantID,
			Appender:         appender,
			Validator:        validator,
			BindCreatedIndex: binder,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("open audited index administration: %w", err)
	}
	return administration, nil
}
