package audit

import (
	"context"
	"database/sql"
	"fmt"

	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
)

var _ lookupcatalog.MutationAuditAppender = (*Store)(nil)

// AppendLookupMutationInTransaction binds the lookup catalog's caller-owned
// database/sql transaction to the audit store. Neither side commits: any audit
// failure therefore rolls the lookup mutation and its asset publication back.
func (store *Store) AppendLookupMutationInTransaction(
	ctx context.Context,
	transaction lookupcatalog.MutationTransaction,
	tenantID string,
	definition lookupcatalog.MutationAuditEvent,
) error {
	action := Action(definition.Action)
	event := SuccessfulEvent{
		OccurredAt:    definition.OccurredAt,
		Action:        action,
		TargetKind:    TargetKindLookup,
		TargetID:      definition.LookupID,
		TargetVersion: definition.LookupVersion,
	}
	if err := validateAppendInputs(ctx, store, tenantID, event); err != nil {
		return err
	}
	connection, ok := transaction.(gorm.ConnPool)
	if !ok {
		return fmt.Errorf(
			"%w: lookup audit append requires a SQL transaction",
			control.ErrInvalidArgument,
		)
	}
	switch connection.(type) {
	case *sql.Tx, *sql.Conn:
	default:
		return fmt.Errorf(
			"%w: lookup audit append requires a SQL transaction",
			control.ErrInvalidArgument,
		)
	}
	database := store.orm.WithContext(ctx).Session(&gorm.Session{NewDB: true})
	database.Statement.ConnPool = connection
	if _, err := store.appendWithDatabase(ctx, database, tenantID, event); err != nil {
		return fmt.Errorf("append lookup mutation audit: %w", err)
	}
	return nil
}
