package featureaudit

import (
	"context"
	"database/sql"
	"fmt"
)

func validateStartupIntegrity(ctx context.Context, database *sql.DB) error {
	tx, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin feature audit startup validation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	for _, trigger := range requiredTriggers {
		var present int
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM sqlite_master
				WHERE type = 'trigger' AND name = ?
			)
		`, trigger).Scan(&present); err != nil {
			return fmt.Errorf("verify feature audit trigger: %w", err)
		}
		if present != 1 {
			return fmt.Errorf("%w: required feature audit trigger is missing", ErrCorrupt)
		}
	}
	var corrupt int
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM feature_operation_audit_tenant_state AS state
			WHERE state.retained_count <> (
				SELECT COUNT(*)
				FROM feature_operation_audit_events AS event
				WHERE event.tenant_id = state.tenant_id
			)
			OR (
				state.retained_count = 0
				AND (
					state.first_sequence <> 1
					OR
					state.next_sequence <> 1
					OR state.last_occurred_at_unix_micro IS NOT NULL
				)
			)
			OR (
				state.retained_count > 0
				AND (
					state.next_sequence - state.first_sequence
						<> state.retained_count
					OR (
						SELECT MIN(event.sequence)
						FROM feature_operation_audit_events AS event
						WHERE event.tenant_id = state.tenant_id
					) <> state.first_sequence
					OR (
						SELECT MAX(event.sequence)
						FROM feature_operation_audit_events AS event
						WHERE event.tenant_id = state.tenant_id
					) <> state.next_sequence - 1
					OR state.last_occurred_at_unix_micro <> (
						SELECT event.occurred_at_unix_micro
						FROM feature_operation_audit_events AS event
						WHERE event.tenant_id = state.tenant_id
						  AND event.sequence = state.next_sequence - 1
					)
				)
			)
		)
		OR EXISTS (
			SELECT 1
			FROM feature_operation_audit_events AS event
			LEFT JOIN feature_operation_audit_tenant_state AS state
			  ON state.tenant_id = event.tenant_id
			LEFT JOIN feature_operation_audit_events AS previous
			  ON previous.tenant_id = event.tenant_id
			 AND previous.sequence = event.sequence - 1
			WHERE state.tenant_id IS NULL
			   OR event.feature NOT BETWEEN 1 AND 3
			   OR event.operation NOT BETWEEN 1 AND 18
			   OR event.outcome NOT BETWEEN 1 AND 14
			   OR event.items < 0
			   OR event.bytes < 0
			   OR (
					event.sequence > state.first_sequence
					AND (
						previous.sequence IS NULL
						OR event.occurred_at_unix_micro
							< previous.occurred_at_unix_micro
					)
			   )
		)
	`).Scan(&corrupt); err != nil {
		return fmt.Errorf("validate feature audit startup integrity: %w", err)
	}
	if corrupt != 0 {
		return ErrCorrupt
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit feature audit startup validation: %w", err)
	}
	committed = true
	return nil
}
