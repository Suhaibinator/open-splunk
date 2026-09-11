package visibility

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

// The caller has resolved any winning durable disposition before this check.
// A denial must precede every sequence, identity, and terminal-ledger mutation.
func planRejectionQuota(ctx context.Context, tx *sql.Tx, request RejectRequest) (*ingestquota.StateUpdate, error) {
	if request.RejectionAdmission == nil {
		return nil, nil
	}
	charge, err := request.RejectionAdmission.Charge(safecast.MustConv[uint64](len(request.Metadata)))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid rejection admission: %w", ErrInvalidArgument, err)
	}
	var state ingestquota.State
	err = tx.QueryRowContext(ctx, `
		SELECT max_rejections_per_second, max_metadata_bytes_per_second,
		       next_rejection_unix_nano, next_metadata_unix_nano, updated_at_unix_micro
		FROM ingest_rejection_buckets WHERE tenant_id = ? AND token_id = ?`,
		charge.Scope.TenantID, charge.Scope.Identity,
	).Scan(&state.Limits.MaxEventsPerSecond, &state.Limits.MaxUncompressedBytesPerSecond,
		&state.NextEventAdmissionUnixNano, &state.NextByteAdmissionUnixNano, &state.UpdatedAtUnixMicro)
	if err == nil {
		charge.State = &state
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("read rejection quota: %w", err)
	}
	decision, err := ingestquota.Evaluate(request.QuotaEvaluatedAt, ingestquota.Admission{
		Charges: []ingestquota.Charge{charge},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: evaluate rejection quota: %w", ErrInvalidArgument, err)
	}
	if !decision.Allowed {
		return nil, &ingestquota.ExceededError{Scope: decision.BlockingScope, RetryAfter: decision.RetryAfter}
	}
	return &decision.Updates[0], nil
}

func persistRejectionQuota(ctx context.Context, tx *sql.Tx, update ingestquota.StateUpdate) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_rejection_buckets (
			tenant_id, token_id, max_rejections_per_second, max_metadata_bytes_per_second,
			next_rejection_unix_nano, next_metadata_unix_nano, updated_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (tenant_id, token_id) DO UPDATE SET
			max_rejections_per_second = excluded.max_rejections_per_second,
			max_metadata_bytes_per_second = excluded.max_metadata_bytes_per_second,
			next_rejection_unix_nano = excluded.next_rejection_unix_nano,
			next_metadata_unix_nano = excluded.next_metadata_unix_nano,
			updated_at_unix_micro = excluded.updated_at_unix_micro`,
		update.Scope.TenantID, update.Scope.Identity,
		update.State.Limits.MaxEventsPerSecond, update.State.Limits.MaxUncompressedBytesPerSecond,
		update.State.NextEventAdmissionUnixNano, update.State.NextByteAdmissionUnixNano,
		update.State.UpdatedAtUnixMicro,
	)
	if err != nil {
		return fmt.Errorf("persist rejection quota: %w", err)
	}
	return nil
}
