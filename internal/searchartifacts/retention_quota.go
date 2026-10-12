package searchartifacts

import (
	"context"
	"database/sql"
	"errors"
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
)

// RetentionQuota joins completed snapshots and private export reservations in
// the same transactional organization and instance storage budget. Reservations
// survive cancellation and are released only after their objects are reclaimed.
type RetentionQuota interface {
	ReserveExport(context.Context, string, string, uint64) error
	ReleaseExport(context.Context, string) error
}

type RetentionUsage struct{ OrganizationBytes, GlobalBytes, OrganizationLimit, GlobalLimit uint64 }

const retainedUsageSQL = `SELECT COALESCE(SUM(reserved_bytes),0), COALESCE(SUM(CASE WHEN organization_id=$1 THEN reserved_bytes ELSE 0 END),0) FROM (SELECT organization_id,reserved_bytes FROM search_capture_sessions UNION ALL SELECT organization_id,reserved_bytes FROM search_retained_export_reservations) AS reservations`

func (store *Store) ReserveExport(ctx context.Context, organizationID, exportID string, bytes uint64) error {
	organization, err := strconv.ParseInt(organizationID, 10, 64)
	if err != nil || organization <= 0 || strconv.FormatInt(organization, 10) != organizationID || exportID == "" {
		return ErrInvalid
	}
	options, err := store.captureOptions.resolved()
	if err != nil {
		return err
	}
	tx, err := pgmeta.BeginSQL(ctx, store.db, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var prior uint64
	err = tx.QueryRowContext(ctx, `SELECT reserved_bytes FROM search_retained_export_reservations WHERE export_job_id=$1 AND organization_id=$2`, exportID, organization).Scan(&prior)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var global, org uint64
	if err = tx.QueryRowContext(ctx, retainedUsageSQL, organization).Scan(&global, &org); err != nil {
		return err
	}
	if prior > global || prior > org {
		return ErrCorrupt
	}
	global -= prior
	org -= prior
	if global > options.GlobalBytes || org > options.OrganizationBytes || bytes > options.GlobalBytes-global || bytes > options.OrganizationBytes-org {
		return ErrCapacity
	}
	result, execErr := tx.ExecContext(ctx, `INSERT INTO search_retained_export_reservations(export_job_id,organization_id,reserved_bytes) VALUES($1,$2,$3) ON CONFLICT(export_job_id) DO UPDATE SET reserved_bytes=EXCLUDED.reserved_bytes WHERE search_retained_export_reservations.organization_id=EXCLUDED.organization_id`, exportID, organization, bytes)
	if execErr != nil {
		return execErr
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrConflict
	}
	return tx.Commit()
}
func (store *Store) ReleaseExport(ctx context.Context, exportID string) error {
	tx, err := pgmeta.BeginSQL(ctx, store.db, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `DELETE FROM search_retained_export_reservations WHERE export_job_id=$1`, exportID); err != nil {
		return err
	}
	return tx.Commit()
}
func (store *Store) RetentionUsage(ctx context.Context, organizationID string) (RetentionUsage, error) {
	organization, err := strconv.ParseInt(organizationID, 10, 64)
	if err != nil || organization <= 0 || strconv.FormatInt(organization, 10) != organizationID {
		return RetentionUsage{}, ErrInvalid
	}
	options, err := store.captureOptions.resolved()
	if err != nil {
		return RetentionUsage{}, err
	}
	usage := RetentionUsage{OrganizationLimit: options.OrganizationBytes, GlobalLimit: options.GlobalBytes}
	err = store.db.QueryRowContext(ctx, retainedUsageSQL, organization).Scan(&usage.GlobalBytes, &usage.OrganizationBytes)
	return usage, err
}

func (store *Store) CaptureLimits() (uint64, uint64) {
	options, err := store.captureOptions.resolved()
	if err != nil {
		return 0, 0
	}
	return options.MaximumRows, options.MaximumBytes
}
