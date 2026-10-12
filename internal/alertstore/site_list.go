package alertstore

import (
	"context"
	"database/sql"
	"errors"

	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"gorm.io/gorm"
)

const alertSummaryProjection = "organization_id, alert_id, version, tenant_id, owner_id, app_id, name, enabled, definition_proto, webhook_hostname, secret_generation, secret_rotated_at_unix_micro, next_run_at_unix_micro, last_outcome, last_outcome_scheduled_at_unix_micro, last_evaluated_at_unix_micro, last_delivered_at_unix_micro, created_at_unix_micro, updated_at_unix_micro"

// ListPage counts and pages a site administrator's selected organization (or
// all organizations) in one repeatable-read snapshot. Ordinary owners use the
// bounded catalog whose source authorization is applied by the server.
func (repository *SQLRepository) ListPage(ctx context.Context, ownerID string, request alerts.ListPageRequest) (alerts.ListPage, error) {
	scope, ok := alerts.AccessScopeFromContext(ctx)
	if !ok || !scope.SiteAdministrator || scope.OwnerID != ownerID || request.Limit < 1 || request.Limit > alerts.MaximumAlertsPerOwner {
		return alerts.ListPage{}, alerts.ErrInvalidArgument
	}
	tx := pgmeta.Begin(repository.db.WithContext(ctx), &sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return alerts.ListPage{}, mapSQLError(ctx, "begin alert catalog page", tx.Error)
	}
	defer tx.Rollback()
	base := func() *gorm.DB {
		q := scopedAlertQuery(tx.Model(&alertSQLRecord{}), "").Where("tenant_id = ?", repository.tenantID)
		if request.Application != "" {
			q = q.Where("app_id = ?", request.Application)
		}
		if request.Text != "" {
			q = q.Where("strpos(lower((name || E'\\n' || COALESCE(convert_from(definition_proto,'UTF8')::jsonb->>'Description','')) COLLATE \"default\"), ?) > 0", request.Text)
		}
		return q
	}
	page := alerts.ListPage{Alerts: []alerts.AlertSummary{}}
	if request.IncludeTotal {
		var count int64
		if err := base().Count(&count).Error; err != nil {
			return alerts.ListPage{}, mapSQLError(ctx, "count site alert catalog", err)
		}
		if count < 0 {
			return alerts.ListPage{}, errors.New("alerts: invalid catalog count")
		}
		total := uint64(count)
		page.TotalSize = &total
	}
	q := base()
	if request.AfterAlertID != "" {
		var boundary alertSQLRecord
		err := base().Select("alert_id, updated_at_unix_micro").Where("alert_id = ?", request.AfterAlertID).Take(&boundary).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return alerts.ListPage{}, alerts.ErrInvalidArgument
		}
		if err != nil {
			return alerts.ListPage{}, mapSQLError(ctx, "read alert page boundary", err)
		}
		q = q.Where("updated_at_unix_micro < ? OR (updated_at_unix_micro = ? AND alert_id < ?)", boundary.UpdatedAt, boundary.UpdatedAt, boundary.AlertID)
	}
	var records []alertSQLRecord
	if err := q.Select(alertSummaryProjection).Order("updated_at_unix_micro DESC, alert_id DESC").Limit(request.Limit + 1).Find(&records).Error; err != nil {
		return alerts.ListPage{}, mapSQLError(ctx, "page site alert catalog", err)
	}
	more := len(records) > request.Limit
	if more {
		records = records[:request.Limit]
	}
	for _, record := range records {
		definition, err := decodeDefinition(record.Definition)
		if err != nil {
			return alerts.ListPage{}, err
		}
		page.Alerts = append(page.Alerts, alertSummaryFromSQL(record, definition))
	}
	if more {
		page.NextAlertID = records[len(records)-1].AlertID
	}
	if err := tx.Commit().Error; err != nil {
		return alerts.ListPage{}, mapSQLError(ctx, "commit alert catalog page", err)
	}
	return page, nil
}
