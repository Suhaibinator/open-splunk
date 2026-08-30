package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

type runtimeTrustedSearchAdmission struct {
	jobs    *searchjobs.Manager
	indexes *control.DB
	apps    *runtimeAppCatalog
	clock   func() time.Time
}

func (admission *runtimeTrustedSearchAdmission) AdmitScheduledReport(ctx context.Context, request scheduledreports.AdmissionRequest) (string, error) {
	if request.Definition == nil || request.Definition.GetSearch() == nil {
		return "", errors.New("scheduled report definition is missing search intent")
	}
	job, err := admission.admit(ctx, request.Definition.GetSearch(), request.OwnerID, request.TenantID, searchjobs.JobSource{
		Origin: searchjobs.JobOriginScheduledReport, ObjectID: request.RunID, ScheduledAt: request.ScheduledAt,
	}, request.RetentionLifetime, request.ScheduledAt)
	if err != nil {
		return "", err
	}
	return job.ID, nil
}

func (admission *runtimeTrustedSearchAdmission) AdmitAlertSearch(ctx context.Context, request alerts.SearchRequest) (string, error) {
	timezone := request.Timezone
	definition := &opensplunk.SearchDefinition{
		Spl: request.SPL, AppId: &request.Application,
		TimeRange:  &opensplunk.TimeRangeSpec{Earliest: &request.Earliest, Latest: &request.Latest, Timezone: &timezone},
		IndexScope: slices.Clone(request.IndexScope),
	}
	job, err := admission.admit(ctx, definition, request.OwnerID, request.TenantID, searchjobs.JobSource{
		Origin: searchjobs.JobOriginAlert, AlertID: request.AlertID, AlertRunID: request.AlertRunID, ScheduledAt: request.ScheduledAt,
	}, request.Retention, request.ScheduledAt)
	if err != nil {
		return "", err
	}
	return job.ID, nil
}

func (admission *runtimeTrustedSearchAdmission) admit(ctx context.Context, definition *opensplunk.SearchDefinition, ownerID, tenantID string, source searchjobs.JobSource, retention time.Duration, anchor time.Time) (searchjobs.Job, error) {
	if admission == nil || admission.jobs == nil || admission.indexes == nil || admission.apps == nil || definition == nil || definition.GetTimeRange() == nil {
		return searchjobs.Job{}, errors.New("trusted scheduled search admission is unavailable")
	}
	appID := strings.TrimSpace(definition.GetAppId())
	if appID == "" || appID != definition.GetAppId() {
		return searchjobs.Job{}, errors.New("scheduled search app is invalid")
	}
	if anchor.IsZero() {
		anchor = admission.now()
	}
	resolved, err := searchtime.Resolve(definition.GetTimeRange().GetEarliest(), definition.GetTimeRange().GetLatest(), definition.GetTimeRange().Timezone, anchor.UTC())
	if err != nil {
		return searchjobs.Job{}, err
	}
	return admission.AdmitTrustedSearch(ctx, server.TrustedSearchAdmissionRequest{
		SPL: definition.GetSpl(), OwnerID: ownerID, TenantID: tenantID, AppID: appID,
		IndexScope: slices.Clone(definition.GetIndexScope()), TimeRange: resolved,
		Source: source, RetentionLifetime: retention,
	})
}

func (admission *runtimeTrustedSearchAdmission) AdmitTrustedSearch(ctx context.Context, request server.TrustedSearchAdmissionRequest) (searchjobs.Job, error) {
	if admission == nil || admission.jobs == nil || admission.indexes == nil || admission.apps == nil {
		return searchjobs.Job{}, server.ErrTrustedSearchAuthorityUnavailable
	}
	appID := strings.TrimSpace(request.AppID)
	if appID != request.AppID {
		return searchjobs.Job{}, server.ErrTrustedSearchAppUnavailable
	}
	if appID != "" {
		if err := authorizeRuntimeSearchApp(ctx, admission.apps, request.TenantID, appID); err != nil {
			return searchjobs.Job{}, err
		}
	}
	requested := make([]string, 0, len(request.IndexScope))
	seen := make(map[string]struct{}, len(request.IndexScope))
	for _, raw := range request.IndexScope {
		name, normalizeErr := control.NormalizeIndexName(raw)
		if normalizeErr != nil {
			return searchjobs.Job{}, server.ErrTrustedSearchIndexUnavailable
		}
		if _, duplicate := seen[name]; !duplicate {
			seen[name] = struct{}{}
			requested = append(requested, name)
		}
	}
	if len(requested) == 0 {
		return searchjobs.Job{}, server.ErrTrustedSearchIndexUnavailable
	}
	indexes, err := admission.indexes.GetIndexesByNames(ctx, requested)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return searchjobs.Job{}, err
		}
		return searchjobs.Job{}, server.ErrTrustedSearchAuthorityUnavailable
	}
	if len(indexes) != len(requested) {
		return searchjobs.Job{}, server.ErrTrustedSearchAuthorityUnavailable
	}
	for index, record := range indexes {
		if record.Definition.Name != requested[index] || record.State != control.IndexStateActive || !record.Definition.SearchEnabled {
			return searchjobs.Job{}, server.ErrTrustedSearchIndexUnavailable
		}
	}
	return admission.jobs.Create(ctx, searchjobs.CreateRequest{
		SPL: request.SPL, OwnerID: request.OwnerID, TenantID: request.TenantID,
		AuthorizedIndexes: slices.Clone(requested), RequestedIndexes: requested,
		TimeRange: request.TimeRange, AppID: appID, Source: request.Source,
		RetentionLifetime: request.RetentionLifetime,
	})
}

func authorizeRuntimeSearchApp(ctx context.Context, apps *runtimeAppCatalog, tenantID, appID string) error {
	app, err := apps.GetApp(ctx, server.AppAdministrationScope{
		TenantID: tenantID,
	}, server.AppAdministrationSelector{AppID: appID})
	if errors.Is(err, server.ErrAppAdministrationNotFound) {
		return server.ErrTrustedSearchAppUnavailable
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return server.ErrTrustedSearchAuthorityUnavailable
	}
	if app.AppID != appID || app.State != server.AppAdministrationStateActive {
		return server.ErrTrustedSearchAppUnavailable
	}
	return nil
}

func (admission *runtimeTrustedSearchAdmission) now() time.Time {
	if admission.clock != nil {
		return admission.clock().UTC()
	}
	return time.Now().UTC()
}

func splitConfiguredValues(input string) []string {
	values := make([]string, 0)
	for candidate := range strings.SplitSeq(input, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate != "" {
			values = append(values, candidate)
		}
	}
	return values
}
