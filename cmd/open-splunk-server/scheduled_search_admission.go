package main

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

type runtimeTrustedSearchAdmission struct {
	jobs     *searchjobs.Manager
	indexes  *control.DB
	apps     *runtimeAppCatalog
	clock    func() time.Time
	identity *runtimeIdentity
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
	job, _, err := admission.admitTrustedSearch(ctx, request, nil)
	return job, err
}

func (admission *runtimeTrustedSearchAdmission) ReplayTrustedSearch(
	ctx context.Context,
	access searchjobs.AccessScope,
	intent requestidempotency.Intent,
) (searchjobs.Job, bool, error) {
	if admission == nil || admission.jobs == nil {
		return searchjobs.Job{}, false, requestidempotency.ErrUnavailable
	}
	return admission.jobs.ReplayIdempotent(ctx, access, intent)
}

func (admission *runtimeTrustedSearchAdmission) AdmitTrustedSearchIdempotent(
	ctx context.Context,
	request server.TrustedSearchAdmissionRequest,
	intent requestidempotency.Intent,
) (searchjobs.Job, bool, error) {
	return admission.admitTrustedSearch(ctx, request, &intent)
}

func (admission *runtimeTrustedSearchAdmission) admitTrustedSearch(
	ctx context.Context,
	request server.TrustedSearchAdmissionRequest,
	intent *requestidempotency.Intent,
) (searchjobs.Job, bool, error) {
	if admission == nil || admission.jobs == nil || admission.indexes == nil || admission.apps == nil {
		return searchjobs.Job{}, false, server.ErrTrustedSearchAuthorityUnavailable
	}
	appID := strings.TrimSpace(request.AppID)
	if appID != request.AppID {
		return searchjobs.Job{}, false, server.ErrTrustedSearchAppUnavailable
	}
	if admission.identity == nil {
		return searchjobs.Job{}, false, server.ErrTrustedSearchAuthorityUnavailable
	}
	organizationID := request.OrganizationID
	if organizationID == "" {
		if appID == "" {
			return searchjobs.Job{}, false, server.ErrTrustedSearchAppUnavailable
		}
		var owningOrg int64
		if err := admission.indexes.SQLDB().QueryRowContext(ctx, "SELECT organization_id FROM app_workspaces WHERE tenant_id=$1 AND app_id=$2", request.TenantID, appID).Scan(&owningOrg); err != nil {
			return searchjobs.Job{}, false, server.ErrTrustedSearchAppUnavailable
		}
		organizationID = strconv.FormatInt(owningOrg, 10)
	}
	fresh, subject, err := admission.identity.freshAccessAndSubject(ctx, request.OwnerID, organizationID)
	if err != nil {
		return searchjobs.Job{}, false, err
	}
	if request.TenantID != fresh.TenantID {
		return searchjobs.Job{}, false, authorization.ErrForbidden
	}
	org, _ := strconv.ParseInt(organizationID, 10, 64)
	if caller, ok := authorization.FromContext(ctx); ok && caller.Subject.UserID != subject.UserID {
		return searchjobs.Job{}, false, authorization.ErrForbidden
	}
	ctx = authorization.WithScope(ctx, authorization.Scope{Subject: subject, OrganizationID: org, PhysicalTenant: fresh.TenantID})
	if appID != "" {
		if err := authorizeRuntimeSearchApp(ctx, admission.apps, request.TenantID, appID); err != nil {
			return searchjobs.Job{}, false, err
		}
	}
	requested := make([]string, 0, len(request.IndexScope))
	seen := make(map[string]struct{}, len(request.IndexScope))
	for _, raw := range request.IndexScope {
		name, normalizeErr := control.NormalizeIndexName(raw)
		if normalizeErr != nil {
			return searchjobs.Job{}, false, server.ErrTrustedSearchIndexUnavailable
		}
		if _, duplicate := seen[name]; !duplicate {
			seen[name] = struct{}{}
			requested = append(requested, name)
		}
	}
	if len(requested) == 0 {
		return searchjobs.Job{}, false, server.ErrTrustedSearchIndexUnavailable
	}
	indexes, err := admission.indexes.GetIndexesByNames(ctx, requested)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return searchjobs.Job{}, false, err
		}
		return searchjobs.Job{}, false, server.ErrTrustedSearchAuthorityUnavailable
	}
	if len(indexes) != len(requested) {
		return searchjobs.Job{}, false, server.ErrTrustedSearchAuthorityUnavailable
	}
	for index, record := range indexes {
		if record.Definition.Name != requested[index] || record.State != control.IndexStateActive || !record.Definition.SearchEnabled {
			return searchjobs.Job{}, false, server.ErrTrustedSearchIndexUnavailable
		}
		if err := admission.identity.authorization.AuthorizeIndex(ctx, subject, org, record.ID, authorization.Search); err != nil {
			return searchjobs.Job{}, false, err
		}
	}
	create := searchjobs.CreateRequest{
		SPL: request.SPL, OwnerID: request.OwnerID, TenantID: request.TenantID, OrganizationID: organizationID,
		AuthorizedIndexes: slices.Clone(requested), RequestedIndexes: requested,
		TimeRange: request.TimeRange, AppID: appID, Source: request.Source,
		RetentionLifetime: request.RetentionLifetime,
	}
	if intent != nil {
		return admission.jobs.CreateIdempotent(ctx, create, *intent)
	}
	job, err := admission.jobs.Create(ctx, create)
	return job, false, err
}

func authorizeRuntimeSearchApp(ctx context.Context, apps *runtimeAppCatalog, tenantID, appID string) error {
	app, err := apps.GetApp(ctx, server.AppAdministrationScope{
		TenantID: tenantID, OrganizationID: runtimeAppReadScope(ctx, tenantID).OrganizationID, SiteAdministrator: runtimeAppReadScope(ctx, tenantID).SiteAdministrator,
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
