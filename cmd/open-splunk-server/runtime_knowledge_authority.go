package main

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func (runtime *runtimeIdentity) authorizeKnowledgeSources(ctx context.Context, access searchjobs.AccessScope, job searchjobs.Job) error {
	if access.SiteAdministrator {
		return nil
	}
	objectIDs, err := knowledgesnapshot.NormalizeObjectIDs(job.KnowledgeSnapshot, job.KnowledgeObjectIDs)
	if err != nil {
		return authorization.ErrForbidden
	}
	lookupIDs := make([]string, 0)
	if job.KnowledgeSnapshot != nil {
		if err := knowledgesnapshot.ValidateSummary(job.KnowledgeSnapshot); err != nil {
			return authorization.ErrForbidden
		}
		for _, asset := range job.KnowledgeSnapshot.GetLookupAssets() {
			lookupIDs = append(lookupIDs, asset.GetLookupId())
		}
	}
	organization, err := strconv.ParseInt(job.OrganizationID, 10, 64)
	if err != nil || organization <= 0 {
		return authorization.ErrForbidden
	}
	return authorizeRuntimeSourceRegistries(ctx, runtime.indexes.SQLDB(), access, organization, job.AppID, objectIDs, lookupIDs)
}

// The immutable inventory contains only registry IDs. Read current visibility
// without loading private definitions or lookup row payloads.
func authorizeRuntimeSourceRegistries(ctx context.Context, database *sql.DB, access searchjobs.AccessScope, organization int64, appID string, objectIDs, lookupIDs []string) error {
	if access.SiteAdministrator {
		return nil
	}
	if (len(objectIDs) != 0 || len(lookupIDs) != 0) && appID == "" {
		return authorization.ErrForbidden
	}
	for _, source := range []struct {
		ids   []string
		query string
	}{
		{objectIDs, `SELECT count(*) FROM knowledge_objects AS object
		 JOIN app_workspaces AS app ON app.tenant_id=object.tenant_id AND app.app_id=object.app_id AND app.organization_id=object.organization_id
		 WHERE object.knowledge_object_id=ANY($1::text[]) AND object.tenant_id=$2 AND object.organization_id=$3
		 AND object.state='active' AND app.state='active'
		 AND ((object.sharing_scope='private' AND object.owner_id=$4 AND object.app_id=$5)
		 OR (object.sharing_scope='app' AND object.app_id=$5) OR object.sharing_scope='global')`},
		{lookupIDs, `SELECT count(*) FROM knowledge_lookup_definitions AS lookup
		 JOIN app_workspaces AS app ON app.tenant_id=lookup.tenant_id AND app.app_id=lookup.app_id AND app.organization_id=lookup.organization_id
		 WHERE lookup.lookup_id=ANY($1::text[]) AND lookup.tenant_id=$2 AND lookup.organization_id=$3
		 AND lookup.state='ACTIVE' AND app.state='active'
		 AND ((lookup.sharing_scope=1 AND lookup.owner_id=$4 AND lookup.app_id=$5)
		 OR (lookup.sharing_scope=2 AND lookup.app_id=$5) OR lookup.sharing_scope=3)`},
	} {
		if len(source.ids) == 0 {
			continue
		}
		var visible int
		if err := database.QueryRowContext(ctx, source.query, source.ids, access.TenantID, organization, access.OwnerID, appID).Scan(&visible); err != nil {
			return err
		}
		if visible != len(source.ids) {
			return authorization.ErrForbidden
		}
	}
	return nil
}
