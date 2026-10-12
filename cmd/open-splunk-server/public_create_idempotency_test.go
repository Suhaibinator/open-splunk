package main

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/exportjournal"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeAuthenticatedCreatesReplayWithoutChangingIdentity(t *testing.T) {
	ctx := t.Context()
	directory := t.TempDir()
	db, err := openTestControl(t, ctx, filepath.Join(directory, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.CreateIndex(ctx, control.IndexDefinition{Name: "main", SearchEnabled: true}); err != nil {
		t.Fatal(err)
	}
	// The component fixture supplies membership through the trusted test
	// authorizer; materialized source filters still use the native mapping table.
	if err := db.GORMDB().Exec(`CREATE TABLE os_index_organizations(index_id text PRIMARY KEY, organization_id bigint NOT NULL)`).Error; err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(directory, "server.key")
	security, err := openRuntimeSecurityStores(ctx, db, keyPath, "tenant")
	if err != nil {
		t.Fatal(err)
	}
	history, err := openSearchHistoryStore(ctx, db, keyPath, searchhistory.Options{AuditAppender: security.searchAttemptAuditEvents, RequireSearchAttemptAudit: true})
	if err != nil {
		t.Fatal(err)
	}
	historyJournal, err := searchhistory.NewJobJournal(history)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := searchartifacts.New(ctx, searchartifacts.Config{DB: db.SQLDB(), Directory: filepath.Join(directory, "searches"), CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := artifacts.Close(); err != nil {
			t.Error(err)
		}
	})
	scheduled, err := newRuntimeScheduledReportLifecycle()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scheduled.Close)
	var sequence atomic.Uint64
	jobs, err := searchjobs.New(searchjobs.Config{
		Executor: runtimeFieldExecutionExecutor{}, Snapshotter: runtimeFieldExecutionSnapshotter{},
		Journal: searchjobs.NewCompositeJournal(artifacts, historyJournal, scheduled.journal),
		NewID:   func() string { return fmt.Sprintf("public-search-%d", sequence.Add(1)) }, CleanupInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := jobs.Close(); err != nil {
			t.Error(err)
		}
	})
	journal, err := exportjournal.New(db.GORMDB(), security.auditEvents)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := exportjobs.New(exportjobs.Config{Source: jobs, Journal: journal, ArtifactDir: filepath.Join(directory, "exports"), CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := exports.Close(); err != nil {
			t.Error(err)
		}
	})
	credential := []byte(runtimeAdministratorToken)
	config := runtimeServerConfig()
	config.IdentityAuthenticator, err = newRuntimeSessionAuthenticator(credential, "tenant", "1", auth.BrowserRoleUser)
	if err != nil {
		t.Fatal(err)
	}
	config.SearchJobs, config.SearchHistory, config.SavedSearches, config.Exports = jobs, history, security.savedSearches, exports
	config.SavedSearches = &runtimeSavedCreateFixture{AuditedStore: security.savedSearches, t: t}
	config.Indexes = db
	config.AdministrativeAllowedHosts = []string{"127.0.0.1"}
	handler, err := newRuntimeTestHandler(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	definition := &opensplunk.SearchDefinition{Spl: "index=main", IndexScope: []string{"main"}, TimeRange: &opensplunk.TimeRangeSpec{Earliest: new("-1h"), Latest: new("now"), Timezone: new("UTC")}}
	search := &opensplunk.CreateSearchJobRequest{Definition: definition, ClientRequestId: new("public-search-key-01")}
	unauthenticated := postRuntimeAppProto(t, handler, "/api/search/jobs/create", search, nil)
	if unauthenticated.Code != http.StatusUnauthorized || sequence.Load() != 0 {
		t.Fatalf("unauthenticated create = %d, admitted searches=%d", unauthenticated.Code, sequence.Load())
	}
	var firstSearch, replaySearch opensplunk.CreateSearchJobResponse
	postRuntimeProtoOK(t, handler, "/api/search/jobs/create", search, &firstSearch, credential)
	searchID := firstSearch.GetSearchJob().GetSearchJobId()
	waitForRuntimeKnowledgeJobState(t, jobs, searchID)
	postRuntimeProtoOK(t, handler, "/api/search/jobs/create", search, &replaySearch, credential)
	if firstSearch.GetReplayed() || !replaySearch.GetReplayed() || searchID == "" || replaySearch.GetSearchJob().GetSearchJobId() != searchID {
		t.Fatalf("search replay = %v / %v", &firstSearch, &replaySearch)
	}
	saved := &opensplunk.CreateSavedSearchRequest{ClientRequestId: new("public-saved-key-01"), Definition: &opensplunk.SavedSearchDefinition{Name: "Public saved", Search: proto.Clone(definition).(*opensplunk.SearchDefinition), SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE}}
	var firstSaved, replaySaved opensplunk.CreateSavedSearchResponse
	postRuntimeProtoOK(t, handler, "/api/saved-searches/create", saved, &firstSaved, credential)
	postRuntimeProtoOK(t, handler, "/api/saved-searches/create", saved, &replaySaved, credential)
	if firstSaved.GetReplayed() || !replaySaved.GetReplayed() || replaySaved.GetSavedSearch().GetSavedSearchId() != firstSaved.GetSavedSearch().GetSavedSearchId() {
		t.Fatalf("saved replay = %v / %v", &firstSaved, &replaySaved)
	}
	duplicate := &opensplunk.DuplicateSavedSearchRequest{ClientRequestId: new("public-duplicate-key-01"), SavedSearchId: firstSaved.GetSavedSearch().GetSavedSearchId(), NewName: "Public copy"}
	var firstDuplicate, replayDuplicate opensplunk.DuplicateSavedSearchResponse
	postRuntimeProtoOK(t, handler, "/api/saved-searches/duplicate", duplicate, &firstDuplicate, credential)
	postRuntimeProtoOK(t, handler, "/api/saved-searches/duplicate", duplicate, &replayDuplicate, credential)
	if firstDuplicate.GetReplayed() || !replayDuplicate.GetReplayed() || replayDuplicate.GetSavedSearch().GetSavedSearchId() != firstDuplicate.GetSavedSearch().GetSavedSearchId() {
		t.Fatalf("duplicate replay = %v / %v", &firstDuplicate, &replayDuplicate)
	}
	export := &opensplunk.CreateExportJobRequest{ClientRequestId: new("public-export-key-01"), Definition: &opensplunk.ExportDefinition{SearchJobId: searchID, FormatOptions: &opensplunk.ExportDefinition_Csv{Csv: &opensplunk.CsvExportOptions{}}}}
	var firstExport, replayExport opensplunk.CreateExportJobResponse
	postRuntimeProtoOK(t, handler, "/api/search/exports/create", export, &firstExport, credential)
	postRuntimeProtoOK(t, handler, "/api/search/exports/create", export, &replayExport, credential)
	if firstExport.GetReplayed() || !replayExport.GetReplayed() || replayExport.GetExportJob().GetExportJobId() != firstExport.GetExportJob().GetExportJobId() {
		t.Fatalf("export replay = %v / %v", &firstExport, &replayExport)
	}
	var receipts, attempts, audits int64
	if err := db.SQLDB().QueryRowContext(ctx, "SELECT count(*) FROM api_mutation_receipts WHERE tenant_id='tenant' AND actor_kind='browser' AND actor_id='1'").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := db.SQLDB().QueryRowContext(ctx, "SELECT count(*) FROM search_attempt_audit_events WHERE tenant_id='tenant' AND actor_kind='browser' AND actor_id='1'").Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.SQLDB().QueryRowContext(ctx, "SELECT count(*) FROM audit_events WHERE tenant_id='tenant' AND actor_kind='browser' AND actor_id='1' AND action IN ('saved_search.create','saved_search.duplicate','export.create')").Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if receipts != 4 || attempts != 1 || audits != 3 {
		t.Fatalf("durable receipts/admission audits = %d/%d/%d", receipts, attempts, audits)
	}
	unkeyed := proto.Clone(search).(*opensplunk.CreateSearchJobRequest)
	unkeyed.ClientRequestId = nil
	var ordinary opensplunk.CreateSearchJobResponse
	postRuntimeProtoOK(t, handler, "/api/search/jobs/create", unkeyed, &ordinary, credential)
	if ordinary.GetReplayed() || ordinary.GetSearchJob().GetSearchJobId() == searchID {
		t.Fatalf("unkeyed behavior changed: %v", &ordinary)
	}
	if err := db.SQLDB().QueryRowContext(ctx, "SELECT count(*) FROM api_mutation_receipts").Scan(&receipts); err != nil || receipts != 4 {
		t.Fatalf("unkeyed request wrote receipt: %d %v", receipts, err)
	}
}

type runtimeSavedCreateFixture struct {
	*savedobjects.AuditedStore
	t *testing.T
}

func (fixture *runtimeSavedCreateFixture) CreateIdempotent(ctx context.Context, scope savedobjects.AccessScope, definition *opensplunk.SavedSearchDefinition, intent requestidempotency.Intent) (*opensplunk.SavedSearch, bool, error) {
	result, replayed, err := fixture.AuditedStore.CreateIdempotent(ctx, scope, definition, intent)
	if err != nil {
		fixture.t.Logf("native saved-search idempotency failure: %v", err)
	}
	return result, replayed, err
}
