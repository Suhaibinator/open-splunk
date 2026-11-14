package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/featureaudit"
	"github.com/Suhaibinator/open-splunk/internal/featureops"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// Hold the shared pool only after the route has read app and index authority.
// This exercises a real resolver deadline instead of failing an earlier read.
type contendedAdmissionResolver struct {
	delegate searchjobs.KnowledgeResolver
	before   func(context.Context) error
	lastErr  error
}

func (resolver *contendedAdmissionResolver) Resolve(
	ctx context.Context,
	scope knowledgecatalog.ResolutionScope,
) (knowledgecatalog.Resolution, error) {
	if resolver.before != nil {
		if err := resolver.before(ctx); err != nil {
			return knowledgecatalog.Resolution{}, err
		}
	}
	resolved, err := resolver.delegate.Resolve(ctx, scope)
	resolver.lastErr = err
	return resolved, err
}

type contentionAdmissionJournal struct{ admissions atomic.Int32 }

func (journal *contentionAdmissionJournal) Admit(context.Context, searchjobs.Job) error {
	journal.admissions.Add(1)
	return nil
}

func (*contentionAdmissionJournal) Finalize(context.Context, searchjobs.Job) error { return nil }

func TestSearchAdmissionSharedPoolTimeoutDiagnosticsAndRecovery(t *testing.T) {
	const (
		tenantID = "admission-contention-tenant"
		ownerID  = "admission-contention-owner"
		appID    = "app_000000000900000000001Q"
		private  = "private-query-marker"
	)
	setupContext := t.Context()
	databasePath := filepath.Join(t.TempDir(), "private-control.sqlite")
	database, err := control.Open(setupContext, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.CreateIndex(setupContext, control.IndexDefinition{
		Name: "main", IngestionEnabled: true, SearchEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	cursorKey := []byte("admission-contention-cursor-key-at-least-32-bytes")
	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey:   cursorKey,
		IDGenerator: func() (string, error) { return appID, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apps.CreateApp(setupContext, control.AppAccessScope{TenantID: tenantID}, control.AppDefinition{
		Slug: "admission-contention", DisplayName: "Admission Contention",
	}); err != nil {
		t.Fatal(err)
	}
	catalog, err := knowledgecatalog.New(database, knowledgecatalog.Options{CursorKey: cursorKey})
	if err != nil {
		t.Fatal(err)
	}
	delegate, err := catalog.NewResolver(knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	audit, err := featureaudit.New(setupContext, database, featureaudit.Options{TenantID: tenantID})
	if err != nil {
		t.Fatal(err)
	}

	raw := database.SQLDB()
	poolSize := raw.Stats().MaxOpenConnections
	if poolSize <= 0 {
		t.Fatal("control database must have a bounded connection pool")
	}
	held := make([]*sql.Conn, 0, poolSize)
	release := func() {
		for _, connection := range held {
			if err := connection.Close(); err != nil {
				t.Errorf("release control connection: %v", err)
			}
		}
		held = nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	auditDone := make(chan error, 1)
	auditPending := false
	defer func() {
		cancel()
		release()
		if auditPending {
			select {
			case <-auditDone:
			case <-time.After(2 * time.Second):
				t.Error("background audit writer did not stop during cleanup")
			}
		}
	}()
	resolver := &contendedAdmissionResolver{delegate: delegate}
	resolver.before = func(acquireContext context.Context) error {
		for range poolSize {
			connection, err := raw.Conn(acquireContext)
			if err != nil {
				return fmt.Errorf("hold shared control connection: %w", err)
			}
			held = append(held, connection)
		}
		waitCount := raw.Stats().WaitCount
		auditPending = true
		go func() {
			_, appendErr := audit.Append(ctx, featureops.Event{
				Feature: featureops.FeatureDurableArtifacts, Operation: featureops.OperationCleanup,
				Outcome: featureops.OutcomeSucceeded, Items: 1,
			})
			auditDone <- appendErr
		}()
		// The background audit write must already be queued before resolution.
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for raw.Stats().WaitCount == waitCount {
			select {
			case <-acquireContext.Done():
				return acquireContext.Err()
			case <-ticker.C:
			}
		}
		return nil
	}
	journal := &contentionAdmissionJournal{}
	releasedExecution := make(chan struct{})
	close(releasedExecution)
	executor := &provenanceIntegrationExecutor{
		entered: make(chan struct{}), release: releasedExecution,
	}
	var allocatedIDs atomic.Int32
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: executor, Snapshotter: provenanceIntegrationSnapshotter(17), Journal: journal,
		KnowledgeResolver: resolver,
		Compiler:          clickhouse.Compiler{Database: "open_splunk", Table: "events"},
		MaxConcurrent:     1,
		CleanupInterval:   -1,
		Now:               func() time.Time { return testNow },
		NewID: func() string {
			return fmt.Sprintf("contention-search-%d", allocatedIDs.Add(1))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	core, logs := observer.New(zapcore.DebugLevel)
	handler := newTestHandler(t, Config{
		SearchJobs: manager, Indexes: database,
		AppCatalog: knowledgeSavedSearchAppCatalog{catalog: apps},
		BrowserAuthenticator: &knowledgeBoundaryAuthenticator{
			principal: browserGatePrincipal(t, tenantID, ownerID, auth.BrowserRoleAdministrator),
		},
		Logger: zap.New(core), WebUI: testUI(),
		OwnerID: ownerID, TenantID: tenantID, Now: func() time.Time { return testNow },
	})
	request := createRequest("-1h", "now", "main")
	request.Definition.AppId = new(appID)
	request.Definition.Spl = `index=main | where message="` + private + `" | table message`
	response := postProtoContext(t, ctx, handler, "/api/search/jobs/create", request)
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), "search service is unavailable") {
		t.Fatalf("contended search status/body = %d/%s", response.Code, response.Body.String())
	}
	if !errors.Is(resolver.lastErr, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("resolver error = %v, caller context = %v", resolver.lastErr, ctx.Err())
	}
	if allocatedIDs.Load() != 0 || journal.admissions.Load() != 0 || len(manager.List()) != 0 {
		t.Fatal("timed-out resolution allocated or admitted a search job")
	}
	entries := logs.FilterMessage("search admission rejected").All()
	if len(entries) != 1 {
		t.Fatalf("admission diagnostic count = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["failure_reason"] != "knowledge_unavailable" ||
		fields["failure_stage"] != "knowledge" || fields["cause_class"] != "context_deadline" ||
		fields["cause_deadline"] != true || fields["cause_canceled"] != false {
		t.Fatalf("timeout classification = %v", fields)
	}
	if fields["admission_step"] != "resolve_catalog" || fields["resolution_step"] != "begin_snapshot" ||
		fields["resolution_attempts"] != int64(1) {
		t.Fatalf("timeout steps/attempts = %v", fields)
	}
	if fields["control_db_pool_open"] != int64(poolSize) ||
		fields["control_db_pool_in_use"] != int64(poolSize) ||
		fields["control_db_pool_idle"] != int64(0) ||
		fields["control_db_pool_max_open"] != int64(poolSize) {
		t.Fatalf("pool exhaustion evidence = %v", fields)
	}
	if count, ok := fields["control_db_pool_wait_count_delta"].(int64); !ok || count < 1 {
		t.Fatalf("missing shared-pool wait count: %v", fields)
	}
	for _, field := range []string{
		"admission_elapsed_ms", "resolution_elapsed_ms", "control_db_pool_wait_duration_ms_delta",
	} {
		if elapsed, ok := fields[field].(float64); !ok || elapsed < 200 {
			t.Fatalf("missing elapsed wait in %s: %v", field, fields)
		}
	}
	if _, found := fields["sqlite_code"]; found {
		t.Fatalf("connection-pool timeout invented a SQLite driver code: %v", fields)
	}
	for _, sensitive := range []string{private, request.Definition.Spl, databasePath} {
		if strings.Contains(fmt.Sprint(logs.All()), sensitive) || strings.Contains(response.Body.String(), sensitive) {
			t.Fatal("admission diagnostics exposed query or database path")
		}
	}

	release()
	resolver.before = nil
	select {
	case appendErr := <-auditDone:
		auditPending = false
		if appendErr != nil {
			t.Fatalf("background audit write did not recover: %v", appendErr)
		}
	case <-ctx.Done():
		t.Fatal("background audit write stayed blocked after releasing the pool")
	}
	response = postProtoContext(t, ctx, handler, "/api/search/jobs/create", request)
	if response.Code != http.StatusOK {
		t.Fatalf("search after pool release status/body = %d/%s", response.Code, response.Body.String())
	}
	var created opensplunk.CreateSearchJobResponse
	unmarshalResponse(t, response, &created)
	if created.GetSearchJob().GetSearchJobId() == "" || allocatedIDs.Load() != 1 || journal.admissions.Load() != 1 {
		t.Fatalf("recovered search admission = %v, IDs=%d, journal=%d", &created, allocatedIDs.Load(), journal.admissions.Load())
	}
	select {
	case <-executor.entered:
	case <-ctx.Done():
		t.Fatalf("recovered search did not execute: jobs=%#v", manager.List())
	}
	if logs.FilterMessage("search admission rejected").Len() != 1 {
		t.Fatal("successful recovery emitted another admission rejection")
	}
}
