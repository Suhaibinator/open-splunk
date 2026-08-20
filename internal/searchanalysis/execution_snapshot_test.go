package searchanalysis

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

var searchAnalysisAuthorityCursorKey = []byte(
	"search-analysis-authority-cursor-key-at-least-32-bytes",
)

const searchAnalysisAuthorityAppID = "app_aaaaaaaaaaaaaaaaaaaaaA"

type searchAnalysisSnapshotExecutor struct{}

func (searchAnalysisSnapshotExecutor) Execute(
	_ context.Context,
	query clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	fields := slices.Clone(query.OutputFields)
	if len(fields) == 0 {
		fields = []string{"_raw"}
	}
	columns := make([]searchjobs.Column, len(fields))
	for index, field := range fields {
		columns[index] = searchjobs.Column{
			Name: field,
			Kind: searchjobs.ValueKindString,
		}
	}
	return sink.SetSchema(searchjobs.Schema{Columns: columns})
}

type searchAnalysisSnapshotter uint64

func (snapshotter searchAnalysisSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return uint64(snapshotter), nil
}

// sealSearchAnalysisSnapshot converts a test's explicit public scope into the
// same private manager-attested ExecutionSnapshot accepted by production
// completed-search analysis. Tests that deliberately change a signed field
// must call this helper again instead of mutating the sealed value in place.
func sealSearchAnalysisSnapshot(
	template searchjobs.ExecutionSnapshot,
) (searchjobs.ExecutionSnapshot, error) {
	return sealSearchAnalysisSnapshotWithResolver(template, nil)
}

func sealSearchAnalysisSnapshotWithResolver(
	template searchjobs.ExecutionSnapshot,
	resolver searchjobs.KnowledgeResolver,
) (searchjobs.ExecutionSnapshot, error) {
	return sealSearchAnalysisSnapshotWithCompiler(
		template,
		resolver,
		clickhouse.Compiler{},
	)
}

func sealSearchAnalysisSnapshotWithCompiler(
	template searchjobs.ExecutionSnapshot,
	resolver searchjobs.KnowledgeResolver,
	compiler clickhouse.Compiler,
) (searchjobs.ExecutionSnapshot, error) {
	return sealSearchAnalysisSnapshotWithAuthorities(
		template,
		resolver,
		nil,
		compiler,
	)
}

func sealSearchAnalysisSnapshotWithAuthorities(
	template searchjobs.ExecutionSnapshot,
	resolver searchjobs.KnowledgeResolver,
	lookupResolver searchjobs.LookupResolver,
	compiler clickhouse.Compiler,
) (searchjobs.ExecutionSnapshot, error) {
	timezone := template.SearchTimezone
	if timezone == "" {
		timezone = "UTC"
	}
	resolved, err := searchtime.Resolve(
		template.Earliest.UTC().Format(time.RFC3339Nano),
		template.Latest.UTC().Format(time.RFC3339Nano),
		&timezone,
		template.SearchStart,
	)
	if err != nil {
		return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
			"resolve search-analysis snapshot time range: %w",
			err,
		)
	}
	retention := template.ExpiresAt.Sub(template.SearchStart)
	if retention <= 0 {
		retention = time.Hour
	}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:          searchAnalysisSnapshotExecutor{},
		Snapshotter:       searchAnalysisSnapshotter(template.VisibilityCutoff),
		Compiler:          compiler,
		KnowledgeResolver: resolver,
		LookupResolver:    lookupResolver,
		RetentionTTL:      retention,
		CleanupInterval:   -1,
		Now:               func() time.Time { return template.SearchStart },
		NewID:             func() string { return template.ID },
	})
	if err != nil {
		return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
			"create search-analysis snapshot manager: %w",
			err,
		)
	}
	defer func() { _ = manager.Close() }()
	request := searchjobs.CreateRequest{
		SPL:               template.SPL,
		OwnerID:           template.OwnerID,
		TenantID:          template.TenantID,
		AppID:             template.AppID,
		AuthorizedIndexes: slices.Clone(template.EffectiveIndexes),
		RequestedIndexes:  slices.Clone(template.EffectiveIndexes),
		TimeRange:         resolved,
	}
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
			"create search-analysis snapshot: %w",
			err,
		)
	}
	access := searchjobs.AccessScope{
		TenantID: template.TenantID,
		OwnerID:  template.OwnerID,
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		job, getErr := manager.GetFor(access, created.ID)
		if getErr != nil {
			return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
				"get search-analysis snapshot job: %w",
				getErr,
			)
		}
		switch job.State {
		case searchjobs.StateCompleted:
			snapshot, snapshotErr := manager.CompletedExecutionSnapshotFor(
				context.Background(),
				access,
				created.ID,
			)
			if snapshotErr != nil {
				return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
					"open search-analysis execution snapshot: %w",
					snapshotErr,
				)
			}
			return snapshot, nil
		case searchjobs.StateFailed, searchjobs.StateCanceled:
			return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
				"search-analysis snapshot job ended in %v",
				job.State,
			)
		}
		if time.Now().After(deadline) {
			return searchjobs.ExecutionSnapshot{}, errors.New(
				"timed out minting search-analysis execution snapshot",
			)
		}
		time.Sleep(time.Millisecond)
	}
}

type searchAnalysisKnowledgeFixture struct {
	database *control.DB
	resolver *knowledgecatalog.Resolver
}

func newSearchAnalysisKnowledgeFixture(
	t *testing.T,
	template searchjobs.ExecutionSnapshot,
) searchAnalysisKnowledgeFixture {
	t.Helper()
	database, err := control.Open(
		context.Background(),
		filepath.Join(t.TempDir(), "control.sqlite"),
	)
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("control DB Close(): %v", closeErr)
		}
	})

	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: searchAnalysisAuthorityCursorKey,
		Clock:     func() time.Time { return template.SearchStart.Add(-2 * time.Minute) },
		IDGenerator: func() (string, error) {
			return searchAnalysisAuthorityAppID, nil
		},
	})
	if err != nil {
		t.Fatalf("control.NewAppCatalog(): %v", err)
	}
	if _, err := apps.CreateApp(
		context.Background(),
		control.AppAccessScope{TenantID: template.TenantID},
		control.AppDefinition{Slug: "search-analysis", DisplayName: "Search Analysis"},
	); err != nil {
		t.Fatalf("CreateApp(): %v", err)
	}
	store, err := knowledgecatalog.New(
		database,
		knowledgecatalog.Options{CursorKey: searchAnalysisAuthorityCursorKey},
	)
	if err != nil {
		t.Fatalf("knowledgecatalog.New(): %v", err)
	}
	resolver, err := knowledgecatalog.NewResolver(store, knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatalf("knowledgecatalog.NewResolver(): %v", err)
	}
	return searchAnalysisKnowledgeFixture{database: database, resolver: resolver}
}

// rawSearchAnalysisSnapshots deliberately bypasses the normal test sealing
// helper. It models a completed-search provider returning a previously valid
// snapshot followed by an unsigned or tampered value, so cache/cursor tests can
// prove the consumer validates Manager authority before reuse.
type rawSearchAnalysisSnapshots struct {
	mu        sync.Mutex
	snapshots []searchjobs.ExecutionSnapshot
	calls     int
}

func (searches *rawSearchAnalysisSnapshots) CompletedExecutionSnapshotFor(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (searchjobs.ExecutionSnapshot, error) {
	if ctx == nil {
		return searchjobs.ExecutionSnapshot{}, errors.New("raw snapshot lookup context is nil")
	}
	if err := ctx.Err(); err != nil {
		return searchjobs.ExecutionSnapshot{}, err
	}
	searches.mu.Lock()
	defer searches.mu.Unlock()
	searches.calls++
	if len(searches.snapshots) == 0 {
		return searchjobs.ExecutionSnapshot{}, searchjobs.ErrNotFound
	}
	index := searches.calls - 1
	if index >= len(searches.snapshots) {
		index = len(searches.snapshots) - 1
	}
	snapshot := searches.snapshots[index]
	if snapshot.ID != id ||
		snapshot.TenantID != access.TenantID ||
		snapshot.OwnerID != access.OwnerID {
		return searchjobs.ExecutionSnapshot{}, searchjobs.ErrNotFound
	}
	return snapshot, nil
}

func (searches *rawSearchAnalysisSnapshots) Calls() int {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	return searches.calls
}

func unsignedSearchAnalysisSnapshot(
	snapshot searchjobs.ExecutionSnapshot,
) searchjobs.ExecutionSnapshot {
	return searchjobs.ExecutionSnapshot{
		ID:                snapshot.ID,
		OwnerID:           snapshot.OwnerID,
		TenantID:          snapshot.TenantID,
		AppID:             snapshot.AppID,
		SPL:               snapshot.SPL,
		EffectiveIndexes:  slices.Clone(snapshot.EffectiveIndexes),
		Earliest:          snapshot.Earliest,
		Latest:            snapshot.Latest,
		SearchStart:       snapshot.SearchStart,
		SearchTimezone:    snapshot.SearchTimezone,
		IndexTimeCutoff:   snapshot.IndexTimeCutoff,
		VisibilityCutoff:  snapshot.VisibilityCutoff,
		CompiledQuery:     snapshot.CompiledQuery,
		KnowledgeSnapshot: snapshot.KnowledgeSnapshot,
		FinishedAt:        snapshot.FinishedAt,
		ExpiresAt:         snapshot.ExpiresAt,
	}
}

// changedEnabledEmptySearchAnalysisSnapshots mints two fully Manager-sealed
// executions with the same public search tuple. A real DRAFT mutation advances
// the tenant catalog authority between them while leaving both executable
// knowledge programs present and empty.
func changedEnabledEmptySearchAnalysisSnapshots(
	t *testing.T,
	template searchjobs.ExecutionSnapshot,
) (searchjobs.ExecutionSnapshot, searchjobs.ExecutionSnapshot) {
	t.Helper()
	template.AppID = searchAnalysisAuthorityAppID
	fixture := newSearchAnalysisKnowledgeFixture(t, template)

	first, err := sealSearchAnalysisSnapshotWithResolver(template, fixture.resolver)
	if err != nil {
		t.Fatalf("seal first enabled-empty snapshot: %v", err)
	}
	auditStore, err := audit.NewStore(
		fixture.database,
		audit.StoreOptions{CursorKey: searchAnalysisAuthorityCursorKey},
	)
	if err != nil {
		t.Fatalf("audit.NewStore(): %v", err)
	}
	writer, err := knowledgecatalog.NewWriter(
		fixture.database,
		auditStore,
		knowledgecatalog.WriterOptions{
			Clock: func() time.Time { return template.SearchStart.Add(-time.Minute) },
			IDGenerator: func() (string, error) {
				return "ko_0000000000000000000001", nil
			},
		},
	)
	if err != nil {
		t.Fatalf("knowledgecatalog.NewWriter(): %v", err)
	}
	actorContext, err := audit.WithActor(context.Background(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "search-analysis-authority-test",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(): %v", err)
	}
	if _, err := writer.Create(
		actorContext,
		knowledgecatalog.WriteScope{
			TenantID:       template.TenantID,
			OwnerID:        template.OwnerID,
			WritableAppIDs: []string{searchAnalysisAuthorityAppID},
		},
		&opensplunk.CreateKnowledgeObjectRequest{
			Definition: &opensplunk.KnowledgeObjectDefinition{
				AppId:        searchAnalysisAuthorityAppID,
				Name:         "field-analysis-draft",
				SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
				Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
					FieldAlias: &opensplunk.FieldAliasDefinition{
						SourceField:       "source_field",
						DestinationField:  "destination_field",
						OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
					},
				},
			},
			InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: "field-analysis-authority-change",
		},
	); err != nil {
		t.Fatalf("Writer.Create(DRAFT): %v", err)
	}
	second, err := sealSearchAnalysisSnapshotWithResolver(template, fixture.resolver)
	if err != nil {
		t.Fatalf("seal second enabled-empty snapshot: %v", err)
	}

	for index, snapshot := range []searchjobs.ExecutionSnapshot{first, second} {
		retained, openErr := snapshot.OpenRetainedKnowledgeExecution()
		if openErr != nil || retained == nil || !retained.KnowledgePrelude.IsEmpty() {
			t.Fatalf(
				"enabled-empty snapshot %d retained authority = (%#v, %v)",
				index,
				retained,
				openErr,
			)
		}
	}
	firstAuthority, firstErr := first.ValidateRetainedKnowledgeAuthority()
	secondAuthority, secondErr := second.ValidateRetainedKnowledgeAuthority()
	if firstErr != nil || secondErr != nil ||
		!firstAuthority.Present || !secondAuthority.Present ||
		firstAuthority.SnapshotDigest == secondAuthority.SnapshotDigest ||
		firstAuthority.CompiledDigest != secondAuthority.CompiledDigest ||
		!sameSearchAnalysisPublicTuple(first, second) {
		t.Fatalf(
			"DRAFT authority rotation = first(%#v, %v) second(%#v, %v) publicEqual=%t, want snapshot-only",
			firstAuthority,
			firstErr,
			secondAuthority,
			secondErr,
			sameSearchAnalysisPublicTuple(first, second),
		)
	}
	return first, second
}

func changedCompiledEnabledEmptySearchAnalysisSnapshots(
	t *testing.T,
	template searchjobs.ExecutionSnapshot,
) (searchjobs.ExecutionSnapshot, searchjobs.ExecutionSnapshot) {
	t.Helper()
	template.AppID = searchAnalysisAuthorityAppID
	fixture := newSearchAnalysisKnowledgeFixture(t, template)
	first, err := sealSearchAnalysisSnapshotWithCompiler(
		template,
		fixture.resolver,
		clickhouse.Compiler{Database: "alpha_db", Table: "events"},
	)
	if err != nil {
		t.Fatalf("seal first alternate-compiler snapshot: %v", err)
	}
	second, err := sealSearchAnalysisSnapshotWithCompiler(
		template,
		fixture.resolver,
		clickhouse.Compiler{Database: "bravo_db", Table: "events"},
	)
	if err != nil {
		t.Fatalf("seal second alternate-compiler snapshot: %v", err)
	}
	firstAuthority, firstErr := first.ValidateRetainedKnowledgeAuthority()
	secondAuthority, secondErr := second.ValidateRetainedKnowledgeAuthority()
	if firstErr != nil || secondErr != nil ||
		!firstAuthority.Present || !secondAuthority.Present ||
		firstAuthority.SnapshotDigest != secondAuthority.SnapshotDigest ||
		firstAuthority.CompiledDigest == secondAuthority.CompiledDigest ||
		!sameSearchAnalysisPublicTuple(first, second) {
		t.Fatalf(
			"alternate compiler rotation = first(%#v, %v) second(%#v, %v) publicEqual=%t, want compiled-only",
			firstAuthority,
			firstErr,
			secondAuthority,
			secondErr,
			sameSearchAnalysisPublicTuple(first, second),
		)
	}
	return first, second
}

func legacyAndEnabledEmptySearchAnalysisSnapshots(
	t *testing.T,
	template searchjobs.ExecutionSnapshot,
) (searchjobs.ExecutionSnapshot, searchjobs.ExecutionSnapshot) {
	t.Helper()
	template.AppID = searchAnalysisAuthorityAppID
	fixture := newSearchAnalysisKnowledgeFixture(t, template)
	legacy, err := sealSearchAnalysisSnapshotWithResolver(template, nil)
	if err != nil {
		t.Fatalf("seal legacy snapshot with AppID: %v", err)
	}
	enabled, err := sealSearchAnalysisSnapshotWithResolver(template, fixture.resolver)
	if err != nil {
		t.Fatalf("seal enabled-empty snapshot: %v", err)
	}
	legacyAuthority, legacyErr := legacy.ValidateRetainedKnowledgeAuthority()
	enabledAuthority, enabledErr := enabled.ValidateRetainedKnowledgeAuthority()
	if legacyErr != nil || enabledErr != nil || legacyAuthority.Present ||
		!enabledAuthority.Present || !sameSearchAnalysisPublicTuple(legacy, enabled) {
		t.Fatalf(
			"presence pair = legacy(%#v, %v) enabled(%#v, %v) publicEqual=%t",
			legacyAuthority,
			legacyErr,
			enabledAuthority,
			enabledErr,
			sameSearchAnalysisPublicTuple(legacy, enabled),
		)
	}
	return legacy, enabled
}

func sameSearchAnalysisPublicTuple(
	left searchjobs.ExecutionSnapshot,
	right searchjobs.ExecutionSnapshot,
) bool {
	return left.ID == right.ID &&
		left.OwnerID == right.OwnerID &&
		left.TenantID == right.TenantID &&
		left.AppID == right.AppID &&
		left.SPL == right.SPL &&
		slices.Equal(left.EffectiveIndexes, right.EffectiveIndexes) &&
		left.Earliest.Equal(right.Earliest) &&
		left.Latest.Equal(right.Latest) &&
		left.SearchStart.Equal(right.SearchStart) &&
		left.SearchTimezone == right.SearchTimezone &&
		left.IndexTimeCutoff.Equal(right.IndexTimeCutoff) &&
		left.VisibilityCutoff == right.VisibilityCutoff &&
		left.FinishedAt.Equal(right.FinishedAt) &&
		left.ExpiresAt.Equal(right.ExpiresAt)
}
