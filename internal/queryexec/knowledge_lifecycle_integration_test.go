package queryexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	knowledgeLifecycleOwner    = "knowledge-lifecycle-owner"
	knowledgeLifecycleAppID    = "app_000000000900000000001A"
	knowledgeLifecycleObjectID = "ko_lifecycle_vertical"
	knowledgeLifecycleField    = "lifecycle_kind"
	knowledgeLifecycleV1Value  = "alpha"
	knowledgeLifecycleV2Value  = "beta"
	knowledgeLifecycleService  = "knowledge-lifecycle"
	knowledgeLifecycleSource   = "knowledge-lifecycle-source"
	knowledgeLifecycleBatchID  = "knowledge-lifecycle-batch"
)

var knowledgeLifecycleCursorKey = []byte(
	"knowledge-lifecycle-cursor-key-at-least-32-bytes",
)

type knowledgeLifecycleCatalog struct {
	writer     *knowledgecatalog.Writer
	resolver   *knowledgecatalog.Resolver
	actor      context.Context
	writeScope knowledgecatalog.WriteScope
	tenantID   string
	indexName  string
}

// TestKnowledgeLifecycleVerticalCompilerRotation is the Docker-free preflight
// for the real lifecycle subtest below. It pins the exact ACTIVE Writer input,
// Resolver output, public compiler seal, and snapshot rotation without making
// a storage-engine claim.
func TestKnowledgeLifecycleVerticalCompilerRotation(t *testing.T) {
	const (
		tenantID  = "knowledge-lifecycle-unit-tenant"
		indexName = "knowledge-lifecycle-unit"
	)
	base := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	indexTime := base.Add(10 * time.Minute)
	catalog := newKnowledgeLifecycleCatalog(t, t.Context(), tenantID, indexName)

	objectV1 := catalog.publishV1(t)
	resolutionV1 := catalog.resolve(t)
	compiledV1 := compileKnowledgeLifecycleQuery(
		t,
		resolutionV1.Prelude(),
		tenantID,
		indexName,
		base,
		indexTime,
	)
	snapshotV1, err := resolutionV1.Finalize(compiledV1)
	if err != nil || snapshotV1.IsZero() {
		t.Fatalf("finalize lifecycle v1: snapshot zero=%t error=%v", snapshotV1.IsZero(), err)
	}
	requireKnowledgeLifecycleSummary(t, snapshotV1.Summary(), objectV1.GetKnowledgeObjectId(), 1)
	requireKnowledgeLifecycleProgram(t, resolutionV1.Prelude(), knowledgeLifecycleV1Value)

	objectV2 := catalog.publishV2(t, objectV1)
	resolutionV2 := catalog.resolve(t)
	compiledV2 := compileKnowledgeLifecycleQuery(
		t,
		resolutionV2.Prelude(),
		tenantID,
		indexName,
		base,
		indexTime,
	)
	snapshotV2, err := resolutionV2.Finalize(compiledV2)
	if err != nil || snapshotV2.IsZero() {
		t.Fatalf("finalize lifecycle v2: snapshot zero=%t error=%v", snapshotV2.IsZero(), err)
	}
	requireKnowledgeLifecycleSummary(t, snapshotV2.Summary(), objectV2.GetKnowledgeObjectId(), 2)
	requireKnowledgeLifecycleProgram(t, resolutionV2.Prelude(), knowledgeLifecycleV2Value)
	if resolutionV1.Prelude().Equal(resolutionV2.Prelude()) ||
		compiledV1.EqualForExecution(compiledV2) ||
		bytes.Equal(snapshotV1.Reference().GetSnapshotSha256(), snapshotV2.Reference().GetSnapshotSha256()) {
		t.Fatal("ACTIVE lifecycle compiler preflight did not rotate program, query, and snapshot")
	}
}

func runKnowledgeLifecycleVertical(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	realExecutor searchjobs.Executor,
	tenantID string,
	indexName string,
	base time.Time,
	indexTime time.Time,
) {
	t.Helper()
	catalog := newKnowledgeLifecycleCatalog(t, ctx, tenantID, indexName)
	objectV1 := catalog.publishV1(t)
	insertKnowledgeLifecycleEvents(t, ctx, connection, tenantID, indexName, base, indexTime)

	paused := newKnowledgeLifecyclePausedExecutor(realExecutor)
	var nextJob atomic.Int32
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:          paused,
		Snapshotter:       knowledgeLifecycleSnapshotter(1),
		KnowledgeResolver: catalog.resolver,
		Compiler:          clickhouse.Compiler{Database: "open_splunk", Table: "events"},
		MaxConcurrent:     1,
		MaxQueued:         4,
		MaxRows:           10,
		MaxBytes:          1 << 20,
		RetentionTTL:      time.Hour,
		CleanupInterval:   -1,
		Now: func() time.Time {
			return indexTime.Add(time.Millisecond)
		},
		NewID: func() string {
			return fmt.Sprintf("knowledge-lifecycle-search-%04d", nextJob.Add(1))
		},
		CursorKey: []byte("knowledge-lifecycle-search-cursor-key-at-least-32-bytes"),
	})
	if err != nil {
		t.Fatalf("create lifecycle search manager: %v", err)
	}
	defer func() {
		paused.Release()
		if closeErr := manager.Close(); closeErr != nil {
			t.Errorf("close lifecycle search manager: %v", closeErr)
		}
	}()
	if !manager.KnowledgeAdmissionEnabled() || !manager.KnowledgeExecutionEnabled() {
		t.Fatal("lifecycle Manager did not enable resolver-backed execution")
	}

	request := knowledgeLifecycleSearchRequest(t, tenantID, indexName, base)
	jobV1, err := manager.Create(ctx, request)
	if err != nil {
		t.Fatalf("admit lifecycle ACTIVE v1: %v", err)
	}
	requireKnowledgeLifecycleSummary(t, jobV1.KnowledgeSnapshot, objectV1.GetKnowledgeObjectId(), 1)
	var observedV1 clickhouse.CompiledQuery
	select {
	case observedV1 = <-paused.FirstStarted():
	case <-ctx.Done():
		t.Fatalf("wait for paused lifecycle v1 dispatch: %v", ctx.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle v1 did not reach the real-executor dispatch boundary")
	}
	if !observedV1.HasValidExecutionSeal() {
		t.Fatal("paused lifecycle v1 omitted its public compiler seal")
	}

	objectV2 := catalog.publishV2(t, objectV1)
	freshV2, err := manager.Create(ctx, request)
	if err != nil {
		t.Fatalf("admit fresh lifecycle ACTIVE v2: %v", err)
	}
	requireKnowledgeLifecycleSummary(t, freshV2.KnowledgeSnapshot, objectV2.GetKnowledgeObjectId(), 2)
	historyRequest := request
	historyRequest.Source = searchjobs.JobSource{
		Origin:   searchjobs.JobOriginHistoryRerun,
		ObjectID: jobV1.ID,
	}
	historyV2, err := manager.Create(ctx, historyRequest)
	if err != nil {
		t.Fatalf("admit history rerun against lifecycle ACTIVE v2: %v", err)
	}
	requireKnowledgeLifecycleSummary(t, historyV2.KnowledgeSnapshot, objectV2.GetKnowledgeObjectId(), 2)
	paused.Release()

	access := searchjobs.AccessScope{TenantID: tenantID, OwnerID: knowledgeLifecycleOwner}
	completedV1 := waitForKnowledgeLifecycleSearch(t, ctx, manager, access, jobV1.ID)
	completedFreshV2 := waitForKnowledgeLifecycleSearch(t, ctx, manager, access, freshV2.ID)
	completedHistoryV2 := waitForKnowledgeLifecycleSearch(t, ctx, manager, access, historyV2.ID)
	if completedHistoryV2.Source != historyRequest.Source {
		t.Fatalf("history rerun provenance = %#v, want %#v", completedHistoryV2.Source, historyRequest.Source)
	}

	pageV1 := knowledgeLifecycleResultPage(t, manager, access, completedV1.ID)
	pageFreshV2 := knowledgeLifecycleResultPage(t, manager, access, completedFreshV2.ID)
	pageHistoryV2 := knowledgeLifecycleResultPage(t, manager, access, completedHistoryV2.ID)
	requireKnowledgeLifecycleRows(
		t,
		pageV1,
		knowledgeLifecycleV1Value,
		[]string{"knowledge-lifecycle-event-alpha"},
	)
	requireKnowledgeLifecycleRows(
		t,
		pageFreshV2,
		knowledgeLifecycleV2Value,
		[]string{"knowledge-lifecycle-event-beta"},
	)
	requireKnowledgeLifecycleRows(
		t,
		pageHistoryV2,
		knowledgeLifecycleV2Value,
		[]string{"knowledge-lifecycle-event-beta"},
	)

	executionV1 := knowledgeLifecycleExecution(t, ctx, manager, access, completedV1.ID)
	executionFreshV2 := knowledgeLifecycleExecution(t, ctx, manager, access, completedFreshV2.ID)
	executionHistoryV2 := knowledgeLifecycleExecution(t, ctx, manager, access, completedHistoryV2.ID)
	requireKnowledgeLifecycleExecution(
		t, executionV1, objectV1.GetKnowledgeObjectId(), 1, knowledgeLifecycleV1Value,
	)
	requireKnowledgeLifecycleExecution(
		t, executionFreshV2, objectV2.GetKnowledgeObjectId(), 2, knowledgeLifecycleV2Value,
	)
	requireKnowledgeLifecycleExecution(
		t, executionHistoryV2, objectV2.GetKnowledgeObjectId(), 2, knowledgeLifecycleV2Value,
	)
	if executionV1.CompiledQuery == nil ||
		!executionV1.CompiledQuery.EqualForExecution(observedV1) ||
		executionFreshV2.CompiledQuery == nil || executionHistoryV2.CompiledQuery == nil ||
		!executionFreshV2.CompiledQuery.EqualForExecution(*executionHistoryV2.CompiledQuery) ||
		executionV1.CompiledQuery.EqualForExecution(*executionFreshV2.CompiledQuery) {
		t.Fatal("Manager-retained lifecycle compiler authorities do not match v1/v2 dispatch")
	}

	exportSource, err := exportjobs.NewReexecutionSource(exportjobs.ReexecutionSourceConfig{
		Searches: manager,
		Executor: paused,
		// A retained knowledge export must never reach this deliberately invalid
		// legacy compiler.
		Compiler:   clickhouse.Compiler{Database: "retained_export_recompile_forbidden"},
		MaxRuntime: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create lifecycle export re-execution source: %v", err)
	}
	exportManager, err := exportjobs.New(exportjobs.Config{
		Source:          exportSource,
		ArtifactDir:     t.TempDir(),
		MaxWorkers:      1,
		MaxQueued:       1,
		CleanupInterval: -1,
		NewID:           func() string { return "knowledge-lifecycle-export" },
	})
	if err != nil {
		t.Fatalf("create lifecycle export manager: %v", err)
	}
	defer func() {
		if closeErr := exportManager.Close(); closeErr != nil {
			t.Errorf("close lifecycle export manager: %v", closeErr)
		}
	}()
	exportJob, err := exportManager.Create(ctx, access, exportjobs.CreateRequest{
		SearchJobID: completedV1.ID,
		Format:      exportjobs.FormatCSV,
		Columns:     []string{"event_id", knowledgeLifecycleField},
		RowLimit:    10,
		ByteLimit:   1 << 20,
		CSV:         exportjobs.CSVOptions{HeaderMode: exportjobs.CSVHeaderFieldNames},
	})
	if err != nil {
		t.Fatalf("create lifecycle v1 export: %v", err)
	}
	exportJob = waitForKnowledgeLifecycleExport(t, ctx, exportManager, access, exportJob.ID)
	requireKnowledgeLifecycleSummary(t, exportJob.KnowledgeSnapshot, objectV1.GetKnowledgeObjectId(), 1)
	if !slices.Equal(exportJob.Columns, []string{"event_id", knowledgeLifecycleField}) ||
		exportJob.Artifact == nil || exportJob.Artifact.RowCount != 1 {
		t.Fatalf("lifecycle v1 export metadata = %#v", exportJob)
	}
	grant, err := exportManager.CreateDownloadGrant(ctx, access, exportJob.ID)
	if err != nil {
		t.Fatalf("create lifecycle export download grant: %v", err)
	}
	download, err := exportManager.RedeemDownload(ctx, grant.Token)
	if err != nil {
		t.Fatalf("redeem lifecycle export download: %v", err)
	}
	payload, readErr := io.ReadAll(download)
	closeErr := download.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read lifecycle export artifact: read=%v close=%v", readErr, closeErr)
	}
	wantCSV := "event_id,lifecycle_kind\n" +
		"knowledge-lifecycle-event-alpha,alpha\n"
	if string(payload) != wantCSV {
		t.Fatalf("retained lifecycle v1 export = %q, want %q", payload, wantCSV)
	}

	executed := paused.Queries()
	if len(executed) != 4 ||
		!executed[0].EqualForExecution(*executionV1.CompiledQuery) ||
		!executed[1].EqualForExecution(*executionFreshV2.CompiledQuery) ||
		!executed[2].EqualForExecution(*executionHistoryV2.CompiledQuery) ||
		!executed[3].EqualForExecution(*executionV1.CompiledQuery) {
		t.Fatalf("lifecycle real-executor authorities = %d calls", len(executed))
	}
}

type knowledgeLifecycleSnapshotter uint64

func (snapshot knowledgeLifecycleSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return uint64(snapshot), nil
}

type knowledgeLifecyclePausedExecutor struct {
	delegate searchjobs.Executor
	started  chan clickhouse.CompiledQuery
	release  chan struct{}
	once     sync.Once
	ordinal  atomic.Int32
	mu       sync.Mutex
	queries  []clickhouse.CompiledQuery
}

func newKnowledgeLifecyclePausedExecutor(delegate searchjobs.Executor) *knowledgeLifecyclePausedExecutor {
	return &knowledgeLifecyclePausedExecutor{
		delegate: delegate,
		started:  make(chan clickhouse.CompiledQuery, 1),
		release:  make(chan struct{}),
	}
}

func (executor *knowledgeLifecyclePausedExecutor) Execute(
	ctx context.Context,
	compiled clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	detached, ok := compiled.CloneForExecution()
	if !ok {
		return errors.New("lifecycle executor received invalid compiler authority")
	}
	executor.mu.Lock()
	executor.queries = append(executor.queries, detached)
	executor.mu.Unlock()
	if executor.ordinal.Add(1) == 1 {
		select {
		case executor.started <- detached:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-executor.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return executor.delegate.Execute(ctx, compiled, sink)
}

func (executor *knowledgeLifecyclePausedExecutor) FirstStarted() <-chan clickhouse.CompiledQuery {
	return executor.started
}

func (executor *knowledgeLifecyclePausedExecutor) Release() {
	executor.once.Do(func() { close(executor.release) })
}

func (executor *knowledgeLifecyclePausedExecutor) Queries() []clickhouse.CompiledQuery {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	result := make([]clickhouse.CompiledQuery, len(executor.queries))
	for index := range executor.queries {
		result[index], _ = executor.queries[index].CloneForExecution()
	}
	return result
}

func newKnowledgeLifecycleCatalog(
	t *testing.T,
	ctx context.Context,
	tenantID string,
	indexName string,
) *knowledgeLifecycleCatalog {
	t.Helper()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open lifecycle control database: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close lifecycle control database: %v", closeErr)
		}
	})
	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: knowledgeLifecycleCursorKey,
		IDGenerator: func() (string, error) {
			return knowledgeLifecycleAppID, nil
		},
	})
	if err != nil {
		t.Fatalf("create lifecycle app catalog: %v", err)
	}
	if _, err := apps.CreateApp(
		ctx,
		control.AppAccessScope{TenantID: tenantID},
		control.AppDefinition{Slug: "knowledge-lifecycle", DisplayName: "Knowledge Lifecycle"},
	); err != nil {
		t.Fatalf("create lifecycle app: %v", err)
	}
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name:             indexName,
		DisplayName:      "Knowledge Lifecycle",
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatalf("create lifecycle index: %v", err)
	}
	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: knowledgeLifecycleCursorKey})
	if err != nil {
		t.Fatalf("create lifecycle audit store: %v", err)
	}
	store, err := knowledgecatalog.New(database, knowledgecatalog.Options{CursorKey: knowledgeLifecycleCursorKey})
	if err != nil {
		t.Fatalf("create lifecycle knowledge catalog: %v", err)
	}
	resolver, err := store.NewResolver(knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatalf("create lifecycle resolver: %v", err)
	}
	writer, err := knowledgecatalog.NewWriter(database, auditStore, knowledgecatalog.WriterOptions{
		IDGenerator: func() (string, error) { return knowledgeLifecycleObjectID, nil },
	})
	if err != nil {
		t.Fatalf("create lifecycle writer: %v", err)
	}
	actor, err := audit.WithActor(ctx, audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   knowledgeLifecycleOwner,
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("create lifecycle audit actor: %v", err)
	}
	return &knowledgeLifecycleCatalog{
		writer:   writer,
		resolver: resolver,
		actor:    actor,
		writeScope: knowledgecatalog.WriteScope{
			TenantID:       tenantID,
			OwnerID:        knowledgeLifecycleOwner,
			WritableAppIDs: []string{knowledgeLifecycleAppID},
		},
		tenantID:  tenantID,
		indexName: indexName,
	}
}

func (catalog *knowledgeLifecycleCatalog) publishV1(t *testing.T) *opensplunkv1.KnowledgeObject {
	t.Helper()
	result, err := catalog.writer.Create(
		catalog.actor,
		catalog.writeScope,
		&opensplunkv1.CreateKnowledgeObjectRequest{
			Definition:      knowledgeLifecycleDefinition(catalog.indexName, knowledgeLifecycleV1Value),
			InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			ClientRequestId: "knowledge-lifecycle-create-v1",
		},
	)
	if err != nil {
		t.Fatalf("publish lifecycle ACTIVE v1: %v", err)
	}
	object := result.GetKnowledgeObject()
	if object.GetKnowledgeObjectId() != knowledgeLifecycleObjectID || object.GetVersion() != 1 ||
		object.GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE {
		t.Fatalf("published lifecycle ACTIVE v1 = %v", object)
	}
	return object
}

func (catalog *knowledgeLifecycleCatalog) publishV2(
	t *testing.T,
	objectV1 *opensplunkv1.KnowledgeObject,
) *opensplunkv1.KnowledgeObject {
	t.Helper()
	definition := proto.Clone(objectV1.GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
	definition.GetFieldExtraction().GetRegex().Pattern = knowledgeLifecyclePattern(knowledgeLifecycleV2Value)
	result, err := catalog.writer.Update(
		catalog.actor,
		catalog.writeScope,
		&opensplunkv1.UpdateKnowledgeObjectRequest{
			KnowledgeObjectId: objectV1.GetKnowledgeObjectId(),
			ExpectedVersion:   1,
			Definition:        definition,
			UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"field_extraction"}},
			ClientRequestId:   "knowledge-lifecycle-update-v2",
		},
	)
	if err != nil {
		t.Fatalf("publish lifecycle ACTIVE v2: %v", err)
	}
	object := result.GetKnowledgeObject()
	if object.GetKnowledgeObjectId() != objectV1.GetKnowledgeObjectId() || object.GetVersion() != 2 ||
		object.GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE ||
		object.GetDefinition().GetFieldExtraction().GetRegex().GetPattern() !=
			knowledgeLifecyclePattern(knowledgeLifecycleV2Value) {
		t.Fatalf("published lifecycle ACTIVE v2 = %v", object)
	}
	return object
}

func (catalog *knowledgeLifecycleCatalog) resolve(t *testing.T) knowledgecatalog.Resolution {
	t.Helper()
	resolution, err := catalog.resolver.Resolve(catalog.actor, knowledgecatalog.ResolutionScope{
		TenantID:                   catalog.tenantID,
		PrincipalID:                knowledgeLifecycleOwner,
		AppID:                      knowledgeLifecycleAppID,
		EffectiveAuthorizedIndexes: []string{catalog.indexName},
	})
	if err != nil {
		t.Fatalf("resolve lifecycle ACTIVE publication: %v", err)
	}
	return resolution
}

func knowledgeLifecycleDefinition(indexName string, value string) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        knowledgeLifecycleAppID,
		Name:         "lifecycle-extract-kind",
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Selector: &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
			MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
			Value:     indexName,
		}}},
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField:        "_raw",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{
						Pattern:      knowledgeLifecyclePattern(value),
						OutputFields: []string{knowledgeLifecycleField},
					},
				},
			},
		},
	}
}

func knowledgeLifecyclePattern(value string) string {
	return `"kind":"(?P<` + knowledgeLifecycleField + `>` + value + `)"`
}

func knowledgeLifecycleCanonicalPattern(value string) string {
	return "(?-s)" + knowledgeLifecyclePattern(value)
}

func compileKnowledgeLifecycleQuery(
	t *testing.T,
	program knowledgeprogram.Program,
	tenantID string,
	indexName string,
	base time.Time,
	indexTime time.Time,
) clickhouse.CompiledQuery {
	t.Helper()
	logical := knowledgeRuntimePlan(
		t,
		knowledgeLifecycleSPL(indexName),
		program,
		tenantID,
		[]string{indexName},
		indexTime,
		base,
		base.Add(2*time.Minute),
	)
	compiled, err := (clickhouse.Compiler{Database: "open_splunk", Table: "events"}).Compile(logical)
	if err != nil {
		t.Fatalf("compile lifecycle query: %v", err)
	}
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("compiled lifecycle query omitted execution seal")
	}
	return compiled
}

func knowledgeLifecycleSPL(indexName string) string {
	return `index=` + indexName + ` service=` + knowledgeLifecycleService +
		` source=` + knowledgeLifecycleSource +
		` | where isnotnull(` + knowledgeLifecycleField + `)` +
		` | sort 0 +event_id` +
		` | table event_id ` + knowledgeLifecycleField
}

func insertKnowledgeLifecycleEvents(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	tenantID string,
	indexName string,
	base time.Time,
	indexTime time.Time,
) {
	t.Helper()
	const eventCount = 2
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"event_time_source, host, source, sourcetype, service, severity, raw, raw_encoding, fields, " +
		"field_names, field_types, field_metadata_version, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatalf("prepare dedicated lifecycle events: %v", err)
	}
	expiresAt := knowledgeRuntimeFixtureExpiresAt()
	for index, value := range []string{knowledgeLifecycleV1Value, knowledgeLifecycleV2Value} {
		service := knowledgeLifecycleService
		if err := batch.Append(
			"knowledge-lifecycle-event-"+value,
			tenantID,
			indexName,
			base.Add(time.Duration(index+1)*time.Second),
			indexTime,
			uint8(1),
			"knowledge-lifecycle-host",
			knowledgeLifecycleSource,
			"knowledge:lifecycle",
			&service,
			uint8(1),
			[]byte(`{"kind":"`+value+`","fixture":"knowledge-lifecycle"}`),
			uint8(1),
			clickhousedriver.NewJSON(),
			[]string{},
			[]uint8{},
			eventfields.CurrentFieldMetadataVersion,
			"knowledge-lifecycle-collector",
			knowledgeLifecycleBatchID,
			uint64(index+1),
			expiresAt,
			uint64(1),
		); err != nil {
			t.Fatalf("append dedicated lifecycle event %q: %v", value, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send dedicated lifecycle events: %v", err)
	}

	var (
		rows           uint64
		alphaIDs       uint64
		betaIDs        uint64
		exactMarkers   uint64
		exactAuthority uint64
	)
	if err := connection.QueryRow(
		ctx,
		`SELECT count(), `+
			`countIf(event_id = 'knowledge-lifecycle-event-alpha'), `+
			`countIf(event_id = 'knowledge-lifecycle-event-beta'), `+
			`countIf(source = ? AND service = ? AND host = 'knowledge-lifecycle-host'), `+
			`countIf(event_time >= ? AND event_time < ? AND index_time = ? AND `+
			`visibility_seq = 1 AND expires_at = ? AND `+
			`field_metadata_version = ? AND length(field_names) = 0 AND length(field_types) = 0) `+
			`FROM open_splunk.events WHERE tenant_id = ? AND index_name = ? AND batch_id = ?`,
		knowledgeLifecycleSource,
		knowledgeLifecycleService,
		base,
		base.Add(2*time.Minute),
		indexTime,
		expiresAt,
		eventfields.CurrentFieldMetadataVersion,
		tenantID,
		indexName,
		knowledgeLifecycleBatchID,
	).Scan(&rows, &alphaIDs, &betaIDs, &exactMarkers, &exactAuthority); err != nil {
		t.Fatalf("read back dedicated lifecycle events: %v", err)
	}
	if rows != eventCount || alphaIDs != 1 || betaIDs != 1 || exactMarkers != eventCount ||
		exactAuthority != eventCount {
		t.Fatalf(
			"dedicated lifecycle events = rows/alpha/beta/markers/authority %d/%d/%d/%d/%d, want %d/1/1/%d/%d",
			rows, alphaIDs, betaIDs, exactMarkers, exactAuthority, eventCount, eventCount, eventCount,
		)
	}
}

func knowledgeLifecycleSearchRequest(
	t *testing.T,
	tenantID string,
	indexName string,
	base time.Time,
) searchjobs.CreateRequest {
	t.Helper()
	return searchjobs.CreateRequest{
		SPL:               knowledgeLifecycleSPL(indexName),
		OwnerID:           knowledgeLifecycleOwner,
		TenantID:          tenantID,
		AuthorizedIndexes: []string{indexName},
		RequestedIndexes:  []string{indexName},
		TimeRange:         queryIntegrationTimeRange(t, base, base.Add(2*time.Minute)),
		AppID:             knowledgeLifecycleAppID,
	}
}

func requireKnowledgeLifecycleProgram(
	t *testing.T,
	program knowledgeprogram.Program,
	value string,
) {
	t.Helper()
	extractions := program.RegexExtractions()
	captures := []knowledgeprogram.Capture(nil)
	if len(extractions) == 1 {
		captures = extractions[0].Captures()
	}
	if program.IsZero() || program.ObjectCount() != 1 || len(extractions) != 1 ||
		extractions[0].Pattern() != knowledgeLifecycleCanonicalPattern(value) ||
		len(captures) != 1 || captures[0].Name() != knowledgeLifecycleField ||
		captures[0].Group() != 1 {
		t.Fatalf("lifecycle %s program = zero:%t objects:%d extractions:%#v", value, program.IsZero(), program.ObjectCount(), extractions)
	}
}

func requireKnowledgeLifecycleSummary(
	t *testing.T,
	summary *opensplunkv1.KnowledgeSnapshotSummary,
	objectID string,
	version uint64,
) {
	t.Helper()
	if summary == nil || summary.GetRef() == nil || summary.GetRef().GetObjectCount() != 1 ||
		len(summary.GetObjects()) != 1 || summary.GetObjects()[0] == nil ||
		summary.GetObjects()[0].GetObjectType() !=
			opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION ||
		summary.GetObjects()[0].GetStage() !=
			opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION ||
		summary.GetObjects()[0].GetAuthorizedObject().GetKnowledgeObjectId() != objectID ||
		summary.GetObjects()[0].GetAuthorizedObject().GetVersion() != version ||
		len(summary.GetRef().GetSnapshotSha256()) == 0 {
		t.Fatalf("lifecycle v%d summary = %v", version, summary)
	}
}

func waitForKnowledgeLifecycleSearch(
	t *testing.T,
	ctx context.Context,
	manager *searchjobs.Manager,
	access searchjobs.AccessScope,
	id string,
) searchjobs.Job {
	t.Helper()
	for {
		job, err := manager.GetForContext(ctx, access, id)
		if err != nil {
			t.Fatalf("read lifecycle search %q: %v", id, err)
		}
		if job.State.Terminal() {
			if job.State != searchjobs.StateCompleted || job.Failure != nil {
				t.Fatalf("lifecycle search %q = state %s failure %#v", id, job.State, job.Failure)
			}
			return job
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for lifecycle search %q: %v", id, ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func knowledgeLifecycleResultPage(
	t *testing.T,
	manager *searchjobs.Manager,
	access searchjobs.AccessScope,
	id string,
) searchjobs.ResultPage {
	t.Helper()
	page, err := manager.ResultsFor(access, id, searchjobs.PageRequest{Limit: 10})
	if err != nil {
		t.Fatalf("read lifecycle results %q: %v", id, err)
	}
	return page
}

func requireKnowledgeLifecycleRows(
	t *testing.T,
	page searchjobs.ResultPage,
	value string,
	eventIDs []string,
) {
	t.Helper()
	if len(page.Schema.Columns) != 2 ||
		page.Schema.Columns[0] != (searchjobs.Column{Name: "event_id", Kind: searchjobs.ValueKindString}) ||
		page.Schema.Columns[1] != (searchjobs.Column{
			Name: knowledgeLifecycleField, Kind: searchjobs.ValueKindMixed, Nullable: true,
		}) ||
		page.TotalRows != uint64(len(eventIDs)) || len(page.Rows) != len(eventIDs) || !page.Complete {
		t.Fatalf("lifecycle %s result envelope = %#v", value, page)
	}
	for index, row := range page.Rows {
		if len(row.Values) != 2 {
			t.Fatalf("lifecycle %s row %d = %#v", value, index, row)
		}
		gotEventID, eventIDOK := row.Values[0].String()
		gotValue, valueOK := row.Values[1].String()
		if !eventIDOK || !valueOK || gotEventID != eventIDs[index] || gotValue != value {
			t.Fatalf("lifecycle %s row %d = (%q/%t, %q/%t), want (%q, %q)", value, index, gotEventID, eventIDOK, gotValue, valueOK, eventIDs[index], value)
		}
	}
}

func knowledgeLifecycleExecution(
	t *testing.T,
	ctx context.Context,
	manager *searchjobs.Manager,
	access searchjobs.AccessScope,
	id string,
) searchjobs.ExecutionSnapshot {
	t.Helper()
	execution, err := manager.CompletedExecutionSnapshotFor(ctx, access, id)
	if err != nil {
		t.Fatalf("read lifecycle execution %q: %v", id, err)
	}
	return execution
}

func requireKnowledgeLifecycleExecution(
	t *testing.T,
	execution searchjobs.ExecutionSnapshot,
	objectID string,
	version uint64,
	value string,
) {
	t.Helper()
	retained, err := execution.OpenRetainedKnowledgeExecution()
	if err != nil || retained == nil {
		t.Fatalf("open lifecycle v%d retained execution: retained=%#v error=%v", version, retained, err)
	}
	requireKnowledgeLifecycleSummary(t, retained.KnowledgeSummary, objectID, version)
	requireKnowledgeLifecycleProgram(t, retained.KnowledgePrelude, value)
	if execution.CompiledQuery == nil || !execution.CompiledQuery.EqualForExecution(retained.CompiledQuery) {
		t.Fatalf("lifecycle v%d retained compiler authority changed", version)
	}
}

func waitForKnowledgeLifecycleExport(
	t *testing.T,
	ctx context.Context,
	manager *exportjobs.Manager,
	access searchjobs.AccessScope,
	id string,
) exportjobs.Job {
	t.Helper()
	for {
		job, err := manager.Get(ctx, access, id)
		if err != nil {
			t.Fatalf("read lifecycle export %q: %v", id, err)
		}
		if job.State == exportjobs.StateCompleted {
			return job
		}
		if job.State == exportjobs.StateFailed || job.State == exportjobs.StateCanceled ||
			job.State == exportjobs.StateExpired {
			t.Fatalf("lifecycle export %q = state %s failure %#v", id, job.State, job.Failure)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for lifecycle export %q: %v", id, ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
