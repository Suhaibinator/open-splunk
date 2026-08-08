package knowledgecatalog

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	writerPublicationBatchObjectID = "ko_writer_publication_batch"
	writerPublicationDependencySQL = "dependency_insert"
	writerPublicationSelectorSQL   = "selector_insert"
	writerPublicationSavepointSQL  = "savepoint"
)

type writerPublicationBatchStatement struct {
	kind   string
	failed bool
}

type writerPublicationBatchTrace struct {
	mutex             sync.Mutex
	statements        []writerPublicationBatchStatement
	events            []string
	dependencyBatches [][]persistedPublicationDependency
	selectorBatches   [][]persistedPublicationSelector
}

type writerPublicationBatchSnapshot struct {
	statements        []writerPublicationBatchStatement
	events            []string
	dependencyBatches [][]persistedPublicationDependency
	selectorBatches   [][]persistedPublicationSelector
}

func TestWriterPublicationBatchesCeilingRowsAndHooks(t *testing.T) {
	harness := newWriterFaultHarness(t)
	before := readWriterFaultSnapshot(t, harness.database)
	trace := &writerPublicationBatchTrace{}
	registerWriterPublicationBatchCapture(t, harness, trace)

	tx := harness.database.GORMDB().Session(&gorm.Session{Logger: trace}).
		WithContext(harness.actorContext).Begin()
	if tx.Error != nil {
		t.Fatalf("begin publication batch transaction: %v", tx.Error)
	}
	transactionOpen := true
	t.Cleanup(func() {
		if transactionOpen {
			_ = tx.Rollback().Error
		}
	})
	prepared, plan := prepareWriterPublicationBatchPlan(
		t, harness, tx, maximumDependencyGraphEdges, knowledge.MaximumSelectorPatterns,
	)
	trace.reset()
	harness.writer.hook = writerPublicationBatchHook(trace)

	// The immutable row ceiling (1,024 direct edges) is intentionally wider
	// than the separately enforced whole-graph node ceiling (256). Exercise the
	// real staging path through both row-completion hooks, then require that the
	// graph guard rejects the otherwise complete transaction before commit.
	if _, _, _, err := harness.writer.publishMutation(
		harness.actorContext, tx, prepared, plan, true,
	); err == nil || !strings.Contains(err.Error(), fmt.Sprintf(
		"dependency graph nodes exceed %d", maximumDependencyGraphNodes,
	)) {
		t.Fatalf(
			"publish dependency/selector persistence ceilings = %v, want post-insert graph bound",
			err,
		)
	}

	snapshot := trace.snapshot()
	assertWriterPublicationBatchStatements(
		t, snapshot, maximumDependencyGraphEdges, knowledge.MaximumSelectorPatterns,
	)
	assertWriterPublicationDependencyBatches(t, snapshot.dependencyBatches, maximumDependencyGraphEdges)
	assertWriterPublicationSelectorBatches(t, snapshot.selectorBatches)
	assertWriterPublicationHookAfterStatements(
		t, snapshot, writerPublicationDependencySQL, "hook:dependencies",
	)
	assertWriterPublicationHookAfterStatements(
		t, snapshot, writerPublicationSelectorSQL, "hook:selectors",
	)

	var dependencies []persistedPublicationDependency
	if err := tx.Where(
		"tenant_id = ? AND source_object_id = ? AND source_object_version = 1",
		writerFaultTenant, writerPublicationBatchObjectID,
	).Order("ordinal").Find(&dependencies).Error; err != nil {
		t.Fatalf("read persisted dependency batches: %v", err)
	}
	assertWriterPublicationDependencyRows(t, dependencies, maximumDependencyGraphEdges)

	var selectors []persistedPublicationSelector
	if err := tx.Where(
		"tenant_id = ? AND knowledge_object_id = ? AND object_version = 1",
		writerFaultTenant, writerPublicationBatchObjectID,
	).Order(`CASE dimension
		WHEN 'index' THEN 0 WHEN 'host' THEN 1
		WHEN 'source' THEN 2 WHEN 'sourcetype' THEN 3 ELSE 4 END`).
		Order("ordinal").Find(&selectors).Error; err != nil {
		t.Fatalf("read persisted selector batch: %v", err)
	}
	assertWriterPublicationSelectorRows(t, selectors)

	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("roll back ceiling publication: %v", err)
	}
	transactionOpen = false
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), before)
	assertWriterFaultIntegrity(t, harness.database)
}

func TestWriterPublicationBatchesSkipZeroRows(t *testing.T) {
	harness := newWriterFaultHarness(t)
	before := readWriterFaultSnapshot(t, harness.database)
	trace := &writerPublicationBatchTrace{}
	registerWriterPublicationBatchCapture(t, harness, trace)

	tx := harness.database.GORMDB().Session(&gorm.Session{Logger: trace}).
		WithContext(harness.actorContext).Begin()
	if tx.Error != nil {
		t.Fatalf("begin empty publication batch transaction: %v", tx.Error)
	}
	transactionOpen := true
	t.Cleanup(func() {
		if transactionOpen {
			_ = tx.Rollback().Error
		}
	})
	prepared, plan := prepareWriterPublicationBatchPlan(t, harness, tx, 0, 0)
	trace.reset()
	harness.writer.hook = writerPublicationBatchHook(trace)

	if _, _, _, err := harness.writer.publishMutation(
		harness.actorContext, tx, prepared, plan, true,
	); err != nil {
		t.Fatalf("publish empty dependency/selector sets: %v", err)
	}

	snapshot := trace.snapshot()
	assertWriterPublicationBatchStatements(t, snapshot, 0, 0)
	if len(snapshot.dependencyBatches) != 0 || len(snapshot.selectorBatches) != 0 {
		t.Fatalf(
			"empty publication emitted Create batches: dependencies=%d selectors=%d",
			len(snapshot.dependencyBatches), len(snapshot.selectorBatches),
		)
	}
	assertWriterPublicationHookAfterStatements(
		t, snapshot, writerPublicationDependencySQL, "hook:dependencies",
	)
	assertWriterPublicationHookAfterStatements(
		t, snapshot, writerPublicationSelectorSQL, "hook:selectors",
	)

	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("roll back empty publication: %v", err)
	}
	transactionOpen = false
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), before)
	assertWriterFaultIntegrity(t, harness.database)
}

func TestWriterPublicationLaterBatchFailureRollsBackOuterTransaction(t *testing.T) {
	harness := newWriterFaultHarness(t)
	before := readWriterFaultSnapshot(t, harness.database)
	trace := &writerPublicationBatchTrace{}
	registerWriterPublicationBatchCapture(t, harness, trace)
	harness.writer.hook = writerPublicationBatchHook(trace)

	publishErr := func() (returnedErr error) {
		tx := harness.database.GORMDB().Session(&gorm.Session{Logger: trace}).
			WithContext(harness.actorContext).Begin()
		if tx.Error != nil {
			return fmt.Errorf("begin later-batch failure transaction: %w", tx.Error)
		}
		defer finishWriterTransaction(tx, &returnedErr)
		prepared, plan := prepareWriterPublicationBatchPlan(
			t, harness, tx, publicationInsertBatchSize+1, 0,
		)
		if err := tx.Exec(`CREATE TRIGGER writer_test_fail_later_dependency_batch
			BEFORE INSERT ON knowledge_object_dependencies
			WHEN NEW.ordinal = 64
			BEGIN
				SELECT RAISE(ABORT, 'injected later dependency batch failure');
			END`).Error; err != nil {
			return fmt.Errorf("install later-batch failure trigger: %w", err)
		}
		trace.reset()
		_, _, _, returnedErr = harness.writer.publishMutation(
			harness.actorContext, tx, prepared, plan, true,
		)
		return returnedErr
	}()
	if publishErr == nil || !strings.Contains(publishErr.Error(), "injected later dependency batch failure") {
		t.Fatalf("later dependency batch failure = %v, want injected database error", publishErr)
	}

	snapshot := trace.snapshot()
	if len(snapshot.dependencyBatches) != 2 ||
		len(snapshot.dependencyBatches[0]) != publicationInsertBatchSize ||
		len(snapshot.dependencyBatches[1]) != 1 {
		t.Fatalf(
			"later-failure dependency batches = %#v, want [%d 1]",
			writerPublicationBatchSizes(snapshot.dependencyBatches), publicationInsertBatchSize,
		)
	}
	dependencyStatements := writerPublicationStatements(snapshot, writerPublicationDependencySQL)
	if len(dependencyStatements) != 2 || dependencyStatements[0].failed || !dependencyStatements[1].failed {
		t.Fatalf(
			"later-failure dependency statements = %#v, want first success then second failure",
			dependencyStatements,
		)
	}
	if writerPublicationEventCount(snapshot.events, "hook:dependencies") != 0 {
		t.Fatalf("dependency completion hook ran after failed later batch: %#v", snapshot.events)
	}
	if got := len(writerPublicationStatements(snapshot, writerPublicationSavepointSQL)); got != 0 {
		t.Fatalf("later-batch path opened %d nested savepoints, want none", got)
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), before)
	assertWriterFaultIntegrity(t, harness.database)
}

func prepareWriterPublicationBatchPlan(
	t *testing.T,
	harness *writerFaultHarness,
	tx *gorm.DB,
	dependencyCount int,
	selectorCount int,
) (preparedMutation, publicationPlan) {
	t.Helper()
	if dependencyCount < 0 || dependencyCount > maximumDependencyGraphEdges {
		t.Fatalf("dependency count %d is outside the publication ceiling", dependencyCount)
	}
	if selectorCount != 0 && selectorCount != knowledge.MaximumSelectorPatterns {
		t.Fatalf("selector count %d must be zero or the publication ceiling", selectorCount)
	}
	request := writerFaultCreateRequest(
		"writer-publication-batch", "writer-publication-batch-request-0001",
	)
	request.Definition.Selector = writerPublicationBatchSelector(t, selectorCount)
	normalizedScope, err := normalizeWriteScope(harness.scope)
	if err != nil {
		t.Fatalf("normalize publication batch scope: %v", err)
	}
	actor, found := audit.ActorFromContext(harness.actorContext)
	if !found {
		t.Fatal("publication batch actor is absent")
	}
	prepared, err := prepareCreateMutation(normalizedScope, actor, request)
	if err != nil {
		t.Fatalf("prepare publication batch request: %v", err)
	}
	normalized, err := normalizeMutationDefinition(prepared.createRequest.GetDefinition())
	if err != nil {
		t.Fatalf("normalize publication batch definition: %v", err)
	}
	authority, err := authorityFromNormalized(normalized)
	if err != nil {
		t.Fatalf("build publication batch definition authority: %v", err)
	}
	_, state, err := harness.writer.prepareMutationTenant(tx, prepared.scope.tenantID)
	if err != nil {
		t.Fatalf("prepare publication batch tenant: %v", err)
	}
	if result := tx.Exec(`INSERT INTO knowledge_definition_blobs (
		tenant_id, definition_digest, definition_proto,
		definition_bytes, created_at_unix_micro
	) VALUES (?, ?, ?, ?, ?)`,
		writerFaultTenant, authority.digest, authority.bytes, len(authority.bytes), 9_000,
	); result.Error != nil {
		t.Fatalf("seed publication batch definition: %v", result.Error)
	}
	if dependencyCount > 0 {
		if result := tx.Exec(`WITH RECURSIVE target(ordinal) AS (
			VALUES (0)
			UNION ALL
			SELECT ordinal + 1 FROM target WHERE ordinal + 1 < ?
		)
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		)
		SELECT ?, printf('ko_writer_batch_target_%04d', ordinal), 1,
		       ?, ?, 'field_alias', printf('writer-batch-target-%04d', ordinal),
		       'private', 'draft', ?, 0, 'create', NULL, 9000
		FROM target`,
			dependencyCount,
			writerFaultTenant,
			writerFaultApp,
			writerFaultOwner,
			authority.digest,
		); result.Error != nil {
			t.Fatalf("seed publication dependency targets: %v", result.Error)
		}
		if result := tx.Exec(`WITH RECURSIVE target(ordinal) AS (
			VALUES (0)
			UNION ALL
			SELECT ordinal + 1 FROM target WHERE ordinal + 1 < ?
		)
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		)
		SELECT ?, printf('ko_writer_batch_target_%04d', ordinal), 1, 0
		FROM target`, dependencyCount, writerFaultTenant); result.Error != nil {
			t.Fatalf("seal publication dependency targets: %v", result.Error)
		}
		if result := tx.Exec(`WITH RECURSIVE target(ordinal) AS (
			VALUES (0)
			UNION ALL
			SELECT ordinal + 1 FROM target WHERE ordinal + 1 < ?
		)
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		)
		SELECT ?, printf('ko_writer_batch_target_%04d', ordinal), 1,
		       ?, ?, 'field_alias', printf('writer-batch-target-%04d', ordinal),
		       'private', 'draft', 0, '', 0, 0, 0, 0, 0, 46
		FROM target`,
			dependencyCount,
			writerFaultTenant,
			writerFaultApp,
			writerFaultOwner,
		); result.Error != nil {
			t.Fatalf("project publication dependency targets: %v", result.Error)
		}
		if result := tx.Exec(`WITH RECURSIVE target(ordinal) AS (
			VALUES (0)
			UNION ALL
			SELECT ordinal + 1 FROM target WHERE ordinal + 1 < ?
		)
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		)
		SELECT ?, printf('ko_writer_batch_target_%04d', ordinal), 1, 0, 46
		FROM target`, dependencyCount, writerFaultTenant); result.Error != nil {
			t.Fatalf("seal publication dependency target projections: %v", result.Error)
		}
		if result := tx.Exec(`WITH RECURSIVE target(ordinal) AS (
			VALUES (0)
			UNION ALL
			SELECT ordinal + 1 FROM target WHERE ordinal + 1 < ?
		)
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro,
			disabled_at_unix_micro, quarantined_at_unix_micro,
			deleted_at_unix_micro, quarantine_reason
		)
		SELECT ?, printf('ko_writer_batch_target_%04d', ordinal), 1,
		       ?, ?, 'field_alias', printf('writer-batch-target-%04d', ordinal),
		       'private', 'draft', ?, 9000, 9000, NULL, NULL, NULL, NULL
		FROM target`,
			dependencyCount,
			writerFaultTenant,
			writerFaultApp,
			writerFaultOwner,
			authority.digest,
		); result.Error != nil {
			t.Fatalf("publish dependency target registries: %v", result.Error)
		}
	}
	dependencies := make([]publicationDependency, 0, dependencyCount)
	for ordinal := dependencyCount - 1; ordinal >= 0; ordinal-- {
		dependencies = append(dependencies, publicationDependency{
			targetObjectID: writerPublicationDependencyTargetID(ordinal),
			targetVersion:  1,
		})
	}
	now := time.UnixMicro(10_000).UTC()
	return prepared, publicationPlan{
		route:           mutationRouteCreate,
		mutationKind:    "create",
		auditAction:     audit.ActionKnowledgeObjectCreate,
		objectID:        writerPublicationBatchObjectID,
		version:         1,
		state:           StateDraft,
		definition:      authority,
		dependencies:    dependencies,
		ownerID:         writerFaultOwner,
		createdAt:       now,
		updatedAt:       now,
		oldCatalogState: state,
	}
}

func writerPublicationBatchSelector(
	t *testing.T,
	selectorCount int,
) *opensplunkv1.KnowledgeSelector {
	t.Helper()
	selector := &opensplunkv1.KnowledgeSelector{}
	if selectorCount == 0 {
		return selector
	}
	for _, dimension := range writerPublicationSelectorDimensions() {
		patterns := make([]*opensplunkv1.KnowledgeSelectorPattern, 0, knowledge.MaximumSelectorPatternsPerDimension)
		for ordinal := 0; ordinal < knowledge.MaximumSelectorPatternsPerDimension; ordinal++ {
			matchKind := opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT
			if ordinal%2 != 0 {
				matchKind = opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD
			}
			patterns = append(patterns, &opensplunkv1.KnowledgeSelectorPattern{
				MatchKind: matchKind,
				Value:     writerPublicationSelectorValue(dimension, ordinal),
			})
		}
		switch dimension {
		case "index":
			selector.IndexPatterns = patterns
		case "host":
			selector.HostPatterns = patterns
		case "source":
			selector.SourcePatterns = patterns
		case "sourcetype":
			selector.SourcetypePatterns = patterns
		default:
			t.Fatalf("unsupported selector dimension %q", dimension)
		}
	}
	return selector
}

func writerPublicationBatchHook(trace *writerPublicationBatchTrace) writerHook {
	return func(_ context.Context, event writerHookEvent) error {
		switch event.Boundary {
		case writerHookDependencyRowsInserted:
			trace.addEvent("hook:dependencies")
		case writerHookSelectorRowsInserted:
			trace.addEvent("hook:selectors")
		}
		return nil
	}
}

func registerWriterPublicationBatchCapture(
	t *testing.T,
	harness *writerFaultHarness,
	trace *writerPublicationBatchTrace,
) {
	t.Helper()
	const callbackName = "test:capture-writer-publication-batches"
	callback := harness.database.GORMDB().Callback().Create()
	if err := callback.Before("gorm:create").Register(callbackName, func(tx *gorm.DB) {
		switch rows := tx.Statement.Dest.(type) {
		case *[]persistedPublicationDependency:
			trace.addDependencyBatch(*rows)
		case *[]persistedPublicationSelector:
			trace.addSelectorBatch(*rows)
		}
	}); err != nil {
		t.Fatalf("register publication batch capture: %v", err)
	}
	t.Cleanup(func() {
		if err := callback.Remove(callbackName); err != nil {
			t.Errorf("remove publication batch capture: %v", err)
		}
	})
}

func assertWriterPublicationBatchStatements(
	t *testing.T,
	snapshot writerPublicationBatchSnapshot,
	dependencyRows int,
	selectorRows int,
) {
	t.Helper()
	wantDependencies := (dependencyRows + publicationInsertBatchSize - 1) / publicationInsertBatchSize
	wantSelectors := (selectorRows + publicationInsertBatchSize - 1) / publicationInsertBatchSize
	dependencyStatements := writerPublicationStatements(snapshot, writerPublicationDependencySQL)
	selectorStatements := writerPublicationStatements(snapshot, writerPublicationSelectorSQL)
	if len(dependencyStatements) != wantDependencies || len(selectorStatements) != wantSelectors {
		t.Fatalf(
			"publication INSERT statements = dependencies %d/%d selectors %d/%d",
			len(dependencyStatements), wantDependencies, len(selectorStatements), wantSelectors,
		)
	}
	for _, statement := range append(dependencyStatements, selectorStatements...) {
		if statement.failed {
			t.Fatalf("successful publication recorded failed INSERT: %#v", statement)
		}
	}
	if got := len(writerPublicationStatements(snapshot, writerPublicationSavepointSQL)); got != 0 {
		t.Fatalf("publication opened %d nested savepoints, want none", got)
	}
}

func assertWriterPublicationDependencyBatches(
	t *testing.T,
	batches [][]persistedPublicationDependency,
	wantRows int,
) {
	t.Helper()
	wantBatches := (wantRows + publicationInsertBatchSize - 1) / publicationInsertBatchSize
	if len(batches) != wantBatches {
		t.Fatalf("dependency Create batches = %d, want %d", len(batches), wantBatches)
	}
	flattened := make([]persistedPublicationDependency, 0, wantRows)
	for batchIndex, batch := range batches {
		wantSize := min(publicationInsertBatchSize, wantRows-batchIndex*publicationInsertBatchSize)
		if len(batch) != wantSize {
			t.Fatalf("dependency batch %d rows = %d, want %d", batchIndex, len(batch), wantSize)
		}
		flattened = append(flattened, batch...)
	}
	assertWriterPublicationDependencyRows(t, flattened, wantRows)
}

func assertWriterPublicationSelectorBatches(
	t *testing.T,
	batches [][]persistedPublicationSelector,
) {
	t.Helper()
	if len(batches) != 1 || len(batches[0]) != knowledge.MaximumSelectorPatterns {
		t.Fatalf(
			"selector Create batches = %d/%v, want one batch of %d",
			len(batches), writerPublicationBatchSizes(batches), knowledge.MaximumSelectorPatterns,
		)
	}
	assertWriterPublicationSelectorRows(t, batches[0])
}

func assertWriterPublicationDependencyRows(
	t *testing.T,
	rows []persistedPublicationDependency,
	wantRows int,
) {
	t.Helper()
	if len(rows) != wantRows {
		t.Fatalf("dependency rows = %d, want %d", len(rows), wantRows)
	}
	for ordinal, row := range rows {
		if row.TenantID != writerFaultTenant ||
			row.SourceObjectID != writerPublicationBatchObjectID ||
			row.SourceObjectVersion != 1 || row.Ordinal != int64(ordinal) ||
			row.TargetKind != "object" ||
			row.TargetObjectID != writerPublicationDependencyTargetID(ordinal) ||
			row.TargetObjectVersion != 1 || row.DependencyRole != "field_input" {
			t.Fatalf("dependency row %d = %#v", ordinal, row)
		}
	}
}

func assertWriterPublicationSelectorRows(
	t *testing.T,
	rows []persistedPublicationSelector,
) {
	t.Helper()
	if len(rows) != knowledge.MaximumSelectorPatterns {
		t.Fatalf("selector rows = %d, want %d", len(rows), knowledge.MaximumSelectorPatterns)
	}
	index := 0
	for _, dimension := range writerPublicationSelectorDimensions() {
		for ordinal := 0; ordinal < knowledge.MaximumSelectorPatternsPerDimension; ordinal++ {
			row := rows[index]
			matchKind := "exact"
			if ordinal%2 != 0 {
				matchKind = "wildcard"
			}
			if row.TenantID != writerFaultTenant ||
				row.KnowledgeObjectID != writerPublicationBatchObjectID ||
				row.ObjectVersion != 1 || row.Dimension != dimension ||
				row.Ordinal != int64(ordinal) || row.MatchKind != matchKind ||
				row.Value != writerPublicationSelectorValue(dimension, ordinal) {
				t.Fatalf("selector row %d = %#v", index, row)
			}
			index++
		}
	}
}

func assertWriterPublicationHookAfterStatements(
	t *testing.T,
	snapshot writerPublicationBatchSnapshot,
	statementKind string,
	hook string,
) {
	t.Helper()
	lastStatement := -1
	hookIndex := -1
	hookCount := 0
	for index, event := range snapshot.events {
		if event == "sql:"+statementKind {
			lastStatement = index
		}
		if event == hook {
			hookIndex = index
			hookCount++
		}
	}
	if hookCount != 1 || hookIndex <= lastStatement {
		t.Fatalf(
			"publication event order for %s = %#v, want one hook after every %s statement",
			hook, snapshot.events, statementKind,
		)
	}
}

func writerPublicationStatements(
	snapshot writerPublicationBatchSnapshot,
	kind string,
) []writerPublicationBatchStatement {
	result := make([]writerPublicationBatchStatement, 0)
	for _, statement := range snapshot.statements {
		if statement.kind == kind {
			result = append(result, statement)
		}
	}
	return result
}

func writerPublicationEventCount(events []string, want string) int {
	count := 0
	for _, event := range events {
		if event == want {
			count++
		}
	}
	return count
}

func writerPublicationBatchSizes[T any](batches [][]T) []int {
	sizes := make([]int, len(batches))
	for index, batch := range batches {
		sizes[index] = len(batch)
	}
	return sizes
}

func writerPublicationDependencyTargetID(ordinal int) string {
	return fmt.Sprintf("ko_writer_batch_target_%04d", ordinal)
}

func writerPublicationSelectorDimensions() []string {
	return []string{"index", "host", "source", "sourcetype"}
}

func writerPublicationSelectorValue(dimension string, ordinal int) string {
	value := fmt.Sprintf("%s-%02d", dimension, ordinal)
	if ordinal%2 != 0 {
		value += "*"
	}
	return value
}

func (trace *writerPublicationBatchTrace) LogMode(logger.LogLevel) logger.Interface {
	return trace
}

func (*writerPublicationBatchTrace) Info(context.Context, string, ...any)  {}
func (*writerPublicationBatchTrace) Warn(context.Context, string, ...any)  {}
func (*writerPublicationBatchTrace) Error(context.Context, string, ...any) {}

func (trace *writerPublicationBatchTrace) Trace(
	_ context.Context,
	_ time.Time,
	statement func() (string, int64),
	err error,
) {
	sql, _ := statement()
	normalized := strings.ToUpper(strings.TrimSpace(sql))
	kind := ""
	switch {
	case strings.HasPrefix(normalized, "INSERT INTO") &&
		strings.Contains(normalized, "KNOWLEDGE_OBJECT_DEPENDENCIES"):
		kind = writerPublicationDependencySQL
	case strings.HasPrefix(normalized, "INSERT INTO") &&
		strings.Contains(normalized, "KNOWLEDGE_OBJECT_LIST_SELECTOR_PATTERNS"):
		kind = writerPublicationSelectorSQL
	case strings.HasPrefix(normalized, "SAVEPOINT ") ||
		strings.HasPrefix(normalized, "ROLLBACK TO SAVEPOINT "):
		kind = writerPublicationSavepointSQL
	}
	if kind == "" {
		return
	}
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	trace.statements = append(trace.statements, writerPublicationBatchStatement{
		kind: kind, failed: err != nil,
	})
	trace.events = append(trace.events, "sql:"+kind)
}

func (trace *writerPublicationBatchTrace) addEvent(event string) {
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	trace.events = append(trace.events, event)
}

func (trace *writerPublicationBatchTrace) addDependencyBatch(
	rows []persistedPublicationDependency,
) {
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	trace.dependencyBatches = append(
		trace.dependencyBatches, append([]persistedPublicationDependency(nil), rows...),
	)
}

func (trace *writerPublicationBatchTrace) addSelectorBatch(
	rows []persistedPublicationSelector,
) {
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	trace.selectorBatches = append(
		trace.selectorBatches, append([]persistedPublicationSelector(nil), rows...),
	)
}

func (trace *writerPublicationBatchTrace) reset() {
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	trace.statements = nil
	trace.events = nil
	trace.dependencyBatches = nil
	trace.selectorBatches = nil
}

func (trace *writerPublicationBatchTrace) snapshot() writerPublicationBatchSnapshot {
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	result := writerPublicationBatchSnapshot{
		statements: append([]writerPublicationBatchStatement(nil), trace.statements...),
		events:     append([]string(nil), trace.events...),
	}
	for _, batch := range trace.dependencyBatches {
		result.dependencyBatches = append(
			result.dependencyBatches, append([]persistedPublicationDependency(nil), batch...),
		)
	}
	for _, batch := range trace.selectorBatches {
		result.selectorBatches = append(
			result.selectorBatches, append([]persistedPublicationSelector(nil), batch...),
		)
	}
	return result
}
