//go:build open_splunk_knowledge_runtime_acceptance && open_splunk_knowledge_snapshot_acceptance

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchinspection"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type runtimeKnowledgeSnapshotExecution struct {
	ordinal  int32
	compiled clickhouse.CompiledQuery
	valid    bool
}

const runtimeKnowledgeSnapshotExplainText = `[{"Plan":{"Node Type":"ReadNothing"}}]`

type runtimeKnowledgeSnapshotExecutor struct {
	counters     *runtimeKnowledgeAdmissionCounters
	observations chan<- runtimeKnowledgeSnapshotExecution
	releaseFirst <-chan struct{}
	ordinal      atomic.Int32
}

type runtimeKnowledgeSnapshotExplainer struct {
	mu      sync.Mutex
	wantV1  clickhouse.CompiledQuery
	wantV2  clickhouse.CompiledQuery
	queries []clickhouse.CompiledQuery
}

type runtimeKnowledgeSnapshotSearches struct {
	manager *searchjobs.Manager
	calls   atomic.Int32
}

func (searches *runtimeKnowledgeSnapshotSearches) CompletedExecutionSnapshotFor(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
) (searchjobs.ExecutionSnapshot, error) {
	searches.calls.Add(1)
	return searches.manager.CompletedExecutionSnapshotFor(ctx, access, jobID)
}

func (explainer *runtimeKnowledgeSnapshotExplainer) Explain(
	ctx context.Context,
	query clickhouse.CompiledQuery,
) (queryexec.ExplainResult, error) {
	if ctx == nil {
		return queryexec.ExplainResult{}, errors.New("runtime snapshot Explainer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return queryexec.ExplainResult{}, err
	}
	detached, ok := query.CloneForExecution()
	if !ok {
		return queryexec.ExplainResult{}, errors.New("runtime snapshot Explainer query is invalid")
	}

	explainer.mu.Lock()
	explainer.queries = append(explainer.queries, detached)
	var version uint64
	switch {
	case detached.EqualForExecution(explainer.wantV1):
		version = 1
	case detached.EqualForExecution(explainer.wantV2):
		version = 2
	}
	explainer.mu.Unlock()
	if version == 0 {
		return queryexec.ExplainResult{}, errors.New("runtime snapshot Explainer received unexpected authority")
	}
	if err := ctx.Err(); err != nil {
		return queryexec.ExplainResult{}, err
	}
	return queryexec.ExplainResult{
		Text:    runtimeKnowledgeSnapshotExplainText,
		QueryID: fmt.Sprintf("open-splunk-explain-runtime-knowledge-v%d", version),
	}, nil
}

func (explainer *runtimeKnowledgeSnapshotExplainer) callCount() int {
	explainer.mu.Lock()
	defer explainer.mu.Unlock()
	return len(explainer.queries)
}

func (explainer *runtimeKnowledgeSnapshotExplainer) recordedQueries() []clickhouse.CompiledQuery {
	explainer.mu.Lock()
	defer explainer.mu.Unlock()
	queries := make([]clickhouse.CompiledQuery, len(explainer.queries))
	for index := range explainer.queries {
		clone, ok := explainer.queries[index].CloneForExecution()
		if !ok {
			return nil
		}
		queries[index] = clone
	}
	return queries
}

func waitForRuntimeKnowledgeSnapshotExecution(
	t *testing.T,
	observations <-chan runtimeKnowledgeSnapshotExecution,
	wantOrdinal int32,
	prelude knowledgeprogram.Program,
	scope knowledgecatalog.ResolutionScope,
) clickhouse.CompiledQuery {
	t.Helper()
	select {
	case observation := <-observations:
		if observation.ordinal != wantOrdinal || !observation.valid ||
			!observation.compiled.HasValidExecutionSeal() {
			t.Fatalf("ACTIVE execution %d observation = %#v", wantOrdinal, observation)
		}
		evidence, ok := observation.compiled.KnowledgeSnapshotEvidenceFor(prelude)
		if !ok || evidence.KnowledgeProgramObjectCount() != prelude.ObjectCount() ||
			evidence.TenantID() != scope.TenantID ||
			!slices.Equal(evidence.EffectiveIndexes(), scope.EffectiveAuthorizedIndexes) {
			t.Fatalf("ACTIVE execution %d compiler evidence = (%#v, %t)", wantOrdinal, evidence, ok)
		}
		return observation.compiled
	case <-time.After(3 * time.Second):
		t.Fatalf("executor did not observe ACTIVE job %d", wantOrdinal)
		return clickhouse.CompiledQuery{}
	}
}

func requireRuntimeKnowledgeSnapshotSummary(
	t *testing.T,
	summary *opensplunkv1.KnowledgeSnapshotSummary,
	objectID string,
	version uint64,
	wantDigest []byte,
) []byte {
	t.Helper()
	if summary == nil || summary.GetRef() == nil ||
		summary.GetRef().GetObjectCount() != 1 ||
		len(summary.GetObjects()) != 1 || summary.GetObjects()[0] == nil ||
		summary.GetObjects()[0].GetResolutionOrdinal() != 0 ||
		summary.GetObjects()[0].GetObjectType() !=
			opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS ||
		summary.GetObjects()[0].GetStage() !=
			opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS ||
		summary.GetObjects()[0].GetAuthorizedObject() == nil ||
		summary.GetObjects()[0].GetAuthorizedObject().GetKnowledgeObjectId() != objectID ||
		summary.GetObjects()[0].GetAuthorizedObject().GetVersion() != version ||
		len(summary.GetRef().GetSnapshotSha256()) == 0 ||
		(wantDigest != nil && !bytes.Equal(summary.GetRef().GetSnapshotSha256(), wantDigest)) {
		t.Fatalf("ACTIVE v%d knowledge summary = %v", version, summary)
	}
	return slices.Clone(summary.GetRef().GetSnapshotSha256())
}

func requireRuntimeKnowledgeInspectionResult(
	t *testing.T,
	result searchinspection.Result,
	wantCompiled clickhouse.CompiledQuery,
	objectID string,
	version uint64,
	destination string,
	wantDigest []byte,
) searchinspection.PlanStage {
	t.Helper()
	if err := searchinspection.ValidateResult(result); err != nil {
		t.Fatalf("ACTIVE v%d inspection validation: %v", version, err)
	}
	if result.GeneratedSQL != wantCompiled.SQL ||
		result.DiagnosticQueryID !=
			fmt.Sprintf("open-splunk-explain-runtime-knowledge-v%d", version) ||
		result.ExplainText != runtimeKnowledgeSnapshotExplainText ||
		!slices.Equal(result.PhysicalPlan.NodeTypes, []string{"ReadNothing"}) ||
		len(result.PhysicalPlan.Reads) != 0 ||
		result.Plan.Output.Kind != searchinspection.OutputKindStatic ||
		!slices.Equal(result.Plan.Output.Fields, []string{"message"}) ||
		result.Plan.Output.MaxDynamicFields != 0 {
		t.Fatalf("ACTIVE v%d inspection result = %#v", version, result)
	}
	requireRuntimeKnowledgeSnapshotSummary(
		t,
		result.KnowledgeSnapshot,
		objectID,
		version,
		wantDigest,
	)

	var generated *searchinspection.PlanStage
	for index := range result.Plan.Stages {
		stage := &result.Plan.Stages[index]
		if stage.Operator != "CopyFieldAlias" {
			continue
		}
		if generated != nil {
			t.Fatalf("ACTIVE v%d inspection has multiple alias stages: %#v", version, result.Plan)
		}
		generated = stage
	}
	wantObject := searchinspection.RedactedObjectProvenance{
		Ordinal:    0,
		ObjectType: opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
		Stage:      opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
	}
	if generated == nil || generated.Index != 1 || generated.SourceRange != nil ||
		!slices.Equal(generated.InputFields, []string{"index", "source_field"}) ||
		!slices.Equal(generated.OutputFields, []string{destination}) ||
		!slices.Equal(generated.KnowledgeObjects, []searchinspection.RedactedObjectProvenance{wantObject}) ||
		!slices.Equal(generated.OutputProvenance, []searchinspection.OutputProvenance{{
			Field: destination, ObjectOrdinal: 0,
		}}) {
		t.Fatalf("ACTIVE v%d generated alias stage = %#v", version, generated)
	}
	return *generated
}

func (executor *runtimeKnowledgeSnapshotExecutor) Execute(
	ctx context.Context,
	compiled clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	ordinal := executor.ordinal.Add(1)
	detached, valid := compiled.CloneForExecution()
	select {
	case executor.observations <- runtimeKnowledgeSnapshotExecution{
		ordinal: ordinal, compiled: detached, valid: valid,
	}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if ordinal == 1 {
		select {
		case <-executor.releaseFirst:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return (runtimeKnowledgeAdmissionExecutor{counters: executor.counters}).Execute(
		ctx,
		compiled,
		sink,
	)
}

// TestKnowledgeSnapshotAcceptanceManagerRetainsWriterResolvedActiveVersions proves
// test-only Writer→Resolver→Manager retention and real searchinspection.Service
// consumption through a deterministic fake Explainer. It does not prove ClickHouse
// EXPLAIN or rows, production wiring, HTTP/server projection or route, capability,
// or browser behavior.
func TestKnowledgeSnapshotAcceptanceManagerRetainsWriterResolvedActiveVersions(t *testing.T) {
	runtime, database := newRuntimeKnowledgeTestRuntime(t)
	defer func() { _ = database.Close() }()
	createRuntimeKnowledgeTestApp(t, database)
	createRuntimeKnowledgeTestIndex(t, database)

	actorContext, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "runtime-knowledge-snapshot-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatal(err)
	}
	writeScope := knowledgecatalog.WriteScope{
		TenantID:       runtimeKnowledgeTestTenant,
		OwnerID:        runtimeKnowledgeTestOwner,
		WritableAppIDs: []string{runtimeKnowledgeTestApp},
	}
	createRequest := runtimeKnowledgeTestCreateRequest(
		"snapshot_alias",
		"runtime-knowledge-snapshot-create-0001",
	)
	createRequest.InitialState = opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE
	createRequest.Definition.Selector = &opensplunkv1.KnowledgeSelector{
		IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "main"}},
	}
	published, err := runtime.writer.Create(actorContext, writeScope, createRequest)
	if err != nil {
		t.Fatalf("publish ACTIVE v1: %v", err)
	}
	objectV1 := published.GetKnowledgeObject()
	if objectV1.GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE ||
		objectV1.GetVersion() != 1 {
		t.Fatalf("published ACTIVE v1 = %v", objectV1)
	}

	resolutionScope := knowledgecatalog.ResolutionScope{
		TenantID:                   runtimeKnowledgeTestTenant,
		PrincipalID:                runtimeKnowledgeTestOwner,
		AppID:                      runtimeKnowledgeTestApp,
		EffectiveAuthorizedIndexes: []string{"main"},
	}
	resolvedV1, err := runtime.resolver.Resolve(t.Context(), resolutionScope)
	if err != nil {
		t.Fatalf("resolve ACTIVE v1: %v", err)
	}
	preludeV1 := resolvedV1.Prelude()
	if preludeV1.IsZero() || preludeV1.ObjectCount() != 1 {
		t.Fatalf("ACTIVE v1 prelude = zero:%t objects:%d", preludeV1.IsZero(), preludeV1.ObjectCount())
	}

	counters := &runtimeKnowledgeAdmissionCounters{finalized: make(chan struct{}, 2)}
	observations := make(chan runtimeKnowledgeSnapshotExecution, 2)
	releaseFirst := make(chan struct{})
	executor := &runtimeKnowledgeSnapshotExecutor{
		counters: counters, observations: observations, releaseFirst: releaseFirst,
	}
	managerConfig := runtimeKnowledgeAdmissionManagerConfig(runtime.resolver, counters)
	managerConfig.Executor = executor
	managerConfig.NewID = func() string {
		return fmt.Sprintf("runtime-knowledge-snapshot-%04d", counters.ids.Add(1))
	}
	manager, err := searchjobs.New(managerConfig)
	if err != nil {
		t.Fatalf("create test-only knowledge manager: %v", err)
	}
	defer func() {
		if closeErr := manager.Close(); closeErr != nil {
			t.Errorf("close test-only knowledge manager: %v", closeErr)
		}
	}()

	request := runtimeKnowledgeSearchRequest(t)
	jobV1, err := manager.Create(t.Context(), request)
	if err != nil {
		t.Fatalf("admit ACTIVE v1: %v", err)
	}
	jobV1Summary := jobV1.KnowledgeSnapshot
	wantV1SummaryDigest := requireRuntimeKnowledgeSnapshotSummary(
		t,
		jobV1Summary,
		objectV1.GetKnowledgeObjectId(),
		1,
		nil,
	)
	observedV1 := waitForRuntimeKnowledgeSnapshotExecution(
		t,
		observations,
		1,
		preludeV1,
		resolutionScope,
	)

	definitionV2 := proto.Clone(objectV1.GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
	definitionV2.GetFieldAlias().DestinationField = "destination_snapshot_alias_v2"
	updated, err := runtime.writer.Update(
		actorContext,
		writeScope,
		&opensplunkv1.UpdateKnowledgeObjectRequest{
			KnowledgeObjectId: objectV1.GetKnowledgeObjectId(),
			ExpectedVersion:   1,
			Definition:        definitionV2,
			UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"field_alias"}},
			ClientRequestId:   "runtime-knowledge-snapshot-update-0001",
		},
	)
	if err != nil {
		t.Fatalf("publish ACTIVE v2 while v1 execution is paused: %v", err)
	}
	objectV2 := updated.GetKnowledgeObject()
	if objectV2.GetKnowledgeObjectId() != objectV1.GetKnowledgeObjectId() ||
		objectV2.GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE ||
		objectV2.GetVersion() != 2 ||
		objectV2.GetDefinition().GetFieldAlias().GetDestinationField() !=
			"destination_snapshot_alias_v2" {
		t.Fatalf("published ACTIVE v2 = %v", objectV2)
	}
	resolvedV2, err := runtime.resolver.Resolve(t.Context(), resolutionScope)
	if err != nil {
		t.Fatalf("resolve ACTIVE v2: %v", err)
	}
	preludeV2 := resolvedV2.Prelude()
	commitmentV1, commitmentV1OK := preludeV1.Commitment()
	commitmentV2, commitmentV2OK := preludeV2.Commitment()
	if preludeV2.IsZero() || preludeV2.ObjectCount() != 1 || preludeV1.Equal(preludeV2) ||
		!commitmentV1OK || !commitmentV2OK || commitmentV1 == commitmentV2 {
		t.Fatalf(
			"ACTIVE program rotation = v1(zero=%t objects=%d commitment=%x/%t) v2(zero=%t objects=%d commitment=%x/%t)",
			preludeV1.IsZero(), preludeV1.ObjectCount(), commitmentV1, commitmentV1OK,
			preludeV2.IsZero(), preludeV2.ObjectCount(), commitmentV2, commitmentV2OK,
		)
	}

	// The Create result is detached from Manager retention. Mutating it while
	// job v1 is still executing cannot rotate the retained snapshot authority.
	jobV1Summary.Ref.SnapshotSha256[0] ^= 0xff
	jobV1Summary.Objects[0].GetAuthorizedObject().Version = 99

	close(releaseFirst)
	completedV1 := waitForRuntimeKnowledgeJobState(t, manager, jobV1.ID, searchjobs.StateCompleted)
	if completedV1.Failure != nil {
		t.Fatalf("ACTIVE v1 completion = %#v", completedV1)
	}
	executionV1, err := manager.CompletedExecutionSnapshotFor(
		t.Context(),
		searchjobs.AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		jobV1.ID,
	)
	if err != nil {
		t.Fatalf("read ACTIVE v1 execution: %v", err)
	}
	retainedV1, err := executionV1.OpenRetainedKnowledgeExecution()
	if err != nil || retainedV1 == nil || !retainedV1.KnowledgePrelude.Equal(preludeV1) ||
		!retainedV1.CompiledQuery.EqualForExecution(observedV1) {
		t.Fatalf("open ACTIVE v1 retained execution = (%#v, %v)", retainedV1, err)
	}
	requireRuntimeKnowledgeSnapshotSummary(
		t,
		retainedV1.KnowledgeSummary,
		objectV1.GetKnowledgeObjectId(),
		1,
		wantV1SummaryDigest,
	)

	access := searchjobs.AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}
	wrongAccess := searchjobs.AccessScope{TenantID: request.TenantID, OwnerID: "other-owner"}
	if wrong, wrongErr := manager.CompletedExecutionSnapshotFor(
		t.Context(), wrongAccess, jobV1.ID,
	); !errors.Is(wrongErr, searchjobs.ErrNotFound) || wrong.ID != "" ||
		wrong.CompiledQuery != nil || !wrong.KnowledgeSnapshot.IsZero() {
		t.Fatalf("wrong-owner metadata read = (%#v, %v)", wrong, wrongErr)
	}
	if wrongLease, wrong, wrongErr := manager.AcquireExecutionFor(
		t.Context(), wrongAccess, jobV1.ID,
	); !errors.Is(wrongErr, searchjobs.ErrNotFound) || wrongLease != nil ||
		wrong.ID != "" || wrong.CompiledQuery != nil || !wrong.KnowledgeSnapshot.IsZero() {
		t.Fatalf("wrong-owner execution acquisition = (%v, %#v, %v)", wrongLease, wrong, wrongErr)
	}
	leaseV1, leasedV1, err := manager.AcquireExecutionFor(t.Context(), access, jobV1.ID)
	if err != nil {
		t.Fatalf("acquire ACTIVE v1 execution: %v", err)
	}
	if !executionV1.Equal(leasedV1) || !leasedV1.ValidFor(leaseV1) {
		_ = leaseV1.Close()
		t.Fatal("ACTIVE v1 lease does not match its Manager-sealed execution")
	}
	if err := leaseV1.Close(); err != nil {
		t.Fatalf("close ACTIVE v1 execution lease: %v", err)
	}

	retainedV1.CompiledQuery.SQL += " -- caller mutation"
	retainedV1.KnowledgeSummary.Ref.SnapshotSha256[0] ^= 0xff
	retainedV1.KnowledgePrelude = preludeV2
	freshV1, err := executionV1.OpenRetainedKnowledgeExecution()
	if err != nil || freshV1 == nil || !freshV1.KnowledgePrelude.Equal(preludeV1) ||
		!freshV1.CompiledQuery.EqualForExecution(observedV1) ||
		!bytes.Equal(freshV1.KnowledgeSummary.GetRef().GetSnapshotSha256(), wantV1SummaryDigest) {
		t.Fatalf("fresh ACTIVE v1 after caller mutation = (%#v, %v)", freshV1, err)
	}
	tamperedV1 := executionV1
	tamperedCompiledV1, ok := executionV1.CompiledQuery.CloneForExecution()
	if !ok || len(tamperedCompiledV1.Args) == 0 {
		t.Fatal("ACTIVE v1 retained compiler authority cannot be cloned for tamper probe")
	}
	tamperedCompiledV1.Args = slices.Clone(tamperedCompiledV1.Args)
	tamperedCompiledV1.Args[0] = "caller-tampered"
	tamperedV1.CompiledQuery = &tamperedCompiledV1
	if opened, openErr := tamperedV1.OpenRetainedKnowledgeExecution(); opened != nil ||
		!errors.Is(openErr, searchjobs.ErrResultsUnavailable) {
		t.Fatalf("tampered ACTIVE v1 opened = (%#v, %v)", opened, openErr)
	}

	jobV2, err := manager.Create(t.Context(), request)
	if err != nil {
		t.Fatalf("admit ACTIVE v2: %v", err)
	}
	jobV2Summary := jobV2.KnowledgeSnapshot
	if jobV2.ID == jobV1.ID {
		t.Fatalf("ACTIVE v2 reused job ID %q", jobV2.ID)
	}
	wantV2SummaryDigest := requireRuntimeKnowledgeSnapshotSummary(
		t,
		jobV2Summary,
		objectV2.GetKnowledgeObjectId(),
		2,
		nil,
	)
	if bytes.Equal(wantV2SummaryDigest, wantV1SummaryDigest) {
		t.Fatal("ACTIVE v2 reused the v1 snapshot digest")
	}
	observedV2 := waitForRuntimeKnowledgeSnapshotExecution(
		t,
		observations,
		2,
		preludeV2,
		resolutionScope,
	)
	if observedV2.EqualForExecution(observedV1) {
		t.Fatal("ACTIVE v2 compiler authority equals v1")
	}
	if _, oldProgramOK := observedV2.KnowledgeSnapshotEvidenceFor(preludeV1); oldProgramOK {
		t.Fatal("ACTIVE v2 compiler authority reopened for v1 program")
	}
	completedV2 := waitForRuntimeKnowledgeJobState(t, manager, jobV2.ID, searchjobs.StateCompleted)
	if completedV2.Failure != nil {
		t.Fatalf("ACTIVE v2 completion = %#v", completedV2)
	}
	executionV2, err := manager.CompletedExecutionSnapshotFor(t.Context(), access, jobV2.ID)
	if err != nil {
		t.Fatalf("read ACTIVE v2 execution: %v", err)
	}
	retainedV2, err := executionV2.OpenRetainedKnowledgeExecution()
	if err != nil || retainedV2 == nil || !retainedV2.KnowledgePrelude.Equal(preludeV2) ||
		retainedV2.KnowledgePrelude.Equal(preludeV1) ||
		!retainedV2.CompiledQuery.EqualForExecution(observedV2) {
		t.Fatalf("open ACTIVE v2 retained execution = (%#v, %v)", retainedV2, err)
	}
	requireRuntimeKnowledgeSnapshotSummary(
		t,
		retainedV2.KnowledgeSummary,
		objectV2.GetKnowledgeObjectId(),
		2,
		wantV2SummaryDigest,
	)
	freshExecutionV1, err := manager.CompletedExecutionSnapshotFor(t.Context(), access, jobV1.ID)
	if err != nil || !executionV1.Equal(freshExecutionV1) {
		t.Fatalf("ACTIVE v1 authority changed after v2 completion = (%#v, %v)", freshExecutionV1, err)
	}
	if executionV1.Equal(executionV2) || executionV2.Equal(executionV1) {
		t.Fatal("ACTIVE v1 and v2 Manager authorities compare equal")
	}
	rotated := executionV1
	rotated.CompiledQuery = executionV2.CompiledQuery
	rotated.KnowledgeSnapshot = executionV2.KnowledgeSnapshot
	if opened, openErr := rotated.OpenRetainedKnowledgeExecution(); opened != nil ||
		!errors.Is(openErr, searchjobs.ErrResultsUnavailable) {
		t.Fatalf("cross-job v2 rotation onto v1 seal opened = (%#v, %v)", opened, openErr)
	}

	wantInspectionV1, ok := freshV1.CompiledQuery.CloneForExecution()
	if !ok {
		t.Fatal("ACTIVE v1 Manager-retained compiler authority cannot be inspected")
	}
	wantInspectionV2, ok := retainedV2.CompiledQuery.CloneForExecution()
	if !ok {
		t.Fatal("ACTIVE v2 Manager-retained compiler authority cannot be inspected")
	}
	inspectionSearches := &runtimeKnowledgeSnapshotSearches{manager: manager}
	inspectionExplainer := &runtimeKnowledgeSnapshotExplainer{
		wantV1: wantInspectionV1,
		wantV2: wantInspectionV2,
	}
	// A retained Knowledge inspection must ignore this deliberately distinct
	// compiler and send the Manager-minted authority directly to Explain.
	inspectionCompiler := clickhouse.Compiler{
		Database: "inspection_recompile_forbidden",
		Table:    "inspection_recompile_forbidden",
	}
	inspectionService, err := searchinspection.New(searchinspection.Config{
		Searches:      inspectionSearches,
		Compiler:      inspectionCompiler,
		Explainer:     inspectionExplainer,
		MaxConcurrent: 1,
		MaxRuntime:    time.Second,
	})
	if err != nil {
		t.Fatalf("create retained search inspection service: %v", err)
	}
	defer func() {
		closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := inspectionService.Close(closeContext); closeErr != nil {
			t.Errorf("close retained search inspection service: %v", closeErr)
		}
	}()

	wrongInspection, wrongInspectionErr := inspectionService.Inspect(
		t.Context(),
		wrongAccess,
		searchinspection.Request{SearchJobID: jobV1.ID},
	)
	if !errors.Is(wrongInspectionErr, searchjobs.ErrNotFound) ||
		wrongInspectionErr.Error() != searchjobs.ErrNotFound.Error() ||
		!reflect.DeepEqual(wrongInspection, searchinspection.Result{}) ||
		inspectionSearches.calls.Load() != 1 ||
		inspectionExplainer.callCount() != 0 {
		t.Fatalf(
			"wrong-owner inspection = (%#v, %v), reads/explains=%d/%d",
			wrongInspection,
			wrongInspectionErr,
			inspectionSearches.calls.Load(),
			inspectionExplainer.callCount(),
		)
	}

	inspect := func(jobID string) searchinspection.Result {
		t.Helper()
		beforeReads := inspectionSearches.calls.Load()
		beforeExplains := inspectionExplainer.callCount()
		result, inspectErr := inspectionService.Inspect(
			t.Context(),
			access,
			searchinspection.Request{SearchJobID: jobID},
		)
		if inspectErr != nil {
			t.Fatalf("inspect retained job %q: %v", jobID, inspectErr)
		}
		if got := inspectionSearches.calls.Load(); got != beforeReads+2 {
			t.Fatalf("inspection metadata reads = %d, want %d", got, beforeReads+2)
		}
		if got := inspectionExplainer.callCount(); got != beforeExplains+1 {
			t.Fatalf("inspection Explainer calls = %d, want %d", got, beforeExplains+1)
		}
		return result
	}

	inspectionV1 := inspect(jobV1.ID)
	stageV1 := requireRuntimeKnowledgeInspectionResult(
		t,
		inspectionV1,
		wantInspectionV1,
		objectV1.GetKnowledgeObjectId(),
		1,
		"destination_snapshot_alias",
		wantV1SummaryDigest,
	)
	inspectionV2 := inspect(jobV2.ID)
	stageV2 := requireRuntimeKnowledgeInspectionResult(
		t,
		inspectionV2,
		wantInspectionV2,
		objectV2.GetKnowledgeObjectId(),
		2,
		"destination_snapshot_alias_v2",
		wantV2SummaryDigest,
	)
	if proto.Equal(inspectionV1.KnowledgeSnapshot, inspectionV2.KnowledgeSnapshot) ||
		bytes.Equal(
			inspectionV1.KnowledgeSnapshot.GetRef().GetSnapshotSha256(),
			inspectionV2.KnowledgeSnapshot.GetRef().GetSnapshotSha256(),
		) ||
		slices.Equal(stageV1.OutputFields, stageV2.OutputFields) ||
		slices.Equal(stageV1.OutputProvenance, stageV2.OutputProvenance) ||
		!slices.Equal(stageV1.KnowledgeObjects, stageV2.KnowledgeObjects) {
		t.Fatalf(
			"ACTIVE inspection rotation = v1:%#v v2:%#v",
			inspectionV1.KnowledgeSnapshot,
			inspectionV2.KnowledgeSnapshot,
		)
	}

	inspectionV1.KnowledgeSnapshot.Ref.SnapshotSha256[0] ^= 0xff
	inspectionV1.KnowledgeSnapshot.Objects[0].GetAuthorizedObject().Version = 99
	inspectionV1.Plan.Stages[stageV1.Index].InputFields[0] = "caller_input"
	inspectionV1.Plan.Stages[stageV1.Index].OutputFields[0] = "caller_output"
	inspectionV1.Plan.Stages[stageV1.Index].KnowledgeObjects[0].Ordinal = 99
	inspectionV1.Plan.Stages[stageV1.Index].OutputProvenance[0].Field = "caller_output"
	inspectionV1.Plan.Output.Fields[0] = "caller_output"
	inspectionV1.PhysicalPlan.NodeTypes[0] = "CallerMutation"
	inspectionV1.GeneratedSQL += " -- caller mutation"
	requireRuntimeKnowledgeInspectionResult(
		t,
		inspectionV2,
		wantInspectionV2,
		objectV2.GetKnowledgeObjectId(),
		2,
		"destination_snapshot_alias_v2",
		wantV2SummaryDigest,
	)
	freshInspectionV1 := inspect(jobV1.ID)
	requireRuntimeKnowledgeInspectionResult(
		t,
		freshInspectionV1,
		wantInspectionV1,
		objectV1.GetKnowledgeObjectId(),
		1,
		"destination_snapshot_alias",
		wantV1SummaryDigest,
	)

	recordedQueries := inspectionExplainer.recordedQueries()
	wantQueries := []clickhouse.CompiledQuery{
		wantInspectionV1,
		wantInspectionV2,
		wantInspectionV1,
	}
	if len(recordedQueries) != len(wantQueries) {
		t.Fatalf("recorded inspection queries = %d, want %d", len(recordedQueries), len(wantQueries))
	}
	for index := range wantQueries {
		if !recordedQueries[index].EqualForExecution(wantQueries[index]) {
			t.Fatalf("inspection query %d did not equal Manager-retained authority", index)
		}
	}
	if inspectionSearches.calls.Load() != 1+2*int32(len(wantQueries)) {
		t.Fatalf("total inspection metadata reads = %d", inspectionSearches.calls.Load())
	}

	for ordinal := 1; ordinal <= 2; ordinal++ {
		select {
		case <-counters.finalized:
		case <-time.After(3 * time.Second):
			t.Fatalf("journal did not finalize ACTIVE job %d", ordinal)
		}
	}
	if counters.snapshots.Load() != 2 || counters.journalAdmissions.Load() != 2 ||
		counters.journalFinalizations.Load() != 2 || counters.executions.Load() != 2 ||
		len(manager.List()) != 2 {
		t.Fatalf(
			"dual-version lifecycle counters = snapshots:%d journal:%d/%d executions:%d jobs:%d",
			counters.snapshots.Load(), counters.journalAdmissions.Load(),
			counters.journalFinalizations.Load(), counters.executions.Load(), len(manager.List()),
		)
	}
}
