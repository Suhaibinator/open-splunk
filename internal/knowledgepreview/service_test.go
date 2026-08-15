package knowledgepreview

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	previewTestTenant = "tenant-preview"
	previewTestOwner  = "owner-preview"
	previewTestApp    = "app_aaaaaaaaaaaaaaaaaaaaaA"
	previewTestJob    = "retained-preview-search"
)

type compilerFunc func(context.Context, searchjobs.ExecutionSnapshot, knowledgeprogram.Program) (clickhouse.CompiledQuery, error)

func (function compilerFunc) CompilePreview(
	ctx context.Context,
	execution searchjobs.ExecutionSnapshot,
	program knowledgeprogram.Program,
) (clickhouse.CompiledQuery, error) {
	return function(ctx, execution, program)
}

type executorFunc func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error

func (function executorFunc) Execute(
	ctx context.Context,
	compiled clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	return function(ctx, compiled, sink)
}

type previewFixture struct {
	database *control.DB
	writer   *knowledgecatalog.Writer
	manager  *searchjobs.Manager
	access   searchjobs.AccessScope
	scope    knowledgecatalog.ValidationScope
}

func newPreviewFixture(t *testing.T) previewFixture {
	t.Helper()
	ctx := context.Background()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name: "main", SearchEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	cursorKey := []byte("knowledge-preview-test-cursor-key-at-least-32-bytes")
	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey:   cursorKey,
		IDGenerator: func() (string, error) { return previewTestApp, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apps.CreateApp(
		ctx,
		control.AppAccessScope{TenantID: previewTestTenant},
		control.AppDefinition{Slug: "preview", DisplayName: "Preview"},
	); err != nil {
		t.Fatal(err)
	}
	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: cursorKey})
	if err != nil {
		t.Fatal(err)
	}
	var objectID atomic.Uint64
	writer, err := knowledgecatalog.NewWriter(
		database,
		auditStore,
		knowledgecatalog.WriterOptions{
			Clock: func() time.Time {
				return time.Date(2026, time.August, 10, 12, 0, int(objectID.Load()), 0, time.UTC)
			},
			IDGenerator: func() (string, error) {
				return fmt.Sprintf("preview-object-%04d", objectID.Add(1)), nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := knowledgecatalog.New(database, knowledgecatalog.Options{CursorKey: cursorKey})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := store.NewResolver(knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: executorFunc(func(
			_ context.Context,
			_ clickhouse.CompiledQuery,
			sink searchjobs.ResultSink,
		) error {
			if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
				Name: "status", Kind: searchjobs.ValueKindSigned,
			}}}); err != nil {
				return err
			}
			return sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(200)})
		}),
		Snapshotter:       snapshotterFunc(func(context.Context) (uint64, error) { return 17, nil }),
		KnowledgeResolver: resolver,
		Compiler:          clickhouse.Compiler{Database: "open_splunk", Table: "events"},
		MaxConcurrent:     1,
		MaxResultLeases:   1,
		RetentionTTL:      time.Hour,
		CleanupInterval:   -1,
		Now:               func() time.Time { return now },
		NewID:             func() string { return previewTestJob },
		CursorKey:         []byte("knowledge-preview-search-cursor-key-at-least-32-bytes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	rangeIntent, err := searchtime.NewAbsoluteRange(now.Add(-time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(ctx, searchjobs.CreateRequest{
		SPL:               "index=main | table status",
		OwnerID:           previewTestOwner,
		TenantID:          previewTestTenant,
		AppID:             previewTestApp,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         rangeIntent,
	})
	if err != nil {
		t.Fatal(err)
	}
	access := searchjobs.AccessScope{TenantID: previewTestTenant, OwnerID: previewTestOwner}
	waitForCompletedPreviewJob(t, manager, access, created.ID)
	return previewFixture{
		database: database,
		writer:   writer,
		manager:  manager,
		access:   access,
		scope: knowledgecatalog.ValidationScope{
			Read: knowledgecatalog.ReadScope{
				TenantID: previewTestTenant, OwnerID: previewTestOwner,
				ReadableAppIDs: []string{previewTestApp},
			},
			Write: knowledgecatalog.WriteScope{
				TenantID: previewTestTenant, OwnerID: previewTestOwner,
				WritableAppIDs: []string{previewTestApp},
			},
		},
	}
}

type snapshotterFunc func(context.Context) (uint64, error)

func (function snapshotterFunc) VisibilityCutoff(ctx context.Context) (uint64, error) {
	return function(ctx)
}

func waitForCompletedPreviewJob(
	t *testing.T,
	manager *searchjobs.Manager,
	access searchjobs.AccessScope,
	id string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.GetFor(access, id)
		if err == nil && job.State == searchjobs.StateCompleted {
			return
		}
		if err == nil && (job.State == searchjobs.StateFailed || job.State == searchjobs.StateCanceled) {
			t.Fatalf("retained job reached %s: %#v", job.State, job.Failure)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for retained preview job")
}

func invalidPreviewRequest() *opensplunkv1.PreviewKnowledgeObjectRequest {
	return &opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: previewTestJob,
		Definition: &opensplunkv1.KnowledgeObjectDefinition{
			AppId: previewTestApp, Name: "invalid-preview",
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		},
	}
}

func validAliasPreviewRequest() *opensplunkv1.PreviewKnowledgeObjectRequest {
	return &opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: previewTestJob,
		Definition: &opensplunkv1.KnowledgeObjectDefinition{
			AppId: previewTestApp, Name: "preview-alias",
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
				FieldAlias: &opensplunkv1.FieldAliasDefinition{
					SourceField: "_raw", DestinationField: "preview_value",
					OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
				},
			},
		},
	}
}

func neverCompiler(calls *atomic.Int64) Compiler {
	return compilerFunc(func(
		context.Context,
		searchjobs.ExecutionSnapshot,
		knowledgeprogram.Program,
	) (clickhouse.CompiledQuery, error) {
		calls.Add(1)
		return clickhouse.CompiledQuery{}, errors.New("compiler must not run")
	})
}

func neverExecutor(calls *atomic.Int64) searchjobs.Executor {
	return executorFunc(func(
		context.Context,
		clickhouse.CompiledQuery,
		searchjobs.ResultSink,
	) error {
		calls.Add(1)
		return errors.New("executor must not run")
	})
}

func TestPreviewMaximumRowsPolicy(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *uint32
		want  uint32
		valid bool
	}{
		{name: "absent", want: DefaultMaximumRows, valid: true},
		{name: "zero", value: new(uint32(0))},
		{name: "one", value: new(uint32(1)), want: 1, valid: true},
		{name: "maximum", value: new(MaximumRows), want: MaximumRows, valid: true},
		{name: "overflow", value: new(MaximumRows + 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := previewMaximumRows(test.value)
			if test.valid && (err != nil || got != test.want) {
				t.Fatalf("previewMaximumRows() = (%d, %v), want %d", got, err, test.want)
			}
			if !test.valid && !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("previewMaximumRows() error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

type typedNilCompiler struct{}

func (*typedNilCompiler) CompilePreview(
	context.Context,
	searchjobs.ExecutionSnapshot,
	knowledgeprogram.Program,
) (clickhouse.CompiledQuery, error) {
	panic("typed-nil compiler must not be called")
}

type typedNilExecutor struct{}

func (*typedNilExecutor) Execute(
	context.Context,
	clickhouse.CompiledQuery,
	searchjobs.ResultSink,
) error {
	panic("typed-nil executor must not be called")
}

type typedNilSearches struct{}

func (*typedNilSearches) AcquireExecutionFor(
	context.Context,
	searchjobs.AccessScope,
	string,
) (searchjobs.ResultLease, searchjobs.ExecutionSnapshot, error) {
	panic("typed-nil retained source must not be called")
}

func TestNewServiceRejectsTypedNilDependencies(t *testing.T) {
	fixture := newPreviewFixture(t)
	var nilSearches *typedNilSearches
	var nilCompiler *typedNilCompiler
	var nilExecutor *typedNilExecutor
	for _, test := range []struct {
		name   string
		config Config
	}{
		{
			name: "retained source",
			config: Config{
				Searches: nilSearches, Writer: fixture.writer,
				Compiler: compilerFunc(nil), Executor: executorFunc(nil),
			},
		},
		{
			name: "compiler",
			config: Config{
				Searches: fixture.manager, Writer: fixture.writer,
				Compiler: nilCompiler, Executor: executorFunc(nil),
			},
		},
		{
			name: "executor",
			config: Config{
				Searches: fixture.manager, Writer: fixture.writer,
				Compiler: compilerFunc(nil), Executor: nilExecutor,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, err := NewService(test.config)
			if err == nil || service != nil {
				t.Fatalf("NewService(typed-nil %s) = (%#v, %v), want nil, error", test.name, service, err)
			}
		})
	}
}

func TestPreviewInvalidCandidateIsValidationOnlyAndReleasesExactLease(t *testing.T) {
	fixture := newPreviewFixture(t)
	var compilerCalls, executorCalls atomic.Int64
	service, err := NewService(Config{
		Searches: fixture.manager,
		Writer:   fixture.writer,
		Compiler: neverCompiler(&compilerCalls),
		Executor: neverExecutor(&executorCalls),
	})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := service.Preview(
		context.Background(), fixture.access, fixture.scope, invalidPreviewRequest(),
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := sealed.Proto(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if response.GetValidation().GetValid() || response.GetBeforeSchema() != nil ||
		response.GetAfterSchema() != nil || len(response.GetBeforeRows()) != 0 ||
		len(response.GetAfterRows()) != 0 || response.GetTruncated() {
		t.Fatalf("invalid response retained execution output: %#v", response)
	}
	if compilerCalls.Load() != 0 || executorCalls.Load() != 0 {
		t.Fatalf("invalid candidate calls = compiler %d executor %d", compilerCalls.Load(), executorCalls.Load())
	}
	// MaxResultLeases is one. A second acquisition proves Preview closed its
	// exact pin before returning the detached validation-only result.
	lease, _, err := fixture.manager.AcquireExecutionFor(
		context.Background(), fixture.access, previewTestJob,
	)
	if err != nil {
		t.Fatalf("AcquireExecutionFor(after Preview): %v", err)
	}
	_ = lease.Close()
}

func TestPreviewUnauthorizedAndMismatchedJobsFailBeforeValidation(t *testing.T) {
	fixture := newPreviewFixture(t)
	var compilerCalls, executorCalls atomic.Int64
	baseConfig := Config{
		Searches: fixture.manager, Writer: fixture.writer,
		Compiler: neverCompiler(&compilerCalls), Executor: neverExecutor(&executorCalls),
	}
	service, err := NewService(baseConfig)
	if err != nil {
		t.Fatal(err)
	}
	wrongAccess := fixture.access
	wrongAccess.OwnerID = "different-owner"
	if _, err := service.Preview(
		context.Background(), wrongAccess, fixture.scope, invalidPreviewRequest(),
	); !errors.Is(err, ErrNotFoundOrForbidden) {
		t.Fatalf("unauthorized Preview error = %v", err)
	}

	baseConfig.Searches = mismatchedExecutionSource{manager: fixture.manager}
	service, err = NewService(baseConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Preview(
		context.Background(), fixture.access, fixture.scope, invalidPreviewRequest(),
	); !errors.Is(err, ErrNotFoundOrForbidden) {
		t.Fatalf("mismatched Preview error = %v", err)
	}
	if compilerCalls.Load() != 0 || executorCalls.Load() != 0 {
		t.Fatal("unauthorized or mismatched job reached execution")
	}
}

type mismatchedExecutionSource struct{ manager *searchjobs.Manager }

func (source mismatchedExecutionSource) AcquireExecutionFor(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (searchjobs.ResultLease, searchjobs.ExecutionSnapshot, error) {
	lease, execution, err := source.manager.AcquireExecutionFor(ctx, access, id)
	if err == nil {
		execution.ID = "different-retained-job"
	}
	return lease, execution, err
}

type countingExecutionSource struct{ calls atomic.Int64 }

func (source *countingExecutionSource) AcquireExecutionFor(
	context.Context,
	searchjobs.AccessScope,
	string,
) (searchjobs.ResultLease, searchjobs.ExecutionSnapshot, error) {
	source.calls.Add(1)
	return nil, searchjobs.ExecutionSnapshot{}, errors.New("unexpected retained acquisition")
}

func TestPreviewInvalidRowBoundsFailBeforeRetainedTraffic(t *testing.T) {
	fixture := newPreviewFixture(t)
	searches := &countingExecutionSource{}
	var compilerCalls, executorCalls atomic.Int64
	service, err := NewService(Config{
		Searches: searches,
		Writer:   fixture.writer,
		Compiler: neverCompiler(&compilerCalls),
		Executor: neverExecutor(&executorCalls),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, maximumRows := range []uint32{0, MaximumRows + 1} {
		request := invalidPreviewRequest()
		request.MaximumRows = new(maximumRows)
		if _, err := service.Preview(
			context.Background(), fixture.access, fixture.scope, request,
		); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("Preview(maximum_rows=%d) error = %v, want ErrInvalidRequest", maximumRows, err)
		}
	}
	if searches.calls.Load() != 0 || compilerCalls.Load() != 0 || executorCalls.Load() != 0 {
		t.Fatalf("invalid row bounds traffic = searches %d compiler %d executor %d",
			searches.calls.Load(), compilerCalls.Load(), executorCalls.Load())
	}
}

func TestPreviewRejectsCurrentCatalogDependencyAbsentFromRetainedSnapshot(t *testing.T) {
	fixture := newPreviewFixture(t)
	actorContext, err := audit.WithActor(context.Background(), audit.Actor{
		Kind: audit.ActorKindBrowser, ID: "preview-administrator", Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := fixture.writer.Create(
		actorContext,
		fixture.scope.Write,
		&opensplunkv1.CreateKnowledgeObjectRequest{
			Definition: &opensplunkv1.KnowledgeObjectDefinition{
				AppId: previewTestApp, Name: "late-extraction-target",
				SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				Selector: &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
					Value: "main",
				}}},
				Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
					FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
						InputField:        "_raw",
						OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
						Extraction: &opensplunkv1.FieldExtractionDefinition_Json{
							Json: &opensplunkv1.JsonFieldExtractionDefinition{
								Path: "payload.value", OutputField: "late_generated_field",
							},
						},
					},
				},
			},
			InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			ClientRequestId: "preview-create-target-0001",
		},
	)
	if err != nil || created.GetKnowledgeObject() == nil {
		t.Fatalf("Create(late target) = (%#v, %v)", created, err)
	}
	request := validAliasPreviewRequest()
	request.Definition.Name = "depends-on-late-target"
	request.Definition.Selector = &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
		Value: "main",
	}}}
	request.Definition.GetFieldAlias().SourceField = "late_generated_field"
	var compilerCalls, executorCalls atomic.Int64
	service, err := NewService(Config{
		Searches: fixture.manager, Writer: fixture.writer,
		Compiler: neverCompiler(&compilerCalls), Executor: neverExecutor(&executorCalls),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Preview(
		context.Background(), fixture.access, fixture.scope, request,
	); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Preview(catalog drift) error = %v, want ErrUnavailable", err)
	}
	if compilerCalls.Load() != 0 || executorCalls.Load() != 0 {
		t.Fatal("current-catalog-only dependency reached compiler or executor")
	}
}

func TestSealResponseRejectsUnknownSchemaAndEnforcesExactResponseCap(t *testing.T) {
	fixture := newPreviewFixture(t)
	sealedValidation, err := fixture.writer.Validate(
		context.Background(),
		fixture.scope,
		activeValidationView(validAliasPreviewRequest()),
	)
	if err != nil {
		t.Fatal(err)
	}
	schema := &opensplunkv1.ResultSchema{
		SchemaId: previewTestJob, Revision: 1,
		ResultKind: opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS,
		Columns: []*opensplunkv1.ResultColumn{{
			FieldName: "status", DisplayName: "status",
			ValueType: opensplunkv1.ValueType_VALUE_TYPE_STRING,
		}},
	}
	unknown := cloneSchema(schema)
	unknown.ProtoReflect().SetUnknown(protowire.AppendVarint(
		protowire.AppendTag(nil, 100, protowire.VarintType), 1,
	))
	if _, err := SealResponse(context.Background(), ResponseInput{
		Validation:   sealedValidation,
		BeforeSchema: unknown,
		AfterSchema:  schema,
		JobID:        previewTestJob,
		MaximumRows:  1,
	}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("SealResponse(unknown schema) error = %v", err)
	}

	validation, err := sealedValidation.Proto(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	responseSize := func(valueBytes int) int {
		candidate := &opensplunkv1.PreviewKnowledgeObjectResponse{
			Validation:   validation.GetResult(),
			BeforeSchema: schema,
			AfterSchema:  schema,
			BeforeRows: []*opensplunkv1.ResultRow{{
				RowId: previewTestJob + ":0",
				Cells: []*opensplunkv1.TypedValue{{
					Kind: &opensplunkv1.TypedValue_StringValue{
						StringValue: strings.Repeat("x", valueBytes),
					},
				}},
			}},
			TenantCatalogRevision: validation.GetTenantCatalogRevision(),
		}
		wire, marshalErr := (proto.MarshalOptions{Deterministic: true}).Marshal(candidate)
		if marshalErr != nil {
			t.Fatalf("marshal cap candidate: %v", marshalErr)
		}
		return len(wire)
	}
	low, high := 0, MaximumResponseBytes
	for low < high {
		middle := low + (high-low)/2
		if responseSize(middle) < MaximumResponseBytes {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if got := responseSize(low); got != MaximumResponseBytes {
		t.Fatalf("could not construct exact response cap: size(%d) = %d", low, got)
	}
	exactRow := func(valueBytes int) *opensplunkv1.ResultRow {
		return &opensplunkv1.ResultRow{
			RowId: previewTestJob + ":0",
			Cells: []*opensplunkv1.TypedValue{{
				Kind: &opensplunkv1.TypedValue_StringValue{
					StringValue: strings.Repeat("x", valueBytes),
				},
			}},
		}
	}
	exact, err := SealResponse(context.Background(), ResponseInput{
		Validation: sealedValidation, BeforeSchema: schema, AfterSchema: schema,
		BeforeRows: []*opensplunkv1.ResultRow{exactRow(low)},
		JobID:      previewTestJob, MaximumRows: 1,
	})
	if err != nil {
		t.Fatalf("SealResponse(exact cap): %v", err)
	}
	if got := len(exact.DeterministicBytes()); got != MaximumResponseBytes {
		t.Fatalf("SealResponse(exact cap) bytes = %d, want %d", got, MaximumResponseBytes)
	}
	if got := responseSize(low + 1); got != MaximumResponseBytes+1 {
		t.Fatalf("one-over candidate size = %d, want %d", got, MaximumResponseBytes+1)
	}
	if _, err := SealResponse(context.Background(), ResponseInput{
		Validation: sealedValidation, BeforeSchema: schema, AfterSchema: schema,
		BeforeRows: []*opensplunkv1.ResultRow{exactRow(low + 1)},
		JobID:      previewTestJob, MaximumRows: 1,
	}); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("SealResponse(one over cap) error = %v, want ErrResponseTooLarge", err)
	}
}

func TestProjectionSinkPreflightRejectsOversizedValuesBeforeConversion(t *testing.T) {
	deepEmpty := searchjobs.ListValue()
	for range 32 {
		deepEmpty = searchjobs.ListValue(deepEmpty)
	}
	tests := []struct {
		name      string
		value     searchjobs.Value
		remaining int
	}{
		{name: "string", value: searchjobs.StringValue(strings.Repeat("s", 64)), remaining: 64},
		{name: "bytes", value: searchjobs.BytesValue(make([]byte, 64)), remaining: 64},
		{name: "deep empty containers", value: deepEmpty, remaining: 129},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var conversionCalls atomic.Int64
			sink := &projectionSink{
				ctx:          context.Background(),
				jobID:        previewTestJob,
				shape:        searchjobproto.ResultShapeForSPL("| stats count"),
				maximumRows:  1,
				maximumBytes: MaximumResponseBytes,
				convertRows: func(
					context.Context,
					string,
					searchjobs.Schema,
					[]searchjobs.ResultRow,
					int,
				) ([]*opensplunkv1.ResultRow, error) {
					conversionCalls.Add(1)
					return nil, errors.New("conversion must not run")
				},
			}
			schema := searchjobs.Schema{Columns: []searchjobs.Column{{
				Name: "value", Kind: searchjobs.ValueKindMixed,
			}}}
			if err := sink.SetSchema(schema); err != nil {
				t.Fatalf("SetSchema: %v", err)
			}
			sink.maximumBytes = sink.bytes + test.remaining
			if err := sink.AddRow([]searchjobs.Value{test.value}); !errors.Is(err, ErrResponseTooLarge) {
				t.Fatalf("AddRow error = %v, want ErrResponseTooLarge", err)
			}
			if calls := conversionCalls.Load(); calls != 0 {
				t.Fatalf("row conversion calls = %d, want 0", calls)
			}
		})
	}
}

func TestPreviewCancellationBeforeAcquisitionIsAtomic(t *testing.T) {
	fixture := newPreviewFixture(t)
	var compilerCalls, executorCalls atomic.Int64
	service, err := NewService(Config{
		Searches: fixture.manager, Writer: fixture.writer,
		Compiler: neverCompiler(&compilerCalls), Executor: neverExecutor(&executorCalls),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Preview(ctx, fixture.access, fixture.scope, invalidPreviewRequest()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Preview(canceled) error = %v", err)
	}
	if compilerCalls.Load() != 0 || executorCalls.Load() != 0 {
		t.Fatal("canceled Preview reached compiler or executor")
	}
}
