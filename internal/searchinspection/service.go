// Package searchinspection provides bounded, internal-only inspection of the
// logical and ClickHouse plans for completed immutable searches.
//
// Generated SQL and EXPLAIN text are administrator-sensitive and may contain
// physical schema detail or rendered bind values. This package deliberately
// has no protobuf, HTTP, runtime-advertisement, or ordinary search-job seam.
package searchinspection

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
	"github.com/Suhaibinator/open-splunk/internal/shutdownbarrier"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	defaultConcurrentInspections = 2
	maximumConcurrentInspections = 2
	defaultInspectionRuntime     = 10 * time.Second
	maximumInspectionRuntime     = 10 * time.Second

	maximumAccessIdentityBytes   = 1 << 10
	maximumSnapshotIndexBytes    = 1 << 10
	maximumSnapshotTimezoneBytes = 1 << 10
	maximumGeneratedSQLBytes     = 256 << 10
)

var (
	// ErrInvalidRequest means the access scope or search-job identifier is
	// malformed or exceeds the inspection admission bounds.
	ErrInvalidRequest = errors.New("invalid search inspection request")
	// ErrInspectionFailed is the stable, detail-free category for an invalid
	// dependency contract or an otherwise unclassifiable inspection failure.
	ErrInspectionFailed = errors.New("search inspection failed")
)

// Request selects one exact completed search. It contains no option that can
// weaken the retained search's immutable execution or authorization scope.
type Request struct {
	SearchJobID string
}

// Result is published atomically after both the complete projection and
// ClickHouse plan validate. It intentionally contains neither SPL, compiler
// arguments, execution snapshots, result rows, nor mutable plan pointers.
type Result struct {
	Plan              LogicalPlan
	PhysicalPlan      queryexec.ExplainPlan
	GeneratedSQL      string
	ExplainText       string
	DiagnosticQueryID string
	// KnowledgeSnapshot is the bounded, definition-free inventory for the
	// exact retained execution authority. It is absent on the legacy path.
	// Browser projections must apply current-policy redaction before release.
	KnowledgeSnapshot *opensplunk.KnowledgeSnapshotSummary
}

type completedSearches interface {
	CompletedExecutionSnapshotFor(
		context.Context,
		searchjobs.AccessScope,
		string,
	) (searchjobs.ExecutionSnapshot, error)
}

type queryCompiler interface {
	Compile(*plan.Query) (clickhouse.CompiledQuery, error)
}

type retainedLookupAuthorityCompiler interface {
	WithRetainedLookupAuthorityContext(
		context.Context,
		clickhouse.CompiledQuery,
		*plan.Query,
	) (*plan.Query, clickhouse.Compiler, error)
}

type queryExplainer interface {
	Explain(
		context.Context,
		clickhouse.CompiledQuery,
	) (queryexec.ExplainResult, error)
}

// Config controls inspection admission and execution. Zero bounds select the
// fixed conservative defaults. Searches and Explainer are borrowed: callers
// must stop this service before closing either shared dependency.
type Config struct {
	Searches completedSearches
	// Compiler is concrete so callers cannot substitute an implementation
	// that returns a validly sealed query for a different same-scope plan.
	Compiler  clickhouse.Compiler
	Explainer queryExplainer

	MaxConcurrent int
	MaxRuntime    time.Duration
}

// Service owns fail-fast admission and lifecycle cancellation. It retains no
// job, plan, compiler query, argument, SQL, or EXPLAIN result state.
type Service struct {
	searches  completedSearches
	compiler  queryCompiler
	explainer queryExplainer

	maxRuntime time.Duration
	gate       chan struct{}

	mu            sync.Mutex
	closed        bool
	activeCancels map[*operationToken]context.CancelFunc
	operations    sync.WaitGroup
	barrier       *shutdownbarrier.Barrier
}

type normalizedRequest struct {
	access searchjobs.AccessScope
	jobID  string
}

// operationToken has non-zero size so distinct admitted operations always
// have distinct map keys.
type operationToken struct {
	_ byte
}

// New validates every dependency and bound before constructing an idle
// internal inspection service.
func New(config Config) (*Service, error) {
	return newService(
		config.Searches,
		config.Compiler,
		config.Explainer,
		config.MaxConcurrent,
		config.MaxRuntime,
	)
}

// newService is the package-internal dependency seam used to exercise
// adversarial compiler behavior. Shipping callers enter through New, whose
// concrete clickhouse.Compiler field prevents compiler substitution.
func newService(
	searches completedSearches,
	compiler queryCompiler,
	explainer queryExplainer,
	maxConcurrent int,
	maxRuntime time.Duration,
) (*Service, error) {
	if nilcheck.IsNil(searches) {
		return nil, errors.New(
			"create search inspection service: completed search snapshots are required",
		)
	}
	if nilcheck.IsNil(compiler) {
		return nil, errors.New(
			"create search inspection service: query compiler is required",
		)
	}
	if nilcheck.IsNil(explainer) {
		return nil, errors.New(
			"create search inspection service: query Explainer is required",
		)
	}
	if maxConcurrent < 0 ||
		maxConcurrent > maximumConcurrentInspections {
		return nil, fmt.Errorf(
			"create search inspection service: concurrent limit must not exceed %d",
			maximumConcurrentInspections,
		)
	}
	if maxConcurrent == 0 {
		maxConcurrent = defaultConcurrentInspections
	}
	if maxRuntime < 0 ||
		maxRuntime > maximumInspectionRuntime {
		return nil, fmt.Errorf(
			"create search inspection service: runtime must not exceed %s",
			maximumInspectionRuntime,
		)
	}
	if maxRuntime == 0 {
		maxRuntime = defaultInspectionRuntime
	}

	return &Service{
		searches:      searches,
		compiler:      compiler,
		explainer:     explainer,
		maxRuntime:    maxRuntime,
		gate:          make(chan struct{}, maxConcurrent),
		activeCancels: make(map[*operationToken]context.CancelFunc),
		barrier:       shutdownbarrier.New(),
	}, nil
}

// Inspect reads one exact completed execution snapshot and produces a safe
// detached logical projection. Legacy snapshots preserve the original
// rebuild-and-compile path. Knowledge-enabled snapshots instead send a clone
// of the exact compiler-sealed retained execution to the bounded Explainer;
// they are never recompiled. A second authoritative metadata lookup prevents
// expiry, tombstone cleanup, or authority replacement during EXPLAIN from
// publishing stale diagnostics.
func (service *Service) Inspect(
	ctx context.Context,
	access searchjobs.AccessScope,
	request Request,
) (result Result, resultErr error) {
	if service == nil {
		return Result{}, errors.New(
			"inspect completed search: service is nil",
		)
	}
	if ctx == nil {
		return Result{}, errors.New(
			"inspect completed search: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	normalized, err := normalizeRequest(access, request)
	if err != nil {
		return Result{}, err
	}
	token, operationContext, cancelOperation, err :=
		service.beginOperation(ctx)
	if err != nil {
		return Result{}, err
	}
	normalized = detachNormalizedRequest(normalized)

	defer func() {
		closed := service.unregisterOperation(token)
		resultErr = finalInspectionError(
			ctx,
			operationContext,
			closed,
			resultErr,
		)
		if resultErr != nil {
			result = Result{}
		}
		cancelOperation()
		service.releaseOperation()
	}()

	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}
	snapshot, err := service.completedSnapshotFor(
		operationContext,
		normalized.access,
		normalized.jobID,
	)
	if err != nil {
		return Result{}, err
	}
	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}
	retainedKnowledge, err := snapshot.OpenRetainedKnowledgeExecution()
	if err != nil {
		return Result{}, ErrInspectionFailed
	}
	if !validSnapshotContract(snapshot, normalized) {
		return Result{}, ErrInspectionFailed
	}

	logical, err := searchsnapshot.BuildExecutionPlan(snapshot)
	if err != nil {
		return Result{}, classifyPlanningError(err)
	}
	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}
	if retainedKnowledge != nil {
		hasLookups, lookupErr := retainedKnowledge.CompiledQuery.
			HasLookupAuthorityContext(operationContext)
		if lookupErr != nil {
			if contextErr := operationContext.Err(); contextErr != nil {
				return Result{}, contextErr
			}
			return Result{}, ErrInspectionFailed
		}
		if hasLookups {
			restorer, ok := service.compiler.(retainedLookupAuthorityCompiler)
			if !ok {
				return Result{}, ErrInspectionFailed
			}
			logical, _, err = restorer.WithRetainedLookupAuthorityContext(
				operationContext,
				retainedKnowledge.CompiledQuery,
				logical,
			)
			if err != nil {
				if contextErr := operationContext.Err(); contextErr != nil {
					return Result{}, contextErr
				}
				return Result{}, ErrInspectionFailed
			}
		}
	}
	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}
	projected, err := projectLogicalPlan(
		operationContext,
		logical,
		snapshot.SPL,
	)
	if err != nil {
		if contextErr := operationContext.Err(); contextErr != nil {
			return Result{}, contextErr
		}
		return Result{}, ErrInspectionFailed
	}
	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}

	var knowledgeSummary *opensplunk.KnowledgeSnapshotSummary
	var compiled clickhouse.CompiledQuery
	if retainedKnowledge == nil {
		compiled, err = service.compiler.Compile(logical)
		if err != nil {
			return Result{}, classifyPlanningError(err)
		}
	} else {
		compiled = retainedKnowledge.CompiledQuery
		knowledgeSummary = retainedKnowledge.KnowledgeSummary
	}
	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}
	if !validGeneratedSQL(compiled) {
		return Result{}, ErrInspectionFailed
	}

	retainedAuthority, ok := compiled.CloneForExecution()
	if !ok {
		return Result{}, ErrInspectionFailed
	}
	compiledTenant, compiledIndexes, ok := retainedAuthority.ReadScope()
	if !ok || compiledTenant != snapshot.TenantID ||
		!slices.Equal(compiledIndexes, snapshot.EffectiveIndexes) {
		return Result{}, ErrInspectionFailed
	}
	explainAuthority, ok := retainedAuthority.CloneForExecution()
	if !ok {
		return Result{}, ErrInspectionFailed
	}
	explained, err := service.explainer.Explain(
		operationContext,
		explainAuthority,
	)
	if err != nil {
		return Result{}, err
	}
	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}
	// Explain receives CompiledQuery by value, but its slices are reference
	// values. Recheck the full private execution seal before using any output
	// so an adversarial dependency cannot mutate a retained clone in place.
	if !explainAuthority.EqualForExecution(retainedAuthority) {
		return Result{}, ErrInspectionFailed
	}
	compiled = retainedAuthority
	physical, err := queryexec.ParseExplainPlan(explained)
	if err != nil {
		return Result{}, ErrInspectionFailed
	}

	postflight, err := service.completedSnapshotFor(
		operationContext,
		normalized.access,
		normalized.jobID,
	)
	if err != nil {
		return Result{}, err
	}
	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}
	if !snapshot.Equal(postflight) {
		return Result{}, ErrInspectionFailed
	}

	result = Result{
		Plan:              projected,
		PhysicalPlan:      physical,
		GeneratedSQL:      strings.Clone(compiled.SQL),
		ExplainText:       strings.Clone(explained.Text),
		DiagnosticQueryID: strings.Clone(explained.QueryID),
		KnowledgeSnapshot: knowledgeSummary,
	}
	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (service *Service) completedSnapshotFor(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
) (searchjobs.ExecutionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.ExecutionSnapshot{}, err
	}
	return service.searches.CompletedExecutionSnapshotFor(
		ctx,
		access,
		jobID,
	)
}

func normalizeRequest(
	access searchjobs.AccessScope,
	request Request,
) (normalizedRequest, error) {
	if !validSearchJobID(request.SearchJobID) ||
		!validAccessIdentity(access.TenantID) ||
		!validAccessIdentity(access.OwnerID) {
		return normalizedRequest{}, ErrInvalidRequest
	}
	return normalizedRequest{
		access: searchjobs.AccessScope{
			TenantID: access.TenantID,
			OwnerID:  access.OwnerID,
		},
		jobID: request.SearchJobID,
	}, nil
}

func detachNormalizedRequest(request normalizedRequest) normalizedRequest {
	return normalizedRequest{
		access: searchjobs.AccessScope{
			TenantID: strings.Clone(request.access.TenantID),
			OwnerID:  strings.Clone(request.access.OwnerID),
		},
		jobID: strings.Clone(request.jobID),
	}
}

func validSearchJobID(value string) bool {
	return validBoundedIdentifier(value, searchjobs.MaximumJobIDBytes)
}

func validAccessIdentity(value string) bool {
	return validBoundedIdentifier(value, maximumAccessIdentityBytes)
}

func validSnapshotIndex(value string) bool {
	return validBoundedIdentifier(value, maximumSnapshotIndexBytes)
}

func validSnapshotTimezone(value string) bool {
	return validBoundedIdentifier(value, maximumSnapshotTimezoneBytes)
}

func validBoundedIdentifier(value string, maximum int) bool {
	if value == "" ||
		len(value) > maximum ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validSnapshotContract(
	snapshot searchjobs.ExecutionSnapshot,
	normalized normalizedRequest,
) bool {
	if snapshot.ID != normalized.jobID ||
		snapshot.TenantID != normalized.access.TenantID ||
		snapshot.OwnerID != normalized.access.OwnerID ||
		len(snapshot.SPL) == 0 ||
		len(snapshot.SPL) > maximumProjectionSourceBytes ||
		!utf8.ValidString(snapshot.SPL) ||
		strings.TrimSpace(snapshot.SPL) == "" ||
		len(snapshot.EffectiveIndexes) == 0 ||
		len(snapshot.EffectiveIndexes) > searchjobs.MaximumScopeIndexes ||
		!validSnapshotTimezone(snapshot.SearchTimezone) ||
		snapshot.FinishedAt.IsZero() ||
		snapshot.ExpiresAt.IsZero() ||
		!snapshot.FinishedAt.Before(snapshot.ExpiresAt) {
		return false
	}
	for _, index := range snapshot.EffectiveIndexes {
		if !validSnapshotIndex(index) {
			return false
		}
	}
	return true
}

func validGeneratedSQL(compiled clickhouse.CompiledQuery) bool {
	if !compiled.HasValidSQLSeal() ||
		len(compiled.SQL) == 0 ||
		len(compiled.SQL) > maximumGeneratedSQLBytes ||
		!utf8.ValidString(compiled.SQL) ||
		strings.TrimSpace(compiled.SQL) == "" {
		return false
	}
	for _, character := range compiled.SQL {
		if unicode.IsControl(character) &&
			character != '\t' &&
			character != '\n' &&
			character != '\r' {
			return false
		}
	}
	return true
}

func classifyPlanningError(err error) error {
	if err == nil {
		return nil
	}
	var parserDiagnostic *spl.Diagnostic
	if errors.As(err, &parserDiagnostic) &&
		parserDiagnostic.Code == "SPL_QUERY_TOO_COMPLEX" {
		return searchjobs.ErrExecutionLimit
	}
	var planDiagnostic *plan.Diagnostic
	if errors.As(err, &planDiagnostic) &&
		planDiagnostic.Code == "SPL_QUERY_TOO_COMPLEX" {
		return searchjobs.ErrExecutionLimit
	}
	return err
}

func safeInspectionError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrInvalidRequest):
		return ErrInvalidRequest
	case errors.Is(err, ErrInspectionFailed):
		return ErrInspectionFailed
	case errors.Is(err, searchjobs.ErrNotFound):
		return searchjobs.ErrNotFound
	case errors.Is(err, searchjobs.ErrExpired):
		return searchjobs.ErrExpired
	case errors.Is(err, searchjobs.ErrResultsNotReady):
		return searchjobs.ErrResultsNotReady
	case errors.Is(err, searchjobs.ErrResultsUnavailable):
		return searchjobs.ErrResultsUnavailable
	case errors.Is(err, searchjobs.ErrCapacity):
		return searchjobs.ErrCapacity
	case errors.Is(err, searchjobs.ErrClosed):
		return searchjobs.ErrClosed
	case errors.Is(err, searchjobs.ErrExecutionLimit):
		return searchjobs.ErrExecutionLimit
	case errors.Is(err, searchjobs.ErrStorageUnavailable):
		return searchjobs.ErrStorageUnavailable
	case errors.Is(err, searchjobs.ErrUnsupportedValue):
		return searchjobs.ErrUnsupportedValue
	case errors.Is(err, searchjobs.ErrInvalidResult):
		return searchjobs.ErrInvalidResult
	default:
		return ErrInspectionFailed
	}
}

func finalInspectionError(
	callerContext context.Context,
	operationContext context.Context,
	closed bool,
	resultErr error,
) error {
	switch {
	case callerContext.Err() != nil:
		return callerContext.Err()
	case closed:
		return searchjobs.ErrClosed
	case operationContext.Err() != nil:
		return operationContext.Err()
	case resultErr != nil:
		return safeInspectionError(resultErr)
	default:
		return nil
	}
}

func (service *Service) beginOperation(
	ctx context.Context,
) (
	*operationToken,
	context.Context,
	context.CancelFunc,
	error,
) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed {
		return nil, nil, nil, searchjobs.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	select {
	case service.gate <- struct{}{}:
		operationContext, cancelOperation := context.WithTimeout(
			ctx,
			service.maxRuntime,
		)
		token := &operationToken{}
		service.activeCancels[token] = cancelOperation
		service.operations.Add(1)
		return token, operationContext, cancelOperation, nil
	default:
		return nil, nil, nil, searchjobs.ErrCapacity
	}
}

func (service *Service) unregisterOperation(
	token *operationToken,
) bool {
	service.mu.Lock()
	closed := service.closed
	delete(service.activeCancels, token)
	service.mu.Unlock()
	return closed
}

func (service *Service) releaseOperation() {
	<-service.gate
	service.operations.Done()
}

// Close stops admission, cancels every admitted inspection, and waits for all
// borrowed dependency calls to unwind. It does not close the shared search
// manager, compiler, or Explainer. Repeated callers share completion and may
// retry after their own close context expires.
func (service *Service) Close(ctx context.Context) error {
	if service == nil {
		return nil
	}
	if ctx == nil {
		return errors.New(
			"close search inspection service: context is nil",
		)
	}
	service.mu.Lock()
	if !service.closed {
		service.closed = true
		for _, cancelOperation := range service.activeCancels {
			cancelOperation()
		}
	}
	service.mu.Unlock()
	return service.barrier.Wait(ctx, func() { service.operations.Wait() })
}
