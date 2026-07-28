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
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
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
	GeneratedSQL      string
	ExplainText       string
	DiagnosticQueryID string
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

type queryExplainer interface {
	Explain(
		context.Context,
		clickhouse.CompiledQuery,
	) (queryexec.ExplainResult, error)
}

// Config controls inspection admission and execution. Zero bounds select the
// fixed conservative defaults. Dependencies are borrowed: callers must stop
// this service before closing a shared Manager or Explainer.
type Config struct {
	Searches  completedSearches
	Compiler  queryCompiler
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
	closeOnce     sync.Once
	closeDone     chan struct{}
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
	if nilInterface(config.Searches) {
		return nil, errors.New(
			"create search inspection service: completed search snapshots are required",
		)
	}
	if nilInterface(config.Compiler) {
		return nil, errors.New(
			"create search inspection service: query compiler is required",
		)
	}
	if nilInterface(config.Explainer) {
		return nil, errors.New(
			"create search inspection service: query Explainer is required",
		)
	}
	if config.MaxConcurrent < 0 ||
		config.MaxConcurrent > maximumConcurrentInspections {
		return nil, fmt.Errorf(
			"create search inspection service: concurrent limit must not exceed %d",
			maximumConcurrentInspections,
		)
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = defaultConcurrentInspections
	}
	if config.MaxRuntime < 0 ||
		config.MaxRuntime > maximumInspectionRuntime {
		return nil, fmt.Errorf(
			"create search inspection service: runtime must not exceed %s",
			maximumInspectionRuntime,
		)
	}
	if config.MaxRuntime == 0 {
		config.MaxRuntime = defaultInspectionRuntime
	}

	return &Service{
		searches:      config.Searches,
		compiler:      config.Compiler,
		explainer:     config.Explainer,
		maxRuntime:    config.MaxRuntime,
		gate:          make(chan struct{}, config.MaxConcurrent),
		activeCancels: make(map[*operationToken]context.CancelFunc),
		closeDone:     make(chan struct{}),
	}, nil
}

// Inspect rebuilds one exact completed execution snapshot, produces a safe
// detached logical projection, compiles it once, and sends that exact sealed
// compiler result unchanged to the bounded Explainer. A second authoritative
// metadata lookup prevents expiry or tombstone cleanup during EXPLAIN from
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

	compiled, err := service.compiler.Compile(logical)
	if err != nil {
		return Result{}, classifyPlanningError(err)
	}
	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}
	if !validGeneratedSQL(compiled) {
		return Result{}, ErrInspectionFailed
	}

	explained, err := service.explainer.Explain(
		operationContext,
		compiled,
	)
	if err != nil {
		return Result{}, err
	}
	if err := operationContext.Err(); err != nil {
		return Result{}, err
	}
	if err := queryexec.ValidateExplainResult(explained); err != nil {
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
		GeneratedSQL:      strings.Clone(compiled.SQL),
		ExplainText:       strings.Clone(explained.Text),
		DiagnosticQueryID: strings.Clone(explained.QueryID),
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
	service.closeOnce.Do(func() {
		go func() {
			service.operations.Wait()
			close(service.closeDone)
		}()
	})
	select {
	case <-service.closeDone:
		return nil
	default:
	}
	select {
	case <-service.closeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
