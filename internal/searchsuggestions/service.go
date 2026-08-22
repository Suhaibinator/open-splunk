// Package searchsuggestions provides bounded, synchronous SPL editor
// completions without creating or retaining search jobs.
package searchsuggestions

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/shutdownbarrier"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	defaultConcurrent = 8
	maximumConcurrent = 128
	defaultRuntime    = 10 * time.Second
	maximumRuntime    = time.Minute
	maximumIdentity   = 1 << 10
	maximumIndexes    = 256

	maximumDiagnostics         = 64
	maximumDiagnosticCodeBytes = 128
	maximumDiagnosticMessage   = 4 << 10
	maximumDiagnosticHints     = 64
	maximumDiagnosticHintBytes = 1 << 10
)

var (
	// ErrInvalidRequest means structural source, cursor, scope, or result-bound
	// metadata was invalid. SPL compatibility diagnostics are returned in-band
	// instead and do not use this error.
	ErrInvalidRequest = errors.New("invalid search suggestion request")
)

// Validator is the no-job SPL validation capability required by Service.
type Validator interface {
	Validate(context.Context, searchjobs.ValidateRequest) (searchjobs.ValidationResult, error)
}

// ScopeSnapshotter captures one no-job storage visibility boundary.
type ScopeSnapshotter interface {
	SnapshotAnalysisScope(
		context.Context,
		searchjobs.AnalysisScopeRequest,
	) (searchjobs.AnalysisScopeSnapshot, error)
}

// FieldCompiler compiles one prefix-filtered, name-only storage lookup.
type FieldCompiler interface {
	CompileFieldSuggestionsContext(
		context.Context,
		*plan.Query,
		clickhouse.FieldSuggestionSpec,
	) (clickhouse.CompiledFieldSuggestions, error)
}

// FieldExecutor executes one compiler-produced field-name lookup.
type FieldExecutor interface {
	ExecuteFieldSuggestions(
		context.Context,
		clickhouse.CompiledFieldSuggestions,
	) (queryexec.FieldSuggestionResult, error)
}

// Config defines dependencies and strict synchronous resource bounds. Zero
// limits select conservative defaults.
type Config struct {
	Validator Validator
	Scopes    ScopeSnapshotter
	Compiler  FieldCompiler
	Executor  FieldExecutor

	MaxConcurrent int
	MaxRuntime    time.Duration
}

// Request is trusted, server-resolved editor state. CursorByteOffset is a
// UTF-8 byte offset, not a browser UTF-16 code-unit offset. A nil maximum uses
// the default; an explicitly supplied zero is invalid.
type Request struct {
	SPL                       string
	CursorByteOffset          int
	TenantID                  string
	AuthorizedIndexes         []string
	RequestedIndexes          []string
	TimeRange                 searchtime.Range
	AuthorizedIndexCandidates []string
	MaxSuggestions            *uint32
}

// Result combines safe ranked completions with full-source SPL diagnostics.
// Context is included for internal adapters that need exact replacement
// semantics; every returned slice is detached from dependencies and inputs.
type Result struct {
	Context     spl.SuggestionContext
	Suggestions []spl.Suggestion
	Diagnostics []searchjobs.Diagnostic
}

// Service owns synchronous admission and cancellation. It retains no query,
// result, diagnostic, job, history, journal, or generated SQL state.
type Service struct {
	validator Validator
	scopes    ScopeSnapshotter
	compiler  FieldCompiler
	executor  FieldExecutor

	maxRuntime time.Duration
	gate       chan struct{}

	lifecycleContext context.Context
	lifecycleCancel  context.CancelFunc
	mu               sync.Mutex
	closed           bool
	operations       sync.WaitGroup
	barrier          *shutdownbarrier.Barrier
}

type normalizedRequest struct {
	source                    string
	tenantID                  string
	authorizedIndexes         []string
	requestedIndexes          []string
	timeRange                 searchtime.Range
	authorizedIndexCandidates []string
	maximum                   int
	analysisBlocked           bool
}

// New validates every dependency and bound before constructing an idle
// suggestion service.
func New(config Config) (*Service, error) {
	if nilcheck.IsNil(config.Validator) {
		return nil, errors.New("create search suggestion service: validator is required")
	}
	if nilcheck.IsNil(config.Scopes) {
		return nil, errors.New("create search suggestion service: scope snapshotter is required")
	}
	if nilcheck.IsNil(config.Compiler) {
		return nil, errors.New("create search suggestion service: field compiler is required")
	}
	if nilcheck.IsNil(config.Executor) {
		return nil, errors.New("create search suggestion service: field executor is required")
	}
	if config.MaxConcurrent < 0 || config.MaxConcurrent > maximumConcurrent {
		return nil, fmt.Errorf(
			"create search suggestion service: concurrent limit must be between 0 and %d",
			maximumConcurrent,
		)
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = defaultConcurrent
	}
	if config.MaxRuntime < 0 || config.MaxRuntime > maximumRuntime {
		return nil, fmt.Errorf(
			"create search suggestion service: runtime must be between 0 and %s",
			maximumRuntime,
		)
	}
	if config.MaxRuntime == 0 {
		config.MaxRuntime = defaultRuntime
	}
	// Close owns lifecycleCancel and invokes it exactly once when admission
	// stops; retaining the function is the service's shutdown mechanism.
	lifecycleContext, lifecycleCancel := context.WithCancel(context.Background())
	return &Service{
		validator:        config.Validator,
		scopes:           config.Scopes,
		compiler:         config.Compiler,
		executor:         config.Executor,
		maxRuntime:       config.MaxRuntime,
		gate:             make(chan struct{}, config.MaxConcurrent),
		lifecycleContext: lifecycleContext,
		lifecycleCancel:  lifecycleCancel,
		barrier:          shutdownbarrier.New(),
	}, nil
}

// MaximumSuggestions returns the fixed cross-layer response bound.
func (service *Service) MaximumSuggestions() uint32 {
	if service == nil {
		return 0
	}
	return uint32(spl.MaximumSuggestionLimit)
}

// Suggest returns bounded editor completions. Static contexts validate the
// full source but never capture storage or invoke the field compiler/executor.
// Field contexts use exactly one immutable analysis snapshot.
func (service *Service) Suggest(
	ctx context.Context,
	request Request,
) (result Result, resultErr error) {
	if service == nil {
		return Result{}, errors.New("suggest search: service is nil")
	}
	if ctx == nil {
		return Result{}, errors.New("suggest search: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	normalized, suggestionContext, err := normalizeRequest(request)
	if err != nil {
		return Result{}, err
	}
	if err := service.beginOperation(); err != nil {
		return Result{}, err
	}
	operationContext, cancelOperation := context.WithTimeout(ctx, service.maxRuntime)
	stopLifecycleCancellation := context.AfterFunc(service.lifecycleContext, cancelOperation)
	defer func() {
		stopLifecycleCancellation()
		cancelOperation()
		if errors.Is(resultErr, context.Canceled) {
			switch {
			case ctx.Err() != nil:
				result = Result{}
				resultErr = ctx.Err()
			case service.lifecycleContext.Err() != nil:
				result = Result{}
				resultErr = searchjobs.ErrClosed
			}
		}
		service.endOperation()
	}()

	validation, err := service.validator.Validate(operationContext, searchjobs.ValidateRequest{
		SPL:               strings.Clone(normalized.source),
		TenantID:          strings.Clone(normalized.tenantID),
		AuthorizedIndexes: slices.Clone(normalized.authorizedIndexes),
		RequestedIndexes:  slices.Clone(normalized.requestedIndexes),
		TimeRange:         normalized.timeRange,
	})
	if err != nil {
		return Result{}, service.operationError(ctx, operationContext, err)
	}
	if err := validateValidationResult(validation, normalized); err != nil {
		return Result{}, service.operationError(ctx, operationContext, err)
	}
	if normalized.analysisBlocked && validation.Valid {
		return Result{}, service.operationError(
			ctx,
			operationContext,
			searchjobs.ErrInvalidResult,
		)
	}
	diagnostics := cloneDiagnostics(validation.Diagnostics)

	candidates := spl.StaticSuggestionCandidates(suggestionContext)
	candidates = append(
		candidates,
		indexSuggestionCandidates(suggestionContext, normalized.authorizedIndexCandidates)...,
	)

	if suggestionContext.Allows(spl.SuggestionKindField) &&
		dynamicFieldLookupAllowed(normalized.source, suggestionContext, diagnostics) {
		dynamic, available, dynamicErr := service.dynamicFieldCandidates(
			operationContext,
			normalized,
			suggestionContext,
		)
		if dynamicErr != nil {
			return Result{}, service.operationError(ctx, operationContext, dynamicErr)
		}
		if available {
			candidates = append(candidates, dynamic...)
		}
	}
	if err := service.operationError(ctx, operationContext, nil); err != nil {
		return Result{}, err
	}

	return Result{
		Context: cloneSuggestionContext(suggestionContext),
		Suggestions: cloneSuggestions(spl.RankSuggestionCandidates(
			suggestionContext,
			candidates,
			normalized.maximum,
		)),
		Diagnostics: diagnostics,
	}, nil
}

func (service *Service) dynamicFieldCandidates(
	ctx context.Context,
	request normalizedRequest,
	suggestionContext spl.SuggestionContext,
) ([]spl.SuggestionCandidate, bool, error) {
	parsed, available, err := parseCompletedPrefix(request.source, suggestionContext)
	if err != nil || !available {
		return nil, available, err
	}
	snapshot, err := service.scopes.SnapshotAnalysisScope(ctx, searchjobs.AnalysisScopeRequest{
		TenantID:          strings.Clone(request.tenantID),
		AuthorizedIndexes: slices.Clone(request.authorizedIndexes),
		RequestedIndexes:  slices.Clone(request.requestedIndexes),
		TimeRange:         request.timeRange,
	})
	if err != nil {
		return nil, false, safeDependencyError(err)
	}
	if !validSnapshot(snapshot, request) {
		return nil, false, searchjobs.ErrInvalidResult
	}
	logical, available, err := buildPrefixPlan(parsed, snapshot)
	if err != nil || !available {
		return nil, available, err
	}
	if err := plan.ValidateFieldAnalysisEligibility(logical); err != nil {
		return nil, false, nil
	}

	spec := clickhouse.FieldSuggestionSpec{
		Prefix: strings.Clone(suggestionContext.Prefix),
		// Fetch the full hard-bounded candidate window before applying editor
		// token eligibility and the global mixed-kind rank. Using the response
		// limit here would let one filtered name consume the entire field budget.
		MaximumFields: clickhouse.MaximumFieldSuggestions,
	}
	compiled, err := service.compiler.CompileFieldSuggestionsContext(ctx, logical, spec)
	if err != nil {
		if sourceDiagnostic(err) {
			return nil, false, nil
		}
		return nil, false, safeDependencyError(err)
	}
	if compiled.Spec != spec {
		return nil, false, searchjobs.ErrInvalidResult
	}
	fieldResult, err := service.executor.ExecuteFieldSuggestions(ctx, compiled)
	if err != nil {
		return nil, false, safeDependencyError(err)
	}
	if err := validateFieldResult(fieldResult, spec); err != nil {
		return nil, false, err
	}

	candidates := make([]spl.SuggestionCandidate, 0, len(fieldResult.FieldNames))
	for _, name := range fieldResult.FieldNames {
		name = strings.Clone(name)
		insertion, ok := fieldSuggestionInsertion(
			name,
			suggestionContext.AllowsQuotedScalarFields,
		)
		if !ok {
			continue
		}
		candidates = append(candidates, spl.SuggestionCandidate{
			Kind:      spl.SuggestionKindField,
			Label:     name,
			Insertion: insertion,
			Detail:    "Field",
		})
	}
	return candidates, true, nil
}

func parseCompletedPrefix(
	source string,
	suggestionContext spl.SuggestionContext,
) (*spl.Query, bool, error) {
	prefixEnd := suggestionContext.PipelinePrefixEnd
	if prefixEnd == 0 || activeSuggestionCommand(source, suggestionContext) == "search" {
		prefixEnd = suggestionContext.Replacement.Start.Offset
	}
	if prefixEnd < 0 || prefixEnd > len(source) {
		return nil, false, searchjobs.ErrInvalidResult
	}
	prefix := source[:prefixEnd]
	if suggestionContext.PipelinePrefixEnd == 0 && strings.TrimSpace(prefix) == "" {
		position := spl.Position{Line: 1, Column: 1}
		return &spl.Query{Range: spl.Range{Start: position, End: position}}, true, nil
	}
	parsed, err := spl.Parse(prefix)
	if err != nil {
		if sourceDiagnostic(err) {
			return nil, false, nil
		}
		return nil, false, searchjobs.ErrInvalidResult
	}
	return parsed, true, nil
}

func activeSuggestionCommand(source string, suggestionContext spl.SuggestionContext) string {
	pipe := suggestionContext.PipelinePrefixEnd
	end := suggestionContext.Replacement.Start.Offset
	if pipe <= 0 || pipe >= end || end > len(source) || source[pipe] != '|' {
		return ""
	}
	stage := strings.TrimLeftFunc(source[pipe+1:end], unicode.IsSpace)
	commandEnd := 0
	for commandEnd < len(stage) {
		character, width := utf8.DecodeRuneInString(stage[commandEnd:])
		if unicode.IsSpace(character) || strings.ContainsRune("|(),=!<>\"", character) {
			break
		}
		commandEnd += width
	}
	return strings.ToLower(stage[:commandEnd])
}

func buildPrefixPlan(
	parsed *spl.Query,
	snapshot searchjobs.AnalysisScopeSnapshot,
) (*plan.Query, bool, error) {
	visibilityCutoff := snapshot.VisibilityCutoff
	intent := snapshot.TimeRange.Intent()
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:                strings.Clone(snapshot.TenantID),
		AuthorizedIndexes:       slices.Clone(snapshot.AuthorizedIndexes),
		RequestedIndexes:        slices.Clone(snapshot.RequestedIndexes),
		AllowUnboundSearchJobID: true,
		Earliest:                snapshot.TimeRange.Earliest(),
		Latest:                  snapshot.TimeRange.Latest(),
		SearchStart:             snapshot.SearchStart,
		SearchTimezone:          strings.Clone(intent.Timezone),
		IndexTimeCutoff:         snapshot.IndexTimeCutoff,
		VisibilityCutoff:        &visibilityCutoff,
	})
	if err != nil {
		if sourceDiagnostic(err) {
			return nil, false, nil
		}
		return nil, false, searchjobs.ErrInvalidResult
	}
	return logical, true, nil
}

func normalizeRequest(
	request Request,
) (normalizedRequest, spl.SuggestionContext, error) {
	maximum := spl.DefaultSuggestionLimit
	if request.MaxSuggestions != nil {
		maximum = int(*request.MaxSuggestions)
		if maximum <= 0 || maximum > spl.MaximumSuggestionLimit {
			return normalizedRequest{}, spl.SuggestionContext{}, ErrInvalidRequest
		}
	}
	if !validSourceAndCursor(request.SPL, request.CursorByteOffset) {
		return normalizedRequest{}, spl.SuggestionContext{}, ErrInvalidRequest
	}
	suggestionContext, diagnostic := spl.AnalyzeSuggestionContext(
		request.SPL,
		request.CursorByteOffset,
	)
	if diagnostic != nil {
		// Tolerant analysis can still encounter syntax or token/pipeline
		// complexity. Full validation owns those in-band diagnostics. A zero
		// context guarantees no completion or storage work can coexist.
		suggestionContext = spl.SuggestionContext{}
	}
	if !validIdentity(request.TenantID) ||
		len(request.AuthorizedIndexes) == 0 ||
		len(request.AuthorizedIndexes) > maximumIndexes ||
		len(request.RequestedIndexes) > maximumIndexes-len(request.AuthorizedIndexes) ||
		len(request.AuthorizedIndexCandidates) > maximumIndexes ||
		!request.TimeRange.Valid() {
		return normalizedRequest{}, spl.SuggestionContext{}, ErrInvalidRequest
	}
	authorized := make(map[string]struct{}, len(request.AuthorizedIndexes))
	for _, index := range request.AuthorizedIndexes {
		if !validIdentity(index) {
			return normalizedRequest{}, spl.SuggestionContext{}, ErrInvalidRequest
		}
		authorized[index] = struct{}{}
	}
	effective := authorized
	if len(request.RequestedIndexes) > 0 {
		effective = make(map[string]struct{}, len(request.RequestedIndexes))
		for _, index := range request.RequestedIndexes {
			if !validIdentity(index) {
				return normalizedRequest{}, spl.SuggestionContext{}, ErrInvalidRequest
			}
			if _, ok := authorized[index]; !ok {
				return normalizedRequest{}, spl.SuggestionContext{}, ErrInvalidRequest
			}
			effective[index] = struct{}{}
		}
	}

	indexCandidates := make([]string, 0, len(request.AuthorizedIndexCandidates))
	seenCandidates := make(map[string]struct{}, len(request.AuthorizedIndexCandidates))
	for _, index := range request.AuthorizedIndexCandidates {
		if !validIdentity(index) {
			return normalizedRequest{}, spl.SuggestionContext{}, ErrInvalidRequest
		}
		if _, ok := authorized[index]; !ok {
			return normalizedRequest{}, spl.SuggestionContext{}, ErrInvalidRequest
		}
		if _, ok := effective[index]; !ok {
			continue
		}
		if _, duplicate := seenCandidates[index]; duplicate {
			continue
		}
		seenCandidates[index] = struct{}{}
		indexCandidates = append(indexCandidates, strings.Clone(index))
	}
	sort.Strings(indexCandidates)

	return normalizedRequest{
		source:                    strings.Clone(request.SPL),
		tenantID:                  strings.Clone(request.TenantID),
		authorizedIndexes:         cloneStrings(request.AuthorizedIndexes),
		requestedIndexes:          cloneStrings(request.RequestedIndexes),
		timeRange:                 request.TimeRange,
		authorizedIndexCandidates: indexCandidates,
		maximum:                   maximum,
		analysisBlocked:           diagnostic != nil,
	}, suggestionContext, nil
}

func validSourceAndCursor(source string, cursor int) bool {
	return len(source) <= spl.MaximumSuggestionSourceBytes &&
		utf8.ValidString(source) &&
		strings.IndexByte(source, 0) < 0 &&
		cursor >= 0 &&
		cursor <= len(source) &&
		(cursor == len(source) || utf8.RuneStart(source[cursor]))
}

func validateValidationResult(
	result searchjobs.ValidationResult,
	request normalizedRequest,
) error {
	if len(result.Diagnostics) > maximumDiagnostics {
		return searchjobs.ErrInvalidResult
	}
	for _, diagnostic := range result.Diagnostics {
		if diagnostic.Code == "" ||
			len(diagnostic.Code) > maximumDiagnosticCodeBytes ||
			!utf8.ValidString(diagnostic.Code) ||
			strings.IndexByte(diagnostic.Code, 0) >= 0 ||
			diagnostic.Message == "" ||
			len(diagnostic.Message) > maximumDiagnosticMessage ||
			!utf8.ValidString(diagnostic.Message) ||
			strings.IndexByte(diagnostic.Message, 0) >= 0 ||
			len(diagnostic.Suggestions) > maximumDiagnosticHints {
			return searchjobs.ErrInvalidResult
		}
		for _, suggestion := range diagnostic.Suggestions {
			if suggestion == "" ||
				len(suggestion) > maximumDiagnosticHintBytes ||
				!utf8.ValidString(suggestion) ||
				strings.IndexByte(suggestion, 0) >= 0 {
				return searchjobs.ErrInvalidResult
			}
		}
	}
	if result.Valid {
		if len(result.Diagnostics) != 0 ||
			result.NormalizedSPL == "" ||
			result.NormalizedSPL != strings.TrimSpace(request.source) ||
			!validValidationResultKind(result.PredictedResultKind) ||
			!validReferencedIndexes(result.ReferencedIndexes, request) ||
			!validReferencedFields(result.ReferencedFields) {
			return searchjobs.ErrInvalidResult
		}
		return nil
	}
	if len(result.Diagnostics) == 0 ||
		result.NormalizedSPL != "" ||
		len(result.ReferencedIndexes) != 0 ||
		len(result.ReferencedFields) != 0 ||
		result.PredictedResultKind != searchjobs.ValidationResultKindInvalid {
		return searchjobs.ErrInvalidResult
	}
	return nil
}

func validValidationResultKind(kind searchjobs.ValidationResultKind) bool {
	switch kind {
	case searchjobs.ValidationResultKindEvents,
		searchjobs.ValidationResultKindStatistics,
		searchjobs.ValidationResultKindTimeSeries:
		return true
	default:
		return false
	}
}

func validReferencedIndexes(indexes []string, request normalizedRequest) bool {
	if len(indexes) == 0 {
		return false
	}
	effective := request.authorizedIndexes
	if len(request.requestedIndexes) != 0 {
		effective = request.requestedIndexes
	}
	allowed := make(map[string]struct{}, len(effective))
	for _, index := range effective {
		allowed[index] = struct{}{}
	}
	var previous string
	for position, index := range indexes {
		if !validIdentity(index) ||
			(position > 0 && previous >= index) {
			return false
		}
		if _, ok := allowed[index]; !ok {
			return false
		}
		previous = index
	}
	return true
}

func validReferencedFields(fields []string) bool {
	var previous string
	for index, field := range fields {
		if field == "" ||
			len(field) > eventfields.MaximumNormalizedFieldNameBytes ||
			!utf8.ValidString(field) ||
			(index > 0 && previous >= field) {
			return false
		}
		resolved, err := plan.ResolveField(field, spl.Range{})
		if err != nil || resolved.Name != field {
			return false
		}
		previous = field
	}
	return true
}

func dynamicFieldLookupAllowed(
	source string,
	context spl.SuggestionContext,
	diagnostics []searchjobs.Diagnostic,
) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == "SPL_INDEX_FORBIDDEN" {
			return false
		}
		if !diagnosticRangeMatchesSource(source, diagnostic) {
			return false
		}
		if diagnostic.EndByteOffset < context.Replacement.Start.Offset {
			return false
		}
	}
	return true
}

func diagnosticRangeMatchesSource(source string, diagnostic searchjobs.Diagnostic) bool {
	if !diagnostic.ValidSourceRange() ||
		diagnostic.EndByteOffset > len(source) {
		return false
	}
	startLine, startColumn, startOK := sourceLineColumn(source, diagnostic.ByteOffset)
	endLine, endColumn, endOK := sourceLineColumn(source, diagnostic.EndByteOffset)
	return startOK && endOK &&
		diagnostic.Line == startLine &&
		diagnostic.Column == startColumn &&
		diagnostic.EndLine == endLine &&
		diagnostic.EndColumn == endColumn
}

func sourceLineColumn(source string, offset int) (int, int, bool) {
	if offset < 0 || offset > len(source) ||
		(offset < len(source) && !utf8.RuneStart(source[offset])) {
		return 0, 0, false
	}
	line, column := 1, 1
	for cursor := 0; cursor < offset; {
		character, width := utf8.DecodeRuneInString(source[cursor:])
		if character == utf8.RuneError && width == 1 {
			return 0, 0, false
		}
		cursor += width
		if character == '\n' {
			line++
			column = 1
		} else {
			column++
		}
	}
	return line, column, true
}

func validSnapshot(snapshot searchjobs.AnalysisScopeSnapshot, request normalizedRequest) bool {
	return snapshot.TenantID == request.tenantID &&
		slices.Equal(snapshot.AuthorizedIndexes, request.authorizedIndexes) &&
		slices.Equal(snapshot.RequestedIndexes, request.requestedIndexes) &&
		snapshot.TimeRange == request.timeRange &&
		!snapshot.SearchStart.IsZero() &&
		snapshot.SearchStart.Equal(snapshot.IndexTimeCutoff)
}

func validateFieldResult(
	result queryexec.FieldSuggestionResult,
	spec clickhouse.FieldSuggestionSpec,
) error {
	if len(result.FieldNames) > int(spec.MaximumFields) ||
		(result.Truncated && len(result.FieldNames) != int(spec.MaximumFields)) {
		return searchjobs.ErrInvalidResult
	}
	var previous string
	for index, name := range result.FieldNames {
		if name == "" ||
			len(name) > eventfields.MaximumNormalizedFieldNameBytes ||
			!utf8.ValidString(name) ||
			strings.IndexByte(name, 0) >= 0 ||
			!strings.HasPrefix(name, spec.Prefix) ||
			!validSuggestionFieldName(name) ||
			(index > 0 && !fieldSuggestionNameBefore(previous, name)) {
			return searchjobs.ErrInvalidResult
		}
		previous = name
	}
	return nil
}

func fieldSuggestionInsertion(name string, allowQuoted bool) (string, bool) {
	if representableFieldName(name) &&
		(!allowQuoted || !requiresQuotedScalarField(name)) {
		return strings.Clone(name), true
	}
	if !allowQuoted || !validSuggestionFieldName(name) {
		return "", false
	}
	var builder strings.Builder
	builder.Grow(len(name) + 2)
	builder.WriteByte('\'')
	for _, character := range name {
		if character == '\\' || character == '\'' {
			builder.WriteByte('\\')
		}
		builder.WriteRune(character)
	}
	builder.WriteByte('\'')
	return builder.String(), true
}

func requiresQuotedScalarField(name string) bool {
	if strings.ContainsAny(name, "+-*/%'") {
		return true
	}
	for _, character := range name {
		if unicode.IsSpace(character) {
			return true
		}
	}
	return false
}

func validSuggestionFieldName(name string) bool {
	if name == "" || !utf8.ValidString(name) {
		return false
	}
	first, _ := utf8.DecodeRuneInString(name)
	last, _ := utf8.DecodeLastRuneInString(name)
	if unicode.IsSpace(first) || unicode.IsSpace(last) {
		return false
	}
	for _, character := range name {
		if unicode.IsControl(character) || !unicode.IsGraphic(character) {
			return false
		}
	}
	resolved, err := plan.ResolveField(name, spl.Range{})
	return err == nil && resolved.Name == name
}

func fieldSuggestionNameBefore(left, right string) bool {
	leftFolded := eventfields.FoldASCII(left)
	rightFolded := eventfields.FoldASCII(right)
	if leftFolded != rightFolded {
		return leftFolded < rightFolded
	}
	return left < right
}

func representableFieldName(name string) bool {
	if !validSuggestionFieldName(name) || name[0] == '+' || name[0] == '-' ||
		strings.ContainsAny(name, "|(),=!<>\"*") {
		return false
	}
	for _, character := range name {
		if unicode.IsSpace(character) || !unicode.IsGraphic(character) {
			return false
		}
	}
	return true
}

func representableIndexName(name string) bool {
	if name == "" || !utf8.ValidString(name) ||
		strings.ContainsAny(name, "|(),=!<>\"*") {
		return false
	}
	for _, character := range name {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func indexSuggestionCandidates(
	context spl.SuggestionContext,
	indexes []string,
) []spl.SuggestionCandidate {
	if !context.Allows(spl.SuggestionKindIndex) {
		return nil
	}
	candidates := make([]spl.SuggestionCandidate, 0, len(indexes))
	for _, index := range indexes {
		if !representableIndexName(index) {
			continue
		}
		candidates = append(candidates, spl.SuggestionCandidate{
			Kind:      spl.SuggestionKindIndex,
			Label:     index,
			Insertion: index,
			Detail:    "Authorized index",
		})
	}
	return candidates
}

func cloneSuggestionContext(context spl.SuggestionContext) spl.SuggestionContext {
	context.Kinds = slices.Clone(context.Kinds)
	context.FunctionNames = cloneStrings(context.FunctionNames)
	context.Keywords = cloneStrings(context.Keywords)
	context.Exclusions = slices.Clone(context.Exclusions)
	for index := range context.Exclusions {
		context.Exclusions[index].Label = strings.Clone(context.Exclusions[index].Label)
	}
	context.Prefix = strings.Clone(context.Prefix)
	return context
}

func cloneSuggestions(suggestions []spl.Suggestion) []spl.Suggestion {
	if len(suggestions) == 0 {
		return nil
	}
	cloned := make([]spl.Suggestion, len(suggestions))
	for index, suggestion := range suggestions {
		suggestion.Label = strings.Clone(suggestion.Label)
		suggestion.Insertion = strings.Clone(suggestion.Insertion)
		suggestion.Detail = strings.Clone(suggestion.Detail)
		cloned[index] = suggestion
	}
	return cloned
}

func cloneDiagnostics(diagnostics []searchjobs.Diagnostic) []searchjobs.Diagnostic {
	if len(diagnostics) == 0 {
		return nil
	}
	cloned := make([]searchjobs.Diagnostic, len(diagnostics))
	for index, diagnostic := range diagnostics {
		diagnostic.Code = strings.Clone(diagnostic.Code)
		diagnostic.Message = strings.Clone(diagnostic.Message)
		diagnostic.Suggestions = cloneStrings(diagnostic.Suggestions)
		cloned[index] = diagnostic
	}
	return cloned
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]string, len(values))
	for index, value := range values {
		cloned[index] = strings.Clone(value)
	}
	return cloned
}

func validIdentity(value string) bool {
	if value == "" || len(value) > maximumIdentity ||
		!utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func sourceDiagnostic(err error) bool {
	if _, ok := errors.AsType[*spl.Diagnostic](err); ok {
		return true
	}
	var planDiagnostic *plan.Diagnostic
	return errors.As(err, &planDiagnostic)
}

func safeDependencyError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, queryexec.ErrFieldMetadataUnavailable):
		return searchjobs.ErrStorageUnavailable
	case errors.Is(err, searchjobs.ErrRequestTooLarge):
		return searchjobs.ErrRequestTooLarge
	case errors.Is(err, searchjobs.ErrCapacity):
		return searchjobs.ErrCapacity
	case errors.Is(err, searchjobs.ErrClosed):
		return searchjobs.ErrClosed
	case errors.Is(err, searchjobs.ErrStorageUnavailable):
		return searchjobs.ErrStorageUnavailable
	case errors.Is(err, searchjobs.ErrExecutionLimit):
		return searchjobs.ErrExecutionLimit
	case errors.Is(err, searchjobs.ErrInvalidResult):
		return searchjobs.ErrInvalidResult
	default:
		return searchjobs.ErrInvalidResult
	}
}

func (service *Service) beginOperation() error {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed {
		return searchjobs.ErrClosed
	}
	select {
	case service.gate <- struct{}{}:
		service.operations.Add(1)
		return nil
	default:
		return searchjobs.ErrCapacity
	}
}

func (service *Service) endOperation() {
	<-service.gate
	service.operations.Done()
}

func (service *Service) operationError(
	callerContext context.Context,
	operationContext context.Context,
	err error,
) error {
	switch {
	case callerContext.Err() != nil:
		return callerContext.Err()
	case service.lifecycleContext.Err() != nil:
		return searchjobs.ErrClosed
	case operationContext.Err() != nil:
		return operationContext.Err()
	default:
		return safeDependencyError(err)
	}
}

// Close stops admission, cancels every in-flight dependency call, and waits
// until no compiler or executor is borrowed. Repeated callers wait on the same
// completion and may retry after their own close context expires.
func (service *Service) Close(ctx context.Context) error {
	if service == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("close search suggestion service: context is nil")
	}
	service.mu.Lock()
	if !service.closed {
		service.closed = true
		service.lifecycleCancel()
	}
	service.mu.Unlock()
	return service.barrier.Wait(ctx, func() { service.operations.Wait() })
}
