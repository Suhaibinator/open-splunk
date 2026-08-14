package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// Validate parses, plans, and compiles one server-scoped search without
// creating a job, consulting storage visibility, executing SQL, or retaining
// compiler output. SPL diagnostics are returned as a successful invalid result;
// service, cancellation, and internal failures are returned as errors.
func (manager *Manager) Validate(ctx context.Context, request ValidateRequest) (ValidationResult, error) {
	if manager == nil {
		return ValidationResult{}, errors.New("validate search: manager is nil")
	}
	if ctx == nil {
		return ValidationResult{}, errors.New("validate search: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return ValidationResult{}, err
	}
	if err := manager.validatePlanningRequestSize(
		request.SPL,
		request.TenantID,
		request.AuthorizedIndexes,
		request.RequestedIndexes,
		request.TimeRange,
	); err != nil {
		return ValidationResult{}, err
	}
	if !request.TimeRange.Valid() {
		return ValidationResult{}, errors.New("validate search: resolved time range is required")
	}
	if !validAccessIdentity(request.TenantID) {
		return ValidationResult{}, errors.New("validate search: tenant identity is invalid")
	}
	if err := manager.beginSynchronousOperation(ctx); err != nil {
		return ValidationResult{}, err
	}
	defer manager.endSynchronousOperation()
	request.AuthorizedIndexes = cloneStrings(request.AuthorizedIndexes)
	request.RequestedIndexes = cloneStrings(request.RequestedIndexes)

	validationContext, cancelValidation := context.WithCancel(ctx)
	stopManagerCancellation := context.AfterFunc(manager.ctx, cancelValidation)
	defer func() {
		stopManagerCancellation()
		cancelValidation()
	}()

	parsed, err := parseSPLQuery(validationContext, request.SPL)
	if err != nil {
		if contextErr := manager.operationContextError(ctx); contextErr != nil {
			return ValidationResult{}, contextErr
		}
		return invalidValidationResult(err)
	}

	anchor := manager.nowUTC()
	visibilityCutoff := uint64(0)
	intent := request.TimeRange.Intent()
	scope := plan.Scope{
		TenantID:                request.TenantID,
		AuthorizedIndexes:       request.AuthorizedIndexes,
		RequestedIndexes:        request.RequestedIndexes,
		AllowUnboundSearchJobID: true,
		Earliest:                request.TimeRange.Earliest(),
		Latest:                  request.TimeRange.Latest(),
		SearchStart:             anchor,
		SearchTimezone:          intent.Timezone,
		IndexTimeCutoff:         anchor,
		VisibilityCutoff:        &visibilityCutoff,
	}
	preparation, err := plan.PrepareStatsWildcard(parsed, scope)
	var logical *plan.Query
	if err == nil {
		logical = preparation.FullPlan()
		if logical != nil {
			_, err = manager.compiler.Compile(logical)
		} else {
			logical = preparation.Prefix()
			request := preparation.Request()
			if logical == nil || request.IsZero() {
				err = errors.New("validate search: stats wildcard preparation is incomplete")
			} else {
				var inventory clickhouse.CompiledStatsWildcardInventory
				inventory, err = manager.compiler.CompileStatsWildcardInventory(
					logical,
					request,
				)
				if err == nil {
					_, ok := inventory.CloneForExecution()
					if !ok {
						err = errors.New("validate search: stats wildcard inventory authority is invalid")
					}
				}
			}
		}
	}
	if err != nil {
		if contextErr := manager.operationContextError(ctx); contextErr != nil {
			return ValidationResult{}, contextErr
		}
		return invalidValidationResult(err)
	}

	analysis, err := plan.Analyze(logical)
	if contextErr := manager.operationContextError(ctx); contextErr != nil {
		return ValidationResult{}, contextErr
	}
	if err != nil {
		return ValidationResult{}, errors.New("validate search: analyze logical query")
	}
	resultKind := validationResultKind(spl.ClassifyResultShape(parsed).Kind)
	if resultKind == ValidationResultKindInvalid {
		return ValidationResult{}, errors.New("validate search: classify result shape")
	}
	return ValidationResult{
		Valid:               true,
		NormalizedSPL:       strings.TrimSpace(request.SPL),
		ReferencedIndexes:   cloneStrings(logical.EffectiveIndexes),
		ReferencedFields:    cloneStrings(analysis.ReferencedFields),
		PredictedResultKind: resultKind,
	}, nil
}

func (manager *Manager) validatePlanningRequestSize(
	source string,
	tenantID string,
	authorizedIndexes []string,
	requestedIndexes []string,
	resolvedRange searchtime.Range,
) error {
	if len(source) > manager.maxSPLBytes {
		return fmt.Errorf("%w: SPL exceeds %d bytes", ErrRequestTooLarge, manager.maxSPLBytes)
	}
	if len(tenantID) > defaultMaxIdentityBytes {
		return fmt.Errorf("%w: owner or tenant identity exceeds %d bytes", ErrRequestTooLarge, defaultMaxIdentityBytes)
	}
	intent := resolvedRange.Intent()
	if len(intent.Earliest) > searchtime.MaximumExpressionBytes || len(intent.Latest) > searchtime.MaximumExpressionBytes {
		return fmt.Errorf("%w: time expression exceeds %d bytes", ErrRequestTooLarge, searchtime.MaximumExpressionBytes)
	}
	if len(intent.Timezone) > searchtime.MaximumTimezoneBytes {
		return fmt.Errorf("%w: timezone exceeds %d bytes", ErrRequestTooLarge, searchtime.MaximumTimezoneBytes)
	}
	for _, value := range []string{intent.Earliest, intent.Latest, intent.Timezone} {
		if len(value) > defaultMaxIdentityBytes || !utf8.ValidString(value) {
			return fmt.Errorf("%w: search intent metadata exceeds %d bytes", ErrRequestTooLarge, defaultMaxIdentityBytes)
		}
	}
	if len(authorizedIndexes) > manager.maxScopeIndexes ||
		len(requestedIndexes) > manager.maxScopeIndexes-len(authorizedIndexes) {
		return fmt.Errorf("%w: index scope exceeds %d entries", ErrRequestTooLarge, manager.maxScopeIndexes)
	}
	for _, indexes := range [][]string{authorizedIndexes, requestedIndexes} {
		for _, index := range indexes {
			if len(index) > defaultMaxIdentityBytes {
				return fmt.Errorf("%w: index name exceeds %d bytes", ErrRequestTooLarge, defaultMaxIdentityBytes)
			}
		}
	}
	return nil
}

func parseSPLQuery(ctx context.Context, source string) (*spl.Query, error) {
	if ctx == nil {
		return nil, errors.New("parse SPL query: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parsed, err := spl.Parse(source)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return parsed, nil
}

func invalidValidationResult(err error) (ValidationResult, error) {
	diagnostic, ok := searchDiagnostic(err)
	if !ok {
		return ValidationResult{}, errors.New("validate search: compile query")
	}
	return ValidationResult{Diagnostics: []Diagnostic{diagnostic}}, nil
}

func searchDiagnostic(err error) (Diagnostic, bool) {
	var parseDiagnostic *spl.Diagnostic
	if errors.As(err, &parseDiagnostic) {
		return diagnosticFromSPL(parseDiagnostic), true
	}
	var planningDiagnostic *plan.Diagnostic
	if errors.As(err, &planningDiagnostic) {
		return diagnosticFromPlan(planningDiagnostic), true
	}
	return Diagnostic{}, false
}

func diagnosticFromSPL(diagnostic *spl.Diagnostic) Diagnostic {
	if diagnostic == nil {
		return Diagnostic{}
	}
	return newSearchDiagnostic(
		diagnostic.Code,
		diagnostic.Message,
		diagnostic.Range,
		diagnostic.Suggestions,
	)
}

func diagnosticFromPlan(diagnostic *plan.Diagnostic) Diagnostic {
	if diagnostic == nil {
		return Diagnostic{}
	}
	return newSearchDiagnostic(
		diagnostic.Code,
		diagnostic.Message,
		diagnostic.Range,
		diagnostic.Suggestions,
	)
}

func newSearchDiagnostic(code string, message string, sourceRange spl.Range, suggestions []string) Diagnostic {
	return Diagnostic{
		Code:          code,
		Message:       message,
		ByteOffset:    sourceRange.Start.Offset,
		Line:          sourceRange.Start.Line,
		Column:        sourceRange.Start.Column,
		EndByteOffset: sourceRange.End.Offset,
		EndLine:       sourceRange.End.Line,
		EndColumn:     sourceRange.End.Column,
		Suggestions:   cloneStrings(suggestions),
	}
}

func validationResultKind(kind spl.ResultKind) ValidationResultKind {
	switch kind {
	case spl.ResultKindEvents:
		return ValidationResultKindEvents
	case spl.ResultKindStatistics:
		return ValidationResultKindStatistics
	case spl.ResultKindTimeSeries:
		return ValidationResultKindTimeSeries
	default:
		return ValidationResultKindInvalid
	}
}
