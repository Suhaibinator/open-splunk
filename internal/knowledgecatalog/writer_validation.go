package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

const (
	// MaximumConcurrentValidations is the process-local, per-control-database
	// bound for full catalog validation. Validation is intentionally fail-fast:
	// a second request never waits behind the admitted transaction.
	MaximumConcurrentValidations  = 1
	maximumValidationRequestBytes = maximumMutationRequestBytes
)

// ValidationScope separates candidate-app mutation authority from dependency
// disclosure authority. Both sides must describe the same authenticated
// tenant and owner, while their app sets remain independent.
type ValidationScope struct {
	Read  ReadScope
	Write WriteScope
}

type normalizedValidationScope struct {
	read  normalizedScope
	write normalizedWriteScope
}

type normalizedValidationRequest struct {
	definition      *opensplunkv1.KnowledgeObjectDefinition
	objectID        string
	expectedVersion int64
	updatePaths     []string
	intent          opensplunkv1.KnowledgeValidationIntent
	update          bool
}

type validationCardinalityIssue uint8

const (
	validationCardinalityNone validationCardinalityIssue = iota
	validationCardinalitySelectorIndex
	validationCardinalitySelectorHost
	validationCardinalitySelectorSource
	validationCardinalitySelectorSourcetype
	validationCardinalityExtractionOutputs
)

// Validate evaluates one detached candidate in a fixed, always-rolled-back
// knowledge/app/index catalog transaction. It never reserves an identity or
// invokes mutation-only audit, idempotency, hook, clock, ID, or DML authority.
func (writer *Writer) Validate(
	ctx context.Context,
	scope ValidationScope,
	request *opensplunkv1.ValidateKnowledgeObjectRequest,
) (sealed knowledgevalidation.SealedValidateResponse, returnedErr error) {
	var authorized *AuthorizedContext
	defer func() {
		if returnedErr != nil && authorized != nil {
			returnedErr = withAuthorizedContext(returnedErr, *authorized)
		}
		returnedErr = withDefaultErrorDisposition(
			returnedErr,
			ErrorDispositionDefinitiveRejection,
		)
	}()
	if ctx == nil {
		return knowledgevalidation.SealedValidateResponse{}, validateContext(ctx)
	}
	if err := validateValidationRequestEnvelope(request); err != nil {
		return knowledgevalidation.SealedValidateResponse{}, err
	}
	normalizedScope, err := normalizeValidationScope(scope)
	if err != nil {
		return knowledgevalidation.SealedValidateResponse{}, err
	}
	if writer == nil || writer.orm == nil || writer.reader == nil ||
		writer.reader.orm != writer.orm || writer.validationGate == nil {
		return knowledgevalidation.SealedValidateResponse{}, fmt.Errorf(
			"%w: knowledge validation writer is unavailable",
			control.ErrInvalidArgument,
		)
	}
	if !writer.validationGate.TryAcquire() {
		return knowledgevalidation.SealedValidateResponse{}, control.ErrCapacityExceeded
	}
	defer writer.validationGate.Release()
	prepared, err := normalizeValidationRequest(request)
	if err != nil {
		return knowledgevalidation.SealedValidateResponse{}, err
	}
	if err := validateContext(ctx); err != nil {
		return knowledgevalidation.SealedValidateResponse{}, err
	}

	result, revision, err := writer.validateInTransaction(
		ctx,
		normalizedScope,
		prepared,
		&authorized,
	)
	if err != nil {
		return knowledgevalidation.SealedValidateResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return knowledgevalidation.SealedValidateResponse{}, err
	}
	sealed, err = knowledgevalidation.SealValidateResponse(ctx, result, revision)
	if err != nil {
		return knowledgevalidation.SealedValidateResponse{}, validationSealError(err)
	}
	return sealed, nil
}

func normalizeValidationScope(scope ValidationScope) (normalizedValidationScope, error) {
	read, err := normalizeScope(scope.Read)
	if err != nil {
		return normalizedValidationScope{}, err
	}
	write, err := normalizeWriteScope(scope.Write)
	if err != nil {
		return normalizedValidationScope{}, err
	}
	if read.tenantID != write.tenantID || read.ownerID != write.ownerID {
		return normalizedValidationScope{}, fmt.Errorf(
			"%w: knowledge validation read and write scopes disagree",
			control.ErrInvalidArgument,
		)
	}
	return normalizedValidationScope{read: read, write: write}, nil
}

// ValidateKnowledgeObjectRequest checks only the registered Validate route's
// request envelope. It performs no clone or candidate definition validation;
// candidate issues deliberately remain for the in-band knowledgevalidation
// builders.
func ValidateKnowledgeObjectRequest(request *opensplunkv1.ValidateKnowledgeObjectRequest) error {
	return validateValidationRequestEnvelope(request)
}

func normalizeValidationRequest(
	request *opensplunkv1.ValidateKnowledgeObjectRequest,
) (normalizedValidationRequest, error) {
	if err := validateValidationRequestEnvelope(request); err != nil {
		return normalizedValidationRequest{}, err
	}
	update := request.KnowledgeObjectId != nil
	var paths []string
	if update {
		var err error
		paths, err = normalizeKnowledgeUpdateMask(request.GetUpdateMask())
		if err != nil {
			return normalizedValidationRequest{}, err
		}
	}

	definitionView := request.GetDefinition()
	issue := appliedValidationCardinalityIssue(definitionView, update, paths)
	var detachedDefinition *opensplunkv1.KnowledgeObjectDefinition
	if issue != validationCardinalityNone {
		// The witness is newly allocated and contains no caller-owned scalar or
		// submessage. Keeping it directly also preserves a selected typed-nil
		// body wrapper for applyKnowledgeDefinitionMask's applicability check.
		detachedDefinition = validationCardinalityWitness(definitionView, issue)
	} else {
		if update {
			definitionView = validationUpdateDefinitionView(definitionView, paths)
		}
		if err := preflightValidationRequestPayload(
			validationRequestView(request, definitionView),
		); err != nil {
			return normalizedValidationRequest{}, err
		}
		var ok bool
		detachedDefinition, ok = proto.Clone(definitionView).(*opensplunkv1.KnowledgeObjectDefinition)
		if !ok || detachedDefinition == nil {
			return normalizedValidationRequest{}, invalidValidationEnvelope("definition cannot be detached")
		}
	}
	prepared := normalizedValidationRequest{
		definition: detachedDefinition,
		intent:     request.GetIntent(),
	}
	if !update {
		return prepared, nil
	}
	prepared.update = true
	prepared.objectID = strings.Clone(request.GetKnowledgeObjectId())
	// #nosec G115 -- validation envelope checks reject expected versions above math.MaxInt64.
	prepared.expectedVersion = int64(request.GetExpectedVersion())
	prepared.updatePaths = slices.Clone(paths)
	return prepared, nil
}

func preflightValidationRequestPayload(request *opensplunkv1.ValidateKnowledgeObjectRequest) error {
	if size := proto.Size(request); size <= 0 || size > maximumValidationRequestBytes {
		return fmt.Errorf(
			"%w: knowledge validation request exceeds its byte limit",
			control.ErrCapacityExceeded,
		)
	}
	return nil
}

func validationRequestView(
	request *opensplunkv1.ValidateKnowledgeObjectRequest,
	definition *opensplunkv1.KnowledgeObjectDefinition,
) *opensplunkv1.ValidateKnowledgeObjectRequest {
	return &opensplunkv1.ValidateKnowledgeObjectRequest{
		Definition:        definition,
		KnowledgeObjectId: request.KnowledgeObjectId,
		ExpectedVersion:   request.ExpectedVersion,
		UpdateMask:        request.UpdateMask,
		Intent:            request.Intent,
	}
}

func appliedValidationCardinalityIssue(
	definition *opensplunkv1.KnowledgeObjectDefinition,
	update bool,
	paths []string,
) validationCardinalityIssue {
	selectorApplied := !update
	extractionApplied := !update
	if update {
		for _, path := range paths {
			selectorApplied = selectorApplied || path == "selector"
			extractionApplied = extractionApplied || path == "field_extraction"
		}
	}
	if selectorApplied {
		selector := definition.GetSelector()
		if selector != nil {
			switch {
			case len(selector.GetIndexPatterns()) > knowledge.MaximumSelectorPatternsPerDimension:
				return validationCardinalitySelectorIndex
			case len(selector.GetHostPatterns()) > knowledge.MaximumSelectorPatternsPerDimension:
				return validationCardinalitySelectorHost
			case len(selector.GetSourcePatterns()) > knowledge.MaximumSelectorPatternsPerDimension:
				return validationCardinalitySelectorSource
			case len(selector.GetSourcetypePatterns()) > knowledge.MaximumSelectorPatternsPerDimension:
				return validationCardinalitySelectorSourcetype
			}
		}
	}
	if extractionApplied {
		extraction := definition.GetFieldExtraction()
		if extraction != nil && extraction.GetRegex() != nil &&
			len(extraction.GetRegex().GetOutputFields()) > knowledgedefinition.MaximumFieldExtractionOutputs {
			return validationCardinalityExtractionOutputs
		}
	}
	return validationCardinalityNone
}

// validationUpdateDefinitionView is a shallow, mask-selected view. It is safe
// to size before detachment because every selected repeated field has already
// passed the O(1) cardinality admission above. Unselected caller data never
// enters either the byte authority or the clone.
func validationUpdateDefinitionView(
	definition *opensplunkv1.KnowledgeObjectDefinition,
	paths []string,
) *opensplunkv1.KnowledgeObjectDefinition {
	result := &opensplunkv1.KnowledgeObjectDefinition{}
	for _, path := range paths {
		switch path {
		case "app_id":
			result.AppId = definition.GetAppId()
		case "name":
			result.Name = definition.GetName()
		case "description":
			result.Description = definition.Description
		case "sharing_scope":
			result.SharingScope = definition.GetSharingScope()
		case "selector":
			result.Selector = definition.GetSelector()
		case "field_extraction":
			if body := definition.GetFieldExtraction(); body != nil {
				result.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
					FieldExtraction: body,
				}
			}
		case "field_alias":
			if body := definition.GetFieldAlias(); body != nil {
				result.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: body}
			}
		case "calculated_field":
			if body := definition.GetCalculatedField(); body != nil {
				result.Body = &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
					CalculatedField: body,
				}
			}
		}
	}
	return result
}

func validationCardinalityWitness(
	definition *opensplunkv1.KnowledgeObjectDefinition,
	issue validationCardinalityIssue,
) *opensplunkv1.KnowledgeObjectDefinition {
	result := &opensplunkv1.KnowledgeObjectDefinition{}
	if issue == validationCardinalityExtractionOutputs {
		result.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{
						OutputFields: make(
							[]string,
							knowledgedefinition.MaximumFieldExtractionOutputs+1,
						),
					},
				},
			},
		}
		return result
	}
	copyValidationBodyKind(result, definition)
	result.Selector = &opensplunkv1.KnowledgeSelector{}
	patterns := make(
		[]*opensplunkv1.KnowledgeSelectorPattern,
		knowledge.MaximumSelectorPatternsPerDimension+1,
	)
	switch issue {
	case validationCardinalitySelectorIndex:
		result.Selector.IndexPatterns = patterns
	case validationCardinalitySelectorHost:
		result.Selector.HostPatterns = patterns
	case validationCardinalitySelectorSource:
		result.Selector.SourcePatterns = patterns
	case validationCardinalitySelectorSourcetype:
		result.Selector.SourcetypePatterns = patterns
	}
	return result
}

func copyValidationBodyKind(
	target *opensplunkv1.KnowledgeObjectDefinition,
	source *opensplunkv1.KnowledgeObjectDefinition,
) {
	switch source.GetBody().(type) {
	case *opensplunkv1.KnowledgeObjectDefinition_FieldExtraction:
		var body *opensplunkv1.FieldExtractionDefinition
		if source.GetFieldExtraction() != nil {
			body = &opensplunkv1.FieldExtractionDefinition{}
		}
		target.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: body,
		}
	case *opensplunkv1.KnowledgeObjectDefinition_FieldAlias:
		var body *opensplunkv1.FieldAliasDefinition
		if source.GetFieldAlias() != nil {
			body = &opensplunkv1.FieldAliasDefinition{}
		}
		target.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: body}
	case *opensplunkv1.KnowledgeObjectDefinition_CalculatedField:
		var body *opensplunkv1.CalculatedFieldDefinition
		if source.GetCalculatedField() != nil {
			body = &opensplunkv1.CalculatedFieldDefinition{}
		}
		target.Body = &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
			CalculatedField: body,
		}
	}
}

func validateValidationRequestEnvelope(
	request *opensplunkv1.ValidateKnowledgeObjectRequest,
) error {
	if request == nil || request.GetDefinition() == nil {
		return invalidValidationEnvelope("request and definition are required")
	}
	if len(request.ProtoReflect().GetUnknown()) != 0 ||
		request.GetUpdateMask() != nil && len(request.GetUpdateMask().ProtoReflect().GetUnknown()) != 0 {
		return invalidValidationEnvelope("request contains unknown envelope fields")
	}
	switch request.GetIntent() {
	case opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION:
	default:
		return invalidValidationEnvelope("intent must select inactive storage or active publication")
	}
	if request.KnowledgeObjectId == nil {
		if request.ExpectedVersion != nil || request.UpdateMask != nil {
			return invalidValidationEnvelope(
				"create validation forbids expected version and update mask",
			)
		}
		return nil
	}
	if !validIdentity(request.GetKnowledgeObjectId(), maximumObjectIDBytes) ||
		request.ExpectedVersion == nil || request.GetExpectedVersion() == 0 ||
		request.GetExpectedVersion() > math.MaxInt64 || request.UpdateMask == nil {
		return invalidValidationEnvelope(
			"update validation requires identity, version, and update mask",
		)
	}
	paths := request.GetUpdateMask().GetPaths()
	if len(paths) == 0 || len(paths) > len(knowledgeUpdatePaths) {
		return invalidValidationEnvelope("update mask must contain supported top-level paths")
	}
	for _, path := range paths {
		if len(path) > len("field_extraction") {
			return invalidValidationEnvelope("update mask contains an unsupported path")
		}
	}
	_, err := normalizeKnowledgeUpdateMask(request.GetUpdateMask())
	return err
}

func invalidValidationEnvelope(reason string) error {
	return fmt.Errorf(
		"%w: knowledge validation %s",
		control.ErrInvalidArgument,
		strings.TrimSpace(reason),
	)
}

func validationSealError(err error) error {
	if errors.Is(err, knowledgevalidation.ErrResponseTooLarge) {
		return errors.Join(control.ErrCapacityExceeded, err)
	}
	return err
}

func (writer *Writer) validateInTransaction(
	ctx context.Context,
	scope normalizedValidationScope,
	request normalizedValidationRequest,
	authorized **AuthorizedContext,
) (result knowledgevalidation.Result, revision uint64, returnedErr error) {
	tx := writer.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return knowledgevalidation.Result{}, 0, writerError(ctx, "begin validation", tx.Error)
	}
	defer finishValidationTransaction(tx, &returnedErr)
	return writer.validateFixedSnapshot(ctx, tx, scope, request, authorized)
}

func finishValidationTransaction(tx *gorm.DB, returnedErr *error) {
	if tx == nil {
		return
	}
	if err := tx.Rollback().Error; err != nil {
		rollbackErr := fmt.Errorf("roll back knowledge validation: %w", err)
		if returnedErr != nil {
			*returnedErr = errors.Join(*returnedErr, rollbackErr)
		}
	}
}
