package knowledgevalidation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"slices"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SealValidateResponse checks the entire result boundary and retains its exact
// deterministic protobuf encoding when it fits the 8 MiB public limit.
func SealValidateResponse(ctx context.Context, result Result, tenantCatalogRevision uint64) (SealedValidateResponse, error) {
	return sealValidateResponse(ctx, result, tenantCatalogRevision, MaximumResponseBytes)
}

func sealValidateResponse(ctx context.Context, result Result, tenantCatalogRevision uint64, maximumBytes int) (SealedValidateResponse, error) {
	if err := contextError(ctx); err != nil {
		return SealedValidateResponse{}, err
	}
	if result.state == nil || result.state.value == nil || maximumBytes < 1 || tenantCatalogRevision > math.MaxInt64 {
		return SealedValidateResponse{}, ErrInvariant
	}
	cloned, ok := proto.Clone(result.state.value).(*opensplunk.KnowledgeValidationResult)
	if !ok || cloned == nil {
		return SealedValidateResponse{}, ErrInvariant
	}
	sources := cloneDiagnosticSources(result.state.diagnosticSources)
	if err := validateResult(ctx, cloned, result.state.kind, sources); err != nil {
		return SealedValidateResponse{}, err
	}
	response := &opensplunk.ValidateKnowledgeObjectResponse{
		Result:                cloned,
		TenantCatalogRevision: tenantCatalogRevision,
	}
	if err := rejectUnknownFields(ctx, response.ProtoReflect(), 0); err != nil {
		return SealedValidateResponse{}, err
	}
	if err := contextError(ctx); err != nil {
		return SealedValidateResponse{}, err
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return SealedValidateResponse{}, ErrInvariant
	}
	if len(wire) > maximumBytes {
		return SealedValidateResponse{}, ErrResponseTooLarge
	}
	if err := contextError(ctx); err != nil {
		return SealedValidateResponse{}, err
	}
	return SealedValidateResponse{
		response: response, wire: wire, kind: result.state.kind,
		diagnosticSources: sources,
	}, nil
}

func validateResult(
	ctx context.Context,
	result *opensplunk.KnowledgeValidationResult,
	kind resultKind,
	diagnosticSources []diagnosticSource,
) error {
	if result == nil || result.GetFieldViolationsTruncated() || result.GetDiagnosticsTruncated() ||
		!validObjectType(result.GetObjectType(), !result.GetValid()) {
		return ErrInvariant
	}
	if err := issueDescriptorContract(); err != nil {
		return err
	}
	if err := rejectUnknownFields(ctx, result.ProtoReflect(), 0); err != nil {
		return err
	}
	if err := validateFieldViolationProjection(ctx, result.GetFieldViolations()); err != nil {
		return err
	}
	if err := validateDiagnosticProjection(ctx, result.GetDiagnostics(), diagnosticSources); err != nil {
		return err
	}
	hasError := false
	for _, value := range result.GetDiagnostics() {
		if value.GetDiagnostic().GetSeverity() == opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR {
			hasError = true
			break
		}
	}
	if !result.GetValid() {
		if kind != resultKindInvalid || result.GetNormalizedDefinition() != nil || result.DefinitionSha256 != nil ||
			result.GetResources() != nil || len(result.GetDependencies()) != 0 ||
			(len(result.GetFieldViolations()) == 0 && !hasError) {
			return ErrInvariant
		}
		return nil
	}
	if kind != resultKindInactive && kind != resultKindActive {
		return ErrInvariant
	}
	if result.GetNormalizedDefinition() == nil || len(result.GetDefinitionSha256()) != sha256.Size ||
		result.GetResources() == nil || len(result.GetFieldViolations()) != 0 ||
		result.GetFieldViolationsTruncated() || hasError {
		return ErrInvariant
	}
	normalized, err := knowledgedefinition.Normalize(result.GetNormalizedDefinition())
	if err != nil || normalized.ObjectType != result.GetObjectType() ||
		!proto.Equal(normalized.Definition, result.GetNormalizedDefinition()) ||
		!bytes.Equal(normalized.Digest[:], result.GetDefinitionSha256()) {
		return ErrInvariant
	}
	resources := result.GetResources()
	patterns := normalized.Selector.Stats().Patterns
	if patterns > math.MaxUint32 || resources.GetSelectorPatterns() != uint32(patterns) ||
		resources.GetNormalizedDefinitionBytes() != uint64(len(normalized.Bytes)) {
		return ErrInvariant
	}
	if err := validateDependencyProjection(ctx, result.GetDependencies(), resources); err != nil {
		return err
	}
	switch kind {
	case resultKindInactive:
		if len(result.GetDependencies()) != 0 || !zeroPublicationResources(resources) {
			return ErrInvariant
		}
	case resultKindActive:
		program, compileErr := compileSingleton(normalized)
		if compileErr != nil || program.IsZero() || program.ObjectCount() != 1 || len(program.Dependencies()) != 0 {
			return ErrInvariant
		}
		if !resourcesMatchCharges(resources, intrinsicChargesFromProgram(program.Charges())) {
			return ErrInvariant
		}
	default:
		return ErrInvariant
	}
	return nil
}

func zeroPublicationResources(resources *opensplunk.KnowledgeResourceEstimate) bool {
	return resources.GetDependencyNodes() == 0 && resources.GetDependencyEdges() == 0 &&
		resources.GetGeneratedOperators() == 0 && resources.GetGeneratedFields() == 0 &&
		resources.GetRegexPrograms() == 0 && resources.GetEstimatedRegexWorkUnits() == 0 &&
		resources.GetScalarExpressions() == 0 && resources.GetScalarExpressionNodes() == 0 &&
		resources.GetExtractionOutputs() == 0 && resources.GetJsonEvaluationWorkUnits() == 0 &&
		resources.GetScalarPredicates() == 0
}

func resourcesMatchCharges(resources *opensplunk.KnowledgeResourceEstimate, charges intrinsicCharges) bool {
	return resources.GetGeneratedOperators() == charges.generatedOperators &&
		resources.GetGeneratedFields() == charges.generatedFields &&
		resources.GetRegexPrograms() == charges.regexPrograms &&
		resources.GetEstimatedRegexWorkUnits() == charges.regexWorkUnits &&
		resources.GetScalarExpressions() == charges.scalarExpressions &&
		resources.GetScalarExpressionNodes() == charges.scalarExpressionNodes &&
		resources.GetExtractionOutputs() == charges.extractionOutputs &&
		resources.GetJsonEvaluationWorkUnits() == charges.jsonEvaluationWork &&
		resources.GetScalarPredicates() == charges.scalarPredicates
}

func validateFieldViolationProjection(ctx context.Context, values []*opensplunk.FieldViolation) error {
	if len(values) > MaximumIssues {
		return ErrInvariant
	}
	textBytes := 0
	for index, value := range values {
		if index%64 == 0 {
			if err := contextError(ctx); err != nil {
				return err
			}
		}
		if value == nil || !validPath(value.GetFieldPath()) ||
			!validIssueScalar(value.GetCode(), maximumIssueCodeBytes) ||
			!validIssueScalar(value.GetMessage(), maximumIssueMessage) {
			return ErrInvariant
		}
		charge := len(value.GetFieldPath()) + len(value.GetCode()) + len(value.GetMessage())
		if charge > MaximumFieldViolationTextBytes-textBytes {
			return ErrInvariant
		}
		textBytes += charge
		if index > 0 && compareFieldViolations(values[index-1], value) >= 0 {
			return ErrInvariant
		}
	}
	return nil
}

func validateDiagnosticProjection(
	ctx context.Context,
	values []*opensplunk.KnowledgeValidationDiagnostic,
	sources []diagnosticSource,
) error {
	if len(values) > MaximumIssues || len(values) != len(sources) {
		return ErrInvariant
	}
	textBytes := 0
	for index, value := range values {
		if index%64 == 0 {
			if err := contextError(ctx); err != nil {
				return err
			}
		}
		if value == nil || value.GetDiagnostic() == nil || !validPath(value.GetFieldPath()) {
			return ErrInvariant
		}
		diagnostic := value.GetDiagnostic()
		if !validIssueScalar(diagnostic.GetCode(), maximumIssueCodeBytes) ||
			!validIssueScalar(diagnostic.GetMessage(), maximumIssueMessage) || severityRank(diagnostic.GetSeverity()) < 0 ||
			len(diagnostic.GetSuggestions()) > maximumSuggestions {
			return ErrInvariant
		}
		charge := len(value.GetFieldPath()) + len(diagnostic.GetCode()) + len(diagnostic.GetMessage())
		for suggestionIndex, suggestion := range diagnostic.GetSuggestions() {
			if !validIssueScalar(suggestion, maximumSuggestionBytes) ||
				suggestionIndex > 0 && diagnostic.GetSuggestions()[suggestionIndex-1] >= suggestion {
				return ErrInvariant
			}
			charge += len(suggestion)
		}
		if charge > MaximumDiagnosticTextBytes-textBytes {
			return ErrInvariant
		}
		textBytes += charge
		if sourceRange := diagnostic.GetSourceRange(); sourceRange != nil {
			if !sources[index].present || sources[index].fieldPath != value.GetFieldPath() ||
				sourceRange.GetStart() == nil || sourceRange.GetEnd() == nil ||
				sourceRange.GetStart().GetByteOffset() > sourceRange.GetEnd().GetByteOffset() ||
				sourceRange.GetStart().GetLine() == 0 || sourceRange.GetStart().GetColumn() == 0 ||
				sourceRange.GetEnd().GetLine() == 0 || sourceRange.GetEnd().GetColumn() == 0 {
				return ErrInvariant
			}
			expected, err := publicRange(ctx, byteRange{
				start:  sourceRange.GetStart().GetByteOffset(),
				end:    sourceRange.GetEnd().GetByteOffset(),
				source: sources[index].value,
			})
			if err != nil || !equalSourceRange(sourceRange, expected) {
				return ErrInvariant
			}
		} else if sources[index].present || sources[index].fieldPath != "" || sources[index].value != "" {
			return ErrInvariant
		}
		if index > 0 && compareDiagnostics(values[index-1], value) >= 0 {
			return ErrInvariant
		}
	}
	return nil
}

func equalSourceRange(left, right *opensplunk.SourceRange) bool {
	if left == nil || right == nil || left.GetStart() == nil || right.GetStart() == nil ||
		left.GetEnd() == nil || right.GetEnd() == nil {
		return false
	}
	return left.GetStart().GetByteOffset() == right.GetStart().GetByteOffset() &&
		left.GetStart().GetLine() == right.GetStart().GetLine() &&
		left.GetStart().GetColumn() == right.GetStart().GetColumn() &&
		left.GetEnd().GetByteOffset() == right.GetEnd().GetByteOffset() &&
		left.GetEnd().GetLine() == right.GetEnd().GetLine() &&
		left.GetEnd().GetColumn() == right.GetEnd().GetColumn()
}

func validateDependencyProjection(ctx context.Context, values []*opensplunk.KnowledgeValidationDependency, resources *opensplunk.KnowledgeResourceEstimate) error {
	if len(values) > MaximumDependencies || resources == nil ||
		resources.GetDependencyEdges() != uint32(len(values)) { // #nosec G115 -- len(values) is bounded by MaximumDependencies first.
		return ErrInvariant
	}
	nodes := make(map[string]struct{}, len(values))
	for index, value := range values {
		if index%64 == 0 {
			if err := contextError(ctx); err != nil {
				return err
			}
		}
		if value == nil || value.GetTarget() == nil ||
			!validIdentity(value.GetTarget().GetKnowledgeObjectId(), maximumObjectIDBytes) ||
			value.GetTarget().GetVersion() == 0 || value.GetTarget().GetVersion() > math.MaxInt64 ||
			value.GetRole() != opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT {
			return ErrInvariant
		}
		if index > 0 {
			previous := values[index-1]
			order := compareDependency(previous, value)
			if order >= 0 {
				return ErrInvariant
			}
		}
		nodes[value.GetTarget().GetKnowledgeObjectId()+"\x00"+stringUint64(value.GetTarget().GetVersion())] = struct{}{}
	}
	if resources.GetDependencyNodes() != uint32(len(nodes)) { // #nosec G115 -- nodes cannot exceed the bounded dependency count.
		return ErrInvariant
	}
	return nil
}

func compareDependency(left, right *opensplunk.KnowledgeValidationDependency) int {
	if left.GetTarget().GetKnowledgeObjectId() < right.GetTarget().GetKnowledgeObjectId() {
		return -1
	}
	if left.GetTarget().GetKnowledgeObjectId() > right.GetTarget().GetKnowledgeObjectId() {
		return 1
	}
	if left.GetTarget().GetVersion() < right.GetTarget().GetVersion() {
		return -1
	}
	if left.GetTarget().GetVersion() > right.GetTarget().GetVersion() {
		return 1
	}
	if left.GetRole() < right.GetRole() {
		return -1
	}
	if left.GetRole() > right.GetRole() {
		return 1
	}
	return 0
}

func rejectUnknownFields(ctx context.Context, message protoreflect.Message, depth int) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if !message.IsValid() || depth > 64 || len(message.GetUnknown()) != 0 {
		return ErrInvariant
	}
	fields := message.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		if index%32 == 0 {
			if err := contextError(ctx); err != nil {
				return err
			}
		}
		field := fields.Get(index)
		if !message.Has(field) {
			continue
		}
		value := message.Get(field)
		if field.IsMap() {
			if field.MapValue().Kind() != protoreflect.MessageKind && field.MapValue().Kind() != protoreflect.GroupKind {
				continue
			}
			var nestedErr error
			value.Map().Range(func(_ protoreflect.MapKey, mapValue protoreflect.Value) bool {
				nestedErr = rejectUnknownFields(ctx, mapValue.Message(), depth+1)
				return nestedErr == nil
			})
			if nestedErr != nil {
				return nestedErr
			}
			continue
		}
		if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
			continue
		}
		if field.IsList() {
			list := value.List()
			for listIndex := 0; listIndex < list.Len(); listIndex++ {
				if err := rejectUnknownFields(ctx, list.Get(listIndex).Message(), depth+1); err != nil {
					return err
				}
			}
			continue
		}
		if err := rejectUnknownFields(ctx, value.Message(), depth+1); err != nil {
			return err
		}
	}
	return nil
}

func issueDescriptorContract() error {
	checks := []struct {
		message protoreflect.MessageDescriptor
		fields  []fieldContract
	}{
		{(&opensplunk.FieldViolation{}).ProtoReflect().Descriptor(), []fieldContract{{"field_path", 1}, {"code", 2}, {"message", 3}}},
		{(&opensplunk.KnowledgeValidationDiagnostic{}).ProtoReflect().Descriptor(), []fieldContract{{"field_path", 1}, {"diagnostic", 2}}},
		{(&opensplunk.Diagnostic{}).ProtoReflect().Descriptor(), []fieldContract{{"code", 1}, {"severity", 2}, {"message", 3}, {"source_range", 4}, {"suggestions", 5}}},
		{(&opensplunk.SourceRange{}).ProtoReflect().Descriptor(), []fieldContract{{"start", 1}, {"end", 2}}},
		{(&opensplunk.SourcePosition{}).ProtoReflect().Descriptor(), []fieldContract{{"byte_offset", 1}, {"line", 2}, {"column", 3}}},
	}
	for _, check := range checks {
		if check.message == nil || check.message.Fields().Len() != len(check.fields) {
			return ErrInvariant
		}
		for index, expected := range check.fields {
			field := check.message.Fields().Get(index)
			if string(field.Name()) != expected.name || field.Number() != expected.number {
				return ErrInvariant
			}
		}
	}
	return nil
}

type fieldContract struct {
	name   string
	number protoreflect.FieldNumber
}

func validObjectType(value opensplunk.KnowledgeObjectType, allowUnspecified bool) bool {
	if allowUnspecified && value == opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED {
		return true
	}
	return slices.Contains([]opensplunk.KnowledgeObjectType{
		opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
		opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
		opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
	}, value)
}
