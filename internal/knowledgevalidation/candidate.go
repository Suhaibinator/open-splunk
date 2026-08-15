package knowledgevalidation

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"google.golang.org/protobuf/proto"
)

const (
	syntheticCandidateID = "knowledge-validation-candidate"
	syntheticOwnerID     = "knowledge-validation-owner"
)

// BuildInactive normalizes a candidate for bounded inactive persistence. It
// deliberately performs no publication compilation or dependency derivation.
func BuildInactive(ctx context.Context, submitted *opensplunkv1.KnowledgeObjectDefinition) (Result, error) {
	if err := contextError(ctx); err != nil {
		return Result{}, err
	}
	if submitted == nil {
		return Result{}, ErrEnvelope
	}
	normalized, err := knowledgedefinition.Normalize(submitted)
	if err != nil {
		if issue, ok := knowledgedefinition.IssueFromError(err); ok {
			return invalidDefinitionResult(ctx, objectTypeFromDefinition(submitted), issue)
		}
		return Result{}, ErrInvariant
	}
	if err := contextError(ctx); err != nil {
		return Result{}, err
	}
	patterns := normalized.Selector.Stats().Patterns
	if patterns > math.MaxUint32 {
		return Result{}, ErrInvariant
	}
	result := &opensplunkv1.KnowledgeValidationResult{
		Valid:                true,
		ObjectType:           normalized.ObjectType,
		NormalizedDefinition: normalized.Definition,
		DefinitionSha256:     bytes.Clone(normalized.Digest[:]),
		Resources: &opensplunkv1.KnowledgeResourceEstimate{
			SelectorPatterns:          uint32(patterns),
			NormalizedDefinitionBytes: uint64(len(normalized.Bytes)),
		},
	}
	return newResult(ctx, resultKindInactive, result, nil)
}

// PrepareActive normalizes and singleton-compiles only the submitted
// candidate. It never compiles a catalog cohort and never classifies catalog or
// transition failures.
func PrepareActive(ctx context.Context, submitted *opensplunkv1.KnowledgeObjectDefinition) (ActivePreparation, error) {
	if err := contextError(ctx); err != nil {
		return ActivePreparation{}, err
	}
	if submitted == nil {
		return ActivePreparation{}, ErrEnvelope
	}
	normalized, err := knowledgedefinition.Normalize(submitted)
	if err != nil {
		if issue, ok := knowledgedefinition.IssueFromError(err); ok {
			invalid, buildErr := invalidDefinitionResult(ctx, objectTypeFromDefinition(submitted), issue)
			if buildErr != nil {
				return ActivePreparation{}, buildErr
			}
			return ActivePreparation{state: &activePreparationState{invalid: invalid.state}}, nil
		}
		return ActivePreparation{}, ErrInvariant
	}
	if err := contextError(ctx); err != nil {
		return ActivePreparation{}, err
	}

	program, compileErr := compileSingleton(normalized)
	if err := contextError(ctx); err != nil {
		return ActivePreparation{}, err
	}
	if compileErr != nil {
		issue, candidateIssue := knowledgeprogram.CandidateIssueFromError(compileErr, 0)
		if !candidateIssue {
			return ActivePreparation{}, ErrInvariant
		}
		var detachedSubmitted *opensplunkv1.KnowledgeObjectDefinition
		if issue.Range != nil {
			var ok bool
			detachedSubmitted, ok = proto.Clone(submitted).(*opensplunkv1.KnowledgeObjectDefinition)
			if !ok || detachedSubmitted == nil {
				return ActivePreparation{}, ErrInvariant
			}
			// A ranged issue is relative to the canonical scalar. Re-normalizing
			// this detached raw source proves it belongs to the compiled
			// canonical authority before public range rebasing.
			detachedNormalized, normalizeErr := knowledgedefinition.Normalize(detachedSubmitted)
			if normalizeErr != nil || !bytes.Equal(detachedNormalized.Bytes, normalized.Bytes) ||
				detachedNormalized.Digest != normalized.Digest {
				return ActivePreparation{}, ErrInvariant
			}
			normalized = detachedNormalized
			if err := contextError(ctx); err != nil {
				return ActivePreparation{}, err
			}
		}
		invalid, buildErr := invalidProgramResult(ctx, detachedSubmitted, normalized.Definition, normalized.ObjectType, issue)
		if buildErr != nil {
			return ActivePreparation{}, buildErr
		}
		return ActivePreparation{state: &activePreparationState{invalid: invalid.state}}, nil
	}
	if program.IsZero() || program.ObjectCount() != 1 || len(program.Dependencies()) != 0 {
		return ActivePreparation{}, ErrInvariant
	}
	patterns := normalized.Selector.Stats().Patterns
	if patterns > math.MaxUint32 {
		return ActivePreparation{}, ErrInvariant
	}
	state := &activeCandidateState{
		normalized:      normalized.Definition,
		normalizedBytes: uint64(len(normalized.Bytes)),
		digest:          normalized.Digest,
		objectType:      normalized.ObjectType,
		patterns:        uint32(patterns),
		charges:         intrinsicChargesFromProgram(program.Charges()),
	}
	return ActivePreparation{state: &activePreparationState{candidate: state}}, nil
}

func compileSingleton(normalized knowledgedefinition.Normalized) (knowledgeprogram.Program, error) {
	stage, _, ok := knowledgedefinition.StageForObjectType(normalized.ObjectType)
	if !ok || normalized.Definition == nil {
		return knowledgeprogram.Program{}, ErrInvariant
	}
	object := &opensplunkv1.KnowledgeSnapshotObject{
		ResolutionOrdinal: 0,
		Stage:             stage,
		StageOrdinal:      0,
		KnowledgeObjectId: syntheticCandidateID,
		Version:           1,
		ObjectType:        normalized.ObjectType,
		Name:              strings.Clone(normalized.Name),
		AppId:             strings.Clone(normalized.AppID),
		OwnerId:           syntheticOwnerID,
		SharingScope:      normalized.SharingScope,
		Definition:        normalized.Definition,
		DefinitionSha256:  bytes.Clone(normalized.Digest[:]),
	}
	return knowledgeprogram.Compile([]*opensplunkv1.KnowledgeSnapshotObject{object})
}

// BuildDependencyUnavailable creates the only closed transition-adjacent
// candidate diagnostic owned by this package. No target identity enters this
// method, so missing and unauthorized targets remain indistinguishable.
func (candidate ActiveCandidate) BuildDependencyUnavailable(ctx context.Context) (Result, error) {
	if err := contextError(ctx); err != nil {
		return Result{}, err
	}
	if candidate.state == nil {
		return Result{}, ErrInvariant
	}
	diagnostics, sources, truncated, err := canonicalDiagnosticsWithSources(ctx, []diagnosticIssue{{
		code:     "KNOWLEDGE_DEPENDENCY_UNAVAILABLE",
		severity: opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
		message:  "one or more candidate dependencies are unavailable",
	}})
	if err != nil || truncated || len(diagnostics) != 1 {
		if err != nil {
			return Result{}, err
		}
		return Result{}, ErrInvariant
	}
	return newResult(ctx, resultKindInvalid, &opensplunkv1.KnowledgeValidationResult{
		ObjectType:  candidate.state.objectType,
		Diagnostics: diagnostics,
	}, sources)
}

func invalidDefinitionResult(ctx context.Context, objectType opensplunkv1.KnowledgeObjectType, issue knowledgedefinition.Issue) (Result, error) {
	message := ""
	switch issue.Code {
	case knowledgedefinition.IssueCodeInvalidDefinition:
		message = "candidate definition field is invalid"
	case knowledgedefinition.IssueCodeUnknownField:
		message = "candidate definition contains an unknown protobuf field"
	case knowledgedefinition.IssueCodeResourceLimit:
		message = "candidate definition exceeds a resource limit"
	default:
		return Result{}, ErrInvariant
	}
	violations, truncated, err := canonicalFieldViolations(ctx, []fieldIssue{{
		path: strings.Clone(issue.FieldPath), code: string(issue.Code), message: message,
	}})
	if err != nil || truncated || len(violations) != 1 {
		if err != nil {
			return Result{}, err
		}
		return Result{}, ErrInvariant
	}
	return newResult(ctx, resultKindInvalid, &opensplunkv1.KnowledgeValidationResult{
		ObjectType:      objectType,
		FieldViolations: violations,
	}, nil)
}

func invalidProgramResult(
	ctx context.Context,
	submitted, normalized *opensplunkv1.KnowledgeObjectDefinition,
	objectType opensplunkv1.KnowledgeObjectType,
	issue knowledgeprogram.Issue,
) (Result, error) {
	if !allowedProgramIssue(issue, normalized) {
		return Result{}, ErrInvariant
	}
	projected := diagnosticIssue{
		path:        strings.Clone(issue.FieldPath),
		code:        string(issue.Code),
		severity:    opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
		message:     strings.Clone(issue.Message),
		suggestions: append([]string(nil), issue.Suggestions...),
	}
	if issue.Range != nil {
		rebased, err := rebaseProgramRange(submitted, normalized, issue)
		if err != nil {
			return Result{}, err
		}
		projected.rangeValue = rebased
	}
	diagnostics, sources, truncated, err := canonicalDiagnosticsWithSources(ctx, []diagnosticIssue{projected})
	if err != nil || truncated || len(diagnostics) != 1 {
		if err != nil {
			return Result{}, err
		}
		return Result{}, ErrInvariant
	}
	return newResult(ctx, resultKindInvalid, &opensplunkv1.KnowledgeValidationResult{
		ObjectType:  objectType,
		Diagnostics: diagnostics,
	}, sources)
}

func allowedProgramIssue(issue knowledgeprogram.Issue, normalized *opensplunkv1.KnowledgeObjectDefinition) bool {
	if issue.FieldPath == "" || issue.Message == "" {
		return false
	}
	switch issue.Code {
	case knowledgeprogram.IssueCodeRegexInvalid, knowledgeprogram.IssueCodeRegexResourceLimit:
		return normalizedRegexDefinition(normalized) != nil &&
			issue.FieldPath == "field_extraction.regex.pattern" && issue.Range == nil && len(issue.Suggestions) == 0
	case knowledgeprogram.IssueCodeRegexCaptureMismatch:
		regex := normalizedRegexDefinition(normalized)
		return regex != nil && validRegexCaptureIssuePath(issue.FieldPath, len(regex.GetOutputFields())) &&
			issue.Range == nil && len(issue.Suggestions) == 0
	case knowledgeprogram.IssueCodeJSONPathInvalid,
		knowledgeprogram.IssueCodeJSONPathUnsupported,
		knowledgeprogram.IssueCodeJSONPathResourceLimit:
		return normalizedJSONDefinition(normalized) != nil && issue.FieldPath == "field_extraction.json.path" &&
			issue.Range != nil && len(issue.Suggestions) == 0
	case knowledgeprogram.IssueCodeCalculatedBoolean:
		if issue.FieldPath != "calculated_field.expression" || issue.Range == nil || len(issue.Suggestions) != 0 ||
			normalized == nil || normalized.GetCalculatedField() == nil {
			return false
		}
		return issue.Range.StartByteOffset == 0 &&
			issue.Range.EndByteOffset == uint32(len(normalized.GetCalculatedField().GetExpression())) // #nosec G115 -- normalized expressions are bounded far below MaxUint32.
	default:
		return normalized != nil && normalized.GetCalculatedField() != nil &&
			issue.FieldPath == "calculated_field.expression" && issue.Range != nil &&
			strings.HasPrefix(string(issue.Code), "SPL_") && len(issue.Code) > len("SPL_")
	}
}

func validRegexCaptureIssuePath(value string, outputCount int) bool {
	if value == "field_extraction.regex.pattern" || value == "field_extraction.regex.output_fields" {
		return true
	}
	const prefix = "field_extraction.regex.output_fields["
	if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, "]") {
		return false
	}
	index := value[len(prefix) : len(value)-1]
	if index == "" || len(index) > 1 && index[0] == '0' {
		return false
	}
	for _, character := range index {
		if character < '0' || character > '9' {
			return false
		}
	}
	parsed, err := strconv.Atoi(index)
	return err == nil && parsed >= 0 && parsed < outputCount
}

func normalizedRegexDefinition(definition *opensplunkv1.KnowledgeObjectDefinition) *opensplunkv1.RegexFieldExtractionDefinition {
	if definition == nil || definition.GetFieldExtraction() == nil {
		return nil
	}
	return definition.GetFieldExtraction().GetRegex()
}

func normalizedJSONDefinition(definition *opensplunkv1.KnowledgeObjectDefinition) *opensplunkv1.JsonFieldExtractionDefinition {
	if definition == nil || definition.GetFieldExtraction() == nil {
		return nil
	}
	return definition.GetFieldExtraction().GetJson()
}

func rebaseProgramRange(
	submitted, normalized *opensplunkv1.KnowledgeObjectDefinition,
	issue knowledgeprogram.Issue,
) (*byteRange, error) {
	if submitted == nil || normalized == nil || issue.Range == nil {
		return nil, ErrInvariant
	}
	start, end := uint64(issue.Range.StartByteOffset), uint64(issue.Range.EndByteOffset)
	var raw, canonical string
	switch issue.FieldPath {
	case "calculated_field.expression":
		if submitted.GetCalculatedField() == nil || normalized.GetCalculatedField() == nil {
			return nil, ErrInvariant
		}
		raw = submitted.GetCalculatedField().GetExpression()
		canonical = normalized.GetCalculatedField().GetExpression()
		left, right := asciiTrimBounds(raw)
		if left > right || raw[left:right] != canonical || start > end || end > uint64(len(canonical)) ||
			!utf8.ValidString(canonical[:start]) || !utf8.ValidString(canonical[:end]) {
			return nil, ErrInvariant
		}
		if start == end && end == uint64(len(canonical)) {
			start, end = uint64(len(raw)), uint64(len(raw))
		} else {
			trimmedPrefixBytes := uint64(left) // #nosec G115 -- left is a nonnegative index into raw.
			start += trimmedPrefixBytes
			end += trimmedPrefixBytes
		}
	case "field_extraction.json.path":
		if submitted.GetFieldExtraction() == nil || submitted.GetFieldExtraction().GetJson() == nil ||
			normalized.GetFieldExtraction() == nil || normalized.GetFieldExtraction().GetJson() == nil {
			return nil, ErrInvariant
		}
		raw = submitted.GetFieldExtraction().GetJson().GetPath()
		canonical = normalized.GetFieldExtraction().GetJson().GetPath()
		if raw != canonical || start > end || end > uint64(len(canonical)) ||
			!utf8.ValidString(canonical[:start]) || !utf8.ValidString(canonical[:end]) {
			return nil, ErrInvariant
		}
	default:
		return nil, ErrInvariant
	}
	if start > end || end > uint64(len(raw)) || !utf8.ValidString(raw) ||
		!utf8.ValidString(raw[:start]) || !utf8.ValidString(raw[:end]) {
		return nil, ErrInvariant
	}
	return &byteRange{start: start, end: end, source: strings.Clone(raw)}, nil
}

func asciiTrimBounds(value string) (int, int) {
	left := 0
	for left < len(value) && asciiWhitespace(value[left]) {
		left++
	}
	right := len(value)
	for right > left && asciiWhitespace(value[right-1]) {
		right--
	}
	return left, right
}

func asciiWhitespace(value byte) bool {
	return value == ' ' || value >= '\t' && value <= '\r'
}

func objectTypeFromDefinition(definition *opensplunkv1.KnowledgeObjectDefinition) opensplunkv1.KnowledgeObjectType {
	if definition == nil {
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED
	}
	switch definition.GetBody().(type) {
	case *opensplunkv1.KnowledgeObjectDefinition_FieldExtraction:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION
	case *opensplunkv1.KnowledgeObjectDefinition_FieldAlias:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS
	case *opensplunkv1.KnowledgeObjectDefinition_CalculatedField:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD
	default:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED
	}
}
