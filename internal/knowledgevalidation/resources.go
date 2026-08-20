package knowledgevalidation

import (
	"cmp"
	"context"
	"math"
	"slices"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

const maximumObjectIDBytes = 128

type intrinsicCharges struct {
	generatedOperators    uint32
	generatedFields       uint32
	regexPrograms         uint32
	regexWorkUnits        uint64
	extractionOutputs     uint32
	jsonEvaluationWork    uint32
	scalarExpressions     uint32
	scalarExpressionNodes uint32
	scalarPredicates      uint32
}

func intrinsicChargesFromProgram(charges knowledgeprogram.Charges) intrinsicCharges {
	return intrinsicCharges{
		generatedOperators:    charges.GeneratedOperators,
		generatedFields:       charges.GeneratedFields,
		regexPrograms:         charges.RegexPrograms,
		regexWorkUnits:        charges.RegexWorkUnits,
		extractionOutputs:     charges.ExtractionOutputs,
		jsonEvaluationWork:    charges.JSONEvaluationWork,
		scalarExpressions:     charges.ScalarExpressions,
		scalarExpressionNodes: charges.ScalarExpressionNodes,
		scalarPredicates:      charges.ScalarPredicates,
	}
}

// BuildValid constructs a complete ACTIVE_PUBLICATION result. The caller must
// already have proven the supplied target projection complete and authorized.
func (candidate ActiveCandidate) BuildValid(ctx context.Context, publication ActivePublication) (Result, error) {
	if err := contextError(ctx); err != nil {
		return Result{}, err
	}
	if candidate.state == nil || !validIdentity(publication.Candidate.KnowledgeObjectID, maximumObjectIDBytes) ||
		publication.Candidate.Version == 0 || publication.Candidate.Version > math.MaxInt64 {
		return Result{}, ErrInvariant
	}
	dependencies, nodes, err := canonicalDependencies(ctx, publication)
	if err != nil {
		return Result{}, err
	}
	charges := candidate.state.charges
	resources := &opensplunk.KnowledgeResourceEstimate{
		SelectorPatterns:          candidate.state.patterns,
		NormalizedDefinitionBytes: candidate.state.normalizedBytes,
		DependencyNodes:           nodes,
		DependencyEdges:           uint32(len(dependencies)), // #nosec G115 -- dependencies are bounded by MaximumDependencies.
		GeneratedOperators:        charges.generatedOperators,
		GeneratedFields:           charges.generatedFields,
		RegexPrograms:             charges.regexPrograms,
		EstimatedRegexWorkUnits:   charges.regexWorkUnits,
		ScalarExpressions:         charges.scalarExpressions,
		ScalarExpressionNodes:     charges.scalarExpressionNodes,
		ExtractionOutputs:         charges.extractionOutputs,
		JsonEvaluationWorkUnits:   charges.jsonEvaluationWork,
		ScalarPredicates:          charges.scalarPredicates,
	}
	result := &opensplunk.KnowledgeValidationResult{
		Valid:                true,
		ObjectType:           candidate.state.objectType,
		NormalizedDefinition: candidate.state.normalized,
		DefinitionSha256:     append([]byte(nil), candidate.state.digest[:]...),
		Resources:            resources,
		Dependencies:         dependencies,
	}
	return newResult(ctx, resultKindActive, result, nil)
}

func canonicalDependencies(ctx context.Context, publication ActivePublication) ([]*opensplunk.KnowledgeValidationDependency, uint32, error) {
	values := make([]*opensplunk.KnowledgeValidationDependency, 0, min(len(publication.Dependencies), MaximumDependencies+1))
	seen := make(map[string]struct{}, min(len(publication.Dependencies), MaximumDependencies+1))
	nodes := make(map[string]struct{}, min(len(publication.Dependencies), MaximumDependencies+1))
	for index, dependency := range publication.Dependencies {
		if index%64 == 0 {
			if err := contextError(ctx); err != nil {
				return nil, 0, err
			}
		}
		if dependency == nil || len(dependency.ProtoReflect().GetUnknown()) != 0 || dependency.GetTarget() == nil ||
			len(dependency.GetTarget().ProtoReflect().GetUnknown()) != 0 ||
			!validIdentity(dependency.GetTarget().GetKnowledgeObjectId(), maximumObjectIDBytes) ||
			dependency.GetTarget().GetVersion() == 0 || dependency.GetTarget().GetVersion() > math.MaxInt64 ||
			dependency.GetRole() != opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT ||
			dependency.GetTarget().GetKnowledgeObjectId() == publication.Candidate.KnowledgeObjectID {
			return nil, 0, ErrInvariant
		}
		key := dependency.GetTarget().GetKnowledgeObjectId() + "\x00" +
			stringUint64(dependency.GetTarget().GetVersion()) + "\x00" +
			stringUint64(uint64(dependency.GetRole())) // #nosec G115 -- role is checked against the closed nonnegative value above.
		if _, duplicate := seen[key]; duplicate {
			return nil, 0, ErrInvariant
		}
		if len(seen) == MaximumDependencies {
			return nil, 0, ErrInvariant
		}
		seen[key] = struct{}{}
		nodeKey := dependency.GetTarget().GetKnowledgeObjectId() + "\x00" + stringUint64(dependency.GetTarget().GetVersion())
		nodes[nodeKey] = struct{}{}
		values = append(values, &opensplunk.KnowledgeValidationDependency{
			Target: &opensplunk.KnowledgeManagementObjectVersionIdentity{
				KnowledgeObjectId: strings.Clone(dependency.GetTarget().GetKnowledgeObjectId()),
				Version:           dependency.GetTarget().GetVersion(),
			},
			Role: dependency.GetRole(),
		})
	}
	slices.SortFunc(values, func(left, right *opensplunk.KnowledgeValidationDependency) int {
		if order := cmp.Compare(left.GetTarget().GetKnowledgeObjectId(), right.GetTarget().GetKnowledgeObjectId()); order != 0 {
			return order
		}
		if order := cmp.Compare(left.GetTarget().GetVersion(), right.GetTarget().GetVersion()); order != 0 {
			return order
		}
		return cmp.Compare(left.GetRole(), right.GetRole())
	})
	return values, uint32(len(nodes)), nil // #nosec G115 -- dependency nodes cannot exceed MaximumDependencies.
}

func validIdentity(value string, maximum int) bool {
	return knowledge.ValidIdentity(value, maximum)
}

func stringUint64(value uint64) string {
	// A fixed-width binary key cannot collide and preserves exact values. Its
	// representation is private and is never emitted.
	var bytes [8]byte
	for index := len(bytes) - 1; index >= 0; index-- {
		bytes[index] = byte(value)
		value >>= 8
	}
	return string(bytes[:])
}
