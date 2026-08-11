// Package knowledgevalidation builds bounded, candidate-only knowledge
// validation results. It deliberately owns no catalog, database, transition,
// authorization, routing, or HTTP policy.
package knowledgevalidation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

const (
	MaximumIssues                  = 256
	MaximumFieldViolationTextBytes = 256 << 10
	MaximumDiagnosticTextBytes     = 768 << 10
	MaximumDependencies            = 1024
	MaximumResponseBytes           = 8 << 20
)

var (
	// ErrEnvelope identifies a caller-owned request-envelope failure. A nil
	// definition is the only envelope concern accepted by this pure package.
	ErrEnvelope = errors.New("invalid knowledge validation envelope")
	// ErrInvariant identifies malformed, incomplete, or otherwise unsafe
	// service-produced validation authority. It must remain a non-2xx failure.
	ErrInvariant = errors.New("invalid knowledge validation authority")
	// ErrResponseTooLarge identifies a deterministic Validate response which
	// exceeds the public 8 MiB protobuf bound.
	ErrResponseTooLarge = errors.New("knowledge validation response exceeds limit")
)

// Result is an immutable validation result. Its zero value is absent. Proto
// returns a detached projection so callers cannot mutate retained authority.
type Result struct{ state *resultState }

type resultState struct {
	kind              resultKind
	value             *opensplunkv1.KnowledgeValidationResult
	diagnosticSources []diagnosticSource
}

type resultKind uint8

const (
	resultKindInvalid resultKind = iota + 1
	resultKindInactive
	resultKindActive
)

// ActivePreparation contains exactly one terminal invalid Result or one
// locally valid ActiveCandidate. Its zero value contains neither.
type ActivePreparation struct{ state *activePreparationState }

type activePreparationState struct {
	invalid   *resultState
	candidate *activeCandidateState
}

// ActiveCandidate is a normalized candidate whose singleton publication
// semantics compiled successfully. It is immutable and contains no catalog
// authority.
type ActiveCandidate struct{ state *activeCandidateState }

type activeCandidateState struct {
	normalized      *opensplunkv1.KnowledgeObjectDefinition
	normalizedBytes uint64
	digest          [sha256.Size]byte
	objectType      opensplunkv1.KnowledgeObjectType
	patterns        uint32
	charges         intrinsicCharges
}

// ExactIdentity is an evaluation-local exact object identity. It is used only
// to reject self dependencies and is never projected into a result issue.
type ExactIdentity struct {
	KnowledgeObjectID string
	Version           uint64
}

// ActivePublication contains the complete, already-authorized direct target
// projection supplied by a full ACTIVE transition. This package validates its
// shape and bounds but cannot prove catalog completeness or visibility.
type ActivePublication struct {
	Candidate    ExactIdentity
	Dependencies []*opensplunkv1.KnowledgeValidationDependency
}

// SealedValidateResponse retains one checked response and its exact
// deterministic protobuf encoding. Accessors always detach their results.
type SealedValidateResponse struct {
	response          *opensplunkv1.ValidateKnowledgeObjectResponse
	wire              []byte
	kind              resultKind
	diagnosticSources []diagnosticSource
}

// Invalid returns the terminal candidate-invalid result, when present.
func (preparation ActivePreparation) Invalid() (Result, bool) {
	if preparation.state == nil || preparation.state.invalid == nil {
		return Result{}, false
	}
	return Result{state: preparation.state.invalid}, true
}

// Candidate returns the locally valid active candidate, when present.
func (preparation ActivePreparation) Candidate() (ActiveCandidate, bool) {
	if preparation.state == nil || preparation.state.candidate == nil {
		return ActiveCandidate{}, false
	}
	return ActiveCandidate{state: preparation.state.candidate}, true
}

// Normalized returns detached normalized definition and digest authorities.
func (candidate ActiveCandidate) Normalized(ctx context.Context) (*opensplunkv1.KnowledgeObjectDefinition, [sha256.Size]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	if candidate.state == nil || candidate.state.normalized == nil {
		return nil, [sha256.Size]byte{}, ErrInvariant
	}
	cloned, ok := proto.Clone(candidate.state.normalized).(*opensplunkv1.KnowledgeObjectDefinition)
	if !ok || cloned == nil {
		return nil, [sha256.Size]byte{}, ErrInvariant
	}
	if err := contextError(ctx); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	return cloned, candidate.state.digest, nil
}

// Proto returns a detached validation-result projection.
func (result Result) Proto(ctx context.Context) (*opensplunkv1.KnowledgeValidationResult, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if result.state == nil || result.state.value == nil {
		return nil, ErrInvariant
	}
	if err := validateResult(ctx, result.state.value, result.state.kind, result.state.diagnosticSources); err != nil {
		return nil, err
	}
	cloned, ok := proto.Clone(result.state.value).(*opensplunkv1.KnowledgeValidationResult)
	if !ok || cloned == nil {
		return nil, ErrInvariant
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return cloned, nil
}

// Proto returns a detached sealed-response projection.
func (response SealedValidateResponse) Proto(ctx context.Context) (*opensplunkv1.ValidateKnowledgeObjectResponse, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if response.response == nil || response.wire == nil {
		return nil, ErrInvariant
	}
	if response.response.GetResult() == nil || response.response.GetTenantCatalogRevision() > math.MaxInt64 {
		return nil, ErrInvariant
	}
	if err := validateResult(ctx, response.response.GetResult(), response.kind, response.diagnosticSources); err != nil {
		return nil, err
	}
	if err := rejectUnknownFields(ctx, response.response.ProtoReflect(), 0); err != nil {
		return nil, err
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response.response)
	if err != nil || len(wire) > MaximumResponseBytes || !bytes.Equal(wire, response.wire) {
		return nil, ErrInvariant
	}
	cloned, ok := proto.Clone(response.response).(*opensplunkv1.ValidateKnowledgeObjectResponse)
	if !ok || cloned == nil {
		return nil, ErrInvariant
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return cloned, nil
}

// DeterministicBytes returns a detached copy of the sealed protobuf encoding.
func (response SealedValidateResponse) DeterministicBytes() []byte {
	return append([]byte(nil), response.wire...)
}

func newResult(
	ctx context.Context,
	kind resultKind,
	value *opensplunkv1.KnowledgeValidationResult,
	diagnosticSources []diagnosticSource,
) (Result, error) {
	if value == nil {
		return Result{}, ErrInvariant
	}
	cloned, ok := proto.Clone(value).(*opensplunkv1.KnowledgeValidationResult)
	if !ok || cloned == nil {
		return Result{}, ErrInvariant
	}
	sources := cloneDiagnosticSources(diagnosticSources)
	if err := validateResult(ctx, cloned, kind, sources); err != nil {
		return Result{}, err
	}
	if err := contextError(ctx); err != nil {
		return Result{}, err
	}
	return Result{state: &resultState{kind: kind, value: cloned, diagnosticSources: sources}}, nil
}

func cloneDiagnosticSources(input []diagnosticSource) []diagnosticSource {
	if input == nil {
		return nil
	}
	result := make([]diagnosticSource, len(input))
	for index, source := range input {
		result[index] = diagnosticSource{
			present:   source.present,
			fieldPath: string(append([]byte(nil), source.fieldPath...)),
			value:     string(append([]byte(nil), source.value...)),
		}
	}
	return result
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return ErrInvariant
	}
	return ctx.Err()
}
