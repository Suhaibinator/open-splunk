package knowledgevalidation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
)

func TestSealValidateResponseRevisionAndDeterministicLimit(t *testing.T) {
	result, err := BuildInactive(context.Background(), aliasDefinition("alias-seal"))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := SealValidateResponse(context.Background(), result, math.MaxInt64)
	if err != nil {
		t.Fatalf("SealValidateResponse(MaxInt64): %v", err)
	}
	wire := sealed.DeterministicBytes()
	if len(wire) == 0 {
		t.Fatal("sealed response wire is empty")
	}
	again, err := SealValidateResponse(context.Background(), result, math.MaxInt64)
	if err != nil || !bytes.Equal(wire, again.DeterministicBytes()) {
		t.Fatalf("deterministic reseal = %x / %v", again.DeterministicBytes(), err)
	}
	if _, err := SealValidateResponse(context.Background(), result, uint64(math.MaxInt64)+1); !errors.Is(err, ErrInvariant) {
		t.Fatalf("revision MaxInt64+1 error = %v", err)
	}
	if _, err := sealValidateResponse(context.Background(), result, math.MaxInt64, len(wire)); err != nil {
		t.Fatalf("exact response bound: %v", err)
	}
	if _, err := sealValidateResponse(context.Background(), result, math.MaxInt64, len(wire)-1); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("response bound -1 error = %v", err)
	}

	wire[0] ^= 0xff
	if bytes.Equal(wire, sealed.DeterministicBytes()) {
		t.Fatal("DeterministicBytes aliases retained wire")
	}
	first, err := sealed.Proto(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first.Result.NormalizedDefinition.Name = "mutated"
	second, err := sealed.Proto(context.Background())
	if err != nil || second.GetResult().GetNormalizedDefinition().GetName() != "alias-seal" {
		t.Fatalf("sealed Proto aliases prior projection: %+v/%v", second, err)
	}
}

func TestResultKindAndChargeTamperingFailBeforeProjection(t *testing.T) {
	inactive, err := BuildInactive(context.Background(), aliasDefinition("alias-kind"))
	if err != nil {
		t.Fatal(err)
	}
	inactive.state.kind = resultKindActive
	if _, err := inactive.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("inactive-as-active error = %v", err)
	}

	candidate := mustActiveCandidate(t, regexDefinition("regex-tamper", `(?P<value>x)`, "value"))
	active, err := candidate.BuildValid(context.Background(), ActivePublication{
		Candidate: ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	active.state.value.Resources.ExtractionOutputs++
	if _, err := active.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("charge drift error = %v", err)
	}

	invalid, err := candidate.BuildDependencyUnavailable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	invalid.state.kind = resultKindInactive
	if _, err := invalid.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("invalid-as-inactive error = %v", err)
	}
}

func TestTruncationFlagsRequireRetainedOmissionProof(t *testing.T) {
	valid, err := BuildInactive(context.Background(), aliasDefinition("alias-truncation"))
	if err != nil {
		t.Fatal(err)
	}
	valid.state.value.FieldViolationsTruncated = true
	if _, err := valid.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("valid field truncation tamper error = %v", err)
	}

	valid, err = BuildInactive(context.Background(), aliasDefinition("alias-diagnostic-truncation"))
	if err != nil {
		t.Fatal(err)
	}
	valid.state.value.DiagnosticsTruncated = true
	if _, err := valid.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("valid diagnostic truncation tamper error = %v", err)
	}

	candidate := mustActiveCandidate(t, aliasDefinition("alias-invalid-truncation"))
	invalid, err := candidate.BuildDependencyUnavailable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	invalid.state.value.DiagnosticsTruncated = true
	if _, err := invalid.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("invalid diagnostic truncation tamper error = %v", err)
	}

	definitionInvalid, err := BuildInactive(context.Background(), &opensplunkv1.KnowledgeObjectDefinition{})
	if err != nil {
		t.Fatal(err)
	}
	definitionInvalid.state.value.FieldViolationsTruncated = true
	if _, err := definitionInvalid.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("invalid field truncation tamper error = %v", err)
	}
}

func TestDiagnosticSourceProvenanceRejectsRangeTampering(t *testing.T) {
	raw := " \n\tcoalesce(\"😀\", mystery(host)) \r\n"
	newInvalid := func(t *testing.T) Result {
		t.Helper()
		preparation, err := PrepareActive(context.Background(), calculatedDefinition("calculated-provenance", raw))
		if err != nil {
			t.Fatal(err)
		}
		invalid, ok := preparation.Invalid()
		if !ok {
			t.Fatal("candidate unexpectedly compiled")
		}
		return invalid
	}

	t.Run("past scalar", func(t *testing.T) {
		result := newInvalid(t)
		sourceRange := result.state.value.Diagnostics[0].Diagnostic.SourceRange
		sourceRange.End.ByteOffset = uint64(len(raw) + 1)
		if _, err := result.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
			t.Fatalf("past-scalar range error = %v", err)
		}
	})

	t.Run("mid rune", func(t *testing.T) {
		result := newInvalid(t)
		sourceRange := result.state.value.Diagnostics[0].Diagnostic.SourceRange
		middle := uint64(bytes.Index([]byte(raw), []byte("😀")) + 1)
		sourceRange.Start.ByteOffset = middle
		if _, err := result.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
			t.Fatalf("mid-rune range error = %v", err)
		}
	})

	t.Run("forged coordinates", func(t *testing.T) {
		result := newInvalid(t)
		result.state.value.Diagnostics[0].Diagnostic.SourceRange.Start.Line++
		if _, err := result.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
			t.Fatalf("forged line error = %v", err)
		}
	})

	t.Run("relabelled field path", func(t *testing.T) {
		result := newInvalid(t)
		result.state.value.Diagnostics[0].FieldPath = "field_extraction.json.path"
		if _, err := result.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
			t.Fatalf("relabelled path error = %v", err)
		}
	})

	t.Run("missing sidecar", func(t *testing.T) {
		result := newInvalid(t)
		result.state.diagnosticSources = nil
		if _, err := result.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
			t.Fatalf("missing source sidecar error = %v", err)
		}
	})
}

func TestSealDoesNotSanitizeMalformedAbsentSourceSidecar(t *testing.T) {
	candidate := mustActiveCandidate(t, aliasDefinition("alias-sidecar-tamper"))
	result, err := candidate.BuildDependencyUnavailable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.state.diagnosticSources) != 1 || result.state.diagnosticSources[0].present {
		t.Fatalf("dependency diagnostic sidecar = %+v", result.state.diagnosticSources)
	}
	result.state.diagnosticSources[0].fieldPath = "hidden.path"
	result.state.diagnosticSources[0].value = "hidden value"
	if _, err := SealValidateResponse(context.Background(), result, 1); !errors.Is(err, ErrInvariant) {
		t.Fatalf("malformed absent sidecar seal error = %v", err)
	}
}

func TestActiveSealNeverRemapsSingletonCompileFailure(t *testing.T) {
	candidate := mustActiveCandidate(t, calculatedDefinition("calculated-tamper", "lower(host)"))
	result, err := candidate.BuildValid(context.Background(), ActivePublication{
		Candidate: ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	result.state.value.NormalizedDefinition.GetCalculatedField().Expression = "mystery(host)"
	normalized, err := knowledgedefinition.Normalize(result.state.value.NormalizedDefinition)
	if err != nil {
		t.Fatal(err)
	}
	result.state.value.NormalizedDefinition = normalized.Definition
	result.state.value.DefinitionSha256 = append([]byte(nil), normalized.Digest[:]...)
	result.state.value.Resources.NormalizedDefinitionBytes = uint64(len(normalized.Bytes))
	if _, err := result.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("singleton compile tamper error = %v", err)
	}
	if !result.state.value.GetValid() || len(result.state.value.GetDiagnostics()) != 0 {
		t.Fatal("seal remapped a singleton compile failure in-band")
	}
}

func TestRecursiveUnknownOutputFailsClosed(t *testing.T) {
	result, err := BuildInactive(context.Background(), aliasDefinition("alias-unknown"))
	if err != nil {
		t.Fatal(err)
	}
	unknown := []byte{0xa0, 0x06, 0x01}
	result.state.value.Resources.ProtoReflect().SetUnknown(unknown)
	if _, err := result.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("nested resource unknown error = %v", err)
	}

	result, err = BuildInactive(context.Background(), aliasDefinition("alias-unknown-definition"))
	if err != nil {
		t.Fatal(err)
	}
	result.state.value.NormalizedDefinition.GetFieldAlias().ProtoReflect().SetUnknown(unknown)
	if _, err := SealValidateResponse(context.Background(), result, 1); !errors.Is(err, ErrInvariant) {
		t.Fatalf("nested definition unknown error = %v", err)
	}
}

func TestDigestTamperingFailsClosed(t *testing.T) {
	result, err := BuildInactive(context.Background(), aliasDefinition("alias-digest"))
	if err != nil {
		t.Fatal(err)
	}
	result.state.value.DefinitionSha256 = make([]byte, sha256.Size)
	if _, err := result.Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("digest tamper error = %v", err)
	}
}

func TestSealedWireMatchesReturnedProto(t *testing.T) {
	result, err := BuildInactive(context.Background(), aliasDefinition("alias-wire"))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := SealValidateResponse(context.Background(), result, 7)
	if err != nil {
		t.Fatal(err)
	}
	message, err := sealed.Proto(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil || !bytes.Equal(wire, sealed.DeterministicBytes()) {
		t.Fatalf("sealed wire mismatch: %v", err)
	}
}
