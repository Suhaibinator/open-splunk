package knowledgesnapshot

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"google.golang.org/protobuf/proto"
)

func TestKnowledgeSnapshotAcceptanceFinalizesExactPublicCompilerAuthority(t *testing.T) {
	input := snapshotGoldenInput(t)
	authority, err := Prepare(input)
	if err != nil {
		t.Fatalf("Prepare(): %v", err)
	}
	authoritySummary := authority.Summary()
	prelude := authority.Prelude()
	compiled, _ := compileSnapshotQueryWithPrelude(
		t,
		input.TenantID,
		input.EffectiveAuthorizedIndexes,
		`index=alpha OR index=zeta | table event_id alias_field calculated_field`,
		prelude,
	)
	if !compiled.HasValidExecutionSeal() {
		t.Fatalf("Compile(nonempty) = %#v", compiled)
	}
	evidence, evidenceOK := compiled.KnowledgeSnapshotEvidenceFor(prelude)
	if !evidenceOK || evidence.TenantID() != authoritySummary.TenantID ||
		!slices.Equal(evidence.EffectiveIndexes(), authoritySummary.EffectiveAuthorizedIndexes) {
		t.Fatalf("compiler evidence = (%#v, %t)", evidence, evidenceOK)
	}

	snapshot, err := authority.Finalize(compiled)
	if err != nil || snapshot.IsZero() {
		t.Fatalf("Finalize(nonempty) = (%#v, %v)", snapshot, err)
	}
	message := snapshot.Proto()
	summary := snapshot.Summary()
	if message.GetTenantId() != authoritySummary.TenantID ||
		message.GetPrincipalId() != authoritySummary.PrincipalID ||
		message.GetAppId() != authoritySummary.AppID ||
		!slices.Equal(message.GetEffectiveAuthorizedIndexes(), authoritySummary.EffectiveAuthorizedIndexes) ||
		len(message.GetObjects()) != int(prelude.ObjectCount()) ||
		message.GetBudgetCharges().GetExecutableObjects() != prelude.ObjectCount() ||
		summary.GetRef().GetObjectCount() != prelude.ObjectCount() ||
		len(summary.GetObjects()) != len(message.GetObjects()) {
		t.Fatalf("finalized nonempty authority = message %#v summary %#v", message, summary)
	}
	if err := ValidateSummary(summary); err != nil {
		t.Fatalf("ValidateSummary(): %v", err)
	}
	digest := snapshot.Digest()
	encoded := snapshot.Encoded()
	if !snapshot.Prelude().Equal(prelude) ||
		len(encoded) == 0 ||
		!bytes.Equal(message.GetSnapshotSha256(), digest[:]) {
		t.Fatal("finalized snapshot did not retain its exact program, encoding, and digest")
	}
	facts, factsOK := snapshot.ValidateRetainedExecutionAuthority(
		authoritySummary.TenantID,
		authoritySummary.PrincipalID,
		authoritySummary.AppID,
		authoritySummary.EffectiveAuthorizedIndexes,
	)
	commitment, commitmentOK := prelude.Commitment()
	if !factsOK || !commitmentOK ||
		!facts.MatchesPreludeAuthority(
			commitment,
			prelude.ObjectCount(),
			prelude.Charges(),
		) ||
		!facts.MatchesRetainedCompilerBudget(
			evidence.GeneratedOperators(),
			evidence.GeneratedFields(),
			evidence.RegexPrograms(),
			evidence.RegexWorkUnits(),
			evidence.RegexCaptureBytes(),
			evidence.ScalarExpressions(),
			evidence.ScalarExpressionNodes(),
			evidence.GeneratedSQLBytes(),
		) {
		t.Fatalf("retained execution facts = (%#v, %t)", facts, factsOK)
	}

	cloned := snapshot.Clone()
	if !snapshot.Equal(cloned) {
		t.Fatal("detached snapshot clone does not equal its authority")
	}
	returned := cloned.Proto()
	returned.Objects[0].Name = "mutated"
	mutableEncoded := cloned.Encoded()
	mutableEncoded[0] ^= 0xff
	if !snapshot.Equal(cloned) || cloned.Proto().GetObjects()[0].GetName() == "mutated" ||
		!bytes.Equal(cloned.Encoded(), encoded) {
		t.Fatal("snapshot clone accessors alias caller-owned mutation")
	}

	tamperedSQL := compiled
	tamperedSQL.SQL += " "
	if len(compiled.Args) == 0 {
		t.Fatal("compiled nonempty authority has no bound arguments for tamper probe")
	}
	tamperedArgs := compiled
	tamperedArgs.Args = slices.Clone(tamperedArgs.Args)
	tamperedArgs.Args[len(tamperedArgs.Args)-1] = "tampered"
	tamperedOutputs := compiled
	tamperedOutputs.OutputFields = append(slices.Clone(tamperedOutputs.OutputFields), "tampered")
	for _, test := range []struct {
		name     string
		compiled clickhouse.CompiledQuery
	}{
		{name: "zero"},
		{name: "SQL", compiled: tamperedSQL},
		{name: "arguments", compiled: tamperedArgs},
		{name: "outputs", compiled: tamperedOutputs},
	} {
		t.Run("reject "+test.name, func(t *testing.T) {
			got, finalizeErr := authority.Finalize(test.compiled)
			if !got.IsZero() || !errors.Is(finalizeErr, ErrInvalidInput) {
				t.Fatalf("Finalize(%s tamper) = (%#v, %v), want zero/ErrInvalidInput", test.name, got, finalizeErr)
			}
		})
	}

	scopeMismatch := input
	scopeMismatch.TenantID = "other-tenant"
	other, err := Prepare(scopeMismatch)
	if err != nil {
		t.Fatalf("Prepare(scope mismatch): %v", err)
	}
	if got, finalizeErr := other.Finalize(compiled); !got.IsZero() ||
		!errors.Is(finalizeErr, ErrInvalidInput) {
		t.Fatalf("Finalize(scope mismatch) = (%#v, %v), want zero/ErrInvalidInput", got, finalizeErr)
	}
	if !other.Prelude().Equal(prelude) {
		t.Fatal("scope-mismatch fixture unexpectedly changed the knowledge program")
	}

	substitutionInput := snapshotGoldenInput(t)
	alias := substitutionInput.Objects[len(substitutionInput.Objects)-1]
	definition := proto.Clone(alias.Definition).(*opensplunk.KnowledgeObjectDefinition)
	definition.Name = "alias-b"
	substitutionInput.Objects[len(substitutionInput.Objects)-1] = snapshotObject(
		t,
		alias.KnowledgeObjectID,
		alias.Version,
		alias.OwnerID,
		definition,
	)
	substitution, err := Prepare(substitutionInput)
	if err != nil {
		t.Fatalf("Prepare(equal-charge substitution): %v", err)
	}
	substitutionPrelude := substitution.Prelude()
	leftCommitment, leftOK := prelude.Commitment()
	rightCommitment, rightOK := substitutionPrelude.Commitment()
	if !leftOK || !rightOK || leftCommitment == rightCommitment ||
		prelude.Charges() != substitutionPrelude.Charges() ||
		prelude.ObjectCount() != substitutionPrelude.ObjectCount() {
		t.Fatal("equal-charge substitution fixture does not isolate program commitment")
	}
	substitutionCompiled, _ := compileSnapshotQueryWithPrelude(
		t,
		substitutionInput.TenantID,
		substitutionInput.EffectiveAuthorizedIndexes,
		`index=alpha OR index=zeta | table event_id alias_field calculated_field`,
		substitutionPrelude,
	)
	if !substitutionCompiled.HasValidExecutionSeal() {
		t.Fatalf("Compile(equal-charge substitution) = %#v", substitutionCompiled)
	}
	if got, finalizeErr := authority.Finalize(substitutionCompiled); !got.IsZero() ||
		!errors.Is(finalizeErr, ErrInvalidInput) {
		t.Fatalf("Finalize(equal-charge substitution) = (%#v, %v), want zero/ErrInvalidInput", got, finalizeErr)
	}
}
