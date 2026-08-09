package knowledgesnapshot

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestPrepareCanonicalAuthorityOrderChargesDigestAndDetachment(t *testing.T) {
	input := snapshotGoldenInput(t)
	first, err := Prepare(input)
	if err != nil {
		t.Fatalf("Prepare(): %v", err)
	}
	if first.IsZero() || first.base.GetSnapshotSha256() != nil ||
		first.base.GetBudgetCharges().GetCanonicalSnapshotBytes() != 0 ||
		first.base.GetBudgetCharges().GetGeneratedOperators() != 0 ||
		first.base.GetBudgetCharges().GetGeneratedSqlBytes() != 0 {
		t.Fatalf("unfinalized authority = %+v", first.base)
	}

	permuted := cloneSnapshotInput(input)
	slices.Reverse(permuted.Objects)
	slices.Reverse(permuted.Dependencies)
	slices.Reverse(permuted.Shadows)
	permuted.EffectiveAuthorizedIndexes = []string{"zeta", "alpha", "zeta"}
	second, err := Prepare(permuted)
	if err != nil {
		t.Fatalf("Prepare(permuted): %v", err)
	}
	if !bytes.Equal(deterministicMessage(t, first.base), deterministicMessage(t, second.base)) {
		t.Fatal("input permutation changed canonical unfinalized authority")
	}

	objects := first.base.GetObjects()
	wantIDs := []string{"ko-extraction", "ko-alias", "ko-shadow-winner", "ko-calculated"}
	wantStageOrdinals := []uint32{0, 0, 1, 0}
	if len(objects) != len(wantIDs) {
		t.Fatalf("objects = %d, want %d", len(objects), len(wantIDs))
	}
	for index, object := range objects {
		if object.GetResolutionOrdinal() != uint32(index) ||
			object.GetKnowledgeObjectId() != wantIDs[index] ||
			object.GetStageOrdinal() != wantStageOrdinals[index] {
			t.Fatalf("object %d = %+v", index, object)
		}
	}
	if got := first.Summary().EffectiveAuthorizedIndexes; !slices.Equal(got, []string{"alpha", "zeta"}) {
		t.Fatalf("effective indexes = %#v", got)
	}

	dependencies := first.base.GetDependencies()
	if len(dependencies) != 2 ||
		dependencies[0].GetCanonicalOrdinal() != 0 || dependencies[0].GetTopologicalDepth() != 1 ||
		dependencies[0].GetSource().GetKnowledgeObjectId() != "ko-alias" ||
		dependencies[1].GetCanonicalOrdinal() != 1 || dependencies[1].GetTopologicalDepth() != 2 ||
		dependencies[1].GetSource().GetKnowledgeObjectId() != "ko-calculated" {
		t.Fatalf("dependencies = %+v", dependencies)
	}
	baseCharges := first.base.GetBudgetCharges()
	if baseCharges.GetExecutableObjects() != 4 || baseCharges.GetDependencyNodes() != 4 ||
		baseCharges.GetDependencyEdges() != 2 || baseCharges.GetDependencyDepth() != 2 {
		t.Fatalf("base charges = %+v", baseCharges)
	}
	compiled, err := splregex.CompileExtractionPattern(`(?P<source_field>[a-z]+)`)
	if err != nil {
		t.Fatal(err)
	}
	wantStatic := StaticCharges{
		GeneratedFields: 4, ExtractionRegexPrograms: 1,
		ExtractionRegexWorkUnits: uint64(compiled.ProgramWorkUnits),
		ExtractionOutputs:        1,
		ScalarExpressions:        1,
		ScalarExpressionNodes:    2,
	}
	if first.StaticCharges() != wantStatic {
		t.Fatalf("static charges = %+v, want %+v", first.StaticCharges(), wantStatic)
	}
	prelude := first.Prelude()
	preludeCharges := prelude.Charges()
	if prelude.IsZero() || prelude.IsEmpty() || prelude.ObjectCount() != uint32(len(wantIDs)) ||
		preludeCharges.GeneratedOperators != 3 || preludeCharges.GeneratedFields != wantStatic.GeneratedFields ||
		preludeCharges.RegexPrograms != wantStatic.ExtractionRegexPrograms ||
		preludeCharges.RegexWorkUnits != wantStatic.ExtractionRegexWorkUnits ||
		preludeCharges.ExtractionOutputs != wantStatic.ExtractionOutputs ||
		preludeCharges.ScalarExpressions != wantStatic.ScalarExpressions ||
		preludeCharges.ScalarExpressionNodes != wantStatic.ScalarExpressionNodes ||
		!prelude.Equal(second.Prelude()) {
		t.Fatalf("prelude authority = objects:%d charges:%+v", prelude.ObjectCount(), preludeCharges)
	}
	kinds := prelude.OperatorKinds()
	if len(kinds) == 0 {
		t.Fatal("prelude operator order is absent")
	}
	kinds[0] = 0
	if first.Prelude().OperatorKinds()[0] == 0 {
		t.Fatal("prelude accessor aliases retained authority")
	}

	shadows := first.base.GetShadows()
	if len(shadows) != 2 || shadows[0].GetKnowledgeObjectId() != "ko-shadow-app" ||
		shadows[1].GetKnowledgeObjectId() != "ko-shadow-global" ||
		shadows[0].GetWinnerResolutionOrdinal() != 2 || shadows[1].GetWinnerResolutionOrdinal() != 2 {
		t.Fatalf("shadows = %+v", shadows)
	}
	for index, warning := range first.base.GetWarnings() {
		if warning.GetWarningOrdinal() != uint32(index) ||
			warning.GetKind() != opensplunkv1.KnowledgeSnapshotWarningKind_KNOWLEDGE_SNAPSHOT_WARNING_KIND_SHADOWED_OBJECT ||
			warning.ObjectResolutionOrdinal != nil || warning.ShadowOrdinal == nil ||
			warning.GetShadowOrdinal() != uint32(index) {
			t.Fatalf("warning %d = %+v", index, warning)
		}
	}

	evidence := evidenceFor(first)
	evidence.generatedSQLBytes = 2048
	firstSnapshot, err := finalize(first, evidence)
	if err != nil {
		t.Fatalf("finalize(): %v", err)
	}
	secondSnapshot, err := finalize(second, evidence)
	if err != nil {
		t.Fatalf("finalize(permuted): %v", err)
	}
	if !bytes.Equal(firstSnapshot.Encoded(), secondSnapshot.Encoded()) ||
		!proto.Equal(firstSnapshot.Proto(), secondSnapshot.Proto()) {
		t.Fatal("input permutation changed finalized snapshot")
	}
	if !firstSnapshot.Prelude().Equal(first.Prelude()) ||
		!firstSnapshot.Prelude().Equal(secondSnapshot.Prelude()) {
		t.Fatal("finalized snapshot changed prelude authority")
	}
	finalCharges := firstSnapshot.Proto().GetBudgetCharges()
	if finalCharges.GetGeneratedOperators() != preludeCharges.GeneratedOperators ||
		finalCharges.GetGeneratedFields() != 4 ||
		finalCharges.GetRegexPrograms() != 1 || finalCharges.GetRegexWorkUnits() != uint64(compiled.ProgramWorkUnits) ||
		finalCharges.GetRegexCaptureBytes() != MaximumRegexCaptureBytes ||
		finalCharges.GetScalarExpressions() != 1 || finalCharges.GetScalarExpressionNodes() != 2 ||
		finalCharges.GetGeneratedSqlBytes() != 2048 {
		t.Fatalf("final charges = %+v", finalCharges)
	}
	assertSnapshotDigest(t, firstSnapshot.Proto())
	digest := firstSnapshot.Digest()
	const wantDigest = "6d7a0758742fbec7123dfc45afffd6dbfa8784d3ecf86527a1a8e2294f5a1231"
	if firstSnapshot.CanonicalBytes() != 1272 || hex.EncodeToString(digest[:]) != wantDigest {
		t.Fatalf("nonzero golden = canonical %d digest %x, want 1272/%s", firstSnapshot.CanonicalBytes(), digest, wantDigest)
	}

	// Input, every clone-returning Authority accessor, and finalized Snapshot
	// accessors are mutually detached.
	baseBytes := deterministicMessage(t, first.base)
	input.TenantCatalogStateToken[0] ^= 0xff
	input.EffectiveAuthorizedIndexes[0] = "mutated"
	input.Objects[0].Definition.Name = "mutated"
	input.Objects[0].DefinitionSHA256[0] ^= 0xff
	input.Shadows[0].DefinitionSHA256[0] ^= 0xff
	if !bytes.Equal(baseBytes, deterministicMessage(t, first.base)) {
		t.Fatal("authority retained caller-owned input")
	}
	summary := first.Summary()
	summary.TenantCatalogStateToken[0] ^= 0xff
	summary.EffectiveAuthorizedIndexes[0] = "mutated"
	*summary.AppCatalogRevision = 99
	if first.Summary().TenantCatalogStateToken[0] != 0x5a ||
		first.Summary().EffectiveAuthorizedIndexes[0] != "alpha" ||
		first.Summary().AppCatalogRevision == nil || *first.Summary().AppCatalogRevision != 19 {
		t.Fatal("summary accessor aliases retained authority")
	}
	returnedObjects := first.Objects()
	returnedObjects[0].Definition.Name = "mutated"
	returnedObjects[0].DefinitionSHA256[0] ^= 0xff
	if first.Objects()[0].Name != "extract-a" || first.Objects()[0].Definition.GetName() != "extract-a" {
		t.Fatal("object accessor aliases retained authority")
	}
	objectSummaries := first.ObjectSummaries()
	if len(objectSummaries) != len(wantIDs) {
		t.Fatalf("object summaries = %d, want %d", len(objectSummaries), len(wantIDs))
	}
	for index, summary := range objectSummaries {
		if summary.KnowledgeObjectID != wantIDs[index] || len(summary.DefinitionSHA256) != sha256.Size {
			t.Fatalf("object summary %d = %+v", index, summary)
		}
	}
	objectSummaries[0].Name = "mutated"
	objectSummaries[0].DefinitionSHA256[0] ^= 0xff
	if first.ObjectSummaries()[0].Name != "extract-a" ||
		bytes.Equal(objectSummaries[0].DefinitionSHA256, first.ObjectSummaries()[0].DefinitionSHA256) {
		t.Fatal("object summary accessor aliases retained authority")
	}
	returnedShadows := first.Shadows()
	if len(returnedShadows) != 2 || returnedShadows[0].Definition != nil {
		t.Fatalf("retained shadow authority = %+v", returnedShadows)
	}
	returnedShadows[0].DefinitionSHA256[0] ^= 0xff
	if bytes.Equal(returnedShadows[0].DefinitionSHA256, first.Shadows()[0].DefinitionSHA256) {
		t.Fatal("shadow accessor aliases retained digest")
	}
	originalEncoded := firstSnapshot.Encoded()
	returnedProto := firstSnapshot.Proto()
	returnedProto.TenantCatalogStateToken[0] ^= 0xff
	returnedProto.Objects[0].Definition.Name = "mutated"
	if !bytes.Equal(firstSnapshot.Encoded(), originalEncoded) {
		t.Fatal("snapshot accessor aliases finalized authority")
	}
}

func TestPrepareEmptyAuthorityAbsentAndPresentRevisionGoldens(t *testing.T) {
	token := make([]byte, sha256.Size)
	for index := range token {
		token[index] = byte(index)
	}
	tests := []struct {
		name        string
		appRevision *uint64
		wantCharge  uint64
		wantDigest  string
	}{
		{name: "absent app revision", wantCharge: 91, wantDigest: "e7c2b2dc3e84fdf116fbf009ce44b7879807249aa1c34c863d68ec0046f5c711"},
		{name: "present app revision", appRevision: uint64Pointer(1), wantCharge: 93, wantDigest: "88d64cd94b61aea342df4bfdd9566fb30091d099f9e108eeff1231186d453b13"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authority, err := Prepare(Input{
				TenantID: "tenant-a", PrincipalID: "principal-a", AppID: "app-a",
				TenantCatalogRevision: 7, TenantCatalogStateToken: token,
				AppCatalogRevision:         test.appRevision,
				EffectiveAuthorizedIndexes: []string{"main", "archive"},
			})
			if err != nil {
				t.Fatalf("Prepare(): %v", err)
			}
			if authority.IsZero() || authority.StaticCharges() != (StaticCharges{}) ||
				authority.base.GetSnapshotSha256() != nil || authority.base.GetBudgetCharges().GetCanonicalSnapshotBytes() != 0 {
				t.Fatalf("empty unfinalized authority = %+v", authority.base)
			}
			if prelude := authority.Prelude(); prelude.IsZero() || !prelude.IsEmpty() || prelude.ObjectCount() != 0 {
				t.Fatalf("empty prelude = zero:%t empty:%t objects:%d", prelude.IsZero(), prelude.IsEmpty(), prelude.ObjectCount())
			}
			snapshot, err := finalize(authority, evidenceFor(authority))
			if err != nil {
				t.Fatalf("finalize(): %v", err)
			}
			digest := snapshot.Digest()
			if snapshot.CanonicalBytes() != test.wantCharge || hex.EncodeToString(digest[:]) != test.wantDigest {
				t.Fatalf("golden = charge %d digest %x, want %d/%s", snapshot.CanonicalBytes(), digest, test.wantCharge, test.wantDigest)
			}
			if !snapshot.Prelude().Equal(authority.Prelude()) {
				t.Fatal("empty finalized snapshot changed prelude authority")
			}
			assertSnapshotDigest(t, snapshot.Proto())
			impossibleEvidence := evidenceFor(authority)
			impossibleEvidence.knowledgeProgramCharges.GeneratedOperators++
			if impossible, impossibleErr := finalize(authority, impossibleEvidence); !impossible.IsZero() || !errors.Is(impossibleErr, ErrInvalidInput) {
				t.Fatalf(
					"finalize(empty with operator) = (%+v, %v), want zero/ErrInvalidInput",
					impossible,
					impossibleErr,
				)
			}
		})
	}
}

func TestFinalizeRequiresCoherentPackagePrivateCompilerEvidence(t *testing.T) {
	authority, err := Prepare(snapshotGoldenInput(t))
	if err != nil {
		t.Fatal(err)
	}
	valid := evidenceFor(authority)
	tests := []struct {
		name   string
		mutate func(*trustedCompilerEvidence)
		want   error
	}{
		{name: "program absent", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramPresent = false }, want: ErrInvalidInput},
		{name: "program commitment", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramCommitment[0] ^= 0xff }, want: ErrInvalidInput},
		{name: "program object count", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramObjects++ }, want: ErrInvalidInput},
		{name: "generated operators are exact", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramCharges.GeneratedOperators++ }, want: ErrInvalidInput},
		{name: "generated fields are exact", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramCharges.GeneratedFields++ }, want: ErrInvalidInput},
		{name: "knowledge regex programs are exact", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramCharges.RegexPrograms++ }, want: ErrInvalidInput},
		{name: "knowledge regex work is exact", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramCharges.RegexWorkUnits++ }, want: ErrInvalidInput},
		{name: "knowledge extraction outputs are exact", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramCharges.ExtractionOutputs++ }, want: ErrInvalidInput},
		{name: "knowledge JSON work is exact", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramCharges.JSONEvaluationWork++ }, want: ErrInvalidInput},
		{name: "knowledge scalar expressions are exact", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramCharges.ScalarExpressions++ }, want: ErrInvalidInput},
		{name: "knowledge scalar nodes are exact", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramCharges.ScalarExpressionNodes++ }, want: ErrInvalidInput},
		{name: "knowledge scalar predicates are exact", mutate: func(value *trustedCompilerEvidence) { value.knowledgeProgramCharges.ScalarPredicates++ }, want: ErrInvalidInput},
		{name: "equal aggregate redistribution", mutate: func(value *trustedCompilerEvidence) {
			value.knowledgeProgramCharges.RegexPrograms--
			value.knowledgeProgramCharges.RegexWorkUnits--
			value.knowledgeProgramCharges.ExtractionOutputs--
			value.authored.regexPrograms++
			value.authored.regexWorkUnits++
			value.authored.extractionOutputs++
		}, want: ErrInvalidInput},
		{name: "added authored regex omits its work", mutate: func(value *trustedCompilerEvidence) {
			value.authored.regexPrograms++
			value.authored.extractionOutputs++
		}, want: ErrInvalidInput},
		{name: "authored regex work exceeds programs", mutate: func(value *trustedCompilerEvidence) {
			value.authored.regexPrograms = 1
			value.authored.regexWorkUnits = splregex.MaximumExtractionProgramWorkUnits + 1
			value.authored.extractionOutputs = 1
		}, want: ErrInvalidInput},
		{name: "authored regex omits output", mutate: func(value *trustedCompilerEvidence) {
			value.authored.regexPrograms = 1
			value.authored.regexWorkUnits = 1
		}, want: ErrInvalidInput},
		{name: "regex capture absent", mutate: func(value *trustedCompilerEvidence) { value.regexCaptureBytes = 0 }, want: ErrInvalidInput},
		{name: "regex capture estimate", mutate: func(value *trustedCompilerEvidence) { value.regexCaptureBytes = 1024 }, want: ErrInvalidInput},
		{name: "SQL bound", mutate: func(value *trustedCompilerEvidence) { value.generatedSQLBytes = MaximumGeneratedSQLBytes + 1 }, want: ErrResourceLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			if result, got := finalize(authority, candidate); !result.IsZero() || !errors.Is(got, test.want) {
				t.Fatalf("finalize() = (%+v, %v), want zero/%v", result, got, test.want)
			}
		})
	}
	if result, err := finalize(Authority{}, trustedCompilerEvidence{}); !result.IsZero() || !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("finalize(zero) = (%+v, %v)", result, err)
	}

	firstEvidence := valid
	firstEvidence.generatedSQLBytes = 1
	first, err := finalize(authority, firstEvidence)
	if err != nil {
		t.Fatal(err)
	}
	secondEvidence := firstEvidence
	secondEvidence.generatedSQLBytes++
	second, err := finalize(authority, secondEvidence)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() == second.Digest() {
		t.Fatal("final compiler evidence did not alter snapshot digest")
	}
}

func TestTrustedCompilerEvidenceExhaustivelyMapsKnowledgeProgramCharges(t *testing.T) {
	t.Parallel()

	typeOfCharges := reflect.TypeFor[knowledgeprogram.Charges]()
	fields := make([]string, typeOfCharges.NumField())
	for index := range fields {
		fields[index] = typeOfCharges.Field(index).Name
	}
	want := []string{
		"GeneratedOperators",
		"GeneratedFields",
		"RegexPrograms",
		"RegexWorkUnits",
		"ExtractionOutputs",
		"JSONEvaluationWork",
		"ScalarExpressions",
		"ScalarExpressionNodes",
		"ScalarPredicates",
	}
	if !slices.Equal(fields, want) {
		t.Fatalf(
			"knowledgeprogram.Charges fields = %#v; update snapshot static, limit, wire, and mutation coverage for %#v",
			fields,
			want,
		)
	}
}

func TestFinalizePreservesExactKnowledgeAndAuthoredChargeSplit(t *testing.T) {
	authority, err := Prepare(snapshotGoldenInput(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence := evidenceFor(authority)
	evidence.authored = trustedAuthoredCompilerEvidence{
		regexPrograms:      1,
		regexWorkUnits:     1,
		extractionOutputs:  2,
		jsonEvaluationWork: 3,
		scalarPredicates:   4,
	}
	evidence.regexCaptureBytes = MaximumRegexCaptureBytes
	evidence.generatedSQLBytes = 99

	snapshot, err := finalize(authority, evidence)
	if err != nil {
		t.Fatalf("finalize(): %v", err)
	}
	charges := snapshot.Proto().GetBudgetCharges()
	knowledge := authority.Prelude().Charges()
	if charges.GetGeneratedOperators() != knowledge.GeneratedOperators ||
		charges.GetGeneratedFields() != knowledge.GeneratedFields ||
		charges.GetRegexPrograms() != knowledge.RegexPrograms+1 ||
		charges.GetRegexWorkUnits() != knowledge.RegexWorkUnits+1 ||
		charges.GetRegexCaptureBytes() != MaximumRegexCaptureBytes ||
		charges.GetScalarExpressions() != knowledge.ScalarExpressions ||
		charges.GetScalarExpressionNodes() != knowledge.ScalarExpressionNodes ||
		charges.GetGeneratedSqlBytes() != 99 {
		t.Fatalf("split final charges = %#v, knowledge = %+v", charges, knowledge)
	}
}

func TestFinalizeChecksEverySharedCompilerCeiling(t *testing.T) {
	authority, err := Prepare(Input{
		TenantID: "tenant-a", PrincipalID: "principal-a", AppID: "app-a",
		TenantCatalogRevision:      1,
		TenantCatalogStateToken:    bytes.Repeat([]byte{0x77}, sha256.Size),
		EffectiveAuthorizedIndexes: []string{"main"},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		exact func(*trustedCompilerEvidence)
		over  func(*trustedCompilerEvidence)
	}{
		{
			name: "regex programs",
			exact: func(value *trustedCompilerEvidence) {
				value.authored.regexPrograms = MaximumRegexPrograms
				value.authored.regexWorkUnits = uint64(MaximumRegexPrograms)
				value.authored.extractionOutputs = MaximumRegexPrograms
				value.regexCaptureBytes = MaximumRegexCaptureBytes
			},
			over: func(value *trustedCompilerEvidence) {
				value.authored.regexPrograms = MaximumRegexPrograms + 1
			},
		},
		{
			name: "regex work",
			exact: func(value *trustedCompilerEvidence) {
				value.authored.regexPrograms = MaximumRegexPrograms
				value.authored.regexWorkUnits = MaximumRegexWorkUnits
				value.authored.extractionOutputs = MaximumRegexPrograms
				value.regexCaptureBytes = MaximumRegexCaptureBytes
			},
			over: func(value *trustedCompilerEvidence) {
				value.authored.regexWorkUnits = MaximumRegexWorkUnits + 1
			},
		},
		{
			name: "extraction outputs",
			exact: func(value *trustedCompilerEvidence) {
				value.authored.extractionOutputs = MaximumExtractionOutputs
			},
			over: func(value *trustedCompilerEvidence) {
				value.authored.extractionOutputs = MaximumExtractionOutputs + 1
			},
		},
		{
			name: "JSON work",
			exact: func(value *trustedCompilerEvidence) {
				value.authored.jsonEvaluationWork = MaximumJSONEvaluationWorkUnits
			},
			over: func(value *trustedCompilerEvidence) {
				value.authored.jsonEvaluationWork = MaximumJSONEvaluationWorkUnits + 1
			},
		},
		{
			name: "scalar predicates",
			exact: func(value *trustedCompilerEvidence) {
				value.authored.scalarPredicates = MaximumScalarPredicates
			},
			over: func(value *trustedCompilerEvidence) {
				value.authored.scalarPredicates = MaximumScalarPredicates + 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exact := evidenceFor(authority)
			test.exact(&exact)
			if snapshot, exactErr := finalize(authority, exact); exactErr != nil || snapshot.IsZero() {
				t.Fatalf("exact ceiling = (%#v, %v)", snapshot, exactErr)
			}
			over := exact
			test.over(&over)
			if snapshot, overErr := finalize(authority, over); !snapshot.IsZero() ||
				!errors.Is(overErr, ErrResourceLimit) {
				t.Fatalf("over ceiling = (%#v, %v), want zero/ErrResourceLimit", snapshot, overErr)
			}
		})
	}
}

func TestPrepareShadowsUseFinalWinnerOrdinalsAcrossInterleavedLosers(t *testing.T) {
	firstWinner := aliasObject(t, "ko-winner-first", 1, "first", "app-a", "owner-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, nil)
	secondWinner := aliasObject(t, "ko-winner-second", 2, "second", "app-a", "owner-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, nil)
	firstApp := aliasObject(t, "ko-zzz-first-app", 3, "first", "app-a", "owner-app", opensplunkv1.SharingScope_SHARING_SCOPE_APP, nil)
	firstGlobal := aliasObject(t, "ko-aaa-first-global", 4, "first", "app-origin", "owner-global", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, nil)
	secondApp := aliasObject(t, "ko-aaa-second-app", 5, "second", "app-a", "owner-app", opensplunkv1.SharingScope_SHARING_SCOPE_APP, nil)
	secondGlobal := aliasObject(t, "ko-zzz-second-global", 6, "second", "app-origin", "owner-global", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, nil)
	authority, err := Prepare(Input{
		TenantID: "tenant-a", PrincipalID: "owner-a", AppID: "app-a",
		TenantCatalogStateToken:    bytes.Repeat([]byte{0x11}, sha256.Size),
		EffectiveAuthorizedIndexes: []string{"main"},
		Objects:                    []Object{secondWinner, firstWinner},
		Shadows: []Shadow{
			shadowFromObject(secondWinner, secondGlobal),
			shadowFromObject(firstWinner, firstGlobal),
			shadowFromObject(secondWinner, secondApp),
			shadowFromObject(firstWinner, firstApp),
		},
	})
	if err != nil {
		t.Fatalf("Prepare(): %v", err)
	}
	wantIDs := []string{"ko-zzz-first-app", "ko-aaa-first-global", "ko-aaa-second-app", "ko-zzz-second-global"}
	wantWinners := []uint32{0, 0, 1, 1}
	for index, shadow := range authority.base.GetShadows() {
		if shadow.GetShadowOrdinal() != uint32(index) || shadow.GetKnowledgeObjectId() != wantIDs[index] ||
			shadow.GetWinnerResolutionOrdinal() != wantWinners[index] {
			t.Fatalf("shadow %d = %+v", index, shadow)
		}
	}
	for index, shadow := range authority.Shadows() {
		if shadow.KnowledgeObjectID != wantIDs[index] || shadow.Definition != nil {
			t.Fatalf("retained shadow %d = %+v", index, shadow)
		}
	}
}

func TestPrepareCompilesEveryWinnerAndShadowSemantically(t *testing.T) {
	tests := []struct {
		name    string
		invalid func(name, app string, scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition
		valid   func(name, app string, scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition
	}{
		{
			name: "unsupported regex",
			invalid: func(name, app string, scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition {
				return regexDefinition(name, app, scope, `(?=x)(?P<out>x)`, []string{"out"})
			},
			valid: validRegexDefinition,
		},
		{
			name: "unnamed regex capture",
			invalid: func(name, app string, scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition {
				return regexDefinition(name, app, scope, `(x)(?P<out>x)`, []string{"out"})
			},
			valid: validRegexDefinition,
		},
		{
			name: "regex output mismatch",
			invalid: func(name, app string, scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition {
				return regexDefinition(name, app, scope, `(?P<actual>x)`, []string{"declared"})
			},
			valid: validRegexDefinition,
		},
		{
			name: "invalid JSON path",
			invalid: func(name, app string, scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition {
				return jsonDefinition(name, app, scope, "a..b", "out")
			},
			valid: func(name, app string, scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition {
				return jsonDefinition(name, app, scope, "a.b[0]", "out")
			},
		},
		{
			name: "invalid calculated expression",
			invalid: func(name, app string, scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition {
				return calculatedDefinition(name, app, scope, "lower(", "out", nil)
			},
			valid: func(name, app string, scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition {
				return calculatedDefinition(name, app, scope, "lower(source)", "out", nil)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name+" winner", func(t *testing.T) {
			object := snapshotObject(t, "ko-invalid", 1, "owner-a", test.invalid("object", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE))
			input := minimalInput([]Object{object})
			if _, err := Prepare(input); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("Prepare() error = %v, want ErrInvalidInput", err)
			}
		})
		t.Run(test.name+" shadow", func(t *testing.T) {
			winner := snapshotObject(t, "ko-winner", 1, "owner-a", test.valid("object", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE))
			loser := snapshotObject(t, "ko-loser", 2, "owner-global", test.invalid("object", "app-origin", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL))
			input := minimalInput([]Object{winner})
			input.Shadows = []Shadow{shadowFromObject(winner, loser)}
			if _, err := Prepare(input); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("Prepare() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestPrepareEnforcesDependencySharingFieldsSelectorsAndIsolatedNodes(t *testing.T) {
	valid := snapshotGoldenInput(t)
	authority, err := Prepare(valid)
	if err != nil {
		t.Fatalf("Prepare(valid): %v", err)
	}
	if authority.base.GetBudgetCharges().GetDependencyNodes() != 4 {
		t.Fatalf("dependency nodes = %d, want all four winners including isolated root", authority.base.GetBudgetCharges().GetDependencyNodes())
	}

	tests := []struct {
		name   string
		mutate func(*Input)
	}{
		{name: "sharing", mutate: func(input *Input) {
			rebuildObject(t, input, "ko-extraction", "owner-app", func(definition *opensplunkv1.KnowledgeObjectDefinition) {
				definition.AppId = "app-current"
				definition.SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_APP
			})
			rebuildObject(t, input, "ko-alias", "owner-global", func(definition *opensplunkv1.KnowledgeObjectDefinition) {
				definition.AppId = "app-origin"
				definition.SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL
			})
		}},
		{name: "field intersection", mutate: func(input *Input) {
			rebuildObject(t, input, "ko-alias", "owner-app", func(definition *opensplunkv1.KnowledgeObjectDefinition) {
				definition.GetFieldAlias().SourceField = "missing_field"
			})
		}},
		{name: "selector implication", mutate: func(input *Input) {
			rebuildObject(t, input, "ko-alias", "owner-app", func(definition *opensplunkv1.KnowledgeObjectDefinition) {
				definition.Selector = nil
			})
		}},
		{name: "calculated inputs", mutate: func(input *Input) {
			rebuildObject(t, input, "ko-calculated", "owner-current", func(definition *opensplunkv1.KnowledgeObjectDefinition) {
				definition.GetCalculatedField().Expression = "lower(missing_field)"
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := cloneSnapshotInput(valid)
			test.mutate(&input)
			if _, err := Prepare(input); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("Prepare() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestPrepareEnforcesAggregateWinningSemanticBounds(t *testing.T) {
	tests := []struct {
		name    string
		objects func(*testing.T) []Object
	}{
		{name: "combined extraction outputs", objects: func(t *testing.T) []Object {
			objects := make([]Object, 5)
			for index := range objects {
				outputs := make([]string, splregex.MaximumExtractionCaptureGroups)
				var pattern strings.Builder
				for capture := range outputs {
					outputs[capture] = fmt.Sprintf("f%02d", capture)
					fmt.Fprintf(&pattern, "(?P<%s>x)", outputs[capture])
				}
				name := fmt.Sprintf("regex-%02d", index)
				objects[index] = snapshotObject(t, "ko-"+name, uint64(index+1), "owner-a", regexDefinition(
					name, "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, pattern.String(), outputs,
				))
			}
			return objects
		}},
		{name: "JSON evaluation work", objects: func(t *testing.T) []Object {
			objects := make([]Object, MaximumJSONEvaluationWorkUnits/3+1)
			for index := range objects {
				name := fmt.Sprintf("json-%02d", index)
				objects[index] = snapshotObject(t, "ko-"+name, uint64(index+1), "owner-a", jsonDefinition(
					name, "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, "root", fmt.Sprintf("out_%02d", index),
				))
			}
			return objects
		}},
		{name: "scalar expressions", objects: func(t *testing.T) []Object {
			objects := make([]Object, MaximumScalarExpressions+1)
			for index := range objects {
				name := fmt.Sprintf("calculated-%02d", index)
				objects[index] = snapshotObject(t, "ko-"+name, uint64(index+1), "owner-a", calculatedDefinition(
					name, "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL,
					"lower(source)", fmt.Sprintf("out_%02d", index), nil,
				))
			}
			return objects
		}},
		{name: "scalar predicates", objects: func(t *testing.T) []Object {
			caseExpression := func(offset int) string {
				arguments := make([]string, 0, 16*2)
				for index := range 16 {
					arguments = append(arguments, "f"+strconv.Itoa(offset+index)+"=1", "1")
				}
				return "case(" + strings.Join(arguments, ",") + ")"
			}
			return []Object{
				snapshotObject(t, "ko-predicates-a", 1, "owner-a", calculatedDefinition(
					"predicates-a", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL,
					caseExpression(0), "out_a", nil,
				)),
				snapshotObject(t, "ko-predicates-b", 2, "owner-a", calculatedDefinition(
					"predicates-b", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL,
					caseExpression(16), "out_b", nil,
				)),
				snapshotObject(t, "ko-predicates-c", 3, "owner-a", calculatedDefinition(
					"predicates-c", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL,
					"if(extra=1,1,0)", "out_c", nil,
				)),
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if result, err := Prepare(minimalInput(test.objects(t))); !result.IsZero() || !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("Prepare() = (%+v, %v), want zero/ErrResourceLimit", result, err)
			}
		})
	}
}

func TestPrepareRejectsCandidateAggregateAndCanonicalSkeletonBounds(t *testing.T) {
	t.Run("candidate definitions", func(t *testing.T) {
		input := minimalInput(nil)
		description := strings.Repeat("d", 12<<10)
		for index := range MaximumExecutableObjects {
			name := fmt.Sprintf("large-%03d", index)
			winner := aliasObject(t, "ko-winner-"+name, uint64(index+1), name, "app-a", "owner-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, &description)
			appLoser := aliasObject(t, "ko-app-"+name, uint64(index+1001), name, "app-a", "owner-app", opensplunkv1.SharingScope_SHARING_SCOPE_APP, &description)
			globalLoser := aliasObject(t, "ko-global-"+name, uint64(index+2001), name, "app-origin", "owner-global", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, &description)
			input.Objects = append(input.Objects, winner)
			input.Shadows = append(input.Shadows, shadowFromObject(winner, appLoser), shadowFromObject(winner, globalLoser))
		}
		if result, err := Prepare(input); !result.IsZero() || !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("Prepare(candidate aggregate) = (%+v, %v), want zero/ErrResourceLimit", result, err)
		}
	})

	t.Run("canonical B0 skeleton", func(t *testing.T) {
		input := minimalInput(nil)
		description := strings.Repeat("d", knowledgedefinition.MaximumDescriptionBytes)
		for index := range MaximumExecutableObjects {
			name := fmt.Sprintf("canonical-%03d", index)
			input.Objects = append(input.Objects, aliasObject(
				t, "ko-"+name, uint64(index+1), name, "app-a", "owner-a",
				opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, &description,
			))
		}
		if result, err := Prepare(input); !result.IsZero() || !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("Prepare(canonical skeleton) = (%+v, %v), want zero/ErrResourceLimit", result, err)
		}
	})
}

func TestPrepareRejectsInvalidAuthoritiesAndStructuralBounds(t *testing.T) {
	valid := snapshotGoldenInput(t)
	tests := []struct {
		name   string
		mutate func(*Input)
		want   error
	}{
		{name: "state token", mutate: func(input *Input) { input.TenantCatalogStateToken = input.TenantCatalogStateToken[:31] }, want: ErrInvalidInput},
		{name: "present zero app revision", mutate: func(input *Input) { input.AppCatalogRevision = uint64Pointer(0) }, want: ErrInvalidInput},
		{name: "nil definition", mutate: func(input *Input) { input.Objects[0].Definition = nil }, want: ErrInvalidInput},
		{name: "recursive unknown definition", mutate: func(input *Input) {
			input.Objects[2].Definition.GetSelector().ProtoReflect().SetUnknown(protowire.AppendTag(nil, 99, protowire.VarintType))
		}, want: ErrInvalidInput},
		{name: "unauthorized private", mutate: func(input *Input) { input.Objects[0].OwnerID = "owner-hidden" }, want: ErrInvalidInput},
		{name: "duplicate resolved name", mutate: func(input *Input) {
			duplicate := input.Objects[1]
			duplicate.KnowledgeObjectID = "ko-duplicate"
			input.Objects[3] = duplicate
		}, want: ErrInvalidInput},
		{name: "missing dependency target", mutate: func(input *Input) { input.Dependencies[0].TargetObjectID = "ko-missing" }, want: ErrInvalidInput},
		{name: "missing derived dependency", mutate: func(input *Input) { input.Dependencies = input.Dependencies[1:] }, want: ErrInvalidInput},
		{name: "later stage dependency", mutate: func(input *Input) {
			input.Dependencies[0].SourceObjectID = "ko-extraction"
			input.Dependencies[0].SourceVersion = 4
			input.Dependencies[0].TargetObjectID = "ko-alias"
			input.Dependencies[0].TargetVersion = 5
		}, want: ErrInvalidInput},
		{name: "unsupported dependency role", mutate: func(input *Input) {
			input.Dependencies[0].Role = opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_LOOKUP_ASSET
		}, want: ErrInvalidInput},
		{name: "shadow winner", mutate: func(input *Input) { input.Shadows[0].WinnerObjectID = "ko-missing" }, want: ErrInvalidInput},
		{name: "shadow precedence", mutate: func(input *Input) { input.Shadows[0].SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE }, want: ErrInvalidInput},
		{name: "object ceiling", mutate: func(input *Input) { input.Objects = make([]Object, MaximumExecutableObjects+1) }, want: ErrResourceLimit},
		{name: "dependency ceiling", mutate: func(input *Input) { input.Dependencies = make([]Dependency, MaximumDependencyEdges+1) }, want: ErrResourceLimit},
		{name: "shadow ceiling", mutate: func(input *Input) { input.Shadows = make([]Shadow, MaximumShadows+1) }, want: ErrResourceLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := cloneSnapshotInput(valid)
			test.mutate(&input)
			if result, err := Prepare(input); !result.IsZero() || !errors.Is(err, test.want) {
				t.Fatalf("Prepare() = (%+v, %v), want zero/%v", result, err, test.want)
			}
		})
	}
}

func TestPrepareRejectsAggregateSelectorWork(t *testing.T) {
	input := minimalInput(nil)
	for index := range 2 {
		patterns := make([]*opensplunkv1.KnowledgeSelectorPattern, 16)
		for pattern := range patterns {
			patterns[pattern] = &opensplunkv1.KnowledgeSelectorPattern{Value: strings.Repeat("*?", 6) + string(rune('a'+pattern))}
		}
		name := fmt.Sprintf("work-%d", index)
		input.Objects = append(input.Objects, aliasObject(
			t, "ko-"+name, uint64(index+1), name, "app-a", "owner-a",
			opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, nil,
		))
		input.Objects[index].Definition.Selector = &opensplunkv1.KnowledgeSelector{HostPatterns: patterns}
		input.Objects[index] = snapshotObject(
			t, input.Objects[index].KnowledgeObjectID, input.Objects[index].Version,
			input.Objects[index].OwnerID, input.Objects[index].Definition,
		)
	}
	if _, err := Prepare(input); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("Prepare(selector work) error = %v, want ErrResourceLimit", err)
	}
}

func TestPrepareRejectsPossibleParallelDestinationCollisionsAndChains(t *testing.T) {
	tests := []struct {
		name          string
		leftSource    string
		leftTarget    string
		rightSource   string
		rightTarget   string
		leftSelector  string
		rightSelector string
		wantError     bool
	}{
		{
			name: "overlapping destination collision", leftSource: "left", leftTarget: "shared",
			rightSource: "right", rightTarget: "shared", wantError: true,
		},
		{
			name: "overlapping same-stage chain", leftSource: "raw", leftTarget: "intermediate",
			rightSource: "intermediate", rightTarget: "result", wantError: true,
		},
		{
			name: "literal-disjoint collision is safe", leftSource: "left", leftTarget: "shared",
			rightSource: "right", rightTarget: "shared", leftSelector: "main", rightSelector: "audit",
		},
		{
			name: "literal outside wildcard is provably disjoint", leftSource: "left", leftTarget: "shared",
			rightSource: "right", rightTarget: "shared", leftSelector: "worker", rightSelector: "api-*",
		},
		{
			name: "wildcard pair fails closed", leftSource: "left", leftTarget: "shared",
			rightSource: "right", rightTarget: "shared", leftSelector: "worker-*", rightSelector: "api-*", wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := func(name, source, destination, selector string) *opensplunkv1.KnowledgeObjectDefinition {
				var selectorMessage *opensplunkv1.KnowledgeSelector
				if selector != "" {
					selectorMessage = &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: selector}}}
				}
				return &opensplunkv1.KnowledgeObjectDefinition{
					AppId: "app-a", Name: name, SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
					Selector: selectorMessage,
					Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
						SourceField: source, DestinationField: destination,
					}},
				}
			}
			input := minimalInput([]Object{
				snapshotObject(t, "ko-left", 1, "owner-a", definition("left", test.leftSource, test.leftTarget, test.leftSelector)),
				snapshotObject(t, "ko-right", 1, "owner-a", definition("right", test.rightSource, test.rightTarget, test.rightSelector)),
			})
			authority, err := Prepare(input)
			if test.wantError {
				if !authority.IsZero() || !errors.Is(err, ErrInvalidInput) {
					t.Fatalf("Prepare() = (%+v, %v), want zero/ErrInvalidInput", authority, err)
				}
				return
			}
			if err != nil || authority.IsZero() {
				t.Fatalf("Prepare() = (%+v, %v), want nonzero/nil", authority, err)
			}
		})
	}
}

func TestPrepareParallelValidationCoversEveryExecutableBody(t *testing.T) {
	tests := []struct {
		name  string
		left  *opensplunkv1.KnowledgeObjectDefinition
		right *opensplunkv1.KnowledgeObjectDefinition
	}{
		{
			name:  "regex and JSON destination collision",
			left:  regexDefinition("regex", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, `(?P<shared>x)`, []string{"shared"}),
			right: jsonDefinition("json", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, "payload.value", "shared"),
		},
		{
			name: "calculated destination collision",
			left: calculatedDefinition(
				"calculated-left", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				"lower(left)", "shared", nil,
			),
			right: calculatedDefinition(
				"calculated-right", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				"upper(right)", "shared", nil,
			),
		},
		{
			name: "calculated same-stage chain",
			left: calculatedDefinition(
				"calculated-left", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				"lower(raw)", "intermediate", nil,
			),
			right: calculatedDefinition(
				"calculated-right", "app-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				"upper(intermediate)", "result", nil,
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := minimalInput([]Object{
				snapshotObject(t, "ko-left", 1, "owner-a", test.left),
				snapshotObject(t, "ko-right", 1, "owner-a", test.right),
			})
			if authority, err := Prepare(input); !authority.IsZero() || !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("Prepare() = (%+v, %v), want zero/ErrInvalidInput", authority, err)
			}
		})
	}
}

func snapshotGoldenInput(t *testing.T) Input {
	t.Helper()
	appRevision := uint64(19)
	alphaSelector := &opensplunkv1.KnowledgeSelector{
		IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "alpha"}},
	}
	extraction := snapshotObject(t, "ko-extraction", 4, "owner-global", &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-origin", Name: "extract-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL,
		Selector: &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "z*"}, {Value: "alpha"}}},
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw", Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{Regex: &opensplunkv1.RegexFieldExtractionDefinition{
				Pattern: `(?P<source_field>[a-z]+)`, OutputFields: []string{"source_field"},
			}},
		}},
	})
	alias := snapshotObject(t, "ko-alias", 5, "owner-app", &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-current", Name: "alias-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Selector: proto.Clone(alphaSelector).(*opensplunkv1.KnowledgeSelector),
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
			SourceField: "source_field", DestinationField: "alias_field",
		}},
	})
	shadowWinner := snapshotObject(t, "ko-shadow-winner", 6, "owner-current", &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-current", Name: "shared", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
			SourceField: "shadow_source", DestinationField: "private_shared",
		}},
	})
	calculated := snapshotObject(t, "ko-calculated", 7, "owner-current", &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-current", Name: "calculated-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Selector: proto.Clone(alphaSelector).(*opensplunkv1.KnowledgeSelector),
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			DestinationField: "calculated_field", Expression: "lower(alias_field)",
		}},
	})
	return Input{
		TenantID:                   "tenant-a",
		PrincipalID:                "owner-current",
		AppID:                      "app-current",
		TenantCatalogRevision:      23,
		TenantCatalogStateToken:    bytes.Repeat([]byte{0x5a}, sha256.Size),
		AppCatalogRevision:         &appRevision,
		EffectiveAuthorizedIndexes: []string{"zeta", "alpha", "zeta"},
		Objects:                    []Object{calculated, shadowWinner, extraction, alias},
		Dependencies: []Dependency{
			{SourceObjectID: "ko-calculated", SourceVersion: 7, TargetObjectID: "ko-alias", TargetVersion: 5, Role: opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT},
			{SourceObjectID: "ko-alias", SourceVersion: 5, TargetObjectID: "ko-extraction", TargetVersion: 4, Role: opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT},
		},
		Shadows: []Shadow{
			snapshotShadow(t, "ko-shadow-winner", 6, "ko-shadow-global", 8, "app-origin", "owner-global", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL),
			snapshotShadow(t, "ko-shadow-winner", 6, "ko-shadow-app", 9, "app-current", "owner-app", opensplunkv1.SharingScope_SHARING_SCOPE_APP),
		},
	}
}

func minimalInput(objects []Object) Input {
	return Input{
		TenantID: "tenant-a", PrincipalID: "owner-a", AppID: "app-a",
		TenantCatalogStateToken:    bytes.Repeat([]byte{0x2a}, sha256.Size),
		EffectiveAuthorizedIndexes: []string{"main"},
		Objects:                    objects,
	}
}

func snapshotObject(t *testing.T, id string, version uint64, owner string, definition *opensplunkv1.KnowledgeObjectDefinition) Object {
	t.Helper()
	normalized, err := knowledgedefinition.Normalize(definition)
	if err != nil {
		t.Fatalf("Normalize(%s): %v", id, err)
	}
	return Object{
		KnowledgeObjectID: id,
		Version:           version,
		ObjectType:        normalized.ObjectType,
		Name:              normalized.Name,
		AppID:             normalized.AppID,
		OwnerID:           owner,
		SharingScope:      normalized.SharingScope,
		Definition:        normalized.Definition,
		DefinitionSHA256:  bytes.Clone(normalized.Digest[:]),
	}
}

func aliasObject(
	t *testing.T,
	id string,
	version uint64,
	name, appID, ownerID string,
	scope opensplunkv1.SharingScope,
	description *string,
) Object {
	t.Helper()
	return snapshotObject(t, id, version, ownerID, &opensplunkv1.KnowledgeObjectDefinition{
		AppId: appID, Name: name, Description: description, SharingScope: scope,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
			SourceField: "source", DestinationField: "destination_" + name,
		}},
	})
}

func regexDefinition(
	name, appID string,
	scope opensplunkv1.SharingScope,
	pattern string,
	outputs []string,
) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: appID, Name: name, SharingScope: scope,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw", Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{Regex: &opensplunkv1.RegexFieldExtractionDefinition{
				Pattern: pattern, OutputFields: slices.Clone(outputs),
			}},
		}},
	}
}

func validRegexDefinition(name, appID string, scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition {
	return regexDefinition(name, appID, scope, `(?P<out>x)`, []string{"out"})
}

func jsonDefinition(
	name, appID string,
	scope opensplunkv1.SharingScope,
	path, output string,
) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: appID, Name: name, SharingScope: scope,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw", Extraction: &opensplunkv1.FieldExtractionDefinition_Json{Json: &opensplunkv1.JsonFieldExtractionDefinition{
				Path: path, OutputField: output,
			}},
		}},
	}
}

func calculatedDefinition(
	name, appID string,
	scope opensplunkv1.SharingScope,
	expression, output string,
	selector *opensplunkv1.KnowledgeSelector,
) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: appID, Name: name, SharingScope: scope, Selector: selector,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			DestinationField: output, Expression: expression,
		}},
	}
}

func snapshotShadow(
	t *testing.T,
	winnerID string,
	winnerVersion uint64,
	id string,
	version uint64,
	appID string,
	ownerID string,
	scope opensplunkv1.SharingScope,
) Shadow {
	t.Helper()
	object := aliasObject(t, id, version, "shared", appID, ownerID, scope, nil)
	return Shadow{
		WinnerObjectID: winnerID, WinnerVersion: winnerVersion,
		KnowledgeObjectID: object.KnowledgeObjectID, Version: object.Version,
		ObjectType: object.ObjectType, Name: object.Name, AppID: object.AppID,
		OwnerID: object.OwnerID, SharingScope: object.SharingScope,
		Definition: object.Definition, DefinitionSHA256: object.DefinitionSHA256,
	}
}

func shadowFromObject(winner, loser Object) Shadow {
	return Shadow{
		WinnerObjectID: winner.KnowledgeObjectID, WinnerVersion: winner.Version,
		KnowledgeObjectID: loser.KnowledgeObjectID, Version: loser.Version,
		ObjectType: loser.ObjectType, Name: loser.Name, AppID: loser.AppID,
		OwnerID: loser.OwnerID, SharingScope: loser.SharingScope,
		Definition: loser.Definition, DefinitionSHA256: bytes.Clone(loser.DefinitionSHA256),
	}
}

func rebuildObject(
	t *testing.T,
	input *Input,
	id, owner string,
	mutate func(*opensplunkv1.KnowledgeObjectDefinition),
) {
	t.Helper()
	for index, object := range input.Objects {
		if object.KnowledgeObjectID != id {
			continue
		}
		definition := proto.Clone(object.Definition).(*opensplunkv1.KnowledgeObjectDefinition)
		mutate(definition)
		input.Objects[index] = snapshotObject(t, id, object.Version, owner, definition)
		return
	}
	t.Fatalf("object %q not found", id)
}

func cloneSnapshotInput(input Input) Input {
	cloned := input
	cloned.TenantCatalogStateToken = bytes.Clone(input.TenantCatalogStateToken)
	cloned.EffectiveAuthorizedIndexes = slices.Clone(input.EffectiveAuthorizedIndexes)
	cloned.AppCatalogRevision = cloneUint64(input.AppCatalogRevision)
	cloned.Objects = make([]Object, len(input.Objects))
	for index := range input.Objects {
		cloned.Objects[index] = input.Objects[index]
		if input.Objects[index].Definition != nil {
			cloned.Objects[index].Definition = proto.Clone(input.Objects[index].Definition).(*opensplunkv1.KnowledgeObjectDefinition)
		}
		cloned.Objects[index].DefinitionSHA256 = bytes.Clone(input.Objects[index].DefinitionSHA256)
	}
	cloned.Dependencies = slices.Clone(input.Dependencies)
	cloned.Shadows = make([]Shadow, len(input.Shadows))
	for index := range input.Shadows {
		cloned.Shadows[index] = input.Shadows[index]
		if input.Shadows[index].Definition != nil {
			cloned.Shadows[index].Definition = proto.Clone(input.Shadows[index].Definition).(*opensplunkv1.KnowledgeObjectDefinition)
		}
		cloned.Shadows[index].DefinitionSHA256 = bytes.Clone(input.Shadows[index].DefinitionSHA256)
	}
	return cloned
}

func evidenceFor(authority Authority) trustedCompilerEvidence {
	prelude := authority.Prelude()
	commitment, ok := prelude.Commitment()
	if !ok {
		panic("test authority has no knowledge program commitment")
	}
	evidence := trustedCompilerEvidence{
		knowledgeProgramPresent:    true,
		knowledgeProgramCommitment: commitment,
		knowledgeProgramObjects:    prelude.ObjectCount(),
		knowledgeProgramCharges:    prelude.Charges(),
	}
	if evidence.knowledgeProgramCharges.RegexPrograms > 0 {
		evidence.regexCaptureBytes = MaximumRegexCaptureBytes
	}
	return evidence
}

func deterministicMessage(t *testing.T, message proto.Message) []byte {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func uint64Pointer(value uint64) *uint64 { return &value }

func assertSnapshotDigest(t *testing.T, snapshot *opensplunkv1.KnowledgeSnapshot) {
	t.Helper()
	clone := proto.Clone(snapshot).(*opensplunkv1.KnowledgeSnapshot)
	wantDigest := bytes.Clone(clone.GetSnapshotSha256())
	clone.SnapshotSha256 = nil
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(clone)
	if err != nil {
		t.Fatalf("marshal digest input: %v", err)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(digestDomain))
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(encoded)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write(encoded)
	if got := hash.Sum(nil); !bytes.Equal(got, wantDigest) {
		t.Fatalf("snapshot digest = %x, recomputed %x", wantDigest, got)
	}

	charge := clone.GetBudgetCharges().GetCanonicalSnapshotBytes()
	clone.BudgetCharges.CanonicalSnapshotBytes = 0
	withoutCharge, err := (proto.MarshalOptions{Deterministic: true}).Marshal(clone)
	if err != nil {
		t.Fatalf("marshal charge input: %v", err)
	}
	if charge != uint64(len(withoutCharge)) {
		t.Fatalf("canonical snapshot byte charge = %d, want %d", charge, len(withoutCharge))
	}
}
