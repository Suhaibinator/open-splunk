package knowledgesnapshot

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestAuthorityFinalizeBindsExactSealedCompilerOutputAndAuthoredCharges(t *testing.T) {
	t.Parallel()

	authority := emptyCompilerAuthority(t, "tenant-1", []string{"gradethis"})
	compiled, _ := compileSnapshotQuery(t,
		"tenant-1",
		[]string{"gradethis"},
		`index=gradethis | rex field=_raw "(?<word>[a-z]+)" `+
			`| spath input=_raw output=selected path=payload.value `+
			`| eval state=if(isnull(selected), "missing", "present") `+
			`| where state="present"`,
	)
	snapshot, err := authority.Finalize(compiled)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	message := snapshot.Proto()
	charges := message.GetBudgetCharges()
	if charges.GetExecutableObjects() != 0 || charges.GetGeneratedOperators() != 0 ||
		charges.GetGeneratedFields() != 0 || charges.GetRegexPrograms() != 1 ||
		charges.GetRegexWorkUnits() == 0 || charges.GetRegexCaptureBytes() != MaximumRegexCaptureBytes ||
		charges.GetScalarExpressions() != 0 || charges.GetScalarExpressionNodes() != 0 ||
		charges.GetGeneratedSqlBytes() != uint64(len(compiled.SQL)) {
		t.Fatalf("finalized compiler charges = %#v", charges)
	}
	if len(message.GetSnapshotSha256()) != sha256.Size || snapshot.IsZero() {
		t.Fatalf("finalized snapshot = %#v", message)
	}

	cloned := snapshot.Clone()
	if !snapshot.Equal(cloned) || cloned.message == snapshot.message ||
		len(cloned.encoded) > 0 && &cloned.encoded[0] == &snapshot.encoded[0] {
		t.Fatal("Snapshot.Clone did not preserve and detach canonical authority")
	}
	returned := cloned.Proto()
	returned.TenantId = "mutated"
	encoded := cloned.Encoded()
	encoded[0] ^= 0xff
	if cloned.Proto().GetTenantId() != "tenant-1" || !bytes.Equal(cloned.Encoded(), snapshot.Encoded()) {
		t.Fatal("cloned snapshot accessors alias caller memory")
	}
}

func TestAuthorityFinalizeRejectsForgeryTamperAndScopeMismatch(t *testing.T) {
	t.Parallel()

	authority := emptyCompilerAuthority(t, "tenant-1", []string{"gradethis"})
	compiled, logical := compileSnapshotQuery(t, "tenant-1", []string{"gradethis"}, `index=gradethis status=200`)

	tests := []struct {
		name      string
		authority Authority
		compiled  clickhouse.CompiledQuery
	}{
		{name: "zero compiled query", authority: authority},
		{name: "forged compiled query", authority: authority, compiled: clickhouse.CompiledQuery{SQL: compiled.SQL, Args: slices.Clone(compiled.Args)}},
		{name: "SQL tamper", authority: authority, compiled: func() clickhouse.CompiledQuery {
			value := compiled
			value.SQL += " "
			return value
		}()},
		{name: "argument tamper", authority: authority, compiled: func() clickhouse.CompiledQuery {
			value := compiled
			value.Args = slices.Clone(value.Args)
			value.Args[len(value.Args)-1] = "tampered"
			return value
		}()},
		{name: "tenant mismatch", authority: emptyCompilerAuthority(t, "tenant-2", []string{"gradethis"}), compiled: compiled},
		{name: "index mismatch", authority: emptyCompilerAuthority(t, "tenant-1", []string{"other"}), compiled: compiled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if snapshot, err := test.authority.Finalize(test.compiled); !snapshot.IsZero() || !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("Finalize = (%#v, %v), want zero/ErrInvalidInput", snapshot, err)
			}
		})
	}

	manual := &plan.Query{
		Operators:        slices.Clone(logical.Operators),
		EffectiveIndexes: slices.Clone(logical.EffectiveIndexes),
		OutputFields:     slices.Clone(logical.OutputFields),
		DynamicOutput:    logical.DynamicOutput,
		SearchStart:      logical.SearchStart,
		SearchTimezone:   logical.SearchTimezone,
	}
	manualCompiled, err := (clickhouse.Compiler{}).Compile(manual)
	if err != nil {
		t.Fatalf("Compile(manual): %v", err)
	}
	if snapshot, err := authority.Finalize(manualCompiled); !snapshot.IsZero() || !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Finalize(manual plan) = (%#v, %v)", snapshot, err)
	}
}

func TestAuthorityFinalizeRejectsNonemptyAuthorityUntilKnowledgePreludeExists(t *testing.T) {
	t.Parallel()

	authority, err := Prepare(snapshotGoldenInput(t))
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	compiled, _ := compileSnapshotQuery(t, "tenant-a", []string{"alpha", "zeta"}, `*`)
	if snapshot, finalizeErr := authority.Finalize(compiled); !snapshot.IsZero() ||
		!errors.Is(finalizeErr, ErrInvalidInput) {
		t.Fatalf("Finalize(nonempty) = (%#v, %v), want zero/ErrInvalidInput", snapshot, finalizeErr)
	}
}

func emptyCompilerAuthority(t *testing.T, tenantID string, indexes []string) Authority {
	t.Helper()
	authority, err := Prepare(Input{
		TenantID:                   tenantID,
		PrincipalID:                "principal-1",
		AppID:                      "app-1",
		TenantCatalogRevision:      1,
		TenantCatalogStateToken:    bytes.Repeat([]byte{0x4a}, sha256.Size),
		EffectiveAuthorizedIndexes: slices.Clone(indexes),
	})
	if err != nil {
		t.Fatalf("Prepare(empty): %v", err)
	}
	return authority
}

func compileSnapshotQuery(
	t *testing.T,
	tenantID string,
	indexes []string,
	source string,
) (clickhouse.CompiledQuery, *plan.Query) {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	visibility := uint64(73)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          tenantID,
		AuthorizedIndexes: slices.Clone(indexes),
		Earliest:          time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC),
		SearchStart:       time.Date(2026, 8, 2, 0, 0, 1, 0, time.UTC),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   time.Date(2026, 8, 2, 0, 0, 2, 0, time.UTC),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return compiled, logical
}
