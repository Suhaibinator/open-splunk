package plan

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestValidateRuntimeWildcardSuffixChecksTimeAndDeclaredSchemas(t *testing.T) {
	t.Parallel()
	for _, source := range []string{
		`index=gradethis | convert num(value*) | convert ctime(_time) | timechart count`,
		`index=gradethis | convert ctime(_*) | timechart count`,
		`index=gradethis | convert num(value*) | stats count AS total | timechart count`,
		`index=gradethis | convert num(value*) | table fixed | stats sum(no*)`,
	} {
		if err := ValidateRuntimeWildcardSuffix(mustParse(t, source), testScope([]string{"gradethis"}, nil)); err == nil {
			t.Errorf("accepted invalid suffix: %s", source)
		}
	}
}

func TestValidateRuntimeWildcardSuffixDefersNamesAndHonorsExclusions(t *testing.T) {
	t.Parallel()
	for _, source := range []string{
		`index=gradethis | convert num(value*) | convert ctime(_time) AS rendered | timechart count`,
		`index=gradethis | convert ctime(_*) none(_time) | timechart count`,
		`index=gradethis | convert num(value*) | stats sum(value*) AS total_* | stats sum(total_a*)`,
		`index=gradethis | convert num(value) AS "First Seen" num(First*)`,
		`index=gradethis | convert num(never_matches*)`,
		"index=gradethis | convert num(value*) | stats sum(" + strings.Repeat("a", 256) + "*)",
		"index=gradethis | fields + value* | convert " + strings.Repeat("num(value) AS copied ", spl.MaximumConvertClauses-1) + "num(*)",
	} {
		if err := ValidateRuntimeWildcardSuffix(mustParse(t, source), testScope([]string{"gradethis"}, nil)); err != nil {
			t.Errorf("validation invented a failure for %s: %v", source, err)
		}
	}
}

func TestValidateRuntimeWildcardSuffixBoundsStagesWithoutEvidence(t *testing.T) {
	t.Parallel()
	source := "index=gradethis" + strings.Repeat(" | convert num(value*)", MaximumRuntimeWildcardStages)
	if err := ValidateRuntimeWildcardSuffix(mustParse(t, source), testScope([]string{"gradethis"}, nil)); err != nil {
		t.Fatal(err)
	}
	_, err := spl.Parse(source + " | convert num(value*)")
	if err != nil {
		t.Fatal(err)
	}
	err = ValidateRuntimeWildcardSuffix(mustParse(t, source+" | convert num(value*)"), testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}
