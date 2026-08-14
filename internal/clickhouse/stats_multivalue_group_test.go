package clickhouse

import (
	"strings"
	"testing"
)

func TestCompileStatsMultivalueByExpandsAndDeduplicates(t *testing.T) {
	t.Parallel()

	withoutDedup := compileSPL(
		t,
		`index=gradethis | stats count sum(score) AS total BY tags`,
	)
	for _, required := range []string{
		` AS "__os_group_values_0"`,
		`ARRAY JOIN "__os_group_values_0" AS "__os_group_value_0"`,
		`GROUP BY "__os_group_value_0"`,
	} {
		if !strings.Contains(withoutDedup.SQL, required) {
			t.Fatalf("multivalue BY SQL missing %q:\n%s", required, withoutDedup.SQL)
		}
	}
	if strings.Contains(withoutDedup.SQL, `arrayDistinct(`) {
		t.Fatalf("default multivalue BY unexpectedly deduplicates values:\n%s", withoutDedup.SQL)
	}

	withDedup := compileSPL(
		t,
		`index=gradethis | stats count sum(score) AS total BY tags `+
			`dedup_splitvals=true`,
	)
	if !strings.Contains(withDedup.SQL, `arrayDistinct(`) {
		t.Fatalf("dedup_splitvals=true did not deduplicate before expansion:\n%s", withDedup.SQL)
	}
}

func TestCompileStatsMultipleMultivalueByUsesCartesianStages(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count BY tags zones dedup_splitvals=true`,
	)
	if got := strings.Count(compiled.SQL, ` ARRAY JOIN `); got != 2 {
		t.Fatalf("multivalue BY expansion stages = %d, want 2:\n%s", got, compiled.SQL)
	}
	first := `ARRAY JOIN "__os_group_values_0" AS "__os_group_value_0"`
	second := `ARRAY JOIN "__os_group_values_1" AS "__os_group_value_1"`
	firstAt := strings.Index(compiled.SQL, first)
	secondAt := strings.Index(compiled.SQL, second)
	if firstAt < 0 || secondAt < 0 || firstAt >= secondAt {
		t.Fatalf("multivalue BY expansions are not staged Cartesian joins:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `ARRAY JOIN "__os_group_values_0" AS "__os_group_value_0",`) {
		t.Fatalf("multivalue BY arrays were positionally zipped:\n%s", compiled.SQL)
	}
}

func TestCompileStatsFixedScalarByDoesNotExpandRows(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count BY host`)
	if strings.Contains(compiled.SQL, "ARRAY JOIN") {
		t.Fatalf("fixed scalar BY unexpectedly expanded rows:\n%s", compiled.SQL)
	}
}

func TestCompileStatsFixedMultivalueResultCanFeedBy(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | stats count BY users`,
	)
	if !strings.Contains(
		compiled.SQL,
		`ARRAY JOIN "__os_group_values_0" AS "__os_group_value_0"`,
	) {
		t.Fatalf("fixed multivalue stats result was not expanded by downstream BY:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, UnsupportedStatsByValueMarker) {
		t.Fatalf("fixed multivalue BY retained a dynamic-container rejection:\n%s", compiled.SQL)
	}
	if !compiled.RequiresAtomicResult() {
		t.Fatal("fixed multivalue BY expansion guard did not require atomic execution")
	}
	descriptors, valid := compiled.ValidatedResultStringOrBytesOutputs()
	if !valid || len(descriptors) != 1 || descriptors[0].OutputIndex != 0 {
		t.Fatalf(
			"fixed byte-capable BY transport = %#v, valid=%t, want output 0",
			descriptors,
			valid,
		)
	}
}

func TestCompileStatsFixedMultivalueByPreservesByteCapabilityAcrossShapeStages(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		field  string
	}{
		{
			name: "values direct",
			source: `index=gradethis | stats values(host) AS members` +
				` | stats count BY members`,
			field: "members",
		},
		{
			name: "list projection rename table",
			source: `index=gradethis | stats list(host) AS members` +
				` | fields members | rename members AS renamed` +
				` | stats count BY renamed | table renamed count`,
			field: "renamed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			if !compiled.RequiresAtomicResult() {
				t.Fatal("fixed multivalue BY did not require atomic execution")
			}
			if len(compiled.OutputFields) != 2 || compiled.OutputFields[0] != test.field {
				t.Fatalf("outputs = %v, want [%s count]", compiled.OutputFields, test.field)
			}
			descriptors, valid := compiled.ValidatedResultStringOrBytesOutputs()
			if !valid || len(descriptors) != 1 || descriptors[0].OutputIndex != 0 {
				t.Fatalf(
					"String-or-Bytes transports = %#v, valid=%t, want output 0",
					descriptors,
					valid,
				)
			}
			clone, ok := compiled.CloneForExecution()
			if !ok || !clone.HasValidExecutionSeal() ||
				len(clone.StringOrBytesOutputs) != 1 ||
				clone.StringOrBytesOutputs[0] != descriptors[0] {
				t.Fatalf("execution clone lost String-or-Bytes transport: %#v", clone.StringOrBytesOutputs)
			}
		})
	}
}

func TestCompileStatsMultivalueResultRemainsTypedListWithoutScalarDescriptor(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats values(host) AS members`,
		`index=gradethis | stats list(host) AS members`,
		`index=gradethis | eventstats values(host) AS members | table members`,
		`index=gradethis | eventstats list(host) AS members | table members`,
	} {
		compiled := compileSPL(t, source)
		if len(compiled.StringOrBytesOutputs) != 0 {
			t.Fatalf(
				"typed list %q acquired scalar String-or-Bytes descriptors: %#v",
				source,
				compiled.StringOrBytesOutputs,
			)
		}
	}
}

func TestCompileStatsFixedMultivalueByFencesUTF8Consumers(t *testing.T) {
	t.Parallel()

	const base = `index=gradethis | stats values(host) AS members` +
		` | stats count BY members`
	text := compileSPL(
		t,
		base+` | eval lowered=lower(members), measured=len(members),`+
			` piece=substr(members,1,1) | table members lowered measured piece`,
	)
	if got := strings.Count(text.SQL, `isValidUTF8("__os_group_0")`); got < 3 {
		t.Fatalf(
			"fixed multivalue BY text consumers have %d member-local UTF-8 guards, want at least 3:\n%s",
			got,
			text.SQL,
		)
	}

	for _, test := range []struct {
		name     string
		suffix   string
		consumer string
	}{
		{name: "search equality", suffix: ` | search members="ascii"`, consumer: `lowerUTF8(`},
		{name: "search wildcard", suffix: ` | search members="a*"`, consumer: `match(`},
		{name: "where equality", suffix: ` | where members="ascii"`, consumer: ` = `},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, base+test.suffix)
			guard := `ifNull(isValidUTF8("__os_group_0"), 0)`
			if !strings.Contains(compiled.SQL, guard) ||
				!strings.Contains(compiled.SQL, test.consumer) {
				t.Fatalf(
					"%s did not fence its UTF-8 consumer with %q:\n%s",
					test.name,
					guard,
					compiled.SQL,
				)
			}
		})
	}
}

func TestCompileStatsFixedMultivalueByRebindsTextProofAcrossShapeStages(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats list(host) AS members`+
			` | stats count BY members | fields members count`+
			` | rename members AS renamed | table renamed count`+
			` | eval lowered=lower(renamed) | table renamed lowered`,
	)
	if !strings.Contains(compiled.SQL, `isValidUTF8("renamed")`) {
		t.Fatalf("shape stages did not rebind the member-local UTF-8 proof:\n%s", compiled.SQL)
	}
}
