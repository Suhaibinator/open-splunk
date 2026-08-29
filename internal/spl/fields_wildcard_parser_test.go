package spl

import (
	"slices"
	"testing"
)

func TestParseFieldsAcceptsOfficialWildcardSelectors(t *testing.T) {
	t.Parallel()

	source := `index=main | fields + host, error*, '_*'`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*FieldsCommand)
	if command.Exclude ||
		!slices.Equal(command.Fields, []string{"host", "error*", "_*"}) ||
		!slices.Equal(command.QuotedFields, []bool{false, false, true}) ||
		!slices.Equal(command.WildcardFields, []bool{false, true, true}) ||
		len(command.FieldRanges) != 3 {
		t.Fatalf("fields command = %#v", command)
	}
	for index, want := range []string{"host", "error*", "'_*'"} {
		assertSourceRangeText(t, source, command.FieldRanges[index], want)
	}
}

func TestParseFieldsWildcardRejectsPrivateAndMalformedPatterns(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | fields __os_*`,
		`index=main | fields +`,
		`index=main | fields error*,`,
	} {
		if _, err := Parse(source); err == nil {
			t.Fatalf("Parse(%q) succeeded", source)
		}
	}
}

func TestMatchFieldsFieldGlobRequiresExplicitInternalPrefix(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"_time", "_raw"} {
		if MatchFieldsFieldGlob("*", name) {
			t.Errorf("broad wildcard matched internal field %q", name)
		}
		if !MatchFieldsFieldGlob("_*", name) {
			t.Errorf("explicit internal wildcard did not match %q", name)
		}
	}
	if !MatchFieldsFieldGlob("*", "host") {
		t.Fatal("broad wildcard did not match public host")
	}
}
