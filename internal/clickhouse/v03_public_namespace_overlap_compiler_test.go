package clickhouse

import (
	"reflect"
	"strings"
	"testing"
)

func TestV03CanonicalQuotedIdentifierRecognition(t *testing.T) {
	t.Parallel()

	for _, identifier := range []string{
		`parent\.child`,
		`double"quote`,
		`question?mark`,
		`dollar$mark`,
		`left{brace`,
		`right}brace`,
		`all\"?$ {braces}`,
	} {
		quoted := quoteIdentifier(identifier)
		if !isCanonicalQuotedIdentifierSQL(quoted) {
			t.Fatalf("canonical identifier %q was not recognized: %s", identifier, quoted)
		}
	}

	for _, value := range []string{
		`field`,
		`"field" + "other"`,
		`"raw?marker"`,
		`"raw\\slash"`,
		`"bad\x5"`,
		`"bad\xGG"`,
		`"lower\x5cslash"`,
		`"unnecessary\x41escape"`,
		"\"encoded\\xFFbyte\"",
		"\"raw\xffbyte\"",
	} {
		if isCanonicalQuotedIdentifierSQL(value) {
			t.Fatalf("noncanonical identifier SQL was recognized: %s", value)
		}
	}
}

func TestV03PublicNamespaceOverlapUsesCanonicalPathSegments(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		want []string
	}{
		{name: "parent.child", want: []string{"parent", "child"}},
		{name: `parent\.child`, want: []string{"parent.child"}},
		{name: `parent\.child.grand`, want: []string{"parent.child", "grand"}},
		{name: `.com`, want: []string{".com"}},
		{name: `_time`, want: []string{"_time"}},
	} {
		got, ok := logicalFieldPathSegments(test.name)
		if !ok || !reflect.DeepEqual(got, test.want) {
			t.Fatalf("logical path %q = %#v / %t, want %#v", test.name, got, ok, test.want)
		}
	}

	for _, test := range []struct {
		first, second string
		want          bool
	}{
		{first: "parent", second: "parent.child", want: true},
		{first: "parent", second: "parent..child", want: true},
		{first: "parent..child", second: "parent", want: true},
		{first: "parent.child", second: "parent.child..grand", want: true},
		{first: "parent", second: `parent\.child`, want: false},
		{first: "parent", second: ".com", want: false},
		{first: ".com", second: ".com.child", want: false},
	} {
		got, _ := logicalFieldNamesOverlap(test.first, test.second)
		if got != test.want {
			t.Fatalf("logical overlap %q / %q = %t, want %t",
				test.first, test.second, got, test.want)
		}
	}

	state := compileState{
		visible: map[string]fieldState{
			"parent":                {valueSQL: quoteIdentifier("parent")},
			"parent.child":          {valueSQL: quoteIdentifier("parent.child")},
			"parent..child":         {valueSQL: quoteIdentifier("parent..child")},
			"parent.child.grand":    {valueSQL: `"__os_private_grand"`},
			"parent.sibling":        {valueSQL: quoteIdentifier("parent.sibling")},
			`parent\.literal`:       {valueSQL: quoteIdentifier(`parent\.literal`)},
			`parent\.literal.child`: {valueSQL: quoteIdentifier(`parent\.literal.child`)},
			"unrelated":             {valueSQL: quoteIdentifier("unrelated")},
		},
		publicOrder: []string{
			"parent", "parent.child", "parent..child", "parent.child.grand", "parent.sibling",
			`parent\.literal`, `parent\.literal.child`, "unrelated",
		},
	}
	alias := `"namespace_source"`

	if got, want := publicFieldNamespaceOverlapReplacements(state, "parent", alias), []string{
		`"namespace_source"."parent.child" AS "parent.child"`,
		`"namespace_source"."parent..child" AS "parent..child"`,
		`"namespace_source"."parent.sibling" AS "parent.sibling"`,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("parent overlaps = %#v, want %#v", got, want)
	}
	if got, want := publicFieldNamespaceOverlapReplacements(state, "parent.child", alias), []string{
		`"namespace_source"."parent" AS "parent"`,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("parent.child overlaps = %#v, want %#v", got, want)
	}
	if got, want := publicFieldNamespaceOverlapReplacements(state, `parent\.literal`, alias), []string{
		alias + "." + quoteIdentifier(`parent\.literal.child`) + " AS " +
			quoteIdentifier(`parent\.literal.child`),
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("literal-dot overlaps = %#v, want %#v", got, want)
	}
}

func TestV03FillNullRepublishesInvalidPathStatsLiteralNamespaceOverlap(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | stats count AS "parent..child"`+
			` | fillnull value="fallback" parent`,
	)
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("invalid-path stats literal fillnull query lacks a valid execution seal")
	}
	invalidLiteral := quoteIdentifier("parent..child")
	if !strings.Contains(compiled.SQL,
		`."parent..child" AS "parent..child"`) {
		t.Fatalf("fillnull did not republish invalid-path physical namespace overlap:\n%s",
			compiled.SQL)
	}
	if strings.Contains(compiled.SQL,
		`."parent" AS "parent"`) {
		t.Fatalf("fillnull unexpectedly requalified the new ancestor output:\n%s",
			compiled.SQL)
	}
	if strings.Count(compiled.SQL, invalidLiteral) < 2 {
		t.Fatalf("invalid-path literal was not retained through publication:\n%s",
			compiled.SQL)
	}

	for _, output := range []string{`.com`, `parent\.child`} {
		compiled := compileSPL(t,
			`index=gradethis | stats count AS "`+output+`"`+
				` | fillnull value="fallback" parent`,
		)
		qualified := `.` + quoteIdentifier(output) + ` AS ` + quoteIdentifier(output)
		if strings.Contains(compiled.SQL, qualified) {
			t.Fatalf("control output %q was treated as a namespace overlap %q:\n%s",
				output, qualified, compiled.SQL)
		}
	}
}

func TestV03CommonUpsertRepublishesPhysicalNamespaceOverlaps(t *testing.T) {
	t.Parallel()

	state := compileState{
		visible: map[string]fieldState{
			"parent":       {valueSQL: `"parent"`},
			"parent.child": {valueSQL: `"parent.child"`},
		},
		publicOrder: []string{"parent", "parent.child"},
	}
	got := upsertFieldProjectionSQL(
		`SELECT "parent", "parent.child"`,
		state,
		"parent",
		`CAST('replacement' AS String)`,
		`"upsert_source"`,
	)
	for _, required := range []string{
		`SELECT * REPLACE ("upsert_source"."parent.child" AS "parent.child", CAST('replacement' AS String) AS "parent")`,
		`FROM (SELECT "parent", "parent.child") AS "upsert_source"`,
	} {
		if !strings.Contains(got, required) {
			t.Fatalf("common upsert lacks %q:\n%s", required, got)
		}
	}

	private := state
	private.visible = map[string]fieldState{
		"parent":       {valueSQL: `"__os_private_parent"`},
		"parent.child": {valueSQL: `"parent.child"`},
	}
	got = upsertFieldProjectionSQL(
		`SELECT "__os_private_parent", "parent.child"`,
		private,
		"parent",
		`"__os_private_parent"`,
		`"private_source"`,
	)
	if !strings.Contains(got,
		`SELECT * REPLACE ("private_source"."parent.child" AS "parent.child"), "__os_private_parent" AS "parent"`) {
		t.Fatalf("private-backed output was not appended after overlap preservation:\n%s", got)
	}
}

func TestV03AuthoredPublishersRepublishDottedNamespaceOverlaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		source          string
		republishedName string
		minimum         int
		marker          string
	}{
		{
			name:            "common eval descendant then ancestor",
			source:          `index=gradethis | eval parent.child="leaf" | eval parent="root" | table parent parent.child`,
			republishedName: "parent.child",
			minimum:         1,
		},
		{
			name:            "fillnull direct descendant then ancestor",
			source:          `index=gradethis | fillnull value="fallback" parent.child parent | table parent parent.child parent.sibling`,
			republishedName: "parent.child",
			minimum:         1,
			marker:          `__os_fillnull_source_`,
		},
		{
			name:            "fillnull direct ancestor then descendant",
			source:          `index=gradethis | fillnull value="fallback" parent parent.child | table parent parent.child parent.sibling`,
			republishedName: "parent",
			minimum:         1,
			marker:          `__os_fillnull_source_`,
		},
		{
			name:            "fillnull fixed String ancestor",
			source:          `index=gradethis | eval parent.child="leaf" | eval parent="root" | fillnull value="fallback" parent | table parent parent.child`,
			republishedName: "parent.child",
			minimum:         2,
			marker:          `__os_fillnull_string_source_`,
		},
		{
			name:            "fillnull fixed Number ancestor",
			source:          `index=gradethis | eval parent.child="leaf" | eval parent=1 | fillnull value="fallback" parent | table parent parent.child`,
			republishedName: "parent.child",
			minimum:         2,
		},
		{
			name:            "strcat ancestor",
			source:          `index=gradethis | eval parent.child="leaf" | strcat allrequired=true host ":" source parent | table parent parent.child`,
			republishedName: "parent.child",
			minimum:         1,
			marker:          `__os_strcat_source_`,
		},
		{
			name:            "addtotals ancestor",
			source:          `index=gradethis | eval parent.child="leaf" | addtotals fieldname=parent severity | table parent parent.child`,
			republishedName: "parent.child",
			minimum:         1,
			marker:          `__os_addtotals_validation_`,
		},
		{
			name:            "delta ancestor",
			source:          `index=gradethis | eval parent.child="leaf" | sort 0 +event_id | delta severity AS parent | table parent parent.child`,
			republishedName: "parent.child",
			minimum:         1,
			marker:          `__os_delta_value_`,
		},
		{
			name:            "makemv ancestor",
			source:          `index=gradethis | eval parent.child="leaf" | eval parent="a,b" | makemv delim="," parent | table parent parent.child`,
			republishedName: "parent.child",
			minimum:         2,
			marker:          `__os_makemv_result_`,
		},
		{
			name:            "mvexpand ancestor",
			source:          `index=gradethis | eval parent.child="leaf" | eval parent="a,b" | makemv delim="," parent | mvexpand parent | table parent parent.child`,
			republishedName: "parent.child",
			minimum:         3,
			marker:          `__os_mvexpand_pair_`,
		},
		{
			name:            "accum ancestor",
			source:          `index=gradethis | eval parent.child="leaf" | sort 0 +event_id | accum severity AS parent | table parent parent.child`,
			republishedName: "parent.child",
			minimum:         1,
			marker:          `__os_streamstats_value_`,
		},
		{
			name:            "eventstats ancestor",
			source:          `index=gradethis | eval parent.child="leaf" | eventstats count AS parent | table parent parent.child`,
			republishedName: "parent.child",
			minimum:         1,
			marker:          `__os_eventstats_raw_count_`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if !compiled.HasValidExecutionSeal() {
				t.Fatalf("compiled query lacks a valid execution seal:\n%s", compiled.SQL)
			}
			needle := `.` + quoteIdentifier(test.republishedName) +
				` AS ` + quoteIdentifier(test.republishedName)
			if got := strings.Count(compiled.SQL, needle); got < test.minimum {
				t.Fatalf("qualified namespace republications of %q = %d, want at least %d:\n%s",
					test.republishedName, got, test.minimum, compiled.SQL)
			}
			if test.marker != "" && !strings.Contains(compiled.SQL, test.marker) {
				t.Fatalf("compiled query lacks command marker %q:\n%s", test.marker, compiled.SQL)
			}
		})
	}
}

func TestV03NamespaceOverlapDoesNotConflateEscapedLiteralDot(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | eval parent\.child="literal" | eval parent="ancestor"`+
			` | table parent parent\.child`,
	)
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("literal-dot namespace query lacks a valid execution seal")
	}
	for _, forbidden := range []string{
		`.` + quoteIdentifier(`parent\.child`) + ` AS ` + quoteIdentifier(`parent\.child`),
		`."parent" AS "parent"`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("escaped literal-dot field was treated as a path overlap %q:\n%s",
				forbidden, compiled.SQL)
		}
	}
}

func TestV03FillNullFixedStringQualifiesEncodedPhysicalIdentifier(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | eval parent\.child="kept"`+
			` | fillnull value="fallback" parent\.child | table parent\.child`,
	)
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("escaped-name fixed-String fillnull query lacks a valid execution seal")
	}
	identifier := quoteIdentifier(`parent\.child`)
	if strings.Contains(compiled.SQL, "arrayJoin([tuple("+identifier+",") {
		t.Fatalf("fillnull captured its same-layer replacement alias instead of the input relation:\n%s",
			compiled.SQL)
	}
	if !strings.Contains(compiled.SQL, "."+identifier+", toUInt8(") {
		t.Fatalf("fillnull did not relation-qualify the encoded physical identifier:\n%s",
			compiled.SQL)
	}
}

func TestV03FillNullNamespaceOverlapComposesWithConsumers(t *testing.T) {
	t.Parallel()

	for _, suffix := range []string{
		` | fields event_id parent parent.child | where parent.child="fallback" | table event_id parent parent.child`,
		` | table event_id parent parent.child | rename parent.child AS leaf | table event_id parent leaf`,
		` | stats count BY parent.child`,
		` | chart count OVER parent.child BY event_id`,
		` | mvexpand parent.child | table event_id parent parent.child`,
	} {
		compiled := compileSPL(t,
			`index=gradethis | fillnull value="fallback" parent.child parent`+suffix,
		)
		if !compiled.HasValidExecutionSeal() ||
			!strings.Contains(compiled.SQL, `."parent.child" AS "parent.child"`) {
			t.Fatalf("fillnull namespace composition is not sealed and republished for %q:\n%s",
				suffix, compiled.SQL)
		}
	}
}
