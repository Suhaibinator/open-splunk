package plan

import (
	"slices"
	"testing"
)

func TestV03PublicIndexRewritesStopPhysicalScopeInference(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prefix    string
		predicate string
	}{
		{
			name:      "fillnull search",
			prefix:    `index=gradethis | table event_id | fillnull value="filled" index`,
			predicate: `search index="filled"`,
		},
		{
			name:      "fillnull where",
			prefix:    `index=gradethis | table event_id | fillnull value="filled" index`,
			predicate: `where index="filled"`,
		},
		{
			name:      "addtotals search",
			prefix:    `index=gradethis | addtotals fieldname=index bytes duration`,
			predicate: `search index=3`,
		},
		{
			name:      "addtotals where",
			prefix:    `index=gradethis | addtotals fieldname=index bytes duration`,
			predicate: `where index=3`,
		},
		{
			name:      "delta search",
			prefix:    `index=gradethis | sort 0 +_time | delta bytes AS index`,
			predicate: `search index=1`,
		},
		{
			name:      "delta where",
			prefix:    `index=gradethis | sort 0 +_time | delta bytes AS index`,
			predicate: `where index=1`,
		},
		{
			name:      "makemv search",
			prefix:    `index=gradethis | makemv delim="," index`,
			predicate: `search index="gradethis"`,
		},
		{
			name:      "makemv where",
			prefix:    `index=gradethis | makemv delim="," index`,
			predicate: `where index="gradethis"`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := test.prefix + ` | ` + test.predicate
			parsed := mustParse(t, source)
			if references := positiveIndexReferences(parsed, nil); len(references) != 1 ||
				references[0].value != "gradethis" {
				t.Fatalf(
					"positiveIndexReferences(%q) = %#v, want only immutable input index gradethis",
					source,
					references,
				)
			}

			logical, err := Build(parsed, testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build(%q): %v", source, err)
			}
			if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
				t.Fatalf("effective indexes = %v, want immutable input scope [gradethis]", logical.EffectiveIndexes)
			}
			filter, ok := logical.Operators[len(logical.Operators)-1].(*Filter)
			if !ok || filter == nil {
				t.Fatalf("last operator = %T, want public-index Filter", logical.Operators[len(logical.Operators)-1])
			}
		})
	}
}

func TestV03NonIndexWritesDoNotHideForbiddenPhysicalIndexSelectors(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fillnull host | search index=secret`,
		`index=gradethis | addtotals fieldname=total bytes duration | search index=secret`,
		`index=gradethis | delta bytes AS difference | search index=secret`,
		`index=gradethis | makemv delim="," tags | search index=secret`,
	} {
		source := source
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			parsed := mustParse(t, source)
			if references := positiveIndexReferences(parsed, nil); len(references) != 2 {
				t.Fatalf("positiveIndexReferences(%q) = %#v, want both physical selectors", source, references)
			}
			_, err := Build(parsed, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
		})
	}
}
