package plan

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// Private order and presence columns are compiler implementation details, not
// authored SPL fields. Command grammar rejects that namespace at the earliest
// source-owned boundary, with ResolveField retaining an independent defense
// against forged command metadata below.
func TestV03CommandsRejectPrivateOrdinalCollisionsDuringParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
	}{
		{name: "regex input", source: `index=gradethis | regex __os_pipeline_ordinal="x"`},
		{name: "accum input", source: `index=gradethis | accum __os_pipeline_ordinal AS running`},
		{name: "accum output", source: `index=gradethis | accum value AS __os_pipeline_ordinal`},
		{name: "strcat input", source: `index=gradethis | strcat __os_pipeline_ordinal route output`},
		{name: "strcat output", source: `index=gradethis | strcat host route __os_pipeline_ordinal`},
		{name: "fillnull field", source: `index=gradethis | fillnull __os_pipeline_ordinal`},
		{name: "addtotals input", source: `index=gradethis | addtotals __os_pipeline_ordinal`},
		{name: "addtotals output", source: `index=gradethis | addtotals fieldname=__os_pipeline_ordinal value`},
		{name: "delta input", source: `index=gradethis | delta __os_pipeline_ordinal`},
		{name: "delta output", source: `index=gradethis | delta value AS __os_pipeline_ordinal`},
		{name: "makemv input", source: `index=gradethis | makemv __os_pipeline_ordinal`},
		{name: "mvexpand input", source: `index=gradethis | mvexpand __os_pipeline_ordinal`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, parseErr := spl.Parse(test.source)
			var diagnostic *spl.Diagnostic
			if !errors.As(parseErr, &diagnostic) || diagnostic.Code != "SPL_RESERVED_FIELD" {
				t.Fatalf("Parse error = %v, want SPL_RESERVED_FIELD", parseErr)
			}
			if diagnostic.Range.Start.Offset < 0 ||
				diagnostic.Range.End.Offset > len(test.source) ||
				diagnostic.Range.End.Offset < diagnostic.Range.Start.Offset {
				t.Fatalf("diagnostic range = %#v for %d-byte source", diagnostic.Range, len(test.source))
			}
			if got := test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != "__os_pipeline_ordinal" {
				t.Fatalf("diagnostic range text = %q, want private authored token", got)
			}
		})
	}
}

func TestV03ForgedCommandsCannotBypassPrivateNamespaceDefense(t *testing.T) {
	t.Parallel()

	sourceRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 22, Line: 1, Column: 23},
	}
	literal := ":"
	tests := []struct {
		name  string
		build func() error
	}{
		{name: "regex input", build: func() error {
			_, err := buildRegexCommand(&spl.RegexCommand{
				Field: "__os_pipeline_ordinal", FieldRange: sourceRange,
				Pattern: "x", PatternRange: sourceRange, Range: sourceRange,
			}, false)
			return err
		}},
		{name: "accum input", build: func() error {
			_, err := buildAccumCommand(&spl.AccumCommand{
				Field: "__os_pipeline_ordinal", FieldRange: sourceRange,
				Output: "running", OutputRange: sourceRange,
				ExplicitOutput: true, Range: sourceRange,
			}, false)
			return err
		}},
		{name: "accum output", build: func() error {
			_, err := buildAccumCommand(&spl.AccumCommand{
				Field: "value", FieldRange: sourceRange,
				Output: "__os_pipeline_ordinal", OutputRange: sourceRange,
				ExplicitOutput: true, Range: sourceRange,
			}, false)
			return err
		}},
		{name: "strcat input", build: func() error {
			_, err := buildStrcatCommand(&spl.StrcatCommand{
				Operands: []spl.StrcatOperand{
					{Field: "__os_pipeline_ordinal", Range: sourceRange},
					{Literal: &literal, Range: sourceRange},
				},
				Destination: "output", DestinationRange: sourceRange, Range: sourceRange,
			}, false, &splExpressionResourceBudget{})
			return err
		}},
		{name: "strcat output", build: func() error {
			_, err := buildStrcatCommand(&spl.StrcatCommand{
				Operands: []spl.StrcatOperand{
					{Field: "host", Range: sourceRange},
					{Literal: &literal, Range: sourceRange},
				},
				Destination: "__os_pipeline_ordinal", DestinationRange: sourceRange, Range: sourceRange,
			}, false, &splExpressionResourceBudget{})
			return err
		}},
		{name: "fillnull field", build: func() error {
			_, err := buildFillNull(&spl.FillNullCommand{
				Value: "0", Fields: []spl.ExactCommandField{{Name: "__os_pipeline_ordinal", Range: sourceRange}}, Range: sourceRange,
			})
			return err
		}},
		{name: "addtotals input", build: func() error {
			_, err := buildRowTotal(&spl.AddTotalsCommand{
				Fields: []spl.ExactCommandField{{Name: "__os_pipeline_ordinal", Range: sourceRange}},
				Output: "Total", OutputRange: sourceRange, Range: sourceRange,
			})
			return err
		}},
		{name: "addtotals output", build: func() error {
			_, err := buildRowTotal(&spl.AddTotalsCommand{
				Fields: []spl.ExactCommandField{{Name: "value", Range: sourceRange}},
				Output: "__os_pipeline_ordinal", OutputRange: sourceRange, Range: sourceRange,
			})
			return err
		}},
		{name: "delta input", build: func() error {
			_, err := buildOrderedDelta(&spl.DeltaCommand{
				Field: "__os_pipeline_ordinal", FieldRange: sourceRange,
				Output: "delta", OutputRange: sourceRange, Previous: 1, Range: sourceRange,
			})
			return err
		}},
		{name: "delta output", build: func() error {
			_, err := buildOrderedDelta(&spl.DeltaCommand{
				Field: "value", FieldRange: sourceRange,
				Output: "__os_pipeline_ordinal", OutputRange: sourceRange, Previous: 1, Range: sourceRange,
			})
			return err
		}},
		{name: "makemv input", build: func() error {
			_, err := buildMakeMultivalue(&spl.MakeMVCommand{
				Field: "__os_pipeline_ordinal", FieldRange: sourceRange,
				Delimiter: ",", Range: sourceRange,
			})
			return err
		}},
		{name: "mvexpand input", build: func() error {
			_, err := buildExpandMultivalue(&spl.MVExpandCommand{
				Field: "__os_pipeline_ordinal", FieldRange: sourceRange, Range: sourceRange,
			}, 1)
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.build(); err == nil {
				t.Fatal("forged private command metadata unexpectedly built a logical operator")
			}
		})
	}
}

func TestV03ReservedFieldsPayloadRequiresAnExactUpstreamSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		raw        string
		exact      string
		code       string
		rangeAtEnd bool
	}{
		{name: "regex input", raw: `index=gradethis | regex fields="x"`, exact: `index=gradethis | table fields | regex fields="x"`, code: "SPL_AMBIGUOUS_REGEX_FIELD"},
		{name: "accum input", raw: `index=gradethis | accum fields AS running`, exact: `index=gradethis | table fields | accum fields AS running`, code: "SPL_AMBIGUOUS_ACCUM_FIELD"},
		{name: "accum output", raw: `index=gradethis | accum value AS fields`, exact: `index=gradethis | table value | accum value AS fields`, code: "SPL_AMBIGUOUS_ACCUM_FIELD", rangeAtEnd: true},
		{name: "strcat input", raw: `index=gradethis | strcat fields ":" route output`, exact: `index=gradethis | table fields route | strcat fields ":" route output`, code: "SPL_AMBIGUOUS_STRCAT_FIELD"},
		{name: "strcat output", raw: `index=gradethis | strcat host route fields`, exact: `index=gradethis | table host route | strcat host route fields`, code: "SPL_AMBIGUOUS_STRCAT_FIELD", rangeAtEnd: true},
		{name: "fillnull field", raw: `index=gradethis | fillnull fields`, exact: `index=gradethis | table fields | fillnull fields`, code: "SPL_AMBIGUOUS_FILLNULL_FIELD", rangeAtEnd: true},
		{name: "addtotals input", raw: `index=gradethis | addtotals fields`, exact: `index=gradethis | table fields | addtotals fields`, code: "SPL_AMBIGUOUS_ADDTOTALS_FIELD", rangeAtEnd: true},
		{name: "addtotals output", raw: `index=gradethis | addtotals fieldname=fields value`, exact: `index=gradethis | table value | addtotals fieldname=fields value`, code: "SPL_AMBIGUOUS_ADDTOTALS_FIELD"},
		{name: "delta input", raw: `index=gradethis | delta fields AS change`, exact: `index=gradethis | table fields | delta fields AS change`, code: "SPL_AMBIGUOUS_DELTA_FIELD"},
		{name: "delta output", raw: `index=gradethis | delta value AS fields`, exact: `index=gradethis | table value | delta value AS fields`, code: "SPL_AMBIGUOUS_DELTA_FIELD", rangeAtEnd: true},
		{name: "makemv field", raw: `index=gradethis | makemv fields`, exact: `index=gradethis | table fields | makemv fields`, code: "SPL_AMBIGUOUS_MAKEMV_FIELD", rangeAtEnd: true},
		{name: "mvexpand field", raw: `index=gradethis | mvexpand fields`, exact: `index=gradethis | table fields | mvexpand fields`, code: "SPL_AMBIGUOUS_MVEXPAND_FIELD", rangeAtEnd: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			parsed, parseErr := spl.Parse(test.raw)
			if parseErr != nil {
				t.Fatalf("Parse raw form: %v", parseErr)
			}
			_, buildErr := Build(parsed, testScope([]string{"gradethis"}, nil))
			var diagnostic *Diagnostic
			if !errors.As(buildErr, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("Build raw form error = %v, want %s", buildErr, test.code)
			}
			wantOffset := strings.Index(test.raw, "fields")
			if test.rangeAtEnd {
				wantOffset = strings.LastIndex(test.raw, "fields")
			}
			if diagnostic.Range.Start.Offset != wantOffset ||
				test.raw[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset] != "fields" {
				t.Fatalf("diagnostic range = %#v in %q", diagnostic.Range, test.raw)
			}

			exactParsed, exactParseErr := spl.Parse(test.exact)
			if exactParseErr != nil {
				t.Fatalf("Parse exact-schema form: %v", exactParseErr)
			}
			if _, exactBuildErr := Build(exactParsed, testScope([]string{"gradethis"}, nil)); exactBuildErr != nil {
				t.Fatalf("Build exact-schema form: %v", exactBuildErr)
			}
		})
	}
}

func TestV03PlanningBoundsRepeatedExpansionAndKeepsOrdinalPrivate(t *testing.T) {
	t.Parallel()

	var accepted strings.Builder
	accepted.WriteString(`index=gradethis`)
	for index := 1; index <= MaximumMVExpandStages; index++ {
		fmt.Fprintf(&accepted, ` | mvexpand tags%d`, index)
		if index != MaximumMVExpandStages {
			accepted.WriteString(` | reverse`)
		}
	}
	parsed, err := spl.Parse(accepted.String())
	if err != nil {
		t.Fatalf("Parse accepted repeated expansion: %v", err)
	}
	logical, err := Build(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build accepted repeated expansion: %v", err)
	}
	var expansions []*ExpandMultivalue
	for _, operator := range logical.Operators {
		if expansion, ok := operator.(*ExpandMultivalue); ok {
			expansions = append(expansions, expansion)
		}
	}
	if len(expansions) != MaximumMVExpandStages {
		t.Fatalf("expansions = %d, want boundary %d", len(expansions), MaximumMVExpandStages)
	}
	for index, expansion := range expansions {
		if expansion.QueryOrdinal != uint8(index+1) {
			t.Fatalf("expansion %d ordinal = %d", index, expansion.QueryOrdinal)
		}
	}
	for _, output := range logical.OutputFields {
		if strings.HasPrefix(strings.ToLower(output), "__os_") {
			t.Fatalf("private ordering field leaked into logical output: %q", output)
		}
	}

	rejected := accepted.String() + ` | mvexpand overflow`
	parsed, err = spl.Parse(rejected)
	if err != nil {
		t.Fatalf("parser should retain the third syntax-shaped expansion for planning: %v", err)
	}
	_, err = Build(parsed, testScope([]string{"gradethis"}, nil))
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("third mvexpand Build error = %v, want SPL_QUERY_TOO_COMPLEX", err)
	}
	wantOffset := strings.LastIndex(rejected, "mvexpand overflow")
	if diagnostic.Range.Start.Offset != wantOffset ||
		rejected[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset] != "mvexpand overflow" {
		t.Fatalf("third mvexpand diagnostic range = %#v", diagnostic.Range)
	}
}
