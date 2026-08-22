package spl

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

// This file is intentionally command-spanning. The ordinary parser fixtures
// pin each happy-path grammar in isolation; these cases try to make one pipeline
// command consume the following command, reinterpret a quoted value as a
// field, or evade a source-dependent resource ceiling.  Reflection keeps the
// tests compilable while the ten AST types land in separate implementation
// phases, without weakening the asserted public Command contract.

func TestPipelineCommandsParseAsSourceLocatedStages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		source  string
		command string
	}{
		{name: "regex raw unicode", source: `index=main | ReGeX "(?i)timeout|拒否"`, command: "regex"},
		{name: "regex negative field", source: `index=main | regex message!="^blocked$"`, command: "regex"},
		{name: "reverse", source: `index=main | ReVeRsE`, command: "reverse"},
		{name: "accum replace", source: `index=main | accum bytes`, command: "accum"},
		{name: "accum alias", source: `index=main | accum bytes AS running_bytes`, command: "accum"},
		{name: "strcat default", source: `index=main | strcat host ":" port endpoint`, command: "strcat"},
		{name: "strcat all required", source: `index=main | strcat allrequired=true service "／" route route_key`, command: "strcat"},
		{name: "addinfo", source: `index=main | AdDiNfO`, command: "addinfo"},
		{name: "fillnull default", source: `index=main | fillnull host route`, command: "fillnull"},
		{name: "fillnull explicit empty", source: `index=main | fillnull value="" host route`, command: "fillnull"},
		{name: "addtotals default", source: `index=main | addtotals bytes_in bytes_out`, command: "addtotals"},
		{name: "addtotals options", source: `index=main | addtotals row=true col=false fieldname=total_bytes bytes_in bytes_out`, command: "addtotals"},
		{name: "delta default", source: `index=main | delta bytes`, command: "delta"},
		{name: "delta alias lag", source: `index=main | delta bytes AS change p=3`, command: "delta"},
		{name: "makemv defaults", source: `index=main | makemv tags`, command: "makemv"},
		{name: "makemv unicode delimiter", source: `index=main | makemv delim="💥界" allowempty=true tags`, command: "makemv"},
		{name: "mvexpand default", source: `index=main | mvexpand tags`, command: "mvexpand"},
		{name: "mvexpand explicit unlimited", source: `index=main | mvexpand tags limit=0`, command: "mvexpand"},
		{name: "mvexpand bounded", source: `index=main | mvexpand tags limit=100`, command: "mvexpand"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command := requirePipelineLastCommand(t, test.source, test.command)
			start := strings.LastIndex(strings.ToLower(test.source), test.command)
			if start < 0 {
				t.Fatalf("test source %q does not contain %q", test.source, test.command)
			}
			assertPipelineRangeText(t, test.source, command.SourceRange(), test.source[start:])
		})
	}
}

func TestPipelineCommandASTDoesNotLoseAuthoredSemantics(t *testing.T) {
	t.Parallel()

	regexSource := `index=main | regex Message!="(?i)timeout|拒否"`
	regex := requirePipelineLastCommand(t, regexSource, "regex")
	assertPipelineConcreteType(t, regex, "RegexCommand")
	assertPipelineStringField(t, regex, "Field", "Message")
	assertPipelineStringField(t, regex, "Pattern", `(?i)timeout|拒否`)
	assertPipelineBoolField(t, regex, "Negated", true)
	assertPipelineRangeFieldText(t, regexSource, regex, "FieldRange", "Message")
	assertPipelineRangeFieldText(t, regexSource, regex, "PatternRange", `"(?i)timeout|拒否"`)

	defaultRegex := requirePipelineLastCommand(t, `index=main | regex "x"`, "regex")
	assertPipelineStringField(t, defaultRegex, "Field", "_raw")
	assertPipelineBoolField(t, defaultRegex, "Negated", false)

	reverse := requirePipelineLastCommand(t, `index=main | reverse`, "reverse")
	assertPipelineConcreteType(t, reverse, "ReverseCommand")

	accumSource := `index=main | accum Payload.Bytes AS Running`
	accum := requirePipelineLastCommand(t, accumSource, "accum")
	assertPipelineConcreteType(t, accum, "AccumCommand")
	assertPipelineStringField(t, accum, "Field", "Payload.Bytes")
	assertPipelineStringField(t, accum, "Output", "Running")
	assertPipelineBoolField(t, accum, "ExplicitOutput", true)
	assertPipelineRangeFieldText(t, accumSource, accum, "FieldRange", "Payload.Bytes")
	assertPipelineRangeFieldText(t, accumSource, accum, "OutputRange", "Running")
	replacingAccum := requirePipelineLastCommand(t, `index=main | accum Payload.Bytes`, "accum")
	assertPipelineStringField(t, replacingAccum, "Output", "Payload.Bytes")
	assertPipelineBoolField(t, replacingAccum, "ExplicitOutput", false)

	strcatSource := `index=main | strcat allrequired=true Host "💥" route destination`
	strcat := requirePipelineLastCommand(t, strcatSource, "strcat")
	assertPipelineConcreteType(t, strcat, "StrcatCommand")
	assertPipelineStringField(t, strcat, "Destination", "destination")
	assertPipelineBoolField(t, strcat, "AllRequired", true)
	assertPipelineBoolField(t, strcat, "AllRequiredSpecified", true)
	assertPipelineRangeFieldText(t, strcatSource, strcat, "AllRequiredRange", "allrequired=true")
	assertPipelineRangeFieldText(t, strcatSource, strcat, "DestinationRange", "destination")
	assertPipelineStrcatOperands(t, strcat, []pipelineOperandExpectation{
		{field: "Host"},
		{literal: "💥"},
		{field: "route"},
	})
	defaultStrcat := requirePipelineLastCommand(t, `index=main | strcat a b out`, "strcat")
	assertPipelineBoolField(t, defaultStrcat, "AllRequired", false)
	assertPipelineBoolField(t, defaultStrcat, "AllRequiredSpecified", false)

	addinfo := requirePipelineLastCommand(t, `index=main | addinfo`, "addinfo")
	assertPipelineConcreteType(t, addinfo, "AddInfoCommand")
}

func TestPipelineCommandASTRetainsOptionsAndDefaults(t *testing.T) {
	t.Parallel()

	fillSource := `index=main | fillnull value="未知" Host route`
	fill := requirePipelineLastCommand(t, fillSource, "fillnull")
	assertPipelineConcreteType(t, fill, "FillNullCommand")
	assertPipelineLiteralOrStringField(t, fill, "Value", "未知")
	assertPipelineStringSliceField(t, fill, "Fields", []string{"Host", "route"})
	defaultFill := requirePipelineLastCommand(t, `index=main | fillnull host`, "fillnull")
	assertPipelineLiteralOrStringField(t, defaultFill, "Value", "0")

	totals := requirePipelineLastCommand(t,
		`index=main | addtotals row=true col=false fieldname=sum_bytes bytes_in bytes_out`,
		"addtotals",
	)
	assertPipelineConcreteType(t, totals, "AddTotalsCommand")
	assertPipelineStringSliceField(t, totals, "Fields", []string{"bytes_in", "bytes_out"})
	assertPipelineStringField(t, totals, "Output", "sum_bytes")
	defaultTotals := requirePipelineLastCommand(t, `index=main | addtotals x`, "addtotals")
	assertPipelineStringField(t, defaultTotals, "Output", "Total")

	delta := requirePipelineLastCommand(t, `index=main | delta Bytes AS Difference p=7`, "delta")
	assertPipelineConcreteType(t, delta, "DeltaCommand")
	assertPipelineStringField(t, delta, "Field", "Bytes")
	assertPipelineStringField(t, delta, "Output", "Difference")
	assertPipelineBoolField(t, delta, "OutputDefault", false)
	assertPipelineUintField(t, delta, "Previous", 7)
	defaultDelta := requirePipelineLastCommand(t, `index=main | delta Bytes`, "delta")
	assertPipelineStringField(t, defaultDelta, "Output", "delta(Bytes)")
	assertPipelineBoolField(t, defaultDelta, "OutputDefault", true)
	assertPipelineUintField(t, defaultDelta, "Previous", 1)

	makeMV := requirePipelineLastCommand(t,
		`index=main | makemv delim="💥界" allowempty=true Tags`,
		"makemv",
	)
	assertPipelineConcreteTypeAny(t, makeMV, "MakeMVCommand", "MakemvCommand")
	assertPipelineStringField(t, makeMV, "Field", "Tags")
	assertPipelineStringField(t, makeMV, "Delimiter", "💥界")
	assertPipelineBoolField(t, makeMV, "AllowEmpty", true)
	defaultMakeMV := requirePipelineLastCommand(t, `index=main | makemv Tags`, "makemv")
	assertPipelineStringField(t, defaultMakeMV, "Delimiter", " ")
	assertPipelineBoolField(t, defaultMakeMV, "AllowEmpty", false)

	expand := requirePipelineLastCommand(t, `index=main | mvexpand Tags limit=37`, "mvexpand")
	assertPipelineConcreteType(t, expand, "MVExpandCommand")
	assertPipelineStringField(t, expand, "Field", "Tags")
	assertPipelineUintField(t, expand, "Limit", 37)
	assertPipelineBoolField(t, expand, "LimitSpecified", true)
	defaultExpand := requirePipelineLastCommand(t, `index=main | mvexpand Tags`, "mvexpand")
	assertPipelineUintField(t, defaultExpand, "Limit", 0)
	assertPipelineBoolField(t, defaultExpand, "LimitSpecified", false)
}

func TestPipelineCommandsRejectDeferredAndAmbiguousSyntaxAtTheOffendingToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, source, rangeText string
		rangeAtLastOccurrence   bool
	}{
		{name: "regex dynamic pattern", source: `index=main | regex message=pattern`, rangeText: "pattern"},
		{name: "regex quoted command field", source: `index=main | regex 'message field'="x"`, rangeText: `'message field'`},
		{name: "regex unsupported lookahead", source: `index=main | regex message="(?=secret)"`, rangeText: `"(?=secret)"`},
		{name: "regex capture backreference", source: `index=main | regex message="(.)\\1"`, rangeText: `"(.)\\1"`},
		{name: "reverse positional argument", source: `index=main | reverse event_id`, rangeText: "event_id"},
		{name: "reverse option", source: `index=main | reverse limit=2`, rangeText: "limit"},
		{name: "accum quoted field", source: `index=main | accum 'bytes total'`, rangeText: `'bytes total'`},
		{name: "accum wildcard", source: `index=main | accum bytes*`, rangeText: "bytes*"},
		{name: "accum multiple fields", source: `index=main | accum first second`, rangeText: "second"},
		{name: "accum quoted destination", source: `index=main | accum bytes AS "running bytes"`, rangeText: `"running bytes"`},
		{name: "strcat calculated operand", source: `index=main | strcat lower(host) route out`, rangeText: "("},
		{name: "strcat wildcard operand", source: `index=main | strcat host* route out`, rangeText: "host*"},
		{name: "strcat bad boolean", source: `index=main | strcat allrequired=maybe host route out`, rangeText: "maybe"},
		{name: "strcat short true is deferred", source: `index=main | strcat allrequired=t host route out`, rangeText: "t"},
		{name: "strcat short false is deferred", source: `index=main | strcat allrequired=f host route out`, rangeText: "f"},
		{name: "strcat duplicate option", source: `index=main | strcat allrequired=true allrequired=false host route out`, rangeText: "="},
		{name: "strcat literal destination", source: `index=main | strcat host route "out"`, rangeText: `"out"`},
		{name: "strcat one source", source: `index=main | strcat host out`, rangeText: "strcat host out"},
		{name: "addinfo positional", source: `index=main | addinfo now`, rangeText: "now"},
		{name: "addinfo option", source: `index=main | addinfo sid=false`, rangeText: "sid"},
		{name: "fillnull requires literal value", source: `index=main | fillnull value=dynamic host`, rangeText: "dynamic"},
		{name: "fillnull wildcard", source: `index=main | fillnull host*`, rangeText: "host*"},
		{name: "fillnull quoted field", source: `index=main | fillnull 'host name'`, rangeText: `'host name'`},
		{name: "fillnull duplicate field", source: `index=main | fillnull host host`, rangeText: "host", rangeAtLastOccurrence: true},
		{name: "fillnull late option", source: `index=main | fillnull host value="x"`, rangeText: "value"},
		{name: "addtotals row false", source: `index=main | addtotals row=false bytes`, rangeText: "false"},
		{name: "addtotals short row true is deferred", source: `index=main | addtotals row=t bytes`, rangeText: "t"},
		{name: "addtotals short col false is deferred", source: `index=main | addtotals col=f bytes`, rangeText: "f"},
		{name: "addtotals col true", source: `index=main | addtotals col=true bytes`, rangeText: "true"},
		{name: "addtotals wildcard", source: `index=main | addtotals bytes*`, rangeText: "bytes*"},
		{name: "addtotals deferred label", source: `index=main | addtotals label="sum" bytes`, rangeText: "label"},
		{name: "addtotals duplicate row", source: `index=main | addtotals row=true row=true bytes`, rangeText: "row", rangeAtLastOccurrence: true},
		{name: "addtotals duplicate col", source: `index=main | addtotals col=false col=false bytes`, rangeText: "col", rangeAtLastOccurrence: true},
		{name: "addtotals duplicate fieldname", source: `index=main | addtotals fieldname=first fieldname=second bytes`, rangeText: "fieldname", rangeAtLastOccurrence: true},
		{name: "addtotals duplicate field", source: `index=main | addtotals bytes bytes`, rangeText: "bytes", rangeAtLastOccurrence: true},
		{name: "addtotals late option", source: `index=main | addtotals bytes fieldname=total`, rangeText: "fieldname"},
		{name: "delta zero lag", source: `index=main | delta bytes p=0`, rangeText: "0"},
		{name: "delta negative lag", source: `index=main | delta bytes p=-1`, rangeText: "-"},
		{name: "delta dynamic lag", source: `index=main | delta bytes p=lag`, rangeText: "lag"},
		{name: "delta multiple fields", source: `index=main | delta bytes packets`, rangeText: "packets"},
		{name: "delta duplicate AS", source: `index=main | delta bytes AS first AS second`, rangeText: "AS", rangeAtLastOccurrence: true},
		{name: "delta duplicate p", source: `index=main | delta bytes p=1 p=2`, rangeText: "p", rangeAtLastOccurrence: true},
		{name: "makemv empty delimiter", source: `index=main | makemv delim="" tags`, rangeText: `""`},
		{name: "makemv dynamic delimiter", source: `index=main | makemv delim=separator tags`, rangeText: "separator"},
		{name: "makemv tokenizer deferred", source: `index=main | makemv tokenizer="[^,]+" tags`, rangeText: "tokenizer"},
		{name: "makemv setsv deferred", source: `index=main | makemv setsv=true tags`, rangeText: "setsv"},
		{name: "makemv quoted field", source: `index=main | makemv 'tag list'`, rangeText: `'tag list'`},
		{name: "makemv duplicate delimiter", source: `index=main | makemv delim="," delim=":" tags`, rangeText: "delim", rangeAtLastOccurrence: true},
		{name: "makemv duplicate allowempty", source: `index=main | makemv allowempty=true allowempty=false tags`, rangeText: "allowempty", rangeAtLastOccurrence: true},
		{name: "makemv short allowempty true is deferred", source: `index=main | makemv allowempty=t tags`, rangeText: "t"},
		{name: "makemv short allowempty false is deferred", source: `index=main | makemv allowempty=f tags`, rangeText: "f"},
		{name: "makemv late option", source: `index=main | makemv tags allowempty=true`, rangeText: "allowempty"},
		{name: "makemv canonical internal field", source: `index=main | makemv _time`, rangeText: "_time"},
		{name: "mvexpand negative limit", source: `index=main | mvexpand tags limit=-1`, rangeText: "-"},
		{name: "mvexpand dynamic limit", source: `index=main | mvexpand tags limit=count`, rangeText: "count"},
		{name: "mvexpand wildcard", source: `index=main | mvexpand tags*`, rangeText: "tags*"},
		{name: "mvexpand second field", source: `index=main | mvexpand tags zones`, rangeText: "zones"},
		{name: "mvexpand duplicate limit", source: `index=main | mvexpand tags limit=1 limit=2`, rangeText: "limit", rangeAtLastOccurrence: true},
		{name: "mvexpand canonical internal field", source: `index=main | mvexpand _time`, rangeText: "_time"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			diagnostic := assertPipelineRecognizedRejection(t, test.source, test.rangeText)
			if test.rangeAtLastOccurrence {
				wantOffset := strings.LastIndex(test.source, test.rangeText)
				if diagnostic.Range.Start.Offset != wantOffset {
					t.Fatalf("diagnostic starts at %d, want final occurrence at %d", diagnostic.Range.Start.Offset, wantOffset)
				}
			}
		})
	}
}

func TestPipelineCommandsHaveExactEmptyTailDiagnostics(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, source, code, rangeText string
		accept                        bool
	}{
		{name: "regex", source: `index=main | regex`, code: "SPL_EXPECTED_REGEX_PATTERN"},
		{name: "reverse", source: `index=main | reverse`, accept: true},
		{name: "accum", source: `index=main | accum`, code: "SPL_EXPECTED_FIELD"},
		{name: "strcat", source: `index=main | strcat`, code: "SPL_EXPECTED_FIELD"},
		{name: "addinfo", source: `index=main | addinfo`, accept: true},
		{name: "fillnull", source: `index=main | fillnull`, code: "SPL_EXPECTED_FIELD"},
		{name: "addtotals", source: `index=main | addtotals`, code: "SPL_EXPECTED_FIELD"},
		{name: "delta", source: `index=main | delta`, code: "SPL_EXPECTED_FIELD"},
		{name: "makemv", source: `index=main | makemv`, code: "SPL_EXPECTED_FIELD"},
		{name: "mvexpand", source: `index=main | mvexpand`, code: "SPL_EXPECTED_FIELD"},
		{name: "accum missing output", source: `index=main | accum bytes AS`, code: "SPL_EXPECTED_FIELD", rangeText: "AS"},
		{name: "delta missing output", source: `index=main | delta bytes AS`, code: "SPL_EXPECTED_FIELD", rangeText: "AS"},
		{name: "fillnull option without field", source: `index=main | fillnull value="x"`, code: "SPL_EXPECTED_FIELD"},
		{name: "addtotals option without field", source: `index=main | addtotals fieldname=total`, code: "SPL_EXPECTED_FIELD"},
		{name: "makemv option without field", source: `index=main | makemv delim=","`, code: "SPL_EXPECTED_FIELD"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query, err := Parse(test.source)
			if test.accept {
				if err != nil {
					t.Fatalf("Parse(%q): %v", test.source, err)
				}
				if len(query.Commands) != 1 || query.Commands[0].Name() != test.name {
					t.Fatalf("Parse(%q) commands = %#v, want one %s", test.source, query.Commands, test.name)
				}
				return
			}

			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("Parse(%q) error = %#v, want %s", test.source, err, test.code)
			}
			wantStart := len(test.source)
			wantEnd := len(test.source)
			if test.rangeText != "" {
				wantStart = strings.LastIndex(test.source, test.rangeText)
				wantEnd = wantStart + len(test.rangeText)
			}
			if diagnostic.Range.Start.Offset != wantStart || diagnostic.Range.End.Offset != wantEnd {
				t.Fatalf(
					"Parse(%q) diagnostic range = %#v, want offsets [%d,%d)",
					test.source,
					diagnostic.Range,
					wantStart,
					wantEnd,
				)
			}
			assertPipelineRangeText(t, test.source, diagnostic.Range, test.rangeText)
		})
	}
}

func TestPipelineSourceDeterminedResourceBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("strcat operand and aggregate budgets", func(t *testing.T) {
		strcatOperands := make([]string, 32)
		for index := range strcatOperands {
			strcatOperands[index] = "field" + strconv.Itoa(index)
		}
		if _, err := Parse(`index=main | strcat ` + strings.Join(strcatOperands, " ") + ` output`); err != nil {
			t.Fatalf("strcat at 32 operands: %v", err)
		}
		assertPipelineResourceRejection(
			t,
			`index=main | strcat `+strings.Join(append(strcatOperands, "overflow"), " ")+` output`,
			"overflow",
		)

		// Eight individually valid 32-source stages exactly consume the
		// 256-occurrence query budget. A ninth, otherwise valid two-source
		// stage must be rejected atomically at that complete stage.
		var boundary strings.Builder
		boundary.WriteString("index=main")
		for stage := range MaximumConcatenationOperandsPerQuery / MaximumConcatenationOperands {
			boundary.WriteString(` | strcat `)
			boundary.WriteString(strings.Join(strcatOperands, " "))
			boundary.WriteString(` output`)
			boundary.WriteString(strconv.Itoa(stage))
		}
		if _, err := Parse(boundary.String()); err != nil {
			t.Fatalf("strcat at aggregate operand boundary: %v", err)
		}
		overflowStage := `strcat host "/" overflow_output`
		assertPipelineResourceRejection(t, boundary.String()+` | `+overflowStage, overflowStage)
	})

	for _, command := range []string{"fillnull", "addtotals"} {
		t.Run(command+" field budget", func(t *testing.T) {
			fields := make([]string, 65)
			for index := range fields {
				fields[index] = "field" + strconv.Itoa(index)
			}
			if _, err := Parse(`index=main | ` + command + ` ` + strings.Join(fields[:64], " ")); err != nil {
				t.Fatalf("%s at 64 fields: %v", command, err)
			}
			assertPipelineResourceRejection(
				t,
				`index=main | `+command+` `+strings.Join(fields, " "),
				fields[64],
			)
		})
	}

	t.Run("delta lag budget", func(t *testing.T) {
		if _, err := Parse(fmt.Sprintf(
			`index=main | delta value p=%d`,
			MaximumStreamStatsWindow,
		)); err != nil {
			t.Fatalf("delta at maximum lag: %v", err)
		}
		assertPipelineResourceRejection(
			t,
			fmt.Sprintf(`index=main | delta value p=%d`, MaximumStreamStatsWindow+1),
			strconv.FormatUint(MaximumStreamStatsWindow+1, 10),
		)
	})

	// Four compact repetitions fit the per-pattern work budget. Five such
	// regex commands exceed the shared query budget even though each stage is
	// independently valid and the authored source is small.
	t.Run("regex per-command and query budgets", func(t *testing.T) {
		pattern := strings.Repeat("a{1000}", 4)
		if _, err := Parse(`index=main | regex message="` + pattern + `"`); err != nil {
			t.Fatalf("regex at per-pattern work boundary fixture: %v", err)
		}
		var aggregate strings.Builder
		aggregate.WriteString("index=main")
		for range 5 {
			aggregate.WriteString(` | regex message="`)
			aggregate.WriteString(pattern)
			aggregate.WriteString(`"`)
		}
		diagnosticSource := `"` + pattern + `"`
		_, err := Parse(aggregate.String())
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
			t.Fatalf("Parse aggregate regex budget error = %v, want SPL_QUERY_TOO_COMPLEX", err)
		}
		assertPipelineRangeText(t, aggregate.String(), diagnostic.Range, diagnosticSource)
		if wantOffset := strings.LastIndex(aggregate.String(), diagnosticSource); diagnostic.Range.Start.Offset != wantOffset {
			t.Fatalf("aggregate regex diagnostic starts at %d, want fifth program at %d", diagnostic.Range.Start.Offset, wantOffset)
		}

		tooLongPattern := strings.Repeat("x", splregex.MaximumMatchPatternBytes+1)
		assertPipelineResourceRejection(
			t,
			`index=main | regex message="`+tooLongPattern+`"`,
			`"`+tooLongPattern+`"`,
		)
	})

	// The delimiter is deliberately well below the global 16 KiB SPL source
	// ceiling. Rejection therefore proves a command-local UTF-8 byte budget.
	t.Run("makemv delimiter byte budget", func(t *testing.T) {
		boundaryDelimiter := strings.Repeat("界", 341) + "a" // 1,024 UTF-8 bytes.
		if len(boundaryDelimiter) != MaximumMakeMVDelimiterBytes {
			t.Fatalf("delimiter fixture = %d bytes, want %d", len(boundaryDelimiter), MaximumMakeMVDelimiterBytes)
		}
		if _, err := Parse(`index=main | makemv delim="` + boundaryDelimiter + `" tags`); err != nil {
			t.Fatalf("makemv delimiter at byte ceiling: %v", err)
		}
		oversizedDelimiter := boundaryDelimiter + "b"
		assertPipelineResourceRejection(
			t,
			`index=main | makemv delim="`+oversizedDelimiter+`" tags`,
			`"`+oversizedDelimiter+`"`,
		)
	})

	t.Run("mvexpand integer overflow", func(t *testing.T) {
		if _, err := Parse(fmt.Sprintf(
			`index=main | mvexpand tags limit=%d`,
			MaximumMVExpandLimit,
		)); err != nil {
			t.Fatalf("mvexpand at authored limit ceiling: %v", err)
		}
		assertPipelineResourceRejection(
			t,
			fmt.Sprintf(`index=main | mvexpand tags limit=%d`, MaximumMVExpandLimit+1),
			strconv.FormatUint(MaximumMVExpandLimit+1, 10),
		)
		assertPipelineResourceRejection(
			t,
			`index=main | mvexpand tags limit=18446744073709551616`,
			"18446744073709551616",
		)
	})
}

func TestPipelineRepeatedOrderAndMultivalueStagesRemainDistinct(t *testing.T) {
	t.Parallel()

	source := `index=main | sort 0 +event_id | reverse | reverse | accum n AS running | delta running AS step p=2 | makemv delim="," tags | mvexpand tags limit=3 | reverse | mvexpand zones | table event_id tags zones running step`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse stacked order/multivalue pipeline: %v", err)
	}
	want := []string{"sort", "reverse", "reverse", "accum", "delta", "makemv", "mvexpand", "reverse", "mvexpand", "table"}
	if len(query.Commands) != len(want) {
		t.Fatalf("commands = %d, want %d: %#v", len(query.Commands), len(want), query.Commands)
	}
	for index, command := range query.Commands {
		if command.Name() != want[index] {
			t.Fatalf("command %d = %q (%T), want %q", index, command.Name(), command, want[index])
		}
		if command.SourceRange() == (Range{}) {
			t.Fatalf("command %d (%s) has an empty source range", index, command.Name())
		}
	}
}

func TestPipelineCommandsAreDiscoverableAndHaveBoundedSuggestionContexts(t *testing.T) {
	t.Parallel()

	commands := []string{
		"regex", "reverse", "accum", "strcat", "addinfo",
		"fillnull", "addtotals", "delta", "makemv", "mvexpand",
	}
	for _, command := range commands {
		t.Run(command+" command completion", func(t *testing.T) {
			t.Parallel()
			prefix := command[:len(command)-1]
			source := "index=main | " + prefix
			result := Suggest(source, len(source), MaximumSuggestionLimit)
			if result.Diagnostic != nil {
				t.Fatalf("Suggest(%q): %v", source, result.Diagnostic)
			}
			labels := suggestionLabels(result.Suggestions)
			if !slices.Contains(labels, command) {
				t.Fatalf("command suggestions for %q = %v, want %q", prefix, labels, command)
			}
			index := slices.Index(labels, command)
			if index < 0 || result.Suggestions[index].Insertion == "" ||
				result.Suggestions[index].Detail == "" {
				t.Fatalf("completion metadata for %q = %#v", command, result.Suggestions[index])
			}
			inserted := "index=main | " + result.Suggestions[index].Insertion
			parsed, parseErr := Parse(inserted)
			if parseErr != nil || len(parsed.Commands) != 1 || parsed.Commands[0].Name() != command {
				t.Fatalf("completion insertion %q for %q does not round-trip: query=%#v error=%v", result.Suggestions[index].Insertion, command, parsed, parseErr)
			}
			wantStart := strings.LastIndex(source, prefix)
			if got := result.Suggestions[index].Replacement; got.Start.Offset != wantStart || got.End.Offset != len(source) {
				t.Fatalf("completion replacement = %#v, want byte range [%d,%d)", got, wantStart, len(source))
			}
		})
	}

	for _, command := range []string{
		"regex", "accum", "strcat", "fillnull", "addtotals", "delta", "makemv", "mvexpand",
	} {
		t.Run(command+" field context", func(t *testing.T) {
			t.Parallel()
			source := "index=main | " + command + " pa界"
			context, diagnostic := AnalyzeSuggestionContext(source, len(source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
			}
			if !context.Allows(SuggestionKindField) || context.AllowsQuotedScalarFields || context.Prefix != "pa界" {
				t.Fatalf("%s context = %#v, want exact unquoted field completion", command, context)
			}
			wantStart := strings.LastIndex(source, "pa界")
			if context.Replacement.Start.Offset != wantStart || context.Replacement.End.Offset != len(source) {
				t.Fatalf("Unicode replacement = %#v, want byte range [%d,%d)", context.Replacement, wantStart, len(source))
			}
		})
	}

	for _, command := range []string{"reverse", "addinfo"} {
		t.Run(command+" argument-free context", func(t *testing.T) {
			t.Parallel()
			source := "index=main | " + command + " "
			context, diagnostic := AnalyzeSuggestionContext(source, len(source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
			}
			if len(context.Kinds) != 0 || context.Prefix != "" {
				t.Fatalf("%s argument-free context = %#v", command, context)
			}
		})
	}
}

func TestPipelineOptionAndAliasSuggestionsFollowTheAcceptedGrammar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		source       string
		wantField    bool
		wantKeywords []string
	}{
		{name: "accum alias", source: `index=main | accum bytes `, wantKeywords: []string{"AS"}},
		{name: "accum destination", source: `index=main | accum bytes AS `, wantField: true},
		{name: "strcat leading option", source: `index=main | strcat `, wantField: true, wantKeywords: []string{"allrequired="}},
		{name: "fillnull leading option", source: `index=main | fillnull `, wantField: true, wantKeywords: []string{"value="}},
		{name: "fillnull after option", source: `index=main | fillnull value="unknown" `, wantField: true},
		{name: "addtotals leading options", source: `index=main | addtotals `, wantField: true, wantKeywords: []string{"col=", "fieldname=", "row="}},
		{name: "addtotals after options", source: `index=main | addtotals row=true col=false fieldname=total `, wantField: true},
		{name: "delta output or lag", source: `index=main | delta bytes `, wantKeywords: []string{"AS", "p="}},
		{name: "delta destination", source: `index=main | delta bytes AS `, wantField: true},
		{name: "delta lag after destination", source: `index=main | delta bytes AS change `, wantKeywords: []string{"p="}},
		{name: "delta alias after leading lag", source: `index=main | delta bytes p=1 `, wantKeywords: []string{"AS"}},
		{name: "makemv leading options", source: `index=main | makemv `, wantField: true, wantKeywords: []string{"allowempty=", "delim="}},
		{name: "makemv remaining option", source: `index=main | makemv delim="," `, wantField: true, wantKeywords: []string{"allowempty="}},
		{name: "makemv field only after options", source: `index=main | makemv delim="," allowempty=true `, wantField: true},
		{name: "mvexpand limit", source: `index=main | mvexpand tags `, wantKeywords: []string{"limit="}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext(%q): %v", test.source, diagnostic)
			}
			if context.Allows(SuggestionKindField) != test.wantField || context.AllowsQuotedScalarFields {
				t.Fatalf("field context for %q = %#v, want exact-unquoted=%t", test.source, context, test.wantField)
			}
			wantKeywords := slices.Clone(test.wantKeywords)
			gotKeywords := slices.Clone(context.Keywords)
			slices.Sort(wantKeywords)
			slices.Sort(gotKeywords)
			if !slices.Equal(gotKeywords, wantKeywords) || context.Allows(SuggestionKindKeyword) != (len(wantKeywords) != 0) {
				t.Fatalf("keyword context for %q = kinds %v keywords %v, want %v", test.source, context.Kinds, context.Keywords, test.wantKeywords)
			}
			if context.Prefix != "" || context.Replacement.Start.Offset != len(test.source) || context.Replacement.End.Offset != len(test.source) {
				t.Fatalf("append-only context for %q = %#v", test.source, context)
			}
		})
	}
}

func TestPipelineSuggestionContextsDoNotLeakAcrossValueOrTerminalGrammarPositions(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | regex message=`,
		`index=main | strcat allrequired=`,
		`index=main | fillnull value=`,
		`index=main | addtotals row=`,
		`index=main | addtotals col=`,
		`index=main | addtotals fieldname=`,
		`index=main | makemv allowempty=`,
		`index=main | makemv delim=`,
		`index=main | makemv tags `,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			context, diagnostic := AnalyzeSuggestionContext(source, len(source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext(%q): %v", source, diagnostic)
			}
			if context.Allows(SuggestionKindField) ||
				context.Allows(SuggestionKindKeyword) ||
				len(context.Keywords) != 0 {
				t.Fatalf("value/terminal context for %q leaked fields/options: %#v", source, context)
			}
			result := Suggest(source, len(source), MaximumSuggestionLimit)
			if result.Diagnostic != nil {
				t.Fatalf("Suggest(%q): %v", source, result.Diagnostic)
			}
			for _, suggestion := range result.Suggestions {
				if suggestion.Kind == SuggestionKindField || suggestion.Kind == SuggestionKindKeyword {
					t.Fatalf("value/terminal suggestions for %q leaked %#v", source, suggestion)
				}
			}
		})
	}
}

func TestPipelineAddTotalsSuggestionsRetainEveryUnusedLeadingOption(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source       string
		wantKeywords []string
	}{
		{source: `index=main | addtotals `, wantKeywords: []string{"col=", "fieldname=", "row="}},
		{source: `index=main | addtotals row=true `, wantKeywords: []string{"col=", "fieldname="}},
		{source: `index=main | addtotals col=false `, wantKeywords: []string{"fieldname=", "row="}},
		{source: `index=main | addtotals fieldname=total `, wantKeywords: []string{"col=", "row="}},
		{source: `index=main | addtotals row=true col=false `, wantKeywords: []string{"fieldname="}},
		{source: `index=main | addtotals row=true fieldname=total `, wantKeywords: []string{"col="}},
		{source: `index=main | addtotals col=false fieldname=total `, wantKeywords: []string{"row="}},
		{source: `index=main | addtotals row=true col=false fieldname=total `},
	} {
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext(%q): %v", test.source, diagnostic)
			}
			if !context.Allows(SuggestionKindField) || context.AllowsQuotedScalarFields {
				t.Fatalf("addtotals field context for %q = %#v", test.source, context)
			}
			got := slices.Clone(context.Keywords)
			want := slices.Clone(test.wantKeywords)
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) || context.Allows(SuggestionKindKeyword) != (len(want) > 0) {
				t.Fatalf("addtotals remaining options for %q = %v, want %v", test.source, got, want)
			}
		})
	}
}

func TestPipelineEveryAdvertisedKeywordInsertionHasAParseableContinuation(t *testing.T) {
	t.Parallel()

	contexts := []string{
		`index=main | accum bytes `,
		`index=main | strcat `,
		`index=main | fillnull `,
		`index=main | addtotals `,
		`index=main | delta bytes `,
		`index=main | makemv `,
		`index=main | mvexpand tags `,
	}
	continuations := map[string][]string{
		"AS":           {"destination"},
		"allrequired=": {`true host "/" route endpoint`, ` host "/" route endpoint`},
		"value=":       {`"0" status`, ` status`},
		"col=":         {`false bytes`, ` bytes`},
		"fieldname=":   {`Total bytes`, ` bytes`},
		"row=":         {`true bytes`, ` bytes`},
		"p=":           {`1`, ``},
		"allowempty=":  {`true tags`, ` tags`},
		"delim=":       {`"," tags`, ` tags`},
		"limit=":       {`100`, ``},
	}

	for _, source := range contexts {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			result := Suggest(source, len(source), MaximumSuggestionLimit)
			if result.Diagnostic != nil {
				t.Fatalf("Suggest(%q): %v", source, result.Diagnostic)
			}
			advertised := make(map[string]Suggestion)
			for _, suggestion := range result.Suggestions {
				if suggestion.Kind == SuggestionKindKeyword {
					advertised[suggestion.Label] = suggestion
				}
			}
			for _, keyword := range result.Context.Keywords {
				t.Run(keyword, func(t *testing.T) {
					suggestion, ok := advertised[keyword]
					if !ok {
						t.Fatalf("context %q advertises keyword %q without catalog insertion; suggestions=%#v", source, keyword, result.Suggestions)
					}
					suffixes, known := continuations[keyword]
					if !known {
						t.Fatalf("pipeline context %q advertised untested keyword %q", source, keyword)
					}
					parseable := false
					var failures []string
					for _, suffix := range suffixes {
						candidate := source[:suggestion.Replacement.Start.Offset] +
							suggestion.Insertion + suffix +
							source[suggestion.Replacement.End.Offset:]
						_, err := Parse(candidate)
						if err == nil {
							parseable = true
							break
						}
						failures = append(failures, fmt.Sprintf("%q: %v", candidate, err))
					}
					if !parseable {
						t.Fatalf("keyword %q insertion %q from %q has no parseable continuation:\n%s", keyword, suggestion.Insertion, source, strings.Join(failures, "\n"))
					}
				})
			}
		})
	}
}

func TestPipelineCommandsPreserveTheCurrentPublicRelationShape(t *testing.T) {
	t.Parallel()

	commands := []string{
		`regex message="x"`, `reverse`, `accum total AS running`,
		`strcat host ":" route endpoint`, `addinfo`,
		`fillnull value="0" optional`, `addtotals fieldname=sum a b`,
		`delta total AS difference`, `makemv delim="," tags`, `mvexpand tags limit=2`,
	}
	for _, command := range commands {
		t.Run(command, func(t *testing.T) {
			t.Parallel()
			for _, prefix := range []struct {
				source string
				want   ResultShape
			}{
				{source: `index=main`, want: ResultShape{Kind: ResultKindEvents}},
				{source: `index=main | table total host route optional a b tags message`, want: ResultShape{Kind: ResultKindStatistics}},
				{source: `index=main | timechart span=5m count BY level`, want: ResultShape{Kind: ResultKindTimeSeries, RuntimeNamedColumns: true}},
			} {
				query, err := Parse(prefix.source + " | " + command)
				if err != nil {
					t.Fatalf("Parse after shape prefix %q: %v", prefix.source, err)
				}
				if got := ClassifyResultShape(query); got != prefix.want {
					t.Fatalf("shape after %q = %#v, want %#v", prefix.source, got, prefix.want)
				}
			}
		})
	}
}

type pipelineOperandExpectation struct {
	field   string
	literal string
}

func requirePipelineLastCommand(t *testing.T, source, wantName string) Command {
	t.Helper()
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse(%q): %v", source, err)
	}
	if len(query.Commands) == 0 {
		t.Fatalf("Parse(%q) returned no commands", source)
	}
	command := query.Commands[len(query.Commands)-1]
	if command.Name() != wantName {
		t.Fatalf("last command = %q (%T), want %q", command.Name(), command, wantName)
	}
	return command
}

func assertPipelineRecognizedRejection(t *testing.T, source, wantRangeText string) *Diagnostic {
	t.Helper()
	_, err := Parse(source)
	if err == nil {
		t.Fatalf("Parse(%q) succeeded, want deliberate rejection", source)
	}
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Parse(%q) error = %T %v, want *Diagnostic", source, err, err)
	}
	if diagnostic.Code == "SPL_UNSUPPORTED_COMMAND" {
		t.Fatalf("Parse(%q) still treats a pipeline command as unsupported: %v", source, diagnostic)
	}
	assertPipelineRangeText(t, source, diagnostic.Range, wantRangeText)
	return diagnostic
}

func assertPipelineResourceRejection(t *testing.T, source, wantRangeText string) {
	t.Helper()
	_, err := Parse(source)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("Parse resource boundary error = %v, want SPL_QUERY_TOO_COMPLEX", err)
	}
	assertPipelineRangeText(t, source, diagnostic.Range, wantRangeText)
}

func assertPipelineRangeText(t *testing.T, source string, sourceRange Range, want string) {
	t.Helper()
	if sourceRange.Start.Offset < 0 || sourceRange.End.Offset < sourceRange.Start.Offset ||
		sourceRange.End.Offset > len(source) {
		t.Fatalf("invalid source range %#v for %d-byte source", sourceRange, len(source))
	}
	if got := source[sourceRange.Start.Offset:sourceRange.End.Offset]; got != want {
		t.Fatalf("source range %#v = %q, want %q", sourceRange, got, want)
	}
}

func assertPipelineConcreteType(t *testing.T, command Command, want string) {
	t.Helper()
	assertPipelineConcreteTypeAny(t, command, want)
}

func assertPipelineConcreteTypeAny(t *testing.T, command Command, wants ...string) {
	t.Helper()
	typeOf := reflect.TypeOf(command)
	if typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	if slices.Contains(wants, typeOf.Name()) {
		return
	}
	t.Fatalf("command type = %s, want one of %v", typeOf.Name(), wants)
}

func pipelineCommandValue(t *testing.T, command Command) reflect.Value {
	t.Helper()
	value := reflect.ValueOf(command)
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			t.Fatal("command is a nil pointer")
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		t.Fatalf("command value = %s, want struct", value.Kind())
	}
	return value
}

func pipelineRequiredField(t *testing.T, command Command, name string) reflect.Value {
	t.Helper()
	field := pipelineCommandValue(t, command).FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("%T has no %s field", command, name)
	}
	return field
}

func assertPipelineStringField(t *testing.T, command Command, name, want string) {
	t.Helper()
	field := pipelineRequiredField(t, command, name)
	if field.Kind() != reflect.String || field.String() != want {
		t.Fatalf("%T.%s = %#v, want %q", command, name, field.Interface(), want)
	}
}

func assertPipelineBoolField(t *testing.T, command Command, name string, want bool) {
	t.Helper()
	field := pipelineRequiredField(t, command, name)
	if field.Kind() != reflect.Bool || field.Bool() != want {
		t.Fatalf("%T.%s = %#v, want %t", command, name, field.Interface(), want)
	}
}

func assertPipelineUintField(t *testing.T, command Command, name string, want uint64) {
	t.Helper()
	field := pipelineRequiredField(t, command, name)
	if field.Kind() < reflect.Uint || field.Kind() > reflect.Uint64 || field.Uint() != want {
		t.Fatalf("%T.%s = %#v, want %d", command, name, field.Interface(), want)
	}
}

func assertPipelineLiteralOrStringField(t *testing.T, command Command, name, want string) {
	t.Helper()
	field := pipelineRequiredField(t, command, name)
	if field.Kind() == reflect.String {
		if field.String() != want {
			t.Fatalf("%T.%s = %q, want %q", command, name, field.String(), want)
		}
		return
	}
	if field.Kind() == reflect.Struct {
		text := field.FieldByName("Text")
		if text.IsValid() && text.Kind() == reflect.String && text.String() == want {
			return
		}
	}
	t.Fatalf("%T.%s = %#v, want literal text %q", command, name, field.Interface(), want)
}

func assertPipelineStringSliceField(t *testing.T, command Command, name string, want []string) {
	t.Helper()
	field := pipelineRequiredField(t, command, name)
	if field.Kind() != reflect.Slice || field.Len() != len(want) {
		t.Fatalf("%T.%s = %#v, want %v", command, name, field.Interface(), want)
	}
	got := make([]string, field.Len())
	for index := 0; index < field.Len(); index++ {
		item := field.Index(index)
		if item.Kind() == reflect.String {
			got[index] = item.String()
			continue
		}
		if item.Kind() == reflect.Struct {
			for _, candidate := range []string{"Name", "Field"} {
				nameField := item.FieldByName(candidate)
				if nameField.IsValid() && nameField.Kind() == reflect.String {
					got[index] = nameField.String()
					break
				}
			}
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%T.%s names = %v, want %v", command, name, got, want)
	}
}

func assertPipelineRangeFieldText(t *testing.T, source string, command Command, name, want string) {
	t.Helper()
	field := pipelineRequiredField(t, command, name)
	rangeType := reflect.TypeFor[Range]()
	if field.Type() != rangeType {
		t.Fatalf("%T.%s has type %s, want %s", command, name, field.Type(), rangeType)
	}
	sourceRange, ok := reflect.TypeAssert[Range](field)
	if !ok {
		t.Fatalf("%T.%s cannot be read as Range", command, name)
	}
	assertPipelineRangeText(t, source, sourceRange, want)
}

func assertPipelineStrcatOperands(t *testing.T, command Command, want []pipelineOperandExpectation) {
	t.Helper()
	operands := pipelineRequiredField(t, command, "Operands")
	if operands.Kind() != reflect.Slice || operands.Len() != len(want) {
		t.Fatalf("%T.Operands = %#v, want %d operands", command, operands.Interface(), len(want))
	}
	for index := range want {
		operand := operands.Index(index)
		if operand.Kind() == reflect.Pointer {
			operand = operand.Elem()
		}
		if operand.Kind() != reflect.Struct {
			t.Fatalf("operand %d has kind %s, want struct", index, operand.Kind())
		}
		field := operand.FieldByName("Field")
		literal := operand.FieldByName("Literal")
		if !field.IsValid() || field.Kind() != reflect.String || !literal.IsValid() {
			t.Fatalf("operand %d = %#v, want field %q literal %q", index, operand.Interface(), want[index].field, want[index].literal)
		}
		literalText := ""
		literalPresent := false
		switch literal.Kind() {
		case reflect.String:
			literalText = literal.String()
			literalPresent = literalText != ""
		case reflect.Pointer:
			if !literal.IsNil() {
				if literal.Elem().Kind() != reflect.String {
					t.Fatalf("operand %d Literal points to %s, want String", index, literal.Elem().Kind())
				}
				literalText = literal.Elem().String()
				literalPresent = true
			}
		default:
			t.Fatalf("operand %d Literal has kind %s, want String or *String", index, literal.Kind())
		}
		if field.String() != want[index].field || literalText != want[index].literal {
			t.Fatalf("operand %d = %#v, want field %q literal %q", index, operand.Interface(), want[index].field, want[index].literal)
		}
		if (field.String() != "") == literalPresent {
			t.Fatalf("operand %d does not have exactly one of field/literal: %#v", index, operand.Interface())
		}
	}
}
