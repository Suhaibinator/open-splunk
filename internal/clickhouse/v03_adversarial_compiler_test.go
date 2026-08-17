package clickhouse

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestV03EveryCommandHasAnIndependentPhysicalLowering(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.SearchJobID = "v03-independent-job"
	tests := []struct {
		name       string
		source     string
		wantFields []string
		bound      string
		notSQL     string
		atomic     bool
	}{
		{
			name: "regex", source: `index=gradethis | regex message!="V03_REGEX_拒否" | table event_id`,
			wantFields: []string{"event_id"}, bound: "(?-s)V03_REGEX_拒否", notSQL: "V03_REGEX_拒否",
		},
		{
			name: "reverse", source: `index=gradethis | sort 0 +event_id | reverse | table event_id`,
			wantFields: []string{"event_id"},
		},
		{
			name: "accum", source: `index=gradethis | sort 0 +event_id | accum n AS running | table event_id running`,
			wantFields: []string{"event_id", "running"},
		},
		{
			name: "strcat", source: `index=gradethis | strcat allrequired=true host "V03_JOIN_/" route endpoint | table endpoint`,
			wantFields: []string{"endpoint"}, bound: "V03_JOIN_/",
		},
		{
			name: "addinfo", source: `index=gradethis | addinfo | table info_sid`,
			wantFields: []string{"info_sid"}, bound: scope.SearchJobID,
		},
		{
			name: "fillnull", source: `index=gradethis | fillnull value="V03_FILL_界" optional | table optional`,
			wantFields: []string{"optional"}, bound: "V03_FILL_界",
		},
		{
			name: "addtotals", source: `index=gradethis | addtotals fieldname=total a b | table total`,
			wantFields: []string{"total"}, atomic: true,
		},
		{
			name: "delta", source: `index=gradethis | sort 0 +event_id | delta n AS step p=2 | table step`,
			wantFields: []string{"step"}, atomic: true,
		},
		{
			name: "makemv", source: `index=gradethis | makemv delim="💥界" allowempty=true tags | table tags`,
			wantFields: []string{"tags"}, bound: "💥界", atomic: true,
		},
		{
			name: "mvexpand", source: `index=gradethis | mvexpand tags limit=2 | table tags`,
			wantFields: []string{"tags"}, atomic: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPLWithScope(t, test.source, scope)
			if !reflect.DeepEqual(compiled.OutputFields, test.wantFields) {
				t.Fatalf("OutputFields = %v, want %v", compiled.OutputFields, test.wantFields)
			}
			if compiled.RequiresAtomicResult() != test.atomic {
				t.Fatalf("RequiresAtomicResult = %t, want %t", compiled.RequiresAtomicResult(), test.atomic)
			}
			if !compiled.HasValidExecutionSeal() {
				t.Fatal("compiled command lacks a valid execution seal")
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d", got, want)
			}
			if test.bound != "" {
				notSQL := test.notSQL
				if notSQL == "" {
					notSQL = test.bound
				}
				if strings.Contains(compiled.SQL, notSQL) {
					t.Fatalf("SQL leaked authored value %q:\n%s", notSQL, compiled.SQL)
				}
				if !v03ArgumentContainsString(compiled.Args, test.bound) {
					t.Fatalf("args omit bound value %q: %#v", test.bound, compiled.Args)
				}
			}
			for _, field := range compiled.OutputFields {
				if strings.HasPrefix(strings.ToLower(field), "__os_") {
					t.Fatalf("private output leaked as %q", field)
				}
			}
		})
	}
}

func TestV03AllTenCommandsComposeWithoutLeakingAuthorityOrPrivateColumns(t *testing.T) {
	t.Parallel()

	const (
		jobID         = "v03/adversarial/job:秘密"
		regexPattern  = "(?i)V03_REGEX_SECRET_拒否"
		strcatLiteral = "V03_STRCAT_SECRET_/"
		fillValue     = "V03_FILL_SECRET_界"
		delimiter     = "💥界"
	)
	wantFields := []string{
		"event_id", "tags", "running", "endpoint", "optional", "total", "step",
		"info_min_time", "info_max_time", "info_search_time", "info_sid",
	}
	scope := testChartScope()
	scope.SearchJobID = jobID
	normalizedRegex, normalizeErr := splregex.CompileMatchPattern(regexPattern)
	if normalizeErr != nil {
		t.Fatalf("normalize regex fixture: %v", normalizeErr)
	}
	compiled := compileSPLWithScope(t,
		`index=gradethis`+
			` | regex message!="`+regexPattern+`"`+
			` | sort 0 +event_id`+
			` | accum bytes AS running`+
			` | strcat allrequired=true host "`+strcatLiteral+`" route endpoint`+
			` | addinfo`+
			` | fillnull value="`+fillValue+`" optional`+
			` | addtotals fieldname=total bytes running`+
			` | delta running AS step p=2`+
			` | makemv delim="`+delimiter+`" allowempty=true tags`+
			` | mvexpand tags limit=3`+
			` | reverse`+
			` | table `+strings.Join(wantFields, " "),
		scope,
	)

	if !reflect.DeepEqual(compiled.OutputFields, wantFields) {
		t.Fatalf("public fields = %v, want %v", compiled.OutputFields, wantFields)
	}
	for _, field := range compiled.OutputFields {
		if strings.HasPrefix(strings.ToLower(field), "__os_") {
			t.Fatalf("compiler-private field leaked into public schema: %q", field)
		}
	}
	wantContainers := []ResultContainerOutput{
		canonicalResultContainerOutput(3),
		canonicalResultContainerOutput(4),
	}
	if !slices.Equal(compiled.ContainerOutputs, wantContainers) {
		t.Fatalf(
			"strcat/fillnull container authority = %#v, want endpoint/optional at outputs 3/4",
			compiled.ContainerOutputs,
		)
	}
	if !compiled.RequiresAtomicResult() {
		t.Fatal("resource-sensitive all-ten pipeline lacks atomic-result evidence")
	}
	if compiled.relationalDepth < 1 || compiled.relationalDepth > maximumCompiledRelationalDepth {
		t.Fatalf("relational depth = %d, want 1..%d", compiled.relationalDepth, maximumCompiledRelationalDepth)
	}
	if len(compiled.SQL) == 0 || len(compiled.SQL) > maxCompiledQueryBytes {
		t.Fatalf("compiled SQL bytes = %d, want 1..%d", len(compiled.SQL), maxCompiledQueryBytes)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}

	// Authored values and the immutable public job ID are bind arguments, never
	// generated SQL. An exact-once SID assertion also catches accidental use of
	// ClickHouse wall-clock/session state in place of admitted job authority.
	for _, value := range []struct {
		authored string
		bound    string
	}{
		{authored: jobID, bound: jobID},
		{authored: regexPattern, bound: normalizedRegex.Pattern},
		{authored: strcatLiteral, bound: strcatLiteral},
		{authored: fillValue, bound: fillValue},
		{authored: delimiter, bound: delimiter},
	} {
		if strings.Contains(compiled.SQL, value.authored) {
			t.Fatalf("compiled SQL leaked authored value %q:\n%s", value.authored, compiled.SQL)
		}
		if !v03ArgumentContainsString(compiled.Args, value.bound) {
			t.Fatalf("bound arguments omit normalized value %q for authored %q: %#v", value.bound, value.authored, compiled.Args)
		}
	}
	if got := v03ArgumentStringCount(compiled.Args, jobID); got != 1 {
		t.Fatalf("immutable info_sid occurs %d times in args, want exactly once: %#v", got, compiled.Args)
	}

	for _, marker := range v03AllRuntimeMarkers() {
		if !strings.Contains(compiled.SQL, marker) {
			t.Fatalf("all-ten pipeline omits runtime guard %q\nSQL: %s", marker, compiled.SQL)
		}
	}
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("all-ten pipeline did not produce a valid sealed execution contract")
	}
}

func TestV03AllTenCommandsComposeWithExactRetainedKnowledgeEvidence(t *testing.T) {
	t.Parallel()

	program := testKnowledgeEvidenceProgram(t, "v03-all-ten-knowledge")
	scope := testChartScope()
	scope.SearchJobID = "v03-knowledge-job"
	authored := buildPlanWithScope(t,
		`index=gradethis`+
			` | regex message="knowledge-visible"`+
			` | sort 0 +event_id`+
			` | accum bytes AS running`+
			` | strcat host "/" route endpoint`+
			` | addinfo`+
			` | fillnull value="0" optional`+
			` | addtotals fieldname=total bytes running`+
			` | delta running AS step p=2`+
			` | makemv delim="," tags`+
			` | mvexpand tags limit=2`+
			` | reverse`+
			` | table event_id tags endpoint total step info_sid`,
		scope,
	)
	logical, err := plan.InjectKnowledgePrelude(authored, program)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude() error = %v", err)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(knowledge + all ten) error = %v", err)
	}
	if !compiled.HasValidExecutionSeal() || !compiled.RequiresAtomicResult() {
		t.Fatalf("compiled knowledge composition = sealed %t atomic %t", compiled.HasValidExecutionSeal(), compiled.RequiresAtomicResult())
	}
	evidence, ok := compiled.KnowledgeSnapshotEvidenceFor(program)
	if !ok {
		t.Fatal("compiled all-ten query omitted exact retained knowledge evidence")
	}
	charges := program.Charges()
	if evidence.AuthoredRegexPrograms() != 1 ||
		evidence.RegexPrograms() != charges.RegexPrograms+1 ||
		evidence.RegexWorkUnits() <= charges.RegexWorkUnits {
		t.Fatalf("knowledge/authored regex accounting = %#v, retained charges %#v", evidence, charges)
	}
	wantFields := []string{"event_id", "tags", "endpoint", "total", "step", "info_sid"}
	if !reflect.DeepEqual(compiled.OutputFields, wantFields) {
		t.Fatalf("knowledge composition outputs = %v, want %v", compiled.OutputFields, wantFields)
	}
	for _, field := range compiled.OutputFields {
		if strings.HasPrefix(strings.ToLower(field), "__os_") {
			t.Fatalf("knowledge composition leaked private field %q", field)
		}
	}
}

func TestV03FixedTypeBoundaryRejectsMakeMVButAdmitsScalarAndArrayMVExpand(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval tags=7 | makemv delim="," tags | table tags`,
		`index=gradethis | stats values(user) AS tags | makemv delim="," tags | table tags`,
	} {
		logical := buildPlan(t, source)
		compiled, err := (Compiler{}).Compile(logical)
		if err == nil || compiled.HasValidExecutionSeal() {
			t.Fatalf("fixed incompatible makemv compiled for %q: sealed=%t error=%v", source, compiled.HasValidExecutionSeal(), err)
		}
	}

	for _, source := range []string{
		`index=gradethis | eval tags=7 | mvexpand tags | table tags`,
		`index=gradethis | eval tags=null | mvexpand tags | table tags`,
		`index=gradethis | stats values(user) AS tags | mvexpand tags | table tags`,
	} {
		compiled := compileSPL(t, source)
		if !compiled.HasValidExecutionSeal() || !compiled.RequiresAtomicResult() ||
			!reflect.DeepEqual(compiled.OutputFields, []string{"tags"}) {
			t.Fatalf("supported mvexpand fixed type for %q = %#v", source, compiled)
		}
	}
}

func TestV03RepeatedExpansionAtStageBoundaryStaysBoundedAndPrivate(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString(`index=gradethis | sort 0 +event_id`)
	wantFields := []string{"event_id"}
	for index := 1; index <= plan.MaximumMVExpandStages; index++ {
		field := "tags" + string(rune('a'+index-1))
		source.WriteString(` | mvexpand `)
		source.WriteString(field)
		source.WriteString(` limit=1 | reverse`)
		wantFields = append(wantFields, field)
	}
	source.WriteString(` | table `)
	source.WriteString(strings.Join(wantFields, " "))
	compiled := compileSPL(t, source.String())

	if !reflect.DeepEqual(compiled.OutputFields, wantFields) {
		t.Fatalf("repeated expansion fields = %v, want %v", compiled.OutputFields, wantFields)
	}
	if !compiled.RequiresAtomicResult() {
		t.Fatal("repeated expansion lacks atomic-result evidence")
	}
	if compiled.relationalDepth > maximumCompiledRelationalDepth ||
		len(compiled.SQL) > maxCompiledQueryBytes {
		t.Fatalf("repeated expansion escaped physical ceilings: depth=%d bytes=%d", compiled.relationalDepth, len(compiled.SQL))
	}
	for _, field := range compiled.OutputFields {
		if strings.HasPrefix(strings.ToLower(field), "__os_") {
			t.Fatalf("private expansion ordinal leaked as %q", field)
		}
	}
	for _, marker := range []string{
		MVExpandRowMembersLimitMarker,
		MVExpandStageRowsLimitMarker,
		MVExpandQueryRowsLimitMarker,
		MVExpandRetainedBytesLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, marker) {
			t.Fatalf("repeated expansion omits %q", marker)
		}
	}
}

func TestV03RepeatedExpansionRetainsQueryWideChargeAcrossIntermediateOperators(t *testing.T) {
	t.Parallel()

	const firstExpansion = `index=gradethis | mvexpand tags limit=2`
	tests := []struct {
		name       string
		middle     string
		finish     string
		wantFields []string
		aggregate  bool
	}{
		{
			name:       "table",
			middle:     ` | table event_id tags zones`,
			finish:     ` | mvexpand zones limit=2 | table event_id tags zones`,
			wantFields: []string{"event_id", "tags", "zones"},
		},
		{
			name:       "fields include",
			middle:     ` | fields event_id tags zones`,
			finish:     ` | mvexpand zones limit=2 | table event_id tags zones`,
			wantFields: []string{"event_id", "tags", "zones"},
		},
		{
			name:       "fields exclude",
			middle:     ` | fields - host`,
			finish:     ` | mvexpand zones limit=2 | table event_id tags zones`,
			wantFields: []string{"event_id", "tags", "zones"},
		},
		{
			name:       "rename",
			middle:     ` | rename tags AS expanded_tag`,
			finish:     ` | mvexpand zones limit=2 | table event_id expanded_tag zones`,
			wantFields: []string{"event_id", "expanded_tag", "zones"},
		},
		{
			name:       "filter",
			middle:     ` | where isnotnull(tags)`,
			finish:     ` | mvexpand zones limit=2 | table event_id tags zones`,
			wantFields: []string{"event_id", "tags", "zones"},
		},
		{
			name:       "calculated field",
			middle:     ` | eval marker="kept"`,
			finish:     ` | mvexpand zones limit=2 | table event_id tags zones marker`,
			wantFields: []string{"event_id", "tags", "zones", "marker"},
		},
		{
			name:       "stats",
			middle:     ` | stats values(zones) AS zones BY tags`,
			finish:     ` | mvexpand zones limit=2 | table tags zones`,
			wantFields: []string{"tags", "zones"},
			aggregate:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, firstExpansion+test.middle+test.finish)
			if !compiled.HasValidExecutionSeal() || !compiled.RequiresAtomicResult() {
				t.Fatalf("two-stage expansion = sealed %t atomic %t", compiled.HasValidExecutionSeal(), compiled.RequiresAtomicResult())
			}
			if !reflect.DeepEqual(compiled.OutputFields, test.wantFields) {
				t.Fatalf("public fields = %v, want %v", compiled.OutputFields, test.wantFields)
			}
			if got := strings.Count(compiled.SQL, MVExpandQueryRowsLimitMarker); got != 2 {
				t.Fatalf("query-row guards = %d, want one per expansion stage\nSQL: %s", got, compiled.SQL)
			}
			const resetCharge = `toUInt64(0) + sum(toUInt64(length("__os_mvexpand_selected_`
			if got := strings.Count(compiled.SQL, resetCharge); got != 1 {
				t.Fatalf("zero-based query charges = %d, want only the first expansion to reset\nSQL: %s", got, compiled.SQL)
			}
			const cumulativeCharge = `max("__os_mvexpand_query_rows_`
			if got := strings.Count(compiled.SQL, cumulativeCharge); got != 1 {
				t.Fatalf("prior-query cumulative charges = %d, want the second expansion to add exactly one\nSQL: %s", got, compiled.SQL)
			}
			if test.aggregate {
				const collapsedCharge = `maxOrDefault("__os_mvexpand_query_rows_`
				if got := strings.Count(compiled.SQL, collapsedCharge); got != 1 {
					t.Fatalf("aggregate charge collapses = %d, want exactly one maxOrDefault private metric\nSQL: %s", got, compiled.SQL)
				}
			}
			for _, field := range compiled.OutputFields {
				if strings.HasPrefix(strings.ToLower(field), "__os_") {
					t.Fatalf("private expansion authority leaked as %q", field)
				}
			}
		})
	}
}

func TestV03MVExpandGeneratedSQLHasBalancedStructuralParenthesesForEveryInputKind(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | mvexpand tags | table tags`,
		`index=gradethis | eval tags="scalar" | mvexpand tags | table tags`,
		`index=gradethis | stats values(user) AS tags | mvexpand tags | table tags`,
		`index=gradethis | makemv delim="," tags | mvexpand tags | table tags`,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, source)
			v03AssertBalancedSQLParentheses(t, compiled.SQL)
		})
	}
}

func TestV03CompilerRevalidatesForgedPhaseTwoAndThreeOperators(t *testing.T) {
	t.Parallel()

	r := v03CompilerRange()
	field := func(name string) plan.FieldRef { return v03CompilerField(t, name, r) }
	fields := func(count int) []plan.FieldRef {
		result := make([]plan.FieldRef, count)
		for index := range result {
			result[index] = field("field" + string(rune('a'+index%26)) + string(rune('a'+index/26)))
		}
		return result
	}
	validExpansion := func(ordinal uint8) *plan.ExpandMultivalue {
		return &plan.ExpandMultivalue{Input: field("tags"), Limit: 1, QueryOrdinal: ordinal, Range: r}
	}
	var nilFill *plan.FillNull
	var nilTotal *plan.RowTotal
	var nilDelta *plan.OrderedDelta
	var nilMake *plan.MakeMultivalue
	var nilExpand *plan.ExpandMultivalue
	invalidUTF8 := string([]byte{0xff})
	if utf8.ValidString(invalidUTF8) {
		t.Fatal("invalid UTF-8 fixture unexpectedly valid")
	}

	tests := []struct {
		name      string
		operators []plan.Operator
	}{
		{name: "typed nil fillnull", operators: []plan.Operator{nilFill}},
		{name: "typed nil row total", operators: []plan.Operator{nilTotal}},
		{name: "typed nil delta", operators: []plan.Operator{nilDelta}},
		{name: "typed nil makemv", operators: []plan.Operator{nilMake}},
		{name: "typed nil mvexpand", operators: []plan.Operator{nilExpand}},
		{name: "fillnull empty fields", operators: []plan.Operator{&plan.FillNull{Value: "0", Range: r}}},
		{name: "fillnull field overflow", operators: []plan.Operator{&plan.FillNull{Fields: fields(spl.MaximumExplicitProjectionFields + 1), Value: "0", Range: r}}},
		{name: "fillnull invalid UTF-8", operators: []plan.Operator{&plan.FillNull{Fields: fields(1), Value: invalidUTF8, Range: r}}},
		{name: "row total empty fields", operators: []plan.Operator{&plan.RowTotal{Output: "Total", Range: r}}},
		{name: "row total private output", operators: []plan.Operator{&plan.RowTotal{Inputs: fields(1), Output: "__os_private", Range: r}}},
		{name: "delta zero lag", operators: []plan.Operator{&plan.OrderedDelta{Input: field("n"), Output: "delta", Previous: 0, Range: r}}},
		{name: "delta lag overflow", operators: []plan.Operator{&plan.OrderedDelta{Input: field("n"), Output: "delta", Previous: spl.MaximumStreamStatsWindow + 1, Range: r}}},
		{name: "delta private output", operators: []plan.Operator{&plan.OrderedDelta{Input: field("n"), Output: "__os_private", Previous: 1, Range: r}}},
		{name: "makemv empty delimiter", operators: []plan.Operator{&plan.MakeMultivalue{Input: field("tags"), Range: r}}},
		{name: "makemv invalid UTF-8 delimiter", operators: []plan.Operator{&plan.MakeMultivalue{Input: field("tags"), Delimiter: invalidUTF8, Range: r}}},
		{name: "makemv delimiter overflow", operators: []plan.Operator{&plan.MakeMultivalue{Input: field("tags"), Delimiter: strings.Repeat("x", spl.MaximumMakeMVDelimiterBytes+1), Range: r}}},
		{name: "makemv canonical internal field", operators: []plan.Operator{&plan.MakeMultivalue{Input: field("_time"), Delimiter: ",", Range: r}}},
		{name: "makemv forged private field", operators: []plan.Operator{&plan.MakeMultivalue{Input: v03ForgedCompilerField("__os_private", r), Delimiter: ",", Range: r}}},
		{name: "mvexpand limit overflow", operators: []plan.Operator{&plan.ExpandMultivalue{Input: field("tags"), Limit: spl.MaximumMVExpandLimit + 1, QueryOrdinal: 1, Range: r}}},
		{name: "mvexpand zero ordinal", operators: []plan.Operator{validExpansion(0)}},
		{name: "mvexpand ordinal overflow", operators: []plan.Operator{validExpansion(uint8(plan.MaximumMVExpandStages + 1))}},
		{name: "mvexpand canonical internal field", operators: []plan.Operator{&plan.ExpandMultivalue{Input: field("_time"), QueryOrdinal: 1, Range: r}}},
		{name: "mvexpand forged private field", operators: []plan.Operator{&plan.ExpandMultivalue{Input: v03ForgedCompilerField("__os_private", r), QueryOrdinal: 1, Range: r}}},
		{name: "mvexpand duplicate ordinal", operators: []plan.Operator{validExpansion(1), validExpansion(1)}},
		{name: "mvexpand skipped ordinal", operators: []plan.Operator{validExpansion(1), validExpansion(3)}},
		{name: "mvexpand stage overflow", operators: func() []plan.Operator {
			operators := make([]plan.Operator, 0, plan.MaximumMVExpandStages+1)
			for ordinal := 1; ordinal <= plan.MaximumMVExpandStages+1; ordinal++ {
				operators = append(operators, validExpansion(uint8(ordinal)))
			}
			return operators
		}()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := v03ForgedCompilerQuery(t, test.operators...)
			compiled, err := (Compiler{}).Compile(query)
			if err == nil {
				t.Fatalf("Compile accepted forged operators %#v; SQL:\n%s", test.operators, compiled.SQL)
			}
		})
	}
}

func TestV03SourceDeterminedLimitsRemainResourceDiagnosticsAtCompilerBoundary(t *testing.T) {
	t.Parallel()

	r := v03CompilerRange()
	field := v03CompilerField(t, "n", r)
	tests := []struct {
		name     string
		operator plan.Operator
	}{
		{
			name: "fillnull projection overflow",
			operator: &plan.FillNull{
				Fields: func() []plan.FieldRef {
					result := make([]plan.FieldRef, spl.MaximumExplicitProjectionFields+1)
					for index := range result {
						result[index] = v03CompilerField(t, "overflow"+string(rune('a'+index%26))+string(rune('a'+index/26)), r)
					}
					return result
				}(),
				Value: "0",
				Range: r,
			},
		},
		{
			name: "delta lag overflow",
			operator: &plan.OrderedDelta{
				Input: field, Output: "delta", Previous: spl.MaximumStreamStatsWindow + 1, Range: r,
			},
		},
		{
			name: "mvexpand limit overflow",
			operator: &plan.ExpandMultivalue{
				Input: field, Limit: spl.MaximumMVExpandLimit + 1, QueryOrdinal: 1, Range: r,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := (Compiler{}).Compile(v03ForgedCompilerQuery(t, test.operator))
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("Compile error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
			}
			if diagnostic.Range != r {
				t.Fatalf("diagnostic range = %#v, want forged operator range %#v", diagnostic.Range, r)
			}
		})
	}
}

func TestV03StrcatInheritsTheSharedConcatenationOutputByteCeiling(t *testing.T) {
	t.Parallel()

	source := `index=gradethis | strcat _raw _raw _raw _raw _raw amplified`
	logical := buildPlan(t, source)
	compiled, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("Compile(%q) = sealed %t error %#v, want shared concatenation resource diagnostic", source, compiled.HasValidExecutionSeal(), err)
	}
	if got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != `strcat _raw _raw _raw _raw _raw amplified` {
		t.Fatalf("concatenation diagnostic range = %q, want complete strcat command", got)
	}
}

func v03AllRuntimeMarkers() []string {
	return []string{
		UnsupportedMakeMVValueMarker,
		MakeMVRowMembersLimitMarker,
		MakeMVRowBytesLimitMarker,
		MakeMVResultMembersLimitMarker,
		MakeMVResultBytesLimitMarker,
		MakeMVRetainedBytesLimitMarker,
		UnsupportedMVExpandValueMarker,
		MVExpandRowMembersLimitMarker,
		MVExpandStageRowsLimitMarker,
		MVExpandQueryRowsLimitMarker,
		MVExpandRetainedBytesLimitMarker,
	}
}

func v03ArgumentContainsString(arguments []any, want string) bool {
	return v03ArgumentStringCount(arguments, want) > 0
}

func v03ArgumentStringCount(arguments []any, want string) int {
	count := 0
	for _, argument := range arguments {
		if value, ok := argument.(string); ok && value == want {
			count++
		}
	}
	return count
}

func v03AssertBalancedSQLParentheses(t *testing.T, sql string) {
	t.Helper()
	depth := 0
	var quote byte
	for offset := 0; offset < len(sql); offset++ {
		current := sql[offset]
		if quote != 0 {
			if current == '\\' && quote == '\'' && offset+1 < len(sql) {
				offset++
				continue
			}
			if current != quote {
				continue
			}
			if offset+1 < len(sql) && sql[offset+1] == quote {
				offset++
				continue
			}
			quote = 0
			continue
		}
		switch current {
		case '\'', '"', '`':
			quote = current
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				start := max(0, offset-96)
				end := min(len(sql), offset+97)
				t.Fatalf("generated SQL has an unmatched closing parenthesis at byte %d near %q", offset, sql[start:end])
			}
		}
	}
	if quote != 0 {
		t.Fatalf("generated SQL has an unterminated %q quote", quote)
	}
	if depth != 0 {
		t.Fatalf("generated SQL has %d unmatched opening parentheses", depth)
	}
}

func v03CompilerRange() spl.Range {
	return spl.Range{
		Start: spl.Position{Offset: 17, Line: 1, Column: 18},
		End:   spl.Position{Offset: 33, Line: 1, Column: 34},
	}
}

func v03CompilerField(t *testing.T, name string, sourceRange spl.Range) plan.FieldRef {
	t.Helper()
	field, err := plan.ResolveField(name, sourceRange)
	if err != nil {
		t.Fatalf("ResolveField(%q): %v", name, err)
	}
	return field
}

func v03ForgedCompilerField(name string, sourceRange spl.Range) plan.FieldRef {
	return plan.FieldRef{Name: name, Path: []string{name}, Range: sourceRange}
}

func v03ForgedCompilerQuery(t *testing.T, operators ...plan.Operator) *plan.Query {
	t.Helper()
	base := buildPlan(t, `index=gradethis`)
	result := &plan.Query{
		Operators:        make([]plan.Operator, 1, 1+len(operators)),
		EffectiveIndexes: append([]string(nil), base.EffectiveIndexes...),
		SearchStart:      base.SearchStart,
		SearchTimezone:   base.SearchTimezone,
	}
	result.Operators[0] = base.Operators[0]
	result.Operators = append(result.Operators, operators...)
	return result
}
