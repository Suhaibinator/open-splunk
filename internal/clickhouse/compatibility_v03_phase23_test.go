package clickhouse

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestCompileRowTotalPublishesNullableFloat64WithZeroForAllIneligible(t *testing.T) {
	t.Parallel()
	compiled, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | table event_id | addtotals fieldname=total missing other | table total`,
	))
	if err != nil {
		t.Fatalf("Compile(addtotals): %v", err)
	}
	if !strings.Contains(compiled.SQL, `CAST(plus(`) ||
		!strings.Contains(compiled.SQL, `AS Nullable(Float64)) AS "total"`) ||
		strings.Count(compiled.SQL, `ifNull(CAST(NULL AS Nullable(Float64)), toFloat64(0))`) < 2 {
		t.Fatalf("addtotals SQL does not pin nullable Float64/all-ineligible zero:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, v03MaterializedValidationSettingsSQL); got != 1 ||
		!compiled.RequiresAtomicResult() {
		t.Fatalf("addtotals validation contract = settings:%d atomic:%t", got, compiled.RequiresAtomicResult())
	}
	for _, required := range []string{
		`"__os_addtotals_validation_`,
		`"__os_addtotals_result_`,
		`"__os_chronological_validation_`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("addtotals deferred validation is missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileRowTotalValidationSurvivesEveryDownstreamConsumer(t *testing.T) {
	t.Parallel()

	for _, suffix := range []string{
		`where event_id="never"`,
		`fields event_id`,
		`table event_id`,
		`head 1`,
		`stats count`,
	} {
		suffix := suffix
		t.Run(suffix, func(t *testing.T) {
			t.Parallel()
			compiled, err := (Compiler{}).Compile(buildPlan(
				t,
				`index=gradethis | addtotals fieldname=total n | `+suffix,
			))
			if err != nil {
				t.Fatalf("Compile(addtotals): %v", err)
			}
			if !compiled.HasValidExecutionSeal() || !compiled.RequiresAtomicResult() {
				t.Fatalf(
					"addtotals %q = sealed %t atomic %t",
					suffix,
					compiled.HasValidExecutionSeal(),
					compiled.RequiresAtomicResult(),
				)
			}
			for _, required := range []string{
				`"__os_addtotals_validation_`,
				`"__os_chronological_validation_`,
				UnsupportedExpressionValueMarker,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("addtotals %q lost %q:\n%s", suffix, required, compiled.SQL)
				}
			}
			if slices.Contains(compiled.OutputFields, "total") &&
				(suffix == `fields event_id` || suffix == `table event_id` || suffix == `stats count`) {
				t.Fatalf("addtotals %q leaked projected-away output: %v", suffix, compiled.OutputFields)
			}
		})
	}
}

func TestCompileRowTotalValidatesThePreOverwriteInputOnDestinationCollision(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | addtotals fieldname=n n other | fields event_id`,
		`index=gradethis | addtotals Total | table event_id`,
	} {
		compiled, err := (Compiler{}).Compile(buildPlan(t, source))
		if err != nil {
			t.Fatalf("Compile(%q): %v", source, err)
		}
		validation := strings.Index(compiled.SQL, ` AS "__os_addtotals_validation_`)
		cells := strings.Index(compiled.SQL, `arrayMap(item ->`)
		inputs := strings.Index(compiled.SQL, ` AS "__os_addtotals_dynamic_inputs_`)
		if validation < 0 || cells < 0 || inputs < 0 ||
			validation >= cells || cells >= inputs {
			t.Fatalf("colliding addtotals does not publish sibling value/validation expressions:\n%s", compiled.SQL)
		}
		if !strings.Contains(compiled.SQL, UnsupportedExpressionValueMarker) ||
			!compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
			t.Fatalf(
				"colliding addtotals = marker %t atomic %t sealed %t",
				strings.Contains(compiled.SQL, UnsupportedExpressionValueMarker),
				compiled.RequiresAtomicResult(),
				compiled.HasValidExecutionSeal(),
			)
		}
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("placeholder count = %d, args = %d:\n%s", got, want, compiled.SQL)
		}
	}
}

func TestCompileRowTotalAcceptsTheContractMaximumDynamicInputs(t *testing.T) {
	t.Parallel()

	fields := make([]string, 64)
	for i := range fields {
		fields[i] = fmt.Sprintf("f%02d", i)
	}
	compiled, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | addtotals fieldname=total `+strings.Join(fields, " ")+` | table total`,
	))
	if err != nil {
		t.Fatalf("Compile(64-field addtotals): %v", err)
	}
	if got := len(compiled.SQL); got > maxCompiledQueryBytes {
		t.Fatalf("64-field addtotals compiled to %d bytes, ceiling is %d", got, maxCompiledQueryBytes)
	}
	if got := strings.Count(compiled.SQL, `arrayMap(item ->`); got != 1 {
		t.Fatalf("64-field addtotals dynamic normalizer count = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("64-field addtotals placeholder count = %d, args = %d", got, want)
	}
	if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
		t.Fatalf(
			"64-field addtotals = atomic %t sealed %t",
			compiled.RequiresAtomicResult(),
			compiled.HasValidExecutionSeal(),
		)
	}
}

func TestCompileDeltaForcesMalformedValidationAfterProjection(t *testing.T) {
	t.Parallel()

	compiled, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | delta n AS step | fields event_id`,
	))
	if err != nil {
		t.Fatalf("Compile(delta projection): %v", err)
	}
	for _, required := range []string{
		`"__os_delta_validation_`,
		`"__os_delta_result_`,
		`"__os_chronological_validation_`,
		`"__os_delta_dynamic_inputs_`,
		`"__os_delta_dynamic_cells_`,
		`arrayMap(item -> tuple(`,
		UnsupportedExpressionValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("projected delta lost %q:\n%s", required, compiled.SQL)
		}
	}
	if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
		t.Fatalf(
			"projected delta = atomic %t sealed %t",
			compiled.RequiresAtomicResult(),
			compiled.HasValidExecutionSeal(),
		)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("projected delta placeholders = %d, args = %d", got, want)
	}
	if strings.Contains(compiled.SQL, "arrayMap((__os_delta_dynamic, __os_delta_present)") {
		t.Fatalf("projected delta retained the unstable parallel-array binding:\n%s", compiled.SQL)
	}
}

func TestCompileDeltaMalformedBarrierSurvivesDownstreamConsumers(t *testing.T) {
	t.Parallel()

	for _, downstream := range []string{
		`where event_id="never"`,
		`fields event_id`,
		`table event_id`,
		`head 1`,
		`stats count`,
	} {
		downstream := downstream
		t.Run(downstream, func(t *testing.T) {
			t.Parallel()
			compiled, err := (Compiler{}).Compile(buildPlan(
				t,
				`index=gradethis | delta n AS step | `+downstream,
			))
			if err != nil {
				t.Fatalf("Compile(delta | %s): %v", downstream, err)
			}
			for _, required := range []string{
				`"__os_delta_dynamic_cells_`,
				`"__os_delta_validation_`,
				`"__os_chronological_validation_`,
				UnsupportedExpressionValueMarker,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("delta | %s lost %q:\n%s", downstream, required, compiled.SQL)
				}
			}
			if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
				t.Fatalf(
					"delta | %s = atomic %t sealed %t",
					downstream,
					compiled.RequiresAtomicResult(),
					compiled.HasValidExecutionSeal(),
				)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf(
					"delta | %s placeholders = %d, args = %d",
					downstream,
					got,
					want,
				)
			}
		})
	}
}

func TestCompileMultivalueRetainedChargeIncludesNamesAndRawFields(t *testing.T) {
	t.Parallel()

	for _, command := range []string{`makemv host`, `mvexpand host`} {
		compiled, err := (Compiler{}).Compile(buildPlan(
			t,
			`index=gradethis | `+command+` | head 1 | table event_id`,
		))
		if err != nil {
			t.Fatalf("Compile(%s): %v", command, err)
		}
		for _, required := range []string{
			`["__os_fields"]`,
			` AS "fields"`,
			` AS "event_id"`,
			`enable_named_columns_in_function_tuple = 1`,
			`output_format_json_named_tuples_as_objects = 1`,
		} {
			if !strings.Contains(compiled.SQL, required) {
				t.Fatalf("%s retained charge lost %q:\n%s", command, required, compiled.SQL)
			}
		}
	}
}

func TestCompileMVExpandPreservesLogicalAbsenceOfFixedArrays(t *testing.T) {
	t.Parallel()

	compiled, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | stats values(missing) AS tags | mvexpand tags | table tags`,
	))
	if err != nil {
		t.Fatalf("Compile(values absent | mvexpand): %v", err)
	}
	if !strings.Contains(
		compiled.SQL,
		`if("__os_mvexpand_source_present_`,
	) || !strings.Contains(compiled.SQL, `[CAST(NULL AS Nullable(String))]`) {
		t.Fatalf("mvexpand does not turn a logically absent fixed array into one null member:\n%s", compiled.SQL)
	}
}

func TestCompileMVExpandValidationDisablesPredicatePushdown(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name         string
		source       string
		wantSettings int
	}{
		{
			name:         "downstream filter",
			wantSettings: 1,
			source: `index=gradethis | mvexpand tags | ` +
				`where event_id="never"`,
		},
		{
			name:         "repeated expansion",
			wantSettings: 1,
			source: `index=gradethis | mvexpand tags | ` +
				`mvexpand zones | where event_id="never"`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled, err := (Compiler{}).Compile(buildPlan(t, test.source))
			if err != nil {
				t.Fatalf("Compile(mvexpand): %v", err)
			}
			if got := strings.Count(compiled.SQL, v03MaterializedValidationSettingsSQL); got != test.wantSettings {
				t.Fatalf(
					"v0.3 validation settings count = %d, want %d:\n%s",
					got,
					test.wantSettings,
					compiled.SQL,
				)
			}
		})
	}

	compiled, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | where event_id="never"`,
	))
	if err != nil {
		t.Fatalf("Compile(ordinary filter): %v", err)
	}
	if strings.Contains(compiled.SQL, "enable_optimize_predicate_expression") {
		t.Fatalf("ordinary filter unexpectedly changes optimizer settings:\n%s", compiled.SQL)
	}
}

func TestCompileDeltaValidationDisablesPlanRewrites(t *testing.T) {
	t.Parallel()
	compiled, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | delta n AS step | where event_id="never"`,
	))
	if err != nil {
		t.Fatalf("Compile(delta): %v", err)
	}
	if got := strings.Count(compiled.SQL, v03MaterializedValidationSettingsSQL); got != 1 {
		t.Fatalf("delta validation settings count = %d, want 1:\n%s", got, compiled.SQL)
	}
	if !compiled.RequiresAtomicResult() {
		t.Fatal("delta query lost atomic-result evidence")
	}
}

func TestCompileMissingMultivalueFieldMatchesClosedLogicalShape(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"makemv missing", "mvexpand missing"} {
		command := command
		t.Run(command, func(t *testing.T) {
			t.Parallel()
			compiled, err := (Compiler{}).Compile(buildPlan(
				t,
				`index=gradethis | table event_id | `+command,
			))
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}
			if want := []string{"event_id", "missing"}; !slices.Equal(compiled.OutputFields, want) {
				t.Fatalf("OutputFields = %v, want %v", compiled.OutputFields, want)
			}
		})
	}
}

func TestCompileMakeMVUsesBoundedLiteralSplitSentinels(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		delimiter string
		wantSQL   string
		notSQL    string
	}{
		{
			name:      "allow empty retains one overflow sentinel",
			source:    `index=gradethis | makemv delim="💥界" allowempty=true tags`,
			delimiter: "💥界",
			wantSQL:   "splitByString(?, ifNull(",
			notSQL:    "splitByRegexp(",
		},
		{
			name:      "filtered empties collapse delimiter runs with two sentinels",
			source:    `index=gradethis | makemv delim="aba" tags`,
			delimiter: "aba",
			wantSQL:   "splitByRegexp(concat('(', regexpQuoteMeta(?), ')+'), ifNull(",
			notSQL:    "splitByString(",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled, err := (Compiler{}).Compile(buildPlan(t, test.source))
			if err != nil {
				t.Fatalf("Compile(makemv): %v", err)
			}
			if !compiled.HasValidExecutionSeal() || !compiled.RequiresAtomicResult() {
				t.Fatalf(
					"bounded makemv = sealed %t atomic %t",
					compiled.HasValidExecutionSeal(),
					compiled.RequiresAtomicResult(),
				)
			}
			if !strings.Contains(compiled.SQL, test.wantSQL) ||
				strings.Contains(compiled.SQL, test.notSQL) {
				t.Fatalf("makemv does not use the expected bounded splitter:\n%s", compiled.SQL)
			}
			wantSentinels := "toUInt64(1001)"
			if strings.Contains(test.wantSQL, "splitByRegexp") {
				wantSentinels = "toUInt64(1002)"
			}
			if got := strings.Count(compiled.SQL, wantSentinels); got != 1 {
				t.Fatalf("sentinel bound count = %d, want 1 for %s:\n%s", got, wantSentinels, compiled.SQL)
			}
			if got := strings.Count(
				compiled.SQL,
				"splitby_max_substrings_includes_remaining_string = 0",
			); got != 1 {
				t.Fatalf("bounded split setting count = %d, want 1:\n%s", got, compiled.SQL)
			}
			if strings.Contains(compiled.SQL, test.delimiter) {
				t.Fatalf("authored delimiter leaked into SQL:\n%s", compiled.SQL)
			}
			if got := v03ArgumentStringCount(compiled.Args, test.delimiter); got != 1 {
				t.Fatalf("bound delimiter count = %d, want 1: %#v", got, compiled.Args)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d", got, want)
			}
		})
	}
}
