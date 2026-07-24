package clickhouse

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// TestBinEdgeNumericDynamicShapesKeepPlaceholderAccountingExact compiles the
// Dynamic bin shapes whose destination already holds a runtime-typed value.
// Those paths append destination presence and stored-type arguments to the
// span and metadata arguments, so a placeholder that is emitted without its
// argument (or the reverse) would bind the wrong value at execution time.
func TestBinEdgeNumericDynamicShapesKeepPlaceholderAccountingExact(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | bin metric span=1 AS band | table band`,
		`index=gradethis | bin metric span=9007199254740991 AS band | table band`,
		// The destination already holds a runtime-typed Dynamic value.
		`index=gradethis | bin metric span=10 AS band | bin other span=7 AS band | table band`,
		// The destination is a flattened object parent, whose presence probe
		// contributes descendant arguments of its own.
		`index=gradethis | bin metric span=10 AS nested | table nested`,
		// The destination is the canonical raw text of the event.
		`index=gradethis | bin metric span=10 AS message | table message`,
		// A runtime-typed source produced by an earlier extraction stage.
		`index=gradethis | rex field=_raw "(?<cap>[0-9]+)" | bin cap span=10 AS band | table cap band`,
		// Consecutive stages over the same runtime-typed source.
		`index=gradethis | bin metric span=10 AS first | bin first span=3 AS second | table first second`,
		// The source is replaced in place rather than aliased.
		`index=gradethis | bin metric span=10 | table metric`,
	} {
		source := source
		t.Run(source, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, source)
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf(
					"placeholder count = %d, args = %d\nSQL: %s\nargs: %#v",
					got, want, compiled.SQL, compiled.Args,
				)
			}
			// The native driver reads a `{name:type}` sequence as a query
			// parameter, so generated SQL must never contain a brace.
			if strings.ContainsAny(compiled.SQL, "{}") {
				t.Fatalf("Dynamic numeric bin SQL introduced a bindable brace:\n%s", compiled.SQL)
			}
		})
	}
}

// TestBinEdgeNumericSpanBoundsAtCompileBoundary pins the documented unitless
// span range for a runtime-typed field: one through 2^53-1 inclusive.
func TestBinEdgeNumericSpanBoundsAtCompileBoundary(t *testing.T) {
	t.Parallel()

	for _, span := range []string{"1", "9007199254740991"} {
		if compiled := compileSPL(t, `index=gradethis | bin metric span=`+span+` AS band`); compiled.SQL == "" {
			t.Fatalf("span=%s produced empty SQL", span)
		}
	}
	for _, span := range []string{"0", "9007199254740992", "-10"} {
		parsed, err := spl.Parse(`index=gradethis | bin metric span=` + span + ` AS band`)
		if err != nil {
			continue
		}
		if _, err := plan.Build(parsed, binEdgeNumericScope()); err == nil {
			t.Fatalf("span=%s was accepted, want a rejected span", span)
		}
	}
}

// TestBinEdgeNumericBucketAliasMatchesBinOnRuntimeFields extends the existing
// alias equivalence check to the runtime-typed Dynamic path, where the two
// spellings compile through a much larger expression.
func TestBinEdgeNumericBucketAliasMatchesBinOnRuntimeFields(t *testing.T) {
	t.Parallel()

	bin := compileSPL(t, `index=gradethis | bin metric span=10 AS band | table band`)
	bucket := compileSPL(t, `index=gradethis | bucket span=10 metric AS band | table band`)
	if bin.SQL != bucket.SQL || !reflect.DeepEqual(bin.Args, bucket.Args) ||
		!slices.Equal(bin.OutputFields, bucket.OutputFields) {
		t.Fatalf("bucket alias diverged on a runtime-typed field\nbin: %#v\nbucket: %#v", bin, bucket)
	}
}

// TestBinEdgeNumericRejectsFixedStringSources keeps the compile-time boundary
// intact: a field whose pipeline type is already a fixed string is known before
// execution and must not fall into the runtime classification path.
func TestBinEdgeNumericRejectsFixedStringSources(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval label="21" | bin label span=10`,
		`index=gradethis | eval label="21" | bin label span=10 AS band`,
	} {
		logical := buildPlan(t, source)
		_, err := (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_BIN_FIELD_TYPE" {
			t.Fatalf("Compile(%q) error = %v, want SPL_UNSUPPORTED_BIN_FIELD_TYPE", source, err)
		}
	}
}

func binEdgeNumericScope() plan.Scope {
	return plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis"},
		Earliest:          time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		IndexTimeCutoff:   time.Date(2026, 7, 22, 0, 0, 1, 0, time.UTC),
		VisibilityCutoff:  uint64Pointer(73),
	}
}
