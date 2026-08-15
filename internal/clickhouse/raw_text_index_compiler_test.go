package clickhouse

import (
	"reflect"
	"strings"
	"testing"
)

const (
	rawTokenIndexCandidateSQL = `has(arrayMap(token -> lower(token), extractAll(translateUTF8("_raw", 'ſK', 'sk'), '[A-Za-z0-9_]+')), lower(?))`
	rawTokenRegexVerifierSQL  = `match(toString("_raw"), ?)`
)

func TestCompileRawTokenIndexCandidatePreservesVerifierAndArguments(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis Error42`)
	assertRawTokenIndexContract(t, compiled, rawTokenIndexExpectation{
		candidates: 1,
		verifiers:  1,
		argumentSuffix: []any{
			"Error42",
			`(?i)(?:^|[^[:alnum:]_])Error42(?:$|[^[:alnum:]_])`,
		},
		forbiddenLiterals: []string{"Error42", "error42"},
	})
	if !strings.Contains(
		compiled.SQL,
		"("+rawTokenIndexCandidateSQL+" AND "+rawTokenRegexVerifierSQL+")",
	) {
		t.Fatalf("token-index candidate is not paired with the regex verifier:\n%s", compiled.SQL)
	}
}

func TestCompileRawTokenIndexCandidateRespectsBooleanPolarity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		source          string
		candidates      int
		argumentSuffix  []any
		orderedLowering []string
	}{
		{
			name:       "positive AND",
			source:     `index=gradethis Alpha1 AND Beta2`,
			candidates: 2,
			argumentSuffix: []any{
				"Alpha1", `(?i)(?:^|[^[:alnum:]_])Alpha1(?:$|[^[:alnum:]_])`,
				"Beta2", `(?i)(?:^|[^[:alnum:]_])Beta2(?:$|[^[:alnum:]_])`,
			},
			orderedLowering: []string{
				rawTokenIndexCandidateSQL, rawTokenRegexVerifierSQL,
				rawTokenIndexCandidateSQL, rawTokenRegexVerifierSQL,
			},
		},
		{
			name:       "positive OR",
			source:     `index=gradethis Alpha1 OR Beta2`,
			candidates: 2,
			argumentSuffix: []any{
				"Alpha1", `(?i)(?:^|[^[:alnum:]_])Alpha1(?:$|[^[:alnum:]_])`,
				"Beta2", `(?i)(?:^|[^[:alnum:]_])Beta2(?:$|[^[:alnum:]_])`,
			},
			orderedLowering: []string{
				rawTokenIndexCandidateSQL, rawTokenRegexVerifierSQL,
				rawTokenIndexCandidateSQL, rawTokenRegexVerifierSQL,
			},
		},
		{
			name:       "positive AND negated",
			source:     `index=gradethis Alpha1 AND NOT Beta2`,
			candidates: 1,
			argumentSuffix: []any{
				"Alpha1", `(?i)(?:^|[^[:alnum:]_])Alpha1(?:$|[^[:alnum:]_])`,
				`(?i)(?:^|[^[:alnum:]_])Beta2(?:$|[^[:alnum:]_])`,
			},
			orderedLowering: []string{
				rawTokenIndexCandidateSQL, rawTokenRegexVerifierSQL,
				rawTokenRegexVerifierSQL,
			},
		},
		{
			name:       "negated OR positive",
			source:     `index=gradethis NOT Alpha1 OR Beta2`,
			candidates: 1,
			argumentSuffix: []any{
				`(?i)(?:^|[^[:alnum:]_])Alpha1(?:$|[^[:alnum:]_])`,
				"Beta2", `(?i)(?:^|[^[:alnum:]_])Beta2(?:$|[^[:alnum:]_])`,
			},
			orderedLowering: []string{
				rawTokenRegexVerifierSQL,
				rawTokenIndexCandidateSQL, rawTokenRegexVerifierSQL,
			},
		},
		{
			name:       "negated group",
			source:     `index=gradethis NOT (Alpha1 OR Beta2)`,
			candidates: 0,
			argumentSuffix: []any{
				`(?i)(?:^|[^[:alnum:]_])Alpha1(?:$|[^[:alnum:]_])`,
				`(?i)(?:^|[^[:alnum:]_])Beta2(?:$|[^[:alnum:]_])`,
			},
			orderedLowering: []string{
				rawTokenRegexVerifierSQL, rawTokenRegexVerifierSQL,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			assertRawTokenIndexContract(t, compiled, rawTokenIndexExpectation{
				candidates:        test.candidates,
				verifiers:         2,
				argumentSuffix:    test.argumentSuffix,
				forbiddenLiterals: []string{"Alpha1", "alpha1", "Beta2", "beta2"},
			})
			assertOrderedSQLFragments(t, compiled.SQL, test.orderedLowering...)
		})
	}
}

func TestCompileRawTokenIndexCandidateRejectsIneligibleTerms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		source           string
		fallback         string
		argumentSuffix   []any
		forbiddenLiteral []string
	}{
		{
			name:             "quoted phrase",
			source:           `index=gradethis "Alpha Beta"`,
			fallback:         `positionCaseInsensitiveUTF8(toString("_raw"), ?) > 0`,
			argumentSuffix:   []any{"Alpha Beta"},
			forbiddenLiteral: []string{"Alpha Beta", "alpha beta"},
		},
		{
			name:             "quoted token",
			source:           `index=gradethis "Alpha42"`,
			fallback:         `positionCaseInsensitiveUTF8(toString("_raw"), ?) > 0`,
			argumentSuffix:   []any{"Alpha42"},
			forbiddenLiteral: []string{"Alpha42", "alpha42"},
		},
		{
			name:             "wildcard",
			source:           `index=gradethis Alpha*`,
			fallback:         rawTokenRegexVerifierSQL,
			argumentSuffix:   []any{`(?i)(?:^|[^[:alnum:]_])Alpha[[:alnum:]_]*(?:$|[^[:alnum:]_])`},
			forbiddenLiteral: []string{"Alpha", "alpha"},
		},
		{
			name:             "unicode",
			source:           `index=gradethis café`,
			fallback:         rawTokenRegexVerifierSQL,
			argumentSuffix:   []any{`(?i)(?:^|[^[:alnum:]_])café(?:$|[^[:alnum:]_])`},
			forbiddenLiteral: []string{"café", "CAFÉ"},
		},
		{
			name:             "punctuation",
			source:           `index=gradethis Alpha-Beta`,
			fallback:         rawTokenRegexVerifierSQL,
			argumentSuffix:   []any{`(?i)(?:^|[^[:alnum:]_])Alpha-Beta(?:$|[^[:alnum:]_])`},
			forbiddenLiteral: []string{"Alpha-Beta", "alpha-beta"},
		},
		{
			name:             "underscore",
			source:           `index=gradethis Alpha_Beta`,
			fallback:         rawTokenRegexVerifierSQL,
			argumentSuffix:   []any{`(?i)(?:^|[^[:alnum:]_])Alpha_Beta(?:$|[^[:alnum:]_])`},
			forbiddenLiteral: []string{"Alpha_Beta", "alpha_beta"},
		},
		{
			name:             "negated",
			source:           `index=gradethis NOT Alpha42`,
			fallback:         rawTokenRegexVerifierSQL,
			argumentSuffix:   []any{`(?i)(?:^|[^[:alnum:]_])Alpha42(?:$|[^[:alnum:]_])`},
			forbiddenLiteral: []string{"Alpha42", "alpha42"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			assertRawTokenIndexContract(t, compiled, rawTokenIndexExpectation{
				candidates:        0,
				verifiers:         strings.Count(test.fallback, rawTokenRegexVerifierSQL),
				argumentSuffix:    test.argumentSuffix,
				forbiddenLiterals: test.forbiddenLiteral,
			})
			if !strings.Contains(compiled.SQL, test.fallback) {
				t.Fatalf("ineligible term lost fallback %q:\n%s", test.fallback, compiled.SQL)
			}
		})
	}
}

func TestCompileRawTokenIndexCandidateRequiresCanonicalPhysicalRaw(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields _raw | search Alpha42`,
		`index=gradethis | table _raw | search Alpha42`,
		`index=gradethis | fields - message | search Alpha42`,
		`index=gradethis | rename _raw AS saved_raw | rename saved_raw AS _raw | search Alpha42`,
	} {
		t.Run("preserved/"+source, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, source)
			assertRawTokenIndexContract(t, compiled, rawTokenIndexExpectation{
				candidates: 1,
				verifiers:  1,
				argumentSuffix: []any{
					"Alpha42",
					`(?i)(?:^|[^[:alnum:]_])Alpha42(?:$|[^[:alnum:]_])`,
				},
				forbiddenLiterals: []string{"Alpha42", "alpha42"},
			})
		})
	}

	for _, source := range []string{
		`index=gradethis | eval _raw=message | search Alpha42`,
		`index=gradethis | eval _raw=_raw | search Alpha42`,
		`index=gradethis | table message | rename message AS _raw | search Alpha42`,
	} {
		t.Run("overwritten/"+source, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, source)
			assertRawTokenIndexContract(t, compiled, rawTokenIndexExpectation{
				candidates:     0,
				verifiers:      1,
				argumentSuffix: []any{`(?i)(?:^|[^[:alnum:]_])Alpha42(?:$|[^[:alnum:]_])`},
				forbiddenLiterals: []string{
					"Alpha42", "alpha42",
				},
			})
			if !strings.Contains(compiled.SQL, rawTokenRegexVerifierSQL) {
				t.Fatalf("overwritten _raw lost regex fallback:\n%s", compiled.SQL)
			}
		})
	}
}

type rawTokenIndexExpectation struct {
	candidates        int
	verifiers         int
	argumentSuffix    []any
	forbiddenLiterals []string
}

func assertRawTokenIndexContract(
	t *testing.T,
	compiled CompiledQuery,
	want rawTokenIndexExpectation,
) {
	t.Helper()

	if got := strings.Count(compiled.SQL, rawTokenIndexCandidateSQL); got != want.candidates {
		t.Fatalf("raw token-index candidate count = %d, want %d:\n%s", got, want.candidates, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, rawTokenRegexVerifierSQL); got != want.verifiers {
		t.Fatalf("raw regex verifier count = %d, want %d:\n%s", got, want.verifiers, compiled.SQL)
	}
	if got, expected := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != expected {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, expected, compiled.SQL, compiled.Args)
	}
	if len(compiled.Args) < len(want.argumentSuffix) {
		t.Fatalf("args = %#v, cannot contain suffix %#v", compiled.Args, want.argumentSuffix)
	}
	gotSuffix := compiled.Args[len(compiled.Args)-len(want.argumentSuffix):]
	if !reflect.DeepEqual(gotSuffix, want.argumentSuffix) {
		t.Fatalf("argument suffix = %#v, want %#v\nSQL: %s", gotSuffix, want.argumentSuffix, compiled.SQL)
	}
	for _, literal := range want.forbiddenLiterals {
		if strings.Contains(compiled.SQL, literal) {
			t.Fatalf("user literal %q leaked into SQL:\n%s", literal, compiled.SQL)
		}
	}
}

func assertOrderedSQLFragments(t *testing.T, sql string, fragments ...string) {
	t.Helper()

	remainder := sql
	for index, fragment := range fragments {
		position := strings.Index(remainder, fragment)
		if position < 0 {
			t.Fatalf("ordered SQL fragment %d %q is absent:\n%s", index, fragment, sql)
		}
		remainder = remainder[position+len(fragment):]
	}
}
