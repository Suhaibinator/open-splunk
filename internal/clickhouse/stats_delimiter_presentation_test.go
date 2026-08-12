package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileStatsDelimiterPresentationIsOrdinalAndListValuesOnly(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | stats delim="::" values(user) AS users `+
			`list(host) AS hosts sum(bytes) AS total sparkline(count,1h) AS trend BY service`,
	)
	wantFields := []string{"service", "users", "hosts", "total", "trend"}
	if len(compiled.OutputFields) != len(wantFields) ||
		len(compiled.OutputPresentations) != len(wantFields) {
		t.Fatalf(
			"compiled output contract = fields %#v presentations %#v",
			compiled.OutputFields,
			compiled.OutputPresentations,
		)
	}
	for index, field := range wantFields {
		if compiled.OutputFields[index] != field {
			t.Fatalf("output field %d = %q, want %q", index, compiled.OutputFields[index], field)
		}
		presentation := compiled.OutputPresentations[index]
		wantDelimiter := field == "users" || field == "hosts"
		if presentation.HasFlatMultivalueDelimiter != wantDelimiter {
			t.Fatalf("presentation %q = %#v", field, presentation)
		}
		if wantDelimiter && presentation.FlatMultivalueDelimiter != "::" {
			t.Fatalf("delimiter %q = %q", field, presentation.FlatMultivalueDelimiter)
		}
		if presentation.StatsSparkline != (field == "trend") {
			t.Fatalf("sparkline presentation %q = %#v", field, presentation)
		}
	}
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("delimiter presentation is not covered by the execution seal")
	}
}

func TestCompileStatsDelimiterPresentationDistinguishesDefaultEmptyAndAbsent(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		source  string
		want    string
		present bool
	}{
		{"default", `index=gradethis | stats values(user) AS users`, spl.DefaultStatsDelimiter, true},
		{"empty", `index=gradethis | stats delim="" values(user) AS users`, "", true},
		{"no array", `index=gradethis | stats count AS events`, "", false},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if !test.present {
				if compiled.OutputPresentations != nil {
					t.Fatalf("absent presentation = %#v", compiled.OutputPresentations)
				}
				return
			}
			if len(compiled.OutputPresentations) != 1 ||
				!compiled.OutputPresentations[0].HasFlatMultivalueDelimiter ||
				compiled.OutputPresentations[0].FlatMultivalueDelimiter != test.want {
				t.Fatalf("presentation = %#v, want present %q", compiled.OutputPresentations, test.want)
			}
		})
	}
	sparkline := compileSPL(t, `index=gradethis | stats sparkline(count,1h) AS trend`)
	if len(sparkline.OutputPresentations) != 1 ||
		!sparkline.OutputPresentations[0].StatsSparkline ||
		sparkline.OutputPresentations[0].HasFlatMultivalueDelimiter {
		t.Fatalf("sparkline presentation = %#v", sparkline.OutputPresentations)
	}
}

func TestCompileStatsDelimiterPresentationSurvivesExactProjectionAndRename(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | stats delim=" / " values(user) AS users list(host) AS hosts `+
			`| rename users AS renamed | table hosts renamed`,
	)
	if len(compiled.OutputFields) != 2 || len(compiled.OutputPresentations) != 2 ||
		compiled.OutputFields[0] != "hosts" || compiled.OutputFields[1] != "renamed" {
		t.Fatalf("projected contract = %#v / %#v", compiled.OutputFields, compiled.OutputPresentations)
	}
	for index, presentation := range compiled.OutputPresentations {
		if !presentation.HasFlatMultivalueDelimiter ||
			presentation.FlatMultivalueDelimiter != " / " {
			t.Fatalf("projected presentation %d = %#v", index, presentation)
		}
	}
}

func TestCompileStatsDelimiterPresentationClearsOnOverwrite(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | stats delim=comma values(user) AS users list(host) AS hosts `+
			`| eval users="replacement" | table users hosts`,
	)
	if len(compiled.OutputPresentations) != 2 ||
		compiled.OutputPresentations[0] != (ResultFieldPresentation{}) ||
		!compiled.OutputPresentations[1].HasFlatMultivalueDelimiter ||
		compiled.OutputPresentations[1].FlatMultivalueDelimiter != "comma" {
		t.Fatalf("overwrite presentation = %#v", compiled.OutputPresentations)
	}
}

func TestCompileStatsDelimiterPresentationResetsAcrossDownstreamStats(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | stats delim=" / " values(user) AS users BY host `+
			`| stats delim=comma list(host) AS hosts BY users`,
	)
	if len(compiled.OutputFields) != 2 || len(compiled.OutputPresentations) != 2 ||
		compiled.OutputFields[0] != "users" || compiled.OutputFields[1] != "hosts" {
		t.Fatalf("downstream contract = %#v / %#v", compiled.OutputFields, compiled.OutputPresentations)
	}
	if compiled.OutputPresentations[0] != (ResultFieldPresentation{}) ||
		!compiled.OutputPresentations[1].HasFlatMultivalueDelimiter ||
		compiled.OutputPresentations[1].FlatMultivalueDelimiter != "comma" {
		t.Fatalf("downstream presentation = %#v", compiled.OutputPresentations)
	}
}

func TestCompiledQueryDelimiterPresentationSealCloneAndRetainedBytes(t *testing.T) {
	t.Parallel()

	withDelimiter := compileSPL(t, `index=gradethis | stats delim="::" values(user) AS users`)
	withoutDelimiter := withDelimiter
	withoutDelimiter.OutputPresentations = nil
	withoutDelimiter.executionSeal = nil
	withoutDelimiter, err := sealCompiledQueryExecution(withoutDelimiter)
	if err != nil {
		t.Fatalf("seal presentation-free comparison query: %v", err)
	}
	baseBytes, baseOK := withoutDelimiter.RetainedBytes()
	delimiterBytes, delimiterOK := withDelimiter.RetainedBytes()
	cloned, cloneOK := withDelimiter.CloneForExecution()
	if !baseOK || !delimiterOK || !cloneOK || delimiterBytes <= baseBytes ||
		!withDelimiter.EqualForExecution(cloned) ||
		&withDelimiter.OutputPresentations[0] == &cloned.OutputPresentations[0] {
		t.Fatalf(
			"delimiter authority = retained %d/%d valid %t/%t/%t",
			delimiterBytes,
			baseBytes,
			baseOK,
			delimiterOK,
			cloneOK,
		)
	}
	cloned.OutputPresentations[0].FlatMultivalueDelimiter = "tampered"
	if cloned.HasValidExecutionSeal() || !withDelimiter.HasValidExecutionSeal() {
		t.Fatal("presentation mutation did not invalidate only the detached clone")
	}

	for _, mutate := range []func(*CompiledQuery){
		func(query *CompiledQuery) {
			query.OutputPresentations = []ResultFieldPresentation{{}, {}}
		},
		func(query *CompiledQuery) {
			query.OutputPresentations[0].HasFlatMultivalueDelimiter = false
		},
		func(query *CompiledQuery) {
			query.OutputPresentations[0].FlatMultivalueDelimiter = string([]byte{0xff})
		},
		func(query *CompiledQuery) {
			query.OutputPresentations[0].FlatMultivalueDelimiter = strings.Repeat(
				"x",
				MaximumResultFieldFlatDelimiterBytes+1,
			)
		},
	} {
		candidate := withDelimiter
		candidate.OutputPresentations = cloneResultFieldPresentations(
			withDelimiter.OutputPresentations,
		)
		candidate.executionSeal = nil
		mutate(&candidate)
		if _, err := sealCompiledQueryExecution(candidate); err == nil {
			t.Fatalf("invalid presentation sealed: %#v", candidate.OutputPresentations)
		}
	}
}
