package spl_test

import (
	"encoding/json"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestLookupCorpusParserExpectations(t *testing.T) {
	t.Parallel()

	corpusPath, _ := compatibilityCorpusPaths(t)
	encoded, err := os.ReadFile(corpusPath)
	if err != nil {
		t.Fatalf("read compatibility corpus: %v", err)
	}
	corpus, err := loadCompatibilityCorpus(encoded)
	if err != nil {
		t.Fatalf("load compatibility corpus: %v", err)
	}

	modeInventory := make(map[string]struct{})
	diagnosticInventory := make(map[string]struct{})
	caseCount := 0
	for _, rule := range corpus.Rules {
		if rule.ID != "SPL-LOOKUP-SYNTAX-001" && rule.ID != "SPL-LOOKUP-BOUNDS-001" {
			continue
		}
		for _, testCase := range rule.Cases {
			caseCount++
			t.Run(rule.ID+"/"+testCase.Name, func(t *testing.T) {
				parse, _ := compatibilityExpectedString(testCase, "parse")
				diagnostic, _ := compatibilityExpectedString(testCase, "diagnostic")
				query, parseErr := spl.Parse(testCase.Source)
				if parse == "reject" {
					var got *spl.Diagnostic
					if !errors.As(parseErr, &got) || got.Code != diagnostic {
						t.Fatalf("Parse error = %v, want %s", parseErr, diagnostic)
					}
					diagnosticInventory[diagnostic] = struct{}{}
					return
				}
				if parseErr != nil {
					t.Fatalf("Parse: %v", parseErr)
				}
				if len(query.Commands) != 1 {
					t.Fatalf("command count = %d, want 1", len(query.Commands))
				}
				lookup, ok := query.Commands[0].(*spl.LookupCommand)
				if !ok || lookup.Name() != "lookup" {
					t.Fatalf("lookup = %#v", query.Commands[0])
				}
				wantKeys := corpusExpectedInt(testCase, "keys")
				wantOutputs := corpusExpectedInt(testCase, "outputs")
				if len(lookup.Keys) != wantKeys || len(lookup.Outputs) != wantOutputs {
					t.Fatalf(
						"lookup key/output counts = %d/%d, want %d/%d",
						len(lookup.Keys), len(lookup.Outputs), wantKeys, wantOutputs,
					)
				}
				wantMode, _ := compatibilityExpectedString(testCase, "mode")
				mode := "OUTPUT"
				if lookup.OutputMode == spl.LookupOutputModePreserveExisting {
					mode = "OUTPUTNEW"
				}
				if mode != wantMode {
					t.Fatalf("mode = %s, want %s", mode, wantMode)
				}
				modeInventory[mode] = struct{}{}
			})
		}
	}
	if caseCount != 6 {
		t.Fatalf("lookup corpus case count = %d, want 6", caseCount)
	}
	if got := sortedSet(modeInventory); !slices.Equal(got, []string{"OUTPUT", "OUTPUTNEW"}) {
		t.Fatalf("mode inventory = %v", got)
	}
	if got := sortedSet(diagnosticInventory); !slices.Equal(got, []string{
		"SPL_EXPECTED_AS",
		"SPL_QUERY_TOO_COMPLEX",
		"SPL_UNSUPPORTED_LOOKUP_SYNTAX",
	}) {
		t.Fatalf("diagnostic inventory = %v", got)
	}
}

func corpusExpectedInt(testCase compatibilityCase, name string) int {
	var value int
	if err := json.Unmarshal(testCase.Expect[name], &value); err != nil {
		panic("validated corpus integer expectation became undecodable: " + err.Error())
	}
	return value
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	slices.Sort(result)
	return result
}
