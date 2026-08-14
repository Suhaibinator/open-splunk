package spl_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	compatibilityV04Format  = "open-splunk-spl-compatibility-v0.4"
	compatibilityV04Version = "0.4"
)

type compatibilityV04Corpus struct {
	FormatVersion        string                 `json:"format_version"`
	CompatibilityVersion string                 `json:"compatibility_version"`
	Rules                []compatibilityV04Rule `json:"rules"`
}

type compatibilityV04Rule struct {
	ID      string                     `json:"id"`
	Summary string                     `json:"summary"`
	Cases   []compatibilityV04TestCase `json:"cases"`
}

type compatibilityV04TestCase struct {
	Name   string                        `json:"name"`
	Source string                        `json:"source"`
	Expect compatibilityV04ExpectedValue `json:"expect"`
}

type compatibilityV04ExpectedValue struct {
	Parse      string `json:"parse"`
	Diagnostic string `json:"diagnostic,omitempty"`
	Command    string `json:"command,omitempty"`
	Mode       string `json:"mode,omitempty"`
	Keys       int    `json:"keys,omitempty"`
	Outputs    int    `json:"outputs,omitempty"`
}

func TestCompatibilityV04CorpusInventoryAndParserExpectations(t *testing.T) {
	t.Parallel()

	corpus := loadCompatibilityV04Corpus(t)
	gotRules := make([]string, len(corpus.Rules))
	for index, rule := range corpus.Rules {
		gotRules[index] = rule.ID
	}
	wantRules := []string{
		"SPL-V04-LOOKUP-SYNTAX-001",
		"SPL-V04-LOOKUP-BOUNDS-001",
	}
	if !slices.Equal(gotRules, wantRules) {
		t.Fatalf("v0.4 corpus rules = %v, want %v", gotRules, wantRules)
	}

	modeInventory := make(map[string]struct{})
	diagnosticInventory := make(map[string]struct{})
	caseCount := 0
	for _, rule := range corpus.Rules {
		for _, testCase := range rule.Cases {
			caseCount++
			t.Run(rule.ID+"/"+testCase.Name, func(t *testing.T) {
				query, err := spl.Parse(testCase.Source)
				if testCase.Expect.Parse == "reject" {
					var diagnostic *spl.Diagnostic
					if !errors.As(err, &diagnostic) ||
						diagnostic.Code != testCase.Expect.Diagnostic {
						t.Fatalf("Parse error = %v, want %s", err, testCase.Expect.Diagnostic)
					}
					diagnosticInventory[testCase.Expect.Diagnostic] = struct{}{}
					return
				}
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				if len(query.Commands) != 1 {
					t.Fatalf("command count = %d, want 1", len(query.Commands))
				}
				lookup, ok := query.Commands[0].(*spl.LookupCommand)
				if !ok || lookup.Name() != testCase.Expect.Command ||
					len(lookup.Keys) != testCase.Expect.Keys ||
					len(lookup.Outputs) != testCase.Expect.Outputs {
					t.Fatalf("lookup = %#v", query.Commands[0])
				}
				mode := "OUTPUT"
				if lookup.OutputMode == spl.LookupOutputModePreserveExisting {
					mode = "OUTPUTNEW"
				}
				if mode != testCase.Expect.Mode {
					t.Fatalf("mode = %s, want %s", mode, testCase.Expect.Mode)
				}
				modeInventory[mode] = struct{}{}
			})
		}
	}
	if caseCount != 6 {
		t.Fatalf("v0.4 corpus case count = %d, want 6", caseCount)
	}
	if got := sortedV04Set(modeInventory); !slices.Equal(got, []string{"OUTPUT", "OUTPUTNEW"}) {
		t.Fatalf("mode inventory = %v", got)
	}
	if got := sortedV04Set(diagnosticInventory); !slices.Equal(got, []string{
		"SPL_EXPECTED_AS",
		"SPL_QUERY_TOO_COMPLEX",
		"SPL_UNSUPPORTED_LOOKUP_SYNTAX",
	}) {
		t.Fatalf("diagnostic inventory = %v", got)
	}
}

func loadCompatibilityV04Corpus(t *testing.T) compatibilityV04Corpus {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate v0.4 corpus test")
	}
	encoded, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "testdata", "compatibility-v0.4.json"))
	if err != nil {
		t.Fatalf("read v0.4 corpus: %v", err)
	}
	if err := rejectDuplicateJSONNames(encoded); err != nil {
		t.Fatalf("validate v0.4 corpus JSON names: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var corpus compatibilityV04Corpus
	if err := decoder.Decode(&corpus); err != nil {
		t.Fatalf("decode v0.4 corpus: %v", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		t.Fatalf("v0.4 corpus must contain one JSON value: %v", err)
	}
	if corpus.FormatVersion != compatibilityV04Format ||
		corpus.CompatibilityVersion != compatibilityV04Version {
		t.Fatalf("v0.4 corpus identity = %q/%q", corpus.FormatVersion, corpus.CompatibilityVersion)
	}
	if err := validateCompatibilityV04Corpus(corpus); err != nil {
		t.Fatal(err)
	}
	return corpus
}

func validateCompatibilityV04Corpus(corpus compatibilityV04Corpus) error {
	if len(corpus.Rules) == 0 {
		return errors.New("v0.4 corpus has no rules")
	}
	seenRules := make(map[string]struct{}, len(corpus.Rules))
	for ruleIndex, rule := range corpus.Rules {
		path := fmt.Sprintf("rules[%d]", ruleIndex)
		if !strings.HasPrefix(rule.ID, "SPL-V04-") || strings.TrimSpace(rule.Summary) == "" {
			return fmt.Errorf("%s has invalid identity or summary", path)
		}
		if _, duplicate := seenRules[rule.ID]; duplicate {
			return fmt.Errorf("%s duplicates rule %q", path, rule.ID)
		}
		seenRules[rule.ID] = struct{}{}
		if len(rule.Cases) == 0 {
			return fmt.Errorf("%s has no cases", path)
		}
		seenCases := make(map[string]struct{}, len(rule.Cases))
		for caseIndex, testCase := range rule.Cases {
			casePath := fmt.Sprintf("%s.cases[%d]", path, caseIndex)
			if testCase.Name == "" || testCase.Source == "" {
				return fmt.Errorf("%s has an empty name or source", casePath)
			}
			if _, duplicate := seenCases[testCase.Name]; duplicate {
				return fmt.Errorf("%s duplicates case %q", casePath, testCase.Name)
			}
			seenCases[testCase.Name] = struct{}{}
			switch testCase.Expect.Parse {
			case "accept":
				if testCase.Expect.Command != "lookup" ||
					(testCase.Expect.Mode != "OUTPUT" && testCase.Expect.Mode != "OUTPUTNEW") ||
					testCase.Expect.Keys < 1 || testCase.Expect.Outputs < 1 ||
					testCase.Expect.Diagnostic != "" {
					return fmt.Errorf("%s has invalid acceptance expectations", casePath)
				}
			case "reject":
				if testCase.Expect.Diagnostic == "" || testCase.Expect.Command != "" ||
					testCase.Expect.Mode != "" || testCase.Expect.Keys != 0 ||
					testCase.Expect.Outputs != 0 {
					return fmt.Errorf("%s has invalid rejection expectations", casePath)
				}
			default:
				return fmt.Errorf("%s has invalid parse expectation %q", casePath, testCase.Expect.Parse)
			}
		}
	}
	return nil
}

func sortedV04Set(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	slices.Sort(result)
	return result
}
