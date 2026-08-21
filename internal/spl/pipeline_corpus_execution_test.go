package spl_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	pipelineCorpusReferenceRunner  = "reference"
	pipelineCorpusEvidenceRunner   = "evidence"
	pipelineCorpusMaximumCellDepth = 8
)

type pipelineExecutableCorpus struct {
	FormatVersion  uint32                         `json:"format_version"`
	Fixtures       map[string]json.RawMessage     `json:"fixtures"`
	Rules          []pipelineExecutableCorpusRule `json:"rules"`
	StatsInventory json.RawMessage                `json:"stats_inventory"`
}

type pipelineExecutableCorpusRule struct {
	ID      string                         `json:"id"`
	Summary string                         `json:"summary"`
	Cases   []pipelineExecutableCorpusCase `json:"cases"`
}

type pipelineExecutableCorpusCase struct {
	Name       string                     `json:"name"`
	Profile    string                     `json:"profile,omitempty"`
	Source     string                     `json:"source,omitempty"`
	Expression string                     `json:"expression,omitempty"`
	Row        json.RawMessage            `json:"row,omitempty"`
	RowFixture string                     `json:"row_fixture,omitempty"`
	Evidence   string                     `json:"evidence,omitempty"`
	Fixture    string                     `json:"fixture,omitempty"`
	Expect     map[string]json.RawMessage `json:"expect"`
}

type pipelineCorpusFixture struct {
	Runner     string                        `json:"runner"`
	Evidence   string                        `json:"evidence,omitempty"`
	Operation  string                        `json:"operation,omitempty"`
	Input      *pipelineCorpusTable          `json:"input,omitempty"`
	Program    []json.RawMessage             `json:"program,omitempty"`
	Expected   *pipelineCorpusTable          `json:"expected,omitempty"`
	Targets    []pipelineCorpusTestTarget    `json:"targets,omitempty"`
	Assertions []string                      `json:"assertions,omitempty"`
	Claim      map[string]json.RawMessage    `json:"claim,omitempty"`
	Failure    *pipelineCorpusFailure        `json:"failure,omitempty"`
	Trials     []pipelineCorpusEvidenceTrial `json:"trials,omitempty"`
}

type pipelineCorpusEvidenceTrial struct {
	Name       string                `json:"name"`
	Input      pipelineCorpusTable   `json:"input"`
	Failure    pipelineCorpusFailure `json:"failure"`
	Assertions []string              `json:"assertions"`
}

type pipelineCorpusTable struct {
	Schema []pipelineCorpusColumn `json:"schema"`
	Rows   [][]pipelineCorpusCell `json:"rows"`
}

type pipelineCorpusColumn struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Nullable   bool   `json:"nullable"`
	Multivalue bool   `json:"multivalue"`
}

type pipelineCorpusCell map[string]json.RawMessage

type pipelineCorpusTestTarget struct {
	Path string `json:"path"`
	Test string `json:"test"`
}

type pipelineCorpusFailure struct {
	Phase       string `json:"phase"`
	Code        string `json:"code"`
	Marker      string `json:"marker,omitempty"`
	Atomic      bool   `json:"atomic"`
	RowCount    uint64 `json:"row_count"`
	ResultBytes uint64 `json:"result_bytes"`
}

type pipelineCorpusOperation struct {
	Op          string                     `json:"op"`
	Field       string                     `json:"field,omitempty"`
	Input       string                     `json:"input,omitempty"`
	Output      string                     `json:"output,omitempty"`
	Pattern     string                     `json:"pattern,omitempty"`
	Negated     bool                       `json:"negated,omitempty"`
	Parts       []pipelineCorpusConcatPart `json:"parts,omitempty"`
	AllRequired bool                       `json:"all_required,omitempty"`
	Fields      []string                   `json:"fields,omitempty"`
	Value       string                     `json:"value,omitempty"`
	Minimum     float64                    `json:"minimum,omitempty"`
	Maximum     float64                    `json:"maximum,omitempty"`
	Started     float64                    `json:"started,omitempty"`
	SID         string                     `json:"sid,omitempty"`
	Period      int                        `json:"period,omitempty"`
	Delimiter   string                     `json:"delimiter,omitempty"`
	AllowEmpty  bool                       `json:"allow_empty,omitempty"`
	Limit       int                        `json:"limit,omitempty"`
}

type pipelineCorpusConcatPart struct {
	Field   string `json:"field,omitempty"`
	Literal string `json:"literal,omitempty"`
}

func TestPipelineCorpusFixturesAreStrictBoundAndExecutable(t *testing.T) {
	t.Parallel()

	corpus := loadPipelineExecutableCorpus(t)
	fixtures, err := validatePipelineExecutableCorpus(corpus, pipelineExecutableCorpusRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(fixtures) != 37 {
		t.Fatalf("fixture count = %d, want 37", len(fixtures))
	}
	caseCount := 0
	for _, rule := range corpus.Rules {
		caseCount += len(rule.Cases)
	}
	if caseCount != 64 {
		t.Fatalf("case count = %d, want 64", caseCount)
	}
	var referenceCount, evidenceCount int
	for id, fixture := range fixtures {
		switch fixture.Runner {
		case pipelineCorpusReferenceRunner:
			referenceCount++
			t.Run(id, func(t *testing.T) {
				runPipelineReferenceCorpusFixture(t, fixture)
			})
		case pipelineCorpusEvidenceRunner:
			evidenceCount++
		default:
			t.Fatalf("fixture %q has unvalidated runner %q", id, fixture.Runner)
		}
	}
	if referenceCount != 22 || evidenceCount != 15 {
		t.Fatalf("runner counts = reference %d evidence %d, want 22/15", referenceCount, evidenceCount)
	}
}

func TestPipelineCorpusFixtureSchemaRejectsHostileMutations(t *testing.T) {
	t.Parallel()

	validReference := `{
		"runner":"reference",
		"input":{"schema":[{"name":"value","kind":"string","nullable":false,"multivalue":false}],"rows":[[{"string":"x"}]]},
		"program":[{"op":"reverse"}],
		"expected":{"schema":[{"name":"value","kind":"string","nullable":false,"multivalue":false}],"rows":[[{"string":"x"}]]}
	}`
	for _, test := range []struct {
		name    string
		encoded string
		want    string
	}{
		{name: "unknown fixture field", encoded: strings.Replace(validReference, `"runner":`, `"typo":true,"runner":`, 1), want: "unknown field"},
		{name: "duplicate fixture field", encoded: strings.Replace(validReference, `"runner":"reference"`, `"runner":"reference","runner":"reference"`, 1), want: "duplicate"},
		{name: "reference polluted by targets", encoded: strings.Replace(validReference, `"program":`, `"targets":[{"path":"x_test.go","test":"TestX"}],"program":`, 1), want: "reference fixture"},
		{name: "multivalue scalar schema", encoded: strings.Replace(validReference, `"multivalue":false`, `"multivalue":true`, 1), want: "multivalue"},
		{name: "row width", encoded: strings.Replace(validReference, `[{"string":"x"}]`, `[]`, 1), want: "row 0 has"},
		{name: "multiple cell tags", encoded: strings.Replace(validReference, `{"string":"x"}`, `{"string":"x","null":true}`, 1), want: "exactly one tag"},
		{name: "operation union pollution", encoded: strings.Replace(validReference, `{"op":"reverse"}`, `{"op":"reverse","field":"value"}`, 1), want: "exact field set"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodePipelineCorpusFixture("hostile", json.RawMessage(test.encoded)); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("decode error = %v, want substring %q", err, test.want)
			}
		})
	}

	evidenceBoth := `{
		"runner":"evidence",
		"targets":[{"path":"internal/queryexec/pipeline_runtime_marker_adversarial_test.go","test":"TestPipelineRuntimeMarkersAreCompletelyClassifiedAndRedacted"}],
		"expected":{"schema":[{"name":"value","kind":"string","nullable":false,"multivalue":false}],"rows":[]},
		"failure":{"phase":"executor","code":"invalid-result","atomic":true,"row_count":0,"result_bytes":0}
	}`
	if _, err := decodePipelineCorpusFixture("ambiguous", json.RawMessage(evidenceBoth)); err == nil ||
		!strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("ambiguous evidence error = %v", err)
	}
}

func TestPipelineCorpusFixtureBindingsRejectDanglingUnusedAndMissingCases(t *testing.T) {
	t.Parallel()

	corpus := loadPipelineExecutableCorpus(t)
	root := pipelineExecutableCorpusRoot(t)

	dangling := clonePipelineExecutableCorpus(corpus)
	dangling.Rules[3].Cases[0].Fixture = "missing.fixture"
	if _, err := validatePipelineExecutableCorpus(dangling, root); err == nil ||
		!strings.Contains(err.Error(), "unknown fixture") {
		t.Fatalf("dangling fixture error = %v", err)
	}

	unused := clonePipelineExecutableCorpus(corpus)
	unused.Fixtures["unused.fixture"] = slices.Clone(unused.Fixtures["reference.reverse"])
	if _, err := validatePipelineExecutableCorpus(unused, root); err == nil ||
		!strings.Contains(err.Error(), "unused") {
		t.Fatalf("unused fixture error = %v", err)
	}

	missing := clonePipelineExecutableCorpus(corpus)
	missing.Rules[3].Cases[0].Fixture = ""
	if _, err := validatePipelineExecutableCorpus(missing, root); err == nil ||
		!strings.Contains(err.Error(), "requires an executable fixture") {
		t.Fatalf("missing semantic fixture error = %v", err)
	}

	duplicated := clonePipelineExecutableCorpus(corpus)
	duplicated.Rules[3].Cases[1].Fixture = duplicated.Rules[3].Cases[0].Fixture
	if _, err := validatePipelineExecutableCorpus(duplicated, root); err == nil ||
		!strings.Contains(err.Error(), "reuses fixture") {
		t.Fatalf("reused fixture error = %v", err)
	}
}

func TestPipelineCorpusSourceProgramRejectsUnmodeledCommands(t *testing.T) {
	t.Parallel()

	reverse := json.RawMessage(`{"op":"reverse"}`)
	addInfo := json.RawMessage(`{"op":"addinfo"}`)
	for _, test := range []struct {
		name    string
		source  string
		program []json.RawMessage
		wantErr bool
	}{
		{
			name:    "non-pipeline setup and projection remain allowed",
			source:  `index=main | sort 0 +event_id | reverse | table event_id`,
			program: []json.RawMessage{reverse},
		},
		{
			name:    "extra pipeline command before modeled program",
			source:  `index=main | addinfo | reverse`,
			program: []json.RawMessage{reverse},
			wantErr: true,
		},
		{
			name:    "extra pipeline command between modeled operations",
			source:  `index=main | reverse | regex "x" | addinfo`,
			program: []json.RawMessage{reverse, addInfo},
			wantErr: true,
		},
		{
			name:    "extra pipeline command after modeled program",
			source:  `index=main | reverse | addinfo`,
			program: []json.RawMessage{reverse},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validatePipelineCorpusSourceProgram(test.source, test.program)
			if test.wantErr && (err == nil || !strings.Contains(err.Error(), "pipeline command count")) {
				t.Fatalf("validation error = %v, want exact pipeline command-count rejection", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

func TestPipelineCorpusOracleRejectsSchemaSourceEvidenceAndFailureDrift(t *testing.T) {
	t.Parallel()

	valid := loadPipelineExecutableCorpus(t)
	root := pipelineExecutableCorpusRoot(t)
	for _, test := range []struct {
		name   string
		mutate func(*pipelineExecutableCorpus)
		want   string
	}{
		{
			name: "derived ordered schema",
			mutate: func(corpus *pipelineExecutableCorpus) {
				raw := string(corpus.Fixtures["reference.reverse"])
				last := strings.LastIndex(raw, `"nullable": false`)
				corpus.Fixtures["reference.reverse"] = json.RawMessage(raw[:last] + `"nullable": true` + raw[last+len(`"nullable": false`):])
			},
			want: "expected schema does not exactly match",
		},
		{
			name: "source arguments",
			mutate: func(corpus *pipelineExecutableCorpus) {
				for ruleIndex := range corpus.Rules {
					for caseIndex := range corpus.Rules[ruleIndex].Cases {
						testCase := &corpus.Rules[ruleIndex].Cases[caseIndex]
						if testCase.Fixture == "reference.regex-positive" {
							testCase.Source = strings.Replace(testCase.Source, `(?i)timeout`, `(?i)different`, 1)
						}
					}
				}
			},
			want: "arguments do not exactly match",
		},
		{
			name: "registry target",
			mutate: func(corpus *pipelineExecutableCorpus) {
				raw := string(corpus.Fixtures["evidence.optional-multivalue"])
				raw = strings.Replace(raw,
					"TestPipelineOptionalMultivalueTransportDistinguishesNullEmptyAndMembers",
					"TestPipelineRealMakeMVCompilerTransportPublishesNullEmptyAndOrderedMembers", 1)
				corpus.Fixtures["evidence.optional-multivalue"] = json.RawMessage(raw)
			},
			want: "do not exactly match evidence registry",
		},
		{
			name: "assertion token",
			mutate: func(corpus *pipelineExecutableCorpus) {
				raw := string(corpus.Fixtures["evidence.sink-ceiling"])
				raw = strings.Replace(raw, `"FailureResourceLimit"`, `"ABSENT_ASSERTION_TOKEN"`, 1)
				corpus.Fixtures["evidence.sink-ceiling"] = json.RawMessage(raw)
			},
			want: "absent from every exact evidence target",
		},
		{
			name: "claim expectation",
			mutate: func(corpus *pipelineExecutableCorpus) {
				for ruleIndex := range corpus.Rules {
					for caseIndex := range corpus.Rules[ruleIndex].Cases {
						testCase := &corpus.Rules[ruleIndex].Cases[caseIndex]
						if testCase.Fixture == "evidence.saved-search" {
							testCase.Expect["surfaces"] = json.RawMessage(`"history"`)
						}
					}
				}
			},
			want: "does not exactly match fixture claim",
		},
		{
			name: "failure marker",
			mutate: func(corpus *pipelineExecutableCorpus) {
				raw := string(corpus.Fixtures["evidence.query-wide-expansion"])
				raw = strings.Replace(raw, "mvexpand query rows exceed the limit", "unknown marker", 1)
				corpus.Fixtures["evidence.query-wide-expansion"] = json.RawMessage(raw)
			},
			want: "unknown marker",
		},
		{
			name: "operation invariant",
			mutate: func(corpus *pipelineExecutableCorpus) {
				raw := string(corpus.Fixtures["reference.accum-output"])
				raw = strings.Replace(raw, `"output": "running_bytes"`, `"output": ""`, 1)
				corpus.Fixtures["reference.accum-output"] = json.RawMessage(raw)
			},
			want: "accum input and output must be nonempty",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			corpus := clonePipelineExecutableCorpus(valid)
			test.mutate(&corpus)
			if _, err := validatePipelineExecutableCorpus(corpus, root); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func loadPipelineExecutableCorpus(t *testing.T) pipelineExecutableCorpus {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join(
		pipelineExecutableCorpusRoot(t), "internal/spl/testdata/compatibility.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := rejectPipelineDuplicateJSONNames(encoded); err != nil {
		t.Fatalf("duplicate corpus JSON name: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var corpus pipelineExecutableCorpus
	if err := decoder.Decode(&corpus); err != nil {
		t.Fatalf("decode executable pipeline corpus: %v", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		t.Fatalf("decode trailing executable corpus data: %v", err)
	}
	corpus.Rules = pipelineExecutableCorpusRules(corpus.Rules)
	return corpus
}

func validatePipelineExecutableCorpus(
	corpus pipelineExecutableCorpus,
	repositoryRoot string,
) (map[string]pipelineCorpusFixture, error) {
	if corpus.FormatVersion != compatibilityCorpusFormat {
		return nil, errors.New("executable corpus identity is invalid")
	}
	if len(corpus.Fixtures) == 0 {
		return nil, errors.New("executable corpus has no fixtures")
	}
	fixtures := make(map[string]pipelineCorpusFixture, len(corpus.Fixtures))
	for id, raw := range corpus.Fixtures {
		if !validPipelineCorpusFixtureID(id) {
			return nil, fmt.Errorf("fixture id %q is invalid", id)
		}
		fixture, err := decodePipelineCorpusFixture(id, raw)
		if err != nil {
			return nil, err
		}
		if fixture.Runner == pipelineCorpusReferenceRunner {
			actualSchema, err := pipelineCorpusReferenceSchema(fixture.Input.Schema, fixture.Program)
			if err != nil {
				return nil, fmt.Errorf("fixture %q: %w", id, err)
			}
			if !reflect.DeepEqual(actualSchema, fixture.Expected.Schema) {
				return nil, fmt.Errorf("fixture %q expected schema does not exactly match derived ordered schema", id)
			}
		}
		if fixture.Runner == pipelineCorpusEvidenceRunner {
			registered, ok := pipelineEvidenceRegistry[fixture.Evidence]
			if !ok || !pipelineCorpusTargetsEqual(fixture.Targets, registered) {
				return nil, fmt.Errorf("fixture %q targets do not exactly match evidence registry %q", id, fixture.Evidence)
			}
			for _, target := range fixture.Targets {
				if err := validatePipelineCorpusTestTarget(repositoryRoot, target); err != nil {
					return nil, fmt.Errorf("fixture %q: %w", id, err)
				}
			}
			assertions := slices.Clone(fixture.Assertions)
			for _, trial := range fixture.Trials {
				assertions = append(assertions, trial.Assertions...)
			}
			if err := validatePipelineCorpusAssertionTargets(repositoryRoot, fixture.Targets, assertions); err != nil {
				return nil, fmt.Errorf("fixture %q: %w", id, err)
			}
		}
		fixtures[id] = fixture
	}

	used := make(map[string]string, len(fixtures))
	for _, rule := range corpus.Rules {
		for _, testCase := range rule.Cases {
			caseID := rule.ID + "/" + testCase.Name
			if pipelineCorpusCaseRequiresFixture(testCase) && testCase.Fixture == "" {
				return nil, fmt.Errorf("case %s requires an executable fixture", caseID)
			}
			if testCase.Fixture == "" {
				if _, surface := testCase.Expect["surfaces"]; surface && testCase.Evidence == "" {
					return nil, fmt.Errorf("surface case %s lacks exact evidence", caseID)
				}
				continue
			}
			fixture, present := fixtures[testCase.Fixture]
			if !present {
				return nil, fmt.Errorf("case %s references unknown fixture %q", caseID, testCase.Fixture)
			}
			if previous, duplicate := used[testCase.Fixture]; duplicate {
				return nil, fmt.Errorf("case %s reuses fixture %q already bound to %s", caseID, testCase.Fixture, previous)
			}
			used[testCase.Fixture] = caseID
			if fixture.Runner == pipelineCorpusEvidenceRunner {
				if testCase.Evidence == "" || testCase.Evidence != fixture.Evidence {
					return nil, fmt.Errorf("case %s does not bind fixture evidence %q exactly", caseID, fixture.Evidence)
				}
				if len(fixture.Claim) != 0 && !pipelineCorpusJSONMapsEqual(fixture.Claim, testCase.Expect) {
					return nil, fmt.Errorf("case %s expectation does not exactly match fixture claim", caseID)
				}
				if err := validatePipelineCorpusFailureExpectation(testCase.Expect, fixture); err != nil {
					return nil, fmt.Errorf("case %s: %w", caseID, err)
				}
			}
			if testCase.Source != "" && fixture.Runner == pipelineCorpusReferenceRunner {
				if err := validatePipelineCorpusSourceProgram(testCase.Source, fixture.Program); err != nil {
					return nil, fmt.Errorf("case %s: %w", caseID, err)
				}
			}
		}
	}
	for id := range fixtures {
		if _, ok := used[id]; !ok {
			return nil, fmt.Errorf("fixture %q is unused", id)
		}
	}
	return fixtures, nil
}

func pipelineExecutableCorpusRules(rules []pipelineExecutableCorpusRule) []pipelineExecutableCorpusRule {
	result := make([]pipelineExecutableCorpusRule, 0, len(rules))
	for _, rule := range rules {
		if isPipelineCorpusRuleID(rule.ID) {
			result = append(result, rule)
			continue
		}
		if rule.ID != "SPL-PROFILE-001" {
			continue
		}
		profile := rule
		profile.Cases = nil
		for _, testCase := range rule.Cases {
			if testCase.Fixture != "" {
				profile.Cases = append(profile.Cases, testCase)
			}
		}
		if len(profile.Cases) != 0 {
			result = append(result, profile)
		}
	}
	return result
}

func validatePipelineCorpusFailureExpectation(
	expect map[string]json.RawMessage,
	fixture pipelineCorpusFixture,
) error {
	failures := make([]pipelineCorpusFailure, 0, 1+len(fixture.Trials))
	if fixture.Failure != nil {
		failures = append(failures, *fixture.Failure)
	}
	for _, trial := range fixture.Trials {
		failures = append(failures, trial.Failure)
	}
	if len(failures) == 0 {
		return nil
	}
	phaseRaw, phasePresent := expect["diagnostic_phase"]
	atomicRaw, atomicPresent := expect["atomic"]
	if !phasePresent || !atomicPresent {
		return errors.New("failure evidence must declare exact diagnostic_phase and atomic expectations")
	}
	var phase string
	var atomic bool
	if decodePipelineCorpusJSON(phaseRaw, &phase) != nil || decodePipelineCorpusJSON(atomicRaw, &atomic) != nil || !atomic {
		return errors.New("failure evidence has invalid phase or atomic expectation")
	}
	for _, failure := range failures {
		if failure.Phase != phase {
			return fmt.Errorf("failure phase %q differs from expectation %q", failure.Phase, phase)
		}
		if diagnosticRaw, present := expect["diagnostic"]; present {
			var diagnostic string
			if decodePipelineCorpusJSON(diagnosticRaw, &diagnostic) != nil || diagnostic != failure.Code {
				return fmt.Errorf("failure code %q differs from diagnostic expectation", failure.Code)
			}
		}
	}
	return nil
}

func pipelineCorpusCaseRequiresFixture(testCase pipelineExecutableCorpusCase) bool {
	for _, name := range []string{"value", "type", "order", "resource", "relation", "compatibility", "surfaces"} {
		if _, present := testCase.Expect[name]; present {
			return true
		}
	}
	if raw, present := testCase.Expect["diagnostic_phase"]; present {
		var phase string
		if json.Unmarshal(raw, &phase) != nil || phase != "parser" {
			return true
		}
	}
	return false
}

func decodePipelineCorpusFixture(id string, raw json.RawMessage) (pipelineCorpusFixture, error) {
	if err := rejectPipelineDuplicateJSONNames(raw); err != nil {
		return pipelineCorpusFixture{}, fmt.Errorf("fixture %q has duplicate JSON name: %w", id, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var fixture pipelineCorpusFixture
	if err := decoder.Decode(&fixture); err != nil {
		return pipelineCorpusFixture{}, fmt.Errorf("decode fixture %q: %w", id, err)
	}
	if fixture.Input != nil {
		if err := validatePipelineCorpusTable(*fixture.Input, "input"); err != nil {
			return pipelineCorpusFixture{}, fmt.Errorf("fixture %q: %w", id, err)
		}
	}
	if fixture.Expected != nil {
		if err := validatePipelineCorpusTable(*fixture.Expected, "expected"); err != nil {
			return pipelineCorpusFixture{}, fmt.Errorf("fixture %q: %w", id, err)
		}
	}
	switch fixture.Runner {
	case pipelineCorpusReferenceRunner:
		if fixture.Input == nil || fixture.Expected == nil || len(fixture.Program) == 0 ||
			fixture.Evidence != "" || fixture.Operation != "" || len(fixture.Targets) != 0 || len(fixture.Assertions) != 0 ||
			len(fixture.Claim) != 0 || fixture.Failure != nil || len(fixture.Trials) != 0 {
			return pipelineCorpusFixture{}, fmt.Errorf("fixture %q reference fixture has an invalid union shape", id)
		}
		for _, operation := range fixture.Program {
			if _, err := decodePipelineCorpusOperation(operation); err != nil {
				return pipelineCorpusFixture{}, fmt.Errorf("fixture %q: %w", id, err)
			}
		}
	case pipelineCorpusEvidenceRunner:
		selected := 0
		for _, present := range []bool{
			fixture.Expected != nil, fixture.Failure != nil, len(fixture.Claim) != 0, len(fixture.Trials) != 0,
		} {
			if present {
				selected++
			}
		}
		if fixture.Evidence == "" || len(fixture.Program) != 0 || len(fixture.Targets) == 0 || selected != 1 {
			return pipelineCorpusFixture{}, fmt.Errorf("fixture %q evidence fixture must select exactly one success, failure, claim, or trial matrix", id)
		}
		if fixture.Failure != nil {
			if err := validatePipelineCorpusFailure(*fixture.Failure); err != nil {
				return pipelineCorpusFixture{}, fmt.Errorf("fixture %q: %w", id, err)
			}
		}
		if len(fixture.Trials) != 0 {
			if fixture.Input != nil || len(fixture.Assertions) != 0 ||
				!slices.Contains([]string{"makemv", "mvexpand"}, fixture.Operation) {
				return pipelineCorpusFixture{}, fmt.Errorf("fixture %q trial matrix has fixture-level input or assertions", id)
			}
			seen := make(map[string]struct{}, len(fixture.Trials))
			for index, trial := range fixture.Trials {
				if trial.Name == "" || strings.TrimSpace(trial.Name) != trial.Name {
					return pipelineCorpusFixture{}, fmt.Errorf("fixture %q trial %d has invalid name", id, index)
				}
				if _, duplicate := seen[trial.Name]; duplicate {
					return pipelineCorpusFixture{}, fmt.Errorf("fixture %q duplicates trial %q", id, trial.Name)
				}
				seen[trial.Name] = struct{}{}
				if err := validatePipelineCorpusTable(trial.Input, "trial input"); err != nil {
					return pipelineCorpusFixture{}, fmt.Errorf("fixture %q trial %q: %w", id, trial.Name, err)
				}
				if err := validatePipelineCorpusFailure(trial.Failure); err != nil {
					return pipelineCorpusFixture{}, fmt.Errorf("fixture %q trial %q: %w", id, trial.Name, err)
				}
				if err := validatePipelineCorpusAssertions(trial.Assertions); err != nil {
					return pipelineCorpusFixture{}, fmt.Errorf("fixture %q trial %q: %w", id, trial.Name, err)
				}
				if err := validatePipelineCorpusEvidenceTrialSemantics(fixture.Operation, trial); err != nil {
					return pipelineCorpusFixture{}, fmt.Errorf("fixture %q trial %q: %w", id, trial.Name, err)
				}
			}
		} else {
			if fixture.Operation != "" {
				return pipelineCorpusFixture{}, fmt.Errorf("fixture %q non-matrix evidence has an operation", id)
			}
			if err := validatePipelineCorpusAssertions(fixture.Assertions); err != nil {
				return pipelineCorpusFixture{}, fmt.Errorf("fixture %q: %w", id, err)
			}
		}
	default:
		return pipelineCorpusFixture{}, fmt.Errorf("fixture %q has invalid runner %q", id, fixture.Runner)
	}
	return fixture, nil
}

func validatePipelineCorpusEvidenceTrialSemantics(operation string, trial pipelineCorpusEvidenceTrial) error {
	if len(trial.Input.Schema) != 1 || len(trial.Input.Rows) != 1 || len(trial.Input.Rows[0]) != 1 {
		return errors.New("hostile evidence trial must contain exactly one typed cell")
	}
	cell := trial.Input.Rows[0][0]
	tag, err := validatePipelineCorpusCell(cell, 0)
	if err != nil {
		return err
	}
	rejected := false
	switch operation {
	case "makemv":
		rejected = !slices.Contains([]string{"missing", "null", "string"}, tag)
	case "mvexpand":
		switch tag {
		case "missing", "null", "string", "number", "bool", "time":
		case "list":
			var members []pipelineCorpusCell
			if err := decodePipelineCorpusJSON(cell["list"], &members); err != nil {
				return err
			}
			for _, member := range members {
				memberTag, err := validatePipelineCorpusCell(member, 1)
				if err != nil {
					return err
				}
				if memberTag != "string" && memberTag != "null" {
					rejected = true
				}
			}
		default:
			rejected = true
		}
	}
	wantMarker := "open-splunk: " + operation + " input value is unsupported"
	if !rejected || trial.Failure.Phase != "executor" || trial.Failure.Code != "unsupported-value" ||
		trial.Failure.Marker != wantMarker {
		return errors.New("hostile typed cell does not independently imply the exact structured failure")
	}
	return nil
}

func validatePipelineCorpusAssertions(assertions []string) error {
	if len(assertions) == 0 {
		return errors.New("evidence assertions are empty")
	}
	seen := make(map[string]struct{}, len(assertions))
	for _, assertion := range assertions {
		if assertion == "" || strings.TrimSpace(assertion) != assertion {
			return errors.New("evidence assertion is empty or padded")
		}
		if _, duplicate := seen[assertion]; duplicate {
			return fmt.Errorf("evidence assertion %q is duplicated", assertion)
		}
		seen[assertion] = struct{}{}
	}
	return nil
}

func validatePipelineCorpusTable(table pipelineCorpusTable, role string) error {
	if len(table.Schema) == 0 {
		return fmt.Errorf("%s schema is empty", role)
	}
	seen := make(map[string]struct{}, len(table.Schema))
	for index, column := range table.Schema {
		if column.Name == "" || strings.TrimSpace(column.Name) != column.Name {
			return fmt.Errorf("%s schema column %d has invalid name", role, index)
		}
		if _, duplicate := seen[column.Name]; duplicate {
			return fmt.Errorf("%s schema duplicates column %q", role, column.Name)
		}
		seen[column.Name] = struct{}{}
		if !slices.Contains([]string{"string", "bytes", "number", "bool", "time", "list", "object", "dynamic"}, column.Kind) {
			return fmt.Errorf("%s schema column %q has invalid kind %q", role, column.Name, column.Kind)
		}
		if column.Multivalue != (column.Kind == "list") {
			return fmt.Errorf("%s schema column %q has inconsistent multivalue flag", role, column.Name)
		}
	}
	for rowIndex, row := range table.Rows {
		if len(row) != len(table.Schema) {
			return fmt.Errorf("%s row %d has %d cells for %d columns", role, rowIndex, len(row), len(table.Schema))
		}
		for columnIndex, cell := range row {
			kind, err := validatePipelineCorpusCell(cell, 0)
			if err != nil {
				return fmt.Errorf("%s row %d cell %d: %w", role, rowIndex, columnIndex, err)
			}
			column := table.Schema[columnIndex]
			if kind == "missing" || kind == "null" {
				if !column.Nullable {
					return fmt.Errorf("%s row %d cell %d is %s in non-nullable column", role, rowIndex, columnIndex, kind)
				}
				continue
			}
			if column.Kind != "dynamic" && kind != column.Kind {
				return fmt.Errorf("%s row %d cell %d kind %s does not match %s", role, rowIndex, columnIndex, kind, column.Kind)
			}
		}
	}
	return nil
}

func validatePipelineCorpusCell(cell pipelineCorpusCell, depth int) (string, error) {
	if depth > pipelineCorpusMaximumCellDepth {
		return "", errors.New("cell nesting exceeds the supported depth")
	}
	if len(cell) != 1 {
		return "", errors.New("cell must contain exactly one tag")
	}
	for tag, raw := range cell {
		switch tag {
		case "missing", "null":
			var value bool
			if json.Unmarshal(raw, &value) != nil || !value {
				return "", fmt.Errorf("%s tag must be Boolean true", tag)
			}
			return tag, nil
		case "string":
			var value string
			if json.Unmarshal(raw, &value) != nil {
				return "", errors.New("string tag must contain a String")
			}
			return "string", nil
		case "bytes_hex":
			var value string
			if json.Unmarshal(raw, &value) != nil || len(value)%2 != 0 {
				return "", errors.New("bytes_hex tag must contain even-length lowercase hex")
			}
			decoded, err := hex.DecodeString(value)
			if err != nil || hex.EncodeToString(decoded) != value {
				return "", errors.New("bytes_hex tag must contain canonical lowercase hex")
			}
			return "bytes", nil
		case "number", "time":
			var value json.Number
			if decodePipelineCorpusJSON(raw, &value) != nil {
				return "", fmt.Errorf("%s tag must contain a number", tag)
			}
			number, err := value.Float64()
			if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
				return "", fmt.Errorf("%s tag must contain a finite number", tag)
			}
			return tag, nil
		case "bool":
			var value bool
			if json.Unmarshal(raw, &value) != nil {
				return "", errors.New("bool tag must contain a Boolean")
			}
			return "bool", nil
		case "list":
			var members []pipelineCorpusCell
			if decodePipelineCorpusJSON(raw, &members) != nil {
				return "", errors.New("list tag must contain tagged cells")
			}
			for index, member := range members {
				if _, err := validatePipelineCorpusCell(member, depth+1); err != nil {
					return "", fmt.Errorf("list member %d: %w", index, err)
				}
			}
			return "list", nil
		case "object":
			var members map[string]pipelineCorpusCell
			if decodePipelineCorpusJSON(raw, &members) != nil {
				return "", errors.New("object tag must contain tagged fields")
			}
			for name, member := range members {
				if name == "" || strings.TrimSpace(name) != name {
					return "", errors.New("object tag has an invalid field name")
				}
				if _, err := validatePipelineCorpusCell(member, depth+1); err != nil {
					return "", fmt.Errorf("object field %q: %w", name, err)
				}
			}
			return "object", nil
		default:
			return "", fmt.Errorf("unsupported cell tag %q", tag)
		}
	}
	panic("unreachable")
}

func decodePipelineCorpusOperation(raw json.RawMessage) (pipelineCorpusOperation, error) {
	if err := rejectPipelineDuplicateJSONNames(raw); err != nil {
		return pipelineCorpusOperation{}, fmt.Errorf("operation has duplicate JSON name: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := decodePipelineCorpusJSON(raw, &fields); err != nil {
		return pipelineCorpusOperation{}, fmt.Errorf("decode operation fields: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var operation pipelineCorpusOperation
	if err := decoder.Decode(&operation); err != nil {
		return pipelineCorpusOperation{}, fmt.Errorf("decode operation: %w", err)
	}
	want := map[string][]string{
		"regex":     {"op", "field", "pattern", "negated"},
		"reverse":   {"op"},
		"accum":     {"op", "input", "output"},
		"strcat":    {"op", "parts", "output", "all_required"},
		"addinfo":   {"op", "minimum", "maximum", "started", "sid"},
		"fillnull":  {"op", "fields", "value"},
		"addtotals": {"op", "fields", "output"},
		"delta":     {"op", "input", "output", "period"},
		"makemv":    {"op", "field", "delimiter", "allow_empty"},
		"mvexpand":  {"op", "field", "limit"},
	}[operation.Op]
	if len(want) == 0 || len(fields) != len(want) {
		return pipelineCorpusOperation{}, fmt.Errorf("operation %q does not use its exact field set", operation.Op)
	}
	for _, name := range want {
		if _, present := fields[name]; !present {
			return pipelineCorpusOperation{}, fmt.Errorf("operation %q does not use its exact field set", operation.Op)
		}
	}
	if operation.Op == "strcat" {
		if len(operation.Parts) < 2 || operation.Output == "" {
			return pipelineCorpusOperation{}, errors.New("strcat operation has too few parts")
		}
		for _, part := range operation.Parts {
			if (part.Field == "") == (part.Literal == "") {
				return pipelineCorpusOperation{}, errors.New("strcat part must select exactly one field or nonempty literal")
			}
		}
	}
	switch operation.Op {
	case "regex":
		if operation.Field == "" || operation.Pattern == "" {
			return pipelineCorpusOperation{}, errors.New("regex field and pattern must be nonempty")
		}
		if _, err := regexp.Compile(operation.Pattern); err != nil {
			return pipelineCorpusOperation{}, fmt.Errorf("regex pattern is invalid: %w", err)
		}
	case "accum":
		if operation.Input == "" || operation.Output == "" {
			return pipelineCorpusOperation{}, errors.New("accum input and output must be nonempty")
		}
	case "addinfo":
		if operation.SID == "" || operation.Minimum > operation.Maximum ||
			math.IsNaN(operation.Minimum) || math.IsInf(operation.Minimum, 0) ||
			math.IsNaN(operation.Maximum) || math.IsInf(operation.Maximum, 0) ||
			math.IsNaN(operation.Started) || math.IsInf(operation.Started, 0) {
			return pipelineCorpusOperation{}, errors.New("addinfo values are invalid")
		}
	case "fillnull":
		if !validPipelineCorpusFieldList(operation.Fields) {
			return pipelineCorpusOperation{}, errors.New("fillnull fields are empty or duplicated")
		}
	case "addtotals":
		if !validPipelineCorpusFieldList(operation.Fields) || operation.Output == "" {
			return pipelineCorpusOperation{}, errors.New("addtotals fields or output are invalid")
		}
	case "delta":
		if operation.Input == "" || operation.Output == "" || operation.Period < 1 {
			return pipelineCorpusOperation{}, errors.New("delta input, output, and period must be valid")
		}
	case "makemv":
		if operation.Field == "" || operation.Delimiter == "" || len(operation.Delimiter) > spl.MaximumMakeMVDelimiterBytes {
			return pipelineCorpusOperation{}, errors.New("makemv field or delimiter is invalid")
		}
	case "mvexpand":
		if operation.Field == "" || operation.Limit < 0 || uint64(operation.Limit) > spl.MaximumMVExpandLimit {
			return pipelineCorpusOperation{}, errors.New("mvexpand field or limit is invalid")
		}
	}
	return operation, nil
}

func validPipelineCorpusFieldList(fields []string) bool {
	if len(fields) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field == "" || strings.TrimSpace(field) != field {
			return false
		}
		if _, duplicate := seen[field]; duplicate {
			return false
		}
		seen[field] = struct{}{}
	}
	return true
}

func validatePipelineCorpusFailure(failure pipelineCorpusFailure) error {
	if !failure.Atomic || failure.RowCount != 0 || failure.ResultBytes != 0 {
		return errors.New("structured failure is outside the exact admitted domain")
	}
	switch failure.Phase + "/" + failure.Code {
	case "executor/invalid-result", "publication-preflight/resource-limit":
		if failure.Marker != "" {
			return errors.New("non-backend structured failure must not carry a marker")
		}
	case "executor/unsupported-value":
		if !slices.Contains([]string{
			"open-splunk: makemv input value is unsupported",
			"open-splunk: mvexpand input value is unsupported",
		}, failure.Marker) {
			return errors.New("unsupported-value failure has an unknown marker")
		}
	case "executor/execution-limit":
		if !slices.Contains([]string{
			"open-splunk: makemv row members exceed the limit",
			"open-splunk: makemv row bytes exceed the limit",
			"open-splunk: makemv result members exceed the limit",
			"open-splunk: makemv result bytes exceed the limit",
			"open-splunk: makemv retained bytes exceed the limit",
			"open-splunk: mvexpand row members exceed the limit",
			"open-splunk: mvexpand stage rows exceed the limit",
			"open-splunk: mvexpand query rows exceed the limit",
			"open-splunk: mvexpand retained bytes exceed the limit",
		}, failure.Marker) {
			return errors.New("execution-limit failure has an unknown marker")
		}
	default:
		return errors.New("structured failure phase/code pair is outside the exact admitted domain")
	}
	return nil
}

func validatePipelineCorpusTestTarget(repositoryRoot string, target pipelineCorpusTestTarget) error {
	if target.Path == "" || target.Test == "" || filepath.IsAbs(target.Path) ||
		(!strings.HasSuffix(target.Path, "_test.go") && !strings.HasSuffix(target.Path, ".test.ts")) {
		return errors.New("evidence target is not a relative Go or TypeScript test file")
	}
	absolute := filepath.Clean(filepath.Join(repositoryRoot, target.Path))
	relative, err := filepath.Rel(repositoryRoot, absolute)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("evidence target escapes the repository")
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("evidence target is missing or not a regular file")
	}
	contents, err := os.ReadFile(absolute)
	if err != nil {
		return err
	}
	declaration := regexp.MustCompile(`(?m)^func\s+` + regexp.QuoteMeta(target.Test) + `\s*\(`)
	if strings.HasSuffix(target.Path, ".test.ts") {
		declaration = regexp.MustCompile(`(?m)\b(?:it|test)\s*\(\s*["'\x60]` + regexp.QuoteMeta(target.Test) + `["'\x60]`)
	}
	if !declaration.Match(contents) {
		return fmt.Errorf("evidence target does not declare %q", target.Test)
	}
	return nil
}

func pipelineCorpusTargetsEqual(
	fixture []pipelineCorpusTestTarget,
	registered []pipelineEvidenceTarget,
) bool {
	if len(fixture) != len(registered) {
		return false
	}
	for index := range fixture {
		if fixture[index].Path != registered[index].Path || fixture[index].Test != registered[index].Identity {
			return false
		}
	}
	return true
}

func validatePipelineCorpusAssertionTargets(
	repositoryRoot string,
	targets []pipelineCorpusTestTarget,
	assertions []string,
) error {
	contents := make([][]byte, len(targets))
	for index, target := range targets {
		var err error
		contents[index], err = pipelineCorpusEvidenceDeclaration(repositoryRoot, target)
		if err != nil {
			return err
		}
	}
	for _, assertion := range assertions {
		found := false
		for _, source := range contents {
			if bytes.Contains(source, []byte(assertion)) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("assertion token %q is absent from every exact evidence target", assertion)
		}
	}
	return nil
}

func pipelineCorpusEvidenceDeclaration(
	repositoryRoot string,
	target pipelineCorpusTestTarget,
) ([]byte, error) {
	path := filepath.Join(repositoryRoot, target.Path)
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(target.Path, "_test.go") {
		return contents, nil
	}
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, path, contents, 0)
	if err != nil {
		return nil, err
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != target.Test {
			continue
		}
		start := files.Position(function.Pos()).Offset
		end := files.Position(function.End()).Offset
		if start < 0 || end < start || end > len(contents) {
			return nil, errors.New("evidence function offsets are invalid")
		}
		return contents[start:end], nil
	}
	return nil, fmt.Errorf("evidence target does not declare %q", target.Test)
}

func pipelineCorpusJSONMapsEqual(left, right map[string]json.RawMessage) bool {
	leftEncoded, leftErr := json.Marshal(left)
	rightEncoded, rightErr := json.Marshal(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	var leftValue, rightValue any
	if decodePipelineCorpusJSON(leftEncoded, &leftValue) != nil || decodePipelineCorpusJSON(rightEncoded, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func validatePipelineCorpusSourceProgram(source string, encoded []json.RawMessage) error {
	query, err := spl.Parse(source)
	if err != nil {
		return fmt.Errorf("parse fixture-bound source: %w", err)
	}
	commands := make([]spl.Command, 0, len(encoded))
	for _, command := range query.Commands {
		if isPipelineCorpusCommandName(command.Name()) {
			commands = append(commands, command)
		}
	}
	if len(commands) != len(encoded) {
		return fmt.Errorf("source pipeline command count = %d, modeled program count = %d", len(commands), len(encoded))
	}
	for operationIndex, raw := range encoded {
		operation, err := decodePipelineCorpusOperation(raw)
		if err != nil {
			return err
		}
		command := commands[operationIndex]
		if command.Name() != operation.Op {
			return fmt.Errorf(
				"operation %d is %s in source, want modeled %s",
				operationIndex, command.Name(), operation.Op,
			)
		}
		if err := matchPipelineCorpusOperation(command, operation); err != nil {
			return fmt.Errorf("operation %d %s differs from source: %w", operationIndex, operation.Op, err)
		}
	}
	return nil
}

func isPipelineCorpusCommandName(name string) bool {
	switch name {
	case "regex", "reverse", "accum", "strcat", "addinfo", "fillnull", "addtotals", "delta", "makemv", "mvexpand":
		return true
	default:
		return false
	}
}

func matchPipelineCorpusOperation(command spl.Command, operation pipelineCorpusOperation) error {
	mismatch := func() error { return errors.New("arguments do not exactly match") }
	switch command := command.(type) {
	case *spl.RegexCommand:
		if command.Field != operation.Field || command.Pattern != operation.Pattern || command.Negated != operation.Negated {
			return mismatch()
		}
	case *spl.ReverseCommand, *spl.AddInfoCommand:
	case *spl.AccumCommand:
		if command.Field != operation.Input || command.Output != operation.Output {
			return mismatch()
		}
	case *spl.StrcatCommand:
		if command.Destination != operation.Output || command.AllRequired != operation.AllRequired ||
			len(command.Operands) != len(operation.Parts) {
			return mismatch()
		}
		for index, part := range operation.Parts {
			operand := command.Operands[index]
			if part.Field != operand.Field || (part.Literal == "") != (operand.Literal == nil) ||
				(operand.Literal != nil && part.Literal != *operand.Literal) {
				return mismatch()
			}
		}
	case *spl.FillNullCommand:
		fields := make([]string, len(command.Fields))
		for index := range command.Fields {
			fields[index] = command.Fields[index].Name
		}
		if command.Value != operation.Value || !slices.Equal(fields, operation.Fields) {
			return mismatch()
		}
	case *spl.AddTotalsCommand:
		fields := make([]string, len(command.Fields))
		for index := range command.Fields {
			fields[index] = command.Fields[index].Name
		}
		if command.Output != operation.Output || !slices.Equal(fields, operation.Fields) {
			return mismatch()
		}
	case *spl.DeltaCommand:
		if command.Field != operation.Input || command.Output != operation.Output || command.Previous != uint64(operation.Period) {
			return mismatch()
		}
	case *spl.MakeMVCommand:
		if command.Field != operation.Field || command.Delimiter != operation.Delimiter || command.AllowEmpty != operation.AllowEmpty {
			return mismatch()
		}
	case *spl.MVExpandCommand:
		if command.Field != operation.Field || command.Limit != uint64(operation.Limit) {
			return mismatch()
		}
	default:
		return fmt.Errorf("source command has unexpected concrete type %T", command)
	}
	return nil
}

func runPipelineReferenceCorpusFixture(t *testing.T, fixture pipelineCorpusFixture) {
	t.Helper()
	actualSchema, err := pipelineCorpusReferenceSchema(fixture.Input.Schema, fixture.Program)
	if err != nil {
		t.Fatalf("derive exact reference schema: %v", err)
	}
	if !reflect.DeepEqual(actualSchema, fixture.Expected.Schema) {
		t.Fatalf("ordered schema = %#v, want %#v", actualSchema, fixture.Expected.Schema)
	}
	rows := make([]pipelineReferenceRow, len(fixture.Input.Rows))
	for rowIndex, encodedRow := range fixture.Input.Rows {
		row := make(pipelineReferenceRow, len(fixture.Input.Schema))
		for columnIndex, cell := range encodedRow {
			value, err := pipelineCorpusReferenceValue(cell, 0)
			if err != nil {
				t.Fatalf("input row %d cell %d: %v", rowIndex, columnIndex, err)
			}
			if value.kind != pipelineReferenceMissing {
				row[fixture.Input.Schema[columnIndex].Name] = value
			}
		}
		rows[rowIndex] = row
	}
	var ledger pipelineReferenceMVExpandLedger
	for index, raw := range fixture.Program {
		operation, err := decodePipelineCorpusOperation(raw)
		if err != nil {
			t.Fatalf("operation %d: %v", index, err)
		}
		switch operation.Op {
		case "regex":
			rows, err = pipelineReferenceRegex(rows, operation.Field, operation.Pattern, operation.Negated)
		case "reverse":
			rows = pipelineReferenceReverse(rows)
		case "accum":
			pipelineReferenceAccum(rows, operation.Input, operation.Output)
		case "strcat":
			parts := make([]pipelineReferenceConcatPart, len(operation.Parts))
			for partIndex, part := range operation.Parts {
				parts[partIndex] = pipelineReferenceConcatPart{field: part.Field, literal: part.Literal}
			}
			pipelineReferenceStrcat(rows, parts, operation.Output, operation.AllRequired)
		case "addinfo":
			pipelineReferenceAddInfo(rows, pipelineReferenceInfo{
				minimum: operation.Minimum, maximum: operation.Maximum,
				started: operation.Started, sid: operation.SID,
			})
		case "fillnull":
			pipelineReferenceFillNull(rows, operation.Fields, operation.Value)
		case "addtotals":
			pipelineReferenceAddTotals(rows, operation.Fields, operation.Output)
		case "delta":
			pipelineReferenceDelta(rows, operation.Input, operation.Output, operation.Period)
		case "makemv":
			err = pipelineReferenceMakeMV(rows, operation.Field, operation.Delimiter, operation.AllowEmpty)
		case "mvexpand":
			rows, err = pipelineReferenceMVExpandWithLedger(rows, operation.Field, operation.Limit, &ledger)
		default:
			t.Fatalf("unhandled operation %q", operation.Op)
		}
		if err != nil {
			t.Fatalf("operation %d %s: %v", index, operation.Op, err)
		}
	}
	if len(rows) != len(fixture.Expected.Rows) {
		t.Fatalf("row count = %d, want %d", len(rows), len(fixture.Expected.Rows))
	}
	knownFields := make(map[string]struct{}, len(actualSchema))
	for _, column := range actualSchema {
		knownFields[column.Name] = struct{}{}
	}
	for rowIndex, row := range rows {
		for field := range row {
			if _, known := knownFields[field]; !known {
				t.Fatalf("row %d contains extra field %q outside exact schema", rowIndex, field)
			}
		}
		for columnIndex, column := range fixture.Expected.Schema {
			actual := pipelineRefGet(row, column.Name)
			want, err := pipelineCorpusReferenceValue(fixture.Expected.Rows[rowIndex][columnIndex], 0)
			if err != nil {
				t.Fatalf("expected row %d cell %d: %v", rowIndex, columnIndex, err)
			}
			if !reflect.DeepEqual(actual, want) {
				t.Fatalf("row %d column %q = %#v, want %#v", rowIndex, column.Name, actual, want)
			}
		}
	}
}

func pipelineCorpusReferenceSchema(
	input []pipelineCorpusColumn,
	program []json.RawMessage,
) ([]pipelineCorpusColumn, error) {
	schema := slices.Clone(input)
	upsert := func(column pipelineCorpusColumn) {
		for index := range schema {
			if schema[index].Name == column.Name {
				schema[index] = column
				return
			}
		}
		schema = append(schema, column)
	}
	lookup := func(name string) (pipelineCorpusColumn, bool) {
		for _, column := range schema {
			if column.Name == name {
				return column, true
			}
		}
		return pipelineCorpusColumn{}, false
	}
	for _, raw := range program {
		operation, err := decodePipelineCorpusOperation(raw)
		if err != nil {
			return nil, err
		}
		switch operation.Op {
		case "regex", "reverse":
		case "accum":
			upsert(pipelineCorpusColumn{Name: operation.Output, Kind: "number", Nullable: true})
		case "strcat":
			upsert(pipelineCorpusColumn{Name: operation.Output, Kind: "string", Nullable: operation.AllRequired})
		case "addinfo":
			for _, column := range []pipelineCorpusColumn{
				{Name: "info_min_time", Kind: "number"},
				{Name: "info_max_time", Kind: "number"},
				{Name: "info_search_time", Kind: "number"},
				{Name: "info_sid", Kind: "string"},
			} {
				upsert(column)
			}
		case "fillnull":
			for _, field := range operation.Fields {
				upsert(pipelineCorpusColumn{Name: field, Kind: "string"})
			}
		case "addtotals":
			upsert(pipelineCorpusColumn{Name: operation.Output, Kind: "number", Nullable: true})
		case "delta":
			upsert(pipelineCorpusColumn{Name: operation.Output, Kind: "number", Nullable: true})
		case "makemv":
			upsert(pipelineCorpusColumn{Name: operation.Field, Kind: "list", Nullable: true, Multivalue: true})
		case "mvexpand":
			column, ok := lookup(operation.Field)
			if !ok {
				column = pipelineCorpusColumn{Name: operation.Field, Kind: "dynamic", Nullable: true}
			}
			if column.Kind == "list" {
				column.Kind = "string"
				column.Nullable = true
				column.Multivalue = false
			}
			upsert(column)
		default:
			return nil, fmt.Errorf("unsupported schema operation %q", operation.Op)
		}
	}
	return schema, nil
}

func pipelineCorpusReferenceValue(cell pipelineCorpusCell, depth int) (pipelineReferenceValue, error) {
	if _, err := validatePipelineCorpusCell(cell, depth); err != nil {
		return pipelineReferenceValue{}, err
	}
	for tag, raw := range cell {
		switch tag {
		case "missing":
			return pipelineRefMissing(), nil
		case "null":
			return pipelineRefNull(), nil
		case "string":
			var value string
			_ = json.Unmarshal(raw, &value)
			return pipelineRefString(value), nil
		case "number":
			value, err := pipelineCorpusFloat(raw)
			return pipelineRefNumber(value), err
		case "bool":
			var value bool
			_ = json.Unmarshal(raw, &value)
			return pipelineReferenceValue{kind: pipelineReferenceBool, boolean: value}, nil
		case "time":
			value, err := pipelineCorpusFloat(raw)
			return pipelineReferenceValue{kind: pipelineReferenceTime, number: value}, err
		case "list":
			var encoded []pipelineCorpusCell
			if err := decodePipelineCorpusJSON(raw, &encoded); err != nil {
				return pipelineReferenceValue{}, err
			}
			members := make([]pipelineReferenceValue, len(encoded))
			for index, member := range encoded {
				value, err := pipelineCorpusReferenceValue(member, depth+1)
				if err != nil {
					return pipelineReferenceValue{}, err
				}
				members[index] = value
			}
			return pipelineReferenceValue{kind: pipelineReferenceList, members: members}, nil
		case "bytes_hex":
			return pipelineReferenceValue{}, errors.New("semantic Bytes are evidence-only in the independent reference model")
		}
	}
	panic("unreachable")
}

func pipelineCorpusFloat(raw json.RawMessage) (float64, error) {
	var number json.Number
	if err := decodePipelineCorpusJSON(raw, &number); err != nil {
		return 0, err
	}
	return number.Float64()
}

func decodePipelineCorpusJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON value has trailing data")
		}
		return err
	}
	return nil
}

func validPipelineCorpusFixtureID(id string) bool {
	if id == "" || len(id) > 96 || strings.TrimSpace(id) != id {
		return false
	}
	for part := range strings.SplitSeq(id, ".") {
		if part == "" {
			return false
		}
		for _, character := range part {
			if character != '-' && (character < 'a' || character > 'z') {
				return false
			}
		}
	}
	return true
}

func pipelineExecutableCorpusRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate executable corpus test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func clonePipelineExecutableCorpus(corpus pipelineExecutableCorpus) pipelineExecutableCorpus {
	clone := corpus
	clone.Fixtures = make(map[string]json.RawMessage, len(corpus.Fixtures))
	for id, raw := range corpus.Fixtures {
		clone.Fixtures[id] = slices.Clone(raw)
	}
	clone.Rules = make([]pipelineExecutableCorpusRule, len(corpus.Rules))
	for ruleIndex, rule := range corpus.Rules {
		clone.Rules[ruleIndex] = rule
		clone.Rules[ruleIndex].Cases = slices.Clone(rule.Cases)
	}
	return clone
}
