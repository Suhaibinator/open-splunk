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
	v03CorpusReferenceRunner  = "reference"
	v03CorpusEvidenceRunner   = "evidence"
	v03CorpusMaximumCellDepth = 8
)

type v03ExecutableCorpus struct {
	FormatVersion        string                     `json:"format_version"`
	CompatibilityVersion string                     `json:"compatibility_version"`
	Fixtures             map[string]json.RawMessage `json:"fixtures"`
	Rules                []v03ExecutableCorpusRule  `json:"rules"`
}

type v03ExecutableCorpusRule struct {
	ID      string                    `json:"id"`
	Summary string                    `json:"summary"`
	Cases   []v03ExecutableCorpusCase `json:"cases"`
}

type v03ExecutableCorpusCase struct {
	Name     string                     `json:"name"`
	Source   string                     `json:"source,omitempty"`
	Evidence string                     `json:"evidence,omitempty"`
	Fixture  string                     `json:"fixture,omitempty"`
	Expect   map[string]json.RawMessage `json:"expect"`
}

type v03CorpusFixture struct {
	Runner     string                     `json:"runner"`
	Evidence   string                     `json:"evidence,omitempty"`
	Operation  string                     `json:"operation,omitempty"`
	Input      *v03CorpusTable            `json:"input,omitempty"`
	Program    []json.RawMessage          `json:"program,omitempty"`
	Expected   *v03CorpusTable            `json:"expected,omitempty"`
	Targets    []v03CorpusTestTarget      `json:"targets,omitempty"`
	Assertions []string                   `json:"assertions,omitempty"`
	Claim      map[string]json.RawMessage `json:"claim,omitempty"`
	Failure    *v03CorpusFailure          `json:"failure,omitempty"`
	Trials     []v03CorpusEvidenceTrial   `json:"trials,omitempty"`
}

type v03CorpusEvidenceTrial struct {
	Name       string           `json:"name"`
	Input      v03CorpusTable   `json:"input"`
	Failure    v03CorpusFailure `json:"failure"`
	Assertions []string         `json:"assertions"`
}

type v03CorpusTable struct {
	Schema []v03CorpusColumn `json:"schema"`
	Rows   [][]v03CorpusCell `json:"rows"`
}

type v03CorpusColumn struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Nullable   bool   `json:"nullable"`
	Multivalue bool   `json:"multivalue"`
}

type v03CorpusCell map[string]json.RawMessage

type v03CorpusTestTarget struct {
	Path string `json:"path"`
	Test string `json:"test"`
}

type v03CorpusFailure struct {
	Phase       string `json:"phase"`
	Code        string `json:"code"`
	Marker      string `json:"marker,omitempty"`
	Atomic      bool   `json:"atomic"`
	RowCount    uint64 `json:"row_count"`
	ResultBytes uint64 `json:"result_bytes"`
}

type v03CorpusOperation struct {
	Op          string                `json:"op"`
	Field       string                `json:"field,omitempty"`
	Input       string                `json:"input,omitempty"`
	Output      string                `json:"output,omitempty"`
	Pattern     string                `json:"pattern,omitempty"`
	Negated     bool                  `json:"negated,omitempty"`
	Parts       []v03CorpusConcatPart `json:"parts,omitempty"`
	AllRequired bool                  `json:"all_required,omitempty"`
	Fields      []string              `json:"fields,omitempty"`
	Value       string                `json:"value,omitempty"`
	Minimum     float64               `json:"minimum,omitempty"`
	Maximum     float64               `json:"maximum,omitempty"`
	Started     float64               `json:"started,omitempty"`
	SID         string                `json:"sid,omitempty"`
	Period      int                   `json:"period,omitempty"`
	Delimiter   string                `json:"delimiter,omitempty"`
	AllowEmpty  bool                  `json:"allow_empty,omitempty"`
	Limit       int                   `json:"limit,omitempty"`
}

type v03CorpusConcatPart struct {
	Field   string `json:"field,omitempty"`
	Literal string `json:"literal,omitempty"`
}

func TestV03CorpusFixturesAreStrictBoundAndExecutable(t *testing.T) {
	t.Parallel()

	corpus := loadV03ExecutableCorpus(t)
	fixtures, err := validateV03ExecutableCorpus(corpus, v03ExecutableCorpusRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(fixtures) != 39 {
		t.Fatalf("fixture count = %d, want 39", len(fixtures))
	}
	caseCount := 0
	for _, rule := range corpus.Rules {
		caseCount += len(rule.Cases)
	}
	if caseCount != 53 {
		t.Fatalf("case count = %d, want 53", caseCount)
	}
	var referenceCount, evidenceCount int
	for id, fixture := range fixtures {
		switch fixture.Runner {
		case v03CorpusReferenceRunner:
			referenceCount++
			t.Run(id, func(t *testing.T) {
				runV03ReferenceCorpusFixture(t, fixture)
			})
		case v03CorpusEvidenceRunner:
			evidenceCount++
		default:
			t.Fatalf("fixture %q has unvalidated runner %q", id, fixture.Runner)
		}
	}
	if referenceCount != 22 || evidenceCount != 17 {
		t.Fatalf("runner counts = reference %d evidence %d, want 22/17", referenceCount, evidenceCount)
	}
}

func TestV03CorpusFixtureSchemaRejectsHostileMutations(t *testing.T) {
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
			if _, err := decodeV03CorpusFixture("hostile", json.RawMessage(test.encoded)); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("decode error = %v, want substring %q", err, test.want)
			}
		})
	}

	evidenceBoth := `{
		"runner":"evidence",
		"targets":[{"path":"internal/queryexec/v03_runtime_marker_adversarial_test.go","test":"TestV03RuntimeMarkersAreCompletelyClassifiedAndRedacted"}],
		"expected":{"schema":[{"name":"value","kind":"string","nullable":false,"multivalue":false}],"rows":[]},
		"failure":{"phase":"executor","code":"invalid-result","atomic":true,"row_count":0,"result_bytes":0}
	}`
	if _, err := decodeV03CorpusFixture("ambiguous", json.RawMessage(evidenceBoth)); err == nil ||
		!strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("ambiguous evidence error = %v", err)
	}
}

func TestV03CorpusFixtureBindingsRejectDanglingUnusedAndMissingCases(t *testing.T) {
	t.Parallel()

	corpus := loadV03ExecutableCorpus(t)
	root := v03ExecutableCorpusRoot(t)

	dangling := cloneV03ExecutableCorpus(corpus)
	dangling.Rules[3].Cases[0].Fixture = "missing.fixture"
	if _, err := validateV03ExecutableCorpus(dangling, root); err == nil ||
		!strings.Contains(err.Error(), "unknown fixture") {
		t.Fatalf("dangling fixture error = %v", err)
	}

	unused := cloneV03ExecutableCorpus(corpus)
	unused.Fixtures["unused.fixture"] = slices.Clone(unused.Fixtures["reference.reverse"])
	if _, err := validateV03ExecutableCorpus(unused, root); err == nil ||
		!strings.Contains(err.Error(), "unused") {
		t.Fatalf("unused fixture error = %v", err)
	}

	missing := cloneV03ExecutableCorpus(corpus)
	missing.Rules[3].Cases[0].Fixture = ""
	if _, err := validateV03ExecutableCorpus(missing, root); err == nil ||
		!strings.Contains(err.Error(), "requires an executable fixture") {
		t.Fatalf("missing semantic fixture error = %v", err)
	}

	duplicated := cloneV03ExecutableCorpus(corpus)
	duplicated.Rules[3].Cases[1].Fixture = duplicated.Rules[3].Cases[0].Fixture
	if _, err := validateV03ExecutableCorpus(duplicated, root); err == nil ||
		!strings.Contains(err.Error(), "reuses fixture") {
		t.Fatalf("reused fixture error = %v", err)
	}
}

func TestV03CorpusSourceProgramRejectsUnmodeledV03Commands(t *testing.T) {
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
			name:    "non-v0.3 setup and projection remain allowed",
			source:  `index=main | sort 0 +event_id | reverse | table event_id`,
			program: []json.RawMessage{reverse},
		},
		{
			name:    "extra v0.3 command before modeled program",
			source:  `index=main | addinfo | reverse`,
			program: []json.RawMessage{reverse},
			wantErr: true,
		},
		{
			name:    "extra v0.3 command between modeled operations",
			source:  `index=main | reverse | regex "x" | addinfo`,
			program: []json.RawMessage{reverse, addInfo},
			wantErr: true,
		},
		{
			name:    "extra v0.3 command after modeled program",
			source:  `index=main | reverse | addinfo`,
			program: []json.RawMessage{reverse},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateV03CorpusSourceProgram(test.source, test.program)
			if test.wantErr && (err == nil || !strings.Contains(err.Error(), "v0.3 command count")) {
				t.Fatalf("validation error = %v, want exact v0.3 command-count rejection", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

func TestV03CorpusOracleRejectsSchemaSourceEvidenceAndFailureDrift(t *testing.T) {
	t.Parallel()

	valid := loadV03ExecutableCorpus(t)
	root := v03ExecutableCorpusRoot(t)
	for _, test := range []struct {
		name   string
		mutate func(*v03ExecutableCorpus)
		want   string
	}{
		{
			name: "derived ordered schema",
			mutate: func(corpus *v03ExecutableCorpus) {
				raw := string(corpus.Fixtures["reference.reverse"])
				last := strings.LastIndex(raw, `"nullable":false`)
				corpus.Fixtures["reference.reverse"] = json.RawMessage(raw[:last] + `"nullable":true` + raw[last+len(`"nullable":false`):])
			},
			want: "expected schema does not exactly match",
		},
		{
			name: "source arguments",
			mutate: func(corpus *v03ExecutableCorpus) {
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
			mutate: func(corpus *v03ExecutableCorpus) {
				raw := string(corpus.Fixtures["evidence.optional-multivalue"])
				raw = strings.Replace(raw,
					"TestV03OptionalMultivalueTransportDistinguishesNullEmptyAndMembers",
					"TestV03RealMakeMVCompilerTransportPublishesNullEmptyAndOrderedMembers", 1)
				corpus.Fixtures["evidence.optional-multivalue"] = json.RawMessage(raw)
			},
			want: "do not exactly match evidence registry",
		},
		{
			name: "assertion token",
			mutate: func(corpus *v03ExecutableCorpus) {
				raw := string(corpus.Fixtures["evidence.sink-ceiling"])
				raw = strings.Replace(raw, `"FailureResourceLimit"`, `"ABSENT_ASSERTION_TOKEN"`, 1)
				corpus.Fixtures["evidence.sink-ceiling"] = json.RawMessage(raw)
			},
			want: "absent from every exact evidence target",
		},
		{
			name: "claim expectation",
			mutate: func(corpus *v03ExecutableCorpus) {
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
			mutate: func(corpus *v03ExecutableCorpus) {
				raw := string(corpus.Fixtures["evidence.query-wide-expansion"])
				raw = strings.Replace(raw, "mvexpand query rows exceed the limit", "unknown marker", 1)
				corpus.Fixtures["evidence.query-wide-expansion"] = json.RawMessage(raw)
			},
			want: "unknown marker",
		},
		{
			name: "operation invariant",
			mutate: func(corpus *v03ExecutableCorpus) {
				raw := string(corpus.Fixtures["reference.accum-output"])
				raw = strings.Replace(raw, `"output":"running_bytes"`, `"output":""`, 1)
				corpus.Fixtures["reference.accum-output"] = json.RawMessage(raw)
			},
			want: "accum input and output must be nonempty",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			corpus := cloneV03ExecutableCorpus(valid)
			test.mutate(&corpus)
			if _, err := validateV03ExecutableCorpus(corpus, root); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func loadV03ExecutableCorpus(t *testing.T) v03ExecutableCorpus {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join(
		v03ExecutableCorpusRoot(t), "internal/spl/testdata/compatibility-v0.3.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := rejectV03DuplicateJSONNames(encoded); err != nil {
		t.Fatalf("duplicate corpus JSON name: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var corpus v03ExecutableCorpus
	if err := decoder.Decode(&corpus); err != nil {
		t.Fatalf("decode executable v0.3 corpus: %v", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		t.Fatalf("decode trailing executable corpus data: %v", err)
	}
	return corpus
}

func validateV03ExecutableCorpus(
	corpus v03ExecutableCorpus,
	repositoryRoot string,
) (map[string]v03CorpusFixture, error) {
	if corpus.FormatVersion != compatibilityV03Format ||
		corpus.CompatibilityVersion != compatibilityV03Version {
		return nil, errors.New("executable corpus identity is invalid")
	}
	if len(corpus.Fixtures) == 0 {
		return nil, errors.New("executable corpus has no fixtures")
	}
	fixtures := make(map[string]v03CorpusFixture, len(corpus.Fixtures))
	for id, raw := range corpus.Fixtures {
		if !validV03CorpusFixtureID(id) {
			return nil, fmt.Errorf("fixture id %q is invalid", id)
		}
		fixture, err := decodeV03CorpusFixture(id, raw)
		if err != nil {
			return nil, err
		}
		if fixture.Runner == v03CorpusReferenceRunner {
			actualSchema, err := v03CorpusReferenceSchema(fixture.Input.Schema, fixture.Program)
			if err != nil {
				return nil, fmt.Errorf("fixture %q: %w", id, err)
			}
			if !reflect.DeepEqual(actualSchema, fixture.Expected.Schema) {
				return nil, fmt.Errorf("fixture %q expected schema does not exactly match derived ordered schema", id)
			}
		}
		if fixture.Runner == v03CorpusEvidenceRunner {
			registered, ok := compatibilityV03EvidenceRegistry[fixture.Evidence]
			if !ok || !v03CorpusTargetsEqual(fixture.Targets, registered) {
				return nil, fmt.Errorf("fixture %q targets do not exactly match evidence registry %q", id, fixture.Evidence)
			}
			for _, target := range fixture.Targets {
				if err := validateV03CorpusTestTarget(repositoryRoot, target); err != nil {
					return nil, fmt.Errorf("fixture %q: %w", id, err)
				}
			}
			assertions := slices.Clone(fixture.Assertions)
			for _, trial := range fixture.Trials {
				assertions = append(assertions, trial.Assertions...)
			}
			if err := validateV03CorpusAssertionTargets(repositoryRoot, fixture.Targets, assertions); err != nil {
				return nil, fmt.Errorf("fixture %q: %w", id, err)
			}
		}
		fixtures[id] = fixture
	}

	used := make(map[string]string, len(fixtures))
	for _, rule := range corpus.Rules {
		for _, testCase := range rule.Cases {
			caseID := rule.ID + "/" + testCase.Name
			if v03CorpusCaseRequiresFixture(testCase) && testCase.Fixture == "" {
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
			if fixture.Runner == v03CorpusEvidenceRunner {
				if testCase.Evidence == "" || testCase.Evidence != fixture.Evidence {
					return nil, fmt.Errorf("case %s does not bind fixture evidence %q exactly", caseID, fixture.Evidence)
				}
				if len(fixture.Claim) != 0 && !v03CorpusJSONMapsEqual(fixture.Claim, testCase.Expect) {
					return nil, fmt.Errorf("case %s expectation does not exactly match fixture claim", caseID)
				}
				if err := validateV03CorpusFailureExpectation(testCase.Expect, fixture); err != nil {
					return nil, fmt.Errorf("case %s: %w", caseID, err)
				}
			}
			if testCase.Source != "" && fixture.Runner == v03CorpusReferenceRunner {
				if err := validateV03CorpusSourceProgram(testCase.Source, fixture.Program); err != nil {
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

func validateV03CorpusFailureExpectation(
	expect map[string]json.RawMessage,
	fixture v03CorpusFixture,
) error {
	failures := make([]v03CorpusFailure, 0, 1+len(fixture.Trials))
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
	if decodeV03CorpusJSON(phaseRaw, &phase) != nil || decodeV03CorpusJSON(atomicRaw, &atomic) != nil || !atomic {
		return errors.New("failure evidence has invalid phase or atomic expectation")
	}
	for _, failure := range failures {
		if failure.Phase != phase {
			return fmt.Errorf("failure phase %q differs from expectation %q", failure.Phase, phase)
		}
		if diagnosticRaw, present := expect["diagnostic"]; present {
			var diagnostic string
			if decodeV03CorpusJSON(diagnosticRaw, &diagnostic) != nil || diagnostic != failure.Code {
				return fmt.Errorf("failure code %q differs from diagnostic expectation", failure.Code)
			}
		}
	}
	return nil
}

func v03CorpusCaseRequiresFixture(testCase v03ExecutableCorpusCase) bool {
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

func decodeV03CorpusFixture(id string, raw json.RawMessage) (v03CorpusFixture, error) {
	if err := rejectV03DuplicateJSONNames(raw); err != nil {
		return v03CorpusFixture{}, fmt.Errorf("fixture %q has duplicate JSON name: %w", id, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var fixture v03CorpusFixture
	if err := decoder.Decode(&fixture); err != nil {
		return v03CorpusFixture{}, fmt.Errorf("decode fixture %q: %w", id, err)
	}
	if fixture.Input != nil {
		if err := validateV03CorpusTable(*fixture.Input, "input"); err != nil {
			return v03CorpusFixture{}, fmt.Errorf("fixture %q: %w", id, err)
		}
	}
	if fixture.Expected != nil {
		if err := validateV03CorpusTable(*fixture.Expected, "expected"); err != nil {
			return v03CorpusFixture{}, fmt.Errorf("fixture %q: %w", id, err)
		}
	}
	switch fixture.Runner {
	case v03CorpusReferenceRunner:
		if fixture.Input == nil || fixture.Expected == nil || len(fixture.Program) == 0 ||
			fixture.Evidence != "" || fixture.Operation != "" || len(fixture.Targets) != 0 || len(fixture.Assertions) != 0 ||
			len(fixture.Claim) != 0 || fixture.Failure != nil || len(fixture.Trials) != 0 {
			return v03CorpusFixture{}, fmt.Errorf("fixture %q reference fixture has an invalid union shape", id)
		}
		for _, operation := range fixture.Program {
			if _, err := decodeV03CorpusOperation(operation); err != nil {
				return v03CorpusFixture{}, fmt.Errorf("fixture %q: %w", id, err)
			}
		}
	case v03CorpusEvidenceRunner:
		selected := 0
		for _, present := range []bool{
			fixture.Expected != nil, fixture.Failure != nil, len(fixture.Claim) != 0, len(fixture.Trials) != 0,
		} {
			if present {
				selected++
			}
		}
		if fixture.Evidence == "" || len(fixture.Program) != 0 || len(fixture.Targets) == 0 || selected != 1 {
			return v03CorpusFixture{}, fmt.Errorf("fixture %q evidence fixture must select exactly one success, failure, claim, or trial matrix", id)
		}
		if fixture.Failure != nil {
			if err := validateV03CorpusFailure(*fixture.Failure); err != nil {
				return v03CorpusFixture{}, fmt.Errorf("fixture %q: %w", id, err)
			}
		}
		if len(fixture.Trials) != 0 {
			if fixture.Input != nil || len(fixture.Assertions) != 0 ||
				!slices.Contains([]string{"makemv", "mvexpand"}, fixture.Operation) {
				return v03CorpusFixture{}, fmt.Errorf("fixture %q trial matrix has fixture-level input or assertions", id)
			}
			seen := make(map[string]struct{}, len(fixture.Trials))
			for index, trial := range fixture.Trials {
				if trial.Name == "" || strings.TrimSpace(trial.Name) != trial.Name {
					return v03CorpusFixture{}, fmt.Errorf("fixture %q trial %d has invalid name", id, index)
				}
				if _, duplicate := seen[trial.Name]; duplicate {
					return v03CorpusFixture{}, fmt.Errorf("fixture %q duplicates trial %q", id, trial.Name)
				}
				seen[trial.Name] = struct{}{}
				if err := validateV03CorpusTable(trial.Input, "trial input"); err != nil {
					return v03CorpusFixture{}, fmt.Errorf("fixture %q trial %q: %w", id, trial.Name, err)
				}
				if err := validateV03CorpusFailure(trial.Failure); err != nil {
					return v03CorpusFixture{}, fmt.Errorf("fixture %q trial %q: %w", id, trial.Name, err)
				}
				if err := validateV03CorpusAssertions(trial.Assertions); err != nil {
					return v03CorpusFixture{}, fmt.Errorf("fixture %q trial %q: %w", id, trial.Name, err)
				}
				if err := validateV03CorpusEvidenceTrialSemantics(fixture.Operation, trial); err != nil {
					return v03CorpusFixture{}, fmt.Errorf("fixture %q trial %q: %w", id, trial.Name, err)
				}
			}
		} else {
			if fixture.Operation != "" {
				return v03CorpusFixture{}, fmt.Errorf("fixture %q non-matrix evidence has an operation", id)
			}
			if err := validateV03CorpusAssertions(fixture.Assertions); err != nil {
				return v03CorpusFixture{}, fmt.Errorf("fixture %q: %w", id, err)
			}
		}
	default:
		return v03CorpusFixture{}, fmt.Errorf("fixture %q has invalid runner %q", id, fixture.Runner)
	}
	return fixture, nil
}

func validateV03CorpusEvidenceTrialSemantics(operation string, trial v03CorpusEvidenceTrial) error {
	if len(trial.Input.Schema) != 1 || len(trial.Input.Rows) != 1 || len(trial.Input.Rows[0]) != 1 {
		return errors.New("hostile evidence trial must contain exactly one typed cell")
	}
	cell := trial.Input.Rows[0][0]
	tag, err := validateV03CorpusCell(cell, 0)
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
			var members []v03CorpusCell
			if err := decodeV03CorpusJSON(cell["list"], &members); err != nil {
				return err
			}
			for _, member := range members {
				memberTag, err := validateV03CorpusCell(member, 1)
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

func validateV03CorpusAssertions(assertions []string) error {
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

func validateV03CorpusTable(table v03CorpusTable, role string) error {
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
			kind, err := validateV03CorpusCell(cell, 0)
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

func validateV03CorpusCell(cell v03CorpusCell, depth int) (string, error) {
	if depth > v03CorpusMaximumCellDepth {
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
			if decodeV03CorpusJSON(raw, &value) != nil {
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
			var members []v03CorpusCell
			if decodeV03CorpusJSON(raw, &members) != nil {
				return "", errors.New("list tag must contain tagged cells")
			}
			for index, member := range members {
				if _, err := validateV03CorpusCell(member, depth+1); err != nil {
					return "", fmt.Errorf("list member %d: %w", index, err)
				}
			}
			return "list", nil
		case "object":
			var members map[string]v03CorpusCell
			if decodeV03CorpusJSON(raw, &members) != nil {
				return "", errors.New("object tag must contain tagged fields")
			}
			for name, member := range members {
				if name == "" || strings.TrimSpace(name) != name {
					return "", errors.New("object tag has an invalid field name")
				}
				if _, err := validateV03CorpusCell(member, depth+1); err != nil {
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

func decodeV03CorpusOperation(raw json.RawMessage) (v03CorpusOperation, error) {
	if err := rejectV03DuplicateJSONNames(raw); err != nil {
		return v03CorpusOperation{}, fmt.Errorf("operation has duplicate JSON name: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := decodeV03CorpusJSON(raw, &fields); err != nil {
		return v03CorpusOperation{}, fmt.Errorf("decode operation fields: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var operation v03CorpusOperation
	if err := decoder.Decode(&operation); err != nil {
		return v03CorpusOperation{}, fmt.Errorf("decode operation: %w", err)
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
		return v03CorpusOperation{}, fmt.Errorf("operation %q does not use its exact field set", operation.Op)
	}
	for _, name := range want {
		if _, present := fields[name]; !present {
			return v03CorpusOperation{}, fmt.Errorf("operation %q does not use its exact field set", operation.Op)
		}
	}
	if operation.Op == "strcat" {
		if len(operation.Parts) < 2 || operation.Output == "" {
			return v03CorpusOperation{}, errors.New("strcat operation has too few parts")
		}
		for _, part := range operation.Parts {
			if (part.Field == "") == (part.Literal == "") {
				return v03CorpusOperation{}, errors.New("strcat part must select exactly one field or nonempty literal")
			}
		}
	}
	switch operation.Op {
	case "regex":
		if operation.Field == "" || operation.Pattern == "" {
			return v03CorpusOperation{}, errors.New("regex field and pattern must be nonempty")
		}
		if _, err := regexp.Compile(operation.Pattern); err != nil {
			return v03CorpusOperation{}, fmt.Errorf("regex pattern is invalid: %w", err)
		}
	case "accum":
		if operation.Input == "" || operation.Output == "" {
			return v03CorpusOperation{}, errors.New("accum input and output must be nonempty")
		}
	case "addinfo":
		if operation.SID == "" || operation.Minimum > operation.Maximum ||
			math.IsNaN(operation.Minimum) || math.IsInf(operation.Minimum, 0) ||
			math.IsNaN(operation.Maximum) || math.IsInf(operation.Maximum, 0) ||
			math.IsNaN(operation.Started) || math.IsInf(operation.Started, 0) {
			return v03CorpusOperation{}, errors.New("addinfo values are invalid")
		}
	case "fillnull":
		if !validV03CorpusFieldList(operation.Fields) {
			return v03CorpusOperation{}, errors.New("fillnull fields are empty or duplicated")
		}
	case "addtotals":
		if !validV03CorpusFieldList(operation.Fields) || operation.Output == "" {
			return v03CorpusOperation{}, errors.New("addtotals fields or output are invalid")
		}
	case "delta":
		if operation.Input == "" || operation.Output == "" || operation.Period < 1 {
			return v03CorpusOperation{}, errors.New("delta input, output, and period must be valid")
		}
	case "makemv":
		if operation.Field == "" || operation.Delimiter == "" || len(operation.Delimiter) > spl.MaximumMakeMVDelimiterBytes {
			return v03CorpusOperation{}, errors.New("makemv field or delimiter is invalid")
		}
	case "mvexpand":
		if operation.Field == "" || operation.Limit < 0 || uint64(operation.Limit) > spl.MaximumMVExpandLimit {
			return v03CorpusOperation{}, errors.New("mvexpand field or limit is invalid")
		}
	}
	return operation, nil
}

func validV03CorpusFieldList(fields []string) bool {
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

func validateV03CorpusFailure(failure v03CorpusFailure) error {
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

func validateV03CorpusTestTarget(repositoryRoot string, target v03CorpusTestTarget) error {
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

func v03CorpusTargetsEqual(
	fixture []v03CorpusTestTarget,
	registered []compatibilityV03EvidenceTarget,
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

func validateV03CorpusAssertionTargets(
	repositoryRoot string,
	targets []v03CorpusTestTarget,
	assertions []string,
) error {
	contents := make([][]byte, len(targets))
	for index, target := range targets {
		var err error
		contents[index], err = v03CorpusEvidenceDeclaration(repositoryRoot, target)
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

func v03CorpusEvidenceDeclaration(
	repositoryRoot string,
	target v03CorpusTestTarget,
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

func v03CorpusJSONMapsEqual(left, right map[string]json.RawMessage) bool {
	leftEncoded, leftErr := json.Marshal(left)
	rightEncoded, rightErr := json.Marshal(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	var leftValue, rightValue any
	if decodeV03CorpusJSON(leftEncoded, &leftValue) != nil || decodeV03CorpusJSON(rightEncoded, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func validateV03CorpusSourceProgram(source string, encoded []json.RawMessage) error {
	query, err := spl.Parse(source)
	if err != nil {
		return fmt.Errorf("parse fixture-bound source: %w", err)
	}
	commands := make([]spl.Command, 0, len(encoded))
	for _, command := range query.Commands {
		if isV03CorpusCommandName(command.Name()) {
			commands = append(commands, command)
		}
	}
	if len(commands) != len(encoded) {
		return fmt.Errorf("source v0.3 command count = %d, modeled program count = %d", len(commands), len(encoded))
	}
	for operationIndex, raw := range encoded {
		operation, err := decodeV03CorpusOperation(raw)
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
		if err := matchV03CorpusOperation(command, operation); err != nil {
			return fmt.Errorf("operation %d %s differs from source: %w", operationIndex, operation.Op, err)
		}
	}
	return nil
}

func isV03CorpusCommandName(name string) bool {
	switch name {
	case "regex", "reverse", "accum", "strcat", "addinfo", "fillnull", "addtotals", "delta", "makemv", "mvexpand":
		return true
	default:
		return false
	}
}

func matchV03CorpusOperation(command spl.Command, operation v03CorpusOperation) error {
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

func runV03ReferenceCorpusFixture(t *testing.T, fixture v03CorpusFixture) {
	t.Helper()
	actualSchema, err := v03CorpusReferenceSchema(fixture.Input.Schema, fixture.Program)
	if err != nil {
		t.Fatalf("derive exact reference schema: %v", err)
	}
	if !reflect.DeepEqual(actualSchema, fixture.Expected.Schema) {
		t.Fatalf("ordered schema = %#v, want %#v", actualSchema, fixture.Expected.Schema)
	}
	rows := make([]v03ReferenceRow, len(fixture.Input.Rows))
	for rowIndex, encodedRow := range fixture.Input.Rows {
		row := make(v03ReferenceRow, len(fixture.Input.Schema))
		for columnIndex, cell := range encodedRow {
			value, err := v03CorpusReferenceValue(cell, 0)
			if err != nil {
				t.Fatalf("input row %d cell %d: %v", rowIndex, columnIndex, err)
			}
			if value.kind != v03ReferenceMissing {
				row[fixture.Input.Schema[columnIndex].Name] = value
			}
		}
		rows[rowIndex] = row
	}
	var ledger v03ReferenceMVExpandLedger
	for index, raw := range fixture.Program {
		operation, err := decodeV03CorpusOperation(raw)
		if err != nil {
			t.Fatalf("operation %d: %v", index, err)
		}
		switch operation.Op {
		case "regex":
			rows, err = v03ReferenceRegex(rows, operation.Field, operation.Pattern, operation.Negated)
		case "reverse":
			rows = v03ReferenceReverse(rows)
		case "accum":
			v03ReferenceAccum(rows, operation.Input, operation.Output)
		case "strcat":
			parts := make([]v03ReferenceConcatPart, len(operation.Parts))
			for partIndex, part := range operation.Parts {
				parts[partIndex] = v03ReferenceConcatPart{field: part.Field, literal: part.Literal}
			}
			v03ReferenceStrcat(rows, parts, operation.Output, operation.AllRequired)
		case "addinfo":
			v03ReferenceAddInfo(rows, v03ReferenceInfo{
				minimum: operation.Minimum, maximum: operation.Maximum,
				started: operation.Started, sid: operation.SID,
			})
		case "fillnull":
			v03ReferenceFillNull(rows, operation.Fields, operation.Value)
		case "addtotals":
			v03ReferenceAddTotals(rows, operation.Fields, operation.Output)
		case "delta":
			v03ReferenceDelta(rows, operation.Input, operation.Output, operation.Period)
		case "makemv":
			err = v03ReferenceMakeMV(rows, operation.Field, operation.Delimiter, operation.AllowEmpty)
		case "mvexpand":
			rows, err = v03ReferenceMVExpandWithLedger(rows, operation.Field, operation.Limit, &ledger)
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
			actual := v03RefGet(row, column.Name)
			want, err := v03CorpusReferenceValue(fixture.Expected.Rows[rowIndex][columnIndex], 0)
			if err != nil {
				t.Fatalf("expected row %d cell %d: %v", rowIndex, columnIndex, err)
			}
			if !reflect.DeepEqual(actual, want) {
				t.Fatalf("row %d column %q = %#v, want %#v", rowIndex, column.Name, actual, want)
			}
		}
	}
}

func v03CorpusReferenceSchema(
	input []v03CorpusColumn,
	program []json.RawMessage,
) ([]v03CorpusColumn, error) {
	schema := slices.Clone(input)
	upsert := func(column v03CorpusColumn) {
		for index := range schema {
			if schema[index].Name == column.Name {
				schema[index] = column
				return
			}
		}
		schema = append(schema, column)
	}
	lookup := func(name string) (v03CorpusColumn, bool) {
		for _, column := range schema {
			if column.Name == name {
				return column, true
			}
		}
		return v03CorpusColumn{}, false
	}
	for _, raw := range program {
		operation, err := decodeV03CorpusOperation(raw)
		if err != nil {
			return nil, err
		}
		switch operation.Op {
		case "regex", "reverse":
		case "accum":
			upsert(v03CorpusColumn{Name: operation.Output, Kind: "number", Nullable: true})
		case "strcat":
			upsert(v03CorpusColumn{Name: operation.Output, Kind: "string", Nullable: operation.AllRequired})
		case "addinfo":
			for _, column := range []v03CorpusColumn{
				{Name: "info_min_time", Kind: "number"},
				{Name: "info_max_time", Kind: "number"},
				{Name: "info_search_time", Kind: "number"},
				{Name: "info_sid", Kind: "string"},
			} {
				upsert(column)
			}
		case "fillnull":
			for _, field := range operation.Fields {
				upsert(v03CorpusColumn{Name: field, Kind: "string"})
			}
		case "addtotals":
			upsert(v03CorpusColumn{Name: operation.Output, Kind: "number", Nullable: true})
		case "delta":
			upsert(v03CorpusColumn{Name: operation.Output, Kind: "number", Nullable: true})
		case "makemv":
			upsert(v03CorpusColumn{Name: operation.Field, Kind: "list", Nullable: true, Multivalue: true})
		case "mvexpand":
			column, ok := lookup(operation.Field)
			if !ok {
				column = v03CorpusColumn{Name: operation.Field, Kind: "dynamic", Nullable: true}
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

func v03CorpusReferenceValue(cell v03CorpusCell, depth int) (v03ReferenceValue, error) {
	if _, err := validateV03CorpusCell(cell, depth); err != nil {
		return v03ReferenceValue{}, err
	}
	for tag, raw := range cell {
		switch tag {
		case "missing":
			return v03RefMissing(), nil
		case "null":
			return v03RefNull(), nil
		case "string":
			var value string
			_ = json.Unmarshal(raw, &value)
			return v03RefString(value), nil
		case "number":
			value, err := v03CorpusFloat(raw)
			return v03RefNumber(value), err
		case "bool":
			var value bool
			_ = json.Unmarshal(raw, &value)
			return v03ReferenceValue{kind: v03ReferenceBool, boolean: value}, nil
		case "time":
			value, err := v03CorpusFloat(raw)
			return v03ReferenceValue{kind: v03ReferenceTime, number: value}, err
		case "list":
			var encoded []v03CorpusCell
			if err := decodeV03CorpusJSON(raw, &encoded); err != nil {
				return v03ReferenceValue{}, err
			}
			members := make([]v03ReferenceValue, len(encoded))
			for index, member := range encoded {
				value, err := v03CorpusReferenceValue(member, depth+1)
				if err != nil {
					return v03ReferenceValue{}, err
				}
				members[index] = value
			}
			return v03ReferenceValue{kind: v03ReferenceList, members: members}, nil
		case "bytes_hex":
			return v03ReferenceValue{}, errors.New("semantic Bytes are evidence-only in the independent reference model")
		}
	}
	panic("unreachable")
}

func v03CorpusFloat(raw json.RawMessage) (float64, error) {
	var number json.Number
	if err := decodeV03CorpusJSON(raw, &number); err != nil {
		return 0, err
	}
	return number.Float64()
}

func decodeV03CorpusJSON(raw []byte, target any) error {
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

func validV03CorpusFixtureID(id string) bool {
	if id == "" || len(id) > 96 || strings.TrimSpace(id) != id {
		return false
	}
	for _, part := range strings.Split(id, ".") {
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

func v03ExecutableCorpusRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate executable corpus test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func cloneV03ExecutableCorpus(corpus v03ExecutableCorpus) v03ExecutableCorpus {
	clone := corpus
	clone.Fixtures = make(map[string]json.RawMessage, len(corpus.Fixtures))
	for id, raw := range corpus.Fixtures {
		clone.Fixtures[id] = slices.Clone(raw)
	}
	clone.Rules = make([]v03ExecutableCorpusRule, len(corpus.Rules))
	for ruleIndex, rule := range corpus.Rules {
		clone.Rules[ruleIndex] = rule
		clone.Rules[ruleIndex].Cases = slices.Clone(rule.Cases)
	}
	return clone
}
