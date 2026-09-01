package spl_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

var pipelineRuleIDPattern = regexp.MustCompile(`^SPL-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)
var pipelineFixtureIDPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`)

type pipelineCorpus struct {
	FormatVersion  uint32                     `json:"format_version"`
	Fixtures       map[string]json.RawMessage `json:"fixtures,omitempty"`
	Rules          []pipelineRule             `json:"rules"`
	StatsInventory json.RawMessage            `json:"stats_inventory"`
}

type pipelineRule struct {
	ID      string         `json:"id"`
	Summary string         `json:"summary"`
	Cases   []pipelineCase `json:"cases"`
}

type pipelineCase struct {
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

type pipelineEvidenceTarget struct {
	Path     string
	Identity string
}

// Corpus evidence claims are deliberately closed over exact, discoverable test
// identities, whether or not the same case also carries a compileable source.
// A prose token cannot make an acceptance claim by itself: adding or renaming
// one requires updating this registry, and every registered target is read
// back from the working tree below.
var pipelineEvidenceRegistry = map[string][]pipelineEvidenceTarget{
	"pipeline-composition-executable-source": {
		{Path: "internal/clickhouse/pipeline_commands_adversarial_compiler_test.go", Identity: "TestPipelineCommandsComposeWithExactRetainedKnowledgeEvidence"},
	},
	"pipeline-command-reference-model": {
		{Path: "internal/spl/pipeline_reference_model_test.go", Identity: "TestPipelineIndependentReferenceModelCoversCommands"},
		{Path: "internal/spl/pipeline_reference_model_test.go", Identity: "TestPipelineIndependentReferenceStrcatUsesSharedConversionAndWritePolicy"},
		{Path: "internal/spl/pipeline_reference_model_test.go", Identity: "TestPipelineIndependentReferenceResourceModel"},
	},
	"pipeline-public-paging": {
		{Path: "internal/export/pipeline_multivalue_vertical_adversarial_external_test.go", Identity: "TestPipelinePinnedClickHousePublishesNullableListsThroughPagingAndExport"},
	},
	"queryexec-optional-multivalue-four-state-transport": {
		{Path: "internal/queryexec/pipeline_optional_multivalue_transport_adversarial_test.go", Identity: "TestPipelineOptionalMultivalueTransportDistinguishesNullEmptyAndMembers"},
	},
	"optional-multivalue-tri-state-3": {
		{Path: "internal/queryexec/pipeline_optional_multivalue_transport_adversarial_test.go", Identity: "TestPipelineOptionalMultivalueTransportRejectsForgedNativeValues"},
		{Path: "internal/queryexec/pipeline_optional_multivalue_transport_adversarial_test.go", Identity: "TestPipelineOptionalMultivalueTransportRejectsLateForgedRowAtomically"},
	},
	"fillnull-and-addtotals-65th-field": {
		{Path: "internal/spl/pipeline_commands_adversarial_parser_test.go", Identity: "TestPipelineSourceDeterminedResourceBoundaries"},
	},
	"two-10000-row-mvexpand-stages": {
		{Path: "internal/clickhouse/pipeline_commands_adversarial_integration_test.go", Identity: "TestPipelineAdversarialAgainstClickHouse"},
	},
	"retained-bytes-before-downstream-head": {
		{Path: "internal/clickhouse/pipeline_commands_adversarial_integration_test.go", Identity: "TestPipelineAdversarialAgainstClickHouse"},
	},
	"fillnull-dynamic-publication-before-mvexpand": {
		{Path: "internal/clickhouse/pipeline_fillnull_mvexpand_composition_regression_test.go", Identity: "TestCompilePipelineFillNullDynamicThenMVExpandMaterializesThePublicField"},
		{Path: "internal/clickhouse/pipeline_fillnull_mvexpand_composition_regression_test.go", Identity: "TestPipelineFillNullDynamicThenMVExpandAgainstClickHouse"},
	},
	"fillnull-private-physical-publication": {
		{Path: "internal/clickhouse/pipeline_fillnull_private_physical_composition_test.go", Identity: "TestCompilePipelineFillNullBindsPrivatePhysicalColumns"},
		{Path: "internal/clickhouse/pipeline_fillnull_private_physical_composition_test.go", Identity: "TestPipelineFillNullAfterPrivatePhysicalProducersAgainstClickHouse"},
	},
	"fillnull-invalid-path-stats-literal-namespace-overlap": {
		{Path: "internal/clickhouse/pipeline_public_namespace_overlap_compiler_test.go", Identity: "TestPipelineFillNullRepublishesInvalidPathStatsLiteralNamespaceOverlap"},
		{Path: "internal/clickhouse/pipeline_commands_adversarial_integration_test.go", Identity: "TestPipelineAdversarialAgainstClickHouse"},
	},
	"atomic-query-valid-first-row-invalid-later-row": {
		{Path: "internal/queryexec/atomic_result_barrier_adversarial_test.go", Identity: "TestAtomicResultBarrierRejectsLateFailureWithoutPublishing"},
		{Path: "internal/queryexec/pipeline_optional_multivalue_transport_adversarial_test.go", Identity: "TestPipelineOptionalMultivalueTransportRejectsLateForgedRowAtomically"},
	},
	"atomic-manager-max-rows-below-produced-rows": {
		{Path: "internal/searchjobs/pipeline_commands_public_paging_adversarial_test.go", Identity: "TestPipelineAtomicResultRowLimitStagesNoPreviewAndPublishesNothing"},
	},
	"server-saved-object-pipeline-launch": {
		{Path: "internal/server/pipeline_commands_saved_history_adversarial_test.go", Identity: "TestPipelineSavedSearchExecutionUsesPersistedCommandDefinition"},
	},
	"server-history-pipeline-rerun": {
		{Path: "internal/server/pipeline_commands_saved_history_adversarial_test.go", Identity: "TestPipelineHistoryRerunReconstructsPersistedCommandDefinition"},
	},
	"1025-byte-makemv-delimiter": {
		{Path: "internal/spl/pipeline_commands_adversarial_parser_test.go", Identity: "TestPipelineSourceDeterminedResourceBoundaries"},
	},
	"clickhouse-code-395-unsupported-mvexpand-marker": {
		{Path: "internal/queryexec/pipeline_runtime_marker_adversarial_test.go", Identity: "TestPipelineRuntimeMarkersAreCompletelyClassifiedAndRedacted"},
		{Path: "internal/clickhouse/pipeline_commands_adversarial_integration_test.go", Identity: "TestPipelineAdversarialAgainstClickHouse"},
	},
	"makemv-rejected-domain-matrix": {
		{Path: "internal/clickhouse/pipeline_commands_adversarial_integration_test.go", Identity: "TestPipelineAdversarialAgainstClickHouse"},
		{Path: "internal/queryexec/pipeline_runtime_marker_adversarial_test.go", Identity: "TestPipelineRuntimeMarkersAreCompletelyClassifiedAndRedacted"},
		{Path: "internal/clickhouse/pipeline_fillnull_container_compiler_test.go", Identity: "TestPipelineFixedRawProvenancePipelinesCompileWithAtomicConsumers"},
	},
}

var pipelineStringExpectationVocabulary = map[string]map[string]struct{}{
	"diagnostic": {
		"SPL_EXPECTED_FIELD": {}, "SPL_RESERVED_FIELD": {},
		"SPL_EXPECTED_REGEX_PATTERN": {}, "SPL_UNSUPPORTED_REVERSE_SYNTAX": {},
		"SPL_UNSUPPORTED_STRCAT_SYNTAX": {}, "SPL_UNSUPPORTED_ADDINFO_SYNTAX": {},
		"SPL_UNSUPPORTED_FILLNULL_SYNTAX": {}, "SPL_UNSUPPORTED_ADDTOTALS_SYNTAX": {},
		"SPL_UNSUPPORTED_DELTA_SYNTAX": {}, "SPL_UNSUPPORTED_MAKEMV_SYNTAX": {},
		"SPL_UNSUPPORTED_DEDUP_SYNTAX": {}, "SPL_QUERY_TOO_COMPLEX": {},
		"SPL_UNSUPPORTED_TOP_SYNTAX": {}, "SPL_UNSUPPORTED_RARE_SYNTAX": {},
		"SPL_DUPLICATE_FIELD": {}, "unsupported-value": {},
	},
	"diagnostic_phase": {
		"executor": {}, "parser": {}, "publication-preflight": {},
	},
	"order": {
		"original-established-order": {}, "pipeline": {}, "member-order": {},
		"durable-private-lineage": {},
	},
	"parse":    {"accept": {}},
	"range":    {"delimiter-token": {}},
	"relation": {"filter": {}},
	"resource": {"15000-cumulative-rows": {}, "pre-downstream-stage-charge": {}},
	"surfaces": {
		"complete": {}, "admitted-snapshot": {}, "private-columns-redacted": {},
		"saved-search": {}, "history": {}, "two-page-public-result": {},
		"no-sql-or-values": {}, "dynamic-composition": {},
	},
	"type": {
		"nullable-float64": {}, "string": {}, "nullable-list-string": {},
		"scalar-preserved": {},
	},
}

func TestPipelineCorpusHasExactExecutableBehavioralEvidence(t *testing.T) {
	t.Parallel()

	root := pipelineRepositoryRoot(t)
	corpusPath := filepath.Join(root, "internal", "spl", "testdata", "compatibility.json")
	corpusBytes, err := os.ReadFile(corpusPath)
	if err != nil {
		t.Fatalf("read compatibility corpus: %v", err)
	}

	corpus, err := loadPipelineCorpus(corpusBytes)
	if err != nil {
		t.Fatalf("load pipeline compatibility corpus: %v", err)
	}
	if _, err := validatePipelineExecutableCorpus(loadPipelineExecutableCorpus(t), root); err != nil {
		t.Fatalf("validate executable pipeline fixtures: %v", err)
	}
	requirePipelineCommandCoverage(t, corpus)
	requirePipelineExecutableSources(t, corpus)
	requirePipelineEvidenceTargets(t, root, corpus)
}

func TestPipelineCorpusLoaderRejectsMalformedSchema(t *testing.T) {
	t.Parallel()

	valid := func(ruleID, caseBody string) string {
		return `{"format_version":1,"stats_inventory":{},"rules":[{"id":"` + ruleID +
			`","summary":"Summary.","cases":[` + caseBody + `]}]}`
	}
	validCase := `{"name":"case","source":"index=main | reverse","expect":{"parse":"accept"}}`
	for _, test := range []struct {
		name, encoded, want string
	}{
		{name: "empty", encoded: ``, want: "decode corpus"},
		{name: "trailing value", encoded: valid("SPL-TEST-001", validCase) + `{}`, want: "one JSON value"},
		{name: "duplicate key", encoded: `{"format_version":1,"format_version":1,"stats_inventory":{},"rules":[]}`, want: `duplicates object key "format_version"`},
		{name: "unknown corpus field", encoded: `{"format_version":1,"stats_inventory":{},"rules":[],"typo":true}`, want: "unknown field"},
		{name: "wrong format", encoded: strings.Replace(valid("SPL-TEST-001", validCase), `"format_version":1`, `"format_version":2`, 1), want: "format_version"},
		{name: "no rules", encoded: `{"format_version":1,"stats_inventory":{},"rules":[]}`, want: "no rules"},
		{name: "bad id", encoded: valid("not-a-rule", validCase), want: "invalid id"},
		{name: "unknown case field", encoded: valid("SPL-TEST-001", `{"name":"case","source":"index=main","expect":{"parse":"accept"},"typo":true}`), want: "unknown field"},
		{name: "empty expectation", encoded: valid("SPL-TEST-001", `{"name":"case","source":"index=main","expect":{}}`), want: "empty expectation"},
		{name: "unknown expectation", encoded: valid("SPL-TEST-001", `{"name":"case","source":"index=main","expect":{"parze":"accept"}}`), want: "unsupported expectation"},
		{name: "atomic vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","evidence":"case","expect":{"atomic":false}}`), want: "expect.atomic"},
		{name: "diagnostic vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","evidence":"case","expect":{"diagnostic":"SPL_UNKNOWN"}}`), want: "expect.diagnostic"},
		{name: "diagnostic phase vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","evidence":"case","expect":{"diagnostic_phase":"runtime"}}`), want: "expect.diagnostic_phase"},
		{name: "order vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","evidence":"case","expect":{"order":"random"}}`), want: "expect.order"},
		{name: "parse vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","source":"index=main","expect":{"parse":"reject"}}`), want: "expect.parse"},
		{name: "range vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","evidence":"case","expect":{"range":"whole-command"}}`), want: "expect.range"},
		{name: "relation vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","evidence":"case","expect":{"relation":"transform"}}`), want: "expect.relation"},
		{name: "resource vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","evidence":"case","expect":{"resource":"unbounded"}}`), want: "expect.resource"},
		{name: "surfaces vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","evidence":"case","expect":{"surfaces":"approximate"}}`), want: "expect.surfaces"},
		{name: "type vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","evidence":"case","expect":{"type":"mixed"}}`), want: "expect.type"},
		{name: "value vocabulary", encoded: valid("SPL-TEST-001", `{"name":"case","evidence":"case","expect":{"value":17}}`), want: "expect.value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := loadPipelineCorpus([]byte(test.encoded))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("load error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func loadPipelineCorpus(encoded []byte) (pipelineCorpus, error) {
	if err := rejectPipelineDuplicateJSONNames(encoded); err != nil {
		return pipelineCorpus{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var corpus pipelineCorpus
	if err := decoder.Decode(&corpus); err != nil {
		return pipelineCorpus{}, fmt.Errorf("decode corpus: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return pipelineCorpus{}, errors.New("corpus must contain exactly one JSON value")
		}
		return pipelineCorpus{}, fmt.Errorf("decode trailing corpus data: %w", err)
	}
	if corpus.FormatVersion != compatibilityCorpusFormat {
		return pipelineCorpus{}, fmt.Errorf("format_version = %d, want %d", corpus.FormatVersion, compatibilityCorpusFormat)
	}
	corpus.Rules = pipelineCorpusRules(corpus.Rules)
	if len(corpus.Rules) == 0 {
		return pipelineCorpus{}, errors.New("corpus has no rules")
	}
	for fixtureID, fixture := range corpus.Fixtures {
		if !pipelineFixtureIDPattern.MatchString(fixtureID) {
			return pipelineCorpus{}, fmt.Errorf("fixtures has invalid id %q", fixtureID)
		}
		trimmed := bytes.TrimSpace(fixture)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return pipelineCorpus{}, fmt.Errorf("fixture %q must be a JSON object", fixtureID)
		}
	}
	usedFixtures := make(map[string]string, len(corpus.Fixtures))
	seenRules := make(map[string]struct{}, len(corpus.Rules))
	for ruleIndex, rule := range corpus.Rules {
		path := fmt.Sprintf("rules[%d]", ruleIndex)
		if !pipelineRuleIDPattern.MatchString(rule.ID) {
			return pipelineCorpus{}, fmt.Errorf("%s has invalid id %q", path, rule.ID)
		}
		if _, duplicate := seenRules[rule.ID]; duplicate {
			return pipelineCorpus{}, fmt.Errorf("%s duplicates rule %q", path, rule.ID)
		}
		seenRules[rule.ID] = struct{}{}
		if strings.TrimSpace(rule.Summary) == "" || strings.TrimSpace(rule.Summary) != rule.Summary {
			return pipelineCorpus{}, fmt.Errorf("%s has invalid summary", path)
		}
		if len(rule.Cases) == 0 {
			return pipelineCorpus{}, fmt.Errorf("%s has no cases", path)
		}
		seenCases := make(map[string]struct{}, len(rule.Cases))
		for caseIndex, testCase := range rule.Cases {
			casePath := fmt.Sprintf("%s.cases[%d]", path, caseIndex)
			if strings.TrimSpace(testCase.Name) == "" || strings.TrimSpace(testCase.Name) != testCase.Name {
				return pipelineCorpus{}, fmt.Errorf("%s has invalid name", casePath)
			}
			if _, duplicate := seenCases[testCase.Name]; duplicate {
				return pipelineCorpus{}, fmt.Errorf("%s duplicates case %q", casePath, testCase.Name)
			}
			seenCases[testCase.Name] = struct{}{}
			if testCase.Source == "" && testCase.Evidence == "" {
				return pipelineCorpus{}, fmt.Errorf("%s has no source or evidence", casePath)
			}
			if len(testCase.Expect) == 0 {
				return pipelineCorpus{}, fmt.Errorf("%s has an empty expectation", casePath)
			}
			if testCase.Fixture != "" {
				if _, ok := corpus.Fixtures[testCase.Fixture]; !ok {
					return pipelineCorpus{}, fmt.Errorf(
						"%s references unknown fixture %q", casePath, testCase.Fixture,
					)
				}
				if prior, duplicate := usedFixtures[testCase.Fixture]; duplicate {
					return pipelineCorpus{}, fmt.Errorf(
						"%s reuses fixture %q already bound by %s", casePath, testCase.Fixture, prior,
					)
				}
				usedFixtures[testCase.Fixture] = casePath
			}
			for expectation, value := range testCase.Expect {
				if err := validatePipelineExpectation(expectation, value); err != nil {
					return pipelineCorpus{}, fmt.Errorf(
						"%s.expect.%s: %w", casePath, expectation, err,
					)
				}
			}
		}
	}
	for fixtureID := range corpus.Fixtures {
		if _, used := usedFixtures[fixtureID]; !used {
			return pipelineCorpus{}, fmt.Errorf("fixture %q is not bound to a corpus case", fixtureID)
		}
	}
	return corpus, nil
}

func validatePipelineExpectation(name string, raw json.RawMessage) error {
	if vocabulary, ok := pipelineStringExpectationVocabulary[name]; ok {
		var value string
		if err := decodePipelineJSON(raw, &value); err != nil {
			return fmt.Errorf("must be a String vocabulary value: %w", err)
		}
		if _, admitted := vocabulary[value]; !admitted {
			return fmt.Errorf("unsupported %s vocabulary value %q", name, value)
		}
		return nil
	}

	switch name {
	case "atomic":
		var value bool
		if err := decodePipelineJSON(raw, &value); err != nil {
			return fmt.Errorf("must be Boolean true: %w", err)
		}
		if !value {
			return errors.New("must be Boolean true")
		}
		return nil
	case "value":
		return validatePipelineValueExpectation(raw)
	default:
		return fmt.Errorf("unsupported expectation %q", name)
	}
}

func validatePipelineValueExpectation(raw json.RawMessage) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return errors.New("value expectation is empty")
	}
	switch trimmed[0] {
	case '"':
		var value string
		if err := decodePipelineJSON(trimmed, &value); err != nil {
			return err
		}
		admitted := map[string]struct{}{
			"0": {}, "missing-and-null-kept": {}, "missing-source-empty": {},
			"present-empty-emits-zero": {}, "preserve-destination-when-source-null": {},
			"missing-whole-null-and-null-member": {},
		}
		if _, ok := admitted[value]; !ok {
			return fmt.Errorf("unsupported String value vocabulary %q", value)
		}
		return nil
	case '{':
		var object map[string]json.RawMessage
		if err := decodePipelineJSON(trimmed, &object); err != nil {
			return err
		}
		switch {
		case len(object) == 1 && object["double"] != nil:
			return requirePipelineJSONZero(object["double"], "double")
		case len(object) == 1 && object["null"] != nil:
			var value bool
			if err := decodePipelineJSON(object["null"], &value); err != nil {
				return fmt.Errorf("null must be Boolean true: %w", err)
			}
			if !value {
				return errors.New("null must be Boolean true")
			}
			return nil
		case len(object) == 2 && object["row_count"] != nil && object["result_bytes"] != nil:
			if err := requirePipelineJSONZero(object["row_count"], "row_count"); err != nil {
				return err
			}
			return requirePipelineJSONZero(object["result_bytes"], "result_bytes")
		default:
			return fmt.Errorf("unsupported object value vocabulary %s", trimmed)
		}
	case '[':
		var pair []json.RawMessage
		if err := decodePipelineJSON(trimmed, &pair); err != nil {
			return err
		}
		if len(pair) != 2 || !bytes.Equal(bytes.TrimSpace(pair[0]), []byte("null")) {
			return errors.New("List value vocabulary must be exactly [null, []]")
		}
		var empty []json.RawMessage
		if err := decodePipelineJSON(pair[1], &empty); err != nil {
			return fmt.Errorf("List value vocabulary must end in an empty List: %w", err)
		}
		if len(empty) != 0 {
			return errors.New("List value vocabulary must end in an empty List")
		}
		return nil
	default:
		return fmt.Errorf("unsupported value vocabulary type %s", trimmed)
	}
}

func requirePipelineJSONZero(raw json.RawMessage, name string) error {
	var value json.Number
	if err := decodePipelineJSON(raw, &value); err != nil {
		return fmt.Errorf("%s must be numeric zero: %w", name, err)
	}
	if value.String() != "0" {
		return fmt.Errorf("%s = %s, want exact numeric zero", name, value)
	}
	return nil
}

func decodePipelineJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("expectation must contain exactly one JSON value")
		}
		return fmt.Errorf("decode trailing expectation data: %w", err)
	}
	return nil
}

func requirePipelineExecutableSources(t *testing.T, corpus pipelineCorpus) {
	t.Helper()
	visibilityCutoff := uint64(23)
	scope := plan.Scope{
		TenantID:          "pipeline-corpus",
		AuthorizedIndexes: []string{"main"},
		SearchJobID:       "pipeline-corpus-job",
		Earliest:          time.Date(2026, time.August, 11, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, time.August, 13, 0, 0, 0, 0, time.UTC),
		SearchStart:       time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   time.Date(2026, time.August, 12, 13, 0, 0, 0, time.UTC),
		VisibilityCutoff:  &visibilityCutoff,
	}

	for _, rule := range corpus.Rules {
		for _, testCase := range rule.Cases {
			if testCase.Source == "" {
				continue
			}
			rule, testCase := rule, testCase
			t.Run(rule.ID+"/"+testCase.Name, func(t *testing.T) {
				wantDiagnostic, expectsDiagnostic := pipelineExpectedDiagnostic(testCase)
				parsed, err := spl.Parse(testCase.Source)
				if pipelineCorpusStageFailed(
					t, testCase.Source, "parse", err, wantDiagnostic, expectsDiagnostic,
				) {
					return
				}
				logical, err := plan.Build(parsed, scope)
				if pipelineCorpusStageFailed(
					t, testCase.Source, "plan", err, wantDiagnostic, expectsDiagnostic,
				) {
					return
				}
				compiled, err := (clickhouse.Compiler{}).Compile(logical)
				if pipelineCorpusStageFailed(
					t, testCase.Source, "compile", err, wantDiagnostic, expectsDiagnostic,
				) {
					return
				}
				if expectsDiagnostic {
					t.Fatalf("source compiled successfully, want diagnostic %s", wantDiagnostic)
				}
				if !compiled.HasValidExecutionSeal() || strings.TrimSpace(compiled.SQL) == "" {
					t.Fatalf(
						"compiled corpus source is not executable: sealed=%t SQL-bytes=%d",
						compiled.HasValidExecutionSeal(), len(compiled.SQL),
					)
				}
			})
		}
	}
}

func pipelineExpectedDiagnostic(testCase pipelineCase) (string, bool) {
	raw, ok := testCase.Expect["diagnostic"]
	if !ok {
		return "", false
	}
	var result string
	if err := decodePipelineJSON(raw, &result); err != nil {
		panic("validated pipeline diagnostic expectation became undecodable: " + err.Error())
	}
	return result, true
}

func pipelineCorpusStageFailed(
	t *testing.T,
	source, stage string,
	err error,
	wantDiagnostic string,
	expectsDiagnostic bool,
) bool {
	t.Helper()
	if err == nil {
		return false
	}
	if !expectsDiagnostic {
		t.Fatalf("%s unexpectedly failed: %v", stage, err)
	}
	code, sourceRange, ok := pipelineDiagnostic(err)
	if !ok {
		t.Fatalf("%s failed with non-diagnostic error %T: %v", stage, err, err)
	}
	if code != wantDiagnostic {
		t.Fatalf("%s diagnostic = %s, want %s (%v)", stage, code, wantDiagnostic, err)
	}
	if sourceRange.Start.Offset < 0 || sourceRange.End.Offset < sourceRange.Start.Offset ||
		sourceRange.End.Offset > len(source) ||
		(sourceRange.End.Offset == sourceRange.Start.Offset && sourceRange.End.Offset != len(source)) {
		t.Fatalf("%s diagnostic has invalid/non-source-located range %+v for %d-byte source", stage, sourceRange, len(source))
	}
	return true
}

func pipelineDiagnostic(err error) (string, spl.Range, bool) {
	var parseDiagnostic *spl.Diagnostic
	if errors.As(err, &parseDiagnostic) && parseDiagnostic != nil {
		return parseDiagnostic.Code, parseDiagnostic.Range, true
	}
	var planDiagnostic *plan.Diagnostic
	if errors.As(err, &planDiagnostic) && planDiagnostic != nil {
		return planDiagnostic.Code, planDiagnostic.Range, true
	}
	return "", spl.Range{}, false
}

func requirePipelineEvidenceTargets(
	t *testing.T,
	root string,
	corpus pipelineCorpus,
) {
	t.Helper()
	seen := make(map[string]struct{})
	for _, rule := range corpus.Rules {
		for _, testCase := range rule.Cases {
			if testCase.Evidence == "" {
				continue
			}
			if _, duplicate := seen[testCase.Evidence]; duplicate {
				t.Fatalf("corpus evidence token %q is reused", testCase.Evidence)
			}
			seen[testCase.Evidence] = struct{}{}
			targets, registered := pipelineEvidenceRegistry[testCase.Evidence]
			if !registered || len(targets) == 0 {
				t.Fatalf("corpus evidence token %q has no concrete test registry entry", testCase.Evidence)
			}
			seenTargets := make(map[pipelineEvidenceTarget]struct{}, len(targets))
			for _, target := range targets {
				if target.Path == "" || target.Identity == "" {
					t.Fatalf("evidence %q contains an empty concrete target: %+v", testCase.Evidence, target)
				}
				if _, duplicate := seenTargets[target]; duplicate {
					t.Fatalf("evidence %q duplicates concrete target %+v", testCase.Evidence, target)
				}
				seenTargets[target] = struct{}{}
				requirePipelineEvidenceTarget(t, root, testCase.Evidence, target)
			}
		}
	}
	for evidence := range pipelineEvidenceRegistry {
		if _, used := seen[evidence]; !used {
			t.Errorf("concrete evidence registry contains stale token %q", evidence)
		}
	}
}

func requirePipelineEvidenceTarget(
	t *testing.T,
	root, evidence string,
	target pipelineEvidenceTarget,
) {
	t.Helper()
	path := filepath.Clean(filepath.Join(root, target.Path))
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		t.Fatalf("evidence %q target escapes repository: %+v", evidence, target)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read evidence %q target %s: %v", evidence, target.Path, err)
	}
	var found bool
	switch filepath.Ext(path) {
	case ".go":
		declaration := regexp.MustCompile(
			`(?m)^func\s+` + regexp.QuoteMeta(target.Identity) + `\s*\(`,
		)
		found = declaration.Match(contents)
	case ".ts", ".tsx", ".js", ".jsx":
		found = bytes.Contains(contents, []byte(`test("`+target.Identity+`"`)) ||
			bytes.Contains(contents, []byte(`test('`+target.Identity+`'`))
	default:
		t.Fatalf("evidence %q target has unsupported test-file type: %+v", evidence, target)
	}
	if !found {
		t.Fatalf("evidence %q target %s does not declare test %q", evidence, target.Path, target.Identity)
	}
}

func isPipelineCorpusRuleID(id string) bool {
	switch id {
	case "SPL-FIELDS-001", "SPL-REGEX-001", "SPL-REVERSE-001", "SPL-ACCUM-001",
		"SPL-STRCAT-001", "SPL-ADDINFO-001", "SPL-FILLNULL-001", "SPL-ADDTOTALS-001",
		"SPL-DELTA-001", "SPL-MULTIVALUE-EVAL-001", "SPL-SPATH-MULTIVALUE-001",
		"SPL-MAKEMV-001", "SPL-MVEXPAND-001", "SPL-NOMV-001", "SPL-MULTIVALUE-TYPE-001",
		"SPL-ORDER-001", "SPL-PIPELINE-LIMITS-001", "SPL-ATOMIC-001",
		"SPL-PIPELINE-DIAGNOSTICS-001", "SPL-DEDUP-001", "SPL-FREQUENCY-BY-001":
		return true
	default:
		return false
	}
}

func pipelineCorpusRules(rules []pipelineRule) []pipelineRule {
	result := make([]pipelineRule, 0, len(rules))
	for _, rule := range rules {
		if isPipelineCorpusRuleID(rule.ID) {
			result = append(result, rule)
			continue
		}
		if strings.HasPrefix(rule.ID, "SPL-TEST-") ||
			!pipelineRuleIDPattern.MatchString(rule.ID) {
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

func requirePipelineCommandCoverage(t *testing.T, corpus pipelineCorpus) {
	t.Helper()
	for _, command := range []string{
		"regex", "reverse", "accum", "strcat", "addinfo",
		"fillnull", "addtotals", "delta", "spath", "makemv", "mvexpand", "nomv",
		"dedup", "head", "top", "rare",
	} {
		found := false
		for _, rule := range corpus.Rules {
			for _, testCase := range rule.Cases {
				if strings.Contains(strings.ToLower(testCase.Source), "| "+command) {
					found = true
					break
				}
			}
		}
		if !found {
			t.Errorf("pipeline corpus has no executable source case for %q", command)
		}
	}
}

func pipelineRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate pipeline corpus test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func rejectPipelineDuplicateJSONNames(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanPipelineJSONValue(decoder, "$", make(map[string]struct{})); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("corpus must contain exactly one JSON value")
		}
		return fmt.Errorf("decode trailing corpus data: %w", err)
	}
	return nil
}

func scanPipelineJSONValue(decoder *json.Decoder, path string, scratch map[string]struct{}) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode corpus at %s: %w", path, err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		clear(scratch)
		for decoder.More() {
			nameToken, nameErr := decoder.Token()
			if nameErr != nil {
				return fmt.Errorf("decode object name at %s: %w", path, nameErr)
			}
			name, nameOK := nameToken.(string)
			if !nameOK {
				return fmt.Errorf("decode object name at %s: got %T", path, nameToken)
			}
			if _, duplicate := scratch[name]; duplicate {
				return fmt.Errorf("%s duplicates object key %q", path, name)
			}
			scratch[name] = struct{}{}
			if err := scanPipelineJSONValue(decoder, path+"."+name, make(map[string]struct{})); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for index := 0; decoder.More(); index++ {
			if err := scanPipelineJSONValue(decoder, fmt.Sprintf("%s[%d]", path, index), make(map[string]struct{})); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("unexpected delimiter %q at %s", delimiter, path)
	}
}
