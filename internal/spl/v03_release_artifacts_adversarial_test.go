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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	compatibilityV03Format  = "open-splunk-spl-compatibility-v0.3"
	compatibilityV03Version = "0.3"
)

var compatibilityV03RuleIDPattern = regexp.MustCompile(`^SPL-V03-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)
var compatibilityV03FixtureIDPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`)

type compatibilityV03Corpus struct {
	FormatVersion        string                     `json:"format_version"`
	CompatibilityVersion string                     `json:"compatibility_version"`
	Fixtures             map[string]json.RawMessage `json:"fixtures,omitempty"`
	Rules                []compatibilityV03Rule     `json:"rules"`
}

type compatibilityV03Rule struct {
	ID      string                 `json:"id"`
	Summary string                 `json:"summary"`
	Cases   []compatibilityV03Case `json:"cases"`
}

type compatibilityV03Case struct {
	Name     string                     `json:"name"`
	Source   string                     `json:"source,omitempty"`
	Evidence string                     `json:"evidence,omitempty"`
	Fixture  string                     `json:"fixture,omitempty"`
	Expect   map[string]json.RawMessage `json:"expect"`
}

type compatibilityV03EvidenceTarget struct {
	Path     string
	Identity string
}

// Corpus evidence claims are deliberately closed over exact, discoverable test
// identities, whether or not the same case also carries a compileable source.
// A prose token cannot make an acceptance claim by itself: adding or renaming
// one requires updating this registry, and every registered target is read
// back from the working tree below.
var compatibilityV03EvidenceRegistry = map[string][]compatibilityV03EvidenceTarget{
	"authored-all-ten-executable-source": {
		{Path: "internal/spl/v03_adversarial_parser_test.go", Identity: "TestV03CompatibilityIdentityActivatesWithTheCompleteCommandSurface"},
		{Path: "internal/clickhouse/v03_adversarial_compiler_test.go", Identity: "TestV03AllTenCommandsComposeWithExactRetainedKnowledgeEvidence"},
	},
	"independent-reference-model-all-ten-commands": {
		{Path: "internal/spl/v03_reference_model_test.go", Identity: "TestV03IndependentReferenceModelCoversAllTenCommands"},
		{Path: "internal/spl/v03_reference_model_test.go", Identity: "TestV03IndependentReferenceStrcatUsesSharedConversionAndWritePolicy"},
		{Path: "internal/spl/v03_reference_model_test.go", Identity: "TestV03IndependentReferenceResourceModel"},
	},
	"pinned-all-ten-public-paging": {
		{Path: "internal/export/v03_live_vertical_adversarial_external_test.go", Identity: "TestV03PinnedClickHousePublishesNullableListsThroughPagingAndExport"},
	},
	"retained-v0.1-knowledge-plus-authored-v0.3-suffix": {
		{Path: "internal/clickhouse/v03_adversarial_compiler_test.go", Identity: "TestV03AllTenCommandsComposeWithExactRetainedKnowledgeEvidence"},
	},
	"bootstrap-job-history-export-browser": {
		{Path: "internal/spl/v03_adversarial_parser_test.go", Identity: "TestV03CompatibilityIdentityActivatesWithTheCompleteCommandSurface"},
		{Path: "internal/searchjobs/v03_public_paging_adversarial_test.go", Identity: "TestV03AllTenCommandsRemainStableAndPrivateFreeAcrossPublicPaging"},
		{Path: "internal/server/v03_saved_history_adversarial_test.go", Identity: "TestV03SavedSearchExecutionUsesPersistedAllTenDefinition"},
		{Path: "internal/server/v03_saved_history_adversarial_test.go", Identity: "TestV03HistoryRerunReconstructsPersistedAllTenDefinition"},
		{Path: "internal/export/v03_adversarial_test.go", Identity: "TestV03ExpandedPublicRowsExportWithoutContainerOrPrivateArtifacts"},
		{Path: "internal/export/v03_live_vertical_adversarial_external_test.go", Identity: "TestV03PinnedClickHousePublishesNullableListsThroughPagingAndExport"},
		{Path: "lib/search/spl-syntax.test.ts", Identity: "v0.3 commands are browser-supported through the shared catalog"},
	},
	"queryexec-optional-multivalue-two-row-transport": {
		{Path: "internal/queryexec/v03_optional_multivalue_transport_adversarial_test.go", Identity: "TestV03OptionalMultivalueTransportDistinguishesNullEmptyAndMembers"},
	},
	"optional-multivalue-presence-bit-2": {
		{Path: "internal/queryexec/v03_optional_multivalue_transport_adversarial_test.go", Identity: "TestV03OptionalMultivalueTransportRejectsForgedNativeValues"},
		{Path: "internal/queryexec/v03_optional_multivalue_transport_adversarial_test.go", Identity: "TestV03OptionalMultivalueTransportRejectsLateForgedRowAtomically"},
	},
	"fillnull-and-addtotals-65th-field": {
		{Path: "internal/spl/v03_adversarial_parser_test.go", Identity: "TestV03SourceDeterminedResourceBoundaries"},
	},
	"two-10000-row-mvexpand-stages": {
		{Path: "internal/clickhouse/v03_adversarial_integration_test.go", Identity: "TestV03AdversarialAgainstClickHouse"},
	},
	"retained-bytes-before-downstream-head": {
		{Path: "internal/clickhouse/v03_adversarial_integration_test.go", Identity: "TestV03AdversarialAgainstClickHouse"},
	},
	"fillnull-dynamic-publication-before-mvexpand": {
		{Path: "internal/clickhouse/v03_fillnull_mvexpand_composition_regression_test.go", Identity: "TestCompileV03FillNullDynamicThenMVExpandMaterializesThePublicField"},
		{Path: "internal/clickhouse/v03_fillnull_mvexpand_composition_regression_test.go", Identity: "TestV03FillNullDynamicThenMVExpandAgainstClickHouse"},
	},
	"fillnull-private-physical-publication": {
		{Path: "internal/clickhouse/v03_fillnull_private_physical_composition_test.go", Identity: "TestCompileV03FillNullBindsPrivatePhysicalColumns"},
		{Path: "internal/clickhouse/v03_fillnull_private_physical_composition_test.go", Identity: "TestV03FillNullAfterPrivatePhysicalProducersAgainstClickHouse"},
	},
	"fillnull-invalid-path-stats-literal-namespace-overlap": {
		{Path: "internal/clickhouse/v03_public_namespace_overlap_compiler_test.go", Identity: "TestV03FillNullRepublishesInvalidPathStatsLiteralNamespaceOverlap"},
		{Path: "internal/clickhouse/v03_adversarial_integration_test.go", Identity: "TestV03AdversarialAgainstClickHouse"},
	},
	"atomic-query-valid-first-row-invalid-later-row": {
		{Path: "internal/queryexec/atomic_result_barrier_adversarial_test.go", Identity: "TestAtomicResultBarrierRejectsLateFailureWithoutPublishing"},
		{Path: "internal/queryexec/v03_optional_multivalue_transport_adversarial_test.go", Identity: "TestV03OptionalMultivalueTransportRejectsLateForgedRowAtomically"},
	},
	"atomic-manager-max-rows-below-produced-rows": {
		{Path: "internal/searchjobs/v03_public_paging_adversarial_test.go", Identity: "TestV03AtomicResultRowLimitStagesNoPreviewAndPublishesNothing"},
	},
	"server-saved-object-all-ten-launch": {
		{Path: "internal/server/v03_saved_history_adversarial_test.go", Identity: "TestV03SavedSearchExecutionUsesPersistedAllTenDefinition"},
	},
	"server-history-all-ten-rerun": {
		{Path: "internal/server/v03_saved_history_adversarial_test.go", Identity: "TestV03HistoryRerunReconstructsPersistedAllTenDefinition"},
	},
	"1025-byte-makemv-delimiter": {
		{Path: "internal/spl/v03_adversarial_parser_test.go", Identity: "TestV03SourceDeterminedResourceBoundaries"},
	},
	"clickhouse-code-395-unsupported-mvexpand-marker": {
		{Path: "internal/queryexec/v03_runtime_marker_adversarial_test.go", Identity: "TestV03RuntimeMarkersAreCompletelyClassifiedAndRedacted"},
		{Path: "internal/clickhouse/v03_adversarial_integration_test.go", Identity: "TestV03AdversarialAgainstClickHouse"},
	},
	"makemv-rejected-domain-matrix": {
		{Path: "internal/clickhouse/v03_adversarial_integration_test.go", Identity: "TestV03AdversarialAgainstClickHouse"},
		{Path: "internal/queryexec/v03_runtime_marker_adversarial_test.go", Identity: "TestV03RuntimeMarkersAreCompletelyClassifiedAndRedacted"},
		{Path: "internal/clickhouse/v03_fillnull_container_compiler_test.go", Identity: "TestV03FixedRawProvenancePipelinesCompileWithAtomicConsumers"},
	},
}

var compatibilityV03StringExpectationVocabulary = map[string]map[string]struct{}{
	"diagnostic": {
		"SPL_EXPECTED_FIELD": {}, "SPL_RESERVED_FIELD": {},
		"SPL_EXPECTED_REGEX_PATTERN": {}, "SPL_UNSUPPORTED_REVERSE_SYNTAX": {},
		"SPL_UNSUPPORTED_STRCAT_SYNTAX": {}, "SPL_UNSUPPORTED_ADDINFO_SYNTAX": {},
		"SPL_UNSUPPORTED_FILLNULL_SYNTAX": {}, "SPL_UNSUPPORTED_ADDTOTALS_SYNTAX": {},
		"SPL_UNSUPPORTED_DELTA_SYNTAX": {}, "SPL_UNSUPPORTED_MAKEMV_SYNTAX": {},
		"SPL_QUERY_TOO_COMPLEX": {}, "unsupported-value": {},
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

// This is an activation gate, not a development-phase unit test. The plan is
// explicit that executable syntax alone is insufficient: a normative contract,
// exact machine-readable parity, and migration guidance must exist before the
// identity can become v0.3.
func TestV03ReleaseArtifactsExistAndHaveExactRuleParity(t *testing.T) {
	t.Parallel()

	root := compatibilityV03RepositoryRoot(t)
	contractPath := filepath.Join(root, "docs", "spl-compatibility-v0.3.md")
	corpusPath := filepath.Join(root, "internal", "spl", "testdata", "compatibility-v0.3.json")
	migrationPath := filepath.Join(root, "docs", "spl-compatibility-v0.3-migration.md")

	contract := requireV03ReleaseFile(t, contractPath)
	corpusBytes := requireV03ReleaseFile(t, corpusPath)
	migration := requireV03ReleaseFile(t, migrationPath)

	corpus, err := loadCompatibilityV03Corpus(corpusBytes)
	if err != nil {
		t.Fatalf("load v0.3 compatibility corpus: %v", err)
	}
	if _, err := validateV03ExecutableCorpus(loadV03ExecutableCorpus(t), root); err != nil {
		t.Fatalf("validate executable v0.3 corpus fixtures: %v", err)
	}
	documentIDs, err := compatibilityV03DocumentRuleIDs(contract)
	if err != nil {
		t.Fatalf("read v0.3 contract rule inventory: %v", err)
	}
	corpusIDs := make([]string, len(corpus.Rules))
	for index, rule := range corpus.Rules {
		corpusIDs[index] = rule.ID
	}
	if documentOnly := v03SetDifference(documentIDs, corpusIDs); len(documentOnly) != 0 {
		t.Fatalf("v0.3 rules missing from corpus: %v", documentOnly)
	}
	if corpusOnly := v03SetDifference(corpusIDs, documentIDs); len(corpusOnly) != 0 {
		t.Fatalf("v0.3 rules missing from contract: %v", corpusOnly)
	}
	requireCompatibilityV03CommandCoverage(t, corpus)
	requireCompatibilityV03ExecutableSources(t, corpus)
	requireCompatibilityV03EvidenceTargets(t, root, corpus)

	for _, command := range []string{
		"regex", "reverse", "accum", "strcat", "addinfo",
		"fillnull", "addtotals", "delta", "makemv", "mvexpand",
	} {
		if !bytes.Contains(bytes.ToLower(migration), []byte(command)) {
			t.Errorf("migration note does not name v0.3 command %q", command)
		}
	}
}

func TestV03CorpusLoaderRejectsMalformedSchema(t *testing.T) {
	t.Parallel()

	valid := func(ruleID, caseBody string) string {
		return `{"format_version":"` + compatibilityV03Format +
			`","compatibility_version":"` + compatibilityV03Version +
			`","rules":[{"id":"` + ruleID +
			`","summary":"Summary.","cases":[` + caseBody + `]}]}`
	}
	validCase := `{"name":"case","source":"index=main | reverse","expect":{"parse":"accept"}}`
	for _, test := range []struct {
		name, encoded, want string
	}{
		{name: "empty", encoded: ``, want: "decode corpus"},
		{name: "trailing value", encoded: valid("SPL-V03-TEST-001", validCase) + `{}`, want: "one JSON value"},
		{name: "duplicate key", encoded: `{"format_version":"` + compatibilityV03Format + `","format_version":"` + compatibilityV03Format + `","compatibility_version":"0.3","rules":[]}`, want: `duplicates object key "format_version"`},
		{name: "unknown corpus field", encoded: `{"format_version":"` + compatibilityV03Format + `","compatibility_version":"0.3","rules":[],"typo":true}`, want: "unknown field"},
		{name: "wrong format", encoded: strings.Replace(valid("SPL-V03-TEST-001", validCase), compatibilityV03Format, "wrong", 1), want: "format_version"},
		{name: "wrong identity", encoded: strings.Replace(valid("SPL-V03-TEST-001", validCase), `"compatibility_version":"0.3"`, `"compatibility_version":"0.2"`, 1), want: "compatibility_version"},
		{name: "no rules", encoded: `{"format_version":"` + compatibilityV03Format + `","compatibility_version":"0.3","rules":[]}`, want: "no rules"},
		{name: "bad id", encoded: valid("not-a-rule", validCase), want: "invalid id"},
		{name: "unknown case field", encoded: valid("SPL-V03-TEST-001", `{"name":"case","source":"index=main","expect":{"parse":"accept"},"typo":true}`), want: "unknown field"},
		{name: "empty expectation", encoded: valid("SPL-V03-TEST-001", `{"name":"case","source":"index=main","expect":{}}`), want: "empty expectation"},
		{name: "unknown expectation", encoded: valid("SPL-V03-TEST-001", `{"name":"case","source":"index=main","expect":{"parze":"accept"}}`), want: "unsupported expectation"},
		{name: "atomic vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"atomic":false}}`), want: "expect.atomic"},
		{name: "compatibility vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"compatibility":"0.2"}}`), want: "expect.compatibility"},
		{name: "diagnostic vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"diagnostic":"SPL_UNKNOWN"}}`), want: "expect.diagnostic"},
		{name: "diagnostic phase vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"diagnostic_phase":"runtime"}}`), want: "expect.diagnostic_phase"},
		{name: "order vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"order":"random"}}`), want: "expect.order"},
		{name: "parse vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","source":"index=main","expect":{"parse":"reject"}}`), want: "expect.parse"},
		{name: "range vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"range":"whole-command"}}`), want: "expect.range"},
		{name: "relation vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"relation":"transform"}}`), want: "expect.relation"},
		{name: "resource vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"resource":"unbounded"}}`), want: "expect.resource"},
		{name: "surfaces vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"surfaces":"approximate"}}`), want: "expect.surfaces"},
		{name: "type vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"type":"mixed"}}`), want: "expect.type"},
		{name: "value vocabulary", encoded: valid("SPL-V03-TEST-001", `{"name":"case","evidence":"case","expect":{"value":17}}`), want: "expect.value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := loadCompatibilityV03Corpus([]byte(test.encoded))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("load error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func loadCompatibilityV03Corpus(encoded []byte) (compatibilityV03Corpus, error) {
	if err := rejectV03DuplicateJSONNames(encoded); err != nil {
		return compatibilityV03Corpus{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var corpus compatibilityV03Corpus
	if err := decoder.Decode(&corpus); err != nil {
		return compatibilityV03Corpus{}, fmt.Errorf("decode corpus: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return compatibilityV03Corpus{}, errors.New("corpus must contain exactly one JSON value")
		}
		return compatibilityV03Corpus{}, fmt.Errorf("decode trailing corpus data: %w", err)
	}
	if corpus.FormatVersion != compatibilityV03Format {
		return compatibilityV03Corpus{}, fmt.Errorf("format_version = %q, want %q", corpus.FormatVersion, compatibilityV03Format)
	}
	if corpus.CompatibilityVersion != compatibilityV03Version {
		return compatibilityV03Corpus{}, fmt.Errorf("compatibility_version = %q, want %q", corpus.CompatibilityVersion, compatibilityV03Version)
	}
	if len(corpus.Rules) == 0 {
		return compatibilityV03Corpus{}, errors.New("corpus has no rules")
	}
	for fixtureID, fixture := range corpus.Fixtures {
		if !compatibilityV03FixtureIDPattern.MatchString(fixtureID) {
			return compatibilityV03Corpus{}, fmt.Errorf("fixtures has invalid id %q", fixtureID)
		}
		trimmed := bytes.TrimSpace(fixture)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return compatibilityV03Corpus{}, fmt.Errorf("fixture %q must be a JSON object", fixtureID)
		}
	}
	usedFixtures := make(map[string]string, len(corpus.Fixtures))
	seenRules := make(map[string]struct{}, len(corpus.Rules))
	for ruleIndex, rule := range corpus.Rules {
		path := fmt.Sprintf("rules[%d]", ruleIndex)
		if !compatibilityV03RuleIDPattern.MatchString(rule.ID) {
			return compatibilityV03Corpus{}, fmt.Errorf("%s has invalid id %q", path, rule.ID)
		}
		if _, duplicate := seenRules[rule.ID]; duplicate {
			return compatibilityV03Corpus{}, fmt.Errorf("%s duplicates rule %q", path, rule.ID)
		}
		seenRules[rule.ID] = struct{}{}
		if strings.TrimSpace(rule.Summary) == "" || strings.TrimSpace(rule.Summary) != rule.Summary {
			return compatibilityV03Corpus{}, fmt.Errorf("%s has invalid summary", path)
		}
		if len(rule.Cases) == 0 {
			return compatibilityV03Corpus{}, fmt.Errorf("%s has no cases", path)
		}
		seenCases := make(map[string]struct{}, len(rule.Cases))
		for caseIndex, testCase := range rule.Cases {
			casePath := fmt.Sprintf("%s.cases[%d]", path, caseIndex)
			if strings.TrimSpace(testCase.Name) == "" || strings.TrimSpace(testCase.Name) != testCase.Name {
				return compatibilityV03Corpus{}, fmt.Errorf("%s has invalid name", casePath)
			}
			if _, duplicate := seenCases[testCase.Name]; duplicate {
				return compatibilityV03Corpus{}, fmt.Errorf("%s duplicates case %q", casePath, testCase.Name)
			}
			seenCases[testCase.Name] = struct{}{}
			if testCase.Source == "" && testCase.Evidence == "" {
				return compatibilityV03Corpus{}, fmt.Errorf("%s has no source or evidence", casePath)
			}
			if len(testCase.Expect) == 0 {
				return compatibilityV03Corpus{}, fmt.Errorf("%s has an empty expectation", casePath)
			}
			if testCase.Fixture != "" {
				if _, ok := corpus.Fixtures[testCase.Fixture]; !ok {
					return compatibilityV03Corpus{}, fmt.Errorf(
						"%s references unknown fixture %q", casePath, testCase.Fixture,
					)
				}
				if prior, duplicate := usedFixtures[testCase.Fixture]; duplicate {
					return compatibilityV03Corpus{}, fmt.Errorf(
						"%s reuses fixture %q already bound by %s", casePath, testCase.Fixture, prior,
					)
				}
				usedFixtures[testCase.Fixture] = casePath
			}
			for expectation, value := range testCase.Expect {
				if err := validateCompatibilityV03Expectation(expectation, value); err != nil {
					return compatibilityV03Corpus{}, fmt.Errorf(
						"%s.expect.%s: %w", casePath, expectation, err,
					)
				}
			}
		}
	}
	for fixtureID := range corpus.Fixtures {
		if _, used := usedFixtures[fixtureID]; !used {
			return compatibilityV03Corpus{}, fmt.Errorf("fixture %q is not bound to a corpus case", fixtureID)
		}
	}
	return corpus, nil
}

func validateCompatibilityV03Expectation(name string, raw json.RawMessage) error {
	if vocabulary, ok := compatibilityV03StringExpectationVocabulary[name]; ok {
		var value string
		if err := decodeCompatibilityV03JSON(raw, &value); err != nil {
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
		if err := decodeCompatibilityV03JSON(raw, &value); err != nil {
			return fmt.Errorf("must be Boolean true: %w", err)
		}
		if !value {
			return errors.New("must be Boolean true")
		}
		return nil
	case "compatibility":
		return validateCompatibilityV03IdentityExpectation(raw)
	case "value":
		return validateCompatibilityV03ValueExpectation(raw)
	default:
		return fmt.Errorf("unsupported expectation %q", name)
	}
}

func validateCompatibilityV03IdentityExpectation(raw json.RawMessage) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return errors.New("compatibility expectation is empty")
	}
	if trimmed[0] == '"' {
		var value string
		if err := decodeCompatibilityV03JSON(trimmed, &value); err != nil {
			return err
		}
		if value != compatibilityV03Version {
			return fmt.Errorf("identity = %q, want %q", value, compatibilityV03Version)
		}
		return nil
	}
	if trimmed[0] != '{' {
		return errors.New("must be the v0.3 String or an authored/knowledge identity object")
	}
	var value struct {
		Authored  string `json:"authored"`
		Knowledge string `json:"knowledge"`
	}
	if err := decodeCompatibilityV03JSON(trimmed, &value); err != nil {
		return err
	}
	if value.Authored != compatibilityV03Version || value.Knowledge != "0.1" {
		return fmt.Errorf(
			"profile identity = authored %q knowledge %q, want 0.3/0.1",
			value.Authored, value.Knowledge,
		)
	}
	return nil
}

func validateCompatibilityV03ValueExpectation(raw json.RawMessage) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return errors.New("value expectation is empty")
	}
	switch trimmed[0] {
	case '"':
		var value string
		if err := decodeCompatibilityV03JSON(trimmed, &value); err != nil {
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
		if err := decodeCompatibilityV03JSON(trimmed, &object); err != nil {
			return err
		}
		switch {
		case len(object) == 1 && object["double"] != nil:
			return requireCompatibilityV03JSONZero(object["double"], "double")
		case len(object) == 1 && object["null"] != nil:
			var value bool
			if err := decodeCompatibilityV03JSON(object["null"], &value); err != nil {
				return fmt.Errorf("null must be Boolean true: %w", err)
			}
			if !value {
				return errors.New("null must be Boolean true")
			}
			return nil
		case len(object) == 2 && object["row_count"] != nil && object["result_bytes"] != nil:
			if err := requireCompatibilityV03JSONZero(object["row_count"], "row_count"); err != nil {
				return err
			}
			return requireCompatibilityV03JSONZero(object["result_bytes"], "result_bytes")
		default:
			return fmt.Errorf("unsupported object value vocabulary %s", trimmed)
		}
	case '[':
		var pair []json.RawMessage
		if err := decodeCompatibilityV03JSON(trimmed, &pair); err != nil {
			return err
		}
		if len(pair) != 2 || !bytes.Equal(bytes.TrimSpace(pair[0]), []byte("null")) {
			return errors.New("List value vocabulary must be exactly [null, []]")
		}
		var empty []json.RawMessage
		if err := decodeCompatibilityV03JSON(pair[1], &empty); err != nil {
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

func requireCompatibilityV03JSONZero(raw json.RawMessage, name string) error {
	var value json.Number
	if err := decodeCompatibilityV03JSON(raw, &value); err != nil {
		return fmt.Errorf("%s must be numeric zero: %w", name, err)
	}
	if value.String() != "0" {
		return fmt.Errorf("%s = %s, want exact numeric zero", name, value)
	}
	return nil
}

func decodeCompatibilityV03JSON(raw []byte, target any) error {
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

func requireCompatibilityV03ExecutableSources(t *testing.T, corpus compatibilityV03Corpus) {
	t.Helper()
	visibilityCutoff := uint64(23)
	scope := plan.Scope{
		TenantID:          "compatibility-v03-corpus",
		AuthorizedIndexes: []string{"main"},
		SearchJobID:       "compatibility-v03-corpus-job",
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
				wantDiagnostic, expectsDiagnostic := compatibilityV03ExpectedDiagnostic(testCase)
				parsed, err := spl.Parse(testCase.Source)
				if compatibilityV03CorpusStageFailed(
					t, testCase.Source, "parse", err, wantDiagnostic, expectsDiagnostic,
				) {
					return
				}
				logical, err := plan.Build(parsed, scope)
				if compatibilityV03CorpusStageFailed(
					t, testCase.Source, "plan", err, wantDiagnostic, expectsDiagnostic,
				) {
					return
				}
				compiled, err := (clickhouse.Compiler{}).Compile(logical)
				if compatibilityV03CorpusStageFailed(
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

func compatibilityV03ExpectedDiagnostic(testCase compatibilityV03Case) (string, bool) {
	raw, ok := testCase.Expect["diagnostic"]
	if !ok {
		return "", false
	}
	var result string
	if err := decodeCompatibilityV03JSON(raw, &result); err != nil {
		panic("validated v0.3 diagnostic expectation became undecodable: " + err.Error())
	}
	return result, true
}

func compatibilityV03CorpusStageFailed(
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
	code, sourceRange, ok := compatibilityV03Diagnostic(err)
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

func compatibilityV03Diagnostic(err error) (string, spl.Range, bool) {
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

func requireCompatibilityV03EvidenceTargets(
	t *testing.T,
	root string,
	corpus compatibilityV03Corpus,
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
			targets, registered := compatibilityV03EvidenceRegistry[testCase.Evidence]
			if !registered || len(targets) == 0 {
				t.Fatalf("corpus evidence token %q has no concrete test registry entry", testCase.Evidence)
			}
			seenTargets := make(map[compatibilityV03EvidenceTarget]struct{}, len(targets))
			for _, target := range targets {
				if target.Path == "" || target.Identity == "" {
					t.Fatalf("evidence %q contains an empty concrete target: %+v", testCase.Evidence, target)
				}
				if _, duplicate := seenTargets[target]; duplicate {
					t.Fatalf("evidence %q duplicates concrete target %+v", testCase.Evidence, target)
				}
				seenTargets[target] = struct{}{}
				requireCompatibilityV03EvidenceTarget(t, root, testCase.Evidence, target)
			}
		}
	}
	for evidence := range compatibilityV03EvidenceRegistry {
		if _, used := seen[evidence]; !used {
			t.Errorf("concrete evidence registry contains stale token %q", evidence)
		}
	}
}

func requireCompatibilityV03EvidenceTarget(
	t *testing.T,
	root, evidence string,
	target compatibilityV03EvidenceTarget,
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

func compatibilityV03DocumentRuleIDs(document []byte) ([]string, error) {
	headingPattern := regexp.MustCompile(`(?m)^### ` + "`" + `(SPL-V03-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3})` + "`" + ` — `)
	allPattern := regexp.MustCompile(`SPL-V03-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}`)
	matches := headingPattern.FindAllSubmatch(document, -1)
	if len(matches) == 0 {
		return nil, errors.New("contract has no rule headings")
	}
	result := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		id := string(match[1])
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("contract duplicates rule heading %q", id)
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	for _, match := range allPattern.FindAll(document, -1) {
		if id := string(match); func() bool { _, ok := seen[id]; return ok }() == false {
			return nil, fmt.Errorf("contract mentions rule %q without a rule heading", id)
		}
	}
	return result, nil
}

func requireCompatibilityV03CommandCoverage(t *testing.T, corpus compatibilityV03Corpus) {
	t.Helper()
	for _, command := range []string{
		"regex", "reverse", "accum", "strcat", "addinfo",
		"fillnull", "addtotals", "delta", "makemv", "mvexpand",
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
			t.Errorf("v0.3 corpus has no executable source case for %q", command)
		}
	}
}

func compatibilityV03RepositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate v0.3 release gate source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func requireV03ReleaseFile(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("required v0.3 release artifact %s: %v", path, err)
	}
	if len(bytes.TrimSpace(contents)) == 0 {
		t.Fatalf("required v0.3 release artifact %s is empty", path)
	}
	return contents
}

func v03SetDifference(left, right []string) []string {
	rightSet := make(map[string]struct{}, len(right))
	for _, value := range right {
		rightSet[value] = struct{}{}
	}
	result := make([]string, 0)
	for _, value := range left {
		if _, ok := rightSet[value]; !ok {
			result = append(result, value)
		}
	}
	slices.Sort(result)
	return result
}

func rejectV03DuplicateJSONNames(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanV03JSONValue(decoder, "$", make(map[string]struct{})); err != nil {
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

func scanV03JSONValue(decoder *json.Decoder, path string, scratch map[string]struct{}) error {
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
			if err := scanV03JSONValue(decoder, path+"."+name, make(map[string]struct{})); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for index := 0; decoder.More(); index++ {
			if err := scanV03JSONValue(decoder, fmt.Sprintf("%s[%d]", path, index), make(map[string]struct{})); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("unexpected delimiter %q at %s", delimiter, path)
	}
}
