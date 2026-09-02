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

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const compatibilityCorpusFormat uint32 = 1

var compatibilityRuleIDPattern = regexp.MustCompile(`^SPL-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)

type compatibilityCorpus struct {
	FormatVersion  uint32                     `json:"format_version"`
	Fixtures       map[string]json.RawMessage `json:"fixtures,omitempty"`
	Rules          []compatibilityRule        `json:"rules"`
	StatsInventory json.RawMessage            `json:"stats_inventory"`
}

type compatibilityRule struct {
	ID      string              `json:"id"`
	Summary string              `json:"summary"`
	Cases   []compatibilityCase `json:"cases"`
}

type compatibilityCase struct {
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

func TestCompatibilityCorpusSchemaAndRuleParity(t *testing.T) {
	t.Parallel()

	corpusPath, contractPath := compatibilityCorpusPaths(t)
	encoded, err := os.ReadFile(corpusPath)
	if err != nil {
		t.Fatalf("read compatibility corpus: %v", err)
	}
	corpus, err := loadCompatibilityCorpus(encoded)
	if err != nil {
		t.Fatalf("load compatibility corpus: %v", err)
	}

	contract, err := os.ReadFile(contractPath)
	if err != nil {
		t.Fatalf("read SPL contract: %v", err)
	}
	documentIDs, err := compatibilityDocumentRuleIDs(contract)
	if err != nil {
		t.Fatalf("read SPL contract rule inventory: %v", err)
	}
	corpusIDs := make([]string, len(corpus.Rules))
	for index, rule := range corpus.Rules {
		corpusIDs[index] = rule.ID
	}

	missingFromCorpus := setDifference(documentIDs, corpusIDs)
	missingFromDocument := setDifference(corpusIDs, documentIDs)
	if len(missingFromCorpus) != 0 || len(missingFromDocument) != 0 {
		t.Fatalf(
			"SPL rule inventories differ: document-only=%v corpus-only=%v",
			missingFromCorpus,
			missingFromDocument,
		)
	}

	requireExpressionCorpusCoverage(t, corpus)
}

func TestCompatibilityCorpusParserExpectations(t *testing.T) {
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

	// These diagnostics are wholly owned by the source parser. Type and
	// execution diagnostics in the same corpus are asserted by their later
	// pipeline stages instead of being accidentally pulled forward to parse.
	parserDiagnostics := map[string]struct{}{
		"SPL_EXPECTED_COMPARISON":           {},
		"SPL_EXPECTED_FIELD":                {},
		"SPL_INVALID_FIELD":                 {},
		"SPL_INVALID_FIELD_QUOTE_ESCAPE":    {},
		"SPL_INVALID_EVAL_ARITY":            {},
		"SPL_UNSUPPORTED_CIDR_PREFIX":       {},
		"SPL_UNSUPPORTED_EVAL_EXPRESSION":   {},
		"SPL_UNSUPPORTED_EXPRESSION":        {},
		"SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX": {},
		"SPL_UNSUPPORTED_STATS_AGGREGATE":   {},
		"SPL_UNSUPPORTED_TOSTRING_FORMAT":   {},
		"SPL_UNSUPPORTED_TRIM_CHARACTERS":   {},
		"SPL_UNTERMINATED_FIELD_QUOTE":      {},
	}

	for _, rule := range corpus.Rules {
		for _, testCase := range rule.Cases {
			parseExpectation, hasParseExpectation := compatibilityExpectedString(testCase, "parse")
			diagnosticExpectation, hasDiagnosticExpectation := compatibilityExpectedString(testCase, "diagnostic")
			_, parserOwnsDiagnostic := parserDiagnostics[diagnosticExpectation]
			if !hasParseExpectation && (!hasDiagnosticExpectation || !parserOwnsDiagnostic) {
				continue
			}

			t.Run(rule.ID+"/"+testCase.Name, func(t *testing.T) {
				var parseErr error
				switch {
				case testCase.Profile == "KnowledgeExpressionProfile":
					expression := testCase.Expression
					if expression == "" {
						expression = testCase.Source
					}
					_, parseErr = spl.ParseScalarExpression(expression)
				case testCase.Source != "":
					_, parseErr = spl.Parse(testCase.Source)
				default:
					t.Skip("corpus case has no parser source fixture")
				}

				if hasParseExpectation && parseExpectation == "accept" {
					if parseErr != nil {
						t.Fatalf("parse: %v", parseErr)
					}
					return
				}
				if !parserOwnsDiagnostic {
					return
				}
				var diagnostic *spl.Diagnostic
				if !errors.As(parseErr, &diagnostic) || diagnostic.Code != diagnosticExpectation {
					t.Fatalf("parse error = %v, want %s", parseErr, diagnosticExpectation)
				}
				if len(diagnostic.Suggestions) > spl.MaximumDiagnosticSuggestions {
					t.Fatalf(
						"diagnostic carries %d suggestions, more than the %d every consumer accepts",
						len(diagnostic.Suggestions),
						spl.MaximumDiagnosticSuggestions,
					)
				}
			})
		}
	}
}

func compatibilityExpectedString(testCase compatibilityCase, name string) (string, bool) {
	raw, exists := testCase.Expect[name]
	if !exists {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	return value, true
}

func TestCompatibilityCorpusLoaderRejectsMalformedSchema(t *testing.T) {
	t.Parallel()

	valid := func(ruleID, caseBody string) string {
		return `{"format_version":1,"stats_inventory":{"format_version":1},"rules":[{"id":"` + ruleID +
			`","summary":"Summary.","cases":[` + caseBody + `]}]}`
	}
	validCase := `{"name":"case","source":"index=main","expect":{"parse":"accept"}}`

	tests := []struct {
		name    string
		encoded string
		want    string
	}{
		{name: "empty", encoded: ``, want: "decode corpus"},
		{name: "trailing value", encoded: valid("SPL-TEST-001", validCase) + `{}`, want: "one JSON value"},
		{name: "duplicate object key", encoded: `{"format_version":1,"format_version":1,"stats_inventory":{"format_version":1},"rules":[]}`, want: `duplicates object key "format_version"`},
		{name: "unknown corpus field", encoded: `{"format_version":1,"stats_inventory":{"format_version":1},"rules":[],"typo":true}`, want: "unknown field"},
		{name: "wrong format", encoded: strings.Replace(valid("SPL-TEST-001", validCase), `"format_version":1`, `"format_version":2`, 1), want: "format_version"},
		{name: "missing stats inventory", encoded: `{"format_version":1,"rules":[]}`, want: "stats_inventory"},
		{name: "no rules", encoded: `{"format_version":1,"stats_inventory":{"format_version":1},"rules":[]}`, want: "no rules"},
		{name: "invalid rule id", encoded: valid("not-a-rule", validCase), want: "invalid id"},
		{name: "duplicate rule id", encoded: strings.Replace(valid("SPL-TEST-001", validCase), `"rules":[`, `"rules":[{"id":"SPL-TEST-001","summary":"Other.","cases":[`+validCase+`]},`, 1), want: "duplicates rule"},
		{name: "empty summary", encoded: strings.Replace(valid("SPL-TEST-001", validCase), "Summary.", "", 1), want: "empty summary"},
		{name: "no cases", encoded: valid("SPL-TEST-001", ""), want: "has no cases"},
		{name: "unknown case field", encoded: valid("SPL-TEST-001", `{"name":"case","source":"index=main","expect":{"parse":"accept"},"typo":true}`), want: "unknown field"},
		{name: "duplicate case name", encoded: valid("SPL-TEST-001", validCase+`,`+validCase), want: "duplicates case"},
		{name: "no case input", encoded: valid("SPL-TEST-001", `{"name":"case","expect":{"parse":"accept"}}`), want: "has no source"},
		{name: "empty expectation", encoded: valid("SPL-TEST-001", `{"name":"case","source":"index=main","expect":{}}`), want: "empty expectation"},
		{name: "unknown expectation", encoded: valid("SPL-TEST-001", `{"name":"case","source":"index=main","expect":{"parze":"accept"}}`), want: "unsupported expectation"},
		{name: "unknown profile", encoded: valid("SPL-TEST-001", `{"name":"case","profile":"unknown","source":"index=main","expect":{"parse":"accept"}}`), want: "invalid profile"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := loadCompatibilityCorpus([]byte(test.encoded))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("load error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func loadCompatibilityCorpus(encoded []byte) (compatibilityCorpus, error) {
	if err := rejectDuplicateJSONNames(encoded); err != nil {
		return compatibilityCorpus{}, err
	}

	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var corpus compatibilityCorpus
	if err := decoder.Decode(&corpus); err != nil {
		return compatibilityCorpus{}, fmt.Errorf("decode corpus: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return compatibilityCorpus{}, errors.New("corpus must contain exactly one JSON value")
		}
		return compatibilityCorpus{}, fmt.Errorf("decode trailing corpus data: %w", err)
	}
	if err := validateCompatibilityCorpus(corpus); err != nil {
		return compatibilityCorpus{}, err
	}
	return corpus, nil
}

func validateCompatibilityCorpus(corpus compatibilityCorpus) error {
	if corpus.FormatVersion != compatibilityCorpusFormat {
		return fmt.Errorf("format_version = %d, want %d", corpus.FormatVersion, compatibilityCorpusFormat)
	}
	if len(corpus.StatsInventory) == 0 || bytes.Equal(bytes.TrimSpace(corpus.StatsInventory), []byte("null")) {
		return errors.New("corpus has no stats_inventory")
	}
	var statsIdentity struct {
		FormatVersion uint32 `json:"format_version"`
	}
	if err := json.Unmarshal(corpus.StatsInventory, &statsIdentity); err != nil ||
		statsIdentity.FormatVersion != compatibilityCorpusFormat {
		return fmt.Errorf("stats_inventory format_version must be %d", compatibilityCorpusFormat)
	}
	if len(corpus.Rules) == 0 {
		return errors.New("corpus has no rules")
	}

	allowedExpectations := map[string]struct{}{
		"audit": {}, "authorized_indexes": {}, "boolean": {}, "bound_arguments": {},
		"compatibility": {}, "diagnostic": {}, "diagnostic_phase": {}, "explain": {},
		"job_failure": {}, "parse": {}, "phase": {}, "range": {}, "range_text": {},
		"relation": {}, "sql_literal_fragments": {}, "suggestion": {}, "surfaces": {},
		"value": {}, "atomic": {}, "command": {}, "commands": {}, "keys": {},
		"mode": {}, "order": {}, "outputs": {}, "resource": {}, "type": {},
	}
	seenRules := make(map[string]struct{}, len(corpus.Rules))
	for ruleIndex, rule := range corpus.Rules {
		path := fmt.Sprintf("rules[%d]", ruleIndex)
		if !compatibilityRuleIDPattern.MatchString(rule.ID) {
			return fmt.Errorf("%s has invalid id %q", path, rule.ID)
		}
		if _, exists := seenRules[rule.ID]; exists {
			return fmt.Errorf("%s duplicates rule %q", path, rule.ID)
		}
		seenRules[rule.ID] = struct{}{}
		if strings.TrimSpace(rule.Summary) == "" {
			return fmt.Errorf("%s has an empty summary", path)
		}
		if strings.TrimSpace(rule.Summary) != rule.Summary {
			return fmt.Errorf("%s summary has surrounding whitespace", path)
		}
		if len(rule.Cases) == 0 {
			return fmt.Errorf("%s has no cases", path)
		}

		seenCases := make(map[string]struct{}, len(rule.Cases))
		for caseIndex, testCase := range rule.Cases {
			casePath := fmt.Sprintf("%s.cases[%d]", path, caseIndex)
			if testCase.Name == "" || strings.TrimSpace(testCase.Name) != testCase.Name {
				return fmt.Errorf("%s has an invalid name %q", casePath, testCase.Name)
			}
			if _, exists := seenCases[testCase.Name]; exists {
				return fmt.Errorf("%s duplicates case %q", casePath, testCase.Name)
			}
			seenCases[testCase.Name] = struct{}{}
			if testCase.Profile != "" && testCase.Profile != "KnowledgeExpressionProfile" &&
				testCase.Profile != "AuthoredExpressionProfile" {
				return fmt.Errorf("%s has invalid profile %q", casePath, testCase.Profile)
			}
			if testCase.Source == "" && testCase.Expression == "" &&
				testCase.RowFixture == "" && testCase.Evidence == "" && testCase.Fixture == "" {
				return fmt.Errorf("%s has no source, expression, row_fixture, or evidence", casePath)
			}
			if len(testCase.Expect) == 0 {
				return fmt.Errorf("%s has an empty expectation", casePath)
			}
			for expectation := range testCase.Expect {
				if _, ok := allowedExpectations[expectation]; !ok {
					return fmt.Errorf("%s has unsupported expectation %q", casePath, expectation)
				}
			}
		}
	}
	return nil
}

func requireExpressionCorpusCoverage(t *testing.T, corpus compatibilityCorpus) {
	t.Helper()

	requiredCases := map[string][]string{
		"SPL-PROFILE-001":                {"authored arithmetic", "knowledge arithmetic remains closed", "production vertical", "downstream composition"},
		"SPL-GRAMMAR-001":                {"eval", "where", "if", "case", "conditional count", "base search membership excluded"},
		"SPL-PRECEDENCE-001":             {"multiplication before addition", "division associates left", "unary associates right", "arithmetic before concatenation prefix"},
		"SPL-GROUPING-001":               {"scalar group", "Boolean group", "grouped scalar is not predicate", "grouped Boolean is not scalar"},
		"SPL-LEXER-001":                  {"unspaced division", "signed exponent", "hyphenated base value preserved", "ambiguous legacy spelling is subtraction"},
		"SPL-QUOTED-FIELD-001":           {"operator field", "quoted destination", "invalid escape", "unterminated", "quoted stats aggregate"},
		"SPL-STATS-BY-MULTIVALUE-001":    {"raw multivalue grouping", "split-value deduplication", "nested container rejection", "late marker publication is atomic"},
		"SPL-ARITHMETIC-TYPE-001":        {"unary plus", "all binary operators", "string plus excluded", "numeric-looking fixed String excluded"},
		"SPL-ARITHMETIC-NULL-001":        {"missing", "explicit null", "numeric String", "String too large", "integer precision boundary", "malformed decimal tag"},
		"SPL-ARITHMETIC-EXCEPTION-001":   {"negative remainder", "negative divisor remainder", "infinity remainder finite", "division retains negative zero", "NaN equality false"},
		"SPL-ARITHMETIC-EVALUATION-001":  {"left associative occurrence", "unselected malformed branch", "arithmetic conditional integer branch rejected", "row cardinality"},
		"SPL-MEMBERSHIP-001":             {"function", "infix", "not infix", "NaN never matches", "later malformed candidate still fails", "thirty third candidate"},
		"SPL-MEMBERSHIP-NULL-001":        {"earlier match before null", "no match plus null", "null input", "not null", "direct assignment rejected"},
		"SPL-EXPRESSION-LIMITS-001":      {"operator 256", "operator 257", "unary chain 33", "membership aggregate 257", "forged cycle", "node SQL 64KiB"},
		"SPL-SECURITY-001":               {"calculated index does not widen", "bound literal", "no expansion"},
		"SPL-EXPRESSION-DIAGNOSTICS-001": {"string plus", "unterminated field quote", "candidate overflow", "Boolean assignment"},
	}

	byRule := make(map[string]map[string]struct{}, len(corpus.Rules))
	for _, rule := range corpus.Rules {
		cases := make(map[string]struct{}, len(rule.Cases))
		for _, testCase := range rule.Cases {
			cases[testCase.Name] = struct{}{}
		}
		byRule[rule.ID] = cases
	}
	for ruleID, cases := range requiredCases {
		actual, ok := byRule[ruleID]
		if !ok {
			t.Errorf("corpus is missing required rule %q", ruleID)
			continue
		}
		for _, caseName := range cases {
			if _, ok := actual[caseName]; !ok {
				t.Errorf("corpus rule %q is missing required case %q", ruleID, caseName)
			}
		}
	}
}

func compatibilityCorpusPaths(t *testing.T) (corpus, contract string) {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate compatibility corpus test source")
	}
	directory := filepath.Dir(source)
	return filepath.Join(directory, "testdata", "compatibility.json"),
		filepath.Clean(filepath.Join(directory, "..", "..", "docs", "spl.md"))
}

func setDifference(left, right []string) []string {
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

func rejectDuplicateJSONNames(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, "$", make(map[string]struct{})); err != nil {
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

func scanJSONValue(decoder *json.Decoder, path string, scratch map[string]struct{}) error {
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
		seen := scratch
		clear(seen)
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode object name at %s: %w", path, err)
			}
			name, ok := nameToken.(string)
			if !ok {
				return fmt.Errorf("decode object name at %s: got %T", path, nameToken)
			}
			if _, exists := seen[name]; exists {
				return fmt.Errorf("%s duplicates object key %q", path, name)
			}
			seen[name] = struct{}{}
			if err := scanJSONValue(decoder, path+"."+name, make(map[string]struct{})); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode object end at %s: %w", path, err)
		}
		if end != json.Delim('}') {
			return fmt.Errorf("decode object end at %s: got %v", path, end)
		}
	case '[':
		for index := 0; decoder.More(); index++ {
			if err := scanJSONValue(decoder, fmt.Sprintf("%s[%d]", path, index), make(map[string]struct{})); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode array end at %s: %w", path, err)
		}
		if end != json.Delim(']') {
			return fmt.Errorf("decode array end at %s: got %v", path, end)
		}
	default:
		return fmt.Errorf("decode corpus at %s: unexpected delimiter %q", path, delimiter)
	}
	return nil
}
