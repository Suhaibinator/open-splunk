// Package officialspl loads the source-backed official SPL conformance corpus.
// It intentionally contains no Open Splunk parser or planner dependencies, so
// the recorded expectations remain independent of the implementation under test.
package officialspl

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"
)

const FormatVersion uint32 = 1

var (
	caseIDPattern  = regexp.MustCompile(`^[a-z0-9]+(?:[.-][a-z0-9]+)*$`)
	commandPattern = regexp.MustCompile(`^[a-z][a-z0-9]*$`)
	releasePattern = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)
)

type Corpus struct {
	FormatVersion uint32 `json:"format_version"`
	Cases         []Case `json:"cases"`
}

type Case struct {
	ID      string      `json:"id"`
	Command string      `json:"command"`
	Source  Source      `json:"source"`
	Query   string      `json:"query"`
	Expect  Expectation `json:"expect"`
}

type Source struct {
	URL        string `json:"url"`
	Release    string `json:"release"`
	Section    string `json:"section"`
	Kind       string `json:"kind"`
	Fragment   string `json:"fragment"`
	VerifiedOn string `json:"verified_on"`
}

type Expectation struct {
	Commands []string           `json:"commands"`
	Sort     *SortExpectation   `json:"sort,omitempty"`
	Fields   *FieldsExpectation `json:"fields,omitempty"`
	// Facets records the documented option surface of the final command as
	// canonical text, keyed by the facet names AllowedFacets grants that
	// command. A value of "" asserts that the optional facet is absent.
	Facets map[string]string `json:"facets,omitempty"`
}

// AllowedFacets names the documented option surface of every command in the
// corpus. The names describe Splunk's documented syntax, not the
// implementation's AST, so a case can pin (for example) rex max_match before
// any implementation honors it. A command with no facets (reverse, addinfo)
// has an empty list; a command missing from this table cannot appear in the
// corpus at all, which keeps the registry complete as commands are added.
var AllowedFacets = map[string][]string{
	"accum":       {"field", "output"},
	"addinfo":     {},
	"addtotals":   {"fields", "output"},
	"bin":         {"field", "output", "span"},
	"bucket":      {"field", "output", "span"},
	"chart":       {"aggregate", "over", "split_by"},
	"dedup":       {"consecutive", "count", "fields", "sortby"},
	"delta":       {"field", "output", "previous"},
	"eval":        {"expressions", "fields"},
	"eventstats":  {"aggregate", "group_by"},
	"fields":      {"mode", "names"},
	"fillnull":    {"fields", "value"},
	"head":        {"count"},
	"lookup":      {"definition", "keys", "output_mode", "outputs"},
	"makemv":      {"allow_empty", "delimiter", "field"},
	"mvexpand":    {"field", "limit"},
	"nomv":        {"field"},
	"rare":        {"fields", "limit"},
	"regex":       {"field", "negated", "pattern"},
	"rename":      {"assignments"},
	"reverse":     {},
	"rex":         {"field", "max_match", "pattern"},
	"search":      {"filter"},
	"sort":        {"keys", "limit"},
	"spath":       {"input", "output", "path"},
	"stats":       {"aggregates", "group_by"},
	"strcat":      {"all_required", "destination", "operands"},
	"streamstats": {"aggregate", "current", "global", "group_by", "window"},
	"table":       {"fields"},
	"tail":        {"count"},
	"timechart":   {"aggregate", "span", "split_by"},
	"top":         {"fields", "limit"},
	"where":       {"predicate"},
}

type SortExpectation struct {
	Limit          uint64                 `json:"limit"`
	LimitSpecified bool                   `json:"limit_specified"`
	Fields         []SortFieldExpectation `json:"fields"`
}

type SortFieldExpectation struct {
	Name           string `json:"name"`
	Quoted         bool   `json:"quoted"`
	Descending     bool   `json:"descending"`
	Mode           string `json:"mode"`
	RangeText      string `json:"range_text"`
	FieldRangeText string `json:"field_range_text"`
}

type FieldsExpectation struct {
	Exclude    bool     `json:"exclude"`
	Names      []string `json:"names"`
	Wildcards  []bool   `json:"wildcards"`
	RangeTexts []string `json:"range_texts"`
}

func Load(path string) (Corpus, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Corpus{}, fmt.Errorf("read official SPL corpus: %w", err)
	}
	return Decode(encoded)
}

func Decode(encoded []byte) (Corpus, error) {
	if err := rejectDuplicateJSONNames(encoded); err != nil {
		return Corpus{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var corpus Corpus
	if err := decoder.Decode(&corpus); err != nil {
		return Corpus{}, fmt.Errorf("decode official SPL corpus: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Corpus{}, errors.New("official SPL corpus must contain exactly one JSON value")
		}
		return Corpus{}, fmt.Errorf("decode trailing official SPL corpus data: %w", err)
	}
	if err := Validate(corpus); err != nil {
		return Corpus{}, err
	}
	return corpus, nil
}

func Validate(corpus Corpus) error {
	if corpus.FormatVersion != FormatVersion {
		return fmt.Errorf("format_version = %d, want %d", corpus.FormatVersion, FormatVersion)
	}
	if len(corpus.Cases) == 0 {
		return errors.New("official SPL corpus has no cases")
	}
	seen := make(map[string]struct{}, len(corpus.Cases))
	for index, testCase := range corpus.Cases {
		path := fmt.Sprintf("cases[%d]", index)
		if !caseIDPattern.MatchString(testCase.ID) {
			return fmt.Errorf("%s has invalid id %q", path, testCase.ID)
		}
		if _, duplicate := seen[testCase.ID]; duplicate {
			return fmt.Errorf("%s duplicates id %q", path, testCase.ID)
		}
		seen[testCase.ID] = struct{}{}
		if !commandPattern.MatchString(testCase.Command) {
			return fmt.Errorf("%s has invalid command %q", path, testCase.Command)
		}
		if _, known := AllowedFacets[testCase.Command]; !known {
			return fmt.Errorf("%s command %q has no AllowedFacets entry", path, testCase.Command)
		}
		if !strings.HasPrefix(testCase.ID, testCase.Command+".") {
			return fmt.Errorf("%s id %q must start with command %q", path, testCase.ID, testCase.Command+".")
		}
		if err := validateSource(path+".source", testCase.Command, testCase.Source); err != nil {
			return err
		}
		if strings.TrimSpace(testCase.Query) != testCase.Query || testCase.Query == "" {
			return fmt.Errorf("%s has invalid query", path)
		}
		if !strings.Contains(testCase.Query, testCase.Source.Fragment) {
			return fmt.Errorf("%s query does not contain the pinned official fragment %q", path, testCase.Source.Fragment)
		}
		if len(testCase.Expect.Commands) == 0 {
			return fmt.Errorf("%s has no expected commands", path)
		}
		if testCase.Expect.Commands[len(testCase.Expect.Commands)-1] != testCase.Command {
			return fmt.Errorf("%s final expected command is not %q", path, testCase.Command)
		}
		if err := validateDetailedExpectation(path+".expect", testCase); err != nil {
			return err
		}
	}
	return nil
}

func validateSource(path, command string, source Source) error {
	parsed, err := url.Parse(source.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "help.splunk.com" {
		return fmt.Errorf("%s.url must be an HTTPS help.splunk.com URL", path)
	}
	if !releasePattern.MatchString(source.Release) {
		return fmt.Errorf("%s has invalid release %q", path, source.Release)
	}
	wantPath := "/spl-search-reference/" + source.Release + "/search-commands/" + command
	if !strings.HasSuffix(strings.TrimSuffix(parsed.Path, "/"), wantPath) ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%s.url path must end with %q and have no query or fragment", path, wantPath)
	}
	if strings.TrimSpace(source.Section) != source.Section || source.Section == "" {
		return fmt.Errorf("%s has invalid section", path)
	}
	if source.Kind != "official-example" && source.Kind != "grammar-derived" {
		return fmt.Errorf("%s has invalid kind %q", path, source.Kind)
	}
	if strings.TrimSpace(source.Fragment) != source.Fragment || source.Fragment == "" {
		return fmt.Errorf("%s has invalid fragment", path)
	}
	verified, err := time.Parse(time.DateOnly, source.VerifiedOn)
	if err != nil || verified.After(time.Now().UTC().Add(24*time.Hour)) {
		return fmt.Errorf("%s has invalid verified_on %q", path, source.VerifiedOn)
	}
	return nil
}

func validateDetailedExpectation(path string, testCase Case) error {
	if testCase.Expect.Sort != nil {
		if testCase.Command != "sort" {
			return fmt.Errorf("%s.sort is only valid for sort cases", path)
		}
		if len(testCase.Expect.Sort.Fields) == 0 {
			return fmt.Errorf("%s.sort has no fields", path)
		}
		for index, field := range testCase.Expect.Sort.Fields {
			fieldPath := fmt.Sprintf("%s.sort.fields[%d]", path, index)
			if field.Name == "" || field.RangeText == "" || field.FieldRangeText == "" {
				return fmt.Errorf("%s is incomplete", fieldPath)
			}
			switch field.Mode {
			case "auto", "string", "number", "ip":
			default:
				return fmt.Errorf("%s has invalid mode %q", fieldPath, field.Mode)
			}
		}
	}
	if testCase.Expect.Fields != nil {
		if testCase.Command != "fields" {
			return fmt.Errorf("%s.fields is only valid for fields cases", path)
		}
		fields := testCase.Expect.Fields
		if len(fields.Names) == 0 || len(fields.Wildcards) != len(fields.Names) ||
			len(fields.RangeTexts) != len(fields.Names) {
			return fmt.Errorf("%s.fields has inconsistent field metadata", path)
		}
	}
	allowed := AllowedFacets[testCase.Command]
	for name, value := range testCase.Expect.Facets {
		if !slices.Contains(allowed, name) {
			return fmt.Errorf("%s.facets[%q] is not a documented %s facet (allowed: %v)",
				path, name, testCase.Command, allowed)
		}
		if strings.TrimSpace(value) != value {
			return fmt.Errorf("%s.facets[%q] has surrounding whitespace", path, name)
		}
	}
	return nil
}

func rejectDuplicateJSONNames(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, "$", make(map[string]struct{})); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("official SPL corpus must contain exactly one JSON value")
		}
		return fmt.Errorf("decode trailing official SPL corpus data: %w", err)
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, path string, scratch map[string]struct{}) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode official SPL corpus at %s: %w", path, err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		clear(scratch)
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode object name at %s: %w", path, err)
			}
			name, ok := nameToken.(string)
			if !ok {
				return fmt.Errorf("decode object name at %s: got %T", path, nameToken)
			}
			if _, duplicate := scratch[name]; duplicate {
				return fmt.Errorf("%s duplicates object key %q", path, name)
			}
			scratch[name] = struct{}{}
			if err := scanJSONValue(decoder, path+"."+name, make(map[string]struct{})); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return fmt.Errorf("decode object end at %s", path)
		}
	case '[':
		for index := 0; decoder.More(); index++ {
			if err := scanJSONValue(decoder, fmt.Sprintf("%s[%d]", path, index), make(map[string]struct{})); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return fmt.Errorf("decode array end at %s", path)
		}
	default:
		return fmt.Errorf("decode official SPL corpus at %s: unexpected delimiter %q", path, delimiter)
	}
	return nil
}
