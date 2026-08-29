package spl_test

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/officialspl"
)

var documentedCommandPattern = regexp.MustCompile("`([a-z][a-z0-9]*)`")

// TestOfficialSPLCompatibilityCorpus is deliberately separate from the local
// compatibility corpus. Its inputs and expectations are pinned to identified
// Splunk documentation sections, so implementation behavior cannot define its
// own compatibility oracle.
func TestOfficialSPLCompatibilityCorpus(t *testing.T) {
	t.Parallel()

	corpus := loadOfficialSPLCorpus(t)
	for _, testCase := range corpus.Cases {
		t.Run(testCase.ID, func(t *testing.T) {
			t.Parallel()
			query, err := spl.Parse(testCase.Query)
			if err != nil {
				t.Fatalf("Parse official %s case from %s (%s): %v", testCase.Command, testCase.Source.URL, testCase.Source.Section, err)
			}
			commands := make([]string, len(query.Commands))
			for index, command := range query.Commands {
				commands[index] = command.Name()
			}
			if !slices.Equal(commands, testCase.Expect.Commands) {
				t.Fatalf("commands = %v, want %v", commands, testCase.Expect.Commands)
			}
			if testCase.Expect.Sort != nil {
				assertOfficialSortExpectation(t, testCase.Query, query, *testCase.Expect.Sort)
			}
			if testCase.Expect.Fields != nil {
				assertOfficialFieldsExpectation(t, testCase.Query, query, *testCase.Expect.Fields)
			}
		})
	}
}

// TestDocumentedCommandSurfaceHasOfficialSPLCases prevents docs/spl.md from
// gaining a compatibility claim that is covered only by implementation-owned
// tests. Every documented command needs an independently sourced syntax case.
func TestDocumentedCommandSurfaceHasOfficialSPLCases(t *testing.T) {
	t.Parallel()

	corpusPath, contractPath := officialSPLPaths(t)
	corpus, err := officialspl.Load(corpusPath)
	if err != nil {
		t.Fatalf("load official SPL corpus: %v", err)
	}
	documented := documentedCommands(t, contractPath)
	coveredSet := make(map[string]struct{}, len(corpus.Cases))
	caseIDs := make(map[string]struct{}, len(corpus.Cases))
	for _, testCase := range corpus.Cases {
		coveredSet[testCase.Command] = struct{}{}
		caseIDs[testCase.ID] = struct{}{}
		if testCase.Command == "sort" && testCase.Expect.Sort == nil {
			t.Errorf("official sort case %q lacks detailed AST expectations", testCase.ID)
		}
		if testCase.Command == "fields" && testCase.Expect.Fields == nil {
			t.Errorf("official fields case %q lacks detailed AST expectations", testCase.ID)
		}
	}
	covered := make([]string, 0, len(coveredSet))
	for command := range coveredSet {
		covered = append(covered, command)
	}
	slices.Sort(covered)
	if missing := difference(documented, covered); len(missing) != 0 {
		t.Fatalf("documented commands lack source-backed official SPL cases: %v", missing)
	}
	for _, required := range []string{
		"fields.exclude-list",
		"fields.explicit-wildcard-include",
		"fields.internal-wildcard-exclude",
		"fields.wildcard-include",
		"sort.bounded-spaced-ascending-time",
		"sort.labeled-count",
		"sort.spaced-ascending-time",
		"sort.spaced-descending-field",
		"sort.terminal-descending",
		"sort.typed-multiple-fields",
	} {
		if _, exists := caseIDs[required]; !exists {
			t.Errorf("official SPL corpus is missing required compatibility case %q", required)
		}
	}
}

func assertOfficialSortExpectation(
	t *testing.T,
	source string,
	query *spl.Query,
	want officialspl.SortExpectation,
) {
	t.Helper()
	command, ok := query.Commands[len(query.Commands)-1].(*spl.SortCommand)
	if !ok {
		t.Fatalf("final command = %T, want *spl.SortCommand", query.Commands[len(query.Commands)-1])
	}
	if command.Limit != want.Limit || command.LimitSpecified != want.LimitSpecified ||
		len(command.Fields) != len(want.Fields) {
		t.Fatalf("sort header = limit %d specified=%t fields=%d, want limit %d specified=%t fields=%d",
			command.Limit, command.LimitSpecified, len(command.Fields),
			want.Limit, want.LimitSpecified, len(want.Fields))
	}
	modes := map[string]spl.SortValueMode{
		"auto": spl.SortValueModeAuto, "string": spl.SortValueModeString,
		"number": spl.SortValueModeNumber, "ip": spl.SortValueModeIP,
	}
	for index, expected := range want.Fields {
		got := command.Fields[index]
		if got.Field != expected.Name || got.Quoted != expected.Quoted ||
			got.Descending != expected.Descending || got.Mode != modes[expected.Mode] {
			t.Fatalf("sort field %d = %#v, want %#v", index, got, expected)
		}
		assertOfficialRangeText(t, source, got.Range, expected.RangeText)
		assertOfficialRangeText(t, source, got.FieldRange, expected.FieldRangeText)
	}
}

func assertOfficialFieldsExpectation(
	t *testing.T,
	source string,
	query *spl.Query,
	want officialspl.FieldsExpectation,
) {
	t.Helper()
	command, ok := query.Commands[len(query.Commands)-1].(*spl.FieldsCommand)
	if !ok {
		t.Fatalf("final command = %T, want *spl.FieldsCommand", query.Commands[len(query.Commands)-1])
	}
	if command.Exclude != want.Exclude || !slices.Equal(command.Fields, want.Names) ||
		!slices.Equal(command.WildcardFields, want.Wildcards) ||
		len(command.FieldRanges) != len(want.RangeTexts) {
		t.Fatalf("fields command = %#v, want %#v", command, want)
	}
	for index, rangeText := range want.RangeTexts {
		assertOfficialRangeText(t, source, command.FieldRanges[index], rangeText)
	}
}

func assertOfficialRangeText(t *testing.T, source string, sourceRange spl.Range, want string) {
	t.Helper()
	if sourceRange.Start.Offset < 0 || sourceRange.End.Offset < sourceRange.Start.Offset ||
		sourceRange.End.Offset > len(source) {
		t.Fatalf("invalid source range %#v for %d-byte query", sourceRange, len(source))
	}
	if got := source[sourceRange.Start.Offset:sourceRange.End.Offset]; got != want {
		t.Fatalf("range text = %q, want %q", got, want)
	}
}

func loadOfficialSPLCorpus(t *testing.T) officialspl.Corpus {
	t.Helper()
	corpusPath, _ := officialSPLPaths(t)
	corpus, err := officialspl.Load(corpusPath)
	if err != nil {
		t.Fatalf("load official SPL corpus: %v", err)
	}
	return corpus
}

func officialSPLPaths(t *testing.T) (corpus, contract string) {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate official SPL compatibility test")
	}
	directory := filepath.Dir(source)
	return filepath.Join(directory, "testdata", "official_compatibility.json"),
		filepath.Clean(filepath.Join(directory, "..", "..", "docs", "spl.md"))
}

func documentedCommands(t *testing.T, path string) []string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open SPL contract: %v", err)
	}
	defer file.Close()

	commands := make(map[string]struct{})
	inCommandsSection := false
	inTable := false
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "## Commands" {
			inCommandsSection = true
			continue
		}
		if !inCommandsSection {
			continue
		}
		if strings.HasPrefix(line, "## ") {
			break
		}
		if line == "| Command | Implemented behavior |" {
			inTable = true
			continue
		}
		if !inTable || !strings.HasPrefix(line, "|") {
			continue
		}
		cells := strings.Split(line, "|")
		if len(cells) < 3 {
			continue
		}
		for _, match := range documentedCommandPattern.FindAllStringSubmatch(cells[1], -1) {
			commands[match[1]] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan SPL contract: %v", err)
	}
	result := make([]string, 0, len(commands))
	for command := range commands {
		result = append(result, command)
	}
	slices.Sort(result)
	if len(result) == 0 {
		t.Fatal("SPL contract command table produced no commands")
	}
	return result
}

func difference(left, right []string) []string {
	result := make([]string, 0)
	for _, value := range left {
		if _, found := slices.BinarySearch(right, value); !found {
			result = append(result, value)
		}
	}
	return result
}
