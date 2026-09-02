package spl

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

//go:embed completion_catalog.json
var completionCatalogJSON []byte

type completionCatalogDocument struct {
	Commands  []completionCommandEntry  `json:"commands"`
	Functions []completionFunctionEntry `json:"functions"`
	Keywords  []completionKeywordEntry  `json:"keywords"`
}

type completionCatalogEntry struct {
	Name      string `json:"name"`
	Insertion string `json:"insertion"`
	Detail    string `json:"detail"`
}

// completionCommandEntry adds the reference-pane fields a command may carry.
// Both are optional so the catalog stays a completion catalog first, but an
// entry that states one must not leave it blank: the UI renders whatever is
// present, and an empty string would render as a missing row.
type completionCommandEntry struct {
	completionCatalogEntry
	Syntax        *string `json:"syntax,omitempty"`
	Documentation *string `json:"documentation,omitempty"`
}

type completionFunctionEntry struct {
	completionCatalogEntry
	Class                 SuggestionFunctionClass `json:"class"`
	HighlightRequiresCall bool                    `json:"highlight_requires_call"`
}

type completionKeywordEntry struct {
	completionCatalogEntry
	Highlight bool `json:"highlight"`
}

var completionCatalog = mustLoadCompletionCatalog()

func mustLoadCompletionCatalog() completionCatalogDocument {
	var catalog completionCatalogDocument
	if err := json.Unmarshal(completionCatalogJSON, &catalog); err != nil {
		panic(fmt.Sprintf("decode SPL completion catalog: %v", err))
	}
	if len(catalog.Commands) == 0 || len(catalog.Functions) == 0 || len(catalog.Keywords) == 0 {
		panic("decode SPL completion catalog: commands, functions, and keywords must be non-empty")
	}
	seen := make(map[string]string)
	validateEntry := func(category string, entry completionCatalogEntry) {
		if entry.Name == "" || entry.Insertion == "" || entry.Detail == "" {
			panic(fmt.Sprintf("decode SPL completion catalog: incomplete %s entry %q", category, entry.Name))
		}
		key := category + "\x00" + eventfields.FoldASCII(entry.Name)
		if prior, exists := seen[key]; exists {
			panic(fmt.Sprintf("decode SPL completion catalog: duplicate %s entries %q and %q", category, prior, entry.Name))
		}
		seen[key] = entry.Name
	}
	for _, command := range catalog.Commands {
		validateEntry("command", command.completionCatalogEntry)
		if command.Syntax != nil && *command.Syntax == "" {
			panic(fmt.Sprintf("decode SPL completion catalog: blank syntax on command %q", command.Name))
		}
		if command.Documentation != nil && *command.Documentation == "" {
			panic(fmt.Sprintf("decode SPL completion catalog: blank documentation on command %q", command.Name))
		}
	}
	for _, function := range catalog.Functions {
		validateEntry("function", function.completionCatalogEntry)
		if function.Class != SuggestionFunctionClassScalar &&
			function.Class != SuggestionFunctionClassAggregate {
			panic(fmt.Sprintf("decode SPL completion catalog: invalid function class %q", function.Class))
		}
	}
	for _, keyword := range catalog.Keywords {
		validateEntry("keyword", keyword.completionCatalogEntry)
	}
	return catalog
}

// StaticSuggestionCandidates returns the shared command, function, and keyword
// metadata applicable to context. Callers may append authorized dynamic field
// and index candidates before passing the combined slice to
// RankSuggestionCandidates.
func StaticSuggestionCandidates(context SuggestionContext) []SuggestionCandidate {
	candidates := make([]SuggestionCandidate, 0, 32)
	if context.Allows(SuggestionKindCommand) {
		for index, command := range completionCatalog.Commands {
			candidates = append(candidates, SuggestionCandidate{
				Kind:      SuggestionKindCommand,
				Label:     command.Name,
				Insertion: command.Insertion,
				Detail:    command.Detail,
				Priority:  len(completionCatalog.Commands) - index,
			})
		}
	}
	if context.Allows(SuggestionKindFunction) {
		for index, function := range completionCatalog.Functions {
			if context.FunctionClass != "" && function.Class != context.FunctionClass {
				continue
			}
			if len(context.FunctionNames) > 0 &&
				!containsASCIIFold(context.FunctionNames, function.Name) {
				continue
			}
			candidates = append(candidates, SuggestionCandidate{
				Kind:          SuggestionKindFunction,
				Label:         function.Name,
				Insertion:     function.Insertion,
				Detail:        function.Detail,
				Priority:      len(completionCatalog.Functions) - index,
				FunctionClass: function.Class,
			})
		}
	}
	if context.Allows(SuggestionKindKeyword) {
		for index, keyword := range completionCatalog.Keywords {
			if len(context.Keywords) == 0 ||
				!containsASCIIFold(context.Keywords, keyword.Name) {
				continue
			}
			candidates = append(candidates, SuggestionCandidate{
				Kind:      SuggestionKindKeyword,
				Label:     keyword.Name,
				Insertion: keyword.Insertion,
				Detail:    keyword.Detail,
				Priority:  len(completionCatalog.Keywords) - index,
			})
		}
	}
	return candidates
}

func containsASCIIFold(values []string, want string) bool {
	for _, value := range values {
		if equalASCIIFold(value, want) {
			return true
		}
	}
	return false
}
