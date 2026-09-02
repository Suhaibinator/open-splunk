package spl

import (
	"os"
	"strings"
	"testing"
)

// The workspace's SPL reference pane is generated from the catalog, so a
// command the catalog names must be one docs/spl.md describes: the pane links
// the two surfaces in the reader's mind, and an undocumented command would
// advertise something the reference document cannot back up. The check is
// one-directional on purpose -- the document may describe behavior the
// completion catalog has no entry for.
func TestCompletionCatalogCommandsAreDocumented(t *testing.T) {
	document, err := os.ReadFile("../../docs/spl.md")
	if err != nil {
		t.Fatalf("read docs/spl.md: %v", err)
	}
	reference := string(document)
	for _, command := range completionCatalog.Commands {
		if !strings.Contains(reference, "`"+command.Name+"`") {
			t.Errorf("catalog command %q has no `%s` mention in docs/spl.md", command.Name, command.Name)
		}
	}
}

// Every shipped command carries the two reference-pane fields. The loader only
// rejects a blank value when the field is present; this pins the stronger
// expectation that no command ships without one, so the pane never shows a
// command with an empty syntax line.
func TestCompletionCatalogCommandsCarryReferenceFields(t *testing.T) {
	for _, command := range completionCatalog.Commands {
		if command.Syntax == nil || strings.TrimSpace(*command.Syntax) == "" {
			t.Errorf("catalog command %q has no syntax", command.Name)
		}
		if command.Documentation == nil || strings.TrimSpace(*command.Documentation) == "" {
			t.Errorf("catalog command %q has no documentation", command.Name)
		}
	}
}

func TestCompletionCatalogRejectsBlankReferenceFields(t *testing.T) {
	original := completionCatalogJSON
	t.Cleanup(func() { completionCatalogJSON = original })
	for _, field := range []string{"syntax", "documentation"} {
		completionCatalogJSON = []byte(`{
  "commands": [{"name": "search", "insertion": "search", "detail": "Filter.", "` + field + `": ""}],
  "functions": [{"name": "count", "insertion": "count", "detail": "Count.", "class": "aggregate", "highlight_requires_call": false}],
  "keywords": [{"name": "AND", "insertion": "AND", "detail": "And.", "highlight": true}]
}`)
		func() {
			defer func() {
				recovered := recover()
				if recovered == nil {
					t.Fatalf("blank %s did not panic", field)
				}
				message, _ := recovered.(string)
				if !strings.Contains(message, "blank "+field) {
					t.Fatalf("blank %s panic = %v, want a blank-%s message", field, recovered, field)
				}
			}()
			mustLoadCompletionCatalog()
		}()
	}
}
