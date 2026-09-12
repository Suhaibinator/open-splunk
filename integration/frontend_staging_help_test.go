//go:build !windows

package integration_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestBackendFrontendStageIncludesCanonicalHelpInputs(t *testing.T) {
	t.Parallel()
	repository := repositoryRoot(t)
	stage := t.TempDir()
	if err := copyBackendFrontendStageSources(t.Context(), repository, stage); err != nil {
		t.Fatalf("stage backend frontend sources: %v", err)
	}
	// The generated bundle records every published source from the canonical
	// registry. Its separate frontend consistency test keeps this inventory in
	// sync with that registry, including linked support docs and examples.
	encoded, err := os.ReadFile(filepath.Join(repository, "app", "help", "help-content.generated.json"))
	if err != nil {
		t.Fatal(err)
	}
	var bundle struct {
		Documents []struct {
			SourcePath string `json:"sourcePath"`
		} `json:"documents"`
	}
	if err := json.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	if len(bundle.Documents) == 0 {
		t.Fatal("canonical Help source inventory is empty")
	}
	inputs := []string{
		// Unpublished contributor instructions are also read by the loader.
		"AGENTS.md",
		"CLAUDE.md",
		"lib/help/documentation-registry.mjs",
		"scripts/build-help.mjs",
	}
	for _, document := range bundle.Documents {
		inputs = append(inputs, document.SourcePath)
	}
	for _, relativePath := range inputs {
		if !filepath.IsLocal(relativePath) {
			t.Fatalf("Help source path is not repository-local: %q", relativePath)
		}
		want, err := os.ReadFile(filepath.Join(repository, filepath.FromSlash(relativePath)))
		if err != nil {
			t.Fatalf("read canonical Help input %q: %v", relativePath, err)
		}
		got, err := os.ReadFile(filepath.Join(stage, filepath.FromSlash(relativePath)))
		if err != nil {
			t.Fatalf("staged Help input %q is unavailable: %v", relativePath, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("staged Help input %q differs from its canonical source", relativePath)
		}
	}
}
