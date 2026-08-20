// Package knowledgecompat binds the normative knowledge compatibility corpus
// to executable, package-local production-path assertions.
//
// It is test support only: production packages do not import it. A corpus case
// owns exactly one (owner, vector) authority. Each owning package supplies a
// compile-time callback registry, and Run rejects missing, stale, or duplicate
// vector ownership before invoking any callback.
package knowledgecompat

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

const FormatVersion = uint32(1)

// Owner is the closed package-level authority taxonomy for the corpus.
type Owner string

const (
	OwnerKnowledge                Owner = "knowledge"
	OwnerKnowledgeDefinition      Owner = "knowledge-definition"
	OwnerKnowledgeProgram         Owner = "knowledge-program"
	OwnerKnowledgeSnapshot        Owner = "knowledge-snapshot"
	OwnerKnowledgeCatalog         Owner = "knowledge-catalog"
	OwnerKnowledgeCatalogBlackbox Owner = "knowledge-catalog-blackbox"
	OwnerKnowledgeAttemptAudit    Owner = "knowledge-attempt-audit"
	OwnerKnowledgePreview         Owner = "knowledge-preview"
	OwnerServer                   Owner = "server"
	OwnerControl                  Owner = "control"
	OwnerQueryExec                Owner = "queryexec"
)

var owners = []Owner{
	OwnerKnowledge,
	OwnerKnowledgeDefinition,
	OwnerKnowledgeProgram,
	OwnerKnowledgeSnapshot,
	OwnerKnowledgeCatalog,
	OwnerKnowledgeCatalogBlackbox,
	OwnerKnowledgeAttemptAudit,
	OwnerKnowledgePreview,
	OwnerServer,
	OwnerControl,
	OwnerQueryExec,
}

// Owners returns the closed owner taxonomy in stable order.
func Owners() []Owner { return slices.Clone(owners) }

func (owner Owner) valid() bool { return slices.Contains(owners, owner) }

// Vector is a package-local executable assertion identity. It is deliberately
// not a Go test name: registries bind it directly to a function value.
type Vector string

// Authority is the single executable owner of one compatibility case.
type Authority struct {
	Owner  Owner  `json:"owner"`
	Vector Vector `json:"vector"`
}

// Case is one strictly decoded normative compatibility record.
type Case struct {
	ID        string    `json:"id"`
	Stage     string    `json:"stage"`
	Rule      string    `json:"rule"`
	Expect    string    `json:"expect"`
	Authority Authority `json:"authority"`
}

// Fixture is the complete knowledge-behavior corpus.
type Fixture struct {
	FormatVersion uint32 `json:"format_version"`
	Cases         []Case `json:"cases"`
}

// WantCase pins the exact normative identity, stage, and typed-result label
// consumed by one executable vector.
type WantCase struct {
	ID     string
	Stage  string
	Expect string
}

// Assertion consumes all corpus records assigned to one vector and then runs
// its concrete production-path checks.
type Assertion func(*testing.T, []Case)

// Exact binds an ordered case set to a concrete callback. The callback is a Go
// function value, never a test-name string. Existing exact tests can therefore
// remain the sole request/result/diagnostic authority while the corpus becomes
// an executable dispatch manifest.
func Exact(want []WantCase, run func(*testing.T)) Assertion {
	ownedWant := slices.Clone(want)
	return func(t *testing.T, cases []Case) {
		t.Helper()
		if run == nil {
			t.Fatal("compatibility assertion callback is nil")
		}
		if len(cases) != len(ownedWant) {
			t.Fatalf("compatibility vector cases = %d, want %d", len(cases), len(ownedWant))
		}
		for index, expected := range ownedWant {
			actual := cases[index]
			if actual.ID != expected.ID || actual.Stage != expected.Stage || actual.Expect != expected.Expect {
				t.Fatalf(
					"compatibility case %d = (%q, %q, %q), want (%q, %q, %q)",
					index,
					actual.ID,
					actual.Stage,
					actual.Expect,
					expected.ID,
					expected.Stage,
					expected.Expect,
				)
			}
		}
		run(t)
	}
}

// Run strictly loads the shared corpus, proves exhaustive package-local
// ownership in both directions, and invokes each vector exactly once.
func Run(t *testing.T, owner Owner, registry map[Vector]Assertion) {
	t.Helper()
	if !owner.valid() {
		t.Fatalf("compatibility owner %q is outside the closed taxonomy", owner)
	}
	if len(registry) == 0 {
		t.Fatalf("compatibility owner %q has an empty registry", owner)
	}
	fixture := Load(t)
	grouped := make(map[Vector][]Case)
	for _, testCase := range fixture.Cases {
		if testCase.Authority.Owner == owner {
			grouped[testCase.Authority.Vector] = append(grouped[testCase.Authority.Vector], testCase)
		}
	}
	if len(grouped) == 0 {
		t.Fatalf("compatibility owner %q owns no corpus cases", owner)
	}
	for vector := range grouped {
		if registry[vector] == nil {
			t.Fatalf("compatibility owner %q has no callback for vector %q", owner, vector)
		}
	}
	for vector, assertion := range registry {
		if assertion == nil {
			t.Fatalf("compatibility owner %q vector %q has a nil callback", owner, vector)
		}
		if len(grouped[vector]) == 0 {
			t.Fatalf("compatibility owner %q registry vector %q owns no corpus cases", owner, vector)
		}
	}

	vectors := make([]Vector, 0, len(grouped))
	for vector := range grouped {
		vectors = append(vectors, vector)
	}
	slices.Sort(vectors)
	for _, vector := range vectors {
		cases := slices.Clone(grouped[vector])
		t.Run(string(vector), func(t *testing.T) {
			registry[vector](t, cases)
		})
	}
}

// Load strictly decodes and validates the shared fixture independent of its
// reviewed SHA-256, which remains pinned by internal/knowledge.
func Load(t testing.TB) Fixture {
	t.Helper()
	encoded, err := os.ReadFile(fixturePath())
	if err != nil {
		t.Fatalf("read knowledge compatibility fixture: %v", err)
	}
	var fixture Fixture
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatalf("decode knowledge compatibility fixture: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("decode trailing knowledge compatibility fixture data: %v", err)
	}
	if fixture.FormatVersion != FormatVersion {
		t.Fatalf(
			"knowledge corpus format = %d, want %d",
			fixture.FormatVersion,
			FormatVersion,
		)
	}
	if len(fixture.Cases) != 55 {
		t.Fatalf("knowledge compatibility cases = %d, want 55", len(fixture.Cases))
	}
	seen := make(map[string]struct{}, len(fixture.Cases))
	for index, testCase := range fixture.Cases {
		if testCase.ID == "" || strings.TrimSpace(testCase.ID) != testCase.ID {
			t.Fatalf("knowledge compatibility case %d has invalid id %q", index, testCase.ID)
		}
		if _, duplicate := seen[testCase.ID]; duplicate {
			t.Fatalf("knowledge compatibility case %d duplicates id %q", index, testCase.ID)
		}
		seen[testCase.ID] = struct{}{}
		if strings.TrimSpace(testCase.Stage) == "" || strings.TrimSpace(testCase.Rule) == "" ||
			strings.TrimSpace(testCase.Expect) == "" {
			t.Fatalf("knowledge compatibility case %q has an empty contract field", testCase.ID)
		}
		if !testCase.Authority.Owner.valid() || !validVector(testCase.Authority.Vector) {
			t.Fatalf(
				"knowledge compatibility case %q has invalid authority (%q, %q)",
				testCase.ID,
				testCase.Authority.Owner,
				testCase.Authority.Vector,
			)
		}
	}
	return fixture
}

func validVector(vector Vector) bool {
	value := string(vector)
	if value == "" || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range value {
		if character != '-' && (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func fixturePath() string {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		panic("knowledge compatibility fixture source path is unavailable")
	}
	return filepath.Clean(filepath.Join(
		filepath.Dir(source),
		"..",
		"..",
		"knowledge",
		"testdata",
		"compatibility.json",
	))
}
