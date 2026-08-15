package splcompataudit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
)

func TestAuditSourceFindsOnlyAmbiguousUnspacedScalarOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		want   int
	}{
		{name: "legacy-shaped eval", source: "index=main | eval value=request-bytes", want: 1},
		{name: "legacy-shaped where", source: "index=main | where used-capacity>0", want: 1},
		{name: "digits inside identifiers", source: "index=main | eval value=ipv4-6to4", want: 1},
		{name: "leading unary-shaped field", source: "index=main | eval value=-legacy", want: 1},
		{name: "trailing invalid field", source: "index=main | eval value=legacy-", want: 1},
		{name: "punctuated destination", source: "index=main | eval error-rate=1", want: 1},
		{name: "numeric-looking incomplete field", source: "index=main | eval value=50%", want: 1},
		{name: "spaced arithmetic", source: "index=main | eval value=request - bytes"},
		{name: "numeric arithmetic", source: "index=main | eval value=1-2"},
		{name: "signed numeric", source: "index=main | eval value=-2"},
		{name: "numeric exponent", source: "index=main | eval value=1e-3"},
		{name: "mixed exponent then field", source: "index=main | eval value=1e-3+field", want: 1},
		{name: "field then mixed exponent", source: "index=main | eval value=field+1e-3", want: 1},
		{name: "numeric unary chain", source: "index=main | eval value=1--2"},
		{name: "quoted field", source: "index=main | eval value='request-bytes'"},
		{name: "quoted string", source: `index=main | eval value="request-bytes"`},
		{name: "base search preserved", source: "index=main source=/var/log/app-1.log"},
		{name: "invalid source", source: "index=main | eval value=("},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := AuditSource(test.source)
			if err != nil {
				t.Fatalf("AuditSource() error = %v", err)
			}
			if len(got) != test.want {
				t.Fatalf("AuditSource() ranges = %#v, want %d", got, test.want)
			}
			for _, sourceRange := range got {
				if sourceRange.Start.Offset < 0 || sourceRange.End.Offset > len(test.source) ||
					sourceRange.End.Offset-sourceRange.Start.Offset != 1 {
					t.Fatalf("AuditSource() invalid range = %#v", sourceRange)
				}
			}
		})
	}
}

func TestAuditSourceMixedExponentReportsOnlyAuthoredBinaryOperator(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		"index=main | eval value=1e-3+field",
		"index=main | eval value=field+1e-3",
	} {
		ranges, err := AuditSource(source)
		if err != nil {
			t.Fatalf("AuditSource(%q): %v", source, err)
		}
		if len(ranges) != 1 || source[ranges[0].Start.Offset:ranges[0].End.Offset] != "+" {
			t.Fatalf("AuditSource(%q) ranges = %#v, want only plus", source, ranges)
		}
	}
}

func TestAuditSourceFindingAndSourceBoundsFailClosed(t *testing.T) {
	t.Parallel()

	overflow := "index=main | eval value=" + strings.Repeat("a-", MaximumAuditFindings+1) + "a"
	if _, err := AuditSource(overflow); err == nil || !strings.Contains(err.Error(), "findings exceed") {
		t.Fatalf("AuditSource() finding overflow error = %v", err)
	}
	oversized := strings.Repeat("x", MaximumAuditSourceBytes+1)
	if _, err := AuditSource(oversized); err == nil || !strings.Contains(err.Error(), "source exceeds") {
		t.Fatalf("AuditSource() source overflow error = %v", err)
	}
}

func BenchmarkAuditSourceLinear(b *testing.B) {
	for _, operators := range []int{1_000, 10_000, MaximumAuditFindings} {
		source := "index=main | eval value=" + strings.Repeat("a-", operators) + "a"
		b.Run(fmt.Sprintf("operators_%06d", operators), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(source)))
			for range b.N {
				ranges, err := AuditSource(source)
				if err != nil || len(ranges) != operators {
					b.Fatalf("AuditSource() = %d ranges, %v", len(ranges), err)
				}
			}
		})
	}
}

func TestAuditControlDatabaseIsReadOnlyRedactedAndDeterministic(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "control.db")
	database, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"saved-audit-affected", "saved-audit-control"}
	nextID := 0
	store, err := savedobjects.New(database, savedobjects.Options{
		CursorKey: bytes.Repeat([]byte("k"), 32),
		IDGenerator: func() (string, error) {
			id := ids[nextID]
			nextID++
			return id, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, source := range []string{
		"index=main | eval value=request-bytes",
		"index=main | eval value=request - bytes",
	} {
		ownerID := []string{"audit-owner-b", "audit-owner-a"}[index]
		_, err := store.Create(ctx, savedobjects.AccessScope{OwnerID: ownerID}, &opensplunkv1.SavedSearchDefinition{
			Name:         "audit search " + string(rune('a'+index)),
			Search:       &opensplunkv1.SearchDefinition{Spl: source},
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		})
		if err != nil {
			t.Fatalf("create saved search %d: %v", index, err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	before := fileDigest(t, databasePath)
	report, err := AuditControlDatabase(ctx, databasePath)
	if err != nil {
		t.Fatalf("AuditControlDatabase(): %v", err)
	}
	after := fileDigest(t, databasePath)
	if before != after {
		t.Fatal("read-only compatibility audit changed the control database")
	}
	if report.CompatibilityVersion != CompatibilityVersion || report.ScannedObjects != 2 ||
		len(report.Findings) != 1 || report.Findings[0].ObjectID != ids[0] ||
		!strings.HasPrefix(report.Findings[0].SourceLocation, "control-db/saved_searches/"+ids[0]+":") {
		t.Fatalf("AuditControlDatabase() = %#v", report)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"request-bytes", "request - bytes", "index=main", "eval value"} {
		if bytes.Contains(encoded, []byte(private)) {
			t.Fatalf("redacted report leaked authored SPL %q: %s", private, encoded)
		}
	}
	second, err := AuditControlDatabase(ctx, databasePath)
	if err != nil || !reflect.DeepEqual(second, report) {
		t.Fatalf("second audit = (%#v, %v), want deterministic %#v", second, err, report)
	}
}

func TestAuditControlDatabaseRejectsCorruptionWithoutLeakingDetails(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "control.db")
	database, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	store, err := savedobjects.New(database, savedobjects.Options{
		CursorKey: bytes.Repeat([]byte("k"), 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Create(ctx, savedobjects.AccessScope{OwnerID: "private-owner"}, &opensplunkv1.SavedSearchDefinition{
		Name:         "private-search-name",
		Search:       &opensplunkv1.SearchDefinition{Spl: "index=secret | eval private-field=1"},
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(
		ctx,
		`UPDATE saved_searches SET definition_proto = x'ff' WHERE saved_search_id = ?`,
		created.GetSavedSearchId(),
	); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	before := fileDigest(t, databasePath)
	_, err = AuditControlDatabase(ctx, databasePath)
	after := fileDigest(t, databasePath)
	if before != after {
		t.Fatal("read-only compatibility audit changed the corrupt control database")
	}
	if err == nil || err.Error() != "read SPL compatibility audit saved searches" {
		t.Fatalf("AuditControlDatabase() corruption error = %q, want redacted error", err)
	}
}

func TestAuditRepositorySkipsDependenciesSymlinksAndGeneratedTrees(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeAuditFixture(t, filepath.Join(root, "fixtures", "affected.spl"), "index=main | eval value=request-bytes\n")
	writeAuditFixture(t, filepath.Join(root, "examples.go"), "const search = `index=main | where used-capacity>0`\n")
	writeAuditFixture(t, filepath.Join(root, "node_modules", "ignored.spl"), "index=main | eval leak=secret-field\n")
	outside := filepath.Join(t.TempDir(), "outside.spl")
	writeAuditFixture(t, outside, "index=main | eval leak=outside-field\n")
	if err := os.Symlink(outside, filepath.Join(root, "linked.spl")); err != nil {
		t.Fatal(err)
	}

	report, err := AuditRepository(context.Background(), root)
	if err != nil {
		t.Fatalf("AuditRepository(): %v", err)
	}
	if report.ScannedObjects != 2 || len(report.Findings) != 2 {
		t.Fatalf("AuditRepository() = %#v", report)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"request-bytes", "used-capacity", "secret-field", "outside-field", "node_modules", "linked.spl"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("repository audit leaked or traversed %q: %s", forbidden, encoded)
		}
	}
}

func TestRepositoryReadRejectsFinalAndIntermediateSymlinkSwaps(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		swap func(*testing.T, string, string)
	}{
		{
			name: "final file",
			swap: func(t *testing.T, root, outside string) {
				t.Helper()
				path := filepath.Join(root, "fixture", "query.spl")
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(outside, "query.spl"), path); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "intermediate directory",
			swap: func(t *testing.T, root, outside string) {
				t.Helper()
				original := filepath.Join(root, "fixture")
				if err := os.Rename(original, filepath.Join(root, "fixture-old")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, original); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			outside := t.TempDir()
			insidePath := filepath.Join(root, "fixture", "query.spl")
			writeAuditFixture(t, insidePath, "index=main | eval value=inside-field\n")
			writeAuditFixture(t, filepath.Join(outside, "query.spl"), "external-secret")
			handle, err := os.OpenRoot(root)
			if err != nil {
				t.Fatal(err)
			}
			defer handle.Close()
			expected, err := handle.Lstat(filepath.Join("fixture", "query.spl"))
			if err != nil {
				t.Fatal(err)
			}
			test.swap(t, root, outside)
			encoded, err := readRepositoryFile(handle, filepath.Join("fixture", "query.spl"), expected)
			if err == nil || bytes.Contains(encoded, []byte("external-secret")) {
				t.Fatalf("symlink swap read = %q, %v", encoded, err)
			}
		})
	}
}

func fileDigest(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(encoded)
}

func writeAuditFixture(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
