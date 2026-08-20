package buildassets

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
)

const (
	testSourceRevision = "0123456789abcdef0123456789abcdef01234567"
	testUIBuildID      = "r4g31m9hnm8k57j757h57g6028068355704j729m1132264gkng3193840986g4j2"
	testManifestGolden = `{
  "format_version": 1,
  "source_revision": "0123456789abcdef0123456789abcdef01234567",
  "ui_build_id": "r4g31m9hnm8k57j757h57g6028068355704j729m1132264gkng3193840986g4j2",
  "ui": {
    "sha256": "7d6d71c176bb18db6cd155fce479a4a463c77a2074f96c416143eb08437e4c9d",
    "file_count": 2,
    "byte_count": 106,
    "files": [
      {
        "path": "_next/static/chunks/app.0123456789ab.js",
        "size": 27,
        "sha256": "4fac7100f92d510f97a098348ad1ad790b582668c3019890a4f005a707698c1d"
      },
      {
        "path": "index.html",
        "size": 79,
        "sha256": "f75cada14ea2517661a60d9c92a11384886360b91a1202f33a2ae302281a2b24"
      }
    ]
  },
  "protobuf_schema": {
    "sha256": "1e5e4cb78d1409c2b3c757cfeedeaf02504cb4a6329208bb61e58fb77b511a8f",
    "file_count": 1,
    "byte_count": 40
  },
  "sqlite_migrations": {
    "sha256": "80b910820be2c9a6c0375361f38e5e4e177d49a31576b682c62ae1023faa9dfb",
    "file_count": 2,
    "byte_count": 90,
    "latest_version": 2
  },
  "clickhouse_migrations": {
    "sha256": "aeba805e42f40f26825a31ebc65e3a3f8399ce4bc80d578cb69124679175de1a",
    "file_count": 1,
    "byte_count": 76,
    "latest_version": 1
  }
}
`
)

func validFixture() fstest.MapFS {
	return fstest.MapFS{
		".next/BUILD_ID": {
			Data: []byte(testUIBuildID + "\n"),
		},
		"out/index.html": {
			Data: []byte(`<!doctype html><script src="/_next/static/chunks/app.0123456789ab.js"></script>`),
		},
		"out/_next/static/chunks/app.0123456789ab.js": {
			Data: []byte("console.log('open splunk');"),
		},
		"proto/open_splunk/system_api.proto": {
			Data: []byte("syntax = \"proto3\";\npackage open_splunk;\n"),
		},
		"migrations/sqlite/0001_control.sql": {
			Data: []byte("CREATE TABLE control (id TEXT PRIMARY KEY);\n"),
		},
		"migrations/sqlite/0002_indexes.sql": {
			Data: []byte("CREATE TABLE indexes (name TEXT PRIMARY KEY);\n"),
		},
		"migrations/clickhouse/0001_events.sql": {
			Data: []byte("CREATE TABLE events (event_id String) ENGINE = MergeTree ORDER BY event_id;\n"),
		},
	}
}

func cloneFixture(source fstest.MapFS) fstest.MapFS {
	result := make(fstest.MapFS, len(source))
	for name, file := range source {
		cloned := *file
		cloned.Data = bytes.Clone(file.Data)
		result[name] = &cloned
	}
	return result
}

type understatedSizeFS struct {
	fs.FS
	path string
}

func (filesystem understatedSizeFS) Open(name string) (fs.File, error) {
	file, err := filesystem.FS.Open(name)
	if err != nil || name != filesystem.path {
		return file, err
	}
	return &understatedSizeFile{File: file}, nil
}

type understatedSizeFile struct {
	fs.File
}

func (file *understatedSizeFile) Stat() (fs.FileInfo, error) {
	info, err := file.File.Stat()
	if err != nil {
		return nil, err
	}
	return understatedSizeInfo{FileInfo: info}, nil
}

type understatedSizeInfo struct {
	fs.FileInfo
}

func (info understatedSizeInfo) Size() int64 {
	return info.FileInfo.Size() - 1
}

func mustGenerate(t *testing.T, filesystem fs.FS) Manifest {
	t.Helper()
	manifest, err := Generate(filesystem, testSourceRevision)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return manifest
}

func TestGenerateBoundsReadsWhenFileSizeChangesAfterInventory(t *testing.T) {
	t.Parallel()

	filesystem := understatedSizeFS{
		FS:   validFixture(),
		path: "out/index.html",
	}
	if _, err := Generate(filesystem, testSourceRevision); err == nil ||
		!strings.Contains(err.Error(), "read") ||
		!strings.Contains(err.Error(), "expected") {
		t.Fatalf("Generate understated-size error = %v", err)
	}
}

func TestGenerateProducesCanonicalComponentBoundManifest(t *testing.T) {
	t.Parallel()

	fixture := validFixture()
	manifest := mustGenerate(t, fixture)
	if manifest.FormatVersion != ManifestFormatVersion {
		t.Fatalf("format version = %d", manifest.FormatVersion)
	}
	if manifest.SourceRevision != testSourceRevision {
		t.Fatalf("source revision = %q", manifest.SourceRevision)
	}
	if manifest.UIBuildID != testUIBuildID {
		t.Fatalf("UI build ID = %q", manifest.UIBuildID)
	}
	if manifest.UI.FileCount != 2 || manifest.UI.ByteCount == 0 || len(manifest.UI.Files) != 2 {
		t.Fatalf("UI summary = %+v", manifest.UI)
	}
	if manifest.UI.Files[0].Path != "_next/static/chunks/app.0123456789ab.js" ||
		manifest.UI.Files[1].Path != "index.html" {
		t.Fatalf("UI files are not canonically ordered: %+v", manifest.UI.Files)
	}
	if manifest.ProtobufSchema.FileCount != 1 ||
		manifest.SQLiteMigrations.LatestVersion != 2 ||
		manifest.ClickHouseMigrations.LatestVersion != 1 {
		t.Fatalf("component summaries = %+v %+v %+v",
			manifest.ProtobufSchema,
			manifest.SQLiteMigrations,
			manifest.ClickHouseMigrations,
		)
	}

	encoded, err := Marshal(manifest)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(encoded) != testManifestGolden {
		t.Fatalf(
			"format v%d manifest changed without an intentional golden/version update:\n%s",
			ManifestFormatVersion,
			encoded,
		)
	}
	decoded, err := Unmarshal(encoded)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	reencoded, err := Marshal(decoded)
	if err != nil {
		t.Fatalf("Marshal decoded: %v", err)
	}
	if !bytes.Equal(encoded, reencoded) || encoded[len(encoded)-1] != '\n' {
		t.Fatalf("manifest encoding is not canonical:\n%s\n---\n%s", encoded, reencoded)
	}
	if err := Validate(fixture, decoded); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestGenerateAcceptsNestedProtobufPackagesCoveredByRecursiveEmbed(t *testing.T) {
	t.Parallel()

	fixture := cloneFixture(validFixture())
	fixture["proto/open_splunk/nested/system_api.proto"] = &fstest.MapFile{
		Data: []byte("syntax = \"proto3\";\npackage open_splunk.nested;\n"),
	}

	manifest := mustGenerate(t, fixture)
	if manifest.ProtobufSchema.FileCount != 2 {
		t.Fatalf("protobuf file count = %d, want 2", manifest.ProtobufSchema.FileCount)
	}
	if err := Validate(fixture, manifest); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestRepositoryProtobufInventoryCoversCurrentSourceContractTree(t *testing.T) {
	t.Parallel()

	inventory, err := inventoryTree(os.DirFS(filepath.Join("..", "..")), "proto", inventoryOptions{})
	if err != nil {
		t.Fatalf("inventory repository protobuf tree: %v", err)
	}
	if inventory.component.FileCount < 2 {
		t.Fatalf("protobuf file count = %d, want multiple files", inventory.component.FileCount)
	}
	if !slices.ContainsFunc(inventory.files, func(file FileDigest) bool {
		return file.Path == "open_splunk/system_api.proto"
	}) {
		t.Fatal("protobuf inventory omitted open_splunk/system_api.proto")
	}
	if slices.ContainsFunc(inventory.files, func(file FileDigest) bool {
		return !strings.HasSuffix(file.Path, ".proto")
	}) {
		t.Fatal("repository proto tree contains a non-.proto file that recursive embedding would include")
	}
}

func TestGenerateRejectsInputsGoEmbedWouldOmit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		data       string
		wantDetail string
	}{
		{
			name:       "invalid UI directory",
			path:       "out/theme:dark/page.css",
			data:       "body {}\n",
			wantDetail: "invalid character",
		},
		{
			name:       "nested UI module",
			path:       "out/docs/go.mod",
			data:       "module example.invalid/docs\n",
			wantDetail: "module boundary",
		},
		{
			name:       "case-folded nested UI module",
			path:       "out/docs/GO.MOD",
			data:       "module example.invalid/docs\n",
			wantDetail: "module boundary",
		},
		{
			name:       "VCS administrative directory",
			path:       "out/docs/.svn/entries",
			data:       "metadata\n",
			wantDetail: "version-control",
		},
		{
			name:       "non-protobuf source file",
			path:       "proto/open_splunk/README.md",
			data:       "not a wire schema\n",
			wantDetail: "non-.proto",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := cloneFixture(validFixture())
			fixture[test.path] = &fstest.MapFile{Data: []byte(test.data)}

			_, err := Generate(fixture, testSourceRevision)
			if err == nil || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("Generate error = %v, want detail %q", err, test.wantDetail)
			}
		})
	}
}

func TestGenerateBindsEachComponentWithoutCrossComponentDrift(t *testing.T) {
	t.Parallel()

	baseline := mustGenerate(t, validFixture())
	tests := []struct {
		name        string
		path        string
		wantChanged func(Manifest) string
	}{
		{
			name: "UI",
			path: "out/_next/static/chunks/app.0123456789ab.js",
			wantChanged: func(manifest Manifest) string {
				return manifest.UI.SHA256
			},
		},
		{
			name: "protobuf",
			path: "proto/open_splunk/system_api.proto",
			wantChanged: func(manifest Manifest) string {
				return manifest.ProtobufSchema.SHA256
			},
		},
		{
			name: "SQLite migration",
			path: "migrations/sqlite/0002_indexes.sql",
			wantChanged: func(manifest Manifest) string {
				return manifest.SQLiteMigrations.SHA256
			},
		},
		{
			name: "ClickHouse migration",
			path: "migrations/clickhouse/0001_events.sql",
			wantChanged: func(manifest Manifest) string {
				return manifest.ClickHouseMigrations.SHA256
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := cloneFixture(validFixture())
			fixture[test.path].Data = append(bytes.Clone(fixture[test.path].Data), ' ')
			changed := mustGenerate(t, fixture)
			if test.wantChanged(changed) == test.wantChanged(baseline) {
				t.Fatalf("%s digest did not change", test.name)
			}
			if test.name != "UI" && changed.UI.SHA256 != baseline.UI.SHA256 {
				t.Fatal("unrelated UI digest changed")
			}
			if test.name != "protobuf" && changed.ProtobufSchema.SHA256 != baseline.ProtobufSchema.SHA256 {
				t.Fatal("unrelated protobuf digest changed")
			}
			if test.name != "SQLite migration" &&
				changed.SQLiteMigrations.SHA256 != baseline.SQLiteMigrations.SHA256 {
				t.Fatal("unrelated SQLite digest changed")
			}
			if test.name != "ClickHouse migration" &&
				changed.ClickHouseMigrations.SHA256 != baseline.ClickHouseMigrations.SHA256 {
				t.Fatal("unrelated ClickHouse digest changed")
			}
		})
	}
}

func TestGenerateExcludesItsOwnManifestAndFramesPaths(t *testing.T) {
	t.Parallel()

	withOldManifest := validFixture()
	withOldManifest["out/"+ManifestFilename] = &fstest.MapFile{Data: []byte("stale")}
	if got, want := mustGenerate(t, withOldManifest).UI.SHA256, mustGenerate(t, validFixture()).UI.SHA256; got != want {
		t.Fatalf("self-excluding UI digest = %q, want %q", got, want)
	}

	left := validFixture()
	delete(left, "out/_next/static/chunks/app.0123456789ab.js")
	left["out/ab"] = &fstest.MapFile{Data: []byte("c")}
	left["out/index.html"].Data = []byte("<!doctype html>")
	right := validFixture()
	delete(right, "out/_next/static/chunks/app.0123456789ab.js")
	right["out/a"] = &fstest.MapFile{Data: []byte("bc")}
	right["out/index.html"].Data = []byte("<!doctype html>")
	if mustGenerate(t, left).UI.SHA256 == mustGenerate(t, right).UI.SHA256 {
		t.Fatal("path/content boundaries collided")
	}
}

func TestGenerateRejectsInvalidOrIncompleteInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		revision   string
		mutate     func(fstest.MapFS)
		wantDetail string
	}{
		{
			name:       "short revision",
			revision:   "01234567",
			wantDetail: "source revision",
		},
		{
			name:       "uppercase revision",
			revision:   strings.ToUpper(testSourceRevision),
			wantDetail: "source revision",
		},
		{
			name:       "missing index",
			revision:   testSourceRevision,
			mutate:     func(files fstest.MapFS) { delete(files, "out/index.html") },
			wantDetail: "index.html",
		},
		{
			name:     "oversized HTML",
			revision: testSourceRevision,
			mutate: func(files fstest.MapFS) {
				files["out/index.html"].Data = bytes.Repeat([]byte("x"), maximumHTMLBytes+1)
			},
			wantDetail: "exceeds",
		},
		{
			name:     "missing referenced asset",
			revision: testSourceRevision,
			mutate: func(files fstest.MapFS) {
				delete(files, "out/_next/static/chunks/app.0123456789ab.js")
			},
			wantDetail: "referenced asset",
		},
		{
			name:     "build ID mismatch",
			revision: testSourceRevision,
			mutate: func(files fstest.MapFS) {
				files[".next/BUILD_ID"].Data = []byte(testSourceRevision + "\n")
			},
			wantDetail: "build ID",
		},
		{
			name:     "migration gap",
			revision: testSourceRevision,
			mutate: func(files fstest.MapFS) {
				delete(files, "migrations/sqlite/0001_control.sql")
			},
			wantDetail: "contiguous",
		},
		{
			name:     "symbolic link",
			revision: testSourceRevision,
			mutate: func(files fstest.MapFS) {
				files["out/link"] = &fstest.MapFile{Mode: fs.ModeSymlink}
			},
			wantDetail: "regular file",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := cloneFixture(validFixture())
			if test.mutate != nil {
				test.mutate(fixture)
			}
			_, err := Generate(fixture, test.revision)
			if err == nil || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("Generate error = %v, want detail %q", err, test.wantDetail)
			}
		})
	}
}

func TestGenerateRejectsSymlinkedTreeRoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	externalUI := t.TempDir()
	writeTestFile := func(name, contents string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeTestFile(filepath.Join(root, ".next", "BUILD_ID"), testUIBuildID)
	writeTestFile(filepath.Join(externalUI, "index.html"), "<!doctype html>")
	writeTestFile(filepath.Join(root, "proto", "open_splunk", "system.proto"), "syntax = \"proto3\";\n")
	writeTestFile(filepath.Join(root, "migrations", "sqlite", "0001_control.sql"), "SELECT 1;\n")
	writeTestFile(filepath.Join(root, "migrations", "clickhouse", "0001_events.sql"), "SELECT 1;\n")
	if err := os.Symlink(externalUI, filepath.Join(root, "out")); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}

	if _, err := Generate(os.DirFS(root), testSourceRevision); err == nil ||
		!strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("Generate error = %v, want symbolic-link root rejection", err)
	}
}

func TestGenerateRejectsAssetMissingFromNonRootRoute(t *testing.T) {
	t.Parallel()

	fixture := validFixture()
	fixture["out/search/index.html"] = &fstest.MapFile{
		Data: []byte(`<!doctype html><script src="/_next/static/chunks/search.0123456789ab.js"></script>`),
	}
	fixture["out/_next/static/chunks/search.0123456789ab.js"] = &fstest.MapFile{
		Data: []byte("console.log('search');"),
	}
	manifest := mustGenerate(t, fixture)
	if err := Validate(fixture, manifest); err != nil {
		t.Fatalf("Validate complete route assets: %v", err)
	}

	delete(fixture, "out/_next/static/chunks/search.0123456789ab.js")
	if _, err := Generate(fixture, testSourceRevision); err == nil ||
		!strings.Contains(err.Error(), "search/index.html") {
		t.Fatalf("Generate error = %v, want missing search route asset", err)
	}
}

func TestValidateRejectsTamperingMissingAndExtraFiles(t *testing.T) {
	t.Parallel()

	baselineFiles := validFixture()
	manifest := mustGenerate(t, baselineFiles)
	tests := []struct {
		name       string
		mutate     func(fstest.MapFS)
		wantDetail string
	}{
		{
			name: "tampered UI",
			mutate: func(files fstest.MapFS) {
				files["out/index.html"].Data = []byte("<!doctype html>tampered")
			},
			wantDetail: "UI",
		},
		{
			name: "missing UI file",
			mutate: func(files fstest.MapFS) {
				delete(files, "out/_next/static/chunks/app.0123456789ab.js")
			},
			wantDetail: "referenced asset",
		},
		{
			name: "extra UI file",
			mutate: func(files fstest.MapFS) {
				files["out/orphan.js"] = &fstest.MapFile{Data: []byte("stale executable")}
			},
			wantDetail: "UI",
		},
		{
			name: "tampered protobuf",
			mutate: func(files fstest.MapFS) {
				files["proto/open_splunk/system_api.proto"].Data = []byte("changed")
			},
			wantDetail: "protobuf",
		},
		{
			name: "tampered SQLite migration",
			mutate: func(files fstest.MapFS) {
				files["migrations/sqlite/0001_control.sql"].Data = []byte("changed")
			},
			wantDetail: "SQLite",
		},
		{
			name: "tampered ClickHouse migration",
			mutate: func(files fstest.MapFS) {
				files["migrations/clickhouse/0001_events.sql"].Data = []byte("changed")
			},
			wantDetail: "ClickHouse",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := cloneFixture(baselineFiles)
			test.mutate(fixture)
			err := Validate(fixture, manifest)
			if err == nil || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("Validate error = %v, want detail %q", err, test.wantDetail)
			}
		})
	}
}

func TestMarshalRejectsManifestLargerThanUnmarshalLimit(t *testing.T) {
	t.Parallel()

	manifest := mustGenerate(t, validFixture())
	manifest.UI.Files = make([]FileDigest, maximumTreeFiles)
	for index := range manifest.UI.Files {
		manifest.UI.Files[index] = FileDigest{
			Path:   fmt.Sprintf("chunks/%05d-%032d.js", index, index),
			SHA256: strings.Repeat("0", sha256.Size*2),
		}
	}
	manifest.UI.FileCount = uint32(len(manifest.UI.Files))
	manifest.UI.ByteCount = 0
	manifest.UI.SHA256 = strings.Repeat("0", sha256.Size*2)

	if _, err := Marshal(manifest); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("Marshal error = %v, want manifest size rejection", err)
	}
}

func TestUnmarshalRejectsUnknownNoncanonicalAndTrailingData(t *testing.T) {
	t.Parallel()

	encoded, err := Marshal(mustGenerate(t, validFixture()))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "unknown field",
			input: bytes.Replace(encoded, []byte(`"format_version": 1`), []byte(`"unknown": true, "format_version": 1`), 1),
		},
		{
			name:  "trailing value",
			input: append(bytes.Clone(encoded), []byte("{}\n")...),
		},
		{
			name:  "noncanonical spacing",
			input: bytes.Replace(encoded, []byte(": "), []byte(":"), 1),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Unmarshal(test.input); err == nil {
				t.Fatal("Unmarshal error = nil")
			}
		})
	}
}
