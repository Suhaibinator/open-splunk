package opensplunk

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Suhaibinator/open-splunk/internal/buildassets"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
)

const releaseFixtureRevision = "abcdef0123456789abcdef0123456789abcdef01"

var releaseFixtureIdentity = buildinfo.Identity{
	ApplicationVersion: "0.1.0",
	SourceRevision:     releaseFixtureRevision,
}

func releaseFixture(t *testing.T) fstest.MapFS {
	t.Helper()
	uiBuildID, err := releaseFixtureIdentity.UIBuildID()
	if err != nil {
		t.Fatal(err)
	}
	files := fstest.MapFS{
		".next/BUILD_ID": {
			Data: []byte(uiBuildID),
		},
		"out/index.html": {
			Data: []byte(`<!doctype html><script src="/_next/static/app.0123456789ab.js"></script>`),
		},
		"out/_next/static/app.0123456789ab.js": {
			Data: []byte("release UI"),
		},
		"proto/open_splunk/v1/system.proto": {
			Data: []byte("syntax = \"proto3\";\n"),
		},
		"migrations/sqlite/0001_control.sql": {
			Data: []byte("SELECT 1;\n"),
		},
		"migrations/clickhouse/0001_events.sql": {
			Data: []byte("SELECT 1;\n"),
		},
	}
	manifest, err := buildassets.Generate(files, "0.1.0", releaseFixtureRevision)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := buildassets.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	files["out/"+buildassets.ManifestFilename] = &fstest.MapFile{Data: encoded}
	delete(files, ".next/BUILD_ID")
	return files
}

func TestLoadReleaseValidatesAndScopesEmbeddedFiles(t *testing.T) {
	t.Parallel()

	release, err := loadRelease(releaseFixture(t), releaseFixtureIdentity)
	if err != nil {
		t.Fatalf("loadRelease: %v", err)
	}
	if release.Metadata.SourceRevision != releaseFixtureRevision {
		t.Fatalf("revision = %q", release.Metadata.SourceRevision)
	}
	index, err := fs.ReadFile(release.WebUI, "index.html")
	if err != nil || !strings.Contains(string(index), "app.0123456789ab.js") {
		t.Fatalf("read index = %q, %v", index, err)
	}
	manifest, err := fs.ReadFile(release.WebUI, buildassets.ManifestFilename)
	if err != nil || len(manifest) == 0 {
		t.Fatalf("read manifest = %d bytes, %v", len(manifest), err)
	}
	if _, err := fs.ReadFile(release.WebUI, "proto/open_splunk/v1/system.proto"); err == nil {
		t.Fatal("WebUI exposed embedded protobuf sources")
	}
}

func TestLoadReleaseRejectsMissingMalformedAndTamperedManifest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(fstest.MapFS)
		wantDetail string
	}{
		{
			name: "missing manifest",
			mutate: func(files fstest.MapFS) {
				delete(files, "out/"+buildassets.ManifestFilename)
			},
			wantDetail: "read",
		},
		{
			name: "malformed manifest",
			mutate: func(files fstest.MapFS) {
				files["out/"+buildassets.ManifestFilename].Data = []byte("{}")
			},
			wantDetail: "unmarshal",
		},
		{
			name: "tampered UI",
			mutate: func(files fstest.MapFS) {
				files["out/index.html"].Data = []byte("<!doctype html>tampered")
			},
			wantDetail: "UI",
		},
		{
			name: "extra UI",
			mutate: func(files fstest.MapFS) {
				files["out/stale.js"] = &fstest.MapFile{Data: []byte("stale")}
			},
			wantDetail: "UI",
		},
		{
			name: "tampered protobuf",
			mutate: func(files fstest.MapFS) {
				files["proto/open_splunk/v1/system.proto"].Data = []byte("tampered")
			},
			wantDetail: "protobuf",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			files := releaseFixture(t)
			test.mutate(files)
			_, err := loadRelease(files, releaseFixtureIdentity)
			if err == nil || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("loadRelease error = %v, want detail %q", err, test.wantDetail)
			}
		})
	}
}

func TestLoadReleaseRejectsSelfConsistentManifestForWrongCompiledIdentity(t *testing.T) {
	t.Parallel()

	files := releaseFixture(t)
	wrongIdentity := buildinfo.Identity{
		ApplicationVersion: "9.9.9",
		SourceRevision:     releaseFixtureRevision,
	}
	wrongBuildID, err := wrongIdentity.UIBuildID()
	if err != nil {
		t.Fatal(err)
	}
	files[".next/BUILD_ID"] = &fstest.MapFile{Data: []byte(wrongBuildID)}
	manifest, err := buildassets.Generate(
		files,
		wrongIdentity.ApplicationVersion,
		wrongIdentity.SourceRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := buildassets.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	files["out/"+buildassets.ManifestFilename].Data = encoded
	delete(files, ".next/BUILD_ID")

	_, err = loadRelease(files, releaseFixtureIdentity)
	if err == nil || !strings.Contains(err.Error(), "compiled identity") {
		t.Fatalf("loadRelease error = %v, want compiled identity mismatch", err)
	}
}
