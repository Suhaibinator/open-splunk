package opensplunk

import (
	"bytes"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/Suhaibinator/open-splunk/internal/buildassets"
)

func helpReleaseFixture(t *testing.T) (fstest.MapFS, map[string][]byte) {
	t.Helper()
	files := releaseFixture(t)
	uiBuildID, err := releaseFixtureIdentity.UIBuildID()
	if err != nil {
		t.Fatal(err)
	}
	files[".next/BUILD_ID"] = &fstest.MapFile{Data: []byte(uiBuildID)}
	help := map[string][]byte{
		"help/index.html":                     []byte(`<main><h1>Documentation</h1><a href="/help/spl/">SPL</a></main>`),
		"help/spl/index.html":                 []byte(`<main><h1>SPL</h1><pre>index=main | stats count</pre></main>`),
		"help/examples/collector/index.html":  []byte(`<main><pre>inputs:\n  - type: file</pre></main>`),
		"_next/static/chunks/help-content.js": []byte(`const revision="` + releaseFixtureRevision + `";`),
	}
	for name, contents := range help {
		files["out/"+name] = &fstest.MapFile{Data: contents}
	}
	manifest, err := buildassets.Generate(files, releaseFixtureRevision)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := buildassets.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	files["out/"+buildassets.ManifestFilename] = &fstest.MapFile{Data: encoded}
	delete(files, ".next/BUILD_ID")
	return files, help
}

func TestLoadReleaseHelpPagesExamplesAndSearchAreManifestBound(t *testing.T) {
	t.Parallel()
	files, help := helpReleaseFixture(t)
	release, err := loadRelease(files, releaseFixtureIdentity)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range help {
		got, err := fs.ReadFile(release.WebUI, name)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("bundled Help asset %q = %q, %v; want %q", name, got, err, want)
		}
		t.Run(name, func(t *testing.T) {
			for _, mutation := range []string{"modified", "missing"} {
				t.Run(mutation, func(t *testing.T) {
					changed, _ := helpReleaseFixture(t)
					if mutation == "missing" {
						delete(changed, "out/"+name)
					} else {
						changed["out/"+name] = &fstest.MapFile{Data: []byte("stale documentation")}
					}
					if _, err := loadRelease(changed, releaseFixtureIdentity); err == nil {
						t.Fatalf("accepted %s Help asset %q", mutation, name)
					}
				})
			}
		})
	}
}
