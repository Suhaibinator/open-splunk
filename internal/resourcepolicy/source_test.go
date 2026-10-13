package resourcepolicy

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func TestSourceNamesIncludesAuthoredSelectorsAndDefaults(t *testing.T) {
	app := "app-one"
	for _, test := range []struct {
		spl      string
		scope    []string
		app      *string
		expected []string
	}{
		{"index=secret | search index=audit", []string{"main"}, nil, []string{"audit", "main", "secret"}},
		{"index=main | where index=\"secret\" OR index IN (\"audit\",\"archive*\")", []string{"main"}, nil, []string{"archive*", "audit", "main", "secret"}},
		{"index=main", nil, &app, []string{AppDefaultsSource, "main"}},
		{"index=main | not_a_command", nil, nil, []string{UnresolvedSource}},
	} {
		encoded, err := SourceNames(&opensplunk.SearchDefinition{Spl: test.spl, IndexScope: test.scope, AppId: test.app})
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		if err := json.Unmarshal(encoded, &names); err != nil || !slices.Equal(names, test.expected) {
			t.Fatalf("%s: %s, %v; want %v", test.spl, encoded, err, test.expected)
		}
	}
}

func TestSourceVisibilityHandlesDeniedWildcardsAndAppDefaults(t *testing.T) {
	snapshot := Snapshot{IndexNames: []string{"main"}, AvailableIndexNames: []string{"main", "secret"}, AppIDs: []string{"app"}, AppDefaultIndexNames: map[string][]string{"app": {"main", "secret"}}}
	for _, sources := range []string{`["secret"]`, `["*"]`, `["kvstore_unresolved_source"]`, `["kvstore_app_defaults_source"]`} {
		if AllowsSource(snapshot, []byte(sources), "app") {
			t.Fatalf("inaccessible source allowed: %s", sources)
		}
	}
	if !AllowsSource(snapshot, []byte(`["main"]`), "app") {
		t.Fatal("authorized source denied")
	}
	snapshot.SiteAdministrator = true
	if !AllowsSource(snapshot, []byte(`["secret"]`), "unknown") {
		t.Fatal("site administrator must bypass source denies")
	}
}

func TestHistorySourceUnionSupportsWideEffectiveCatalogAndRemainsBounded(t *testing.T) {
	indexes := make([]string, 1024)
	for i := range indexes {
		indexes[i] = fmt.Sprintf("index_%04d", i)
	}
	encoded, err := SourceNames(&opensplunk.SearchDefinition{Spl: "index=*"}, indexes...)
	if err != nil {
		t.Fatalf("valid full catalog source scope: %v", err)
	}
	var sources []string
	if err := json.Unmarshal(encoded, &sources); err != nil || len(sources) != 1025 {
		t.Fatalf("wide source union: %d %v", len(sources), err)
	}
	tooMany := make([]string, MaximumSourceNames+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("index_%04d", i)
	}
	if _, err := SourceNames(nil, tooMany...); err == nil {
		t.Fatal("unbounded source names accepted")
	}
}
