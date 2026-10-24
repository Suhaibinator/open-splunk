package control_test

import (
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/migrations"
)

// postgresTriggerLanes is every trigger lane of the PostgreSQL port. Each
// lane ships exactly one NNNN_<lane>_triggers.sql migration, and a fixture
// scenario's depends_on_lanes may name only these. Every lane is merged, so
// TestPostgresTriggerLanesAreAllEmbedded pins this list to the embedded set
// and a scenario can no longer legitimately wait for an absent lane.
var postgresTriggerLanes = []string{
	"audit_1",
	"audit_2",
	"collectors_apps_search_1",
	"collectors_apps_search_2",
	"core_ingest_1",
	"core_ingest_2",
	"core_ingest_3",
	"knowledge_catalog_1",
	"knowledge_catalog_2",
	"knowledge_objects_1",
	"knowledge_objects_2",
	"lookups",
}

var postgresTriggerLaneMigration = regexp.MustCompile(`^[0-9]{4}_([a-z0-9_]+)_triggers\.sql$`)

// postgresEmbeddedTriggerLanes returns the lanes whose trigger migration the
// shipped PostgreSQL set embeds.
func postgresEmbeddedTriggerLanes(t testing.TB) map[string]bool {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	lanes := make(map[string]bool)
	for _, entry := range entries {
		if matches := postgresTriggerLaneMigration.FindStringSubmatch(entry.Name()); matches != nil {
			if lanes[matches[1]] {
				t.Fatalf("embedded PostgreSQL migrations hold two trigger files for lane %s", matches[1])
			}
			lanes[matches[1]] = true
		}
	}
	return lanes
}

// checkPostgresTriggerLaneDependencies reports why a scenario of lane owner
// may not depend on lanes: a lane that is not a trigger lane (a misspelling
// would otherwise leave the scenario waiting forever), owner itself, or a
// repeated lane.
func checkPostgresTriggerLaneDependencies(owner string, lanes []string) error {
	var problems []string
	for index, lane := range lanes {
		switch {
		case lane == owner:
			problems = append(problems, fmt.Sprintf("names its own lane %q", lane))
		case !slices.Contains(postgresTriggerLanes, lane):
			problems = append(problems, fmt.Sprintf("names %q, which is not a trigger lane", lane))
		case slices.Contains(lanes[:index], lane):
			problems = append(problems, fmt.Sprintf("repeats lane %q", lane))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("depends_on_lanes %s", strings.Join(problems, "; "))
}

// requirePostgresTriggerLanesEmbedded fails t unless every lane is in the
// embedded migration set. With every lane merged, a scenario that would wait
// for a lane is a fixture error, never a pending case to log.
func requirePostgresTriggerLanesEmbedded(t testing.TB, embedded map[string]bool, lanes []string) {
	t.Helper()
	var missing []string
	for _, lane := range lanes {
		if !embedded[lane] {
			missing = append(missing, lane)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("the embedded PostgreSQL migrations carry no trigger file for lane(s) %s", strings.Join(missing, ", "))
	}
}

// TestPostgresTriggerLanesAreAllEmbedded pins postgresTriggerLanes to the
// shipped migration set, so a lane cannot be dropped, renamed or added on one
// side only.
func TestPostgresTriggerLanesAreAllEmbedded(t *testing.T) {
	t.Parallel()
	embedded := postgresEmbeddedTriggerLanes(t)
	for _, lane := range postgresTriggerLanes {
		if !embedded[lane] {
			t.Errorf("trigger lane %s has no NNNN_%s_triggers.sql in the embedded migrations", lane, lane)
		}
	}
	for lane := range embedded {
		if !slices.Contains(postgresTriggerLanes, lane) {
			t.Errorf("embedded trigger file for lane %s is missing from postgresTriggerLanes", lane)
		}
	}
}

// TestPostgresTriggerLaneDependenciesRejectUnknownLanes guards the shared
// depends_on_lanes check: the misspellings below once left scenarios with a
// wrong expectation logged as "waiting" while the suite passed.
func TestPostgresTriggerLaneDependenciesRejectUnknownLanes(t *testing.T) {
	t.Parallel()
	if err := checkPostgresTriggerLaneDependencies("collectors_apps_search_2",
		[]string{"collectors_apps_search_1", "knowledge_catalog_2"}); err != nil {
		t.Fatalf("valid dependencies rejected: %v", err)
	}
	for _, test := range []struct {
		owner string
		lanes []string
		want  string
	}{
		{"collectors_apps_search_2", []string{"collectors_apps_search_one"}, `"collectors_apps_search_one", which is not a trigger lane`},
		{"collectors_apps_search_2", []string{"knowledge_catalog_2x"}, `"knowledge_catalog_2x", which is not a trigger lane`},
		{"knowledge_objects_1", []string{"knowledge_objects_two"}, `"knowledge_objects_two", which is not a trigger lane`},
		{"core_ingest_2", []string{"core_ingest_2"}, `names its own lane "core_ingest_2"`},
		{"core_ingest_2", []string{"core_ingest_1", "core_ingest_1"}, `repeats lane "core_ingest_1"`},
	} {
		err := checkPostgresTriggerLaneDependencies(test.owner, test.lanes)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("checkPostgresTriggerLaneDependencies(%s, %v) = %v, want an error containing %s",
				test.owner, test.lanes, err, test.want)
		}
	}
}

// TestPostgresTriggerLanesEmbeddedRequirementFails proves the requirement
// fails, rather than logs, on a lane the migrations do not carry.
func TestPostgresTriggerLanesEmbeddedRequirementFails(t *testing.T) {
	t.Parallel()
	message := recordBreakHelperFatal(t, func(tb testing.TB) {
		requirePostgresTriggerLanesEmbedded(tb, map[string]bool{"audit_1": true}, []string{"audit_1", "audit_2"})
	})
	if !strings.Contains(message, "lane(s) audit_2") {
		t.Fatalf("missing lane recorded %q, want a failure naming audit_2", message)
	}
	message = recordBreakHelperFatal(t, func(tb testing.TB) {
		requirePostgresTriggerLanesEmbedded(tb, map[string]bool{"audit_1": true}, []string{"audit_1"})
	})
	if message != "" {
		t.Fatalf("embedded lanes failed with %q", message)
	}
}
