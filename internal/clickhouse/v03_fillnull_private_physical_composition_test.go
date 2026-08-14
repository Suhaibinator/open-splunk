package clickhouse

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestCompileV03FillNullBindsPrivatePhysicalColumns protects the
// logical-to-physical boundary after commands whose public field is backed by
// a compiler-private column. fillnull must bind that real source column and
// append the authored column's first public materialization; REPLACE is valid
// only when the authored column already exists physically.
func TestCompileV03FillNullBindsPrivatePhysicalColumns(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name         string
		source       string
		physical     string
		publicName   string
		appendPublic bool
		fixedString  bool
	}{
		{
			name: "dynamic stats BY group",
			source: `index=gradethis | stats count BY optional` +
				` | fillnull value="fallback" optional | table optional count`,
			physical: `"__os_group_0"`, publicName: `"optional"`, appendPublic: true, fixedString: true,
		},
		{
			name: "fixed stats BY group",
			source: `index=gradethis | stats count BY level` +
				` | fillnull value="fallback" level | table level count`,
			physical: `"__os_group_0"`, publicName: `"level"`, appendPublic: true, fixedString: true,
		},
		{
			name: "projected stats BY group",
			source: `index=gradethis | stats count BY level | fields level count` +
				` | fillnull value="fallback" level | table level count`,
			physical: `"level"`, publicName: `"level"`, appendPublic: false, fixedString: true,
		},
		{
			name: "fixed Time stats BY group",
			source: `index=gradethis | stats count BY _time` +
				` | fillnull value="fallback" _time | table _time count`,
			physical: `"__os_group_0"`, publicName: `"_time"`, appendPublic: true,
		},
		{
			name: "fixed Number stats BY group",
			source: `index=gradethis | eval n=1 | stats count BY n` +
				` | fillnull value="fallback" n | table n count`,
			physical: `"__os_group_0"`, publicName: `"n"`, appendPublic: true,
		},
		{
			name: "fixed Bool stats BY group",
			source: `index=gradethis | eval flag=true | stats count BY flag` +
				` | fillnull value="fallback" flag | table flag count`,
			physical: `"__os_group_0"`, publicName: `"flag"`, appendPublic: true,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if !compiled.HasValidExecutionSeal() {
				t.Fatal("fillnull private-column composition lacks an execution seal")
			}
			if test.fixedString {
				if !strings.Contains(compiled.SQL, `tuple("_stage_`) ||
					!strings.Contains(compiled.SQL, `.`+test.physical+`,`) {
					t.Fatalf("fillnull did not bind physical source %s:\n%s", test.physical, compiled.SQL)
				}
			} else if !strings.Contains(compiled.SQL,
				`isNotNull(`+test.physical+`), CAST(`+test.physical+` AS Dynamic)`) {
				t.Fatalf("fillnull did not consume non-String physical source %s:\n%s", test.physical, compiled.SQL)
			}
			if test.appendPublic {
				terminator := ` AS ` + test.publicName + ` FROM (`
				if test.fixedString {
					terminator = ` AS ` + test.publicName + `,`
				}
				if !strings.Contains(compiled.SQL, `SELECT *, if(`) ||
					!strings.Contains(compiled.SQL, terminator) {
					t.Fatalf("fillnull did not append first public %s materialization:\n%s", test.publicName, compiled.SQL)
				}
			} else if !strings.Contains(compiled.SQL, `SELECT * REPLACE (if(`) {
				t.Fatalf("fillnull did not replace physical public %s:\n%s", test.publicName, compiled.SQL)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d", got, want)
			}
		})
	}
}

// TestV03FillNullAfterPrivatePhysicalProducersAgainstClickHouse covers real
// private aggregate outputs, public values()/mode output, eventstats and
// streamstats publications, and direct/fields/table barriers on the pinned
// ClickHouse engine.
func TestV03FillNullAfterPrivatePhysicalProducersAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	eventAnchor := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	newEvent := func(id, group string, ordinal int) *ingest.StoredEvent {
		event := testStoredEvent(id, "spl-v03-fillnull-private", indexTime)
		event.Event.Source = "v03-fillnull-private"
		event.Event.Level = stringPointer(group)
		event.Event.EventTime = timestamppb.New(eventAnchor.Add(time.Duration(ordinal) * time.Second))
		event.Event.CollectedAt = timestamppb.New(event.Event.EventTime.AsTime().Add(-time.Second))
		event.Event.Fields = typedObjectValue(typedField("group", typedString(group)))
		return event
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx, t, store, indexTime, "spl-v03-fillnull-private", "v03-fillnull-private", 1,
		newEvent("one", "alpha", 1), newEvent("two", "beta", 2), newEvent("three", "alpha", 3),
	)
	base := `index=spl-v03-fillnull-private source="v03-fillnull-private"`
	tests := []struct {
		name   string
		source string
		fields []string
		want   [][]string
	}{
		{
			name: "stats dynamic BY direct",
			source: base + ` | stats count BY group | fillnull value="fallback" group` +
				` | sort 0 +group | table group count`,
			fields: []string{"group", "count"}, want: [][]string{{"alpha", "2"}, {"beta", "1"}},
		},
		{
			name: "stats fixed BY fields barrier",
			source: base + ` | stats count BY level | fields level count` +
				` | fillnull value="fallback" level | sort 0 +level | table level count`,
			fields: []string{"level", "count"}, want: [][]string{{"alpha", "2"}, {"beta", "1"}},
		},
		{
			name: "stats fixed BY table barrier",
			source: base + ` | stats count BY level | table level count` +
				` | fillnull value="fallback" level | sort 0 +level | table level count`,
			fields: []string{"level", "count"}, want: [][]string{{"alpha", "2"}, {"beta", "1"}},
		},
		{
			name: "stats mode output",
			source: base + ` | stats mode(level) AS winner | fillnull value="fallback" winner` +
				` | table winner`,
			fields: []string{"winner"}, want: [][]string{{"alpha"}},
		},
		{
			name: "stats values output",
			source: base + ` | stats values(level) AS winners | fillnull value="fallback" winners` +
				` | table winners`,
			fields: []string{"winners"}, want: [][]string{{"[alpha,beta]"}},
		},
		{
			name: "eventstats output",
			source: base + ` | sort 0 +event_id | eventstats earliest(level) AS first_level` +
				` | fillnull value="fallback" first_level | table event_id first_level`,
			fields: []string{"event_id", "first_level"},
			want:   [][]string{{"one", "alpha"}, {"three", "alpha"}, {"two", "alpha"}},
		},
		{
			name: "streamstats output",
			source: base + ` | sort 0 +event_id | streamstats earliest(level) AS first_level` +
				` | fillnull value="fallback" first_level | table event_id first_level`,
			fields: []string{"event_id", "first_level"},
			want:   [][]string{{"one", "alpha"}, {"three", "alpha"}, {"two", "alpha"}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			compiled := compile(test.source)
			v03AssertJSONRows(t, queryContext, connection, compiled, test.fields, test.want)
		})
	}

	t.Run("fixed Time stats BY direct", func(t *testing.T) {
		compiled := compile(base + ` event_id="one" | stats count BY _time` +
			` | fillnull value="fallback" _time | table _time count`)
		rows := v03JSONRows(t, queryContext, connection, compiled, []string{"_time", "count"})
		if len(rows) != 1 || len(rows[0]) != 2 || rows[0][0] == nil || v03JSONText(rows[0][1]) != "1" {
			t.Fatalf("fillnull private Time rows = %#v, want one present Time and count 1", rows)
		}
	})

	for _, test := range []struct {
		name, assignment, field, want string
	}{
		{name: "fixed Number stats BY direct", assignment: "n=1", field: "n", want: "1"},
		{name: "fixed Bool stats BY direct", assignment: "flag=true", field: "flag", want: "true"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			compiled := compile(base + ` | eval ` + test.assignment + ` | stats count BY ` +
				test.field + ` | fillnull value="fallback" ` + test.field +
				` | table ` + test.field + ` count`)
			v03AssertJSONRows(
				t, queryContext, connection, compiled,
				[]string{test.field, "count"}, [][]string{{test.want, "3"}},
			)
		})
	}
}
