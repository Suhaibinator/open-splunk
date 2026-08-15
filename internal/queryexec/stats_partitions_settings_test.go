package queryexec

import (
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestStatsPartitionsMaxThreadsHintChangesExecutorSettings(t *testing.T) {
	t.Parallel()

	indexTime := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	compile := func(t *testing.T, source string) clickhouse.CompiledQuery {
		t.Helper()
		return queryIntegrationCompileSearchRange(
			t,
			source,
			indexTime,
			indexTime.Add(-time.Hour),
			indexTime,
		)
	}

	queries := map[string]clickhouse.CompiledQuery{
		"no stats": compile(t, `index=main | where status=200`),
		"default":  compile(t, `index=main | stats count`),
		"two":      compile(t, `index=main | stats partitions=2 count`),
		"maximum":  compile(t, `index=main | stats partitions=100 count`),
		"multiple": compile(t, `index=main | stats partitions=4 count AS events | stats partitions=2 sum(events) AS total`),
	}

	tests := []struct {
		name       string
		maxThreads uint64
		query      string
		want       uint64
	}{
		{name: "ordinary query unchanged", query: "no stats", want: 4},
		{name: "default stats reduces to one", query: "default", want: 1},
		{name: "explicit two has observable effect", query: "two", want: 2},
		{name: "partitions limit cannot exceed four", query: "maximum", want: 4},
		{name: "multiple stages use minimum", query: "multiple", want: 2},
		{name: "hint never raises stricter base", maxThreads: 1, query: "maximum", want: 1},
		{name: "stats hint caps permissive base at four", maxThreads: 8, query: "maximum", want: 4},
		{name: "ordinary query preserves permissive base", maxThreads: 8, query: "no stats", want: 8},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			settings, err := querySettings(Config{MaxThreads: test.maxThreads})
			if err != nil {
				t.Fatalf("querySettings: %v", err)
			}
			base := settings["max_threads"]
			executor := &Executor{settings: mustValidatedSettings(t, settings)}
			got := executor.settingsFor(queries[test.query])["max_threads"]
			if got != test.want {
				t.Fatalf("max_threads = %#v, want %d", got, test.want)
			}
			if settings["max_threads"] != base {
				t.Fatalf("base max_threads mutated from %#v to %#v", base, settings["max_threads"])
			}
		})
	}
}
