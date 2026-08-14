package server

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// v03CompilingSearchJobs is the real parser/planner/compiler seam behind the
// HTTP product routes. It deliberately stops before storage execution, while
// still proving that the exact trusted SPL reconstructed by saved/history
// handling reaches a sealed all-ten physical program.
type v03CompilingSearchJobs struct {
	*fakeSearchJobs
	requests []searchjobs.CreateRequest
	queries  []clickhouse.CompiledQuery
}

func (jobs *v03CompilingSearchJobs) Create(
	_ context.Context,
	request searchjobs.CreateRequest,
) (searchjobs.Job, error) {
	parsed, err := spl.Parse(request.SPL)
	if err != nil {
		return searchjobs.Job{}, err
	}
	visibility := uint64(9)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          request.TenantID,
		AuthorizedIndexes: request.AuthorizedIndexes,
		SearchJobID:       "job-v03-api-compile",
		Earliest:          request.TimeRange.Earliest(),
		Latest:            request.TimeRange.Latest(),
		SearchStart:       time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   time.Date(2026, 8, 12, 2, 0, 0, 0, time.UTC),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		return searchjobs.Job{}, err
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		return searchjobs.Job{}, err
	}
	jobs.requests = append(jobs.requests, request)
	jobs.queries = append(jobs.queries, compiled)
	return jobs.fakeSearchJobs.Create(context.Background(), request)
}

func requireV03APICompiledAllTen(t *testing.T, compiled clickhouse.CompiledQuery) {
	t.Helper()
	wantFields := []string{"event_id", "tags", "info_sid"}
	if !compiled.HasValidExecutionSeal() || !compiled.RequiresAtomicResult() ||
		!reflect.DeepEqual(compiled.OutputFields, wantFields) {
		t.Fatalf("API all-ten compile = sealed %t atomic %t fields %v", compiled.HasValidExecutionSeal(), compiled.RequiresAtomicResult(), compiled.OutputFields)
	}
	for _, field := range compiled.OutputFields {
		if strings.HasPrefix(strings.ToLower(field), "__os_") {
			t.Fatalf("API all-ten compile leaked private field %q", field)
		}
	}
}
