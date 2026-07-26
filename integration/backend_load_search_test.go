//go:build !windows

package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

type backendLoadSearchObservation struct {
	WallTime     time.Duration
	Elapsed      time.Duration
	QueueWait    time.Duration
	ScannedRows  uint64
	ScannedBytes uint64
}

func runBackendLoadSearch(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	plan backendLoadPlan,
	fixtureStart time.Time,
	source backendLoadSourceCorpus,
) backendLoadSearchObservation {
	t.Helper()
	spl := fmt.Sprintf(
		`index=%q | stats count AS events dc(event_id) AS event_ids dc(request_id) AS request_ids dc(user_id) AS user_ids`,
		plan.IndexName,
	)
	earliest := fixtureStart.Format(time.RFC3339Nano)
	latestTime := fixtureStart.Add(time.Duration(plan.eventCount()) * plan.interval())
	latest := latestTime.Format(time.RFC3339Nano)
	timezone := "UTC"
	started := time.Now()
	var created opensplunkv1.CreateSearchJobResponse
	postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/create", &opensplunkv1.CreateSearchJobRequest{
		Definition: &opensplunkv1.SearchDefinition{
			Spl: spl,
			TimeRange: &opensplunkv1.TimeRangeSpec{
				Earliest: &earliest,
				Latest:   &latest,
				Timezone: &timezone,
			},
			IndexScope: []string{plan.IndexName},
		},
	}, &created)
	jobID := created.GetSearchJob().GetSearchJobId()
	if jobID == "" {
		t.Fatalf("created backend load search = %+v", created.GetSearchJob())
	}
	completed := waitForCompletedSearch(t, ctx, client, baseURL, jobID, 60*time.Second)
	wallTime := time.Since(started)
	progress := completed.GetProgress()
	resolved := completed.GetResolvedTimeRange()
	if completed.GetDefinition().GetSpl() != spl ||
		completed.GetResultsTruncated() ||
		progress.GetProducedRows() != 1 ||
		progress.GetScannedRows() == 0 ||
		progress.GetScannedBytes() == 0 ||
		progress.GetCountersAreEstimates() ||
		!slices.Equal(completed.GetEffectiveIndexScope(), []string{plan.IndexName}) ||
		resolved == nil ||
		resolved.GetEarliest() == nil ||
		resolved.GetLatest() == nil ||
		!resolved.GetEarliest().AsTime().Equal(fixtureStart) ||
		!resolved.GetLatest().AsTime().Equal(latestTime) ||
		resolved.GetTimezone() != timezone {
		t.Fatalf("completed backend load search = %+v", completed)
	}

	results := fetchAllCompletedSearchResults(t, ctx, client, baseURL, jobID, 1, 1)
	wantNames := []string{"events", "event_ids", "request_ids", "user_ids"}
	columns := results.schema.GetColumns()
	if results.schema.GetResultKind() != opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS ||
		len(columns) != len(wantNames) ||
		len(results.rows) != 1 ||
		len(results.rows[0].GetCells()) != len(wantNames) {
		t.Fatalf("backend load aggregate result = %+v rows=%+v", results.schema, results.rows)
	}
	for index, name := range wantNames {
		if columns[index].GetFieldName() != name ||
			columns[index].GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_UINT64 ||
			columns[index].GetNullable() ||
			columns[index].GetMultivalue() {
			t.Fatalf("backend load aggregate column %d = %+v, want UInt64 %q", index, columns[index], name)
		}
	}
	wantValues := []uint64{
		plan.eventCount(),
		plan.eventCount(),
		uint64(len(source.RequestIDs)),
		uint64(len(source.UserIDs)),
	}
	for index, want := range wantValues {
		value := results.rows[0].GetCells()[index]
		if _, ok := value.GetKind().(*opensplunkv1.TypedValue_Uint64Value); !ok ||
			value.GetUint64Value() != want {
			t.Fatalf("backend load aggregate cell %q = %+v, want %d", wantNames[index], value, want)
		}
	}

	return backendLoadSearchObservation{
		WallTime:     wallTime,
		Elapsed:      validBackendLoadDuration(t, "elapsed", progress.GetElapsed()),
		QueueWait:    validBackendLoadDuration(t, "queue wait", progress.GetQueueWait()),
		ScannedRows:  progress.GetScannedRows(),
		ScannedBytes: progress.GetScannedBytes(),
	}
}

func validBackendLoadDuration(t *testing.T, name string, value *durationpb.Duration) time.Duration {
	t.Helper()
	if value == nil {
		t.Fatalf("backend load search %s is absent", name)
	}
	if err := value.CheckValid(); err != nil {
		t.Fatalf("backend load search %s is invalid: %v", name, err)
	}
	duration := value.AsDuration()
	if duration < 0 {
		t.Fatalf("backend load search %s = %s", name, duration)
	}
	return duration
}
