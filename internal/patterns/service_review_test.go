package patterns

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func newReviewService(t *testing.T, config Config) *Service {
	t.Helper()
	if config.CursorKey == nil {
		config.CursorKey = []byte("pattern-adversarial-review-cursor-key")
	}
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	return service
}

func reviewRows() *testSource {
	rows := make([]searchjobs.ResultRow, 7)
	for index, raw := range []string{"z 1", "a 2", "z 3", "b 4", "a 5", "z 6", "b 7"} {
		rows[index] = searchjobs.ResultRow{Ordinal: uint64(index), Values: []searchjobs.Value{searchjobs.StringValue(raw), searchjobs.UnsignedValue(uint64(index))}}
	}
	return &testSource{generation: 7, schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindString}, {Name: "number", Kind: searchjobs.ValueKindUnsigned}}}, rows: rows}
}

func TestServiceReviewAllGroupsAndMembersConserveTheFullRelation(t *testing.T) {
	source := reviewRows()
	service := newReviewService(t, Config{Source: source, MaximumCacheBytes: 1}) // Every page must also work without cache admission.
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	query := ListRequest{SearchJobID: "job", Generation: 7, Sensitivity: Balanced, PageSize: 1, IncludeTotal: true}
	var signatures []string
	var observed []uint64
	var grouped uint64
	for {
		page, err := service.List(context.Background(), access, query)
		if err != nil {
			t.Fatal(err)
		}
		if page.TotalSize == nil || *page.TotalSize != 3 || page.EligibleEventCount != 7 || page.ExcludedEventCount != 0 || page.RetainedEventCount != 7 {
			t.Fatalf("coverage = %+v", page)
		}
		groups := slices.Clone(page.Patterns)
		query.PageToken = page.NextPageToken
		page.Close()
		for _, group := range groups {
			signatures = append(signatures, group.Signature)
			grouped += group.EventCount
			membersQuery := MemberRequest{SearchJobID: "job", Generation: 7, Sensitivity: Balanced, PatternID: group.ID, PageSize: 1, IncludeTotal: true, Columns: []string{"number", "_raw"}}
			var count uint64
			for {
				members, err := service.Members(context.Background(), access, membersQuery)
				if err != nil {
					t.Fatal(err)
				}
				if members.TotalSize == nil || *members.TotalSize != group.EventCount || members.Schema.Columns[0].Name != "number" {
					t.Fatalf("member metadata = %+v", members)
				}
				for _, row := range members.Rows {
					value, ok := row.Values[0].Unsigned()
					if !ok || value != row.Ordinal {
						t.Fatalf("projected member changed: %+v", row)
					}
					observed = append(observed, row.Ordinal)
					count++
				}
				membersQuery.PageToken = members.NextPageToken
				members.Close()
				if membersQuery.PageToken == "" {
					break
				}
			}
			if count != group.EventCount {
				t.Fatalf("member count=%d want%d", count, group.EventCount)
			}
		}
		if query.PageToken == "" {
			break
		}
	}
	if !reflect.DeepEqual(signatures, []string{"z <int>", "a <int>", "b <int>"}) {
		t.Fatalf("group order=%v", signatures)
	}
	slices.Sort(observed)
	if grouped != 7 || !reflect.DeepEqual(observed, []uint64{0, 1, 2, 3, 4, 5, 6}) {
		t.Fatalf("group/member conservation=%d / %v", grouped, observed)
	}
}

func TestServiceReviewCursorsBindScopeGenerationSensitivityAndProjection(t *testing.T) {
	service := newReviewService(t, Config{Source: reviewRows()})
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	query := ListRequest{SearchJobID: "job", Generation: 7, Sensitivity: Balanced, PageSize: 1}
	first, err := service.List(context.Background(), access, query)
	if err != nil {
		t.Fatal(err)
	}
	query.PageToken = first.NextPageToken
	patternID := first.Patterns[0].ID
	first.Close()
	for name, mutate := range map[string]func(*searchjobs.AccessScope, *ListRequest){
		"job":         func(_ *searchjobs.AccessScope, q *ListRequest) { q.SearchJobID = "other" },
		"owner":       func(a *searchjobs.AccessScope, _ *ListRequest) { a.OwnerID = "other" },
		"tenant":      func(a *searchjobs.AccessScope, _ *ListRequest) { a.TenantID = "other" },
		"sensitivity": func(_ *searchjobs.AccessScope, q *ListRequest) { q.Sensitivity = Precise },
		"generation":  func(_ *searchjobs.AccessScope, q *ListRequest) { q.Generation++ },
		"tampered":    func(_ *searchjobs.AccessScope, q *ListRequest) { q.PageToken = "x" + q.PageToken },
	} {
		t.Run(name, func(t *testing.T) {
			changedAccess, changedQuery := access, query
			mutate(&changedAccess, &changedQuery)
			result, err := service.List(context.Background(), changedAccess, changedQuery)
			defer result.Close()
			if !errors.Is(err, ErrInvalidCursor) && !errors.Is(err, ErrGenerationMismatch) {
				t.Fatalf("cross-context cursor error=%v", err)
			}
		})
	}
	membersQuery := MemberRequest{SearchJobID: "job", Generation: 7, Sensitivity: Balanced, PatternID: patternID, PageSize: 1, Columns: []string{"_raw", "number"}}
	members, err := service.Members(context.Background(), access, membersQuery)
	if err != nil {
		t.Fatal(err)
	}
	membersQuery.PageToken = members.NextPageToken
	members.Close()
	membersQuery.Columns = []string{"number", "_raw"}
	changed, err := service.Members(context.Background(), access, membersQuery)
	defer changed.Close()
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("projection-replayed cursor error=%v", err)
	}
}

type failingReviewSource struct {
	source     *testSource
	acquireErr error
	failAt     int
}

func (source *failingReviewSource) Acquire(ctx context.Context, access searchjobs.AccessScope, job string) (searchjobs.ResultLease, error) {
	if source.acquireErr != nil {
		return nil, source.acquireErr
	}
	lease, err := source.source.Acquire(ctx, access, job)
	if err != nil {
		return nil, err
	}
	return &failingReviewLease{ResultLease: lease, failAt: source.failAt}, nil
}

type failingReviewLease struct {
	searchjobs.ResultLease
	calls, failAt int
}

func (lease *failingReviewLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	lease.calls++
	if lease.failAt > 0 && lease.calls == lease.failAt {
		return searchjobs.ResultRow{}, false, searchjobs.ErrResultsUnavailable
	}
	return lease.ResultLease.Next(ctx)
}

func TestServiceReviewScanErrorsNeverPublishPrefixOrCachedDeletedArtifact(t *testing.T) {
	source := &failingReviewSource{source: reviewRows(), failAt: 3}
	service := newReviewService(t, Config{Source: source})
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	query := ListRequest{SearchJobID: "job", Generation: 7, Sensitivity: Balanced}
	failed, err := service.List(context.Background(), access, query)
	failed.Close()
	if !errors.Is(err, searchjobs.ErrResultsUnavailable) || len(failed.Patterns) != 0 {
		t.Fatalf("partial relation published: %+v / %v", failed, err)
	}
	source.failAt = 0
	complete, err := service.List(context.Background(), access, query)
	if err != nil || complete.EligibleEventCount != 7 {
		t.Fatalf("retry reused prefix: %+v / %v", complete, err)
	}
	complete.Close()
	source.acquireErr = searchjobs.ErrNotFound
	deleted, err := service.List(context.Background(), access, query)
	deleted.Close()
	if !errors.Is(err, searchjobs.ErrNotFound) || len(deleted.Patterns) != 0 {
		t.Fatalf("cache bypassed source deletion: %+v / %v", deleted, err)
	}
}

func TestServiceReviewInputAndGroupLimitsAreAtomic(t *testing.T) {
	for name, config := range map[string]Config{
		"rows": {MaximumRows: 2}, "groups": {MaximumGroups: 2}, "all-cell-input": {MaximumInputBytes: 512},
	} {
		t.Run(name, func(t *testing.T) {
			config.Source = reviewRows()
			service := newReviewService(t, config)
			result, err := service.List(context.Background(), searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}, ListRequest{SearchJobID: "job", Generation: 7, Sensitivity: Balanced})
			defer result.Close()
			if !errors.Is(err, ErrLimit) || len(result.Patterns) != 0 {
				t.Fatalf("bound published partial relation: %+v / %v", result, err)
			}
		})
	}
}

func TestServiceReviewExportsIncludeEverySummaryAndMemberBeyondThePage(t *testing.T) {
	service := newReviewService(t, Config{Source: reviewRows(), MaximumPageSize: 1})
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	query := ListRequest{SearchJobID: "job", Generation: 7, Sensitivity: Balanced}
	first, err := service.List(context.Background(), access, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Patterns) != 1 || first.NextPageToken == "" {
		t.Fatal("fixture must expose only one group page")
	}
	selected := first.Patterns[0].ID
	first.Close()
	request := ExportRequest{SearchJobID: "job", Generation: 7, SnapshotRef: "snapshot", Sensitivity: Balanced}
	summary, err := service.AcquirePatternSummary(context.Background(), access, request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = summary.Close() }()
	var signatures []string
	var count uint64
	for {
		row, ok, err := summary.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		signature, ok := row.Values[0].String()
		if !ok {
			t.Fatal("summary signature lost string type")
		}
		value, ok := row.Values[1].Unsigned()
		if !ok {
			t.Fatal("summary count lost unsigned type")
		}
		signatures = append(signatures, signature)
		count += value
	}
	if summary.RowCount() != 3 || count != 7 || !reflect.DeepEqual(signatures, []string{"z <int>", "a <int>", "b <int>"}) {
		t.Fatalf("summary export was paged or reordered: count=%d signatures=%v", count, signatures)
	}
	request.PatternID = selected
	members, err := service.AcquirePatternMembers(context.Background(), access, request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = members.Close() }()
	var ordinals []uint64
	for {
		row, ok, err := members.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		value, ok := row.Values[1].Unsigned()
		if !ok || value != row.Ordinal {
			t.Fatal("member export changed original types/order")
		}
		ordinals = append(ordinals, row.Ordinal)
	}
	if members.RowCount() != 3 || !reflect.DeepEqual(ordinals, []uint64{0, 2, 5}) {
		t.Fatalf("member export was paged or overinclusive: %v", ordinals)
	}
}
