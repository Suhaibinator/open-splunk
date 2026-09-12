package patterns

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestServiceGroupsFinalRawStringsAndConservesCounts(t *testing.T) {
	t.Parallel()

	rows := []searchjobs.ResultRow{
		{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("request id=0123456789abcdef status=200 cost=1.25 literal=* <int>")}},
		{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue("request\tid=fedcba9876543210 status=201 cost=1.25 literal=* <int>")}},
		{Ordinal: 2, Values: []searchjobs.Value{searchjobs.NullValue()}},
		{Ordinal: 3, Values: []searchjobs.Value{searchjobs.SignedValue(12)}},
		{Ordinal: 4, Values: []searchjobs.Value{searchjobs.StringValue(" request id=0123456789abcdef status=200 cost=1.25 literal=* <int>  ")}},
	}
	source := &testSource{schema: searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_raw", Kind: searchjobs.ValueKindMixed, Nullable: true},
	}}, rows: rows, generation: 7, truncated: true}
	service, err := New(Config{Source: source, CursorKey: []byte("patterns-test-cursor-key-32-bytes!")})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(func() { _ = service.Close(context.Background()) })

	result, err := service.List(context.Background(), searchjobs.AccessScope{
		TenantID: "tenant", OwnerID: "owner",
	}, ListRequest{
		SearchJobID: "job", Generation: 7, Sensitivity: Balanced,
		PageSize: 10, IncludeTotal: true,
	})
	if err != nil {
		t.Fatalf("List(): %v", err)
	}
	defer result.Close()
	if result.RetainedEventCount != 5 || result.EligibleEventCount != 3 || result.ExcludedEventCount != 2 {
		t.Fatalf("counts = retained %d eligible %d excluded %d", result.RetainedEventCount, result.EligibleEventCount, result.ExcludedEventCount)
	}
	if !result.RetainedTruncated || result.SnapshotComplete {
		t.Fatalf("completeness = truncated %t complete %t", result.RetainedTruncated, result.SnapshotComplete)
	}
	if len(result.Patterns) != 1 || result.Patterns[0].EventCount != 3 {
		t.Fatalf("patterns = %+v, want one three-event group", result.Patterns)
	}
	const wantSignature = `request id=<hex> status=<int> cost=1.25 literal=* \<int>`
	if result.Patterns[0].Signature != wantSignature {
		t.Fatalf("signature = %q, want %q", result.Patterns[0].Signature, wantSignature)
	}
	if result.TotalSize == nil || *result.TotalSize != 1 || !result.TotalSizeExact {
		t.Fatalf("total = %v exact=%t", result.TotalSize, result.TotalSizeExact)
	}

	members, err := service.Members(context.Background(), searchjobs.AccessScope{
		TenantID: "tenant", OwnerID: "owner",
	}, MemberRequest{
		SearchJobID: "job", Generation: 7, Sensitivity: Balanced,
		PatternID: result.Patterns[0].ID, PageSize: 2,
	})
	if err != nil {
		t.Fatalf("Members(): %v", err)
	}
	defer members.Close()
	if len(members.Rows) != 2 || members.Rows[0].Ordinal != 0 || members.Rows[1].Ordinal != 1 || members.NextPageToken == "" {
		t.Fatalf("first member page = %+v token=%q", members.Rows, members.NextPageToken)
	}
	second, err := service.Members(context.Background(), searchjobs.AccessScope{
		TenantID: "tenant", OwnerID: "owner",
	}, MemberRequest{
		SearchJobID: "job", Generation: 7, Sensitivity: Balanced,
		PatternID: result.Patterns[0].ID, PageSize: 2, PageToken: members.NextPageToken,
	})
	if err != nil {
		t.Fatalf("Members(second): %v", err)
	}
	defer second.Close()
	if len(second.Rows) != 1 || second.Rows[0].Ordinal != 4 || second.NextPageToken != "" {
		t.Fatalf("second member page = %+v token=%q", second.Rows, second.NextPageToken)
	}
	if source.acquisitions != 3 {
		t.Fatalf("durable acquisitions = %d, want one per request", source.acquisitions)
	}
}

func TestServiceTreatsSchemaWithoutRawAsEntirelyExcluded(t *testing.T) {
	t.Parallel()
	source := &testSource{
		schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}},
		rows: []searchjobs.ResultRow{
			{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("first")}},
			{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue("second")}},
		},
		generation: 3,
	}
	service, err := New(Config{Source: source, CursorKey: []byte("patterns-missing-raw-key")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	result, err := service.List(context.Background(), searchjobs.AccessScope{
		TenantID: "tenant", OwnerID: "owner",
	}, ListRequest{SearchJobID: "job", Generation: 3, Sensitivity: Precise, IncludeTotal: true})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.RetainedEventCount != 2 || result.EligibleEventCount != 0 || result.ExcludedEventCount != 2 ||
		len(result.Patterns) != 0 || result.TotalSize == nil || *result.TotalSize != 0 {
		t.Fatalf("missing _raw result = %+v", result)
	}
}

type switchingPatternSource struct {
	mu      sync.Mutex
	acquire func() searchjobs.ResultLease
}

func (source *switchingPatternSource) Acquire(
	ctx context.Context,
	_ searchjobs.AccessScope,
	_ string,
) (searchjobs.ResultLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.acquire(), nil
}

type blockingPatternLease struct {
	schema     searchjobs.Schema
	started    chan struct{}
	startOnce  sync.Once
	closeOnce  sync.Once
	generation uint64
}

func (lease *blockingPatternLease) Schema() searchjobs.Schema { return lease.schema }
func (*blockingPatternLease) RowCount() uint64                { return 1 }
func (*blockingPatternLease) RowCountExact() bool             { return true }
func (*blockingPatternLease) ResultsTruncated() bool          { return false }
func (lease *blockingPatternLease) Generation() uint64        { return lease.generation }
func (lease *blockingPatternLease) Seek(context.Context, uint64) error {
	return nil
}
func (lease *blockingPatternLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	lease.startOnce.Do(func() { close(lease.started) })
	<-ctx.Done()
	return searchjobs.ResultRow{}, false, ctx.Err()
}
func (lease *blockingPatternLease) Close() error {
	lease.closeOnce.Do(func() {})
	return nil
}

func TestPatternMemberExportCloseCancelsBlockedNext(t *testing.T) {
	rows := []searchjobs.ResultRow{{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("item 1")}}}
	base := &testSource{
		schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindString}}},
		rows:   rows, generation: 4,
	}
	source := &switchingPatternSource{acquire: func() searchjobs.ResultLease {
		return &testLease{source: base}
	}}
	service, err := New(Config{Source: source, CursorKey: []byte("patterns-blocking-export-key")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	listed, err := service.List(context.Background(), access, ListRequest{
		SearchJobID: "job", Generation: 4, Sensitivity: Balanced,
	})
	if err != nil {
		t.Fatal(err)
	}
	patternID := listed.Patterns[0].ID
	listed.Close()
	blocking := &blockingPatternLease{schema: base.schema, generation: 4, started: make(chan struct{})}
	source.mu.Lock()
	source.acquire = func() searchjobs.ResultLease { return blocking }
	source.mu.Unlock()
	exported, err := service.AcquirePatternMembers(context.Background(), access, ExportRequest{
		SearchJobID: "job", SnapshotRef: "snapshot", Generation: 4,
		Sensitivity: Balanced, PatternID: patternID,
	})
	if err != nil {
		t.Fatal(err)
	}
	nextDone := make(chan error, 1)
	go func() {
		_, _, err := exported.Next(context.Background())
		nextDone <- err
	}()
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("export Next did not reach the retained source")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- exported.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not promptly cancel the retained read")
	}
	select {
	case err := <-nextDone:
		if !errors.Is(err, searchjobs.ErrResultLeaseClosed) {
			t.Fatalf("blocked Next error = %v, want result lease closed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Next did not return after Close")
	}
}

type boundedAccountingSource struct {
	row          searchjobs.ResultRow
	schema       searchjobs.Schema
	acquireBytes uint64
	rowBytes     uint64
}

func (source *boundedAccountingSource) Acquire(
	context.Context,
	searchjobs.AccessScope,
	string,
) (searchjobs.ResultLease, error) {
	return nil, errors.New("unbounded acquire must not be used")
}

func (source *boundedAccountingSource) AcquireBounded(
	_ context.Context,
	_ searchjobs.AccessScope,
	_ string,
	reserve func(uint64) (func(), bool),
) (searchjobs.ResultLease, error) {
	release, ok := reserve(source.acquireBytes)
	if !ok {
		return nil, ErrCapacity
	}
	return &boundedAccountingLease{source: source, reserve: reserve, release: release}, nil
}

type boundedAccountingLease struct {
	source  *boundedAccountingSource
	reserve func(uint64) (func(), bool)
	release func()
	next    bool
}

func (lease *boundedAccountingLease) Schema() searchjobs.Schema { return lease.source.schema }
func (*boundedAccountingLease) RowCount() uint64                { return 1 }
func (*boundedAccountingLease) RowCountExact() bool             { return true }
func (*boundedAccountingLease) ResultsTruncated() bool          { return false }
func (*boundedAccountingLease) Generation() uint64              { return 1 }
func (*boundedAccountingLease) BoundedRead() bool               { return true }
func (*boundedAccountingLease) Next(context.Context) (searchjobs.ResultRow, bool, error) {
	return searchjobs.ResultRow{}, false, errors.New("unbounded next must not be used")
}
func (lease *boundedAccountingLease) NextBounded(context.Context) (searchjobs.ResultRow, bool, uint64, func(), error) {
	if lease.next {
		return searchjobs.ResultRow{}, false, 0, nil, nil
	}
	release, ok := lease.reserve(lease.source.rowBytes)
	if !ok {
		return searchjobs.ResultRow{}, false, 0, nil, ErrCapacity
	}
	lease.next = true
	return lease.source.row, true, lease.source.rowBytes, release, nil
}
func (lease *boundedAccountingLease) Close() error {
	lease.release()
	return nil
}

func TestAnalysisWorkingLimitCombinesAcquireDecodeAndNormalization(t *testing.T) {
	t.Parallel()
	raw := strings.Repeat("z", 30<<10)
	source := &boundedAccountingSource{
		schema:       searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindString}}},
		row:          searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue(raw)}},
		acquireBytes: 600 << 10,
		rowBytes:     100 << 10,
	}
	service, err := New(Config{
		Source: source, CursorKey: []byte("patterns-combined-working-key"), MaximumWorkingBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	_, err = service.List(context.Background(), searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}, ListRequest{
		SearchJobID: "job", Generation: 1, Sensitivity: Precise,
	})
	if !errors.Is(err, ErrLimit) {
		t.Fatalf("combined working limit error = %v, want ErrLimit", err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.globalBytes != service.cacheBytes {
		t.Fatalf("failed analysis retained %d non-cache bytes", service.globalBytes-service.cacheBytes)
	}
}

type testSource struct {
	schema       searchjobs.Schema
	rows         []searchjobs.ResultRow
	generation   uint64
	truncated    bool
	acquisitions int
}

func (source *testSource) Acquire(ctx context.Context, _ searchjobs.AccessScope, _ string) (searchjobs.ResultLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	source.acquisitions++
	return &testLease{source: source}, nil
}

type testLease struct {
	source *testSource
	next   int
	closed bool
}

func (lease *testLease) Schema() searchjobs.Schema { return lease.source.schema }
func (lease *testLease) RowCount() uint64          { return uint64(len(lease.source.rows)) }
func (*testLease) RowCountExact() bool             { return true }
func (lease *testLease) ResultsTruncated() bool    { return lease.source.truncated }
func (lease *testLease) Generation() uint64        { return lease.source.generation }

func (lease *testLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if lease.closed {
		return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
	}
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if lease.next == len(lease.source.rows) {
		return searchjobs.ResultRow{}, false, nil
	}
	row := lease.source.rows[lease.next]
	lease.next++
	return row, true, nil
}

func (lease *testLease) Seek(ctx context.Context, offset uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if offset > uint64(len(lease.source.rows)) {
		return ErrInvalidRequest
	}
	lease.next = int(offset)
	return nil
}

func (lease *testLease) Close() error {
	if lease.closed {
		return errors.New("lease closed more than once")
	}
	lease.closed = true
	return nil
}

func TestListCursorHandlesMaximumLengthSignatures(t *testing.T) {
	t.Parallel()

	first := strings.Repeat("g", DefaultMaximumSignatureBytes-1) + "1"
	second := strings.Repeat("h", DefaultMaximumSignatureBytes-1) + "2"
	source := &testSource{
		schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindString}}},
		rows: []searchjobs.ResultRow{
			{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue(first)}},
			{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue(second)}},
		},
		generation: 9,
	}
	service, err := New(Config{Source: source, CursorKey: []byte("patterns-long-cursor-test-key")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	page, err := service.List(context.Background(), access, ListRequest{
		SearchJobID: "job", Generation: 9, Sensitivity: Precise, PageSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer page.Close()
	if len(page.Patterns) != 1 || page.NextPageToken == "" || len(page.NextPageToken) > MaximumCursorBytes {
		t.Fatalf("first page patterns=%d cursor bytes=%d", len(page.Patterns), len(page.NextPageToken))
	}
	next, err := service.List(context.Background(), access, ListRequest{
		SearchJobID: "job", Generation: 9, Sensitivity: Precise, PageSize: 1, PageToken: page.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if len(next.Patterns) != 1 || next.NextPageToken != "" || next.Patterns[0].ID == page.Patterns[0].ID {
		t.Fatalf("second page = %+v cursor=%q", next.Patterns, next.NextPageToken)
	}
}

func TestPatternExportLeasesUseTheRetainedRelation(t *testing.T) {
	t.Parallel()

	source := &testSource{
		schema: searchjobs.Schema{Columns: []searchjobs.Column{
			{Name: "_raw", Kind: searchjobs.ValueKindString},
			{Name: "value", Kind: searchjobs.ValueKindUnsigned},
		}},
		rows: []searchjobs.ResultRow{
			{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("item 1"), searchjobs.UnsignedValue(10)}},
			{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue("item 2"), searchjobs.UnsignedValue(20)}},
			{Ordinal: 2, Values: []searchjobs.Value{searchjobs.StringValue("other 3"), searchjobs.UnsignedValue(30)}},
		},
		generation: 11,
	}
	service, err := New(Config{Source: source, CursorKey: []byte("patterns-export-test-key")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	listed, err := service.List(context.Background(), access, ListRequest{
		SearchJobID: "job", Generation: 11, Sensitivity: Balanced,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Patterns) != 2 || listed.Patterns[0].EventCount != 2 {
		t.Fatalf("patterns = %+v", listed.Patterns)
	}
	patternID := listed.Patterns[0].ID
	listed.Close()

	summary, err := service.AcquirePatternSummary(context.Background(), access, ExportRequest{
		SearchJobID: "job", SnapshotRef: "snapshot", Generation: 11, Sensitivity: Balanced,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = summary.Close() }()
	if summary.RowCount() != 2 || summary.Schema().Columns[0].Name != "pattern" {
		t.Fatalf("summary metadata rows=%d schema=%+v", summary.RowCount(), summary.Schema())
	}
	row, ok, err := summary.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("summary Next() ok=%t err=%v", ok, err)
	}
	count, countOK := row.Values[1].Unsigned()
	percent, percentOK := row.Values[2].Double()
	if !countOK || count != 2 || !percentOK || percent < 66.6 || percent > 66.7 {
		t.Fatalf("summary values count=%d/%t percent=%v/%t", count, countOK, percent, percentOK)
	}

	members, err := service.AcquirePatternMembers(context.Background(), access, ExportRequest{
		SearchJobID: "job", SnapshotRef: "snapshot", Generation: 11,
		Sensitivity: Balanced, PatternID: patternID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = members.Close() }()
	if members.RowCount() != 2 || len(members.Schema().Columns) != 2 {
		t.Fatalf("member metadata rows=%d schema=%+v", members.RowCount(), members.Schema())
	}
	for wantOrdinal := uint64(0); wantOrdinal < 2; wantOrdinal++ {
		row, ok, err := members.Next(context.Background())
		if err != nil || !ok || row.Ordinal != wantOrdinal {
			t.Fatalf("member %d = ordinal %d ok=%t err=%v", wantOrdinal, row.Ordinal, ok, err)
		}
	}
}
