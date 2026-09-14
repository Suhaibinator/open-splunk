package patterns

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

const (
	qualificationFixtureRows       = uint64(10_000)
	qualificationFixtureGroups     = uint64(100)
	qualificationFixtureGeneration = uint64(17)
)

type qualificationFixture struct {
	Store         *searchartifacts.Store
	Access        searchjobs.AccessScope
	JobID         string
	Generation    uint64
	Rows          uint64
	Groups        uint64
	SHA256        string
	ArtifactBytes uint64
}

// newQualificationFixture persists one deterministic, typed, final relation
// through the production artifact format. The returned Store stays open until
// test cleanup so qualification runners can measure cold services, cache hits,
// restart-independent reads, and process memory against the same artifact.
func newQualificationFixture(t *testing.T) qualificationFixture {
	t.Helper()
	ctx := t.Context()
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	directory := t.TempDir()
	database, err := control.Open(ctx, filepath.Join(directory, "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store, err := searchartifacts.New(ctx, searchartifacts.Config{
		DB: database.SQLDB(), Directory: filepath.Join(directory, "artifacts"),
		Clock: func() time.Time { return now }, CleanupInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	access := searchjobs.AccessScope{TenantID: "qualification-tenant", OwnerID: "qualification-owner"}
	const jobID = "patterns-performance-qualification"
	rangeValue, err := searchtime.NewAbsoluteRange(now.Add(-time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_raw", Kind: searchjobs.ValueKindString},
		{Name: "sequence", Kind: searchjobs.ValueKindUnsigned},
		{Name: "observed_at", Kind: searchjobs.ValueKindTime, Nullable: true},
	}}
	job := searchjobs.Job{
		ID: jobID, Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=qualification", TimeRange: rangeValue.Intent(), State: searchjobs.StateQueued, CreatedAt: now,
	}
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	job.Version = 2
	job.State = searchjobs.StateCompleted
	job.Schema = &schema
	job.StartedAt = now
	job.FinishedAt = now
	job.ExpiresAt = now.Add(time.Hour)
	job.RowCount = qualificationFixtureRows
	job.ResultsTruncated = true
	if err := store.Finalize(ctx, job); err != nil {
		t.Fatal(err)
	}

	rows, fixtureHash := qualificationRows(now)
	record, err := store.PersistResults(ctx, access, jobID, &qualificationInputLease{
		schema: schema, rows: rows, generation: qualificationFixtureGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	return qualificationFixture{
		Store: store, Access: access, JobID: jobID, Generation: qualificationFixtureGeneration,
		Rows: qualificationFixtureRows, Groups: qualificationFixtureGroups,
		SHA256: fixtureHash, ArtifactBytes: record.ArtifactBytes,
	}
}

func qualificationRows(epoch time.Time) ([]searchjobs.ResultRow, string) {
	rows := make([]searchjobs.ResultRow, qualificationFixtureRows)
	digest := sha256.New()
	var encoded [8]byte
	for index := range qualificationFixtureRows {
		class := qualificationClass(index % qualificationFixtureGroups)
		raw := fmt.Sprintf("class%s request=%016x status=%d", class, index, 200+index%5)
		sequence := ^uint64(0) - index
		observed := searchjobs.NullValue()
		timestamp := int64(-1)
		if index%10 != 0 {
			stamp := epoch.Add(time.Duration(index) * time.Millisecond)
			observed = searchjobs.TimeValue(stamp)
			timestamp = stamp.UnixNano()
		}
		rows[index] = searchjobs.ResultRow{Ordinal: index, Values: []searchjobs.Value{
			searchjobs.StringValue(raw), searchjobs.UnsignedValue(sequence), observed,
		}}
		binary.BigEndian.PutUint64(encoded[:], uint64(len(raw)))
		_, _ = digest.Write(encoded[:])
		_, _ = digest.Write([]byte(raw))
		binary.BigEndian.PutUint64(encoded[:], sequence)
		_, _ = digest.Write(encoded[:])
		binary.BigEndian.PutUint64(encoded[:], uint64(timestamp))
		_, _ = digest.Write(encoded[:])
	}
	return rows, hex.EncodeToString(digest.Sum(nil))
}

func qualificationClass(index uint64) string {
	return string([]byte{'A' + byte(index/26), 'A' + byte(index%26)})
}

type qualificationInputLease struct {
	schema     searchjobs.Schema
	rows       []searchjobs.ResultRow
	generation uint64
	next       int
}

func (lease *qualificationInputLease) Schema() searchjobs.Schema { return lease.schema }
func (lease *qualificationInputLease) RowCount() uint64          { return uint64(len(lease.rows)) }
func (*qualificationInputLease) RowCountExact() bool             { return true }
func (*qualificationInputLease) ResultsTruncated() bool          { return true }
func (lease *qualificationInputLease) Generation() uint64        { return lease.generation }
func (*qualificationInputLease) Close() error                    { return nil }
func (lease *qualificationInputLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if lease.next == len(lease.rows) {
		return searchjobs.ResultRow{}, false, nil
	}
	row := lease.rows[lease.next]
	lease.next++
	return row, true, nil
}

type qualificationBlockedSource struct {
	store   *searchartifacts.Store
	entered chan struct{}
	once    sync.Once
}

// newQualificationBlockedSource preserves the production bounded acquisition
// and lease types, but gates the first bounded row read until its context is
// canceled. The entered channel is the barrier for cancellation latency.
func newQualificationBlockedSource(
	store *searchartifacts.Store,
) (*qualificationBlockedSource, <-chan struct{}) {
	entered := make(chan struct{})
	return &qualificationBlockedSource{store: store, entered: entered}, entered
}

func (source *qualificationBlockedSource) Acquire(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
) (searchjobs.ResultLease, error) {
	return source.store.Acquire(ctx, access, jobID)
}

func (source *qualificationBlockedSource) AcquireBounded(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	reserve func(uint64) (func(), bool),
) (searchjobs.ResultLease, error) {
	lease, err := source.store.AcquireBounded(ctx, access, jobID, reserve)
	if err != nil {
		return nil, err
	}
	bounded, ok := lease.(boundedResultLease)
	if !ok || !bounded.BoundedRead() {
		_ = lease.Close()
		return nil, ErrUnsupported
	}
	return &qualificationBlockedLease{
		ResultLease: lease, bounded: bounded, source: source,
	}, nil
}

type qualificationBlockedLease struct {
	searchjobs.ResultLease
	bounded boundedResultLease
	source  *qualificationBlockedSource
}

func (*qualificationBlockedLease) BoundedRead() bool { return true }

func (lease *qualificationBlockedLease) NextBounded(ctx context.Context) (
	searchjobs.ResultRow,
	bool,
	uint64,
	func(),
	error,
) {
	blocked := false
	lease.source.once.Do(func() {
		blocked = true
		close(lease.source.entered)
	})
	if blocked {
		if ctx == nil {
			return searchjobs.ResultRow{}, false, 0, nil, ErrInvalidRequest
		}
		<-ctx.Done()
		return searchjobs.ResultRow{}, false, 0, nil, ctx.Err()
	}
	return lease.bounded.NextBounded(ctx)
}

func (lease *qualificationBlockedLease) Seek(ctx context.Context, offset uint64) error {
	seekable, ok := lease.ResultLease.(searchartifacts.SeekableResultLease)
	if !ok {
		return ErrUnsupported
	}
	return seekable.Seek(ctx, offset)
}

var _ Source = (*searchartifacts.Store)(nil)
var _ Source = (*qualificationBlockedSource)(nil)
var _ boundedSource = (*qualificationBlockedSource)(nil)
var _ boundedResultLease = (*qualificationBlockedLease)(nil)
