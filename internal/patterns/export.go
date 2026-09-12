package patterns

import (
	"context"
	"slices"
	"sync"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// AcquirePatternSummary returns all groups from one immutable retained
// generation as a typed export lease. The durable source lease remains pinned
// until Close even though its rows were consumed while building the catalog.
func (service *Service) AcquirePatternSummary(
	ctx context.Context,
	access searchjobs.AccessScope,
	request ExportRequest,
) (searchjobs.ResultLease, error) {
	if err := service.validateExportRequest(access, request, false); err != nil {
		return nil, err
	}
	operation, budget, finish, err := service.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	leaseContext, cancelLease := context.WithCancel(ctx)
	stopDeadline := context.AfterFunc(operation, cancelLease)
	lease, release, _, err := service.acquire(leaseContext, access, request.SearchJobID, budget.working)
	stopDeadline()
	if err != nil {
		cancelLease()
		return nil, err
	}
	owned := true
	defer func() {
		if owned {
			cancelLease()
			release()
		}
	}()
	if err := validateLease(lease, request.Generation, service.maximumRows); err != nil {
		return nil, err
	}
	key := catalogKey{access: access, jobID: request.SearchJobID, generation: request.Generation, sensitivity: request.Sensitivity}
	catalog := service.cachedCatalog(key, budget)
	if catalog == nil {
		catalog, err = service.buildCatalog(operation, budget, lease, request.SearchJobID, request.Sensitivity)
		if err != nil {
			return nil, err
		}
		service.storeCatalog(key, catalog)
	}
	reservation, ok := budget.detach(catalog.inputBytes + catalog.bytes)
	if !ok {
		return nil, ErrCapacity
	}
	owned = false
	result := &summaryExportLease{
		patterns: slicesOfPatterns(catalog.patterns), eligible: catalog.eligibleEventCount,
		generation: request.Generation, truncated: catalog.retainedTruncated,
		cancel: cancelLease, closeSource: release, reservation: reservation,
	}
	result.watchContext(ctx)
	return result, nil
}

// AcquirePatternMembers returns every original retained row in one group. The
// production durable artifact lease is seekable, allowing catalog construction
// and export streaming under one uninterrupted source pin.
func (service *Service) AcquirePatternMembers(
	ctx context.Context,
	access searchjobs.AccessScope,
	request ExportRequest,
) (searchjobs.ResultLease, error) {
	if err := service.validateExportRequest(access, request, true); err != nil {
		return nil, err
	}
	operation, budget, finish, err := service.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	leaseContext, cancelLease := context.WithCancel(ctx)
	stopDeadline := context.AfterFunc(operation, cancelLease)
	lease, release, readBudget, err := service.acquire(leaseContext, access, request.SearchJobID, budget.working)
	stopDeadline()
	if err != nil {
		cancelLease()
		return nil, err
	}
	owned := true
	defer func() {
		if owned {
			cancelLease()
			release()
		}
	}()
	if err := validateLease(lease, request.Generation, service.maximumRows); err != nil {
		return nil, err
	}
	key := catalogKey{access: access, jobID: request.SearchJobID, generation: request.Generation, sensitivity: request.Sensitivity}
	catalog := service.cachedCatalog(key, budget)
	if catalog == nil {
		catalog, err = service.buildCatalog(operation, budget, lease, request.SearchJobID, request.Sensitivity)
		if err != nil {
			return nil, err
		}
		service.storeCatalog(key, catalog)
	}
	var selected *Pattern
	for index := range catalog.patterns {
		if catalog.patterns[index].ID == request.PatternID {
			selected = &catalog.patterns[index]
			break
		}
	}
	if selected == nil {
		return nil, ErrPatternNotFound
	}
	seekable, ok := lease.(interface {
		Seek(context.Context, uint64) error
	})
	if !ok {
		return nil, ErrUnsupported
	}
	if err := seekable.Seek(operation, 0); err != nil {
		return nil, err
	}
	reservation, ok := budget.detach(catalog.inputBytes)
	if !ok {
		return nil, ErrCapacity
	}
	owned = false
	result := &memberExportLease{
		source: lease, closeSource: release, reservation: reservation,
		jobID: request.SearchJobID, generation: request.Generation, sensitivity: request.Sensitivity,
		patternID: request.PatternID, rowCount: selected.EventCount,
		schema: lease.Schema(), truncated: lease.ResultsTruncated(), maximumSignatureBytes: service.maximumSignatureBytes,
		maximumWorkingBytes: service.maximumWorkingBytes, ctx: leaseContext, cancel: cancelLease,
		readBudget: readBudget,
	}
	result.watchContext(ctx)
	return result, nil
}

func (service *Service) validateExportRequest(access searchjobs.AccessScope, request ExportRequest, members bool) error {
	if !validIdentity(access, request.SearchJobID) || request.Generation == 0 || !request.Sensitivity.valid() ||
		request.SnapshotRef == "" || len(request.SnapshotRef) > maximumIdentityBytes || !utf8.ValidString(request.SnapshotRef) {
		return ErrInvalidRequest
	}
	if members != validPatternID(request.PatternID) {
		return ErrInvalidRequest
	}
	return nil
}

var summarySchema = searchjobs.Schema{Columns: []searchjobs.Column{
	{Name: "pattern", Kind: searchjobs.ValueKindString},
	{Name: "count", Kind: searchjobs.ValueKindUnsigned},
	{Name: "percent", Kind: searchjobs.ValueKindDouble},
}}

type summaryExportLease struct {
	patterns    []Pattern
	eligible    uint64
	generation  uint64
	truncated   bool
	closeSource func()
	reservation *resultReservation
	cancel      context.CancelFunc
	stopContext func() bool

	mu        sync.Mutex
	closeOnce sync.Once
	next      int
	closed    bool
}

func (lease *summaryExportLease) watchContext(ctx context.Context) {
	stop := context.AfterFunc(ctx, func() { _ = lease.Close() })
	lease.mu.Lock()
	if lease.closed {
		lease.mu.Unlock()
		stop()
		return
	}
	lease.stopContext = stop
	lease.mu.Unlock()
}

func (*summaryExportLease) Schema() searchjobs.Schema {
	return searchjobs.Schema{Columns: slices.Clone(summarySchema.Columns)}
}
func (lease *summaryExportLease) RowCount() uint64       { return uint64(len(lease.patterns)) }
func (*summaryExportLease) RowCountExact() bool          { return true }
func (lease *summaryExportLease) ResultsTruncated() bool { return lease.truncated }
func (lease *summaryExportLease) Generation() uint64     { return lease.generation }

func (lease *summaryExportLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if ctx == nil {
		return searchjobs.ResultRow{}, false, ErrInvalidRequest
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
	}
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if lease.next == len(lease.patterns) {
		return searchjobs.ResultRow{}, false, nil
	}
	pattern := lease.patterns[lease.next]
	percent := float64(0)
	if lease.eligible != 0 {
		percent = float64(pattern.EventCount) * 100 / float64(lease.eligible)
	}
	row := searchjobs.ResultRow{Ordinal: uint64(lease.next), Values: []searchjobs.Value{
		searchjobs.StringValue(pattern.Signature),
		searchjobs.UnsignedValue(pattern.EventCount),
		searchjobs.DoubleValue(percent),
	}}
	lease.next++
	return row, true, nil
}

func (lease *summaryExportLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.closeOnce.Do(func() {
		lease.cancel()
		lease.mu.Lock()
		lease.closed = true
		stopContext := lease.stopContext
		lease.stopContext = nil
		lease.mu.Unlock()
		if stopContext != nil {
			stopContext()
		}
		lease.closeSource()
		lease.reservation.Close()
	})
	return nil
}

type memberExportLease struct {
	source                searchjobs.ResultLease
	closeSource           func()
	reservation           *resultReservation
	jobID                 string
	generation            uint64
	sensitivity           Sensitivity
	patternID             string
	rowCount              uint64
	schema                searchjobs.Schema
	truncated             bool
	maximumSignatureBytes int
	maximumWorkingBytes   uint64
	readBudget            *operationBudget
	ctx                   context.Context
	cancel                context.CancelFunc
	stopContext           func() bool

	nextMu             sync.Mutex
	stateMu            sync.Mutex
	closeOnce          sync.Once
	returned           uint64
	closed             bool
	previousRowRelease func()
}

func (lease *memberExportLease) watchContext(ctx context.Context) {
	stop := context.AfterFunc(ctx, func() { _ = lease.Close() })
	lease.stateMu.Lock()
	if lease.closed {
		lease.stateMu.Unlock()
		stop()
		return
	}
	lease.stopContext = stop
	lease.stateMu.Unlock()
}

func (lease *memberExportLease) Schema() searchjobs.Schema {
	return searchjobs.Schema{Columns: slices.Clone(lease.schema.Columns)}
}
func (lease *memberExportLease) RowCount() uint64       { return lease.rowCount }
func (*memberExportLease) RowCountExact() bool          { return true }
func (lease *memberExportLease) ResultsTruncated() bool { return lease.truncated }
func (lease *memberExportLease) Generation() uint64     { return lease.generation }

func (lease *memberExportLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if ctx == nil {
		return searchjobs.ResultRow{}, false, ErrInvalidRequest
	}
	lease.nextMu.Lock()
	defer lease.nextMu.Unlock()
	if lease.previousRowRelease != nil {
		lease.previousRowRelease()
		lease.previousRowRelease = nil
	}
	lease.stateMu.Lock()
	if lease.closed {
		lease.stateMu.Unlock()
		return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
	}
	lease.stateMu.Unlock()
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	rawIndex, valid := rawColumnIndex(lease.schema)
	if !valid || rawIndex < 0 {
		return searchjobs.ResultRow{}, false, ErrUnsupported
	}
	for {
		nextContext, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(lease.ctx, cancel)
		row, present, _, releaseRow, err := nextResultRow(nextContext, lease.source)
		if err != nil {
			stop()
			cancel()
			if lease.ctx.Err() != nil {
				return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
			}
			return searchjobs.ResultRow{}, false, err
		}
		if !present {
			stop()
			cancel()
			if lease.returned != lease.rowCount {
				return searchjobs.ResultRow{}, false, ErrUnsupported
			}
			return searchjobs.ResultRow{}, false, nil
		}
		if len(row.Values) != len(lease.schema.Columns) {
			stop()
			cancel()
			if releaseRow != nil {
				releaseRow()
			}
			return searchjobs.ResultRow{}, false, ErrUnsupported
		}
		raw, eligible := row.Values[rawIndex].String()
		if !eligible {
			stop()
			cancel()
			if releaseRow != nil {
				releaseRow()
			}
			continue
		}
		normalized, releaseNormalized, err := normalizeWithBudget(
			nextContext, lease.readBudget, raw, lease.sensitivity, lease.maximumSignatureBytes, lease.maximumWorkingBytes,
		)
		stop()
		cancel()
		if err != nil {
			if releaseRow != nil {
				releaseRow()
			}
			if lease.ctx.Err() != nil {
				return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
			}
			return searchjobs.ResultRow{}, false, err
		}
		matches := patternID(lease.jobID, lease.generation, lease.sensitivity, normalized.canonical) == lease.patternID
		releaseNormalized()
		if !matches {
			if releaseRow != nil {
				releaseRow()
			}
			continue
		}
		lease.returned++
		lease.previousRowRelease = releaseRow
		return row, true, nil
	}
}

func (lease *memberExportLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.closeOnce.Do(func() {
		lease.cancel()
		lease.nextMu.Lock()
		lease.stateMu.Lock()
		lease.closed = true
		stopContext := lease.stopContext
		lease.stopContext = nil
		lease.stateMu.Unlock()
		if stopContext != nil {
			stopContext()
		}
		if lease.previousRowRelease != nil {
			lease.previousRowRelease()
			lease.previousRowRelease = nil
		}
		lease.closeSource()
		lease.reservation.Close()
		lease.nextMu.Unlock()
	})
	return nil
}

func slicesOfPatterns(patterns []Pattern) []Pattern {
	result := make([]Pattern, len(patterns))
	copy(result, patterns)
	return result
}

var _ searchjobs.ResultLease = (*summaryExportLease)(nil)
var _ searchjobs.ResultLease = (*memberExportLease)(nil)
