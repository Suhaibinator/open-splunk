package clickhouse

import (
	"sync"
	"time"
)

const (
	coalescerShapeBucketCount   = 13
	coalescerLatencyBucketCount = 11
)

// CoalescerFillReason is a fixed-cardinality explanation for sealing a write
// group. It deliberately cannot carry caller or payload-derived text.
type CoalescerFillReason uint8

const (
	CoalescerFillUnknown CoalescerFillReason = iota
	CoalescerFillRowTarget
	CoalescerFillByteTarget
	CoalescerFillHardBoundary
	CoalescerFillLinger
	CoalescerFillDrain
	CoalescerFillRecovery
	coalescerFillReasonCount
)

// CoalescerQueueSnapshot is an aggregate durable-queue observation. Callers
// compute OldestPendingAge at the same read boundary as the counts.
type CoalescerQueueSnapshot struct {
	PendingReservations   uint64
	UngroupedReservations uint64
	ReadyGroups           uint64
	AmbiguousGroups       uint64
	LeasedGroups          uint64
	PendingOutboxBytes    uint64
	PendingMetadataBytes  uint64
	OldestPendingAge      time.Duration
}

// CoalescerHistogramSnapshot is a detached fixed-bucket histogram. Bounds are
// inclusive and Counts has one final overflow bucket.
type CoalescerHistogramSnapshot struct {
	Bounds [coalescerShapeBucketCount]uint64
	Counts [coalescerShapeBucketCount + 1]uint64
	Count  uint64
	Sum    uint64
	Max    uint64
}

// CoalescerLatencyHistogramSnapshot records microseconds so its entire public
// shape remains scalar and cannot retain identities or payloads.
type CoalescerLatencyHistogramSnapshot struct {
	BoundsMicroseconds [coalescerLatencyBucketCount]uint64
	Counts             [coalescerLatencyBucketCount + 1]uint64
	Count              uint64
	SumMicroseconds    uint64
	MaxMicroseconds    uint64
}

// CoalescerMetricsSnapshot is one coherent, payload-free operational view.
// All cardinality is compile-time bounded: there are no maps, strings, or
// caller-controlled labels.
type CoalescerMetricsSnapshot struct {
	StagedLogicalBatches uint64
	StagedLogicalRows    uint64

	FormedGroups       uint64
	PhysicalSends      uint64
	SuccessfulGroups   uint64
	Retries            uint64
	Ambiguities        uint64
	GroupsByFillReason [coalescerFillReasonCount]uint64

	MemberBatchesPerGroup     CoalescerHistogramSnapshot
	RowsPerGroup              CoalescerHistogramSnapshot
	DecodedBytesPerGroup      CoalescerHistogramSnapshot
	MonthlyPartitionsPerGroup CoalescerHistogramSnapshot
	RowsPerPhysicalInsert     CoalescerHistogramSnapshot

	CreationToSeal   CoalescerLatencyHistogramSnapshot
	CreationToSend   CoalescerLatencyHistogramSnapshot
	CreationToCommit CoalescerLatencyHistogramSnapshot

	NativeWaiters         uint64
	PeakNativeWaiters     uint64
	NativeWaiterWakeups   uint64
	NativeWaiterCancels   uint64
	NativeTerminalLookups uint64

	Queue CoalescerQueueSnapshot
}

type coalescerHistogram struct {
	bounds [coalescerShapeBucketCount]uint64
	counts [coalescerShapeBucketCount + 1]uint64
	count  uint64
	sum    uint64
	max    uint64
}

func newCoalescerHistogram(bounds [coalescerShapeBucketCount]uint64) coalescerHistogram {
	return coalescerHistogram{bounds: bounds}
}

func (histogram *coalescerHistogram) observe(value uint64) {
	bucket := len(histogram.bounds)
	for index, bound := range histogram.bounds {
		if value <= bound {
			bucket = index
			break
		}
	}
	histogram.counts[bucket]++
	histogram.count++
	histogram.sum = saturatingAdd(histogram.sum, value)
	histogram.max = max(histogram.max, value)
}

func (histogram coalescerHistogram) snapshot() CoalescerHistogramSnapshot {
	return CoalescerHistogramSnapshot{
		Bounds: histogram.bounds,
		Counts: histogram.counts,
		Count:  histogram.count,
		Sum:    histogram.sum,
		Max:    histogram.max,
	}
}

type coalescerLatencyHistogram struct {
	bounds [coalescerLatencyBucketCount]uint64
	counts [coalescerLatencyBucketCount + 1]uint64
	count  uint64
	sum    uint64
	max    uint64
}

func (histogram *coalescerLatencyHistogram) observe(duration time.Duration) {
	if duration < 0 {
		return
	}
	microseconds := uint64(duration / time.Microsecond)
	bucket := len(histogram.bounds)
	for index, bound := range histogram.bounds {
		if microseconds <= bound {
			bucket = index
			break
		}
	}
	histogram.counts[bucket]++
	histogram.count++
	histogram.sum = saturatingAdd(histogram.sum, microseconds)
	histogram.max = max(histogram.max, microseconds)
}

func (histogram coalescerLatencyHistogram) snapshot() CoalescerLatencyHistogramSnapshot {
	return CoalescerLatencyHistogramSnapshot{
		BoundsMicroseconds: histogram.bounds,
		Counts:             histogram.counts,
		Count:              histogram.count,
		SumMicroseconds:    histogram.sum,
		MaxMicroseconds:    histogram.max,
	}
}

// CoalescerMetrics owns bounded aggregate counters, gauges, and histograms.
type CoalescerMetrics struct {
	mu sync.Mutex

	stagedLogicalBatches uint64
	stagedLogicalRows    uint64
	formedGroups         uint64
	physicalSends        uint64
	successfulGroups     uint64
	retries              uint64
	ambiguities          uint64
	groupsByFillReason   [coalescerFillReasonCount]uint64

	memberBatchesPerGroup     coalescerHistogram
	rowsPerGroup              coalescerHistogram
	decodedBytesPerGroup      coalescerHistogram
	monthlyPartitionsPerGroup coalescerHistogram
	rowsPerPhysicalInsert     coalescerHistogram
	creationToSeal            coalescerLatencyHistogram
	creationToSend            coalescerLatencyHistogram
	creationToCommit          coalescerLatencyHistogram

	nativeWaiters         uint64
	peakNativeWaiters     uint64
	nativeWaiterWakeups   uint64
	nativeWaiterCancels   uint64
	nativeTerminalLookups uint64
	queue                 CoalescerQueueSnapshot
}

// NewCoalescerMetrics creates an empty fixed-cardinality metrics owner.
func NewCoalescerMetrics() *CoalescerMetrics {
	shapeBounds := [coalescerShapeBucketCount]uint64{
		1, 10, 64, 100, 500, 1_000, 2_500, 5_000, 10_000, 16_384, 32_768, 50_000, 65_536,
	}
	byteBounds := [coalescerShapeBucketCount]uint64{
		1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20,
		8 << 20, 16 << 20, 24 << 20, 32 << 20, 48 << 20, 64 << 20,
	}
	latencyBounds := [coalescerLatencyBucketCount]uint64{
		100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 200_000, 500_000, 1_000_000, 5_000_000,
	}
	return &CoalescerMetrics{
		memberBatchesPerGroup:     newCoalescerHistogram(shapeBounds),
		rowsPerGroup:              newCoalescerHistogram(shapeBounds),
		decodedBytesPerGroup:      newCoalescerHistogram(byteBounds),
		monthlyPartitionsPerGroup: newCoalescerHistogram(shapeBounds),
		rowsPerPhysicalInsert:     newCoalescerHistogram(shapeBounds),
		creationToSeal:            coalescerLatencyHistogram{bounds: latencyBounds},
		creationToSend:            coalescerLatencyHistogram{bounds: latencyBounds},
		creationToCommit:          coalescerLatencyHistogram{bounds: latencyBounds},
	}
}

// ObserveStaged records one accepted logical batch. Rows may be zero for a
// durable whole-batch rejection, which never enters a write group.
func (metrics *CoalescerMetrics) ObserveStaged(rows uint64) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.stagedLogicalBatches++
	metrics.stagedLogicalRows = saturatingAdd(metrics.stagedLogicalRows, rows)
	metrics.mu.Unlock()
}

// ObserveGroupFormed records the immutable shape and durable seal latency of
// one new group. Invalid reasons are safely folded into the unknown bucket.
func (metrics *CoalescerMetrics) ObserveGroupFormed(
	reason CoalescerFillReason,
	members, rows, decodedBytes, monthlyPartitions uint64,
	creationToSeal time.Duration,
) {
	if metrics == nil {
		return
	}
	if reason >= coalescerFillReasonCount {
		reason = CoalescerFillUnknown
	}
	metrics.mu.Lock()
	metrics.formedGroups++
	metrics.groupsByFillReason[reason]++
	metrics.memberBatchesPerGroup.observe(members)
	metrics.rowsPerGroup.observe(rows)
	metrics.decodedBytesPerGroup.observe(decodedBytes)
	metrics.monthlyPartitionsPerGroup.observe(monthlyPartitions)
	metrics.creationToSeal.observe(creationToSeal)
	metrics.mu.Unlock()
}

// ObserveRecoveredGroup distinguishes a replay acquisition without counting
// the immutable group as newly formed or recording its shape a second time.
func (metrics *CoalescerMetrics) ObserveRecoveredGroup() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.groupsByFillReason[CoalescerFillRecovery]++
	metrics.mu.Unlock()
}

// ObservePhysicalSend records one call that can touch ClickHouse, including
// replay attempts, and the exact row shape presented to the driver.
func (metrics *CoalescerMetrics) ObservePhysicalSend(rows uint64, creationToSend time.Duration) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.physicalSends++
	metrics.rowsPerPhysicalInsert.observe(rows)
	metrics.creationToSend.observe(creationToSend)
	metrics.mu.Unlock()
}

// ObserveGroupSuccess records the terminal completion of one whole group.
func (metrics *CoalescerMetrics) ObserveGroupSuccess(creationToCommit time.Duration) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.successfulGroups++
	metrics.creationToCommit.observe(creationToCommit)
	metrics.mu.Unlock()
}

func (metrics *CoalescerMetrics) ObserveRetry() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.retries++
	metrics.mu.Unlock()
}

func (metrics *CoalescerMetrics) ObserveAmbiguity() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.ambiguities++
	metrics.mu.Unlock()
}

func (metrics *CoalescerMetrics) AddNativeWaiter() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.nativeWaiters++
	metrics.peakNativeWaiters = max(metrics.peakNativeWaiters, metrics.nativeWaiters)
	metrics.mu.Unlock()
}

func (metrics *CoalescerMetrics) RemoveNativeWaiter() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	if metrics.nativeWaiters > 0 {
		metrics.nativeWaiters--
	}
	metrics.mu.Unlock()
}

func (metrics *CoalescerMetrics) ObserveNativeWaiterWakeups(count uint64) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.nativeWaiterWakeups = saturatingAdd(metrics.nativeWaiterWakeups, count)
	metrics.mu.Unlock()
}

func (metrics *CoalescerMetrics) ObserveNativeWaiterCancellation() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.nativeWaiterCancels++
	metrics.mu.Unlock()
}

func (metrics *CoalescerMetrics) ObserveNativeTerminalLookup() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.nativeTerminalLookups++
	metrics.mu.Unlock()
}

// SetQueue replaces gauges from one authoritative SQLite snapshot.
func (metrics *CoalescerMetrics) SetQueue(queue CoalescerQueueSnapshot) {
	if metrics == nil {
		return
	}
	if queue.OldestPendingAge < 0 {
		queue.OldestPendingAge = 0
	}
	metrics.mu.Lock()
	metrics.queue = queue
	metrics.mu.Unlock()
}

// Snapshot returns one coherent, detached view.
func (metrics *CoalescerMetrics) Snapshot() CoalescerMetricsSnapshot {
	if metrics == nil {
		return CoalescerMetricsSnapshot{}
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	return CoalescerMetricsSnapshot{
		StagedLogicalBatches:      metrics.stagedLogicalBatches,
		StagedLogicalRows:         metrics.stagedLogicalRows,
		FormedGroups:              metrics.formedGroups,
		PhysicalSends:             metrics.physicalSends,
		SuccessfulGroups:          metrics.successfulGroups,
		Retries:                   metrics.retries,
		Ambiguities:               metrics.ambiguities,
		GroupsByFillReason:        metrics.groupsByFillReason,
		MemberBatchesPerGroup:     metrics.memberBatchesPerGroup.snapshot(),
		RowsPerGroup:              metrics.rowsPerGroup.snapshot(),
		DecodedBytesPerGroup:      metrics.decodedBytesPerGroup.snapshot(),
		MonthlyPartitionsPerGroup: metrics.monthlyPartitionsPerGroup.snapshot(),
		RowsPerPhysicalInsert:     metrics.rowsPerPhysicalInsert.snapshot(),
		CreationToSeal:            metrics.creationToSeal.snapshot(),
		CreationToSend:            metrics.creationToSend.snapshot(),
		CreationToCommit:          metrics.creationToCommit.snapshot(),
		NativeWaiters:             metrics.nativeWaiters,
		PeakNativeWaiters:         metrics.peakNativeWaiters,
		NativeWaiterWakeups:       metrics.nativeWaiterWakeups,
		NativeWaiterCancels:       metrics.nativeWaiterCancels,
		NativeTerminalLookups:     metrics.nativeTerminalLookups,
		Queue:                     metrics.queue,
	}
}

func saturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
