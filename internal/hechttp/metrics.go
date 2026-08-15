package hechttp

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/hec"
)

// Metrics owns only bounded aggregate counters. It intentionally has no
// dynamic labels and therefore cannot retain tokens, channels, metadata, or
// event content.
type Metrics struct {
	// observationMu lets request goroutines update atomic counters concurrently
	// while Snapshot establishes one coherent boundary across every field.
	observationMu            sync.RWMutex
	requests                 atomic.Uint64
	events                   atomic.Uint64
	uncompressedBytes        atomic.Uint64
	authenticationFailures   atomic.Uint64
	decodeFailures           atomic.Uint64
	eventPolicyFailures      atomic.Uint64
	acceptedRequests         atomic.Uint64
	rateLimitedRequests      atomic.Uint64
	stagingFailures          atomic.Uint64
	stagingNanoseconds       atomic.Int64
	acknowledgmentQueries    atomic.Uint64
	acknowledgmentIDsQueried atomic.Uint64
	acknowledgmentMisses     atomic.Uint64
	shutdownRejections       atomic.Uint64
	protocolFailures         [28]atomic.Uint64
}

// MetricsSnapshot is a detached aggregate operational projection.
type MetricsSnapshot struct {
	Requests                 uint64
	Events                   uint64
	UncompressedBytes        uint64
	AuthenticationFailures   uint64
	DecodeFailures           uint64
	EventPolicyFailures      uint64
	AcceptedRequests         uint64
	RateLimitedRequests      uint64
	StagingFailures          uint64
	StagingDuration          time.Duration
	AcknowledgmentQueries    uint64
	AcknowledgmentIDsQueried uint64
	AcknowledgmentMisses     uint64
	ShutdownRejections       uint64
	ProtocolFailures         [28]uint64
}

func NewMetrics() *Metrics { return &Metrics{} }

func (metrics *Metrics) Snapshot() MetricsSnapshot {
	if metrics == nil {
		return MetricsSnapshot{}
	}
	metrics.observationMu.Lock()
	defer metrics.observationMu.Unlock()
	result := MetricsSnapshot{
		Requests:                 metrics.requests.Load(),
		Events:                   metrics.events.Load(),
		UncompressedBytes:        metrics.uncompressedBytes.Load(),
		AuthenticationFailures:   metrics.authenticationFailures.Load(),
		DecodeFailures:           metrics.decodeFailures.Load(),
		EventPolicyFailures:      metrics.eventPolicyFailures.Load(),
		AcceptedRequests:         metrics.acceptedRequests.Load(),
		RateLimitedRequests:      metrics.rateLimitedRequests.Load(),
		StagingFailures:          metrics.stagingFailures.Load(),
		StagingDuration:          time.Duration(metrics.stagingNanoseconds.Load()),
		AcknowledgmentQueries:    metrics.acknowledgmentQueries.Load(),
		AcknowledgmentIDsQueried: metrics.acknowledgmentIDsQueried.Load(),
		AcknowledgmentMisses:     metrics.acknowledgmentMisses.Load(),
		ShutdownRejections:       metrics.shutdownRejections.Load(),
	}
	for index := range result.ProtocolFailures {
		result.ProtocolFailures[index] = metrics.protocolFailures[index].Load()
	}
	return result
}

// bump increments one aggregate counter under the observation boundary.
func (metrics *Metrics) bump(counter *atomic.Uint64) {
	metrics.observationMu.RLock()
	counter.Add(1)
	metrics.observationMu.RUnlock()
}
func (metrics *Metrics) observeRequest() {
	metrics.bump(&metrics.requests)
}
func (metrics *Metrics) observeAuthenticationFailure() {
	metrics.bump(&metrics.authenticationFailures)
}
func (metrics *Metrics) observeDecodeFailure() {
	metrics.bump(&metrics.decodeFailures)
}
func (metrics *Metrics) observeEventPolicyFailure() {
	metrics.bump(&metrics.eventPolicyFailures)
}
func (metrics *Metrics) observeRateLimitedRequest() {
	metrics.bump(&metrics.rateLimitedRequests)
}
func (metrics *Metrics) observeStagingFailure() {
	metrics.bump(&metrics.stagingFailures)
}
func (metrics *Metrics) observeShutdownRejection() {
	metrics.bump(&metrics.shutdownRejections)
}
func (metrics *Metrics) observeStagingLatency(duration time.Duration) {
	if duration > 0 {
		metrics.observationMu.RLock()
		metrics.stagingNanoseconds.Add(duration.Nanoseconds())
		metrics.observationMu.RUnlock()
	}
}
func (metrics *Metrics) observeAccepted(events, bytes uint64) {
	metrics.observationMu.RLock()
	defer metrics.observationMu.RUnlock()
	metrics.acceptedRequests.Add(1)
	metrics.events.Add(events)
	metrics.uncompressedBytes.Add(bytes)
}
func (metrics *Metrics) observeAcknowledgmentQuery(ids, misses uint64) {
	metrics.observationMu.RLock()
	defer metrics.observationMu.RUnlock()
	metrics.acknowledgmentQueries.Add(1)
	metrics.acknowledgmentIDsQueried.Add(ids)
	metrics.acknowledgmentMisses.Add(misses)
}
func (metrics *Metrics) observeFailure(code hec.ResultCode) {
	switch code {
	case hec.ResultSuccess, hec.ResultHealthy,
		hec.ResultQueueApproachingCapacity, hec.ResultAckApproachingCapacity:
		return
	}
	if code > hec.ResultSuccess && int(code) < len(metrics.protocolFailures) {
		metrics.bump(&metrics.protocolFailures[code])
	}
}
