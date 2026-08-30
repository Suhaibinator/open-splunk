package main

import (
	"sync"

	"github.com/Suhaibinator/open-splunk/internal/featureops"
	"go.uber.org/zap"
)

// runtimeFeatureOperations keeps fixed-cardinality counters and emits one
// identity-free operational log record. It cannot accept names, queries,
// URLs, hostnames, IDs, or secrets because featureops.Event has no field for
// caller-controlled labels.
type runtimeFeatureOperations struct {
	logger  *zap.Logger
	metrics *featureops.Metrics
	auditMu sync.RWMutex
	auditor featureops.Observer
}

func newRuntimeFeatureOperations(logger *zap.Logger) *runtimeFeatureOperations {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &runtimeFeatureOperations{logger: logger, metrics: featureops.NewMetrics()}
}

func (operations *runtimeFeatureOperations) Observe(event featureops.Event) {
	if operations == nil || !event.Valid() {
		return
	}
	operations.metrics.Observe(event)
	operations.logger.Info(
		"feature operation",
		zap.Uint8("feature", uint8(event.Feature)),
		zap.Uint8("operation", uint8(event.Operation)),
		zap.Uint8("outcome", uint8(event.Outcome)),
		zap.Uint64("items", event.Items),
		zap.Uint64("bytes", event.Bytes),
	)
	operations.auditMu.RLock()
	auditor := operations.auditor
	operations.auditMu.RUnlock()
	featureops.Emit(auditor, event)
}

// SetAuditor installs the durable identity-free journal after the control
// database has opened. Runtime setup calls it before feature services start.
func (operations *runtimeFeatureOperations) SetAuditor(auditor featureops.Observer) {
	if operations == nil {
		return
	}
	operations.auditMu.Lock()
	operations.auditor = auditor
	operations.auditMu.Unlock()
}

func (operations *runtimeFeatureOperations) Snapshot() featureops.Snapshot {
	if operations == nil {
		return featureops.Snapshot{}
	}
	return operations.metrics.Snapshot()
}
