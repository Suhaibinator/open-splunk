package searchjobs

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
)

// ExecutionSnapshot is the immutable execution scope retained for a
// successfully completed search. It is detached from Manager storage and does
// not pin the job's retained result rows or schema.
type ExecutionSnapshot struct {
	ID               string
	OwnerID          string
	TenantID         string
	AppID            string
	SPL              string
	EffectiveIndexes []string
	Earliest         time.Time
	Latest           time.Time
	SearchStart      time.Time
	SearchTimezone   string
	IndexTimeCutoff  time.Time
	VisibilityCutoff uint64
	// CompiledQuery is the detached compiler-sealed executable authority. It is
	// present exactly when KnowledgeSnapshot is nonzero and lets postflight
	// consumers avoid recompilation drift after upgrades.
	CompiledQuery *clickhouse.CompiledQuery
	// KnowledgeSnapshot is the opaque finalized authority consumed by this
	// exact execution. Its zero value identifies the legacy knowledge-disabled
	// path; value copies remain immutable and all accessors detach.
	KnowledgeSnapshot knowledgesnapshot.Snapshot
	FinishedAt        time.Time
	ExpiresAt         time.Time

	// knowledgeAuthoritySeal is present on every manager-minted execution. It
	// commits an explicit knowledge-enabled bit, the public tuple above, and the
	// exact completed result generation without exposing a construction seam.
	knowledgeAuthoritySeal knowledgeExecutionAuthoritySeal
}

// RetainedKnowledgeExecution is the fully detached compiler and summary
// authority retained for one knowledge-enabled execution.
type RetainedKnowledgeExecution struct {
	CompiledQuery    clickhouse.CompiledQuery
	KnowledgeSummary *opensplunkv1.KnowledgeSnapshotSummary
}

// OpenRetainedKnowledgeExecution verifies and opens the exact knowledge
// authority committed by this manager-minted execution snapshot. A nil result
// with a nil error identifies a valid legacy execution with no retained
// knowledge authority. Invalid, incomplete, or internally inconsistent
// authorities fail closed.
func (snapshot ExecutionSnapshot) OpenRetainedKnowledgeExecution() (*RetainedKnowledgeExecution, error) {
	if !snapshot.ValidKnowledgeAuthority() {
		return nil, ErrResultsUnavailable
	}
	if snapshot.KnowledgeSnapshot.IsZero() {
		if snapshot.CompiledQuery != nil {
			return nil, ErrResultsUnavailable
		}
		return nil, nil
	}
	if snapshot.CompiledQuery == nil {
		return nil, ErrResultsUnavailable
	}

	authority := snapshot.KnowledgeSnapshot.Proto()
	if authority == nil ||
		authority.GetFormatVersion() != knowledgesnapshot.FormatVersion ||
		authority.GetCompilerCompatibilityVersion() !=
			knowledgesnapshot.CompilerCompatibilityVersion ||
		authority.GetTenantId() != snapshot.TenantID ||
		authority.GetPrincipalId() != snapshot.OwnerID ||
		authority.GetAppId() == "" ||
		authority.GetAppId() != snapshot.AppID ||
		!slices.Equal(
			authority.GetEffectiveAuthorizedIndexes(),
			snapshot.EffectiveIndexes,
		) ||
		authority.GetBudgetCharges() == nil {
		return nil, ErrResultsUnavailable
	}
	digest := snapshot.KnowledgeSnapshot.Digest()
	if !bytes.Equal(authority.GetSnapshotSha256(), digest[:]) {
		return nil, ErrResultsUnavailable
	}

	compiled, ok := snapshot.CompiledQuery.CloneForExecution()
	if !ok {
		return nil, ErrResultsUnavailable
	}
	evidence, ok := compiled.KnowledgeSnapshotEvidence()
	if !ok || evidence.TenantID() != snapshot.TenantID ||
		!slices.Equal(evidence.EffectiveIndexes(), snapshot.EffectiveIndexes) ||
		!knowledgeCompilerEvidenceMatches(
			authority.GetBudgetCharges(),
			evidence,
		) {
		return nil, ErrResultsUnavailable
	}
	summary, err := knowledgesnapshot.CloneSummary(snapshot.KnowledgeSnapshot.Summary())
	if err != nil {
		return nil, ErrResultsUnavailable
	}
	return &RetainedKnowledgeExecution{
		CompiledQuery:    compiled,
		KnowledgeSummary: summary,
	}, nil
}

func knowledgeCompilerEvidenceMatches(
	charges *opensplunkv1.KnowledgeSnapshotBudgetCharges,
	evidence clickhouse.KnowledgeSnapshotEvidence,
) bool {
	return charges != nil &&
		charges.GetGeneratedOperators() == evidence.GeneratedOperators() &&
		charges.GetGeneratedFields() == evidence.GeneratedFields() &&
		charges.GetRegexPrograms() == evidence.RegexPrograms() &&
		charges.GetRegexWorkUnits() == evidence.RegexWorkUnits() &&
		charges.GetRegexCaptureBytes() == evidence.RegexCaptureBytes() &&
		charges.GetScalarExpressions() == evidence.ScalarExpressions() &&
		charges.GetScalarExpressionNodes() == evidence.ScalarExpressionNodes() &&
		charges.GetGeneratedSqlBytes() == evidence.GeneratedSQLBytes()
}

// Equal reports whether other identifies the same immutable completed-search
// execution. EffectiveIndexes is ordered. Timestamps compare by instant only
// after both exact manager-signed public tuples have validated.
func (snapshot ExecutionSnapshot) Equal(other ExecutionSnapshot) bool {
	if !snapshot.ValidKnowledgeAuthority() || !other.ValidKnowledgeAuthority() {
		return false
	}
	equal := snapshot.ID == other.ID &&
		snapshot.OwnerID == other.OwnerID &&
		snapshot.TenantID == other.TenantID &&
		snapshot.AppID == other.AppID &&
		snapshot.SPL == other.SPL &&
		slices.Equal(snapshot.EffectiveIndexes, other.EffectiveIndexes) &&
		snapshot.Earliest.Equal(other.Earliest) &&
		snapshot.Latest.Equal(other.Latest) &&
		snapshot.SearchStart.Equal(other.SearchStart) &&
		snapshot.SearchTimezone == other.SearchTimezone &&
		snapshot.IndexTimeCutoff.Equal(other.IndexTimeCutoff) &&
		snapshot.VisibilityCutoff == other.VisibilityCutoff &&
		equalCompiledExecution(snapshot.CompiledQuery, other.CompiledQuery) &&
		snapshot.KnowledgeSnapshot.Equal(other.KnowledgeSnapshot) &&
		snapshot.FinishedAt.Equal(other.FinishedAt) &&
		snapshot.ExpiresAt.Equal(other.ExpiresAt)
	return equal && snapshot.knowledgeAuthoritySeal.equal(other.knowledgeAuthoritySeal)
}

// CompletedExecutionSnapshotFor returns the detached execution scope of a
// completed, unexpired job owned by access. Unlike AcquireResultsFor, this
// metadata-only read does not consume result-lease capacity or extend the
// lifetime of retained result storage.
func (manager *Manager) CompletedExecutionSnapshotFor(ctx context.Context, access AccessScope, id string) (ExecutionSnapshot, error) {
	if ctx == nil {
		return ExecutionSnapshot{}, errors.New("read completed search execution snapshot: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return ExecutionSnapshot{}, err
	}
	if !validAccessScope(access) {
		return ExecutionSnapshot{}, ErrNotFound
	}

	// Retain manager.mu until entry.mu is acquired so shutdown and tombstone
	// removal are ordered with this read. This is the manager -> entry lock
	// order also used by result-lease admission.
	manager.mu.RLock()
	if manager.closed {
		manager.mu.RUnlock()
		return ExecutionSnapshot{}, ErrClosed
	}
	entry := manager.jobs[id]
	if entry == nil {
		manager.mu.RUnlock()
		return ExecutionSnapshot{}, ErrNotFound
	}
	entry.mu.Lock()
	manager.mu.RUnlock()
	defer entry.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return ExecutionSnapshot{}, err
	}
	if entry.job.TenantID != access.TenantID || entry.job.OwnerID != access.OwnerID {
		return ExecutionSnapshot{}, ErrNotFound
	}
	now := manager.nowUTC()
	if canExpireLocked(entry, now) {
		manager.expireLocked(entry, now)
	}
	switch entry.job.State {
	case StateCompleted:
		// Continue below.
	case StateExpired:
		return ExecutionSnapshot{}, ErrExpired
	case StateFailed, StateCanceled:
		return ExecutionSnapshot{}, ErrResultsUnavailable
	default:
		return ExecutionSnapshot{}, ErrResultsNotReady
	}

	snapshot, _, err := manager.executionSnapshotLocked(entry)
	if err != nil {
		return ExecutionSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return ExecutionSnapshot{}, err
	}
	return snapshot, nil
}

// executionSnapshotLocked detaches the complete compiler and scope authority
// while the caller holds entry.mu. A finalized knowledge snapshot and retained
// compiled query are an indivisible pair; any partial or tampered pair fails
// closed before a result pin can be admitted.
func (manager *Manager) executionSnapshotLocked(
	entry *jobEntry,
) (ExecutionSnapshot, executionResultAuthority, error) {
	if entry == nil {
		return ExecutionSnapshot{}, executionResultAuthority{}, ErrResultsUnavailable
	}
	snapshot := ExecutionSnapshot{
		ID:                strings.Clone(entry.job.ID),
		OwnerID:           strings.Clone(entry.job.OwnerID),
		TenantID:          strings.Clone(entry.job.TenantID),
		AppID:             strings.Clone(entry.job.AppID),
		SPL:               strings.Clone(entry.job.SPL),
		EffectiveIndexes:  cloneStrings(entry.job.EffectiveIndexes),
		Earliest:          entry.job.Earliest,
		Latest:            entry.job.Latest,
		SearchStart:       entry.job.CreatedAt,
		SearchTimezone:    strings.Clone(entry.job.TimeRange.Timezone),
		IndexTimeCutoff:   entry.job.IndexTimeCutoff,
		VisibilityCutoff:  entry.job.VisibilityCutoff,
		KnowledgeSnapshot: entry.knowledgeSnapshot,
		FinishedAt:        entry.job.FinishedAt,
		ExpiresAt:         entry.job.ExpiresAt,
	}
	hasKnowledge := !entry.knowledgeSnapshot.IsZero()
	if (entry.preparedCompiled != nil) != hasKnowledge {
		return ExecutionSnapshot{}, executionResultAuthority{}, ErrResultsUnavailable
	}
	if entry.resultSchema == nil || entry.resultGeneration == 0 ||
		uint64(len(entry.rows)) != entry.job.RowCount {
		return ExecutionSnapshot{}, executionResultAuthority{}, ErrResultsUnavailable
	}
	if hasKnowledge {
		compiled, ok := entry.preparedCompiled.CloneForExecution()
		if !ok {
			return ExecutionSnapshot{}, executionResultAuthority{}, ErrResultsUnavailable
		}
		snapshot.CompiledQuery = &compiled
	}
	resultAuthority, ok := manager.sealExecutionSnapshot(
		&snapshot,
		executionResultMetadata{
			jobID:            snapshot.ID,
			generation:       entry.resultGeneration,
			schema:           *entry.resultSchema,
			rowCount:         uint64(len(entry.rows)),
			resultsTruncated: entry.job.ResultsTruncated,
		},
	)
	if !ok {
		return ExecutionSnapshot{}, executionResultAuthority{}, ErrResultsUnavailable
	}
	return snapshot, resultAuthority, nil
}

func equalCompiledExecution(left, right *clickhouse.CompiledQuery) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.EqualForExecution(*right)
}
