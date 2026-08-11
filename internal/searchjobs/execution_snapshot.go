package searchjobs

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
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
	CompilerVersion  string
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

// RetainedKnowledgeExecution is the fully detached compiler, summary, and
// backend-neutral prelude authority retained for one knowledge-enabled
// execution.
type RetainedKnowledgeExecution struct {
	CompiledQuery    clickhouse.CompiledQuery
	KnowledgeSummary *opensplunkv1.KnowledgeSnapshotSummary
	KnowledgePrelude knowledgeprogram.Program
}

// RetainedKnowledgeAuthorityDigests is the fixed-size identity of the exact
// retained knowledge pair committed by one Manager-minted execution. Present
// distinguishes a valid legacy execution from a knowledge-enabled execution,
// including an enabled execution whose program contains no objects. The two
// digests are zero when Present is false.
type RetainedKnowledgeAuthorityDigests struct {
	Present        bool
	SnapshotDigest [sha256.Size]byte
	CompiledDigest [sha256.Size]byte
}

type knowledgeExecutionAuthorityFacts struct {
	digests       RetainedKnowledgeAuthorityDigests
	snapshotFacts knowledgesnapshot.RetainedExecutionAuthorityFacts
}

type retainedKnowledgeAuthorityValidation struct {
	digests  RetainedKnowledgeAuthorityDigests
	compiled clickhouse.CompiledQuery
	summary  *opensplunkv1.KnowledgeSnapshotSummary
	prelude  knowledgeprogram.Program
}

type retainedKnowledgePayloadMode uint8

const (
	retainedKnowledgePayloadNone retainedKnowledgePayloadMode = iota
	retainedKnowledgePayloadPrelude
	retainedKnowledgePayloadExecution
)

// ValidateRetainedKnowledgeAuthority verifies the Manager signature and the
// complete retained knowledge contract without returning variable-size
// execution, summary, or program payloads. A valid legacy execution returns a
// zero-digest result with Present false. Invalid, incomplete, incompatible, or
// internally inconsistent authority fails closed.
func (snapshot ExecutionSnapshot) ValidateRetainedKnowledgeAuthority() (
	RetainedKnowledgeAuthorityDigests,
	error,
) {
	validated, err := snapshot.validateRetainedKnowledgeAuthority(
		retainedKnowledgePayloadNone,
	)
	if err != nil {
		return RetainedKnowledgeAuthorityDigests{}, err
	}
	return validated.digests, nil
}

// OpenRetainedKnowledgeExecution verifies and opens the exact knowledge
// authority committed by this manager-minted execution snapshot. A nil result
// with a nil error identifies a valid legacy execution with no retained
// knowledge authority. Invalid, incomplete, or internally inconsistent
// authorities fail closed.
func (snapshot ExecutionSnapshot) OpenRetainedKnowledgeExecution() (*RetainedKnowledgeExecution, error) {
	validated, err := snapshot.validateRetainedKnowledgeAuthority(
		retainedKnowledgePayloadExecution,
	)
	if err != nil {
		return nil, err
	}
	if !validated.digests.Present {
		return nil, nil
	}
	return &RetainedKnowledgeExecution{
		CompiledQuery:    validated.compiled,
		KnowledgeSummary: validated.summary,
		KnowledgePrelude: validated.prelude,
	}, nil
}

func (snapshot ExecutionSnapshot) validateRetainedKnowledgeAuthority(
	payloadMode retainedKnowledgePayloadMode,
) (retainedKnowledgeAuthorityValidation, error) {
	facts, ok := snapshot.validatedKnowledgeAuthoritySeal()
	if !ok {
		return retainedKnowledgeAuthorityValidation{}, ErrResultsUnavailable
	}
	validated := retainedKnowledgeAuthorityValidation{digests: facts.digests}
	if !facts.digests.Present {
		return validated, nil
	}
	if payloadMode == retainedKnowledgePayloadNone {
		return validated, nil
	}
	prelude := snapshot.KnowledgeSnapshot.Prelude()
	commitment, commitmentOK := prelude.Commitment()
	if !commitmentOK || !facts.snapshotFacts.MatchesPreludeAuthority(
		commitment,
		prelude.ObjectCount(),
		prelude.Charges(),
	) {
		return retainedKnowledgeAuthorityValidation{}, ErrResultsUnavailable
	}
	validated.prelude = prelude
	if payloadMode == retainedKnowledgePayloadPrelude {
		return validated, nil
	}
	if payloadMode != retainedKnowledgePayloadExecution {
		return retainedKnowledgeAuthorityValidation{}, ErrResultsUnavailable
	}
	compiled, ok := snapshot.CompiledQuery.CloneForExecution()
	if !ok {
		return retainedKnowledgeAuthorityValidation{}, ErrResultsUnavailable
	}
	compiledDigest, ok := compiled.ExecutionAuthorityDigest()
	if !ok || compiledDigest != facts.digests.CompiledDigest {
		return retainedKnowledgeAuthorityValidation{}, ErrResultsUnavailable
	}
	evidence, ok := compiled.KnowledgeSnapshotEvidenceFor(prelude)
	if !ok || evidence.TenantID() != snapshot.TenantID ||
		!slices.Equal(evidence.EffectiveIndexes(), snapshot.EffectiveIndexes) ||
		!knowledgeCompilerEvidenceMatches(facts.snapshotFacts, evidence) {
		return retainedKnowledgeAuthorityValidation{}, ErrResultsUnavailable
	}
	summary := snapshot.KnowledgeSnapshot.Summary()
	if err := knowledgesnapshot.ValidateSummary(summary); err != nil {
		return retainedKnowledgeAuthorityValidation{}, ErrResultsUnavailable
	}
	validated.compiled = compiled
	validated.summary = summary
	return validated, nil
}

// OpenRetainedKnowledgePrelude verifies and opens the exact backend-neutral
// knowledge program retained for this execution. Every snapshot is delegated
// to the complete retained-execution boundary, so unsigned values fail closed
// while a valid manager-sealed legacy execution returns an absent program. The
// returned program is detached.
func (snapshot ExecutionSnapshot) OpenRetainedKnowledgePrelude() (
	knowledgeprogram.Program,
	bool,
	error,
) {
	validated, err := snapshot.validateRetainedKnowledgeAuthority(
		retainedKnowledgePayloadPrelude,
	)
	if err != nil {
		return knowledgeprogram.Program{}, false, err
	}
	if !validated.digests.Present {
		return knowledgeprogram.Program{}, false, nil
	}
	if validated.prelude.IsZero() {
		return knowledgeprogram.Program{}, false, ErrResultsUnavailable
	}
	return validated.prelude, true, nil
}

func knowledgeCompilerEvidenceMatches(
	facts knowledgesnapshot.RetainedExecutionAuthorityFacts,
	evidence clickhouse.KnowledgeSnapshotEvidence,
) bool {
	commitment, commitmentOK := evidence.KnowledgeProgramCommitment()
	return evidence.KnowledgeProgramPresent() && commitmentOK &&
		facts.MatchesPreludeAuthority(
			commitment,
			evidence.KnowledgeProgramObjectCount(),
			evidence.KnowledgeProgramCharges(),
		) &&
		facts.MatchesRetainedCompilerBudget(
			evidence.GeneratedOperators(),
			evidence.GeneratedFields(),
			evidence.RegexPrograms(),
			evidence.RegexWorkUnits(),
			evidence.RegexCaptureBytes(),
			evidence.ScalarExpressions(),
			evidence.ScalarExpressionNodes(),
			evidence.GeneratedSQLBytes(),
		)
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
		snapshot.CompilerVersion == other.CompilerVersion &&
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
		CompilerVersion:   strings.Clone(entry.job.CompilerVersion),
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
