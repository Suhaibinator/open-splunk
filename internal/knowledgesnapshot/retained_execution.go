package knowledgesnapshot

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

// RetainedExecutionAuthorityFacts is the opaque, fixed-size execution identity
// minted with one finalized immutable Snapshot. It deliberately exposes no
// variable-size snapshot, encoding, summary, or program storage.
type RetainedExecutionAuthorityFacts struct {
	valid                 bool
	snapshotDigest        [sha256.Size]byte
	encodedDigest         [sha256.Size]byte
	encodedBytes          uint64
	preludeCommitment     [sha256.Size]byte
	preludeObjectCount    uint32
	preludeCharges        knowledgeprogram.Charges
	generatedOperators    uint32
	generatedFields       uint32
	regexPrograms         uint32
	regexWorkUnits        uint64
	regexCaptureBytes     uint64
	scalarExpressions     uint32
	scalarExpressionNodes uint32
	generatedSQLBytes     uint64
}

// SnapshotDigest returns the canonical snapshot commitment.
func (facts RetainedExecutionAuthorityFacts) SnapshotDigest() [sha256.Size]byte {
	return facts.snapshotDigest
}

// EncodedDigest returns the commitment to the exact final deterministic wire
// encoding, including the embedded snapshot digest.
func (facts RetainedExecutionAuthorityFacts) EncodedDigest() [sha256.Size]byte {
	return facts.encodedDigest
}

// MatchesPreludeAuthority reports exact equality with the immutable program
// identity used when the snapshot facts were minted.
func (facts RetainedExecutionAuthorityFacts) MatchesPreludeAuthority(
	commitment [sha256.Size]byte,
	objectCount uint32,
	charges knowledgeprogram.Charges,
) bool {
	return facts.valid && facts.preludeCommitment == commitment &&
		facts.preludeObjectCount == objectCount && facts.preludeCharges == charges
}

// MatchesRetainedCompilerBudget reports exact equality with the eight
// compiler counters retained on the snapshot wire authority.
func (facts RetainedExecutionAuthorityFacts) MatchesRetainedCompilerBudget(
	generatedOperators uint32,
	generatedFields uint32,
	regexPrograms uint32,
	regexWorkUnits uint64,
	regexCaptureBytes uint64,
	scalarExpressions uint32,
	scalarExpressionNodes uint32,
	generatedSQLBytes uint64,
) bool {
	return facts.valid &&
		facts.generatedOperators == generatedOperators &&
		facts.generatedFields == generatedFields &&
		facts.regexPrograms == regexPrograms &&
		facts.regexWorkUnits == regexWorkUnits &&
		facts.regexCaptureBytes == regexCaptureBytes &&
		facts.scalarExpressions == scalarExpressions &&
		facts.scalarExpressionNodes == scalarExpressionNodes &&
		facts.generatedSQLBytes == generatedSQLBytes
}

// ValidateRetainedExecutionAuthority compares one expected execution scope
// directly with private immutable snapshot state and returns only the facts
// minted during finalization. It never clones or returns Proto, Encoded,
// Summary, or Prelude storage.
func (snapshot Snapshot) ValidateRetainedExecutionAuthority(
	tenantID string,
	principalID string,
	appID string,
	effectiveIndexes []string,
) (RetainedExecutionAuthorityFacts, bool) {
	facts := snapshot.retainedExecutionFacts
	message := snapshot.message
	if message == nil || !facts.valid ||
		message.GetFormatVersion() != FormatVersion ||
		message.GetCompilerCompatibilityVersion() != CompilerCompatibilityVersion ||
		message.GetTenantId() != tenantID ||
		message.GetPrincipalId() != principalID ||
		message.GetAppId() == "" || message.GetAppId() != appID ||
		!slices.Equal(message.GetEffectiveAuthorizedIndexes(), effectiveIndexes) ||
		message.GetBudgetCharges() == nil ||
		snapshot.digest != facts.snapshotDigest ||
		!bytes.Equal(message.GetSnapshotSha256(), facts.snapshotDigest[:]) ||
		uint64(len(snapshot.encoded)) != facts.encodedBytes {
		return RetainedExecutionAuthorityFacts{}, false
	}
	commitment, commitmentOK := snapshot.prelude.Commitment()
	if !commitmentOK ||
		!facts.MatchesPreludeAuthority(
			commitment,
			snapshot.prelude.ObjectCount(),
			snapshot.prelude.Charges(),
		) {
		return RetainedExecutionAuthorityFacts{}, false
	}
	budget := message.GetBudgetCharges()
	if !facts.MatchesRetainedCompilerBudget(
		budget.GetGeneratedOperators(),
		budget.GetGeneratedFields(),
		budget.GetRegexPrograms(),
		budget.GetRegexWorkUnits(),
		budget.GetRegexCaptureBytes(),
		budget.GetScalarExpressions(),
		budget.GetScalarExpressionNodes(),
		budget.GetGeneratedSqlBytes(),
	) {
		return RetainedExecutionAuthorityFacts{}, false
	}
	return facts, true
}

func mintRetainedExecutionAuthorityFacts(
	snapshot Snapshot,
) (RetainedExecutionAuthorityFacts, error) {
	message := snapshot.message
	if message == nil || message.GetBudgetCharges() == nil ||
		message.GetFormatVersion() != FormatVersion ||
		message.GetCompilerCompatibilityVersion() != CompilerCompatibilityVersion ||
		message.GetAppId() == "" || snapshot.prelude.IsZero() ||
		len(snapshot.encoded) == 0 ||
		!bytes.Equal(message.GetSnapshotSha256(), snapshot.digest[:]) {
		return RetainedExecutionAuthorityFacts{}, fmt.Errorf(
			"%w: finalized retained execution authority is incomplete",
			ErrInvalidInput,
		)
	}
	summary := snapshot.Summary()
	if err := ValidateSummary(summary); err != nil {
		return RetainedExecutionAuthorityFacts{}, fmt.Errorf(
			"validate finalized retained execution summary: %w",
			err,
		)
	}
	commitment, ok := snapshot.prelude.Commitment()
	if !ok {
		return RetainedExecutionAuthorityFacts{}, fmt.Errorf(
			"%w: finalized knowledge prelude commitment is absent",
			ErrInvalidInput,
		)
	}
	budget := message.GetBudgetCharges()
	facts := RetainedExecutionAuthorityFacts{
		valid:                 true,
		snapshotDigest:        snapshot.digest,
		encodedDigest:         sha256.Sum256(snapshot.encoded),
		encodedBytes:          uint64(len(snapshot.encoded)),
		preludeCommitment:     commitment,
		preludeObjectCount:    snapshot.prelude.ObjectCount(),
		preludeCharges:        snapshot.prelude.Charges(),
		generatedOperators:    budget.GetGeneratedOperators(),
		generatedFields:       budget.GetGeneratedFields(),
		regexPrograms:         budget.GetRegexPrograms(),
		regexWorkUnits:        budget.GetRegexWorkUnits(),
		regexCaptureBytes:     budget.GetRegexCaptureBytes(),
		scalarExpressions:     budget.GetScalarExpressions(),
		scalarExpressionNodes: budget.GetScalarExpressionNodes(),
		generatedSQLBytes:     budget.GetGeneratedSqlBytes(),
	}
	snapshot.retainedExecutionFacts = facts
	if _, valid := snapshot.ValidateRetainedExecutionAuthority(
		message.GetTenantId(),
		message.GetPrincipalId(),
		message.GetAppId(),
		message.GetEffectiveAuthorizedIndexes(),
	); !valid {
		return RetainedExecutionAuthorityFacts{}, fmt.Errorf(
			"%w: finalized retained execution facts are invalid",
			ErrInvalidInput,
		)
	}
	return facts, nil
}
