package searchjobs

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"hash"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
)

const (
	knowledgeExecutionSigningKeyDomain  = "open-splunk-search-job-knowledge-signing-key-v1"
	knowledgeExecutionAuthorityDomain   = "open-splunk-search-job-knowledge-execution-authority-v3"
	knowledgeExecutionResultDomain      = "open-splunk-search-job-result-generation-v1"
	knowledgeExecutionResultNonceDomain = "open-splunk-search-job-result-generation-nonce-v1"
)

// knowledgeExecutionAuthoritySeal is a manager signature over the complete
// immutable execution and result-generation authority. Keeping this field
// private prevents callers from attaching a valid seal to a constructed
// ExecutionSnapshot; the public verification key makes disclosure of a value
// harmless and avoids retaining manager secret material in detached results.
type knowledgeExecutionAuthoritySeal struct {
	knowledgeEnabled bool
	publicKey        [ed25519.PublicKeySize]byte
	signature        [ed25519.SignatureSize]byte
	authorityDigest  [sha256.Size]byte
	resultDigest     [sha256.Size]byte
}

// executionResultMetadata is the complete immutable public result-generation
// tuple committed by an execution snapshot. The private nonce below turns this
// tuple into manager-specific lease authority without making ordinary result
// leases retain job identity or perform cryptographic work.
type executionResultMetadata struct {
	jobID            string
	generation       uint64
	schema           Schema
	rowCount         uint64
	resultsTruncated bool
}

type executionResultAuthority struct {
	metadata executionResultMetadata
	nonce    [sha256.Size]byte
}

// ValidatedResultMetadata is the immutable result-generation tuple opened from
// a manager-owned result lease after its private authority has been verified.
type ValidatedResultMetadata struct {
	Schema           Schema
	Generation       uint64
	RowCount         uint64
	RowCountExact    bool
	ResultsTruncated bool
}

func deriveKnowledgeExecutionSigningKey(
	cursorKey []byte,
	cursorScope string,
	listCursorEpoch string,
) ed25519.PrivateKey {
	digest := hmac.New(sha256.New, cursorKey)
	writeKnowledgeSealString(digest, knowledgeExecutionSigningKeyDomain)
	writeKnowledgeSealString(digest, cursorScope)
	writeKnowledgeSealString(digest, listCursorEpoch)
	var seed [ed25519.SeedSize]byte
	digest.Sum(seed[:0])
	return ed25519.NewKeyFromSeed(seed[:])
}

// ValidKnowledgeAuthority reports whether the complete public execution tuple
// still matches its manager-minted signature. The signed knowledge-enabled
// bit distinguishes a legacy zero/nil pair from an enabled pair, so a caller
// cannot downgrade retained knowledge authority by stripping public fields.
// Result-generation metadata is committed here and matched to an acquired pin
// by ValidFor.
func (snapshot ExecutionSnapshot) ValidKnowledgeAuthority() bool {
	_, valid := snapshot.validatedKnowledgeAuthoritySeal()
	return valid
}

func (snapshot ExecutionSnapshot) validatedKnowledgeAuthoritySeal() (
	knowledgeExecutionAuthorityFacts,
	bool,
) {
	facts, ok := retainedKnowledgeAuthorityFacts(snapshot)
	if !ok || snapshot.knowledgeAuthoritySeal.isZero() ||
		snapshot.knowledgeAuthoritySeal.knowledgeEnabled != facts.digests.Present {
		return knowledgeExecutionAuthorityFacts{}, false
	}
	expected, ok := knowledgeExecutionAuthorityDigest(
		snapshot,
		snapshot.knowledgeAuthoritySeal.resultDigest,
		facts,
	)
	if !ok || !snapshot.knowledgeAuthoritySeal.validates(expected) {
		return knowledgeExecutionAuthorityFacts{}, false
	}
	return facts, true
}

// ValidFor verifies that an acquired result pin is the exact generation,
// schema, row count, and completeness state committed by this execution
// snapshot. Both legacy and knowledge-enabled snapshots require the manager
// signature and an exact pin commitment match.
func (snapshot ExecutionSnapshot) ValidFor(lease ResultLease) (valid bool) {
	_, valid = snapshot.ValidatedResultLease(lease)
	return valid
}

// ValidatedResultLease atomically opens the immutable result metadata of a
// manager-owned pin and verifies its private nonce against this execution
// snapshot. Arbitrary ResultLease implementations cannot satisfy the private
// attestation interface, and consumers never need to perform repeated public
// getter calls that could observe inconsistent values.
func (snapshot ExecutionSnapshot) ValidatedResultLease(
	lease ResultLease,
) (ValidatedResultMetadata, bool) {
	if !snapshot.ValidKnowledgeAuthority() {
		return ValidatedResultMetadata{}, false
	}
	sealed, ok := lease.(sealedExecutionResultLease)
	if !ok {
		return ValidatedResultMetadata{}, false
	}
	resultAuthority, ok := sealed.sealedExecutionResultLease()
	metadata := resultAuthority.metadata
	if !ok || metadata.generation == 0 || metadata.jobID != snapshot.ID {
		return ValidatedResultMetadata{}, false
	}
	expected := knowledgeExecutionResultDigest(resultAuthority)
	if subtle.ConstantTimeCompare(
		expected[:],
		snapshot.knowledgeAuthoritySeal.resultDigest[:],
	) != 1 {
		return ValidatedResultMetadata{}, false
	}
	return ValidatedResultMetadata{
		Schema:           cloneSchema(metadata.schema),
		Generation:       metadata.generation,
		RowCount:         metadata.rowCount,
		RowCountExact:    true,
		ResultsTruncated: metadata.resultsTruncated,
	}, true
}

func (manager *Manager) sealExecutionSnapshot(
	snapshot *ExecutionSnapshot,
	resultMetadata executionResultMetadata,
) (executionResultAuthority, bool) {
	if snapshot == nil || len(manager.knowledgeExecutionSigner) != ed25519.PrivateKeySize ||
		!snapshot.knowledgeAuthoritySeal.isZero() || resultMetadata.jobID == "" ||
		resultMetadata.jobID != snapshot.ID || resultMetadata.generation == 0 {
		return executionResultAuthority{}, false
	}
	facts, ok := retainedKnowledgeAuthorityFacts(*snapshot)
	if !ok || (facts.digests.Present &&
		!validRetainedKnowledgeExecutionPair(*snapshot, facts.snapshotFacts)) {
		return executionResultAuthority{}, false
	}
	resultAuthority := manager.mintExecutionResultAuthority(resultMetadata)
	resultDigest := knowledgeExecutionResultDigest(resultAuthority)
	authorityDigest, ok := knowledgeExecutionAuthorityDigest(
		*snapshot,
		resultDigest,
		facts,
	)
	if !ok {
		return executionResultAuthority{}, false
	}
	publicKey, ok := manager.knowledgeExecutionSigner.Public().(ed25519.PublicKey)
	if !ok || len(publicKey) != ed25519.PublicKeySize {
		return executionResultAuthority{}, false
	}
	signature := ed25519.Sign(manager.knowledgeExecutionSigner, authorityDigest[:])
	if len(signature) != ed25519.SignatureSize {
		return executionResultAuthority{}, false
	}
	seal := knowledgeExecutionAuthoritySeal{
		knowledgeEnabled: facts.digests.Present,
		authorityDigest:  authorityDigest,
		resultDigest:     resultDigest,
	}
	copy(seal.publicKey[:], publicKey)
	copy(seal.signature[:], signature)
	snapshot.knowledgeAuthoritySeal = seal
	return resultAuthority, seal.validates(authorityDigest)
}

func (manager *Manager) mintExecutionResultAuthority(
	metadata executionResultMetadata,
) executionResultAuthority {
	metadata = cloneExecutionResultMetadata(metadata)
	return executionResultAuthority{
		metadata: metadata,
		nonce:    manager.executionResultLeaseNonce(metadata),
	}
}

func cloneExecutionResultMetadata(metadata executionResultMetadata) executionResultMetadata {
	metadata.jobID = strings.Clone(metadata.jobID)
	metadata.schema = cloneSchema(metadata.schema)
	return metadata
}

func cloneExecutionResultAuthority(authority executionResultAuthority) executionResultAuthority {
	authority.metadata = cloneExecutionResultMetadata(authority.metadata)
	return authority
}

func knowledgeExecutionAuthorityDigest(
	snapshot ExecutionSnapshot,
	resultDigest [sha256.Size]byte,
	facts knowledgeExecutionAuthorityFacts,
) ([sha256.Size]byte, bool) {
	if !ValidCompilerVersion(snapshot.CompilerVersion) {
		return [sha256.Size]byte{}, false
	}
	hasKnowledge := !snapshot.KnowledgeSnapshot.IsZero()
	if facts.digests.Present != hasKnowledge ||
		hasKnowledge != (snapshot.CompiledQuery != nil) {
		return [sha256.Size]byte{}, false
	}

	digest := sha256.New()
	writeKnowledgeSealString(digest, knowledgeExecutionAuthorityDomain)
	writeKnowledgeSealString(digest, snapshot.ID)
	writeKnowledgeSealString(digest, snapshot.OwnerID)
	writeKnowledgeSealString(digest, snapshot.TenantID)
	writeKnowledgeSealString(digest, snapshot.AppID)
	writeKnowledgeSealString(digest, snapshot.SPL)
	writeKnowledgeSealString(digest, snapshot.CompilerVersion)
	writeKnowledgeSealStrings(digest, snapshot.EffectiveIndexes)
	if !writeKnowledgeSealTime(digest, snapshot.Earliest) ||
		!writeKnowledgeSealTime(digest, snapshot.Latest) ||
		!writeKnowledgeSealTime(digest, snapshot.SearchStart) {
		return [sha256.Size]byte{}, false
	}
	writeKnowledgeSealString(digest, snapshot.SearchTimezone)
	if !writeKnowledgeSealTime(digest, snapshot.IndexTimeCutoff) {
		return [sha256.Size]byte{}, false
	}
	writeKnowledgeSealUint64(digest, snapshot.VisibilityCutoff)
	if !writeKnowledgeSealTime(digest, snapshot.FinishedAt) ||
		!writeKnowledgeSealTime(digest, snapshot.ExpiresAt) {
		return [sha256.Size]byte{}, false
	}
	writeKnowledgeSealBool(digest, hasKnowledge)
	if hasKnowledge {
		writeKnowledgeSealString(digest, snapshot.AppID)
		writeKnowledgeSealBytes(digest, facts.digests.CompiledDigest[:])
		writeKnowledgeSealBytes(digest, facts.digests.SnapshotDigest[:])
		encodedDigest := facts.snapshotFacts.EncodedDigest()
		writeKnowledgeSealBytes(digest, encodedDigest[:])
	}
	writeKnowledgeSealBytes(digest, resultDigest[:])
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result, true
}

func retainedKnowledgeAuthorityFacts(
	snapshot ExecutionSnapshot,
) (knowledgeExecutionAuthorityFacts, bool) {
	hasKnowledge := !snapshot.KnowledgeSnapshot.IsZero()
	if hasKnowledge != (snapshot.CompiledQuery != nil) {
		return knowledgeExecutionAuthorityFacts{}, false
	}
	facts := knowledgeExecutionAuthorityFacts{
		digests: RetainedKnowledgeAuthorityDigests{Present: hasKnowledge},
	}
	if !hasKnowledge {
		return facts, true
	}
	snapshotFacts, ok := snapshot.KnowledgeSnapshot.ValidateRetainedExecutionAuthority(
		snapshot.TenantID,
		snapshot.OwnerID,
		snapshot.AppID,
		snapshot.EffectiveIndexes,
	)
	if !ok {
		return knowledgeExecutionAuthorityFacts{}, false
	}
	compiledDigest, ok := snapshot.CompiledQuery.ExecutionAuthorityDigest()
	if !ok {
		return knowledgeExecutionAuthorityFacts{}, false
	}
	facts.snapshotFacts = snapshotFacts
	facts.digests.SnapshotDigest = snapshotFacts.SnapshotDigest()
	facts.digests.CompiledDigest = compiledDigest
	return facts, true
}

func validRetainedKnowledgeExecutionPair(
	snapshot ExecutionSnapshot,
	facts knowledgesnapshot.RetainedExecutionAuthorityFacts,
) bool {
	if snapshot.CompiledQuery == nil {
		return false
	}
	evidence, ok := snapshot.CompiledQuery.KnowledgeSnapshotEvidence()
	return ok && evidence.TenantID() == snapshot.TenantID &&
		slices.Equal(evidence.EffectiveIndexes(), snapshot.EffectiveIndexes) &&
		knowledgeCompilerEvidenceMatches(facts, evidence)
}

func knowledgeExecutionResultDigest(authority executionResultAuthority) [sha256.Size]byte {
	digest := sha256.New()
	writeKnowledgeSealString(digest, knowledgeExecutionResultDomain)
	writeKnowledgeExecutionResultMetadata(digest, authority.metadata)
	writeKnowledgeSealBytes(digest, authority.nonce[:])
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result
}

func (manager *Manager) executionResultLeaseNonce(
	metadata executionResultMetadata,
) [sha256.Size]byte {
	digest := hmac.New(sha256.New, manager.knowledgeExecutionSigner)
	writeKnowledgeSealString(digest, knowledgeExecutionResultNonceDomain)
	writeKnowledgeExecutionResultMetadata(digest, metadata)
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result
}

func writeKnowledgeExecutionResultMetadata(
	writer hash.Hash,
	metadata executionResultMetadata,
) {
	writeKnowledgeSealString(writer, metadata.jobID)
	writeKnowledgeSealUint64(writer, metadata.generation)
	writeKnowledgeSealBool(writer, metadata.schema.Columns == nil)
	writeKnowledgeSealUint64(writer, uint64(len(metadata.schema.Columns)))
	for _, column := range metadata.schema.Columns {
		writeKnowledgeSealString(writer, column.Name)
		writeKnowledgeSealUint64(writer, uint64(column.Kind))
		writeKnowledgeSealBool(writer, column.Nullable)
		writeKnowledgeSealBool(writer, column.Multivalue)
	}
	writeKnowledgeSealUint64(writer, metadata.rowCount)
	writeKnowledgeSealBool(writer, metadata.resultsTruncated)
}

func (seal knowledgeExecutionAuthoritySeal) isZero() bool {
	return seal == knowledgeExecutionAuthoritySeal{}
}

func (seal knowledgeExecutionAuthoritySeal) equal(other knowledgeExecutionAuthoritySeal) bool {
	return subtle.ConstantTimeCompare(seal.publicKey[:], other.publicKey[:]) == 1 &&
		subtle.ConstantTimeCompare(seal.signature[:], other.signature[:]) == 1 &&
		subtle.ConstantTimeCompare(seal.authorityDigest[:], other.authorityDigest[:]) == 1 &&
		subtle.ConstantTimeCompare(seal.resultDigest[:], other.resultDigest[:]) == 1
}

func (seal knowledgeExecutionAuthoritySeal) validates(
	expected [sha256.Size]byte,
) bool {
	return !seal.isZero() &&
		subtle.ConstantTimeCompare(
			expected[:],
			seal.authorityDigest[:],
		) == 1 &&
		ed25519.Verify(
			ed25519.PublicKey(seal.publicKey[:]),
			expected[:],
			seal.signature[:],
		)
}

func writeKnowledgeSealTime(writer hash.Hash, value time.Time) bool {
	encoded, err := value.MarshalBinary()
	if err != nil {
		return false
	}
	writeKnowledgeSealBytes(writer, encoded)
	writeKnowledgeSealString(writer, value.String())
	location := value.Location()
	if location == nil {
		writeKnowledgeSealString(writer, "")
	} else {
		writeKnowledgeSealString(writer, location.String())
	}
	zoneName, zoneOffset := value.Zone()
	writeKnowledgeSealString(writer, zoneName)
	writeKnowledgeSealUint64(writer, uint64(int64(zoneOffset))) // #nosec G115 -- two's-complement encoding preserves the signed zone offset in the stable seal.
	return true
}

func writeKnowledgeSealStrings(writer hash.Hash, values []string) {
	writeKnowledgeSealBool(writer, values == nil)
	writeKnowledgeSealUint64(writer, uint64(len(values)))
	for _, value := range values {
		writeKnowledgeSealString(writer, value)
	}
}

func writeKnowledgeSealString(writer hash.Hash, value string) {
	writeKnowledgeSealBytes(writer, []byte(value))
}

func writeKnowledgeSealBytes(writer hash.Hash, value []byte) {
	writeKnowledgeSealUint64(writer, uint64(len(value)))
	_, _ = writer.Write(value)
}

func writeKnowledgeSealBool(writer hash.Hash, value bool) {
	if value {
		_, _ = writer.Write([]byte{1})
		return
	}
	_, _ = writer.Write([]byte{0})
}

func writeKnowledgeSealUint64(writer hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}
