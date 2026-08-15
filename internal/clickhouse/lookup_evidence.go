package clickhouse

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sort"
	"strings"
)

// LookupAssetVersionEvidence is an opaque read-only view of one exact logical
// lookup-definition revision and its pinned physical asset version consumed by
// a sealed executable. Row payloads remain private; snapshot/history
// provenance receives immutable logical and physical identities, size, and
// digest.
type LookupAssetVersionEvidence struct {
	tenantID       string
	definitionName string
	lookupID       string
	lookupVersion  uint64
	objectID       string
	version        uint64
	sizeBytes      uint64
	contentSHA256  [sha256.Size]byte
}

func (evidence LookupAssetVersionEvidence) DefinitionName() string {
	return strings.Clone(evidence.definitionName)
}

func (evidence LookupAssetVersionEvidence) LookupID() string {
	return strings.Clone(evidence.lookupID)
}

func (evidence LookupAssetVersionEvidence) LookupVersion() uint64 {
	return evidence.lookupVersion
}

func (evidence LookupAssetVersionEvidence) AssetID() string {
	return strings.Clone(evidence.objectID)
}

func (evidence LookupAssetVersionEvidence) AssetVersion() uint64 {
	return evidence.version
}

func (evidence LookupAssetVersionEvidence) TenantID() string {
	return strings.Clone(evidence.tenantID)
}

func (evidence LookupAssetVersionEvidence) ObjectID() string {
	return strings.Clone(evidence.objectID)
}

func (evidence LookupAssetVersionEvidence) Version() uint64 {
	return evidence.version
}

func (evidence LookupAssetVersionEvidence) SizeBytes() uint64 {
	return evidence.sizeBytes
}

func (evidence LookupAssetVersionEvidence) ContentSHA256() [sha256.Size]byte {
	return evidence.contentSHA256
}

// LookupAssetVersions opens canonical unique lookup provenance only from a
// valid execution seal. Repeated stages using the same logical revision and
// physical asset collapse to one entry. One logical tenant/ID/version bound to
// conflicting definition or asset authority fails closed.
func (query CompiledQuery) LookupAssetVersions() ([]LookupAssetVersionEvidence, bool) {
	if !query.HasValidExecutionSeal() ||
		validateCompiledLookupExternalTables(query.lookupTables) != nil {
		return nil, false
	}
	byVersion := make(map[string]LookupAssetVersionEvidence, len(query.lookupTables))
	for _, table := range query.lookupTables {
		key := table.tenantID + "\x00" + table.logicalID + "\x00" +
			string(binary.BigEndian.AppendUint64(nil, table.logicalVersion))
		candidate := LookupAssetVersionEvidence{
			tenantID:       strings.Clone(table.tenantID),
			definitionName: strings.Clone(table.definitionName),
			lookupID:       strings.Clone(table.logicalID),
			lookupVersion:  table.logicalVersion,
			objectID:       strings.Clone(table.objectID),
			version:        table.version,
			sizeBytes:      table.sizeBytes,
			contentSHA256:  table.contentSHA256,
		}
		if existing, duplicate := byVersion[key]; duplicate {
			if existing.definitionName != candidate.definitionName ||
				existing.objectID != candidate.objectID ||
				existing.version != candidate.version ||
				existing.sizeBytes != candidate.sizeBytes ||
				existing.contentSHA256 != candidate.contentSHA256 {
				return nil, false
			}
			continue
		}
		byVersion[key] = candidate
	}
	result := make([]LookupAssetVersionEvidence, 0, len(byVersion))
	for _, evidence := range byVersion {
		result = append(result, evidence)
	}
	sort.Slice(result, func(left, right int) bool {
		if compared := strings.Compare(result[left].tenantID, result[right].tenantID); compared != 0 {
			return compared < 0
		}
		if compared := strings.Compare(result[left].lookupID, result[right].lookupID); compared != 0 {
			return compared < 0
		}
		if result[left].lookupVersion != result[right].lookupVersion {
			return result[left].lookupVersion < result[right].lookupVersion
		}
		if compared := strings.Compare(result[left].objectID, result[right].objectID); compared != 0 {
			return compared < 0
		}
		if result[left].version != result[right].version {
			return result[left].version < result[right].version
		}
		if result[left].sizeBytes != result[right].sizeBytes {
			return result[left].sizeBytes < result[right].sizeBytes
		}
		return bytes.Compare(
			result[left].contentSHA256[:],
			result[right].contentSHA256[:],
		) < 0
	})
	return slices.Clone(result), true
}
