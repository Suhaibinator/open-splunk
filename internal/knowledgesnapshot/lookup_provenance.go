package knowledgesnapshot

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"
	"strings"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"google.golang.org/protobuf/proto"
)

const MaximumLookupAssets = clickhouse.MaximumLookupStagesPerQuery

// trustedLookupAssetEvidence is detached from the compiler's sealed read-only
// view before snapshot construction. Tenant authority remains here even though
// the snapshot wire message inherits it from KnowledgeSnapshot.tenant_id.
type trustedLookupAssetEvidence struct {
	tenantID      string
	lookupID      string
	lookupVersion uint64
	objectID      string
	version       uint64
	sizeBytes     uint64
	contentSHA256 [sha256.Size]byte
}

func canonicalSnapshotLookupAssets(
	tenantID string,
	evidence []trustedLookupAssetEvidence,
) ([]*opensplunkv1.KnowledgeSnapshotLookupAsset, error) {
	if len(evidence) > MaximumLookupAssets {
		return nil, fmt.Errorf(
			"%w: lookup asset versions exceed %d",
			ErrResourceLimit,
			MaximumLookupAssets,
		)
	}
	assets := make([]*opensplunkv1.KnowledgeSnapshotLookupAsset, len(evidence))
	for index, item := range evidence {
		if item.tenantID != tenantID {
			return nil, fmt.Errorf(
				"%w: lookup asset %d tenant authority disagrees",
				ErrInvalidInput,
				index,
			)
		}
		assets[index] = &opensplunkv1.KnowledgeSnapshotLookupAsset{
			AssetOrdinal:  uint32(index), // #nosec G115 -- compiler lookup stages are bounded by sixteen.
			LookupId:      strings.Clone(item.lookupID),
			LookupVersion: item.lookupVersion,
			Asset: &opensplunkv1.KnowledgeLookupAssetVersionReference{
				LookupAssetId: strings.Clone(item.objectID),
				Version:       item.version,
				SizeBytes:     item.sizeBytes,
				ContentSha256: bytes.Clone(item.contentSHA256[:]),
			},
		}
	}
	if err := validateSnapshotLookupAssets(assets); err != nil {
		return nil, err
	}
	return assets, nil
}

func validateSnapshotLookupAssets(
	assets []*opensplunkv1.KnowledgeSnapshotLookupAsset,
) error {
	if len(assets) > MaximumLookupAssets {
		return fmt.Errorf(
			"%w: lookup asset inventory exceeds %d",
			ErrResourceLimit,
			MaximumLookupAssets,
		)
	}
	var previous *opensplunkv1.KnowledgeSnapshotLookupAsset
	for index, entry := range assets {
		if entry == nil || entry.GetAsset() == nil {
			return fmt.Errorf(
				"%w: lookup asset inventory entry %d is incomplete",
				ErrInvalidInput,
				index,
			)
		}
		if len(entry.ProtoReflect().GetUnknown()) != 0 ||
			len(entry.GetAsset().ProtoReflect().GetUnknown()) != 0 {
			return fmt.Errorf(
				"%w: lookup asset inventory entry %d contains unknown fields",
				ErrInvalidInput,
				index,
			)
		}
		if entry.GetAssetOrdinal() != uint32(index) { // #nosec G115 -- inventory is bounded by sixteen.
			return fmt.Errorf(
				"%w: lookup asset inventory entry %d has a noncanonical ordinal",
				ErrInvalidInput,
				index,
			)
		}
		asset := entry.GetAsset()
		if !validIdentity(entry.GetLookupId(), maximumObjectIDBytes) ||
			entry.GetLookupVersion() == 0 ||
			entry.GetLookupVersion() > math.MaxInt64 ||
			!validIdentity(asset.GetLookupAssetId(), maximumObjectIDBytes) ||
			asset.GetVersion() == 0 || asset.GetVersion() > math.MaxInt64 ||
			asset.GetSizeBytes() == 0 ||
			asset.GetSizeBytes() > lookupasset.MaximumSourceBytes ||
			len(asset.GetContentSha256()) != sha256.Size {
			return fmt.Errorf(
				"%w: lookup asset inventory entry %d has invalid identity",
				ErrInvalidInput,
				index,
			)
		}
		if previous != nil {
			if previous.GetLookupId() == entry.GetLookupId() &&
				previous.GetLookupVersion() == entry.GetLookupVersion() {
				return fmt.Errorf(
					"%w: lookup asset inventory repeats one logical lookup version",
					ErrInvalidInput,
				)
			}
			if compareSnapshotLookupAssetReferences(previous, entry) >= 0 {
				return fmt.Errorf(
					"%w: lookup asset inventory is not in canonical order",
					ErrInvalidInput,
				)
			}
		}
		previous = entry
	}
	return nil
}

func compareSnapshotLookupAssetReferences(
	left *opensplunkv1.KnowledgeSnapshotLookupAsset,
	right *opensplunkv1.KnowledgeSnapshotLookupAsset,
) int {
	if compared := strings.Compare(left.GetLookupId(), right.GetLookupId()); compared != 0 {
		return compared
	}
	if left.GetLookupVersion() < right.GetLookupVersion() {
		return -1
	}
	if left.GetLookupVersion() > right.GetLookupVersion() {
		return 1
	}
	leftAsset := left.GetAsset()
	rightAsset := right.GetAsset()
	if compared := strings.Compare(leftAsset.GetLookupAssetId(), rightAsset.GetLookupAssetId()); compared != 0 {
		return compared
	}
	if leftAsset.GetVersion() < rightAsset.GetVersion() {
		return -1
	}
	if leftAsset.GetVersion() > rightAsset.GetVersion() {
		return 1
	}
	if leftAsset.GetSizeBytes() < rightAsset.GetSizeBytes() {
		return -1
	}
	if leftAsset.GetSizeBytes() > rightAsset.GetSizeBytes() {
		return 1
	}
	return bytes.Compare(leftAsset.GetContentSha256(), rightAsset.GetContentSha256())
}

func cloneSnapshotLookupAssets(
	assets []*opensplunkv1.KnowledgeSnapshotLookupAsset,
) []*opensplunkv1.KnowledgeSnapshotLookupAsset {
	if assets == nil {
		return nil
	}
	result := make([]*opensplunkv1.KnowledgeSnapshotLookupAsset, len(assets))
	for index, asset := range assets {
		if asset == nil {
			continue
		}
		result[index], _ = proto.Clone(asset).(*opensplunkv1.KnowledgeSnapshotLookupAsset)
	}
	return result
}
