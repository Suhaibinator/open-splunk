package knowledgesnapshot

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"
	"strings"

	"fortio.org/safecast"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
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
) ([]*opensplunk.KnowledgeSnapshotLookupAsset, error) {
	if len(evidence) > MaximumLookupAssets {
		return nil, fmt.Errorf(
			"%w: lookup asset versions exceed %d",
			ErrResourceLimit,
			MaximumLookupAssets,
		)
	}
	assets := make([]*opensplunk.KnowledgeSnapshotLookupAsset, len(evidence))
	for index, item := range evidence {
		if item.tenantID != tenantID {
			return nil, fmt.Errorf(
				"%w: lookup asset %d tenant authority disagrees",
				ErrInvalidInput,
				index,
			)
		}
		assets[index] = &opensplunk.KnowledgeSnapshotLookupAsset{
			AssetOrdinal:  safecast.MustConv[uint32](index),
			LookupId:      strings.Clone(item.lookupID),
			LookupVersion: item.lookupVersion,
			Asset: &opensplunk.KnowledgeLookupAssetVersionReference{
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
	assets []*opensplunk.KnowledgeSnapshotLookupAsset,
) error {
	if len(assets) > MaximumLookupAssets {
		return fmt.Errorf(
			"%w: lookup asset inventory exceeds %d",
			ErrResourceLimit,
			MaximumLookupAssets,
		)
	}
	var previous *opensplunk.KnowledgeSnapshotLookupAsset
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
		if entry.GetAssetOrdinal() != safecast.MustConv[uint32](index) {
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
	left *opensplunk.KnowledgeSnapshotLookupAsset,
	right *opensplunk.KnowledgeSnapshotLookupAsset,
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
	assets []*opensplunk.KnowledgeSnapshotLookupAsset,
) []*opensplunk.KnowledgeSnapshotLookupAsset {
	if assets == nil {
		return nil
	}
	result := make([]*opensplunk.KnowledgeSnapshotLookupAsset, len(assets))
	for index, asset := range assets {
		if asset == nil {
			continue
		}
		result[index], _ = proto.Clone(asset).(*opensplunk.KnowledgeSnapshotLookupAsset)
	}
	return result
}
