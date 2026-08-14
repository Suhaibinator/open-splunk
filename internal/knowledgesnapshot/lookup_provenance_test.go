package knowledgesnapshot

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"google.golang.org/protobuf/proto"
)

func TestLookupVersionProvenanceChangesSnapshotDigestAndRetainedSummary(
	t *testing.T,
) {
	t.Parallel()

	authority := emptyCompilerAuthority(t, "tenant-1", []string{"gradethis"})
	firstCompiled := compileSnapshotLookupVersion(t, authority, 1, 1, "lookup-v1")
	secondCompiled := compileSnapshotLookupVersion(t, authority, 2, 2, "lookup-v2")
	first, err := authority.Finalize(firstCompiled)
	if err != nil {
		t.Fatalf("Finalize(v1): %v", err)
	}
	second, err := authority.Finalize(secondCompiled)
	if err != nil {
		t.Fatalf("Finalize(v2): %v", err)
	}
	if first.Digest() == second.Digest() || bytes.Equal(first.Encoded(), second.Encoded()) {
		t.Fatal("different exact lookup versions produced the same snapshot authority")
	}

	firstSummary := first.Summary()
	secondSummary := second.Summary()
	for name, summary := range map[string]*opensplunkv1.KnowledgeSnapshotSummary{
		"v1": firstSummary,
		"v2": secondSummary,
	} {
		if err := ValidateSummary(summary); err != nil {
			t.Fatalf("ValidateSummary(%s): %v", name, err)
		}
		if summary.GetRef().GetLookupAssetCount() != 1 ||
			len(summary.GetLookupAssets()) != 1 {
			t.Fatalf("%s retained lookup inventory = %#v", name, summary)
		}
		asset := summary.GetLookupAssets()[0]
		if asset.GetAssetOrdinal() != 0 ||
			asset.GetLookupId() != "lookup-services" ||
			asset.GetAsset().GetLookupAssetId() != "asset-services" ||
			asset.GetAsset().GetSizeBytes() == 0 ||
			len(asset.GetAsset().GetContentSha256()) != sha256.Size {
			t.Fatalf("%s retained lookup asset = %#v", name, asset)
		}
	}
	if firstSummary.GetLookupAssets()[0].GetAsset().GetVersion() != 1 ||
		secondSummary.GetLookupAssets()[0].GetAsset().GetVersion() != 2 ||
		proto.Equal(firstSummary, secondSummary) {
		t.Fatalf("retained history summaries do not distinguish lookup versions: %#v / %#v", firstSummary, secondSummary)
	}
	firstSummary.LookupAssets[0].Asset.ContentSha256[0] ^= 0xff
	if bytes.Equal(
		first.Summary().GetLookupAssets()[0].GetAsset().GetContentSha256(),
		firstSummary.GetLookupAssets()[0].GetAsset().GetContentSha256(),
	) {
		t.Fatal("retained lookup summary aliases finalized snapshot authority")
	}

	tampered := firstCompiled
	tampered.SQL += " "
	if snapshot, finalizeErr := authority.Finalize(tampered); !snapshot.IsZero() ||
		!errors.Is(finalizeErr, ErrInvalidInput) {
		t.Fatalf("Finalize(tampered lookup execution) = (%#v, %v)", snapshot, finalizeErr)
	}
}

func TestLookupMetadataOnlyVersionChangesSnapshotProvenance(t *testing.T) {
	t.Parallel()

	authority := emptyCompilerAuthority(t, "tenant-1", []string{"gradethis"})
	firstCompiled := compileSnapshotLookupVersion(t, authority, 11, 7, "stable-asset")
	secondCompiled := compileSnapshotLookupVersion(t, authority, 12, 7, "stable-asset")
	if firstCompiled.SQL != secondCompiled.SQL {
		t.Fatal("metadata-only lookup version changed physical SQL in the fixture")
	}
	first, err := authority.Finalize(firstCompiled)
	if err != nil {
		t.Fatalf("Finalize(logical v11): %v", err)
	}
	second, err := authority.Finalize(secondCompiled)
	if err != nil {
		t.Fatalf("Finalize(logical v12): %v", err)
	}
	if first.Digest() == second.Digest() {
		t.Fatal("metadata-only logical lookup replacement retained the old snapshot digest")
	}
	firstLookup := first.Summary().GetLookupAssets()[0]
	secondLookup := second.Summary().GetLookupAssets()[0]
	if firstLookup.GetLookupVersion() != 11 ||
		secondLookup.GetLookupVersion() != 12 ||
		!proto.Equal(firstLookup.GetAsset(), secondLookup.GetAsset()) ||
		bytes.Equal(
			first.Summary().GetRef().GetSnapshotSha256(),
			second.Summary().GetRef().GetSnapshotSha256(),
		) {
		t.Fatalf("metadata-only retained provenance = %#v / %#v", firstLookup, secondLookup)
	}
}

func TestLookupVersionProvenanceRejectsNoncanonicalOrTamperedInventory(
	t *testing.T,
) {
	t.Parallel()

	authority := emptyCompilerAuthority(t, "tenant-1", []string{"gradethis"})
	digestA := sha256.Sum256([]byte("asset-a"))
	digestB := sha256.Sum256([]byte("asset-b"))
	validEvidence := evidenceFor(authority)
	validEvidence.lookupAssets = []trustedLookupAssetEvidence{{
		tenantID: "tenant-1", lookupID: "lookup-a", lookupVersion: 1,
		objectID: "asset-a", version: 1,
		sizeBytes: 31, contentSHA256: digestA,
	}}
	valid, err := finalize(authority, validEvidence)
	if err != nil {
		t.Fatalf("finalize(valid lookup evidence): %v", err)
	}
	if err := ValidateSummary(valid.Summary()); err != nil {
		t.Fatalf("ValidateSummary(valid lookup evidence): %v", err)
	}

	tests := []struct {
		name   string
		assets []trustedLookupAssetEvidence
	}{
		{name: "tenant mismatch", assets: []trustedLookupAssetEvidence{{
			tenantID: "tenant-2", lookupID: "lookup-a", lookupVersion: 1,
			objectID: "asset-a", version: 1,
			sizeBytes: 31, contentSHA256: digestA,
		}}},
		{name: "zero logical version", assets: []trustedLookupAssetEvidence{{
			tenantID: "tenant-1", lookupID: "lookup-a",
			objectID: "asset-a", version: 1, sizeBytes: 31,
			contentSHA256: digestA,
		}}},
		{name: "zero asset version", assets: []trustedLookupAssetEvidence{{
			tenantID: "tenant-1", lookupID: "lookup-a", lookupVersion: 1,
			objectID: "asset-a", sizeBytes: 31,
			contentSHA256: digestA,
		}}},
		{name: "conflicting duplicate", assets: []trustedLookupAssetEvidence{
			{tenantID: "tenant-1", lookupID: "lookup-a", lookupVersion: 1, objectID: "asset-a", version: 1, sizeBytes: 31, contentSHA256: digestA},
			{tenantID: "tenant-1", lookupID: "lookup-a", lookupVersion: 1, objectID: "asset-b", version: 1, sizeBytes: 32, contentSHA256: digestB},
		}},
		{name: "noncanonical order", assets: []trustedLookupAssetEvidence{
			{tenantID: "tenant-1", lookupID: "lookup-b", lookupVersion: 1, objectID: "asset-b", version: 1, sizeBytes: 32, contentSHA256: digestB},
			{tenantID: "tenant-1", lookupID: "lookup-a", lookupVersion: 1, objectID: "asset-a", version: 1, sizeBytes: 31, contentSHA256: digestA},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence := evidenceFor(authority)
			evidence.lookupAssets = slices.Clone(test.assets)
			if snapshot, finalizeErr := finalize(authority, evidence); !snapshot.IsZero() || !errors.Is(finalizeErr, ErrInvalidInput) {
				t.Fatalf("finalize(tampered evidence) = (%#v, %v)", snapshot, finalizeErr)
			}
		})
	}

	summaryTests := []struct {
		name   string
		mutate func(*opensplunkv1.KnowledgeSnapshotSummary)
	}{
		{name: "count mismatch", mutate: func(value *opensplunkv1.KnowledgeSnapshotSummary) {
			value.Ref.LookupAssetCount = 0
		}},
		{name: "ordinal", mutate: func(value *opensplunkv1.KnowledgeSnapshotSummary) {
			value.LookupAssets[0].AssetOrdinal = 1
		}},
		{name: "logical identity", mutate: func(value *opensplunkv1.KnowledgeSnapshotSummary) {
			value.LookupAssets[0].LookupId = ""
		}},
		{name: "logical version", mutate: func(value *opensplunkv1.KnowledgeSnapshotSummary) {
			value.LookupAssets[0].LookupVersion = 0
		}},
		{name: "size", mutate: func(value *opensplunkv1.KnowledgeSnapshotSummary) {
			value.LookupAssets[0].Asset.SizeBytes = 0
		}},
		{name: "digest", mutate: func(value *opensplunkv1.KnowledgeSnapshotSummary) {
			value.LookupAssets[0].Asset.ContentSha256 = nil
		}},
		{name: "unknown", mutate: func(value *opensplunkv1.KnowledgeSnapshotSummary) {
			value.LookupAssets[0].ProtoReflect().SetUnknown(smallUnknownField())
		}},
	}
	for _, test := range summaryTests {
		t.Run("summary "+test.name, func(t *testing.T) {
			candidate := proto.Clone(valid.Summary()).(*opensplunkv1.KnowledgeSnapshotSummary)
			test.mutate(candidate)
			if validateErr := ValidateSummary(candidate); !errors.Is(validateErr, ErrInvalidInput) {
				t.Fatalf("ValidateSummary(tampered) = %v", validateErr)
			}
		})
	}
}

func compileSnapshotLookupVersion(
	t *testing.T,
	authority Authority,
	lookupVersion uint64,
	assetVersion uint64,
	digestSeed string,
) clickhouse.CompiledQuery {
	t.Helper()
	logical := buildSnapshotQuery(
		t,
		"tenant-1",
		[]string{"gradethis"},
		`index=gradethis | lookup services service_id AS service_id OUTPUT owner AS service_owner | table message`,
	)
	var err error
	logical, err = plan.InjectKnowledgePrelude(logical, authority.Prelude())
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude(lookup): %v", err)
	}
	var contract *plan.Lookup
	for _, operator := range logical.Operators {
		if lookup, ok := operator.(*plan.Lookup); ok {
			contract = lookup
			break
		}
	}
	if contract == nil {
		t.Fatal("lookup plan omitted its logical contract")
	}
	resolution, err := clickhouse.NewLookupResolution(
		"tenant-1",
		"services",
		"asset-services",
		assetVersion,
		uint64(len("service_id,owner\napi,platform\n")),
		sha256.Sum256([]byte(digestSeed)),
		[]string{"service_id", "owner"},
		[][]string{{"api", "platform"}},
	)
	if err != nil {
		t.Fatalf("NewLookupResolution(asset v%d): %v", assetVersion, err)
	}
	resolution, err = resolution.WithLogicalContract(
		*contract,
		"lookup-services",
		lookupVersion,
	)
	if err != nil {
		t.Fatalf("WithLogicalContract(logical v%d): %v", lookupVersion, err)
	}
	compiled, err := (clickhouse.Compiler{}).CompileWithLookupResolutions(
		logical,
		[]clickhouse.LookupResolution{resolution},
	)
	if err != nil {
		t.Fatalf(
			"CompileWithLookupResolutions(logical v%d, asset v%d): %v",
			lookupVersion,
			assetVersion,
			err,
		)
	}
	return compiled
}
