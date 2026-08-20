package opensplunk_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

const maximumJavaScriptSafeInteger = uint64(1<<53 - 1)

type knowledgeSnapshotSummaryWireRecord struct {
	ByteLength int    `json:"byteLength"`
	SHA256     string `json:"sha256"`
	WireBase64 string `json:"wireBase64"`
}

type knowledgeSnapshotReferenceContract struct {
	SnapshotSHA256               string `json:"snapshotSha256"`
	TenantCatalogRevision        string `json:"tenantCatalogRevision"`
	TenantCatalogStateToken      string `json:"tenantCatalogStateToken"`
	ObjectCount                  uint32 `json:"objectCount"`
	CompilerCompatibilityVersion string `json:"compilerCompatibilityVersion"`
	LookupAssetCount             uint32 `json:"lookupAssetCount"`
	LookupAssetCountUnknown      bool   `json:"lookupAssetCountUnknown"`
}

type knowledgeSnapshotAuthorizedObjectContract struct {
	KnowledgeObjectID string `json:"knowledgeObjectId"`
	Version           string `json:"version"`
	Name              string `json:"name"`
}

type knowledgeSnapshotObjectSummaryContract struct {
	ResolutionOrdinal uint32                                     `json:"resolutionOrdinal"`
	ObjectType        int32                                      `json:"objectType"`
	Stage             int32                                      `json:"stage"`
	AuthorizedObject  *knowledgeSnapshotAuthorizedObjectContract `json:"authorizedObject,omitempty"`
	Redacted          *bool                                      `json:"redacted,omitempty"`
}

type knowledgeSnapshotSummaryWireCase struct {
	Name             string                                   `json:"name"`
	Ref              *knowledgeSnapshotReferenceContract      `json:"ref"`
	Objects          []knowledgeSnapshotObjectSummaryContract `json:"objects,omitempty"`
	ObjectsTruncated bool                                     `json:"objectsTruncated,omitempty"`
	RefWire          *knowledgeSnapshotSummaryWireRecord      `json:"refWire"`
	SummaryWire      *knowledgeSnapshotSummaryWireRecord      `json:"summaryWire"`
	SearchJobWire    knowledgeSnapshotSummaryWireRecord       `json:"searchJobWire"`
}

type knowledgeSnapshotSummaryWireFixture struct {
	Version uint32                             `json:"version"`
	Cases   []knowledgeSnapshotSummaryWireCase `json:"cases"`
}

func TestKnowledgeSnapshotReferenceAndSummarySharedGoTypeScriptWireGolden(t *testing.T) {
	t.Parallel()

	encodedFixture, err := os.ReadFile("testdata/knowledge-snapshot-summary-wire.json")
	if err != nil {
		t.Fatalf("read shared knowledge snapshot summary fixture: %v", err)
	}
	var fixture knowledgeSnapshotSummaryWireFixture
	if err := json.Unmarshal(encodedFixture, &fixture); err != nil {
		t.Fatalf("decode shared knowledge snapshot summary fixture: %v", err)
	}
	if fixture.Version != 2 || len(fixture.Cases) != 3 {
		t.Fatalf("fixture = version %d with %d cases, want version 2 with 3 cases", fixture.Version, len(fixture.Cases))
	}

	seen := make(map[string]bool, len(fixture.Cases))
	var absentWire []byte
	var enabledEmptyWire []byte
	for _, contract := range fixture.Cases {
		t.Run(contract.Name, func(t *testing.T) {
			if seen[contract.Name] {
				t.Fatalf("duplicate fixture case %q", contract.Name)
			}
			seen[contract.Name] = true

			summary := knowledgeSnapshotSummaryFromContract(t, contract)
			if summary == nil {
				if contract.Name != "absent" || contract.RefWire != nil || contract.SummaryWire != nil ||
					len(contract.Objects) != 0 || contract.ObjectsTruncated {
					t.Fatal("only the absent case may omit the summary and its standalone wire records")
				}
			} else {
				if contract.RefWire == nil || contract.SummaryWire == nil {
					t.Fatal("present summary omitted its standalone reference or summary wire record")
				}
				refWire := deterministicContractWire(t, summary.GetRef())
				assertKnowledgeSummaryWireRecord(t, "reference", refWire, *contract.RefWire)
				assertKnowledgeSummaryWireRoundTrip(t, "reference", refWire, summary.GetRef(), &opensplunkv1.KnowledgeSnapshotRef{})

				summaryWire := deterministicContractWire(t, summary)
				assertKnowledgeSummaryWireRecord(t, "summary", summaryWire, *contract.SummaryWire)
				assertKnowledgeSummaryWireRoundTrip(t, "summary", summaryWire, summary, &opensplunkv1.KnowledgeSnapshotSummary{})
			}

			job := &opensplunkv1.SearchJob{KnowledgeSnapshot: summary}
			jobWire := deterministicContractWire(t, job)
			assertKnowledgeSummaryWireRecord(t, "SearchJob attachment", jobWire, contract.SearchJobWire)
			var decodedJob opensplunkv1.SearchJob
			if err := proto.Unmarshal(jobWire, &decodedJob); err != nil {
				t.Fatalf("unmarshal SearchJob attachment: %v", err)
			}
			if gotPresence := decodedJob.KnowledgeSnapshot != nil; gotPresence != (summary != nil) {
				t.Fatalf("SearchJob knowledge snapshot presence = %t, want %t", gotPresence, summary != nil)
			}
			if !proto.Equal(job, &decodedJob) {
				t.Fatalf("SearchJob attachment round trip changed message: want %v, got %v", job, &decodedJob)
			}

			switch contract.Name {
			case "absent":
				absentWire = append([]byte(nil), jobWire...)
			case "enabled-empty":
				enabledEmptyWire = append([]byte(nil), jobWire...)
				if summary == nil || summary.GetRef() == nil || summary.GetRef().GetObjectCount() != 0 ||
					len(summary.GetObjects()) != 0 || summary.GetObjectsTruncated() {
					t.Fatalf("enabled-empty summary = %v, want present zero-object authority", summary)
				}
				if summary.GetRef().GetTenantCatalogRevision() <= maximumJavaScriptSafeInteger {
					t.Fatalf("enabled-empty revision = %d, want greater than JavaScript safe integer", summary.GetRef().GetTenantCatalogRevision())
				}
			case "authorized-and-redacted":
				assertAuthorizedAndRedactedSummaryContract(t, summary)
			default:
				t.Fatalf("unexpected fixture case %q", contract.Name)
			}
		})
	}

	for _, name := range []string{"absent", "enabled-empty", "authorized-and-redacted"} {
		if !seen[name] {
			t.Errorf("fixture case %q is missing", name)
		}
	}
	if bytes.Equal(absentWire, enabledEmptyWire) || len(absentWire) != 0 || len(enabledEmptyWire) == 0 {
		t.Fatalf("absent and enabled-empty attachments are not wire-distinct: absent=%x enabled-empty=%x", absentWire, enabledEmptyWire)
	}
}

func knowledgeSnapshotSummaryFromContract(
	t *testing.T,
	contract knowledgeSnapshotSummaryWireCase,
) *opensplunkv1.KnowledgeSnapshotSummary {
	t.Helper()
	if contract.Ref == nil {
		return nil
	}

	revision, err := strconv.ParseUint(contract.Ref.TenantCatalogRevision, 10, 64)
	if err != nil {
		t.Fatalf("parse tenant catalog revision: %v", err)
	}
	digest := decodeKnowledgeSummaryHex(t, "snapshot SHA-256", contract.Ref.SnapshotSHA256, sha256.Size)
	stateToken := decodeKnowledgeSummaryHex(t, "tenant catalog state token", contract.Ref.TenantCatalogStateToken, sha256.Size)
	summary := &opensplunkv1.KnowledgeSnapshotSummary{
		Ref: &opensplunkv1.KnowledgeSnapshotRef{
			SnapshotSha256:               digest,
			TenantCatalogRevision:        revision,
			TenantCatalogStateToken:      stateToken,
			ObjectCount:                  contract.Ref.ObjectCount,
			CompilerCompatibilityVersion: contract.Ref.CompilerCompatibilityVersion,
			LookupAssetCount:             contract.Ref.LookupAssetCount,
			LookupAssetCountUnknown:      contract.Ref.LookupAssetCountUnknown,
		},
		ObjectsTruncated: contract.ObjectsTruncated,
	}
	for index, object := range contract.Objects {
		entry := &opensplunkv1.KnowledgeSnapshotObjectSummary{
			ResolutionOrdinal: object.ResolutionOrdinal,
			ObjectType:        opensplunkv1.KnowledgeObjectType(object.ObjectType),
			Stage:             opensplunkv1.KnowledgeSearchStage(object.Stage),
		}
		switch {
		case object.AuthorizedObject != nil && object.Redacted == nil:
			version, err := strconv.ParseUint(object.AuthorizedObject.Version, 10, 64)
			if err != nil {
				t.Fatalf("parse object %d authorized version: %v", index, err)
			}
			entry.Disclosure = &opensplunkv1.KnowledgeSnapshotObjectSummary_AuthorizedObject{
				AuthorizedObject: &opensplunkv1.KnowledgeSnapshotAuthorizedObjectSummary{
					KnowledgeObjectId: object.AuthorizedObject.KnowledgeObjectID,
					Version:           version,
					Name:              object.AuthorizedObject.Name,
				},
			}
		case object.AuthorizedObject == nil && object.Redacted != nil:
			if !*object.Redacted {
				t.Fatalf("object %d redacted disclosure must be true", index)
			}
			entry.Disclosure = &opensplunkv1.KnowledgeSnapshotObjectSummary_Redacted{Redacted: true}
		default:
			t.Fatalf("object %d must contain exactly one authorized or redacted disclosure", index)
		}
		summary.Objects = append(summary.Objects, entry)
	}
	return summary
}

func assertAuthorizedAndRedactedSummaryContract(t *testing.T, summary *opensplunkv1.KnowledgeSnapshotSummary) {
	t.Helper()
	if summary == nil || summary.GetRef() == nil || summary.GetRef().GetObjectCount() != 2 ||
		len(summary.GetObjects()) != 2 || summary.GetObjectsTruncated() {
		t.Fatalf("authorized-and-redacted summary = %v, want exact two-object inventory", summary)
	}
	if summary.GetRef().GetTenantCatalogRevision() <= maximumJavaScriptSafeInteger {
		t.Fatalf("tenant catalog revision = %d, want greater than JavaScript safe integer", summary.GetRef().GetTenantCatalogRevision())
	}
	authorized := summary.GetObjects()[0].GetAuthorizedObject()
	if authorized == nil || authorized.GetKnowledgeObjectId() != "ko-visible" || authorized.GetName() != "visible_field" {
		t.Fatalf("authorized object summary = %v", authorized)
	}
	if authorized.GetVersion() <= maximumJavaScriptSafeInteger {
		t.Fatalf("authorized object version = %d, want greater than JavaScript safe integer", authorized.GetVersion())
	}
	if _, ok := summary.GetObjects()[0].GetDisclosure().(*opensplunkv1.KnowledgeSnapshotObjectSummary_AuthorizedObject); !ok {
		t.Fatalf("first disclosure = %T, want authorized object", summary.GetObjects()[0].GetDisclosure())
	}
	if disclosure, ok := summary.GetObjects()[1].GetDisclosure().(*opensplunkv1.KnowledgeSnapshotObjectSummary_Redacted); !ok || !disclosure.Redacted {
		t.Fatalf("second disclosure = %#v, want explicit redacted=true", summary.GetObjects()[1].GetDisclosure())
	}
}

func deterministicContractWire(t *testing.T, message proto.Message) []byte {
	t.Helper()
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatalf("marshal deterministic contract wire: %v", err)
	}
	second, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatalf("marshal deterministic contract wire again: %v", err)
	}
	if !bytes.Equal(wire, second) {
		t.Fatalf("deterministic contract wire changed between runs: first=%x second=%x", wire, second)
	}
	return wire
}

func assertKnowledgeSummaryWireRecord(
	t *testing.T,
	name string,
	wire []byte,
	want knowledgeSnapshotSummaryWireRecord,
) {
	t.Helper()
	digest := sha256.Sum256(wire)
	actual := knowledgeSnapshotSummaryWireRecord{
		ByteLength: len(wire),
		SHA256:     hex.EncodeToString(digest[:]),
		WireBase64: base64.StdEncoding.EncodeToString(wire),
	}
	wantWire, err := base64.StdEncoding.DecodeString(want.WireBase64)
	if err != nil || want.ByteLength != actual.ByteLength || want.SHA256 != actual.SHA256 || !bytes.Equal(wantWire, wire) {
		t.Errorf("%s wire contract = %s, want %+v (base64 decode error: %v)", name, formatKnowledgeSummaryWireRecord(actual), want, err)
	}
}

func assertKnowledgeSummaryWireRoundTrip(
	t *testing.T,
	name string,
	wire []byte,
	want proto.Message,
	decoded proto.Message,
) {
	t.Helper()
	if err := proto.Unmarshal(wire, decoded); err != nil {
		t.Fatalf("unmarshal %s: %v", name, err)
	}
	if !proto.Equal(want, decoded) {
		t.Fatalf("%s round trip changed message: want %v, got %v", name, want, decoded)
	}
}

func decodeKnowledgeSummaryHex(t *testing.T, name, value string, wantBytes int) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	if len(decoded) != wantBytes {
		t.Fatalf("%s length = %d, want %d", name, len(decoded), wantBytes)
	}
	return decoded
}

func formatKnowledgeSummaryWireRecord(record knowledgeSnapshotSummaryWireRecord) string {
	return fmt.Sprintf(
		`{"byteLength":%d,"sha256":%q,"wireBase64":%q}`,
		record.ByteLength,
		record.SHA256,
		record.WireBase64,
	)
}
