package knowledgesnapshot

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

type snapshotWireFixture struct {
	Version                uint32 `json:"version"`
	DigestDomain           string `json:"digestDomain"`
	CanonicalSnapshotBytes uint64 `json:"canonicalSnapshotBytes"`
	B0                     struct {
		ByteLength int    `json:"byteLength"`
		SHA256     string `json:"sha256"`
	} `json:"b0"`
	B1 struct {
		ByteLength int    `json:"byteLength"`
		SHA256     string `json:"sha256"`
	} `json:"b1"`
	SnapshotSHA256 string `json:"snapshotSha256"`
	Final          struct {
		ByteLength int    `json:"byteLength"`
		SHA256     string `json:"sha256"`
		WireBase64 string `json:"wireBase64"`
	} `json:"final"`
}

func TestKnowledgeSnapshotSharedGoTypeScriptWireGolden(t *testing.T) {
	encodedFixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "knowledge-snapshot-wire.json"))
	if err != nil {
		t.Fatalf("read shared fixture: %v", err)
	}
	var fixture snapshotWireFixture
	if err := json.Unmarshal(encodedFixture, &fixture); err != nil {
		t.Fatalf("decode shared fixture: %v", err)
	}
	if fixture.Version != 1 || fixture.DigestDomain != digestDomain {
		t.Fatalf("fixture compatibility identity = version %d/domain %q", fixture.Version, fixture.DigestDomain)
	}

	authority, err := Prepare(snapshotGoldenInput(t))
	if err != nil {
		t.Fatalf("Prepare(): %v", err)
	}
	evidence := evidenceFor(authority)
	evidence.generatedOperators = 7
	evidence.generatedSQLBytes = 2048
	snapshot, err := finalize(authority, evidence)
	if err != nil {
		t.Fatalf("finalize(): %v", err)
	}

	wantFinal, err := base64.StdEncoding.DecodeString(fixture.Final.WireBase64)
	if err != nil {
		t.Fatalf("decode final wire: %v", err)
	}
	assertSnapshotWireHash(t, "final", snapshot.Encoded(), fixture.Final.ByteLength, fixture.Final.SHA256)
	if !bytes.Equal(snapshot.Encoded(), wantFinal) {
		t.Fatal("Go deterministic final wire differs from the shared Go/TypeScript fixture")
	}

	b1 := proto.Clone(snapshot.Proto()).(*opensplunkv1.KnowledgeSnapshot)
	b1.SnapshotSha256 = nil
	b1Wire := deterministicMessage(t, b1)
	assertSnapshotWireHash(t, "B1", b1Wire, fixture.B1.ByteLength, fixture.B1.SHA256)

	b0 := proto.Clone(b1).(*opensplunkv1.KnowledgeSnapshot)
	if b0.BudgetCharges == nil {
		t.Fatal("finalized snapshot omitted budget charges")
	}
	b0.BudgetCharges.CanonicalSnapshotBytes = 0
	b0Wire := deterministicMessage(t, b0)
	assertSnapshotWireHash(t, "B0", b0Wire, fixture.B0.ByteLength, fixture.B0.SHA256)
	if fixture.CanonicalSnapshotBytes != uint64(len(b0Wire)) ||
		b1.GetBudgetCharges().GetCanonicalSnapshotBytes() != fixture.CanonicalSnapshotBytes {
		t.Fatalf(
			"canonical byte charge = fixture %d/B0 %d/B1 field %d",
			fixture.CanonicalSnapshotBytes,
			len(b0Wire),
			b1.GetBudgetCharges().GetCanonicalSnapshotBytes(),
		)
	}

	hash := sha256.New()
	_, _ = hash.Write([]byte(fixture.DigestDomain))
	var framedLength [8]byte
	binary.BigEndian.PutUint64(framedLength[:], uint64(len(b1Wire)))
	_, _ = hash.Write(framedLength[:])
	_, _ = hash.Write(b1Wire)
	wantDigest := hash.Sum(nil)
	if hex.EncodeToString(wantDigest) != fixture.SnapshotSHA256 ||
		!bytes.Equal(snapshot.Proto().GetSnapshotSha256(), wantDigest) {
		t.Fatalf("framed snapshot digest = %x, fixture %s", wantDigest, fixture.SnapshotSHA256)
	}
}

func assertSnapshotWireHash(t *testing.T, name string, wire []byte, wantLength int, wantSHA256 string) {
	t.Helper()
	gotSHA256 := sha256.Sum256(wire)
	if len(wire) != wantLength || hex.EncodeToString(gotSHA256[:]) != wantSHA256 {
		t.Fatalf("%s wire = %d bytes/SHA-256 %x, want %d/%s", name, len(wire), gotSHA256, wantLength, wantSHA256)
	}
}
