package knowledgesnapshot

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func TestSnapshotObjectIDsRetainCompleteInventoryBeyondPublicPrefix(t *testing.T) {
	objects := make([]Object, MaximumSummaryObjects+1)
	for index := range objects {
		name := fmt.Sprintf("alias-%03d", index)
		objects[index] = aliasObject(t, "ko-"+name, uint64(index+1), name, "app-a", "owner-a", opensplunk.SharingScope_SHARING_SCOPE_PRIVATE, nil)
	}
	authority, err := Prepare(minimalInput(objects))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := finalize(authority, evidenceFor(authority))
	if err != nil {
		t.Fatal(err)
	}
	ids := snapshot.ObjectIDs()
	summary := snapshot.Summary()
	if len(ids) != len(objects) || len(summary.GetObjects()) != MaximumSummaryObjects || !summary.GetObjectsTruncated() {
		t.Fatal("private inventory reused truncated public prefix")
	}
	if err := ValidateObjectIDs(summary, ids); err != nil {
		t.Fatal(err)
	}
	if _, err := NormalizeObjectIDs(summary, nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("truncated summary reconstructed full inventory: %v", err)
	}
	ids[0] = "mutated"
	if snapshot.ObjectIDs()[0] == "mutated" {
		t.Fatal("private inventory accessor retained caller-owned slice")
	}
	ids = snapshot.ObjectIDs()
	for _, candidate := range [][]string{ids[:MaximumSummaryObjects], append(slices.Clone(ids[:len(ids)-1]), ids[0]), append([]string{"wrong-prefix"}, ids[1:]...)} {
		if err := ValidateObjectIDs(summary, candidate); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid inventory accepted: %v", err)
		}
	}
}

func TestNormalizeObjectIDsRequiresEntireAuthorizedSummary(t *testing.T) {
	objects := []Object{
		aliasObject(t, "ko-first", 1, "first", "app-a", "owner-a", opensplunk.SharingScope_SHARING_SCOPE_PRIVATE, nil),
		aliasObject(t, "ko-second", 2, "second", "app-a", "owner-a", opensplunk.SharingScope_SHARING_SCOPE_PRIVATE, nil),
	}
	authority, err := Prepare(minimalInput(objects))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := finalize(authority, evidenceFor(authority))
	if err != nil {
		t.Fatal(err)
	}
	summary := snapshot.Summary()
	ids, err := NormalizeObjectIDs(summary, nil)
	if err != nil || !slices.Equal(ids, snapshot.ObjectIDs()) {
		t.Fatalf("complete summary inventory=%v err=%v", ids, err)
	}
	ids[0] = "detached"
	if snapshot.ObjectIDs()[0] == "detached" {
		t.Fatal("normalized inventory aliases finalized authority")
	}
	summary.Objects[0].Disclosure = &opensplunk.KnowledgeSnapshotObjectSummary_Redacted{Redacted: true}
	if _, err := NormalizeObjectIDs(summary, nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("redacted summary reconstructed inventory: %v", err)
	}
	summary = snapshot.Summary()
	summary.Objects[1].GetAuthorizedObject().KnowledgeObjectId = summary.Objects[0].GetAuthorizedObject().KnowledgeObjectId
	if _, err := NormalizeObjectIDs(summary, nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("duplicate disclosed identities accepted: %v", err)
	}
}
