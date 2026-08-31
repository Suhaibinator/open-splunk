package server

import (
	"context"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

func TestSanitizeGetHECOperationalSnapshotRequest(t *testing.T) {
	t.Parallel()

	unknown := []byte{0xf8, 0x3f, 0x01}
	request := &opensplunk.GetHECOperationalSnapshotRequest{}
	request.ProtoReflect().SetUnknown(unknown)

	sanitized, err := sanitizeGetHECOperationalSnapshotRequest(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized != request {
		t.Fatal("sanitizer returned a different request pointer")
	}
	assertUnknownFieldTolerated(t, sanitized, unknown)
	want := &opensplunk.GetHECOperationalSnapshotRequest{}
	want.ProtoReflect().SetUnknown(unknown)
	if !proto.Equal(sanitized, want) {
		t.Fatalf("sanitized request = %v, want %v", sanitized, want)
	}
}
