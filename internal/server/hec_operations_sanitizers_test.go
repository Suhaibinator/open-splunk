package server

import (
	"context"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

func TestSanitizeGetHECOperationalSnapshotRequest(t *testing.T) {
	t.Parallel()

	request := &opensplunk.GetHECOperationalSnapshotRequest{}
	request.ProtoReflect().SetUnknown([]byte{0xf8, 0x3f, 0x01})

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
	if len(sanitized.ProtoReflect().GetUnknown()) != 0 {
		t.Fatal("sanitizer retained unknown fields")
	}
	if !proto.Equal(
		sanitized,
		&opensplunk.GetHECOperationalSnapshotRequest{},
	) {
		t.Fatalf("sanitized request = %v, want empty", sanitized)
	}
}

func TestSanitizeGetHECOperationalSnapshotRequestRejectsMissingBody(t *testing.T) {
	t.Parallel()

	var request *opensplunk.GetHECOperationalSnapshotRequest
	_, err := sanitizeGetHECOperationalSnapshotRequest(
		context.Background(),
		request,
	)
	assertSanitizerRejection(t, err, "request body is required")
}
