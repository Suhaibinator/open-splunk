package server

import (
	"context"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// sanitizeGetHECOperationalSnapshotRequest states the whole request contract for
// the operational snapshot: it is unparameterised, so the only guarantee the
// handler needs is a decoded body whose unknown fields have been discarded.
func sanitizeGetHECOperationalSnapshotRequest(
	ctx context.Context,
	request *opensplunk.GetHECOperationalSnapshotRequest,
) (*opensplunk.GetHECOperationalSnapshotRequest, error) {
	return discardUnknownProtoFields(ctx, request)
}
