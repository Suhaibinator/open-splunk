package server

import (
	"context"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// sanitizeGetHECOperationalSnapshotRequest states the whole request contract for
// the operational snapshot: it is unparameterised, so there is nothing to
// enforce and nothing to normalize, and unknown fields are tolerated.
func sanitizeGetHECOperationalSnapshotRequest(
	_ context.Context,
	request *opensplunk.GetHECOperationalSnapshotRequest,
) (*opensplunk.GetHECOperationalSnapshotRequest, error) {
	return request, nil
}
