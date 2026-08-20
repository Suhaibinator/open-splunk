package server

import (
	"errors"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
)

// projectKnowledgeSnapshotSummary validates dependency output before cloning
// it, then removes every retained object and lookup identity. Until the browser
// boundary has a current-policy provenance authorizer, aggregate counts and
// commitments are safe to expose but an authorized object or exact logical or
// physical lookup identity is not.
func projectKnowledgeSnapshotSummary(
	input *opensplunk.KnowledgeSnapshotSummary,
) (*opensplunk.KnowledgeSnapshotSummary, error) {
	if input == nil {
		return nil, nil
	}
	summary, err := knowledgesnapshot.CloneSummary(input)
	if err != nil {
		return nil, errors.New("dependency returned an invalid knowledge snapshot summary")
	}
	for _, object := range summary.GetObjects() {
		object.Disclosure = &opensplunk.KnowledgeSnapshotObjectSummary_Redacted{Redacted: true}
	}
	summary.LookupAssets = nil
	return summary, nil
}
