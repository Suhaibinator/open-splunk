package searchhistory

import (
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
)

// cloneKnowledgeSnapshotSummary preserves absence as the legacy lifecycle
// state while validating and detaching every present summary through the
// snapshot authority. In particular, the authority rejects an amplified
// repeated shape before walking or cloning it.
func cloneKnowledgeSnapshotSummary(
	input *opensplunk.KnowledgeSnapshotSummary,
) (*opensplunk.KnowledgeSnapshotSummary, error) {
	if input == nil {
		return nil, nil
	}
	cloned, err := knowledgesnapshot.CloneSummary(input)
	if err != nil {
		return nil, invalid("knowledge snapshot summary is invalid")
	}
	return cloned, nil
}
