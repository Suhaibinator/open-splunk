package server

import (
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
)

// boundedKnowledgeDefinitionRepeatedShape runs before reflection, proto.Size,
// cloning, or marshaling at configurable dependency boundaries. A protobuf
// wire ceiling does not bound the heap represented by repeated empty messages.
func boundedKnowledgeDefinitionRepeatedShape(
	definition *opensplunk.KnowledgeObjectDefinition,
) bool {
	if definition == nil {
		return true
	}
	if selector := definition.GetSelector(); selector != nil {
		if len(selector.GetIndexPatterns()) > knowledge.MaximumSelectorPatternsPerDimension ||
			len(selector.GetHostPatterns()) > knowledge.MaximumSelectorPatternsPerDimension ||
			len(selector.GetSourcePatterns()) > knowledge.MaximumSelectorPatternsPerDimension ||
			len(selector.GetSourcetypePatterns()) > knowledge.MaximumSelectorPatternsPerDimension {
			return false
		}
	}
	extraction := definition.GetFieldExtraction()
	return extraction == nil || extraction.GetRegex() == nil ||
		len(extraction.GetRegex().GetOutputFields()) <= knowledgedefinition.MaximumFieldExtractionOutputs
}
