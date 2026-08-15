package knowledgedefinition

import (
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

// StageForObjectType maps a knowledge object type to its search stage and the
// 1-based stage rank that orders the prelude.
func StageForObjectType(objectType opensplunkv1.KnowledgeObjectType) (opensplunkv1.KnowledgeSearchStage, uint8, bool) {
	switch objectType {
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION, 1, true
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS, 2, true
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD, 3, true
	default:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_UNSPECIFIED, 0, false
	}
}
