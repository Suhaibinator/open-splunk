package knowledgedefinition

import (
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// StageForObjectType maps a knowledge object type to its search stage and the
// 1-based stage rank that orders the prelude.
func StageForObjectType(objectType opensplunk.KnowledgeObjectType) (opensplunk.KnowledgeSearchStage, uint8, bool) {
	switch objectType {
	case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION, 1, true
	case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS, 2, true
	case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD, 3, true
	default:
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_UNSPECIFIED, 0, false
	}
}
