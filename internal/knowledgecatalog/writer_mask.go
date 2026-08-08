package knowledgecatalog

import (
	"fmt"
	"slices"
	"strings"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var knowledgeUpdatePaths = map[string]struct{}{
	"app_id":           {},
	"name":             {},
	"description":      {},
	"sharing_scope":    {},
	"selector":         {},
	"field_extraction": {},
	"field_alias":      {},
	"calculated_field": {},
}

func normalizeKnowledgeUpdateMask(mask *fieldmaskpb.FieldMask) ([]string, error) {
	if mask == nil || len(mask.GetPaths()) == 0 || len(mask.GetPaths()) > len(knowledgeUpdatePaths) {
		return nil, invalidMutation("update mask must contain supported top-level paths")
	}
	paths := append([]string(nil), mask.GetPaths()...)
	if !slices.IsSorted(paths) {
		return nil, invalidMutation("update mask paths must be in canonical binary order")
	}
	bodyPaths := 0
	for index, path := range paths {
		if path == "" || strings.TrimSpace(path) != path || strings.Contains(path, ".") || path == "*" {
			return nil, invalidMutation("update mask contains a noncanonical path")
		}
		if _, allowed := knowledgeUpdatePaths[path]; !allowed {
			return nil, invalidMutation("update mask contains an unsupported path")
		}
		if index > 0 && paths[index-1] == path {
			return nil, invalidMutation("update mask contains duplicate paths")
		}
		if path == "field_extraction" || path == "field_alias" || path == "calculated_field" {
			bodyPaths++
		}
	}
	if bodyPaths > 1 {
		return nil, invalidMutation("update mask cannot select multiple body alternatives")
	}
	return paths, nil
}

func applyKnowledgeDefinitionMask(
	current *opensplunkv1.KnowledgeObjectDefinition,
	incoming *opensplunkv1.KnowledgeObjectDefinition,
	paths []string,
) (*opensplunkv1.KnowledgeObjectDefinition, error) {
	if current == nil || incoming == nil || len(paths) == 0 {
		return nil, fmt.Errorf("%w: update definition and mask are required", control.ErrInvalidArgument)
	}
	result, ok := proto.Clone(current).(*opensplunkv1.KnowledgeObjectDefinition)
	if !ok || result == nil {
		return nil, fmt.Errorf("%w: current definition cannot be cloned", ErrCorrupt)
	}
	for _, path := range paths {
		switch path {
		case "app_id":
			result.AppId = incoming.GetAppId()
		case "name":
			result.Name = incoming.GetName()
		case "description":
			result.Description = cloneString(incoming.Description)
		case "sharing_scope":
			result.SharingScope = incoming.GetSharingScope()
		case "selector":
			if incoming.GetSelector() == nil {
				result.Selector = nil
			} else {
				result.Selector = proto.Clone(incoming.GetSelector()).(*opensplunkv1.KnowledgeSelector)
			}
		case "field_extraction":
			if current.GetFieldExtraction() == nil || incoming.GetFieldExtraction() == nil {
				return nil, invalidMutation("knowledge object type is immutable")
			}
			result.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: proto.Clone(incoming.GetFieldExtraction()).(*opensplunkv1.FieldExtractionDefinition),
			}
		case "field_alias":
			if current.GetFieldAlias() == nil || incoming.GetFieldAlias() == nil {
				return nil, invalidMutation("knowledge object type is immutable")
			}
			result.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
				FieldAlias: proto.Clone(incoming.GetFieldAlias()).(*opensplunkv1.FieldAliasDefinition),
			}
		case "calculated_field":
			if current.GetCalculatedField() == nil || incoming.GetCalculatedField() == nil {
				return nil, invalidMutation("knowledge object type is immutable")
			}
			result.Body = &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
				CalculatedField: proto.Clone(incoming.GetCalculatedField()).(*opensplunkv1.CalculatedFieldDefinition),
			}
		default:
			return nil, invalidMutation("update mask contains an unsupported path")
		}
	}
	return result, nil
}
