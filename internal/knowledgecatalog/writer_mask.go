package knowledgecatalog

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
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

// UpdateChangesSharingScope validates mask with the Writer's exact canonical
// update-mask authority and reports whether it selects app_id or
// sharing_scope. Callers may use the result to classify a rejected update as a
// scope-change attempt without trusting an unvalidated request mask.
func UpdateChangesSharingScope(mask *fieldmaskpb.FieldMask) (bool, error) {
	paths, err := normalizeKnowledgeUpdateMask(mask)
	if err != nil {
		return false, err
	}
	for _, path := range paths {
		if path == "app_id" || path == "sharing_scope" {
			return true, nil
		}
	}
	return false, nil
}

// UpdateResultMatchesRequest proves that every canonical mask-selected field
// in result agrees with the submitted definition after applying Writer's exact
// normalization rules. Unselected fields are taken from result, so callers do
// not need a racy pre-update read. Inactive opaque future bodies retain their
// canonical bytes and may match only metadata-only updates.
func UpdateResultMatchesRequest(
	result *opensplunk.KnowledgeObjectDefinition,
	submitted *opensplunk.KnowledgeObjectDefinition,
	mask *fieldmaskpb.FieldMask,
	state opensplunk.KnowledgeObjectState,
) bool {
	paths, err := normalizeKnowledgeUpdateMask(mask)
	if err != nil {
		return false
	}
	patched, err := applyKnowledgeDefinitionMask(result, submitted, paths)
	if err != nil {
		return false
	}
	resultBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(result)
	if err != nil || len(resultBytes) == 0 {
		return false
	}
	if normalized, normalizeErr := knowledgedefinition.Normalize(patched); normalizeErr == nil {
		return bytes.Equal(normalized.Bytes, resultBytes)
	}
	digest := sha256.Sum256(resultBytes)
	if _, err := knowledgedefinition.DecodeCanonicalInactiveFutureBody(
		resultBytes,
		digest[:],
		state,
	); err != nil {
		return false
	}
	patchedBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(patched)
	return err == nil && bytes.Equal(patchedBytes, resultBytes)
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
	current *opensplunk.KnowledgeObjectDefinition,
	incoming *opensplunk.KnowledgeObjectDefinition,
	paths []string,
) (*opensplunk.KnowledgeObjectDefinition, error) {
	if current == nil || incoming == nil || len(paths) == 0 {
		return nil, fmt.Errorf("%w: update definition and mask are required", control.ErrInvalidArgument)
	}
	result, ok := proto.Clone(current).(*opensplunk.KnowledgeObjectDefinition)
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
				result.Selector = proto.Clone(incoming.GetSelector()).(*opensplunk.KnowledgeSelector)
			}
		case "field_extraction":
			if current.GetFieldExtraction() == nil || incoming.GetFieldExtraction() == nil {
				return nil, invalidMutation("knowledge object type is immutable")
			}
			result.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: proto.Clone(incoming.GetFieldExtraction()).(*opensplunk.FieldExtractionDefinition),
			}
		case "field_alias":
			if current.GetFieldAlias() == nil || incoming.GetFieldAlias() == nil {
				return nil, invalidMutation("knowledge object type is immutable")
			}
			result.Body = &opensplunk.KnowledgeObjectDefinition_FieldAlias{
				FieldAlias: proto.Clone(incoming.GetFieldAlias()).(*opensplunk.FieldAliasDefinition),
			}
		case "calculated_field":
			if current.GetCalculatedField() == nil || incoming.GetCalculatedField() == nil {
				return nil, invalidMutation("knowledge object type is immutable")
			}
			result.Body = &opensplunk.KnowledgeObjectDefinition_CalculatedField{
				CalculatedField: proto.Clone(incoming.GetCalculatedField()).(*opensplunk.CalculatedFieldDefinition),
			}
		default:
			return nil, invalidMutation("update mask contains an unsupported path")
		}
	}
	return result, nil
}
