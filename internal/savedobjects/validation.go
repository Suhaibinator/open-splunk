package savedobjects

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	maximumDefinitionBytes = 262144
	maximumSPLBytes        = 131072
	maximumDescription     = 16384
	maximumExpressionBytes = 1024
	maximumFieldNameBytes  = 255
	maximumRepeatedFields  = 256
	maximumListFilterText  = 255
)

type indexedDefinition struct {
	name         string
	appID        string
	ownerID      string
	sharingScope opensplunkv1.SharingScope
}

func normalizeAndEncodeDefinition(input *opensplunkv1.SavedSearchDefinition, ownerID string) (*opensplunkv1.SavedSearchDefinition, indexedDefinition, []byte, error) {
	if input == nil {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: saved-search definition is required", control.ErrInvalidArgument)
	}
	definition := proto.Clone(input).(*opensplunkv1.SavedSearchDefinition)
	ownerID = strings.TrimSpace(ownerID)
	if err := validateIdentifierText("owner ID", ownerID, 255, false); err != nil {
		return nil, indexedDefinition{}, nil, err
	}
	if err := rejectUnknownFields(definition.ProtoReflect()); err != nil {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: %v", control.ErrInvalidArgument, err)
	}

	name, err := normalizeRequiredText("name", definition.Name, 255)
	if err != nil {
		return nil, indexedDefinition{}, nil, err
	}
	definition.Name = name
	if definition.Description != nil {
		description := strings.TrimSpace(*definition.Description)
		if err := validateText("description", description, maximumDescription, true); err != nil {
			return nil, indexedDefinition{}, nil, err
		}
		if description == "" {
			definition.Description = nil
		} else {
			definition.Description = stringPointer(description)
		}
	}
	if definition.Search == nil {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: search definition is required", control.ErrInvalidArgument)
	}
	if err := normalizeSearchDefinition(definition.Search); err != nil {
		return nil, indexedDefinition{}, nil, err
	}

	if definition.OwnerId != nil {
		claimed := strings.TrimSpace(*definition.OwnerId)
		if claimed != "" && claimed != ownerID {
			return nil, indexedDefinition{}, nil, fmt.Errorf("%w: definition owner does not match authenticated owner", control.ErrInvalidArgument)
		}
	}
	definition.OwnerId = stringPointer(ownerID)
	if definition.SharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_UNSPECIFIED {
		definition.SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE
	}
	if !validSharingScope(definition.SharingScope) {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: sharing scope is invalid", control.ErrInvalidArgument)
	}
	appID := definition.Search.GetAppId()
	if definition.SharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_APP && appID == "" {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: app-shared saved searches require an app ID", control.ErrInvalidArgument)
	}

	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil {
		return nil, indexedDefinition{}, nil, fmt.Errorf("encode saved-search definition: %w", err)
	}
	if len(encoded) == 0 || len(encoded) > maximumDefinitionBytes {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: saved-search definition exceeds %d bytes", control.ErrInvalidArgument, maximumDefinitionBytes)
	}
	return definition, indexedDefinition{
		name: name, appID: appID, ownerID: ownerID, sharingScope: definition.SharingScope,
	}, encoded, nil
}

func normalizeSearchDefinition(search *opensplunkv1.SearchDefinition) error {
	if !utf8.ValidString(search.Spl) || strings.TrimSpace(search.Spl) == "" || len(search.Spl) > maximumSPLBytes {
		return fmt.Errorf("%w: SPL must contain between 1 and %d valid UTF-8 bytes", control.ErrInvalidArgument, maximumSPLBytes)
	}
	if strings.IndexByte(search.Spl, 0) >= 0 {
		return fmt.Errorf("%w: SPL cannot contain NUL", control.ErrInvalidArgument)
	}
	if search.AppId != nil {
		appID := strings.TrimSpace(*search.AppId)
		if err := validateIdentifierText("app ID", appID, 255, true); err != nil {
			return err
		}
		if appID == "" {
			search.AppId = nil
		} else {
			search.AppId = stringPointer(appID)
		}
	}
	if search.TimeRange != nil {
		for _, field := range []struct {
			name  string
			value *string
		}{
			{name: "earliest time", value: search.TimeRange.Earliest},
			{name: "latest time", value: search.TimeRange.Latest},
		} {
			name, value := field.name, field.value
			if value == nil {
				continue
			}
			normalized := strings.TrimSpace(*value)
			if err := validateIdentifierText(name, normalized, maximumExpressionBytes, false); err != nil {
				return err
			}
			*value = normalized
		}
		if search.TimeRange.Timezone != nil {
			timezone := strings.TrimSpace(*search.TimeRange.Timezone)
			if err := validateIdentifierText("timezone", timezone, maximumFieldNameBytes, false); err != nil {
				return err
			}
			search.TimeRange.Timezone = stringPointer(timezone)
		}
	}
	var err error
	search.IndexScope, err = normalizeStringList("index scope", search.IndexScope, maximumRepeatedFields)
	if err != nil {
		return err
	}
	search.SelectedFields, err = normalizeStringList("selected field", search.SelectedFields, maximumRepeatedFields)
	if err != nil {
		return err
	}
	if search.PreferredResultTab == opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED {
		search.PreferredResultTab = opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_EVENTS
	}
	if search.PreferredResultTab < opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_EVENTS || search.PreferredResultTab > opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_VISUALIZATION {
		return fmt.Errorf("%w: preferred result tab is invalid", control.ErrInvalidArgument)
	}
	if search.Visualization != nil {
		if err := normalizeVisualization(search.Visualization); err != nil {
			return err
		}
	}
	return nil
}

func normalizeVisualization(visualization *opensplunkv1.VisualizationSpec) error {
	if visualization.Type < opensplunkv1.VisualizationType_VISUALIZATION_TYPE_TABLE || visualization.Type > opensplunkv1.VisualizationType_VISUALIZATION_TYPE_SCATTER {
		return fmt.Errorf("%w: visualization type is invalid", control.ErrInvalidArgument)
	}
	for _, field := range []struct {
		name  string
		value **string
	}{
		{name: "visualization title", value: &visualization.Title},
		{name: "visualization x field", value: &visualization.XField},
		{name: "visualization series field", value: &visualization.SeriesField},
	} {
		name, value := field.name, field.value
		if *value == nil {
			continue
		}
		normalized := strings.TrimSpace(**value)
		if err := validateIdentifierText(name, normalized, maximumFieldNameBytes, true); err != nil {
			return err
		}
		if normalized == "" {
			*value = nil
		} else {
			*value = stringPointer(normalized)
		}
	}
	var err error
	visualization.YFields, err = normalizeStringList("visualization y field", visualization.YFields, maximumRepeatedFields)
	if err != nil {
		return err
	}
	if visualization.StackMode == opensplunkv1.VisualizationStackMode_VISUALIZATION_STACK_MODE_UNSPECIFIED {
		visualization.StackMode = opensplunkv1.VisualizationStackMode_VISUALIZATION_STACK_MODE_NONE
	}
	if visualization.StackMode < opensplunkv1.VisualizationStackMode_VISUALIZATION_STACK_MODE_NONE || visualization.StackMode > opensplunkv1.VisualizationStackMode_VISUALIZATION_STACK_MODE_STACKED_100_PERCENT {
		return fmt.Errorf("%w: visualization stack mode is invalid", control.ErrInvalidArgument)
	}
	if duration := visualization.TimeBucketWidth; duration != nil {
		if err := duration.CheckValid(); err != nil || duration.AsDuration() <= 0 {
			return fmt.Errorf("%w: visualization time bucket width must be a valid positive duration", control.ErrInvalidArgument)
		}
	}
	return nil
}

func normalizeStringList(field string, values []string, maximum int) ([]string, error) {
	if len(values) > maximum {
		return nil, fmt.Errorf("%w: %s contains more than %d entries", control.ErrInvalidArgument, field, maximum)
	}
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if err := validateIdentifierText(field, value, maximumFieldNameBytes, false); err != nil {
			return nil, err
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	return normalized, nil
}

func normalizeRequiredText(field, value string, maximum int) (string, error) {
	value = strings.TrimSpace(value)
	if err := validateIdentifierText(field, value, maximum, false); err != nil {
		return "", err
	}
	return value, nil
}

func validateIdentifierText(field, value string, maximum int, allowEmpty bool) error {
	if err := validateText(field, value, maximum, allowEmpty); err != nil {
		return err
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: %s cannot contain control characters", control.ErrInvalidArgument, field)
		}
	}
	return nil
}

func validateText(field, value string, maximum int, allowEmpty bool) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s must be valid UTF-8", control.ErrInvalidArgument, field)
	}
	if (!allowEmpty && value == "") || len(value) > maximum {
		return fmt.Errorf("%w: %s must contain between %d and %d bytes", control.ErrInvalidArgument, field, boolMinimum(allowEmpty), maximum)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: %s cannot contain NUL", control.ErrInvalidArgument, field)
	}
	return nil
}

func boolMinimum(allowEmpty bool) int {
	if allowEmpty {
		return 0
	}
	return 1
}

func validSharingScope(scope opensplunkv1.SharingScope) bool {
	return scope >= opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE && scope <= opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL
}

func rejectUnknownFields(message protoreflect.Message) error {
	if len(message.GetUnknown()) != 0 {
		return errors.New("saved-search definition contains unknown protobuf fields")
	}
	var visitErr error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsMap() {
			if field.MapValue().Kind() != protoreflect.MessageKind {
				return true
			}
			value.Map().Range(func(_ protoreflect.MapKey, mapValue protoreflect.Value) bool {
				visitErr = rejectUnknownFields(mapValue.Message())
				return visitErr == nil
			})
			return visitErr == nil
		}
		if field.IsList() {
			if field.Kind() != protoreflect.MessageKind {
				return true
			}
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if visitErr = rejectUnknownFields(list.Get(index).Message()); visitErr != nil {
					return false
				}
			}
			return true
		}
		if field.Kind() == protoreflect.MessageKind {
			visitErr = rejectUnknownFields(value.Message())
			return visitErr == nil
		}
		return true
	})
	return visitErr
}

type updatePaths struct {
	full  bool
	paths []string
}

func normalizeUpdateMask(mask *fieldmaskpb.FieldMask) (updatePaths, error) {
	if mask == nil || len(mask.Paths) == 0 {
		return updatePaths{full: true}, nil
	}
	if len(mask.Paths) > 32 {
		return updatePaths{}, fmt.Errorf("%w: update mask contains too many paths", control.ErrInvalidArgument)
	}
	allowed := map[string]struct{}{
		"name": {}, "description": {}, "search": {}, "sharing_scope": {}, "owner_id": {},
	}
	paths := make([]string, 0, len(mask.Paths))
	seen := make(map[string]struct{}, len(mask.Paths))
	full := false
	for _, path := range mask.Paths {
		path = strings.TrimSpace(path)
		path = strings.TrimPrefix(path, "definition.")
		if path == "*" || path == "definition" {
			full = true
			continue
		}
		if _, ok := allowed[path]; !ok {
			return updatePaths{}, fmt.Errorf("%w: update mask path %q is unsupported", control.ErrInvalidArgument, path)
		}
		if _, duplicate := seen[path]; duplicate {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	if full {
		return updatePaths{full: true}, nil
	}
	slices.Sort(paths)
	return updatePaths{paths: paths}, nil
}

func applyDefinitionUpdate(current, incoming *opensplunkv1.SavedSearchDefinition, paths updatePaths) (*opensplunkv1.SavedSearchDefinition, error) {
	if current == nil || incoming == nil {
		return nil, fmt.Errorf("%w: saved-search definition is required", control.ErrInvalidArgument)
	}
	if paths.full {
		return proto.Clone(incoming).(*opensplunkv1.SavedSearchDefinition), nil
	}
	result := proto.Clone(current).(*opensplunkv1.SavedSearchDefinition)
	for _, path := range paths.paths {
		switch path {
		case "name":
			result.Name = incoming.Name
		case "description":
			result.Description = cloneStringPointer(incoming.Description)
		case "search":
			result.Search = cloneSearch(incoming.Search)
		case "sharing_scope":
			result.SharingScope = incoming.SharingScope
		case "owner_id":
			result.OwnerId = cloneStringPointer(incoming.OwnerId)
		}
	}
	return result, nil
}

func cloneSearch(value *opensplunkv1.SearchDefinition) *opensplunkv1.SearchDefinition {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*opensplunkv1.SearchDefinition)
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	return stringPointer(*value)
}
