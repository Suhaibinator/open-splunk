package dashboards

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/protostrict"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maximumDashboardDefinitionBytes = 96 << 10
	maximumDashboardNameBytes       = 255
	maximumDashboardDescription     = 16 << 10
	maximumDashboardAppIDBytes      = 255
	maximumDashboardOwnerIDBytes    = 255
	maximumDashboardIDBytes         = 128
	maximumDashboardPanels          = 24
	maximumPanelIDBytes             = 128
	maximumPanelTitleBytes          = 255
	maximumPanelDescription         = 16 << 10
)

type indexedDefinition struct {
	name         string
	appID        string
	sharingScope opensplunk.SharingScope
}

func normalizeDefinition(
	input *opensplunk.DashboardDefinition,
	ownerID string,
) (*opensplunk.DashboardDefinition, indexedDefinition, []byte, error) {
	if input == nil {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: dashboard definition is required", control.ErrInvalidArgument)
	}
	definition := proto.Clone(input).(*opensplunk.DashboardDefinition)
	if err := protostrict.RejectUnknownFields(definition.ProtoReflect(), "dashboard definition"); err != nil {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: %w", control.ErrInvalidArgument, err)
	}
	ownerID, err := canonicalRequiredText("owner ID", ownerID, maximumDashboardOwnerIDBytes)
	if err != nil {
		return nil, indexedDefinition{}, nil, err
	}
	definition.Name, err = canonicalRequiredText("dashboard name", definition.GetName(), maximumDashboardNameBytes)
	if err != nil {
		return nil, indexedDefinition{}, nil, err
	}
	definition.Description, err = canonicalOptionalText("dashboard description", definition.Description, maximumDashboardDescription)
	if err != nil {
		return nil, indexedDefinition{}, nil, err
	}
	definition.AppId, err = canonicalRequiredText("dashboard app ID", definition.GetAppId(), maximumDashboardAppIDBytes)
	if err != nil {
		return nil, indexedDefinition{}, nil, err
	}
	if definition.OwnerId != nil {
		claimed := strings.TrimSpace(definition.GetOwnerId())
		if claimed != "" && claimed != ownerID {
			return nil, indexedDefinition{}, nil, fmt.Errorf("%w: definition owner does not match authenticated owner", control.ErrInvalidArgument)
		}
	}
	definition.OwnerId = pointer(ownerID)
	if definition.SharingScope == opensplunk.SharingScope_SHARING_SCOPE_UNSPECIFIED {
		definition.SharingScope = opensplunk.SharingScope_SHARING_SCOPE_PRIVATE
	}
	if definition.SharingScope < opensplunk.SharingScope_SHARING_SCOPE_PRIVATE ||
		definition.SharingScope > opensplunk.SharingScope_SHARING_SCOPE_GLOBAL {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: dashboard sharing scope is invalid", control.ErrInvalidArgument)
	}
	if len(definition.GetPanels()) > maximumDashboardPanels {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: dashboard contains more than %d panels", control.ErrInvalidArgument, maximumDashboardPanels)
	}
	seen := make(map[string]struct{}, len(definition.GetPanels()))
	for index, panel := range definition.GetPanels() {
		if panel == nil {
			return nil, indexedDefinition{}, nil, fmt.Errorf("%w: dashboard panel %d is required", control.ErrInvalidArgument, index)
		}
		panel.PanelId, err = canonicalRequiredText("dashboard panel ID", panel.GetPanelId(), maximumPanelIDBytes)
		if err != nil {
			return nil, indexedDefinition{}, nil, err
		}
		if _, duplicate := seen[panel.GetPanelId()]; duplicate {
			return nil, indexedDefinition{}, nil, fmt.Errorf("%w: dashboard panel IDs must be unique", control.ErrInvalidArgument)
		}
		seen[panel.GetPanelId()] = struct{}{}
		panel.Title, err = canonicalRequiredText("dashboard panel title", panel.GetTitle(), maximumPanelTitleBytes)
		if err != nil {
			return nil, indexedDefinition{}, nil, err
		}
		panel.Description, err = canonicalOptionalText("dashboard panel description", panel.Description, maximumPanelDescription)
		if err != nil {
			return nil, indexedDefinition{}, nil, err
		}
		if panel.GetSearch() == nil {
			return nil, indexedDefinition{}, nil, fmt.Errorf("%w: dashboard panel search is required", control.ErrInvalidArgument)
		}
		if panel.GetSearch().AppId != nil {
			searchApp := strings.TrimSpace(panel.GetSearch().GetAppId())
			if searchApp != "" && searchApp != definition.GetAppId() {
				return nil, indexedDefinition{}, nil, fmt.Errorf("%w: dashboard panel app ID does not match the dashboard", control.ErrInvalidArgument)
			}
		}
		panel.Search.AppId = pointer(definition.GetAppId())
		if panel.GetWidth() == 0 {
			panel.Width = 12
		}
		if panel.GetHeight() == 0 {
			panel.Height = 4
		}
		if panel.GetColumn() >= 12 || panel.GetWidth() > 12 || panel.GetColumn()+panel.GetWidth() > 12 || panel.GetHeight() > 12 || panel.GetRow() > 10_000 {
			return nil, indexedDefinition{}, nil, fmt.Errorf("%w: dashboard panel grid position is invalid", control.ErrInvalidArgument)
		}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil {
		return nil, indexedDefinition{}, nil, fmt.Errorf("encode dashboard definition: %w", err)
	}
	if len(encoded) == 0 || len(encoded) > maximumDashboardDefinitionBytes {
		return nil, indexedDefinition{}, nil, fmt.Errorf("%w: dashboard definition exceeds %d bytes", control.ErrInvalidArgument, maximumDashboardDefinitionBytes)
	}
	return definition, indexedDefinition{
		name: definition.GetName(), appID: definition.GetAppId(), sharingScope: definition.GetSharingScope(),
	}, encoded, nil
}

func canonicalRequiredText(field, value string, maximum int) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maximum || !validText(value) {
		return "", fmt.Errorf("%w: %s must contain between 1 and %d valid UTF-8 bytes", control.ErrInvalidArgument, field, maximum)
	}
	return value, nil
}

func canonicalOptionalText(field string, value *string, maximum int) (*string, error) {
	if value == nil {
		return nil, nil
	}
	normalized := strings.TrimSpace(*value)
	if normalized == "" {
		return nil, nil
	}
	if len(normalized) > maximum || !validText(normalized) {
		return nil, fmt.Errorf("%w: %s exceeds %d valid UTF-8 bytes", control.ErrInvalidArgument, field, maximum)
	}
	return pointer(normalized), nil
}

func validText(value string) bool {
	if !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) && character != '\n' && character != '\t' {
			return false
		}
	}
	return true
}

func validateID(field, value string) error {
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: %s is not canonical", control.ErrInvalidArgument, field)
	}
	_, err := canonicalRequiredText(field, value, maximumDashboardIDBytes)
	return err
}

func validateVersion(version uint64) error {
	if version == 0 || version > math.MaxInt64 {
		return fmt.Errorf("%w: expected dashboard version is invalid", control.ErrInvalidArgument)
	}
	return nil
}

func normalizeTime(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, errors.New("dashboard clock returned the zero time")
	}
	value = time.UnixMicro(value.UTC().UnixMicro()).UTC()
	if err := timestamppb.New(value).CheckValid(); err != nil {
		return time.Time{}, errors.New("dashboard clock returned an invalid protobuf timestamp")
	}
	return value, nil
}

func nextTime(value, previous time.Time) (time.Time, error) {
	normalized, err := normalizeTime(value)
	if err != nil {
		return time.Time{}, err
	}
	if normalized.After(previous) {
		return normalized, nil
	}
	return normalizeTime(time.UnixMicro(previous.UnixMicro() + 1))
}

func pointer(value string) *string {
	copy := strings.Clone(value)
	return &copy
}
