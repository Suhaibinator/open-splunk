package modelconfig

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
)

// overridable is an SDK request built with param.Override or param.SetJSON.
// The any result is the SDK's own signature.
type overridable interface {
	Overrides() (any, bool)
}

// OverrideMembers decodes the JSON object a whole-object SDK override sends;
// ok is false when params has none. The SDK ignores typed fields beside an
// override, and SetExtraFields replaces it, so code changing an overridden
// request edits these members and writes them back with the SDK's
// param.SetJSON and EncodeOverrideMembers.
func OverrideMembers[T overridable](params T) (members map[string]jsontext.Value, ok bool, err error) {
	if _, ok := params.Overrides(); !ok {
		return nil, false, nil
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		return nil, true, fmt.Errorf("encode native override: %w", err)
	}
	if err := json.Unmarshal(encoded, &members); err != nil {
		return nil, true, fmt.Errorf("native override must be a JSON object: %w", err)
	}
	if members == nil {
		return nil, true, errors.New("native override must be a JSON object, not null")
	}
	return members, true, nil
}

// SetOverrideMember encodes value as the override member key.
func SetOverrideMember[V any](members map[string]jsontext.Value, key string, value V) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode native override member %q: %w", key, err)
	}
	members[key] = encoded
	return nil
}

// EncodeOverrideMembers encodes members for the SDK's param.SetJSON.
func EncodeOverrideMembers(members map[string]jsontext.Value) ([]byte, error) {
	encoded, err := json.Marshal(members, json.Deterministic(true))
	if err != nil {
		return nil, fmt.Errorf("encode native override: %w", err)
	}
	return encoded, nil
}
