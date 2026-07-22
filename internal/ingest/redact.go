package ingest

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"unicode"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

var mandatorySensitiveFields = []string{
	"authorization",
	"proxy_authorization",
	"cookie",
	"set_cookie",
	"password",
	"passwd",
	"secret",
	"token",
	"access_token",
	"refresh_token",
	"session_token",
	"auth_token",
	"api_key",
	"apikey",
	"client_secret",
	"private_key",
}

func sensitiveFieldSet(additional []string) map[string]struct{} {
	result := make(map[string]struct{}, len(mandatorySensitiveFields)+len(additional))
	for _, name := range mandatorySensitiveFields {
		result[normalizeSensitiveName(name)] = struct{}{}
	}
	for _, name := range additional {
		if normalized := normalizeSensitiveName(name); normalized != "" {
			result[normalized] = struct{}{}
		}
	}
	return result
}

func normalizeSensitiveName(name string) string {
	var builder strings.Builder
	lastSeparator := false
	for _, r := range strings.TrimSpace(strings.ToLower(name)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
			lastSeparator = false
			continue
		}
		if !lastSeparator && builder.Len() > 0 {
			builder.WriteByte('_')
			lastSeparator = true
		}
	}
	return strings.TrimSuffix(builder.String(), "_")
}

func (v *Validator) isSensitive(name string) bool {
	normalized := normalizeSensitiveName(name)
	if _, ok := v.sensitive[normalized]; ok {
		return true
	}
	return strings.HasSuffix(normalized, "_password") ||
		strings.HasSuffix(normalized, "_secret") ||
		strings.HasSuffix(normalized, "_token")
}

func (v *Validator) redactObject(object *opensplunkv1.TypedObject) {
	if object == nil {
		return
	}
	for _, field := range object.GetFields() {
		if field == nil {
			continue
		}
		if v.isSensitive(field.GetName()) {
			field.Value = &opensplunkv1.TypedValue{
				Kind: &opensplunkv1.TypedValue_StringValue{StringValue: v.replacement},
			}
			continue
		}
		v.redactValue(field.GetValue())
	}
}

func (v *Validator) redactValue(value *opensplunkv1.TypedValue) {
	if value == nil {
		return
	}
	switch kind := value.GetKind().(type) {
	case *opensplunkv1.TypedValue_ObjectValue:
		v.redactObject(kind.ObjectValue)
	case *opensplunkv1.TypedValue_ListValue:
		if kind.ListValue == nil {
			return
		}
		for _, item := range kind.ListValue.GetValues() {
			v.redactValue(item)
		}
	}
}

// redactJSON preserves non-JSON and JSON without sensitive keys byte-for-byte.
// JSON numbers are decoded with UseNumber so sanitization does not coerce large
// integers through float64.
func (v *Validator) redactJSON(raw []byte) []byte {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return raw
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return raw
	}
	redacted, changed := v.redactJSONValue(value)
	if !changed {
		return raw
	}
	encoded, err := json.Marshal(redacted)
	if err != nil {
		return raw
	}
	return encoded
}

func (v *Validator) redactJSONValue(value any) (any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		changed := false
		for name, child := range typed {
			if v.isSensitive(name) {
				typed[name] = v.replacement
				changed = true
				continue
			}
			redacted, childChanged := v.redactJSONValue(child)
			if childChanged {
				typed[name] = redacted
				changed = true
			}
		}
		return typed, changed
	case []any:
		changed := false
		for i, child := range typed {
			redacted, childChanged := v.redactJSONValue(child)
			if childChanged {
				typed[i] = redacted
				changed = true
			}
		}
		return typed, changed
	default:
		return value, false
	}
}
