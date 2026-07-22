package ingest

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
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

var rawKeyValueSecretPattern = regexp.MustCompile(`(?i)(^|[^a-z0-9_])((?:authorization|proxy[-_. ]authorization|password|passwd|secret|access[-_. ]token|refresh[-_. ]token|session[-_. ]token|auth[-_. ]token|token|api[-_. ]key|client[-_. ]secret))([[:space:]]*[:=][[:space:]]*)(?:"[^"\r\n]*"|'[^'\r\n]*'|[^[:space:],;}\]"']+)`)
var rawAuthorizationSecretPattern = regexp.MustCompile(`(?i)(^|[^a-z0-9_])((?:authorization|proxy[-_. ]authorization))([[:space:]]*[:=][[:space:]]*)(?:(?:bearer|basic)[[:space:]]+)?(?:"[^"\r\n]*"|'[^'\r\n]*'|[^[:space:],;}\]"']+)`)

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
	previousWasLowerOrDigit := false
	for _, r := range strings.TrimSpace(name) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if unicode.IsUpper(r) && previousWasLowerOrDigit && !lastSeparator {
				builder.WriteByte('_')
			}
			builder.WriteRune(unicode.ToLower(r))
			lastSeparator = false
			previousWasLowerOrDigit = unicode.IsLower(r) || unicode.IsDigit(r)
			continue
		}
		if !lastSeparator && builder.Len() > 0 {
			builder.WriteByte('_')
			lastSeparator = true
		}
		previousWasLowerOrDigit = false
	}
	return strings.TrimSuffix(builder.String(), "_")
}

func (v *Validator) isSensitive(name string) bool {
	normalized := normalizeSensitiveName(name)
	if _, ok := v.sensitive[normalized]; ok {
		return true
	}
	for _, component := range strings.Split(normalized, "_") {
		switch component {
		case "authorization", "cookie", "password", "passwd", "secret", "token":
			return true
		}
	}
	return false
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
	case *opensplunkv1.TypedValue_StringValue:
		kind.StringValue = string(v.redactKeyValueText([]byte(kind.StringValue)))
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

func (v *Validator) redactText(raw []byte) []byte {
	redactedJSON := v.redactJSON(raw)
	if !bytes.Equal(redactedJSON, raw) {
		return redactedJSON
	}
	return v.redactKeyValueText(raw)
}

func (v *Validator) redactKeyValueText(raw []byte) []byte {
	raw = v.replaceSensitiveMatches(rawAuthorizationSecretPattern, raw)
	return v.replaceSensitiveMatches(rawKeyValueSecretPattern, raw)
}

func (v *Validator) replaceSensitiveMatches(pattern *regexp.Regexp, raw []byte) []byte {
	return pattern.ReplaceAllFunc(raw, func(match []byte) []byte {
		indexes := pattern.FindSubmatchIndex(match)
		if len(indexes) < 8 {
			return match
		}
		result := make([]byte, 0, len(match)+len(v.replacement))
		for group := 1; group <= 3; group++ {
			start, end := indexes[group*2], indexes[group*2+1]
			if start >= 0 {
				result = append(result, match[start:end]...)
			}
		}
		result = append(result, '"')
		result = append(result, v.replacement...)
		result = append(result, '"')
		return result
	})
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
	case string:
		redacted := v.redactKeyValueText([]byte(typed))
		return string(redacted), !bytes.Equal(redacted, []byte(typed))
	default:
		return value, false
	}
}
