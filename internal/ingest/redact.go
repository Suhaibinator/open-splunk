package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
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

type rawSecretKind uint8

type redactionMatch struct {
	kind        rawSecretKind
	replacement string
	order       int
}

type redactionChange struct {
	changed                 bool
	match                   redactionMatch
	requiresOrderedFallback bool
}

func changedBy(match redactionMatch) redactionChange {
	return redactionChange{
		changed: true,
		match:   match,
	}
}

func boundaryChangedBy(match redactionMatch) redactionChange {
	change := changedBy(match)
	change.requiresOrderedFallback = true
	return change
}

func canonicalizedJSON() redactionChange {
	return redactionChange{changed: true}
}

func (change redactionChange) hasPolicyMatch() bool {
	return change.match.kind != rawSecretNone
}

// A text replacement generated before the last policy can supply quoting or a
// key boundary that makes previously malformed trailing text match later.
func (change redactionChange) canAffectLaterPolicy(policyCount int) bool {
	return change.hasPolicyMatch() && change.match.order < policyCount-1
}

func (change *redactionChange) merge(other redactionChange) {
	if !other.changed {
		return
	}
	if other.hasPolicyMatch() &&
		(!change.hasPolicyMatch() || other.match.order < change.match.order) {
		change.match = other.match
	}
	change.requiresOrderedFallback = change.requiresOrderedFallback || other.requiresOrderedFallback
	change.changed = true
}

const (
	maxEmbeddedJSONRedactionDepth = 8
	maxEscapedQuotedKeyCandidates = 8
)

const (
	rawSecretNone rawSecretKind = iota
	rawSecretValue
	rawSecretAuthorization
	rawSecretCookie
	rawSecretPrivateKey
)

func sensitiveFieldSet(additional []string, mandatory, exact bool) map[string]struct{} {
	capacity := len(additional)
	if mandatory {
		capacity += len(mandatorySensitiveFields)
	}
	result := make(map[string]struct{}, capacity)
	if mandatory {
		for _, name := range mandatorySensitiveFields {
			result[normalizeSensitiveName(name)] = struct{}{}
		}
	}
	for _, name := range additional {
		if exact {
			result[name] = struct{}{}
			continue
		}
		if normalized := normalizeSensitiveName(name); normalized != "" {
			result[normalized] = struct{}{}
		}
	}
	return result
}

func normalizeSensitiveName(name string) string {
	runes := []rune(strings.TrimSpace(name))
	var builder strings.Builder
	lastSeparator := false
	previousWasLowerOrDigit := false
	previousWasUpper := false
	for index, r := range runes {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			nextIsLower := index+1 < len(runes) && unicode.IsLower(runes[index+1])
			if unicode.IsUpper(r) && !lastSeparator && (previousWasLowerOrDigit || previousWasUpper && nextIsLower) {
				builder.WriteByte('_')
			}
			builder.WriteRune(unicode.ToLower(r))
			lastSeparator = false
			previousWasLowerOrDigit = unicode.IsLower(r) || unicode.IsDigit(r)
			previousWasUpper = unicode.IsUpper(r)
			continue
		}
		if !lastSeparator && builder.Len() > 0 {
			builder.WriteByte('_')
			lastSeparator = true
		}
		previousWasLowerOrDigit = false
		previousWasUpper = false
	}
	return strings.TrimSuffix(builder.String(), "_")
}

func (v *Validator) isSensitive(name string) bool {
	return v.matchSensitiveName(name, true).kind != rawSecretNone
}

func (v *Validator) matchSensitiveName(name string, allowComponentMatch bool) redactionMatch {
	if v.exact {
		if match, ok := v.replacementByName[name]; ok {
			return match
		}
		if _, ok := v.sensitive[name]; ok {
			return redactionMatch{
				kind:        rawSecretKindForName(name),
				replacement: v.replacement,
			}
		}
		return redactionMatch{}
	}
	normalized := normalizeSensitiveName(name)
	components := strings.Split(normalized, "_")
	if _, ok := v.sensitive[normalized]; ok {
		return redactionMatch{
			kind:        rawSecretKindForComponents(components),
			replacement: v.replacement,
		}
	}
	if v.mandatory && allowComponentMatch &&
		hasMandatorySensitiveComponent(components) {
		return redactionMatch{
			kind:        rawSecretKindForComponents(components),
			replacement: v.replacement,
		}
	}
	return redactionMatch{}
}

func (v *Validator) fallbackMatch() redactionMatch {
	return redactionMatch{
		kind:        rawSecretValue,
		replacement: v.replacement,
	}
}

func (v *Validator) depthLimitReplacement() string {
	if v.depthReplacement != "" {
		return v.depthReplacement
	}
	return v.replacement
}

// IsSensitiveField reports whether name is covered by the validator's
// mandatory or deployment-specific redaction policy. Collectors use it while
// tracing rename processors backwards so the original raw key is sanitized
// when a pipeline later gives that value a sensitive name.
func (v *Validator) IsSensitiveField(name string) bool {
	return v != nil && v.isSensitive(name)
}

func hasMandatorySensitiveComponent(components []string) bool {
	for index, component := range components {
		if hasMandatorySensitiveFamilyAffix(component) {
			return true
		}
		if index+1 < len(components) &&
			(((component == "api" || strings.HasSuffix(component, "api")) && components[index+1] == "key") ||
				((component == "private" || strings.HasSuffix(component, "private")) && components[index+1] == "key")) {
			return true
		}
	}
	return false
}

func hasMandatorySensitiveFamilyAffix(component string) bool {
	for _, family := range []string{"authorization", "cookie", "password", "passwd", "secret", "token", "apikey", "privatekey"} {
		if hasSensitiveFamilyAffix(component, family) {
			return true
		}
	}
	return false
}

func hasSensitiveFamilyAffix(component, family string) bool {
	return component == family || strings.HasPrefix(component, family) || strings.HasSuffix(component, family)
}

func (v *Validator) redactObject(object *opensplunk.TypedObject) {
	if object == nil {
		return
	}
	for _, field := range object.GetFields() {
		if field == nil {
			continue
		}
		if v.orderedOnChange {
			if match := v.matchSensitiveName(field.GetName(), true); match.kind != rawSecretNone {
				v.redactFieldInPolicyOrder(field, match.order)
				continue
			}
		}
		if v.redactFieldByName(field) {
			continue
		}
		v.redactValue(field.GetValue())
	}
}

func (v *Validator) redactFieldInPolicyOrder(
	field *opensplunk.TypedObjectField,
	startPolicy int,
) {
	for _, redactor := range v.ordered[startPolicy:] {
		if redactor.redactFieldByName(field) {
			continue
		}
		redactor.redactValue(field.GetValue())
	}
}

func (v *Validator) redactFieldByName(field *opensplunk.TypedObjectField) bool {
	match := v.matchSensitiveName(field.GetName(), true)
	if match.kind == rawSecretNone {
		return false
	}
	redactTypedFieldValue(field, match.replacement)
	return true
}

func redactTypedFieldValue(
	field *opensplunk.TypedObjectField,
	replacement string,
) {
	if len(field.ProtoReflect().GetUnknown()) > 0 {
		field.ProtoReflect().SetUnknown(nil)
	}
	value := field.GetValue()
	if current, ok := value.GetKind().(*opensplunk.TypedValue_StringValue); ok &&
		current.StringValue == replacement &&
		len(value.ProtoReflect().GetUnknown()) == 0 {
		return
	}
	field.Value = &opensplunk.TypedValue{
		Kind: &opensplunk.TypedValue_StringValue{StringValue: replacement},
	}
}

func (v *Validator) redactValue(value *opensplunk.TypedValue) {
	if value == nil {
		return
	}
	switch kind := value.GetKind().(type) {
	case *opensplunk.TypedValue_StringValue:
		kind.StringValue = string(v.redactText([]byte(kind.StringValue)))
	case *opensplunk.TypedValue_ObjectValue:
		v.redactObject(kind.ObjectValue)
	case *opensplunk.TypedValue_ListValue:
		if kind.ListValue == nil {
			return
		}
		for _, item := range kind.ListValue.GetValues() {
			v.redactValue(item)
		}
	}
}

func redactTopLevelObjectWithReplacements(
	object *opensplunk.TypedObject,
	replacements map[string]string,
) {
	if object == nil {
		return
	}
	for _, field := range object.GetFields() {
		if field == nil {
			continue
		}
		replacement, sensitive := replacements[field.GetName()]
		if !sensitive {
			continue
		}
		redactTypedFieldValue(field, replacement)
	}
}

// redactTopLevelJSONWithReplacements replaces only selected members of a root
// JSON object. parsed distinguishes ordinary non-JSON text from valid unchanged
// JSON so the caller does not scan nested JSON content as flat key/value text.
func redactTopLevelJSONWithReplacements(
	raw []byte,
	replacements map[string]string,
) (redacted []byte, parsed bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, duplicateKey, err := decodeJSONValue(decoder)
	if err != nil {
		return raw, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return raw, false
	}
	object, isObject := value.(map[string]any)
	if !isObject {
		return raw, true
	}
	changed := false
	for name, current := range object {
		replacement, sensitive := replacements[name]
		if !sensitive {
			continue
		}
		if text, ok := current.(string); ok && text == replacement {
			continue
		}
		object[name] = replacement
		changed = true
	}
	if !changed && !duplicateKey {
		return raw, true
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		// Values decoded by encoding/json plus validated replacement strings are
		// marshalable. Fail closed if that invariant changes.
		return []byte(DefaultRedactionReplacement), true
	}
	return encoded, true
}

// redactJSON preserves ordinary JSON without sensitive keys byte-for-byte.
// Objects with duplicate keys are canonicalized with deterministic last-key
// semantics so a secret in a shadowed member cannot survive in the raw bytes.
// parsed is separate from changed so valid, unchanged JSON does not take the
// plain-text scanner path a second time.
// JSON numbers are decoded with UseNumber so sanitization does not coerce large
// integers through float64.
func (v *Validator) redactJSON(raw []byte) (redacted []byte, parsed bool) {
	redacted, parsed, _ = v.redactJSONDepth(raw, 0, true)
	return redacted, parsed
}

func (v *Validator) redactJSONDepth(
	raw []byte,
	embeddedDepth int,
	allowOrderedReplay bool,
) (redacted []byte, parsed bool, change redactionChange) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, duplicateKey, err := decodeJSONValue(decoder)
	if err != nil {
		return raw, false, redactionChange{}
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return raw, false, redactionChange{}
	}
	redactedValue, change := v.redactJSONValue(value, embeddedDepth, allowOrderedReplay)
	if duplicateKey {
		change.merge(canonicalizedJSON())
	}
	if !change.changed {
		return raw, true, redactionChange{}
	}
	if !allowOrderedReplay && change.hasPolicyMatch() {
		return raw, true, change
	}
	encoded, err := json.Marshal(redactedValue)
	if err != nil {
		// Values produced by encoding/json plus the replacement string are
		// marshalable. Keep this branch fail-closed if that invariant changes.
		fallback := v.fallbackMatch()
		return []byte(fallback.replacement), true, changedBy(fallback)
	}
	return encoded, true, change
}

func decodeJSONValue(decoder *json.Decoder) (any, bool, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, false, err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return token, false, nil
	}

	switch delimiter {
	case '{':
		object := make(map[string]any)
		duplicate := false
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return nil, false, keyErr
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, false, fmt.Errorf("JSON object key has type %T", keyToken)
			}
			child, childDuplicate, childErr := decodeJSONValue(decoder)
			if childErr != nil {
				return nil, false, childErr
			}
			if _, exists := object[key]; exists {
				duplicate = true
			}
			object[key] = child
			duplicate = duplicate || childDuplicate
		}
		closing, closeErr := decoder.Token()
		if closeErr != nil {
			return nil, false, closeErr
		}
		if closing != json.Delim('}') {
			return nil, false, fmt.Errorf("JSON object closed with %v", closing)
		}
		return object, duplicate, nil
	case '[':
		array := make([]any, 0)
		duplicate := false
		for decoder.More() {
			child, childDuplicate, childErr := decodeJSONValue(decoder)
			if childErr != nil {
				return nil, false, childErr
			}
			array = append(array, child)
			duplicate = duplicate || childDuplicate
		}
		closing, closeErr := decoder.Token()
		if closeErr != nil {
			return nil, false, closeErr
		}
		if closing != json.Delim(']') {
			return nil, false, fmt.Errorf("JSON array closed with %v", closing)
		}
		return array, duplicate, nil
	default:
		return nil, false, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func (v *Validator) redactText(raw []byte) []byte {
	redacted, change := v.redactTextChanged(raw, !v.orderedOnChange)
	if v.orderedOnChange {
		if !change.hasPolicyMatch() {
			return redacted
		}
		redacted, _ = v.redactTextInPolicyOrder(raw)
		return redacted
	}
	return redacted
}

func (v *Validator) redactEventRaw(
	raw []byte,
	encoding opensplunk.RawEncoding,
) []byte {
	if encoding == opensplunk.RawEncoding_RAW_ENCODING_UTF8 || utf8.Valid(raw) {
		return v.redactText(raw)
	}

	// Binary payloads can still contain ASCII credentials. The raw scanner is
	// byte-oriented and preserves unrelated invalid UTF-8 verbatim. If a policy
	// before the last one changes the payload, replaying in policy order also
	// preserves the historical transition to the full text scanner when that
	// rewrite removes the only invalid byte.
	redacted, change := v.redactKeyValueTextChanged(raw, !v.orderedOnChange)
	if v.orderedOnChange {
		if !change.changed {
			return redacted
		}
		return v.redactEventRawInPolicyOrder(raw, encoding)
	}
	if len(v.ordered) > 0 &&
		(change.requiresOrderedFallback ||
			change.canAffectLaterPolicy(len(v.ordered))) {
		return v.redactEventRawInPolicyOrder(raw, encoding)
	}
	return redacted
}

func (v *Validator) redactEventRawInPolicyOrder(
	raw []byte,
	encoding opensplunk.RawEncoding,
) []byte {
	redacted := raw
	for _, redactor := range v.ordered {
		redacted = redactor.redactEventRaw(redacted, encoding)
	}
	return redacted
}

// redactTextChanged reports a possible change even when ordered replay is
// disabled. Composite configurations that must preserve historical cascades
// use that mode for one bounded detection pass, then replay the original text
// exactly once.
func (v *Validator) redactTextChanged(
	raw []byte,
	allowOrderedReplay bool,
) ([]byte, redactionChange) {
	if redactedJSON, parsed, change := v.redactJSONDepth(raw, 0, allowOrderedReplay); parsed {
		return redactedJSON, change
	}
	embedded, embeddedChange := v.redactEmbeddedJSONStringLiteralsChanged(
		raw,
		0,
		allowOrderedReplay,
	)
	if embeddedChange.hasPolicyMatch() && len(v.ordered) > 0 {
		if !allowOrderedReplay {
			return raw, embeddedChange
		}
		return v.redactTextInPolicyOrder(raw)
	}
	redacted, textChange := v.redactKeyValueTextChanged(embedded, allowOrderedReplay)
	if len(v.ordered) > 0 &&
		(textChange.requiresOrderedFallback ||
			textChange.canAffectLaterPolicy(len(v.ordered))) {
		if !allowOrderedReplay {
			return raw, textChange
		}
		return v.redactTextInPolicyOrder(raw)
	}
	embeddedChange.merge(textChange)
	return redacted, embeddedChange
}

func (v *Validator) redactKeyValueText(raw []byte) []byte {
	redacted, change := v.redactKeyValueTextChanged(raw, !v.orderedOnChange)
	if v.orderedOnChange {
		if !change.changed {
			return redacted
		}
		return v.redactKeyValueTextInPolicyOrder(raw)
	}
	if len(v.ordered) > 0 &&
		(change.requiresOrderedFallback ||
			change.canAffectLaterPolicy(len(v.ordered))) {
		redacted = v.redactKeyValueTextInPolicyOrder(raw)
	}
	return redacted
}

func (v *Validator) redactTextInPolicyOrder(raw []byte) ([]byte, redactionChange) {
	redacted := raw
	var change redactionChange
	for order, redactor := range v.ordered {
		next := redactor.redactText(redacted)
		if !bytes.Equal(next, redacted) {
			change.merge(changedBy(redactionMatch{
				kind:        rawSecretValue,
				replacement: redactor.replacement,
				order:       order,
			}))
		}
		redacted = next
	}
	return redacted, change
}

func (v *Validator) redactKeyValueTextInPolicyOrder(raw []byte) []byte {
	redacted := raw
	for _, redactor := range v.ordered {
		redacted = redactor.redactKeyValueText(redacted)
	}
	return redacted
}

func (v *Validator) redactKeyValueTextChanged(
	raw []byte,
	materializeChanges bool,
) ([]byte, redactionChange) {
	var result []byte
	copyFrom := 0
	var change redactionChange
	for cursor := 0; cursor < len(raw); cursor++ {
		if raw[cursor] != '=' && raw[cursor] != ':' {
			continue
		}
		match, quotedKey, encodedQuotedKey := v.rawSecretMatchBefore(raw, cursor)
		if match.kind == rawSecretNone {
			continue
		}
		if encodedQuotedKey {
			// This is a quoted key inside another encoded string (for example,
			// prose containing {\"password\":\"...\"}). Precisely rewriting one
			// value would require decoding and re-encoding the surrounding string.
			// Fail closed at the current string/raw boundary instead.
			boundaryChange := boundaryChangedBy(match)
			if len(v.ordered) > 0 {
				return raw, boundaryChange
			}
			return []byte(match.replacement), boundaryChange
		}

		valueStart := cursor + 1
		for valueStart < len(raw) && (isHorizontalSpace(raw[valueStart]) ||
			(quotedKey && (raw[valueStart] == '\r' || raw[valueStart] == '\n'))) {
			valueStart++
		}
		valueEnd, ambiguousValue := v.rawSecretValueEnd(raw, valueStart, match.kind)
		if ambiguousValue {
			boundaryChange := boundaryChangedBy(match)
			if len(v.ordered) > 0 {
				return raw, boundaryChange
			}
			return []byte(match.replacement), boundaryChange
		}
		if valueEnd < valueStart || (valueEnd == valueStart && match.kind == rawSecretValue) {
			continue
		}
		requiresOrderedFallback := match.order > 0 &&
			bytes.ContainsAny(raw[valueStart:valueEnd], "=:")
		matchChange := changedBy(match)
		matchChange.requiresOrderedFallback = requiresOrderedFallback
		if len(v.ordered) > 0 &&
			(matchChange.requiresOrderedFallback ||
				matchChange.canAffectLaterPolicy(len(v.ordered))) {
			return raw, matchChange
		}
		if !materializeChanges {
			return raw, matchChange
		}

		if result == nil {
			result = make([]byte, 0, len(raw)+len(match.replacement))
		}
		result = append(result, raw[copyFrom:valueStart]...)
		result = append(result, '"')
		result = append(result, match.replacement...)
		result = append(result, '"')
		copyFrom = valueEnd
		cursor = valueEnd - 1
		change.merge(matchChange)
	}
	if result == nil {
		return raw, redactionChange{}
	}
	return append(result, raw[copyFrom:]...), change
}

func (v *Validator) redactEmbeddedJSONStringLiteralsChanged(
	raw []byte,
	embeddedDepth int,
	allowOrderedReplay bool,
) ([]byte, redactionChange) {
	for cursor := 0; cursor < len(raw); cursor++ {
		if raw[cursor] != '"' || rawByteEscaped(raw, cursor, 0) {
			continue
		}
		end, closed := quotedValueEndWithStatus(raw, cursor, '"')
		if !closed {
			break
		}
		var decoded string
		if err := json.Unmarshal(raw[cursor:end], &decoded); err != nil {
			// The closing quote might instead open the next candidate when the
			// prose before it contains an unmatched quote. Considering adjacent
			// quote pairs keeps the scan linear and prevents a malformed prefix
			// from hiding a later encoded payload.
			cursor = end - 2
			continue
		}
		_, change := v.redactJSONValue(
			decoded,
			embeddedDepth+1,
			allowOrderedReplay,
		)
		if !change.changed {
			cursor = end - 2
			continue
		}
		if !change.hasPolicyMatch() {
			// Precisely re-encoding one literal inside otherwise malformed prose
			// is ambiguous. Preserve the historical fail-closed boundary even
			// when duplicate-key canonicalization was the inner JSON change.
			change = boundaryChangedBy(v.fallbackMatch())
		}
		// A valid embedded string required redaction. Replace the current
		// raw/string boundary rather than attempt overlapping rewrites in
		// malformed prose; structured outer JSON fields remain intact.
		return []byte(change.match.replacement), change
	}
	return raw, redactionChange{}
}

func (v *Validator) rawSecretMatchBefore(raw []byte, delimiter int) (redactionMatch, bool, bool) {
	quotedKeyEnd, ambiguousEncodedWhitespace := rawQuotedKeyEnd(raw, delimiter, int(v.limits.MaxFieldNameBytes)+4)
	if ambiguousEncodedWhitespace {
		// A delimiter preceded by more encoded JSON whitespace than we are
		// willing to scan could be hiding a sensitive quoted key. The caller
		// replaces the current raw/string boundary rather than resuming from a
		// partial parse and risking disclosure.
		return v.fallbackMatch(), true, true
	}
	// A decoded key byte may occupy six bytes as a JSON Unicode escape. Include
	// quote delimiters and outer escape bytes as well. The hard field-name limit
	// keeps this lookback small and independent of event size.
	quotedKeyBudget := int(v.limits.MaxFieldNameBytes)*6 + 4
	quotedLowerBound := max(quotedKeyEnd-quotedKeyBudget, 0)
	for quotedLowerBound < quotedKeyEnd && !utf8.RuneStart(raw[quotedLowerBound]) {
		quotedLowerBound++
	}
	if quotedName, ok := rawQuotedKeyBefore(raw, quotedKeyEnd, quotedLowerBound); ok {
		return v.matchSensitiveName(quotedName, true), true, false
	}
	if match, parsed := v.rawEscapedQuotedSecretMatchBefore(raw, quotedKeyEnd, quotedLowerBound, quotedKeyBudget); parsed {
		return match, true, match.kind != rawSecretNone
	}
	if quotedLowerBound > 0 && quotedKeyEnd > 0 && (raw[quotedKeyEnd-1] == '"' || raw[quotedKeyEnd-1] == '\'') &&
		rawByteEscaped(raw, quotedKeyEnd-1, quotedLowerBound) {
		// A quote-like key tail with no opener inside the maximum encoded-key
		// budget is ambiguous. It may be a repeatedly encoded sensitive key, so
		// fail closed instead of treating the tail as an ordinary bare name.
		return v.fallbackMatch(), true, true
	}

	keyEnd := delimiter
	for keyEnd > 0 && isHorizontalSpace(raw[keyEnd-1]) {
		keyEnd--
	}
	if keyEnd == 0 {
		return redactionMatch{}, false, false
	}

	lowerBound := max(keyEnd-int(v.limits.MaxFieldNameBytes), 0)
	for lowerBound < delimiter && !utf8.RuneStart(raw[lowerBound]) {
		lowerBound++
	}
	keyStart := keyEnd
	for keyStart > lowerBound && !isRawKeyBoundary(raw[keyStart-1]) && !isHorizontalSpace(raw[keyStart-1]) {
		keyStart--
	}
	if keyStart == keyEnd {
		return redactionMatch{}, false, false
	}
	if match := v.matchSensitiveName(string(raw[keyStart:keyEnd]), true); match.kind != rawSecretNone {
		return match, false, false
	}

	// Exact configured names may use horizontal whitespace between components
	// (for example, "api key"). Extend only for exact names: applying the
	// component heuristic across arbitrary prose would make
	// "password reset complete status=ok" redact the safe status value.
	for extendedStart := keyStart; extendedStart > lowerBound; {
		separatorStart := extendedStart
		for separatorStart > lowerBound && isHorizontalSpace(raw[separatorStart-1]) {
			separatorStart--
		}
		if separatorStart == extendedStart {
			break
		}
		previousStart := separatorStart
		for previousStart > lowerBound && !isRawKeyBoundary(raw[previousStart-1]) && !isHorizontalSpace(raw[previousStart-1]) {
			previousStart--
		}
		if previousStart == separatorStart {
			break
		}
		if match := v.matchSensitiveName(string(raw[previousStart:keyEnd]), false); match.kind != rawSecretNone {
			return match, false, false
		}
		extendedStart = previousStart
	}
	return redactionMatch{}, false, false
}

func rawQuotedKeyEnd(raw []byte, delimiter, encodedWhitespaceBudget int) (int, bool) {
	end := delimiter
	encodedWhitespaceBytes := 0
	for {
		for end > 0 && isRawAssignmentSpace(raw[end-1]) {
			end--
		}
		if end < 2 {
			return end, false
		}
		slashEnd := end - 1
		shortEscape := raw[end-1] == 'n' || raw[end-1] == 'r' || raw[end-1] == 't'
		unicodeEscape := false
		if !shortEscape && end >= 5 && raw[end-5] == 'u' {
			code := string(raw[end-4 : end])
			switch code {
			case "0009", "000a", "000A", "000d", "000D", "0020":
				unicodeEscape = true
				slashEnd = end - 5
			}
		}
		if !shortEscape && !unicodeEscape {
			return end, false
		}
		slashStart := slashEnd
		for slashStart > 0 && raw[slashStart-1] == '\\' {
			slashStart--
		}
		escapeBytes := end - slashStart
		if slashStart == slashEnd {
			return end, false
		}
		if encodedWhitespaceBytes+escapeBytes > encodedWhitespaceBudget {
			return end, true
		}
		// A literal JSON newline/tab/CR becomes \n/\t/\r when that JSON is
		// embedded in a string. Further encoding doubles the slash run. Any
		// non-empty run is safe to treat as encoded assignment whitespace here:
		// it is considered only immediately before ':'/'=' and a sensitive
		// quoted key must still be found within its independent name budget.
		encodedWhitespaceBytes += escapeBytes
		end = slashStart
	}
}

func (v *Validator) rawEscapedQuotedSecretMatchBefore(
	raw []byte,
	keyEnd int,
	lowerBound int,
	encodedKeyBudget int,
) (redactionMatch, bool) {
	if keyEnd < 2 || (raw[keyEnd-1] != '"' && raw[keyEnd-1] != '\'') {
		return redactionMatch{}, false
	}
	closingEscapeRun, complete := rawBackslashRunBefore(raw, keyEnd-1, lowerBound)
	if !complete {
		return v.fallbackMatch(), true
	}
	if closingEscapeRun%2 == 0 {
		return redactionMatch{}, false
	}
	closingEscapeLayer := rawEscapeLayer(closingEscapeRun)
	quote := raw[keyEnd-1]
	// The byte immediately before the closing quote belongs to its outer
	// escape. Excluding it leaves an encoded key body that strconv.Unquote can
	// decode one level at a time.
	contentEnd := keyEnd - 2
	attempts := 0
	for openingQuote := contentEnd - 1; openingQuote > lowerBound; openingQuote-- {
		if raw[openingQuote] != quote {
			continue
		}
		openingEscapeRun, openingComplete := rawBackslashRunBefore(raw, openingQuote, lowerBound)
		if !openingComplete {
			return v.fallbackMatch(), true
		}
		if rawEscapeLayer(openingEscapeRun) != closingEscapeLayer {
			continue
		}
		attempts++
		if attempts > maxEscapedQuotedKeyCandidates {
			// Adversarial quote runs must not turn the bounded lookback into
			// quadratic allocation/unquoting work. An unresolved quote tail is
			// ambiguous, so the caller fail-closes the current boundary.
			return v.fallbackMatch(), true
		}
		contentStart := openingQuote + 1
		if contentStart > contentEnd || contentEnd-contentStart > encodedKeyBudget {
			continue
		}
		encoded := make([]byte, 0, contentEnd-contentStart+2)
		encoded = append(encoded, '"')
		encoded = append(encoded, raw[contentStart:contentEnd]...)
		encoded = append(encoded, '"')
		decoded, err := strconv.Unquote(string(encoded))
		if err != nil {
			continue
		}
		// Matching escape layers identify the syntactically nearest key opener.
		// Quotes inside the encoded key have a deeper slash run and are skipped.
		return v.classifyRecursivelyEncodedKey(decoded), true
	}
	return redactionMatch{}, false
}

func (v *Validator) classifyRecursivelyEncodedKey(decoded string) redactionMatch {
	for depth := 0; ; depth++ {
		if len(decoded) <= int(v.limits.MaxFieldNameBytes) {
			if match := v.matchSensitiveName(decoded, true); match.kind != rawSecretNone {
				return match
			}
		}
		if !strings.Contains(decoded, `\`) {
			if len(decoded) > int(v.limits.MaxFieldNameBytes) {
				return v.fallbackMatch()
			}
			return redactionMatch{}
		}
		if depth >= maxEmbeddedJSONRedactionDepth {
			return v.fallbackMatch()
		}
		encoded := make([]byte, 0, len(decoded)+2)
		encoded = append(encoded, '"')
		encoded = append(encoded, decoded...)
		encoded = append(encoded, '"')
		next, err := strconv.Unquote(string(encoded))
		if err != nil || next == decoded {
			// A matching encoded quote layer with unresolved escape syntax is
			// ambiguous; fail closed rather than classify a partially decoded key
			// as safe.
			return v.fallbackMatch()
		}
		decoded = next
	}
}

func rawQuotedKeyBefore(raw []byte, keyEnd, lowerBound int) (string, bool) {
	if keyEnd == 0 || (raw[keyEnd-1] != '"' && raw[keyEnd-1] != '\'') {
		return "", false
	}
	if rawByteEscaped(raw, keyEnd-1, lowerBound) {
		return "", false
	}
	quote := raw[keyEnd-1]
	for start := keyEnd - 2; start >= lowerBound && raw[start] != '\r' && raw[start] != '\n'; start-- {
		if raw[start] != quote || rawByteEscaped(raw, start, lowerBound) {
			continue
		}
		encoded := raw[start:keyEnd]
		if quote == '"' {
			if decoded, err := strconv.Unquote(string(encoded)); err == nil {
				return decoded, true
			}
		}
		return string(raw[start+1 : keyEnd-1]), true
	}
	return "", false
}

func rawByteEscaped(raw []byte, index, lowerBound int) bool {
	backslashes := 0
	for index > lowerBound && raw[index-1] == '\\' {
		backslashes++
		index--
	}
	if index == lowerBound && lowerBound > 0 && raw[lowerBound-1] == '\\' {
		return true
	}
	return backslashes%2 != 0
}

func rawBackslashRunBefore(raw []byte, index, lowerBound int) (int, bool) {
	start := index
	for start > lowerBound && raw[start-1] == '\\' {
		start--
	}
	if start == lowerBound && lowerBound > 0 && raw[lowerBound-1] == '\\' {
		return index - start, false
	}
	return index - start, true
}

func rawEscapeLayer(backslashRun int) int {
	layer := 0
	for encodedWidth := backslashRun + 1; encodedWidth > 0 && encodedWidth%2 == 0; encodedWidth /= 2 {
		layer++
	}
	return layer
}

func rawSecretKindForName(name string) rawSecretKind {
	return rawSecretKindForComponents(strings.Split(normalizeSensitiveName(name), "_"))
}

func rawSecretKindForComponents(components []string) rawSecretKind {
	for index, component := range components {
		switch {
		case hasSensitiveFamilyAffix(component, "authorization"):
			return rawSecretAuthorization
		case hasSensitiveFamilyAffix(component, "cookie"):
			return rawSecretCookie
		case hasSensitiveFamilyAffix(component, "privatekey"):
			return rawSecretPrivateKey
		case component == "private" || strings.HasSuffix(component, "private"):
			if index+1 < len(components) && components[index+1] == "key" {
				return rawSecretPrivateKey
			}
		}
	}
	return rawSecretValue
}

func (v *Validator) rawSecretValueEnd(raw []byte, start int, kind rawSecretKind) (int, bool) {
	if start < len(raw) && (raw[start] == '{' || raw[start] == '[') {
		return compositeValueEnd(raw, start), false
	}
	switch kind {
	case rawSecretAuthorization:
		return authorizationValueEnd(raw, start)
	case rawSecretCookie:
		return foldedLineEnd(raw, start), false
	case rawSecretPrivateKey:
		if encodedQuotedValueAt(raw, start) {
			return 0, true
		}
		if start < len(raw) && (raw[start] == '"' || raw[start] == '\'') {
			return quotedValueEnd(raw, start, raw[start]), false
		}
		if end, ok := pemPrivateKeyEnd(raw, start); ok {
			return end, false
		}
		return physicalLineEnd(raw, start), false
	case rawSecretValue:
		if start >= len(raw) {
			return start, false
		}
		if encodedQuotedValueAt(raw, start) {
			return 0, true
		}
		if raw[start] == '"' || raw[start] == '\'' {
			return quotedValueEnd(raw, start, raw[start]), false
		}
		return unquotedSecretValueEnd(raw, start), false
	default:
		return start, false
	}
}

func encodedQuotedValueAt(raw []byte, start int) bool {
	end := start
	for end < len(raw) && raw[end] == '\\' {
		end++
	}
	return end > start && end < len(raw) && (raw[end] == '"' || raw[end] == '\'')
}

func unquotedSecretValueEnd(raw []byte, start int) int {
	lineEnd := physicalLineEnd(raw, start)
	for cursor := start; cursor < lineEnd; cursor++ {
		if !isHorizontalSpace(raw[cursor]) && raw[cursor] != ',' && raw[cursor] != ';' {
			continue
		}
		separatorStart := cursor
		candidateStart := cursor + 1
		for candidateStart < lineEnd && (isHorizontalSpace(raw[candidateStart]) || raw[candidateStart] == ',' || raw[candidateStart] == ';') {
			candidateStart++
		}
		candidateEnd := candidateStart
		for candidateEnd < lineEnd && !isRawKeyBoundary(raw[candidateEnd]) && !isHorizontalSpace(raw[candidateEnd]) {
			candidateEnd++
		}
		if candidateEnd > candidateStart && candidateEnd < lineEnd &&
			(raw[candidateEnd] == '=' || raw[candidateEnd] == ':') {
			return separatorStart
		}
		cursor = candidateStart - 1
	}
	return lineEnd
}

func compositeValueEnd(raw []byte, start int) int {
	stack := make([]byte, 1, 8)
	if raw[start] == '{' {
		stack[0] = '}'
	} else {
		stack[0] = ']'
	}
	var quote byte
	escaped := false
	for cursor := start + 1; cursor < len(raw); cursor++ {
		character := raw[cursor]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' {
				escaped = true
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		switch character {
		case '"', '\'':
			quote = character
		case '{':
			stack = append(stack, '}')
		case '[':
			stack = append(stack, ']')
		case '}', ']':
			if len(stack) == 0 || stack[len(stack)-1] != character {
				return len(raw)
			}
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				return cursor + 1
			}
		}
	}
	return len(raw)
}

func authorizationValueEnd(raw []byte, start int) (int, bool) {
	if start >= len(raw) {
		return start, false
	}
	if encodedQuotedValueAt(raw, start) {
		return 0, true
	}
	if raw[start] == '"' || raw[start] == '\'' {
		return quotedValueEnd(raw, start, raw[start]), false
	}
	schemeEnd := start
	for schemeEnd < len(raw) && !isRawValueBoundary(raw[schemeEnd]) {
		schemeEnd++
	}
	if schemeEnd == start || schemeEnd >= len(raw) || !isHorizontalSpace(raw[schemeEnd]) {
		return foldedLineEnd(raw, start), false
	}
	scheme := string(raw[start:schemeEnd])
	if !strings.EqualFold(scheme, "bearer") && !strings.EqualFold(scheme, "basic") {
		return foldedLineEnd(raw, start), false
	}
	credentialStart := schemeEnd
	for credentialStart < len(raw) && isHorizontalSpace(raw[credentialStart]) {
		credentialStart++
	}
	if credentialStart >= len(raw) || raw[credentialStart] == '\r' || raw[credentialStart] == '\n' {
		return foldedLineEnd(raw, start), false
	}
	if encodedQuotedValueAt(raw, credentialStart) {
		return 0, true
	}
	var end int
	if raw[credentialStart] == '"' || raw[credentialStart] == '\'' {
		end = quotedValueEnd(raw, credentialStart, raw[credentialStart])
	} else {
		end = credentialStart
		for end < len(raw) && !isRawValueBoundary(raw[end]) {
			end++
		}
	}
	lineTail := end
	for lineTail < len(raw) && isHorizontalSpace(raw[lineTail]) {
		lineTail++
	}
	if lineTail == len(raw) || raw[lineTail] == '\r' || raw[lineTail] == '\n' {
		return foldedLineEnd(raw, start), false
	}
	return end, false
}

func isHorizontalSpace(value byte) bool {
	switch value {
	case ' ', '\t', '\v', '\f':
		return true
	default:
		return false
	}
}

func isRawAssignmentSpace(value byte) bool {
	return isHorizontalSpace(value) || value == '\r' || value == '\n'
}

func isRawKeyBoundary(value byte) bool {
	switch value {
	case '\r', '\n', '=', ':', ',', ';', '{', '}', '[', ']', '(', ')', '"', '\'', '`':
		return true
	default:
		return false
	}
}

func isRawValueBoundary(value byte) bool {
	if isHorizontalSpace(value) {
		return true
	}
	switch value {
	case '\r', '\n', ',', ';', '}', ']', '"', '\'':
		return true
	default:
		return false
	}
}

func quotedValueEnd(raw []byte, start int, quote byte) int {
	end, closed := quotedValueEndWithStatus(raw, start, quote)
	if closed {
		return end
	}
	// An unterminated quoted secret is ambiguous; consume the remainder rather
	// than preserve a possible credential tail.
	return len(raw)
}

func quotedValueEndWithStatus(raw []byte, start int, quote byte) (int, bool) {
	escaped := false
	for cursor := start + 1; cursor < len(raw); cursor++ {
		if escaped {
			escaped = false
			continue
		}
		switch raw[cursor] {
		case '\\':
			escaped = true
		case quote:
			return cursor + 1, true
		}
	}
	return len(raw), false
}

func physicalLineEnd(raw []byte, start int) int {
	for start < len(raw) && raw[start] != '\r' && raw[start] != '\n' {
		start++
	}
	return start
}

func foldedLineEnd(raw []byte, start int) int {
	end := physicalLineEnd(raw, start)
	for end < len(raw) {
		next := end
		if raw[next] == '\r' {
			next++
		}
		if next < len(raw) && raw[next] == '\n' {
			next++
		} else if raw[end] == '\n' {
			next = end + 1
		}
		if next >= len(raw) || !isHorizontalSpace(raw[next]) {
			return end
		}
		end = physicalLineEnd(raw, next)
	}
	return end
}

func pemPrivateKeyEnd(raw []byte, start int) (int, bool) {
	cursor := start
	const beginPrefix = "-----BEGIN "
	lineEnd := physicalLineEnd(raw, cursor)
	beginOffset := bytes.Index(raw[cursor:lineEnd], []byte(beginPrefix))
	if beginOffset < 0 {
		// A common representation uses a short preamble such as "PEM follows:"
		// and then one or more blank lines. Inspect only the whitespace prefix
		// immediately following the assignment line, keeping repeated non-PEM
		// assignments linear in the event size.
		probe := lineEnd
		if probe < len(raw) && raw[probe] == '\r' {
			probe++
		}
		if probe < len(raw) && raw[probe] == '\n' {
			probe++
		}
		for probe < len(raw) && (isHorizontalSpace(raw[probe]) || raw[probe] == '\r' || raw[probe] == '\n') {
			probe++
		}
		if !bytes.HasPrefix(raw[probe:], []byte(beginPrefix)) {
			return 0, false
		}
		beginOffset = probe - cursor
	}
	cursor += beginOffset
	labelStart := cursor + len(beginPrefix)
	labelEndOffset := bytes.Index(raw[labelStart:], []byte("-----"))
	if labelEndOffset < 0 {
		return len(raw), true
	}
	labelEnd := labelStart + labelEndOffset
	label := string(raw[labelStart:labelEnd])
	if !strings.HasSuffix(strings.ToUpper(label), "PRIVATE KEY") {
		return 0, false
	}
	endMarker := []byte("-----END " + label + "-----")
	markerOffset := bytes.Index(raw[labelEnd+5:], endMarker)
	if markerOffset < 0 {
		return len(raw), true
	}
	return labelEnd + 5 + markerOffset + len(endMarker), true
}

func (v *Validator) redactJSONValue(
	value any,
	embeddedDepth int,
	allowOrderedReplay bool,
) (any, redactionChange) {
	switch typed := value.(type) {
	case map[string]any:
		var change redactionChange
		for name, child := range typed {
			if match := v.matchSensitiveName(name, true); match.kind != rawSecretNone {
				matchChange := changedBy(match)
				replacement := match.replacement
				if embeddedDepth >= maxEmbeddedJSONRedactionDepth && len(v.ordered) > 0 {
					replacement = v.depthLimitReplacement()
					matchChange = changedBy(redactionMatch{
						kind:        rawSecretValue,
						replacement: v.ordered[0].replacement,
					})
				}
				typed[name] = replacement
				change.merge(matchChange)
				continue
			}
			redacted, childChange := v.redactJSONValue(
				child,
				embeddedDepth,
				allowOrderedReplay,
			)
			if childChange.changed {
				typed[name] = redacted
				change.merge(childChange)
			}
		}
		return typed, change
	case []any:
		var change redactionChange
		for i, child := range typed {
			redacted, childChange := v.redactJSONValue(
				child,
				embeddedDepth,
				allowOrderedReplay,
			)
			if childChange.changed {
				typed[i] = redacted
				change.merge(childChange)
			}
		}
		return typed, change
	case string:
		if embeddedDepth >= maxEmbeddedJSONRedactionDepth {
			// Deeply encoded JSON is unusual and expensive to inspect without a
			// bound. Every string at this encoded depth is replaced: requiring the
			// leaf itself to be valid JSON would permit prose-wrapped payloads to
			// bypass the bound.
			change := changedBy(v.fallbackMatch())
			return v.depthLimitReplacement(), change
		}
		typedBytes := []byte(typed)
		if embedded, parsed, change := v.redactJSONDepth(
			typedBytes,
			embeddedDepth+1,
			allowOrderedReplay,
		); parsed {
			if !change.changed {
				return typed, redactionChange{}
			}
			if !allowOrderedReplay && change.hasPolicyMatch() {
				return typed, change
			}
			return string(embedded), change
		}
		embedded, change := v.redactEmbeddedJSONStringLiteralsChanged(
			typedBytes,
			embeddedDepth,
			allowOrderedReplay,
		)
		if change.hasPolicyMatch() && len(v.ordered) > 0 {
			if !allowOrderedReplay {
				return typed, change
			}
			replayed, orderedChange := v.redactTextInPolicyOrder(typedBytes)
			if bytes.Equal(replayed, typedBytes) {
				return typed, redactionChange{}
			}
			return string(replayed), orderedChange
		}
		redacted, textChange := v.redactKeyValueTextChanged(
			embedded,
			allowOrderedReplay,
		)
		if len(v.ordered) > 0 &&
			(textChange.requiresOrderedFallback ||
				textChange.canAffectLaterPolicy(len(v.ordered))) {
			if !allowOrderedReplay {
				return typed, textChange
			}
			replayed, orderedChange := v.redactTextInPolicyOrder(typedBytes)
			if bytes.Equal(replayed, typedBytes) {
				return typed, redactionChange{}
			}
			return string(replayed), orderedChange
		}
		if !allowOrderedReplay && textChange.changed {
			return typed, textChange
		}
		change.merge(textChange)
		if bytes.Equal(redacted, typedBytes) {
			return typed, redactionChange{}
		}
		return string(redacted), change
	default:
		return value, redactionChange{}
	}
}
