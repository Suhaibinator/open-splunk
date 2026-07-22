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

type rawSecretKind uint8

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
	normalized := normalizeSensitiveName(name)
	if _, ok := v.sensitive[normalized]; ok {
		return true
	}
	return hasMandatorySensitiveComponent(strings.Split(normalized, "_"))
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
		kind.StringValue = string(v.redactText([]byte(kind.StringValue)))
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

// redactJSON preserves ordinary JSON without sensitive keys byte-for-byte.
// Objects with duplicate keys are canonicalized with deterministic last-key
// semantics so a secret in a shadowed member cannot survive in the raw bytes.
// parsed is separate from changed so valid, unchanged JSON does not take the
// plain-text scanner path a second time.
// JSON numbers are decoded with UseNumber so sanitization does not coerce large
// integers through float64.
func (v *Validator) redactJSON(raw []byte) (redacted []byte, parsed bool) {
	return v.redactJSONDepth(raw, 0)
}

func (v *Validator) redactJSONDepth(raw []byte, embeddedDepth int) (redacted []byte, parsed bool) {
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
	redactedValue, changed := v.redactJSONValue(value, embeddedDepth)
	if !changed && !duplicateKey {
		return raw, true
	}
	encoded, err := json.Marshal(redactedValue)
	if err != nil {
		// Values produced by encoding/json plus the replacement string are
		// marshalable. Keep this branch fail-closed if that invariant changes.
		return []byte(v.replacement), true
	}
	return encoded, true
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
	if redactedJSON, parsed := v.redactJSON(raw); parsed {
		return redactedJSON
	}
	embedded, _ := v.redactEmbeddedJSONStringLiterals(raw, 0)
	return v.redactKeyValueText(embedded)
}

func (v *Validator) redactKeyValueText(raw []byte) []byte {
	var result []byte
	copyFrom := 0
	for cursor := 0; cursor < len(raw); cursor++ {
		if raw[cursor] != '=' && raw[cursor] != ':' {
			continue
		}
		kind, quotedKey, encodedQuotedKey := v.rawSecretKindBefore(raw, cursor)
		if kind == rawSecretNone {
			continue
		}
		if encodedQuotedKey {
			// This is a quoted key inside another encoded string (for example,
			// prose containing {\"password\":\"...\"}). Precisely rewriting one
			// value would require decoding and re-encoding the surrounding string.
			// Fail closed at the current string/raw boundary instead.
			return []byte(v.replacement)
		}

		valueStart := cursor + 1
		for valueStart < len(raw) && (isHorizontalSpace(raw[valueStart]) ||
			(quotedKey && (raw[valueStart] == '\r' || raw[valueStart] == '\n'))) {
			valueStart++
		}
		valueEnd, ambiguousValue := v.rawSecretValueEnd(raw, valueStart, kind)
		if ambiguousValue {
			return []byte(v.replacement)
		}
		if valueEnd < valueStart || (valueEnd == valueStart && kind == rawSecretValue) {
			continue
		}

		if result == nil {
			result = make([]byte, 0, len(raw)+len(v.replacement))
		}
		result = append(result, raw[copyFrom:valueStart]...)
		result = append(result, '"')
		result = append(result, v.replacement...)
		result = append(result, '"')
		copyFrom = valueEnd
		cursor = valueEnd - 1
	}
	if result == nil {
		return raw
	}
	return append(result, raw[copyFrom:]...)
}

func (v *Validator) redactEmbeddedJSONStringLiterals(raw []byte, embeddedDepth int) ([]byte, bool) {
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
		_, changed := v.redactJSONValue(decoded, embeddedDepth+1)
		if !changed {
			cursor = end - 2
			continue
		}
		// A valid embedded string required redaction. Replace the current
		// raw/string boundary rather than attempt overlapping rewrites in
		// malformed prose; structured outer JSON fields remain intact.
		return []byte(v.replacement), true
	}
	return raw, false
}

func (v *Validator) rawSecretKindBefore(raw []byte, delimiter int) (rawSecretKind, bool, bool) {
	quotedKeyEnd, ambiguousEncodedWhitespace := rawQuotedKeyEnd(raw, delimiter, int(v.limits.MaxFieldNameBytes)+4)
	if ambiguousEncodedWhitespace {
		// A delimiter preceded by more encoded JSON whitespace than we are
		// willing to scan could be hiding a sensitive quoted key. The caller
		// replaces the current raw/string boundary rather than resuming from a
		// partial parse and risking disclosure.
		return rawSecretValue, true, true
	}
	// A decoded key byte may occupy six bytes as a JSON Unicode escape. Include
	// quote delimiters and outer escape bytes as well. The hard field-name limit
	// keeps this lookback small and independent of event size.
	quotedKeyBudget := int(v.limits.MaxFieldNameBytes)*6 + 4
	quotedLowerBound := quotedKeyEnd - quotedKeyBudget
	if quotedLowerBound < 0 {
		quotedLowerBound = 0
	}
	for quotedLowerBound < quotedKeyEnd && !utf8.RuneStart(raw[quotedLowerBound]) {
		quotedLowerBound++
	}
	if quotedName, ok := rawQuotedKeyBefore(raw, quotedKeyEnd, quotedLowerBound); ok {
		return v.classifyRawSensitiveName(quotedName, true), true, false
	}
	if kind, parsed := v.rawEscapedQuotedSecretKindBefore(raw, quotedKeyEnd, quotedLowerBound, quotedKeyBudget); parsed {
		return kind, true, kind != rawSecretNone
	}
	if quotedLowerBound > 0 && quotedKeyEnd > 0 && (raw[quotedKeyEnd-1] == '"' || raw[quotedKeyEnd-1] == '\'') &&
		rawByteEscaped(raw, quotedKeyEnd-1, quotedLowerBound) {
		// A quote-like key tail with no opener inside the maximum encoded-key
		// budget is ambiguous. It may be a repeatedly encoded sensitive key, so
		// fail closed instead of treating the tail as an ordinary bare name.
		return rawSecretValue, true, true
	}

	keyEnd := delimiter
	for keyEnd > 0 && isHorizontalSpace(raw[keyEnd-1]) {
		keyEnd--
	}
	if keyEnd == 0 {
		return rawSecretNone, false, false
	}

	lowerBound := keyEnd - int(v.limits.MaxFieldNameBytes)
	if lowerBound < 0 {
		lowerBound = 0
	}
	for lowerBound < delimiter && !utf8.RuneStart(raw[lowerBound]) {
		lowerBound++
	}
	keyStart := keyEnd
	for keyStart > lowerBound && !isRawKeyBoundary(raw[keyStart-1]) && !isHorizontalSpace(raw[keyStart-1]) {
		keyStart--
	}
	if keyStart == keyEnd {
		return rawSecretNone, false, false
	}
	if kind := v.classifyRawSensitiveName(string(raw[keyStart:keyEnd]), true); kind != rawSecretNone {
		return kind, false, false
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
		if kind := v.classifyRawSensitiveName(string(raw[previousStart:keyEnd]), false); kind != rawSecretNone {
			return kind, false, false
		}
		extendedStart = previousStart
	}
	return rawSecretNone, false, false
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

func (v *Validator) rawEscapedQuotedSecretKindBefore(raw []byte, keyEnd, lowerBound, encodedKeyBudget int) (rawSecretKind, bool) {
	if keyEnd < 2 || (raw[keyEnd-1] != '"' && raw[keyEnd-1] != '\'') {
		return rawSecretNone, false
	}
	closingEscapeRun, complete := rawBackslashRunBefore(raw, keyEnd-1, lowerBound)
	if !complete {
		return rawSecretValue, true
	}
	if closingEscapeRun%2 == 0 {
		return rawSecretNone, false
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
			return rawSecretValue, true
		}
		if rawEscapeLayer(openingEscapeRun) != closingEscapeLayer {
			continue
		}
		attempts++
		if attempts > maxEscapedQuotedKeyCandidates {
			// Adversarial quote runs must not turn the bounded lookback into
			// quadratic allocation/unquoting work. An unresolved quote tail is
			// ambiguous, so the caller fail-closes the current boundary.
			return rawSecretValue, true
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
	return rawSecretNone, false
}

func (v *Validator) classifyRecursivelyEncodedKey(decoded string) rawSecretKind {
	for depth := 0; ; depth++ {
		if len(decoded) <= int(v.limits.MaxFieldNameBytes) {
			if kind := v.classifyRawSensitiveName(decoded, true); kind != rawSecretNone {
				return kind
			}
		}
		if !strings.Contains(decoded, `\`) {
			if len(decoded) > int(v.limits.MaxFieldNameBytes) {
				return rawSecretValue
			}
			return rawSecretNone
		}
		if depth >= maxEmbeddedJSONRedactionDepth {
			return rawSecretValue
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
			return rawSecretValue
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

func (v *Validator) classifyRawSensitiveName(name string, allowComponentMatch bool) rawSecretKind {
	normalized := normalizeSensitiveName(name)
	_, exact := v.sensitive[normalized]
	components := strings.Split(normalized, "_")
	sensitive := exact || allowComponentMatch && hasMandatorySensitiveComponent(components)
	if !sensitive {
		return rawSecretNone
	}
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

func (v *Validator) redactJSONValue(value any, embeddedDepth int) (any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		changed := false
		for name, child := range typed {
			if v.isSensitive(name) {
				typed[name] = v.replacement
				changed = true
				continue
			}
			redacted, childChanged := v.redactJSONValue(child, embeddedDepth)
			if childChanged {
				typed[name] = redacted
				changed = true
			}
		}
		return typed, changed
	case []any:
		changed := false
		for i, child := range typed {
			redacted, childChanged := v.redactJSONValue(child, embeddedDepth)
			if childChanged {
				typed[i] = redacted
				changed = true
			}
		}
		return typed, changed
	case string:
		if embeddedDepth >= maxEmbeddedJSONRedactionDepth {
			// Deeply encoded JSON is unusual and expensive to inspect without a
			// bound. Every string at this encoded depth is replaced: requiring the
			// leaf itself to be valid JSON would permit prose-wrapped payloads to
			// bypass the bound.
			return v.replacement, true
		}
		if embedded, parsed := v.redactJSONDepth([]byte(typed), embeddedDepth+1); parsed {
			return string(embedded), !bytes.Equal(embedded, []byte(typed))
		}
		typedBytes := []byte(typed)
		embedded, embeddedChanged := v.redactEmbeddedJSONStringLiterals(typedBytes, embeddedDepth)
		redacted := v.redactKeyValueText(embedded)
		return string(redacted), embeddedChanged || !bytes.Equal(redacted, typedBytes)
	default:
		return value, false
	}
}
