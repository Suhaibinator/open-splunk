package gradethiscorpus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

var (
	emailPattern = regexp.MustCompile(
		`(?i)[a-z0-9.!#$%&'*+/=?^_` + "`" + `{|}~-]+@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+`,
	)
	ipv4Pattern        = regexp.MustCompile(`(?:^|[^0-9])((?:[0-9]{1,3}\.){3}[0-9]{1,3})(?:$|[^0-9])`)
	bracketedIPPattern = regexp.MustCompile(`\[[0-9a-fA-F:.%]+\](?::[0-9]{1,5})?`)
	sqlPattern         = regexp.MustCompile(`(?i)\b(?:select|insert|update|delete|alter|create|drop|truncate)\b[ \t]+(?:\*|[0-9]|all\b|distinct\b|into\b|from\b|table\b|database\b|view\b|index\b|[a-z_"` + "`" + `])`)
	urlPattern         = regexp.MustCompile(`(?i)https?://[^\s<>"']+`)
	credentialPattern  = regexp.MustCompile(
		`(?i)(?:^|[ \t,;{(\[])(?:access[_. -]*token|api[_. -]*key|auth(?:orization)?|client[_. -]*secret|password|passwd|private[_. -]*key|refresh[_. -]*token|secret|token|credential(?:s)?|cookie|session(?:[_. -]*id)?)[ \t]*[:=][ \t]*(?:bearer[ \t]+)?[^ \t,;}\])]+`,
	)
	bearerPattern = regexp.MustCompile(`(?i)(?:^|[^a-z0-9])bearer[ \t]+[a-z0-9._~+/=-]+`)
)

var documentationNetworks = []netip.Prefix{
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("2001:db8::/32"),
}

var blockedKeys = map[string]string{
	"accesstoken":   "secret-like key",
	"apikey":        "secret-like key",
	"authtoken":     "secret-like key",
	"authorization": "secret-like key",
	"clientsecret":  "secret-like key",
	"cookie":        "secret-like key",
	"credential":    "secret-like key",
	"credentials":   "secret-like key",
	"email":         "email identifier key",
	"emailaddress":  "email identifier key",
	"password":      "secret-like key",
	"passwd":        "secret-like key",
	"privatekey":    "secret-like key",
	"query":         "SQL/query key",
	"refreshtoken":  "secret-like key",
	"secret":        "secret-like key",
	"session":       "session identifier key",
	"sessionid":     "session identifier key",
	"setcookie":     "secret-like key",
	"sql":           "SQL/query key",
	"sqlstatement":  "SQL/query key",
	"stacktrace":    "stack-trace key",
	"token":         "secret-like key",
	"user":          "user identifier key",
	"userid":        "user identifier key",
	"username":      "user identifier key",
}

// ScanNDJSON rejects fixture bytes that contain common sensitive production
// data or non-documentation network metadata. Errors identify only a line and
// JSON path; they deliberately do not echo the rejected value.
func ScanNDJSON(payload []byte) error {
	if len(payload) == 0 {
		return errors.New("fixture is empty")
	}
	if !bytes.HasSuffix(payload, []byte{'\n'}) {
		return errors.New("fixture must end with a newline")
	}
	lines := bytes.Split(payload, []byte{'\n'})
	for index, line := range lines[:len(lines)-1] {
		if len(line) == 0 {
			return fmt.Errorf("line %d is empty", index+1)
		}
		if err := scanJSONLine(line, index+1); err != nil {
			return err
		}
	}
	return nil
}

func scanJSONLine(line []byte, lineNumber int) error {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("line %d is not valid JSON: %w", lineNumber, err)
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return fmt.Errorf("line %d must be a JSON object", lineNumber)
	}
	if err := scanObject(decoder, lineNumber, "$"); err != nil {
		return err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("line %d has trailing invalid JSON: %w", lineNumber, err)
		}
		return fmt.Errorf("line %d has trailing JSON token %T", lineNumber, token)
	}
	return nil
}

func scanObject(decoder *json.Decoder, lineNumber int, path string) error {
	seen := make(map[string]struct{})
	for ordinal := 0; decoder.More(); ordinal++ {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("line %d path %s has an invalid key: %w", lineNumber, path, err)
		}
		key, ok := token.(string)
		if !ok {
			return fmt.Errorf("line %d path %s has a non-string key", lineNumber, path)
		}
		keyPath := path + "{key[" + strconv.Itoa(ordinal) + "]}"
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("line %d path %s is duplicated", lineNumber, keyPath)
		}
		seen[key] = struct{}{}
		if reason, blocked := blockedKeyReason(key); blocked {
			return fmt.Errorf("line %d path %s contains a %s", lineNumber, keyPath, reason)
		}
		if err := scanString(key, lineNumber, keyPath+"<name>", false); err != nil {
			return err
		}
		childPath := path + "[" + strconv.Itoa(ordinal) + "]"
		value, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("line %d path %s has an invalid value: %w", lineNumber, childPath, err)
		}
		if err := scanToken(decoder, value, lineNumber, childPath, normalizeKey(key) == "path"); err != nil {
			return err
		}
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("line %d path %s has an unterminated object: %w", lineNumber, path, err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return fmt.Errorf("line %d path %s has an invalid object terminator", lineNumber, path)
	}
	return nil
}

func scanToken(
	decoder *json.Decoder,
	token json.Token,
	lineNumber int,
	path string,
	allowSyntheticRoute bool,
) error {
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			return scanObject(decoder, lineNumber, path)
		case '[':
			for index := 0; decoder.More(); index++ {
				item, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("line %d path %s has an invalid array item: %w", lineNumber, path, err)
				}
				if err := scanToken(decoder, item, lineNumber, path+"["+strconv.Itoa(index)+"]", false); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("line %d path %s has an unterminated array: %w", lineNumber, path, err)
			}
			if delim, ok := end.(json.Delim); !ok || delim != ']' {
				return fmt.Errorf("line %d path %s has an invalid array terminator", lineNumber, path)
			}
		default:
			return fmt.Errorf("line %d path %s has unexpected delimiter %q", lineNumber, path, value)
		}
	case string:
		return scanString(value, lineNumber, path, allowSyntheticRoute)
	case nil, bool, json.Number:
		return nil
	default:
		return fmt.Errorf("line %d path %s has unsupported JSON token %T", lineNumber, path, token)
	}
	return nil
}

func scanString(value string, lineNumber int, path string, allowSyntheticRoute bool) error {
	reject := func(reason string) error {
		return fmt.Errorf("line %d path %s contains %s", lineNumber, path, reason)
	}
	if strings.ContainsAny(value, "\r\n\x00") {
		return reject("control text or a possible stack trace")
	}
	lower := strings.ToLower(value)
	if strings.Contains(lower, "file://") ||
		containsAbsolutePath(value) && (!allowSyntheticRoute || !allowedSyntheticRoute(value)) {
		return reject("an absolute filesystem path")
	}
	if emailPattern.MatchString(value) {
		return reject("an email address")
	}
	if credentialPattern.MatchString(value) || bearerPattern.MatchString(value) {
		return reject("credential-like text")
	}
	if sqlPattern.MatchString(value) {
		return reject("a SQL statement")
	}
	if strings.Contains(lower, "goroutine ") ||
		strings.Contains(lower, "runtime/debug.stack") ||
		strings.Contains(lower, ".go:") && strings.Contains(value, "\t") {
		return reject("a stack trace")
	}
	for _, match := range ipv4Pattern.FindAllStringSubmatch(value, -1) {
		address, err := netip.ParseAddr(match[1])
		if err == nil && !isDocumentationAddress(address) {
			return reject("a non-documentation IP address")
		}
	}
	for _, candidate := range bracketedIPPattern.FindAllString(value, -1) {
		var address netip.Addr
		var err error
		if strings.Contains(candidate, "]:") {
			var addressPort netip.AddrPort
			addressPort, err = netip.ParseAddrPort(candidate)
			address = addressPort.Addr()
		} else {
			address, err = netip.ParseAddr(strings.Trim(candidate, "[]"))
		}
		if err == nil && !isDocumentationAddress(address) {
			return reject("a non-documentation IP address")
		}
	}
	for _, token := range strings.FieldsFunc(value, func(character rune) bool {
		return strings.ContainsRune(" \t,;=(){}<>\"'", character)
	}) {
		candidate := strings.Trim(token, "[]")
		if strings.Count(candidate, ":") < 2 {
			continue
		}
		address, err := netip.ParseAddr(candidate)
		if err == nil && !isDocumentationAddress(address) {
			return reject("a non-documentation IP address")
		}
	}
	for _, candidate := range urlPattern.FindAllString(value, -1) {
		candidate = strings.TrimRight(candidate, ".,;:!?)]}")
		parsed, err := url.Parse(candidate)
		if err != nil || parsed.Hostname() == "" || parsed.User != nil {
			return reject("an invalid or credential-bearing URL")
		}
		if err := validateDocumentationHostname(parsed.Hostname()); err != nil {
			return reject("a non-documentation domain")
		}
	}
	return nil
}

func normalizeKey(key string) string {
	return strings.Map(func(character rune) rune {
		switch character {
		case '-', '_', '.', ' ', '\t':
			return -1
		default:
			return character
		}
	}, strings.ToLower(strings.TrimSpace(key)))
}

func blockedKeyReason(key string) (string, bool) {
	normalized := normalizeKey(key)
	if normalized == "useragent" {
		return "", false
	}
	if reason, blocked := blockedKeys[normalized]; blocked {
		return reason, true
	}
	components := keyComponents(key)
	for index, component := range components {
		if containsAny(component, "authorization", "cookie", "credential", "password",
			"passwd", "privatekey", "secret", "token", "apikey") {
			return "secret-like key", true
		}
		if strings.Contains(component, "email") {
			return "email identifier key", true
		}
		if strings.Contains(component, "userid") || strings.Contains(component, "username") ||
			component == "user" && (index+1 >= len(components) || components[index+1] != "agent") {
			return "user identifier key", true
		}
		if strings.Contains(component, "sessionid") ||
			component == "session" && index+1 < len(components) && components[index+1] == "id" {
			return "session identifier key", true
		}
		if index+1 < len(components) && components[index+1] == "key" &&
			(component == "api" || strings.HasSuffix(component, "api") ||
				component == "private" || strings.HasSuffix(component, "private")) {
			return "secret-like key", true
		}
	}
	return "", false
}

func containsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}

func keyComponents(key string) []string {
	runes := []rune(strings.TrimSpace(key))
	components := make([]string, 0, 4)
	start := -1
	flush := func(end int) {
		if start >= 0 && start < end {
			components = append(components, strings.ToLower(string(runes[start:end])))
		}
		start = -1
	}
	for index, character := range runes {
		if !unicode.IsLetter(character) && !unicode.IsDigit(character) {
			flush(index)
			continue
		}
		if start < 0 {
			start = index
			continue
		}
		previous := runes[index-1]
		nextIsLower := index+1 < len(runes) && unicode.IsLower(runes[index+1])
		if unicode.IsUpper(character) &&
			(unicode.IsLower(previous) || unicode.IsDigit(previous) ||
				unicode.IsUpper(previous) && nextIsLower) {
			flush(index)
			start = index
		}
	}
	flush(len(runes))
	return components
}

func containsAbsolutePath(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] == '/' &&
			(index == 0 || isPathDelimiter(value[index-1])) &&
			!startsURLAuthority(value, index) {
			return true
		}
		if value[index] == '\\' && index+1 < len(value) && value[index+1] == '\\' &&
			(index == 0 || isPathDelimiter(value[index-1])) {
			return true
		}
		if index+2 < len(value) &&
			((value[index] >= 'a' && value[index] <= 'z') || (value[index] >= 'A' && value[index] <= 'Z')) &&
			value[index+1] == ':' && (value[index+2] == '\\' || value[index+2] == '/') &&
			(index == 0 || isPathDelimiter(value[index-1])) {
			return true
		}
	}
	return false
}

func isPathDelimiter(character byte) bool {
	return strings.ContainsRune(" \t\r\n=:\"'()[]{}<>,;", rune(character))
}

func startsURLAuthority(value string, slash int) bool {
	if slash+1 >= len(value) || value[slash+1] != '/' || slash == 0 || value[slash-1] != ':' {
		return false
	}
	schemeEnd := slash - 1
	schemeStart := schemeEnd
	for schemeStart > 0 {
		character := value[schemeStart-1]
		if (character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') &&
			!strings.ContainsRune("+.-", rune(character)) {
			break
		}
		schemeStart--
	}
	if schemeStart == schemeEnd {
		return false
	}
	first := value[schemeStart]
	return (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z')
}

func allowedSyntheticRoute(value string) bool {
	if !strings.HasPrefix(value, "/api/") || strings.Contains(value, `\`) ||
		strings.Contains(value, "..") || strings.ContainsAny(value, " \t\r\n") {
		return false
	}
	parsed, err := url.ParseRequestURI(value)
	return err == nil && parsed.Host == "" && !parsed.IsAbs()
}

func isDocumentationAddress(address netip.Addr) bool {
	for _, network := range documentationNetworks {
		if network.Contains(address) {
			return true
		}
	}
	return false
}

func validateDocumentationHostname(host string) error {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "" {
		return errors.New("hostname is empty")
	}
	if address, err := netip.ParseAddr(host); err == nil {
		if !isDocumentationAddress(address) {
			return errors.New("IP address is not from a documentation network")
		}
		return nil
	}
	for _, suffix := range []string{".test", ".invalid", ".example", ".localhost"} {
		if host == strings.TrimPrefix(suffix, ".") || strings.HasSuffix(host, suffix) {
			return nil
		}
	}
	for _, domain := range []string{"example.com", "example.net", "example.org"} {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return nil
		}
	}
	return errors.New("hostname is not reserved for documentation")
}
