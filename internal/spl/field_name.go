package spl

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

// IsExactUnquotedFieldName reports whether name is representable as one
// unquoted field token. Re-lexing keeps forged AST and logical-plan validation
// aligned with the parser's delimiter and Unicode whitespace rules instead of
// maintaining a second approximation.
func IsExactUnquotedFieldName(name string) bool {
	if name == "" || strings.ContainsAny(name, "'\"`*") {
		return false
	}
	tokens, err := lex(name)
	return err == nil &&
		len(tokens) == 2 &&
		tokens[0].kind == tokenWord &&
		tokens[0].text == name &&
		tokens[1].kind == tokenEOF
}

// IsExactQuotedFieldName reports whether name is the decoded logical spelling
// of one repository-standard single-quoted field reference. Quoting admits
// expression punctuation but does not bypass canonical-path, wildcard,
// whitespace, private-root, or resource validation.
func IsExactQuotedFieldName(name string) bool {
	if name == "" || !utf8.ValidString(name) || strings.ContainsAny(name, "*?") ||
		strings.TrimFunc(name, unicode.IsSpace) != name {
		return false
	}
	for _, value := range name {
		if unicode.IsControl(value) {
			return false
		}
	}
	segments, err := eventfields.ParseNormalizedSearchFieldPath(name)
	return err == nil && len(segments) > 0 &&
		(eventfields.IsCanonicalSPLField(name) ||
			!eventfields.IsReservedDynamicRoot(segments[0]))
}

// IsStatsLiteralOutputName reports whether name is safe as one literal stats
// output column. It covers both a documented double-quoted AS name and the
// deterministic source-derived name of an unaliased eval aggregate. Literal
// names are not parsed as paths, so values such as "Product Name" and ".com"
// remain intact. They still cannot mint private or reserved event roots.
func IsStatsLiteralOutputName(name string) bool {
	if name == "" || len(name) > eventfields.MaximumNormalizedFieldNameBytes ||
		!utf8.ValidString(name) {
		return false
	}
	for _, value := range name {
		if unicode.IsControl(value) {
			return false
		}
	}
	root := name
	if index := strings.IndexByte(root, '.'); index >= 0 {
		root = root[:index]
	}
	if eventfields.IsCanonicalSPLField(name) {
		return true
	}
	return root == "" || !eventfields.IsReservedDynamicRoot(root)
}

// IsStatsLiteralFieldReference reports whether a single-quoted exact field
// can safely refer to a literal stats output whose decoded name is not a
// canonical dotted event path, such as ".com". Wildcards remain patterns and
// are never reinterpreted as exact literal references.
func IsStatsLiteralFieldReference(name string) bool {
	if IsExactQuotedFieldName(name) {
		return true
	}
	return len(name) > 1 && strings.HasPrefix(name, ".") &&
		len(name) <= eventfields.MaximumDynamicPathSegmentBytes &&
		strings.TrimFunc(name, unicode.IsSpace) == name &&
		!strings.ContainsAny(name, "*?") && IsStatsLiteralOutputName(name)
}

// IsSafeStatsInventoryFieldName validates one exact field name crossing the
// runtime stats inventory trust boundary. Canonical promoted fields remain
// available, while compiler-private, storage, container, and descendants of
// reserved dynamic roots can never mint wildcard expansion authority.
func IsSafeStatsInventoryFieldName(name string) bool {
	if name == "" || len(name) > eventfields.MaximumNormalizedFieldNameBytes ||
		strings.HasPrefix(strings.ToLower(name), "__os_") {
		return false
	}
	if eventfields.IsCanonicalSPLField(name) {
		return true
	}
	root := name
	if index := strings.IndexByte(root, '.'); index >= 0 {
		root = root[:index]
	}
	if eventfields.IsReservedDynamicRoot(root) {
		return false
	}
	return IsExactUnquotedFieldName(name) || IsStatsLiteralFieldReference(name)
}
