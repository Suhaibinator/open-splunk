package spl

import "strings"

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
