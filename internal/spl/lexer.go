package spl

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokenKind uint8

const (
	tokenInvalid tokenKind = iota
	tokenEOF
	tokenWord
	tokenString
	tokenQuotedField
	tokenScalarComposite
	tokenPipe
	tokenLeftParen
	tokenRightParen
	tokenComma
	tokenEqual
	tokenNotEqual
	tokenLess
	tokenLessEqual
	tokenGreater
	tokenGreaterEqual
	tokenConcat
	tokenPlus
	tokenMinus
	tokenMultiply
	tokenDivide
	tokenRemainder
	tokenEqualEqual
)

type token struct {
	kind        tokenKind
	text        string
	raw         string
	quoted      bool
	sourceRange Range

	// scalarDiagnostic is deferred until a token is consumed by the authored
	// scalar grammar. Base search can expand the same raw token through base
	// tokenization without inheriting quoted-field escape semantics.
	scalarDiagnostic *Diagnostic
}

type lexer struct {
	source string
	offset int
	line   int
	column int

	quotedFields bool
}

func lex(source string) ([]token, error) {
	return lexWithQuotedFields(source, true)
}

func lexWithQuotedFields(source string, quotedFields bool) ([]token, error) {
	l := lexer{source: source, line: 1, column: 1, quotedFields: quotedFields}
	tokens := make([]token, 0, 16)
	for {
		tok, err := l.next()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
		if tok.kind == tokenEOF {
			return tokens, nil
		}
	}
}

func (l *lexer) next() (token, error) {
	l.skipSpace()
	start := l.position()
	if l.offset >= len(l.source) {
		return token{kind: tokenEOF, sourceRange: Range{Start: start, End: start}}, nil
	}

	switch l.source[l.offset] {
	case '|':
		l.advanceASCII()
		return l.single(tokenPipe, "|", start), nil
	case '(':
		l.advanceASCII()
		return l.single(tokenLeftParen, "(", start), nil
	case ')':
		l.advanceASCII()
		return l.single(tokenRightParen, ")", start), nil
	case ',':
		l.advanceASCII()
		return l.single(tokenComma, ",", start), nil
	case '=':
		l.advanceASCII()
		if l.consumeEquals() {
			return l.single(tokenEqualEqual, "==", start), nil
		}
		return l.single(tokenEqual, "=", start), nil
	case '!':
		l.advanceASCII()
		if l.consumeEquals() {
			return l.single(tokenNotEqual, "!=", start), nil
		}
		return token{}, l.diagnostic("SPL_UNEXPECTED_CHARACTER", "expected '=' after '!'", start, l.position())
	case '<':
		l.advanceASCII()
		if l.consumeEquals() {
			return l.single(tokenLessEqual, "<=", start), nil
		}
		return l.single(tokenLess, "<", start), nil
	case '>':
		l.advanceASCII()
		if l.consumeEquals() {
			return l.single(tokenGreaterEqual, ">=", start), nil
		}
		return l.single(tokenGreater, ">", start), nil
	case '.':
		if concatenationDotAt(l.source, l.offset) {
			l.advanceASCII()
			return l.single(tokenConcat, ".", start), nil
		}
		return l.scanWord(start)
	case '"':
		return l.scanString(start)
	case '\'':
		if l.quotedFields && l.hasClosingSingleQuote() {
			return l.scanQuotedField(start)
		}
		return l.scanWord(start)
	default:
		return l.scanWord(start)
	}
}

// scanQuotedField decodes the deliberately small escape language used by an
// exact scalar field reference. Single quotes are recognized only when they
// open a token; an apostrophe inside an ordinary word (for example O'Reilly)
// remains part of that word and preserves the legacy non-scalar grammar.
func (l *lexer) scanQuotedField(start Position) (token, error) {
	startOffset := l.offset
	l.advanceASCII() // opening quote
	var value strings.Builder
	var deferred *Diagnostic
	for l.offset < len(l.source) {
		if l.source[l.offset] == '\'' {
			l.advanceASCII()
			return token{
				kind:             tokenQuotedField,
				text:             value.String(),
				raw:              l.source[startOffset:l.offset],
				quoted:           true,
				sourceRange:      Range{Start: start, End: l.position()},
				scalarDiagnostic: deferred,
			}, nil
		}
		if l.source[l.offset] == '\\' {
			escapeStart := l.position()
			l.advanceASCII()
			if l.offset >= len(l.source) {
				break
			}
			escaped, width := utf8.DecodeRuneInString(l.source[l.offset:])
			if escaped != '\\' && escaped != '\'' {
				if escaped == utf8.RuneError && width == 1 {
					l.advanceASCII()
				} else {
					l.advanceRune(escaped, width)
				}
				if deferred == nil {
					deferred = l.diagnostic(
						"SPL_INVALID_FIELD_QUOTE_ESCAPE",
						"single-quoted field references support only \\\\ and \\' escapes",
						escapeStart,
						l.position(),
					)
				}
				continue
			}
			value.WriteRune(escaped)
			l.advanceRune(escaped, width)
			continue
		}
		r, width := utf8.DecodeRuneInString(l.source[l.offset:])
		if r == utf8.RuneError && width == 1 {
			invalidStart := l.position()
			l.advanceASCII()
			if deferred == nil {
				deferred = l.diagnostic(
					"SPL_INVALID_FIELD",
					"single-quoted field reference must contain valid UTF-8",
					invalidStart,
					l.position(),
				)
			}
			continue
		}
		value.WriteRune(r)
		l.advanceRune(r, width)
	}
	return token{}, l.diagnostic(
		"SPL_UNTERMINATED_FIELD_QUOTE",
		"unterminated single-quoted field reference",
		start,
		l.position(),
	)
}

func (l *lexer) hasClosingSingleQuote() bool {
	for offset := l.offset + 1; offset < len(l.source); {
		if l.source[offset] == '\'' {
			return true
		}
		if l.source[offset] == '\\' {
			offset++
			if offset >= len(l.source) {
				return false
			}
			_, width := utf8.DecodeRuneInString(l.source[offset:])
			offset += width
			continue
		}
		_, width := utf8.DecodeRuneInString(l.source[offset:])
		offset += width
	}
	return false
}

func (l *lexer) scanString(start Position) (token, error) {
	l.advanceASCII() // opening quote
	var value strings.Builder
	for l.offset < len(l.source) {
		if l.source[l.offset] == '"' {
			l.advanceASCII()
			return token{kind: tokenString, text: value.String(), quoted: true, sourceRange: Range{Start: start, End: l.position()}}, nil
		}
		if l.source[l.offset] == '\\' {
			l.advanceASCII()
			if l.offset >= len(l.source) {
				return token{}, l.diagnostic("SPL_UNTERMINATED_STRING", "unterminated quoted string", start, l.position())
			}
			escapedRune, width := utf8.DecodeRuneInString(l.source[l.offset:])
			if escapedRune == utf8.RuneError && width == 1 {
				escapedRune = rune(l.source[l.offset])
			}
			l.advanceRune(escapedRune, width)
			switch escapedRune {
			case '"', '\\':
				value.WriteRune(escapedRune)
			case 'n':
				value.WriteByte('\n')
			case 'r':
				value.WriteByte('\r')
			case 't':
				value.WriteByte('\t')
			default:
				// SPL regexes and replacement backreferences use single
				// backslashes (for example \d and \2). Preserve escapes that
				// are not string-control escapes for the consuming command.
				value.WriteByte('\\')
				value.WriteRune(escapedRune)
			}
			continue
		}
		r, width := utf8.DecodeRuneInString(l.source[l.offset:])
		if r == utf8.RuneError && width == 1 {
			value.WriteByte(l.source[l.offset])
			l.advanceASCII()
			continue
		}
		value.WriteRune(r)
		l.advanceRune(r, width)
	}
	return token{}, l.diagnostic("SPL_UNTERMINATED_STRING", "unterminated quoted string", start, l.position())
}

func (l *lexer) scanWord(start Position) (token, error) {
	startOffset := l.offset
	scan := scanScalarWordBoundary(l.source, start, l.quotedFields)
	l.offset = scan.end.Offset
	l.line = scan.end.Line
	l.column = scan.end.Column
	if startOffset == l.offset {
		l.advanceASCII()
		return token{}, l.diagnostic(
			"SPL_UNEXPECTED_CHARACTER",
			fmt.Sprintf("unexpected character %q", l.source[startOffset:l.offset]),
			start,
			l.position(),
		)
	}
	kind := tokenWord
	raw := ""
	if scan.composite {
		kind = tokenScalarComposite
		raw = l.source[startOffset:l.offset]
	}
	return token{
		kind:        kind,
		text:        l.source[startOffset:l.offset],
		raw:         raw,
		sourceRange: Range{Start: start, End: l.position()},
	}, nil
}

type scalarWordScan struct {
	end       Position
	composite bool
}

// scanScalarWordBoundary is the pure boundary scanner shared by ordinary and
// authored composite words. An operator-adjacent single quote makes the word a
// composite and temporarily suppresses delimiter/space boundaries; unquoted
// mode never opens that quote and therefore retains its original boundary.
func scanScalarWordBoundary(
	source string,
	start Position,
	quotedFields bool,
) scalarWordScan {
	position := start
	segmentStart := start.Offset
	inQuote := false
	composite := false
	for position.Offset < len(source) {
		value := source[position.Offset]
		if inQuote {
			switch value {
			case '\\':
				position = advancePositionByRune(position, '\\', 1)
				if position.Offset < len(source) {
					r, width := utf8.DecodeRuneInString(source[position.Offset:])
					position = advancePositionByRune(position, r, width)
				}
				continue
			case '\'':
				position = advancePositionByRune(position, '\'', 1)
				inQuote = false
				segmentStart = -1
				continue
			}
			r, width := utf8.DecodeRuneInString(source[position.Offset:])
			position = advancePositionByRune(position, r, width)
			continue
		}

		if value == '.' && concatenationDotAt(source, position.Offset) ||
			isDelimiter(value) {
			break
		}
		r, width := utf8.DecodeRuneInString(source[position.Offset:])
		if unicode.IsSpace(r) {
			break
		}
		if quotedFields && value == '\'' &&
			position.Offset == segmentStart && position.Offset > start.Offset {
			position = advancePositionByRune(position, '\'', 1)
			inQuote = true
			composite = true
			continue
		}
		if _, operator := scalarOperatorToken(value); operator {
			position = advancePositionByRune(position, r, width)
			segmentStart = position.Offset
			continue
		}
		position = advancePositionByRune(position, r, width)
	}
	return scalarWordScan{end: position, composite: composite}
}

// concatenationDotAt recognizes only spellings that cannot change the
// repository's existing contiguous dotted-field and decimal-token grammar.
// SPL's canonical first_name." ".last_name spelling is unambiguous because
// each operator neighbors a quoted literal (with optional Unicode space).
// Bare field-to-field concatenation must use a fully separated dot:
// left . right.
func concatenationDotAt(source string, offset int) bool {
	if offset < 0 || offset >= len(source) || source[offset] != '.' {
		return false
	}
	if quoteBeforeIgnoringSpace(source, offset) {
		return true
	}
	if quoteAfterIgnoringSpace(source, offset) {
		return true
	}
	return dotBoundaryBefore(source, offset) && dotBoundaryAfter(source, offset)
}

func quoteBeforeIgnoringSpace(source string, offset int) bool {
	for cursor := offset; cursor > 0; {
		r, width := utf8.DecodeLastRuneInString(source[:cursor])
		cursor -= width
		if unicode.IsSpace(r) {
			continue
		}
		return r == '"'
	}
	return false
}

func quoteAfterIgnoringSpace(source string, offset int) bool {
	for cursor := offset + 1; cursor < len(source); {
		r, width := utf8.DecodeRuneInString(source[cursor:])
		cursor += width
		if unicode.IsSpace(r) {
			continue
		}
		return r == '"'
	}
	return false
}

func dotBoundaryBefore(source string, offset int) bool {
	if offset == 0 {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(source[:offset])
	if unicode.IsSpace(r) {
		return true
	}
	return isDelimiterRune(r)
}

func dotBoundaryAfter(source string, offset int) bool {
	if offset+1 >= len(source) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(source[offset+1:])
	if unicode.IsSpace(r) {
		return true
	}
	return isDelimiterRune(r)
}

func isDelimiter(b byte) bool {
	return isDelimiterRune(rune(b))
}

func isDelimiterRune(r rune) bool {
	switch r {
	case '|', '(', ')', ',', '=', '!', '<', '>', '"':
		return true
	default:
		return false
	}
}

func (l *lexer) skipSpace() {
	for l.offset < len(l.source) {
		r, width := utf8.DecodeRuneInString(l.source[l.offset:])
		if !unicode.IsSpace(r) {
			return
		}
		if r == utf8.RuneError && width == 1 {
			return
		}
		l.advanceRune(r, width)
	}
}

func (l *lexer) single(kind tokenKind, text string, start Position) token {
	return token{kind: kind, text: text, sourceRange: Range{Start: start, End: l.position()}}
}

func (l *lexer) consumeEquals() bool {
	if l.offset >= len(l.source) || l.source[l.offset] != '=' {
		return false
	}
	l.advanceASCII()
	return true
}

func (l *lexer) advanceASCII() {
	l.advanceRune(rune(l.source[l.offset]), 1)
}

func (l *lexer) advanceRune(r rune, width int) {
	position := advancePositionByRune(l.position(), r, width)
	l.offset = position.Offset
	l.line = position.Line
	l.column = position.Column
}

func advancePositionByRune(position Position, r rune, width int) Position {
	position.Offset += width
	if r == '\n' {
		position.Line++
		position.Column = 1
	} else {
		position.Column++
	}
	return position
}

func (l *lexer) position() Position {
	return Position{Offset: l.offset, Line: l.line, Column: l.column}
}

func (*lexer) diagnostic(code, message string, start, end Position) *Diagnostic {
	return &Diagnostic{Code: code, Message: message, Range: Range{Start: start, End: end}}
}
