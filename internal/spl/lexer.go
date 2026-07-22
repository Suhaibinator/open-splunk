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
)

type token struct {
	kind   tokenKind
	text   string
	quoted bool
	range_ Range
}

type lexer struct {
	source string
	offset int
	line   int
	column int
}

func lex(source string) ([]token, error) {
	l := lexer{source: source, line: 1, column: 1}
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
		return token{kind: tokenEOF, range_: Range{Start: start, End: start}}, nil
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
		return l.single(tokenEqual, "=", start), nil
	case '!':
		l.advanceASCII()
		if l.consumeASCII('=') {
			return l.single(tokenNotEqual, "!=", start), nil
		}
		return token{}, l.diagnostic("SPL_UNEXPECTED_CHARACTER", "expected '=' after '!'", start, l.position())
	case '<':
		l.advanceASCII()
		if l.consumeASCII('=') {
			return l.single(tokenLessEqual, "<=", start), nil
		}
		return l.single(tokenLess, "<", start), nil
	case '>':
		l.advanceASCII()
		if l.consumeASCII('=') {
			return l.single(tokenGreaterEqual, ">=", start), nil
		}
		return l.single(tokenGreater, ">", start), nil
	case '"':
		return l.scanString(start)
	default:
		return l.scanWord(start)
	}
}

func (l *lexer) scanString(start Position) (token, error) {
	l.advanceASCII() // opening quote
	var value strings.Builder
	for l.offset < len(l.source) {
		if l.source[l.offset] == '"' {
			l.advanceASCII()
			return token{kind: tokenString, text: value.String(), quoted: true, range_: Range{Start: start, End: l.position()}}, nil
		}
		if l.source[l.offset] == '\\' {
			escapeStart := l.position()
			l.advanceASCII()
			if l.offset >= len(l.source) {
				return token{}, l.diagnostic("SPL_UNTERMINATED_STRING", "unterminated quoted string", start, l.position())
			}
			escaped := l.source[l.offset]
			l.advanceASCII()
			switch escaped {
			case '"', '\\':
				value.WriteByte(escaped)
			case 'n':
				value.WriteByte('\n')
			case 'r':
				value.WriteByte('\r')
			case 't':
				value.WriteByte('\t')
			default:
				return token{}, l.diagnostic(
					"SPL_INVALID_ESCAPE",
					fmt.Sprintf("unsupported escape sequence \\%c", escaped),
					escapeStart,
					l.position(),
				)
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
	for l.offset < len(l.source) {
		b := l.source[l.offset]
		if isDelimiter(b) {
			break
		}
		r, width := utf8.DecodeRuneInString(l.source[l.offset:])
		if unicode.IsSpace(r) {
			break
		}
		if r == utf8.RuneError && width == 1 {
			l.advanceASCII()
			continue
		}
		l.advanceRune(r, width)
	}
	if startOffset == l.offset {
		l.advanceASCII()
		return token{}, l.diagnostic(
			"SPL_UNEXPECTED_CHARACTER",
			fmt.Sprintf("unexpected character %q", l.source[startOffset:l.offset]),
			start,
			l.position(),
		)
	}
	return token{kind: tokenWord, text: l.source[startOffset:l.offset], range_: Range{Start: start, End: l.position()}}, nil
}

func isDelimiter(b byte) bool {
	switch b {
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
	return token{kind: kind, text: text, range_: Range{Start: start, End: l.position()}}
}

func (l *lexer) consumeASCII(want byte) bool {
	if l.offset >= len(l.source) || l.source[l.offset] != want {
		return false
	}
	l.advanceASCII()
	return true
}

func (l *lexer) advanceASCII() {
	b := l.source[l.offset]
	l.offset++
	if b == '\n' {
		l.line++
		l.column = 1
		return
	}
	l.column++
}

func (l *lexer) advanceRune(r rune, width int) {
	l.offset += width
	if r == '\n' {
		l.line++
		l.column = 1
		return
	}
	l.column++
}

func (l *lexer) position() Position {
	return Position{Offset: l.offset, Line: l.line, Column: l.column}
}

func (*lexer) diagnostic(code, message string, start, end Position) *Diagnostic {
	return &Diagnostic{Code: code, Message: message, Range: Range{Start: start, End: end}}
}
