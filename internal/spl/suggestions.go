package spl

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	// MaximumSuggestionSourceBytes bounds the pure cursor analyzer to the same
	// source budget as Parse.
	MaximumSuggestionSourceBytes = maxSPLSourceBytes
	// DefaultSuggestionLimit is used when callers do not request a positive
	// result limit.
	DefaultSuggestionLimit = 20
	// MaximumSuggestionLimit is the hard result bound applied by the pure API.
	MaximumSuggestionLimit = 100
)

// SuggestionKind identifies one completion namespace.
type SuggestionKind string

const (
	SuggestionKindCommand  SuggestionKind = "command"
	SuggestionKindFunction SuggestionKind = "function"
	SuggestionKindField    SuggestionKind = "field"
	SuggestionKindIndex    SuggestionKind = "index"
	SuggestionKindKeyword  SuggestionKind = "keyword"
)

// SuggestionFunctionClass separates scalar expression functions from stats
// aggregate functions.
type SuggestionFunctionClass string

const (
	SuggestionFunctionClassScalar    SuggestionFunctionClass = "scalar"
	SuggestionFunctionClassAggregate SuggestionFunctionClass = "aggregate"
)

// SuggestionContext is the bounded syntactic state at one UTF-8 byte cursor.
// Replacement is always a half-open byte range and Prefix is the source text
// from Replacement.Start through the cursor.
type SuggestionContext struct {
	Kinds         []SuggestionKind
	FunctionClass SuggestionFunctionClass
	FunctionNames []string
	Keywords      []string
	Exclusions    []SuggestionExclusion
	// AllowsQuotedScalarFields is true only where the v0.2 scalar grammar
	// accepts exact single-quoted field references.
	AllowsQuotedScalarFields bool
	Prefix                   string
	Replacement              Range
	// PipelinePrefixEnd is the byte offset immediately before the real pipe
	// that introduces the active stage. source[:PipelinePrefixEnd] is the
	// preceding pipeline, and the value is zero for the base-search stage.
	PipelinePrefixEnd int
}

// SuggestionExclusion removes one candidate label from a context. Labels are
// matched exactly unless ASCIIFold is set. Kind scopes the exclusion so a
// reserved keyword does not hide an unrelated candidate namespace.
type SuggestionExclusion struct {
	Kind      SuggestionKind
	Label     string
	ASCIIFold bool
}

// Allows reports whether context admits candidates of kind.
func (context SuggestionContext) Allows(kind SuggestionKind) bool {
	for _, allowed := range context.Kinds {
		if allowed == kind {
			return true
		}
	}
	return false
}

// SuggestionCandidate is static catalog metadata or an authorized dynamic
// field/index candidate supplied by a caller. Larger priorities rank first
// after exactness and context kind.
type SuggestionCandidate struct {
	Kind          SuggestionKind
	Label         string
	Insertion     string
	Detail        string
	Priority      int
	FunctionClass SuggestionFunctionClass
}

// Suggestion is a ranked candidate located at the cursor replacement range.
type Suggestion struct {
	SuggestionCandidate
	Replacement Range
	Relevance   float64
}

// SuggestionResult contains either a diagnostic or a safe context and its
// ranked static suggestions. Diagnostics deliberately carry no suggestions.
type SuggestionResult struct {
	Context     SuggestionContext
	Suggestions []Suggestion
	Diagnostic  *Diagnostic
}

// Suggest analyzes source at cursorByteOffset and ranks the shared static
// catalog. A service can instead call AnalyzeSuggestionContext, append
// authorized field/index candidates to StaticSuggestionCandidates, and rank
// the combined set with RankSuggestionCandidates.
func Suggest(source string, cursorByteOffset, limit int) SuggestionResult {
	context, diagnostic := AnalyzeSuggestionContext(source, cursorByteOffset)
	if diagnostic != nil {
		return SuggestionResult{Diagnostic: diagnostic}
	}
	return SuggestionResult{
		Context:     context,
		Suggestions: RankSuggestionCandidates(context, StaticSuggestionCandidates(context), limit),
	}
}

// AnalyzeSuggestionContext performs a tolerant, bounded scan using the SPL
// lexer's token and quote semantics. It does not require the source suffix to
// form a complete parse tree.
func AnalyzeSuggestionContext(source string, cursorByteOffset int) (SuggestionContext, *Diagnostic) {
	if len(source) > MaximumSuggestionSourceBytes {
		start := sourcePositionAtOffset(source, MaximumSuggestionSourceBytes)
		end := sourcePositionAtOffset(source, MaximumSuggestionSourceBytes+1)
		return SuggestionContext{}, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search source exceeds %d UTF-8 bytes", MaximumSuggestionSourceBytes),
			Range:   Range{Start: start, End: end},
		}
	}
	if invalidOffset := firstInvalidUTF8Offset(source); invalidOffset >= 0 {
		return SuggestionContext{}, suggestionDiagnosticAt(
			source,
			"SPL_INVALID_UTF8",
			"search source must be valid UTF-8",
			invalidOffset,
			invalidOffset+1,
		)
	}
	if nulOffset := strings.IndexByte(source, 0); nulOffset >= 0 {
		return SuggestionContext{}, suggestionDiagnosticAt(
			source,
			"SPL_INVALID_SOURCE",
			"search source must not contain NUL bytes",
			nulOffset,
			nulOffset+1,
		)
	}
	if cursorByteOffset < 0 || cursorByteOffset > len(source) {
		position := sourcePositionAtOffset(source, cursorByteOffset)
		return SuggestionContext{}, &Diagnostic{
			Code:    "SPL_INVALID_CURSOR",
			Message: "cursor byte offset is outside the search source",
			Range:   Range{Start: position, End: position},
		}
	}
	if cursorByteOffset < len(source) && !utf8.RuneStart(source[cursorByteOffset]) {
		position := sourcePositionAtOffset(source, cursorByteOffset)
		return SuggestionContext{}, &Diagnostic{
			Code:    "SPL_INVALID_CURSOR",
			Message: "cursor byte offset must be on a UTF-8 boundary",
			Range:   Range{Start: position, End: position},
		}
	}

	scan, diagnostic := scanSuggestionCursor(source, cursorByteOffset)
	if diagnostic != nil {
		return SuggestionContext{}, diagnostic
	}
	if scan.blocked {
		return SuggestionContext{Prefix: scan.prefix, Replacement: scan.replacement}, nil
	}
	scan, diagnostic = prepareScalarSuggestionScan(source, cursorByteOffset, scan)
	if diagnostic != nil {
		return SuggestionContext{}, diagnostic
	}

	context := classifySuggestionContext(scan.tokens, scan.prefix, scan.replacement)
	if scan.activeWord && suggestionStageCommand(scan.tokens) == "sort" &&
		context.Allows(SuggestionKindField) &&
		len(context.Prefix) > 0 &&
		(context.Prefix[0] == '-' || context.Prefix[0] == '+') {
		startOffset := context.Replacement.Start.Offset + 1
		context.Prefix = context.Prefix[1:]
		context.Replacement.Start = sourcePositionAtOffset(source, startOffset)
	}
	if context.Prefix != "" && classifyLiteral(context.Prefix, false) != LiteralKindString {
		context.Kinds = removeLiteralSuggestionKinds(context.Kinds)
		context.FunctionClass = ""
		context.FunctionNames = nil
		context.Keywords = nil
	}
	context = addFrequencySuggestionSuffixExclusions(source, scan, context)
	return context, nil
}

type suggestionCursorScan struct {
	tokens         []token
	prefix         string
	replacement    Range
	cursorPosition Position
	activeWord     bool
	blocked        bool
}

func scanSuggestionCursor(source string, cursor int) (suggestionCursorScan, *Diagnostic) {
	position := sourcePositionAtOffset(source, cursor)
	scan := suggestionCursorScan{
		replacement:    Range{Start: position, End: position},
		cursorPosition: position,
	}
	l := lexer{source: source, line: 1, column: 1}
	parenthesisDepth := 0
	pipelineCommands := 0

	for {
		l.skipSpace()
		if l.offset > cursor {
			return scan, nil
		}
		if l.offset >= len(source) {
			return scan, nil
		}

		tok, err := l.next()
		if err != nil {
			var diagnostic *Diagnostic
			if errors.As(err, &diagnostic) {
				return suggestionCursorScan{}, diagnostic
			}
			return suggestionCursorScan{}, &Diagnostic{
				Code:    "SPL_UNEXPECTED_CHARACTER",
				Message: err.Error(),
				Range:   Range{Start: position, End: position},
			}
		}
		start := tok.sourceRange.Start.Offset
		end := tok.sourceRange.End.Offset

		if cursor < end {
			if (tok.kind == tokenWord || tok.kind == tokenScalarComposite) && cursor >= start {
				if len(scan.tokens) >= maxSPLTokens {
					return suggestionCursorScan{}, tooManySuggestionTokens(tok.sourceRange)
				}
				scan.prefix = source[start:cursor]
				scan.replacement = tok.sourceRange
				scan.activeWord = true
				return scan, nil
			}
			scan.blocked = true
			return scan, nil
		}
		if cursor == end &&
			(tok.kind == tokenWord || tok.kind == tokenScalarComposite) {
			if len(scan.tokens) >= maxSPLTokens {
				return suggestionCursorScan{}, tooManySuggestionTokens(tok.sourceRange)
			}
			scan.prefix = source[start:cursor]
			scan.replacement = tok.sourceRange
			scan.activeWord = true
			return scan, nil
		}

		if len(scan.tokens) >= maxSPLTokens {
			return suggestionCursorScan{}, tooManySuggestionTokens(tok.sourceRange)
		}
		switch tok.kind {
		case tokenLeftParen:
			parenthesisDepth++
		case tokenRightParen:
			if parenthesisDepth == 0 {
				return suggestionCursorScan{}, &Diagnostic{
					Code:    "SPL_UNEXPECTED_TOKEN",
					Message: "unmatched closing parenthesis",
					Range:   tok.sourceRange,
				}
			}
			parenthesisDepth--
		case tokenPipe:
			if parenthesisDepth != 0 {
				return suggestionCursorScan{}, &Diagnostic{
					Code:    "SPL_UNEXPECTED_TOKEN",
					Message: "pipeline separator cannot appear inside parentheses",
					Range:   tok.sourceRange,
				}
			}
			pipelineCommands++
			if pipelineCommands > maxPipelineCommands {
				return suggestionCursorScan{}, &Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("search contains more than %d pipeline commands", maxPipelineCommands),
					Range:   tok.sourceRange,
				}
			}
		}
		scan.tokens = append(scan.tokens, tok)
		if cursor == end {
			return scan, nil
		}
	}
}

func prepareScalarSuggestionScan(
	source string,
	cursor int,
	scan suggestionCursorScan,
) (suggestionCursorScan, *Diagnostic) {
	scalarStart, scalar := scalarSuggestionTokenStart(scan.tokens)
	if !scalar {
		return scan, nil
	}

	activeStart := scan.replacement.Start.Offset
	activeEnd := scan.replacement.End.Offset
	normalizationStart := scan.cursorPosition
	if scalarStart < len(scan.tokens) {
		normalizationStart = scan.tokens[scalarStart].sourceRange.Start
	} else if scan.activeWord {
		normalizationStart = scan.replacement.Start
	}
	capacity := scalarStart + activeEnd - normalizationStart.Offset
	if capacity > maxSPLTokens {
		capacity = maxSPLTokens
	}
	tokens := make([]token, 0, capacity)
	tokens = append(tokens, scan.tokens[:scalarStart]...)

	if normalizationStart.Offset < activeEnd {
		l := lexer{
			source:       source[:activeEnd],
			offset:       normalizationStart.Offset,
			line:         normalizationStart.Line,
			column:       normalizationStart.Column,
			quotedFields: true,
		}
		for {
			tok, err := l.next()
			if err != nil {
				var diagnostic *Diagnostic
				if errors.As(err, &diagnostic) && diagnostic != nil {
					return suggestionCursorScan{}, diagnostic
				}
				return suggestionCursorScan{}, &Diagnostic{
					Code:    "SPL_UNEXPECTED_TOKEN",
					Message: err.Error(),
					Range:   Range{Start: scan.cursorPosition, End: scan.cursorPosition},
				}
			}
			if tok.kind == tokenEOF {
				break
			}
			firstPrepared := len(tokens)
			tokens, err = appendScalarSuggestionToken(tokens, source, tok)
			if err != nil {
				var diagnostic *Diagnostic
				if errors.As(err, &diagnostic) && diagnostic != nil {
					return suggestionCursorScan{}, diagnostic
				}
				return suggestionCursorScan{}, &Diagnostic{
					Code:    "SPL_UNEXPECTED_TOKEN",
					Message: err.Error(),
					Range:   tok.sourceRange,
				}
			}
			if len(tokens) > maxSPLTokens {
				return suggestionCursorScan{}, tooManySuggestionTokens(tok.sourceRange)
			}
			if !scan.activeWord {
				continue
			}
			for index := firstPrepared; index < len(tokens); index++ {
				prepared := tokens[index]
				if prepared.kind != tokenWord ||
					prepared.sourceRange.Start.Offset < activeStart ||
					prepared.sourceRange.End.Offset > activeEnd ||
					cursor < prepared.sourceRange.Start.Offset ||
					cursor > prepared.sourceRange.End.Offset {
					continue
				}
				scan.prefix = source[prepared.sourceRange.Start.Offset:cursor]
				scan.replacement = prepared.sourceRange
				scan.tokens = tokens[:index]
				return scan, nil
			}
		}
	}
	scan.tokens = tokens
	if !scan.activeWord {
		return scan, nil
	}

	completed := 0
	for completed < len(scan.tokens) &&
		scan.tokens[completed].sourceRange.End.Offset <= cursor {
		completed++
	}
	scan.tokens = scan.tokens[:completed]
	scan.prefix = ""
	scan.replacement = Range{Start: scan.cursorPosition, End: scan.cursorPosition}
	scan.activeWord = false
	return scan, nil
}

func appendScalarSuggestionToken(
	tokens []token,
	source string,
	tok token,
) ([]token, error) {
	switch tok.kind {
	case tokenWord:
		return appendScalarSuggestionWord(tokens, tok), nil
	case tokenScalarComposite:
		return appendScalarSuggestionComposite(tokens, source, tok)
	default:
		return append(tokens, tok), nil
	}
}

func appendScalarSuggestionWord(tokens []token, tok token) []token {
	prepared, split := appendV02ScalarWord(tokens, tok)
	if !split {
		return append(tokens, tok)
	}
	return prepared
}

func appendScalarSuggestionComposite(
	tokens []token,
	source string,
	composite token,
) ([]token, error) {
	text := composite.text
	position := composite.sourceRange.Start
	fragmentStart := 0
	fragmentPosition := position
	scalarSegmentStart := 0

	for offset := 0; offset < len(text); {
		value := text[offset]
		if value == '\'' && offset == scalarSegmentStart && offset > 0 {
			if fragmentStart < offset {
				tokens = appendScalarSuggestionWord(tokens, token{
					kind: tokenWord,
					text: text[fragmentStart:offset],
					sourceRange: Range{
						Start: fragmentPosition,
						End:   position,
					},
				})
			}

			quoteLexer := lexer{
				source:       source,
				offset:       position.Offset,
				line:         position.Line,
				column:       position.Column,
				quotedFields: true,
			}
			if !quoteLexer.hasClosingSingleQuote() {
				return nil, &Diagnostic{
					Code:    "SPL_UNTERMINATED_FIELD_QUOTE",
					Message: "unterminated single-quoted field reference",
					Range: Range{
						Start: position,
						End:   advanceSourcePosition(position, "'"),
					},
				}
			}
			quoted, err := quoteLexer.scanQuotedField(position)
			if err != nil {
				return nil, err
			}
			if quoted.sourceRange.End.Offset > composite.sourceRange.End.Offset {
				return nil, &Diagnostic{
					Code:    "SPL_UNTERMINATED_FIELD_QUOTE",
					Message: "unterminated single-quoted field reference",
					Range: Range{
						Start: position,
						End:   advanceSourcePosition(position, "'"),
					},
				}
			}
			tokens = append(tokens, quoted)
			offset += quoted.sourceRange.End.Offset - position.Offset
			position = quoted.sourceRange.End
			fragmentStart = offset
			fragmentPosition = position
			scalarSegmentStart = -1
			continue
		}

		r, width := utf8.DecodeRuneInString(text[offset:])
		if _, operator := scalarOperatorToken(value); operator {
			scalarSegmentStart = offset + width
		}
		position = advancePositionByRune(position, r, width)
		offset += width
	}
	if fragmentStart < len(text) {
		tokens = appendScalarSuggestionWord(tokens, token{
			kind: tokenWord,
			text: text[fragmentStart:],
			sourceRange: Range{
				Start: fragmentPosition,
				End:   position,
			},
		})
	}
	return tokens, nil
}

func tooManySuggestionTokens(sourceRange Range) *Diagnostic {
	return &Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf("search contains more than %d syntax tokens", maxSPLTokens),
		Range:   sourceRange,
	}
}

func classifySuggestionContext(tokens []token, prefix string, replacement Range) SuggestionContext {
	base := SuggestionContext{Prefix: prefix, Replacement: replacement}
	stage, stageStart, followsPipeline := activeSuggestionStage(tokens)
	if !followsPipeline {
		return classifySearchSuggestion(base, stage)
	}
	base.PipelinePrefixEnd = tokens[stageStart-1].sourceRange.Start.Offset

	if len(stage) == 0 {
		base.Kinds = []SuggestionKind{SuggestionKindCommand}
		return base
	}
	if stage[0].kind != tokenWord {
		return base
	}
	command := asciiFold(stage[0].text)
	body := stage[1:]
	switch command {
	case "search":
		return classifySearchSuggestion(base, body)
	case "where":
		base.AllowsQuotedScalarFields = true
		return classifyWhereSuggestion(base, body)
	case "eval":
		base.AllowsQuotedScalarFields = true
		return classifyEvalSuggestion(base, body)
	case "table":
		base.Kinds = []SuggestionKind{SuggestionKindField}
		base.AllowsQuotedScalarFields = true
		return base
	case "fields", "sort", "dedup":
		base.Kinds = []SuggestionKind{SuggestionKindField}
		return base
	case "regex":
		return classifyRegexSuggestion(base, body)
	case "accum":
		return classifyAccumSuggestion(base, body)
	case "strcat":
		return classifyStrcatSuggestion(base, body)
	case "fillnull":
		return classifyFillNullSuggestion(base, body)
	case "addtotals":
		return classifyAddTotalsSuggestion(base, body)
	case "delta":
		return classifyDeltaSuggestion(base, body)
	case "makemv":
		return classifyMakeMVSuggestion(base, body)
	case "mvexpand":
		return classifyMVExpandSuggestion(base, body)
	case "rename":
		return classifyRenameSuggestion(base, body)
	case "stats":
		return classifyStatsSuggestion(base, body)
	case "eventstats":
		return classifyEventStatsSuggestion(base, body)
	case "streamstats":
		return classifyStreamStatsSuggestion(base, body)
	case "chart":
		return classifyChartSuggestion(base, body)
	case "timechart":
		return classifyTimechartSuggestion(base, body)
	case "bin", "bucket":
		return classifyBinSuggestion(base, body)
	case "top", "rare":
		return classifyFrequencySuggestion(base, body)
	case "rex":
		return classifyRexSuggestion(base, body)
	case "spath":
		return classifySpathSuggestion(base, body)
	case "head", "tail", "reverse", "addinfo":
		return base
	default:
		return base
	}
}

func v03FieldKeywordSuggestion(
	context SuggestionContext,
	field bool,
	keywords ...string,
) SuggestionContext {
	if field {
		context.Kinds = append(context.Kinds, SuggestionKindField)
	}
	if len(keywords) > 0 {
		context.Kinds = append(context.Kinds, SuggestionKindKeyword)
		context.Keywords = append(context.Keywords, keywords...)
	}
	return context
}

func classifyAccumSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if len(tokens) == 0 {
		return v03FieldKeywordSuggestion(context, true)
	}
	if tokenWordEqual(tokens[len(tokens)-1], "AS") {
		return v03FieldKeywordSuggestion(context, true)
	}
	if len(tokens) == 1 {
		return v03FieldKeywordSuggestion(context, false, "AS")
	}
	return context
}

func classifyRegexSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	// The only catalog-backed operand is the optional exact field at the start.
	// After that field, its comparison operator, or a complete quoted pattern,
	// the grammar expects punctuation/a literal or is already complete.
	if len(tokens) == 0 {
		return v03FieldKeywordSuggestion(context, true)
	}
	return context
}

func classifyStrcatSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if endsOptionEqual(tokens, "allrequired") {
		return context
	}
	body, _, valid := consumeV03LeadingOptions(tokens, map[string]func(token) bool{
		"allrequired": v03BooleanSuggestionValue,
	})
	if !valid {
		return context
	}
	if len(body) == 0 && len(tokens) == 0 {
		return v03FieldKeywordSuggestion(context, true, "allrequired=")
	}
	return v03FieldKeywordSuggestion(context, true)
}

func classifyFillNullSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if endsOptionEqual(tokens, "value") {
		return context
	}
	body, _, valid := consumeV03LeadingOptions(tokens, map[string]func(token) bool{
		"value": func(value token) bool { return value.kind == tokenString },
	})
	if !valid {
		return context
	}
	if len(body) == 0 && len(tokens) == 0 {
		return v03FieldKeywordSuggestion(context, true, "value=")
	}
	return v03FieldKeywordSuggestion(context, true)
}

func classifyAddTotalsSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if endsOptionEqual(tokens, "fieldname") {
		return context
	}
	if endsOptionEqual(tokens, "row") || endsOptionEqual(tokens, "col") {
		return context
	}
	body, used, valid := consumeV03LeadingOptions(tokens, map[string]func(token) bool{
		"row": func(value token) bool {
			return value.kind == tokenWord && (tokenWordEqual(value, "t") || tokenWordEqual(value, "true"))
		},
		"col": func(value token) bool {
			return value.kind == tokenWord && (tokenWordEqual(value, "f") || tokenWordEqual(value, "false"))
		},
		"fieldname": v03ExactFieldSuggestionValue,
	})
	if !valid {
		return context
	}
	if len(body) != 0 {
		return v03FieldKeywordSuggestion(context, true)
	}
	keywords := make([]string, 0, 3-len(used))
	for _, option := range []string{"col", "fieldname", "row"} {
		if _, exists := used[option]; !exists {
			keywords = append(keywords, option+"=")
		}
	}
	return v03FieldKeywordSuggestion(context, true, keywords...)
}

func classifyDeltaSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if len(tokens) == 0 {
		return v03FieldKeywordSuggestion(context, true)
	}
	last := tokens[len(tokens)-1]
	if tokenWordEqual(last, "AS") {
		return v03FieldKeywordSuggestion(context, true)
	}
	if endsOptionEqual(tokens, "p") {
		return context
	}
	pIndex := topLevelOptionAssignmentIndex(tokens, "p")
	asIndex := topLevelWordIndex(tokens, "AS")
	if pIndex >= 0 {
		if pIndex+2 >= len(tokens) || tokens[pIndex+2].kind != tokenWord ||
			!unsignedIntegerSyntax(tokens[pIndex+2].text) {
			return context
		}
		if asIndex < 0 {
			return v03FieldKeywordSuggestion(context, false, "AS")
		}
		return context
	}
	if asIndex >= 0 {
		return v03FieldKeywordSuggestion(context, false, "p=")
	}
	return v03FieldKeywordSuggestion(context, false, "AS", "p=")
}

func classifyMakeMVSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if endsOptionEqual(tokens, "allowempty") || endsOptionEqual(tokens, "delim") {
		return context
	}
	body, used, valid := consumeV03LeadingOptions(tokens, map[string]func(token) bool{
		"allowempty": v03BooleanSuggestionValue,
		"delim": func(value token) bool {
			return value.kind == tokenString && value.text != ""
		},
	})
	if !valid || len(body) != 0 {
		// One non-option token is the terminal exact field. Invalid or additional
		// tokens also stay fail-closed rather than inviting a second field.
		return context
	}
	keywords := make([]string, 0, 2-len(used))
	for _, option := range []string{"allowempty", "delim"} {
		if _, exists := used[option]; !exists {
			keywords = append(keywords, option+"=")
		}
	}
	return v03FieldKeywordSuggestion(context, true, keywords...)
}

func classifyMVExpandSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if len(tokens) == 0 {
		return v03FieldKeywordSuggestion(context, true)
	}
	if topLevelOptionAssignmentIndex(tokens, "limit") < 0 {
		return v03FieldKeywordSuggestion(context, false, "limit=")
	}
	return context
}

// consumeV03LeadingOptions recognizes complete name=value triples at the
// beginning of a command suggestion body. It is intentionally stricter than
// generic option discovery: duplicate, malformed, or late options stop all
// completion so the editor never encourages an already-invalid command.
func consumeV03LeadingOptions(
	tokens []token,
	validators map[string]func(token) bool,
) (body []token, used map[string]struct{}, valid bool) {
	used = make(map[string]struct{}, len(validators))
	index := 0
	for index < len(tokens) {
		name := tokens[index]
		if name.kind != tokenWord || index+1 >= len(tokens) ||
			tokens[index+1].kind != tokenEqual {
			break
		}
		key := asciiFold(name.text)
		validator, supported := validators[key]
		if !supported || index+2 >= len(tokens) {
			return nil, used, false
		}
		if _, duplicate := used[key]; duplicate || !validator(tokens[index+2]) {
			return nil, used, false
		}
		used[key] = struct{}{}
		index += 3
	}
	return tokens[index:], used, true
}

func v03BooleanSuggestionValue(value token) bool {
	if value.kind != tokenWord {
		return false
	}
	_, ok := parseStreamStatsBool(value.text)
	return ok
}

func v03ExactFieldSuggestionValue(value token) bool {
	return value.kind == tokenWord && IsExactUnquotedFieldName(value.text) &&
		!strings.HasPrefix(strings.ToLower(value.text), "__os_")
}

func classifySearchSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if len(tokens) >= 2 &&
		tokens[len(tokens)-1].kind == tokenEqual &&
		tokenWordEqual(tokens[len(tokens)-2], "index") {
		context.Kinds = []SuggestionKind{SuggestionKindIndex}
		return context
	}
	if len(tokens) > 0 && isComparisonToken(tokens[len(tokens)-1].kind) {
		return context
	}
	if len(tokens) == 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField, SuggestionKindKeyword}
		context.Keywords = []string{"NOT"}
		return context
	}
	last := tokens[len(tokens)-1]
	if last.kind == tokenLeftParen ||
		(last.kind == tokenWord &&
			(tokenWordEqual(last, "AND") || tokenWordEqual(last, "OR") || tokenWordEqual(last, "NOT"))) {
		context.Kinds = []SuggestionKind{SuggestionKindField, SuggestionKindKeyword}
		context.Keywords = []string{"NOT"}
		return context
	}
	if last.kind == tokenWord || last.kind == tokenString || last.kind == tokenRightParen {
		context.Kinds = []SuggestionKind{SuggestionKindField, SuggestionKindKeyword}
		context.Keywords = []string{"AND", "OR", "NOT"}
	}
	return context
}

func classifyWhereSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if len(tokens) == 0 {
		return scalarSuggestionContext(context, true)
	}
	last := tokens[len(tokens)-1]
	if isScalarOperandBoundaryToken(last.kind) {
		return scalarSuggestionContext(context, false)
	}
	if last.kind == tokenLeftParen {
		includeNot := len(tokens) < 2 || tokens[len(tokens)-2].kind != tokenWord
		return scalarSuggestionContext(context, includeNot)
	}
	if last.kind == tokenWord && tokenWordEqual(last, "NOT") {
		if len(tokens) > 1 && !startsPredicateOperand(tokens[len(tokens)-2]) {
			context.Kinds = []SuggestionKind{SuggestionKindKeyword}
			context.Keywords = []string{"IN"}
			return context
		}
		return scalarSuggestionContext(context, true)
	}
	if last.kind == tokenWord &&
		(tokenWordEqual(last, "AND") || tokenWordEqual(last, "OR")) {
		return scalarSuggestionContext(context, true)
	}
	if last.kind == tokenWord || last.kind == tokenString || last.kind == tokenRightParen {
		context.Kinds = []SuggestionKind{SuggestionKindKeyword}
		context.Keywords = []string{"AND", "OR", "IN", "NOT"}
	}
	return context
}

func classifyEvalSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	segment := tokensAfterLastTopLevelComma(tokens)
	equalIndex := topLevelTokenIndex(segment, tokenEqual)
	if equalIndex < 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	expression := segment[equalIndex+1:]
	if len(expression) == 0 {
		return scalarSuggestionContext(context, false)
	}
	last := expression[len(expression)-1]
	if isScalarOperandBoundaryToken(last.kind) ||
		last.kind == tokenLeftParen ||
		(last.kind == tokenWord &&
			(tokenWordEqual(last, "AND") || tokenWordEqual(last, "OR") || tokenWordEqual(last, "NOT"))) {
		return scalarSuggestionContext(context, false)
	}
	return context
}

func classifyRenameSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	segment := tokensAfterLastTopLevelComma(tokens)
	switch len(segment) {
	case 0:
		context.Kinds = []SuggestionKind{SuggestionKindField}
	case 1:
		context.Kinds = []SuggestionKind{SuggestionKindKeyword}
		context.Keywords = []string{"AS"}
	default:
		if tokenWordEqual(segment[len(segment)-1], "AS") {
			context.Kinds = []SuggestionKind{SuggestionKindField}
		}
	}
	return context
}

func classifyStatsSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	for _, option := range []string{"partitions", "allnum", "delim", "dedup_splitvals"} {
		if endsOptionEqual(tokens, option) {
			return context
		}
	}
	body, usedLeadingOptions := statsSuggestionBody(tokens)
	trailingOption := topLevelOptionAssignmentIndex(body, "dedup_splitvals")
	if trailingOption >= 0 {
		// dedup_splitvals is terminal. Once its value is present, no append-only
		// completion can legally extend the command.
		if trailingOption+2 < len(body) {
			return context
		}
	}
	if index := topLevelWordIndex(body, "BY"); index >= 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		context.AllowsQuotedScalarFields = true
		if trailingOption < 0 && len(body) > index+1 {
			context.Kinds = append(context.Kinds, SuggestionKindKeyword)
			context.Keywords = []string{"dedup_splitvals="}
		}
		return context
	}
	if legacyContext, active := classifyStatsSparklineLegacySuggestion(
		context,
		body,
	); active {
		return legacyContext
	}
	if parenthesisDepth(body) > 0 {
		if sparklineContext, active := classifyActiveStatsSparklineSuggestion(
			context,
			body,
		); active {
			return sparklineContext
		}
		if predicateContext, active := classifyActiveCountEvalPredicateSuggestion(
			context,
			body,
		); active {
			return predicateContext
		}
		if scalarContext, active := classifyActiveStatsScalarEvalInputSuggestion(
			context,
			body,
		); active {
			return scalarContext
		}
		context.Kinds = []SuggestionKind{SuggestionKindField}
		context.AllowsQuotedScalarFields = true
		if star := strings.LastIndexByte(context.Prefix, '*'); star >= 0 {
			// Rank exact-field replacements by the literal suffix after the last
			// star instead of treating the metacharacter as part of a field prefix.
			context.Replacement.Start = advanceSourcePosition(
				context.Replacement.Start,
				context.Prefix[:star+1],
			)
			context.Prefix = context.Prefix[star+1:]
			context.AllowsQuotedScalarFields = false
		}
		return context
	}
	if len(body) == 0 {
		context = aggregateSuggestionContext(context)
		context.Kinds = append(context.Kinds, SuggestionKindKeyword)
		context.Keywords = remainingLeadingStatsSuggestionOptions(usedLeadingOptions)
		return context
	}
	last := body[len(body)-1]
	if tokenWordEqual(last, "AS") {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if last.kind == tokenLeftParen {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		context.AllowsQuotedScalarFields = true
		return context
	}
	context = aggregateSuggestionContext(context)
	context.Kinds = append(context.Kinds, SuggestionKindKeyword)
	context.Keywords = []string{"AS", "BY", "dedup_splitvals="}
	if !statsSuggestionAggregateStarted(body) {
		context.Keywords = append(
			context.Keywords,
			remainingLeadingStatsSuggestionOptions(usedLeadingOptions)...,
		)
	}
	return context
}

func classifyStatsSparklineLegacySuggestion(
	context SuggestionContext,
	tokens []token,
) (SuggestionContext, bool) {
	depth := 0
	start := -1
	for index, tok := range tokens {
		if depth == 0 && tokenWordEqual(tok, "sparkline") &&
			(index+1 >= len(tokens) || tokens[index+1].kind != tokenLeftParen) {
			start = index
		}
		switch tok.kind {
		case tokenLeftParen:
			depth++
		case tokenRightParen:
			if depth > 0 {
				depth--
			}
		}
	}
	if start < 0 {
		return context, false
	}
	suffix := tokens[start+1:]
	if len(suffix) > 1 ||
		(len(suffix) == 1 && tokenWordEqual(suffix[0], "count")) {
		return context, false
	}
	context = aggregateSuggestionContext(context)
	context.FunctionNames = []string{"count"}
	return context, true
}

func statsSparklineFunctionNames() []string {
	return []string{
		"c", "count", "dc", "mean", "avg", "stdev", "stdevp", "var",
		"varp", "sum", "sumsq", "min", "max", "range",
	}
}

func classifyActiveStatsSparklineSuggestion(
	context SuggestionContext,
	tokens []token,
) (SuggestionContext, bool) {
	start := -1
	depth := 0
	for index, tok := range tokens {
		if depth == 0 && tokenWordEqual(tok, "sparkline") &&
			index+1 < len(tokens) && tokens[index+1].kind == tokenLeftParen {
			start = index
		}
		switch tok.kind {
		case tokenLeftParen:
			depth++
		case tokenRightParen:
			if depth > 0 {
				depth--
			}
		}
	}
	if start < 0 || depth == 0 || start+2 > len(tokens) {
		return context, false
	}
	inner := tokens[start+2:]
	if len(inner) == 0 {
		context = aggregateSuggestionContext(context)
		context.FunctionNames = statsSparklineFunctionNames()
		return context, true
	}

	function := inner[0]
	if function.kind != tokenWord {
		return context, true
	}
	_, supported := statsSparklineAggregateSpecForName(function.text)
	if len(inner) == 1 || !supported {
		context = aggregateSuggestionContext(context)
		context.FunctionNames = statsSparklineFunctionNames()
		return context, true
	}
	if inner[1].kind == tokenLeftParen && parenthesisDepth(inner[1:]) > 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		context.AllowsQuotedScalarFields = true
		return context, true
	}
	// The inner aggregate has closed, or bare count is followed by the optional
	// span. There is no field/function completion at this point.
	return context, true
}

func statsSuggestionBody(tokens []token) ([]token, map[string]struct{}) {
	used := make(map[string]struct{}, 3)
	index := 0
	for index+2 < len(tokens) {
		name := tokens[index]
		if name.kind != tokenWord || !isLeadingStatsOptionName(name.text) ||
			tokens[index+1].kind != tokenEqual {
			break
		}
		value := tokens[index+2]
		if value.kind != tokenWord && value.kind != tokenString {
			break
		}
		used[asciiFold(name.text)] = struct{}{}
		index += 3
	}
	return tokens[index:], used
}

func remainingLeadingStatsSuggestionOptions(used map[string]struct{}) []string {
	result := make([]string, 0, 3-len(used))
	for _, name := range []string{"partitions=", "allnum=", "delim="} {
		key := strings.TrimSuffix(name, "=")
		if _, exists := used[key]; !exists {
			result = append(result, name)
		}
	}
	return result
}

func statsSuggestionAggregateStarted(tokens []token) bool {
	if len(tokens) == 0 || tokens[0].kind != tokenWord {
		return false
	}
	if supportedStatsAggregateName(tokens[0].text) {
		return true
	}
	return len(tokens) > 1 && tokens[1].kind == tokenLeftParen
}

func topLevelOptionAssignmentIndex(tokens []token, name string) int {
	depth := 0
	for index, tok := range tokens {
		if depth == 0 && tokenWordEqual(tok, name) && index+1 < len(tokens) &&
			tokens[index+1].kind == tokenEqual {
			return index
		}
		switch tok.kind {
		case tokenLeftParen:
			depth++
		case tokenRightParen:
			if depth > 0 {
				depth--
			}
		}
	}
	return -1
}

func classifyEventStatsSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if topLevelWordIndex(tokens, "BY") >= 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if parenthesisDepth(tokens) > 0 {
		if predicateContext, active := classifyActiveCountEvalPredicateSuggestion(
			context,
			tokens,
		); active {
			return predicateContext
		}
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if len(tokens) == 0 {
		context = aggregateSuggestionContext(context)
		context.FunctionNames = eventStatsFunctionNames()
		return context
	}
	last := tokens[len(tokens)-1]
	if tokenWordEqual(last, "AS") {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	context.Kinds = []SuggestionKind{SuggestionKindKeyword}
	if eventStatsMeasureRequiresAlias(tokens) &&
		topLevelWordIndex(tokens, "AS") < 0 {
		context.Keywords = []string{"AS"}
	} else if len(tokens) == 1 {
		context.Keywords = []string{"AS", "BY"}
	} else {
		context.Keywords = []string{"BY"}
	}
	return context
}

func classifyStreamStatsSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	optionKeywords, byClosed := streamStatsSuggestionOptions(tokens)
	aggregateIndex := topLevelStreamStatsAggregateIndex(tokens)
	var aggregateTokens []token
	if aggregateIndex >= 0 {
		aggregateTokens = tokens[aggregateIndex:]
	}
	countPredicate := startsCountEvalCall(aggregateTokens, 0)
	if endsOptionEqual(tokens, "current") || endsOptionEqual(tokens, "window") ||
		endsOptionEqual(tokens, "global") {
		return context
	}
	if parenthesisDepth(tokens) > 0 {
		if countPredicate {
			if predicateContext, active := classifyActiveCountEvalPredicateSuggestion(
				context,
				aggregateTokens,
			); active {
				return predicateContext
			}
		}
		if aggregateIndex >= 0 && aggregateIndex+1 < len(tokens) &&
			tokens[aggregateIndex+1].kind == tokenLeftParen {
			context.Kinds = []SuggestionKind{SuggestionKindField}
		}
		return context
	}
	for _, descriptor := range streamStatsAggregateDescriptors {
		if descriptor.allowsBare {
			continue
		}
		if functionIndex := topLevelWordIndex(tokens, descriptor.name); functionIndex >= 0 {
			byIndex := topLevelWordIndex(tokens, "BY")
			alias := functionIndex > 0 && tokenWordEqual(tokens[functionIndex-1], "AS")
			group := byIndex >= 0 && functionIndex > byIndex
			optionValue := functionIndex > 0 && tokens[functionIndex-1].kind == tokenEqual
			call := functionIndex+1 < len(tokens) &&
				tokens[functionIndex+1].kind == tokenLeftParen
			if !alias && !group && !optionValue && !call {
				// Unlike bare count, bare numeric aggregates are incomplete. Do
				// not offer trailing syntax that would extend an invalid command.
				return context
			}
		}
	}
	if len(tokens) > 0 && tokenWordEqual(tokens[len(tokens)-1], "AS") {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	requiresAlias := countPredicate
	aliasMissing := topLevelWordIndex(tokens, "AS") < 0
	if topLevelWordIndex(tokens, "BY") >= 0 {
		if requiresAlias && aliasMissing {
			// The parser requires the conditional-count alias before BY. Once a
			// user has manually typed the invalid order, no append-only completion
			// can repair it.
			context.Kinds = nil
			context.FunctionNames = nil
			context.Keywords = nil
			return context
		}
		if !byClosed {
			context.Kinds = append(context.Kinds, SuggestionKindField)
		}
		if len(optionKeywords) > 0 {
			context.Kinds = append(context.Kinds, SuggestionKindKeyword)
		}
		context.Keywords = optionKeywords
		return context
	}
	if aggregateIndex < 0 {
		context = aggregateSuggestionContext(context)
		context.FunctionNames = streamStatsFunctionNames()
		context.Kinds = append(context.Kinds, SuggestionKindKeyword)
		context.Keywords = optionKeywords
		return context
	}
	context.Kinds = []SuggestionKind{SuggestionKindKeyword}
	if aliasMissing {
		context.Keywords = append(context.Keywords, "AS")
	}
	if !requiresAlias || !aliasMissing {
		context.Keywords = append(context.Keywords, "BY")
	}
	context.Keywords = append(context.Keywords, optionKeywords...)
	return context
}

func topLevelStreamStatsAggregateIndex(tokens []token) int {
	for _, descriptor := range streamStatsAggregateDescriptors {
		if index := topLevelWordIndex(tokens, descriptor.name); index >= 0 {
			return index
		}
	}
	return -1
}

func streamStatsSuggestionOptions(tokens []token) ([]string, bool) {
	used := make(map[string]struct{}, 3)
	byIndex := topLevelWordIndex(tokens, "BY")
	byClosed := false
	for index := 0; index+1 < len(tokens); index++ {
		if tokens[index].kind != tokenWord || tokens[index+1].kind != tokenEqual {
			continue
		}
		name := asciiFold(tokens[index].text)
		switch name {
		case "current", "window", "global":
			used[name] = struct{}{}
			if byIndex >= 0 && index > byIndex {
				byClosed = true
			}
		}
	}
	keywords := make([]string, 0, 3-len(used))
	for _, name := range []string{"current", "window", "global"} {
		if _, exists := used[name]; !exists {
			keywords = append(keywords, name+"=")
		}
	}
	return keywords, byClosed
}

func eventStatsMeasureRequiresAlias(tokens []token) bool {
	if len(tokens) < 4 || tokens[1].kind != tokenLeftParen ||
		tokens[len(tokens)-1].kind != tokenRightParen {
		return false
	}
	if tokenWordEqual(tokens[0], "count") {
		return true
	}
	_, supported := eventStatsFieldAggregateSpecForName(tokens[0].text)
	return supported
}

func classifyChartSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if len(tokens) == 0 {
		context = aggregateSuggestionContext(context)
		context.FunctionNames = []string{"count", "p50", "p95", "sum", "avg"}
		return context
	}
	if _, supported := pivotFieldAggregateSpecForName(tokens[0].text); supported &&
		parenthesisDepth(tokens) > 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if byIndex := topLevelWordIndex(tokens, "BY"); byIndex >= 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if overIndex := topLevelWordIndex(tokens, "OVER"); overIndex >= 0 {
		if len(tokens) == overIndex+1 {
			context.Kinds = []SuggestionKind{SuggestionKindField}
			return context
		}
		context.Kinds = []SuggestionKind{SuggestionKindKeyword}
		context.Keywords = []string{"BY"}
		return context
	}
	context.Kinds = []SuggestionKind{SuggestionKindKeyword}
	context.Keywords = []string{"OVER", "BY"}
	return context
}

func classifyTimechartSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if len(tokens) == 0 {
		context.Kinds = []SuggestionKind{SuggestionKindKeyword}
		context.Keywords = []string{"span="}
		return context
	}
	if endsOptionEqual(tokens, "span") {
		return context
	}
	if len(tokens) == 3 &&
		tokenWordEqual(tokens[0], "span") &&
		tokens[1].kind == tokenEqual &&
		tokens[2].kind == tokenWord {
		context = aggregateSuggestionContext(context)
		context.FunctionNames = []string{"count", "p50", "p95", "sum", "avg"}
		return context
	}
	if len(tokens) < 4 ||
		!tokenWordEqual(tokens[0], "span") ||
		tokens[1].kind != tokenEqual ||
		tokens[2].kind != tokenWord {
		return context
	}
	aggregate := tokens[3:]
	if tokenWordEqual(aggregate[0], "count") {
		if len(aggregate) == 1 || aggregate[1].kind != tokenLeftParen {
			if topLevelWordIndex(aggregate, "BY") >= 0 {
				context.Kinds = []SuggestionKind{SuggestionKindField}
				return context
			}
			if len(aggregate) != 1 {
				return context
			}
			context.Kinds = []SuggestionKind{SuggestionKindKeyword}
			context.Keywords = []string{"BY"}
			return context
		}
	}
	spec, supported := pivotFieldAggregateSpecForName(aggregate[0].text)
	if !supported {
		return context
	}
	splitSupported := timechartAggregateSupportsSplit(spec.function)
	if parenthesisDepth(aggregate) > 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if byIndex := topLevelWordIndex(aggregate, "BY"); byIndex >= 0 {
		if splitSupported {
			context.Kinds = []SuggestionKind{SuggestionKindField}
		}
		return context
	}
	if asIndex := topLevelWordIndex(aggregate, "AS"); asIndex >= 0 {
		if len(aggregate) == asIndex+1 {
			context.Kinds = []SuggestionKind{SuggestionKindField}
		} else if splitSupported && len(aggregate) == asIndex+2 {
			context.Kinds = []SuggestionKind{SuggestionKindKeyword}
			context.Keywords = []string{"BY"}
		}
		return context
	}
	if len(aggregate) == 4 &&
		aggregate[1].kind == tokenLeftParen &&
		aggregate[2].kind == tokenWord &&
		aggregate[3].kind == tokenRightParen {
		context.Kinds = []SuggestionKind{SuggestionKindKeyword}
		context.Keywords = []string{"AS"}
		if splitSupported {
			context.Keywords = append(context.Keywords, "BY")
		}
	}
	return context
}

func classifyBinSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if endsOptionEqual(tokens, "span") {
		return context
	}
	state := analyzeBinSuggestionTokens(tokens)
	if state.invalid || state.destinationSeen {
		return context
	}
	if state.asSeen {
		if state.fieldSeen && state.spanSeen {
			context.Kinds = []SuggestionKind{SuggestionKindField}
		}
		return context
	}
	if !state.fieldSeen {
		context.Kinds = append(context.Kinds, SuggestionKindField)
	}
	if !state.spanSeen || state.fieldSeen && state.spanSeen {
		context.Kinds = append(context.Kinds, SuggestionKindKeyword)
		if !state.spanSeen {
			context.Keywords = append(context.Keywords, "span=")
		}
		if state.fieldSeen && state.spanSeen {
			context.Keywords = append(context.Keywords, "AS")
		}
	}
	return context
}

func classifyFrequencySuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if endsOptionEqual(tokens, "limit") {
		return context
	}
	if len(tokens) == 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField, SuggestionKindKeyword}
		context.Keywords = []string{"limit="}
		context.Exclusions = frequencySuggestionExclusions(frequencySuggestionState{})
		return context
	}
	state := analyzeFrequencySuggestionTokens(tokens)
	if !state.invalid && state.fieldOnly {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		context.Exclusions = frequencySuggestionExclusions(state)
	}
	return context
}

func classifyRexSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if endsOptionEqual(tokens, "field") {
		prior := analyzeRexSuggestionTokens(tokens[:len(tokens)-2])
		if !prior.invalid && !prior.patternSeen && !prior.fieldSeen {
			context.Kinds = []SuggestionKind{SuggestionKindField}
		}
		return context
	}
	if endsOptionEqual(tokens, "max_match") {
		return context
	}
	state := analyzeRexSuggestionTokens(tokens)
	if state.invalid {
		return context
	}
	context.Kinds = []SuggestionKind{SuggestionKindKeyword}
	if !state.patternSeen && !state.fieldSeen {
		context.Keywords = append(context.Keywords, "field=")
	}
	if !state.maxMatchSeen {
		context.Keywords = append(context.Keywords, "max_match=")
	}
	if len(context.Keywords) == 0 {
		context.Kinds = nil
	}
	return context
}

func classifySpathSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if endsOptionEqual(tokens, "input") || endsOptionEqual(tokens, "output") {
		option := asciiFold(tokens[len(tokens)-2].text)
		prior := analyzeSpathSuggestionTokens(tokens[:len(tokens)-2])
		if !prior.invalid &&
			(option == "input" && !prior.inputSeen ||
				option == "output" && !prior.outputSeen) {
			context.Kinds = []SuggestionKind{SuggestionKindField}
		}
		return context
	}
	if endsOptionEqual(tokens, "path") {
		return context
	}
	state := analyzeSpathSuggestionTokens(tokens)
	if state.invalid {
		return context
	}
	context.Kinds = []SuggestionKind{SuggestionKindKeyword}
	if !state.inputSeen {
		context.Keywords = append(context.Keywords, "input=")
	}
	if !state.outputSeen {
		context.Keywords = append(context.Keywords, "output=")
	}
	if !state.pathSeen {
		context.Keywords = append(context.Keywords, "path=")
	}
	if len(context.Keywords) == 0 {
		context.Kinds = nil
	}
	return context
}

type binSuggestionState struct {
	fieldSeen       bool
	spanSeen        bool
	asSeen          bool
	destinationSeen bool
	invalid         bool
}

func analyzeBinSuggestionTokens(tokens []token) binSuggestionState {
	var state binSuggestionState
	for index := 0; index < len(tokens); {
		current := tokens[index]
		if current.kind == tokenWord && tokenWordEqual(current, "span") &&
			index+1 < len(tokens) && tokens[index+1].kind == tokenEqual {
			if state.spanSeen || state.asSeen {
				state.invalid = true
				return state
			}
			state.spanSeen = true
			index += 2
			if index < len(tokens) && tokens[index].kind == tokenWord {
				index++
			}
			continue
		}
		if tokenWordEqual(current, "AS") {
			if state.asSeen || !state.fieldSeen || !state.spanSeen {
				state.invalid = true
				return state
			}
			state.asSeen = true
			index++
			if index < len(tokens) && tokens[index].kind == tokenWord {
				state.destinationSeen = true
				index++
			}
			if index != len(tokens) {
				state.invalid = true
			}
			return state
		}
		if current.kind != tokenWord || state.fieldSeen {
			state.invalid = true
			return state
		}
		state.fieldSeen = true
		index++
	}
	return state
}

type frequencySuggestionState struct {
	invalid    bool
	fieldOnly  bool
	fields     [MaximumFrequencyFields]string
	fieldCount int
}

func analyzeFrequencySuggestionTokens(tokens []token) frequencySuggestionState {
	if len(tokens) > 0 && tokens[0].kind == tokenWord && unsignedIntegerSyntax(tokens[0].text) {
		if !validFrequencySuggestionLimit(tokens[0]) {
			return frequencySuggestionState{invalid: true}
		}
		tokens = tokens[1:]
	} else if len(tokens) > 0 &&
		tokens[0].kind == tokenWord &&
		strings.HasPrefix(tokens[0].text, "-") &&
		integerSyntax(tokens[0].text) {
		return frequencySuggestionState{invalid: true}
	} else if len(tokens) >= 2 &&
		tokenWordEqual(tokens[0], "limit") &&
		tokens[1].kind == tokenEqual {
		if len(tokens) < 3 || !validFrequencySuggestionLimit(tokens[2]) {
			return frequencySuggestionState{invalid: true}
		}
		tokens = tokens[3:]
	}
	if len(tokens) == 0 {
		return frequencySuggestionState{fieldOnly: true}
	}

	wantField := true
	var state frequencySuggestionState
	for _, tok := range tokens {
		if wantField {
			if tok.kind != tokenWord ||
				tokenWordEqual(tok, "BY") ||
				strings.Contains(tok.text, "*") {
				return frequencySuggestionState{invalid: true}
			}
			for index := 0; index < state.fieldCount; index++ {
				if state.fields[index] == tok.text {
					return frequencySuggestionState{invalid: true}
				}
			}
			if state.fieldCount >= MaximumFrequencyFields {
				return frequencySuggestionState{invalid: true}
			}
			state.fields[state.fieldCount] = tok.text
			state.fieldCount++
			wantField = false
			continue
		}
		if tok.kind != tokenComma {
			return frequencySuggestionState{invalid: true}
		}
		wantField = true
	}
	state.fieldOnly = wantField && state.fieldCount > 0 &&
		state.fieldCount < MaximumFrequencyFields
	return state
}

func validFrequencySuggestionLimit(tok token) bool {
	if tok.kind != tokenWord || !unsignedIntegerSyntax(tok.text) {
		return false
	}
	_, err := strconv.ParseUint(tok.text, 10, 64)
	return err == nil
}

func frequencySuggestionExclusions(state frequencySuggestionState) []SuggestionExclusion {
	exclusions := make([]SuggestionExclusion, 0, state.fieldCount+3)
	for index := 0; index < state.fieldCount; index++ {
		exclusions = append(exclusions, SuggestionExclusion{
			Kind:  SuggestionKindField,
			Label: state.fields[index],
		})
	}
	return append(exclusions,
		SuggestionExclusion{Kind: SuggestionKindField, Label: "count"},
		SuggestionExclusion{Kind: SuggestionKindField, Label: "percent"},
		SuggestionExclusion{Kind: SuggestionKindField, Label: "BY", ASCIIFold: true},
	)
}

const frequencySuggestionFixedExclusions = 3

func addFrequencySuggestionSuffixExclusions(
	source string,
	scan suggestionCursorScan,
	context SuggestionContext,
) SuggestionContext {
	command := suggestionStageCommand(scan.tokens)
	if (command != "top" && command != "rare") ||
		!context.Allows(SuggestionKindField) ||
		len(context.Exclusions) < frequencySuggestionFixedExclusions {
		return context
	}

	// The active candidate itself consumes one tuple slot. Prefix analysis has
	// already admitted at most MaximumFrequencyFields-1 committed fields, and
	// every frequency context ends with the three fixed output/keyword
	// exclusions, so suffix work and the returned exclusion list stay within the
	// same 16-field grammar boundary.
	committedFields := len(context.Exclusions) - frequencySuggestionFixedExclusions
	maximumSuffixFields := MaximumFrequencyFields - committedFields - 1
	if maximumSuffixFields <= 0 {
		return context
	}
	for _, field := range frequencySuggestionSuffixFields(
		source,
		context.Replacement.End,
		scan.activeWord,
		maximumSuffixFields,
	) {
		context.Exclusions = append(context.Exclusions, SuggestionExclusion{
			Kind:  SuggestionKindField,
			Label: field,
		})
	}
	return context
}

// frequencySuggestionSuffixFields tolerantly recognizes only the bounded
// comma-separated exact fields in the remainder of the active top/rare stage.
// An active word fills the current field slot, so its suffix must begin with a
// comma. With an empty slot, the suffix may begin with a field. Lexer or grammar
// errors simply stop collection: full validation owns diagnostics, and a pipe
// always ends this local scan before an unrelated malformed stage.
func frequencySuggestionSuffixFields(
	source string,
	start Position,
	activeWord bool,
	maximumFields int,
) []string {
	if maximumFields <= 0 || start.Offset < 0 || start.Offset > len(source) {
		return nil
	}
	suffix := lexer{
		source: source,
		offset: start.Offset,
		line:   start.Line,
		column: start.Column,
	}
	fields := make([]string, 0, maximumFields)
	wantField := !activeWord
	pendingField := ""
	maximumTokens := maximumFields*2 + 1
	for range maximumTokens {
		tok, err := suffix.next()
		if err != nil {
			return fields
		}
		if tok.kind == tokenPipe || tok.kind == tokenEOF {
			if pendingField != "" {
				fields = append(fields, pendingField)
			}
			return fields
		}
		if wantField {
			if tok.kind != tokenWord ||
				tokenWordEqual(tok, "BY") ||
				strings.Contains(tok.text, "*") {
				return fields
			}
			pendingField = tok.text
			wantField = false
			continue
		}
		if tok.kind != tokenComma {
			return fields
		}
		if pendingField != "" {
			fields = append(fields, pendingField)
			pendingField = ""
			if len(fields) == maximumFields {
				return fields
			}
		}
		wantField = true
	}
	return fields
}

type rexSuggestionState struct {
	fieldSeen    bool
	maxMatchSeen bool
	patternSeen  bool
	invalid      bool
}

func analyzeRexSuggestionTokens(tokens []token) rexSuggestionState {
	var state rexSuggestionState
	for index := 0; index < len(tokens); {
		current := tokens[index]
		if current.kind == tokenString {
			if state.patternSeen {
				state.invalid = true
				return state
			}
			state.patternSeen = true
			index++
			continue
		}
		if current.kind != tokenWord || index+2 >= len(tokens) ||
			tokens[index+1].kind != tokenEqual ||
			tokens[index+2].kind != tokenWord {
			state.invalid = true
			return state
		}
		switch asciiFold(current.text) {
		case "field":
			if state.patternSeen || state.fieldSeen {
				state.invalid = true
				return state
			}
			state.fieldSeen = true
		case "max_match":
			if state.maxMatchSeen {
				state.invalid = true
				return state
			}
			state.maxMatchSeen = true
		default:
			state.invalid = true
			return state
		}
		index += 3
	}
	return state
}

type spathSuggestionState struct {
	inputSeen  bool
	outputSeen bool
	pathSeen   bool
	invalid    bool
}

func analyzeSpathSuggestionTokens(tokens []token) spathSuggestionState {
	var state spathSuggestionState
	for index := 0; index < len(tokens); {
		current := tokens[index]
		if current.kind == tokenWord && index+1 < len(tokens) &&
			tokens[index+1].kind == tokenEqual {
			if index+2 >= len(tokens) ||
				tokens[index+2].kind != tokenWord && tokens[index+2].kind != tokenString {
				state.invalid = true
				return state
			}
			switch asciiFold(current.text) {
			case "input":
				if state.inputSeen {
					state.invalid = true
					return state
				}
				state.inputSeen = true
			case "output":
				if state.outputSeen {
					state.invalid = true
					return state
				}
				state.outputSeen = true
			case "path":
				if state.pathSeen {
					state.invalid = true
					return state
				}
				state.pathSeen = true
			default:
				state.invalid = true
				return state
			}
			index += 3
			continue
		}
		if state.pathSeen ||
			current.kind != tokenWord && current.kind != tokenString {
			state.invalid = true
			return state
		}
		state.pathSeen = true
		index++
	}
	return state
}

func scalarSuggestionContext(context SuggestionContext, includeNot bool) SuggestionContext {
	context.Kinds = []SuggestionKind{SuggestionKindFunction, SuggestionKindField}
	context.FunctionClass = SuggestionFunctionClassScalar
	if includeNot {
		context.Kinds = append(context.Kinds, SuggestionKindKeyword)
		context.Keywords = []string{"NOT"}
	}
	return context
}

func aggregateSuggestionContext(context SuggestionContext) SuggestionContext {
	context.Kinds = []SuggestionKind{SuggestionKindFunction}
	context.FunctionClass = SuggestionFunctionClassAggregate
	return context
}

func tokensAfterLastTopLevelComma(tokens []token) []token {
	depth := 0
	start := 0
	for index, tok := range tokens {
		switch tok.kind {
		case tokenLeftParen:
			depth++
		case tokenRightParen:
			if depth > 0 {
				depth--
			}
		case tokenComma:
			if depth == 0 {
				start = index + 1
			}
		}
	}
	return tokens[start:]
}

func topLevelTokenIndex(tokens []token, want tokenKind) int {
	depth := 0
	for index, tok := range tokens {
		if tok.kind == want && depth == 0 {
			return index
		}
		switch tok.kind {
		case tokenLeftParen:
			depth++
		case tokenRightParen:
			if depth > 0 {
				depth--
			}
		}
	}
	return -1
}

func topLevelWordIndex(tokens []token, want string) int {
	depth := 0
	for index, tok := range tokens {
		if depth == 0 && tokenWordEqual(tok, want) {
			return index
		}
		switch tok.kind {
		case tokenLeftParen:
			depth++
		case tokenRightParen:
			if depth > 0 {
				depth--
			}
		}
	}
	return -1
}

func parenthesisDepth(tokens []token) int {
	depth := 0
	for _, tok := range tokens {
		switch tok.kind {
		case tokenLeftParen:
			depth++
		case tokenRightParen:
			if depth > 0 {
				depth--
			}
		}
	}
	return depth
}

func classifyActiveCountEvalPredicateSuggestion(
	context SuggestionContext,
	tokens []token,
) (SuggestionContext, bool) {
	start := activeCountEvalPredicateStart(tokens)
	if start < 0 {
		return context, false
	}
	context.AllowsQuotedScalarFields = true
	predicate := tokens[start:]
	if len(predicate) == 0 {
		return scalarSuggestionContext(context, false), true
	}
	return classifyWhereSuggestion(context, predicate), true
}

func activeCountEvalPredicateStart(tokens []token) int {
	activeStart := -1
	for index := 0; index+3 < len(tokens); index++ {
		if !startsCountEvalCall(tokens, index) {
			continue
		}
		depth := 1
		closed := false
		for _, tok := range tokens[index+4:] {
			switch tok.kind {
			case tokenLeftParen:
				depth++
			case tokenRightParen:
				depth--
				if depth == 0 {
					closed = true
				}
			}
			if closed {
				break
			}
		}
		if !closed {
			activeStart = index + 4
		}
	}
	return activeStart
}

func classifyActiveStatsScalarEvalInputSuggestion(
	context SuggestionContext,
	tokens []token,
) (SuggestionContext, bool) {
	start := activeStatsScalarEvalInputStart(tokens)
	if start < 0 {
		return context, false
	}
	context.AllowsQuotedScalarFields = true
	expression := tokens[start:]
	if len(expression) == 0 {
		return scalarSuggestionContext(context, false), true
	}
	last := expression[len(expression)-1]
	if isScalarOperandBoundaryToken(last.kind) ||
		last.kind == tokenLeftParen ||
		(last.kind == tokenWord &&
			(tokenWordEqual(last, "AND") || tokenWordEqual(last, "OR") ||
				tokenWordEqual(last, "NOT"))) {
		return scalarSuggestionContext(context, false), true
	}
	return context, true
}

func activeStatsScalarEvalInputStart(tokens []token) int {
	activeStart := -1
	for index := 0; index+3 < len(tokens); index++ {
		if tokens[index].kind != tokenWord {
			continue
		}
		spec, supported := statsAggregateSpecForName(tokens[index].text)
		if !supported || !spec.supportsExpressionInput ||
			!startsEvalPredicateArgument(tokens, index+1) {
			continue
		}
		depth := 1
		closed := false
		for _, tok := range tokens[index+4:] {
			switch tok.kind {
			case tokenLeftParen:
				depth++
			case tokenRightParen:
				depth--
				if depth == 0 {
					closed = true
				}
			}
			if closed {
				break
			}
		}
		if !closed {
			activeStart = index + 4
		}
	}
	return activeStart
}

func scalarSuggestionTokenStart(tokens []token) (int, bool) {
	stage, stageStart, followsPipeline := activeSuggestionStage(tokens)
	if !followsPipeline || len(stage) == 0 || stage[0].kind != tokenWord {
		return 0, false
	}
	switch asciiFold(stage[0].text) {
	case "eval", "where":
		return stageStart + 1, true
	case "stats":
		bodyStart := stageStart + 1
		predicateStart := activeCountEvalPredicateStart(stage[1:])
		if predicateStart >= 0 {
			return bodyStart + predicateStart, true
		}
		scalarStart := activeStatsScalarEvalInputStart(stage[1:])
		if scalarStart >= 0 {
			return bodyStart + scalarStart, true
		}
	case "eventstats", "streamstats":
		bodyStart := stageStart + 1
		predicateStart := activeCountEvalPredicateStart(stage[1:])
		if predicateStart >= 0 {
			return bodyStart + predicateStart, true
		}
	}
	return 0, false
}

func endsOptionEqual(tokens []token, name string) bool {
	return len(tokens) >= 2 &&
		tokens[len(tokens)-1].kind == tokenEqual &&
		tokenWordEqual(tokens[len(tokens)-2], name)
}

func tokenWordEqual(tok token, want string) bool {
	return tok.kind == tokenWord && asciiFold(tok.text) == asciiFold(want)
}

func isComparisonToken(kind tokenKind) bool {
	switch kind {
	case tokenEqual, tokenNotEqual, tokenLess, tokenLessEqual, tokenGreater, tokenGreaterEqual:
		return true
	default:
		return false
	}
}

func isScalarOperandBoundaryToken(kind tokenKind) bool {
	if _, comparison := evalComparisonOperator(kind, expressionProfileV02); comparison {
		return true
	}
	return kind == tokenComma ||
		kind == tokenConcat ||
		kind == tokenPlus ||
		kind == tokenMinus ||
		kind == tokenMultiply ||
		kind == tokenDivide ||
		kind == tokenRemainder
}

func startsPredicateOperand(tok token) bool {
	return tok.kind == tokenLeftParen ||
		(tok.kind == tokenWord &&
			(tokenWordEqual(tok, "AND") ||
				tokenWordEqual(tok, "OR") ||
				tokenWordEqual(tok, "NOT")))
}

func activeSuggestionStage(tokens []token) (stage []token, start int, followsPipeline bool) {
	lastPipe := -1
	for index, tok := range tokens {
		if tok.kind == tokenPipe {
			lastPipe = index
		}
	}
	start = lastPipe + 1
	return tokens[start:], start, lastPipe >= 0
}

func suggestionStageCommand(tokens []token) string {
	stage, _, followsPipeline := activeSuggestionStage(tokens)
	if !followsPipeline || len(stage) == 0 || stage[0].kind != tokenWord {
		return ""
	}
	return asciiFold(stage[0].text)
}

func removeLiteralSuggestionKinds(kinds []SuggestionKind) []SuggestionKind {
	filtered := make([]SuggestionKind, 0, len(kinds))
	for _, kind := range kinds {
		if kind == SuggestionKindIndex {
			filtered = append(filtered, kind)
		}
	}
	return filtered
}

// RankSuggestionCandidates applies kind-aware prefix matching, deterministic
// ordering, deduplication, a fixed relevance tier, and the hard result bound.
func RankSuggestionCandidates(context SuggestionContext, candidates []SuggestionCandidate, limit int) []Suggestion {
	if limit <= 0 {
		limit = DefaultSuggestionLimit
	}
	if limit > MaximumSuggestionLimit {
		limit = MaximumSuggestionLimit
	}
	type rankedSuggestion struct {
		suggestion Suggestion
		exact      bool
		kindRank   int
	}
	ranked := make([]rankedSuggestion, 0, len(candidates))
	for _, candidate := range candidates {
		kindRank := suggestionKindRank(context, candidate.Kind)
		if kindRank < 0 || candidate.Label == "" || candidate.Insertion == "" {
			continue
		}
		if suggestionCandidateExcluded(context, candidate) {
			continue
		}
		if candidate.Kind == SuggestionKindFunction {
			if context.FunctionClass != "" &&
				candidate.FunctionClass != "" &&
				context.FunctionClass != candidate.FunctionClass {
				continue
			}
			if len(context.FunctionNames) > 0 &&
				!containsASCIIFold(context.FunctionNames, candidate.Label) {
				continue
			}
		}
		if candidate.Kind == SuggestionKindKeyword &&
			(len(context.Keywords) == 0 ||
				!containsASCIIFold(context.Keywords, candidate.Label)) {
			continue
		}
		matched, exact := suggestionPrefixMatch(candidate.Kind, candidate.Label, context.Prefix)
		if !matched {
			continue
		}
		relevance := 0.75
		if context.Prefix == "" {
			relevance = 0.5
		} else if exact {
			relevance = 1
		}
		ranked = append(ranked, rankedSuggestion{
			suggestion: Suggestion{
				SuggestionCandidate: candidate,
				Replacement:         context.Replacement,
				Relevance:           relevance,
			},
			exact:    exact,
			kindRank: kindRank,
		})
	}
	sort.Slice(ranked, func(left, right int) bool {
		a := ranked[left]
		b := ranked[right]
		if a.exact != b.exact {
			return a.exact
		}
		if a.kindRank != b.kindRank {
			return a.kindRank < b.kindRank
		}
		if a.suggestion.Priority != b.suggestion.Priority {
			return a.suggestion.Priority > b.suggestion.Priority
		}
		aFolded := asciiFold(a.suggestion.Label)
		bFolded := asciiFold(b.suggestion.Label)
		if aFolded != bFolded {
			return aFolded < bFolded
		}
		if a.suggestion.Label != b.suggestion.Label {
			return a.suggestion.Label < b.suggestion.Label
		}
		if a.suggestion.Insertion != b.suggestion.Insertion {
			return a.suggestion.Insertion < b.suggestion.Insertion
		}
		if a.suggestion.Detail != b.suggestion.Detail {
			return a.suggestion.Detail < b.suggestion.Detail
		}
		return a.suggestion.FunctionClass < b.suggestion.FunctionClass
	})

	suggestions := make([]Suggestion, 0, min(limit, len(ranked)))
	seen := make(map[string]struct{}, len(ranked))
	for _, candidate := range ranked {
		key := suggestionDeduplicationKey(candidate.suggestion)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		suggestions = append(suggestions, candidate.suggestion)
		if len(suggestions) == limit {
			break
		}
	}
	return suggestions
}

func suggestionKindRank(context SuggestionContext, kind SuggestionKind) int {
	for index, allowed := range context.Kinds {
		if allowed == kind {
			return index
		}
	}
	return -1
}

func suggestionCandidateExcluded(context SuggestionContext, candidate SuggestionCandidate) bool {
	for _, exclusion := range context.Exclusions {
		if exclusion.Kind != candidate.Kind {
			continue
		}
		if exclusion.ASCIIFold {
			if equalASCIIFold(exclusion.Label, candidate.Label) {
				return true
			}
			continue
		}
		if exclusion.Label == candidate.Label {
			return true
		}
	}
	return false
}

func equalASCIIFold(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range len(left) {
		leftByte := left[index]
		if leftByte >= 'A' && leftByte <= 'Z' {
			leftByte += 'a' - 'A'
		}
		rightByte := right[index]
		if rightByte >= 'A' && rightByte <= 'Z' {
			rightByte += 'a' - 'A'
		}
		if leftByte != rightByte {
			return false
		}
	}
	return true
}

func suggestionPrefixMatch(kind SuggestionKind, label, prefix string) (bool, bool) {
	if kind == SuggestionKindField {
		return strings.HasPrefix(label, prefix), label == prefix
	}
	foldedLabel := asciiFold(label)
	foldedPrefix := asciiFold(prefix)
	return strings.HasPrefix(foldedLabel, foldedPrefix), foldedLabel == foldedPrefix
}

func suggestionDeduplicationKey(suggestion Suggestion) string {
	label := suggestion.Label
	if suggestion.Kind != SuggestionKindField {
		label = asciiFold(label)
	}
	return string(suggestion.Kind) + "\x00" + label
}

func asciiFold(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'A' && character <= 'Z' {
			character += 'a' - 'A'
		}
		builder.WriteByte(character)
	}
	return builder.String()
}

func firstInvalidUTF8Offset(source string) int {
	for offset := 0; offset < len(source); {
		r, width := utf8.DecodeRuneInString(source[offset:])
		if r == utf8.RuneError && width == 1 {
			return offset
		}
		offset += width
	}
	return -1
}

func suggestionDiagnosticAt(source, code, message string, startOffset, endOffset int) *Diagnostic {
	return &Diagnostic{
		Code:    code,
		Message: message,
		Range: Range{
			Start: sourcePositionAtOffset(source, startOffset),
			End:   sourcePositionAtOffset(source, endOffset),
		},
	}
}
