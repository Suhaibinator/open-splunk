package spl

import (
	"errors"
	"fmt"
	"sort"
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
	Prefix        string
	Replacement   Range
	// PipelinePrefixEnd is the byte offset immediately before the real pipe
	// that introduces the active stage. source[:PipelinePrefixEnd] is the
	// preceding pipeline, and the value is zero for the base-search stage.
	PipelinePrefixEnd int
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
	return context, nil
}

type suggestionCursorScan struct {
	tokens      []token
	prefix      string
	replacement Range
	activeWord  bool
	blocked     bool
}

func scanSuggestionCursor(source string, cursor int) (suggestionCursorScan, *Diagnostic) {
	position := sourcePositionAtOffset(source, cursor)
	scan := suggestionCursorScan{
		replacement: Range{Start: position, End: position},
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
			if tok.kind == tokenWord && cursor >= start {
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
		if cursor == end && tok.kind == tokenWord {
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

func tooManySuggestionTokens(sourceRange Range) *Diagnostic {
	return &Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf("search contains more than %d syntax tokens", maxSPLTokens),
		Range:   sourceRange,
	}
}

func classifySuggestionContext(tokens []token, prefix string, replacement Range) SuggestionContext {
	base := SuggestionContext{Prefix: prefix, Replacement: replacement}
	lastPipe := -1
	for index, tok := range tokens {
		if tok.kind == tokenPipe {
			lastPipe = index
		}
	}
	if lastPipe < 0 {
		return classifySearchSuggestion(base, tokens)
	}
	base.PipelinePrefixEnd = tokens[lastPipe].sourceRange.Start.Offset

	stage := tokens[lastPipe+1:]
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
		return classifyWhereSuggestion(base, body)
	case "eval":
		return classifyEvalSuggestion(base, body)
	case "fields", "table", "sort", "dedup":
		base.Kinds = []SuggestionKind{SuggestionKindField}
		return base
	case "rename":
		return classifyRenameSuggestion(base, body)
	case "stats":
		return classifyStatsSuggestion(base, body)
	case "eventstats":
		return classifyEventStatsSuggestion(base, body)
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
	case "head", "tail":
		return base
	default:
		return base
	}
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
	if isComparisonToken(last.kind) || last.kind == tokenComma || last.kind == tokenConcat {
		return scalarSuggestionContext(context, false)
	}
	if last.kind == tokenLeftParen {
		includeNot := len(tokens) < 2 || tokens[len(tokens)-2].kind != tokenWord
		return scalarSuggestionContext(context, includeNot)
	}
	if last.kind == tokenWord &&
		(tokenWordEqual(last, "AND") || tokenWordEqual(last, "OR") || tokenWordEqual(last, "NOT")) {
		return scalarSuggestionContext(context, true)
	}
	if last.kind == tokenWord || last.kind == tokenString || last.kind == tokenRightParen {
		context.Kinds = []SuggestionKind{SuggestionKindKeyword}
		context.Keywords = []string{"AND", "OR"}
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
	if isComparisonToken(last.kind) ||
		last.kind == tokenComma ||
		last.kind == tokenLeftParen ||
		last.kind == tokenConcat ||
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
	if index := topLevelWordIndex(tokens, "BY"); index >= 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if parenthesisDepth(tokens) > 0 {
		if insideStatsEval(tokens) {
			return scalarSuggestionContext(context, false)
		}
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if len(tokens) == 0 {
		return aggregateSuggestionContext(context)
	}
	last := tokens[len(tokens)-1]
	if tokenWordEqual(last, "AS") {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if last.kind == tokenLeftParen {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	context = aggregateSuggestionContext(context)
	context.Kinds = append(context.Kinds, SuggestionKindKeyword)
	context.Keywords = []string{"AS", "BY"}
	return context
}

func classifyEventStatsSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	if topLevelWordIndex(tokens, "BY") >= 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if parenthesisDepth(tokens) > 0 {
		if insideEventStatsCountEval(tokens) {
			return scalarSuggestionContext(context, false)
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

func insideEventStatsCountEval(tokens []token) bool {
	return len(tokens) >= 4 &&
		tokenWordEqual(tokens[0], "count") &&
		tokens[1].kind == tokenLeftParen &&
		tokenWordEqual(tokens[2], "eval") &&
		tokens[3].kind == tokenLeftParen &&
		insideStatsEval(tokens)
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
		context.FunctionNames = []string{"count"}
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
	if _, supported := timechartFieldAggregateSpecForName(aggregate[0].text); !supported {
		return context
	}
	if parenthesisDepth(aggregate) > 0 {
		context.Kinds = []SuggestionKind{SuggestionKindField}
		return context
	}
	if asIndex := topLevelWordIndex(aggregate, "AS"); asIndex >= 0 {
		if len(aggregate) == asIndex+1 {
			context.Kinds = []SuggestionKind{SuggestionKindField}
		}
		return context
	}
	if len(aggregate) == 4 &&
		aggregate[1].kind == tokenLeftParen &&
		aggregate[2].kind == tokenWord &&
		aggregate[3].kind == tokenRightParen {
		context.Kinds = []SuggestionKind{SuggestionKindKeyword}
		context.Keywords = []string{"AS"}
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
		return context
	}
	if frequencyLimitIsComplete(tokens) {
		context.Kinds = []SuggestionKind{SuggestionKindField}
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

func frequencyLimitIsComplete(tokens []token) bool {
	if len(tokens) == 1 {
		return tokens[0].kind == tokenWord && unsignedIntegerSyntax(tokens[0].text)
	}
	return len(tokens) == 3 &&
		tokenWordEqual(tokens[0], "limit") &&
		tokens[1].kind == tokenEqual &&
		tokens[2].kind == tokenWord &&
		unsignedIntegerSyntax(tokens[2].text)
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

func insideStatsEval(tokens []token) bool {
	depth := 0
	evalDepth := -1
	for index, tok := range tokens {
		switch tok.kind {
		case tokenLeftParen:
			depth++
			if index > 0 && tokenWordEqual(tokens[index-1], "eval") {
				evalDepth = depth
			}
		case tokenRightParen:
			if evalDepth == depth {
				evalDepth = -1
			}
			if depth > 0 {
				depth--
			}
		}
	}
	return evalDepth > 0
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

func suggestionStageCommand(tokens []token) string {
	lastPipe := -1
	for index, tok := range tokens {
		if tok.kind == tokenPipe {
			lastPipe = index
		}
	}
	if lastPipe < 0 || lastPipe+1 >= len(tokens) || tokens[lastPipe+1].kind != tokenWord {
		return ""
	}
	return asciiFold(tokens[lastPipe+1].text)
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
