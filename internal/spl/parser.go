package spl

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"fortio.org/safecast"
)

const (
	// MaximumEvalPredicates is the shared authored eval/where predicate-leaf
	// ceiling retained as sealed compiler evidence for knowledge admission.
	MaximumEvalPredicates = 32
	// MaximumSearchSourceBytes is the exact authored-search ceiling. Logical
	// knowledge contracts that promise an authored spelling use this same
	// authority rather than mirroring a numeric limit in another package.
	MaximumSearchSourceBytes = 16 << 10

	maxSPLSourceBytes     = MaximumSearchSourceBytes
	maxSPLTokens          = 1024
	maxPipelineCommands   = 64
	maxEvalAssignments    = 64
	maxRenameAssignments  = 64
	maxDedupFields        = 16
	maxEvalPredicates     = MaximumEvalPredicates
	maxScalarNestingDepth = 32
)

// expressionProfile is deliberately closed and internal. Authored searches use
// the full scalar grammar while reusable knowledge expressions retain their
// smaller grammar; callers cannot accidentally construct a hybrid profile.
type expressionProfile uint8

const (
	expressionProfileInvalid expressionProfile = iota
	expressionProfileKnowledge
	expressionProfileAuthored
)

// Parse parses the supported authored SPL grammar. Unsupported commands and
// syntax are rejected; a valid prefix is never returned as a partial query.
func Parse(source string) (*Query, error) {
	if len(source) > maxSPLSourceBytes {
		start := sourcePositionAtOffset(source, maxSPLSourceBytes)
		end := sourcePositionAtOffset(source, maxSPLSourceBytes+1)
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search source exceeds %d UTF-8 bytes", maxSPLSourceBytes),
			Range:   Range{Start: start, End: end},
		}
	}
	tokens, err := lex(source)
	if err != nil {
		return nil, err
	}
	// Bound syntax before constructing recursive ASTs or nested SQL. The server
	// also caps source bytes, but a short token stream can still create deeply
	// nested expressions and quadratic compiler work.
	if len(tokens)-1 > maxSPLTokens { // exclude EOF
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d syntax tokens", maxSPLTokens),
			Range:   tokens[maxSPLTokens].sourceRange,
		}
	}
	p := parser{source: source, tokens: tokens, profile: expressionProfileAuthored}
	return p.parseQuery()
}

func sourcePositionAtOffset(source string, offset int) Position {
	if offset > len(source) {
		offset = len(source)
	}
	position := Position{Line: 1, Column: 1}
	for position.Offset < offset {
		r, width := utf8.DecodeRuneInString(source[position.Offset:])
		if r == utf8.RuneError && width == 1 {
			width = 1
		}
		if position.Offset+width > offset {
			position.Offset = offset
			return position
		}
		position.Offset += width
		if r == '\n' {
			position.Line++
			position.Column = 1
		} else {
			position.Column++
		}
	}
	return position
}

type parser struct {
	source                string
	tokens                []token
	index                 int
	profile               expressionProfile
	scalarDepth           int
	unaryDepth            int
	evalPredicates        int
	concatenationOperands int
	arithmeticOperators   int
	membershipCandidates  int
	matchProgramWorkUnits int
	preserveSignedLiteral int
}

func (p *parser) parseQuery() (*Query, error) {
	start := p.current().sourceRange.Start
	query := &Query{}

	if p.current().kind != tokenPipe && p.current().kind != tokenEOF {
		expression, err := p.parseSearchExpression()
		if err != nil {
			return nil, err
		}
		query.Search = expression
	}
	query.parsedEvalPredicatePrefixes = append(
		query.parsedEvalPredicatePrefixes,
		safecast.MustConv[uint32](p.evalPredicates),
	)

	stage := 0
	for p.match(tokenPipe) {
		stage++
		if stage > maxPipelineCommands {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("search contains more than %d pipeline commands", maxPipelineCommands),
				Range:   p.current().sourceRange,
			}
		}
		command, err := p.parseCommand(stage)
		if err != nil {
			return nil, err
		}
		query.Commands = append(query.Commands, command)
		query.parsedEvalPredicatePrefixes = append(
			query.parsedEvalPredicatePrefixes,
			safecast.MustConv[uint32](p.evalPredicates),
		)
	}
	if p.current().kind != tokenEOF {
		return nil, p.errorAtCurrent("SPL_UNEXPECTED_TOKEN", fmt.Sprintf("unexpected token %q", p.current().text))
	}
	if query.Search == nil && len(query.Commands) == 0 {
		return nil, p.errorAtCurrent("SPL_EMPTY_QUERY", "search query is empty")
	}
	query.Range = Range{Start: start, End: p.current().sourceRange.End}

	query.parsedEvalPredicates = safecast.MustConv[uint32](p.evalPredicates)
	query.sourceDigest = sha256.Sum256([]byte(p.source))
	query.parsedSource = strings.Clone(p.source)
	query.parsed = true
	return query, nil
}

func (p *parser) parseCommand(stage int) (Command, error) {
	nameToken := p.current()
	if nameToken.kind == tokenEOF || nameToken.kind == tokenPipe {
		return nil, p.errorAtCurrent("SPL_EXPECTED_COMMAND", "expected a command after '|'")
	}
	if nameToken.kind != tokenWord {
		return nil, p.errorAtCurrent("SPL_EXPECTED_COMMAND", "expected a command name after '|'")
	}
	p.advance()
	name := strings.ToLower(nameToken.text)
	if name != "search" && name != "where" && name != "eval" &&
		name != "stats" && name != "eventstats" && name != "streamstats" && name != "sort" {
		if err := p.expandLegacyScalarCompositesUntilCommandEnd(); err != nil {
			return nil, err
		}
	}
	switch name {
	case "search":
		return p.parseSearchCommand(nameToken)
	case "where":
		return p.parseWhereCommand(nameToken)
	case "eval":
		return p.parseEvalCommand(nameToken)
	case "rex":
		return p.parseRexCommand(nameToken)
	case "regex":
		return p.parseRegexCommand(nameToken)
	case "reverse":
		return p.parseArgumentFreeCommand(nameToken, "reverse", func(sourceRange Range) Command {
			return &ReverseCommand{Range: sourceRange}
		})
	case "accum":
		return p.parseAccumCommand(nameToken)
	case "strcat":
		return p.parseStrcatCommand(nameToken)
	case "addinfo":
		return p.parseArgumentFreeCommand(nameToken, "addinfo", func(sourceRange Range) Command {
			return &AddInfoCommand{Range: sourceRange}
		})
	case "fillnull":
		return p.parseFillNullCommand(nameToken)
	case "addtotals":
		return p.parseAddTotalsCommand(nameToken)
	case "delta":
		return p.parseDeltaCommand(nameToken)
	case "makemv":
		return p.parseMakeMVCommand(nameToken)
	case "mvexpand":
		return p.parseMVExpandCommand(nameToken)
	case "nomv":
		return p.parseNoMVCommand(nameToken)
	case "spath":
		return p.parseSpathCommand(nameToken)
	case "lookup":
		return p.parseLookupCommand(nameToken)
	case "rename":
		return p.parseRenameCommand(nameToken)
	case "fields":
		return p.parseFieldsCommand(nameToken)
	case "table":
		return p.parseTableCommand(nameToken)
	case "sort":
		return p.parseSortCommand(nameToken)
	case "dedup":
		return p.parseDedupCommand(nameToken)
	case "head", "tail":
		return p.parseLimitCommand(name, nameToken)
	case "stats":
		return p.parseStatsCommand(nameToken)
	case "eventstats":
		return p.parseEventStatsCommand(nameToken)
	case "streamstats":
		return p.parseStreamStatsCommand(nameToken)
	case "top":
		return p.parseTopCommand(nameToken)
	case "rare":
		return p.parseRareCommand(nameToken)
	case "bin", "bucket":
		return p.parseBinCommand(nameToken)
	case "timechart":
		return p.parseTimechartCommand(nameToken)
	case "chart":
		return p.parseChartCommand(nameToken)
	default:
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_COMMAND",
			Message: fmt.Sprintf("unsupported command %q at pipeline stage %d", nameToken.text, stage),
			Range:   nameToken.sourceRange,
		}
	}
}

// parseSearchExpression implements Splunk base-search precedence:
// parentheses, NOT, OR, AND. Adjacent operands imply AND.
func (p *parser) parseSearchExpression() (Expr, error) {
	return p.parseSearchAnd()
}

// parseWhereExpression implements expression-language precedence:
// parentheses, NOT, AND, OR. Unlike search, adjacent operands do not imply
// AND and a primary must be a scalar-to-scalar comparison.
func (p *parser) parseWhereExpression() (WhereExpr, error) {
	expression, err := p.parseWhereOr()
	if err != nil {
		return nil, err
	}
	if p.profile == expressionProfileAuthored && whereExpressionContainsMembership(expression) {
		if _, comparison := evalComparisonOperator(p.current().kind, p.profile); comparison {
			return nil, p.membershipSyntaxError(
				p.current().sourceRange,
				"membership is already a Boolean predicate and cannot be compared explicitly",
			)
		}
	}
	return expression, nil
}

func whereExpressionContainsMembership(expression WhereExpr) bool {
	switch expression := expression.(type) {
	case *WhereMembershipExpr:
		return expression != nil
	case *WhereBoolExpr:
		return expression != nil &&
			(whereExpressionContainsMembership(expression.Left) ||
				whereExpressionContainsMembership(expression.Right))
	case *WhereNotExpr:
		return expression != nil && whereExpressionContainsMembership(expression.Operand)
	default:
		return false
	}
}

func (p *parser) parseWhereOr() (WhereExpr, error) {
	left, err := p.parseWhereAnd()
	if err != nil {
		return nil, err
	}
	for {
		if err := p.prepareScalarToken(); err != nil {
			return nil, err
		}
		if !p.isKeyword("OR") {
			return left, nil
		}
		p.advance()
		if !p.canStartWhereOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after OR")
		}
		right, parseErr := p.parseWhereAnd()
		if parseErr != nil {
			return nil, parseErr
		}
		left = &WhereBoolExpr{Op: BoolOpOr, Left: left, Right: right, Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End}}
	}
}

func (p *parser) parseWhereAnd() (WhereExpr, error) {
	left, err := p.parseWhereUnary()
	if err != nil {
		return nil, err
	}
	for {
		if err := p.prepareScalarToken(); err != nil {
			return nil, err
		}
		if !p.isKeyword("AND") {
			return left, nil
		}
		p.advance()
		if !p.canStartWhereOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after AND")
		}
		right, parseErr := p.parseWhereUnary()
		if parseErr != nil {
			return nil, parseErr
		}
		left = &WhereBoolExpr{Op: BoolOpAnd, Left: left, Right: right, Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End}}
	}
}

func (p *parser) parseWhereUnary() (WhereExpr, error) {
	if err := p.prepareScalarToken(); err != nil {
		return nil, err
	}
	if p.isKeyword("NOT") {
		start := p.current().sourceRange.Start
		p.advance()
		if !p.canStartWhereOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after NOT")
		}
		operand, err := p.parseWhereUnary()
		if err != nil {
			return nil, err
		}
		return &WhereNotExpr{Operand: operand, Range: Range{Start: start, End: operand.SourceRange().End}}, nil
	}
	return p.parseWherePrimary()
}

func (p *parser) parseWherePrimary() (WhereExpr, error) {
	if p.profile == expressionProfileAuthored && p.isKeyword("IN") && p.nextIs(tokenLeftParen) {
		return p.parseWhereMembershipFunction()
	}

	if p.current().kind == tokenLeftParen && p.profile == expressionProfileAuthored {
		// Parse the scalar interpretation on an isolated parser snapshot. This
		// resolves grouped scalar/Boolean syntax from grammar alone while
		// preserving lazy token splits and all complexity counters atomically.
		trial := *p
		trial.tokens = append([]token(nil), p.tokens...)
		left, scalarErr := trial.parseScalarExpression()
		if scalarErr == nil {
			*p = trial
			return p.parseWherePredicateAfterScalar(left)
		}
	}

	if p.match(tokenLeftParen) {
		start := p.previous().sourceRange.Start
		if p.current().kind == tokenRightParen {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "empty parenthesized where expression")
		}
		expression, err := p.parseWhereExpression()
		if err != nil {
			return nil, err
		}
		if !p.match(tokenRightParen) {
			return nil, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' to close where expression")
		}
		setWhereExpressionRange(expression, Range{Start: start, End: p.previous().sourceRange.End})
		return expression, nil
	}

	left, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	return p.parseWherePredicateAfterScalar(left)
}

func (p *parser) parseWherePredicateAfterScalar(left ScalarExpr) (WhereExpr, error) {
	if p.profile == expressionProfileAuthored {
		negated := false
		membership := false
		if p.isKeyword("IN") {
			membership = true
			p.advance()
		} else if p.isKeyword("NOT") && p.index+1 < len(p.tokens) &&
			p.tokens[p.index+1].kind == tokenWord && strings.EqualFold(p.tokens[p.index+1].text, "IN") {
			membership = true
			negated = true
			p.advance()
			p.advance()
		}
		if membership {
			candidates, end, err := p.parseMembershipList()
			if err != nil {
				return nil, err
			}
			if countErr := p.countEvalPredicate(left.SourceRange()); countErr != nil {
				return nil, countErr
			}
			return &WhereMembershipExpr{
				Value:      left,
				Candidates: candidates,
				Negated:    negated,
				Range:      Range{Start: left.SourceRange().Start, End: end},
			}, nil
		}
	}

	op, ok := evalComparisonOperator(p.current().kind, p.profile)
	if !ok {
		if scalarExpressionCanBeDirectPredicate(left) {
			if countErr := p.countEvalPredicate(left.SourceRange()); countErr != nil {
				return nil, countErr
			}
			return &WhereScalarPredicateExpr{
				Value: left,
				Range: left.SourceRange(),
			}, nil
		}
		if p.current().kind == tokenWord && unsupportedScalarIdentifier(p.current().text) {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message:     fmt.Sprintf("unsupported where scalar operator %q", p.current().text),
				Range:       p.current().sourceRange,
				Suggestions: []string{"use a supported comparison operator"},
			}
		}
		return nil, &Diagnostic{
			Code:        "SPL_EXPECTED_COMPARISON",
			Message:     "where scalar expression must be followed by a comparison operator",
			Range:       left.SourceRange(),
			Suggestions: []string{"where field=value"},
		}
	}
	p.advance()
	right, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if countErr := p.countEvalPredicate(left.SourceRange()); countErr != nil {
		return nil, countErr
	}
	return &WhereComparisonExpr{
		Left:  left,
		Op:    op,
		Right: right,
		Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End},
	}, nil
}

func (p *parser) parseWhereMembershipFunction() (WhereExpr, error) {
	name := p.current()
	p.advance()
	if !p.match(tokenLeftParen) {
		return nil, p.membershipSyntaxError(name.sourceRange, "in requires a parenthesized value and candidate list")
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, p.membershipSyntaxError(name.sourceRange, "in requires a value followed by at least one candidate")
	}
	value, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if !p.match(tokenComma) {
		return nil, p.membershipSyntaxError(name.sourceRange, "in requires at least one candidate after its value")
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, p.membershipSyntaxError(p.current().sourceRange, "in requires a candidate after comma")
	}
	candidates, end, err := p.parseMembershipCandidates(name.sourceRange)
	if err != nil {
		return nil, err
	}
	if countErr := p.countEvalPredicate(name.sourceRange); countErr != nil {
		return nil, countErr
	}
	return &WhereMembershipExpr{
		Value:      value,
		Candidates: candidates,
		Range:      Range{Start: name.sourceRange.Start, End: end},
	}, nil
}

func (p *parser) parseMembershipList() ([]ScalarExpr, Position, error) {
	start := p.current().sourceRange
	if !p.match(tokenLeftParen) {
		return nil, start.End, p.membershipSyntaxError(start, "membership requires a parenthesized candidate list")
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, start.End, p.membershipSyntaxError(p.current().sourceRange, "membership requires at least one candidate")
	}
	return p.parseMembershipCandidates(start)
}

func (p *parser) parseMembershipCandidates(listRange Range) ([]ScalarExpr, Position, error) {
	candidates := make([]ScalarExpr, 0, 4)
	for {
		candidate, err := p.parseScalarExpression()
		if err != nil {
			return nil, listRange.End, err
		}
		candidates = append(candidates, candidate)
		if len(candidates) > MaximumMembershipCandidates {
			return nil, candidate.SourceRange().End, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("membership contains more than %d candidates", MaximumMembershipCandidates),
				Range:   Range{Start: listRange.Start, End: candidate.SourceRange().End},
			}
		}
		if p.match(tokenRightParen) {
			end := p.previous().sourceRange.End
			if err := p.chargeMembershipCandidates(len(candidates), Range{Start: listRange.Start, End: end}); err != nil {
				return nil, end, err
			}
			return candidates, end, nil
		}
		if !p.match(tokenComma) {
			return nil, candidate.SourceRange().End, p.membershipSyntaxError(
				p.current().sourceRange,
				"expected ',' or ')' after membership candidate",
			)
		}
		if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
			return nil, candidate.SourceRange().End, p.membershipSyntaxError(
				p.current().sourceRange,
				"membership candidate list cannot contain an empty or trailing candidate",
			)
		}
	}
}

func (p *parser) chargeMembershipCandidates(count int, sourceRange Range) error {
	if p.membershipCandidates > MaximumMembershipCandidatesPerQuery-count {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d membership candidates", MaximumMembershipCandidatesPerQuery),
			Range:   sourceRange,
		}
	}
	p.membershipCandidates += count
	return nil
}

func (p *parser) membershipSyntaxError(sourceRange Range, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX",
		Message:     message,
		Range:       sourceRange,
		Suggestions: []string{"where field IN (value)", "where in(field, value)"},
	}
}

func (p *parser) parseScalarCall(name token) (ScalarExpr, error) {
	arguments := make([]ScalarExpr, 0, 3)
	functionName := strings.ToLower(name.text)
	if p.current().kind != tokenRightParen {
		for {
			argumentIndex := len(arguments)
			preserveSignedLiteral :=
				(functionName == "substr" && argumentIndex >= 1) ||
					(functionName == "round" && argumentIndex == 1) ||
					(functionName == "mvindex" && argumentIndex >= 1)
			if preserveSignedLiteral {
				p.preserveSignedLiteral++
			}
			argument, err := p.parseScalarExpression()
			if preserveSignedLiteral {
				p.preserveSignedLiteral--
			}
			if err != nil {
				return nil, err
			}
			arguments = append(arguments, argument)
			if functionName == "coalesce" && len(arguments) > MaximumCoalesceArguments {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"coalesce contains more than %d arguments",
						MaximumCoalesceArguments,
					),
					Range: name.sourceRange,
				}
			}
			if functionName == "mvappend" && len(arguments) > MaximumMVAppendArguments {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"mvappend contains more than %d arguments",
						MaximumMVAppendArguments,
					),
					Range: name.sourceRange,
				}
			}
			if !p.match(tokenComma) {
				break
			}
			if p.current().kind == tokenRightParen {
				return nil, p.errorAtCurrent("SPL_EXPECTED_SCALAR_EXPRESSION", "expected a function argument after comma")
			}
		}
	}
	if !p.match(tokenRightParen) {
		return nil, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' to close function call")
	}
	switch functionName {
	case "now", "strftime", "strptime", "relative_time":
		return p.parseTimeScalarCall(name, functionName, arguments)
	case "tonumber", "tostring", "isnull", "isnotnull", "coalesce", "typeof", "nullif":
		return p.parseConversionScalarCall(name, functionName, arguments)
	case "replace", "lower", "upper", "len", "length", "substr", "trim", "ltrim", "rtrim",
		"urldecode", "md5", "sha1", "sha256", "sha512":
		return p.parseTextScalarCall(name, functionName, arguments)
	case "round", "ceil", "ceiling", "floor", "abs", "sqrt", "exp", "ln", "log", "pow", "pi":
		return p.parseNumericScalarCall(name, functionName, arguments)
	case "mvcount", "mvsort", "split", "mvappend", "mvdedup", "mvindex", "mvjoin", "mvzip", "mvfind":
		return p.parseMultivalueScalarCall(name, functionName, arguments)
	case "match", "like", "cidrmatch":
		return p.parsePredicateScalarCall(name, functionName, arguments)
	default:
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_FUNCTION",
			Message: fmt.Sprintf("eval function %q is not supported", name.text),
			Range:   name.sourceRange,
			// One representative per family keeps this list inside
			// MaximumDiagnosticSuggestions; the completion catalog carries the
			// full inventory.
			Suggestions: []string{
				"tonumber(value)",
				"tostring(value)",
				`tostring(value, "commas")`,
				"typeof(value)",
				"isnull(value)",
				"isnotnull(value)",
				"coalesce(value, fallback)",
				"nullif(value, sentinel)",
				`if(predicate, true_value, false_value)`,
				"lower(value)",
				"upper(value)",
				"trim(value)",
				"len(value)",
				"substr(value, start, length)",
				`replace(value, "pattern", "replacement")`,
				"urldecode(value)",
				"md5(value)",
				"round(value, precision)",
				"floor(value)",
				"ceil(value)",
				"abs(value)",
				"sqrt(value)",
				"pow(value, exponent)",
				"log(value, base)",
				"mvcount(value)",
				`split(value, ",")`,
				"mvindex(multivalue_field, start, end)",
				`mvjoin(multivalue_field, ",")`,
				`match(value, "pattern")`,
				`like(value, "pattern%")`,
				`cidrmatch("10.0.0.0/8", ip)`,
				"now()",
			},
		}
	}
}

func parsedScalarCall(p *parser, name token, function ScalarFunction, arguments []ScalarExpr) *ScalarCallExpr {
	return &ScalarCallExpr{
		Function:  function,
		Arguments: arguments,
		Range:     Range{Start: name.sourceRange.Start, End: p.previous().sourceRange.End},
	}
}

func (p *parser) parseSearchAnd() (Expr, error) {
	left, err := p.parseSearchOr()
	if err != nil {
		return nil, err
	}
	for {
		explicit := p.isKeyword("AND")
		if explicit {
			p.advance()
		}
		if !explicit && !p.canStartSearchOperand() {
			break
		}
		if explicit && !p.canStartSearchOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after AND")
		}
		right, parseErr := p.parseSearchOr()
		if parseErr != nil {
			return nil, parseErr
		}
		left = &BinaryExpr{Op: BoolOpAnd, Left: left, Right: right, Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End}}
	}
	return left, nil
}

func (p *parser) parseSearchOr() (Expr, error) {
	left, err := p.parseSearchUnary()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("OR") {
		p.advance()
		if !p.canStartSearchOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after OR")
		}
		right, parseErr := p.parseSearchUnary()
		if parseErr != nil {
			return nil, parseErr
		}
		left = &BinaryExpr{Op: BoolOpOr, Left: left, Right: right, Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End}}
	}
	return left, nil
}

func (p *parser) parseSearchUnary() (Expr, error) {
	if p.isKeyword("NOT") {
		start := p.current().sourceRange.Start
		p.advance()
		if !p.canStartSearchOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after NOT")
		}
		operand, err := p.parseSearchUnary()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Operand: operand, Range: Range{Start: start, End: operand.SourceRange().End}}, nil
	}
	return p.parseSearchPrimary()
}

func (p *parser) parseSearchPrimary() (Expr, error) {
	if err := p.prepareSearchToken(); err != nil {
		return nil, err
	}
	if p.match(tokenLeftParen) {
		start := p.previous().sourceRange.Start
		if p.current().kind == tokenRightParen {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "empty parenthesized expression")
		}
		expression, err := p.parseSearchExpression()
		if err != nil {
			return nil, err
		}
		if !p.match(tokenRightParen) {
			return nil, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' to close search expression")
		}
		setExpressionRange(expression, Range{Start: start, End: p.previous().sourceRange.End})
		return expression, nil
	}

	tok := p.current()
	if tok.kind == tokenString {
		p.advance()
		return &TermExpr{Value: tok.text, Quoted: true, Range: tok.sourceRange}, nil
	}
	if (tok.kind != tokenWord && tok.kind != tokenConcat) ||
		p.isKeyword("AND") || p.isKeyword("OR") {
		return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected a search term or field comparison")
	}
	p.advance()
	if op, ok := comparisonOperator(p.current().kind); ok {
		p.advance()
		literal, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		return &ComparisonExpr{
			Field: tok.text,
			Op:    op,
			Value: literal,
			Range: Range{Start: tok.sourceRange.Start, End: literal.Range.End},
		}, nil
	}
	if (strings.EqualFold(tok.text, "IN") && p.current().kind == tokenLeftParen) ||
		(p.isKeyword("IN") && p.nextIs(tokenLeftParen)) ||
		(p.isKeyword("NOT") && p.index+2 < len(p.tokens) &&
			p.tokens[p.index+1].kind == tokenWord && strings.EqualFold(p.tokens[p.index+1].text, "IN") &&
			p.tokens[p.index+2].kind == tokenLeftParen) {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_EXPRESSION",
			Message:     "membership is supported only in eval-language predicate positions, not base search",
			Range:       Range{Start: tok.sourceRange.Start, End: p.current().sourceRange.End},
			Suggestions: []string{"use a where command for exact eval-language membership"},
		}
	}
	return &TermExpr{Value: tok.text, Range: tok.sourceRange}, nil
}

func (p *parser) parseLiteral() (Literal, error) {
	if err := p.prepareSearchToken(); err != nil {
		return Literal{}, err
	}
	tok := p.current()
	if tok.kind != tokenWord && tok.kind != tokenString &&
		tok.kind != tokenConcat {
		return Literal{}, p.errorAtCurrent("SPL_EXPECTED_LITERAL", "expected a comparison value")
	}
	p.advance()
	kind := classifyLiteral(tok.text, tok.quoted)
	return Literal{Kind: kind, Text: tok.text, Quoted: tok.quoted, Range: tok.sourceRange}, nil
}

func classifyLiteral(text string, quoted bool) LiteralKind {
	if quoted {
		return LiteralKindString
	}
	switch strings.ToLower(text) {
	case "true", "false":
		return LiteralKindBool
	case "null":
		return LiteralKindNull
	}
	if integerSyntax(text) {
		return LiteralKindInteger
	}
	if floatSyntax(text) {
		return LiteralKindFloat
	}
	return LiteralKindString
}

func unsignedIntegerSyntax(text string) bool {
	if text == "" {
		return false
	}
	for i := range len(text) {
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return true
}

func floatSyntax(text string) bool {
	if text == "" {
		return false
	}
	i := 0
	if text[i] == '-' || text[i] == '+' {
		i++
	}
	digits := 0
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
		digits++
	}
	hasDecimalPoint := false
	if i < len(text) && text[i] == '.' {
		hasDecimalPoint = true
		i++
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	hasExponent := false
	if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		hasExponent = true
		i++
		if i < len(text) && (text[i] == '-' || text[i] == '+') {
			i++
		}
		exponentStart := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if exponentStart == i {
			return false
		}
	}
	return i == len(text) && (hasDecimalPoint || hasExponent)
}

func integerSyntax(text string) bool {
	if text == "" {
		return false
	}
	start := 0
	if text[0] == '-' || text[0] == '+' {
		start = 1
	}
	if start == len(text) {
		return false
	}
	for i := start; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return true
}

func comparisonOperator(kind tokenKind) (CompareOp, bool) {
	switch kind {
	case tokenEqual:
		return CompareOpEqual, true
	case tokenNotEqual:
		return CompareOpNotEqual, true
	case tokenLess:
		return CompareOpLess, true
	case tokenLessEqual:
		return CompareOpLessEqual, true
	case tokenGreater:
		return CompareOpGreater, true
	case tokenGreaterEqual:
		return CompareOpGreaterEqual, true
	default:
		return CompareOpInvalid, false
	}
}

func evalComparisonOperator(kind tokenKind, profile expressionProfile) (CompareOp, bool) {
	if kind == tokenEqualEqual && profile == expressionProfileAuthored {
		return CompareOpEqual, true
	}
	return comparisonOperator(kind)
}

func setExpressionRange(expression Expr, sourceRange Range) {
	switch e := expression.(type) {
	case *BinaryExpr:
		e.Range = sourceRange
	case *NotExpr:
		e.Range = sourceRange
	case *TermExpr:
		e.Range = sourceRange
	case *ComparisonExpr:
		e.Range = sourceRange
	}
}

func setWhereExpressionRange(expression WhereExpr, sourceRange Range) {
	switch expression := expression.(type) {
	case *WhereBoolExpr:
		expression.Range = sourceRange
	case *WhereNotExpr:
		expression.Range = sourceRange
	case *WhereComparisonExpr:
		expression.Range = sourceRange
	case *WhereMembershipExpr:
		expression.Range = sourceRange
	case *WhereScalarPredicateExpr:
		expression.Range = sourceRange
	}
}

func setScalarExpressionRange(expression ScalarExpr, sourceRange Range) {
	switch expression := expression.(type) {
	case *ScalarFieldExpr:
		expression.Range = sourceRange
	case *ScalarLiteralExpr:
		expression.Range = sourceRange
		expression.Value.Range = sourceRange
	case *ScalarUnaryExpr:
		expression.Range = sourceRange
	case *ScalarBinaryExpr:
		expression.Range = sourceRange
	case *ScalarCallExpr:
		expression.Range = sourceRange
	case *ScalarIfExpr:
		expression.Range = sourceRange
	case *ScalarCaseExpr:
		expression.Range = sourceRange
	}
}

func (p *parser) canStartSearchOperand() bool {
	tok := p.current()
	if tok.kind == tokenString || tok.kind == tokenLeftParen ||
		tok.kind == tokenConcat || tok.kind == tokenQuotedField ||
		tok.kind == tokenScalarComposite {
		return true
	}
	if tok.kind != tokenWord {
		return false
	}
	return !p.isKeyword("AND") && !p.isKeyword("OR")
}

func (p *parser) canStartWhereOperand() bool {
	tok := p.current()
	if tok.kind == tokenLeftParen || tok.kind == tokenString ||
		tok.kind == tokenQuotedField || tok.kind == tokenScalarComposite ||
		tok.kind == tokenPlus || tok.kind == tokenMinus {
		return true
	}
	return tok.kind == tokenWord && !p.isKeyword("AND") && !p.isKeyword("OR")
}

func (p *parser) atCommandEnd() bool {
	return p.current().kind == tokenPipe || p.current().kind == tokenEOF
}

func (p *parser) isKeyword(keyword string) bool {
	return p.current().kind == tokenWord && strings.EqualFold(p.current().text, keyword)
}

func (p *parser) match(kind tokenKind) bool {
	if p.current().kind != kind {
		return false
	}
	p.advance()
	return true
}

func (p *parser) advance() {
	if p.index < len(p.tokens)-1 {
		p.index++
	}
}

func (p *parser) current() token {
	return p.tokens[p.index]
}

func (p *parser) nextIs(kind tokenKind) bool {
	return p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == kind
}

func (p *parser) previous() token {
	return p.tokens[p.index-1]
}

func (p *parser) errorAtCurrent(code, message string) *Diagnostic {
	return &Diagnostic{Code: code, Message: message, Range: p.current().sourceRange}
}

// prepareSearchToken expands a single-quoted scalar-field token back into the
// exact legacy token stream when it occurs in base search. This keeps quoted
// fields confined to scalar grammar without changing base-search apostrophes,
// whitespace, or punctuation boundaries.
func (p *parser) prepareSearchToken() error {
	tok := p.current()
	if tok.kind != tokenQuotedField && tok.kind != tokenScalarComposite {
		return nil
	}
	legacy, err := lexWithQuotedFields(tok.raw, false)
	if err != nil {
		var diagnostic *Diagnostic
		if errors.As(err, &diagnostic) && diagnostic != nil {
			diagnosticCopy := *diagnostic
			diagnosticCopy.Range = translateFragmentRange(tok.sourceRange.Start, diagnostic.Range)
			return &diagnosticCopy
		}
		return err
	}
	legacy = legacy[:len(legacy)-1] // enclosing stream already owns EOF
	if len(p.tokens)-1+len(legacy)-1 > maxSPLTokens {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d syntax tokens", maxSPLTokens),
			Range:   tok.sourceRange,
		}
	}
	for index := range legacy {
		legacy[index].sourceRange = translateFragmentRange(tok.sourceRange.Start, legacy[index].sourceRange)
	}
	replacement := make([]token, 0, len(p.tokens)+len(legacy)-1)
	replacement = append(replacement, p.tokens[:p.index]...)
	replacement = append(replacement, legacy...)
	replacement = append(replacement, p.tokens[p.index+1:]...)
	p.tokens = replacement
	return nil
}

func (p *parser) expandLegacyScalarCompositesUntilCommandEnd() error {
	current := p.index
	for index := current; index < len(p.tokens); {
		kind := p.tokens[index].kind
		if kind == tokenEOF || kind == tokenPipe {
			return nil
		}
		if kind != tokenScalarComposite {
			index++
			continue
		}
		p.index = index
		if err := p.prepareSearchToken(); err != nil {
			p.index = current
			return err
		}
		p.index = current
		index++
	}
	p.index = current
	return nil
}

func translateFragmentRange(base Position, relative Range) Range {
	return Range{
		Start: translateFragmentPosition(base, relative.Start),
		End:   translateFragmentPosition(base, relative.End),
	}
}

func translateFragmentPosition(base, relative Position) Position {
	position := Position{
		Offset: base.Offset + relative.Offset,
		Line:   base.Line + relative.Line - 1,
		Column: relative.Column,
	}
	if relative.Line == 1 {
		position.Column = base.Column + relative.Column - 1
	}
	return position
}

// prepareScalarToken reserves arithmetic punctuation only while the authored
// scalar grammar is active. The base lexer intentionally keeps words whole so
// base-search values, command aliases, sort prefixes, and paths retain their
// established tokenization.
func (p *parser) prepareScalarToken() error {
	if p.profile != expressionProfileAuthored ||
		p.current().kind != tokenWord && p.current().kind != tokenScalarComposite {
		return nil
	}
	if p.preserveSignedLiteral > 0 && signedIntegerLiteralToken(p.current().text) {
		return nil
	}
	preparedQuote, err := p.prepareScalarQuotedOperand()
	if err != nil {
		return err
	}
	if preparedQuote {
		return nil
	}
	parts := splitScalarWord(p.current())
	if len(parts) == 0 {
		return nil
	}
	if len(p.tokens)-1+len(parts)-1 > maxSPLTokens {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d syntax tokens", maxSPLTokens),
			Range:   p.current().sourceRange,
		}
	}
	replacement := make([]token, 0, len(p.tokens)+len(parts)-1)
	replacement = append(replacement, p.tokens[:p.index]...)
	replacement = append(replacement, parts...)
	replacement = append(replacement, p.tokens[p.index+1:]...)
	p.tokens = replacement
	return nil
}

// prepareScalarQuotedOperand repairs the one context that the legacy lexer
// deliberately cannot recognize globally: a single-quoted scalar operand
// immediately following unspaced arithmetic punctuation. The initial token
// stream retains base-search boundaries; only an authored scalar parse uses the
// original source to replace the overlapping tokens with one decoded
// quoted-field token.
func (p *parser) prepareScalarQuotedOperand() (bool, error) {
	tok := p.current()
	if tok.kind != tokenWord && tok.kind != tokenScalarComposite ||
		p.source == "" || !strings.Contains(tok.text, "'") {
		return false, nil
	}

	segmentStart := 0
	opening := -1
	for offset := 0; offset < len(tok.text); offset++ {
		if tok.text[offset] == '\'' && offset == segmentStart && offset > 0 {
			opening = offset
			break
		}
		if _, operator := scalarOperatorToken(tok.text[offset]); operator {
			segmentStart = offset + 1
		}
	}
	if opening < 0 {
		return false, nil
	}

	openingPosition := advanceSourcePosition(tok.sourceRange.Start, tok.text[:opening])
	quoteLexer := lexer{
		source:       p.source,
		offset:       openingPosition.Offset,
		line:         openingPosition.Line,
		column:       openingPosition.Column,
		quotedFields: true,
	}
	if !quoteLexer.hasClosingSingleQuote() {
		return false, &Diagnostic{
			Code:    "SPL_UNTERMINATED_FIELD_QUOTE",
			Message: "unterminated single-quoted field reference",
			Range:   Range{Start: openingPosition, End: advanceSourcePosition(openingPosition, "'")},
		}
	}
	quoted, scanErr := quoteLexer.scanQuotedField(openingPosition)
	if scanErr != nil {
		return false, scanErr
	}
	quoteEnd := quoted.sourceRange.End.Offset

	consumedEnd := p.index
	for consumedEnd+1 < len(p.tokens) &&
		p.tokens[consumedEnd+1].kind != tokenEOF &&
		p.tokens[consumedEnd+1].sourceRange.Start.Offset < quoteEnd {
		consumedEnd++
	}
	if p.tokens[consumedEnd].sourceRange.End.Offset < quoteEnd {
		return false, &Diagnostic{
			Code:    "SPL_UNTERMINATED_FIELD_QUOTE",
			Message: "unterminated single-quoted field reference",
			Range:   Range{Start: openingPosition, End: advanceSourcePosition(openingPosition, "'")},
		}
	}

	prefix := token{
		kind: tokenWord,
		text: tok.text[:opening],
		sourceRange: Range{
			Start: tok.sourceRange.Start,
			End:   openingPosition,
		},
	}
	parts := splitScalarWord(prefix)
	if len(parts) == 0 && prefix.text != "" {
		parts = append(parts, prefix)
	}
	parts = append(parts, quoted)

	consumedSourceEnd := p.tokens[consumedEnd].sourceRange.End.Offset
	if consumedSourceEnd > quoteEnd {
		suffixStart := quoted.sourceRange.End
		suffixText := p.source[quoteEnd:consumedSourceEnd]
		parts = append(parts, token{
			kind: tokenWord,
			text: suffixText,
			sourceRange: Range{
				Start: suffixStart,
				End:   advanceSourcePosition(suffixStart, suffixText),
			},
		})
	}

	consumed := consumedEnd - p.index + 1
	if len(p.tokens)-1-consumed+len(parts) > maxSPLTokens {
		return false, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d syntax tokens", maxSPLTokens),
			Range:   tok.sourceRange,
		}
	}
	replacement := make([]token, 0, len(p.tokens)-consumed+len(parts))
	replacement = append(replacement, p.tokens[:p.index]...)
	replacement = append(replacement, parts...)
	replacement = append(replacement, p.tokens[consumedEnd+1:]...)
	p.tokens = replacement
	return true, nil
}

func splitScalarWord(tok token) []token {
	parts, split := appendScalarWord(nil, tok)
	if !split {
		return nil
	}
	return parts
}

func appendScalarWord(parts []token, tok token) ([]token, bool) {
	if tok.kind != tokenWord || !strings.ContainsAny(tok.text, "+-*/%") {
		return parts, false
	}
	start := len(parts)
	segmentStart := 0
	segmentPosition := tok.sourceRange.Start
	position := segmentPosition
	for offset := 0; offset < len(tok.text); {
		value := tok.text[offset]
		r, width := utf8.DecodeRuneInString(tok.text[offset:])
		kind, operator := scalarOperatorToken(value)
		if !operator {
			position = advancePositionByRune(position, r, width)
			offset += width
			continue
		}
		// A sign directly following e/E belongs to a numeric exponent only when
		// the complete current segment is a Float literal. Prefix probing would
		// misclassify field-like spellings such as 1e-foo and 1e--3.
		if (value == '+' || value == '-') &&
			offset > segmentStart &&
			(tok.text[offset-1] == 'e' || tok.text[offset-1] == 'E') {
			segmentEnd := len(tok.text)
			for candidateEnd := offset + 1; candidateEnd < len(tok.text); candidateEnd++ {
				if _, nextOperator := scalarOperatorToken(tok.text[candidateEnd]); nextOperator {
					segmentEnd = candidateEnd
					break
				}
			}
			if classifyLiteral(tok.text[segmentStart:segmentEnd], false) == LiteralKindFloat {
				position = advancePositionByRune(position, r, width)
				offset += width
				continue
			}
		}
		if offset > segmentStart {
			parts = appendScalarWordFragment(
				parts,
				tok.text[segmentStart:offset],
				segmentPosition,
				position,
			)
		}
		operatorEnd := advancePositionByRune(position, r, width)
		parts = append(parts, token{
			kind: kind,
			text: tok.text[offset : offset+width],
			sourceRange: Range{
				Start: position,
				End:   operatorEnd,
			},
		})
		offset += width
		position = operatorEnd
		segmentStart = offset
		segmentPosition = position
	}
	if segmentStart < len(tok.text) {
		parts = appendScalarWordFragment(
			parts,
			tok.text[segmentStart:],
			segmentPosition,
			position,
		)
	}
	if len(parts) == start+1 && parts[start].kind == tokenWord {
		return parts[:start], false
	}
	return parts, true
}

func appendScalarWordFragment(
	parts []token,
	text string,
	start Position,
	end Position,
) []token {
	fragment := token{
		kind:        tokenWord,
		text:        text,
		sourceRange: Range{Start: start, End: end},
	}
	// Arithmetic splitting removes only operator bytes from one legacy word.
	// With no whitespace, quote, or delimiter introduced, the sole fragment
	// whose isolated legacy token kind can differ is ".": both of its dot
	// boundaries become visible and it is concatenation, not a minted field.
	if fragment.text == "." {
		fragment.kind = tokenConcat
	}
	return append(parts, fragment)
}

func signedIntegerLiteralToken(value string) bool {
	return len(value) > 1 && (value[0] == '+' || value[0] == '-') &&
		classifyLiteral(value, false) == LiteralKindInteger
}

func scalarOperatorToken(value byte) (tokenKind, bool) {
	switch value {
	case '+':
		return tokenPlus, true
	case '-':
		return tokenMinus, true
	case '*':
		return tokenMultiply, true
	case '/':
		return tokenDivide, true
	case '%':
		return tokenRemainder, true
	default:
		return tokenInvalid, false
	}
}

func advanceSourcePosition(position Position, value string) Position {
	for len(value) > 0 {
		r, width := utf8.DecodeRuneInString(value)
		if r == utf8.RuneError && width == 1 {
			width = 1
		}
		position.Offset += width
		if r == '\n' {
			position.Line++
			position.Column = 1
		} else {
			position.Column++
		}
		value = value[width:]
	}
	return position
}
