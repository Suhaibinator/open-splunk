package spl

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/splpath"
)

// Position identifies a UTF-8 source position. Offset is zero-based while
// Line and Column are one-based.
type Position struct {
	Offset int
	Line   int
	Column int
}

// Range is a half-open source range [Start, End).
type Range struct {
	Start Position
	End   Position
}

// Node is implemented by every source-located syntax tree node.
type Node interface {
	SourceRange() Range
}

// Query is an SPL base search followed by zero or more pipe commands.
type Query struct {
	Search   Expr
	Commands []Command
	Range    Range

	// parsedEvalPredicates is parser-owned provenance for the exact number of
	// eval/where predicate leaves admitted from source. Logical planning keeps
	// it private so a later whole-query compiler can combine authored and
	// knowledge work without trusting a caller-constructible counter.
	parsedEvalPredicates        uint32
	parsedEvalPredicatePrefixes []uint32
	// sourceDigest binds private parser provenance to the exact authored byte
	// sequence. ASTs assembled or spliced by callers cannot mint this identity.
	sourceDigest [sha256.Size]byte
	parsedSource string
	parsed       bool
}

// ParsedCanonicalClone reparses the exact privately retained source and
// returns a detached AST only when every current public and private AST field
// still agrees. This is the parser-to-planner integrity boundary for features
// that mint replayable authority: retaining a source digest is insufficient
// if a caller mutates public nodes after Parse returns.
func (q *Query) ParsedCanonicalClone() (*Query, bool) {
	if q == nil || !q.parsed || q.parsedSource == "" ||
		q.sourceDigest == ([sha256.Size]byte{}) ||
		sha256.Sum256([]byte(q.parsedSource)) != q.sourceDigest {
		return nil, false
	}
	canonical, err := Parse(q.parsedSource)
	if err != nil || canonical == nil || !reflect.DeepEqual(q, canonical) {
		return nil, false
	}
	canonical.parsedSource = strings.Clone(canonical.parsedSource)
	return canonical, true
}

// ParsedSourceDigest returns parser-owned identity for the exact SPL source.
func (q *Query) ParsedSourceDigest() ([sha256.Size]byte, bool) {
	if q == nil || !q.parsed || q.parsedSource == "" ||
		q.sourceDigest == ([sha256.Size]byte{}) ||
		sha256.Sum256([]byte(q.parsedSource)) != q.sourceDigest {
		return [sha256.Size]byte{}, false
	}
	return q.sourceDigest, true
}

// ParsedSource returns a detached copy of parser-retained authored SPL only
// while the public AST still agrees exactly with a fresh parse of that source.
// It is intentionally not an authority by itself; planner APIs reparse it and
// bind the resulting canonical plan to a trusted scope.
func (q *Query) ParsedSource() (string, bool) {
	if _, ok := q.ParsedCanonicalClone(); !ok {
		return "", false
	}
	return strings.Clone(q.parsedSource), true
}

// ParsedPrefix returns parser-owned provenance for exactly the base search and
// the first commandCount pipeline commands. Command nodes are immutable parser
// output; the command slice and provenance vectors are detached.
func (q *Query) ParsedPrefix(commandCount int) (*Query, bool) {
	canonical, ok := q.ParsedCanonicalClone()
	if !ok || commandCount < 0 || commandCount > len(canonical.Commands) ||
		len(canonical.parsedEvalPredicatePrefixes) != len(canonical.Commands)+1 {
		return nil, false
	}
	result := *canonical
	result.Commands = slices.Clone(canonical.Commands[:commandCount])
	result.parsedEvalPredicates = canonical.parsedEvalPredicatePrefixes[commandCount]
	result.parsedEvalPredicatePrefixes = slices.Clone(
		canonical.parsedEvalPredicatePrefixes[:commandCount+1],
	)
	return &result, true
}

// SourceRange implements Node.
func (q *Query) SourceRange() Range { return q.Range }

// ParsedEvalPredicateCount returns parser-owned predicate provenance. Queries
// assembled directly as ASTs deliberately have no provenance: they remain
// valid planning fixtures, but cannot mint sealed knowledge-snapshot evidence.
func (q *Query) ParsedEvalPredicateCount() (uint32, bool) {
	if q == nil || !q.parsed {
		return 0, false
	}
	return q.parsedEvalPredicates, true
}

// Expr is an SPL search expression.
type Expr interface {
	Node
	expression()
}

// BoolOp combines search expressions. Base-search parsing deliberately gives
// OR higher precedence than AND, matching Splunk's search-command semantics.
type BoolOp uint8

const (
	BoolOpInvalid BoolOp = iota
	BoolOpAnd
	BoolOpOr
)

// String returns the SPL spelling of op.
func (op BoolOp) String() string {
	switch op {
	case BoolOpAnd:
		return "AND"
	case BoolOpOr:
		return "OR"
	default:
		return "INVALID"
	}
}

// BinaryExpr combines two expressions.
type BinaryExpr struct {
	Op    BoolOp
	Left  Expr
	Right Expr
	Range Range
}

func (*BinaryExpr) expression()          {}
func (e *BinaryExpr) SourceRange() Range { return e.Range }

// NotExpr negates one expression.
type NotExpr struct {
	Operand Expr
	Range   Range
}

func (*NotExpr) expression()          {}
func (e *NotExpr) SourceRange() Range { return e.Range }

// TermExpr searches the event's free-text fields. Quoted records whether the
// source used a quoted phrase rather than a bare term.
type TermExpr struct {
	Value  string
	Quoted bool
	Range  Range
}

func (*TermExpr) expression()          {}
func (e *TermExpr) SourceRange() Range { return e.Range }

// CompareOp is an SPL field-comparison operator.
type CompareOp uint8

const (
	CompareOpInvalid CompareOp = iota
	CompareOpEqual
	CompareOpNotEqual
	CompareOpLess
	CompareOpLessEqual
	CompareOpGreater
	CompareOpGreaterEqual
)

// String returns the SPL spelling of op.
func (op CompareOp) String() string {
	switch op {
	case CompareOpEqual:
		return "="
	case CompareOpNotEqual:
		return "!="
	case CompareOpLess:
		return "<"
	case CompareOpLessEqual:
		return "<="
	case CompareOpGreater:
		return ">"
	case CompareOpGreaterEqual:
		return ">="
	default:
		return "INVALID"
	}
}

// LiteralKind distinguishes typed syntax literals without prematurely
// assigning a field-dependent semantic type.
type LiteralKind uint8

const (
	LiteralKindInvalid LiteralKind = iota
	LiteralKindString
	LiteralKindInteger
	LiteralKindFloat
	LiteralKindBool
	LiteralKindNull
)

// Literal is the exact comparison value from SPL. Text is unescaped for quoted
// strings and otherwise preserves the source spelling.
type Literal struct {
	Kind   LiteralKind
	Text   string
	Quoted bool
	Range  Range
}

// ComparisonExpr compares a field with one literal.
type ComparisonExpr struct {
	Field string
	Op    CompareOp
	Value Literal
	Range Range
}

func (*ComparisonExpr) expression()          {}
func (e *ComparisonExpr) SourceRange() Range { return e.Range }

// Command is one pipe-oriented SPL command.
type Command interface {
	Node
	command()
	Name() string
}

// SearchCommand applies search-command boolean semantics at a pipeline stage.
type SearchCommand struct {
	Expression Expr
	Range      Range
}

func (*SearchCommand) command()             {}
func (*SearchCommand) Name() string         { return "search" }
func (c *SearchCommand) SourceRange() Range { return c.Range }

// ScalarExpr is an eval-language value expression. It is deliberately
// separate from base-search Expr because bare names are field references and
// string comparisons are case-sensitive in eval and where expressions.
type ScalarExpr interface {
	Node
	scalarExpression()
}

// ScalarFieldExpr reads one field from the current pipeline row.
type ScalarFieldExpr struct {
	Field  string
	Quoted bool
	Range  Range
}

func (*ScalarFieldExpr) scalarExpression()    {}
func (e *ScalarFieldExpr) SourceRange() Range { return e.Range }

// ScalarLiteralExpr is one typed eval-language literal.
type ScalarLiteralExpr struct {
	Value Literal
	Range Range
}

func (*ScalarLiteralExpr) scalarExpression()    {}
func (e *ScalarLiteralExpr) SourceRange() Range { return e.Range }

// ScalarFunction identifies a supported, typed eval function.
type ScalarFunction uint8

const (
	// MaximumCoalesceArguments bounds parser work and generated backend
	// expressions for one coalesce call. This is an Open Splunk resource
	// limit, not a restriction imposed by SPL.
	MaximumCoalesceArguments = 32

	// MaximumMVAppendArguments bounds one multivalue concatenation before any
	// per-row member and payload limits are applied by the backend.
	MaximumMVAppendArguments = 32

	// MaximumMVDelimiterBytes bounds the decoded UTF-8 delimiter accepted by
	// split, mvjoin, and mvzip. Source-size limits remain independently
	// enforced for the surrounding SPL query.
	MaximumMVDelimiterBytes = 1 << 10

	// MaximumNativeMVValues and MaximumNativeMVPayloadBytes are the shared
	// per-row construction limits for native multivalue results.
	MaximumNativeMVValues       = 1000
	MaximumNativeMVPayloadBytes = 1 << 20

	// MaximumCaseBranches bounds predicate work and generated backend
	// expressions for one case call. This is an Open Splunk resource limit,
	// not a restriction imposed by SPL.
	MaximumCaseBranches = 16

	// MaximumRoundPrecision is the largest non-negative decimal precision
	// accepted by the pinned ClickHouse Float64 round implementation.
	MaximumRoundPrecision = 18

	// MaximumConcatenationOperands bounds one flattened period-concatenation
	// expression. This is an Open Splunk resource limit, not a restriction
	// imposed by SPL.
	MaximumConcatenationOperands = 32

	// MaximumConcatenationOperandsPerQuery bounds the aggregate number of
	// operand occurrences across all period-concatenation expressions in one
	// parsed query. Nested expressions are charged independently.
	MaximumConcatenationOperandsPerQuery = 256

	// MaximumArithmeticOperatorsPerQuery bounds authored unary and binary
	// arithmetic occurrences across one parsed query.
	MaximumArithmeticOperatorsPerQuery = 256

	// MaximumUnaryOperatorChain bounds one right-associative unary chain.
	MaximumUnaryOperatorChain = 32

	// MaximumMembershipCandidates bounds one function or infix membership
	// candidate list.
	MaximumMembershipCandidates = 32

	// MaximumMembershipCandidatesPerQuery bounds candidate occurrences across
	// all membership predicates in one parsed query.
	MaximumMembershipCandidatesPerQuery = 256
)

// ScalarUnaryOp identifies one supported unary arithmetic operator.
type ScalarUnaryOp uint8

const (
	ScalarUnaryOpInvalid ScalarUnaryOp = iota
	ScalarUnaryOpPositive
	ScalarUnaryOpNegative
	ScalarUnaryOpCount
)

// ScalarUnaryExpr applies a unary arithmetic operator to one scalar value.
type ScalarUnaryExpr struct {
	Op      ScalarUnaryOp
	Operand ScalarExpr
	Range   Range
}

func (*ScalarUnaryExpr) scalarExpression()    {}
func (e *ScalarUnaryExpr) SourceRange() Range { return e.Range }

// ScalarBinaryOp identifies one supported binary arithmetic operator.
type ScalarBinaryOp uint8

const (
	ScalarBinaryOpInvalid ScalarBinaryOp = iota
	ScalarBinaryOpMultiply
	ScalarBinaryOpDivide
	ScalarBinaryOpRemainder
	ScalarBinaryOpAdd
	ScalarBinaryOpSubtract
	ScalarBinaryOpCount
)

// ScalarBinaryExpr applies a binary arithmetic operator to two scalar values.
type ScalarBinaryExpr struct {
	Op    ScalarBinaryOp
	Left  ScalarExpr
	Right ScalarExpr
	Range Range
}

func (*ScalarBinaryExpr) scalarExpression()    {}
func (e *ScalarBinaryExpr) SourceRange() Range { return e.Range }

const (
	ScalarFunctionInvalid ScalarFunction = iota
	ScalarFunctionToNumber
	ScalarFunctionReplace
	ScalarFunctionIsNull
	ScalarFunctionIsNotNull
	ScalarFunctionCoalesce
	ScalarFunctionLower
	ScalarFunctionUpper
	ScalarFunctionLength
	ScalarFunctionSubstring
	ScalarFunctionToString
	ScalarFunctionRound
	ScalarFunctionCeil
	ScalarFunctionFloor
	ScalarFunctionMVCount
	ScalarFunctionMVSort
	ScalarFunctionMatch
	ScalarFunctionLike
	ScalarFunctionNow
	ScalarFunctionStrftime
	ScalarFunctionStrptime
	ScalarFunctionRelativeTime
	ScalarFunctionConcat
	ScalarFunctionSplit
	ScalarFunctionMVAppend
	ScalarFunctionMVDedup
	ScalarFunctionMVIndex
	ScalarFunctionMVJoin
	ScalarFunctionMVZip
	ScalarFunctionMVFind
	ScalarFunctionAbs
	ScalarFunctionSqrt
	ScalarFunctionExp
	ScalarFunctionLn
	ScalarFunctionLog
	ScalarFunctionPow
	ScalarFunctionPi
	ScalarFunctionTrim
	ScalarFunctionLTrim
	ScalarFunctionRTrim
	ScalarFunctionURLDecode
	ScalarFunctionMD5
	ScalarFunctionSHA1
	ScalarFunctionSHA256
	ScalarFunctionSHA512
	ScalarFunctionTypeOf
	ScalarFunctionCIDRMatch
	ScalarFunctionCount
)

// ReturnsBoolean reports the atomic result trait shared by parser, planner,
// and backend consumer checks. Composite coalesce/if/case expressions still
// require branch-aware analysis.
func (function ScalarFunction) ReturnsBoolean() bool {
	switch function {
	case ScalarFunctionIsNull, ScalarFunctionIsNotNull, ScalarFunctionMatch,
		ScalarFunctionLike, ScalarFunctionCIDRMatch:
		return true
	default:
		return false
	}
}

// ScalarCallExpr invokes a supported eval function. Function names are
// resolved by the parser so no user-authored identifier reaches a backend.
type ScalarCallExpr struct {
	Function  ScalarFunction
	Arguments []ScalarExpr
	Range     Range
}

func (*ScalarCallExpr) scalarExpression()    {}
func (e *ScalarCallExpr) SourceRange() Range { return e.Range }

// ScalarIfExpr selects one of two scalar values with an eval-language Boolean
// condition. Keeping the condition in the predicate AST prevents arbitrary
// scalar truthiness while allowing the same NOT/AND/OR precedence as where.
type ScalarIfExpr struct {
	Condition WhereExpr
	True      ScalarExpr
	False     ScalarExpr
	Range     Range
}

func (*ScalarIfExpr) scalarExpression()    {}
func (e *ScalarIfExpr) SourceRange() Range { return e.Range }

// ScalarCaseBranch is one ordered condition/value pair in a case expression.
type ScalarCaseBranch struct {
	Condition WhereExpr
	Value     ScalarExpr
	Range     Range
}

// ScalarCaseExpr selects the value from the first branch whose condition is
// true. If no condition is true, the result is null.
type ScalarCaseExpr struct {
	Branches []ScalarCaseBranch
	Range    Range
}

func (*ScalarCaseExpr) scalarExpression()    {}
func (e *ScalarCaseExpr) SourceRange() Range { return e.Range }

// WhereExpr is a Boolean eval expression accepted by where.
type WhereExpr interface {
	Node
	whereExpression()
}

// WhereBoolExpr combines where predicates with eval-language precedence.
type WhereBoolExpr struct {
	Op    BoolOp
	Left  WhereExpr
	Right WhereExpr
	Range Range
}

func (*WhereBoolExpr) whereExpression()     {}
func (e *WhereBoolExpr) SourceRange() Range { return e.Range }

// WhereNotExpr negates one where predicate.
type WhereNotExpr struct {
	Operand WhereExpr
	Range   Range
}

func (*WhereNotExpr) whereExpression()     {}
func (e *WhereNotExpr) SourceRange() Range { return e.Range }

// WhereComparisonExpr compares two scalar eval expressions.
type WhereComparisonExpr struct {
	Left  ScalarExpr
	Op    CompareOp
	Right ScalarExpr
	Range Range
}

func (*WhereComparisonExpr) whereExpression()     {}
func (e *WhereComparisonExpr) SourceRange() Range { return e.Range }

// WhereMembershipExpr compares one scalar value with a bounded, ordered list
// of scalar candidates using eval-language equality semantics.
type WhereMembershipExpr struct {
	Value      ScalarExpr
	Candidates []ScalarExpr
	Negated    bool
	Range      Range
}

func (*WhereMembershipExpr) whereExpression()     {}
func (e *WhereMembershipExpr) SourceRange() Range { return e.Range }

// WhereScalarPredicateExpr consumes a scalar function whose result is
// statically Boolean. The parser admits only functions with an explicit
// Boolean contract so an arbitrary scalar cannot silently acquire truthiness.
type WhereScalarPredicateExpr struct {
	Value ScalarExpr
	Range Range
}

func (*WhereScalarPredicateExpr) whereExpression()     {}
func (e *WhereScalarPredicateExpr) SourceRange() Range { return e.Range }

// WhereCommand filters rows with eval-language boolean precedence.
type WhereCommand struct {
	Expression WhereExpr
	Range      Range
}

func (*WhereCommand) command()             {}
func (*WhereCommand) Name() string         { return "where" }
func (c *WhereCommand) SourceRange() Range { return c.Range }

// EvalAssignment writes one scalar expression to a field. Assignments retain
// source order because later assignments in the same eval command may read
// fields produced by earlier assignments.
type EvalAssignment struct {
	Field      string
	FieldRange Range
	Expression ScalarExpr
	Range      Range
}

// EvalCommand evaluates one or more assignments from left to right.
type EvalCommand struct {
	Assignments []EvalAssignment
	Range       Range
}

func (*EvalCommand) command()             {}
func (*EvalCommand) Name() string         { return "eval" }
func (c *EvalCommand) SourceRange() Range { return c.Range }

// RexCommand extracts the first match from one current-pipeline string field.
// Pattern is the user spelling; parser validation guarantees a bounded,
// RE2-compatible pattern and uniquely named captures.
type RexCommand struct {
	Field        string
	FieldRange   Range
	Pattern      string
	PatternRange Range
	MaxMatch     uint64
	Range        Range
}

func (*RexCommand) command()             {}
func (*RexCommand) Name() string         { return "rex" }
func (c *RexCommand) SourceRange() Range { return c.Range }

// RegexCommand filters rows with one bounded RE2-compatible regular
// expression. Field defaults to _raw. Negated deliberately preserves rows
// whose input field is missing or null.
type RegexCommand struct {
	Field        string
	FieldRange   Range
	Pattern      string
	PatternRange Range
	Negated      bool
	Range        Range
}

func (*RegexCommand) command()             {}
func (*RegexCommand) Name() string         { return "regex" }
func (c *RegexCommand) SourceRange() Range { return c.Range }

// ReverseCommand reverses the complete established pipeline order without
// changing row cardinality or public fields.
type ReverseCommand struct {
	Range Range
}

func (*ReverseCommand) command()             {}
func (*ReverseCommand) Name() string         { return "reverse" }
func (c *ReverseCommand) SourceRange() Range { return c.Range }

// AccumCommand computes the ordered running numeric sum of one exact field.
// Output equals Field when AS is omitted; ExplicitOutput preserves that source
// distinction at the parser-to-planner trust boundary.
type AccumCommand struct {
	Field          string
	FieldRange     Range
	Output         string
	OutputRange    Range
	ExplicitOutput bool
	Range          Range
}

func (*AccumCommand) command()             {}
func (*AccumCommand) Name() string         { return "accum" }
func (c *AccumCommand) SourceRange() Range { return c.Range }

// StrcatOperand is either one exact field or one quoted String literal.
// Literal is a pointer so the empty quoted String remains distinguishable
// from a field operand.
type StrcatOperand struct {
	Field   string
	Literal *string
	Range   Range
}

// StrcatCommand concatenates between two and 32 operands into one exact
// destination. AllRequiredSpecified and AllRequiredRange preserve the authored
// option independently from its false default.
type StrcatCommand struct {
	Operands             []StrcatOperand
	Destination          string
	DestinationRange     Range
	AllRequired          bool
	AllRequiredSpecified bool
	AllRequiredRange     Range
	Range                Range
}

func (*StrcatCommand) command()             {}
func (*StrcatCommand) Name() string         { return "strcat" }
func (c *StrcatCommand) SourceRange() Range { return c.Range }

// AddInfoCommand projects the immutable resolved search boundaries,
// admission time, and public search-job identifier onto every row.
type AddInfoCommand struct {
	Range Range
}

func (*AddInfoCommand) command()             {}
func (*AddInfoCommand) Name() string         { return "addinfo" }
func (c *AddInfoCommand) SourceRange() Range { return c.Range }

// SpathCommand extracts explicitly addressed JSON scalar values from a current
// pipeline String field. Wildcard array selectors can produce a multivalue;
// Steps are the validated, constant location path. Input defaults to _raw and
// Output defaults to the decoded Path spelling.
type SpathCommand struct {
	Input       string
	InputRange  Range
	Output      string
	OutputRange Range
	Path        string
	PathRange   Range
	Steps       []splpath.Step
	Range       Range
}

func (*SpathCommand) command()             {}
func (*SpathCommand) Name() string         { return "spath" }
func (c *SpathCommand) SourceRange() Range { return c.Range }

// RenameAssignment moves one exact field name to another. Assignments retain
// source order because SPL applies multiple rename pairs from left to right.
type RenameAssignment struct {
	Source           string
	SourceRange      Range
	Destination      string
	DestinationRange Range
	Range            Range
}

// RenameCommand applies one or more exact field-to-field renames. Wildcard
// patterns are deliberately outside this compatibility slice.
type RenameCommand struct {
	Assignments []RenameAssignment
	Range       Range
}

func (*RenameCommand) command()             {}
func (*RenameCommand) Name() string         { return "rename" }
func (c *RenameCommand) SourceRange() Range { return c.Range }

// FieldsCommand includes or excludes fields.
type FieldsCommand struct {
	Fields         []string
	QuotedFields   []bool
	WildcardFields []bool
	FieldRanges    []Range
	Exclude        bool
	Range          Range
}

func (*FieldsCommand) command()             {}
func (*FieldsCommand) Name() string         { return "fields" }
func (c *FieldsCommand) SourceRange() Range { return c.Range }

// TableCommand selects an ordered result schema.
type TableCommand struct {
	Fields       []string
	QuotedFields []bool
	FieldRanges  []Range
	Range        Range
}

func (*TableCommand) command()             {}
func (*TableCommand) Name() string         { return "table" }
func (c *TableCommand) SourceRange() Range { return c.Range }

// SortValueMode controls the comparison interpretation for one sort key.
// Plain fields and auto(field) use SortValueModeAuto.
type SortValueMode uint8

const (
	SortValueModeAuto SortValueMode = iota
	SortValueModeString
	SortValueModeNumber
	SortValueModeIP
)

// SortField is one ordered sort key. FieldRange identifies only the exact
// field reference, while Range also includes any direction prefix or typed
// wrapper authored for the key.
type SortField struct {
	Field      string
	Quoted     bool
	Descending bool
	Mode       SortValueMode
	FieldRange Range
	Range      Range
}

// SortCommand establishes result order. LimitSpecified distinguishes an
// omitted count (Splunk's 10,000-row default) from explicit zero (unlimited).
type SortCommand struct {
	Limit          uint64
	LimitSpecified bool
	Fields         []SortField
	Range          Range
}

func (*SortCommand) command()             {}
func (*SortCommand) Name() string         { return "sort" }
func (c *SortCommand) SourceRange() Range { return c.Range }

// DedupField is one exact field in a deduplication key tuple.
type DedupField struct {
	Name  string
	Range Range
}

// DedupCommand retains the first Count rows for each complete key tuple in
// the ordering established by the preceding pipeline, or by SortBy when the
// sortby clause is present. Consecutive restricts removal to rows whose key
// tuple repeats the immediately preceding retained-eligible row.
type DedupCommand struct {
	Count       uint64
	Fields      []DedupField
	Consecutive bool
	SortBy      []SortField
	SortByRange Range
	Range       Range
}

func (*DedupCommand) command()             {}
func (*DedupCommand) Name() string         { return "dedup" }
func (c *DedupCommand) SourceRange() Range { return c.Range }

// LimitCommand implements head and tail.
type LimitCommand struct {
	CommandName string
	Count       uint64
	Range       Range
}

func (*LimitCommand) command()             {}
func (c *LimitCommand) Name() string       { return c.CommandName }
func (c *LimitCommand) SourceRange() Range { return c.Range }

// MaximumFrequencyFields shares the stats grouping-tuple ceiling because top
// and rare lower through that exact bounded aggregate path.
const MaximumFrequencyFields = MaximumStatsGroupFields

// FrequencyField shares the source-located stats grouping-field contract.
// The alias keeps top/rare call sites descriptive without creating a parallel
// tuple representation.
type FrequencyField = StatsGroupField

// TopCommand returns the most frequent scalar tuples for one or more fields,
// optionally per BY group. CountField and PercentField rename the generated
// outputs when non-empty; HideCount and HidePercent record showcount=false and
// showperc=false so a zero-value command keeps Splunk's default outputs.
type TopCommand struct {
	Fields       []FrequencyField
	By           []FrequencyField
	Limit        uint64
	CountField   string
	PercentField string
	HideCount    bool
	HidePercent  bool
	Range        Range
}

func (*TopCommand) command()             {}
func (*TopCommand) Name() string         { return "top" }
func (c *TopCommand) SourceRange() Range { return c.Range }

// RareCommand returns the least frequent scalar tuples for one or more fields.
// It has the same option surface as TopCommand.
type RareCommand struct {
	Fields       []FrequencyField
	By           []FrequencyField
	Limit        uint64
	CountField   string
	PercentField string
	HideCount    bool
	HidePercent  bool
	Range        Range
}

func (*RareCommand) command()             {}
func (*RareCommand) Name() string         { return "rare" }
func (c *RareCommand) SourceRange() Range { return c.Range }

// AggregateFunction identifies a supported stats aggregation.
type AggregateFunction uint8

const (
	// MaximumStatsMeasures is revalidated by every layer that accepts an AST or
	// logical plan so hand-built inputs cannot bypass the parser's resource
	// ceiling.
	MaximumStatsMeasures = 16
	// MaximumStatsGroupFields is the corresponding ceiling for one stats,
	// eventstats, or streamstats BY tuple.
	MaximumStatsGroupFields = 16
	// MaximumStatsPartitions is the documented stats partitions_limit. An
	// authored zero requests the configured default, which this compatibility
	// surface resolves to DefaultStatsPartitions; authored values above the
	// limit are retained in the AST and clamp to this effective maximum while
	// building the logical plan.
	MaximumStatsPartitions uint8 = 100
	DefaultStatsPartitions uint8 = 1
	DefaultStatsDelimiter        = " "
	// MaximumStatsSparklinePoints is the fixed publication ceiling carried by
	// every sparkline plan. Splunk's sparkline_maxsize defaults to list_maxsize,
	// whose pinned value is 100 for this compatibility surface.
	MaximumStatsSparklinePoints uint16 = 100
	// MaximumStreamStatsWindow is the largest explicit row window accepted by
	// the bounded streamstats compatibility surface. The backend separately
	// caps the complete input relation so window=0 remains exact rather than
	// silently degrading after an installation-specific memory threshold.
	MaximumStreamStatsWindow uint64 = 10_000
)

const (
	AggregateFunctionInvalid AggregateFunction = iota
	AggregateFunctionCount
	AggregateFunctionCountValues
	AggregateFunctionCountPredicate
	AggregateFunctionPercentile
	AggregateFunctionExactPercentile
	AggregateFunctionUpperPercentile
	AggregateFunctionMedian
	AggregateFunctionSum
	AggregateFunctionAverage
	AggregateFunctionRange
	AggregateFunctionSumSquares
	AggregateFunctionStandardDeviationSample
	AggregateFunctionStandardDeviationPopulation
	AggregateFunctionVarianceSample
	AggregateFunctionVariancePopulation
	AggregateFunctionDistinctCount
	AggregateFunctionEstimatedDistinctCount
	AggregateFunctionEstimatedDistinctCountError
	AggregateFunctionValues
	AggregateFunctionList
	AggregateFunctionMinimum
	AggregateFunctionMaximum
	AggregateFunctionMode
	AggregateFunctionFirst
	AggregateFunctionLast
	AggregateFunctionEarliest
	AggregateFunctionLatest
	AggregateFunctionEarliestTime
	AggregateFunctionLatestTime
	AggregateFunctionRate
)

// SparklineSpanKind distinguishes an omitted, search-range-derived span from
// one explicitly authored by the user. Invalid is reserved for forged or
// incomplete metadata; a parsed sparkline always uses Automatic or Explicit.
type SparklineSpanKind uint8

const (
	SparklineSpanKindInvalid SparklineSpanKind = iota
	SparklineSpanKindAutomatic
	SparklineSpanKindExplicit
)

// SparklineSpanUnit preserves the full documented sparkline span vocabulary.
// Calendar months deliberately remain distinct from fixed durations, and the
// subsecond units retain their authored scale for exact divisibility checks.
type SparklineSpanUnit uint8

const (
	SparklineSpanUnitInvalid SparklineSpanUnit = iota
	SparklineSpanUnitMicrosecond
	SparklineSpanUnitMillisecond
	SparklineSpanUnitCentisecond
	SparklineSpanUnitDecisecond
	SparklineSpanUnitSecond
	SparklineSpanUnitMinute
	SparklineSpanUnitHour
	SparklineSpanUnitDay
	SparklineSpanUnitMonth
)

// String returns the canonical SPL suffix for unit.
func (unit SparklineSpanUnit) String() string {
	switch unit {
	case SparklineSpanUnitMicrosecond:
		return "us"
	case SparklineSpanUnitMillisecond:
		return "ms"
	case SparklineSpanUnitCentisecond:
		return "cs"
	case SparklineSpanUnitDecisecond:
		return "ds"
	case SparklineSpanUnitSecond:
		return "s"
	case SparklineSpanUnitMinute:
		return "m"
	case SparklineSpanUnitHour:
		return "h"
	case SparklineSpanUnitDay:
		return "d"
	case SparklineSpanUnitMonth:
		return "mon"
	default:
		return ""
	}
}

// SparklineSpan is either an automatic marker or one source-located positive
// magnitude and documented time unit. Month is calendar-relative and must not
// be approximated as a fixed duration by downstream compilers.
type SparklineSpan struct {
	Kind      SparklineSpanKind
	Magnitude uint64
	Unit      SparklineSpanUnit
	Range     Range
}

// StatsSparkline is the nested time-series specification for one stats output.
// Function is restricted to the documented sparkline inventory. Input is empty
// only for an unscoped count or while InputGlob carries a wildcard input that
// logical planning has not yet expanded.
type StatsSparkline struct {
	Function AggregateFunction
	Input    string
	// InputGlob is mutually exclusive with Input and is consumed by stats
	// planning against a proven closed upstream schema.
	InputGlob *StatsFieldGlob
	// InputQuoted records the single-quoted exact-field spelling. Input always
	// contains the decoded logical name; quoting is syntax provenance rather
	// than a distinct runtime field type.
	InputQuoted bool
	InputRange  Range
	Span        SparklineSpan
	Range       Range
}

// StatsFieldGlob is one ordinary stats wc-field input. Pattern retains the
// exact authored wildcard spelling and Range identifies either that explicit
// input token or, for the deprecated bare-function form, the function token
// that implied "*". Logical planning expands this arm only when the upstream
// output schema is closed; no wildcard metadata crosses into a backend plan.
type StatsFieldGlob struct {
	Pattern  string
	Range    Range
	Implicit bool
}

// StatsAggregate is one source-located aggregate expression and its public
// output name.
type StatsAggregate struct {
	// Sparkline selects the distinct time-series arm of this ordered stats
	// measure. It is mutually exclusive with every ordinary aggregate field
	// below except the shared alias and source-range metadata.
	Sparkline *StatsSparkline
	Function  AggregateFunction
	Input     string
	// InputGlob is mutually exclusive with Input, InputExpression, Predicate,
	// and Sparkline. It is expanded to exact ordinary aggregates by logical
	// planning against a proven closed upstream schema.
	InputGlob *StatsFieldGlob
	// AliasGlob is the optional wc-field on the right side of AS. It must have
	// exactly as many '*' captures as either the ordinary InputGlob or the
	// Sparkline InputGlob and is consumed by the same closed-schema expansion
	// step.
	AliasGlob *StatsFieldGlob
	// InputQuoted records the single-quoted exact-field spelling for an
	// ordinary field input. It must be false for eval and predicate inputs.
	InputQuoted bool
	InputRange  Range
	// InputExpression is populated only for a supported field-taking
	// function(eval(<scalar expression>)) input. It is mutually exclusive with
	// the exact-field Input and the Boolean Predicate used by count(eval(...)).
	InputExpression ScalarExpr
	// Predicate is populated only for count(eval(<predicate>)). It remains
	// separate from Input so later layers cannot reinterpret a conditional
	// count as either a row count or count(field).
	Predicate WhereExpr
	// Percentile is the validated integer pN/percN, exactpercN, or upperpercN
	// suffix in [1, 99].
	Percentile    uint8
	Alias         string
	ExplicitAlias bool
	// AliasQuoted records a documented double-quoted AS output. Alias contains
	// the decoded output name. It is false for unquoted AS and every implicit
	// aggregate output.
	AliasQuoted bool
	// AliasSourceDerived identifies the deterministic Open Splunk default for
	// stats function(eval(...)) and count(eval(...)): Alias preserves the
	// authored aggregate invocation, except that control whitespace is replaced
	// with spaces so the result remains a safe output name. This is deliberately
	// separate from quoted AS provenance and remains governed by O-alias-schema.
	AliasSourceDerived bool
	// AliasWildcardDerived identifies the deterministic default name minted
	// while a planner expands one wc-field input (for example avg(Product Name)).
	// It preserves that complete spelling as one literal result column.
	AliasWildcardDerived bool
	Range                Range
	AliasRange           Range
}

// StatsGroupField is one source-located field in a stats BY clause.
type StatsGroupField struct {
	Name   string
	Quoted bool
	Range  Range
}

// StatsOptions preserves the authored stats command options and their source
// locations. Zero-valued unspecified metadata is distinct from explicitly
// authored false, zero, or an empty quoted delimiter.
type StatsOptions struct {
	Partitions          uint64
	PartitionsSpecified bool
	PartitionsRange     Range

	AllNumeric          bool
	AllNumericSpecified bool
	AllNumericRange     Range

	Delimiter          string
	DelimiterSpecified bool
	DelimiterRange     Range

	DeduplicateSplitValues          bool
	DeduplicateSplitValuesSpecified bool
	DeduplicateSplitValuesRange     Range
}

// StatsCommand transforms events into one row per distinct group (or one
// global row) and removes non-grouped event fields from the result schema.
type StatsCommand struct {
	Aggregates []StatsAggregate
	GroupBy    []StatsGroupField
	Options    StatsOptions
	Range      Range
}

func (*StatsCommand) command()             {}
func (*StatsCommand) Name() string         { return "stats" }
func (c *StatsCommand) SourceRange() Range { return c.Range }

// EventStatsCommand adds one aggregate to every input row, either globally or
// within an exact BY tuple. The bounded compatibility surface accepts
// argument-free AggregateFunctionCount, exact-field
// AggregateFunctionCountValues, true-only AggregateFunctionCountPredicate, or
// exact-field AggregateFunctionPercentile/AggregateFunctionMinimum/
// AggregateFunctionMaximum/AggregateFunctionEarliest/
// AggregateFunctionLatest/
// AggregateFunctionSum/AggregateFunctionAverage/
// AggregateFunctionDistinctCount/AggregateFunctionValues/
// AggregateFunctionList while retaining the singular aggregate's source
// locations through StatsAggregate.
type EventStatsCommand struct {
	Aggregate StatsAggregate
	GroupBy   []StatsGroupField
	Range     Range
}

func (*EventStatsCommand) command()             {}
func (*EventStatsCommand) Name() string         { return "eventstats" }
func (c *EventStatsCommand) SourceRange() Range { return c.Range }

// StreamStatsCommand appends one running row count, exact-field occurrence
// count, true-only predicate count, exact-field numeric sum or average,
// exact-field mixed extremum, or exact-field chronological value to every input
// row. Aggregate carries the input or predicate, source locations, and alias
// representation. Current controls whether the present row contributes,
// Window is zero for the complete bounded prefix, and Global is meaningful only
// for a positive window.
// GlobalSpecified distinguishes Splunk's default global window from the
// explicit global=false required by the supported grouped finite-window form.
type StreamStatsCommand struct {
	Aggregate        StatsAggregate
	Current          bool
	CurrentSpecified bool
	CurrentRange     Range
	Window           uint64
	WindowSpecified  bool
	WindowRange      Range
	Global           bool
	GlobalSpecified  bool
	GlobalRange      Range
	GroupBy          []StatsGroupField
	Range            Range
}

func (*StreamStatsCommand) command()             {}
func (*StreamStatsCommand) Name() string         { return "streamstats" }
func (c *StreamStatsCommand) SourceRange() Range { return c.Range }

// TimeSpanUnit identifies the fixed-duration units shared by the initial bin
// and timechart compatibility slices. Calendar and subsecond spans require
// separate alignment semantics and are rejected rather than approximated.
type TimeSpanUnit uint8

const (
	TimeSpanUnitInvalid TimeSpanUnit = iota
	TimeSpanUnitSecond
	TimeSpanUnitMinute
	TimeSpanUnitHour
)

// String returns the canonical SPL suffix for unit.
func (unit TimeSpanUnit) String() string {
	switch unit {
	case TimeSpanUnitSecond:
		return "s"
	case TimeSpanUnitMinute:
		return "m"
	case TimeSpanUnitHour:
		return "h"
	default:
		return ""
	}
}

// TimeSpan is one source-located positive fixed-duration span.
type TimeSpan struct {
	Magnitude uint64
	Unit      TimeSpanUnit
	Range     Range
}

// BinSpanKind distinguishes unitless numeric widths from fixed-duration
// widths. The planner can then apply field-aware semantics without recovering
// information discarded by parsing.
type BinSpanKind uint8

const (
	BinSpanKindInvalid BinSpanKind = iota
	BinSpanKindNumeric
	BinSpanKindTime
)

// BinSpan is one source-located positive bin width. Unit is set only for
// BinSpanKindTime.
type BinSpan struct {
	Kind      BinSpanKind
	Magnitude uint64
	Unit      TimeSpanUnit
	Range     Range
}

// BinCommand buckets one exact field into fixed-width intervals. Output is the
// source field for in-place binning or the explicit AS destination. CommandName
// preserves whether the user selected bin or its bucket alias while
// normalizing command spelling to lower case.
type BinCommand struct {
	CommandName string
	Field       string
	FieldRange  Range
	Output      string
	OutputRange Range
	Span        BinSpan
	Range       Range
}

func (*BinCommand) command()             {}
func (c *BinCommand) Name() string       { return c.CommandName }
func (c *BinCommand) SourceRange() Range { return c.Range }

// TimechartCommand produces one aggregate series over fixed _time buckets.
// Aggregate retains the same source-located representation used by stats.
// Argument-free count, exact-field count, percentile, sum, and average may
// optionally use SplitBy for a bounded runtime-wide relation. Every unsplit
// form has a static _time/aggregate-output schema, and every form is a terminal
// transforming command.
type TimechartCommand struct {
	Span      TimeSpan
	Aggregate StatsAggregate
	SplitBy   *StatsGroupField
	Range     Range
}

func (*TimechartCommand) command()             {}
func (*TimechartCommand) Name() string         { return "timechart" }
func (c *TimechartCommand) SourceRange() Range { return c.Range }

// ChartCommand produces a bounded runtime-wide pivot: one row per distinct
// value of the row split and one runtime series per retained value of the
// column split. Aggregate retains the same source-located representation used
// by stats and timechart. The bounded compatibility surface supports one
// argument-free count or one exact-field count/percentile/sum/average plus one
// or two distinct split fields, and is a terminal transforming command. With a
// single split field the command is the stats BY table of that field.
type ChartCommand struct {
	Aggregate StatsAggregate
	// Over is Splunk's row-split field: the first output column.
	Over StatsGroupField
	// SplitBy is Splunk's column-split field: its runtime values become the
	// remaining output column names. It is zero-valued for the single-split
	// forms "OVER <row>" and "BY <row>", whose only other column is the
	// aggregate output.
	SplitBy StatsGroupField
	// OverSpelledOver records whether the user wrote OVER <row> BY <column>
	// rather than the equivalent BY <row>, <column>. Both spellings describe
	// the same pivot and must lower to identical plans; the flag exists only
	// so diagnostics and source round-trips stay exact.
	OverSpelledOver bool
	Range           Range
}

func (*ChartCommand) command()             {}
func (*ChartCommand) Name() string         { return "chart" }
func (c *ChartCommand) SourceRange() Range { return c.Range }

// SingleSplit reports whether the command names only a row split field and
// therefore produces the stats BY table instead of a runtime-named pivot.
func (c *ChartCommand) SingleSplit() bool { return c.SplitBy == (StatsGroupField{}) }

// MaximumDiagnosticSuggestions bounds the suggestion list carried by any
// diagnostic. Stored search history, knowledge validation results, and the
// suggestion API all reject diagnostics with more suggestions, so the parser
// must never emit a longer list.
const MaximumDiagnosticSuggestions = 32

// Diagnostic is a stable, source-located parse or compatibility error.
type Diagnostic struct {
	Code        string
	Message     string
	Range       Range
	Suggestions []string
}

// Error implements error.
func (d *Diagnostic) Error() string {
	return fmt.Sprintf("%s at line %d, column %d: %s", d.Code, d.Range.Start.Line, d.Range.Start.Column, d.Message)
}
