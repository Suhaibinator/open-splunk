package spl

import (
	"fmt"

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
}

// SourceRange implements Node.
func (q *Query) SourceRange() Range { return q.Range }

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
	Field string
	Range Range
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
)

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
	ScalarFunctionMatch
	ScalarFunctionLike
	ScalarFunctionNow
	ScalarFunctionStrftime
	ScalarFunctionStrptime
	ScalarFunctionRelativeTime
	ScalarFunctionConcat
	ScalarFunctionCount
)

// ReturnsBoolean reports the atomic result trait shared by parser, planner,
// and backend consumer checks. Composite coalesce/if/case expressions still
// require branch-aware analysis.
func (function ScalarFunction) ReturnsBoolean() bool {
	switch function {
	case ScalarFunctionIsNull, ScalarFunctionIsNotNull, ScalarFunctionMatch,
		ScalarFunctionLike:
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

// SpathCommand extracts one explicitly addressed JSON value from a current
// pipeline String field. Steps are the validated, constant location path;
// Input defaults to _raw and Output defaults to the decoded Path spelling.
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
	Fields  []string
	Exclude bool
	Range   Range
}

func (*FieldsCommand) command()             {}
func (*FieldsCommand) Name() string         { return "fields" }
func (c *FieldsCommand) SourceRange() Range { return c.Range }

// TableCommand selects an ordered result schema.
type TableCommand struct {
	Fields []string
	Range  Range
}

func (*TableCommand) command()             {}
func (*TableCommand) Name() string         { return "table" }
func (c *TableCommand) SourceRange() Range { return c.Range }

// SortField is one ordered sort key.
type SortField struct {
	Field      string
	Descending bool
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
// the ordering established by the preceding pipeline.
type DedupCommand struct {
	Count  uint64
	Fields []DedupField
	Range  Range
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

// TopCommand returns the most frequent scalar tuples for one or more fields.
// Its compatibility slice keeps Splunk's default count and percent output
// fields while rejecting BY and output-renaming options.
type TopCommand struct {
	Fields []FrequencyField
	Limit  uint64
	Range  Range
}

func (*TopCommand) command()             {}
func (*TopCommand) Name() string         { return "top" }
func (c *TopCommand) SourceRange() Range { return c.Range }

// RareCommand returns the least frequent scalar tuples for one or more fields.
// It has the same deliberately bounded compatibility surface as TopCommand.
type RareCommand struct {
	Fields []FrequencyField
	Limit  uint64
	Range  Range
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
	// MaximumStatsGroupFields is the corresponding ceiling for one stats or
	// eventstats BY tuple.
	MaximumStatsGroupFields = 16
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
	AggregateFunctionSum
	AggregateFunctionAverage
	AggregateFunctionDistinctCount
	AggregateFunctionValues
	AggregateFunctionList
	AggregateFunctionMinimum
	AggregateFunctionMaximum
	AggregateFunctionEarliest
	AggregateFunctionLatest
)

// StatsAggregate is one source-located aggregate expression and its public
// output name.
type StatsAggregate struct {
	Function   AggregateFunction
	Input      string
	InputRange Range
	// Predicate is populated only for count(eval(<predicate>)). It remains
	// separate from Input so later layers cannot reinterpret a conditional
	// count as either a row count or count(field).
	Predicate WhereExpr
	// Percentile is the validated integer pN/percN suffix in [1, 99].
	Percentile    uint8
	Alias         string
	ExplicitAlias bool
	Range         Range
	AliasRange    Range
}

// StatsGroupField is one source-located field in a stats BY clause.
type StatsGroupField struct {
	Name  string
	Range Range
}

// StatsCommand transforms events into one row per distinct group (or one
// global row) and removes non-grouped event fields from the result schema.
type StatsCommand struct {
	Aggregates []StatsAggregate
	GroupBy    []StatsGroupField
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

// StreamStatsCommand appends one running row count to every input row. The
// first compatibility slice deliberately accepts only argument-free count;
// Aggregate still carries its source locations and alias using the common
// stats representation. Current controls whether the present row contributes,
// Window is zero for the complete bounded prefix, and Global is meaningful
// only for a positive window. GlobalSpecified distinguishes Splunk's default
// global window from the explicit global=false required by the supported
// grouped finite-window form.
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
// Argument-free count, sum, and average may optionally use SplitBy for a
// bounded runtime-wide relation. Percentile remains deliberately unsplit and
// therefore has a static _time/aggregate-output schema. Every form is a
// terminal transforming command.
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
// argument-free count or one exact-field percentile/sum/average plus two
// distinct split fields, and is a terminal transforming command.
type ChartCommand struct {
	Aggregate StatsAggregate
	// Over is Splunk's row-split field: the first output column.
	Over StatsGroupField
	// SplitBy is Splunk's column-split field: its runtime values become the
	// remaining output column names.
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
