package spl

import "fmt"

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
	ScalarFunctionInvalid ScalarFunction = iota
	ScalarFunctionToNumber
	ScalarFunctionReplace
)

// ScalarCallExpr invokes a supported eval function. Function names are
// resolved by the parser so no user-authored identifier reaches a backend.
type ScalarCallExpr struct {
	Function  ScalarFunction
	Arguments []ScalarExpr
	Range     Range
}

func (*ScalarCallExpr) scalarExpression()    {}
func (e *ScalarCallExpr) SourceRange() Range { return e.Range }

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

// TopCommand returns the most frequent scalar values for one field. The
// initial compatibility slice keeps Splunk's default count and percent output
// fields while rejecting multi-field, BY, and output-renaming options.
type TopCommand struct {
	Field      string
	FieldRange Range
	Limit      uint64
	Range      Range
}

func (*TopCommand) command()             {}
func (*TopCommand) Name() string         { return "top" }
func (c *TopCommand) SourceRange() Range { return c.Range }

// RareCommand returns the least frequent scalar values for one field. It has
// the same deliberately bounded compatibility surface as TopCommand.
type RareCommand struct {
	Field      string
	FieldRange Range
	Limit      uint64
	Range      Range
}

func (*RareCommand) command()             {}
func (*RareCommand) Name() string         { return "rare" }
func (c *RareCommand) SourceRange() Range { return c.Range }

// AggregateFunction identifies a supported stats aggregation.
type AggregateFunction uint8

const (
	AggregateFunctionInvalid AggregateFunction = iota
	AggregateFunctionCount
	AggregateFunctionP95
	AggregateFunctionSum
	AggregateFunctionAverage
)

// StatsAggregate is one source-located aggregate expression and its public
// output name.
type StatsAggregate struct {
	Function   AggregateFunction
	Input      string
	InputRange Range
	Alias      string
	Range      Range
	AliasRange Range
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

// TimeSpanUnit identifies the fixed-duration units supported by the initial
// timechart compatibility slice. Calendar and subsecond spans require separate
// alignment semantics and are rejected rather than approximated.
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

// TimechartCommand produces a runtime-wide count series over fixed _time
// buckets. The initial compatibility slice supports one split field and is a
// terminal transforming command.
type TimechartCommand struct {
	Span           TimeSpan
	Function       AggregateFunction
	AggregateRange Range
	SplitBy        StatsGroupField
	Range          Range
}

func (*TimechartCommand) command()             {}
func (*TimechartCommand) Name() string         { return "timechart" }
func (c *TimechartCommand) SourceRange() Range { return c.Range }

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
