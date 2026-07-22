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

// LimitCommand implements head and tail.
type LimitCommand struct {
	CommandName string
	Count       uint64
	Range       Range
}

func (*LimitCommand) command()             {}
func (c *LimitCommand) Name() string       { return c.CommandName }
func (c *LimitCommand) SourceRange() Range { return c.Range }

// AggregateFunction identifies a supported stats aggregation. The initial
// compatibility slice intentionally models only an exact row count; accepting
// other function spellings without their full SPL semantics would be unsafe.
type AggregateFunction uint8

const (
	AggregateFunctionInvalid AggregateFunction = iota
	AggregateFunctionCount
)

// StatsAggregate is one source-located aggregate expression and its public
// output name.
type StatsAggregate struct {
	Function   AggregateFunction
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
	Aggregate StatsAggregate
	GroupBy   []StatsGroupField
	Range     Range
}

func (*StatsCommand) command()             {}
func (*StatsCommand) Name() string         { return "stats" }
func (c *StatsCommand) SourceRange() Range { return c.Range }

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
