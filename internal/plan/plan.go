// Package plan defines the backend-neutral logical query plan produced from
// supported SPL. Security scope is carried by Scan and cannot be expressed or
// weakened by user-authored pipeline operators.
package plan

import (
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// Query is an ordered logical operator pipeline.
type Query struct {
	Operators        []Operator
	EffectiveIndexes []string
	OutputFields     []string
}

// Operator is one logical pipeline stage.
type Operator interface {
	LogicalName() string
	SourceRange() spl.Range
	operator()
}

// Scan is the mandatory, authorization-constrained event source.
type Scan struct {
	TenantID        string
	Indexes         []string
	Earliest        time.Time
	Latest          time.Time
	IndexTimeCutoff time.Time
	// VisibilityCutoff is the highest storage commit sequence visible to the
	// search job. Zero is a valid cutoff for a snapshot of an empty table.
	VisibilityCutoff uint64
	Range            spl.Range
}

func (*Scan) operator()                 {}
func (*Scan) LogicalName() string       { return "Scan" }
func (op *Scan) SourceRange() spl.Range { return op.Range }

// Filter retains rows matching Expression.
type Filter struct {
	Expression Expression
	Range      spl.Range
}

func (*Filter) operator()                 {}
func (*Filter) LogicalName() string       { return "Filter" }
func (op *Filter) SourceRange() spl.Range { return op.Range }

// ProjectMode identifies inclusion, exclusion, and table projections.
type ProjectMode uint8

const (
	ProjectModeInvalid ProjectMode = iota
	ProjectModeInclude
	ProjectModeExclude
	ProjectModeTable
)

// Project changes the visible result fields. Table also establishes exact
// field order and discards fields not selected by the command.
type Project struct {
	Mode   ProjectMode
	Fields []FieldRef
	Range  spl.Range
}

func (*Project) operator()                 {}
func (*Project) LogicalName() string       { return "Project" }
func (op *Project) SourceRange() spl.Range { return op.Range }

// AggregateFunction identifies an exact backend-neutral aggregation.
type AggregateFunction uint8

const (
	AggregateFunctionInvalid AggregateFunction = iota
	AggregateFunctionCountRows
)

// AggregateMeasure is one aggregate output column.
type AggregateMeasure struct {
	Function AggregateFunction
	Output   string
}

// Aggregate transforms its input into one row per distinct GroupBy tuple, or
// one global row when GroupBy is empty. Only grouping fields and measures
// remain visible after this stage.
type Aggregate struct {
	GroupBy  []FieldRef
	Measures []AggregateMeasure
	Range    spl.Range
}

func (*Aggregate) operator()                 {}
func (*Aggregate) LogicalName() string       { return "Aggregate" }
func (op *Aggregate) SourceRange() spl.Range { return op.Range }

// WindowFunction identifies a row-preserving calculation over the complete
// input relation.
type WindowFunction uint8

const (
	WindowFunctionInvalid WindowFunction = iota
	WindowFunctionPercentOfTotal
)

// Window appends one derived output field without changing row cardinality.
// PercentOfTotal is evaluated before any downstream top-N limit.
type Window struct {
	Function WindowFunction
	Input    FieldRef
	Output   string
	Range    spl.Range
}

func (*Window) operator()                 {}
func (*Window) LogicalName() string       { return "Window" }
func (op *Window) SourceRange() spl.Range { return op.Range }

// SortValueMode controls how one field is interpreted for ordering.
type SortValueMode uint8

const (
	SortValueModeAuto SortValueMode = iota
	SortValueModeLexical
)

// SortKey is one logical sort key.
type SortKey struct {
	Field      FieldRef
	Descending bool
	Mode       SortValueMode
}

// Sort establishes row ordering. Limit is zero when the sort command did not
// request Splunk's optional top-N optimization.
type Sort struct {
	Keys  []SortKey
	Limit uint64
	Range spl.Range
}

func (*Sort) operator()                 {}
func (*Sort) LogicalName() string       { return "Sort" }
func (op *Sort) SourceRange() spl.Range { return op.Range }

// Limit implements head or tail. FromEnd is true for tail.
type Limit struct {
	Count   uint64
	FromEnd bool
	Range   spl.Range
}

func (*Limit) operator()                 {}
func (*Limit) LogicalName() string       { return "Limit" }
func (op *Limit) SourceRange() spl.Range { return op.Range }

// Expression is a typed logical predicate.
type Expression interface {
	SourceRange() spl.Range
	expression()
}

// BooleanOp combines predicates.
type BooleanOp uint8

const (
	BooleanOpInvalid BooleanOp = iota
	BooleanOpAnd
	BooleanOpOr
)

// BooleanExpression combines two predicates.
type BooleanExpression struct {
	Op    BooleanOp
	Left  Expression
	Right Expression
	Range spl.Range
}

func (*BooleanExpression) expression()              {}
func (e *BooleanExpression) SourceRange() spl.Range { return e.Range }

// NotExpression negates a predicate. Compilers must preserve missing-field
// semantics rather than relying on SQL's three-valued NOT NULL behavior.
type NotExpression struct {
	Operand Expression
	Range   spl.Range
}

func (*NotExpression) expression()              {}
func (e *NotExpression) SourceRange() spl.Range { return e.Range }

// TextExpression searches the canonical raw representation.
type TextExpression struct {
	Value    string
	Quoted   bool
	Wildcard bool
	Range    spl.Range
}

func (*TextExpression) expression()              {}
func (e *TextExpression) SourceRange() spl.Range { return e.Range }

// ComparisonOp compares one field and typed literal.
type ComparisonOp uint8

const (
	ComparisonOpInvalid ComparisonOp = iota
	ComparisonOpEqual
	ComparisonOpNotEqual
	ComparisonOpLess
	ComparisonOpLessEqual
	ComparisonOpGreater
	ComparisonOpGreaterEqual
)

// ValueKind is a backend-neutral scalar literal type.
type ValueKind uint8

const (
	ValueKindInvalid ValueKind = iota
	ValueKindNull
	ValueKindString
	ValueKindInt64
	ValueKindUint64
	ValueKindFloat64
	ValueKindBool
)

// Value contains exactly one scalar selected by Kind.
type Value struct {
	Kind       ValueKind
	String     string
	Int64      int64
	Uint64     uint64
	Float64    float64
	Bool       bool
	Quoted     bool
	SourceText string
}

// FieldRef is a canonical field or a deterministic dotted dynamic path.
type FieldRef struct {
	Name      string
	Canonical bool
	Path      []string
}

// ComparisonExpression compares a field with one scalar.
type ComparisonExpression struct {
	Field FieldRef
	Op    ComparisonOp
	Value Value
	Range spl.Range
}

func (*ComparisonExpression) expression()              {}
func (e *ComparisonExpression) SourceRange() spl.Range { return e.Range }

// Diagnostic is a source-located semantic/planning error.
type Diagnostic struct {
	Code        string
	Message     string
	Range       spl.Range
	Suggestions []string
}

// Error implements error.
func (d *Diagnostic) Error() string {
	return fmt.Sprintf("%s at line %d, column %d: %s", d.Code, d.Range.Start.Line, d.Range.Start.Column, d.Message)
}
