package spl

import "reflect"

// ResultKind is the backend-neutral relation kind produced by an SPL
// pipeline.
type ResultKind uint8

const (
	ResultKindInvalid ResultKind = iota
	ResultKindEvents
	ResultKindStatistics
	ResultKindTimeSeries
)

// ResultShape describes the public relation shape implied by an SPL pipeline.
// RuntimeNamedColumns is true for wide pivots whose output column names after
// the first come from runtime field values.
type ResultShape struct {
	Kind                ResultKind
	RuntimeNamedColumns bool
}

// ClassifyResultShape applies transforming-command transitions in pipeline
// order. Every known command is classified explicitly so a future AST command
// fails closed until its result semantics are defined here.
func ClassifyResultShape(query *Query) ResultShape {
	if query == nil {
		return ResultShape{}
	}
	result := ResultShape{Kind: ResultKindEvents}
	for _, command := range query.Commands {
		if command == nil || isNilCommand(command) {
			return ResultShape{}
		}
		switch command.(type) {
		case *SearchCommand,
			*WhereCommand,
			*EvalCommand,
			*RexCommand,
			*SpathCommand,
			*RenameCommand,
			*FieldsCommand,
			*SortCommand,
			*DedupCommand,
			*LimitCommand,
			*BinCommand:
			// These commands preserve the current relation shape.
		case *TableCommand, *StatsCommand, *TopCommand, *RareCommand:
			result = ResultShape{Kind: ResultKindStatistics}
		case *TimechartCommand:
			result = ResultShape{
				Kind:                ResultKindTimeSeries,
				RuntimeNamedColumns: true,
			}
		case *ChartCommand:
			result = ResultShape{
				Kind:                ResultKindStatistics,
				RuntimeNamedColumns: true,
			}
		default:
			return ResultShape{}
		}
	}
	return result
}

func isNilCommand(command Command) bool {
	value := reflect.ValueOf(command)
	return value.Kind() == reflect.Pointer && value.IsNil()
}
