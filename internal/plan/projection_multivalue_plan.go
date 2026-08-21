package plan

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	// MaximumMakeMVMembersPerRow is the hard construction ceiling. It matches
	// mvexpand's per-input-row ceiling so every successfully constructed value
	// can be consumed without a command-local compatibility surprise.
	MaximumMakeMVMembersPerRow uint64 = spl.MaximumMVExpandLimit

	// MaximumMakeMVMemberBytesPerRow bounds the sum of raw member bytes retained
	// by one output cell.
	MaximumMakeMVMemberBytesPerRow uint64 = 1 << 20

	// MaximumMakeMVMembersPerResult and MaximumMakeMVMemberBytesPerResult bound
	// a complete pre-downstream command result. Violations fail atomically.
	MaximumMakeMVMembersPerResult     uint64 = 100_000
	MaximumMakeMVMemberBytesPerResult uint64 = 8 << 20
	// MaximumMakeMVRetainedBytesPerResult charges the complete public relation
	// retained at the makemv stage, not only the constructed member payload.
	MaximumMakeMVRetainedBytesPerResult uint64 = 64 << 20

	// MaximumMVExpandRowsPerStage is checked against the complete stage output,
	// before any later filter or limit. At most MaximumMVExpandStages are
	// admitted.
	MaximumMVExpandRowsPerStage uint64 = 10_000
	MaximumMVExpandStages       int    = 2
	// The cumulative ceiling is intentionally tighter than two full stage
	// ceilings, so repeated moderate expansions can trip an independent query
	// budget before either individual stage reaches its maximum.
	MaximumMVExpandRowsPerQuery uint64 = 15_000

	// MaximumMVExpandRetainedBytesPerStage counts the canonical public-row JSON
	// bytes copied by one expansion stage. It is independent of final result
	// paging and execution memory settings.
	MaximumMVExpandRetainedBytesPerStage uint64 = 64 << 20
)

// FillNull replaces missing or explicit-null exact fields with one String
// literal. It is a simultaneous projection over the pre-command row.
type FillNull struct {
	Fields []FieldRef
	Value  string
	Range  spl.Range
}

func (*FillNull) operator()                 {}
func (*FillNull) LogicalName() string       { return "FillNull" }
func (op *FillNull) SourceRange() spl.Range { return op.Range }

// RowTotal writes a Float64 total over all eligible numeric input fields.
// Missing, null, Boolean, multivalue, nonnumeric, and non-finite inputs do not
// contribute; an all-ineligible row receives zero.
type RowTotal struct {
	Inputs      []FieldRef
	Output      string
	OutputRange spl.Range
	Range       spl.Range
}

func (*RowTotal) operator()                 {}
func (*RowTotal) LogicalName() string       { return "RowTotal" }
func (op *RowTotal) SourceRange() spl.Range { return op.Range }

// OrderedDelta subtracts the value Previous established rows earlier from the
// current value. Output is a literal public column name: the parser-generated
// default contains parentheses and is intentionally not a command field path.
type OrderedDelta struct {
	Input       FieldRef
	Output      string
	OutputRange spl.Range
	Previous    uint64
	Range       spl.Range
}

func (*OrderedDelta) operator()                 {}
func (*OrderedDelta) LogicalName() string       { return "OrderedDelta" }
func (op *OrderedDelta) SourceRange() spl.Range { return op.Range }

// MakeMultivalue splits one present scalar String. Missing and explicit-null
// inputs retain their state; incompatible present runtime types fail atomically.
type MakeMultivalue struct {
	Input      FieldRef
	Delimiter  string
	AllowEmpty bool
	Range      spl.Range
}

func (*MakeMultivalue) operator()                 {}
func (*MakeMultivalue) LogicalName() string       { return "MakeMultivalue" }
func (op *MakeMultivalue) SourceRange() spl.Range { return op.Range }

// ExpandMultivalue emits one row per selected member in member order. Limit is
// zero when the user selected no smaller limit; the hard ceiling still applies.
// QueryOrdinal is one-based and bounded by MaximumMVExpandStages.
type ExpandMultivalue struct {
	Input        FieldRef
	Limit        uint64
	QueryOrdinal uint8
	Range        spl.Range
}

func (*ExpandMultivalue) operator()                 {}
func (*ExpandMultivalue) LogicalName() string       { return "ExpandMultivalue" }
func (op *ExpandMultivalue) SourceRange() spl.Range { return op.Range }

// NoMultivalue applies flat newline presentation metadata to one native
// multivalue field. The underlying typed value and row cardinality remain
// unchanged for downstream planning and execution.
type NoMultivalue struct {
	Input FieldRef
	Range spl.Range
}

func (*NoMultivalue) operator()                 {}
func (*NoMultivalue) LogicalName() string       { return "NoMultivalue" }
func (op *NoMultivalue) SourceRange() spl.Range { return op.Range }

func buildFillNull(command *spl.FillNullCommand) (*FillNull, error) {
	if command == nil || len(command.Fields) < 1 ||
		len(command.Fields) > spl.MaximumExplicitProjectionFields ||
		!utf8.ValidString(command.Value) {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "fillnull command metadata is invalid",
		}
	}
	fields := make([]FieldRef, 0, len(command.Fields))
	seen := make(map[string]struct{}, len(command.Fields))
	for _, field := range command.Fields {
		if !spl.IsExactUnquotedFieldName(field.Name) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_FILLNULL_SYNTAX",
				Message: "fillnull requires exact unquoted fields",
				Range:   field.Range,
			}
		}
		if _, duplicate := seen[field.Name]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_FILLNULL_SYNTAX",
				Message: fmt.Sprintf("fillnull field %q is repeated", field.Name),
				Range:   field.Range,
			}
		}
		seen[field.Name] = struct{}{}
		resolved, err := ResolveField(field.Name, field.Range)
		if err != nil {
			return nil, err
		}
		fields = append(fields, resolved)
	}
	return &FillNull{Fields: fields, Value: strings.Clone(command.Value), Range: command.Range}, nil
}

func buildRowTotal(command *spl.AddTotalsCommand) (*RowTotal, error) {
	if command == nil || len(command.Fields) < 1 ||
		len(command.Fields) > spl.MaximumExplicitProjectionFields ||
		!spl.IsExactUnquotedFieldName(command.Output) {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "addtotals command metadata is invalid",
		}
	}
	inputs := make([]FieldRef, 0, len(command.Fields))
	seen := make(map[string]struct{}, len(command.Fields))
	for _, field := range command.Fields {
		if !spl.IsExactUnquotedFieldName(field.Name) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ADDTOTALS_SYNTAX",
				Message: "addtotals requires exact unquoted fields",
				Range:   field.Range,
			}
		}
		if _, duplicate := seen[field.Name]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ADDTOTALS_SYNTAX",
				Message: fmt.Sprintf("addtotals field %q is repeated", field.Name),
				Range:   field.Range,
			}
		}
		seen[field.Name] = struct{}{}
		resolved, err := ResolveField(field.Name, field.Range)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, resolved)
	}
	if _, err := ResolveField(command.Output, command.OutputRange); err != nil {
		return nil, err
	}
	return &RowTotal{
		Inputs:      inputs,
		Output:      strings.Clone(command.Output),
		OutputRange: command.OutputRange,
		Range:       command.Range,
	}, nil
}

func buildOrderedDelta(command *spl.DeltaCommand) (*OrderedDelta, error) {
	if command == nil || !spl.IsExactUnquotedFieldName(command.Field) ||
		!validDeltaCommandOutput(command) ||
		command.Previous < 1 || command.Previous > spl.MaximumStreamStatsWindow {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "delta command metadata is invalid",
		}
	}
	input, err := ResolveField(command.Field, command.FieldRange)
	if err != nil {
		return nil, err
	}
	if _, err := ResolveField(command.Output, command.OutputRange); err != nil {
		return nil, err
	}
	return &OrderedDelta{
		Input:       input,
		Output:      strings.Clone(command.Output),
		OutputRange: command.OutputRange,
		Previous:    command.Previous,
		Range:       command.Range,
	}, nil
}

func validDeltaCommandOutput(command *spl.DeltaCommand) bool {
	if command.OutputDefault {
		return command.Output == "delta("+command.Field+")" &&
			command.OutputRange == command.FieldRange
	}
	return spl.IsExactUnquotedFieldName(command.Output)
}

// validOrderedDeltaOutput admits either the parser's deterministic default or
// one exact command destination. An exact upstream schema may legitimately
// expose the public field named "fields"; stats-alias rules are too strict for
// that command-field spelling.
func validOrderedDeltaOutput(input, output string) bool {
	return spl.IsExactUnquotedFieldName(output) || output == "delta("+input+")"
}

func buildMakeMultivalue(command *spl.MakeMVCommand) (*MakeMultivalue, error) {
	if command == nil || !spl.IsExactUnquotedFieldName(command.Field) ||
		strings.HasPrefix(command.Field, "_") || command.Delimiter == "" ||
		!utf8.ValidString(command.Delimiter) ||
		len(command.Delimiter) > spl.MaximumMakeMVDelimiterBytes {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "makemv command metadata is invalid",
		}
	}
	input, err := ResolveField(command.Field, command.FieldRange)
	if err != nil {
		return nil, err
	}
	return &MakeMultivalue{
		Input:      input,
		Delimiter:  strings.Clone(command.Delimiter),
		AllowEmpty: command.AllowEmpty,
		Range:      command.Range,
	}, nil
}

func buildExpandMultivalue(
	command *spl.MVExpandCommand,
	queryOrdinal int,
) (*ExpandMultivalue, error) {
	if command == nil || !spl.IsExactUnquotedFieldName(command.Field) ||
		strings.HasPrefix(command.Field, "_") ||
		command.Limit > spl.MaximumMVExpandLimit || queryOrdinal < 1 ||
		queryOrdinal > MaximumMVExpandStages {
		code := "SPL_INVALID_QUERY"
		message := "mvexpand command metadata is invalid"
		if queryOrdinal > MaximumMVExpandStages {
			code = "SPL_QUERY_TOO_COMPLEX"
			message = fmt.Sprintf("search contains more than %d mvexpand stages", MaximumMVExpandStages)
		}
		return nil, &Diagnostic{Code: code, Message: message, Range: command.Range}
	}
	input, err := ResolveField(command.Field, command.FieldRange)
	if err != nil {
		return nil, err
	}
	return &ExpandMultivalue{
		Input:        input,
		Limit:        command.Limit,
		QueryOrdinal: uint8(queryOrdinal), // #nosec G115 -- checked against a two-stage ceiling.
		Range:        command.Range,
	}, nil
}

func buildNoMultivalue(command *spl.NoMVCommand) (*NoMultivalue, error) {
	if command == nil || !spl.IsExactUnquotedFieldName(command.Field) ||
		strings.HasPrefix(command.Field, "_") {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "nomv command metadata is invalid",
			Range:   safeSPLNodeRange(command),
		}
	}
	input, err := ResolveField(command.Field, command.FieldRange)
	if err != nil {
		return nil, err
	}
	return &NoMultivalue{Input: input, Range: command.Range}, nil
}
