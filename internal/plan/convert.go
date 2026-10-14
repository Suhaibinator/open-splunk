package plan

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// ConvertAssignment is one exact, ordered field conversion after wildcard expansion.
type ConvertAssignment struct {
	Function spl.ConvertFunction
	Input    FieldRef
	Output   FieldRef
	Range    spl.Range
}

// Convert preserves row identity and established order, including for whole-column auto.
type Convert struct {
	Assignments     []ConvertAssignment
	TimeFormat      string
	TimeFormatRange spl.Range
	Range           spl.Range
}

func (*Convert) operator()                 {}
func (*Convert) LogicalName() string       { return "Convert" }
func (op *Convert) SourceRange() spl.Range { return op.Range }

func convertWriteLimit(sourceRange spl.Range) *Diagnostic {
	return &Diagnostic{Code: "SPL_QUERY_TOO_COMPLEX", Message: fmt.Sprintf("convert expands to more than %d field writes", spl.MaximumConvertWrites), Range: sourceRange}
}

// ConvertFieldExcluded applies only to wildcard writes; explicit clauses remain authoritative.
func ConvertFieldExcluded(command *spl.ConvertCommand, name string) bool {
	if command == nil {
		return false
	}
	for _, clause := range command.Clauses {
		if clause.Function == spl.ConvertFunctionNone && (clause.Input == name || spl.MatchConvertFieldGlob(clause.Input, name)) {
			return true
		}
	}
	return false
}

func validateConvertFormat(command *spl.ConvertCommand) error {
	if err := spl.ValidateConvertTimeFormat(command); err != nil {
		if diagnostic, ok := errors.AsType[*spl.Diagnostic](err); ok {
			return &Diagnostic{Code: diagnostic.Code, Message: diagnostic.Message, Range: diagnostic.Range, Suggestions: slices.Clone(diagnostic.Suggestions)}
		}
		return err
	}
	return nil
}

// resolveConvertField preserves existing literal statistics names when a
// wildcard or an exact alias refers to a name that is not a dotted event path.
func resolveConvertField(name string, sourceRange spl.Range) (FieldRef, error) {
	field, err := ResolveField(name, sourceRange)
	if err != nil && spl.IsStatsLiteralFieldReference(name) {
		return ResolveQuotedField(name, sourceRange)
	}
	return field, err
}

func buildConvert(command *spl.ConvertCommand, schemaKnown bool, upstream []string) (*Convert, error) {
	if command == nil || len(command.Clauses) < 1 || len(command.Clauses) > spl.MaximumConvertClauses {
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_CONVERT_SYNTAX", Message: "convert requires a bounded nonempty clause list"}
	}
	if err := validateConvertFormat(command); err != nil {
		return nil, err
	}
	operator := &Convert{TimeFormat: strings.Clone(command.TimeFormat), TimeFormatRange: command.TimeFormatRange, Range: command.Range}
	fields := slices.Clone(upstream)
	for _, clause := range command.Clauses {
		if !clause.Function.Valid() || clause.Input == "" {
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_CONVERT_SYNTAX", Message: "convert function or selector is invalid", Range: clause.Range}
		}
		wildcard := spl.IsFieldsFieldGlob(clause.Input)
		if strings.Contains(clause.Input, "*") && !wildcard {
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_CONVERT_SYNTAX", Message: "convert wildcard selector is invalid", Range: clause.InputRange}
		}
		if (wildcard || clause.Function == spl.ConvertFunctionNone) && clause.ExplicitOutput {
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_CONVERT_SYNTAX", Message: "convert wildcard and none clauses do not support AS", Range: clause.OutputRange}
		}
		if !clause.ExplicitOutput && clause.Output != clause.Input {
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_CONVERT_SYNTAX", Message: "convert implicit output must equal its input", Range: clause.OutputRange}
		}
		if !wildcard {
			if _, err := resolveConvertField(clause.Input, clause.InputRange); err != nil {
				return nil, err
			}
		}
		if clause.Function == spl.ConvertFunctionNone {
			continue
		}
		if wildcard && !schemaKnown {
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_CONVERT_WILDCARD", Message: "convert wildcard requires bounded runtime field discovery", Range: clause.InputRange}
		}
		selected := []string{clause.Input}
		if wildcard {
			selected = nil
			for _, field := range fields {
				if !strings.HasPrefix(strings.ToLower(field), "__os_") && spl.MatchConvertFieldGlob(clause.Input, field) && !ConvertFieldExcluded(command, field) {
					selected = append(selected, field)
				}
			}
		}
		for _, name := range selected {
			if len(operator.Assignments) >= spl.MaximumConvertWrites {
				return nil, convertWriteLimit(clause.Range)
			}
			input, err := resolveConvertField(name, clause.InputRange)
			if err != nil {
				return nil, err
			}
			destination := clause.Output
			if wildcard {
				destination = name
			}
			output, err := resolveConvertField(destination, clause.OutputRange)
			if err != nil {
				return nil, err
			}
			if !schemaKnown && destination == "fields" {
				return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_CONVERT_SYNTAX", Message: "convert cannot overwrite the event fields payload without an exact upstream schema", Range: clause.OutputRange}
			}
			operator.Assignments = append(operator.Assignments, ConvertAssignment{Function: clause.Function, Input: input, Output: output, Range: clause.Range})
			if !slices.Contains(fields, destination) {
				fields = append(fields, destination)
			}
		}
	}
	return operator, nil
}
