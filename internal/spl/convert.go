package spl

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
)

const (
	MaximumConvertClauses    = 64
	MaximumConvertWrites     = 64
	DefaultConvertTimeFormat = "%m/%d/%Y %H:%M:%S"
)

// ConvertFunction identifies one command-only conversion, including none exclusions.
type ConvertFunction uint8

const (
	ConvertFunctionInvalid ConvertFunction = iota
	ConvertFunctionCTime
	ConvertFunctionMktime
	ConvertFunctionDur2Sec
	ConvertFunctionNum
	ConvertFunctionAuto
	ConvertFunctionRmUnit
	ConvertFunctionNone
)

func (f ConvertFunction) Valid() bool { return f >= ConvertFunctionCTime && f <= ConvertFunctionNone }
func (f ConvertFunction) String() string {
	switch f {
	case ConvertFunctionCTime:
		return "ctime"
	case ConvertFunctionMktime:
		return "mktime"
	case ConvertFunctionDur2Sec:
		return "dur2sec"
	case ConvertFunctionNum:
		return "num"
	case ConvertFunctionAuto:
		return "auto"
	case ConvertFunctionRmUnit:
		return "rmunit"
	case ConvertFunctionNone:
		return "none"
	default:
		return ""
	}
}

// ConvertClause retains one exact or wildcard selector and its optional literal output.
type ConvertClause struct {
	Function       ConvertFunction
	Input          string
	InputRange     Range
	Output         string
	OutputRange    Range
	ExplicitOutput bool
	Range          Range
}

// ConvertCommand converts fields in clause order. None clauses exclude wildcard matches.
type ConvertCommand struct {
	Clauses         []ConvertClause
	TimeFormat      string
	TimeFormatRange Range
	Range           Range
}

func (*ConvertCommand) command()             {}
func (*ConvertCommand) Name() string         { return "convert" }
func (c *ConvertCommand) SourceRange() Range { return c.Range }

// MatchConvertFieldGlob uses wc-field matching, with the fields-command internal-name rule.
func MatchConvertFieldGlob(pattern, name string) bool {
	return MatchFieldsFieldGlob(pattern, name)
}

// ValidateConvertTimeFormat rechecks both parser-produced and directly constructed commands.
func ValidateConvertTimeFormat(command *ConvertCommand) error {
	if command == nil {
		return &Diagnostic{Code: "SPL_UNSUPPORTED_CONVERT_SYNTAX", Message: "convert command is nil"}
	}
	for _, clause := range command.Clauses {
		var err error
		switch clause.Function {
		case ConvertFunctionCTime:
			_, err = spltimeformat.CompileStrftimeFormat(command.TimeFormat)
		case ConvertFunctionMktime:
			_, err = spltimeformat.CompileStrptimeFormat(command.TimeFormat)
		default:
			continue
		}
		if err == nil {
			continue
		}
		code := "SPL_UNSUPPORTED_TIME_FORMAT"
		message := "convert timeformat is outside the supported deterministic date/time subset"
		if errors.Is(err, spltimeformat.ErrStrftimeFormatTooLarge) || errors.Is(err, spltimeformat.ErrStrptimeFormatTooLarge) {
			code = "SPL_QUERY_TOO_COMPLEX"
			message = "convert timeformat exceeds its resource limit"
		}
		return &Diagnostic{Code: code, Message: message, Range: command.TimeFormatRange}
	}
	return nil
}

func (p *parser) parseConvertCommand(name token) (Command, error) {
	command := &ConvertCommand{TimeFormat: DefaultConvertTimeFormat, TimeFormatRange: name.sourceRange}
	end := name.sourceRange.End
	if p.isKeyword("timeformat") && p.nextIs(tokenEqual) {
		p.advance()
		p.advance()
		format := p.current()
		if format.kind != tokenString {
			return nil, p.errorAtCurrent("SPL_UNSUPPORTED_CONVERT_SYNTAX", "convert timeformat requires a quoted String literal")
		}
		command.TimeFormat, command.TimeFormatRange = format.text, format.sourceRange
		end = format.sourceRange.End
		p.advance()
	}
	for !p.atCommandEnd() {
		if len(command.Clauses) >= MaximumConvertClauses {
			return nil, p.errorAtCurrent("SPL_QUERY_TOO_COMPLEX", fmt.Sprintf("convert contains more than %d clauses", MaximumConvertClauses))
		}
		function := p.current()
		var kind ConvertFunction
		for candidate := ConvertFunctionCTime; candidate <= ConvertFunctionNone; candidate++ {
			if strings.EqualFold(function.text, candidate.String()) {
				kind = candidate
				break
			}
		}
		if function.kind != tokenWord || !kind.Valid() {
			return nil, p.errorAtCurrent("SPL_UNSUPPORTED_CONVERT_SYNTAX", "convert supports ctime, mktime, dur2sec, num, auto, rmunit, and none")
		}
		p.advance()
		if !p.match(tokenLeftParen) {
			return nil, p.errorAtCurrent("SPL_UNSUPPORTED_CONVERT_SYNTAX", "convert function requires a parenthesized field selector")
		}
		input := p.current()
		if input.kind != tokenWord || (!IsExactUnquotedFieldName(input.text) && !IsFieldsFieldGlob(input.text)) {
			return nil, p.errorAtCurrent("SPL_UNSUPPORTED_CONVERT_SYNTAX", "convert requires one exact unquoted field or wildcard selector")
		}
		if err := rejectCompilerPrivateField("convert", input); err != nil {
			return nil, err
		}
		clause := ConvertClause{Function: kind, Input: input.text, InputRange: input.sourceRange, Output: input.text, OutputRange: input.sourceRange}
		p.advance()
		closing := p.current()
		if !p.match(tokenRightParen) {
			return nil, p.errorAtCurrent("SPL_UNSUPPORTED_CONVERT_SYNTAX", "convert function requires exactly one field selector")
		}
		end = closing.sourceRange.End
		if p.isKeyword("AS") {
			if kind == ConvertFunctionNone || IsFieldsFieldGlob(input.text) {
				return nil, p.errorAtCurrent("SPL_UNSUPPORTED_CONVERT_SYNTAX", "convert wildcard and none clauses do not support AS")
			}
			p.advance()
			output := p.current()
			if output.kind != tokenWord && output.kind != tokenString && output.kind != tokenQuotedField {
				return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "convert AS requires an exact output field")
			}
			if strings.Contains(output.text, "*") || output.text == "" {
				return nil, p.errorAtCurrent("SPL_UNSUPPORTED_CONVERT_SYNTAX", "convert AS requires an exact nonempty output field")
			}
			if err := rejectCompilerPrivateField("convert", output); err != nil {
				return nil, err
			}
			clause.Output, clause.OutputRange, clause.ExplicitOutput = output.text, output.sourceRange, true
			end = output.sourceRange.End
			p.advance()
		}
		clause.Range = Range{Start: function.sourceRange.Start, End: end}
		command.Clauses = append(command.Clauses, clause)
		if p.match(tokenComma) && p.atCommandEnd() {
			return nil, p.errorAtCurrent("SPL_UNSUPPORTED_CONVERT_SYNTAX", "convert requires a clause after comma")
		}
	}
	if len(command.Clauses) == 0 {
		return nil, p.errorAtCurrent("SPL_UNSUPPORTED_CONVERT_SYNTAX", "convert requires at least one conversion clause")
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	if err := ValidateConvertTimeFormat(command); err != nil {
		return nil, err
	}
	return command, nil
}
