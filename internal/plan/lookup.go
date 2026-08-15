package plan

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// LookupWriteMode is the backend-neutral output collision policy for one
// explicit lookup stage.
type LookupWriteMode uint8

const (
	LookupWriteModeInvalid LookupWriteMode = iota
	LookupWriteModeOverwrite
	LookupWriteModePreserveExisting
)

// LookupKey maps one exact lookup-definition key column to an event field.
// LookupField remains a schema identifier rather than an event FieldRef.
type LookupKey struct {
	LookupField      string
	LookupFieldRange spl.Range
	EventField       FieldRef
	Range            spl.Range
}

// LookupOutput maps one exact lookup-definition value column to an event
// field. Outputs are ordered and are applied simultaneously by the backend.
type LookupOutput struct {
	LookupField      string
	LookupFieldRange spl.Range
	EventField       FieldRef
	Range            spl.Range
}

// Lookup enriches rows from an authored, unresolved definition name. Runtime
// resolution authority is intentionally absent: tenant, object, version, and
// immutable rows must come from a separately sealed search snapshot.
type Lookup struct {
	DefinitionName  string
	DefinitionRange spl.Range
	Keys            []LookupKey
	Outputs         []LookupOutput
	WriteMode       LookupWriteMode
	Range           spl.Range
}

func (*Lookup) operator()                 {}
func (*Lookup) LogicalName() string       { return "Lookup" }
func (op *Lookup) SourceRange() spl.Range { return op.Range }

func buildLookupCommand(command *spl.LookupCommand, outputSchemaKnown bool) (*Lookup, error) {
	if command == nil {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "lookup command is nil",
		}
	}
	if command.DefinitionName == "" ||
		len(command.DefinitionName) > spl.MaximumLookupDefinitionNameBytes ||
		!spl.IsExactUnquotedFieldName(command.DefinitionName) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
			Message: "lookup definition name must be exact and unquoted",
			Range:   command.DefinitionRange,
		}
	}
	if len(command.Keys) < 1 || len(command.Keys) > spl.MaximumLookupKeys {
		return nil, &Diagnostic{
			Code: "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
			Message: fmt.Sprintf(
				"lookup requires between 1 and %d key mappings",
				spl.MaximumLookupKeys,
			),
			Range: command.Range,
		}
	}
	if len(command.Outputs) < 1 || len(command.Outputs) > spl.MaximumLookupOutputs {
		return nil, &Diagnostic{
			Code: "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
			Message: fmt.Sprintf(
				"lookup requires between 1 and %d output mappings",
				spl.MaximumLookupOutputs,
			),
			Range: command.Range,
		}
	}

	var writeMode LookupWriteMode
	switch command.OutputMode {
	case spl.LookupOutputModeOverwrite:
		writeMode = LookupWriteModeOverwrite
	case spl.LookupOutputModePreserveExisting:
		writeMode = LookupWriteModePreserveExisting
	default:
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
			Message: "lookup requires explicit OUTPUT or OUTPUTNEW semantics",
			Range:   command.OutputModeRange,
		}
	}

	result := &Lookup{
		DefinitionName:  command.DefinitionName,
		DefinitionRange: command.DefinitionRange,
		Keys:            make([]LookupKey, 0, len(command.Keys)),
		Outputs:         make([]LookupOutput, 0, len(command.Outputs)),
		WriteMode:       writeMode,
		Range:           command.Range,
	}
	lookupKeys := make(map[string]struct{}, len(command.Keys))
	eventKeys := make(map[string]struct{}, len(command.Keys))
	for _, mapping := range command.Keys {
		if !validLookupSchemaField(mapping.LookupField) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: "lookup key columns must be exact and unquoted",
				Range:   mapping.LookupFieldRange,
			}
		}
		if !spl.IsExactUnquotedFieldName(mapping.EventField) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: "lookup key event fields must be exact and unquoted",
				Range:   mapping.EventFieldRange,
			}
		}
		if !outputSchemaKnown && mapping.EventField == "fields" {
			return nil, &Diagnostic{
				Code:    "SPL_AMBIGUOUS_LOOKUP_FIELD",
				Message: "lookup cannot read the event result's reserved fields payload without an exact upstream schema",
				Range:   mapping.EventFieldRange,
			}
		}
		if _, duplicate := lookupKeys[mapping.LookupField]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: fmt.Sprintf("lookup key column %q is repeated", mapping.LookupField),
				Range:   mapping.LookupFieldRange,
			}
		}
		if _, duplicate := eventKeys[mapping.EventField]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: fmt.Sprintf("lookup event key field %q is repeated", mapping.EventField),
				Range:   mapping.EventFieldRange,
			}
		}
		eventField, err := ResolveField(mapping.EventField, mapping.EventFieldRange)
		if err != nil {
			return nil, err
		}
		lookupKeys[mapping.LookupField] = struct{}{}
		eventKeys[mapping.EventField] = struct{}{}
		result.Keys = append(result.Keys, LookupKey{
			LookupField:      mapping.LookupField,
			LookupFieldRange: mapping.LookupFieldRange,
			EventField:       eventField,
			Range:            mapping.Range,
		})
	}

	lookupOutputs := make(map[string]struct{}, len(command.Outputs))
	eventOutputs := make(map[string]struct{}, len(command.Outputs))
	for _, mapping := range command.Outputs {
		if !validLookupSchemaField(mapping.LookupField) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: "lookup output columns must be exact and unquoted",
				Range:   mapping.LookupFieldRange,
			}
		}
		if !spl.IsExactUnquotedFieldName(mapping.EventField) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: "lookup output event fields must be exact and unquoted",
				Range:   mapping.EventFieldRange,
			}
		}
		if !outputSchemaKnown && mapping.EventField == "fields" {
			return nil, &Diagnostic{
				Code:    "SPL_AMBIGUOUS_LOOKUP_FIELD",
				Message: "lookup cannot replace the event result's reserved fields payload without an exact upstream schema",
				Range:   mapping.EventFieldRange,
			}
		}
		if _, duplicate := lookupOutputs[mapping.LookupField]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: fmt.Sprintf("lookup output column %q is repeated", mapping.LookupField),
				Range:   mapping.LookupFieldRange,
			}
		}
		if _, duplicate := eventOutputs[mapping.EventField]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: fmt.Sprintf("lookup event output field %q is repeated", mapping.EventField),
				Range:   mapping.EventFieldRange,
			}
		}
		eventField, err := ResolveField(mapping.EventField, mapping.EventFieldRange)
		if err != nil {
			return nil, err
		}
		lookupOutputs[mapping.LookupField] = struct{}{}
		eventOutputs[mapping.EventField] = struct{}{}
		result.Outputs = append(result.Outputs, LookupOutput{
			LookupField:      mapping.LookupField,
			LookupFieldRange: mapping.LookupFieldRange,
			EventField:       eventField,
			Range:            mapping.Range,
		})
	}
	return result, nil
}

func validLookupContract(operator *Lookup) bool {
	if operator == nil ||
		operator.DefinitionName == "" ||
		len(operator.DefinitionName) > spl.MaximumLookupDefinitionNameBytes ||
		!spl.IsExactUnquotedFieldName(operator.DefinitionName) ||
		len(operator.Keys) < 1 || len(operator.Keys) > spl.MaximumLookupKeys ||
		len(operator.Outputs) < 1 || len(operator.Outputs) > spl.MaximumLookupOutputs ||
		(operator.WriteMode != LookupWriteModeOverwrite &&
			operator.WriteMode != LookupWriteModePreserveExisting) {
		return false
	}

	lookupKeys := make(map[string]struct{}, len(operator.Keys))
	eventKeys := make(map[string]struct{}, len(operator.Keys))
	for _, key := range operator.Keys {
		if !validLookupSchemaField(key.LookupField) ||
			!validLookupEventField(key.EventField) {
			return false
		}
		if _, duplicate := lookupKeys[key.LookupField]; duplicate {
			return false
		}
		if _, duplicate := eventKeys[key.EventField.Name]; duplicate {
			return false
		}
		lookupKeys[key.LookupField] = struct{}{}
		eventKeys[key.EventField.Name] = struct{}{}
	}

	lookupOutputs := make(map[string]struct{}, len(operator.Outputs))
	eventOutputs := make(map[string]struct{}, len(operator.Outputs))
	for _, output := range operator.Outputs {
		if !validLookupSchemaField(output.LookupField) ||
			!validLookupEventField(output.EventField) {
			return false
		}
		if _, duplicate := lookupOutputs[output.LookupField]; duplicate {
			return false
		}
		if _, duplicate := eventOutputs[output.EventField.Name]; duplicate {
			return false
		}
		lookupOutputs[output.LookupField] = struct{}{}
		eventOutputs[output.EventField.Name] = struct{}{}
	}
	return true
}

func validLookupSchemaField(name string) bool {
	return spl.IsExactUnquotedFieldName(name)
}

func validLookupEventField(field FieldRef) bool {
	return spl.IsExactUnquotedFieldName(field.Name) &&
		validResolvedEventAggregateField(field)
}

func (analyzer *queryAnalyzer) visitLookup(operator *Lookup, depth int) error {
	if !validLookupContract(operator) {
		return &lookupAnalysisError{}
	}
	for _, key := range operator.Keys {
		if err := analyzer.validateOutputName(key.LookupField, depth); err != nil {
			return err
		}
		if err := analyzer.addField(key.EventField, depth); err != nil {
			return err
		}
	}
	for _, output := range operator.Outputs {
		if err := analyzer.validateOutputName(output.LookupField, depth); err != nil {
			return err
		}
		if err := analyzer.validateField(output.EventField, depth); err != nil {
			return err
		}
	}
	return nil
}

// lookupAnalysisError has a stable, non-authority-bearing message while
// keeping lookup validation in the same fail-closed analysis path as every
// other logical operator.
type lookupAnalysisError struct{}

func (*lookupAnalysisError) Error() string {
	return "analyze logical query: lookup contract is invalid"
}
