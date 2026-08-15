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

// lookupMappingMessages carries the diagnostic wording that distinguishes the
// key half of a lookup stage from the output half.
type lookupMappingMessages struct {
	schemaField   string
	eventField    string
	reserved      string
	repeatedField string
	repeatedEvent string
}

// buildLookupMappings validates one ordered mapping list and resolves its event
// fields. view projects the source mapping onto the shared shape.
func buildLookupMappings[T any](
	mappings []T,
	view func(T) (lookupField string, lookupFieldRange spl.Range, eventField string, eventFieldRange spl.Range, sourceRange spl.Range),
	outputSchemaKnown bool,
	messages lookupMappingMessages,
) ([]LookupKey, error) {
	resolved := make([]LookupKey, 0, len(mappings))
	lookupFields := make(map[string]struct{}, len(mappings))
	eventFields := make(map[string]struct{}, len(mappings))
	for _, mapping := range mappings {
		lookupField, lookupFieldRange, eventField, eventFieldRange, sourceRange := view(mapping)
		if !validLookupSchemaField(lookupField) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: messages.schemaField,
				Range:   lookupFieldRange,
			}
		}
		if !spl.IsExactUnquotedFieldName(eventField) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: messages.eventField,
				Range:   eventFieldRange,
			}
		}
		if !outputSchemaKnown && eventField == "fields" {
			return nil, &Diagnostic{
				Code:    "SPL_AMBIGUOUS_LOOKUP_FIELD",
				Message: messages.reserved,
				Range:   eventFieldRange,
			}
		}
		if _, duplicate := lookupFields[lookupField]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: fmt.Sprintf(messages.repeatedField, lookupField),
				Range:   lookupFieldRange,
			}
		}
		if _, duplicate := eventFields[eventField]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
				Message: fmt.Sprintf(messages.repeatedEvent, eventField),
				Range:   eventFieldRange,
			}
		}
		resolvedEventField, err := ResolveField(eventField, eventFieldRange)
		if err != nil {
			return nil, err
		}
		lookupFields[lookupField] = struct{}{}
		eventFields[eventField] = struct{}{}
		resolved = append(resolved, LookupKey{
			LookupField:      lookupField,
			LookupFieldRange: lookupFieldRange,
			EventField:       resolvedEventField,
			Range:            sourceRange,
		})
	}
	return resolved, nil
}

// validLookupMappingSet reports whether one resolved mapping list carries exact
// fields with no repeated lookup column or event field.
func validLookupMappingSet[T any](mappings []T, view func(T) (string, FieldRef)) bool {
	lookupFields := make(map[string]struct{}, len(mappings))
	eventFields := make(map[string]struct{}, len(mappings))
	for _, mapping := range mappings {
		lookupField, eventField := view(mapping)
		if !validLookupSchemaField(lookupField) ||
			!validLookupEventField(eventField) {
			return false
		}
		if _, duplicate := lookupFields[lookupField]; duplicate {
			return false
		}
		if _, duplicate := eventFields[eventField.Name]; duplicate {
			return false
		}
		lookupFields[lookupField] = struct{}{}
		eventFields[eventField.Name] = struct{}{}
	}
	return true
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
		WriteMode:       writeMode,
		Range:           command.Range,
	}
	keys, err := buildLookupMappings(
		command.Keys,
		func(mapping spl.LookupKeyMapping) (string, spl.Range, string, spl.Range, spl.Range) {
			return mapping.LookupField, mapping.LookupFieldRange,
				mapping.EventField, mapping.EventFieldRange, mapping.Range
		},
		outputSchemaKnown,
		lookupMappingMessages{
			schemaField:   "lookup key columns must be exact and unquoted",
			eventField:    "lookup key event fields must be exact and unquoted",
			reserved:      "lookup cannot read the event result's reserved fields payload without an exact upstream schema",
			repeatedField: "lookup key column %q is repeated",
			repeatedEvent: "lookup event key field %q is repeated",
		},
	)
	if err != nil {
		return nil, err
	}
	outputs, err := buildLookupMappings(
		command.Outputs,
		func(mapping spl.LookupOutputMapping) (string, spl.Range, string, spl.Range, spl.Range) {
			return mapping.LookupField, mapping.LookupFieldRange,
				mapping.EventField, mapping.EventFieldRange, mapping.Range
		},
		outputSchemaKnown,
		lookupMappingMessages{
			schemaField:   "lookup output columns must be exact and unquoted",
			eventField:    "lookup output event fields must be exact and unquoted",
			reserved:      "lookup cannot replace the event result's reserved fields payload without an exact upstream schema",
			repeatedField: "lookup output column %q is repeated",
			repeatedEvent: "lookup event output field %q is repeated",
		},
	)
	if err != nil {
		return nil, err
	}
	result.Keys = keys
	result.Outputs = make([]LookupOutput, len(outputs))
	for index, mapping := range outputs {
		result.Outputs[index] = LookupOutput(mapping)
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

	return validLookupMappingSet(operator.Keys, func(key LookupKey) (string, FieldRef) {
		return key.LookupField, key.EventField
	}) && validLookupMappingSet(operator.Outputs, func(output LookupOutput) (string, FieldRef) {
		return output.LookupField, output.EventField
	})
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
