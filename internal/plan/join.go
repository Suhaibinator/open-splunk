package plan

import (
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// Join reads a filtered branch of the same scoped event source. Only selected
// non-key fields are imported from the first matching row.
type Join struct {
	Keys, Outputs []FieldRef
	Filter        Expression
	Left          bool
	Range         spl.Range
}

func (*Join) operator()                 {}
func (*Join) LogicalName() string       { return "Join" }
func (op *Join) SourceRange() spl.Range { return op.Range }

func buildJoin(command *spl.JoinCommand, scope Scope) (*Join, error) {
	if command == nil || command.Search == nil || len(command.Keys) == 0 || len(command.Keys) > spl.MaximumExplicitProjectionFields || len(command.Fields) == 0 || len(command.Fields) > spl.MaximumExplicitProjectionFields {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "invalid join search, keys, or projection", Range: safeSPLNodeRange(command)}
	}
	if _, err := resolveIndexes(scope, &spl.Query{Search: command.Search, Range: command.Range}, nil); err != nil {
		return nil, err
	}
	filter, err := convertExpression(command.Search)
	if err != nil {
		return nil, err
	}
	op := &Join{Filter: filter, Left: command.Left, Range: command.Range}
	for _, key := range command.Keys {
		ref, err := ResolveField(key.Name, key.Range)
		if err != nil {
			return nil, err
		}
		if !slices.ContainsFunc(command.Fields, func(field spl.ExactCommandField) bool { return field.Name == key.Name }) {
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_JOIN_SYNTAX", Message: "subsearch must project each join key", Range: key.Range}
		}
		op.Keys = append(op.Keys, ref)
	}
	for _, field := range command.Fields {
		if slices.ContainsFunc(op.Keys, func(key FieldRef) bool { return field.Name == key.Name }) {
			continue
		}
		ref, err := ResolveField(field.Name, field.Range)
		if err != nil {
			return nil, err
		}
		if ref.Name == "fields" {
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_JOIN_SYNTAX", Message: "name the individual subsearch fields to import", Range: field.Range}
		}
		op.Outputs = append(op.Outputs, ref)
	}
	return op, nil
}
