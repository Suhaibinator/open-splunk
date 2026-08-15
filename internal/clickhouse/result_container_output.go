package clickhouse

import (
	"errors"
	"fmt"
	"math"
	"slices"
)

// ResultContainerOutput binds one public output ordinal to a compiler-defined
// three-column relative metadata transport. Hidden names are derived solely
// from the ordinal, avoiding redundant mutable authority in the execution
// contract. The descriptor itself is part of CompiledQuery's execution seal.
type ResultContainerOutput struct {
	OutputIndex uint16
}

func canonicalResultContainerOutput(index uint16) ResultContainerOutput {
	return ResultContainerOutput{OutputIndex: index}
}

// NamesColumn returns the private relative-presence column for this output.
func (output ResultContainerOutput) NamesColumn() string {
	return fmt.Sprintf("__os_result_container_names_%d", output.OutputIndex)
}

// TypesColumn returns the private aligned semantic-type column for this output.
func (output ResultContainerOutput) TypesColumn() string {
	return fmt.Sprintf("__os_result_container_types_%d", output.OutputIndex)

}

// MetadataVersionColumn returns the private metadata-version column for this
// output.
func (output ResultContainerOutput) MetadataVersionColumn() string {
	return fmt.Sprintf("__os_result_container_metadata_version_%d", output.OutputIndex)
}

// ValidatedResultContainerOutputs returns a detached descriptor slice only
// when the complete ordinary-result transport shape is canonical. Execution
// callers still require HasValidExecutionSeal independently; keeping this
// structural check usable for package tests avoids creating a second
// forgeable sealing API.
func (compiled CompiledQuery) ValidatedResultContainerOutputs() ([]ResultContainerOutput, bool) {
	if !validResultContainerOutputs(compiled) {
		return nil, false
	}
	return slices.Clone(compiled.ContainerOutputs), true
}

// publicResultOutputNames returns the set of public result column names,
// including the sparse field-names column, or false when OutputFields is not a
// canonical set of distinct non-empty names. extraCapacity is an allocation
// hint for the private names the caller will add.
func publicResultOutputNames(compiled CompiledQuery, extraCapacity int) (map[string]struct{}, bool) {
	names := make(map[string]struct{}, len(compiled.OutputFields)+1+extraCapacity)
	for _, name := range compiled.OutputFields {
		if name == "" {
			return nil, false
		}
		if _, duplicate := names[name]; duplicate {
			return nil, false
		}
		names[name] = struct{}{}
	}
	if compiled.SparseFields {
		names[SparseEventFieldNamesColumn] = struct{}{}
	}
	return names, true
}

// validResultOutputOrdinal reports whether a transport output ordinal is
// strictly ascending, inside the declared output arity, and not the sparse
// fields payload column.
func validResultOutputOrdinal(compiled CompiledQuery, index, previous int) bool {
	return index > previous && index < len(compiled.OutputFields) &&
		(!compiled.SparseFields || compiled.OutputFields[index] != "fields")
}

func validResultContainerOutputs(compiled CompiledQuery) bool {
	if len(compiled.ContainerOutputs) == 0 {
		return true
	}
	if compiled.Timechart != nil || compiled.Chart != nil ||
		len(compiled.ContainerOutputs) > len(compiled.OutputFields) {
		return false
	}
	public, ok := publicResultOutputNames(compiled, 0)
	if !ok {
		return false
	}
	hidden := make(map[string]struct{}, len(compiled.ContainerOutputs)*3)
	previous := -1
	for _, output := range compiled.ContainerOutputs {
		index := int(output.OutputIndex)
		if !validResultOutputOrdinal(compiled, index, previous) {
			return false
		}
		previous = index
		for _, name := range []string{
			output.NamesColumn(),
			output.TypesColumn(),
			output.MetadataVersionColumn(),
		} {
			if _, exposed := public[name]; exposed {
				return false
			}
			if _, duplicate := hidden[name]; duplicate {
				return false
			}
			hidden[name] = struct{}{}
		}
	}
	return true
}

func compileResultContainerOutputs(
	state compileState,
	outputFields []string,
) ([]ResultContainerOutput, []string, error) {
	outputs := make([]ResultContainerOutput, 0)
	projection := make([]string, 0)
	public := make(map[string]struct{}, len(outputFields)+1)
	for _, name := range outputFields {
		public[name] = struct{}{}
	}
	public[SparseEventFieldNamesColumn] = struct{}{}
	for index, name := range outputFields {
		field, visible := state.visible[name]
		if !visible {
			continue
		}
		if err := validateKnowledgeFieldSidecars(
			field.relativeFieldNamesSQL,
			field.relativeFieldTypesSQL,
			field.fieldMetadataVersionSQL,
		); err != nil {
			return nil, nil, err
		}
		if field.relativeFieldNamesSQL == "" {
			continue
		}
		if field.kind != fieldKindDynamic || index > math.MaxUint16 {
			return nil, nil, errors.New(
				"compile ClickHouse query: container result transport is invalid",
			)
		}
		descriptor := canonicalResultContainerOutput(uint16(index))
		for _, hidden := range []string{
			descriptor.NamesColumn(),
			descriptor.TypesColumn(),
			descriptor.MetadataVersionColumn(),
		} {
			if _, collision := public[hidden]; collision {
				return nil, nil, errors.New(
					"compile ClickHouse query: container result column collides with public output",
				)
			}
			public[hidden] = struct{}{}
		}
		outputs = append(outputs, descriptor)
		projection = append(
			projection,
			field.relativeFieldNamesSQL+" AS "+quoteIdentifier(descriptor.NamesColumn()),
			field.relativeFieldTypesSQL+" AS "+quoteIdentifier(descriptor.TypesColumn()),
			field.fieldMetadataVersionSQL+" AS "+quoteIdentifier(descriptor.MetadataVersionColumn()),
		)
	}
	if len(outputs) == 0 {
		return nil, nil, nil
	}
	contract := CompiledQuery{
		OutputFields:     slices.Clone(outputFields),
		ContainerOutputs: slices.Clone(outputs),
	}
	if !validResultContainerOutputs(contract) {
		return nil, nil, errors.New(
			"compile ClickHouse query: container result transport is invalid",
		)
	}
	return outputs, projection, nil
}
