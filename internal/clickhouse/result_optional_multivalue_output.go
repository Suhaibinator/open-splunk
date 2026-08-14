package clickhouse

import (
	"errors"
	"fmt"
	"math"
	"slices"
)

// ResultOptionalMultivalueOutput binds a public Array(String) ordinal to one
// compiler-owned value-presence bit. ClickHouse cannot represent
// Nullable(Array(String)); the sealed sidecar lets the executor publish a
// typed nullable List while preserving the difference between null/missing
// and a present empty multivalue.
type ResultOptionalMultivalueOutput struct {
	OutputIndex uint16
}

func canonicalResultOptionalMultivalueOutput(index uint16) ResultOptionalMultivalueOutput {
	return ResultOptionalMultivalueOutput{OutputIndex: index}
}

// PresentColumn returns the private non-null UInt8 column for this output.
func (output ResultOptionalMultivalueOutput) PresentColumn() string {
	return fmt.Sprintf("__os_result_multivalue_present_%d", output.OutputIndex)
}

// ValidatedResultOptionalMultivalueOutputs returns a detached descriptor slice
// only when the complete ordinary-result transport is canonical.
func (compiled CompiledQuery) ValidatedResultOptionalMultivalueOutputs() (
	[]ResultOptionalMultivalueOutput,
	bool,
) {
	if !validResultOptionalMultivalueOutputs(compiled) {
		return nil, false
	}
	return slices.Clone(compiled.OptionalMultivalueOutputs), true
}

func validResultOptionalMultivalueOutputs(compiled CompiledQuery) bool {
	if len(compiled.OptionalMultivalueOutputs) == 0 {
		return true
	}
	if compiled.Timechart != nil || compiled.Chart != nil ||
		len(compiled.OptionalMultivalueOutputs) > len(compiled.OutputFields) ||
		!validResultContainerOutputs(compiled) {
		return false
	}
	usedNames := make(map[string]struct{}, len(compiled.OutputFields)+1+
		len(compiled.ContainerOutputs)*3+len(compiled.OptionalMultivalueOutputs))
	usedIndexes := make(map[int]struct{}, len(compiled.ContainerOutputs)+
		len(compiled.OptionalMultivalueOutputs))
	for _, name := range compiled.OutputFields {
		if name == "" {
			return false
		}
		if _, duplicate := usedNames[name]; duplicate {
			return false
		}
		usedNames[name] = struct{}{}
	}
	if compiled.SparseFields {
		usedNames[SparseEventFieldNamesColumn] = struct{}{}
	}
	for _, output := range compiled.ContainerOutputs {
		usedIndexes[int(output.OutputIndex)] = struct{}{}
		for _, name := range []string{
			output.NamesColumn(),
			output.TypesColumn(),
			output.MetadataVersionColumn(),
		} {
			if _, collision := usedNames[name]; collision {
				return false
			}
			usedNames[name] = struct{}{}
		}
	}
	previous := -1
	for _, output := range compiled.OptionalMultivalueOutputs {
		index := int(output.OutputIndex)
		if index <= previous || index >= len(compiled.OutputFields) ||
			compiled.SparseFields && compiled.OutputFields[index] == "fields" {
			return false
		}
		if _, overlap := usedIndexes[index]; overlap {
			return false
		}
		previous = index
		name := output.PresentColumn()
		if _, collision := usedNames[name]; collision {
			return false
		}
		usedNames[name] = struct{}{}
	}
	return true
}

func compileResultOptionalMultivalueOutputs(
	state compileState,
	outputFields []string,
	containerOutputs []ResultContainerOutput,
) ([]ResultOptionalMultivalueOutput, []string, error) {
	outputs := make([]ResultOptionalMultivalueOutput, 0)
	projection := make([]string, 0)
	for index, name := range outputFields {
		field, visible := state.visible[name]
		if !visible || field.optionalMultivaluePresentSQL == "" {
			continue
		}
		if field.kind != fieldKindStringArray || index > math.MaxUint16 {
			return nil, nil, errors.New(
				"compile ClickHouse query: optional multivalue result transport is invalid",
			)
		}
		descriptor := canonicalResultOptionalMultivalueOutput(uint16(index))
		outputs = append(outputs, descriptor)
		projection = append(
			projection,
			"toUInt8("+field.optionalMultivaluePresentSQL+") AS "+
				quoteIdentifier(descriptor.PresentColumn()),
		)
	}
	if len(outputs) == 0 {
		return nil, nil, nil
	}
	contract := CompiledQuery{
		OutputFields:              slices.Clone(outputFields),
		ContainerOutputs:          slices.Clone(containerOutputs),
		OptionalMultivalueOutputs: slices.Clone(outputs),
	}
	if !validResultOptionalMultivalueOutputs(contract) {
		return nil, nil, errors.New(
			"compile ClickHouse query: optional multivalue result transport is invalid",
		)
	}
	return outputs, projection, nil
}
