package clickhouse

import (
	"errors"
	"fmt"
	"math"
	"slices"
)

// ResultStringOrBytesOutput binds one public physical String ordinal to its
// byte-preserving SPL cell domain. The executor publishes the column as Mixed
// and admits only String or Bytes cells, so invalid UTF-8 is preserved without
// weakening ordinary String columns or trusting a field name heuristic.
type ResultStringOrBytesOutput struct {
	OutputIndex uint16
	Nullable    bool
}

func canonicalResultStringOrBytesOutput(index uint16, nullable bool) ResultStringOrBytesOutput {
	return ResultStringOrBytesOutput{OutputIndex: index, Nullable: nullable}
}

// SemanticBytesColumn returns the compiler-owned non-null UInt8 authority for
// the corresponding public String value. One means the cell is semantic Bytes
// even when its payload is valid UTF-8.
func (output ResultStringOrBytesOutput) SemanticBytesColumn() string {
	return fmt.Sprintf("__os_result_semantic_bytes_%d", output.OutputIndex)
}

// ValidatedResultStringOrBytesOutputs returns a detached descriptor slice only
// when the complete ordinary-result transport is canonical.
func (compiled CompiledQuery) ValidatedResultStringOrBytesOutputs() (
	[]ResultStringOrBytesOutput,
	bool,
) {
	if !validResultStringOrBytesOutputs(compiled) {
		return nil, false
	}
	return slices.Clone(compiled.StringOrBytesOutputs), true
}

func validResultStringOrBytesOutputs(compiled CompiledQuery) bool {
	if len(compiled.StringOrBytesOutputs) == 0 {
		return true
	}
	if compiled.Timechart != nil || compiled.Chart != nil ||
		len(compiled.StringOrBytesOutputs) > len(compiled.OutputFields) ||
		!validResultContainerOutputs(compiled) ||
		!validResultOptionalMultivalueOutputs(compiled) {
		return false
	}
	usedNames, ok := publicResultOutputNames(compiled,
		len(compiled.ContainerOutputs)*3+len(compiled.OptionalMultivalueOutputs)+
			len(compiled.StringOrBytesOutputs))
	if !ok {
		return false
	}
	reservedIndexes := make(map[int]struct{}, len(compiled.ContainerOutputs)+
		len(compiled.OptionalMultivalueOutputs))
	for _, output := range compiled.ContainerOutputs {
		reservedIndexes[int(output.OutputIndex)] = struct{}{}
		for _, name := range []string{
			output.NamesColumn(), output.TypesColumn(), output.MetadataVersionColumn(),
		} {
			usedNames[name] = struct{}{}
		}
	}
	for _, output := range compiled.OptionalMultivalueOutputs {
		reservedIndexes[int(output.OutputIndex)] = struct{}{}
		usedNames[output.PresentColumn()] = struct{}{}
	}
	previous := -1
	for _, output := range compiled.StringOrBytesOutputs {
		index := int(output.OutputIndex)
		if !validResultOutputOrdinal(compiled, index, previous) {
			return false
		}
		if _, overlap := reservedIndexes[index]; overlap {
			return false
		}
		name := output.SemanticBytesColumn()
		if _, collision := usedNames[name]; collision {
			return false
		}
		usedNames[name] = struct{}{}
		previous = index
	}
	return true
}

func compileResultStringOrBytesOutputs(
	state compileState,
	outputFields []string,
	containerOutputs []ResultContainerOutput,
	optionalMultivalueOutputs []ResultOptionalMultivalueOutput,
) ([]ResultStringOrBytesOutput, []string, error) {
	outputs := make([]ResultStringOrBytesOutput, 0)
	projection := make([]string, 0)
	for index, name := range outputFields {
		field, visible := state.visible[name]
		if !visible || !field.stringOrBytes {
			continue
		}
		// Native multivalues already publish as a typed List. Keep any String
		// provenance for later relational consumers, but do not replace the
		// list's public schema here.
		if isNativeMultivalueKind(field.kind) {
			continue
		}
		if field.kind != fieldKindString || index > math.MaxUint16 {
			return nil, nil, errors.New(
				"compile ClickHouse query: String-or-Bytes result transport is invalid",
			)
		}
		if field.semanticBytesSQL == "" {
			return nil, nil, errors.New(
				"compile ClickHouse query: String-or-Bytes result lacks semantic Bytes provenance",
			)
		}
		descriptor := canonicalResultStringOrBytesOutput(
			uint16(index),
			field.stringOrBytesNullable,
		)
		outputs = append(
			outputs,
			descriptor,
		)
		projection = append(
			projection,
			"toUInt8(ifNull("+field.semanticBytesSQL+", 0)) AS "+
				quoteIdentifier(descriptor.SemanticBytesColumn()),
		)
	}
	if len(outputs) == 0 {
		return nil, nil, nil
	}
	contract := CompiledQuery{
		OutputFields:              slices.Clone(outputFields),
		ContainerOutputs:          slices.Clone(containerOutputs),
		OptionalMultivalueOutputs: slices.Clone(optionalMultivalueOutputs),
		StringOrBytesOutputs:      slices.Clone(outputs),
	}
	if !validResultStringOrBytesOutputs(contract) {
		return nil, nil, errors.New(
			"compile ClickHouse query: String-or-Bytes result transport is invalid",
		)
	}
	return outputs, projection, nil
}
