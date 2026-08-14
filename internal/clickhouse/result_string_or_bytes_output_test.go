package clickhouse

import (
	"testing"
	"unsafe"
)

func TestCompiledStringOrBytesOutputIsStructurallyValidatedAndSealed(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(host) AS members | stats count BY members`,
	)
	descriptors, valid := compiled.ValidatedResultStringOrBytesOutputs()
	if !valid || len(descriptors) != 1 || descriptors[0].OutputIndex != 0 {
		t.Fatalf("descriptors = %#v, valid=%t", descriptors, valid)
	}

	tampered, ok := compiled.CloneForExecution()
	if !ok {
		t.Fatal("clone compiler-sealed String-or-Bytes query")
	}
	tampered.StringOrBytesOutputs[0].OutputIndex = 1
	if tampered.HasValidExecutionSeal() {
		t.Fatal("descriptor ordinal mutation retained a valid execution seal")
	}

	for _, test := range []struct {
		name  string
		query CompiledQuery
	}{
		{
			name: "out of range",
			query: CompiledQuery{
				OutputFields:         []string{"member"},
				StringOrBytesOutputs: []ResultStringOrBytesOutput{{OutputIndex: 1}},
			},
		},
		{
			name: "duplicate ordinal",
			query: CompiledQuery{
				OutputFields: []string{"member", "count"},
				StringOrBytesOutputs: []ResultStringOrBytesOutput{
					{OutputIndex: 0}, {OutputIndex: 0},
				},
			},
		},
		{
			name: "optional overlap",
			query: CompiledQuery{
				OutputFields:              []string{"member"},
				OptionalMultivalueOutputs: []ResultOptionalMultivalueOutput{{OutputIndex: 0}},
				StringOrBytesOutputs:      []ResultStringOrBytesOutput{{OutputIndex: 0}},
			},
		},
		{
			name: "container overlap",
			query: CompiledQuery{
				OutputFields:         []string{"member"},
				ContainerOutputs:     []ResultContainerOutput{{OutputIndex: 0}},
				StringOrBytesOutputs: []ResultStringOrBytesOutput{{OutputIndex: 0}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if descriptors, valid := test.query.ValidatedResultStringOrBytesOutputs(); valid || descriptors != nil {
				t.Fatalf("descriptors = %#v, valid=%t, want invalid", descriptors, valid)
			}
		})
	}
}

func TestCompiledStringOrBytesOutputRetainedBytesChargesDescriptorBacking(t *testing.T) {
	t.Parallel()

	withDescriptor := compileSPL(
		t,
		`index=gradethis | stats values(host) AS members | stats count BY members`,
	)
	withBytes, ok := withDescriptor.RetainedBytes()
	if !ok {
		t.Fatal("measure compiled query with String-or-Bytes descriptor")
	}

	withoutDescriptor := withDescriptor
	withoutDescriptor.StringOrBytesOutputs = nil
	withoutDescriptor.executionSeal = nil
	withoutDescriptor, err := sealCompiledQueryExecution(withoutDescriptor)
	if err != nil {
		t.Fatalf("seal descriptor-free comparison query: %v", err)
	}
	withoutBytes, ok := withoutDescriptor.RetainedBytes()
	if !ok {
		t.Fatal("measure descriptor-free comparison query")
	}
	wantDelta := uint64(cap(withDescriptor.StringOrBytesOutputs)) *
		uint64(unsafe.Sizeof(ResultStringOrBytesOutput{}))
	if withBytes < withoutBytes || withBytes-withoutBytes != wantDelta {
		t.Fatalf(
			"retained bytes with/without descriptor = %d/%d (delta %d), want %d",
			withBytes,
			withoutBytes,
			withBytes-withoutBytes,
			wantDelta,
		)
	}
}
