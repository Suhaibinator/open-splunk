package clickhouse

import "testing"

func TestResultOptionalMultivalueTransportAcceptsNativeDynamicArray(t *testing.T) {
	t.Parallel()

	state := compileState{visible: map[string]fieldState{
		"members": {
			kind:                         fieldKindDynamicArray,
			optionalMultivaluePresentSQL: "members_present",
		},
	}}
	outputs, projection, err := compileResultOptionalMultivalueOutputs(
		state,
		[]string{"members"},
		nil,
	)
	if err != nil {
		t.Fatalf("compile native dynamic multivalue result transport: %v", err)
	}
	if len(outputs) != 1 || outputs[0].OutputIndex != 0 || len(projection) != 1 ||
		projection[0] != `multiIf(toUInt8(members_present) != 0, toUInt8(1), toUInt8(1) != 0, toUInt8(2), toUInt8(0)) AS "__os_result_multivalue_present_0"` {
		t.Fatalf("native dynamic multivalue transport = %#v / %#v", outputs, projection)
	}
}

func TestResultFieldPresentationAcceptsFlatNativeDynamicMultivalue(t *testing.T) {
	t.Parallel()

	state := compileState{visible: map[string]fieldState{
		"members": {
			kind:                       fieldKindDynamicArray,
			hasFlatMultivalueDelimiter: true,
			flatMultivalueDelimiter:    "\n",
		},
	}}
	presentations, err := resultFieldPresentations(state, []string{"members"})
	if err != nil {
		t.Fatalf("flat native dynamic multivalue presentation: %v", err)
	}
	if len(presentations) != 1 || !presentations[0].HasFlatMultivalueDelimiter ||
		presentations[0].FlatMultivalueDelimiter != "\n" || presentations[0].StatsSparkline {
		t.Fatalf("flat native dynamic multivalue presentation = %#v", presentations)
	}
}

func TestResultFieldPresentationKeepsSparklineStringOnly(t *testing.T) {
	t.Parallel()

	state := compileState{visible: map[string]fieldState{
		"trend": {kind: fieldKindDynamicArray, statsSparkline: true},
	}}
	if presentations, err := resultFieldPresentations(state, []string{"trend"}); err == nil || presentations != nil {
		t.Fatalf("dynamic sparkline presentation = (%#v, %v), want invalid", presentations, err)
	}
}
