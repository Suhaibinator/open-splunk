package hec

import "testing"

func TestDefaultLimitsAreValidAndPinnedToHardCeilings(t *testing.T) {
	t.Parallel()
	limits := DefaultLimits()
	if err := limits.Validate(); err != nil {
		t.Fatal(err)
	}
	if limits.MaximumCompressedBodyBytes != HardMaximumCompressedBodyBytes ||
		limits.MaximumDecompressedBodyBytes != HardMaximumDecompressedBodyBytes ||
		limits.MaximumNormalizedBytes != HardMaximumNormalizedBytes ||
		limits.MaximumEventBytes != HardMaximumEventBytes ||
		limits.MaximumEvents != HardMaximumEvents ||
		limits.MaximumJSONDepth != HardMaximumJSONDepth ||
		limits.MaximumJSONValues != HardMaximumJSONValues ||
		limits.MaximumObjectMembers != HardMaximumObjectMembers ||
		limits.MaximumHeaderBytes != HardMaximumHeaderBytes ||
		limits.MaximumRequestTargetBytes != HardMaximumRequestTargetBytes ||
		limits.MaximumChannelBytes != HardMaximumChannelBytes ||
		limits.MaximumAcknowledgmentIDs != HardMaximumAcknowledgmentIDs ||
		limits.MaximumAcknowledgmentBodyBytes != HardMaximumAcknowledgmentBodyBytes ||
		limits.MaximumResponseBytes != HardMaximumResponseBytes {
		t.Fatalf("DefaultLimits() = %#v", limits)
	}
}

func TestLimitsRejectZeroAboveHardAndInconsistentValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		edit func(*Limits)
	}{
		{"compressed zero", func(value *Limits) { value.MaximumCompressedBodyBytes = 0 }},
		{"compressed above", func(value *Limits) { value.MaximumCompressedBodyBytes = HardMaximumCompressedBodyBytes + 1 }},
		{"decompressed zero", func(value *Limits) { value.MaximumDecompressedBodyBytes = 0 }},
		{"decompressed above", func(value *Limits) { value.MaximumDecompressedBodyBytes = HardMaximumDecompressedBodyBytes + 1 }},
		{"normalized zero", func(value *Limits) { value.MaximumNormalizedBytes = 0 }},
		{"normalized above", func(value *Limits) { value.MaximumNormalizedBytes = HardMaximumNormalizedBytes + 1 }},
		{"event zero", func(value *Limits) { value.MaximumEventBytes = 0 }},
		{"event above", func(value *Limits) { value.MaximumEventBytes = HardMaximumEventBytes + 1 }},
		{"events zero", func(value *Limits) { value.MaximumEvents = 0 }},
		{"events above", func(value *Limits) { value.MaximumEvents = HardMaximumEvents + 1 }},
		{"depth zero", func(value *Limits) { value.MaximumJSONDepth = 0 }},
		{"depth above", func(value *Limits) { value.MaximumJSONDepth = HardMaximumJSONDepth + 1 }},
		{"values zero", func(value *Limits) { value.MaximumJSONValues = 0 }},
		{"values above", func(value *Limits) { value.MaximumJSONValues = HardMaximumJSONValues + 1 }},
		{"members zero", func(value *Limits) { value.MaximumObjectMembers = 0 }},
		{"members above", func(value *Limits) { value.MaximumObjectMembers = HardMaximumObjectMembers + 1 }},
		{"header zero", func(value *Limits) { value.MaximumHeaderBytes = 0 }},
		{"header above", func(value *Limits) { value.MaximumHeaderBytes = HardMaximumHeaderBytes + 1 }},
		{"request target zero", func(value *Limits) { value.MaximumRequestTargetBytes = 0 }},
		{"request target above", func(value *Limits) { value.MaximumRequestTargetBytes = HardMaximumRequestTargetBytes + 1 }},
		{"channel zero", func(value *Limits) { value.MaximumChannelBytes = 0 }},
		{"channel above", func(value *Limits) { value.MaximumChannelBytes = HardMaximumChannelBytes + 1 }},
		{"acks zero", func(value *Limits) { value.MaximumAcknowledgmentIDs = 0 }},
		{"acks above", func(value *Limits) { value.MaximumAcknowledgmentIDs = HardMaximumAcknowledgmentIDs + 1 }},
		{"ack body zero", func(value *Limits) { value.MaximumAcknowledgmentBodyBytes = 0 }},
		{"ack body above", func(value *Limits) { value.MaximumAcknowledgmentBodyBytes = HardMaximumAcknowledgmentBodyBytes + 1 }},
		{"response zero", func(value *Limits) { value.MaximumResponseBytes = 0 }},
		{"response above", func(value *Limits) { value.MaximumResponseBytes = HardMaximumResponseBytes + 1 }},
		{"event over decompressed", func(value *Limits) { value.MaximumDecompressedBodyBytes = value.MaximumEventBytes - 1 }},
		{"normalized below event", func(value *Limits) { value.MaximumNormalizedBytes = value.MaximumEventBytes - 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			limits := DefaultLimits()
			test.edit(&limits)
			if err := limits.Validate(); err == nil {
				t.Fatalf("Limits.Validate(%#v) succeeded", limits)
			}
		})
	}
}
