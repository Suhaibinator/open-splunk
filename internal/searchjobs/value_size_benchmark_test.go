package searchjobs

import "testing"

func BenchmarkValueRetainedSizeBytes(b *testing.B) {
	items := make([]Value, 1024)
	for i := range items {
		items[i] = UnsignedValue(uint64(i))
	}
	for _, testCase := range []struct {
		name  string
		value Value
	}{
		{"scalar", UnsignedValue(42)},
		{"nested", ListValue(items...)},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if size, err := testCase.value.RetainedSizeBytes(); err != nil || size == 0 {
					b.Fatalf("size=%d err=%v", size, err)
				}
			}
		})
	}
}
