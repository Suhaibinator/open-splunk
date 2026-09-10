package queryexec

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func BenchmarkNativeValueConversion(b *testing.B) {
	nested := make([]any, 1024)
	for index := range nested {
		nested[index] = map[string]any{"value": []any{uint64(index), "text"}}
	}
	for _, test := range []struct {
		name  string
		value any
	}{{"scalar", uint64(42)}, {"nested", nested}} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := convertValue(test.value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type decodeBenchmarkSink struct{}

func (decodeBenchmarkSink) SetSchema(searchjobs.Schema) error { return nil }
func (decodeBenchmarkSink) AddRow([]searchjobs.Value) error   { return nil }

func BenchmarkFixedTimechartPublication(b *testing.B) {
	counts := make([]uint64, 1000)
	for index := range counts {
		counts[index] = uint64(index)
	}
	b.ReportAllocs()
	for b.Loop() {
		err := publishFixedGrid(context.Background(), decodeBenchmarkSink{}, time.Unix(0, 0), time.Second, searchjobs.Column{Name: "count", Kind: searchjobs.ValueKindUnsigned}, counts, searchjobs.UnsignedValue)
		if err != nil {
			b.Fatal(err)
		}
	}
}
