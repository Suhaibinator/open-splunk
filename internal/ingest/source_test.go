package ingest

import (
	"strings"
	"testing"
)

func TestCanonicalIngestionSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    IngestionSource
		collector string
		want      IngestionSource
		wantError bool
	}{
		{
			name:      "legacy native",
			collector: "collector-a",
			want:      NativeCollectorSource("collector-a"),
		},
		{
			name:      "explicit native",
			source:    NativeCollectorSource("collector-a"),
			collector: "collector-a",
			want:      NativeCollectorSource("collector-a"),
		},
		{
			name:   "HEC",
			source: HECSource("token-record-a"),
			want:   HECSource("token-record-a"),
		},
		{
			name:      "HEC cannot inherit collector",
			source:    HECSource("token-record-a"),
			collector: "collector-a",
			wantError: true,
		},
		{
			name:      "native mismatch",
			source:    NativeCollectorSource("collector-a"),
			collector: "collector-b",
			wantError: true,
		},
		{
			name:      "empty",
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := CanonicalIngestionSource(test.source, test.collector)
			if test.wantError {
				if err == nil {
					t.Fatalf("CanonicalIngestionSource(%+v, %q) succeeded", test.source, test.collector)
				}
				return
			}
			if err != nil {
				t.Fatalf("CanonicalIngestionSource(%+v, %q): %v", test.source, test.collector, err)
			}
			if got != test.want {
				t.Fatalf("source = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestIngestionSourceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source IngestionSource
	}{
		{name: "unspecified", source: IngestionSource{ID: "source"}},
		{name: "empty HEC", source: HECSource("")},
		{name: "whitespace", source: HECSource(" token")},
		{name: "NUL", source: HECSource("token\x00id")},
		{name: "overlong", source: HECSource(strings.Repeat("x", int(HardMaxIDBytes)+1))},
		{name: "HEC collector", source: IngestionSource{Kind: IngestionSourceKindHEC, ID: "token", CollectorID: "collector"}},
		{name: "native mismatch", source: IngestionSource{Kind: IngestionSourceKindNativeCollector, ID: "a", CollectorID: "b"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.source.Validate(); err == nil {
				t.Fatalf("Validate(%+v) succeeded", test.source)
			}
		})
	}
	if err := HECSource("token-a").Validate(); err != nil {
		t.Fatalf("valid HEC source: %v", err)
	}
	if err := NativeCollectorSource("collector-a").Validate(); err != nil {
		t.Fatalf("valid native source: %v", err)
	}
}
