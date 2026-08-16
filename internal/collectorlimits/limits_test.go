package collectorlimits

import "testing"

func TestClampFleetCounter(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value uint64
		want  uint64
	}{
		{value: 0, want: 0},
		{value: MaximumFleetCounter, want: MaximumFleetCounter},
		{value: MaximumFleetCounter + 1, want: MaximumFleetCounter},
		{value: ^uint64(0), want: MaximumFleetCounter},
	} {
		if got := ClampFleetCounter(test.value); got != test.want {
			t.Fatalf("ClampFleetCounter(%d) = %d, want %d", test.value, got, test.want)
		}
	}
}

func TestSaturatingAddFleetCounters(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		values []uint64
		want   uint64
	}{
		{name: "empty"},
		{name: "sum", values: []uint64{1, 2, 3}, want: 6},
		{name: "sum saturates", values: []uint64{MaximumFleetCounter - 1, 2}, want: MaximumFleetCounter},
		{name: "operand clamps", values: []uint64{^uint64(0), 1}, want: MaximumFleetCounter},
		{name: "no intermediate wrap", values: []uint64{^uint64(0), ^uint64(0)}, want: MaximumFleetCounter},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := SaturatingAddFleetCounters(test.values...); got != test.want {
				t.Fatalf("SaturatingAddFleetCounters(%v) = %d, want %d", test.values, got, test.want)
			}
		})
	}
}
