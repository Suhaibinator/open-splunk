package nilcheck

import "testing"

func TestIsNil(t *testing.T) {
	t.Parallel()

	var (
		channel  chan int
		function func()
		mapping  map[string]int
		pointer  *int
		slice    []int
	)
	for _, test := range []struct {
		name  string
		value any
		want  bool
	}{
		{name: "nil interface", want: true},
		{name: "nil channel", value: channel, want: true},
		{name: "nil function", value: function, want: true},
		{name: "nil map", value: mapping, want: true},
		{name: "nil pointer", value: pointer, want: true},
		{name: "nil slice", value: slice, want: true},
		{name: "zero integer", value: 0},
		{name: "empty string", value: ""},
		{name: "empty struct", value: struct{}{}},
		{name: "non-nil slice", value: []int{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := IsNil(test.value); got != test.want {
				t.Fatalf("IsNil(%T) = %t, want %t", test.value, got, test.want)
			}
		})
	}
}
