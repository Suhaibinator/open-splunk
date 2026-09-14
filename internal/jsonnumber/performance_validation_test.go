package jsonnumber

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidMatchesJSONNumberGrammar(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		text string
		want bool
	}{
		{"0", true}, {"-0", true}, {"1", true}, {"-1", true},
		{"1.25", true}, {"1e2", true}, {"1E+2", true}, {"1e-2", true},
		{"", false}, {"+1", false}, {"01", false}, {"-01", false},
		{".1", false}, {"1.", false}, {"1e", false}, {"1e+", false},
		{" 1", false}, {"1 ", false}, {"true", false}, {`"1"`, false},
	} {
		if got := Valid(test.text); got != test.want {
			t.Errorf("Valid(%q) = %v, want %v", test.text, got, test.want)
		}
	}
}

func FuzzValidMatchesEncodingJSONNumber(f *testing.F) {
	for _, seed := range []string{"0", "-0", "12", "01", "1.5", "1e+2", "1e", "+1", " 1", "1 ", "true", `"1"`, "\x00"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		oracle := text != "" && !strings.ContainsAny(text, " \t\r\n") &&
			(text[0] == '-' || text[0] >= '0' && text[0] <= '9') && json.Valid([]byte(text))
		if got := Valid(text); got != oracle {
			t.Fatalf("Valid(%q) = %v, encoding/json number validity = %v", text, got, oracle)
		}
	})
}
