package spl

import (
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/splpath"
)

func TestParseSpathExplicitJSONPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		input     string
		output    string
		path      string
		wantSteps []splpath.Step
	}{
		{
			name:      "defaults input and output",
			source:    `* | spath path=server.name`,
			input:     "_raw",
			output:    "server.name",
			path:      "server.name",
			wantSteps: []splpath.Step{{Key: "server"}, {Key: "name"}},
		},
		{
			name:      "unlabelled path",
			source:    `* | spath output=myfield server.name`,
			input:     "_raw",
			output:    "myfield",
			path:      "server.name",
			wantSteps: []splpath.Step{{Key: "server"}, {Key: "name"}},
		},
		{
			name:   "options in arbitrary order and fixed array index",
			source: `* | SpAtH PaTh=vendor.products{0}.price OuTpUt=first_price InPuT=payload`,
			input:  "payload",
			output: "first_price",
			path:   "vendor.products{0}.price",
			wantSteps: []splpath.Step{
				{Key: "vendor"},
				{Key: "products", HasIndex: true, Index: 0},
				{Key: "price"},
			},
		},
		{
			name:      "quoted values",
			source:    `* | spath input="json payload" path="display name" output="selected value"`,
			input:     "json payload",
			output:    "selected value",
			path:      "display name",
			wantSteps: []splpath.Step{{Key: "display name"}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command, ok := query.Commands[0].(*SpathCommand)
			if !ok {
				t.Fatalf("command = %T, want *SpathCommand", query.Commands[0])
			}
			if command.Input != test.input || command.Output != test.output || command.Path != test.path {
				t.Fatalf("spath command = %#v, want input=%q output=%q path=%q", command, test.input, test.output, test.path)
			}
			if len(command.Steps) != len(test.wantSteps) {
				t.Fatalf("steps = %#v, want %#v", command.Steps, test.wantSteps)
			}
			for index := range command.Steps {
				if command.Steps[index] != test.wantSteps[index] {
					t.Fatalf("step %d = %#v, want %#v", index, command.Steps[index], test.wantSteps[index])
				}
			}
			if got := test.source[command.InputRange.Start.Offset:command.InputRange.End.Offset]; command.Input != "_raw" && !strings.Contains(got, command.Input) {
				t.Fatalf("input range selects %q, want %q", got, command.Input)
			}
			if got := test.source[command.PathRange.Start.Offset:command.PathRange.End.Offset]; !strings.Contains(got, command.Path) {
				t.Fatalf("path range selects %q, want %q", got, command.Path)
			}
			if got := test.source[command.OutputRange.Start.Offset:command.OutputRange.End.Offset]; !strings.Contains(got, command.Output) {
				t.Fatalf("output range selects %q, want %q", got, command.Output)
			}
		})
	}
}

func TestParseSpathRejectsUnsupportedOrAmbiguousSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		code   string
	}{
		{`* | spath`, "SPL_UNSUPPORTED_SPATH_SYNTAX"},
		{`* | spath input=payload`, "SPL_UNSUPPORTED_SPATH_SYNTAX"},
		{`* | spath output=value`, "SPL_EXPECTED_SPATH_PATH"},
		{`* | spath path=`, "SPL_EXPECTED_SPATH_PATH"},
		{`* | spath input=`, "SPL_EXPECTED_FIELD"},
		{`* | spath output=`, "SPL_EXPECTED_FIELD"},
		{`* | spath path=a path=b`, "SPL_UNSUPPORTED_SPATH_SYNTAX"},
		{`* | spath input=a input=b path=x`, "SPL_UNSUPPORTED_SPATH_SYNTAX"},
		{`* | spath output=a output=b path=x`, "SPL_UNSUPPORTED_SPATH_SYNTAX"},
		{`* | spath path=a extra`, "SPL_UNSUPPORTED_SPATH_SYNTAX"},
		{`* | spath mode=json path=a`, "SPL_UNSUPPORTED_SPATH_SYNTAX"},
		{`* | spath path=items{}.value`, "SPL_UNSUPPORTED_SPATH_PATH"},
		{`* | spath path=item{@id}`, "SPL_UNSUPPORTED_SPATH_PATH"},
		{`* | spath path=one..two`, "SPL_INVALID_SPATH_PATH"},
		{`* | spath path=a,output=b`, "SPL_UNSUPPORTED_SPATH_SYNTAX"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("Parse(%q) error = %v, want *Diagnostic", test.source, err)
			}
			if diagnostic.Code != test.code {
				t.Fatalf("Parse(%q) diagnostic = %s (%v), want %s", test.source, diagnostic.Code, err, test.code)
			}
		})
	}
}

func TestParseSpathBoundsPathComplexity(t *testing.T) {
	t.Parallel()

	path := strings.TrimSuffix(strings.Repeat("a.", splpath.MaximumPathSteps), ".") + ".overflow"
	_, err := Parse(`* | spath path="` + path + `" output=value`)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("Parse over-depth spath error = %v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}

func TestParseSpathUnknownOptionDiagnosticStartsAtOption(t *testing.T) {
	t.Parallel()

	source := `* | spath mode=json path=a`
	_, err := Parse(source)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Parse error = %v, want *Diagnostic", err)
	}
	if diagnostic.Code != "SPL_UNSUPPORTED_SPATH_SYNTAX" ||
		!strings.Contains(diagnostic.Message, `"mode"`) {
		t.Fatalf("diagnostic = %#v, want unknown mode option", diagnostic)
	}
	if got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != "mode" {
		t.Fatalf("diagnostic range selects %q, want mode", got)
	}
}

func TestParseSpathPathDiagnosticIncludesDecodedByteOffset(t *testing.T) {
	t.Parallel()

	_, err := Parse(`* | spath path=one..two`)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Parse error = %v, want *Diagnostic", err)
	}
	if diagnostic.Code != "SPL_INVALID_SPATH_PATH" ||
		!strings.Contains(diagnostic.Message, "decoded path byte 4") {
		t.Fatalf("diagnostic = %#v, want decoded byte offset", diagnostic)
	}
}
