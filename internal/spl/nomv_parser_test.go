package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseNoMVExactFieldCommand(t *testing.T) {
	t.Parallel()
	source := `index=main | NoMv users`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(query.Commands) != 1 {
		t.Fatalf("commands = %#v, want one", query.Commands)
	}
	command, ok := query.Commands[0].(*NoMVCommand)
	if !ok || command == nil {
		t.Fatalf("command = %T, want *NoMVCommand", query.Commands[0])
	}
	if command.Name() != "nomv" || command.Field != "users" {
		t.Fatalf("command = %#v", command)
	}
	if got := source[command.FieldRange.Start.Offset:command.FieldRange.End.Offset]; got != "users" {
		t.Fatalf("field range text = %q", got)
	}
	if got := source[command.Range.Start.Offset:command.Range.End.Offset]; got != "NoMv users" {
		t.Fatalf("command range text = %q", got)
	}
	if shape := ClassifyResultShape(query); shape != (ResultShape{Kind: ResultKindEvents}) {
		t.Fatalf("shape = %#v, want events", shape)
	}
}

func TestParseNoMVRejectsNonExactAndInternalFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		source string
		code   string
		text   string
	}{
		{name: "missing", source: `index=main | nomv`, code: "SPL_EXPECTED_FIELD"},
		{name: "quoted", source: `index=main | nomv 'users'`, code: "SPL_EXPECTED_FIELD", text: `'users'`},
		{name: "wildcard", source: `index=main | nomv users*`, code: "SPL_EXPECTED_FIELD", text: `users*`},
		{name: "second field", source: `index=main | nomv users groups`, code: "SPL_UNSUPPORTED_NOMV_SYNTAX", text: `groups`},
		{name: "option", source: `index=main | nomv delim="," users`, code: "SPL_UNSUPPORTED_NOMV_SYNTAX", text: `=`},
		{name: "internal", source: `index=main | nomv _time`, code: "SPL_UNSUPPORTED_NOMV_SYNTAX", text: `_time`},
		{name: "private", source: `index=main | nomv __os_pipeline_ordinal`, code: "SPL_RESERVED_FIELD", text: `__os_pipeline_ordinal`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("Parse error = %v, want %s", err, test.code)
			}
			if test.text != "" {
				got := test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]
				if got != test.text {
					t.Fatalf("diagnostic range text = %q, want %q", got, test.text)
				}
			}
		})
	}
}

func TestNoMVSuggestionContextAcceptsOnlyOneExactField(t *testing.T) {
	t.Parallel()
	context, diagnostic := AnalyzeSuggestionContext(`index=main | nomv us`, len(`index=main | nomv us`))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
	}
	if !context.Allows(SuggestionKindField) || context.AllowsQuotedScalarFields || context.Prefix != "us" {
		t.Fatalf("context = %#v", context)
	}

	complete, diagnostic := AnalyzeSuggestionContext(`index=main | nomv users `, len(`index=main | nomv users `))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext complete: %v", diagnostic)
	}
	if slices.Contains(complete.Kinds, SuggestionKindField) {
		t.Fatalf("complete context offers a second field: %#v", complete)
	}
}
