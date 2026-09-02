package spl

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestParseFrequencyCommandsAcceptOrderedCommaSeparatedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		source     string
		command    string
		wantFields []string
		wantLimit  uint64
	}{
		{
			name:       "top default limit",
			source:     "index=main | top host, source, sourcetype",
			command:    "top",
			wantFields: []string{"host", "source", "sourcetype"},
			wantLimit:  10,
		},
		{
			name:       "top named limit",
			source:     "index=main | top limit=20 host,status",
			command:    "top",
			wantFields: []string{"host", "status"},
			wantLimit:  20,
		},
		{
			name:       "rare positional limit",
			source:     "index=main | rare 5 host,status",
			command:    "rare",
			wantFields: []string{"host", "status"},
			wantLimit:  5,
		},
		{
			name:       "rare unlimited",
			source:     "index=main | rare limit=0 host,source",
			command:    "rare",
			wantFields: []string{"host", "source"},
			wantLimit:  0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			fields, limit, sourceRange := frequencyCommandParts(t, query.Commands[0])
			gotFields := make([]string, len(fields))
			for index, field := range fields {
				gotFields[index] = field.Name
				if got := test.source[field.Range.Start.Offset:field.Range.End.Offset]; got != field.Name {
					t.Fatalf("field %d range text = %q, want %q", index, got, field.Name)
				}
			}
			if !slices.Equal(gotFields, test.wantFields) || limit != test.wantLimit {
				t.Fatalf("frequency command = fields %v limit %d, want fields %v limit %d", gotFields, limit, test.wantFields, test.wantLimit)
			}
			wantCommandText := test.source[strings.Index(test.source, test.command):]
			if got := test.source[sourceRange.Start.Offset:sourceRange.End.Offset]; got != wantCommandText {
				t.Fatalf("command range text = %q, want %q", got, wantCommandText)
			}
		})
	}
}

func TestParseFrequencyCommandsAcceptMaximumDistinctFields(t *testing.T) {
	t.Parallel()

	fieldNames := make([]string, MaximumFrequencyFields)
	for index := range fieldNames {
		fieldNames[index] = fmt.Sprintf("field%d", index)
	}
	for _, commandName := range []string{"top", "rare"} {
		t.Run(commandName, func(t *testing.T) {
			t.Parallel()

			query, err := Parse("index=main | " + commandName + " " + strings.Join(fieldNames, ","))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			fields, _, _ := frequencyCommandParts(t, query.Commands[0])
			if len(fields) != MaximumFrequencyFields {
				t.Fatalf("fields = %d, want %d", len(fields), MaximumFrequencyFields)
			}
		})
	}
}

func TestParseFrequencyCommandsRejectMoreThanMaximumFields(t *testing.T) {
	t.Parallel()

	fieldNames := make([]string, MaximumFrequencyFields+1)
	for index := range fieldNames {
		fieldNames[index] = fmt.Sprintf("field%d", index)
	}
	for _, commandName := range []string{"top", "rare"} {
		t.Run(commandName, func(t *testing.T) {
			t.Parallel()

			source := "index=main | " + commandName + " " + strings.Join(fieldNames, ",")
			_, err := Parse(source)
			diagnostic := frequencyDiagnostic(t, err)
			if diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("diagnostic = %#v, want SPL_QUERY_TOO_COMPLEX", diagnostic)
			}
			if got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != fieldNames[MaximumFrequencyFields] {
				t.Fatalf("diagnostic range text = %q, want %q", got, fieldNames[MaximumFrequencyFields])
			}
		})
	}
}

func TestParseFrequencyCommandsRequireExactDistinctCommaSeparatedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		arguments string
		wantCode  string
		wantRange string
	}{
		{name: "whitespace separator", arguments: "host source", wantRange: "source"},
		{name: "leading comma", arguments: ",host", wantCode: "SPL_EXPECTED_FIELD", wantRange: ","},
		{name: "repeated comma", arguments: "host,,source", wantCode: "SPL_EXPECTED_FIELD", wantRange: ","},
		{name: "trailing comma", arguments: "host,", wantCode: "SPL_EXPECTED_FIELD"},
		{name: "duplicate", arguments: "host,source,host", wantRange: "host"},
		{name: "quoted field", arguments: `host,"source"`, wantRange: `"source"`},
		{name: "wildcard field", arguments: "host,sour*", wantRange: "sour*"},
		{name: "empty by clause", arguments: "host BY", wantCode: "SPL_EXPECTED_FIELD"},
		{name: "by before field", arguments: "BY source", wantCode: "SPL_EXPECTED_FIELD", wantRange: "BY"},
		{name: "second by clause", arguments: "host BY source BY level", wantRange: "BY"},
		{name: "by repeats counted field", arguments: "host BY source, host", wantRange: "host"},
		{name: "by wildcard", arguments: "host BY sour*", wantRange: "sour*"},
		{name: "output option", arguments: "host,showperc=false", wantRange: "showperc"},
		{name: "option after by", arguments: "host BY source limit=5", wantRange: "limit"},
	}
	for _, commandName := range []string{"top", "rare"} {
		for _, test := range tests {
			t.Run(commandName+"/"+test.name, func(t *testing.T) {
				t.Parallel()

				source := "index=main | " + commandName + " " + test.arguments
				_, err := Parse(source)
				diagnostic := frequencyDiagnostic(t, err)
				wantCode := test.wantCode
				if wantCode == "" {
					wantCode = "SPL_UNSUPPORTED_" + strings.ToUpper(commandName) + "_SYNTAX"
				}
				if diagnostic.Code != wantCode {
					t.Fatalf("diagnostic = %#v, want %s", diagnostic, wantCode)
				}
				if test.wantRange != "" {
					if got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != test.wantRange {
						t.Fatalf("diagnostic range text = %q, want %q", got, test.wantRange)
					}
				}
			})
		}
	}
}

func frequencyCommandParts(t *testing.T, command Command) ([]FrequencyField, uint64, Range) {
	t.Helper()

	switch command := command.(type) {
	case *TopCommand:
		return command.Fields, command.Limit, command.Range
	case *RareCommand:
		return command.Fields, command.Limit, command.Range
	default:
		t.Fatalf("command = %T, want top or rare", command)
		return nil, 0, Range{}
	}
}

func frequencyDiagnostic(t *testing.T, err error) *Diagnostic {
	t.Helper()

	if err == nil {
		t.Fatal("Parse succeeded, want error")
	}
	diagnostic := &Diagnostic{}
	if !errors.As(err, &diagnostic) {
		t.Fatalf("error = %T, want *Diagnostic", err)
	}
	return diagnostic
}
