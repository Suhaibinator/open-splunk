package spl

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseLookupPreservesOrderedExactMappingsAndMode(t *testing.T) {
	t.Parallel()

	source := "* | LoOkUp service_catalog service_id AS service_key environment AS env OUTPUTNEW owner AS service_owner tier"
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command, ok := query.Commands[0].(*LookupCommand)
	if !ok {
		t.Fatalf("command = %T, want *LookupCommand", query.Commands[0])
	}
	if command.Name() != "lookup" || command.DefinitionName != "service_catalog" ||
		command.OutputMode != LookupOutputModePreserveExisting {
		t.Fatalf("lookup header = %#v", command)
	}
	if len(command.Keys) != 2 ||
		command.Keys[0].LookupField != "service_id" ||
		command.Keys[0].EventField != "service_key" ||
		command.Keys[1].LookupField != "environment" ||
		command.Keys[1].EventField != "env" {
		t.Fatalf("lookup keys = %#v", command.Keys)
	}
	if len(command.Outputs) != 2 ||
		command.Outputs[0].LookupField != "owner" ||
		command.Outputs[0].EventField != "service_owner" ||
		command.Outputs[1].LookupField != "tier" ||
		command.Outputs[1].EventField != "tier" {
		t.Fatalf("lookup outputs = %#v", command.Outputs)
	}

	ranges := []struct {
		got  Range
		want string
	}{
		{command.DefinitionRange, "service_catalog"},
		{command.Keys[0].LookupFieldRange, "service_id"},
		{command.Keys[0].EventFieldRange, "service_key"},
		{command.OutputModeRange, "OUTPUTNEW"},
		{command.Outputs[0].LookupFieldRange, "owner"},
		{command.Outputs[0].EventFieldRange, "service_owner"},
		{command.Outputs[1].EventFieldRange, "tier"},
	}
	for _, check := range ranges {
		if got := source[check.got.Start.Offset:check.got.End.Offset]; got != check.want {
			t.Fatalf("source range = %q, want %q", got, check.want)
		}
	}
	if got := source[command.Range.Start.Offset:command.Range.End.Offset]; got != source[strings.Index(source, "LoOkUp"):] {
		t.Fatalf("command range = %q", got)
	}
}

func TestParseLookupOutputOverwritesAndPreservesDefaultOutputName(t *testing.T) {
	t.Parallel()

	query, err := Parse("* | lookup catalog id AS event_id OUTPUT owner")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*LookupCommand)
	if command.OutputMode != LookupOutputModeOverwrite ||
		len(command.Outputs) != 1 ||
		command.Outputs[0].EventField != "owner" {
		t.Fatalf("lookup = %#v", command)
	}
	if got := ClassifyResultShape(query); got != (ResultShape{Kind: ResultKindEvents}) {
		t.Fatalf("result shape = %#v, want events", got)
	}
}

func TestParseLookupAcceptsKeyAndOutputBounds(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString("* | lookup catalog")
	for index := range MaximumLookupKeys {
		fmt.Fprintf(&source, " key%d AS event_key%d", index, index)
	}
	source.WriteString(" OUTPUT")
	for index := range MaximumLookupOutputs {
		fmt.Fprintf(&source, " output%d", index)
	}
	query, err := Parse(source.String())
	if err != nil {
		t.Fatalf("Parse at bounds: %v", err)
	}
	command := query.Commands[0].(*LookupCommand)
	if len(command.Keys) != MaximumLookupKeys || len(command.Outputs) != MaximumLookupOutputs {
		t.Fatalf("mapping counts = %d/%d", len(command.Keys), len(command.Outputs))
	}
}

func TestParseLookupRejectsUnsupportedAmbiguousAndOverBoundedSyntax(t *testing.T) {
	t.Parallel()

	overKeys := "* | lookup catalog a AS ea b AS eb c AS ec d AS ed e AS ee OUTPUT value"
	var overOutputs strings.Builder
	overOutputs.WriteString("* | lookup catalog key AS event_key OUTPUT")
	for index := 0; index <= MaximumLookupOutputs; index++ {
		fmt.Fprintf(&overOutputs, " output%d", index)
	}
	overDefinition := strings.Repeat("d", MaximumLookupDefinitionNameBytes+1)

	tests := []struct {
		source string
		code   string
	}{
		{"* | lookup", "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{`* | lookup "catalog" key AS event_key OUTPUT owner`, "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{"* | lookup catalog OUTPUT owner", "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{"* | lookup catalog key event_key OUTPUT owner", "SPL_EXPECTED_AS"},
		{"* | lookup catalog key AS OUTPUT owner", "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{"* | lookup catalog key AS event_key", "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{"* | lookup catalog key AS event_key OUTPUT", "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{`* | lookup catalog "key" AS event_key OUTPUT owner`, "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{`* | lookup catalog key AS "event key" OUTPUT owner`, "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{"* | lookup catalog key AS event_key key AS other OUTPUT owner", "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{"* | lookup catalog first AS event_key second AS event_key OUTPUT owner", "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{"* | lookup catalog key AS event_key OUTPUT owner owner", "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{"* | lookup catalog key AS event_key OUTPUT first AS owner second AS owner", "SPL_UNSUPPORTED_LOOKUP_SYNTAX"},
		{"* | lookup catalog key AS __os_key OUTPUT owner", "SPL_RESERVED_FIELD"},
		{"* | lookup catalog key AS event_key OUTPUT owner AS __os_owner", "SPL_RESERVED_FIELD"},
		{"* | lookup __os_catalog key AS event_key OUTPUT owner", "SPL_RESERVED_FIELD"},
		{"* | lookup catalog __os_key AS event_key OUTPUT owner", "SPL_RESERVED_FIELD"},
		{"* | lookup catalog key AS event_key OUTPUT __os_owner", "SPL_RESERVED_FIELD"},
		{"* | lookup " + overDefinition + " key AS event_key OUTPUT owner", "SPL_QUERY_TOO_COMPLEX"},
		{overKeys, "SPL_QUERY_TOO_COMPLEX"},
		{overOutputs.String(), "SPL_QUERY_TOO_COMPLEX"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("Parse error = %v, want %s", err, test.code)
			}
		})
	}
}

func TestLookupSuggestionContextsExposeOnlyGrammarAndEventFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source   string
		kinds    []SuggestionKind
		keywords []string
	}{
		{source: "| lookup "},
		{source: "| lookup catalog "},
		{source: "| lookup catalog key ", kinds: []SuggestionKind{SuggestionKindKeyword}, keywords: []string{"AS"}},
		{source: "| lookup catalog key AS ", kinds: []SuggestionKind{SuggestionKindField}},
		{source: "| lookup catalog key AS event_key ", kinds: []SuggestionKind{SuggestionKindKeyword}, keywords: []string{"OUTPUT", "OUTPUTNEW"}},
		{source: "| lookup catalog key AS event_key OUTPUT "},
		{source: "| lookup catalog key AS event_key OUTPUT owner ", kinds: []SuggestionKind{SuggestionKindKeyword}, keywords: []string{"AS"}},
		{source: "| lookup catalog key AS event_key OUTPUT owner AS ", kinds: []SuggestionKind{SuggestionKindField}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
			}
			if fmt.Sprint(context.Kinds) != fmt.Sprint(test.kinds) ||
				fmt.Sprint(context.Keywords) != fmt.Sprint(test.keywords) {
				t.Fatalf("context = %#v, want kinds=%v keywords=%v", context, test.kinds, test.keywords)
			}
		})
	}

	result := Suggest("| look", len("| look"), 20)
	if result.Diagnostic != nil || len(result.Suggestions) != 1 ||
		result.Suggestions[0].Label != "lookup" {
		t.Fatalf("lookup command suggestions = %#v / %v", result.Suggestions, result.Diagnostic)
	}
}
