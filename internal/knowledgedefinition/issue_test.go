package knowledgedefinition

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"google.golang.org/protobuf/proto"
)

func TestNormalizeReturnsDeterministicCandidateIssuesInFailFastOrder(t *testing.T) {
	t.Parallel()

	regexDefinition := func() *opensplunkv1.KnowledgeObjectDefinition {
		definition := validBaseDefinition()
		definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{
						Pattern:      `(?<value>.+)`,
						OutputFields: []string{"value", "other"},
					},
				},
			},
		}
		return definition
	}

	tests := []struct {
		name       string
		definition func() *opensplunkv1.KnowledgeObjectDefinition
		want       Issue
		wantText   string
	}{
		{
			name:       "definition root",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition { return nil },
			want: Issue{
				Code:    IssueCodeInvalidDefinition,
				Message: "is required",
			},
			wantText: "invalid knowledge definition: definition: is required",
		},
		{
			name: "metadata",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validAliasDefinition()
				definition.AppId = " \t "
				return definition
			},
			want: Issue{
				FieldPath: "app_id",
				Code:      IssueCodeInvalidDefinition,
				Message:   "is empty",
			},
			wantText: "invalid knowledge definition: app_id: is empty",
		},
		{
			name: "fail fast before body",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validAliasDefinition()
				definition.AppId = ""
				definition.Body = nil
				return definition
			},
			want: Issue{
				FieldPath: "app_id",
				Code:      IssueCodeInvalidDefinition,
				Message:   "is empty",
			},
			wantText: "invalid knowledge definition: app_id: is empty",
		},
		{
			name: "nested selector value",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validAliasDefinition()
				definition.Selector.IndexPatterns[0].Value = "prod*"
				definition.Selector.IndexPatterns[0].MatchKind = opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT
				return definition
			},
			want: Issue{
				FieldPath: "selector.index_patterns[0].match_kind",
				Code:      IssueCodeInvalidDefinition,
				Message:   "disagrees with normalized value",
			},
			wantText: "invalid knowledge definition: selector.index_patterns[0].match_kind: disagrees with normalized value",
		},
		{
			name: "body",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validBaseDefinition()
				definition.Body = nil
				return definition
			},
			want: Issue{
				FieldPath: "body",
				Code:      IssueCodeInvalidDefinition,
				Message:   "is missing or unknown",
			},
			wantText: "invalid knowledge definition: body: is missing or unknown",
		},
		{
			name: "repeated body field",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := regexDefinition()
				definition.GetFieldExtraction().GetRegex().OutputFields[1] = " value "
				return definition
			},
			want: Issue{
				FieldPath: "field_extraction.regex.output_fields[1]",
				Code:      IssueCodeInvalidDefinition,
				Message:   "duplicates a prior output",
			},
			wantText: "invalid knowledge definition: field_extraction.regex.output_fields[1]: duplicates a prior output",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Normalize(test.definition())
			if err == nil {
				t.Fatal("Normalize succeeded")
			}
			if err.Error() != test.wantText {
				t.Fatalf("Normalize error = %q, want %q", err, test.wantText)
			}
			assertDefinitionIssue(t, err, test.want)
			assertDefinitionErrorRoots(t, err, ErrInvalidDefinition)
		})
	}
}

func TestNormalizeUnknownFieldIssuesUseDefinitionRelativePaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(*opensplunkv1.KnowledgeObjectDefinition)
		wantPath string
		wantText string
	}{
		{
			name: "root",
			mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
				definition.ProtoReflect().SetUnknown(testUnknownField())
			},
			wantText: "knowledge definition contains unknown fields: definition",
		},
		{
			name: "message",
			mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
				definition.Selector.ProtoReflect().SetUnknown(testUnknownField())
			},
			wantPath: "selector",
			wantText: "knowledge definition contains unknown fields: definition.selector",
		},
		{
			name: "repeated message",
			mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
				definition.Selector.IndexPatterns[0].ProtoReflect().SetUnknown(testUnknownField())
			},
			wantPath: "selector.index_patterns[0]",
			wantText: "knowledge definition contains unknown fields: definition.selector.index_patterns[0]",
		},
		{
			name: "oneof message",
			mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
				definition.GetFieldAlias().ProtoReflect().SetUnknown(testUnknownField())
			},
			wantPath: "field_alias",
			wantText: "knowledge definition contains unknown fields: definition.field_alias",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			definition := validAliasDefinition()
			test.mutate(definition)
			_, err := Normalize(definition)
			if err == nil {
				t.Fatal("Normalize succeeded")
			}
			if err.Error() != test.wantText {
				t.Fatalf("Normalize error = %q, want %q", err, test.wantText)
			}
			want := Issue{
				FieldPath: test.wantPath,
				Code:      IssueCodeUnknownField,
				Message:   "contains an unknown protobuf field",
			}
			assertDefinitionIssue(t, err, want)
			assertDefinitionErrorRoots(t, err, ErrUnknownFields)

		})
	}
}

func TestNormalizeUnknownFieldIssueOrderIsDeterministic(t *testing.T) {
	t.Parallel()

	definition := validAliasDefinition()
	definition.Selector.ProtoReflect().SetUnknown(testUnknownField())
	definition.GetFieldAlias().ProtoReflect().SetUnknown(testUnknownField())
	for range 100 {
		_, err := Normalize(definition)
		assertDefinitionIssue(t, err, Issue{
			FieldPath: "selector",
			Code:      IssueCodeUnknownField,
			Message:   "contains an unknown protobuf field",
		})
	}
}

func TestNormalizePreflightIssuesCoverEveryBoundedShape(t *testing.T) {
	t.Parallel()

	type selectorDimension struct {
		path string
		set  func(*opensplunkv1.KnowledgeSelector, []*opensplunkv1.KnowledgeSelectorPattern)
	}
	dimensions := []selectorDimension{
		{path: "selector.index_patterns", set: func(selector *opensplunkv1.KnowledgeSelector, patterns []*opensplunkv1.KnowledgeSelectorPattern) {
			selector.IndexPatterns = patterns
		}},
		{path: "selector.host_patterns", set: func(selector *opensplunkv1.KnowledgeSelector, patterns []*opensplunkv1.KnowledgeSelectorPattern) {
			selector.HostPatterns = patterns
		}},
		{path: "selector.source_patterns", set: func(selector *opensplunkv1.KnowledgeSelector, patterns []*opensplunkv1.KnowledgeSelectorPattern) {
			selector.SourcePatterns = patterns
		}},
		{path: "selector.sourcetype_patterns", set: func(selector *opensplunkv1.KnowledgeSelector, patterns []*opensplunkv1.KnowledgeSelectorPattern) {
			selector.SourcetypePatterns = patterns
		}},
	}
	for _, dimension := range dimensions {
		t.Run(dimension.path, func(t *testing.T) {
			t.Parallel()
			definition := validAliasDefinition()
			patterns := make(
				[]*opensplunkv1.KnowledgeSelectorPattern,
				knowledge.MaximumSelectorPatternsPerDimension+1,
			)
			dimension.set(definition.Selector, patterns)
			_, err := Normalize(definition)
			message := fmt.Sprintf(
				"exceeds %d entries",
				knowledge.MaximumSelectorPatternsPerDimension,
			)
			assertDefinitionIssue(t, err, Issue{
				FieldPath: dimension.path,
				Code:      IssueCodeResourceLimit,
				Message:   message,
			})
			wantText := fmt.Sprintf("%v: %s %s", ErrDefinitionTooLarge, dimension.path, message)
			if err.Error() != wantText {
				t.Fatalf("Normalize error = %q, want %q", err, wantText)
			}
			assertDefinitionErrorRoots(t, err, ErrDefinitionTooLarge)
		})
	}

	t.Run("regex outputs", func(t *testing.T) {
		t.Parallel()
		definition := validBaseDefinition()
		definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{
						Pattern:      `(?<value>.+)`,
						OutputFields: make([]string, MaximumFieldExtractionOutputs+1),
					},
				},
			},
		}
		_, err := Normalize(definition)
		message := fmt.Sprintf("exceeds %d entries", MaximumFieldExtractionOutputs)
		assertDefinitionIssue(t, err, Issue{
			FieldPath: "field_extraction.regex.output_fields",
			Code:      IssueCodeResourceLimit,
			Message:   message,
		})
		assertDefinitionErrorRoots(t, err, ErrDefinitionTooLarge)
	})

	t.Run("submitted protobuf bytes", func(t *testing.T) {
		definition := validAliasDefinition()
		definition.Name = strings.Repeat("x", MaximumCanonicalBytes+1)
		size := proto.Size(definition)
		_, err := Normalize(definition)
		message := fmt.Sprintf(
			"submitted protobuf contains %d bytes, maximum is %d",
			size,
			MaximumCanonicalBytes,
		)
		assertDefinitionIssue(t, err, Issue{
			Code:    IssueCodeResourceLimit,
			Message: message,
		})
		wantText := fmt.Sprintf("%v: %s", ErrDefinitionTooLarge, message)
		if err.Error() != wantText {
			t.Fatalf("Normalize error = %q, want %q", err, wantText)
		}
		assertDefinitionErrorRoots(t, err, ErrDefinitionTooLarge)
	})
}

func TestCanonicalByteBoundReturnsOnlyCandidateActionableIssues(t *testing.T) {
	t.Parallel()

	if err := validateCanonicalBytes([]byte{1}); err != nil {
		t.Fatalf("validateCanonicalBytes(one byte): %v", err)
	}
	if err := validateCanonicalBytes(make([]byte, MaximumCanonicalBytes)); err != nil {
		t.Fatalf("validateCanonicalBytes(exact ceiling): %v", err)
	}

	message := fmt.Sprintf(
		"canonical bytes must be between 1 and %d",
		MaximumCanonicalBytes,
	)
	overLimit := validateCanonicalBytes(make([]byte, MaximumCanonicalBytes+1))
	assertDefinitionIssue(t, overLimit, Issue{
		Code:    IssueCodeResourceLimit,
		Message: message,
	})
	wantText := fmt.Sprintf("%v: %s", ErrDefinitionTooLarge, message)
	if overLimit.Error() != wantText {
		t.Fatalf("validateCanonicalBytes error = %q, want %q", overLimit, wantText)
	}
	assertDefinitionErrorRoots(t, overLimit, ErrDefinitionTooLarge)

	empty := validateCanonicalBytes(nil)
	if empty == nil || empty.Error() != wantText || !errors.Is(empty, ErrDefinitionTooLarge) {
		t.Fatalf("validateCanonicalBytes(empty) = %v, want untyped %q", empty, wantText)
	}
	if issue, ok := IssueFromError(empty); ok {
		t.Fatalf("IssueFromError(empty invariant) = %#v, want untyped failure", issue)
	}

	for _, err := range []error{
		nil,
		ErrInvalidDefinition,
		fmt.Errorf("%w: deterministic marshal: impossible", ErrInvalidDefinition),
		fmt.Errorf("%w: selector canonicalization failed", ErrInvalidDefinition),
		fmt.Errorf("%w: known metadata is not canonical", ErrNonCanonical),
	} {
		if issue, ok := IssueFromError(err); ok {
			t.Fatalf("IssueFromError(%v) = %#v, want untyped failure", err, issue)
		}
	}
}

func TestNormalizeIssueErrorsPreserveLegacyRootParity(t *testing.T) {
	t.Parallel()

	lowerLevelCauses := []struct {
		name       string
		definition func() *opensplunkv1.KnowledgeObjectDefinition
		cause      error
	}{
		{
			name: "invalid text",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validAliasDefinition()
				definition.Name = ""
				return definition
			},
			cause: knowledge.ErrInvalidText,
		},
		{
			name: "field destination",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validAliasDefinition()
				definition.GetFieldAlias().DestinationField = "INDEX.private"
				return definition
			},
			cause: knowledge.ErrInvalidFieldDestination,
		},
		{
			name: "selector",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validAliasDefinition()
				definition.Selector.IndexPatterns[0].Value = `bad\qescape`
				return definition
			},
			cause: knowledge.ErrInvalidSelector,
		},
		{
			name: "nested resource limit",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validAliasDefinition()
				definition.Name = strings.Repeat("n", knowledge.MaximumObjectNameBytes+1)
				return definition
			},
			cause: knowledge.ErrResourceLimit,
		},
	}
	for _, test := range lowerLevelCauses {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Normalize(test.definition())
			assertDefinitionErrorRoots(t, err, ErrInvalidDefinition)
			if errors.Is(err, test.cause) {
				t.Fatalf("Normalize error newly unwraps lower-level cause %v", test.cause)
			}
		})
	}

	overlongName := lowerLevelCauses[len(lowerLevelCauses)-1].definition()
	_, invalidErr := Normalize(overlongName)
	assertDefinitionIssue(t, invalidErr, Issue{
		FieldPath: "name",
		Code:      IssueCodeInvalidDefinition,
		Message: fmt.Sprintf(
			"%v: object name exceeds %d bytes",
			knowledge.ErrResourceLimit,
			knowledge.MaximumObjectNameBytes,
		),
	})
	assertDefinitionErrorRoots(t, invalidErr, ErrInvalidDefinition)

	unknown := validAliasDefinition()
	unknown.ProtoReflect().SetUnknown(testUnknownField())
	_, unknownErr := Normalize(unknown)
	assertDefinitionErrorRoots(t, unknownErr, ErrUnknownFields)

	overWire := validAliasDefinition()
	overWire.Name = strings.Repeat("w", MaximumCanonicalBytes+1)
	_, resourceErr := Normalize(overWire)
	assertDefinitionErrorRoots(t, resourceErr, ErrDefinitionTooLarge)

	first, ok := IssueFromError(invalidErr)
	if !ok {
		t.Fatal("IssueFromError(invalid) did not find issue")
	}
	first.FieldPath = "mutated"
	first.Code = "MUTATED"
	first.Message = "mutated"
	second, ok := IssueFromError(invalidErr)
	if !ok || second.FieldPath != "name" ||
		second.Code != IssueCodeInvalidDefinition ||
		second.Message == "mutated" {
		t.Fatalf("IssueFromError aliases a prior projection: %#v", second)
	}
}

func assertDefinitionIssue(t *testing.T, err error, want Issue) {
	t.Helper()
	if err == nil {
		t.Fatal("error is nil")
	}
	for _, candidate := range []error{
		err,
		fmt.Errorf("wrapped validation: %w", err),
	} {
		got, ok := IssueFromError(candidate)
		if !ok {
			t.Fatalf("IssueFromError(%v) did not find a structured issue", candidate)
		}
		if got != want {
			t.Fatalf("IssueFromError(%v) = %#v, want %#v", candidate, got, want)
		}
	}
}

func assertDefinitionErrorRoots(t *testing.T, err error, want error) {
	t.Helper()
	if err == nil {
		t.Fatal("error is nil")
	}
	for _, candidate := range []error{
		ErrInvalidDefinition,
		ErrUnknownFields,
		ErrDefinitionTooLarge,
		ErrDigestMismatch,
		ErrNonCanonical,
		ErrUnknownFutureBody,
	} {
		wantMatch := errors.Is(want, candidate)
		if got := errors.Is(err, candidate); got != wantMatch {
			t.Errorf("errors.Is(%v, %v) = %t, want %t", err, candidate, got, wantMatch)
		}
	}
}
