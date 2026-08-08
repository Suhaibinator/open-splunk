package server

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
)

func TestKnowledgeDefinitionSanitizersRejectRepeatedShapeBeforeUnknownWalks(
	t *testing.T,
) {
	t.Parallel()

	newDefinition := func() *opensplunkv1.KnowledgeObjectDefinition {
		patterns := make(
			[]*opensplunkv1.KnowledgeSelectorPattern,
			knowledge.MaximumSelectorPatternsPerDimension+1,
		)
		for index := range patterns {
			patterns[index] = &opensplunkv1.KnowledgeSelectorPattern{}
		}
		// If reflection traversal ran first, this would produce the distinct
		// unsupported-fields error. The shape preflight must win instead.
		addKnowledgeHTTPUnknown(patterns[0])
		return &opensplunkv1.KnowledgeObjectDefinition{
			Selector: &opensplunkv1.KnowledgeSelector{IndexPatterns: patterns},
		}
	}
	tests := []struct {
		name     string
		sanitize func(*opensplunkv1.KnowledgeObjectDefinition) error
	}{
		{
			name: "create",
			sanitize: func(definition *opensplunkv1.KnowledgeObjectDefinition) error {
				_, err := sanitizeCreateKnowledgeObjectRequest(
					&opensplunkv1.CreateKnowledgeObjectRequest{Definition: definition},
				)
				return err
			},
		},
		{
			name: "update",
			sanitize: func(definition *opensplunkv1.KnowledgeObjectDefinition) error {
				_, err := sanitizeUpdateKnowledgeObjectRequest(
					&opensplunkv1.UpdateKnowledgeObjectRequest{Definition: definition},
				)
				return err
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			definition := newDefinition()
			err := test.sanitize(definition)
			var httpError *router.HTTPError
			if !errors.As(err, &httpError) || httpError.StatusCode != http.StatusBadRequest ||
				httpError.Message != "knowledge mutation definition exceeds its entry limit" {
				t.Fatalf("error=%T %v", err, err)
			}
			if len(definition.GetSelector().GetIndexPatterns()[0].ProtoReflect().GetUnknown()) == 0 {
				t.Fatal("preflight rejection traversed and sanitized the hostile definition")
			}
		})
	}
}

func TestKnowledgeDefinitionRepeatedShapePinsEveryEntryBoundary(t *testing.T) {
	t.Parallel()

	patterns := func(count int) []*opensplunkv1.KnowledgeSelectorPattern {
		return make([]*opensplunkv1.KnowledgeSelectorPattern, count)
	}
	selectorMaximum := knowledge.MaximumSelectorPatternsPerDimension
	outputMaximum := knowledgedefinition.MaximumFieldExtractionOutputs
	tests := []struct {
		name       string
		definition *opensplunkv1.KnowledgeObjectDefinition
		want       bool
	}{
		{
			name: "all selector dimensions at maximum",
			definition: &opensplunkv1.KnowledgeObjectDefinition{Selector: &opensplunkv1.KnowledgeSelector{
				IndexPatterns:      patterns(selectorMaximum),
				HostPatterns:       patterns(selectorMaximum),
				SourcePatterns:     patterns(selectorMaximum),
				SourcetypePatterns: patterns(selectorMaximum),
			}},
			want: true,
		},
		{
			name: "host dimension above maximum",
			definition: &opensplunkv1.KnowledgeObjectDefinition{Selector: &opensplunkv1.KnowledgeSelector{
				HostPatterns: patterns(selectorMaximum + 1),
			}},
		},
		{
			name: "regex outputs at maximum",
			definition: &opensplunkv1.KnowledgeObjectDefinition{Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunkv1.FieldExtractionDefinition{Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{OutputFields: make([]string, outputMaximum)},
				}},
			}},
			want: true,
		},
		{
			name: "regex outputs above maximum",
			definition: &opensplunkv1.KnowledgeObjectDefinition{Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunkv1.FieldExtractionDefinition{Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{OutputFields: make([]string, outputMaximum+1)},
				}},
			}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := boundedKnowledgeDefinitionRepeatedShape(test.definition); got != test.want {
				t.Fatalf("bounded=%v, want %v", got, test.want)
			}
		})
	}
}
