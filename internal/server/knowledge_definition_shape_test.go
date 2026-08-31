package server

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
)

func TestKnowledgeDefinitionSanitizersRejectRepeatedShapeBeforeUnknownWalks(
	t *testing.T,
) {
	t.Parallel()

	newDefinition := func() *opensplunk.KnowledgeObjectDefinition {
		patterns := make(
			[]*opensplunk.KnowledgeSelectorPattern,
			knowledge.MaximumSelectorPatternsPerDimension+1,
		)
		for index := range patterns {
			patterns[index] = &opensplunk.KnowledgeSelectorPattern{}
		}
		// If reflection traversal ran first, this would produce the distinct
		// unsupported-fields error. The shape preflight must win instead.
		addKnowledgeHTTPUnknown(patterns[0])
		return &opensplunk.KnowledgeObjectDefinition{
			Selector: &opensplunk.KnowledgeSelector{IndexPatterns: patterns},
		}
	}
	tests := []struct {
		name     string
		sanitize func(*opensplunk.KnowledgeObjectDefinition) error
	}{
		{
			name: "create",
			sanitize: func(definition *opensplunk.KnowledgeObjectDefinition) error {
				_, err := sanitizeCreateKnowledgeObjectRequest(
					t.Context(),
					&opensplunk.CreateKnowledgeObjectRequest{Definition: definition},
				)
				return err
			},
		},
		{
			name: "update",
			sanitize: func(definition *opensplunk.KnowledgeObjectDefinition) error {
				_, err := sanitizeUpdateKnowledgeObjectRequest(
					t.Context(),
					&opensplunk.UpdateKnowledgeObjectRequest{Definition: definition},
				)
				return err
			},
		},
	}
	for _, test := range tests {
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

	patterns := func(count int) []*opensplunk.KnowledgeSelectorPattern {
		return make([]*opensplunk.KnowledgeSelectorPattern, count)
	}
	selectorMaximum := knowledge.MaximumSelectorPatternsPerDimension
	outputMaximum := knowledgedefinition.MaximumFieldExtractionOutputs
	tests := []struct {
		name       string
		definition *opensplunk.KnowledgeObjectDefinition
		want       bool
	}{
		{
			name: "all selector dimensions at maximum",
			definition: &opensplunk.KnowledgeObjectDefinition{Selector: &opensplunk.KnowledgeSelector{
				IndexPatterns:      patterns(selectorMaximum),
				HostPatterns:       patterns(selectorMaximum),
				SourcePatterns:     patterns(selectorMaximum),
				SourcetypePatterns: patterns(selectorMaximum),
			}},
			want: true,
		},
		{
			name: "host dimension above maximum",
			definition: &opensplunk.KnowledgeObjectDefinition{Selector: &opensplunk.KnowledgeSelector{
				HostPatterns: patterns(selectorMaximum + 1),
			}},
		},
		{
			name: "regex outputs at maximum",
			definition: &opensplunk.KnowledgeObjectDefinition{Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunk.FieldExtractionDefinition{Extraction: &opensplunk.FieldExtractionDefinition_Regex{
					Regex: &opensplunk.RegexFieldExtractionDefinition{OutputFields: make([]string, outputMaximum)},
				}},
			}},
			want: true,
		},
		{
			name: "regex outputs above maximum",
			definition: &opensplunk.KnowledgeObjectDefinition{Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunk.FieldExtractionDefinition{Extraction: &opensplunk.FieldExtractionDefinition_Regex{
					Regex: &opensplunk.RegexFieldExtractionDefinition{OutputFields: make([]string, outputMaximum+1)},
				}},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := boundedKnowledgeDefinitionRepeatedShape(test.definition); got != test.want {
				t.Fatalf("bounded=%v, want %v", got, test.want)
			}
		})
	}
}
