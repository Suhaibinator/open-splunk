package server

import (
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
)

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
