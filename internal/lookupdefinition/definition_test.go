package lookupdefinition

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"google.golang.org/protobuf/proto"
)

func TestNormalizeDetachesAndCanonicalizes(t *testing.T) {
	input := &opensplunkv1.LookupDefinition{
		AppId:        "app-main",
		Name:         "service_catalog",
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Selector: &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{
			{Value: " api* ", MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_UNSPECIFIED},
		}},
		KeyMappings: []*opensplunkv1.LookupFieldMapping{{LookupField: "service_id", EventField: "service_key"}},
		OutputMappings: []*opensplunkv1.LookupFieldMapping{
			{LookupField: "owner", EventField: "owner"},
			{LookupField: "tier", EventField: "service_tier"},
		},
		OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
	}
	normalized, err := Normalize(input, []string{"service_id", "owner", "tier"})
	if err != nil {
		t.Fatalf("Normalize(): %v", err)
	}
	if normalized.Definition.GetSelector().GetIndexPatterns()[0].GetValue() != "api*" || normalized.Selector == nil {
		t.Fatalf("selector = %#v", normalized.Definition.GetSelector())
	}
	input.KeyMappings[0].EventField = "mutated"
	if normalized.Definition.GetKeyMappings()[0].GetEventField() != "service_key" {
		t.Fatal("normalized definition aliases caller memory")
	}
}

func TestNormalizeRejectsAmbiguousOrDuplicateMappings(t *testing.T) {
	base := func() *opensplunkv1.LookupDefinition {
		return &opensplunkv1.LookupDefinition{
			AppId: "app", Name: "catalog", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			KeyMappings:       []*opensplunkv1.LookupFieldMapping{{LookupField: "id", EventField: "id"}},
			OutputMappings:    []*opensplunkv1.LookupFieldMapping{{LookupField: "value", EventField: "value"}},
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		}
	}
	tests := []func(*opensplunkv1.LookupDefinition){
		func(value *opensplunkv1.LookupDefinition) { value.OutputMappings[0].EventField = "fields" },
		func(value *opensplunkv1.LookupDefinition) { value.OutputMappings[0].EventField = "__os_private" },
		func(value *opensplunkv1.LookupDefinition) {
			value.KeyMappings = append(value.KeyMappings, &opensplunkv1.LookupFieldMapping{LookupField: "id", EventField: "other"})
		},
		func(value *opensplunkv1.LookupDefinition) { value.OutputMappings[0].LookupField = "missing" },
	}
	for index, mutate := range tests {
		candidate := base()
		mutate(candidate)
		if _, err := Normalize(candidate, []string{"id", "value"}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
}

func TestNormalizeRejectsMappedAssetColumnsOutsideAuthoredLookupGrammar(t *testing.T) {
	for _, column := range []string{"value with space", "__os_value"} {
		definition := &opensplunkv1.LookupDefinition{
			AppId: "app", Name: "catalog", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			KeyMappings:       []*opensplunkv1.LookupFieldMapping{{LookupField: "id", EventField: "id"}},
			OutputMappings:    []*opensplunkv1.LookupFieldMapping{{LookupField: column, EventField: "value"}},
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		}
		if _, err := Normalize(definition, []string{"id", column}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Normalize(%q) error = %v, want ErrInvalid", column, err)
		}
	}
}

func TestNormalizeRejectsSelectorMatchKindDisagreement(t *testing.T) {
	definition := &opensplunkv1.LookupDefinition{
		AppId: "app", Name: "catalog", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Selector: &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
			Value: "api*", MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
		}}},
		KeyMappings:       []*opensplunkv1.LookupFieldMapping{{LookupField: "id", EventField: "id"}},
		OutputMappings:    []*opensplunkv1.LookupFieldMapping{{LookupField: "value", EventField: "value"}},
		OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
	}
	if _, err := Normalize(definition, []string{"id", "value"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Normalize() error = %v, want ErrInvalid", err)
	}
}

func TestNormalizeKeepsLookupColumnsDistinctFromEventFields(t *testing.T) {
	definition := &opensplunkv1.LookupDefinition{
		AppId: "app", Name: "catalog", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		KeyMappings:       []*opensplunkv1.LookupFieldMapping{{LookupField: "fields", EventField: "event_fields"}},
		OutputMappings:    []*opensplunkv1.LookupFieldMapping{{LookupField: "OUTPUT", EventField: "OUTPUTNEW"}},
		OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
	}
	if _, err := Normalize(definition, []string{"fields", "OUTPUT"}); err != nil {
		t.Fatalf("Normalize(representable lookup columns): %v", err)
	}

	for _, marker := range []string{"OUTPUT", "outputnew"} {
		candidate := proto.Clone(definition).(*opensplunkv1.LookupDefinition)
		candidate.KeyMappings[0].LookupField = marker
		if _, err := Normalize(candidate, []string{marker, "OUTPUT"}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Normalize(key lookup marker %q) error = %v, want ErrInvalid", marker, err)
		}
		candidate = proto.Clone(definition).(*opensplunkv1.LookupDefinition)
		candidate.KeyMappings[0].EventField = marker
		if _, err := Normalize(candidate, []string{"fields", "OUTPUT"}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Normalize(key event marker %q) error = %v, want ErrInvalid", marker, err)
		}
	}
}

func TestNormalizeRejectsEventFieldsRuntimeCannotResolve(t *testing.T) {
	definition := &opensplunkv1.LookupDefinition{
		AppId: "app", Name: "catalog", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		KeyMappings:       []*opensplunkv1.LookupFieldMapping{{LookupField: "id", EventField: "id"}},
		OutputMappings:    []*opensplunkv1.LookupFieldMapping{{LookupField: "value", EventField: "value"}},
		OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
	}
	invalid := []string{
		"a..b",
		`a\q`,
		`a\`,
		strings.Repeat("x", 257),
		strings.Repeat("a.", 17) + "a",
		"event\x00field",
		"event\u200bfield",
	}
	for _, eventField := range invalid {
		candidate := proto.Clone(definition).(*opensplunkv1.LookupDefinition)
		candidate.OutputMappings[0].EventField = eventField
		if _, err := Normalize(candidate, []string{"id", "value"}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Normalize(event field %q) error = %v, want ErrInvalid", eventField, err)
		}
	}
	for _, eventField := range []string{`a\.b`, `a\\b`, "a[b]", "a?b"} {
		candidate := proto.Clone(definition).(*opensplunkv1.LookupDefinition)
		candidate.OutputMappings[0].EventField = eventField
		if _, err := Normalize(candidate, []string{"id", "value"}); err != nil {
			t.Fatalf("Normalize(event field %q): %v", eventField, err)
		}
	}
}

func TestLookupNameRejectsControlAndFormatCharacters(t *testing.T) {
	for _, name := range []string{"catalog\x00name", "catalog\u200bname"} {
		if IsValidLookupName(name) {
			t.Fatalf("IsValidLookupName(%q) = true", name)
		}
	}
}

func TestNormalizeRejectsControlAndFormatCharactersInLookupColumns(t *testing.T) {
	for _, column := range []string{"catalog\x00key", "catalog\u200bkey"} {
		definition := &opensplunkv1.LookupDefinition{
			AppId: "app", Name: "catalog", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			KeyMappings:       []*opensplunkv1.LookupFieldMapping{{LookupField: column, EventField: "key"}},
			OutputMappings:    []*opensplunkv1.LookupFieldMapping{{LookupField: "value", EventField: "value"}},
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		}
		if _, err := Normalize(definition, []string{column, "value"}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Normalize(lookup column %q) error = %v, want ErrInvalid", column, err)
		}
	}
}

func TestNormalizePinsCanonicalAuthoredSourceBoundary(t *testing.T) {
	definition := &opensplunkv1.LookupDefinition{
		AppId: "app", Name: "n", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		KeyMappings:       []*opensplunkv1.LookupFieldMapping{{LookupField: "k", EventField: "a"}},
		OutputMappings:    []*opensplunkv1.LookupFieldMapping{{LookupField: "v", EventField: "b"}},
		OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
	}
	baseBytes := len(canonicalAuthoredLookupSource(
		definition.GetName(),
		definition.GetKeyMappings(),
		definition.GetOutputMappings(),
		definition.GetOverwriteBehavior(),
	))
	eventBytes := 2 + spl.MaximumSearchSourceBytes - baseBytes
	definition.KeyMappings[0].EventField = exactEventPathBytes(8_000)
	definition.OutputMappings[0].EventField = exactEventPathBytes(eventBytes - 8_000)

	source := canonicalAuthoredLookupSource(
		definition.GetName(),
		definition.GetKeyMappings(),
		definition.GetOutputMappings(),
		definition.GetOverwriteBehavior(),
	)
	if len(source) != spl.MaximumSearchSourceBytes {
		t.Fatalf("canonical source = %d bytes, want %d", len(source), spl.MaximumSearchSourceBytes)
	}
	if _, err := spl.Parse(source); err != nil {
		t.Fatalf("Parse(exact source boundary): %v", err)
	}
	if _, err := Normalize(definition, []string{"k", "v"}); err != nil {
		t.Fatalf("Normalize(exact source boundary): %v", err)
	}

	over := proto.Clone(definition).(*opensplunkv1.LookupDefinition)
	over.OutputMappings[0].EventField = exactEventPathBytes(len(over.OutputMappings[0].GetEventField()) + 1)
	if got := len(canonicalAuthoredLookupSource(
		over.GetName(),
		over.GetKeyMappings(),
		over.GetOutputMappings(),
		over.GetOverwriteBehavior(),
	)); got != spl.MaximumSearchSourceBytes+1 {
		t.Fatalf("one-byte-over source = %d bytes", got)
	}
	if _, err := Normalize(over, []string{"k", "v"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Normalize(one byte over source boundary) error = %v, want ErrInvalid", err)
	}
}

func TestNormalizeRejectsMultiMappingAuthoredSourceAggregate(t *testing.T) {
	definition := &opensplunkv1.LookupDefinition{
		AppId: "app", Name: "catalog", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
	}
	columns := make([]string, 0, MaximumKeyMappings+MaximumOutputMappings)
	for index := range MaximumKeyMappings + MaximumOutputMappings {
		column := fmt.Sprintf("column_%02d", index)
		event := fmt.Sprintf("event_%02d.%s.%s.%s.%s", index,
			strings.Repeat("a", 250),
			strings.Repeat("b", 250),
			strings.Repeat("c", 250),
			strings.Repeat("d", 250),
		)
		mapping := &opensplunkv1.LookupFieldMapping{LookupField: column, EventField: event}
		if index < MaximumKeyMappings {
			definition.KeyMappings = append(definition.KeyMappings, mapping)
		} else {
			definition.OutputMappings = append(definition.OutputMappings, mapping)
		}
		columns = append(columns, column)
	}
	if sourceBytes := len(canonicalAuthoredLookupSource(
		definition.GetName(),
		definition.GetKeyMappings(),
		definition.GetOutputMappings(),
		definition.GetOverwriteBehavior(),
	)); sourceBytes <= spl.MaximumSearchSourceBytes {
		t.Fatalf("multi-mapping source = %d bytes, want over %d", sourceBytes, spl.MaximumSearchSourceBytes)
	}
	if _, err := Normalize(definition, columns); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Normalize(multi-mapping source aggregate) error = %v, want ErrInvalid", err)
	}
}

func exactEventPathBytes(length int) string {
	const maximumEncodedSegmentBytes = 2 * 256
	const maximumSegments = 17
	segments := (length + maximumEncodedSegmentBytes) / (maximumEncodedSegmentBytes + 1)
	if segments < 1 || segments > maximumSegments {
		panic(fmt.Sprintf("event path length %d is outside the test helper's range", length))
	}
	remaining := length - (segments - 1)
	result := make([]string, 0, segments)
	for index := 0; index < segments; index++ {
		segmentsLeft := segments - index
		segmentBytes := min(maximumEncodedSegmentBytes, remaining-(segmentsLeft-1))
		segment := ""
		if segmentBytes%2 != 0 {
			segment = "a"
		}
		segment += strings.Repeat(`\.`, segmentBytes/2)
		result = append(result, segment)
		remaining -= segmentBytes
	}
	value := strings.Join(result, ".")
	if len(value) != length {
		panic(fmt.Sprintf("event path helper produced %d bytes, want %d", len(value), length))
	}
	return value
}

func TestValidLookupNameMatchesExactSPLWordTokens(t *testing.T) {
	for _, name := range []string{"service/catalog", "service+catalog", "世界", "a?b", "a[b]", "a{b}", ".com"} {
		if !IsValidLookupName(name) {
			t.Errorf("IsValidLookupName(%q) = false", name)
		}
	}
	for _, name := range []string{"", ".", "a!b", "a<b", "a>b", "a=b", "a|b", "a,b", "a b", "fields", "__os_private"} {
		if IsValidLookupName(name) {
			t.Errorf("IsValidLookupName(%q) = true", name)
		}
	}
}
