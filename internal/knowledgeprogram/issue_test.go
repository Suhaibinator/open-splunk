package knowledgeprogram

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestCandidateRegexIssuesPreserveLegacyFailureContract(t *testing.T) {
	programBomb := `(?P<value>` + strings.Repeat("a{1000}", 5) + `)`
	var tooManyCaptures strings.Builder
	for index := 0; index <= splregex.MaximumExtractionCaptureGroups; index++ {
		fmt.Fprintf(&tooManyCaptures, "(?P<f%02d>x)", index)
	}
	tests := []struct {
		name        string
		pattern     string
		outputs     []string
		wantCode    IssueCode
		wantPath    string
		wantMessage string
		wantError   string
	}{
		{
			name:        "invalid syntax",
			pattern:     `(?P<value>`,
			outputs:     []string{"value"},
			wantCode:    IssueCodeRegexInvalid,
			wantPath:    "field_extraction.regex.pattern",
			wantMessage: "pattern is not a supported RE2 extraction expression",
		},
		{
			name:        "no named capture",
			pattern:     `(x)`,
			outputs:     []string{"value"},
			wantCode:    IssueCodeRegexInvalid,
			wantPath:    "field_extraction.regex.pattern",
			wantMessage: "pattern must contain at least one named capture",
		},
		{
			name:        "duplicate named capture",
			pattern:     `(?P<value>x)|(?P<value>y)`,
			outputs:     []string{"value"},
			wantCode:    IssueCodeRegexInvalid,
			wantPath:    "field_extraction.regex.pattern",
			wantMessage: "pattern must use each named capture name once",
		},
		{
			name:        "program resource limit",
			pattern:     programBomb,
			outputs:     []string{"value"},
			wantCode:    IssueCodeRegexResourceLimit,
			wantPath:    "field_extraction.regex.pattern",
			wantMessage: "pattern exceeds extraction complexity limits",
		},
		{
			name:        "capture resource limit",
			pattern:     tooManyCaptures.String(),
			outputs:     []string{"f00"},
			wantCode:    IssueCodeRegexResourceLimit,
			wantPath:    "field_extraction.regex.pattern",
			wantMessage: "pattern exceeds extraction complexity limits",
		},
		{
			name:        "unnamed capture",
			pattern:     `([a])(?P<named>b)`,
			outputs:     []string{"named"},
			wantCode:    IssueCodeRegexCaptureMismatch,
			wantPath:    "field_extraction.regex.pattern",
			wantMessage: "every capture group must be named",
		},
		{
			name:        "output count",
			pattern:     `(?P<first>x)(?P<second>y)`,
			outputs:     []string{"first"},
			wantCode:    IssueCodeRegexCaptureMismatch,
			wantPath:    "field_extraction.regex.output_fields",
			wantMessage: "output fields must exactly match named captures",
		},
		{
			name:        "output order",
			pattern:     `(?P<first>x)(?P<second>y)`,
			outputs:     []string{"second", "first"},
			wantCode:    IssueCodeRegexCaptureMismatch,
			wantPath:    "field_extraction.regex.output_fields[0]",
			wantMessage: "output field must match the named capture at the same position",
			wantError:   "invalid knowledge program: object 0 regex captures disagree with declared outputs",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := regexIssueDefinition(test.pattern, test.outputs)
			input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{definition})
			_, err := Compile(input.Objects)
			if err == nil {
				t.Fatal("Compile(candidate) succeeded")
			}
			wantError := test.wantError
			if wantError == "" {
				wantError = "invalid knowledge program: object 0 regex extraction is not executable"
			}
			if err.Error() != wantError || !errors.Is(err, ErrInvalidProgram) ||
				errors.Is(err, ErrResourceLimit) || errors.Unwrap(err) != ErrInvalidProgram {
				t.Fatalf("Compile error = %q, ErrInvalidProgram=%t ErrResourceLimit=%t", err, errors.Is(err, ErrInvalidProgram), errors.Is(err, ErrResourceLimit))
			}
			issue, ok := CandidateIssueFromError(err, 0)
			if !ok {
				t.Fatal("CandidateIssueFromError did not find candidate issue")
			}
			if issue.Code != test.wantCode || issue.FieldPath != test.wantPath ||
				issue.Message != test.wantMessage || issue.Range != nil || issue.Suggestions != nil {
				t.Fatalf("issue = %#v", issue)
			}
			if errors.Is(err, splregex.ErrInvalidExtractionPattern) ||
				errors.Is(err, splregex.ErrNoNamedCapture) ||
				errors.Is(err, splregex.ErrDuplicateNamedCapture) ||
				errors.Is(err, splregex.ErrExtractionPatternTooLarge) ||
				errors.Is(err, splregex.ErrTooManyExtractionCaptures) {
				t.Fatal("Compile error newly unwraps a regex implementation sentinel")
			}
		})
	}
}

func TestCandidateJSONPathIssuesHaveCanonicalUTF8ByteRanges(t *testing.T) {
	overSteps := strings.TrimSuffix(strings.Repeat("a.", splpath.MaximumPathSteps+1), ".")
	tests := []struct {
		name      string
		path      string
		wantCode  IssueCode
		wantText  string
		wantStart int
		wantEnd   int
		wantSlice string
	}{
		{
			name: "empty step", path: "payload..name", wantCode: IssueCodeJSONPathInvalid,
			wantText: "path contains an empty location step", wantStart: 8, wantEnd: 9, wantSlice: ".",
		},
		{
			name: "unsupported wildcard", path: "items{*}.name", wantCode: IssueCodeJSONPathUnsupported,
			wantText: "wildcard JSON path keys are not supported", wantStart: 6, wantEnd: 7, wantSlice: "*",
		},
		{
			name: "step resource limit", path: overSteps, wantCode: IssueCodeJSONPathResourceLimit,
			wantText:  fmt.Sprintf("path contains more than %d location steps", splpath.MaximumPathSteps),
			wantStart: splpath.MaximumPathSteps * 2, wantEnd: splpath.MaximumPathSteps*2 + 1, wantSlice: "a",
		},
		{
			name: "multibyte prefix", path: "😀{x}", wantCode: IssueCodeJSONPathInvalid,
			wantText: "array index must be an unsigned decimal integer", wantStart: 5, wantEnd: 6, wantSlice: "x",
		},
		{
			name: "trailing step EOF", path: "payload.", wantCode: IssueCodeJSONPathInvalid,
			wantText: "path contains an empty trailing location step", wantStart: 8, wantEnd: 8,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := jsonIssueDefinition(test.path)
			input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{definition})
			_, err := Compile(input.Objects)
			if err == nil || err.Error() != "invalid knowledge program: object 0 JSON extraction is not executable" ||
				!errors.Is(err, ErrInvalidProgram) {
				t.Fatalf("Compile error = %v", err)
			}
			var pathErr *splpath.Error
			if errors.As(err, &pathErr) {
				t.Fatalf("Compile error newly exposes path error: %#v", pathErr)
			}
			issue, ok := CandidateIssueFromError(err, 0)
			if !ok || issue.Code != test.wantCode ||
				issue.FieldPath != "field_extraction.json.path" || issue.Message != test.wantText ||
				issue.Range == nil || int(issue.Range.StartByteOffset) != test.wantStart ||
				int(issue.Range.EndByteOffset) != test.wantEnd || issue.Suggestions != nil {
				t.Fatalf("issue = %#v", issue)
			}
			if !utf8.ValidString(test.path[:issue.Range.StartByteOffset]) ||
				!utf8.ValidString(test.path[:issue.Range.EndByteOffset]) ||
				test.path[issue.Range.StartByteOffset:issue.Range.EndByteOffset] != test.wantSlice {
				t.Fatalf("issue range = %#v in %q", issue.Range, test.path)
			}
		})
	}
}

func TestCandidateCalculatedIssueRetainsSPLDiagnosticAndCanonicalTrimBasis(t *testing.T) {
	raw := " \n\tcoalesce(\"😀\", mystery(host)) \r\n"
	definition := calculatedIssueDefinition(raw)
	input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{definition})
	canonical := input.Objects[0].Definition.GetCalculatedField().GetExpression()
	if canonical != `coalesce("😀", mystery(host))` {
		t.Fatalf("canonical expression = %q", canonical)
	}
	_, err := Compile(input.Objects)
	if err == nil || err.Error() != "invalid knowledge program: object 0 calculated expression is not executable" ||
		!errors.Is(err, ErrInvalidProgram) || errors.Is(err, ErrResourceLimit) {
		t.Fatalf("Compile error = %v", err)
	}
	var diagnostic *spl.Diagnostic
	if errors.As(err, &diagnostic) {
		t.Fatalf("Compile error newly exposes SPL diagnostic: %#v", diagnostic)
	}
	issue, ok := CandidateIssueFromError(err, 0)
	if !ok || issue.FieldPath != "calculated_field.expression" ||
		issue.Code != IssueCode("SPL_UNSUPPORTED_EVAL_FUNCTION") ||
		issue.Message != `eval function "mystery" is not supported` || issue.Range == nil ||
		len(issue.Suggestions) == 0 {
		t.Fatalf("issue = %#v", issue)
	}
	wantStart := strings.Index(canonical, "mystery")
	wantEnd := wantStart + len("mystery")
	if int(issue.Range.StartByteOffset) != wantStart || int(issue.Range.EndByteOffset) != wantEnd ||
		canonical[issue.Range.StartByteOffset:issue.Range.EndByteOffset] != "mystery" {
		t.Fatalf("canonical range = %#v in %q", issue.Range, canonical)
	}
	leadingTrim := len(raw) - len(strings.TrimLeft(raw, " \t\n\v\f\r"))
	if raw[leadingTrim+wantStart:leadingTrim+wantEnd] != "mystery" {
		t.Fatalf("rebased range does not select candidate token in %q", raw)
	}
}

func TestCandidateCalculatedEOFBasisAndBooleanRange(t *testing.T) {
	t.Run("parser EOF after trailing trim", func(t *testing.T) {
		raw := "\n\tlower(host \r\n"
		input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{calculatedIssueDefinition(raw)})
		canonical := input.Objects[0].Definition.GetCalculatedField().GetExpression()
		_, err := Compile(input.Objects)
		issue, ok := CandidateIssueFromError(err, 0)
		if !ok || issue.Code != IssueCode("SPL_EXPECTED_RIGHT_PAREN") || issue.Range == nil ||
			issue.Range.StartByteOffset != uint32(len(canonical)) ||
			issue.Range.EndByteOffset != uint32(len(canonical)) {
			t.Fatalf("EOF issue = %#v, error = %v", issue, err)
		}
		// A public adapter can reproduce the untrimmed SPL lexer location: this
		// canonical zero-width EOF maps to len(raw), after trailing whitespace.
		leadingTrim := len(raw) - len(strings.TrimLeft(raw, " \t\n\v\f\r"))
		coreEnd := len(strings.TrimRight(raw, " \t\n\v\f\r"))
		canonicalEOF := leadingTrim + int(issue.Range.StartByteOffset)
		if canonicalEOF != coreEnd || canonicalEOF >= len(raw) {
			t.Fatalf("canonical EOF rebase = %d, core/raw ends = %d/%d", canonicalEOF, coreEnd, len(raw))
		}
		publicEOF := canonicalEOF
		if issue.Range.StartByteOffset == issue.Range.EndByteOffset &&
			issue.Range.EndByteOffset == uint32(len(canonical)) {
			publicEOF = len(raw)
		}
		if publicEOF != len(raw) {
			t.Fatalf("public EOF = %d, want %d", publicEOF, len(raw))
		}
	})

	t.Run("direct Boolean covers canonical expression", func(t *testing.T) {
		raw := " \tisnull(host)\r\n"
		input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{calculatedIssueDefinition(raw)})
		canonical := input.Objects[0].Definition.GetCalculatedField().GetExpression()
		_, err := Compile(input.Objects)
		if err == nil || err.Error() != "invalid knowledge program: object 0 calculated expression cannot directly assign a Boolean function result" {
			t.Fatalf("Compile error = %v", err)
		}
		issue, ok := CandidateIssueFromError(err, 0)
		if !ok || issue.Code != IssueCodeCalculatedBoolean || issue.Range == nil ||
			issue.Range.StartByteOffset != 0 || issue.Range.EndByteOffset != uint32(len(canonical)) ||
			canonical[issue.Range.StartByteOffset:issue.Range.EndByteOffset] != canonical ||
			issue.Suggestions != nil {
			t.Fatalf("Boolean issue = %#v", issue)
		}
	})
}

func TestCandidateIssueExtractionIsIndexBoundWrappedAndDetached(t *testing.T) {
	definitions := []*opensplunkv1.KnowledgeObjectDefinition{
		regexIssueDefinition(`(?P<first>x)`, []string{"first"}),
		regexIssueDefinition(`(?P<second>`, []string{"second"}),
	}
	definitions[0].Name = "extract-a"
	definitions[1].Name = "extract-b"
	input := inputFromDefinitions(t, definitions)
	_, err := Compile(input.Objects)
	if err == nil || err.Error() != "invalid knowledge program: object 1 regex extraction is not executable" {
		t.Fatalf("Compile error = %v", err)
	}
	if issue, ok := CandidateIssueFromError(err, 0); ok {
		t.Fatalf("wrong candidate index extracted %#v", issue)
	}
	wrapped := fmt.Errorf("outer context: %w", err)
	first, ok := CandidateIssueFromError(wrapped, 1)
	if !ok || first.Code != IssueCodeRegexInvalid {
		t.Fatalf("wrapped issue = %#v/%t", first, ok)
	}
	first.FieldPath = "mutated"
	first.Message = "mutated"
	second, ok := CandidateIssueFromError(wrapped, 1)
	if !ok || second.FieldPath != "field_extraction.regex.pattern" ||
		second.Message != "pattern is not a supported RE2 extraction expression" {
		t.Fatalf("issue aliases scalar projection: %#v", second)
	}

	calculated := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{
		calculatedIssueDefinition("mystery(host)"),
	})
	_, calculatedErr := Compile(calculated.Objects)
	projected, ok := CandidateIssueFromError(calculatedErr, 0)
	if !ok || projected.Range == nil || len(projected.Suggestions) == 0 {
		t.Fatalf("calculated issue = %#v", projected)
	}
	wantRange := *projected.Range
	wantSuggestion := projected.Suggestions[0]
	projected.Range.StartByteOffset++
	projected.Suggestions[0] = "mutated"
	again, ok := CandidateIssueFromError(calculatedErr, 0)
	if !ok || again.Range == nil || *again.Range != wantRange || again.Suggestions[0] != wantSuggestion {
		t.Fatalf("issue aliases range or suggestions: %#v", again)
	}
	if issue, ok := CandidateIssueFromError(nil, 0); ok || !reflect.DeepEqual(issue, Issue{}) {
		t.Fatalf("nil error issue = %#v/%t", issue, ok)
	}
	if issue, ok := CandidateIssueFromError(ErrInvalidProgram, 0); ok || !reflect.DeepEqual(issue, Issue{}) {
		t.Fatalf("bare sentinel issue = %#v/%t", issue, ok)
	}
}

func TestPrepareAndCohortAuthorityFailuresRemainUntyped(t *testing.T) {
	t.Run("Prepare candidate syntax remains opaque", func(t *testing.T) {
		input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{
			calculatedIssueDefinition("mystery(host)"),
		})
		_, err := Prepare(input)
		if err == nil || err.Error() != "invalid knowledge program: object 0 calculated expression is not executable" ||
			!errors.Is(err, ErrInvalidProgram) {
			t.Fatalf("Prepare error = %v", err)
		}
		if issue, ok := CandidateIssueFromError(err, 0); ok {
			t.Fatalf("Prepare exposed candidate issue %#v", issue)
		}
	})

	t.Run("object authority fails before later candidate", func(t *testing.T) {
		input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{
			regexIssueDefinition(`(?P<first>x)`, []string{"first"}),
			calculatedIssueDefinition("mystery(host)"),
		})
		input.Objects[0].Version = 0
		_, err := Compile(input.Objects)
		if err == nil || err.Error() != "invalid knowledge program: object 0 object authority is invalid" {
			t.Fatalf("Compile error = %v", err)
		}
		if issue, ok := CandidateIssueFromError(err, 1); ok {
			t.Fatalf("later candidate bypassed fail-fast authority: %#v", issue)
		}
	})

	t.Run("earlier candidate issue fails before later authority", func(t *testing.T) {
		input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{
			regexIssueDefinition(`(?P<first>`, []string{"first"}),
			calculatedIssueDefinition("lower(host)"),
		})
		input.Objects[1].Version = 0
		_, err := Compile(input.Objects)
		if err == nil || err.Error() != "invalid knowledge program: object 0 regex extraction is not executable" {
			t.Fatalf("Compile error = %v", err)
		}
		if issue, ok := CandidateIssueFromError(err, 0); !ok || issue.Code != IssueCodeRegexInvalid {
			t.Fatalf("earlier candidate issue = %#v/%t", issue, ok)
		}
	})

	t.Run("definition authority disagreement", func(t *testing.T) {
		input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{
			calculatedIssueDefinition("lower(host)"),
		})
		input.Objects[0].Definition.GetCalculatedField().Expression = "mystery(host)"
		_, err := Compile(input.Objects)
		if err == nil || err.Error() != "invalid knowledge program: object 0 definition authority disagrees" {
			t.Fatalf("Compile error = %v", err)
		}
		if issue, ok := CandidateIssueFromError(err, 0); ok {
			t.Fatalf("noncanonical authority exposed issue %#v", issue)
		}
	})

	t.Run("aggregate object limit", func(t *testing.T) {
		_, err := Compile(make([]*opensplunkv1.KnowledgeSnapshotObject, MaximumObjects+1))
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("Compile error = %v", err)
		}
		if issue, ok := CandidateIssueFromError(err, 0); ok {
			t.Fatalf("aggregate limit exposed issue %#v", issue)
		}
	})

	t.Run("same-stage cohort conflict", func(t *testing.T) {
		left := aliasDefinition("alias-a", "source_a", "shared", nil, opensplunkv1.SharingScope_SHARING_SCOPE_APP, "app-a")
		right := aliasDefinition("alias-b", "source_b", "shared", nil, opensplunkv1.SharingScope_SHARING_SCOPE_APP, "app-a")
		input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{left, right})
		_, err := Compile(input.Objects)
		if !errors.Is(err, ErrInvalidProgram) {
			t.Fatalf("Compile error = %v", err)
		}
		for index := range input.Objects {
			if issue, ok := CandidateIssueFromError(err, uint32(index)); ok {
				t.Fatalf("cohort conflict exposed issue %#v", issue)
			}
		}
	})

	t.Run("selector implication conflict", func(t *testing.T) {
		extraction := regexDefinition("extract-a", "derived", hostSelector("api-*"), opensplunkv1.SharingScope_SHARING_SCOPE_APP, "app-a")
		alias := aliasDefinition("alias-a", "derived", "alias_value", hostSelector("api-?"), opensplunkv1.SharingScope_SHARING_SCOPE_APP, "app-a")
		input := inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{extraction, alias})
		_, err := Compile(input.Objects)
		if !errors.Is(err, ErrInvalidProgram) {
			t.Fatalf("Compile error = %v", err)
		}
		for index := range input.Objects {
			if issue, ok := CandidateIssueFromError(err, uint32(index)); ok {
				t.Fatalf("selector/cohort conflict exposed issue %#v", issue)
			}
		}
	})
}

func TestUnknownOrMalformedLowerLevelIssuesFailClosed(t *testing.T) {
	if issue, ok := regexCompilationIssue(errors.New("future regex failure")); ok || !reflect.DeepEqual(issue, Issue{}) {
		t.Fatalf("unknown regex issue = %#v/%t", issue, ok)
	}
	if issue, ok := jsonPathCompilationIssue("a", &splpath.Error{Kind: 99, Offset: 0, Message: "future"}); ok || !reflect.DeepEqual(issue, Issue{}) {
		t.Fatalf("unknown path kind issue = %#v/%t", issue, ok)
	}
	if issue, ok := jsonPathCompilationIssue("a", &splpath.Error{Kind: splpath.ErrorKindInvalid, Offset: 2, Message: "outside"}); ok || !reflect.DeepEqual(issue, Issue{}) {
		t.Fatalf("invalid path range issue = %#v/%t", issue, ok)
	}
	badRange := &spl.Diagnostic{
		Code: "SPL_FUTURE", Message: "future",
		Range: spl.Range{Start: spl.Position{Offset: 2}, End: spl.Position{Offset: 2}},
	}
	if issue, ok := calculatedExpressionCompilationIssue("a", badRange); ok || !reflect.DeepEqual(issue, Issue{}) {
		t.Fatalf("invalid SPL range issue = %#v/%t", issue, ok)
	}
	if issue, ok := calculatedExpressionCompilationIssue("a", errors.New("future scalar failure")); ok || !reflect.DeepEqual(issue, Issue{}) {
		t.Fatalf("unknown scalar issue = %#v/%t", issue, ok)
	}
}

func TestIssuePublicShapeCarriesNoObjectOrCatalogAuthority(t *testing.T) {
	typeOfIssue := reflect.TypeOf(Issue{})
	want := []struct {
		name   string
		typeOf reflect.Type
	}{
		{name: "FieldPath", typeOf: reflect.TypeOf("")},
		{name: "Code", typeOf: reflect.TypeOf(IssueCode(""))},
		{name: "Message", typeOf: reflect.TypeOf("")},
		{name: "Range", typeOf: reflect.TypeOf((*ScalarRange)(nil))},
		{name: "Suggestions", typeOf: reflect.TypeOf([]string(nil))},
	}
	if typeOfIssue.NumField() != len(want) {
		t.Fatalf("Issue has %d fields, want %d", typeOfIssue.NumField(), len(want))
	}
	for index, expected := range want {
		field := typeOfIssue.Field(index)
		if field.Name != expected.name || field.Type != expected.typeOf {
			t.Fatalf("Issue field %d = %s %v, want %s %v", index, field.Name, field.Type, expected.name, expected.typeOf)
		}
	}
}

func regexIssueDefinition(pattern string, outputs []string) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-a", Name: "extract-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw",
			Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{Regex: &opensplunkv1.RegexFieldExtractionDefinition{
				Pattern: pattern, OutputFields: append([]string(nil), outputs...),
			}},
		}},
	}
}

func jsonIssueDefinition(path string) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-a", Name: "extract-json", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw",
			Extraction: &opensplunkv1.FieldExtractionDefinition_Json{Json: &opensplunkv1.JsonFieldExtractionDefinition{
				Path: path, OutputField: "json_value",
			}},
		}},
	}
}

func calculatedIssueDefinition(expression string) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-a", Name: "calculated-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			DestinationField: "calculated_value", Expression: expression,
		}},
	}
}
