package clickhouse

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

const (
	// One knowledge regex result is a positional tuple. The first three
	// elements are aggregate charges; every later element is one
	// (present UInt8, value String) capture tuple in definition order.
	knowledgeRegexSelectorInputBytesElement = 1
	knowledgeRegexSelectorQueryUnitsElement = 2
	knowledgeRegexCapturedBytesElement      = 3
	knowledgeRegexFirstCaptureElement       = 4

	maxCompiledKnowledgeRegexExtractionSQLBytes = 64 << 10
)

type compiledKnowledgeRegexCapture struct {
	name               string
	group              uint16
	tupleElement       int
	definitionLocation string
}

func (capture compiledKnowledgeRegexCapture) presentSQL(resultSQL string) string {
	return "toUInt8(tupleElement(tupleElement(" + resultSQL + ", " +
		strconv.Itoa(capture.tupleElement) + "), 1))"
}

func (capture compiledKnowledgeRegexCapture) valueSQL(resultSQL string) string {
	return "tupleElement(tupleElement(" + resultSQL + ", " +
		strconv.Itoa(capture.tupleElement) + "), 2)"
}

// compiledKnowledgeRegexExtraction is one row-local, immutable emission. The
// caller owns stage-wide overwrite merging and the aggregate selector/capture
// guards. operation retains the exact typed provenance and overwrite authority
// against which final compiler evidence can be checked.
type compiledKnowledgeRegexExtraction struct {
	sql              string
	args             []any
	operation        knowledgeprogram.RegexExtraction
	captures         []compiledKnowledgeRegexCapture
	programWorkUnits uint64
}

func (compiled compiledKnowledgeRegexExtraction) selectorInputBytesSQL(resultSQL string) string {
	return "toUInt128(tupleElement(" + resultSQL + ", " +
		strconv.Itoa(knowledgeRegexSelectorInputBytesElement) + "))"
}

func (compiled compiledKnowledgeRegexExtraction) selectorQueryUnitsSQL(resultSQL string) string {
	return "toUInt128(tupleElement(" + resultSQL + ", " +
		strconv.Itoa(knowledgeRegexSelectorQueryUnitsElement) + "))"
}

func (compiled compiledKnowledgeRegexExtraction) capturedBytesSQL(resultSQL string) string {
	return "toUInt128(tupleElement(" + resultSQL + ", " +
		strconv.Itoa(knowledgeRegexCapturedBytesElement) + "))"
}

type knowledgeRegexCaptureAuthority struct {
	name               string
	group              uint16
	definitionLocation string
}

type knowledgeRegexExtractionAuthority struct {
	origin    knowledgeprogram.Origin
	selector  knowledgeprogram.Selector
	overwrite knowledgeprogram.OverwriteBehavior
	input     string
	pattern   string
	captures  []knowledgeRegexCaptureAuthority
	workUnits uint64
	operation knowledgeprogram.RegexExtraction
}

// compileKnowledgeRegexExtraction lowers exactly one immutable regex object.
// It intentionally reads only the canonical stored _raw lineage and canonical
// selector columns. A false selector, non-UTF-8/Bytes/null raw value, or regex
// miss returns every capture as absent without evaluating a later branch.
func compileKnowledgeRegexExtraction(
	operation knowledgeprogram.RegexExtraction,
) (compiledKnowledgeRegexExtraction, error) {
	return compileKnowledgeRegexExtractionAuthority(
		knowledgeRegexExtractionAuthorityFromOperation(operation),
	)
}

func knowledgeRegexExtractionAuthorityFromOperation(
	operation knowledgeprogram.RegexExtraction,
) knowledgeRegexExtractionAuthority {
	captures := operation.Captures()
	authority := knowledgeRegexExtractionAuthority{
		origin:    operation.Origin(),
		selector:  operation.Selector(),
		overwrite: operation.Overwrite(),
		input:     operation.Input(),
		pattern:   operation.Pattern(),
		captures:  make([]knowledgeRegexCaptureAuthority, len(captures)),
		workUnits: operation.ProgramWorkUnits(),
		operation: operation,
	}
	for index, capture := range captures {
		authority.captures[index] = knowledgeRegexCaptureAuthority{
			name:               capture.Name(),
			group:              capture.Group(),
			definitionLocation: capture.DefinitionLocation(),
		}
	}
	return authority
}

func compileKnowledgeRegexExtractionAuthority(
	authority knowledgeRegexExtractionAuthority,
) (compiledKnowledgeRegexExtraction, error) {
	if !knowledgeRegexExtractionOperationMatchesAuthority(authority.operation, authority) {
		return compiledKnowledgeRegexExtraction{}, errors.New(
			"compile ClickHouse knowledge regex extraction: retained operation disagrees with authority",
		)
	}
	validated, err := validateKnowledgeRegexExtractionAuthority(authority)
	if err != nil {
		return compiledKnowledgeRegexExtraction{}, err
	}
	selector, err := compileKnowledgeSelector(authority.selector)
	if err != nil {
		return compiledKnowledgeRegexExtraction{}, fmt.Errorf(
			"compile ClickHouse knowledge regex extraction selector: %w",
			err,
		)
	}

	const (
		selectorVariable = "__os_ko_regex_selector"
		groupsVariable   = "__os_ko_regex_groups"
		matchedVariable  = "__os_ko_regex_matched"
	)
	resultCaptures := make([]compiledKnowledgeRegexCapture, len(authority.captures))
	tupleElements := []string{
		"toUInt128(tupleElement(" + selectorVariable + ", 2))",
		"toUInt128(tupleElement(" + selectorVariable + ", 3))",
		"toUInt128(arraySum(value -> toUInt128(length(value)), " + groupsVariable + "))",
	}
	for index, capture := range authority.captures {
		tupleElement := knowledgeRegexFirstCaptureElement + index
		tupleElements = append(tupleElements,
			"tuple(toUInt8("+matchedVariable+"), arrayElement("+groupsVariable+", "+
				strconv.Itoa(int(capture.group))+"))",
		)
		resultCaptures[index] = compiledKnowledgeRegexCapture{
			name:               capture.name,
			group:              capture.group,
			tupleElement:       tupleElement,
			definitionLocation: capture.definitionLocation,
		}
	}

	resultSQL := "tuple(" + strings.Join(tupleElements, ", ") + ")"
	resultSQL = bindSQLExpressions(
		[]string{matchedVariable},
		[]string{"toUInt8(notEmpty(" + groupsVariable + "))"},
		resultSQL,
	)
	inputEligible := "isNotNull(" + quoteIdentifier("_raw") + ") AND " +
		quoteIdentifier(internalRawEncodingColumn) + " = " + strconv.Itoa(rawEncodingUTF8) +
		" AND isValidUTF8(" + quoteIdentifier("_raw") + ")"
	groupsSQL := "if(tupleElement(" + selectorVariable + ", 1) != 0, " +
		"if(" + inputEligible + ", extractGroups(assumeNotNull(" +
		quoteIdentifier("_raw") + "), ?), CAST([], 'Array(String)')), " +
		"CAST([], 'Array(String)'))"
	resultSQL = bindSQLExpressions(
		[]string{groupsVariable},
		[]string{groupsSQL},
		resultSQL,
	)
	resultSQL = bindSQLExpressions(
		[]string{selectorVariable},
		[]string{selector.sql},
		resultSQL,
	)
	if len(resultSQL) > maxCompiledKnowledgeRegexExtractionSQLBytes {
		return compiledKnowledgeRegexExtraction{}, errors.New(
			"compile ClickHouse knowledge regex extraction: generated SQL exceeds the per-object limit",
		)
	}
	args := make([]any, 0, 1+len(selector.args))
	args = append(args, authority.pattern)
	args = append(args, selector.args...)
	return compiledKnowledgeRegexExtraction{
		sql:              resultSQL,
		args:             args,
		operation:        authority.operation,
		captures:         resultCaptures,
		programWorkUnits: uint64(validated.ProgramWorkUnits), // #nosec G115 -- the regex compiler returns a positive bounded work estimate.
	}, nil
}

func knowledgeRegexExtractionOperationMatchesAuthority(
	operation knowledgeprogram.RegexExtraction,
	authority knowledgeRegexExtractionAuthority,
) bool {
	origin := operation.Origin()
	wantOrigin := authority.origin
	if origin.ResolutionOrdinal() != wantOrigin.ResolutionOrdinal() ||
		origin.StageOrdinal() != wantOrigin.StageOrdinal() ||
		origin.ObjectID() != wantOrigin.ObjectID() ||
		origin.Version() != wantOrigin.Version() ||
		origin.ObjectType() != wantOrigin.ObjectType() ||
		origin.Name() != wantOrigin.Name() ||
		origin.AppID() != wantOrigin.AppID() ||
		origin.OwnerID() != wantOrigin.OwnerID() ||
		origin.SharingScope() != wantOrigin.SharingScope() ||
		origin.Stage() != wantOrigin.Stage() ||
		origin.DefinitionDigest() != wantOrigin.DefinitionDigest() ||
		origin.DefinitionLocation() != wantOrigin.DefinitionLocation() ||
		operation.Overwrite() != authority.overwrite ||
		operation.Input() != authority.input || operation.Pattern() != authority.pattern ||
		operation.ProgramWorkUnits() != authority.workUnits ||
		!bytes.Equal(operation.Selector().CanonicalBytes(), authority.selector.CanonicalBytes()) {
		return false
	}
	captures := operation.Captures()
	if len(captures) != len(authority.captures) {
		return false
	}
	for index, capture := range captures {
		want := authority.captures[index]
		if capture.Name() != want.name || capture.Group() != want.group ||
			capture.DefinitionLocation() != want.definitionLocation {
			return false
		}
	}
	return true
}

func validateKnowledgeRegexExtractionAuthority(
	authority knowledgeRegexExtractionAuthority,
) (splregex.ExtractionPattern, error) {
	origin := authority.origin
	if origin.ObjectType() != opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION ||
		origin.Stage() != opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION ||
		origin.Version() == 0 || origin.ObjectID() == "" || origin.Name() == "" ||
		origin.AppID() == "" || origin.OwnerID() == "" ||
		origin.DefinitionLocation() != "field_extraction.regex.pattern" {
		return splregex.ExtractionPattern{}, errors.New(
			"compile ClickHouse knowledge regex extraction: object provenance is invalid",
		)
	}
	switch origin.SharingScope() {
	case opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		opensplunk.SharingScope_SHARING_SCOPE_APP,
		opensplunk.SharingScope_SHARING_SCOPE_GLOBAL:
	default:
		return splregex.ExtractionPattern{}, errors.New(
			"compile ClickHouse knowledge regex extraction: sharing provenance is invalid",
		)
	}
	if authority.input != "_raw" {
		return splregex.ExtractionPattern{}, errors.New(
			"compile ClickHouse knowledge regex extraction: input is not canonical _raw",
		)
	}
	switch authority.overwrite {
	case knowledgeprogram.PreserveExisting, knowledgeprogram.ReplaceExisting:
	default:
		return splregex.ExtractionPattern{}, errors.New(
			"compile ClickHouse knowledge regex extraction: overwrite authority is invalid",
		)
	}
	if len(authority.captures) == 0 ||
		len(authority.captures) > splregex.MaximumExtractionCaptureGroups ||
		!strings.HasPrefix(authority.pattern, "(?-s)") {
		return splregex.ExtractionPattern{}, errors.New(
			"compile ClickHouse knowledge regex extraction: pattern authority is invalid",
		)
	}
	validated, err := splregex.CompileExtractionPattern(
		strings.TrimPrefix(authority.pattern, "(?-s)"),
	)
	if err != nil || validated.Pattern != authority.pattern ||
		validated.GroupCount != len(validated.Captures) ||
		len(validated.Captures) != len(authority.captures) ||
		uint64(validated.ProgramWorkUnits) != authority.workUnits { // #nosec G115 -- successful compilation bounds this by MaximumExtractionProgramWorkUnits.
		if err == nil {
			err = errors.New("retained regex inventory disagrees with a fresh compilation")
		}
		return splregex.ExtractionPattern{}, fmt.Errorf(
			"compile ClickHouse knowledge regex extraction: pattern validation: %w",
			err,
		)
	}
	seen := make(map[string]struct{}, len(authority.captures))
	for index, capture := range authority.captures {
		expected := validated.Captures[index]
		if capture.name != expected.Name || capture.group == 0 ||
			int(capture.group) != expected.Group ||
			capture.definitionLocation != fmt.Sprintf(
				"field_extraction.regex.output_fields[%d]",
				index,
			) {
			return splregex.ExtractionPattern{}, errors.New(
				"compile ClickHouse knowledge regex extraction: capture authority disagrees",
			)
		}
		if _, duplicate := seen[capture.name]; duplicate {
			return splregex.ExtractionPattern{}, errors.New(
				"compile ClickHouse knowledge regex extraction: capture output is repeated",
			)
		}
		seen[capture.name] = struct{}{}
		field, resolveErr := plan.ResolveField(capture.name, spl.Range{})
		if resolveErr != nil || field.Canonical || len(field.Path) == 0 ||
			eventfields.IsReservedDynamicRoot(field.Path[0]) {
			return splregex.ExtractionPattern{}, errors.New(
				"compile ClickHouse knowledge regex extraction: capture output is invalid",
			)
		}
	}
	return validated, nil
}
