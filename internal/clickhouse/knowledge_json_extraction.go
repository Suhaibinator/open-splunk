package clickhouse

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
)

const (
	knowledgeJSONProducedElement           = 1
	knowledgeJSONValueElement              = 2
	knowledgeJSONSelectorInputBytesElement = 3
	knowledgeJSONSelectorQueryUnitsElement = 4

	maxCompiledKnowledgeJSONExtractionSQLBytes = 64 << 10
)

// compiledKnowledgeJSONExtraction is one row-local knowledge extraction. SQL
// returns this closed tuple:
//
//  1. produced: UInt8 (separate from a produced JSON null),
//  2. value: Dynamic,
//  3. selector input bytes: UInt128, and
//  4. selector query units: UInt128.
//
// Destination merging is deliberately a later fused-stage operation. Keeping
// the validated output and overwrite policy beside the tuple prevents that
// stage from having to reinterpret the retained operation.
type compiledKnowledgeJSONExtraction struct {
	sql                 string
	args                []any
	operation           knowledgeprogram.JSONExtraction
	evaluationWorkUnits uint32
}

func (compiled compiledKnowledgeJSONExtraction) producedSQL(resultSQL string) string {
	return "toUInt8(tupleElement(" + resultSQL + ", " +
		strconv.Itoa(knowledgeJSONProducedElement) + "))"
}

func (compiled compiledKnowledgeJSONExtraction) valueSQL(resultSQL string) string {
	return "tupleElement(" + resultSQL + ", " +
		strconv.Itoa(knowledgeJSONValueElement) + ")"
}

func (compiled compiledKnowledgeJSONExtraction) selectorInputBytesSQL(resultSQL string) string {
	return "toUInt128(tupleElement(" + resultSQL + ", " +
		strconv.Itoa(knowledgeJSONSelectorInputBytesElement) + "))"
}

func (compiled compiledKnowledgeJSONExtraction) selectorQueryUnitsSQL(resultSQL string) string {
	return "toUInt128(tupleElement(" + resultSQL + ", " +
		strconv.Itoa(knowledgeJSONSelectorQueryUnitsElement) + "))"
}

type knowledgeJSONExtractionAuthority struct {
	origin         knowledgeprogram.Origin
	selector       knowledgeprogram.Selector
	overwrite      knowledgeprogram.OverwriteBehavior
	input          string
	path           string
	steps          []splpath.Step
	output         string
	outputLocation string
	workUnits      uint32
	operation      knowledgeprogram.JSONExtraction
}

// compileKnowledgeJSONExtraction lowers one immutable JSON extraction without
// opening a destination merge. Selector evaluation encloses every source and
// JSON operation, so an unselected row pays only the selector's fixed-order
// charge. Missing/null/unsupported sources and malformed, absent, null-parent,
// or container leaves produce no output. A selected explicit JSON null is a
// produced Dynamic null.
func compileKnowledgeJSONExtraction(
	extraction knowledgeprogram.JSONExtraction,
) (compiledKnowledgeJSONExtraction, error) {
	return compileKnowledgeJSONExtractionAuthority(
		knowledgeJSONExtractionAuthorityFromOperation(extraction),
	)
}

func knowledgeJSONExtractionAuthorityFromOperation(
	operation knowledgeprogram.JSONExtraction,
) knowledgeJSONExtractionAuthority {
	return knowledgeJSONExtractionAuthority{
		origin:         operation.Origin(),
		selector:       operation.Selector(),
		overwrite:      operation.Overwrite(),
		input:          operation.Input(),
		path:           operation.Path(),
		steps:          operation.Steps(),
		output:         operation.Output(),
		outputLocation: operation.OutputDefinitionLocation(),
		workUnits:      operation.EvaluationWorkUnits(),
		operation:      operation,
	}
}

func compileKnowledgeJSONExtractionAuthority(
	authority knowledgeJSONExtractionAuthority,
) (compiledKnowledgeJSONExtraction, error) {
	if !knowledgeJSONExtractionOperationMatchesAuthority(authority.operation, authority) {
		return compiledKnowledgeJSONExtraction{}, errors.New(
			"compile ClickHouse knowledge JSON extraction: retained operation disagrees with authority",
		)
	}
	steps, err := validateKnowledgeJSONExtractionAuthority(authority)
	if err != nil {
		return compiledKnowledgeJSONExtraction{}, err
	}
	selector, err := compileKnowledgeSelector(authority.selector)
	if err != nil {
		return compiledKnowledgeJSONExtraction{}, fmt.Errorf(
			"compile ClickHouse knowledge JSON extraction selector: %w",
			err,
		)
	}

	candidateSQL, candidateArgs := knowledgeJSONScalarCandidateSQL(steps)
	const (
		selectorVariable  = "__os_ko_json_selector"
		candidateVariable = "__os_ko_json_candidate"
	)
	selectedCandidate := "if(tupleElement(" + selectorVariable + ", 1) != 0, " +
		candidateSQL + ", " + knowledgeJSONNoCandidateTuple() + ")"
	result := "tuple(" +
		"toUInt8(tupleElement(" + candidateVariable + ", 1)), " +
		"tupleElement(" + candidateVariable + ", 2), " +
		"toUInt128(tupleElement(" + selectorVariable + ", 2)), " +
		"toUInt128(tupleElement(" + selectorVariable + ", 3)))"
	result = bindSQLExpressions(
		[]string{candidateVariable},
		[]string{selectedCandidate},
		result,
	)
	result = bindSQLExpressions(
		[]string{selectorVariable},
		[]string{selector.sql},
		result,
	)
	if len(result) > maxCompiledKnowledgeJSONExtractionSQLBytes {
		return compiledKnowledgeJSONExtraction{}, errors.New(
			"compile ClickHouse knowledge JSON extraction: generated SQL exceeds the per-object limit",
		)
	}

	// Lambda bodies precede their bound values textually. The complete JSON
	// candidate therefore contributes its placeholders before the selector.
	args := make([]any, 0, len(candidateArgs)+len(selector.args))
	args = append(args, candidateArgs...)
	args = append(args, selector.args...)
	return compiledKnowledgeJSONExtraction{
		sql:                 result,
		args:                args,
		operation:           authority.operation,
		evaluationWorkUnits: authority.workUnits,
	}, nil
}

func knowledgeJSONExtractionOperationMatchesAuthority(
	operation knowledgeprogram.JSONExtraction,
	authority knowledgeJSONExtractionAuthority,
) bool {
	origin := operation.Origin()
	wantOrigin := authority.origin
	return origin.ResolutionOrdinal() == wantOrigin.ResolutionOrdinal() &&
		origin.StageOrdinal() == wantOrigin.StageOrdinal() &&
		origin.ObjectID() == wantOrigin.ObjectID() &&
		origin.Version() == wantOrigin.Version() &&
		origin.ObjectType() == wantOrigin.ObjectType() &&
		origin.Name() == wantOrigin.Name() &&
		origin.AppID() == wantOrigin.AppID() &&
		origin.OwnerID() == wantOrigin.OwnerID() &&
		origin.SharingScope() == wantOrigin.SharingScope() &&
		origin.Stage() == wantOrigin.Stage() &&
		origin.DefinitionDigest() == wantOrigin.DefinitionDigest() &&
		origin.DefinitionLocation() == wantOrigin.DefinitionLocation() &&
		bytes.Equal(operation.Selector().CanonicalBytes(), authority.selector.CanonicalBytes()) &&
		operation.Overwrite() == authority.overwrite &&
		operation.Input() == authority.input &&
		operation.Path() == authority.path &&
		slices.Equal(operation.Steps(), authority.steps) &&
		operation.Output() == authority.output &&
		operation.OutputDefinitionLocation() == authority.outputLocation &&
		operation.EvaluationWorkUnits() == authority.workUnits
}

func validateKnowledgeJSONExtractionAuthority(
	authority knowledgeJSONExtractionAuthority,
) ([]splpath.Step, error) {
	origin := authority.origin
	if origin.ObjectType() != opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION ||
		origin.Stage() != opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION ||
		origin.Version() == 0 || origin.ObjectID() == "" || origin.Name() == "" ||
		origin.AppID() == "" || origin.OwnerID() == "" ||
		origin.DefinitionLocation() != "field_extraction.json.path" {
		return nil, errors.New(
			"compile ClickHouse knowledge JSON extraction: object provenance is invalid",
		)
	}
	switch origin.SharingScope() {
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
	default:
		return nil, errors.New(
			"compile ClickHouse knowledge JSON extraction: sharing provenance is invalid",
		)
	}
	if authority.input != "_raw" {
		return nil, errors.New(
			"compile ClickHouse knowledge JSON extraction: input is not canonical _raw",
		)
	}
	switch authority.overwrite {
	case knowledgeprogram.PreserveExisting, knowledgeprogram.ReplaceExisting:
	default:
		return nil, errors.New(
			"compile ClickHouse knowledge JSON extraction: overwrite authority is invalid",
		)
	}
	if authority.path == "" || authority.output == "" ||
		authority.outputLocation != "field_extraction.json.output_field" {
		return nil, errors.New(
			"compile ClickHouse knowledge JSON extraction: path or output authority is invalid",
		)
	}
	output, err := plan.ResolveField(authority.output, spl.Range{})
	if err != nil || output.Canonical || len(output.Path) == 0 ||
		eventfields.IsReservedDynamicRoot(output.Path[0]) {
		return nil, errors.New(
			"compile ClickHouse knowledge JSON extraction: output field is invalid",
		)
	}
	steps, err := splpath.ParseJSON(authority.path)
	if err != nil {
		return nil, fmt.Errorf(
			"compile ClickHouse knowledge JSON extraction: path validation: %w",
			err,
		)
	}
	workUnits := uint32(splpath.EvaluationWorkUnits(steps))
	if !slices.Equal(steps, authority.steps) || workUnits != authority.workUnits {
		return nil, errors.New(
			"compile ClickHouse knowledge JSON extraction: path authority is inconsistent",
		)
	}
	return steps, nil
}

// knowledgeJSONScalarCandidateSQL returns (produced UInt8, value Dynamic).
// It uses the authored spath scalar decoder so integer, floating, and exact
// decimal spellings retain the same typed meaning. Unlike authored spath, a
// selected object or array is a no-output result rather than an error.
func knowledgeJSONScalarCandidateSQL(
	steps []splpath.Step,
) (string, []any) {
	const (
		inputVariable          = "__os_ko_json_input"
		preflightVariable      = "__os_ko_json_needs_preflight"
		tokenCountVariable     = "__os_ko_json_token_count"
		tokensVariable         = "__os_ko_json_tokens"
		numberFlagsVariable    = "__os_ko_json_number_flags"
		nulledJSONVariable     = "__os_ko_json_nulled"
		pathEligibleVariable   = "__os_ko_json_path_eligible"
		nullRawVariable        = "__os_ko_json_null_raw"
		numberMarkerVariable   = "__os_ko_json_number_marker"
		numberSelectedVariable = "__os_ko_json_number_selected"
		rawVariable            = "__os_ko_json_raw"
		producedVariable       = "__os_ko_json_produced"
	)

	pathSQL, pathArgs := spathPathSQL(steps)
	arrayGuardSQL, arrayGuardArgs := spathArrayGuardSQL(nulledJSONVariable, steps)

	supportedScalar := numberSelectedVariable + " != 0 OR " + rawVariable +
		" IN ('null', 'true', 'false') OR startsWith(" + rawVariable + ", char(34))"
	producedSQL := "toUInt8(notEmpty(" + rawVariable + ") AND (" + supportedScalar + "))"
	valueSQL := "if(" + producedVariable + " != 0, if(" + numberSelectedVariable +
		" != 0, " + spathJSONNumberDynamicSQL(rawVariable) + ", JSONExtract(" +
		rawVariable + ", 'Dynamic')), CAST(NULL AS Dynamic))"
	result := "tuple(toUInt8(" + producedVariable + "), " + valueSQL + ")"
	result = bindSQLExpressions(
		[]string{producedVariable},
		[]string{producedSQL},
		result,
	)

	markerIndex := "toUInt64OrZero(substring(" + numberMarkerVariable + ", " +
		strconv.Itoa(len(spathJSONNumberMarkerPrefix)+1) + ", length(" + numberMarkerVariable +
		") - " + strconv.Itoa(len(spathJSONNumberMarkerPrefix)+len(spathJSONNumberMarkerSuffix)) + "))"
	rawSQL := "if(" + numberSelectedVariable + " != 0, arrayElement(" + tokensVariable +
		", " + markerIndex + "), " + nullRawVariable + ")"
	result = bindSQLExpressions([]string{rawVariable}, []string{rawSQL}, result)

	numberSelectedSQL := "toUInt8(startsWith(" + numberMarkerVariable + ", '" +
		spathJSONNumberMarkerPrefix + "') AND endsWith(" + numberMarkerVariable + ", '" +
		spathJSONNumberMarkerSuffix + "'))"
	result = bindSQLExpressions(
		[]string{numberSelectedVariable},
		[]string{numberSelectedSQL},
		result,
	)

	markedJSONSQL := "arrayStringConcat(arrayMap((token, flag, token_index) -> if(flag != 0, " +
		"concat(char(34), '" + spathJSONNumberMarkerPrefix + "', toString(token_index), '" +
		spathJSONNumberMarkerSuffix + "', char(34)), token), " + tokensVariable + ", " +
		numberFlagsVariable + ", arrayEnumerate(" + tokensVariable + ")))"
	numberMarkerSQL := "if(" + pathEligibleVariable + " != 0 AND " + nullRawVariable +
		" = 'null' AND has(" + numberFlagsVariable + ", toUInt8(1)), JSONExtractString(" +
		markedJSONSQL + ", " + pathSQL + "), CAST('' AS String))"
	result = bindSQLExpressions(
		[]string{numberMarkerVariable},
		[]string{numberMarkerSQL},
		result,
	)

	nullRawSQL := "if(" + pathEligibleVariable + " != 0, JSONExtractRaw(" +
		nulledJSONVariable + ", " + pathSQL + "), CAST('' AS String))"
	result = bindSQLExpressions([]string{nullRawVariable}, []string{nullRawSQL}, result)

	pathEligibleSQL := "toUInt8(1)"
	if arrayGuardSQL != "" {
		pathEligibleSQL = "toUInt8(" + arrayGuardSQL + ")"
	}
	result = bindSQLExpressions(
		[]string{pathEligibleVariable},
		[]string{pathEligibleSQL},
		result,
	)

	nulledJSONSQL := "if(has(" + numberFlagsVariable + ", toUInt8(1)), " +
		"arrayStringConcat(arrayMap((token, flag) -> if(flag != 0, CAST('null' AS String), token), " +
		tokensVariable + ", " + numberFlagsVariable + ")), " + inputVariable + ")"
	result = bindSQLExpressions(
		[]string{nulledJSONVariable},
		[]string{nulledJSONSQL},
		result,
	)

	numberFlagsSQL := "arrayMap(token -> toUInt8(match(token, ?)), " + tokensVariable + ")"
	result = bindSQLExpressions(
		[]string{numberFlagsVariable},
		[]string{numberFlagsSQL},
		result,
	)

	tokensSQL := "extractAll(" + inputVariable + ", ?)"
	result = bindSQLExpressions([]string{tokensVariable}, []string{tokensSQL}, result)

	overTokenLimit := tokenCountVariable + " > " + strconv.Itoa(MaximumSpathJSONTokens)
	result = "if(" + overTokenLimit + ", " +
		knowledgeJSONThrowCandidateTuple(overTokenLimit, SpathJSONTokenLimitMarker) +
		", " + result + ")"
	preflightInput := "if(" + preflightVariable + " != 0, " + inputVariable +
		", CAST('' AS String))"
	tokenCountSQL := "if(" + preflightVariable + " != 0, countMatches(" +
		preflightInput + ", ?), toUInt64(0))"
	result = bindSQLExpressions(
		[]string{tokenCountVariable},
		[]string{tokenCountSQL},
		result,
	)
	preflightSQL := "toUInt8(length(" + inputVariable + ") > " +
		strconv.Itoa(MaximumSpathJSONTokens) + ")"
	result = bindSQLExpressions(
		[]string{preflightVariable},
		[]string{preflightSQL},
		result,
	)

	overInputLimit := "length(" + inputVariable + ") > " + strconv.Itoa(MaximumSpathInputBytes)
	result = "if(" + overInputLimit + ", " +
		knowledgeJSONThrowCandidateTuple(overInputLimit, SpathInputLimitMarker) +
		", " + result + ")"
	inputSQL := quoteIdentifier("_raw")
	inputEligibleSQL := "isNotNull(" + inputSQL + ") AND " +
		quoteIdentifier(internalRawEncodingColumn) + " = " + strconv.Itoa(rawEncodingUTF8) +
		" AND isValidUTF8(" + inputSQL + ")"
	result = "if(" + inputEligibleSQL + ", " + result +
		", " + knowledgeJSONNoCandidateTuple() + ")"
	result = bindSQLExpressions(
		[]string{inputVariable},
		[]string{"assumeNotNull(" + inputSQL + ")"},
		result,
	)

	// Binding dependencies outside their consumers fixes textual placeholder
	// order independently of runtime short-circuit order.
	args := make([]any, 0, 2*len(pathArgs)+len(arrayGuardArgs)+3)
	args = append(args, pathArgs...)
	args = append(args, pathArgs...)
	args = append(args, arrayGuardArgs...)
	args = append(args, spathJSONNumberPattern)
	args = append(args, spathJSONTokenPattern)
	args = append(args, spathJSONTokenPattern)
	return result, args
}

func knowledgeJSONNoCandidateTuple() string {
	return "tuple(toUInt8(0), CAST(NULL AS Dynamic))"
}

func knowledgeJSONThrowCandidateTuple(condition, marker string) string {
	return "tuple(toUInt8(throwIf(toUInt8(" + condition + "), '" + marker +
		"') = 0), CAST(NULL AS Dynamic))"
}
