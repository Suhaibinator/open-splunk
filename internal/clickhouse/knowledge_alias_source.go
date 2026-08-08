package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

const (
	maxCompiledKnowledgeAliasSourceSQLBytes = 64 << 10

	knowledgeAliasSourceProducedElement   = 1
	knowledgeAliasSourceValueElement      = 2
	knowledgeAliasSourceStoredTypeElement = 3
	knowledgeAliasSourceNamesElement      = 4
	knowledgeAliasSourceTypesElement      = 5
	knowledgeAliasSourceMetadataElement   = 6
)

// storedPathAuthority is compiler-minted proof that one Dynamic field is an
// exact path in the immutable stored event document. Logical and physical
// segments are retained separately: normalized metadata uses the former,
// while ClickHouse JSON traversal uses the latter one segment at a time.
// Every slice is detached when the authority crosses a compiler boundary.
type storedPathAuthority struct {
	logicalSegments            []string
	normalizedExactPath        string
	normalizedDescendantPrefix string
	physicalSegments           []string
}

// mintStoredPathAuthority is called only by resolveCompiledField, after the
// planner has classified the field as a noncanonical stored Dynamic path.
func mintStoredPathAuthority(logicalSegments []string) (storedPathAuthority, error) {
	if len(logicalSegments) == 0 ||
		len(logicalSegments) > eventfields.MaximumDynamicPathSegments+1 {
		return storedPathAuthority{}, errors.New(
			"stored Dynamic path authority has invalid segment count",
		)
	}
	normalizedBytes := 0
	for _, segment := range logicalSegments {
		if len(segment) == 0 ||
			len(segment) > eventfields.MaximumDynamicPathSegmentBytes {
			return storedPathAuthority{}, errors.New(
				"stored Dynamic path authority has invalid segment length",
			)
		}
		normalizedBytes = eventfields.NormalizedDynamicPathBytes(
			normalizedBytes,
			segment,
		)
		if normalizedBytes > eventfields.MaximumNormalizedFieldNameBytes {
			return storedPathAuthority{}, errors.New(
				"stored Dynamic path authority exceeds normalized path limit",
			)
		}
	}
	authority := storedPathAuthority{
		logicalSegments: slices.Clone(logicalSegments),
	}
	authority.normalizedExactPath = eventfields.NormalizeDynamicPath(
		authority.logicalSegments,
	)
	authority.normalizedDescendantPrefix = authority.normalizedExactPath + "."
	authority.physicalSegments = make([]string, len(authority.logicalSegments))
	for index, segment := range authority.logicalSegments {
		authority.physicalSegments[index] = eventfields.EncodePhysicalPathSegment(segment)
	}
	if err := validateStoredPathAuthority(authority); err != nil {
		return storedPathAuthority{}, err
	}
	return authority, nil
}

func validateStoredPathAuthority(authority storedPathAuthority) error {
	if len(authority.logicalSegments) == 0 ||
		len(authority.logicalSegments) != len(authority.physicalSegments) ||
		authority.normalizedExactPath == "" ||
		authority.normalizedDescendantPrefix != authority.normalizedExactPath+"." {
		return errors.New("stored Dynamic path authority is incomplete")
	}
	parsed, err := eventfields.ParseNormalizedSearchFieldPath(
		authority.normalizedExactPath,
	)
	if err != nil || !slices.Equal(parsed, authority.logicalSegments) ||
		eventfields.NormalizeDynamicPath(authority.logicalSegments) !=
			authority.normalizedExactPath {
		return errors.New("stored Dynamic path authority has invalid logical metadata")
	}
	for index, segment := range authority.logicalSegments {
		if segment == "" || authority.physicalSegments[index] == "" ||
			authority.physicalSegments[index] !=
				eventfields.EncodePhysicalPathSegment(segment) {
			return errors.New("stored Dynamic path authority has invalid physical metadata")
		}
	}
	return nil
}

func (authority storedPathAuthority) clone() storedPathAuthority {
	return storedPathAuthority{
		logicalSegments:            cloneStrings(authority.logicalSegments),
		normalizedExactPath:        strings.Clone(authority.normalizedExactPath),
		normalizedDescendantPrefix: strings.Clone(authority.normalizedDescendantPrefix),
		physicalSegments:           cloneStrings(authority.physicalSegments),
	}
}

func (authority storedPathAuthority) equal(other storedPathAuthority) bool {
	return authority.normalizedExactPath == other.normalizedExactPath &&
		authority.normalizedDescendantPrefix == other.normalizedDescendantPrefix &&
		slices.Equal(authority.logicalSegments, other.logicalSegments) &&
		slices.Equal(authority.physicalSegments, other.physicalSegments)
}

func (authority storedPathAuthority) valueSQL() string {
	value := quoteIdentifier(internalFieldsColumn)
	for _, segment := range authority.physicalSegments {
		value += "." + quoteIdentifier(segment)
	}
	return value
}

type compiledKnowledgeAliasSourceProof struct {
	storedPath      storedPathAuthority
	sourceValue     string
	exactPresence   string
	descendants     string
	metadataVersion string
}

// compiledKnowledgeAliasSource is an unreachable, relation-neutral source
// descriptor for a future lossless alias merge. Its SQL returns one tuple:
// produced, Dynamic value, semantic stored type, relative descendant names,
// aligned descendant types, and the source metadata version. Exact stored
// leaves deliberately carry empty sidecars; only a flattened nonempty object
// parent needs them.
type compiledKnowledgeAliasSource struct {
	sql   string
	args  []any
	proof compiledKnowledgeAliasSourceProof
}

func (compiled compiledKnowledgeAliasSource) producedSQL(resultSQL string) string {
	return knowledgeTupleElementUInt8(resultSQL, knowledgeAliasSourceProducedElement)
}

func (compiled compiledKnowledgeAliasSource) valueSQL(resultSQL string) string {
	return knowledgeTupleElement(resultSQL, knowledgeAliasSourceValueElement)
}

func (compiled compiledKnowledgeAliasSource) storedTypeSQL(resultSQL string) string {
	return knowledgeTupleElementUInt8(resultSQL, knowledgeAliasSourceStoredTypeElement)
}

func (compiled compiledKnowledgeAliasSource) namesSQL(resultSQL string) string {
	return knowledgeTupleElement(resultSQL, knowledgeAliasSourceNamesElement)
}

func (compiled compiledKnowledgeAliasSource) typesSQL(resultSQL string) string {
	return knowledgeTupleElement(resultSQL, knowledgeAliasSourceTypesElement)
}

func (compiled compiledKnowledgeAliasSource) metadataVersionSQL(resultSQL string) string {
	return knowledgeTupleElementUInt8(resultSQL, knowledgeAliasSourceMetadataElement)
}

// compileKnowledgeAliasSource lowers only a direct stored Dynamic source. It
// is intentionally not called by compileKnowledgeAliasAssignment: the
// nonempty execution gate stays closed until the result-side sidecar decoder
// and runtime copy budget have their pinned ClickHouse evidence.
func compileKnowledgeAliasSource(field fieldState) (compiledKnowledgeAliasSource, error) {
	if err := validateKnowledgeAliasSourceField(field); err != nil {
		return compiledKnowledgeAliasSource{}, err
	}
	sql, args := buildKnowledgeAliasSourceSQL(field.storedPath)
	compiled := compiledKnowledgeAliasSource{
		sql:  sql,
		args: cloneKnowledgeAliasSourceArgs(args),
		proof: compiledKnowledgeAliasSourceProof{
			storedPath:      field.storedPath.clone(),
			sourceValue:     strings.Clone(field.valueSQL),
			exactPresence:   strings.Clone(field.existsSQL),
			descendants:     strings.Clone(field.descendantSQL),
			metadataVersion: strings.Clone(knowledgeAliasSourceMetadataVersionSQL()),
		},
	}
	if err := validateCompiledKnowledgeAliasSource(compiled); err != nil {
		return compiledKnowledgeAliasSource{}, err
	}
	return compiled, nil
}

func validateKnowledgeAliasSourceField(field fieldState) error {
	if err := validateStoredPathAuthority(field.storedPath); err != nil {
		return fmt.Errorf("compile ClickHouse knowledge alias source: %w", err)
	}
	authority := field.storedPath
	expectedExact := knowledgeAliasSourceExactPresenceSQL()
	expectedDescendants := knowledgeAliasSourceDescendantPresenceSQL()
	if field.kind != fieldKindDynamic || field.valueSQL != authority.valueSQL() ||
		field.dynamicTypeSQL != "dynamicType("+authority.valueSQL()+")" ||
		field.storedTypeSQL != "" || field.existsSQL != expectedExact ||
		field.descendantSQL != expectedDescendants ||
		!knowledgeAliasSourceSingleStringArgument(
			field.existsArgs,
			authority.normalizedExactPath,
		) || !knowledgeAliasSourceSingleStringArgument(
		field.descendantArgs,
		authority.normalizedDescendantPrefix,
	) {
		return errors.New(
			"compile ClickHouse knowledge alias source: field is not a direct stored Dynamic path",
		)
	}
	return nil
}

func validateCompiledKnowledgeAliasSource(compiled compiledKnowledgeAliasSource) error {
	if err := validateStoredPathAuthority(compiled.proof.storedPath); err != nil {
		return fmt.Errorf("validate compiled ClickHouse knowledge alias source: %w", err)
	}
	authority := compiled.proof.storedPath
	if compiled.proof.sourceValue != authority.valueSQL() ||
		compiled.proof.exactPresence != knowledgeAliasSourceExactPresenceSQL() ||
		compiled.proof.descendants != knowledgeAliasSourceDescendantPresenceSQL() ||
		compiled.proof.metadataVersion != knowledgeAliasSourceMetadataVersionSQL() {
		return errors.New(
			"validate compiled ClickHouse knowledge alias source: retained proof is invalid",
		)
	}
	expectedSQL, expectedArgs := buildKnowledgeAliasSourceSQL(authority)
	if compiled.sql != expectedSQL || len(compiled.sql) == 0 ||
		len(compiled.sql) > maxCompiledKnowledgeAliasSourceSQLBytes ||
		strings.Count(compiled.sql, "?") != len(compiled.args) ||
		!knowledgeAliasSourceArgumentsEqual(compiled.args, expectedArgs) {
		return errors.New(
			"validate compiled ClickHouse knowledge alias source: SQL or arguments disagree with authority",
		)
	}
	return nil
}

func buildKnowledgeAliasSourceSQL(
	authority storedPathAuthority,
) (string, []any) {
	q := quoteIdentifier
	fields := q(internalFieldsColumn)
	names := q(internalFieldNamesColumn)
	types := q(internalFieldTypesColumn)
	metadataVersion := knowledgeAliasSourceMetadataVersionSQL()
	alignedMetadata := "length(" + types + ") = length(" + names + ")"
	emptyNames := "CAST([], 'Array(String)')"
	emptyTypes := "CAST([], 'Array(UInt8)')"

	var materialized strings.Builder
	materialized.WriteString("JSONExtract(")
	materialized.WriteString(fields)
	for range authority.physicalSegments {
		materialized.WriteString(", CAST(? AS String)")
	}
	materialized.WriteString(", 'Dynamic')")

	relativeNames := "arrayMap(field_name -> substring(field_name, " +
		"length(CAST(? AS String)) + 1), " +
		"arrayFilter(field_name -> startsWith(field_name, CAST(? AS String)), " +
		names + "))"
	relativeTypes := "if(" + alignedMetadata +
		", arrayMap(field_index -> toUInt8(arrayElement(" + types +
		", field_index)), arrayFilter(field_index -> startsWith(arrayElement(" +
		names + ", field_index), CAST(? AS String)), arrayEnumerate(" + names +
		"))), " + emptyTypes + ")"

	exact := "has(" + names + ", CAST(? AS String))"
	exactType := "if(" + alignedMetadata +
		", toUInt8(arrayElement(" + types + ", indexOf(" + names +
		", CAST(? AS String)))), " + knowledgeDynamicStoredTypeSQL(
		authority.valueSQL(),
	) + ")"
	descendant := "arrayExists(field_name -> startsWith(field_name, CAST(? AS String)), " +
		names + ")"
	exactTuple := "tuple(toUInt8(1), CAST(" + authority.valueSQL() +
		" AS Dynamic), " + exactType + ", " + emptyNames + ", " + emptyTypes +
		", " + metadataVersion + ")"
	descendantTuple := "tuple(toUInt8(1), " + materialized.String() +
		", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeObject)) + "), " +
		relativeNames + ", " + relativeTypes + ", " + metadataVersion + ")"
	missingTuple := "tuple(toUInt8(0), CAST(NULL AS Dynamic), toUInt8(0), " +
		emptyNames + ", " + emptyTypes + ", " + metadataVersion + ")"
	sql := "multiIf(" + exact + ", " + exactTuple + ", " + descendant +
		", " + descendantTuple + ", " + missingTuple + ")"

	args := make([]any, 0, len(authority.physicalSegments)+6)
	args = append(args,
		authority.normalizedExactPath,
		authority.normalizedExactPath,
		authority.normalizedDescendantPrefix,
	)
	for _, segment := range authority.physicalSegments {
		args = append(args, segment)
	}
	args = append(args,
		authority.normalizedDescendantPrefix,
		authority.normalizedDescendantPrefix,
		authority.normalizedDescendantPrefix,
	)
	return sql, args
}

func knowledgeAliasSourceExactPresenceSQL() string {
	return "has(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)"
}

func knowledgeAliasSourceDescendantPresenceSQL() string {
	return "arrayExists(name -> startsWith(name, ?), " +
		quoteIdentifier(internalFieldNamesColumn) + ")"
}

func knowledgeAliasSourceMetadataVersionSQL() string {
	return "toUInt8(" + quoteIdentifier(internalFieldMetadataVersionColumn) + ")"
}

func knowledgeAliasSourceSingleStringArgument(arguments []any, expected string) bool {
	if len(arguments) != 1 {
		return false
	}
	value, ok := arguments[0].(string)
	return ok && value == expected
}

func cloneKnowledgeAliasSourceArgs(arguments []any) []any {
	if arguments == nil {
		return nil
	}
	cloned := make([]any, len(arguments))
	for index, argument := range arguments {
		value, _ := argument.(string)
		cloned[index] = strings.Clone(value)
	}
	return cloned
}

func knowledgeAliasSourceArgumentsEqual(left, right []any) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		leftValue, leftOK := left[index].(string)
		rightValue, rightOK := right[index].(string)
		if !leftOK || !rightOK || leftValue != rightValue {
			return false
		}
	}
	return true
}
