package clickhouse

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

const (
	maxCompiledKnowledgeAliasSourceSQLBytes = 64 << 10
	compiledKnowledgeFieldSourceSealDomain  = "open-splunk/clickhouse/knowledge-field-source/v2"

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

func (authority storedPathAuthority) isZero() bool {
	return len(authority.logicalSegments) == 0 &&
		authority.normalizedExactPath == "" &&
		authority.normalizedDescendantPrefix == "" &&
		len(authority.physicalSegments) == 0
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

// compiledKnowledgeFieldSource is the canonical frozen representation used by
// every field assignment and prior-destination binding. The tuple layout is
// shared with compiledKnowledgeAliasSource so a direct stored path can retain
// that descriptor's stronger path authority without being lowered twice.
type compiledKnowledgeFieldSource struct {
	sql                 string
	args                []any
	presenceSQL         string
	presenceArgs        []any
	authority           string
	inputStateAuthority [sha256.Size]byte
	maxStringBytes      uint64
	seal                [sha256.Size]byte
}

func authorizeCompiledKnowledgeFieldSource(
	compiled compiledKnowledgeFieldSource,
	authority string,
	inputStateAuthority [sha256.Size]byte,
	maxStringBytes uint64,
) (compiledKnowledgeFieldSource, error) {
	if authority == "" || inputStateAuthority == ([sha256.Size]byte{}) ||
		compiled.authority != "" || compiled.seal != ([sha256.Size]byte{}) {
		return compiledKnowledgeFieldSource{}, errors.New(
			"compile ClickHouse knowledge field source: authority is invalid",
		)
	}
	compiled.authority = authority
	compiled.inputStateAuthority = inputStateAuthority
	compiled.maxStringBytes = maxStringBytes
	seal, ok := compiledKnowledgeFieldSourceDigest(compiled)
	if !ok {
		return compiledKnowledgeFieldSource{}, errors.New(
			"compile ClickHouse knowledge field source: authority cannot be sealed",
		)
	}
	compiled.seal = seal
	return compiled, nil
}

func validCompiledKnowledgeFieldSourceAuthority(
	compiled compiledKnowledgeFieldSource,
	authority string,
	inputStateAuthority [sha256.Size]byte,
) bool {
	if authority == "" || compiled.authority != authority ||
		compiled.seal == ([sha256.Size]byte{}) ||
		subtle.ConstantTimeCompare(
			compiled.inputStateAuthority[:],
			inputStateAuthority[:],
		) != 1 {
		return false
	}
	expected, ok := compiledKnowledgeFieldSourceDigest(compiled)
	return ok && subtle.ConstantTimeCompare(compiled.seal[:], expected[:]) == 1
}

func compiledKnowledgeFieldSourceDigest(
	compiled compiledKnowledgeFieldSource,
) ([sha256.Size]byte, bool) {
	digest := sha256.New()
	writeTokenPart(digest, compiledKnowledgeFieldSourceSealDomain)
	writeTokenPart(digest, compiled.sql)
	writeTokenPart(digest, compiled.presenceSQL)
	writeTokenPart(digest, compiled.authority)
	_, _ = digest.Write(compiled.inputStateAuthority[:])
	writeUint64(digest, compiled.maxStringBytes)
	writeUint64(digest, uint64(len(compiled.args)))
	for _, argument := range compiled.args {
		if !writeCompiledArgument(digest, argument, 0) {
			return [sha256.Size]byte{}, false
		}
	}
	writeUint64(digest, uint64(len(compiled.presenceArgs)))
	for _, argument := range compiled.presenceArgs {
		if !writeCompiledArgument(digest, argument, 0) {
			return [sha256.Size]byte{}, false
		}
	}
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result, true
}

func (compiled compiledKnowledgeFieldSource) producedSQL(resultSQL string) string {
	return knowledgeTupleElementUInt8(resultSQL, knowledgeAliasSourceProducedElement)
}

func (compiled compiledKnowledgeFieldSource) valueSQL(resultSQL string) string {
	return knowledgeTupleElement(resultSQL, knowledgeAliasSourceValueElement)
}

func (compiled compiledKnowledgeFieldSource) storedTypeSQL(resultSQL string) string {
	return knowledgeTupleElementUInt8(resultSQL, knowledgeAliasSourceStoredTypeElement)
}

func (compiled compiledKnowledgeFieldSource) namesSQL(resultSQL string) string {
	return knowledgeTupleElement(resultSQL, knowledgeAliasSourceNamesElement)
}

func (compiled compiledKnowledgeFieldSource) typesSQL(resultSQL string) string {
	return knowledgeTupleElement(resultSQL, knowledgeAliasSourceTypesElement)
}

func (compiled compiledKnowledgeFieldSource) metadataVersionSQL(resultSQL string) string {
	return knowledgeTupleElementUInt8(resultSQL, knowledgeAliasSourceMetadataElement)
}

func compileKnowledgeFieldSourceFromField(
	field fieldState,
	present bool,
) (compiledKnowledgeFieldSource, error) {
	if !present {
		return newCompiledKnowledgeFieldSource(
			knowledgeMissingFieldSourceSQL(),
			nil,
			"0",
			nil,
		)
	}
	return compileKnowledgeFieldSourceFromScalar(compiledScalarFromField(field), true)
}

func compileKnowledgeFieldSourceFromScalar(
	value compiledScalar,
	retainDirectSidecars bool,
) (compiledKnowledgeFieldSource, error) {
	if !retainDirectSidecars {
		value.storedPath = storedPathAuthority{}
		value.relativeFieldNamesSQL = ""
		value.relativeFieldTypesSQL = ""
		value.fieldMetadataVersionSQL = ""
	}
	if err := validateKnowledgeFieldSidecars(
		value.relativeFieldNamesSQL,
		value.relativeFieldTypesSQL,
		value.fieldMetadataVersionSQL,
	); err != nil {
		return compiledKnowledgeFieldSource{}, err
	}
	if !value.storedPath.isZero() {
		if value.relativeFieldNamesSQL != "" {
			return compiledKnowledgeFieldSource{}, errors.New(
				"compile ClickHouse knowledge field source: stored path overlaps materialized sidecars",
			)
		}
		if len(value.valueArgs) != 0 {
			return compiledKnowledgeFieldSource{}, errors.New(
				"compile ClickHouse knowledge field source: stored path has value arguments",
			)
		}
		field := knowledgeFieldStateFromScalar(value)
		direct, err := compileKnowledgeAliasSource(field)
		if err != nil {
			return compiledKnowledgeFieldSource{}, err
		}
		presenceSQL, presenceArgs := knownFieldPresenceSQL(field)
		return newCompiledKnowledgeFieldSource(
			direct.sql,
			direct.args,
			presenceSQL,
			presenceArgs,
		)
	}

	const (
		valueVariable   = "__os_ko_source_value"
		presentVariable = "__os_ko_source_present"
		typeVariable    = "__os_ko_source_type"
	)
	presenceSQL, presenceArgs := knowledgeScalarPresenceSQL(value)
	typeSQL, typeArgs, err := knowledgeScalarStoredTypeSQL(value, valueVariable)
	if err != nil {
		return compiledKnowledgeFieldSource{}, err
	}
	namesSQL := knowledgeEmptyRelativeFieldNamesSQL()
	typesSQL := knowledgeEmptyRelativeFieldTypesSQL()
	metadataVersionSQL := "toUInt8(0)"
	if value.relativeFieldNamesSQL != "" {
		namesSQL = value.relativeFieldNamesSQL
		typesSQL = value.relativeFieldTypesSQL
		metadataVersionSQL = "toUInt8(" + value.fieldMetadataVersionSQL + ")"
	}
	source := "tuple(" +
		"toUInt8(ifNull(" + presentVariable + ", 0)), " +
		"if(ifNull(" + presentVariable + ", 0), " + valueVariable +
		", CAST(NULL AS Dynamic)), " +
		"toUInt8(if(ifNull(" + presentVariable + ", 0), " + typeVariable + ", 0)), " +
		"if(ifNull(" + presentVariable + ", 0), " + namesSQL + ", " +
		knowledgeEmptyRelativeFieldNamesSQL() + "), " +
		"if(ifNull(" + presentVariable + ", 0), " + typesSQL + ", " +
		knowledgeEmptyRelativeFieldTypesSQL() + "), " +
		"toUInt8(if(ifNull(" + presentVariable + ", 0), " +
		metadataVersionSQL + ", 0)))"
	source = bindSQLExpressions(
		[]string{presentVariable, typeVariable},
		[]string{presenceSQL, typeSQL},
		source,
	)
	source = bindSQLExpressions(
		[]string{valueVariable},
		[]string{"CAST(" + value.valueSQL + " AS Dynamic)"},
		source,
	)
	args := make([]any, 0,
		len(presenceArgs)+len(typeArgs)+len(value.valueArgs),
	)
	args = append(args, presenceArgs...)
	args = append(args, typeArgs...)
	args = append(args, value.valueArgs...)
	return newCompiledKnowledgeFieldSource(source, args, presenceSQL, presenceArgs)
}

func newCompiledKnowledgeFieldSource(
	sql string,
	args []any,
	presenceSQL string,
	presenceArgs []any,
) (compiledKnowledgeFieldSource, error) {
	if sql == "" || len(sql) > maxCompiledKnowledgeAliasSourceSQLBytes ||
		strings.Count(sql, "?") != len(args) || presenceSQL == "" ||
		strings.Count(presenceSQL, "?") != len(presenceArgs) {
		return compiledKnowledgeFieldSource{}, errors.New(
			"compile ClickHouse knowledge field source: SQL or arguments are invalid",
		)
	}
	cloneArguments := func(arguments []any) ([]any, error) {
		cloned := make([]any, len(arguments))
		for index, argument := range arguments {
			value, ok := cloneCompiledArgument(argument)
			if !ok {
				return nil, errors.New(
					"compile ClickHouse knowledge field source: argument is unsupported",
				)
			}
			cloned[index] = value
		}
		return cloned, nil
	}
	clonedArgs, err := cloneArguments(args)
	if err != nil {
		return compiledKnowledgeFieldSource{}, err
	}
	clonedPresenceArgs, err := cloneArguments(presenceArgs)
	if err != nil {
		return compiledKnowledgeFieldSource{}, err
	}
	return compiledKnowledgeFieldSource{
		sql:          sql,
		args:         clonedArgs,
		presenceSQL:  presenceSQL,
		presenceArgs: clonedPresenceArgs,
	}, nil
}

func validateKnowledgeFieldSidecars(namesSQL, typesSQL, metadataVersionSQL string) error {
	count := 0
	for _, expression := range []string{namesSQL, typesSQL, metadataVersionSQL} {
		if expression != "" {
			count++
		}
	}
	if count != 0 && count != 3 {
		return errors.New(
			"compile ClickHouse knowledge field source: container sidecars are incomplete",
		)
	}
	return nil
}

func knowledgeEmptyRelativeFieldNamesSQL() string {
	return "CAST([], 'Array(String)')"
}

func knowledgeEmptyRelativeFieldTypesSQL() string {
	return "CAST([], 'Array(UInt8)')"
}

func knowledgeMissingFieldSourceSQL() string {
	return "tuple(toUInt8(0), CAST(NULL AS Dynamic), toUInt8(0), " +
		knowledgeEmptyRelativeFieldNamesSQL() + ", " +
		knowledgeEmptyRelativeFieldTypesSQL() + ", toUInt8(0))"
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

// compileKnowledgeAliasSource lowers only a direct stored Dynamic source for
// the lossless field-assignment merge. The nonempty execution gate stays
// closed until the result-side sidecar decoder and runtime copy budget have
// their pinned ClickHouse evidence.
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

	physicalSegmentSQL := make([]string, 0, len(authority.physicalSegments))
	for range authority.physicalSegments {
		physicalSegmentSQL = append(physicalSegmentSQL, "CAST(? AS String)")
	}
	materialized := knowledgeAliasMaterializedDynamicSQL(fields, physicalSegmentSQL)

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
	descendantTuple := "tuple(toUInt8(1), " + materialized +
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

type knowledgeAliasSourceExpressions struct {
	producedSQL        string
	valueSQL           string
	storedTypeSQL      string
	namesSQL           string
	typesSQL           string
	metadataVersionSQL string
}

// buildKnowledgeAliasSourceExpressions is the component form of the direct
// stored-path materializer. Callers provide compiler-owned SQL expressions for
// path values, allowing a prior destination to bind those values cheaply in an
// inner layer while leaving JSONExtract exclusively in an outer no-write
// fallback.
func buildKnowledgeAliasSourceExpressions(
	authority storedPathAuthority,
	exactPathSQL string,
	descendantPrefixSQL string,
	physicalSegmentSQL []string,
) (knowledgeAliasSourceExpressions, error) {
	if err := validateStoredPathAuthority(authority); err != nil {
		return knowledgeAliasSourceExpressions{}, err
	}
	if exactPathSQL == "" || descendantPrefixSQL == "" ||
		len(physicalSegmentSQL) != len(authority.physicalSegments) {
		return knowledgeAliasSourceExpressions{}, errors.New(
			"compile ClickHouse knowledge alias source expressions: path authority is incomplete",
		)
	}
	if slices.Contains(physicalSegmentSQL, "") {
		return knowledgeAliasSourceExpressions{}, errors.New(
			"compile ClickHouse knowledge alias source expressions: physical path is incomplete",
		)
	}

	q := quoteIdentifier
	fields := q(internalFieldsColumn)
	names := q(internalFieldNamesColumn)
	types := q(internalFieldTypesColumn)
	metadataVersion := knowledgeAliasSourceMetadataVersionSQL()
	alignedMetadata := "length(" + types + ") = length(" + names + ")"
	emptyNames := knowledgeEmptyRelativeFieldNamesSQL()
	emptyTypes := knowledgeEmptyRelativeFieldTypesSQL()

	segments := make([]string, 0, len(physicalSegmentSQL))
	for _, segmentSQL := range physicalSegmentSQL {
		segments = append(segments, "CAST("+segmentSQL+" AS String)")
	}
	materialized := knowledgeAliasMaterializedDynamicSQL(fields, segments)
	exact := "has(" + names + ", CAST(" + exactPathSQL + " AS String))"
	descendant := "arrayExists(field_name -> startsWith(field_name, CAST(" +
		descendantPrefixSQL + " AS String)), " + names + ")"
	container := "NOT (" + exact + ") AND (" + descendant + ")"
	relativeNames := "arrayMap(field_name -> substring(field_name, length(CAST(" +
		descendantPrefixSQL + " AS String)) + 1), arrayFilter(field_name -> startsWith(field_name, CAST(" +
		descendantPrefixSQL + " AS String)), " + names + "))"
	relativeTypes := "if(" + alignedMetadata +
		", arrayMap(field_index -> toUInt8(arrayElement(" + types +
		", field_index)), arrayFilter(field_index -> startsWith(arrayElement(" +
		names + ", field_index), CAST(" + descendantPrefixSQL +
		" AS String)), arrayEnumerate(" + names + "))), " + emptyTypes + ")"
	exactType := "if(" + alignedMetadata +
		", toUInt8(arrayElement(" + types + ", indexOf(" + names + ", CAST(" +
		exactPathSQL + " AS String)))), " + knowledgeDynamicStoredTypeSQL(
		authority.valueSQL(),
	) + ")"
	return knowledgeAliasSourceExpressions{
		producedSQL: "toUInt8(" + exact + " OR " + descendant + ")",
		valueSQL: "multiIf(" + exact + ", CAST(" + authority.valueSQL() +
			" AS Dynamic), " + descendant + ", " + materialized +
			", CAST(NULL AS Dynamic))",
		storedTypeSQL: "toUInt8(multiIf(" + exact + ", " + exactType +
			", " + descendant + ", toUInt8(" +
			strconv.Itoa(int(eventfields.StoredValueTypeObject)) + "), 0))",
		namesSQL: "if(" + container + ", " + relativeNames + ", " +
			emptyNames + ")",
		typesSQL: "if(" + container + ", " + relativeTypes + ", " +
			emptyTypes + ")",
		metadataVersionSQL: metadataVersion,
	}, nil
}

// knowledgeAliasMaterializedDynamicSQL reconstructs a flattened JSON parent
// only on the descendant branch. ClickHouse 26.3 makes JSONExtract over a
// native JSON column nullable; requesting Dynamic there would construct the
// forbidden Nullable(Dynamic) type even when the branch is not selected.
// Serializing the native document before extracting raw JSON also preserves
// parameterized stored paths: JSONExtractRaw accepts only constant paths when
// its input remains the native JSON type. Stripping the nullable raw wrapper
// and then parsing a concrete object map retains heterogeneous descendants
// without producing the native JSON variant. The explicit Dynamic cast keeps
// the public field contract stable without crossing either restriction.
func knowledgeAliasMaterializedDynamicSQL(
	fieldsSQL string,
	physicalSegmentSQL []string,
) string {
	raw := "JSONExtractRaw(toJSONString(" + fieldsSQL + ")"
	for _, segmentSQL := range physicalSegmentSQL {
		raw += ", " + segmentSQL
	}
	raw += ")"
	return "CAST(JSONExtract(ifNull(" + raw +
		", CAST('' AS String)), 'Map(String, Dynamic)') AS Dynamic)"
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
