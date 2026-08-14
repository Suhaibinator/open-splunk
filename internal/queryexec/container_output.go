package queryexec

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type resultContainerTransport struct {
	valid                 bool
	namesColumn           int
	typesColumn           int
	metadataVersionColumn int
}

type resultOptionalMultivalueTransport struct {
	valid         bool
	presentColumn int
}

type resultStringOrBytesTransport struct {
	valid               bool
	semanticBytesColumn int
	nullable            bool
}

func validateOrdinaryResultColumns(
	query clickhouse.CompiledQuery,
	columns []string,
	columnTypes []driver.ColumnType,
	sparseFieldIndex int,
) ([]resultContainerTransport, []resultOptionalMultivalueTransport, error) {
	outputs, ok := query.ValidatedResultContainerOutputs()
	if !ok {
		return nil, nil, fmt.Errorf(
			"%w: compiled container output contract is invalid",
			searchjobs.ErrInvalidResult,
		)
	}
	optionalOutputs, ok := query.ValidatedResultOptionalMultivalueOutputs()
	if !ok {
		return nil, nil, fmt.Errorf(
			"%w: compiled optional multivalue output contract is invalid",
			searchjobs.ErrInvalidResult,
		)
	}
	stringOrBytesOutputs, ok := query.ValidatedResultStringOrBytesOutputs()
	if !ok {
		return nil, nil, fmt.Errorf(
			"%w: compiled String-or-Bytes output contract is invalid",
			searchjobs.ErrInvalidResult,
		)
	}
	expected := slices.Clone(query.OutputFields)
	if sparseFieldIndex >= 0 {
		expected = append(expected, clickhouse.SparseEventFieldNamesColumn)
	}
	transports := make([]resultContainerTransport, len(query.OutputFields))
	optionalTransports := make(
		[]resultOptionalMultivalueTransport,
		len(query.OutputFields),
	)
	for _, output := range outputs {
		base := len(expected)
		expected = append(
			expected,
			output.NamesColumn(),
			output.TypesColumn(),
			output.MetadataVersionColumn(),
		)
		transports[int(output.OutputIndex)] = resultContainerTransport{
			valid:                 true,
			namesColumn:           base,
			typesColumn:           base + 1,
			metadataVersionColumn: base + 2,
		}
	}
	for _, output := range optionalOutputs {
		column := len(expected)
		expected = append(expected, output.PresentColumn())
		optionalTransports[int(output.OutputIndex)] = resultOptionalMultivalueTransport{
			valid:         true,
			presentColumn: column,
		}
	}
	for _, output := range stringOrBytesOutputs {
		expected = append(expected, output.SemanticBytesColumn())
	}
	if len(columns) != len(expected) || len(columnTypes) != len(columns) ||
		!slices.Equal(columns, expected) {
		return nil, nil, fmt.Errorf(
			"%w: ClickHouse result columns do not match the compiled output",
			searchjobs.ErrInvalidResult,
		)
	}
	if sparseFieldIndex >= 0 {
		hiddenType := columnTypes[len(query.OutputFields)]
		if hiddenType.Nullable() || unwrapType(hiddenType.DatabaseTypeName()) != "Array(String)" ||
			hiddenType.ScanType() != reflect.TypeOf([]string{}) ||
			!strings.HasPrefix(unwrapType(columnTypes[sparseFieldIndex].DatabaseTypeName()), "JSON") {
			return nil, nil, fmt.Errorf(
				"%w: sparse event fields transport has invalid column types",
				searchjobs.ErrInvalidResult,
			)
		}
	}
	for outputIndex, transport := range transports {
		if !transport.valid {
			continue
		}
		if !exactContainerPublicColumnType(
			columnTypes[outputIndex].DatabaseTypeName(),
		) || !exactContainerHiddenColumnType(
			columnTypes[transport.namesColumn],
			"Array(String)",
			reflect.TypeOf([]string{}),
		) || !exactContainerHiddenColumnType(
			columnTypes[transport.typesColumn],
			"Array(UInt8)",
			reflect.TypeOf([]uint8{}),
		) || !exactContainerHiddenColumnType(
			columnTypes[transport.metadataVersionColumn],
			"UInt8",
			reflect.TypeOf(uint8(0)),
		) {
			return nil, nil, fmt.Errorf(
				"%w: container output transport has invalid column types",
				searchjobs.ErrInvalidResult,
			)
		}
	}
	for outputIndex, transport := range optionalTransports {
		if !transport.valid {
			continue
		}
		if strings.TrimSpace(columnTypes[outputIndex].DatabaseTypeName()) != "Array(String)" ||
			columnTypes[outputIndex].Nullable() ||
			columnTypes[outputIndex].ScanType() != reflect.TypeOf([]string{}) ||
			!exactContainerHiddenColumnType(
				columnTypes[transport.presentColumn],
				"UInt8",
				reflect.TypeOf(uint8(0)),
			) {
			return nil, nil, fmt.Errorf(
				"%w: optional multivalue output transport has invalid column types",
				searchjobs.ErrInvalidResult,
			)
		}
	}
	return transports, optionalTransports, nil
}

func validateStringOrBytesResultColumns(
	query clickhouse.CompiledQuery,
	columns []string,
	columnTypes []driver.ColumnType,
) ([]resultStringOrBytesTransport, error) {
	outputs, ok := query.ValidatedResultStringOrBytesOutputs()
	if !ok {
		return nil, fmt.Errorf(
			"%w: compiled String-or-Bytes output contract is invalid",
			searchjobs.ErrInvalidResult,
		)
	}
	transports := make([]resultStringOrBytesTransport, len(query.OutputFields))
	columnIndexes := make(map[string]int, len(columns))
	for index, name := range columns {
		columnIndexes[name] = index
	}
	for _, output := range outputs {
		index := int(output.OutputIndex)
		semanticColumn, present := columnIndexes[output.SemanticBytesColumn()]
		if index >= len(columnTypes) || !present || semanticColumn >= len(columnTypes) {
			return nil, fmt.Errorf(
				"%w: String-or-Bytes output transport is missing its column",
				searchjobs.ErrInvalidResult,
			)
		}
		columnType := columnTypes[index]
		expectedDatabaseType := "String"
		expectedScanType := reflect.TypeOf("")
		if output.Nullable {
			expectedDatabaseType = "Nullable(String)"
			expectedScanType = reflect.TypeOf((*string)(nil))
		}
		if strings.TrimSpace(columnType.DatabaseTypeName()) != expectedDatabaseType ||
			columnType.Nullable() != output.Nullable ||
			columnType.ScanType() != expectedScanType ||
			!exactContainerHiddenColumnType(
				columnTypes[semanticColumn],
				"UInt8",
				reflect.TypeOf(uint8(0)),
			) {
			return nil, fmt.Errorf(
				"%w: String-or-Bytes output transport has an invalid column type",
				searchjobs.ErrInvalidResult,
			)
		}
		transports[index] = resultStringOrBytesTransport{
			valid:               true,
			semanticBytesColumn: semanticColumn,
			nullable:            output.Nullable,
		}
	}
	return transports, nil
}

func convertStringOrBytesOutput(
	destinations []any,
	valueColumn int,
	transport resultStringOrBytesTransport,
) (searchjobs.Value, error) {
	semanticBytes, ok := scannedValue(
		destinations[transport.semanticBytesColumn],
	).(uint8)
	if !ok || semanticBytes > 1 {
		return searchjobs.Value{}, errors.New(
			"String-or-Bytes semantic flag has an invalid native value",
		)
	}
	return convertSemanticStringOrBytes(
		scannedValue(destinations[valueColumn]),
		semanticBytes,
		transport.nullable,
	)
}

func convertSemanticStringOrBytes(
	raw any,
	semanticBytes uint8,
	nullable bool,
) (searchjobs.Value, error) {
	if semanticBytes > 1 {
		return searchjobs.Value{}, errors.New(
			"String-or-Bytes semantic flag is outside the supported domain",
		)
	}
	value, err := convertValue(raw)
	if err != nil {
		return searchjobs.Value{}, err
	}
	switch value.Kind() {
	case searchjobs.ValueKindNull:
		if !nullable || semanticBytes != 0 {
			return searchjobs.Value{}, errors.New(
				"String-or-Bytes null has inconsistent semantic provenance",
			)
		}
		return value, nil
	case searchjobs.ValueKindString:
		if semanticBytes == 0 {
			return value, nil
		}
		text, _ := value.String()
		return searchjobs.BytesValue([]byte(text)), nil
	case searchjobs.ValueKindBytes:
		// Invalid UTF-8 is intrinsically Bytes even for fixed multivalue
		// producers whose sidecar records only semantic binary declarations.
		return value, nil
	default:
		return searchjobs.Value{}, errors.New(
			"String-or-Bytes output has an invalid cell kind",
		)
	}
}

func convertOptionalMultivalueOutput(
	destinations []any,
	valueColumn int,
	transport resultOptionalMultivalueTransport,
) (searchjobs.Value, error) {
	present, ok := scannedValue(destinations[transport.presentColumn]).(uint8)
	if !ok || present > 1 {
		return searchjobs.Value{}, errors.New(
			"optional multivalue presence has an invalid native value",
		)
	}
	raw, ok := scannedValue(destinations[valueColumn]).([]string)
	if !ok {
		return searchjobs.Value{}, errors.New(
			"optional multivalue has an invalid native value",
		)
	}
	if present == 0 {
		if len(raw) != 0 {
			return searchjobs.Value{}, errors.New(
				"absent optional multivalue retained a public payload",
			)
		}
		return searchjobs.NullValue(), nil
	}
	for _, member := range raw {
		if !utf8.ValidString(member) {
			return searchjobs.Value{}, errors.New(
				"optional multivalue contains an invalid UTF-8 String member",
			)
		}
	}
	return convertValue(raw)
}

func exactContainerPublicColumnType(databaseType string) bool {
	base := strings.TrimSpace(databaseType)
	return base == "Dynamic" ||
		strings.HasPrefix(base, "Dynamic(") && strings.HasSuffix(base, ")")
}

func exactContainerHiddenColumnType(
	column driver.ColumnType,
	databaseType string,
	scanType reflect.Type,
) bool {
	return !column.Nullable() &&
		strings.TrimSpace(column.DatabaseTypeName()) == databaseType &&
		column.ScanType() == scanType
}

func scannedContainerMetadata(
	destinations []any,
	transport resultContainerTransport,
) ([]string, []uint8, uint8, error) {
	names, namesOK := scannedValue(destinations[transport.namesColumn]).([]string)
	types, typesOK := scannedValue(destinations[transport.typesColumn]).([]uint8)
	version, versionOK := scannedValue(
		destinations[transport.metadataVersionColumn],
	).(uint8)
	if !namesOK || !typesOK || !versionOK {
		return nil, nil, 0, errors.New(
			"container output metadata has an invalid native type",
		)
	}
	return names, types, version, nil
}

type resultContainerNode struct {
	leaf       bool
	storedType eventfields.StoredValueType
	typed      bool
	children   map[string]*resultContainerNode
}

func convertContainerOutput(
	value any,
	names []string,
	types []uint8,
	metadataVersion uint8,
) (searchjobs.Value, error) {
	metadata, err := eventfields.ParseStoredContainerMetadata(
		names,
		types,
		metadataVersion,
	)
	if err != nil {
		return searchjobs.Value{}, err
	}
	if len(metadata.Paths) == 0 {
		return convertValue(value)
	}
	root := &resultContainerNode{children: make(map[string]*resultContainerNode)}
	for index, path := range metadata.Paths {
		current := root
		for _, segment := range path {
			if current.children == nil {
				current.children = make(map[string]*resultContainerNode)
			}
			next := current.children[segment]
			if next == nil {
				next = &resultContainerNode{}
				current.children[segment] = next
			}
			current = next
		}
		current.leaf = true
		if metadata.Types != nil {
			current.typed = true
			current.storedType = metadata.Types[index]
		}
	}
	return convertContainerNode(root, value, true)
}

func convertContainerNode(
	node *resultContainerNode,
	raw any,
	present bool,
) (searchjobs.Value, error) {
	if node == nil {
		return searchjobs.Value{}, errors.New("container metadata node is absent")
	}
	if node.leaf {
		if !present || raw == nil {
			if node.typed && node.storedType != eventfields.StoredValueTypeNull {
				return searchjobs.Value{}, errors.New(
					"container metadata is missing a non-null value",
				)
			}
			return searchjobs.NullValue(), nil
		}
		converted, err := convertValue(raw)
		if err != nil {
			return searchjobs.Value{}, err
		}
		if node.typed && !storedContainerValueKindMatches(node.storedType, converted.Kind()) {
			return searchjobs.Value{}, errors.New(
				"container value disagrees with its stored type",
			)
		}
		return converted, nil
	}
	rawFields, err := containerObjectFields(raw, present)
	if err != nil {
		return searchjobs.Value{}, err
	}
	names := make([]string, 0, len(node.children))
	for name := range node.children {
		names = append(names, name)
	}
	slices.Sort(names)
	fields := make([]searchjobs.ObjectField, 0, len(names))
	for _, name := range names {
		childRaw, childPresent := rawFields[name]
		child, convertErr := convertContainerNode(
			node.children[name],
			childRaw,
			childPresent,
		)
		if convertErr != nil {
			return searchjobs.Value{}, fmt.Errorf("container field %q: %w", name, convertErr)
		}
		fields = append(fields, searchjobs.ObjectField{Name: name, Value: child})
		delete(rawFields, name)
	}
	for _, extra := range rawFields {
		if !containerNativeValueIsOnlyNull(extra) {
			return searchjobs.Value{}, errors.New(
				"container value contains fields absent from its metadata",
			)
		}
	}
	return searchjobs.ObjectValue(fields...)
}

func containerObjectFields(raw any, present bool) (map[string]any, error) {
	if !present || raw == nil {
		return make(map[string]any), nil
	}
	switch value := raw.(type) {
	case chcol.Dynamic:
		if value.Nil() {
			return make(map[string]any), nil
		}
		raw = value.Any()
	case *chcol.Dynamic:
		if value == nil || value.Nil() {
			return make(map[string]any), nil
		}
		raw = value.Any()
	}
	switch value := raw.(type) {
	case chcol.JSON:
		return containerJSONFields(&value)
	case *chcol.JSON:
		return containerJSONFields(value)
	}
	reflected := reflect.ValueOf(raw)
	for reflected.IsValid() && (reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface) {
		if reflected.IsNil() {
			return make(map[string]any), nil
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() || reflected.Kind() != reflect.Map ||
		reflected.Type().Key().Kind() != reflect.String {
		return nil, errors.New("container value is not an object")
	}
	result := make(map[string]any, reflected.Len())
	for _, key := range reflected.MapKeys() {
		name, err := eventfields.DecodePhysicalPathSegment(key.String())
		if err != nil {
			return nil, fmt.Errorf("container object key %q: %w", key.String(), err)
		}
		if _, duplicate := result[name]; duplicate {
			return nil, errors.New("container object keys collide after decoding")
		}
		result[name] = reflected.MapIndex(key).Interface()
	}
	return result, nil
}

func containerJSONFields(document *chcol.JSON) (map[string]any, error) {
	if document == nil {
		return make(map[string]any), nil
	}
	values, err := normalizedJSONValues(document)
	if err != nil {
		return nil, errors.New("container JSON paths are invalid")
	}
	paths := make([]string, 0, len(values))
	for path := range values {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	root := make(map[string]any)
	for _, path := range paths {
		segments, parseErr := eventfields.ParseNormalizedDynamicPath(path)
		if parseErr != nil || insertResultPath(root, segments, values[path]) != nil {
			return nil, errors.New("container JSON paths collide after decoding")
		}
	}
	return root, nil
}

func containerNativeValueIsOnlyNull(value any) bool {
	if isNullJSONPathValue(value) {
		return true
	}
	switch value := value.(type) {
	case chcol.Dynamic:
		return containerNativeValueIsOnlyNull(value.Any())
	case *chcol.Dynamic:
		if value == nil {
			return true
		}
		return containerNativeValueIsOnlyNull(value.Any())
	case chcol.JSON:
		return containerJSONIsOnlyNull(&value)
	case *chcol.JSON:
		return containerJSONIsOnlyNull(value)
	}
	reflected := reflect.ValueOf(value)
	for reflected.IsValid() && (reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface) {
		if reflected.IsNil() {
			return true
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() || reflected.Kind() != reflect.Map || reflected.Len() == 0 {
		return false
	}
	for _, key := range reflected.MapKeys() {
		if !containerNativeValueIsOnlyNull(reflected.MapIndex(key).Interface()) {
			return false
		}
	}
	return true
}

func containerJSONIsOnlyNull(document *chcol.JSON) bool {
	if document == nil {
		return true
	}
	values, err := normalizedJSONValues(document)
	if err != nil || len(values) == 0 {
		return false
	}
	for _, value := range values {
		if !containerNativeValueIsOnlyNull(value) {
			return false
		}
	}
	return true
}

func storedContainerValueKindMatches(
	stored eventfields.StoredValueType,
	kind searchjobs.ValueKind,
) bool {
	switch stored {
	case eventfields.StoredValueTypeNull:
		return kind == searchjobs.ValueKindNull
	case eventfields.StoredValueTypeString:
		return kind == searchjobs.ValueKindString
	case eventfields.StoredValueTypeSint64:
		return kind == searchjobs.ValueKindSigned
	case eventfields.StoredValueTypeUint64:
		return kind == searchjobs.ValueKindUnsigned
	case eventfields.StoredValueTypeDouble:
		return kind == searchjobs.ValueKindDouble
	case eventfields.StoredValueTypeBool:
		return kind == searchjobs.ValueKindBool
	case eventfields.StoredValueTypeBytes:
		return kind == searchjobs.ValueKindBytes
	case eventfields.StoredValueTypeTimestamp:
		return kind == searchjobs.ValueKindTime
	case eventfields.StoredValueTypeDuration:
		return kind == searchjobs.ValueKindDuration
	case eventfields.StoredValueTypeList:
		return kind == searchjobs.ValueKindList
	case eventfields.StoredValueTypeObject:
		return kind == searchjobs.ValueKindObject
	case eventfields.StoredValueTypeDecimal:
		return kind == searchjobs.ValueKindDecimal
	default:
		return false
	}
}
