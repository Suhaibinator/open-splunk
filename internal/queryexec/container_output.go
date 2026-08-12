package queryexec

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

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

func validateOrdinaryResultColumns(
	query clickhouse.CompiledQuery,
	columns []string,
	columnTypes []driver.ColumnType,
	sparseFieldIndex int,
) ([]resultContainerTransport, error) {
	outputs, ok := query.ValidatedResultContainerOutputs()
	if !ok {
		return nil, fmt.Errorf(
			"%w: compiled container output contract is invalid",
			searchjobs.ErrInvalidResult,
		)
	}
	expected := slices.Clone(query.OutputFields)
	if sparseFieldIndex >= 0 {
		expected = append(expected, clickhouse.SparseEventFieldNamesColumn)
	}
	transports := make([]resultContainerTransport, len(query.OutputFields))
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
	if len(columns) != len(expected) || len(columnTypes) != len(columns) ||
		!slices.Equal(columns, expected) {
		return nil, fmt.Errorf(
			"%w: ClickHouse result columns do not match the compiled output",
			searchjobs.ErrInvalidResult,
		)
	}
	if sparseFieldIndex >= 0 {
		hiddenType := columnTypes[len(query.OutputFields)]
		if hiddenType.Nullable() || unwrapType(hiddenType.DatabaseTypeName()) != "Array(String)" ||
			hiddenType.ScanType() != reflect.TypeOf([]string{}) ||
			!strings.HasPrefix(unwrapType(columnTypes[sparseFieldIndex].DatabaseTypeName()), "JSON") {
			return nil, fmt.Errorf(
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
			return nil, fmt.Errorf(
				"%w: container output transport has invalid column types",
				searchjobs.ErrInvalidResult,
			)
		}
	}
	return transports, nil
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
