package queryexec

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type resultContainerTransport struct {
	valid                 bool
	namesColumn           int
	typesColumn           int
	metadataVersionColumn int
	metadataCache         *resultMetadataCache
}

type resultOptionalMultivalueTransport struct {
	valid         bool
	presentColumn int
	dynamic       bool
}

const (
	// The executor repeats the shared SPL construction bounds at the sealed
	// transport boundary so a forged or incompatible backend cannot publish an
	// oversized cell.
	maximumOptionalMultivalueMembers      = spl.MaximumNativeMVValues
	maximumOptionalMultivaluePayloadBytes = spl.MaximumNativeMVPayloadBytes
)

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
			metadataCache:         new(resultMetadataCache),
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
			hiddenType.ScanType() != reflect.TypeFor[[]string]() ||
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
			reflect.TypeFor[[]string](),
		) || !exactContainerHiddenColumnType(
			columnTypes[transport.typesColumn],
			"Array(UInt8)",
			reflect.TypeFor[[]uint8](),
		) || !exactContainerHiddenColumnType(
			columnTypes[transport.metadataVersionColumn],
			"UInt8",
			reflect.TypeFor[uint8](),
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
		valueType := columnTypes[outputIndex]
		databaseType := strings.TrimSpace(valueType.DatabaseTypeName())
		dynamic := databaseType == "Array(Dynamic)" &&
			valueType.ScanType() == reflect.TypeFor[[]chcol.Dynamic]()
		stringsOnly := databaseType == "Array(String)" &&
			valueType.ScanType() == reflect.TypeFor[[]string]()
		if valueType.Nullable() || (!stringsOnly && !dynamic) ||
			!exactContainerHiddenColumnType(
				columnTypes[transport.presentColumn],
				"UInt8",
				reflect.TypeFor[uint8](),
			) {
			return nil, nil, fmt.Errorf(
				"%w: optional multivalue output transport has invalid column types",
				searchjobs.ErrInvalidResult,
			)
		}
		optionalTransports[outputIndex].dynamic = dynamic
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
		expectedScanType := reflect.TypeFor[string]()
		if output.Nullable {
			expectedDatabaseType = "Nullable(String)"
			expectedScanType = reflect.TypeFor[*string]()
		}
		if strings.TrimSpace(columnType.DatabaseTypeName()) != expectedDatabaseType ||
			columnType.Nullable() != output.Nullable ||
			columnType.ScanType() != expectedScanType ||
			!exactContainerHiddenColumnType(
				columnTypes[semanticColumn],
				"UInt8",
				reflect.TypeFor[uint8](),
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

func (decoder *resultValueDecoder) convertStringOrBytesOutput(
	destinations []any,
	valueColumn int,
	transport resultStringOrBytesTransport,
) (searchjobs.Value, error) {
	if err := decoder.check(); err != nil {
		return searchjobs.Value{}, err
	}
	semanticBytes, ok := scannedValue(
		destinations[transport.semanticBytesColumn],
	).(uint8)
	if !ok || semanticBytes > 1 {
		return searchjobs.Value{}, errors.New(
			"String-or-Bytes semantic flag has an invalid native value",
		)
	}
	return decoder.convertSemanticStringOrBytes(
		scannedValue(destinations[valueColumn]),
		semanticBytes,
		transport.nullable,
	)
}

func (decoder *resultValueDecoder) convertSemanticStringOrBytes(
	raw any,
	semanticBytes uint8,
	nullable bool,
) (searchjobs.Value, error) {
	if err := decoder.check(); err != nil {
		return searchjobs.Value{}, err
	}
	if semanticBytes > 1 {
		return searchjobs.Value{}, errors.New(
			"String-or-Bytes semantic flag is outside the supported domain",
		)
	}
	value, err := decoder.convertValue(raw)
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

func (decoder *resultValueDecoder) convertOptionalMultivalueOutput(
	destinations []any,
	valueColumn int,
	transport resultOptionalMultivalueTransport,
) (searchjobs.Value, error) {
	if err := decoder.check(); err != nil {
		return searchjobs.Value{}, err
	}
	state, ok := scannedValue(destinations[transport.presentColumn]).(uint8)
	if !ok || state > 2 {
		return searchjobs.Value{}, errors.New(
			"optional multivalue state has an invalid native value",
		)
	}
	raw := scannedValue(destinations[valueColumn])
	var (
		members        []string
		dynamicMembers []chcol.Dynamic
		nativeLength   int
	)
	if transport.dynamic {
		var ok bool
		dynamicMembers, ok = raw.([]chcol.Dynamic)
		if !ok {
			return searchjobs.Value{}, errors.New(
				"optional multivalue has an invalid native value",
			)
		}
		nativeLength = len(dynamicMembers)
	} else {
		var ok bool
		members, ok = raw.([]string)
		if !ok {
			return searchjobs.Value{}, errors.New(
				"optional multivalue has an invalid native value",
			)
		}
		nativeLength = len(members)
	}
	if state != 1 {
		if nativeLength != 0 {
			return searchjobs.Value{}, errors.New(
				"non-list optional multivalue retained a public payload",
			)
		}
		if state == 2 {
			return searchjobs.NullValue(), nil
		}
		return searchjobs.MissingValue(), nil
	}
	if transport.dynamic {
		return decoder.convertOptionalDynamicMultivalue(dynamicMembers)
	}
	if len(members) > maximumOptionalMultivalueMembers {
		return searchjobs.Value{}, errors.New(
			"optional multivalue exceeds the member limit",
		)
	}
	payloadBytes := 0
	for _, member := range members {
		if err := decoder.check(); err != nil {
			return searchjobs.Value{}, err
		}
		if !utf8.ValidString(member) {
			return searchjobs.Value{}, errors.New(
				"optional multivalue contains an invalid UTF-8 String member",
			)
		}
		if len(member) > maximumOptionalMultivaluePayloadBytes-payloadBytes {
			return searchjobs.Value{}, errors.New(
				"optional multivalue exceeds the payload limit",
			)
		}
		payloadBytes += len(member)
	}
	return decoder.convertValue(members)
}

func (decoder *resultValueDecoder) convertOptionalDynamicMultivalue(raw []chcol.Dynamic) (searchjobs.Value, error) {
	if err := decoder.check(); err != nil {
		return searchjobs.Value{}, err
	}
	if len(raw) > maximumOptionalMultivalueMembers {
		return searchjobs.Value{}, errors.New(
			"optional multivalue exceeds the member limit",
		)
	}
	payloadBytes := 0
	return decoder.list(len(raw), func(index int) (searchjobs.Value, error) {
		native := raw[index]
		if err := decoder.check(); err != nil {
			return searchjobs.Value{}, err
		}
		member, err := decoder.convertValue(native)
		if err != nil {
			return searchjobs.Value{}, fmt.Errorf(
				"optional multivalue member %d: %w",
				index,
				err,
			)
		}
		memberBytes, ok := canonicalOptionalMultivalueMemberBytes(member)
		if !ok {
			return searchjobs.Value{}, fmt.Errorf(
				"optional multivalue member %d has an unsupported type",
				index,
			)
		}
		if memberBytes > maximumOptionalMultivaluePayloadBytes-payloadBytes {
			return searchjobs.Value{}, errors.New(
				"optional multivalue exceeds the payload limit",
			)
		}
		payloadBytes += memberBytes
		return member, nil
	})
}

func canonicalOptionalMultivalueMemberBytes(member searchjobs.Value) (int, bool) {
	switch member.Kind() {
	case searchjobs.ValueKindNull:
		return len("null"), true
	case searchjobs.ValueKindString:
		value, _ := member.String()
		if !utf8.ValidString(value) {
			return 0, false
		}
		return len(value), true
	case searchjobs.ValueKindSigned:
		value, _ := member.Signed()
		return len(strconv.FormatInt(value, 10)), true
	case searchjobs.ValueKindUnsigned:
		value, _ := member.Unsigned()
		return len(strconv.FormatUint(value, 10)), true
	case searchjobs.ValueKindDouble:
		value, _ := member.Double()
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, false
		}
		return len(strconv.FormatFloat(value, 'g', -1, 64)), true
	case searchjobs.ValueKindBool:
		value, _ := member.Bool()
		return len(strconv.FormatBool(value)), true
	case searchjobs.ValueKindDecimal:
		value, _ := member.Decimal()
		canonical, err := searchjobs.CanonicalDecimal(value)
		if err != nil {
			return 0, false
		}
		return len(canonical), true
	default:
		return 0, false
	}
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
	name       string
	leaf       bool
	storedType eventfields.StoredValueType
	typed      bool
	children   []resultContainerNode
}

// A query may encounter a different shape on every row. Retain at most one
// reused shape per output, with one shared budget for all outputs.
// Cache entries own their keys and trees; driver buffers are never retained.
const (
	maximumResultMetadataCacheBytes = uint64(1 << 20)
	maximumMetadataPromotionMisses  = uint8(8)
	metadataPromotionCooldownRows   = uint8(64)
)

type resultMetadataCacheBudget struct {
	bytes uint64
}

type resultMetadataCache struct {
	names            []string
	types            []uint8
	version          uint8
	root             *resultContainerNode
	paths            [][]string
	bytes            uint64
	seed             maphash.Seed
	candidate        uint64
	candidatePresent bool
	promotionMisses  uint8
	cooldownRows     uint8
}

func (cache *resultMetadataCache) matches(names []string, types []uint8, version uint8) bool {
	return cache != nil && cache.bytes != 0 && cache.version == version &&
		slices.Equal(cache.names, names) && slices.Equal(cache.types, types)
}

func (cache *resultMetadataCache) recordHit() {
	cache.candidatePresent = false
	cache.promotionMisses, cache.cooldownRows = 0, 0
}

func (cache *resultMetadataCache) recordPromotionMiss() {
	cache.promotionMisses++
	if cache.promotionMisses == maximumMetadataPromotionMisses {
		cache.promotionMisses = 0
		cache.cooldownRows = metadataPromotionCooldownRows
		cache.candidatePresent = false
	}
}

func (cache *resultMetadataCache) retain(
	budget *resultMetadataCacheBudget,
	names []string,
	types []uint8,
	version uint8,
	root *resultContainerNode,
	paths [][]string,
) {
	if cache == nil || budget == nil {
		return
	}
	// Diverse streams should not pay for hashing every row indefinitely. During
	// this bounded pause, callers still check exact cache keys and fully parse
	// and validate every miss. A successful exact hit resets the pause.
	if cache.cooldownRows != 0 {
		cache.cooldownRows--
		return
	}
	// Promote only a shape seen on two successive successful misses. A fixed
	// fingerprint avoids cloning keys for streams whose metadata changes every
	// row. A collision can only admit an unhelpful cache entry: the current row
	// was parsed and validated in full, and every future hit compares exact keys.
	if cache.seed == (maphash.Seed{}) {
		cache.seed = maphash.MakeSeed()
	}
	var fingerprint maphash.Hash
	fingerprint.SetSeed(cache.seed)
	for _, name := range names {
		_, _ = fingerprint.WriteString(name)
		_ = fingerprint.WriteByte(0)
	}
	_, _ = fingerprint.Write(types)
	_ = fingerprint.WriteByte(version)
	candidate := fingerprint.Sum64()
	if !cache.candidatePresent || cache.candidate != candidate {
		cache.candidate, cache.candidatePresent = candidate, true
		cache.recordPromotionMiss()
		return
	}
	// Sizes come only from successfully parsed, bounded metadata. Exact-sized
	// slices and owned text make this accounting independent of driver
	// capacities, parser buffers, and Go's map allocation strategy.
	size := uint64(unsafe.Sizeof(resultMetadataCache{})) +
		uint64(len(names))*uint64(unsafe.Sizeof("")) + uint64(len(types))
	textBytes, textSlots := 0, len(names)
	for _, name := range names {
		size += uint64(len(name))
		textBytes += len(name)
	}
	if root != nil {
		size += retainedContainerNodeBytes(root)
	}
	size += uint64(len(paths)) * uint64(unsafe.Sizeof([]string{}))
	for _, path := range paths {
		size += uint64(len(path)) * uint64(unsafe.Sizeof(""))
		textSlots += len(path)
		for _, segment := range path {
			size += uint64(len(segment))
			textBytes += len(segment)
		}
	}
	budget.bytes -= cache.bytes
	*cache = resultMetadataCache{seed: cache.seed, promotionMisses: cache.promotionMisses}
	if size > maximumResultMetadataCacheBytes-budget.bytes {
		cache.recordPromotionMiss()
		return
	}
	cache.recordHit()
	// Pack detached text and path headers instead of allocating one string per
	// segment on every cache miss. The completed backing string is immutable;
	// replacing an entry never rewrites storage used by another entry.
	var text strings.Builder
	text.Grow(textBytes)
	for _, name := range names {
		text.WriteString(name)
	}
	for _, path := range paths {
		for _, segment := range path {
			text.WriteString(segment)
		}
	}
	packed := text.String()
	stringsByPath := make([]string, textSlots)
	cache.names = stringsByPath[:len(names):len(names)]
	offset := 0
	for index, name := range names {
		cache.names[index] = packed[offset : offset+len(name)]
		offset += len(name)
	}
	cache.types = make([]uint8, len(types))
	copy(cache.types, types)
	cache.paths = make([][]string, len(paths))
	first := len(names)
	for index, path := range paths {
		end := first + len(path)
		cache.paths[index] = stringsByPath[first:end:end]
		for segmentIndex, segment := range path {
			cache.paths[index][segmentIndex] = packed[offset : offset+len(segment)]
			offset += len(segment)
		}
		first = end
	}
	cache.version, cache.root, cache.bytes = version, root, size
	budget.bytes += size
}

func retainedContainerNodeBytes(node *resultContainerNode) uint64 {
	size := uint64(unsafe.Sizeof(resultContainerNode{})) + uint64(len(node.name))
	for index := range node.children {
		size += retainedContainerNodeBytes(&node.children[index])
	}
	return size
}

func convertContainerOutput(
	value any,
	names []string,
	types []uint8,
	metadataVersion uint8,
) (searchjobs.Value, error) {
	return convertContainerOutputWithCache(value, names, types, metadataVersion, nil, nil)
}

func (decoder *resultValueDecoder) convertContainerOutputWithCache(
	value any,
	names []string,
	types []uint8,
	metadataVersion uint8,
	cache *resultMetadataCache,
	budget *resultMetadataCacheBudget,
) (searchjobs.Value, error) {
	if err := decoder.check(); err != nil {
		return searchjobs.Value{}, err
	}
	// Container-capable outputs can be overwritten by a scalar on an individual
	// row. The compiler seals that case as version zero with both relative
	// metadata arrays empty; no container reconstruction is required. A version
	// zero row carrying either sidecar remains invalid and is rejected below.
	if metadataVersion == 0 && len(names) == 0 && len(types) == 0 {
		return decoder.convertValue(value)
	}
	if cache.matches(names, types, metadataVersion) {
		converted, err := decoder.convertParsedContainerOutput(cache.root, value)
		if err == nil {
			cache.recordHit()
		}
		return converted, err
	}
	metadata, err := eventfields.ParseStoredContainerMetadata(
		names,
		types,
		metadataVersion,
	)
	if err != nil {
		return searchjobs.Value{}, err
	}
	if err := decoder.phase(); err != nil {
		return searchjobs.Value{}, err
	}
	leaves := make([]resultContainerLeaf, len(metadata.Paths))
	for index, path := range metadata.Paths {
		if err := decoder.check(); err != nil {
			return searchjobs.Value{}, err
		}
		leaves[index].path = path
		if metadata.Types != nil {
			leaves[index].typed = true
			leaves[index].storedType = metadata.Types[index]
		}
	}
	// Stored names sort by escaped spelling. Public object keys sort by their
	// decoded spelling, so establish that order once when constructing a tree.
	if err := sortDecodedValues(decoder, leaves, func(left, right resultContainerLeaf) int {
		return slices.Compare(left.path, right.path)
	}); err != nil {
		return searchjobs.Value{}, err
	}
	var root *resultContainerNode
	if len(leaves) != 0 {
		built := decoder.buildResultContainerNode(leaves, 0)
		if err := decoder.phase(); err != nil {
			return searchjobs.Value{}, err
		}
		root = &built
	}
	converted, err := decoder.convertParsedContainerOutput(root, value)
	if err == nil {
		if err := decoder.phase(); err != nil {
			return searchjobs.Value{}, err
		}
		cache.retain(budget, names, types, metadataVersion, root, nil)
	}
	return converted, err
}

type resultContainerLeaf struct {
	path       []string
	storedType eventfields.StoredValueType
	typed      bool
}

func (decoder *resultValueDecoder) buildResultContainerNode(leaves []resultContainerLeaf, depth int) resultContainerNode {
	if decoder.check() != nil {
		return resultContainerNode{}
	}
	if len(leaves[0].path) == depth {
		return resultContainerNode{leaf: true, typed: leaves[0].typed, storedType: leaves[0].storedType}
	}
	groups := 1
	for index := 1; index < len(leaves); index++ {
		if decoder.check() != nil {
			return resultContainerNode{}
		}
		if leaves[index-1].path[depth] != leaves[index].path[depth] {
			groups++
		}
	}
	node := resultContainerNode{children: make([]resultContainerNode, groups)}
	for first, group := 0, 0; first < len(leaves); group++ {
		if decoder.check() != nil {
			return resultContainerNode{}
		}
		name := leaves[first].path[depth]
		end := first + 1
		for end < len(leaves) && leaves[end].path[depth] == name {
			if decoder.check() != nil {
				return resultContainerNode{}
			}
			end++
		}
		node.children[group] = decoder.buildResultContainerNode(leaves[first:end], depth+1)
		node.children[group].name = strings.Clone(name)
		first = end
	}
	return node
}

func (decoder *resultValueDecoder) convertParsedContainerOutput(root *resultContainerNode, value any) (searchjobs.Value, error) {
	if err := decoder.check(); err != nil {
		return searchjobs.Value{}, err
	}
	if root == nil {
		return decoder.convertValue(value)
	}
	return decoder.convertContainerNode(root, value, true)
}

func (decoder *resultValueDecoder) convertContainerNode(
	node *resultContainerNode,
	raw any,
	present bool,
) (searchjobs.Value, error) {
	if err := decoder.check(); err != nil {
		return searchjobs.Value{}, err
	}
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
		converted, err := decoder.convertValue(raw)
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
	rawFields, err := decoder.containerObjectFields(raw, present)
	if err != nil {
		return searchjobs.Value{}, err
	}
	result, err := decoder.object(len(node.children), func(index int) (searchjobs.ObjectField, error) {
		if err := decoder.check(); err != nil {
			return searchjobs.ObjectField{}, err
		}
		childNode := &node.children[index]
		name := childNode.name
		childRaw, childPresent := rawFields[name]
		child, convertErr := decoder.convertContainerNode(
			childNode,
			childRaw,
			childPresent,
		)
		if convertErr != nil {
			return searchjobs.ObjectField{}, fmt.Errorf("container field %q: %w", name, convertErr)
		}
		delete(rawFields, name)
		return searchjobs.ObjectField{Name: name, Value: child}, nil
	})
	if err != nil {
		return searchjobs.Value{}, err
	}
	for _, extra := range rawFields {
		if err := decoder.check(); err != nil {
			return searchjobs.Value{}, err
		}
		if !decoder.containerNativeValueIsOnlyNull(extra) {
			return searchjobs.Value{}, errors.New(
				"container value contains fields absent from its metadata",
			)
		}
	}
	if err := decoder.phase(); err != nil {
		return searchjobs.Value{}, err
	}
	return result, nil
}

func (decoder *resultValueDecoder) containerObjectFields(raw any, present bool) (map[string]any, error) {
	if err := decoder.check(); err != nil {
		return nil, err
	}
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
		return decoder.containerJSONFields(&value)
	case *chcol.JSON:
		return decoder.containerJSONFields(value)
	}
	reflected := reflect.ValueOf(raw)
	for reflected.IsValid() && (reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface) {
		if err := decoder.check(); err != nil {
			return nil, err
		}
		if reflected.IsNil() {
			return make(map[string]any), nil
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() || reflected.Kind() != reflect.Map ||
		reflected.Type().Key().Kind() != reflect.String {
		return nil, errors.New("container value is not an object")
	}
	if err := decoder.phase(); err != nil {
		return nil, err
	}
	result := make(map[string]any, reflected.Len())
	for _, key := range reflected.MapKeys() {
		if err := decoder.check(); err != nil {
			return nil, err
		}
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

func (decoder *resultValueDecoder) containerJSONFields(document *chcol.JSON) (map[string]any, error) {
	if err := decoder.check(); err != nil {
		return nil, err
	}
	if document == nil {
		return make(map[string]any), nil
	}
	values, err := decoder.normalizedJSONValues(document)
	if err != nil {
		return nil, errors.New("container JSON paths are invalid")
	}
	if err := decoder.phase(); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(values))
	for path := range values {
		if err := decoder.check(); err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	if err := sortDecodedValues(decoder, paths, strings.Compare); err != nil {
		return nil, err
	}
	if err := decoder.phase(); err != nil {
		return nil, err
	}
	root := make(map[string]any)
	for _, path := range paths {
		if err := decoder.check(); err != nil {
			return nil, err
		}
		segments, parseErr := eventfields.ParseNormalizedDynamicPath(path)
		if parseErr != nil || decoder.insertResultPath(root, segments, values[path]) != nil {
			return nil, errors.New("container JSON paths collide after decoding")
		}
	}
	return root, nil
}

func (decoder *resultValueDecoder) containerNativeValueIsOnlyNull(value any) bool {
	if decoder.check() != nil {
		return false
	}
	if isNullJSONPathValue(value) {
		return true
	}
	switch value := value.(type) {
	case chcol.Dynamic:
		return decoder.containerNativeValueIsOnlyNull(value.Any())
	case *chcol.Dynamic:
		if value == nil {
			return true
		}
		return decoder.containerNativeValueIsOnlyNull(value.Any())
	case chcol.JSON:
		return decoder.containerJSONIsOnlyNull(&value)
	case *chcol.JSON:
		return decoder.containerJSONIsOnlyNull(value)
	}
	reflected := reflect.ValueOf(value)
	for reflected.IsValid() && (reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface) {
		if decoder.check() != nil {
			return false
		}
		if reflected.IsNil() {
			return true
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() || reflected.Kind() != reflect.Map || reflected.Len() == 0 {
		return false
	}
	for _, key := range reflected.MapKeys() {
		if decoder.check() != nil {
			return false
		}
		if !decoder.containerNativeValueIsOnlyNull(reflected.MapIndex(key).Interface()) {
			return false
		}
	}
	return true
}

func (decoder *resultValueDecoder) containerJSONIsOnlyNull(document *chcol.JSON) bool {
	if decoder.check() != nil {
		return false
	}
	if document == nil {
		return true
	}
	values, err := decoder.normalizedJSONValues(document)
	if err != nil || len(values) == 0 {
		return false
	}
	for _, value := range values {
		if decoder.check() != nil {
			return false
		}
		if !decoder.containerNativeValueIsOnlyNull(value) {
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
