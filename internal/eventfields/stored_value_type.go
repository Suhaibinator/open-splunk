package eventfields

// StoredValueType is the stable semantic type code persisted alongside each
// flattened dynamic field path. Codes deliberately match the corresponding
// open_splunk.v1.ValueType values. Only concrete protobuf leaf types are
// storable: unspecified, mixed, and missing are query-result concepts.
type StoredValueType uint8

const (
	// CurrentFieldMetadataVersion is written when field_names and field_types
	// carry the complete, aligned metadata contract defined in this package.
	CurrentFieldMetadataVersion uint8 = 1

	StoredValueTypeNull      StoredValueType = 1
	StoredValueTypeString    StoredValueType = 2
	StoredValueTypeSint64    StoredValueType = 3
	StoredValueTypeUint64    StoredValueType = 4
	StoredValueTypeDouble    StoredValueType = 5
	StoredValueTypeBool      StoredValueType = 6
	StoredValueTypeBytes     StoredValueType = 7
	StoredValueTypeTimestamp StoredValueType = 8
	StoredValueTypeDuration  StoredValueType = 9
	StoredValueTypeList      StoredValueType = 10
	StoredValueTypeObject    StoredValueType = 11
	StoredValueTypeDecimal   StoredValueType = 12
)

// IsStoredValueType reports whether code is a concrete semantic type that the
// event writer may persist in field_types.
func IsStoredValueType(code uint8) bool {
	return code >= uint8(StoredValueTypeNull) && code <= uint8(StoredValueTypeDecimal)
}
