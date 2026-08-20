package eventfields

import (
	"errors"
	"fmt"
)

// StoredContainerMetadata is validated metadata for the flattened leaves
// relative to one stored container value. Paths are decoded logical segments.
// Types is aligned one-for-one with Paths, including a non-nil empty slice for
// an empty current-format container.
// The returned slices are owned by the caller.
type StoredContainerMetadata struct {
	Paths [][]string
	Types []StoredValueType
}

// ParseStoredContainerMetadata validates the bounded, canonical metadata used
// to reconstruct one stored container. Unlike ParseStoredFieldNames, relative
// paths may begin with a globally reserved event root because they are nested
// below an already-authorized destination field.
//
// Version 1 requires one valid stored semantic type per name. Every other
// version fails closed until its exact contract is implemented here.
func ParseStoredContainerMetadata(
	names []string,
	types []uint8,
	version uint8,
) (StoredContainerMetadata, error) {
	if version != CurrentFieldMetadataVersion {
		return StoredContainerMetadata{}, fmt.Errorf(
			"stored container metadata version %d is unsupported; provision fresh event storage",
			version,
		)
	}
	if len(types) != len(names) {
		return StoredContainerMetadata{}, errors.New(
			"stored container metadata names and types are not aligned",
		)
	}

	paths, err := parseStoredFieldNames(names, true)
	if err != nil {
		return StoredContainerMetadata{}, fmt.Errorf(
			"stored container field names: %w",
			err,
		)
	}
	parsedTypes := make([]StoredValueType, len(types))
	for index, code := range types {
		if !IsStoredValueType(code) {
			return StoredContainerMetadata{}, fmt.Errorf(
				"stored container field type %d at index %d is invalid",
				code,
				index,
			)
		}
		parsedTypes[index] = StoredValueType(code)
	}
	return StoredContainerMetadata{Paths: paths, Types: parsedTypes}, nil
}
