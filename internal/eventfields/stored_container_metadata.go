package eventfields

import (
	"errors"
	"fmt"
)

// StoredContainerMetadata is validated metadata for the flattened leaves
// relative to one stored container value. Paths are decoded logical segments.
// Types is nil for legacy v0 names-only metadata and is aligned one-for-one
// with Paths for v1, including a non-nil empty slice for an empty v1 container.
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
// Version 0 is the legacy names-only format and requires an empty types array.
// Version 1 requires one valid stored semantic type per name. Every other
// version fails closed until its exact contract is implemented here.
func ParseStoredContainerMetadata(
	names []string,
	types []uint8,
	version uint8,
) (StoredContainerMetadata, error) {
	switch version {
	case 0:
		if len(types) != 0 {
			return StoredContainerMetadata{}, errors.New(
				"stored container metadata v0 contains field types",
			)
		}
	case CurrentFieldMetadataVersion:
		if len(types) != len(names) {
			return StoredContainerMetadata{}, errors.New(
				"stored container metadata v1 names and types are not aligned",
			)
		}
	default:
		return StoredContainerMetadata{}, fmt.Errorf(
			"stored container metadata version %d is unsupported",
			version,
		)
	}

	paths, err := parseStoredFieldNames(names, true)
	if err != nil {
		return StoredContainerMetadata{}, fmt.Errorf(
			"stored container field names: %w",
			err,
		)
	}
	if version == 0 {
		return StoredContainerMetadata{Paths: paths}, nil
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
