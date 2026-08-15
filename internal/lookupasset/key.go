package lookupasset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"unicode/utf8"
)

const exactKeyDomain = "open-splunk/exact-lookup-key/v1\x00"

// ExactKey carries both an acceleration digest and authoritative collision-
// resistant framing bytes. Consumers must compare Bytes after selecting a
// digest bucket; a hash match alone is never equality authority.
type ExactKey struct {
	digest    [sha256.Size]byte
	canonical []byte
}

func (key ExactKey) SHA256() [sha256.Size]byte { return key.digest }
func (key ExactKey) Bytes() []byte             { return bytes.Clone(key.canonical) }

// CanonicalizeExactKey length-frames one through four case-sensitive UTF-8
// string components. Empty strings are ordinary values. Missing/null event
// fields are represented by the caller declining to construct a key.
func CanonicalizeExactKey(values []string) (ExactKey, error) {
	return canonicalizeExactKey(values, sha256.Sum256)
}

type exactKeyHasher func([]byte) [sha256.Size]byte

func canonicalizeExactKey(values []string, hasher exactKeyHasher) (ExactKey, error) {
	if len(values) < 1 || len(values) > MaximumKeyColumns || hasher == nil {
		return ExactKey{}, fmt.Errorf("%w: an exact lookup key requires one through %d components", ErrInvalidArgument, MaximumKeyColumns)
	}
	capacity := len(exactKeyDomain) + 1
	for _, value := range values {
		if !utf8.ValidString(value) || len(value) > MaximumCellBytes {
			return ExactKey{}, fmt.Errorf("%w: exact lookup key component is not a bounded UTF-8 string", ErrInvalidArgument)
		}
		capacity += binary.MaxVarintLen64 + len(value)
	}
	canonical := make([]byte, 0, capacity)
	canonical = append(canonical, exactKeyDomain...)
	canonical = binary.AppendUvarint(canonical, uint64(len(values)))
	for _, value := range values {
		canonical = binary.AppendUvarint(canonical, uint64(len(value)))
		canonical = append(canonical, value...)
	}
	return ExactKey{digest: hasher(canonical), canonical: canonical}, nil
}

type indexedRow struct {
	key []byte
	row []string
}

// ExactIndex is a detached immutable hash-bucket index over one Asset. Hash
// collisions remain distinct because Find compares canonical key bytes.
type ExactIndex struct {
	keyColumns []string
	buckets    map[[sha256.Size]byte][]indexedRow
	hasher     exactKeyHasher
}

// BuildExactIndex resolves ordered lookup key columns and rejects duplicate
// exact composite keys before a logical definition can be published.
func BuildExactIndex(asset *Asset, keyColumns []string) (*ExactIndex, error) {
	return buildExactIndex(asset, keyColumns, sha256.Sum256)
}

// ValidateUniqueKeys performs publication-time key validation without
// retaining an index.
func ValidateUniqueKeys(asset *Asset, keyColumns []string) error {
	return ValidateUniqueKeysContext(context.Background(), asset, keyColumns)
}

// ValidateUniqueKeysContext performs publication-time key validation without
// retaining an index and permits callers to abandon a maximum-row asset.
func ValidateUniqueKeysContext(ctx context.Context, asset *Asset, keyColumns []string) error {
	if ctx == nil {
		return fmt.Errorf("%w: exact lookup key context is required", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ordinals, err := exactKeyOrdinals(asset, keyColumns)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(asset.rows))
	values := make([]string, len(ordinals))
	for rowOrdinal, row := range asset.rows {
		if rowOrdinal%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		for keyOrdinal, columnOrdinal := range ordinals {
			values[keyOrdinal] = row[columnOrdinal]
		}
		key, err := CanonicalizeExactKey(values)
		if err != nil {
			return fmt.Errorf("validate lookup row %d: %w", rowOrdinal+1, err)
		}
		canonical := string(key.canonical)
		if _, duplicate := seen[canonical]; duplicate {
			return fmt.Errorf("%w: lookup rows share the same key at data row %d", ErrDuplicateKey, rowOrdinal+1)
		}
		seen[canonical] = struct{}{}
	}
	return ctx.Err()
}

func buildExactIndex(asset *Asset, keyColumns []string, hasher exactKeyHasher) (*ExactIndex, error) {
	if hasher == nil {
		return nil, fmt.Errorf("%w: asset and one through %d key columns are required", ErrInvalidArgument, MaximumKeyColumns)
	}
	ordinals, err := exactKeyOrdinals(asset, keyColumns)
	if err != nil {
		return nil, err
	}

	index := &ExactIndex{
		keyColumns: slices.Clone(keyColumns),
		buckets:    make(map[[sha256.Size]byte][]indexedRow, len(asset.rows)),
		hasher:     hasher,
	}
	values := make([]string, len(ordinals))
	for rowOrdinal, row := range asset.rows {
		for keyOrdinal, columnOrdinal := range ordinals {
			values[keyOrdinal] = row[columnOrdinal]
		}
		key, err := canonicalizeExactKey(values, hasher)
		if err != nil {
			return nil, fmt.Errorf("index lookup row %d: %w", rowOrdinal+1, err)
		}
		bucket := index.buckets[key.digest]
		for _, prior := range bucket {
			if bytes.Equal(prior.key, key.canonical) {
				return nil, fmt.Errorf("%w: lookup rows share the same key at data row %d", ErrDuplicateKey, rowOrdinal+1)
			}
		}
		index.buckets[key.digest] = append(bucket, indexedRow{
			key: bytes.Clone(key.canonical),
			row: slices.Clone(row),
		})
	}
	return index, nil
}

func exactKeyOrdinals(asset *Asset, keyColumns []string) ([]int, error) {
	if asset == nil || len(keyColumns) < 1 || len(keyColumns) > MaximumKeyColumns {
		return nil, fmt.Errorf("%w: asset and one through %d key columns are required", ErrInvalidArgument, MaximumKeyColumns)
	}
	ordinals := make([]int, len(keyColumns))
	seenColumns := make(map[string]struct{}, len(keyColumns))
	for index, column := range keyColumns {
		if _, duplicate := seenColumns[column]; duplicate {
			return nil, fmt.Errorf("%w: exact lookup key column %q is repeated", ErrInvalidArgument, column)
		}
		seenColumns[column] = struct{}{}
		ordinal, found := asset.columnOrdinal(column)
		if !found {
			return nil, fmt.Errorf("%w: exact lookup key column %q is absent", ErrInvalidArgument, column)
		}
		ordinals[index] = ordinal
	}
	return ordinals, nil
}

func (index *ExactIndex) KeyColumns() []string {
	if index == nil {
		return nil
	}
	return slices.Clone(index.keyColumns)
}

// Find returns a detached exact row. Hash equality only selects the bounded
// candidate bucket; canonical bytes decide whether a row matched.
func (index *ExactIndex) Find(values []string) ([]string, bool, error) {
	if index == nil || len(values) != len(index.keyColumns) || index.hasher == nil {
		return nil, false, fmt.Errorf("%w: exact lookup key arity does not match the index", ErrInvalidArgument)
	}
	key, err := canonicalizeExactKey(values, index.hasher)
	if err != nil {
		return nil, false, err
	}
	for _, candidate := range index.buckets[key.digest] {
		if bytes.Equal(candidate.key, key.canonical) {
			return slices.Clone(candidate.row), true, nil
		}
	}
	return nil, false, nil
}
