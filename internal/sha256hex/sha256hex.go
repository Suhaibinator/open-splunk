// Package sha256hex validates canonical hexadecimal SHA-256 digests without
// allocating a decoded byte slice.
package sha256hex

import "crypto/sha256"

// EncodedSize is the number of bytes in a hexadecimal SHA-256 digest.
const EncodedSize = sha256.Size * 2

// Valid reports whether value is exactly one lowercase hexadecimal SHA-256
// digest. Canonical validation needs no decoding because every pair of valid
// lowercase hexadecimal digits maps to exactly one digest byte.
func Valid(value string) bool {
	if len(value) != EncodedSize {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if (character < '0' || character > '9') &&
			(character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
