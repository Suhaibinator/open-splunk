package auth

import (
	"crypto/sha256"
	"fmt"
)

// ServerMasterKeyBytes is the exact external key size bound to a control
// database. Keeping the size beside the fingerprint contract prevents backup,
// restore, and runtime startup from drifting independently.
const ServerMasterKeyBytes = 32

// FingerprintServerMasterKey derives the non-secret identity persisted in the
// control plane. The domain separator prevents this digest from being reused
// as an ordinary SHA-256 of the key bytes.
func FingerprintServerMasterKey(key []byte) ([sha256.Size]byte, error) {
	if len(key) != ServerMasterKeyBytes {
		return [sha256.Size]byte{}, fmt.Errorf(
			"server master key must contain exactly %d bytes",
			ServerMasterKeyBytes,
		)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("open-splunk/server-master-key-fingerprint/v1\x00"))
	_, _ = hash.Write(key)
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}
