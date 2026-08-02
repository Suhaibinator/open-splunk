package auth

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestFingerprintServerMasterKeyIsValidatedAndDomainSeparated(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x5a}, ServerMasterKeyBytes)
	fingerprint, err := FingerprintServerMasterKey(key)
	if err != nil {
		t.Fatal(err)
	}
	const want = "a3c034bd2dc441d9917aae485a53f9388db932d5dd0498584126756f68a9c1ad"
	if got := hex.EncodeToString(fingerprint[:]); got != want {
		t.Fatalf("fingerprint = %s, want %s", got, want)
	}

	key[0] ^= 0xff
	changed, err := FingerprintServerMasterKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if changed == fingerprint {
		t.Fatal("different master keys produced the same test fingerprint")
	}

	for _, invalid := range [][]byte{nil, make([]byte, ServerMasterKeyBytes-1), make([]byte, ServerMasterKeyBytes+1)} {
		if _, err := FingerprintServerMasterKey(invalid); err == nil {
			t.Fatalf("FingerprintServerMasterKey(%d bytes) succeeded", len(invalid))
		}
	}
}
