package cursorcodec

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

type testPayload struct {
	Version int    `json:"v"`
	ID      string `json:"i"`
}

func TestRoundTripAndDomainSeparation(t *testing.T) {
	t.Parallel()

	key := []byte("cursor-codec-test-key-32-byte-value")
	token, err := Encode(key, "test-cursor", 1, 1024, testPayload{Version: 1, ID: "item"})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	var decoded testPayload
	if err := Decode(key, "test-cursor", 1, 1024, token, &decoded); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded != (testPayload{Version: 1, ID: "item"}) {
		t.Fatalf("decoded = %+v", decoded)
	}
	for _, mutate := range []func() ([]byte, string, int){
		func() ([]byte, string, int) { return []byte("different-cursor-codec-test-key"), "test-cursor", 1 },
		func() ([]byte, string, int) { return key, "different-cursor", 1 },
		func() ([]byte, string, int) { return key, "test-cursor", 2 },
	} {
		candidateKey, domain, version := mutate()
		if err := Decode(candidateKey, domain, version, 1024, token, &decoded); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("cross-domain Decode() error = %v", err)
		}
	}
}

func TestDecodeRejectsMalformedAndNonCanonicalTokens(t *testing.T) {
	t.Parallel()

	key := []byte("cursor-codec-test-key-32-byte-value")
	domain := "test-cursor"
	valid, err := Encode(key, domain, 1, 1024, testPayload{Version: 1, ID: "item"})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	replacement := byte('A')
	if valid[len(valid)-1] == replacement {
		replacement = 'B'
	}
	tampered := valid[:len(valid)-1] + string(replacement)
	unknownPayload := []byte(`{"v":1,"i":"item","unknown":true}`)
	unknown := base64.RawURLEncoding.EncodeToString(unknownPayload) + "." +
		base64.RawURLEncoding.EncodeToString(sign(key, domain, 1, unknownPayload))
	trailingPayload := []byte(`{"v":1,"i":"item"} {}`)
	trailing := base64.RawURLEncoding.EncodeToString(trailingPayload) + "." +
		base64.RawURLEncoding.EncodeToString(sign(key, domain, 1, trailingPayload))
	parts := strings.SplitN(valid, ".", 2)
	nonCanonical := parts[0] + "=." + parts[1]
	for _, token := range []string{"", "one-part", valid + ".extra", tampered, unknown, trailing, nonCanonical} {
		var decoded testPayload
		if err := Decode(key, domain, 1, 1024, token, &decoded); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("Decode(%q) error = %v", token, err)
		}
	}
	if err := Decode(key, domain, 1, len(valid)-1, valid, &testPayload{}); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("oversized Decode() error = %v", err)
	}
}

func TestEncodeValidatesConfigurationAndSize(t *testing.T) {
	t.Parallel()

	key := []byte("cursor-codec-test-key-32-byte-value")
	for _, test := range []struct {
		key     []byte
		domain  string
		version int
		maximum int
	}{
		{domain: "test", version: 1, maximum: 10},
		{key: key, version: 1, maximum: 10},
		{key: key, domain: "test", maximum: 10},
		{key: key, domain: "test", version: 1},
	} {
		if _, err := Encode(test.key, test.domain, test.version, test.maximum, testPayload{}); err == nil {
			t.Error("Encode(invalid configuration) error = nil")
		}
	}
	if _, err := Encode(key, "test", 1, 1, testPayload{ID: "too large"}); err == nil {
		t.Fatal("Encode(oversized token) error = nil")
	}
}
