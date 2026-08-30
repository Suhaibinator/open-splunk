package alerts

import (
	"bytes"
	"testing"
)

func TestAESGCMCipherRoundTripAndContextBinding(t *testing.T) {
	t.Parallel()
	randomBytes := append(bytes.Repeat([]byte{3}, 12), bytes.Repeat([]byte{4}, 12)...)
	cipher, err := NewAESGCMCipher(bytes.Repeat([]byte{7}, 32), bytes.NewReader(randomBytes))
	if err != nil {
		t.Fatalf("NewAESGCMCipher() error = %v", err)
	}
	secretContext := SecretContext{AlertID: "alert-1", Purpose: PurposeEndpoint, Generation: 1}
	first, err := cipher.Encrypt(secretContext, []byte("https://hooks.example.test/alert"))
	if err != nil {
		t.Fatalf("Encrypt() error = %v", err)
	}
	second, err := cipher.Encrypt(secretContext, []byte("https://hooks.example.test/alert"))
	if err != nil {
		t.Fatalf("second Encrypt() error = %v", err)
	}
	if bytes.Equal(first.Nonce, second.Nonce) {
		t.Fatal("Encrypt() reused a nonce")
	}
	plaintext, err := cipher.Decrypt(secretContext, first)
	if err != nil {
		t.Fatalf("Decrypt() error = %v", err)
	}
	if string(plaintext) != "https://hooks.example.test/alert" {
		t.Fatalf("plaintext = %q", plaintext)
	}
	wrongContext := secretContext
	wrongContext.Generation = 2
	if _, err := cipher.Decrypt(wrongContext, first); err == nil {
		t.Fatal("Decrypt() accepted ciphertext under a different generation")
	}
	tampered := first
	tampered.Ciphertext = append([]byte(nil), first.Ciphertext...)
	tampered.Ciphertext[0] ^= 1
	if _, err := cipher.Decrypt(secretContext, tampered); err == nil {
		t.Fatal("Decrypt() accepted tampered ciphertext")
	}
}

func TestCipherAndSecretValidation(t *testing.T) {
	t.Parallel()
	if _, err := NewAESGCMCipher(make([]byte, 31), nil); err == nil {
		t.Fatal("NewAESGCMCipher() accepted a non-AES-256 key")
	}
	if _, err := EncodeIssuedSecret(make([]byte, SecretBytes-1)); err == nil {
		t.Fatal("EncodeIssuedSecret() accepted the wrong length")
	}
	generated, err := (RandomSecretGenerator{Random: bytes.NewReader(make([]byte, SecretBytes))}).Generate()
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(generated) != SecretBytes {
		t.Fatalf("secret length = %d", len(generated))
	}
}
