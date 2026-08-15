package lookupasset

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestCanonicalizeExactKeyIsUnambiguousAndDetached(t *testing.T) {
	left, err := CanonicalizeExactKey([]string{"a", "bc"})
	if err != nil {
		t.Fatalf("canonicalize left: %v", err)
	}
	right, err := CanonicalizeExactKey([]string{"ab", "c"})
	if err != nil {
		t.Fatalf("canonicalize right: %v", err)
	}
	if bytes.Equal(left.Bytes(), right.Bytes()) || left.SHA256() == right.SHA256() {
		t.Fatal("component boundaries were ambiguous")
	}
	empty, err := CanonicalizeExactKey([]string{""})
	if err != nil {
		t.Fatalf("canonicalize empty string: %v", err)
	}
	if len(empty.Bytes()) == 0 {
		t.Fatal("empty string did not produce a framed exact key")
	}
	mutated := left.Bytes()
	mutated[0] ^= 0xff
	if bytes.Equal(mutated, left.Bytes()) {
		t.Fatal("ExactKey.Bytes exposed mutable authoritative storage")
	}
}

func TestCanonicalizeExactKeyRejectsInvalidShape(t *testing.T) {
	tests := [][]string{
		nil,
		{},
		{"a", "b", "c", "d", "e"},
		{string([]byte{0xff})},
		{strings.Repeat("x", MaximumCellBytes+1)},
	}
	for _, values := range tests {
		if _, err := CanonicalizeExactKey(values); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("CanonicalizeExactKey(%q) error = %v", values, err)
		}
	}
}

func TestExactKeyOrdinalsRejectKeyDefinitionAndDuplicateRows(t *testing.T) {
	asset := mustParseAsset(t, "key,other\na,1\na,2\n")
	tests := [][]string{nil, {}, {"missing"}, {"key", "key"}}
	for _, columns := range tests {
		if err := ValidateUniqueKeysContext(t.Context(), asset, columns); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("ValidateUniqueKeysContext(%q) error = %v", columns, err)
		}
	}
	if err := ValidateUniqueKeysContext(t.Context(), asset, []string{"key"}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate key error = %v", err)
	}
}

func TestValidateUniqueKeysContextRejectsNilAndCanceledContexts(t *testing.T) {
	asset := mustParseAsset(t, "key\na\n")
	//nolint:staticcheck // The nil context is the invalid input under test.
	if err := ValidateUniqueKeysContext(nil, asset, []string{"key"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil context error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ValidateUniqueKeysContext(ctx, asset, []string{"key"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
}

func mustParseAsset(t *testing.T, source string) *Asset {
	t.Helper()
	asset, err := ParseCSV(strings.NewReader(source), Limits{})
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	return asset
}
