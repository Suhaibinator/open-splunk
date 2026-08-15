package lookupasset

import (
	"bytes"
	"context"
	"crypto/sha256"
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

func TestBuildExactIndexFindsCompositeAndEmptyKeys(t *testing.T) {
	asset := mustParseAsset(t, "region,service,owner\nwest,api,alice\nwest,,empty-owner\neast,api,bob\n")
	index, err := BuildExactIndex(asset, []string{"region", "service"})
	if err != nil {
		t.Fatalf("build exact index: %v", err)
	}
	if got := index.KeyColumns(); !equalStrings(got, []string{"region", "service"}) {
		t.Fatalf("key columns = %#v", got)
	}
	row, found, err := index.Find([]string{"west", ""})
	if err != nil || !found || row[2] != "empty-owner" {
		t.Fatalf("empty exact key lookup = %#v, %v, %v", row, found, err)
	}
	row[2] = "changed"
	again, found, err := index.Find([]string{"west", ""})
	if err != nil || !found || again[2] != "empty-owner" {
		t.Fatal("Find exposed mutable index storage")
	}
	if row, found, err := index.Find([]string{"north", "api"}); err != nil || found || row != nil {
		t.Fatalf("non-match = %#v, %v, %v", row, found, err)
	}
	if _, _, err := index.Find([]string{"west"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("arity error = %v", err)
	}
}

func TestBuildExactIndexRejectsKeyDefinitionAndDuplicateRows(t *testing.T) {
	asset := mustParseAsset(t, "key,other\na,1\na,2\n")
	tests := [][]string{nil, {}, {"missing"}, {"key", "key"}}
	for _, columns := range tests {
		if _, err := BuildExactIndex(asset, columns); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("BuildExactIndex(%q) error = %v", columns, err)
		}
	}
	if err := ValidateUniqueKeys(asset, []string{"key"}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate key error = %v", err)
	}
}

func TestValidateUniqueKeysContextRejectsNilAndCanceledContexts(t *testing.T) {
	asset := mustParseAsset(t, "key\na\n")
	if err := ValidateUniqueKeysContext(nil, asset, []string{"key"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil context error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ValidateUniqueKeysContext(ctx, asset, []string{"key"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
}

func TestExactIndexHashCollisionStillUsesAuthoritativeBytes(t *testing.T) {
	asset := mustParseAsset(t, "key,value\na,one\nb,two\n")
	constantHash := func([]byte) [sha256.Size]byte { return [sha256.Size]byte{1} }
	index, err := buildExactIndex(asset, []string{"key"}, constantHash)
	if err != nil {
		t.Fatalf("build collision index: %v", err)
	}
	for key, want := range map[string]string{"a": "one", "b": "two"} {
		row, found, err := index.Find([]string{key})
		if err != nil || !found || row[1] != want {
			t.Fatalf("collision lookup %q = %#v, %v, %v", key, row, found, err)
		}
	}
	if row, found, err := index.Find([]string{"c"}); err != nil || found || row != nil {
		t.Fatalf("collision non-match = %#v, %v, %v", row, found, err)
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

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
