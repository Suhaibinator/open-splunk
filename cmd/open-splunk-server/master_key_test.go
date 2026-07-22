package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestLoadOrCreateMasterKeyPersistsPrivateStableKey(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "server.key")
	want := bytes.Repeat([]byte{0x5a}, masterKeyBytes)
	first, err := loadOrCreateMasterKey(path, bytes.NewReader(want))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, want) {
		t.Fatalf("created key = %x, want %x", first, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if permissions := info.Mode().Perm(); permissions != 0o600 {
		t.Fatalf("key permissions = %#o, want 0600", permissions)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := loadOrCreateMasterKey(path, bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(second, want) {
		t.Fatalf("reopened key = %x, want %x", second, want)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if permissions := info.Mode().Perm(); permissions != 0o600 {
		t.Fatalf("resecured key permissions = %#o, want 0600", permissions)
	}
}

func TestLoadOrCreateMasterKeyRejectsUnsafeOrCorruptFiles(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	short := filepath.Join(directory, "short.key")
	if err := os.WriteFile(short, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateMasterKey(short, nil); err == nil {
		t.Fatal("short master key was accepted")
	}
	long := filepath.Join(directory, "long.key")
	if err := os.WriteFile(long, bytes.Repeat([]byte{1}, masterKeyBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateMasterKey(long, nil); err == nil {
		t.Fatal("oversized master key was accepted")
	}
	target := filepath.Join(directory, "target.key")
	if err := os.WriteFile(target, bytes.Repeat([]byte{1}, masterKeyBytes), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link.key")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateMasterKey(link, nil); err == nil {
		t.Fatal("symlink master key was accepted")
	}
}

func TestDeriveServerKeySeparatesPurposes(t *testing.T) {
	t.Parallel()
	master := bytes.Repeat([]byte{7}, masterKeyBytes)
	first, err := deriveServerKey(master, "saved-search-cursors")
	if err != nil {
		t.Fatal(err)
	}
	second, err := deriveServerKey(master, "collector-token-digests")
	if err != nil {
		t.Fatal(err)
	}
	firstAgain, err := deriveServerKey(master, "saved-search-cursors")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) || !bytes.Equal(first, firstAgain) || len(first) != 32 {
		t.Fatalf("derived keys do not provide deterministic purpose separation")
	}
}

func TestOpenSecurityStoresKeepsCollectorCredentialsValidAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	db, err := control.Open(ctx, filepath.Join(directory, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.CreateIndex(ctx, control.IndexDefinition{
		Name: "main", RetentionPeriod: 30 * 24 * time.Hour,
		IngestionEnabled: true, SearchEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(directory, "server.key")
	_, firstTokens, err := openSecurityStores(ctx, db, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := firstTokens.CreateCollectorToken(ctx, auth.CreateCollectorTokenRequest{
		Name: "restart test", AllowedIndexNames: []string{"main"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, secondTokens, err := openSecurityStores(ctx, db, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	authentication, err := secondTokens.Authenticate(ctx, issued.Secret.Plaintext())
	if err != nil {
		t.Fatal(err)
	}
	if authentication.TokenID != issued.Token.ID || len(authentication.AllowedIndexNames) != 1 || authentication.AllowedIndexNames[0] != "main" {
		t.Fatalf("authentication after reopen = %+v", authentication)
	}
	if err := os.Remove(keyPath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openSecurityStores(ctx, db, keyPath); err == nil {
		t.Fatal("missing registered master key was silently replaced")
	}
	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Fatalf("failed reopen recreated registered key: %v", err)
	}
	if err := os.WriteFile(keyPath, bytes.Repeat([]byte{0x33}, masterKeyBytes), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openSecurityStores(ctx, db, keyPath); err == nil {
		t.Fatal("replacement master key was accepted for registered database")
	}
}

func TestOpenSecurityStoresRefusesUnverifiableExistingTokens(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	db, err := control.Open(ctx, filepath.Join(directory, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.CreateIndex(ctx, control.IndexDefinition{
		Name: "main", IngestionEnabled: true, RetentionPeriod: 24 * time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	legacyTokens, err := auth.NewStore(db, bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacyTokens.CreateCollectorToken(ctx, auth.CreateCollectorTokenRequest{
		Name: "legacy", AllowedIndexNames: []string{"main"},
	}); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(directory, "server.key")
	if _, _, err := openSecurityStores(ctx, db, keyPath); err == nil {
		t.Fatal("unbound database with existing token was accepted")
	}
	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Fatalf("refused registration created a key anyway: %v", err)
	}
}
