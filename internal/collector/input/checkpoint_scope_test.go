package input

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckpointStoreScopesPhysicalIdentityByInputID(t *testing.T) {
	t.Parallel()
	store, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	identity := canonicalIdentityForTest(7, 9, 1, "ab", 64)
	if err := store.Set(Checkpoint{
		InputID: "input-a", Identity: identity, Path: "/logs/shared.log", Offset: 100,
	}); err != nil {
		t.Fatalf("Set input-a: %v", err)
	}
	if err := store.Set(Checkpoint{
		InputID: "input-b", Identity: identity, Path: "/logs/shared.log", Offset: 20,
	}); err != nil {
		t.Fatalf("Set input-b: %v", err)
	}

	for inputID, wantOffset := range map[string]uint64{
		"input-a": 100,
		"input-b": 20,
	} {
		checkpoint, ok, getErr := store.Get(inputID, identity)
		if getErr != nil || !ok || checkpoint.InputID != inputID ||
			checkpoint.Offset != wantOffset {
			t.Fatalf(
				"Get(%q) = (%+v, %t, %v), want offset %d",
				inputID,
				checkpoint,
				ok,
				getErr,
				wantOffset,
			)
		}
	}

	if err := store.Delete("input-a", identity); err != nil {
		t.Fatalf("Delete input-a: %v", err)
	}
	if _, ok, getErr := store.Get("input-a", identity); getErr != nil || ok {
		t.Fatalf("Get deleted input-a: ok=%t err=%v", ok, getErr)
	}
	checkpoint, ok, err := store.Get("input-b", identity)
	if err != nil || !ok || checkpoint.Offset != 20 {
		t.Fatalf("input-b after deleting input-a = (%+v, %t, %v)", checkpoint, ok, err)
	}
}

func TestCheckpointStoreRoundTripAndDeterministicOrder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}

	first := canonicalIdentityForTest(1, 1, 1, "ab", 64)
	second := canonicalIdentityForTest(1, 2, 1, "cd", 64)
	if err := store.SetMany([]Checkpoint{
		{InputID: "input-b", Identity: first, Path: "/logs/first.log", Offset: 30},
		{InputID: "input-a", Identity: second, Path: "/logs/second.log", Offset: 20},
		{InputID: "input-a", Identity: first, Path: "/logs/first.log", Offset: 10},
	}); err != nil {
		t.Fatalf("SetMany: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, checkpointFileName))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var document checkpointDoc
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if document.Version != checkpointFormatVersion {
		t.Fatalf("checkpoint version = %d, want %d", document.Version, checkpointFormatVersion)
	}
	if len(document.Checkpoints) != 3 {
		t.Fatalf("checkpoint count = %d, want 3", len(document.Checkpoints))
	}
	gotOrder := []string{
		document.Checkpoints[0].InputID + "/" + document.Checkpoints[0].Identity.TrackingKey(),
		document.Checkpoints[1].InputID + "/" + document.Checkpoints[1].Identity.TrackingKey(),
		document.Checkpoints[2].InputID + "/" + document.Checkpoints[2].Identity.TrackingKey(),
	}
	wantOrder := []string{
		"input-a/" + first.TrackingKey(),
		"input-a/" + second.TrackingKey(),
		"input-b/" + first.TrackingKey(),
	}
	for index := range wantOrder {
		if gotOrder[index] != wantOrder[index] {
			t.Fatalf("checkpoint order = %v, want %v", gotOrder, wantOrder)
		}
	}

	reopened, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	for inputID, wantOffset := range map[string]uint64{
		"input-a": 10,
		"input-b": 30,
	} {
		checkpoint, ok, getErr := reopened.Get(inputID, first)
		if getErr != nil || !ok || checkpoint.Offset != wantOffset {
			t.Fatalf(
				"reopened Get(%q) = (%+v, %t, %v), want offset %d",
				inputID,
				checkpoint,
				ok,
				getErr,
				wantOffset,
			)
		}
	}
}

func TestCheckpointStoreAcceptsSupportedFormatVersions(t *testing.T) {
	t.Parallel()
	identity := canonicalIdentityForTest(1, 2, 1, "ab", 64)

	t.Run("current version", func(t *testing.T) {
		dir := t.TempDir()
		writeCheckpointDocument(t, dir, checkpointDoc{
			Version: checkpointFormatVersion,
			Checkpoints: []Checkpoint{{
				InputID: "input-a", Identity: identity, Path: "/logs/app.log", Offset: 10,
			}},
		})
		if err := os.WriteFile(filepath.Join(dir, checkpointJournalName), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		store, err := NewCheckpointStore(dir)
		if err != nil {
			t.Fatalf("NewCheckpointStore: %v", err)
		}
		defer store.Close()
	})

	t.Run("unsupported version", func(t *testing.T) {
		dir := t.TempDir()
		writeCheckpointDocument(t, dir, checkpointDoc{Version: checkpointFormatVersion + 1})
		_, err := NewCheckpointStore(dir)
		if err == nil || !strings.Contains(err.Error(), "unsupported version 3") ||
			!strings.Contains(err.Error(), "fresh collector state") {
			t.Fatalf("NewCheckpointStore error = %v, want explicit fresh-state rejection", err)
		}
	})
}

func TestCheckpointStoreRejectsInvalidInputIDs(t *testing.T) {
	t.Parallel()
	identity := canonicalIdentityForTest(1, 2, 1, "ab", 64)

	for _, inputID := range []string{
		"",
		" leading-space",
		"bad/input",
		strings.Repeat("a", 129),
	} {
		t.Run(inputID, func(t *testing.T) {
			store, err := NewCheckpointStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewCheckpointStore: %v", err)
			}
			defer store.Close()

			if err := store.Set(Checkpoint{
				InputID: inputID, Identity: identity, Path: "/logs/app.log",
			}); err == nil {
				t.Fatalf("Set InputID %q error = nil", inputID)
			}
			if _, _, err := store.Get(inputID, identity); err == nil {
				t.Fatalf("Get InputID %q error = nil", inputID)
			}
			if err := store.Delete(inputID, identity); err == nil {
				t.Fatalf("Delete InputID %q error = nil", inputID)
			}
		})
	}
}

func TestCheckpointStoreRejectsInvalidInputIDOnLoad(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCheckpointDocument(t, dir, checkpointDoc{
		Version: checkpointFormatVersion,
		Checkpoints: []Checkpoint{{
			Identity: canonicalIdentityForTest(1, 2, 1, "ab", 64),
			Path:     "/logs/app.log",
		}},
	})
	_, err := NewCheckpointStore(dir)
	if err == nil || !strings.Contains(err.Error(), "input ID") {
		t.Fatalf("NewCheckpointStore error = %v, want invalid input ID", err)
	}
}

func TestCheckpointStoreRejectsHostileCurrentState(t *testing.T) {
	t.Parallel()
	for _, test := range hostileCheckpointMutations() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			checkpoint := validCheckpointForTest(
				"input-a",
				canonicalIdentityForTest(11, 13, 1, "ab", 64),
				"/logs/app.log",
				64,
			)
			test.mutate(&checkpoint)
			dir := t.TempDir()
			writeCheckpointDocument(t, dir, checkpointDoc{
				Version:     checkpointFormatVersion,
				Checkpoints: []Checkpoint{checkpoint},
			})
			if _, err := NewCheckpointStore(dir); err == nil {
				t.Fatalf("NewCheckpointStore accepted hostile checkpoint: %+v", checkpoint)
			}
		})
	}
}

func TestCheckpointStoreSetManyRejectsHostileBatchAtomically(t *testing.T) {
	t.Parallel()
	for _, test := range hostileCheckpointMutations() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			storeAPI, err := NewCheckpointStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewCheckpointStore: %v", err)
			}
			defer storeAPI.Close()
			store := storeAPI.(*fileCheckpointStore)

			existingIdentity := canonicalIdentityForTest(21, 23, 1, "cd", 64)
			existing := validCheckpointForTest(
				"input-a",
				existingIdentity,
				"/logs/existing.log",
				10,
			)
			if err := storeAPI.Set(existing); err != nil {
				t.Fatalf("seed existing checkpoint: %v", err)
			}

			persistCalls := 0
			store.persistUpdates = func([]Checkpoint) error {
				persistCalls++
				return nil
			}
			advance := existing
			advance.Offset = 20
			advance.LineNumber = 2
			advance.NextLineNumber = 3
			newIdentity := canonicalIdentityForTest(31, 37, 1, "ef", 64)
			hostile := validCheckpointForTest(
				"input-b",
				newIdentity,
				"/logs/new.log",
				30,
			)
			test.mutate(&hostile)

			if err := storeAPI.SetMany([]Checkpoint{advance, hostile}); err == nil {
				t.Fatalf("SetMany accepted hostile checkpoint: %+v", hostile)
			}
			if persistCalls != 0 {
				t.Fatalf("persistence calls = %d, want zero", persistCalls)
			}
			got, ok, err := storeAPI.Get(existing.InputID, existingIdentity)
			if err != nil || !ok || got.Offset != existing.Offset {
				t.Fatalf(
					"existing checkpoint after rejection = (%+v, %t, %v), want offset %d",
					got,
					ok,
					err,
					existing.Offset,
				)
			}
			if _, ok, err := storeAPI.Get("input-b", newIdentity); err != nil || ok {
				t.Fatalf("hostile checkpoint published after rejection: ok=%t err=%v", ok, err)
			}
		})
	}
}

func TestValidInputID(t *testing.T) {
	t.Parallel()
	for _, inputID := range []string{
		"a",
		"A0._:-",
		strings.Repeat("a", maximumCheckpointInputIDBytes),
	} {
		if !ValidInputID(inputID) {
			t.Errorf("ValidInputID(%q) = false, want true", inputID)
		}
	}
	for _, inputID := range []string{
		"",
		"-leading",
		"bad/input",
		"nonascii-\u00e9",
		strings.Repeat("a", maximumCheckpointInputIDBytes+1),
	} {
		if ValidInputID(inputID) {
			t.Errorf("ValidInputID(%q) = true, want false", inputID)
		}
	}
}

func hostileCheckpointMutations() []struct {
	name   string
	mutate func(*Checkpoint)
} {
	return []struct {
		name   string
		mutate func(*Checkpoint)
	}{
		{
			name: "four-gib fingerprint",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.Identity.FingerprintLength = ^uint32(0)
			},
		},
		{
			name: "zero-length fingerprint at nonzero offset",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.Identity.Fingerprint = emptyFingerprintSHA256
				checkpoint.Identity.FingerprintLength = 0
			},
		},
		{
			name: "zero generation",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.Identity.Generation = 0
			},
		},
		{
			name: "noncanonical fingerprint",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.Identity.Fingerprint = strings.Repeat("GG", 32)
			},
		},
		{
			name: "empty path",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.Path = ""
			},
		},
		{
			name: "offset beyond int64",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.Offset = uint64(math.MaxInt64) + 1
			},
		},
		{
			name: "fingerprint without rewrite guard length",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.GuardFingerprint = strings.Repeat("ab", 32)
			},
		},
		{
			name: "rewrite guard without fingerprint",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.GuardLength = 1
			},
		},
		{
			name: "rewrite guard beyond offset",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.GuardLength = uint32(checkpoint.Offset + 1)
				checkpoint.GuardFingerprint = strings.Repeat("ab", 32)
			},
		},
		{
			name: "rewrite guard beyond absolute maximum",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.Offset = maximumFingerprintBytes + 1
				checkpoint.GuardLength = maximumFingerprintBytes + 1
				checkpoint.GuardFingerprint = strings.Repeat("ab", 32)
			},
		},
		{
			name: "uppercase rewrite guard fingerprint",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.GuardLength = 1
				checkpoint.GuardFingerprint = strings.Repeat("AB", 32)
			},
		},
		{
			name: "nonhex rewrite guard fingerprint",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.GuardLength = 1
				checkpoint.GuardFingerprint = strings.Repeat("gg", 32)
			},
		},
		{
			name: "nonadvancing next line",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.LineNumber = 4
				checkpoint.NextLineNumber = 4
			},
		},
		{
			name: "unanchored advanced next line",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.LineNumber = 0
				checkpoint.NextLineNumber = 2
			},
		},
		{
			name: "exhausted next line",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.LineNumber = math.MaxUint64 - 1
				checkpoint.NextLineNumber = math.MaxUint64
			},
		},
		{
			name: "exhausted legacy line",
			mutate: func(checkpoint *Checkpoint) {
				checkpoint.LineNumber = math.MaxUint64 - 1
				checkpoint.NextLineNumber = 0
			},
		},
	}
}

func canonicalIdentityForTest(
	device, inode, generation uint64,
	fingerprintPair string,
	fingerprintLength uint32,
) FileIdentity {
	return FileIdentity{
		Device:            device,
		Inode:             inode,
		Generation:        generation,
		Fingerprint:       strings.Repeat(fingerprintPair, 32),
		FingerprintLength: fingerprintLength,
	}
}

func validCheckpointForTest(
	inputID string,
	identity FileIdentity,
	path string,
	offset uint64,
) Checkpoint {
	return Checkpoint{
		InputID:        inputID,
		Identity:       identity,
		Path:           path,
		Offset:         offset,
		LineNumber:     1,
		NextLineNumber: 2,
	}
}

func TestManagerResolveStartScopesCheckpointByConfiguredInputID(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.log")
	const contents = "historical\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	identity, err := identityFor(file, info, defaultFingerprintBytes)
	if err != nil {
		t.Fatalf("identityFor: %v", err)
	}
	store, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	first := &manager{
		cfg: Config{InputID: "input-a", StartAt: StartAtEnd}, checkpoints: store,
	}
	firstStart, err := first.resolveStart(identity, path, uint64(len(contents)), true, file)
	if err != nil {
		t.Fatalf("input-a resolveStart: %v", err)
	}
	if firstStart.offset != uint64(len(contents)) {
		t.Fatalf("input-a offset = %d, want EOF %d", firstStart.offset, len(contents))
	}

	second := &manager{
		cfg: Config{InputID: "input-b", StartAt: StartAtBeginning}, checkpoints: store,
	}
	secondStart, err := second.resolveStart(identity, path, uint64(len(contents)), true, file)
	if err != nil {
		t.Fatalf("input-b resolveStart: %v", err)
	}
	if secondStart.offset != 0 {
		t.Fatalf("input-b offset = %d, want beginning", secondStart.offset)
	}

	checkpoints, err := store.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(checkpoints) != 2 ||
		checkpoints[0].InputID != "input-a" ||
		checkpoints[1].InputID != "input-b" {
		t.Fatalf("manager checkpoints = %+v, want input-a and input-b", checkpoints)
	}
}

func TestNewManagerRejectsInvalidInputID(t *testing.T) {
	t.Parallel()
	store, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for _, inputID := range []string{"", "bad/input", strings.Repeat("a", 129)} {
		if _, err := NewManager(Config{
			InputID: inputID,
			Include: []string{filepath.Join(t.TempDir(), "*.log")},
		}, store); err == nil {
			t.Fatalf("NewManager InputID %q error = nil", inputID)
		}
	}
}

func writeCheckpointDocument(t *testing.T, dir string, document checkpointDoc) {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, checkpointFileName), data, 0o600); err != nil {
		t.Fatalf("WriteFile checkpoint: %v", err)
	}
}
