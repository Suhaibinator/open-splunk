//go:build darwin || linux

package privatefs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestStableRegularFileWriteInspectAndRetain(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	contents := []byte("stable private member")
	written, err := WriteStableRegularFile(
		t.Context(),
		directory,
		"member",
		contents,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest := sha256.Sum256(contents)
	if written.SizeBytes != uint64(len(contents)) || written.SHA256 != wantDigest ||
		len(written.Contents) != 0 {
		t.Fatalf("written inspection = %#v", written)
	}
	info, err := os.Lstat(filepath.Join(path, "member"))
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("written member mode = %v", info.Mode())
	}

	retained, err := InspectStableRegularFile(
		t.Context(),
		directory,
		"member",
		FilePolicy{
			AllowedModes: []fs.FileMode{0o600},
			MinimumSize:  int64(len(contents)),
			MaximumSize:  int64(len(contents)),
		},
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if retained.SizeBytes != uint64(len(contents)) || retained.SHA256 != wantDigest ||
		!bytes.Equal(retained.Contents, contents) {
		t.Fatalf("retained inspection = %#v", retained)
	}
}

func TestCopyBufferWithContextStopsAtCancellationBoundary(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	source := &cancelAfterFirstRead{
		reader: bytes.NewReader([]byte("payload")),
		cancel: cancel,
	}
	var destination writeOnlyBuffer
	read, err := CopyBufferWithContext(
		ctx,
		&destination,
		source,
		make([]byte, 3),
	)
	if read != 3 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled copy = (%d, %v), want (3, context.Canceled)", read, err)
	}
	if got := destination.buffer.String(); got != "pay" {
		t.Fatalf("canceled copy contents = %q, want %q", got, "pay")
	}
}

func TestStableRegularFileRejectsUnsafeMetadata(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	policy := FilePolicy{
		AllowedModes: []fs.FileMode{0o600},
		MinimumSize:  1,
		MaximumSize:  16,
	}
	target := filepath.Join(path, "target")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(path, "symlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(target, filepath.Join(path, "hardlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "wide"), []byte("wide"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"target", "symlink", "hardlink", "wide"} {
		if _, err := InspectStableRegularFile(
			t.Context(),
			directory,
			name,
			policy,
			false,
		); err == nil {
			t.Errorf("InspectStableRegularFile(%q) accepted unsafe metadata", name)
		}
	}
	if _, err := WriteStableRegularFile(
		t.Context(),
		directory,
		"wide",
		[]byte("replacement"),
	); err == nil {
		t.Fatal("WriteStableRegularFile replaced an existing unsafe member")
	}
	if got, err := os.ReadFile(filepath.Join(path, "wide")); err != nil || string(got) != "wide" {
		t.Fatalf("existing member changed: contents=%q err=%v", got, err)
	}
}

func TestStableRegularFileCleanupRejectsConformingPathReplacement(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	contents := []byte("stable private member")
	memberPath := filepath.Join(path, "member")
	originalPath := filepath.Join(path, "member.original")
	_, err := writeStableRegularFileWithHooks(
		t.Context(),
		directory,
		"member",
		contents,
		writeStableRegularFileHooks{
			afterWriterClose: func() {
				if renameErr := os.Rename(memberPath, originalPath); renameErr != nil {
					t.Fatal(renameErr)
				}
				if writeErr := os.WriteFile(memberPath, contents, 0o600); writeErr != nil {
					t.Fatal(writeErr)
				}
			},
		},
	)
	if err == nil {
		t.Fatal("stable write accepted a content-conforming path replacement")
	}
	for _, candidate := range []string{memberPath, originalPath} {
		got, readErr := os.ReadFile(candidate)
		if readErr != nil || !bytes.Equal(got, contents) {
			t.Fatalf("preserved candidate %q = %q, error=%v", candidate, got, readErr)
		}
	}
}

func TestStableRegularFileCleanupOpenRejectsConformingPathReplacement(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	contents := []byte("stable private member")
	memberPath := filepath.Join(path, "member")
	originalPath := filepath.Join(path, "member.original")
	var hookErr error
	_, err := writeStableRegularFileWithHooks(
		t.Context(),
		directory,
		"member",
		contents,
		writeStableRegularFileHooks{
			beforeCleanupOpen: func() {
				hookErr = os.Rename(memberPath, originalPath)
				if hookErr != nil {
					return
				}
				hookErr = os.WriteFile(memberPath, contents, 0o600)
			},
		},
	)
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if err == nil {
		t.Fatal("stable write trusted a conforming replacement as its cleanup descriptor")
	}
	for _, candidate := range []string{memberPath, originalPath} {
		got, readErr := os.ReadFile(candidate)
		if readErr != nil || !bytes.Equal(got, contents) {
			t.Fatalf("preserved candidate %q = %q, error=%v", candidate, got, readErr)
		}
	}
}

type cancelAfterFirstRead struct {
	reader *bytes.Reader
	cancel context.CancelFunc
	read   bool
}

type writeOnlyBuffer struct {
	buffer bytes.Buffer
}

func (writer *writeOnlyBuffer) Write(buffer []byte) (int, error) {
	return writer.buffer.Write(buffer)
}

func (reader *cancelAfterFirstRead) Read(buffer []byte) (int, error) {
	read, err := reader.reader.Read(buffer)
	if !reader.read {
		reader.read = true
		reader.cancel()
	}
	return read, err
}
