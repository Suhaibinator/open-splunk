//go:build darwin || linux

package privatefs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"

	"fortio.org/safecast"
)

const stableRegularFileBufferBytes = 64 << 10

var stableRegularFileModes = []fs.FileMode{0o600}

// StableRegularFileInspection is the identity-neutral result of reading one
// exact private regular file through a pinned directory. Contents is populated
// only when the caller requests retention.
type StableRegularFileInspection struct {
	SizeBytes uint64
	SHA256    [sha256.Size]byte
	Contents  []byte
}

// InspectStableRegularFile opens, reads, hashes, closes, reopens, and
// revalidates one exact private file. Metadata and pathname identity must stay
// stable across the complete read, including the final directory revalidation.
func InspectStableRegularFile(
	ctx context.Context,
	directory *Directory,
	name string,
	policy FilePolicy,
	retainContents bool,
) (StableRegularFileInspection, error) {
	if ctx == nil {
		return StableRegularFileInspection{}, errors.New(
			"inspect stable private file: context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return StableRegularFileInspection{}, err
	}
	file, err := directory.OpenRegular(name, policy)
	if err != nil {
		return StableRegularFileInspection{}, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()

	before, err := file.Stat()
	if err != nil {
		return StableRegularFileInspection{}, fmt.Errorf(
			"inspect stable private file %q: %w",
			name,
			err,
		)
	}
	hash := sha256.New()
	var retained bytes.Buffer
	var writer io.Writer = hash
	if retainContents {
		writer = io.MultiWriter(hash, &retained)
	}
	limit := policy.MaximumSize
	if limit < math.MaxInt64 {
		limit++
	}
	read, err := CopyBufferWithContext(
		ctx,
		writer,
		io.LimitReader(file, limit),
		make([]byte, stableRegularFileBufferBytes),
	)
	if err != nil {
		return StableRegularFileInspection{}, fmt.Errorf(
			"read stable private file %q: %w",
			name,
			err,
		)
	}
	if read < policy.MinimumSize || read > policy.MaximumSize || read != before.Size() {
		return StableRegularFileInspection{}, fmt.Errorf(
			"stable private file %q changed while it was read",
			name,
		)
	}
	after, err := file.Stat()
	if err != nil {
		return StableRegularFileInspection{}, fmt.Errorf(
			"reinspect stable private file %q: %w",
			name,
			err,
		)
	}
	if !sameStableFileState(before, after) {
		return StableRegularFileInspection{}, fmt.Errorf(
			"stable private file %q changed while it was read",
			name,
		)
	}
	if err := ctx.Err(); err != nil {
		return StableRegularFileInspection{}, err
	}
	if err := file.Close(); err != nil {
		return StableRegularFileInspection{}, fmt.Errorf(
			"close stable private file %q: %w",
			name,
			err,
		)
	}
	closed = true

	reopened, err := directory.OpenRegular(name, policy)
	if err != nil {
		return StableRegularFileInspection{}, fmt.Errorf(
			"reopen stable private file %q: %w",
			name,
			err,
		)
	}
	reopenedInfo, statErr := reopened.Stat()
	closeErr := reopened.Close()
	if err := errors.Join(statErr, closeErr); err != nil {
		return StableRegularFileInspection{}, fmt.Errorf(
			"reinspect stable private file %q: %w",
			name,
			err,
		)
	}
	if !sameStableFileState(before, reopenedInfo) {
		return StableRegularFileInspection{}, fmt.Errorf(
			"stable private file %q changed after it was read",
			name,
		)
	}
	if err := directory.Revalidate(); err != nil {
		return StableRegularFileInspection{}, err
	}

	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return StableRegularFileInspection{
		SizeBytes: safecast.MustConv[uint64](read),
		SHA256:    digest,
		Contents:  retained.Bytes(),
	}, nil
}

// WriteStableRegularFile exclusively creates one exact 0600 member, writes
// all bytes with cancellation checks, flushes and closes it, then returns the
// result of a complete stable-file inspection. Failed writes remove only the
// exact private file created by this call.
func WriteStableRegularFile(
	ctx context.Context,
	directory *Directory,
	name string,
	contents []byte,
) (inspection StableRegularFileInspection, returnedErr error) {
	return writeStableRegularFileWithHooks(
		ctx,
		directory,
		name,
		contents,
		writeStableRegularFileHooks{},
	)
}

type writeStableRegularFileHooks struct {
	beforeCleanupOpen func()
	afterWriterClose  func()
}

func writeStableRegularFileWithHooks(
	ctx context.Context,
	directory *Directory,
	name string,
	contents []byte,
	hooks writeStableRegularFileHooks,
) (inspection StableRegularFileInspection, returnedErr error) {
	if ctx == nil {
		return StableRegularFileInspection{}, errors.New(
			"write stable private file: context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return StableRegularFileInspection{}, err
	}
	temporaryName, file, err := directory.CreateTemporaryFile(
		func() (string, error) { return name, nil },
	)
	if err != nil {
		return StableRegularFileInspection{}, err
	}
	exists := true
	writerOpen := true
	var cleanupFile *os.File
	defer func() {
		if exists {
			expected := cleanupFile
			if expected == nil && writerOpen {
				expected = file
			}
			if expected != nil {
				if unlinkErr := directory.UnlinkPinnedRegular(temporaryName, expected); unlinkErr != nil &&
					!errors.Is(unlinkErr, os.ErrNotExist) {
					returnedErr = errors.Join(returnedErr, unlinkErr)
				}
			}
		}
		if cleanupFile != nil {
			returnedErr = errors.Join(returnedErr, cleanupFile.Close())
		}
		if writerOpen {
			returnedErr = errors.Join(returnedErr, file.Close())
		}
	}()
	if err := writeAllWithContext(ctx, file, contents); err != nil {
		return StableRegularFileInspection{}, fmt.Errorf(
			"write stable private file %q: %w",
			name,
			err,
		)
	}
	if err := SyncFile(file); err != nil {
		return StableRegularFileInspection{}, err
	}
	if err := ctx.Err(); err != nil {
		return StableRegularFileInspection{}, err
	}
	if hooks.beforeCleanupOpen != nil {
		hooks.beforeCleanupOpen()
	}
	size := int64(len(contents))
	cleanupCandidate, err := directory.OpenRegular(name, FilePolicy{
		AllowedModes: stableRegularFileModes,
		MinimumSize:  size,
		MaximumSize:  size,
	})
	if err != nil {
		return StableRegularFileInspection{}, fmt.Errorf(
			"pin stable private file %q for cleanup: %w",
			name,
			err,
		)
	}
	writerInfo, writerStatErr := file.Stat()
	candidateInfo, candidateStatErr := cleanupCandidate.Stat()
	identityErr := errors.Join(writerStatErr, candidateStatErr)
	if identityErr == nil && !os.SameFile(writerInfo, candidateInfo) {
		identityErr = errors.New(
			"reopened cleanup descriptor does not identify the created file",
		)
	}
	if identityErr != nil {
		return StableRegularFileInspection{}, errors.Join(
			fmt.Errorf(
				"pin stable private file %q for cleanup: %w",
				name,
				identityErr,
			),
			cleanupCandidate.Close(),
		)
	}
	cleanupFile = cleanupCandidate
	if err := file.Close(); err != nil {
		writerOpen = false
		return StableRegularFileInspection{}, fmt.Errorf(
			"close stable private file %q: %w",
			name,
			err,
		)
	}
	writerOpen = false
	if hooks.afterWriterClose != nil {
		hooks.afterWriterClose()
	}
	inspection, err = InspectStableRegularFile(
		ctx,
		directory,
		name,
		FilePolicy{
			AllowedModes: stableRegularFileModes,
			MinimumSize:  size,
			MaximumSize:  size,
		},
		false,
	)
	if err != nil {
		return StableRegularFileInspection{}, err
	}
	if err := directory.RequirePinnedRegular(name, cleanupFile); err != nil {
		return StableRegularFileInspection{}, fmt.Errorf(
			"rebind stable private file %q after inspection: %w",
			name,
			err,
		)
	}
	exists = false
	if err := cleanupFile.Close(); err != nil {
		cleanupFile = nil
		return StableRegularFileInspection{}, fmt.Errorf(
			"close stable private file %q cleanup descriptor: %w",
			name,
			err,
		)
	}
	cleanupFile = nil
	return inspection, nil
}

// CopyBufferWithContext copies through a reader that checks cancellation
// immediately before and after each underlying read.
func CopyBufferWithContext(
	ctx context.Context,
	destination io.Writer,
	source io.Reader,
	buffer []byte,
) (int64, error) {
	if ctx == nil {
		return 0, errors.New("copy with context: context is required")
	}
	if destination == nil || source == nil {
		return 0, errors.New("copy with context: source and destination are required")
	}
	if len(buffer) == 0 {
		return 0, errors.New("copy with context: buffer must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return io.CopyBuffer(
		destination,
		contextReader{ctx: ctx, reader: source},
		buffer,
	)
}

func writeAllWithContext(ctx context.Context, file *os.File, contents []byte) error {
	for len(contents) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := file.Write(contents)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(contents) {
			return io.ErrShortWrite
		}
		contents = contents[written:]
	}
	return nil
}

func sameStableFileState(left, right os.FileInfo) bool {
	return left != nil && right != nil &&
		os.SameFile(left, right) &&
		left.Mode() == right.Mode() &&
		left.Size() == right.Size() &&
		left.ModTime().Equal(right.ModTime())
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	read, err := reader.reader.Read(buffer)
	if err == nil {
		if contextErr := reader.ctx.Err(); contextErr != nil {
			return read, contextErr
		}
	}
	return read, err
}
