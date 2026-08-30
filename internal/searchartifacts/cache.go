package searchartifacts

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"slices"
)

type artifactCatalogIdentity struct {
	JobID   string
	Name    string
	Digest  [sha256.Size]byte
	Bytes   uint64
	Version uint64
}

type artifactFileIdentity struct {
	Device          uint64
	Inode           uint64
	Size            int64
	ModifiedSeconds int64
	ModifiedNanos   int64
	ChangedSeconds  int64
	ChangedNanos    int64
}

type artifactVerification struct {
	Format   uint32
	Header   artifactHeader
	Metadata artifactMetadata
	Offsets  []uint64
}

type cachedArtifact struct {
	Catalog      artifactCatalogIdentity
	FileIdentity artifactFileIdentity
	Verification artifactVerification
}

type artifactVerificationFlight struct {
	done         chan struct{}
	err          error
	fileIdentity artifactFileIdentity
}

type artifactVerifier func(
	context.Context,
	*os.File,
	artifactCatalogIdentity,
) (artifactVerification, error)

type artifactLoader func(
	context.Context,
	*os.File,
	string,
	artifactVerification,
) (artifactMetadata, artifactRowSource, error)

func newArtifactCatalogIdentity(
	jobID string,
	name string,
	digest []byte,
	bytes uint64,
	version uint64,
) (artifactCatalogIdentity, error) {
	if jobID == "" || name == "" || len(digest) != sha256.Size || bytes == 0 || version == 0 {
		return artifactCatalogIdentity{}, ErrCorrupt
	}
	identity := artifactCatalogIdentity{
		JobID: jobID, Name: name, Bytes: bytes, Version: version,
	}
	copy(identity.Digest[:], digest)
	return identity, nil
}

func verifyArtifact(
	ctx context.Context,
	file *os.File,
	catalog artifactCatalogIdentity,
) (artifactVerification, error) {
	format, header, offsets, err := verifyStoredArtifact(
		ctx,
		file,
		catalog.Bytes,
		catalog.JobID,
		catalog.Digest[:],
	)
	if err != nil {
		return artifactVerification{}, err
	}
	verification := artifactVerification{
		Format:  format,
		Header:  header,
		Offsets: append([]uint64(nil), offsets...),
	}
	if format == artifactFormatVersion {
		verification.Metadata = metadataFromHeader(header)
		return verification, nil
	}
	metadata, err := readLegacyArtifactMetadata(file, catalog.JobID)
	if err != nil {
		return artifactVerification{}, err
	}
	verification.Metadata = metadata
	return verification, nil
}

func loadVerifiedArtifact(
	ctx context.Context,
	file *os.File,
	jobID string,
	verification artifactVerification,
) (artifactMetadata, artifactRowSource, error) {
	if ctx == nil || file == nil || jobID == "" {
		return artifactMetadata{}, nil, ErrCorrupt
	}
	if err := ctx.Err(); err != nil {
		return artifactMetadata{}, nil, err
	}
	switch verification.Format {
	case artifactFormatVersion:
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return artifactMetadata{}, nil, ErrCorrupt
		}
		reader := bufio.NewReaderSize(file, 64<<10)
		header, err := readFramedHeader(reader, jobID)
		if err != nil || !artifactHeadersEqual(header, verification.Header) {
			return artifactMetadata{}, nil, ErrCorrupt
		}
		if err := ctx.Err(); err != nil {
			return artifactMetadata{}, nil, err
		}
		return cloneArtifactMetadata(verification.Metadata), &framedRowSource{
			file:     file,
			reader:   reader,
			rowCount: verification.Metadata.RowCount,
			offsets:  verification.Offsets,
		}, nil
	case legacyArtifactFormatVersion:
		decoder, err := positionLegacyRows(file)
		if err != nil {
			return artifactMetadata{}, nil, err
		}
		return cloneArtifactMetadata(verification.Metadata), &legacyRowSource{
			file: file, decoder: decoder, rowCount: verification.Metadata.RowCount,
		}, nil
	default:
		return artifactMetadata{}, nil, ErrCorrupt
	}
}

func (store *Store) artifactVerification(
	ctx context.Context,
	file *os.File,
	catalog artifactCatalogIdentity,
	fileIdentity artifactFileIdentity,
) (artifactVerification, error) {
	for {
		store.mu.Lock()
		if cached, ok := store.verified[catalog.JobID]; ok {
			if cached.Catalog == catalog && cached.FileIdentity == fileIdentity {
				verification := cached.Verification
				store.mu.Unlock()
				return verification, nil
			}
			delete(store.verified, catalog.JobID)
		}
		if flight, ok := store.verifying[catalog]; ok {
			done := flight.done
			sameFile := flight.fileIdentity == fileIdentity
			store.mu.Unlock()
			select {
			case <-ctx.Done():
				return artifactVerification{}, ctx.Err()
			case <-done:
			}
			if sameFile && flight.err != nil &&
				!errors.Is(flight.err, context.Canceled) &&
				!errors.Is(flight.err, context.DeadlineExceeded) {
				return artifactVerification{}, flight.err
			}
			continue
		}
		flight := &artifactVerificationFlight{
			done: make(chan struct{}), fileIdentity: fileIdentity,
		}
		store.verifying[catalog] = flight
		verifier := store.verify
		store.mu.Unlock()

		verification, err := verifier(ctx, file, catalog)
		if err == nil {
			currentIdentity, identityErr := statArtifactFile(file)
			if identityErr != nil || currentIdentity != fileIdentity {
				err = ErrCorrupt
			}
		}

		store.mu.Lock()
		if err == nil && !store.closed {
			store.verified[catalog.JobID] = cachedArtifact{
				Catalog: catalog, FileIdentity: fileIdentity,
				Verification: cloneArtifactVerification(verification),
			}
		} else {
			delete(store.verified, catalog.JobID)
		}
		flight.err = err
		delete(store.verifying, catalog)
		close(flight.done)
		store.mu.Unlock()
		return verification, err
	}
}

func (store *Store) cacheArtifactLocked(
	catalog artifactCatalogIdentity,
	fileIdentity artifactFileIdentity,
	verification artifactVerification,
) {
	store.verified[catalog.JobID] = cachedArtifact{
		Catalog: catalog, FileIdentity: fileIdentity,
		Verification: cloneArtifactVerification(verification),
	}
}

func (store *Store) invalidateArtifactLocked(jobID string) {
	delete(store.verified, jobID)
}

func (store *Store) invalidateArtifactNameLocked(name string) {
	for jobID, cached := range store.verified {
		if cached.Catalog.Name == name {
			delete(store.verified, jobID)
		}
	}
}

func cloneArtifactVerification(verification artifactVerification) artifactVerification {
	verification.Header.Schema = cloneSchema(verification.Header.Schema)
	verification.Metadata = cloneArtifactMetadata(verification.Metadata)
	verification.Offsets = slices.Clone(verification.Offsets)
	return verification
}

func cloneArtifactMetadata(metadata artifactMetadata) artifactMetadata {
	metadata.Schema = cloneSchema(metadata.Schema)
	return metadata
}
