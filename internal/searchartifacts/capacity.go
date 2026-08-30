package searchartifacts

import (
	"errors"
	"hash"
	"io"
	"os"
)

const artifactWriteBufferBytes = 64 << 10

func (store *Store) reserveArtifactBytes(bytes uint64) error {
	if bytes == 0 {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	used := store.artifactBytes + store.reservedBytes
	if used < store.artifactBytes || used > store.maximumBytes || bytes > store.maximumBytes-used {
		return ErrCapacity
	}
	store.reservedBytes += bytes
	return nil
}

func (store *Store) releaseArtifactReservation(bytes uint64) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if bytes > store.reservedBytes {
		store.reservedBytes = 0
		return
	}
	store.reservedBytes -= bytes
}

// commitArtifactReservation converts already-reserved bytes into durable
// usage. The caller holds Store.mu after the metadata update commits.
func (store *Store) commitArtifactReservation(bytes uint64) {
	if bytes > store.reservedBytes {
		store.reservedBytes = 0
		store.artifactBytes = store.maximumBytes
		return
	}
	store.reservedBytes -= bytes
	store.artifactBytes += bytes
}

type artifactReservationWriter struct {
	store       *Store
	destination *os.File
	digest      hash.Hash
	reserved    *uint64
	attempted   *uint64
}

func (writer *artifactReservationWriter) Write(payload []byte) (int, error) {
	length := uint64(len(payload))
	*writer.attempted = saturatingAdd(*writer.attempted, length)
	if err := writer.store.reserveArtifactBytes(length); err != nil {
		return 0, err
	}
	*writer.reserved += length
	written, err := writer.destination.Write(payload)
	if written > 0 {
		_, _ = writer.digest.Write(payload[:written])
	}
	if err != nil {
		return written, err
	}
	if written != len(payload) {
		return written, io.ErrShortWrite
	}
	return written, nil
}

func subtractCapacity(current, decrement uint64) (uint64, error) {
	if decrement > current {
		return 0, errors.New("search artifact capacity accounting is corrupt")
	}
	return current - decrement, nil
}
