package clickhouse

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"google.golang.org/protobuf/proto"
)

const batchRejectionMetadataVersion = byte(1)

var batchRejectionMetadataMagic = [4]byte{'O', 'S', 'B', 'R'}

func encodeBatchRejectionMetadata(rejection *opensplunkv1.BatchReject) ([]byte, error) {
	if err := ingest.ValidateDurableBatchRejection(rejection); err != nil {
		return nil, fmt.Errorf("store terminal batch rejection: %w", err)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(rejection)
	if err != nil {
		return nil, fmt.Errorf("store terminal batch rejection: encode response: %w", err)
	}
	const framingBytes = len(batchRejectionMetadataMagic) + 1 + 8 + sha256.Size
	if len(encoded) > visibility.MaxMetadataBytes-framingBytes {
		return nil, errors.New("store terminal batch rejection: response metadata exceeds visibility ledger limit")
	}

	metadata := make([]byte, 0, framingBytes+len(encoded))
	metadata = append(metadata, batchRejectionMetadataMagic[:]...)
	metadata = append(metadata, batchRejectionMetadataVersion)
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(encoded)))
	metadata = append(metadata, length[:]...)
	metadata = append(metadata, encoded...)
	checksum := sha256.Sum256(metadata)
	metadata = append(metadata, checksum[:]...)
	return metadata, nil
}

func decodeBatchRejectionMetadata(metadata []byte) (*opensplunkv1.BatchReject, error) {
	const (
		headerBytes  = len(batchRejectionMetadataMagic) + 1 + 8
		minimumBytes = headerBytes + sha256.Size
	)
	if len(metadata) < minimumBytes || len(metadata) > visibility.MaxMetadataBytes {
		return nil, errors.New("terminal batch rejection metadata has an invalid size")
	}
	payload := metadata[:len(metadata)-sha256.Size]
	var storedChecksum [sha256.Size]byte
	copy(storedChecksum[:], metadata[len(payload):])
	if sha256.Sum256(payload) != storedChecksum {
		return nil, errors.New("terminal batch rejection metadata checksum mismatch")
	}
	if !bytes.Equal(payload[:len(batchRejectionMetadataMagic)], batchRejectionMetadataMagic[:]) {
		return nil, errors.New("terminal batch rejection metadata has an invalid header")
	}
	if payload[len(batchRejectionMetadataMagic)] != batchRejectionMetadataVersion {
		return nil, errors.New("terminal batch rejection metadata has an invalid version")
	}
	length := binary.BigEndian.Uint64(payload[len(batchRejectionMetadataMagic)+1 : headerBytes])
	encoded := payload[headerBytes:]
	if length == 0 || length != uint64(len(encoded)) {
		return nil, errors.New("terminal batch rejection metadata has an invalid response length")
	}
	rejection := new(opensplunkv1.BatchReject)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, rejection); err != nil {
		return nil, fmt.Errorf("decode terminal batch rejection response: %w", err)
	}
	if err := ingest.ValidateDurableBatchRejection(rejection); err != nil {
		return nil, fmt.Errorf("decode terminal batch rejection response: %w", err)
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(rejection)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, errors.New("terminal batch rejection metadata is not canonical")
	}
	return rejection, nil
}
