package clickhouse

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
	"unicode/utf8"

	"fortio.org/safecast"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

const maxDurableBatchEvents = 1000

var (
	legacyStoreOutboxHeader = [5]byte{'O', 'S', 'O', 'B', 2}
	storeOutboxHeader       = [5]byte{'O', 'S', 'O', 'B', 3}
)

// encodeStoreOutbox persists the exact normalized/redacted protobuf block
// which the ClickHouse writer will replay after an ambiguous failure. It does
// not include mutable retention: that snapshot lives in reservation metadata.
func encodeStoreOutbox(batch ingest.StoreBatch) ([]byte, error) {
	if err := validateOutboxBatch(batch); err != nil {
		return nil, err
	}
	source, _ := ingest.CanonicalIngestionSource(batch.Source, batch.CollectorID)
	var body bytes.Buffer
	writeOutboxBytes(&body, []byte(batch.TenantID))
	writeOutboxBytes(&body, []byte(batch.CollectorID))
	_ = body.WriteByte(byte(source.Kind))
	writeOutboxBytes(&body, []byte(source.ID))
	writeOutboxBytes(&body, []byte(batch.BatchID))
	writeOutboxUint64(&body, batch.BatchSequence)
	_, _ = body.Write(batch.SourceBatchSHA256[:])
	writeOutboxUint64(&body, uint64(batch.ReceivedAt.UTC().UnixMilli()))
	writeOutboxUint32(&body, batch.OriginalEventCount)

	writeOutboxUint32(&body, safecast.MustConv[uint32](len(batch.Events)))
	marshal := proto.MarshalOptions{Deterministic: true}
	for index, stored := range batch.Events {
		encoded, err := marshal.Marshal(stored.Event)
		if err != nil {
			return nil, fmt.Errorf("encode ClickHouse outbox event %d: %w", index, err)
		}
		writeOutboxBytes(&body, encoded)
		if body.Len()+len(storeOutboxHeader)+sha256.Size > visibility.MaxOutboxBytes {
			return nil, errors.New("store ClickHouse outbox exceeds durable replay limit")
		}
	}
	digest := sha256.Sum256(body.Bytes())
	var output bytes.Buffer
	output.Grow(len(storeOutboxHeader) + sha256.Size + body.Len())
	_, _ = output.Write(storeOutboxHeader[:])
	_, _ = output.Write(digest[:])
	_, _ = output.Write(body.Bytes())
	return output.Bytes(), nil
}

func decodeStoreOutbox(encoded []byte) (ingest.StoreBatch, error) {
	if len(encoded) > visibility.MaxOutboxBytes {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox exceeds durable replay limit")
	}
	reader := bytes.NewReader(encoded)
	header := make([]byte, len(storeOutboxHeader))
	if _, err := io.ReadFull(reader, header); err != nil {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox has an invalid version")
	}
	legacy := bytes.Equal(header, legacyStoreOutboxHeader[:])
	if !legacy && !bytes.Equal(header, storeOutboxHeader[:]) {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox has an invalid version")
	}
	var storedDigest [sha256.Size]byte
	if _, err := io.ReadFull(reader, storedDigest[:]); err != nil {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox has no payload checksum")
	}
	body := make([]byte, reader.Len())
	if _, err := io.ReadFull(reader, body); err != nil {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox payload is truncated")
	}
	if sha256.Sum256(body) != storedDigest {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox payload checksum mismatch")
	}
	reader = bytes.NewReader(body)
	tenantID, err := readOutboxString(reader, 255)
	if err != nil {
		return ingest.StoreBatch{}, fmt.Errorf("decode ClickHouse outbox tenant: %w", err)
	}
	collectorID, err := readOutboxOptionalString(reader, 128)
	if err != nil {
		return ingest.StoreBatch{}, fmt.Errorf("decode ClickHouse outbox collector: %w", err)
	}
	var source ingest.IngestionSource
	if legacy {
		if collectorID == "" {
			return ingest.StoreBatch{}, errors.New("decode ClickHouse outbox collector: empty legacy collector")
		}
		source = ingest.NativeCollectorSource(collectorID)
	} else {
		kind, readErr := reader.ReadByte()
		if readErr != nil {
			return ingest.StoreBatch{}, errors.New("ClickHouse outbox is truncated at ingestion source kind")
		}
		sourceID, readErr := readOutboxString(reader, uint64(ingest.HardMaxIDBytes))
		if readErr != nil {
			return ingest.StoreBatch{}, fmt.Errorf("decode ClickHouse outbox ingestion source: %w", readErr)
		}
		source = ingest.IngestionSource{
			Kind:        ingest.IngestionSourceKind(kind),
			ID:          sourceID,
			CollectorID: collectorID,
		}
		if _, sourceErr := ingest.CanonicalIngestionSource(source, collectorID); sourceErr != nil {
			return ingest.StoreBatch{}, fmt.Errorf("decode ClickHouse outbox ingestion source: %w", sourceErr)
		}
	}
	batchID, err := readOutboxString(reader, 128)
	if err != nil {
		return ingest.StoreBatch{}, fmt.Errorf("decode ClickHouse outbox batch: %w", err)
	}
	batchSequence, err := readOutboxUint64(reader)
	if err != nil {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox is truncated at batch sequence")
	}
	var sourceDigest [sha256.Size]byte
	if _, err := io.ReadFull(reader, sourceDigest[:]); err != nil {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox is truncated at source digest")
	}
	receivedMillis, err := readOutboxUint64(reader)
	if err != nil || receivedMillis > math.MaxInt64 {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox has an invalid received time")
	}
	originalEventCount, err := readOutboxUint32(reader)
	if err != nil || originalEventCount == 0 || originalEventCount > maxDurableBatchEvents {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox has an invalid source event count")
	}
	eventCount, err := readOutboxUint32(reader)
	if err != nil || eventCount == 0 || eventCount > originalEventCount ||
		uint64(eventCount) > safecast.MustConv[uint64](reader.Len())/9 {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox has an invalid accepted event count")
	}
	receivedAt := time.UnixMilli(int64(receivedMillis)).UTC()
	batch := ingest.StoreBatch{
		TenantID:           tenantID,
		Source:             source,
		CollectorID:        collectorID,
		BatchID:            batchID,
		BatchSequence:      batchSequence,
		OriginalEventCount: originalEventCount,
		SourceBatchSHA256:  sourceDigest,
		ReceivedAt:         receivedAt,
		Events:             make([]*ingest.StoredEvent, 0, eventCount),
	}
	for index := range eventCount {
		payload, readErr := readOutboxBytes(reader, math.MaxUint64)
		if readErr != nil || len(payload) == 0 {
			return ingest.StoreBatch{}, fmt.Errorf("decode ClickHouse outbox event %d: invalid payload", index)
		}
		event := new(opensplunk.LogEvent)
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payload, event); err != nil {
			return ingest.StoreBatch{}, fmt.Errorf("decode ClickHouse outbox event %d: %w", index, err)
		}
		batch.Events = append(batch.Events, &ingest.StoredEvent{
			Event:       event,
			TenantID:    tenantID,
			Source:      source,
			CollectorID: collectorID,
			BatchID:     batchID,
			IndexTime:   receivedAt,
		})
	}
	if reader.Len() != 0 {
		return ingest.StoreBatch{}, errors.New("ClickHouse outbox has trailing bytes")
	}
	if err := validateOutboxBatch(batch); err != nil {
		return ingest.StoreBatch{}, fmt.Errorf("validate ClickHouse outbox: %w", err)
	}
	return batch, nil
}

func cloneEventRejections(values []*opensplunk.EventRejection) []*opensplunk.EventRejection {
	if values == nil {
		return nil
	}
	cloned := make([]*opensplunk.EventRejection, len(values))
	for index, value := range values {
		if value != nil {
			cloned[index] = proto.Clone(value).(*opensplunk.EventRejection)
		}
	}
	return cloned
}

func validateOutboxBatch(batch ingest.StoreBatch) error {
	source, sourceErr := ingest.CanonicalIngestionSource(batch.Source, batch.CollectorID)
	if sourceErr != nil {
		return fmt.Errorf("store ClickHouse outbox: invalid ingestion source: %w", sourceErr)
	}
	if batch.TenantID == "" || batch.BatchID == "" ||
		!utf8.ValidString(batch.TenantID) || !utf8.ValidString(batch.CollectorID) || !utf8.ValidString(batch.BatchID) ||
		len(batch.TenantID) > 255 || len(batch.CollectorID) > 128 || len(batch.BatchID) > 128 ||
		batch.BatchSequence == 0 || batch.SourceBatchSHA256 == ([sha256.Size]byte{}) || batch.ReceivedAt.IsZero() ||
		batch.ReceivedAt.UnixMilli() < 0 {
		return errors.New("store ClickHouse outbox: complete valid identity is required")
	}
	if batch.OriginalEventCount == 0 || batch.OriginalEventCount > maxDurableBatchEvents ||
		len(batch.Events) == 0 || uint64(len(batch.Events)) > uint64(batch.OriginalEventCount) {
		return errors.New("store ClickHouse outbox: accepted and source event counts are invalid")
	}
	for index, stored := range batch.Events {
		if stored == nil || stored.Event == nil {
			return fmt.Errorf("store ClickHouse outbox: event %d is nil", index)
		}
		storedSource, err := ingest.CanonicalIngestionSource(stored.Source, stored.CollectorID)
		if err != nil || stored.TenantID != batch.TenantID || storedSource != source || stored.BatchID != batch.BatchID {
			return fmt.Errorf("store ClickHouse outbox: event %d server metadata does not match its batch", index)
		}
	}
	return nil
}

func writeOutboxBytes(output *bytes.Buffer, value []byte) {
	writeOutboxUint64(output, uint64(len(value)))
	_, _ = output.Write(value)
}

func writeOutboxUint32(output *bytes.Buffer, value uint32) {
	var number [4]byte
	binary.BigEndian.PutUint32(number[:], value)
	_, _ = output.Write(number[:])
}

func writeOutboxUint64(output *bytes.Buffer, value uint64) {
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], value)
	_, _ = output.Write(number[:])
}

func readOutboxString(reader *bytes.Reader, maximum uint64) (string, error) {
	value, err := readOutboxBytes(reader, maximum)
	if err != nil || len(value) == 0 || !utf8.Valid(value) {
		return "", errors.New("invalid UTF-8 string")
	}
	return string(value), nil
}

func readOutboxOptionalString(reader *bytes.Reader, maximum uint64) (string, error) {
	value, err := readOutboxBytes(reader, maximum)
	if err != nil || !utf8.Valid(value) {
		return "", errors.New("invalid UTF-8 string")
	}
	return string(value), nil
}

func readOutboxBytes(reader *bytes.Reader, maximum uint64) ([]byte, error) {
	length, err := readOutboxUint64(reader)

	if err != nil || length > maximum ||
		length > safecast.MustConv[uint64](reader.Len()) ||
		length > safecast.MustConv[uint64](math.MaxInt) {
		return nil, errors.New("invalid length")
	}
	value := make([]byte, safecast.MustConv[int](length))
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, err
	}
	return value, nil
}

func readOutboxUint32(reader *bytes.Reader) (uint32, error) {
	var number [4]byte
	if _, err := io.ReadFull(reader, number[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(number[:]), nil
}

func readOutboxUint64(reader *bytes.Reader) (uint64, error) {
	var number [8]byte
	if _, err := io.ReadFull(reader, number[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(number[:]), nil
}
