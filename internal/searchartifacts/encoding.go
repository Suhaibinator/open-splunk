package searchartifacts

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"math"
	"os"
	"slices"
	"time"

	"fortio.org/safecast"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

const (
	legacyArtifactFormatVersion = uint32(1)
	artifactFormatVersion       = uint32(2)
	maximumArtifactRows         = uint64(10_000_000)
	maximumArtifactHeaderBytes  = uint32(2 << 20)
	maximumArtifactRowBytes     = uint32(64 << 20)
	artifactIndexStride         = uint32(256)
)

var artifactMagic = [8]byte{'O', 'S', 'R', 'E', 'S', 'U', 'L', 'T'}
var artifactIndexMagic = [8]byte{'O', 'S', 'I', 'N', 'D', 'E', 'X', '2'}

type storedJob struct {
	Job               searchjobs.Job `json:"job"`
	KnowledgeSnapshot []byte         `json:"knowledge_snapshot,omitempty"`
}

type storedArtifact struct {
	Version          uint32            `json:"version"`
	JobID            string            `json:"job_id"`
	Generation       uint64            `json:"generation"`
	Schema           searchjobs.Schema `json:"schema"`
	Rows             []storedResultRow `json:"rows"`
	ResultsTruncated bool              `json:"results_truncated"`
}

type artifactHeader struct {
	Version          uint32            `json:"version"`
	JobID            string            `json:"job_id"`
	Generation       uint64            `json:"generation"`
	Schema           searchjobs.Schema `json:"schema"`
	RowCount         uint64            `json:"row_count"`
	RowCountExact    bool              `json:"row_count_exact"`
	ResultsTruncated bool              `json:"results_truncated"`
}

type artifactMetadata struct {
	Generation       uint64
	Schema           searchjobs.Schema
	RowCount         uint64
	RowCountExact    bool
	ResultsTruncated bool
}

type artifactRowSource interface {
	Next(context.Context) (searchjobs.ResultRow, bool, error)
	Seek(context.Context, uint64) error
	Close() error
}

type framedRowSource struct {
	file     *os.File
	reader   *bufio.Reader
	rowCount uint64
	next     uint64
	offsets  []uint64
}

type legacyRowSource struct {
	file     *os.File
	decoder  *json.Decoder
	rowCount uint64
	next     uint64
}

func writeVerifiedArtifact(
	ctx context.Context,
	destination io.Writer,
	jobID string,
	results searchjobs.ResultLease,
) (artifactVerification, error) {
	if ctx == nil || destination == nil || results == nil || jobID == "" {
		return artifactVerification{}, ErrInvalid
	}
	header := artifactHeader{
		Version: artifactFormatVersion, JobID: jobID, Generation: results.Generation(),
		Schema: results.Schema(), RowCount: results.RowCount(), RowCountExact: results.RowCountExact(),
		ResultsTruncated: results.ResultsTruncated(),
	}
	if err := validateArtifactHeader(header, jobID); err != nil {
		return artifactVerification{}, err
	}
	encodedHeader, err := json.Marshal(header)
	if err != nil || len(encodedHeader) == 0 || uint64(len(encodedHeader)) > uint64(maximumArtifactHeaderBytes) {
		return artifactVerification{}, ErrCorrupt
	}
	positioned := &positionWriter{writer: destination}
	if err := writeArtifactBytes(positioned, artifactMagic[:]); err != nil {
		return artifactVerification{}, err
	}
	headerLength, err := safecast.Conv[uint32](len(encodedHeader))
	if err != nil {
		return artifactVerification{}, ErrCorrupt
	}
	if err := writeArtifactLength(positioned, headerLength); err != nil {
		return artifactVerification{}, err
	}
	if err := writeArtifactBytes(positioned, encodedHeader); err != nil {
		return artifactVerification{}, err
	}
	offsets := make([]uint64, 0, artifactIndexCount(header.RowCount))
	for ordinal := uint64(0); ordinal < header.RowCount; ordinal++ {
		if err := ctx.Err(); err != nil {
			return artifactVerification{}, err
		}
		row, ok, err := results.Next(ctx)
		if err != nil {
			return artifactVerification{}, err
		}
		if !ok || row.Ordinal != ordinal {
			return artifactVerification{}, ErrCorrupt
		}
		stored, err := storedRow(row)
		if err != nil {
			return artifactVerification{}, err
		}
		encoded, err := json.Marshal(stored)
		if err != nil || len(encoded) == 0 || uint64(len(encoded)) > uint64(maximumArtifactRowBytes) {
			return artifactVerification{}, ErrCapacity
		}
		if ordinal%uint64(artifactIndexStride) == 0 {
			offsets = append(offsets, positioned.offset)
		}
		rowLength, err := safecast.Conv[uint32](len(encoded))
		if err != nil {
			return artifactVerification{}, ErrCapacity
		}
		if err := writeArtifactLength(positioned, rowLength); err != nil {
			return artifactVerification{}, err
		}
		if err := writeArtifactBytes(positioned, encoded); err != nil {
			return artifactVerification{}, err
		}
	}
	if _, ok, err := results.Next(ctx); err != nil {
		return artifactVerification{}, err
	} else if ok {
		return artifactVerification{}, ErrCorrupt
	}
	if err := writeArtifactBytes(positioned, artifactIndexMagic[:]); err != nil {
		return artifactVerification{}, err
	}
	if err := writeArtifactLength(positioned, artifactIndexStride); err != nil {
		return artifactVerification{}, err
	}
	offsetCount, err := safecast.Conv[uint32](len(offsets))
	if err != nil {
		return artifactVerification{}, ErrCorrupt
	}
	if err := writeArtifactUint32(positioned, offsetCount); err != nil {
		return artifactVerification{}, err
	}
	for _, offset := range offsets {
		if err := writeArtifactUint64(positioned, offset); err != nil {
			return artifactVerification{}, err
		}
	}
	return artifactVerification{
		Format: artifactFormatVersion, Header: header,
		Metadata: metadataFromHeader(header), Offsets: offsets,
	}, nil
}

func loadStoredArtifact(
	ctx context.Context,
	file *os.File,
	maximumBytes uint64,
	jobID string,
	digest []byte,
) (artifactMetadata, artifactRowSource, error) {
	if ctx == nil || file == nil || maximumBytes == 0 || len(digest) != sha256.Size {
		return artifactMetadata{}, nil, ErrCorrupt
	}
	format, header, offsets, err := verifyStoredArtifact(ctx, file, maximumBytes, jobID, digest)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return artifactMetadata{}, nil, contextErr
		}
		return artifactMetadata{}, nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return artifactMetadata{}, nil, ErrCorrupt
	}
	if format == artifactFormatVersion {
		reader := bufio.NewReaderSize(file, 64<<10)
		loaded, err := readFramedHeader(reader, jobID)
		if err != nil || !artifactHeadersEqual(loaded, header) {
			return artifactMetadata{}, nil, ErrCorrupt
		}
		return metadataFromHeader(header), &framedRowSource{
			file: file, reader: reader, rowCount: header.RowCount, offsets: offsets,
		}, nil
	}
	legacy, err := readLegacyArtifactMetadata(file, jobID)
	if err != nil {
		return artifactMetadata{}, nil, err
	}
	decoder, err := positionLegacyRows(file)
	if err != nil {
		return artifactMetadata{}, nil, err
	}
	return legacy, &legacyRowSource{file: file, decoder: decoder, rowCount: legacy.RowCount}, nil
}

func verifyStoredArtifact(
	ctx context.Context,
	file *os.File,
	maximumBytes uint64,
	jobID string,
	digest []byte,
) (uint32, artifactHeader, []uint64, error) {
	maximumRead, err := safecast.Conv[int64](maximumBytes)
	if err != nil || maximumRead == math.MaxInt64 {
		return 0, artifactHeader{}, nil, ErrCorrupt
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, artifactHeader{}, nil, ErrCorrupt
	}
	hasher := sha256.New()
	limited := &contextHashReader{ctx: ctx, reader: io.LimitReader(file, maximumRead+1), hash: hasher}
	reader := bufio.NewReaderSize(limited, 64<<10)
	logical := &positionReader{reader: reader}
	prefix := make([]byte, len(artifactMagic))
	if _, err := io.ReadFull(logical, prefix); err != nil {
		return 0, artifactHeader{}, nil, ErrCorrupt
	}
	if !bytes.Equal(prefix, artifactMagic[:]) {
		if _, err := io.Copy(io.Discard, logical); err != nil {
			return 0, artifactHeader{}, nil, ErrCorrupt
		}
		if limited.read != maximumBytes || !bytes.Equal(hasher.Sum(nil), digest) {
			return 0, artifactHeader{}, nil, ErrCorrupt
		}
		return legacyArtifactFormatVersion, artifactHeader{}, nil, nil
	}
	header, err := readFramedHeaderAfterMagic(logical, jobID)
	if err != nil {
		return 0, artifactHeader{}, nil, err
	}
	offsets := make([]uint64, 0, artifactIndexCount(header.RowCount))
	for ordinal := uint64(0); ordinal < header.RowCount; ordinal++ {
		if ordinal%uint64(artifactIndexStride) == 0 {
			offsets = append(offsets, logical.offset)
		}
		length, err := readArtifactLength(logical, maximumArtifactRowBytes)
		if err != nil {
			return 0, artifactHeader{}, nil, err
		}
		if _, err := io.CopyN(io.Discard, logical, int64(length)); err != nil {
			return 0, artifactHeader{}, nil, ErrCorrupt
		}
	}
	indexMagic := make([]byte, len(artifactIndexMagic))
	if _, err := io.ReadFull(logical, indexMagic); err != nil || !bytes.Equal(indexMagic, artifactIndexMagic[:]) {
		return 0, artifactHeader{}, nil, ErrCorrupt
	}
	stride, err := readArtifactLength(logical, artifactIndexStride)
	if err != nil || stride != artifactIndexStride {
		return 0, artifactHeader{}, nil, ErrCorrupt
	}
	count, err := readArtifactUint32(logical)
	if err != nil || uint64(count) > artifactIndexCount(maximumArtifactRows) ||
		uint64(count) != artifactIndexCount(header.RowCount) {
		return 0, artifactHeader{}, nil, ErrCorrupt
	}
	for _, expected := range offsets {
		stored, err := readArtifactUint64(logical)
		if err != nil || stored != expected {
			return 0, artifactHeader{}, nil, ErrCorrupt
		}
	}
	var trailing [1]byte
	if _, err := logical.Read(trailing[:]); !errors.Is(err, io.EOF) {
		return 0, artifactHeader{}, nil, ErrCorrupt
	}
	if limited.read != maximumBytes || !bytes.Equal(hasher.Sum(nil), digest) {
		return 0, artifactHeader{}, nil, ErrCorrupt
	}
	return artifactFormatVersion, header, offsets, nil
}

func readFramedHeader(reader io.Reader, jobID string) (artifactHeader, error) {
	prefix := make([]byte, len(artifactMagic))
	if _, err := io.ReadFull(reader, prefix); err != nil || !bytes.Equal(prefix, artifactMagic[:]) {
		return artifactHeader{}, ErrCorrupt
	}
	return readFramedHeaderAfterMagic(reader, jobID)
}

func readFramedHeaderAfterMagic(reader io.Reader, jobID string) (artifactHeader, error) {
	length, err := readArtifactLength(reader, maximumArtifactHeaderBytes)
	if err != nil {
		return artifactHeader{}, err
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return artifactHeader{}, ErrCorrupt
	}
	var header artifactHeader
	if err := decodeExactJSON(payload, &header); err != nil || validateArtifactHeader(header, jobID) != nil {
		return artifactHeader{}, ErrCorrupt
	}
	return header, nil
}

func validateArtifactHeader(header artifactHeader, jobID string) error {
	if header.Version != artifactFormatVersion || header.JobID != jobID || header.Generation == 0 ||
		header.RowCount > maximumArtifactRows || len(header.Schema.Columns) > int(maximumArtifactRows) {
		return ErrCorrupt
	}
	return nil
}

func metadataFromHeader(header artifactHeader) artifactMetadata {
	return artifactMetadata{
		Generation: header.Generation, Schema: cloneSchema(header.Schema),
		RowCount: header.RowCount, RowCountExact: header.RowCountExact,
		ResultsTruncated: header.ResultsTruncated,
	}
}

func artifactHeadersEqual(left, right artifactHeader) bool {
	return left.Version == right.Version && left.JobID == right.JobID &&
		left.Generation == right.Generation && left.RowCount == right.RowCount &&
		left.RowCountExact == right.RowCountExact &&
		left.ResultsTruncated == right.ResultsTruncated &&
		slices.Equal(left.Schema.Columns, right.Schema.Columns)
}

func (source *framedRowSource) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if source.next >= source.rowCount {
		return searchjobs.ResultRow{}, false, nil
	}
	length, err := readArtifactLength(source.reader, maximumArtifactRowBytes)
	if err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(source.reader, payload); err != nil {
		return searchjobs.ResultRow{}, false, ErrCorrupt
	}
	var stored storedResultRow
	if err := decodeExactJSON(payload, &stored); err != nil {
		return searchjobs.ResultRow{}, false, ErrCorrupt
	}
	row, err := restoreRow(stored)
	if err != nil || row.Ordinal != source.next {
		return searchjobs.ResultRow{}, false, ErrCorrupt
	}
	source.next++
	return row, true, nil
}

func (source *framedRowSource) Seek(ctx context.Context, offset uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if offset > source.rowCount {
		return ErrInvalid
	}
	if offset == source.rowCount {
		source.next = offset
		return nil
	}
	checkpoint := offset / uint64(artifactIndexStride)
	if checkpoint >= uint64(len(source.offsets)) {
		return ErrCorrupt
	}
	position, err := safecast.Conv[int64](source.offsets[checkpoint])
	if err != nil {
		return ErrCorrupt
	}
	if _, err := source.file.Seek(position, io.SeekStart); err != nil {
		return ErrCorrupt
	}
	source.reader.Reset(source.file)
	source.next = checkpoint * uint64(artifactIndexStride)
	for source.next < offset {
		if _, ok, err := source.Next(ctx); err != nil {
			return err
		} else if !ok {
			return ErrCorrupt
		}
	}
	return nil
}

func (source *framedRowSource) Close() error {
	if source == nil || source.file == nil {
		return nil
	}
	err := source.file.Close()
	source.file = nil
	return err
}

func (source *legacyRowSource) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if source.next >= source.rowCount {
		return searchjobs.ResultRow{}, false, nil
	}
	if !source.decoder.More() {
		return searchjobs.ResultRow{}, false, ErrCorrupt
	}
	var stored storedResultRow
	if err := source.decoder.Decode(&stored); err != nil {
		return searchjobs.ResultRow{}, false, ErrCorrupt
	}
	row, err := restoreRow(stored)
	if err != nil || row.Ordinal != source.next {
		return searchjobs.ResultRow{}, false, ErrCorrupt
	}
	source.next++
	return row, true, nil
}

func (source *legacyRowSource) Seek(ctx context.Context, offset uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if offset > source.rowCount {
		return ErrInvalid
	}
	decoder, err := positionLegacyRows(source.file)
	if err != nil {
		return err
	}
	source.decoder = decoder
	source.next = 0
	for source.next < offset {
		if _, ok, err := source.Next(ctx); err != nil {
			return err
		} else if !ok {
			return ErrCorrupt
		}
	}
	return nil
}

func (source *legacyRowSource) Close() error {
	if source == nil || source.file == nil {
		return nil
	}
	err := source.file.Close()
	source.file = nil
	source.decoder = nil
	return err
}

func readLegacyArtifactMetadata(file *os.File, jobID string) (artifactMetadata, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return artifactMetadata{}, ErrCorrupt
	}
	decoder := json.NewDecoder(bufio.NewReaderSize(file, 64<<10))
	decoder.DisallowUnknownFields()
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return artifactMetadata{}, ErrCorrupt
	}
	var (
		version          uint32
		storedJobID      string
		generation       uint64
		schema           searchjobs.Schema
		resultsTruncated bool
		rowCount         uint64
		seen             uint8
	)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return artifactMetadata{}, ErrCorrupt
		}
		switch key {
		case "version":
			err = decoder.Decode(&version)
			seen |= 1 << 0
		case "job_id":
			err = decoder.Decode(&storedJobID)
			seen |= 1 << 1
		case "generation":
			err = decoder.Decode(&generation)
			seen |= 1 << 2
		case "schema":
			err = decoder.Decode(&schema)
			seen |= 1 << 3
		case "rows":
			if token, tokenErr := decoder.Token(); tokenErr != nil || token != json.Delim('[') {
				return artifactMetadata{}, ErrCorrupt
			}
			for decoder.More() {
				if rowCount >= maximumArtifactRows {
					return artifactMetadata{}, ErrCorrupt
				}
				var stored storedResultRow
				if err := decoder.Decode(&stored); err != nil {
					return artifactMetadata{}, ErrCorrupt
				}
				row, restoreErr := restoreRow(stored)
				if restoreErr != nil || row.Ordinal != rowCount {
					return artifactMetadata{}, ErrCorrupt
				}
				rowCount++
			}
			if token, tokenErr := decoder.Token(); tokenErr != nil || token != json.Delim(']') {
				return artifactMetadata{}, ErrCorrupt
			}
			seen |= 1 << 4
		case "results_truncated":
			err = decoder.Decode(&resultsTruncated)
			seen |= 1 << 5
		default:
			return artifactMetadata{}, ErrCorrupt
		}
		if err != nil {
			return artifactMetadata{}, ErrCorrupt
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return artifactMetadata{}, ErrCorrupt
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return artifactMetadata{}, ErrCorrupt
	}
	if seen != (1<<6)-1 || version != legacyArtifactFormatVersion || storedJobID != jobID || generation == 0 {
		return artifactMetadata{}, ErrCorrupt
	}
	return artifactMetadata{
		Generation: generation, Schema: cloneSchema(schema), RowCount: rowCount,
		RowCountExact: !resultsTruncated, ResultsTruncated: resultsTruncated,
	}, nil
}

func positionLegacyRows(file *os.File) (*json.Decoder, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, ErrCorrupt
	}
	decoder := json.NewDecoder(bufio.NewReaderSize(file, 64<<10))
	decoder.DisallowUnknownFields()
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, ErrCorrupt
	}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, ErrCorrupt
		}
		if key == "rows" {
			if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
				return nil, ErrCorrupt
			}
			return decoder, nil
		}
		var discarded json.RawMessage
		if err := decoder.Decode(&discarded); err != nil {
			return nil, ErrCorrupt
		}
	}
	return nil, ErrCorrupt
}

func writeArtifactLength(destination io.Writer, length uint32) error {
	if length == 0 {
		return ErrCorrupt
	}
	return writeArtifactUint32(destination, length)
}

func writeArtifactUint32(destination io.Writer, value uint32) error {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return writeArtifactBytes(destination, encoded[:])
}

func writeArtifactUint64(destination io.Writer, value uint64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return writeArtifactBytes(destination, encoded[:])
}

func writeArtifactBytes(destination io.Writer, payload []byte) error {
	for len(payload) != 0 {
		written, err := destination.Write(payload)
		if written > 0 {
			payload = payload[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func readArtifactLength(source io.Reader, maximum uint32) (uint32, error) {
	length, err := readArtifactUint32(source)
	if err != nil {
		return 0, ErrCorrupt
	}
	if length == 0 || length > maximum {
		return 0, ErrCorrupt
	}
	return length, nil
}

func readArtifactUint32(source io.Reader) (uint32, error) {
	var encoded [4]byte
	if _, err := io.ReadFull(source, encoded[:]); err != nil {
		return 0, ErrCorrupt
	}
	return binary.BigEndian.Uint32(encoded[:]), nil
}

func readArtifactUint64(source io.Reader) (uint64, error) {
	var encoded [8]byte
	if _, err := io.ReadFull(source, encoded[:]); err != nil {
		return 0, ErrCorrupt
	}
	return binary.BigEndian.Uint64(encoded[:]), nil
}

func artifactIndexCount(rowCount uint64) uint64 {
	if rowCount == 0 {
		return 0
	}
	return (rowCount-1)/uint64(artifactIndexStride) + 1
}

type positionWriter struct {
	writer io.Writer
	offset uint64
}

func (writer *positionWriter) Write(payload []byte) (int, error) {
	written, err := writer.writer.Write(payload)
	increment, conversionErr := safecast.Conv[uint64](written)
	if conversionErr != nil || increment > math.MaxUint64-writer.offset || written > len(payload) {
		return 0, io.ErrShortWrite
	}
	writer.offset += increment
	return written, err
}

type positionReader struct {
	reader io.Reader
	offset uint64
}

func (reader *positionReader) Read(payload []byte) (int, error) {
	read, err := reader.reader.Read(payload)
	increment, conversionErr := safecast.Conv[uint64](read)
	if conversionErr != nil || increment > math.MaxUint64-reader.offset || read > len(payload) {
		return 0, ErrCorrupt
	}
	reader.offset += increment
	return read, err
}

func decodeExactJSON(payload []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("search result artifact contains multiple JSON values")
		}
		return err
	}
	return nil
}

type contextHashReader struct {
	ctx    context.Context
	reader io.Reader
	hash   hash.Hash
	read   uint64
}

func (reader *contextHashReader) Read(payload []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	count, err := reader.reader.Read(payload)
	if count < 0 || count > len(payload) {
		return 0, ErrCorrupt
	}
	if count > 0 {
		increment, conversionErr := safecast.Conv[uint64](count)
		if conversionErr != nil || increment > math.MaxUint64-reader.read || count > len(payload) {
			return 0, ErrCorrupt
		}
		_, _ = reader.hash.Write(payload[:count])
		reader.read += increment
	}
	return count, err
}

type storedResultRow struct {
	Ordinal uint64        `json:"ordinal"`
	Values  []storedValue `json:"values"`
}

type storedObjectField struct {
	Name  string      `json:"name"`
	Value storedValue `json:"value"`
}

type storedValue struct {
	Kind      searchjobs.ValueKind `json:"kind"`
	String    string               `json:"string,omitempty"`
	Signed    int64                `json:"signed,omitempty"`
	Unsigned  uint64               `json:"unsigned,omitempty"`
	FloatBits uint64               `json:"float_bits,omitempty"`
	Bool      bool                 `json:"bool,omitempty"`
	Bytes     []byte               `json:"bytes,omitempty"`
	UnixNano  int64                `json:"unix_nano,omitempty"`
	Duration  int64                `json:"duration,omitempty"`
	Decimal   string               `json:"decimal,omitempty"`
	List      []storedValue        `json:"list,omitempty"`
	Object    []storedObjectField  `json:"object,omitempty"`
}

func encodeJob(job searchjobs.Job) ([]byte, error) {
	detached := job
	detached.Schema = nil
	var knowledge []byte
	if detached.KnowledgeSnapshot != nil {
		encoded, err := proto.Marshal(detached.KnowledgeSnapshot)
		if err != nil {
			return nil, err
		}
		knowledge = encoded
		detached.KnowledgeSnapshot = nil
	}
	return json.Marshal(storedJob{Job: detached, KnowledgeSnapshot: knowledge})
}

func decodeJob(payload []byte) (searchjobs.Job, error) {
	var stored storedJob
	if err := json.Unmarshal(payload, &stored); err != nil {
		return searchjobs.Job{}, err
	}
	if len(stored.KnowledgeSnapshot) != 0 {
		stored.Job.KnowledgeSnapshot = &opensplunk.KnowledgeSnapshotSummary{}
		if err := proto.Unmarshal(stored.KnowledgeSnapshot, stored.Job.KnowledgeSnapshot); err != nil {
			return searchjobs.Job{}, err
		}
	}
	return stored.Job, nil
}

func storedRow(row searchjobs.ResultRow) (storedResultRow, error) {
	values := make([]storedValue, len(row.Values))
	for index, value := range row.Values {
		encoded, err := storeValue(value)
		if err != nil {
			return storedResultRow{}, err
		}
		values[index] = encoded
	}
	return storedResultRow{Ordinal: row.Ordinal, Values: values}, nil
}

func restoreRow(row storedResultRow) (searchjobs.ResultRow, error) {
	values := make([]searchjobs.Value, len(row.Values))
	for index, value := range row.Values {
		decoded, err := restoreValue(value)
		if err != nil {
			return searchjobs.ResultRow{}, err
		}
		values[index] = decoded
	}
	return searchjobs.ResultRow{Ordinal: row.Ordinal, Values: values}, nil
}

func storeValue(value searchjobs.Value) (storedValue, error) {
	stored := storedValue{Kind: value.Kind()}
	switch value.Kind() {
	case searchjobs.ValueKindNull, searchjobs.ValueKindMissing:
	case searchjobs.ValueKindString:
		stored.String, _ = value.String()
	case searchjobs.ValueKindSigned:
		stored.Signed, _ = value.Signed()
	case searchjobs.ValueKindUnsigned:
		stored.Unsigned, _ = value.Unsigned()
	case searchjobs.ValueKindDouble:
		number, _ := value.Double()
		stored.FloatBits = math.Float64bits(number)
	case searchjobs.ValueKindBool:
		stored.Bool, _ = value.Bool()
	case searchjobs.ValueKindBytes:
		stored.Bytes, _ = value.Bytes()
	case searchjobs.ValueKindTime:
		stamp, _ := value.Time()
		stored.UnixNano = stamp.UnixNano()
	case searchjobs.ValueKindDuration:
		duration, _ := value.Duration()
		stored.Duration = int64(duration)
	case searchjobs.ValueKindDecimal:
		stored.Decimal, _ = value.Decimal()
	case searchjobs.ValueKindList:
		items, _ := value.List()
		stored.List = make([]storedValue, len(items))
		for index, item := range items {
			encoded, err := storeValue(item)
			if err != nil {
				return storedValue{}, err
			}
			stored.List[index] = encoded
		}
	case searchjobs.ValueKindObject:
		fields, _ := value.Object()
		stored.Object = make([]storedObjectField, len(fields))
		for index, field := range fields {
			encoded, err := storeValue(field.Value)
			if err != nil {
				return storedValue{}, err
			}
			stored.Object[index] = storedObjectField{Name: field.Name, Value: encoded}
		}
	default:
		return storedValue{}, errors.New("unsupported search result value kind")
	}
	return stored, nil
}

func restoreValue(stored storedValue) (searchjobs.Value, error) {
	switch stored.Kind {
	case searchjobs.ValueKindNull:
		return searchjobs.NullValue(), nil
	case searchjobs.ValueKindMissing:
		return searchjobs.MissingValue(), nil
	case searchjobs.ValueKindString:
		return searchjobs.StringValue(stored.String), nil
	case searchjobs.ValueKindSigned:
		return searchjobs.SignedValue(stored.Signed), nil
	case searchjobs.ValueKindUnsigned:
		return searchjobs.UnsignedValue(stored.Unsigned), nil
	case searchjobs.ValueKindDouble:
		return searchjobs.DoubleValue(math.Float64frombits(stored.FloatBits)), nil
	case searchjobs.ValueKindBool:
		return searchjobs.BoolValue(stored.Bool), nil
	case searchjobs.ValueKindBytes:
		return searchjobs.BytesValue(stored.Bytes), nil
	case searchjobs.ValueKindTime:
		return searchjobs.TimeValue(time.Unix(0, stored.UnixNano)), nil
	case searchjobs.ValueKindDuration:
		return searchjobs.DurationValue(time.Duration(stored.Duration)), nil
	case searchjobs.ValueKindDecimal:
		return searchjobs.DecimalValue(stored.Decimal)
	case searchjobs.ValueKindList:
		items := make([]searchjobs.Value, len(stored.List))
		for index, item := range stored.List {
			decoded, err := restoreValue(item)
			if err != nil {
				return searchjobs.Value{}, err
			}
			items[index] = decoded
		}
		value := searchjobs.ListValue(items...)
		if value.Kind() != searchjobs.ValueKindList {
			return searchjobs.Value{}, errors.New("stored search result list is invalid")
		}
		return value, nil
	case searchjobs.ValueKindObject:
		fields := make([]searchjobs.ObjectField, len(stored.Object))
		for index, field := range stored.Object {
			decoded, err := restoreValue(field.Value)
			if err != nil {
				return searchjobs.Value{}, err
			}
			fields[index] = searchjobs.ObjectField{Name: field.Name, Value: decoded}
		}
		return searchjobs.ObjectValue(fields...)
	default:
		return searchjobs.Value{}, errors.New("stored search result value kind is invalid")
	}
}
