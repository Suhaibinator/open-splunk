package hec

import (
	"bufio"
	"compress/gzip"
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

// NewBodyReader applies the compressed and decompressed byte ceilings while
// exposing either an identity body or exactly one gzip member. The caller must
// read through EOF to prove gzip checksum/trailing-data and exact-size limits.
func NewBodyReader(source io.Reader, contentEncoding string, limits Limits) (io.ReadCloser, error) {
	if source == nil {
		return nil, NewProtocolError(ErrorInternal, errors.New("HEC body reader is nil"))
	}
	if err := limits.Validate(); err != nil {
		return nil, NewProtocolError(ErrorInternal, err)
	}
	if contentEncoding == "" {
		return &readCloser{
			Reader: newHardLimitReader(source, limits.MaximumDecompressedBodyBytes, ErrorDecompressedBodyTooLarge),
		}, nil
	}
	if !strings.EqualFold(contentEncoding, "gzip") {
		return nil, NewProtocolError(ErrorUnsupportedContentEncoding, nil)
	}

	compressed := newHardLimitReader(source, limits.MaximumCompressedBodyBytes, ErrorCompressedBodyTooLarge)
	buffered := bufio.NewReaderSize(compressed, 32<<10)
	member, err := gzip.NewReader(buffered)
	if err != nil {
		if _, ok := ErrorKindOf(err); ok {
			return nil, err
		}
		return nil, NewProtocolError(ErrorInvalidCompressedBody, err)
	}
	member.Multistream(false)
	singleMember := &singleGzipMemberReader{member: member, source: buffered}
	return &readCloser{
		Reader: newHardLimitReader(singleMember, limits.MaximumDecompressedBodyBytes, ErrorDecompressedBodyTooLarge),
		close:  member.Close,
	}, nil
}

type readCloser struct {
	io.Reader
	close func() error
}

func (reader *readCloser) Close() error {
	if reader == nil || reader.close == nil {
		return nil
	}
	return reader.close()
}

type hardLimitReader struct {
	source io.Reader
	limit  int64
	read   int64
	kind   ErrorKind
	failed error
}

func newHardLimitReader(source io.Reader, limit int64, kind ErrorKind) *hardLimitReader {
	return &hardLimitReader{source: source, limit: limit, kind: kind}
}

func (reader *hardLimitReader) Read(destination []byte) (int, error) {
	if reader.failed != nil {
		return 0, reader.failed
	}
	if len(destination) == 0 {
		return 0, nil
	}
	remaining := reader.limit - reader.read
	if remaining < 0 {
		reader.failed = NewProtocolError(reader.kind, nil)
		return 0, reader.failed
	}
	maximumRead := int64(len(destination))
	if maximumRead > remaining+1 {
		maximumRead = remaining + 1
	}
	n, err := reader.source.Read(destination[:int(maximumRead)])
	if int64(n) > remaining {
		allowed := int(remaining)
		reader.read += int64(allowed)
		reader.failed = NewProtocolError(reader.kind, nil)
		return allowed, reader.failed
	}
	reader.read += int64(n)
	return n, err
}

type singleGzipMemberReader struct {
	member   *gzip.Reader
	source   *bufio.Reader
	finished bool
	failed   error
}

func (reader *singleGzipMemberReader) Read(destination []byte) (int, error) {
	if reader.failed != nil {
		return 0, reader.failed
	}
	if reader.finished {
		return 0, io.EOF
	}
	n, err := reader.member.Read(destination)
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, io.EOF) {
		if _, ok := ErrorKindOf(err); ok {
			reader.failed = err
		} else {
			reader.failed = NewProtocolError(ErrorInvalidCompressedBody, err)
		}
		return n, reader.failed
	}
	if n > 0 {
		// Verify trailing data on the next read so bytes already produced are not
		// discarded by a caller which follows the io.Reader contract.
		return n, nil
	}
	value, trailingErr := reader.source.ReadByte()
	if trailingErr == nil {
		_ = value
		reader.failed = NewProtocolError(ErrorInvalidCompressedBody, errors.New("gzip body contains trailing data or another member"))
		return 0, reader.failed
	}
	if !errors.Is(trailingErr, io.EOF) {
		if _, ok := ErrorKindOf(trailingErr); ok {
			reader.failed = trailingErr
		} else {
			reader.failed = NewProtocolError(ErrorInvalidCompressedBody, trailingErr)
		}
		return 0, reader.failed
	}
	reader.finished = true
	return 0, io.EOF
}

// newUTF8ValidatingReader rejects malformed source UTF-8 without normalizing
// it through encoding/json's replacement-rune behavior. Escaped JSON Unicode
// remains ASCII on this boundary and is interpreted later by the decoder.
func newUTF8ValidatingReader(source io.Reader) io.Reader {
	return &utf8ValidatingReader{source: source}
}

type utf8ValidatingReader struct {
	source   io.Reader
	ready    []byte
	pending  []byte
	terminal error
}

func (reader *utf8ValidatingReader) Read(destination []byte) (int, error) {
	if len(destination) == 0 {
		return 0, nil
	}
	for len(reader.ready) == 0 && reader.terminal == nil {
		if !reader.fill() {
			return 0, nil
		}
	}
	if len(reader.ready) != 0 {
		n := copy(destination, reader.ready)
		reader.ready = reader.ready[n:]
		return n, nil
	}
	return 0, reader.terminal
}

func (reader *utf8ValidatingReader) fill() bool {
	buffer := make([]byte, 32<<10)
	n, err := reader.source.Read(buffer)
	if n == 0 && err == nil {
		return false
	}
	data := make([]byte, 0, len(reader.pending)+n)
	data = append(data, reader.pending...)
	data = append(data, buffer[:n]...)
	reader.pending = nil
	reader.validateChunk(data, errors.Is(err, io.EOF))
	if err != nil && !errors.Is(err, io.EOF) && reader.terminal == nil {
		reader.terminal = err
	}
	if errors.Is(err, io.EOF) && reader.terminal == nil {
		reader.terminal = io.EOF
	}
	return true
}

func (reader *utf8ValidatingReader) validateChunk(source []byte, final bool) {
	position := 0
	for position < len(source) {
		lead := source[position]
		width := utf8LeadWidth(lead)
		if width == 0 {
			reader.ready = append(reader.ready, source[:position]...)
			reader.terminal = NewProtocolError(ErrorInvalidUTF8, errors.New("invalid UTF-8 leading byte"))
			return
		}
		if width == 1 {
			position++
			continue
		}
		if len(source)-position < width {
			reader.ready = append(reader.ready, source[:position]...)
			if final {
				reader.terminal = NewProtocolError(ErrorInvalidUTF8, errors.New("truncated UTF-8 sequence"))
			} else {
				reader.pending = append(reader.pending, source[position:]...)
			}
			return
		}
		if !utf8.Valid(source[position : position+width]) {
			reader.ready = append(reader.ready, source[:position]...)
			reader.terminal = NewProtocolError(ErrorInvalidUTF8, errors.New("invalid UTF-8 sequence"))
			return
		}
		position += width
	}
	reader.ready = append(reader.ready, source...)
}

func utf8LeadWidth(value byte) int {
	switch {
	case value <= 0x7f:
		return 1
	case value >= 0xc2 && value <= 0xdf:
		return 2
	case value >= 0xe0 && value <= 0xef:
		return 3
	case value >= 0xf0 && value <= 0xf4:
		return 4
	default:
		return 0
	}
}
