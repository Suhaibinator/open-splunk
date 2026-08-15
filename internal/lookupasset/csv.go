package lookupasset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

var utf8BOM = []byte{0xef, 0xbb, 0xbf}

const csvContextCheckpointBytes = 4 << 10

// ParseCSV reads, verifies, and normalizes one complete CSV document. It reads
// at most limits.SourceBytes+1 bytes, so an oversized or adversarial source is
// rejected without unbounded buffering. The first record is the header and is
// not included in RowCount. Empty cells are exact empty strings, never nulls.
func ParseCSV(source io.Reader, requested Limits) (*Asset, error) {
	return ParseCSVContext(context.Background(), source, requested)
}

// ParseCSVContext is ParseCSV with cancellation and deadline support. It
// checks ctx while reading and throughout the bounded parsing,
// canonicalization, and digest work so callers can abandon a maximum-sized
// document without waiting for the complete verification pass.
func ParseCSVContext(ctx context.Context, source io.Reader, requested Limits) (*Asset, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: CSV context is required", ErrInvalidArgument)
	}
	if source == nil {
		return nil, fmt.Errorf("%w: CSV source is required", ErrInvalidArgument)
	}
	limits, err := normalizeLimits(requested)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	limited := &io.LimitedReader{
		R: &contextReader{ctx: ctx, reader: source},
		N: int64(limits.SourceBytes) + 1,
	}
	raw, err := io.ReadAll(limited)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		return nil, fmt.Errorf("read lookup CSV: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(raw) > limits.SourceBytes {
		return nil, fmt.Errorf("%w: CSV source exceeds %d bytes", ErrResourceLimit, limits.SourceBytes)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: CSV source is empty", ErrMalformedCSV)
	}
	sourceDigest, err := digestCSVContext(ctx, raw)
	if err != nil {
		return nil, err
	}
	parseBytes := bytes.TrimPrefix(raw, utf8BOM)
	validUTF8, err := validCSVUTF8Context(ctx, parseBytes)
	if err != nil {
		return nil, err
	}
	if len(parseBytes) == 0 || !validUTF8 {
		return nil, fmt.Errorf("%w: CSV source must be valid nonempty UTF-8", ErrMalformedCSV)
	}
	// encoding/csv intentionally ignores physical blank lines. Lookup CSVs do
	// not: outside a quoted field, a blank line is one record containing one
	// exact empty string. Make that record explicit before handing the bounded
	// source to the standard parser. This also makes a one-column empty record
	// survive canonicalization and reparsing.
	parseBytes, err = preserveBlankCSVRecordsContext(ctx, parseBytes)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(&contextReader{ctx: ctx, reader: bytes.NewReader(parseBytes)})
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = false
	reader.TrimLeadingSpace = false
	reader.ReuseRecord = false

	headers, err := reader.Read()
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		return nil, csvReadError("read header", err)
	}
	if err := validateHeaderContext(ctx, headers, limits); err != nil {
		return nil, err
	}
	reader.FieldsPerRecord = len(headers)

	rows := make([][]string, 0, min(limits.Rows, 1024))
	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			break
		}
		if readErr != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return nil, contextErr
			}
			return nil, csvReadError("read data row", readErr)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(rows) == limits.Rows {
			return nil, fmt.Errorf("%w: CSV contains more than %d data rows", ErrResourceLimit, limits.Rows)
		}
		rowBytes := 0
		for cellIndex, cell := range record {
			if cellIndex&7 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if strings.IndexByte(cell, 0) >= 0 {
				return nil, fmt.Errorf("%w: CSV cell contains NUL", ErrMalformedCSV)
			}
			if len(cell) > limits.CellBytes {
				return nil, fmt.Errorf("%w: CSV cell exceeds %d decoded bytes", ErrResourceLimit, limits.CellBytes)
			}
			rowBytes += len(cell)
			if rowBytes > limits.RowBytes {
				return nil, fmt.Errorf("%w: CSV row exceeds %d decoded bytes", ErrResourceLimit, limits.RowBytes)
			}
		}
		rows = append(rows, record)
	}

	canonical, err := marshalCanonicalCSVContext(ctx, headers, rows, limits.SourceBytes)
	if err != nil {
		return nil, err
	}
	digest, err := digestCSVContext(ctx, canonical)
	if err != nil {
		return nil, err
	}
	ordinals := make(map[string]int, len(headers))
	for index, header := range headers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ordinals[header] = index
	}
	return &Asset{
		headers:        headers,
		rows:           rows,
		canonicalCSV:   canonical,
		contentSHA256:  digest,
		sourceSHA256:   sourceDigest,
		sourceBytes:    uint64(len(raw)),
		headerOrdinals: ordinals,
	}, nil
}

func validateHeaderContext(ctx context.Context, headers []string, limits Limits) error {
	if len(headers) == 0 {
		return fmt.Errorf("%w: CSV header is empty", ErrMalformedCSV)
	}
	if len(headers) > limits.Columns {
		return fmt.Errorf("%w: CSV contains more than %d columns", ErrResourceLimit, limits.Columns)
	}
	seen := make(map[string]struct{}, len(headers))
	rowBytes := 0
	for _, header := range headers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if header == "" || strings.TrimSpace(header) != header {
			return fmt.Errorf("%w: CSV headers must be nonempty and have no surrounding whitespace", ErrMalformedCSV)
		}
		if len(header) > limits.HeaderBytes {
			return fmt.Errorf("%w: CSV header exceeds %d bytes", ErrResourceLimit, limits.HeaderBytes)
		}
		if strings.IndexByte(header, 0) >= 0 || strings.ContainsFunc(header, func(character rune) bool {
			return unicode.IsControl(character) || unicode.Is(unicode.Cf, character)
		}) {
			return fmt.Errorf("%w: CSV header contains a control character", ErrMalformedCSV)
		}
		if _, duplicate := seen[header]; duplicate {
			return fmt.Errorf("%w: duplicate CSV header %q", ErrMalformedCSV, header)
		}
		seen[header] = struct{}{}
		rowBytes += len(header)
		if rowBytes > limits.RowBytes {
			return fmt.Errorf("%w: CSV header row exceeds %d decoded bytes", ErrResourceLimit, limits.RowBytes)
		}
	}
	return nil
}

func csvReadError(operation string, err error) error {
	var parseError *csv.ParseError
	if errors.As(err, &parseError) {
		return fmt.Errorf("%w: %s at line %d, column %d", ErrMalformedCSV, operation, parseError.Line, parseError.Column)
	}
	return fmt.Errorf("%w: %s", ErrMalformedCSV, operation)
}

func marshalCanonicalCSVContext(ctx context.Context, headers []string, rows [][]string, maximumBytes int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	bounded := &boundedBuffer{ctx: ctx, buffer: &output, maximum: maximumBytes}
	writer := csv.NewWriter(bounded)
	writer.UseCRLF = false
	if err := writer.Write(headers); err != nil {
		return nil, canonicalCSVWriteError(ctx, maximumBytes)
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// csv.Writer serializes []string{""} as a blank physical line, which
		// csv.Reader skips. Use the RFC 4180-equivalent quoted spelling so the
		// canonical document is self-parsing and retains the row.
		if len(row) == 1 && row[0] == "" {
			writer.Flush()
			if writer.Error() != nil {
				return nil, canonicalCSVWriteError(ctx, maximumBytes)
			}
			if _, err := bounded.Write([]byte("\"\"\n")); err != nil {
				return nil, canonicalCSVWriteError(ctx, maximumBytes)
			}
			continue
		}
		if err := writer.Write(row); err != nil {
			return nil, canonicalCSVWriteError(ctx, maximumBytes)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, canonicalCSVWriteError(ctx, maximumBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	canonical := bytes.Clone(output.Bytes())
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return canonical, nil
}

func canonicalCSVWriteError(ctx context.Context, maximumBytes int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: canonical CSV exceeds %d bytes", ErrResourceLimit, maximumBytes)
}

// preserveBlankCSVRecords rewrites only blank physical lines outside quoted
// fields. It leaves malformed quote handling to encoding/csv and returns the
// original slice without allocating when no rewrite is needed.
func preserveBlankCSVRecordsContext(ctx context.Context, source []byte) ([]byte, error) {
	inQuotes := false
	lineStart := 0
	firstBlank := -1
	nextCheckpoint := 0
	for index := 0; index < len(source); index++ {
		if index >= nextCheckpoint {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			nextCheckpoint = index + csvContextCheckpointBytes
		}
		switch source[index] {
		case '"':
			if inQuotes && index+1 < len(source) && source[index+1] == '"' {
				index++
				continue
			}
			inQuotes = !inQuotes
		case '\n':
			if !inQuotes && blankPhysicalLine(source, lineStart, index) {
				firstBlank = index
				index = len(source)
				continue
			}
			if !inQuotes {
				lineStart = index + 1
			}
		}
	}
	if firstBlank < 0 {
		return source, ctx.Err()
	}

	output := make([]byte, 0, len(source)+16)
	inQuotes = false
	lineStart = 0
	nextCheckpoint = 0
	for index := 0; index < len(source); index++ {
		if index >= nextCheckpoint {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			nextCheckpoint = index + csvContextCheckpointBytes
		}
		switch source[index] {
		case '"':
			if inQuotes && index+1 < len(source) && source[index+1] == '"' {
				output = append(output, source[index], source[index+1])
				index++
				continue
			}
			inQuotes = !inQuotes
		case '\n':
			if !inQuotes && blankPhysicalLine(source, lineStart, index) {
				// Preserve CRLF exactly around the inserted quoted empty cell.
				if index > lineStart && source[index-1] == '\r' {
					output = output[:len(output)-1]
					output = append(output, '"', '"', '\r')
				} else {
					output = append(output, '"', '"')
				}
			}
			lineStart = index + 1
		}
		output = append(output, source[index])
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return output, nil
}

func blankPhysicalLine(source []byte, start, newline int) bool {
	return newline == start || newline == start+1 && source[start] == '\r'
}

type boundedBuffer struct {
	ctx     context.Context
	buffer  *bytes.Buffer
	maximum int
}

func (writer *boundedBuffer) Write(value []byte) (int, error) {
	if err := writer.ctx.Err(); err != nil {
		return 0, err
	}
	if len(value) > writer.maximum-writer.buffer.Len() {
		return 0, ErrResourceLimit
	}
	written, err := writer.buffer.Write(value)
	if contextErr := writer.ctx.Err(); contextErr != nil {
		return written, contextErr
	}
	return written, err
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(destination []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	read, err := reader.reader.Read(destination)
	if contextErr := reader.ctx.Err(); contextErr != nil {
		return read, contextErr
	}
	return read, err
}

func digestCSVContext(ctx context.Context, value []byte) ([sha256.Size]byte, error) {
	digest := sha256.New()
	for offset := 0; offset < len(value); offset += csvContextCheckpointBytes {
		if err := ctx.Err(); err != nil {
			return [sha256.Size]byte{}, err
		}
		end := min(offset+csvContextCheckpointBytes, len(value))
		_, _ = digest.Write(value[offset:end])
	}
	if err := ctx.Err(); err != nil {
		return [sha256.Size]byte{}, err
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func validCSVUTF8Context(ctx context.Context, value []byte) (bool, error) {
	nextCheckpoint := 0
	for offset := 0; offset < len(value); {
		if offset >= nextCheckpoint {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			nextCheckpoint = offset + csvContextCheckpointBytes
		}
		character, size := utf8.DecodeRune(value[offset:])
		if character == utf8.RuneError && size == 1 {
			return false, nil
		}
		offset += size
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}
