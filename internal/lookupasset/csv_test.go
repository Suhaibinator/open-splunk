package lookupasset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseCSVNormalizesAndDetaches(t *testing.T) {
	source := append(bytes.Clone(utf8BOM), []byte("service_id,owner,tier\r\nsvc-1,alice,one\r\nsvc-2,\"ops, west\",\"two\"\r\n")...)
	asset, err := ParseCSV(bytes.NewReader(source), Limits{})
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}
	if got, want := string(asset.CanonicalCSV()), "service_id,owner,tier\nsvc-1,alice,one\nsvc-2,\"ops, west\",two\n"; got != want {
		t.Fatalf("canonical CSV = %q, want %q", got, want)
	}
	if asset.SourceSHA256() != sha256.Sum256(source) {
		t.Fatal("source digest does not authenticate original BOM/CRLF bytes")
	}
	if asset.ContentSHA256() != sha256.Sum256(asset.CanonicalCSV()) {
		t.Fatal("content digest does not authenticate canonical bytes")
	}
	if asset.RowCount() != 2 || asset.ColumnCount() != 3 ||
		asset.SourceBytes() != uint64(len(source)) ||
		asset.CanonicalSizeBytes() != uint64(len(asset.CanonicalCSV())) {
		t.Fatalf(
			"unexpected dimensions: rows=%d columns=%d source=%d canonical=%d",
			asset.RowCount(),
			asset.ColumnCount(),
			asset.SourceBytes(),
			asset.CanonicalSizeBytes(),
		)
	}

	headers := asset.Headers()
	headers[0] = "changed"
	rows := asset.Rows()
	rows[0][0] = "changed"
	canonical := asset.CanonicalCSV()
	canonical[0] = 'X'
	if got := asset.Headers()[0]; got != "service_id" {
		t.Fatalf("caller mutated headers through accessor: %q", got)
	}
	row, ok := asset.Row(0)
	if !ok || row[0] != "svc-1" {
		t.Fatalf("caller mutated rows through accessor: %v, %v", row, ok)
	}
	if header, ok := asset.Header(0); !ok || header != "service_id" {
		t.Fatalf("Header(0) = %q, %v", header, ok)
	}
	if cell, ok := asset.Cell(1, 1); !ok || cell != "ops, west" {
		t.Fatalf("Cell(1, 1) = %q, %v", cell, ok)
	}
	if _, ok := asset.Header(-1); ok {
		t.Fatal("negative header ordinal unexpectedly resolved")
	}
	if _, ok := asset.Cell(0, int(asset.ColumnCount())); ok {
		t.Fatal("out-of-range cell ordinal unexpectedly resolved")
	}
	if got := asset.CanonicalCSV()[0]; got != 's' {
		t.Fatalf("caller mutated canonical bytes through accessor: %q", got)
	}
	if _, ok := asset.Row(-1); ok {
		t.Fatal("negative row ordinal unexpectedly resolved")
	}
}

func TestParseCSVEquivalentSourcesHaveCanonicalIdentity(t *testing.T) {
	left, err := ParseCSV(strings.NewReader("key,value\r\na,\"plain\"\r\n"), Limits{})
	if err != nil {
		t.Fatalf("parse left: %v", err)
	}
	right, err := ParseCSV(strings.NewReader("key,value\na,plain\n"), Limits{})
	if err != nil {
		t.Fatalf("parse right: %v", err)
	}
	if left.SourceSHA256() == right.SourceSHA256() {
		t.Fatal("different source encodings unexpectedly have the same source identity")
	}
	if left.ContentSHA256() != right.ContentSHA256() || !bytes.Equal(left.CanonicalCSV(), right.CanonicalCSV()) {
		t.Fatal("equivalent CSV sources did not normalize to one content identity")
	}
}

func TestParseCSVEmptyCellsAreExactStrings(t *testing.T) {
	asset, err := ParseCSV(strings.NewReader("key,value\n,\na,\n"), Limits{})
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}
	rows := asset.Rows()
	if rows[0][0] != "" || rows[0][1] != "" || rows[1][1] != "" {
		t.Fatalf("empty fields were not retained as exact empty strings: %#v", rows)
	}
}

func TestParseCSVRetainsBlankPhysicalRecords(t *testing.T) {
	asset, err := ParseCSV(strings.NewReader("key\n\n"), Limits{})
	if err != nil {
		t.Fatalf("parse one-column blank record: %v", err)
	}
	if asset.RowCount() != 1 {
		t.Fatalf("row count = %d, want 1", asset.RowCount())
	}
	row, ok := asset.Row(0)
	if !ok || len(row) != 1 || row[0] != "" {
		t.Fatalf("blank record = %#v, %v", row, ok)
	}
	if got, want := string(asset.CanonicalCSV()), "key\n\"\"\n"; got != want {
		t.Fatalf("canonical blank record = %q, want %q", got, want)
	}
	reparsed, err := ParseCSV(bytes.NewReader(asset.CanonicalCSV()), Limits{})
	if err != nil || reparsed.RowCount() != 1 || reparsed.ContentSHA256() != asset.ContentSHA256() {
		t.Fatalf("reparse canonical blank record = %#v, %v", reparsed, err)
	}

	crlf, err := ParseCSV(strings.NewReader("key\r\n\r\n"), Limits{})
	if err != nil || crlf.RowCount() != 1 {
		t.Fatalf("parse CRLF blank record = %#v, %v", crlf, err)
	}
}

func TestParseCSVBlankLineSemanticsRespectWidthAndQuotedNewlines(t *testing.T) {
	if _, err := ParseCSV(strings.NewReader("a,b\n\n"), Limits{}); !errors.Is(err, ErrMalformedCSV) {
		t.Fatalf("two-column blank record error = %v, want malformed CSV", err)
	}
	asset, err := ParseCSV(strings.NewReader("key\n\"a\n\nb\"\n"), Limits{})
	if err != nil {
		t.Fatalf("parse quoted blank physical line: %v", err)
	}
	row, ok := asset.Row(0)
	if !ok || len(row) != 1 || row[0] != "a\n\nb" {
		t.Fatalf("quoted multiline record = %#v, %v", row, ok)
	}
	if _, err := ParseCSV(strings.NewReader("\nkey\n"), Limits{}); !errors.Is(err, ErrMalformedCSV) {
		t.Fatalf("blank header error = %v, want malformed CSV", err)
	}
}

func TestParseCSVRejectsMalformedAndBoundedInputs(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		limits Limits
		want   error
	}{
		{name: "empty", input: "", want: ErrMalformedCSV},
		{name: "bom only", input: string(utf8BOM), want: ErrMalformedCSV},
		{name: "invalid utf8", input: "key\n\xff\n", want: ErrMalformedCSV},
		{name: "unclosed quote", input: "key\n\"value\n", want: ErrMalformedCSV},
		{name: "duplicate header", input: "key,key\na,b\n", want: ErrMalformedCSV},
		{name: "empty header", input: "key,\na,b\n", want: ErrMalformedCSV},
		{name: "header whitespace", input: " key,value\na,b\n", want: ErrMalformedCSV},
		{name: "header control", input: "key,\"bad\nname\"\na,b\n", want: ErrMalformedCSV},
		{name: "header format control", input: "key,bad\u200bname\na,b\n", want: ErrMalformedCSV},
		{name: "nul cell", input: "key\na\x00b\n", want: ErrMalformedCSV},
		{name: "field count", input: "key,value\na\n", want: ErrMalformedCSV},
		{name: "source bound", input: "key\nvalue\n", limits: Limits{SourceBytes: 8}, want: ErrResourceLimit},
		{name: "column bound", input: "a,b\n1,2\n", limits: Limits{Columns: 1}, want: ErrResourceLimit},
		{name: "header bound", input: "long\na\n", limits: Limits{HeaderBytes: 3}, want: ErrResourceLimit},
		{name: "cell bound", input: "key\nlong\n", limits: Limits{CellBytes: 3, HeaderBytes: 3}, want: ErrResourceLimit},
		{name: "row bound", input: "a,b\n12,34\n", limits: Limits{CellBytes: 3, HeaderBytes: 1, RowBytes: 3}, want: ErrResourceLimit},
		{name: "row count", input: "key\na\nb\n", limits: Limits{Rows: 1}, want: ErrResourceLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseCSV(strings.NewReader(test.input), test.limits)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, test.want)
			}
		})
	}
}

func TestParseCSVRejectsInvalidLimitsAndReaderFailure(t *testing.T) {
	if _, err := ParseCSV(nil, Limits{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil source error = %v", err)
	}
	if _, err := ParseCSV(strings.NewReader("key\na\n"), Limits{Rows: MaximumRows + 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized limits error = %v", err)
	}
	if _, err := ParseCSV(strings.NewReader("key\na\n"), Limits{HeaderBytes: 3, CellBytes: 2}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("inconsistent limits error = %v", err)
	}
	readErr := errors.New("source failed")
	if _, err := ParseCSV(errorReader{err: readErr}, Limits{}); !errors.Is(err, readErr) {
		t.Fatalf("reader failure = %v", err)
	}
}

func TestParseCSVContextRejectsNilAndPreCanceledContexts(t *testing.T) {
	var nilContext context.Context

	if _, err := ParseCSVContext(nilContext, strings.NewReader("key\na\n"), Limits{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil context error = %v, want invalid argument", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ParseCSVContext(ctx, strings.NewReader("key\na\n"), Limits{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled parse error = %v, want context cancellation", err)
	}
}

func TestParseCSVContextCancelsDuringSourceRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &cancelingCSVReader{
		reader:   strings.NewReader("key,value\n" + strings.Repeat("a,b\n", 2_000)),
		cancel:   cancel,
		cancelAt: 3,
		chunk:    64,
	}
	if _, err := ParseCSVContext(ctx, reader, Limits{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-read parse error = %v, want context cancellation", err)
	}
	if reader.calls != reader.cancelAt {
		t.Fatalf("source read calls = %d, want cancellation on call %d", reader.calls, reader.cancelAt)
	}
}

func TestParseCSVContextCancelsDuringPostReadWork(t *testing.T) {
	ctx := newCheckpointCancellationContext()
	reader := &eofArmingCSVReader{
		reader: strings.NewReader("key,value\n" + strings.Repeat("a,b\n", 2_000)),
		ctx:    ctx,
	}
	if _, err := ParseCSVContext(ctx, reader, Limits{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-read parse error = %v, want context cancellation", err)
	}
	if !reader.reachedEOF {
		t.Fatal("parse canceled before the complete bounded source was read")
	}
}

type errorReader struct{ err error }

func (reader errorReader) Read([]byte) (int, error) { return 0, reader.err }

type cancelingCSVReader struct {
	reader   io.Reader
	cancel   context.CancelFunc
	cancelAt int
	chunk    int
	calls    int
}

func (reader *cancelingCSVReader) Read(destination []byte) (int, error) {
	reader.calls++
	if len(destination) > reader.chunk {
		destination = destination[:reader.chunk]
	}
	read, err := reader.reader.Read(destination)
	if reader.calls == reader.cancelAt {
		reader.cancel()
	}
	return read, err
}

// checkpointCancellationContext is armed after its source reaches EOF. The
// next two context observations let the reader and io.ReadAll finish; the
// third deterministically cancels inside ParseCSVContext's bounded CPU work.
type checkpointCancellationContext struct {
	mu        sync.Mutex
	done      chan struct{}
	armed     bool
	remaining int
	canceled  bool
}

func newCheckpointCancellationContext() *checkpointCancellationContext {
	return &checkpointCancellationContext{done: make(chan struct{})}
}

func (ctx *checkpointCancellationContext) arm(checkpoints int) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if !ctx.armed && !ctx.canceled {
		ctx.armed = true
		ctx.remaining = checkpoints
	}
}

func (*checkpointCancellationContext) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (ctx *checkpointCancellationContext) Done() <-chan struct{} { return ctx.done }

func (ctx *checkpointCancellationContext) Err() error {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.canceled {
		return context.Canceled
	}
	if !ctx.armed {
		return nil
	}
	ctx.remaining--
	if ctx.remaining > 0 {
		return nil
	}
	ctx.canceled = true
	close(ctx.done)
	return context.Canceled
}

func (*checkpointCancellationContext) Value(any) any { return nil }

type eofArmingCSVReader struct {
	reader     io.Reader
	ctx        *checkpointCancellationContext
	reachedEOF bool
}

func (reader *eofArmingCSVReader) Read(destination []byte) (int, error) {
	read, err := reader.reader.Read(destination)
	if errors.Is(err, io.EOF) {
		reader.reachedEOF = true
		reader.ctx.arm(3)
	}
	return read, err
}

func FuzzParseCSVIsBoundedAndCanonical(f *testing.F) {
	f.Add([]byte("key,value\na,b\n"))
	f.Add([]byte("key\n\"multi\nline\"\n"))
	f.Add([]byte{0xff, 0xfe})
	f.Fuzz(func(t *testing.T, input []byte) {
		limits := Limits{SourceBytes: 4096, Rows: 64, Columns: 8, CellBytes: 512, RowBytes: 1024, HeaderBytes: 64}
		asset, err := ParseCSV(bytes.NewReader(input), limits)
		if err != nil {
			return
		}
		if len(asset.CanonicalCSV()) > limits.SourceBytes || asset.RowCount() > uint64(limits.Rows) || asset.ColumnCount() > uint32(limits.Columns) {
			t.Fatal("successful parse exceeded a configured resource limit")
		}
		reparsed, err := ParseCSV(bytes.NewReader(asset.CanonicalCSV()), limits)
		if err != nil {
			t.Fatalf("canonical output did not parse: %v", err)
		}
		if reparsed.ContentSHA256() != asset.ContentSHA256() || !bytes.Equal(reparsed.CanonicalCSV(), asset.CanonicalCSV()) {
			t.Fatal("canonical parse was not idempotent")
		}
	})
}

var _ io.Reader = errorReader{}
