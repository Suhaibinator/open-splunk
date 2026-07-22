package framing

import (
	"bytes"
	"errors"
	"io"
	"regexp"
	"strings"
	"testing"
	"testing/iotest"
)

// wantFrame is the expected shape of one emitted frame.
type wantFrame struct {
	bytes string
	start uint64
	end   uint64
	line  uint64
	err   error // nil, ErrEventTooLarge, ErrPartialFrame, or io.EOF
}

// drain runs f to exhaustion, returning every emitted frame paired with its
// terminating error (the loop stops on io.EOF, ErrPartialFrame, or a read
// error, which are recorded as a final frame-less entry).
func drainFramer(t *testing.T, f Framer) []wantFrame {
	t.Helper()
	var got []wantFrame
	for i := 0; i < 10000; i++ {
		fr, err := f.Next()
		switch {
		case err == nil:
			got = append(got, wantFrame{bytes: string(fr.Bytes), start: fr.StartOffset, end: fr.EndOffset, line: fr.LineNumber})
		case errors.Is(err, ErrEventTooLarge):
			got = append(got, wantFrame{bytes: string(fr.Bytes), start: fr.StartOffset, end: fr.EndOffset, line: fr.LineNumber, err: ErrEventTooLarge})
		case errors.Is(err, io.EOF):
			got = append(got, wantFrame{err: io.EOF})
			return got
		case errors.Is(err, ErrPartialFrame):
			got = append(got, wantFrame{err: ErrPartialFrame})
			return got
		default:
			t.Fatalf("unexpected error from Next: %v", err)
		}
	}
	t.Fatal("drainFramer: exceeded frame budget (probable infinite loop)")
	return nil
}

func eqFrames(t *testing.T, got, want []wantFrame) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("frame count = %d, want %d\n got=%#v\nwant=%#v", len(got), len(want), got, want)
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.bytes != w.bytes || g.start != w.start || g.end != w.end || g.line != w.line || !errors.Is(g.err, w.err) {
			t.Errorf("frame[%d] = {bytes=%q start=%d end=%d line=%d err=%v}, want {bytes=%q start=%d end=%d line=%d err=%v}",
				i, g.bytes, g.start, g.end, g.line, g.err, w.bytes, w.start, w.end, w.line, w.err)
		}
	}
}

func TestLineFramer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       string
		startOffset uint64
		maxBytes    int
		want        []wantFrame
	}{
		{
			name:  "three newline records",
			input: "alpha\nbeta\ngamma\n",
			want: []wantFrame{
				{bytes: "alpha", start: 0, end: 6, line: 1},
				{bytes: "beta", start: 6, end: 11, line: 2},
				{bytes: "gamma", start: 11, end: 17, line: 3},
				{err: io.EOF},
			},
		},
		{
			name:  "crlf stripped from bytes but counted in offset",
			input: "alpha\r\nbeta\r\n",
			want: []wantFrame{
				{bytes: "alpha", start: 0, end: 7, line: 1},
				{bytes: "beta", start: 7, end: 13, line: 2},
				{err: io.EOF},
			},
		},
		{
			name:  "mixed lf and crlf",
			input: "a\nb\r\nc\n",
			want: []wantFrame{
				{bytes: "a", start: 0, end: 2, line: 1},
				{bytes: "b", start: 2, end: 5, line: 2},
				{bytes: "c", start: 5, end: 7, line: 3},
				{err: io.EOF},
			},
		},
		{
			name:  "empty lines preserved",
			input: "\n\nx\n",
			want: []wantFrame{
				{bytes: "", start: 0, end: 1, line: 1},
				{bytes: "", start: 1, end: 2, line: 2},
				{bytes: "x", start: 2, end: 4, line: 3},
				{err: io.EOF},
			},
		},
		{
			name:  "unterminated trailing line is partial",
			input: "done\npartial",
			want: []wantFrame{
				{bytes: "done", start: 0, end: 5, line: 1},
				{err: ErrPartialFrame},
			},
		},
		{
			name:        "start offset seeds coordinates",
			input:       "one\ntwo\n",
			startOffset: 100,
			want: []wantFrame{
				{bytes: "one", start: 100, end: 104, line: 1},
				{bytes: "two", start: 104, end: 108, line: 2},
				{err: io.EOF},
			},
		},
		{
			name:     "oversized record then normal record",
			input:    "abcdefghij\nshort\n",
			maxBytes: 5,
			want: []wantFrame{
				{bytes: "abcde", start: 0, end: 11, line: 1, err: ErrEventTooLarge},
				{bytes: "short", start: 11, end: 17, line: 2},
				{err: io.EOF},
			},
		},
		{
			name:     "record exactly at cap is normal",
			input:    "abcd\nef\n",
			maxBytes: 4,
			want: []wantFrame{
				{bytes: "abcd", start: 0, end: 5, line: 1},
				{bytes: "ef", start: 5, end: 8, line: 2},
				{err: io.EOF},
			},
		},
		{
			name:     "oversized unterminated at eof spans to eof",
			input:    "keep\nabcdefgh",
			maxBytes: 4,
			want: []wantFrame{
				{bytes: "keep", start: 0, end: 5, line: 1},
				{bytes: "abcd", start: 5, end: 13, line: 2, err: ErrEventTooLarge},
				{err: io.EOF},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := NewLineFramer(strings.NewReader(tc.input), tc.startOffset, Options{MaxEventBytes: tc.maxBytes})
			if err != nil {
				t.Fatalf("NewLineFramer: %v", err)
			}
			eqFrames(t, drainFramer(t, f), tc.want)
		})
	}
}

func TestLineFramerOneByteReaderEquivalence(t *testing.T) {
	t.Parallel()

	inputs := []string{
		"alpha\nbeta\r\ngamma\n",
		"\n\n\n",
		"no-terminator",
		"a\nbb\nccc\ndddd\n",
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			bulk, err := NewLineFramer(strings.NewReader(in), 0, Options{})
			if err != nil {
				t.Fatalf("NewLineFramer bulk: %v", err)
			}
			oneByte, err := NewLineFramer(iotest.OneByteReader(strings.NewReader(in)), 0, Options{})
			if err != nil {
				t.Fatalf("NewLineFramer onebyte: %v", err)
			}
			eqFrames(t, drainFramer(t, oneByte), drainFramer(t, bulk))
		})
	}
}

func TestLineFramerPendingAndFlush(t *testing.T) {
	t.Parallel()

	f, err := NewLineFramer(strings.NewReader("committed\ntrailing"), 0, Options{})
	if err != nil {
		t.Fatalf("NewLineFramer: %v", err)
	}
	fr, err := f.Next()
	if err != nil {
		t.Fatalf("Next #1: %v", err)
	}
	if got := string(fr.Bytes); got != "committed" {
		t.Fatalf("frame 1 bytes = %q", got)
	}
	if _, err := f.Next(); !errors.Is(err, ErrPartialFrame) {
		t.Fatalf("Next #2 err = %v, want ErrPartialFrame", err)
	}
	start, length := f.Pending()
	if start != 10 || length != len("trailing") {
		t.Fatalf("Pending = (%d, %d), want (10, %d)", start, length, len("trailing"))
	}
	flushed, ok := f.Flush()
	if !ok {
		t.Fatal("Flush ok = false, want true")
	}
	if got := string(flushed.Bytes); got != "trailing" {
		t.Fatalf("flushed bytes = %q", got)
	}
	if flushed.StartOffset != 10 || flushed.EndOffset != 18 || flushed.LineNumber != 2 {
		t.Fatalf("flushed = {start=%d end=%d line=%d}", flushed.StartOffset, flushed.EndOffset, flushed.LineNumber)
	}
	if _, ok := f.Flush(); ok {
		t.Fatal("second Flush ok = true, want false")
	}
	if _, err := f.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after flush err = %v, want io.EOF", err)
	}
}

func TestLineFramerFlushEmptyBuffer(t *testing.T) {
	t.Parallel()
	f, err := NewLineFramer(strings.NewReader(""), 0, Options{})
	if err != nil {
		t.Fatalf("NewLineFramer: %v", err)
	}
	if _, ok := f.Flush(); ok {
		t.Fatal("Flush on empty reader ok = true, want false")
	}
}

func mustPattern(t *testing.T, expr string) *regexp.Regexp {
	t.Helper()
	re, err := regexp.Compile(expr)
	if err != nil {
		t.Fatalf("regexp.Compile(%q): %v", expr, err)
	}
	return re
}

func TestMultilineFramer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		pattern  string
		maxLines int
		maxBytes int
		want     []wantFrame
	}{
		{
			name:    "stack trace assembles into one event emitted at next start",
			input:   "ERROR boom\n\tat foo\n\tat bar\nERROR next\n",
			pattern: `^ERROR`,
			want: []wantFrame{
				{bytes: "ERROR boom\n\tat foo\n\tat bar", start: 0, end: 27, line: 1},
				// trailing "ERROR next\n" is unterminated (no following start) => partial
				{err: ErrPartialFrame},
			},
		},
		{
			name:    "two complete events",
			input:   "INFO a\n\tcont\nINFO b\nINFO c\n",
			pattern: `^INFO`,
			want: []wantFrame{
				{bytes: "INFO a\n\tcont", start: 0, end: 13, line: 1},
				{bytes: "INFO b", start: 13, end: 20, line: 3},
				{err: ErrPartialFrame},
			},
		},
		{
			name:    "leading non-matching lines form their own frame",
			input:   "preamble one\npreamble two\nSTART real\nSTART second\n",
			pattern: `^START`,
			want: []wantFrame{
				{bytes: "preamble one\npreamble two", start: 0, end: 26, line: 1},
				{bytes: "START real", start: 26, end: 37, line: 3},
				{err: ErrPartialFrame},
			},
		},
		{
			name:    "crlf continuation preserves internal bytes strips trailing",
			input:   "E a\r\n b\r\nE c\r\n",
			pattern: `^E `,
			want: []wantFrame{
				{bytes: "E a\r\n b", start: 0, end: 9, line: 1},
				{err: ErrPartialFrame},
			},
		},
		{
			name:     "maxlines bounds an event",
			input:    "S head\ncont1\ncont2\ncont3\nS next\n",
			pattern:  `^S `,
			maxLines: 2,
			want: []wantFrame{
				{bytes: "S head\ncont1", start: 0, end: 13, line: 1},
				{bytes: "cont2\ncont3", start: 13, end: 25, line: 3},
				{err: ErrPartialFrame},
			},
		},
		{
			name:    "start line immediately followed by start line",
			input:   "S one\nS two\nS three\n",
			pattern: `^S `,
			want: []wantFrame{
				{bytes: "S one", start: 0, end: 6, line: 1},
				{bytes: "S two", start: 6, end: 12, line: 2},
				{err: ErrPartialFrame},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := NewMultilineFramer(strings.NewReader(tc.input), 0, Options{
				LineStartPattern: mustPattern(t, tc.pattern),
				MaxLines:         tc.maxLines,
				MaxEventBytes:    tc.maxBytes,
			})
			if err != nil {
				t.Fatalf("NewMultilineFramer: %v", err)
			}
			eqFrames(t, drainFramer(t, f), tc.want)
		})
	}
}

func TestMultilineFramerFlushReleasesTrailingEvent(t *testing.T) {
	t.Parallel()

	input := "ERR boom\n\tat foo\n\tat bar\n"
	f, err := NewMultilineFramer(strings.NewReader(input), 0, Options{LineStartPattern: mustPattern(t, `^ERR`)})
	if err != nil {
		t.Fatalf("NewMultilineFramer: %v", err)
	}
	// The single logical event is never terminated by a following start line,
	// so Next reports it as partial until Flush releases it.
	if _, err := f.Next(); !errors.Is(err, ErrPartialFrame) {
		t.Fatalf("Next err = %v, want ErrPartialFrame", err)
	}
	start, length := f.Pending()
	if start != 0 || length != len(input) {
		t.Fatalf("Pending = (%d, %d), want (0, %d)", start, length, len(input))
	}
	fr, ok := f.Flush()
	if !ok {
		t.Fatal("Flush ok = false, want true")
	}
	if got, want := string(fr.Bytes), "ERR boom\n\tat foo\n\tat bar"; got != want {
		t.Fatalf("flushed bytes = %q, want %q", got, want)
	}
	if fr.StartOffset != 0 || fr.EndOffset != uint64(len(input)) || fr.LineNumber != 1 {
		t.Fatalf("flushed = {start=%d end=%d line=%d}", fr.StartOffset, fr.EndOffset, fr.LineNumber)
	}
	if _, err := f.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after flush err = %v, want io.EOF", err)
	}
}

func TestMultilineFramerOneByteReaderEquivalence(t *testing.T) {
	t.Parallel()

	inputs := []string{
		"ERROR a\n\tx\n\ty\nERROR b\n\tz\nERROR c\n",
		"lead\nERROR only\n\tframe\n",
		"ERROR crlf\r\n\tcont\r\nERROR two\r\n",
	}
	pat := mustPattern(t, `^ERROR`)
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			bulk, err := NewMultilineFramer(strings.NewReader(in), 0, Options{LineStartPattern: pat})
			if err != nil {
				t.Fatalf("bulk: %v", err)
			}
			oneByte, err := NewMultilineFramer(iotest.OneByteReader(strings.NewReader(in)), 0, Options{LineStartPattern: pat})
			if err != nil {
				t.Fatalf("onebyte: %v", err)
			}
			eqFrames(t, drainFramer(t, oneByte), drainFramer(t, bulk))
		})
	}
}

func TestMultilineFramerRequiresPattern(t *testing.T) {
	t.Parallel()
	if _, err := NewMultilineFramer(strings.NewReader("x"), 0, Options{}); err == nil {
		t.Fatal("NewMultilineFramer without LineStartPattern: err = nil, want error")
	}
}

func TestMultilineFramerOversizedEventThenNormal(t *testing.T) {
	t.Parallel()

	// The continuation line balloons past the cap with no delimiter reachable
	// within the budget; the framer skips to the next line and resumes.
	input := "S x\n" + strings.Repeat("y", 50) + "\nS z\nS w\n"
	f, err := NewMultilineFramer(strings.NewReader(input), 0, Options{
		LineStartPattern: mustPattern(t, `^S `),
		MaxEventBytes:    8,
	})
	if err != nil {
		t.Fatalf("NewMultilineFramer: %v", err)
	}
	got := drainFramer(t, f)
	// First frame must be the oversized record; last a partial trailing event.
	if len(got) < 2 {
		t.Fatalf("expected at least 2 frames, got %d: %#v", len(got), got)
	}
	if !errors.Is(got[0].err, ErrEventTooLarge) {
		t.Fatalf("frame[0] err = %v, want ErrEventTooLarge", got[0].err)
	}
	if got[0].start != 0 {
		t.Fatalf("frame[0] start = %d, want 0", got[0].start)
	}
	assertContiguous(t, got)
}

func TestNilReaderRejected(t *testing.T) {
	t.Parallel()
	if _, err := NewLineFramer(nil, 0, Options{}); err == nil {
		t.Fatal("NewLineFramer(nil): err = nil, want error")
	}
	if _, err := NewMultilineFramer(nil, 0, Options{LineStartPattern: mustPattern(t, `^x`)}); err == nil {
		t.Fatal("NewMultilineFramer(nil): err = nil, want error")
	}
}

// assertContiguous checks the successful/oversized frames are non-overlapping,
// contiguous, and forward-ordered. Terminal-only entries (io.EOF/partial with
// no bytes) are ignored.
func assertContiguous(t *testing.T, got []wantFrame) {
	t.Helper()
	var prevEnd uint64
	haveFirst := false
	for i, g := range got {
		if g.err != nil && g.err != ErrEventTooLarge {
			continue // terminal marker
		}
		if g.start > g.end {
			t.Fatalf("frame[%d] start %d > end %d", i, g.start, g.end)
		}
		if haveFirst && g.start != prevEnd {
			t.Fatalf("frame[%d] start %d != previous end %d (gap/overlap)", i, g.start, prevEnd)
		}
		prevEnd = g.end
		haveFirst = true
	}
}

func FuzzLineFramer(f *testing.F) {
	seeds := []string{
		"",
		"\n",
		"a\nb\n",
		"a\r\nb\r\n",
		"no-terminator",
		"trailing\npartial",
		"\n\n\n",
		"long-line-without-delimiter-exceeding-the-cap",
		"x\nyyyyyyyyyyyyyyyy\nz\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s), uint8(0))
	}

	f.Fuzz(func(t *testing.T, data []byte, maxRaw uint8) {
		maxBytes := int(maxRaw) // 0 => package default
		fr, err := NewLineFramer(bytes.NewReader(data), 0, Options{MaxEventBytes: maxBytes})
		if err != nil {
			t.Fatalf("NewLineFramer: %v", err)
		}
		total := uint64(len(data))
		var prevEnd uint64
		haveFrame := false
		for i := 0; i < len(data)+16; i++ {
			frame, err := fr.Next()
			isFrame := err == nil || errors.Is(err, ErrEventTooLarge)
			if isFrame {
				if frame.StartOffset > frame.EndOffset {
					t.Fatalf("start %d > end %d", frame.StartOffset, frame.EndOffset)
				}
				if frame.EndOffset > total {
					t.Fatalf("end %d > stream length %d", frame.EndOffset, total)
				}
				if haveFrame && frame.StartOffset != prevEnd {
					t.Fatalf("non-contiguous: start %d != prev end %d", frame.StartOffset, prevEnd)
				}
				if !errors.Is(err, ErrEventTooLarge) && uint64(len(frame.Bytes)) > frame.EndOffset-frame.StartOffset {
					t.Fatalf("bytes %d exceed span %d", len(frame.Bytes), frame.EndOffset-frame.StartOffset)
				}
				prevEnd = frame.EndOffset
				haveFrame = true
				continue
			}
			if errors.Is(err, io.EOF) || errors.Is(err, ErrPartialFrame) {
				// Pending bytes plus consumed offset must not exceed the stream.
				start, length := fr.Pending()
				if start+uint64(length) > total {
					t.Fatalf("pending end %d > stream length %d", start+uint64(length), total)
				}
				if errors.Is(err, ErrPartialFrame) && (!haveFrame && start != 0) {
					// first partial must start at stream origin
					t.Fatalf("partial start %d, want 0", start)
				}
				return
			}
			t.Fatalf("unexpected error: %v", err)
		}
		t.Fatal("did not terminate within frame budget")
	})
}
