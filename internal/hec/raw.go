package hec

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"unicode/utf8"
)

// RawDecoder implements the fixed breaker: LF terminates, one immediately
// preceding CR is stripped, empty resulting segments are skipped, and a final
// nonempty unterminated segment is emitted. It carries no state across HTTP
// requests.
type RawDecoder struct {
	reader     *bufio.Reader
	limits     Limits
	nextNumber int
	failed     error
	done       bool
}

// NewRawDecoder constructs a bounded streaming raw breaker. The source should
// normally already be wrapped by NewBodyReader.
func NewRawDecoder(source io.Reader, limits Limits) (*RawDecoder, error) {
	if source == nil {
		return nil, NewProtocolError(ErrorInternal, errors.New("HEC raw reader is nil"))
	}
	if err := limits.Validate(); err != nil {
		return nil, NewProtocolError(ErrorInternal, err)
	}
	return &RawDecoder{
		reader: bufio.NewReaderSize(source, 32<<10),
		limits: limits,
	}, nil
}

// Next returns the next nonempty raw event. Event bytes are newly owned by the
// caller. io.EOF means the rest of the request contained only empty segments.
func (decoder *RawDecoder) Next() ([]byte, error) {
	if decoder == nil {
		return nil, NewProtocolError(ErrorInternal, errors.New("HEC raw decoder is nil"))
	}
	if decoder.failed != nil {
		return nil, decoder.failed
	}
	if decoder.done {
		return nil, io.EOF
	}
	var event []byte
	for {
		fragment, err := decoder.reader.ReadSlice('\n')
		hasLF := len(fragment) != 0 && fragment[len(fragment)-1] == '\n'
		if hasLF {
			fragment = fragment[:len(fragment)-1]
			if len(fragment) != 0 && fragment[len(fragment)-1] == '\r' {
				fragment = fragment[:len(fragment)-1]
			}
		}
		if int64(len(event))+int64(len(fragment)) > decoder.limits.MaximumEventBytes {
			return nil, decoder.fail(NewEventError(ErrorEventTooLarge, decoder.nextNumber, errors.New("HEC raw event exceeds its size limit")))
		}
		event = append(event, fragment...)

		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case err != nil && !errors.Is(err, io.EOF):
			if failure, ok := errors.AsType[*ProtocolError](err); ok {
				return nil, decoder.fail(failure)
			}
			return nil, decoder.fail(NewEventError(ErrorInvalidDataFormat, decoder.nextNumber, err))
		case hasLF:
			if len(event) == 0 {
				event = event[:0]
				if errors.Is(err, io.EOF) {
					decoder.done = true
					return nil, io.EOF
				}
				continue
			}
		case errors.Is(err, io.EOF):
			decoder.done = true
			if len(event) == 0 {
				return nil, io.EOF
			}
		}

		if !utf8.Valid(event) {
			return nil, decoder.fail(NewProtocolError(ErrorInvalidUTF8, errors.New("HEC raw body is not valid UTF-8")))
		}
		if bytes.IndexByte(event, 0) >= 0 {
			return nil, decoder.fail(NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC raw body contains NUL")))
		}
		if decoder.nextNumber >= decoder.limits.MaximumEvents {
			return nil, decoder.fail(NewEventError(ErrorInvalidDataFormat, decoder.nextNumber, errors.New("HEC raw event count exceeds its limit")))
		}
		decoder.nextNumber++
		return bytes.Clone(event), nil
	}
}

// Count returns the number of raw events emitted before EOF or failure.
func (decoder *RawDecoder) Count() int {
	if decoder == nil {
		return 0
	}
	return decoder.nextNumber
}

func (decoder *RawDecoder) fail(err error) error {
	decoder.failed = err
	return err
}
