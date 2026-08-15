package hec

import (
	"encoding/json"
	"errors"
	"io"
	"strconv"
)

// AcknowledgmentRequest is a completely validated, order-preserving ACK
// query. IDs are unique positive signed 64-bit integers.
type AcknowledgmentRequest struct {
	IDs []int64
}

// DecodeAcknowledgmentRequest decodes exactly {"acks":[...]} and no second
// top-level value. The caller applies NewBodyReader first when gzip is present;
// this decoder independently enforces the smaller 64 KiB ACK body ceiling.
func DecodeAcknowledgmentRequest(source io.Reader, limits Limits) (AcknowledgmentRequest, error) {
	if source == nil {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInternal, errors.New("HEC acknowledgment reader is nil"))
	}
	if err := limits.Validate(); err != nil {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInternal, err)
	}
	maximumBody := min(limits.MaximumAcknowledgmentBodyBytes, limits.MaximumDecompressedBodyBytes)
	limited := newHardLimitReader(source, maximumBody, ErrorDecompressedBodyTooLarge)
	decoder := json.NewDecoder(newUTF8ValidatingReader(limited))
	decoder.UseNumber()

	opening, err := decoder.Token()
	if errors.Is(err, io.EOF) {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorNoData, nil)
	}
	if err != nil {
		return AcknowledgmentRequest{}, acknowledgmentDecodeError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment body is not an object"))
	}
	if !decoder.More() {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment member is missing"))
	}
	nameToken, err := decoder.Token()
	if err != nil {
		return AcknowledgmentRequest{}, acknowledgmentDecodeError(err)
	}
	name, ok := nameToken.(string)
	if !ok || name != "acks" {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment member is unknown"))
	}
	arrayOpening, err := decoder.Token()
	if err != nil {
		return AcknowledgmentRequest{}, acknowledgmentDecodeError(err)
	}
	if delimiter, ok := arrayOpening.(json.Delim); !ok || delimiter != '[' {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgments are not an array"))
	}
	request := AcknowledgmentRequest{IDs: make([]int64, 0, min(16, limits.MaximumAcknowledgmentIDs))}
	seen := make(map[int64]struct{})
	for decoder.More() {
		if len(request.IDs) >= limits.MaximumAcknowledgmentIDs {
			return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment count exceeds its limit"))
		}
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return AcknowledgmentRequest{}, acknowledgmentDecodeError(tokenErr)
		}
		number, ok := token.(json.Number)
		if !ok {
			return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment ID is not an integer"))
		}
		text := number.String()
		if len(text) == 0 || containsJSONFractionOrExponent(text) {
			return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment ID is not a plain integer"))
		}
		id, parseErr := strconv.ParseInt(text, 10, 64)
		if parseErr != nil || id <= 0 {
			return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment ID is outside its range"))
		}
		if _, duplicate := seen[id]; duplicate {
			return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment ID is duplicated"))
		}
		seen[id] = struct{}{}
		request.IDs = append(request.IDs, id)
	}
	if len(request.IDs) == 0 {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment array is empty"))
	}
	if closing, closeErr := decoder.Token(); closeErr != nil {
		return AcknowledgmentRequest{}, acknowledgmentDecodeError(closeErr)
	} else if delimiter, ok := closing.(json.Delim); !ok || delimiter != ']' {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment array is not closed"))
	}
	if decoder.More() {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment object has an extra or duplicate member"))
	}
	if closing, closeErr := decoder.Token(); closeErr != nil {
		return AcknowledgmentRequest{}, acknowledgmentDecodeError(closeErr)
	} else if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment object is not closed"))
	}
	if trailing, trailingErr := decoder.Token(); !errors.Is(trailingErr, io.EOF) {
		_ = trailing
		if trailingErr != nil {
			return AcknowledgmentRequest{}, acknowledgmentDecodeError(trailingErr)
		}
		return AcknowledgmentRequest{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC acknowledgment body has a second value"))
	}
	return request, nil
}

func acknowledgmentDecodeError(err error) error {
	return wrapProtocolError(ErrorInvalidDataFormat, err)
}

func containsJSONFractionOrExponent(value string) bool {
	for index := 0; index < len(value); index++ {
		switch value[index] {
		case '.', 'e', 'E':
			return true
		}
	}
	return false
}
