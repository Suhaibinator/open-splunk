package hec

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrorKind is a closed classification for failures at the HEC boundary.
// Error text is intentionally fixed and never incorporates a credential,
// channel, metadata value, event body, or decoder diagnostic.
type ErrorKind uint8

const (
	ErrorUnknownPath ErrorKind = iota + 1
	ErrorMethodNotAllowed
	ErrorCompressedBodyTooLarge
	ErrorDecompressedBodyTooLarge
	ErrorNormalizedBodyTooLarge
	ErrorEventTooLarge
	ErrorHeaderTooLarge
	ErrorUnsupportedMediaType
	ErrorUnsupportedContentEncoding
	ErrorInvalidCompressedBody
	ErrorInvalidUTF8
	ErrorQueryAuthorizationDisabled
	ErrorTokenRequired
	ErrorInvalidAuthorization
	ErrorInvalidToken
	ErrorTokenDisabled
	ErrorChannelMissing
	ErrorChannelInvalid
	ErrorNoData
	ErrorInvalidDataFormat
	ErrorEventRequired
	ErrorEventBlank
	ErrorIndexedFields
	ErrorIncorrectIndex
	ErrorAcknowledgmentDisabled
	ErrorServerBusy
	ErrorQueueCapacity
	ErrorAcknowledgmentCapacity
	ErrorShuttingDown
	ErrorInternal
)

type errorDefinition struct {
	name       string
	result     ResultCode
	httpStatus int
}

var errorDefinitions = map[ErrorKind]errorDefinition{
	ErrorUnknownPath:                {"unknown HEC path", ResultInvalidDataFormat, http.StatusNotFound},
	ErrorMethodNotAllowed:           {"HEC method is not allowed", ResultInvalidDataFormat, http.StatusMethodNotAllowed},
	ErrorCompressedBodyTooLarge:     {"compressed HEC body exceeds its limit", ResultInvalidDataFormat, http.StatusRequestEntityTooLarge},
	ErrorDecompressedBodyTooLarge:   {"decompressed HEC body exceeds its limit", ResultInvalidDataFormat, http.StatusRequestEntityTooLarge},
	ErrorNormalizedBodyTooLarge:     {"normalized HEC body exceeds its limit", ResultInvalidDataFormat, http.StatusRequestEntityTooLarge},
	ErrorEventTooLarge:              {"HEC event exceeds its limit", ResultInvalidDataFormat, http.StatusRequestEntityTooLarge},
	ErrorHeaderTooLarge:             {"HEC headers exceed their limit", ResultInvalidDataFormat, http.StatusRequestHeaderFieldsTooLarge},
	ErrorUnsupportedMediaType:       {"unsupported HEC media type", ResultInvalidDataFormat, http.StatusUnsupportedMediaType},
	ErrorUnsupportedContentEncoding: {"unsupported HEC content encoding", ResultInvalidDataFormat, http.StatusUnsupportedMediaType},
	ErrorInvalidCompressedBody:      {"invalid compressed HEC body", ResultInvalidDataFormat, http.StatusBadRequest},
	ErrorInvalidUTF8:                {"invalid UTF-8 HEC body", ResultInvalidDataFormat, http.StatusBadRequest},
	ErrorQueryAuthorizationDisabled: {"HEC query authorization is disabled", ResultQueryAuthorizationDisabled, http.StatusBadRequest},
	ErrorTokenRequired:              {"HEC token is required", ResultTokenRequired, http.StatusUnauthorized},
	ErrorInvalidAuthorization:       {"invalid HEC authorization", ResultInvalidAuthorization, http.StatusUnauthorized},
	ErrorInvalidToken:               {"invalid HEC token", ResultInvalidToken, http.StatusForbidden},
	ErrorTokenDisabled:              {"HEC token is disabled", ResultTokenDisabled, http.StatusForbidden},
	ErrorChannelMissing:             {"HEC channel is missing", ResultDataChannelMissing, http.StatusBadRequest},
	ErrorChannelInvalid:             {"invalid HEC channel", ResultInvalidDataChannel, http.StatusBadRequest},
	ErrorNoData:                     {"HEC request contains no data", ResultNoData, http.StatusBadRequest},
	ErrorInvalidDataFormat:          {"invalid HEC data format", ResultInvalidDataFormat, http.StatusBadRequest},
	ErrorEventRequired:              {"HEC event member is required", ResultEventFieldRequired, http.StatusBadRequest},
	ErrorEventBlank:                 {"HEC event member is blank", ResultEventFieldBlank, http.StatusBadRequest},
	ErrorIndexedFields:              {"invalid HEC indexed fields", ResultIndexedFieldsError, http.StatusBadRequest},
	ErrorIncorrectIndex:             {"incorrect HEC index", ResultIncorrectIndex, http.StatusBadRequest},
	ErrorAcknowledgmentDisabled:     {"HEC acknowledgment is disabled", ResultAcknowledgmentDisabled, http.StatusBadRequest},
	ErrorServerBusy:                 {"HEC server is busy", ResultServerBusy, http.StatusServiceUnavailable},
	ErrorQueueCapacity:              {"HEC queue is at capacity", ResultQueueAtCapacity, http.StatusTooManyRequests},
	ErrorAcknowledgmentCapacity:     {"HEC acknowledgment channel is at capacity", ResultAcknowledgmentAtCapacity, http.StatusTooManyRequests},
	ErrorShuttingDown:               {"HEC server is shutting down", ResultServerShuttingDown, http.StatusServiceUnavailable},
	ErrorInternal:                   {"internal HEC failure", ResultInternalServerError, http.StatusInternalServerError},
}

// ProtocolError carries only a closed public classification and an optional
// zero-based invalid event number. Cause is retained for errors.Is/errors.As,
// while Error deliberately omits cause text.
type ProtocolError struct {
	Kind               ErrorKind
	InvalidEventNumber *int
	cause              error
}

func (failure *ProtocolError) Error() string {
	if failure == nil {
		return "<nil>"
	}
	definition, ok := errorDefinitions[failure.Kind]
	if !ok {
		return "unknown HEC protocol failure"
	}
	return definition.name
}

func (failure *ProtocolError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.cause
}

// HTTPStatus returns the fixed status associated with the failure. Unknown or
// corrupt classifications fail closed as an internal error.
func (failure *ProtocolError) HTTPStatus() int {
	if failure == nil {
		return http.StatusInternalServerError
	}
	definition, ok := errorDefinitions[failure.Kind]
	if !ok {
		return http.StatusInternalServerError
	}
	return definition.httpStatus
}

// Response returns the bounded public HEC response for this failure.
func (failure *ProtocolError) Response() Response {
	if failure == nil {
		return NewResponse(ResultInternalServerError)
	}
	definition, ok := errorDefinitions[failure.Kind]
	if !ok {
		return NewResponse(ResultInternalServerError)
	}
	response := NewResponse(definition.result)
	if failure.InvalidEventNumber != nil {
		response.InvalidEventNumber = copyInt(failure.InvalidEventNumber)
	}
	return response
}

// NewProtocolError constructs a closed failure without an event position.
func NewProtocolError(kind ErrorKind, cause error) *ProtocolError {
	if _, ok := errorDefinitions[kind]; !ok {
		kind = ErrorInternal
	}
	return &ProtocolError{Kind: kind, cause: cause}
}

// NewEventError constructs a failure associated with one zero-based envelope
// or raw-event position.
func NewEventError(kind ErrorKind, eventNumber int, cause error) *ProtocolError {
	if eventNumber < 0 {
		return NewProtocolError(ErrorInternal, errors.New("negative HEC event number"))
	}
	failure := NewProtocolError(kind, cause)
	failure.InvalidEventNumber = &eventNumber
	return failure
}

// ErrorKindOf extracts the closed classification without string matching.
func ErrorKindOf(err error) (ErrorKind, bool) {
	var failure *ProtocolError
	if !errors.As(err, &failure) || failure == nil {
		return 0, false
	}
	return failure.Kind, true
}

// IsErrorKind reports whether err contains the requested HEC classification.
func IsErrorKind(err error, kind ErrorKind) bool {
	got, ok := ErrorKindOf(err)
	return ok && got == kind
}

func (kind ErrorKind) String() string {
	definition, ok := errorDefinitions[kind]
	if !ok {
		return fmt.Sprintf("ErrorKind(%d)", kind)
	}
	return definition.name
}

func copyInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
