package hec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"unicode/utf8"
)

// ResultCode is the documented Splunk HEC numeric result taxonomy. Open
// Splunk emits only the selected subset, but retaining
// the complete bounded catalog keeps health and capacity results unambiguous.
type ResultCode int

const (
	// MaximumEmittedAcknowledgmentID is the largest positive integer common
	// JSON clients, including JavaScript, can round-trip exactly.
	MaximumEmittedAcknowledgmentID int64 = 1<<53 - 1

	ResultSuccess                    ResultCode = 0
	ResultTokenDisabled              ResultCode = 1
	ResultTokenRequired              ResultCode = 2
	ResultInvalidAuthorization       ResultCode = 3
	ResultInvalidToken               ResultCode = 4
	ResultNoData                     ResultCode = 5
	ResultInvalidDataFormat          ResultCode = 6
	ResultIncorrectIndex             ResultCode = 7
	ResultInternalServerError        ResultCode = 8
	ResultServerBusy                 ResultCode = 9
	ResultDataChannelMissing         ResultCode = 10
	ResultInvalidDataChannel         ResultCode = 11
	ResultEventFieldRequired         ResultCode = 12
	ResultEventFieldBlank            ResultCode = 13
	ResultAcknowledgmentDisabled     ResultCode = 14
	ResultIndexedFieldsError         ResultCode = 15
	ResultQueryAuthorizationDisabled ResultCode = 16
	ResultHealthy                    ResultCode = 17
	ResultUnhealthyQueuesFull        ResultCode = 18
	ResultUnhealthyAcknowledgment    ResultCode = 19
	ResultUnhealthyQueuesAndAck      ResultCode = 20
	ResultHealthInvalidToken         ResultCode = 21
	ResultHealthTokenDisabled        ResultCode = 22
	ResultServerShuttingDown         ResultCode = 23
	ResultQueueApproachingCapacity   ResultCode = 24
	ResultAckApproachingCapacity     ResultCode = 25
	ResultQueueAtCapacity            ResultCode = 26
	ResultAcknowledgmentAtCapacity   ResultCode = 27
)

type resultDefinition struct {
	text       string
	httpStatus int
}

var resultDefinitions = map[ResultCode]resultDefinition{
	ResultSuccess:                    {"Success", http.StatusOK},
	ResultTokenDisabled:              {"Token disabled", http.StatusForbidden},
	ResultTokenRequired:              {"Token is required", http.StatusUnauthorized},
	ResultInvalidAuthorization:       {"Invalid authorization", http.StatusUnauthorized},
	ResultInvalidToken:               {"Invalid token", http.StatusForbidden},
	ResultNoData:                     {"No data", http.StatusBadRequest},
	ResultInvalidDataFormat:          {"Invalid data format", http.StatusBadRequest},
	ResultIncorrectIndex:             {"Incorrect index", http.StatusBadRequest},
	ResultInternalServerError:        {"Internal server error", http.StatusInternalServerError},
	ResultServerBusy:                 {"Server is busy", http.StatusServiceUnavailable},
	ResultDataChannelMissing:         {"Data channel is missing", http.StatusBadRequest},
	ResultInvalidDataChannel:         {"Invalid data channel", http.StatusBadRequest},
	ResultEventFieldRequired:         {"Event field is required", http.StatusBadRequest},
	ResultEventFieldBlank:            {"Event field cannot be blank", http.StatusBadRequest},
	ResultAcknowledgmentDisabled:     {"ACK is disabled", http.StatusBadRequest},
	ResultIndexedFieldsError:         {"Error in handling indexed fields", http.StatusBadRequest},
	ResultQueryAuthorizationDisabled: {"Query string authorization is not enabled", http.StatusBadRequest},
	ResultHealthy:                    {"HEC is healthy", http.StatusOK},
	ResultUnhealthyQueuesFull:        {"HEC is unhealthy, queues are full", http.StatusServiceUnavailable},
	ResultUnhealthyAcknowledgment:    {"HEC is unhealthy, ack service unavailable", http.StatusServiceUnavailable},
	ResultUnhealthyQueuesAndAck:      {"HEC is unhealthy, queues are full, ack service unavailable", http.StatusServiceUnavailable},
	ResultHealthInvalidToken:         {"Invalid token", http.StatusBadRequest},
	ResultHealthTokenDisabled:        {"Token disabled", http.StatusBadRequest},
	ResultServerShuttingDown:         {"Server is shutting down", http.StatusServiceUnavailable},
	ResultQueueApproachingCapacity:   {"HEC queue is approaching its capacity limit", http.StatusOK},
	ResultAckApproachingCapacity:     {"HEC ACK is approaching its capacity limit", http.StatusOK},
	ResultQueueAtCapacity:            {"HEC queue is at capacity and cannot process any more requests", http.StatusTooManyRequests},
	ResultAcknowledgmentAtCapacity:   {"HEC ACK channel is at capacity and cannot process any more requests", http.StatusTooManyRequests},
}

// Response is the stable base HEC response. Optional members are serialized
// after code in this exact order. AckID is valid only for success and
// InvalidEventNumber is valid only for a failure.
type Response struct {
	Code               ResultCode
	InvalidEventNumber *int
	AckID              *int64
}

// NewResponse returns a response using only catalog-owned public text.
// Unknown codes are converted to the internal-error result.
func NewResponse(code ResultCode) Response {
	if _, ok := resultDefinitions[code]; !ok {
		code = ResultInternalServerError
	}
	return Response{Code: code}
}

// HTTPStatus returns the catalog status for the response code.
func (response Response) HTTPStatus() int {
	definition, ok := resultDefinitions[response.Code]
	if !ok {
		return http.StatusInternalServerError
	}
	return definition.httpStatus
}

// MarshalResponse emits bounded compact UTF-8 JSON with stable member order.
func MarshalResponse(response Response, maximumBytes int64) ([]byte, error) {
	if maximumBytes <= 0 || maximumBytes > HardMaximumResponseBytes {
		return nil, errors.New("HEC response limit is invalid")
	}
	definition, ok := resultDefinitions[response.Code]
	if !ok || !utf8.ValidString(definition.text) {
		return nil, errors.New("HEC response code is invalid")
	}
	if response.InvalidEventNumber != nil {
		if *response.InvalidEventNumber < 0 || !allowsInvalidEventNumber(response.Code) {
			return nil, errors.New("HEC invalid-event-number is invalid")
		}
	}
	if response.AckID != nil {
		if *response.AckID <= 0 || *response.AckID > MaximumEmittedAcknowledgmentID ||
			response.Code != ResultSuccess {
			return nil, errors.New("HEC acknowledgment ID is invalid")
		}
	}
	if response.AckID != nil && response.InvalidEventNumber != nil {
		return nil, errors.New("HEC response optional members conflict")
	}

	encodedText, err := json.Marshal(definition.text)
	if err != nil {
		return nil, fmt.Errorf("marshal HEC result text: %w", err)
	}
	result := make([]byte, 0, len(encodedText)+96)
	result = append(result, `{"text":`...)
	result = append(result, encodedText...)
	result = append(result, `,"code":`...)
	result = strconv.AppendInt(result, int64(response.Code), 10)
	if response.InvalidEventNumber != nil {
		result = append(result, `,"invalid-event-number":`...)
		result = strconv.AppendInt(result, int64(*response.InvalidEventNumber), 10)
	}
	if response.AckID != nil {
		result = append(result, `,"ackId":`...)
		result = strconv.AppendInt(result, *response.AckID, 10)
	}
	result = append(result, '}')
	if int64(len(result)) > maximumBytes {
		return nil, errors.New("HEC response exceeds its size limit")
	}
	return result, nil
}

func allowsInvalidEventNumber(code ResultCode) bool {
	switch code {
	case ResultInvalidDataFormat, ResultIncorrectIndex, ResultEventFieldRequired,
		ResultEventFieldBlank, ResultIndexedFieldsError:
		return true
	default:
		return false
	}
}

// AcknowledgmentResult is one channel-scoped indexer acknowledgment result.
type AcknowledgmentResult struct {
	ID      int64
	Indexed bool
}

// MarshalAcknowledgments emits {"acks":{"id":bool,...}} in the supplied ID
// order. IDs must be positive and unique.
func MarshalAcknowledgments(results []AcknowledgmentResult, limits Limits) ([]byte, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if len(results) == 0 || len(results) > limits.MaximumAcknowledgmentIDs {
		return nil, errors.New("HEC acknowledgment result count exceeds its limit")
	}
	seen := make(map[int64]struct{}, len(results))
	var encoded bytes.Buffer
	encoded.Grow(32 + len(results)*16)
	encoded.WriteString(`{"acks":{`)
	for index, result := range results {
		if result.ID <= 0 {
			return nil, errors.New("HEC acknowledgment ID must be positive")
		}
		if _, duplicate := seen[result.ID]; duplicate {
			return nil, errors.New("HEC acknowledgment ID is duplicated")
		}
		seen[result.ID] = struct{}{}
		if index != 0 {
			encoded.WriteByte(',')
		}
		encoded.WriteByte('"')
		encoded.WriteString(strconv.FormatInt(result.ID, 10))
		encoded.WriteString(`":`)
		encoded.WriteString(strconv.FormatBool(result.Indexed))
		if int64(encoded.Len()+2) > limits.MaximumResponseBytes {
			return nil, errors.New("HEC acknowledgment response exceeds its size limit")
		}
	}
	encoded.WriteString("}}")
	return encoded.Bytes(), nil
}
