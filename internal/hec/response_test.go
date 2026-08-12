package hec

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestResultCatalogAndStableSerialization(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code   ResultCode
		text   string
		status int
	}{
		{0, "Success", 200},
		{1, "Token disabled", 403},
		{2, "Token is required", 401},
		{3, "Invalid authorization", 401},
		{4, "Invalid token", 403},
		{5, "No data", 400},
		{6, "Invalid data format", 400},
		{7, "Incorrect index", 400},
		{8, "Internal server error", 500},
		{9, "Server is busy", 503},
		{10, "Data channel is missing", 400},
		{11, "Invalid data channel", 400},
		{12, "Event field is required", 400},
		{13, "Event field cannot be blank", 400},
		{14, "ACK is disabled", 400},
		{15, "Error in handling indexed fields", 400},
		{16, "Query string authorization is not enabled", 400},
		{17, "HEC is healthy", 200},
		{18, "HEC is unhealthy, queues are full", 503},
		{19, "HEC is unhealthy, ack service unavailable", 503},
		{20, "HEC is unhealthy, queues are full, ack service unavailable", 503},
		{21, "Invalid token", 400},
		{22, "Token disabled", 400},
		{23, "Server is shutting down", 503},
		{24, "HEC queue is approaching its capacity limit", 200},
		{25, "HEC ACK is approaching its capacity limit", 200},
		{26, "HEC queue is at capacity and cannot process any more requests", 429},
		{27, "HEC ACK channel is at capacity and cannot process any more requests", 429},
	}
	for _, test := range tests {
		t.Run(strconv.Itoa(int(test.code)), func(t *testing.T) {
			t.Parallel()
			response := NewResponse(test.code)
			if response.Code != test.code || response.HTTPStatus() != test.status {
				t.Fatalf("NewResponse(%d) = %#v status %d", test.code, response, response.HTTPStatus())
			}
			got, err := MarshalResponse(response, HardMaximumResponseBytes)
			if err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf(`{"text":%q,"code":%d}`, test.text, test.code)
			if string(got) != want || !json.Valid(got) {
				t.Fatalf("MarshalResponse() = %s, want %s", got, want)
			}
		})
	}
}

func TestResponseOptionalMembersAreValidatedAndOrdered(t *testing.T) {
	t.Parallel()
	eventNumber := 4
	ackID := MaximumEmittedAcknowledgmentID
	tests := []struct {
		name     string
		response Response
		limit    int64
		want     string
		wantErr  bool
	}{
		{name: "invalid event", response: Response{Code: ResultInvalidDataFormat, InvalidEventNumber: &eventNumber}, limit: HardMaximumResponseBytes, want: `{"text":"Invalid data format","code":6,"invalid-event-number":4}`},
		{name: "ack", response: Response{Code: ResultSuccess, AckID: &ackID}, limit: HardMaximumResponseBytes, want: `{"text":"Success","code":0,"ackId":9007199254740991}`},
		{name: "unknown code", response: Response{Code: 100}, limit: HardMaximumResponseBytes, wantErr: true},
		{name: "negative event", response: Response{Code: ResultInvalidDataFormat, InvalidEventNumber: pointer(-1)}, limit: HardMaximumResponseBytes, wantErr: true},
		{name: "event on success", response: Response{Code: ResultSuccess, InvalidEventNumber: &eventNumber}, limit: HardMaximumResponseBytes, wantErr: true},
		{name: "zero ack", response: Response{Code: ResultSuccess, AckID: pointer(int64(0))}, limit: HardMaximumResponseBytes, wantErr: true},
		{name: "negative ack", response: Response{Code: ResultSuccess, AckID: pointer(int64(-1))}, limit: HardMaximumResponseBytes, wantErr: true},
		{name: "inexact ack", response: Response{Code: ResultSuccess, AckID: pointer(MaximumEmittedAcknowledgmentID + 1)}, limit: HardMaximumResponseBytes, wantErr: true},
		{name: "ack on failure", response: Response{Code: ResultInvalidDataFormat, AckID: &ackID}, limit: HardMaximumResponseBytes, wantErr: true},
		{name: "both optionals", response: Response{Code: ResultSuccess, AckID: &ackID, InvalidEventNumber: &eventNumber}, limit: HardMaximumResponseBytes, wantErr: true},
		{name: "invalid zero limit", response: NewResponse(ResultSuccess), limit: 0, wantErr: true},
		{name: "over hard limit", response: NewResponse(ResultSuccess), limit: HardMaximumResponseBytes + 1, wantErr: true},
		{name: "encoded over tight limit", response: NewResponse(ResultSuccess), limit: 1, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := MarshalResponse(test.response, test.limit)
			if test.wantErr {
				if err == nil || got != nil {
					t.Fatalf("MarshalResponse() = %q, %v, want error", got, err)
				}
				return
			}
			if err != nil || string(got) != test.want {
				t.Fatalf("MarshalResponse() = %q, %v, want %q", got, err, test.want)
			}
		})
	}
}

func TestMarshalAcknowledgmentsIsBoundedStableAndDuplicateFree(t *testing.T) {
	t.Parallel()
	limits := DefaultLimits()
	got, err := MarshalAcknowledgments([]AcknowledgmentResult{
		{ID: 3, Indexed: false},
		{ID: 1, Indexed: true},
		{ID: 9223372036854775807, Indexed: true},
	}, limits)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"acks":{"3":false,"1":true,"9223372036854775807":true}}`
	if string(got) != want || !json.Valid(got) {
		t.Fatalf("MarshalAcknowledgments() = %s, want %s", got, want)
	}
	empty, err := MarshalAcknowledgments(nil, limits)
	if err == nil || empty != nil {
		t.Fatalf("empty MarshalAcknowledgments() = %s, %v, want error", empty, err)
	}
	for name, values := range map[string][]AcknowledgmentResult{
		"zero":      {{ID: 0}},
		"negative":  {{ID: -1}},
		"duplicate": {{ID: 1}, {ID: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := MarshalAcknowledgments(values, limits); err == nil || got != nil {
				t.Fatalf("MarshalAcknowledgments() = %q, %v, want error", got, err)
			}
		})
	}
	tight := limits
	tight.MaximumAcknowledgmentIDs = 1
	if got, err := MarshalAcknowledgments([]AcknowledgmentResult{{ID: 1}, {ID: 2}}, tight); err == nil || got != nil {
		t.Fatalf("count-bounded MarshalAcknowledgments() = %q, %v", got, err)
	}
	tight = limits
	tight.MaximumResponseBytes = int64(len(`{"acks":{"1":false}}`) - 1)
	if got, err := MarshalAcknowledgments([]AcknowledgmentResult{{ID: 1}}, tight); err == nil || got != nil {
		t.Fatalf("byte-bounded MarshalAcknowledgments() = %q, %v", got, err)
	}
}

func TestProtocolErrorTaxonomyMapsWithoutLeakingCause(t *testing.T) {
	t.Parallel()
	secret := "secret-token-and-event-body"
	tests := []struct {
		kind   ErrorKind
		code   ResultCode
		status int
	}{
		{ErrorUnknownPath, ResultInvalidDataFormat, 404},
		{ErrorMethodNotAllowed, ResultInvalidDataFormat, 405},
		{ErrorCompressedBodyTooLarge, ResultInvalidDataFormat, 413},
		{ErrorDecompressedBodyTooLarge, ResultInvalidDataFormat, 413},
		{ErrorNormalizedBodyTooLarge, ResultInvalidDataFormat, 413},
		{ErrorEventTooLarge, ResultInvalidDataFormat, 413},
		{ErrorHeaderTooLarge, ResultInvalidDataFormat, 431},
		{ErrorUnsupportedMediaType, ResultInvalidDataFormat, 415},
		{ErrorUnsupportedContentEncoding, ResultInvalidDataFormat, 415},
		{ErrorInvalidCompressedBody, ResultInvalidDataFormat, 400},
		{ErrorInvalidUTF8, ResultInvalidDataFormat, 400},
		{ErrorQueryAuthorizationDisabled, ResultQueryAuthorizationDisabled, 400},
		{ErrorTokenRequired, ResultTokenRequired, 401},
		{ErrorInvalidAuthorization, ResultInvalidAuthorization, 401},
		{ErrorInvalidToken, ResultInvalidToken, 403},
		{ErrorTokenDisabled, ResultTokenDisabled, 403},
		{ErrorChannelMissing, ResultDataChannelMissing, 400},
		{ErrorChannelInvalid, ResultInvalidDataChannel, 400},
		{ErrorNoData, ResultNoData, 400},
		{ErrorInvalidDataFormat, ResultInvalidDataFormat, 400},
		{ErrorEventRequired, ResultEventFieldRequired, 400},
		{ErrorEventBlank, ResultEventFieldBlank, 400},
		{ErrorIndexedFields, ResultIndexedFieldsError, 400},
		{ErrorIncorrectIndex, ResultIncorrectIndex, 400},
		{ErrorAcknowledgmentDisabled, ResultAcknowledgmentDisabled, 400},
		{ErrorServerBusy, ResultServerBusy, 503},
		{ErrorQueueCapacity, ResultQueueAtCapacity, 429},
		{ErrorAcknowledgmentCapacity, ResultAcknowledgmentAtCapacity, 429},
		{ErrorShuttingDown, ResultServerShuttingDown, 503},
		{ErrorInternal, ResultInternalServerError, 500},
	}
	for _, test := range tests {
		failure := NewProtocolError(test.kind, errors.New(secret))
		if failure.HTTPStatus() != test.status || failure.Response().Code != test.code ||
			!IsErrorKind(failure, test.kind) || strings.Contains(failure.Error(), secret) {
			t.Errorf("kind %v mapped to status=%d response=%#v error=%q", test.kind, failure.HTTPStatus(), failure.Response(), failure.Error())
		}
		if !errors.Is(failure, failure.Unwrap()) {
			t.Errorf("kind %v did not retain cause for errors.Is", test.kind)
		}
	}
	eventFailure := NewEventError(ErrorInvalidDataFormat, 7, errors.New(secret))
	response := eventFailure.Response()
	if response.InvalidEventNumber == nil || *response.InvalidEventNumber != 7 {
		t.Fatalf("event failure response = %#v", response)
	}
	*eventFailure.InvalidEventNumber = 8
	if *response.InvalidEventNumber != 7 {
		t.Fatal("response retained mutable pointer alias")
	}
	if NewEventError(ErrorInvalidDataFormat, -1, nil).Kind != ErrorInternal {
		t.Fatal("negative event number did not fail closed")
	}
}

func FuzzMarshalResponse(f *testing.F) {
	f.Add(int64(0), int64(0), int64(1), uint8(0))
	f.Add(int64(6), int64(2), int64(0), uint8(1))
	f.Add(int64(100), int64(-1), int64(-1), uint8(3))
	f.Fuzz(func(t *testing.T, rawCode, rawEvent, rawAck int64, option uint8) {
		response := Response{Code: ResultCode(rawCode)}
		if option&1 != 0 {
			event := int(rawEvent)
			response.InvalidEventNumber = &event
		}
		if option&2 != 0 {
			ack := rawAck
			response.AckID = &ack
		}
		encoded, err := MarshalResponse(response, HardMaximumResponseBytes)
		if err != nil {
			if encoded != nil {
				t.Fatalf("error returned bytes %q", encoded)
			}
			return
		}
		if len(encoded) > int(HardMaximumResponseBytes) || !json.Valid(encoded) {
			t.Fatalf("invalid bounded response %q", encoded)
		}
	})
}

func pointer[T any](value T) *T { return &value }
