package searchws

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func commandWireBytes(number protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), value)
}

func TestDecodeCommandCardinality(t *testing.T) {
	const subscriptions, unsubscribeIDs = 2, 7
	for _, test := range []struct {
		name     string
		data     []byte
		wantCode opensplunk.SearchWebSocketProtocolErrorCode
	}{
		{name: "subscriptions at limit", data: commandWireBytes(10, bytes.Repeat(commandWireBytes(1, nil), subscriptions))},
		{name: "subscriptions over limit", data: commandWireBytes(10, bytes.Repeat(commandWireBytes(1, nil), subscriptions+1)), wantCode: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS},
		{name: "merged subscriptions over limit", data: bytes.Repeat(commandWireBytes(10, commandWireBytes(1, nil)), subscriptions+1), wantCode: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS},
		{name: "overwritten subscriptions over limit", data: bytes.Repeat(append(commandWireBytes(10, commandWireBytes(1, nil)), commandWireBytes(12, nil)...), subscriptions+1), wantCode: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS},
		{name: "unsubscribe IDs at queue limit", data: commandWireBytes(11, bytes.Repeat(commandWireBytes(1, nil), unsubscribeIDs))},
		{name: "unsubscribe IDs over queue limit", data: commandWireBytes(11, bytes.Repeat(commandWireBytes(1, nil), unsubscribeIDs+1)), wantCode: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND},
		{name: "merged unsubscribe IDs over limit", data: bytes.Repeat(commandWireBytes(11, commandWireBytes(1, nil)), unsubscribeIDs+1), wantCode: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND},
		{name: "overwritten unsubscribe IDs over limit", data: bytes.Repeat(append(commandWireBytes(11, commandWireBytes(1, nil)), commandWireBytes(12, nil)...), unsubscribeIDs+1), wantCode: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND},
		{name: "duplicate singular messages", data: bytes.Repeat(commandWireBytes(12, nil), 16+8*subscriptions+unsubscribeIDs+1), wantCode: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND},
		{name: "duplicate nested targets", data: commandWireBytes(10, commandWireBytes(1, bytes.Repeat(commandWireBytes(2, nil), 16+8*subscriptions+unsubscribeIDs+1))), wantCode: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Request IDs can appear after a payload and use last-value semantics.
			data := append(commandWireBytes(1, []byte("earlier")), test.data...)
			data = append(data, commandWireBytes(1, []byte("request"))...)
			command, failure := decodeCommand(data, subscriptions, unsubscribeIDs)
			if command == nil || command.GetRequestId() != "request" {
				t.Fatalf("command = %v", command)
			}
			if test.wantCode == 0 {
				if failure != nil {
					t.Fatalf("unexpected failure: %v", failure)
				}
				var expected opensplunk.SearchWebSocketCommand
				if err := proto.Unmarshal(data, &expected); err != nil || !proto.Equal(command, &expected) {
					t.Fatalf("decode differs from protobuf: %v", err)
				}
				return
			}
			if failure == nil || failure.code != test.wantCode || failure.connectionWillClose {
				t.Fatalf("failure = %+v, want recoverable %v", failure, test.wantCode)
			}
			if command.GetPayload() != nil {
				t.Fatal("over-budget payload was materialized")
			}
		})
	}
}

func TestDecodeCommandPreservesProtobufCompatibility(t *testing.T) {
	ping := commandWireBytes(12, commandWireBytes(1, []byte("nonce")))
	for _, data := range [][]byte{
		append(commandWireBytes(1, []byte("request")), ping...),
		append(commandWireBytes(12, commandWireBytes(1, []byte("old"))), ping...),
		append(commandWireBytes(10, commandWireBytes(1, nil)), ping...),
		append(bytes.Clone(ping), protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 7)...),
		append(bytes.Clone(ping), protowire.AppendVarint(protowire.AppendTag(nil, 10, protowire.VarintType), 7)...),
		commandWireBytes(10, protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 7)),
		commandWireBytes(12, commandWireBytes(100, []byte("opaque"))),
		append(bytes.Clone(ping), protowire.AppendTag(protowire.AppendTag(nil, 100, protowire.StartGroupType), 100, protowire.EndGroupType)...),
		// Protobuf accepts non-minimal tag and length varints.
		{0xe2, 0x00, 0x80, 0x00},
	} {
		var expected opensplunk.SearchWebSocketCommand
		if err := proto.Unmarshal(data, &expected); err != nil {
			t.Fatal(err)
		}
		command, failure := decodeCommand(data, 2, 7)
		if failure != nil || !proto.Equal(command, &expected) {
			t.Fatalf("decode differs from protobuf: command=%v failure=%v expected=%v", command, failure, &expected)
		}
	}
	for _, data := range [][]byte{
		{0xff}, {0}, {0x52, 0xff}, {0x52, 1, 0xff},
		commandWireBytes(12, commandWireBytes(1, []byte{0xff})),
		protowire.AppendTag(nil, 100, protowire.EndGroupType),
	} {
		if command, _ := decodeCommand(data, 2, 7); command != nil {
			t.Fatalf("accepted malformed protobuf: %v", command)
		}
	}
}

func TestDecodeCommandAcceptsFullSubscriptionLimit(t *testing.T) {
	for _, limit := range []uint32{defaultMaximumSubscriptions, maximumSubscriptionsCeiling} {
		input := &opensplunk.SearchWebSocketCommand{RequestId: "maximum", Payload: &opensplunk.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunk.SubscribeSearchJobsCommand{}}}
		previewRows := uint32(1)
		for index := range limit {
			input.GetSubscribe().Subscriptions = append(input.GetSubscribe().Subscriptions, &opensplunk.SearchSubscription{
				SubscriptionId: fmt.Sprintf("subscription-%d", index),
				Target:         &opensplunk.JobTarget{Target: &opensplunk.JobTarget_SearchJobId{SearchJobId: "job"}},
				AfterSequence:  1, IncludePreviews: true, PreviewRowLimit: &previewRows,
			})
		}
		data, err := proto.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		command, failure := decodeCommand(data, limit, minimumQueuedFrames)
		if failure != nil || !proto.Equal(command, input) {
			t.Fatalf("full subscription command failed: %v", failure)
		}
	}
}

func TestDecodeCommandRejectedAllocationBound(t *testing.T) {
	// Exercise only the bounded decoder with a large opaque suffix. The entry
	// count crosses the limit by one; the frame must never reach proto.Unmarshal.
	payload := commandWireBytes(10, bytes.Repeat(commandWireBytes(1, nil), int(defaultMaximumSubscriptions)+1))
	payload = append(payload, commandWireBytes(100, make([]byte, int(defaultMaximumFrameBytes)-len(payload)-32))...)
	payload = append(payload, commandWireBytes(1, []byte("request"))...)
	allocations := testing.AllocsPerRun(20, func() {
		command, failure := decodeCommand(payload, defaultMaximumSubscriptions, defaultMaximumQueuedFrames)
		if failure == nil || command == nil || command.GetPayload() != nil || command.GetRequestId() != "request" {
			t.Fatal("over-budget command reached full decode")
		}
	})
	if allocations > 4 {
		t.Fatalf("rejected frame allocations = %.0f, want at most 4", allocations)
	}
	result := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			_, _ = decodeCommand(payload, defaultMaximumSubscriptions, defaultMaximumQueuedFrames)
		}
	})
	if result.AllocedBytesPerOp() > 1024 {
		t.Fatalf("rejected frame bytes/op = %d, want at most 1024", result.AllocedBytesPerOp())
	}
	t.Logf("rejected frame: %d bytes, %.0f allocations, %d allocated bytes/op", len(payload), allocations, result.AllocedBytesPerOp())
}

func TestWebSocketDecodeLimitIsCorrelatedAndRecoverable(t *testing.T) {
	fixture := newWebSocketFixture(t, newMutableSearchSnapshots(), func(config *Config) {
		config.MaximumSubscriptions = 1
	})
	client := fixture.dial()
	data := commandWireBytes(10, bytes.Repeat(commandWireBytes(1, nil), 2))
	data = append(data, commandWireBytes(1, []byte("over-limit"))...)
	if err := client.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatal(err)
	}
	if got := readEvent(t, client).GetProtocolError(); got == nil || got.GetRequestId() != "over-limit" || got.GetCode() != opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS || got.GetConnectionWillClose() {
		t.Fatalf("decode limit response = %v", got)
	}
	// Invalid request IDs retain validation precedence over command limits.
	data = append(data, commandWireBytes(1, []byte(strings.Repeat("x", maximumRequestIDBytes+1)))...)
	if err := client.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatal(err)
	}
	if got := readEvent(t, client).GetProtocolError(); got == nil || got.GetRequestId() != "" || got.GetCode() != opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND {
		t.Fatalf("invalid request ID response = %v", got)
	}
	writeCommand(t, client, &opensplunk.SearchWebSocketCommand{RequestId: "ping", Payload: &opensplunk.SearchWebSocketCommand_Ping{Ping: &opensplunk.SearchWebSocketPing{Nonce: "after-limit"}}})
	if got := readEvent(t, client).GetPong(); got == nil || got.GetNonce() != "after-limit" {
		t.Fatalf("recovery pong = %v", got)
	}
	// Unknown IDs still receive acknowledgments beyond the subscription limit.
	writeCommand(t, client, &opensplunk.SearchWebSocketCommand{RequestId: "unsubscribe", Payload: &opensplunk.SearchWebSocketCommand_Unsubscribe{Unsubscribe: &opensplunk.UnsubscribeSearchJobsCommand{SubscriptionIds: []string{"unknown-1", "unknown-2"}}}})
	for _, id := range []string{"unknown-1", "unknown-2"} {
		if got := readEvent(t, client).GetSubscriptionRemoved(); got == nil || got.GetSubscriptionId() != id {
			t.Fatalf("unknown unsubscribe response = %v", got)
		}
	}
}
