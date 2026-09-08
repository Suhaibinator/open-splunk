package searchws

import (
	"errors"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

type commandWireMessage uint8

const (
	commandWireRoot commandWireMessage = iota
	commandWireSubscribe
	commandWireUnsubscribe
	commandWireSubscription
	commandWireTarget
	commandWirePing
)

var errCommandWire = errors.New("command is not valid protobuf")

type commandDecodeBudget struct {
	subscriptions  int
	unsubscribeIDs int
	fields         int
	failure        *commandFailure
}

// decodeCommand bounds materialized objects before protobuf decoding. Counts
// cover every wire occurrence, including merged or subsequently overwritten
// oneof payloads. The field budget also bounds duplicate singular messages and
// optional scalar allocations; it allows every ordinary command at its limits.
func decodeCommand(data []byte, maximumSubscriptions uint32, maximumUnsubscribeIDs int) (*opensplunk.SearchWebSocketCommand, *commandFailure) {
	budget := commandDecodeBudget{
		subscriptions:  int(maximumSubscriptions),
		unsubscribeIDs: maximumUnsubscribeIDs,
		fields:         16 + 8*int(maximumSubscriptions) + maximumUnsubscribeIDs,
	}
	requestID, err := budget.scan(data, commandWireRoot)
	if err != nil {
		return nil, nil
	}
	command := &opensplunk.SearchWebSocketCommand{}
	if budget.failure != nil {
		// Retain only the bounded final request ID for the ordinary validation
		// and correlated error path. No repeated message has been allocated.
		if len(requestID) <= maximumRequestIDBytes {
			command.RequestId = string(requestID)
		}
		return command, budget.failure
	}
	if err := proto.Unmarshal(data, command); err != nil {
		return nil, nil
	}
	return command, nil
}

func (budget *commandDecodeBudget) scan(data []byte, message commandWireMessage) ([]byte, error) {
	var requestID []byte
	for len(data) != 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(data)
		if tagBytes < 0 {
			return nil, errCommandWire
		}
		data = data[tagBytes:]
		var value []byte
		var valueBytes int
		if wireType == protowire.BytesType {
			value, valueBytes = protowire.ConsumeBytes(data)
		} else {
			valueBytes = protowire.ConsumeFieldValue(number, wireType, data)
		}
		if valueBytes < 0 {
			return nil, errCommandWire
		}
		data = data[valueBytes:]
		if message == commandWireRoot && number == 1 && wireType == protowire.BytesType {
			requestID = value
		}
		if budget.failure != nil {
			// Finish only the outer envelope to find a request ID even when it
			// follows the payload. Length-delimited children remain opaque.
			if message == commandWireRoot {
				continue
			}
			return nil, nil
		}
		budget.fields--
		if budget.fields < 0 {
			budget.failure = invalidCommand("command exceeds the protobuf decode limit", fieldViolation("payload", "RESOURCE_LIMIT", "command contains too many protobuf field occurrences"))
			continue
		}
		// A valid unexpected wire type is an unknown field to proto.Unmarshal.
		if wireType != protowire.BytesType {
			continue
		}
		child := commandWireRoot
		switch message {
		case commandWireRoot:
			switch number {
			case 10:
				child = commandWireSubscribe
			case 11:
				child = commandWireUnsubscribe
			case 12:
				child = commandWirePing
			}
		case commandWireSubscribe:
			if number == 1 {
				budget.subscriptions--
				if budget.subscriptions < 0 {
					budget.failure = &commandFailure{code: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS, message: "subscription limit exceeded"}
					return nil, nil
				}
				child = commandWireSubscription
			}
		case commandWireUnsubscribe:
			if number == 1 {
				budget.unsubscribeIDs--
				if budget.unsubscribeIDs < 0 {
					budget.failure = invalidCommand("unsubscribe response exceeds the configured queue capacity", fieldViolation("unsubscribe.subscription_ids", "RESOURCE_LIMIT", "command contains too many subscription_ids for one response batch"))
					return nil, nil
				}
			}
		case commandWireSubscription:
			if number == 2 {
				child = commandWireTarget
			}
		case commandWireTarget, commandWirePing:
		}
		if child != commandWireRoot {
			if _, err := budget.scan(value, child); err != nil {
				return nil, err
			}
		}
	}
	return requestID, nil
}
