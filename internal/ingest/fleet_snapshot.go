package ingest

import (
	"fmt"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/collectorlimits"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maximumCollectorHelloCapabilities = collectorlimits.MaximumCapabilities
	maximumCollectorSnapshotInputs    = collectorlimits.MaximumInputs
)

func collectorHelloSnapshot(
	hello *opensplunkv1.CollectorHello,
) (collectorfleet.Hello, error) {
	if hello == nil ||
		hello.GetStartedAt() == nil ||
		hello.GetStartedAt().CheckValid() != nil {
		return collectorfleet.Hello{}, invalidCollectorSnapshot(
			"collector hello started_at is invalid",
		)
	}
	if len(hello.GetCapabilities()) > maximumCollectorHelloCapabilities {
		return collectorfleet.Hello{}, invalidCollectorSnapshot(
			"collector hello has too many capabilities",
		)
	}
	if len(hello.GetInputs()) > maximumCollectorSnapshotInputs {
		return collectorfleet.Hello{}, invalidCollectorSnapshot(
			"collector hello has too many inputs",
		)
	}
	capabilities := make([]uint32, len(hello.GetCapabilities()))
	for index, capability := range hello.GetCapabilities() {
		value, err := positiveFleetEnum(
			int32(capability),
			"collector hello contains an invalid capability",
		)
		if err != nil {
			return collectorfleet.Hello{}, err
		}
		capabilities[index] = value
	}
	inputs := make([]collectorfleet.InputRegistration, len(hello.GetInputs()))
	for index, input := range hello.GetInputs() {
		if input == nil {
			return collectorfleet.Hello{}, invalidCollectorSnapshot(
				"collector hello contains a missing input",
			)
		}
		inputType, err := positiveFleetEnum(
			int32(input.GetInputType()),
			"collector hello contains an invalid input type",
		)
		if err != nil {
			return collectorfleet.Hello{}, err
		}
		inputs[index] = collectorfleet.InputRegistration{
			InputID:    input.GetInputId(),
			InputType:  inputType,
			IndexName:  input.GetIndexName(),
			Source:     cloneOptionalString(input.Source),
			Sourcetype: cloneOptionalString(input.Sourcetype),
		}
	}
	return collectorfleet.Hello{
		InstanceID:                    hello.GetInstanceId(),
		ProtocolMajor:                 hello.GetProtocolMajor(),
		ProtocolMinor:                 hello.GetProtocolMinor(),
		CollectorVersion:              hello.GetCollectorVersion(),
		Hostname:                      hello.GetHostname(),
		OperatingSystem:               hello.GetOperatingSystem(),
		Architecture:                  hello.GetArchitecture(),
		StartedAt:                     hello.GetStartedAt().AsTime(),
		Capabilities:                  capabilities,
		Inputs:                        inputs,
		LastAcknowledgedBatchSequence: cloneOptionalUint64(hello.LastAcknowledgedBatchSequence),
	}, nil
}

func collectorHeartbeatSnapshot(
	heartbeat *opensplunkv1.CollectorHeartbeat,
	observationSequence uint64,
	receivedAt time.Time,
) (collectorfleet.Heartbeat, error) {
	if heartbeat == nil ||
		heartbeat.GetObservedAt() == nil ||
		heartbeat.GetObservedAt().CheckValid() != nil {
		return collectorfleet.Heartbeat{}, invalidCollectorSnapshot(
			"collector heartbeat observed_at is invalid",
		)
	}
	if len(heartbeat.GetInputs()) > maximumCollectorSnapshotInputs {
		return collectorfleet.Heartbeat{}, invalidCollectorSnapshot(
			"collector heartbeat has too many input health values",
		)
	}
	queue := collectorfleet.QueueTelemetry{}
	if heartbeat.GetQueue() != nil {
		wireQueue := heartbeat.GetQueue()
		queue = collectorfleet.QueueTelemetry{
			QueuedEvents:            wireQueue.GetQueuedEvents(),
			QueuedBytes:             wireQueue.GetQueuedBytes(),
			SentEventsTotal:         wireQueue.GetSentEventsTotal(),
			AcknowledgedEventsTotal: wireQueue.GetAcknowledgedEventsTotal(),
			RetriedBatchesTotal:     wireQueue.GetRetriedBatchesTotal(),
			RejectedEventsTotal:     wireQueue.GetRejectedEventsTotal(),
			DroppedEventsTotal:      wireQueue.GetDroppedEventsTotal(),
		}
		if wireQueue.GetOldestEventAge() != nil {
			if !DurationFitsResultRange(wireQueue.GetOldestEventAge()) {
				return collectorfleet.Heartbeat{}, invalidCollectorSnapshot(
					"collector heartbeat queue age is invalid",
				)
			}
			age := wireQueue.GetOldestEventAge().AsDuration()
			queue.OldestEventAge = &age
		}
	}
	inputs := make([]collectorfleet.InputHealth, len(heartbeat.GetInputs()))
	for index, input := range heartbeat.GetInputs() {
		if input == nil {
			return collectorfleet.Heartbeat{}, invalidCollectorSnapshot(
				"collector heartbeat contains missing input health",
			)
		}
		lastEventAt, err := optionalFleetTimestamp(input.GetLastEventAt())
		if err != nil {
			return collectorfleet.Heartbeat{}, err
		}
		lastErrorAt, err := optionalFleetTimestamp(input.GetLastErrorAt())
		if err != nil {
			return collectorfleet.Heartbeat{}, err
		}
		state, err := positiveFleetEnum(
			int32(input.GetState()),
			"collector heartbeat contains an invalid input state",
		)
		if err != nil {
			return collectorfleet.Heartbeat{}, err
		}
		inputs[index] = collectorfleet.InputHealth{
			InputID:           input.GetInputId(),
			State:             state,
			StatusMessage:     input.GetStatusMessage(),
			DiscoveredSources: input.GetDiscoveredSources(),
			ActiveSources:     input.GetActiveSources(),
			EventsReadTotal:   input.GetEventsReadTotal(),
			BytesReadTotal:    input.GetBytesReadTotal(),
			LastEventAt:       lastEventAt,
			LastErrorAt:       lastErrorAt,
		}
	}
	return collectorfleet.Heartbeat{
		ObservationSequence:           observationSequence,
		ObservedAt:                    heartbeat.GetObservedAt().AsTime(),
		ReceivedAt:                    receivedAt,
		Queue:                         queue,
		Inputs:                        inputs,
		LastSentBatchSequence:         cloneOptionalUint64(heartbeat.LastSentBatchSequence),
		LastAcknowledgedBatchSequence: cloneOptionalUint64(heartbeat.LastAcknowledgedBatchSequence),
		ProcessResidentMemoryBytes:    heartbeat.GetProcessResidentMemoryBytes(),
		ProcessCPUPercent:             heartbeat.GetProcessCpuPercent(),
	}, nil
}

func optionalFleetTimestamp(value *timestamppb.Timestamp) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	if value.CheckValid() != nil {
		return nil, invalidCollectorSnapshot(
			"collector heartbeat contains an invalid input timestamp",
		)
	}
	result := value.AsTime()
	return &result, nil
}

func cloneOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func cloneOptionalUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func positiveFleetEnum(value int32, message string) (uint32, error) {
	if value <= 0 {
		return 0, invalidCollectorSnapshot(message)
	}
	return uint32(value), nil
}

func invalidCollectorSnapshot(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidCollectorSnapshot, message)
}
