package ingest

import (
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCollectorHelloSnapshotMapsAndDetachesCompleteWireState(t *testing.T) {
	t.Parallel()
	source := "/var/log/app.log"
	sourcetype := "json"
	lastAcknowledged := uint64(41)
	hello := &opensplunkv1.CollectorHello{
		CollectorId:      "collector-a",
		InstanceId:       "instance-a",
		ProtocolMajor:    1,
		ProtocolMinor:    2,
		CollectorVersion: "1.2.3",
		Hostname:         "host-a",
		OperatingSystem:  "linux",
		Architecture:     "arm64",
		StartedAt:        timestamppb.New(validationTestNow.Add(-time.Hour)),
		Capabilities: []opensplunkv1.CollectorCapability{
			opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_FILE_INPUT,
			opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_DURABLE_QUEUE,
		},
		Inputs: []*opensplunkv1.CollectorInputRegistration{{
			InputId:    "input-a",
			InputType:  opensplunkv1.CollectorInputType_COLLECTOR_INPUT_TYPE_FILE,
			IndexName:  "main",
			Source:     &source,
			Sourcetype: &sourcetype,
		}},
		LastAcknowledgedBatchSequence: &lastAcknowledged,
	}
	snapshot, err := collectorHelloSnapshot(hello)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.InstanceID != "instance-a" ||
		snapshot.ProtocolMinor != 2 ||
		snapshot.CollectorVersion != "1.2.3" ||
		snapshot.Hostname != "host-a" ||
		snapshot.OperatingSystem != "linux" ||
		snapshot.Architecture != "arm64" ||
		!snapshot.StartedAt.Equal(hello.GetStartedAt().AsTime()) ||
		len(snapshot.Capabilities) != 2 ||
		len(snapshot.Inputs) != 1 ||
		snapshot.Inputs[0].InputID != "input-a" ||
		snapshot.Inputs[0].Source == nil ||
		*snapshot.Inputs[0].Source != source ||
		snapshot.LastAcknowledgedBatchSequence == nil ||
		*snapshot.LastAcknowledgedBatchSequence != lastAcknowledged {
		t.Fatalf("hello snapshot = %+v", snapshot)
	}
	hello.Capabilities[0] = opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_GZIP
	*hello.Inputs[0].Source = "mutated"
	*hello.LastAcknowledgedBatchSequence = 99
	if snapshot.Capabilities[0] != uint32(
		opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_FILE_INPUT,
	) ||
		*snapshot.Inputs[0].Source != "/var/log/app.log" ||
		*snapshot.LastAcknowledgedBatchSequence != 41 {
		t.Fatal("hello snapshot aliases mutable protobuf state")
	}
}

func TestCollectorHelloSnapshotRejectsUnboundedOrInvalidWireState(t *testing.T) {
	t.Parallel()
	valid := func() *opensplunkv1.CollectorHello {
		return &opensplunkv1.CollectorHello{
			StartedAt: timestamppb.New(validationTestNow),
		}
	}
	tests := []struct {
		name   string
		mutate func(*opensplunkv1.CollectorHello)
	}{
		{
			name: "invalid time",
			mutate: func(hello *opensplunkv1.CollectorHello) {
				hello.StartedAt = &timestamppb.Timestamp{Seconds: 253_402_300_800}
			},
		},
		{
			name: "too many capabilities",
			mutate: func(hello *opensplunkv1.CollectorHello) {
				hello.Capabilities = make(
					[]opensplunkv1.CollectorCapability,
					maximumCollectorHelloCapabilities+1,
				)
			},
		},
		{
			name: "too many inputs",
			mutate: func(hello *opensplunkv1.CollectorHello) {
				hello.Inputs = make(
					[]*opensplunkv1.CollectorInputRegistration,
					maximumCollectorSnapshotInputs+1,
				)
			},
		},
		{
			name: "missing input",
			mutate: func(hello *opensplunkv1.CollectorHello) {
				hello.Inputs = []*opensplunkv1.CollectorInputRegistration{nil}
			},
		},
		{
			name: "unspecified capability",
			mutate: func(hello *opensplunkv1.CollectorHello) {
				hello.Capabilities = []opensplunkv1.CollectorCapability{0}
			},
		},
		{
			name: "negative capability",
			mutate: func(hello *opensplunkv1.CollectorHello) {
				hello.Capabilities = []opensplunkv1.CollectorCapability{-1}
			},
		},
		{
			name: "unspecified input type",
			mutate: func(hello *opensplunkv1.CollectorHello) {
				hello.Inputs = []*opensplunkv1.CollectorInputRegistration{{
					InputType: 0,
				}}
			},
		},
		{
			name: "negative input type",
			mutate: func(hello *opensplunkv1.CollectorHello) {
				hello.Inputs = []*opensplunkv1.CollectorInputRegistration{{
					InputType: -1,
				}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hello := valid()
			test.mutate(hello)
			if _, err := collectorHelloSnapshot(hello); err == nil {
				t.Fatal("collectorHelloSnapshot unexpectedly succeeded")
			}
		})
	}
}

func TestCollectorHeartbeatSnapshotMapsCompleteWireState(t *testing.T) {
	t.Parallel()
	oldestAge := 10*time.Second + 5*time.Nanosecond
	lastSent := uint64(12)
	lastAcknowledged := uint64(11)
	lastEventAt := validationTestNow.Add(-time.Minute)
	lastErrorAt := validationTestNow.Add(-2 * time.Minute)
	heartbeat := &opensplunkv1.CollectorHeartbeat{
		CollectorId: "collector-a",
		InstanceId:  "instance-a",
		ObservedAt:  timestamppb.New(validationTestNow),
		Queue: &opensplunkv1.CollectorQueueStats{
			QueuedEvents:            1,
			QueuedBytes:             2,
			OldestEventAge:          durationpb.New(oldestAge),
			SentEventsTotal:         3,
			AcknowledgedEventsTotal: 4,
			RetriedBatchesTotal:     5,
			RejectedEventsTotal:     6,
			DroppedEventsTotal:      7,
		},
		Inputs: []*opensplunkv1.CollectorInputHealth{{
			InputId:           "input-a",
			State:             opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY,
			StatusMessage:     "healthy",
			DiscoveredSources: 8,
			ActiveSources:     7,
			EventsReadTotal:   9,
			BytesReadTotal:    10,
			LastEventAt:       timestamppb.New(lastEventAt),
			LastErrorAt:       timestamppb.New(lastErrorAt),
		}},
		LastSentBatchSequence:         &lastSent,
		LastAcknowledgedBatchSequence: &lastAcknowledged,
		ProcessResidentMemoryBytes:    1024,
		ProcessCpuPercent:             12.5,
	}
	snapshot, err := collectorHeartbeatSnapshot(
		heartbeat,
		17,
		validationTestNow.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ObservationSequence != 17 ||
		snapshot.Queue.OldestEventAge == nil ||
		*snapshot.Queue.OldestEventAge != oldestAge ||
		snapshot.Queue.DroppedEventsTotal != 7 ||
		len(snapshot.Inputs) != 1 ||
		snapshot.Inputs[0].State != uint32(
			opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY,
		) ||
		snapshot.Inputs[0].LastEventAt == nil ||
		!snapshot.Inputs[0].LastEventAt.Equal(lastEventAt) ||
		snapshot.LastSentBatchSequence == nil ||
		*snapshot.LastSentBatchSequence != lastSent ||
		snapshot.ProcessResidentMemoryBytes != 1024 ||
		snapshot.ProcessCPUPercent != 12.5 {
		t.Fatalf("heartbeat snapshot = %+v", snapshot)
	}
	heartbeat.Inputs[0].StatusMessage = "mutated"
	*heartbeat.LastSentBatchSequence = 99
	if snapshot.Inputs[0].StatusMessage != "healthy" ||
		*snapshot.LastSentBatchSequence != 12 {
		t.Fatal("heartbeat snapshot aliases mutable protobuf state")
	}
}

func TestCollectorHeartbeatSnapshotRejectsLossyOrUnboundedWireState(t *testing.T) {
	t.Parallel()
	valid := func() *opensplunkv1.CollectorHeartbeat {
		return &opensplunkv1.CollectorHeartbeat{
			ObservedAt: timestamppb.New(validationTestNow),
		}
	}
	tests := []struct {
		name   string
		mutate func(*opensplunkv1.CollectorHeartbeat)
	}{
		{
			name: "duration outside Go range",
			mutate: func(heartbeat *opensplunkv1.CollectorHeartbeat) {
				heartbeat.Queue = &opensplunkv1.CollectorQueueStats{
					OldestEventAge: &durationpb.Duration{
						Seconds: 315_576_000_000,
					},
				}
			},
		},
		{
			name: "invalid optional timestamp",
			mutate: func(heartbeat *opensplunkv1.CollectorHeartbeat) {
				heartbeat.Inputs = []*opensplunkv1.CollectorInputHealth{{
					LastEventAt: &timestamppb.Timestamp{
						Seconds: 253_402_300_800,
					},
				}}
			},
		},
		{
			name: "too many inputs",
			mutate: func(heartbeat *opensplunkv1.CollectorHeartbeat) {
				heartbeat.Inputs = make(
					[]*opensplunkv1.CollectorInputHealth,
					maximumCollectorSnapshotInputs+1,
				)
			},
		},
		{
			name: "missing input",
			mutate: func(heartbeat *opensplunkv1.CollectorHeartbeat) {
				heartbeat.Inputs = []*opensplunkv1.CollectorInputHealth{nil}
			},
		},
		{
			name: "unspecified input state",
			mutate: func(heartbeat *opensplunkv1.CollectorHeartbeat) {
				heartbeat.Inputs = []*opensplunkv1.CollectorInputHealth{{
					State: 0,
				}}
			},
		},
		{
			name: "negative input state",
			mutate: func(heartbeat *opensplunkv1.CollectorHeartbeat) {
				heartbeat.Inputs = []*opensplunkv1.CollectorInputHealth{{
					State: -1,
				}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			heartbeat := valid()
			test.mutate(heartbeat)
			if _, err := collectorHeartbeatSnapshot(
				heartbeat,
				2,
				validationTestNow,
			); err == nil {
				t.Fatal("collectorHeartbeatSnapshot unexpectedly succeeded")
			}
		})
	}

	heartbeat := valid()
	heartbeat.Inputs = []*opensplunkv1.CollectorInputHealth{{
		StatusMessage: strings.Repeat("x", 1),
		LastErrorAt:   timestamppb.New(validationTestNow),
		State:         opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY,
	}}
	snapshot, err := collectorHeartbeatSnapshot(heartbeat, 2, validationTestNow)
	if err != nil || snapshot.Inputs[0].LastErrorAt == nil {
		t.Fatalf("valid optional heartbeat timestamp = (%+v, %v)", snapshot, err)
	}
}
