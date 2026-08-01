package server

import (
	"net/http"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestIndexAdministrationPolicyRoundTripAgainstSQLite(t *testing.T) {
	t.Parallel()

	handler, _, _ := newAdminIntegrationHandler(t)
	definition := adminTestIndexProto("policy-round-trip")
	definition.DefaultSourcetype = stringPointer("go:zap:json")
	definition.Limits = adminIndexPolicyLimits(
		ingest.HardMaxEventBytes,
		ingest.HardMaxFields,
		ingest.HardMaxNestingDepth,
		ingest.HardMaxFutureSkew,
		ingest.HardMaxEventAge,
	)

	response := postProto(t, handler, "/api/v1/indexes/create", &opensplunkv1.CreateIndexRequest{Definition: definition})
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	var created opensplunkv1.CreateIndexResponse
	unmarshalResponse(t, response, &created)
	current := created.GetIndex()
	if current.GetIndexId() == "" || current.GetVersion() != 1 || !proto.Equal(current.GetDefinition(), definition) {
		t.Fatalf("created index = %+v, want definition %+v", current, definition)
	}

	response = postProto(t, handler, "/api/v1/indexes/get", &opensplunkv1.GetIndexRequest{Selector: adminIndexPolicySelector(current.GetIndexId())})
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", response.Code, response.Body.String())
	}
	var got opensplunkv1.GetIndexResponse
	unmarshalResponse(t, response, &got)
	if !proto.Equal(got.GetIndex(), current) {
		t.Fatalf("get index = %+v, want %+v", got.GetIndex(), current)
	}

	response = postProto(t, handler, "/api/v1/indexes/list", &opensplunkv1.ListIndexesRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", response.Code, response.Body.String())
	}
	var listed opensplunkv1.ListIndexesResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetIndexes()) != 1 || !proto.Equal(listed.GetIndexes()[0].GetIndex(), current) {
		t.Fatalf("listed indexes = %+v, want [%+v]", listed.GetIndexes(), current)
	}

	response = postProto(t, handler, "/api/v1/indexes/update", &opensplunkv1.UpdateIndexRequest{
		Selector:        adminIndexPolicySelector(current.GetIndexId()),
		ExpectedVersion: current.GetVersion(),
		Definition: &opensplunkv1.IndexDefinition{Limits: &opensplunkv1.IndexLimits{
			MaxEventBytes: uint64Pointer(ingest.HardMaxEventBytes + 1),
		}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"limits.max_event_bytes"}},
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("over-limit update status = %d, body = %s", response.Code, response.Body.String())
	}
	response = postProto(t, handler, "/api/v1/indexes/get", &opensplunkv1.GetIndexRequest{Selector: adminIndexPolicySelector(current.GetIndexId())})
	if response.Code != http.StatusOK {
		t.Fatalf("get after rejected update status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &got)
	if !proto.Equal(got.GetIndex(), current) {
		t.Fatalf("index after rejected update = %+v, want %+v", got.GetIndex(), current)
	}

	replacement := adminIndexPolicyLimits(256<<10, 128, 16, 2*time.Minute, 90*24*time.Hour)
	response = postProto(t, handler, "/api/v1/indexes/update", &opensplunkv1.UpdateIndexRequest{
		Selector:        adminIndexPolicySelector(current.GetIndexId()),
		ExpectedVersion: current.GetVersion(),
		Definition:      &opensplunkv1.IndexDefinition{Limits: replacement},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"limits"}},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("parent limits update status = %d, body = %s", response.Code, response.Body.String())
	}
	var updated opensplunkv1.UpdateIndexResponse
	unmarshalResponse(t, response, &updated)
	current = updated.GetIndex()
	if current.GetVersion() != 2 || current.GetDefinition().GetDefaultSourcetype() != "go:zap:json" ||
		!proto.Equal(current.GetDefinition().GetLimits(), replacement) {
		t.Fatalf("parent limits update = %+v", current)
	}

	response = postProto(t, handler, "/api/v1/indexes/update", &opensplunkv1.UpdateIndexRequest{
		Selector:        adminIndexPolicySelector(current.GetIndexId()),
		ExpectedVersion: current.GetVersion(),
		Definition: &opensplunkv1.IndexDefinition{Limits: &opensplunkv1.IndexLimits{
			MaxEventBytes:     uint64Pointer(0),
			MaxFieldCount:     uint32Pointer(0),
			MaxNestingDepth:   uint32Pointer(0),
			MaximumFutureSkew: durationpb.New(0),
			MaximumEventAge:   durationpb.New(0),
		}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
			"limits.max_event_bytes",
			"limits.max_field_count",
			"limits.max_nesting_depth",
			"limits.maximum_future_skew",
			"limits.maximum_event_age",
		}},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("leaf limits clear status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIndex()
	if current.GetVersion() != 3 || current.GetDefinition().GetLimits() != nil ||
		current.GetDefinition().GetDefaultSourcetype() != "go:zap:json" {
		t.Fatalf("leaf limits clear = %+v", current)
	}

	response = postProto(t, handler, "/api/v1/indexes/update", &opensplunkv1.UpdateIndexRequest{
		Selector:        adminIndexPolicySelector(current.GetIndexId()),
		ExpectedVersion: current.GetVersion(),
		Definition:      &opensplunkv1.IndexDefinition{DefaultSourcetype: stringPointer("")},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"default_sourcetype"}},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("default sourcetype clear status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIndex()
	if current.GetVersion() != 4 || current.GetDefinition().DefaultSourcetype != nil || current.GetDefinition().GetLimits() != nil {
		t.Fatalf("default sourcetype clear = %+v", current)
	}
}

func TestIndexAdministrationPolicyRejectsInvalidLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		limits *opensplunkv1.IndexLimits
	}{
		{
			name: "event bytes above hard ceiling",
			limits: &opensplunkv1.IndexLimits{
				MaxEventBytes: uint64Pointer(ingest.HardMaxEventBytes + 1),
			},
		},
		{
			name: "field count above hard ceiling",
			limits: &opensplunkv1.IndexLimits{
				MaxFieldCount: uint32Pointer(ingest.HardMaxFields + 1),
			},
		},
		{
			name: "nesting depth above hard ceiling",
			limits: &opensplunkv1.IndexLimits{
				MaxNestingDepth: uint32Pointer(ingest.HardMaxNestingDepth + 1),
			},
		},
		{
			name: "future skew above hard ceiling",
			limits: &opensplunkv1.IndexLimits{
				MaximumFutureSkew: durationpb.New(ingest.HardMaxFutureSkew + time.Nanosecond),
			},
		},
		{
			name: "event age above hard ceiling",
			limits: &opensplunkv1.IndexLimits{
				MaximumEventAge: durationpb.New(ingest.HardMaxEventAge + time.Nanosecond),
			},
		},
		{
			name: "negative future skew",
			limits: &opensplunkv1.IndexLimits{
				MaximumFutureSkew: durationpb.New(-time.Nanosecond),
			},
		},
		{
			name: "invalid event age duration",
			limits: &opensplunkv1.IndexLimits{
				MaximumEventAge: &durationpb.Duration{Seconds: 1, Nanos: 1_000_000_000},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			handler, _, _ := newAdminIntegrationHandler(t)
			definition := adminTestIndexProto("invalid-policy")
			definition.Limits = test.limits
			response := postProto(t, handler, "/api/v1/indexes/create", &opensplunkv1.CreateIndexRequest{Definition: definition})
			if response.Code != http.StatusBadRequest {
				t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
			}
			response = postProto(t, handler, "/api/v1/indexes/list", &opensplunkv1.ListIndexesRequest{})
			if response.Code != http.StatusOK {
				t.Fatalf("list status = %d, body = %s", response.Code, response.Body.String())
			}
			var listed opensplunkv1.ListIndexesResponse
			unmarshalResponse(t, response, &listed)
			if len(listed.GetIndexes()) != 0 {
				t.Fatalf("rejected create persisted indexes = %+v", listed.GetIndexes())
			}
		})
	}
}

func TestIndexAdministrationPolicyRejectsRetentionPastStorageHorizon(t *testing.T) {
	t.Parallel()

	handler, _, _ := newAdminIntegrationHandler(t)
	definition := adminTestIndexProto("invalid-retention-horizon")
	definition.RetentionPeriod = durationpb.New(8_000_000_000 * time.Second)
	response := postProto(
		t,
		handler,
		"/api/v1/indexes/create",
		&opensplunkv1.CreateIndexRequest{Definition: definition},
	)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	response = postProto(t, handler, "/api/v1/indexes/list", &opensplunkv1.ListIndexesRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", response.Code, response.Body.String())
	}
	var listed opensplunkv1.ListIndexesResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetIndexes()) != 0 {
		t.Fatalf("rejected retention persisted indexes = %+v", listed.GetIndexes())
	}
}

func TestIndexPolicySerializationRejectsLimitsAboveHardCeilings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*control.IndexLimits)
	}{
		{name: "event bytes", mutate: func(limits *control.IndexLimits) { limits.MaxEventBytes = ingest.HardMaxEventBytes + 1 }},
		{name: "field count", mutate: func(limits *control.IndexLimits) { limits.MaxFieldCount = ingest.HardMaxFields + 1 }},
		{name: "nesting depth", mutate: func(limits *control.IndexLimits) { limits.MaxNestingDepth = ingest.HardMaxNestingDepth + 1 }},
		{name: "future skew", mutate: func(limits *control.IndexLimits) {
			limits.MaximumFutureSkew = ingest.HardMaxFutureSkew + time.Nanosecond
		}},
		{name: "event age", mutate: func(limits *control.IndexLimits) { limits.MaximumEventAge = ingest.HardMaxEventAge + time.Nanosecond }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			record := control.Index{
				ID: "idx_policy", Version: 1, Definition: adminTestIndex("policy-serialization"),
				State: control.IndexStateActive, CreatedAt: testNow, UpdatedAt: testNow,
			}
			test.mutate(&record.Definition.Limits)
			if _, err := indexToProto(record); err == nil {
				t.Fatal("indexToProto accepted a limit above its ingestion hard ceiling")
			}
		})
	}
}

func TestIndexPolicySerializationRejectsRetentionPastStorageHorizon(t *testing.T) {
	t.Parallel()

	record := control.Index{
		ID: "idx_policy", Version: 1, Definition: adminTestIndex("policy-serialization"),
		State: control.IndexStateActive, CreatedAt: testNow, UpdatedAt: testNow,
	}
	record.Definition.RetentionPeriod = 8_000_000_000 * time.Second
	if _, err := indexToProto(record); err == nil {
		t.Fatal("indexToProto accepted retention past the storage horizon")
	}
}

func adminIndexPolicyLimits(
	maxEventBytes uint64,
	maxFieldCount uint32,
	maxNestingDepth uint32,
	maximumFutureSkew time.Duration,
	maximumEventAge time.Duration,
) *opensplunkv1.IndexLimits {
	return &opensplunkv1.IndexLimits{
		MaxEventBytes:     uint64Pointer(maxEventBytes),
		MaxFieldCount:     uint32Pointer(maxFieldCount),
		MaxNestingDepth:   uint32Pointer(maxNestingDepth),
		MaximumFutureSkew: durationpb.New(maximumFutureSkew),
		MaximumEventAge:   durationpb.New(maximumEventAge),
	}
}

func adminIndexPolicySelector(indexID string) *opensplunkv1.IndexSelector {
	return &opensplunkv1.IndexSelector{
		Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: indexID},
	}
}
