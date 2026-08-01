package server

import (
	"context"
	"net/http"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestIndexAdministrationIngestionRateLimitsRoundTripAndMasks(t *testing.T) {
	t.Parallel()

	handler, _, _ := newAdminIntegrationHandler(t)
	definition := adminTestIndexProto("rate-limited-index")
	definition.IngestionRateLimits = adminIngestionRateLimits(
		ingestquota.HardMaxEventsPerSecond,
		ingestquota.HardMaxUncompressedBytesPerSecond,
	)
	response := postProto(
		t,
		handler,
		"/api/v1/indexes/create",
		&opensplunkv1.CreateIndexRequest{Definition: definition},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	var created opensplunkv1.CreateIndexResponse
	unmarshalResponse(t, response, &created)
	current := created.GetIndex()
	if current.GetVersion() != 1 {
		t.Fatalf("created version = %d, want 1", current.GetVersion())
	}
	assertAdminIngestionRateLimits(
		t,
		current.GetDefinition().GetIngestionRateLimits(),
		ingestquota.HardMaxEventsPerSecond,
		ingestquota.HardMaxUncompressedBytesPerSecond,
	)

	response = postProto(
		t,
		handler,
		"/api/v1/indexes/get",
		&opensplunkv1.GetIndexRequest{
			Selector: adminIndexPolicySelector(current.GetIndexId()),
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", response.Code, response.Body.String())
	}
	var got opensplunkv1.GetIndexResponse
	unmarshalResponse(t, response, &got)
	assertAdminIngestionRateLimits(
		t,
		got.GetIndex().GetDefinition().GetIngestionRateLimits(),
		ingestquota.HardMaxEventsPerSecond,
		ingestquota.HardMaxUncompressedBytesPerSecond,
	)

	for name, invalid := range map[string]*opensplunkv1.IngestionRateLimits{
		"events": adminIngestionRateLimits(
			ingestquota.HardMaxEventsPerSecond+1,
			ingestquota.HardMaxUncompressedBytesPerSecond,
		),
		"bytes": adminIngestionRateLimits(
			ingestquota.HardMaxEventsPerSecond,
			ingestquota.HardMaxUncompressedBytesPerSecond+1,
		),
	} {
		t.Run("reject "+name+" above hard maximum", func(t *testing.T) {
			invalidResponse := postProto(
				t,
				handler,
				"/api/v1/indexes/update",
				&opensplunkv1.UpdateIndexRequest{
					Selector:        adminIndexPolicySelector(current.GetIndexId()),
					ExpectedVersion: current.GetVersion(),
					Definition: &opensplunkv1.IndexDefinition{
						IngestionRateLimits: invalid,
					},
					UpdateMask: &fieldmaskpb.FieldMask{
						Paths: []string{"ingestion_rate_limits"},
					},
				},
			)
			if invalidResponse.Code != http.StatusBadRequest {
				t.Fatalf(
					"over-limit update status = %d, body = %s",
					invalidResponse.Code,
					invalidResponse.Body.String(),
				)
			}
		})
	}

	response = postProto(
		t,
		handler,
		"/api/v1/indexes/update",
		&opensplunkv1.UpdateIndexRequest{
			Selector:        adminIndexPolicySelector(current.GetIndexId()),
			ExpectedVersion: current.GetVersion(),
			Definition: &opensplunkv1.IndexDefinition{
				IngestionRateLimits: adminIngestionRateLimits(400, 4<<20),
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"definition.ingestion_rate_limits"},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("whole update status = %d, body = %s", response.Code, response.Body.String())
	}
	var updated opensplunkv1.UpdateIndexResponse
	unmarshalResponse(t, response, &updated)
	current = updated.GetIndex()
	assertAdminIngestionRateLimits(
		t,
		current.GetDefinition().GetIngestionRateLimits(),
		400,
		4<<20,
	)

	response = postProto(
		t,
		handler,
		"/api/v1/indexes/update",
		&opensplunkv1.UpdateIndexRequest{
			Selector:        adminIndexPolicySelector(current.GetIndexId()),
			ExpectedVersion: current.GetVersion(),
			Definition: &opensplunkv1.IndexDefinition{
				IngestionRateLimits: adminIngestionRateLimits(250, 0),
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"ingestion_rate_limits.max_events_per_second",
				},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("leaf update status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIndex()
	assertAdminIngestionRateLimits(
		t,
		current.GetDefinition().GetIngestionRateLimits(),
		250,
		4<<20,
	)

	response = postProto(
		t,
		handler,
		"/api/v1/indexes/update",
		&opensplunkv1.UpdateIndexRequest{
			Selector:        adminIndexPolicySelector(current.GetIndexId()),
			ExpectedVersion: current.GetVersion(),
			Definition: &opensplunkv1.IndexDefinition{
				IngestionRateLimits: adminIngestionRateLimits(0, 0),
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"definition.ingestion_rate_limits.max_events_per_second",
				},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("leaf clear status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIndex()
	assertAdminIngestionRateLimits(
		t,
		current.GetDefinition().GetIngestionRateLimits(),
		0,
		4<<20,
	)

	response = postProto(
		t,
		handler,
		"/api/v1/indexes/update",
		&opensplunkv1.UpdateIndexRequest{
			Selector:        adminIndexPolicySelector(current.GetIndexId()),
			ExpectedVersion: current.GetVersion(),
			Definition: &opensplunkv1.IndexDefinition{
				IngestionRateLimits: adminIngestionRateLimits(0, 0),
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"ingestion_rate_limits.max_uncompressed_bytes_per_second",
				},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("byte leaf clear status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIndex()
	assertAdminIngestionRateLimits(
		t,
		current.GetDefinition().GetIngestionRateLimits(),
		0,
		0,
	)

	response = postProto(
		t,
		handler,
		"/api/v1/indexes/update",
		&opensplunkv1.UpdateIndexRequest{
			Selector:        adminIndexPolicySelector(current.GetIndexId()),
			ExpectedVersion: current.GetVersion(),
			Definition: &opensplunkv1.IndexDefinition{
				IngestionRateLimits: adminIngestionRateLimits(125, 1<<20),
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"ingestion_rate_limits"},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("whole restore status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIndex()
	assertAdminIngestionRateLimits(
		t,
		current.GetDefinition().GetIngestionRateLimits(),
		125,
		1<<20,
	)

	response = postProto(
		t,
		handler,
		"/api/v1/indexes/update",
		&opensplunkv1.UpdateIndexRequest{
			Selector:        adminIndexPolicySelector(current.GetIndexId()),
			ExpectedVersion: current.GetVersion(),
			Definition:      &opensplunkv1.IndexDefinition{},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"ingestion_rate_limits"},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("whole clear status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIndex()
	assertAdminIngestionRateLimits(
		t,
		current.GetDefinition().GetIngestionRateLimits(),
		0,
		0,
	)

	response = postProto(
		t,
		handler,
		"/api/v1/indexes/get",
		&opensplunkv1.GetIndexRequest{
			Selector: adminIndexPolicySelector(current.GetIndexId()),
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("get cleared index status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &got)
	assertAdminIngestionRateLimits(
		t,
		got.GetIndex().GetDefinition().GetIngestionRateLimits(),
		0,
		0,
	)
}

func TestIngestionTokenAdministrationRateLimitsRoundTripAndMasks(t *testing.T) {
	t.Parallel()

	handler, database, _ := newAdminIntegrationHandler(t)
	if _, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("rate-token-index"),
	); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	collectorID := "collector-rate-limits"
	response := postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/create",
		&opensplunkv1.CreateIngestionTokenRequest{
			Definition: &opensplunkv1.IngestionTokenDefinition{
				Name: "rate limited token",
				Constraints: &opensplunkv1.IngestionTokenConstraints{
					AllowedIndexNames: []string{"rate-token-index"},
					BoundCollectorId:  &collectorID,
				},
				IngestionRateLimits: adminIngestionRateLimits(
					ingestquota.HardMaxEventsPerSecond,
					ingestquota.HardMaxUncompressedBytesPerSecond,
				),
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	var created opensplunkv1.CreateIngestionTokenResponse
	unmarshalResponse(t, response, &created)
	current := created.GetIngestionToken()
	if current.GetVersion() != 1 || created.GetPlaintextToken() == "" {
		t.Fatalf("created token = %+v", current)
	}
	assertAdminIngestionRateLimits(
		t,
		current.GetIngestionRateLimits(),
		ingestquota.HardMaxEventsPerSecond,
		ingestquota.HardMaxUncompressedBytesPerSecond,
	)

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/get",
		&opensplunkv1.GetIngestionTokenRequest{
			IngestionTokenId: current.GetIngestionTokenId(),
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", response.Code, response.Body.String())
	}
	var got opensplunkv1.GetIngestionTokenResponse
	unmarshalResponse(t, response, &got)
	assertAdminIngestionRateLimits(
		t,
		got.GetIngestionToken().GetIngestionRateLimits(),
		ingestquota.HardMaxEventsPerSecond,
		ingestquota.HardMaxUncompressedBytesPerSecond,
	)

	for name, invalid := range map[string]*opensplunkv1.IngestionRateLimits{
		"events": adminIngestionRateLimits(
			ingestquota.HardMaxEventsPerSecond+1,
			ingestquota.HardMaxUncompressedBytesPerSecond,
		),
		"bytes": adminIngestionRateLimits(
			ingestquota.HardMaxEventsPerSecond,
			ingestquota.HardMaxUncompressedBytesPerSecond+1,
		),
	} {
		t.Run("reject "+name+" above hard maximum", func(t *testing.T) {
			invalidResponse := postProto(
				t,
				handler,
				"/api/v1/ingestion-tokens/update",
				&opensplunkv1.UpdateIngestionTokenRequest{
					IngestionTokenId: current.GetIngestionTokenId(),
					ExpectedVersion:  current.GetVersion(),
					Definition: &opensplunkv1.IngestionTokenDefinition{
						IngestionRateLimits: invalid,
					},
					UpdateMask: &fieldmaskpb.FieldMask{
						Paths: []string{"ingestion_rate_limits"},
					},
				},
			)
			if invalidResponse.Code != http.StatusBadRequest {
				t.Fatalf(
					"over-limit update status = %d, body = %s",
					invalidResponse.Code,
					invalidResponse.Body.String(),
				)
			}
		})
	}

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/update",
		&opensplunkv1.UpdateIngestionTokenRequest{
			IngestionTokenId: current.GetIngestionTokenId(),
			ExpectedVersion:  current.GetVersion(),
			Definition: &opensplunkv1.IngestionTokenDefinition{
				IngestionRateLimits: adminIngestionRateLimits(800, 8<<20),
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"definition.ingestion_rate_limits"},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("whole update status = %d, body = %s", response.Code, response.Body.String())
	}
	var updated opensplunkv1.UpdateIngestionTokenResponse
	unmarshalResponse(t, response, &updated)
	current = updated.GetIngestionToken()
	assertAdminIngestionRateLimits(t, current.GetIngestionRateLimits(), 800, 8<<20)

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/update",
		&opensplunkv1.UpdateIngestionTokenRequest{
			IngestionTokenId: current.GetIngestionTokenId(),
			ExpectedVersion:  current.GetVersion(),
			Definition: &opensplunkv1.IngestionTokenDefinition{
				IngestionRateLimits: adminIngestionRateLimits(600, 0),
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"ingestion_rate_limits.max_events_per_second",
				},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("leaf update status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIngestionToken()
	assertAdminIngestionRateLimits(t, current.GetIngestionRateLimits(), 600, 8<<20)

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/update",
		&opensplunkv1.UpdateIngestionTokenRequest{
			IngestionTokenId: current.GetIngestionTokenId(),
			ExpectedVersion:  current.GetVersion(),
			Definition: &opensplunkv1.IngestionTokenDefinition{
				IngestionRateLimits: adminIngestionRateLimits(0, 0),
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"definition.ingestion_rate_limits.max_events_per_second",
				},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("leaf clear status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIngestionToken()
	assertAdminIngestionRateLimits(t, current.GetIngestionRateLimits(), 0, 8<<20)

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/update",
		&opensplunkv1.UpdateIngestionTokenRequest{
			IngestionTokenId: current.GetIngestionTokenId(),
			ExpectedVersion:  current.GetVersion(),
			Definition: &opensplunkv1.IngestionTokenDefinition{
				IngestionRateLimits: adminIngestionRateLimits(0, 0),
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"ingestion_rate_limits.max_uncompressed_bytes_per_second",
				},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("byte leaf clear status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIngestionToken()
	assertAdminIngestionRateLimits(t, current.GetIngestionRateLimits(), 0, 0)

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/update",
		&opensplunkv1.UpdateIngestionTokenRequest{
			IngestionTokenId: current.GetIngestionTokenId(),
			ExpectedVersion:  current.GetVersion(),
			Definition: &opensplunkv1.IngestionTokenDefinition{
				IngestionRateLimits: adminIngestionRateLimits(350, 2<<20),
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"ingestion_rate_limits"},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("whole restore status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIngestionToken()
	assertAdminIngestionRateLimits(t, current.GetIngestionRateLimits(), 350, 2<<20)

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/update",
		&opensplunkv1.UpdateIngestionTokenRequest{
			IngestionTokenId: current.GetIngestionTokenId(),
			ExpectedVersion:  current.GetVersion(),
			Definition:       &opensplunkv1.IngestionTokenDefinition{},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"ingestion_rate_limits"},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("whole clear status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &updated)
	current = updated.GetIngestionToken()
	assertAdminIngestionRateLimits(t, current.GetIngestionRateLimits(), 0, 0)

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/get",
		&opensplunkv1.GetIngestionTokenRequest{
			IngestionTokenId: current.GetIngestionTokenId(),
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("get cleared token status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, &got)
	assertAdminIngestionRateLimits(
		t,
		got.GetIngestionToken().GetIngestionRateLimits(),
		0,
		0,
	)
}

func adminIngestionRateLimits(
	eventsPerSecond uint64,
	uncompressedBytesPerSecond uint64,
) *opensplunkv1.IngestionRateLimits {
	return &opensplunkv1.IngestionRateLimits{
		MaxEventsPerSecond:            uint64Pointer(eventsPerSecond),
		MaxUncompressedBytesPerSecond: uint64Pointer(uncompressedBytesPerSecond),
	}
}

func assertAdminIngestionRateLimits(
	t *testing.T,
	limits *opensplunkv1.IngestionRateLimits,
	wantEventsPerSecond uint64,
	wantUncompressedBytesPerSecond uint64,
) {
	t.Helper()

	if wantEventsPerSecond == 0 && wantUncompressedBytesPerSecond == 0 {
		if limits != nil {
			t.Fatalf("unlimited rate policy = %+v, want nil canonical encoding", limits)
		}
		return
	}
	if limits == nil ||
		limits.GetMaxEventsPerSecond() != wantEventsPerSecond ||
		limits.GetMaxUncompressedBytesPerSecond() != wantUncompressedBytesPerSecond {
		t.Fatalf(
			"rate limits = %+v, want events=%d bytes=%d",
			limits,
			wantEventsPerSecond,
			wantUncompressedBytesPerSecond,
		)
	}
	if (wantEventsPerSecond == 0) != (limits.MaxEventsPerSecond == nil) ||
		(wantUncompressedBytesPerSecond == 0) !=
			(limits.MaxUncompressedBytesPerSecond == nil) {
		t.Fatalf("rate-limit optional-field encoding = %+v", limits)
	}
}
