package main

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectoradmission"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

func TestCollectorRuntimePropagatesTrustedIngestionRateLimits(t *testing.T) {
	t.Parallel()

	tokenLimits := ingestquota.Limits{
		MaxEventsPerSecond:            700,
		MaxUncompressedBytesPerSecond: 7 << 20,
	}
	indexLimits := ingestquota.Limits{
		MaxEventsPerSecond:            300,
		MaxUncompressedBytesPerSecond: 3 << 20,
	}
	indexes := []auth.AuthorizedIndexPolicy{
		runtimeAuthorizedIndexPolicy("main", 30*24*time.Hour),
	}
	indexes[0].IngestionRateLimits = indexLimits
	authentication := auth.Authentication{
		TokenID:           "token-rate-limits",
		BoundCollectorID:  "collector-rate-limits",
		TokenRateLimits:   tokenLimits,
		AuthorizedIndexes: indexes,
	}

	t.Run("preliminary authorizer", func(t *testing.T) {
		authorization, err := (collectorAuthorizer{
			store: fakeCollectorAuthenticationStore{
				authentication: authentication,
			},
			tenantID: "tenant-rate-limits",
		}).Authorize(context.Background(), "secret")
		if err != nil {
			t.Fatal(err)
		}
		assertRuntimeIngestionRateLimits(
			t,
			authorization,
			tokenLimits,
			indexLimits,
		)
		authorization.AuthorizedIndexes[0].IngestionRateLimits = ingestquota.Limits{}
		if indexes[0].IngestionRateLimits != indexLimits {
			t.Fatal("preliminary authorization aliases authenticated index policy")
		}
	})

	t.Run("durable stream admission", func(t *testing.T) {
		lease := collectorfleet.Lease{
			TenantID:    "tenant-rate-limits",
			CollectorID: "collector-rate-limits",
			BootEpoch:   "boot-rate-limits",
			StreamID:    "stream-rate-limits",
			Generation:  1,
		}
		store := &fakeCollectorAdmissionRuntimeStore{
			admitResult: collectoradmission.Result{
				Authentication: authentication,
				Lease:          lease,
			},
		}
		admission, err := (collectorSessionManager{admission: store}).Admit(
			context.Background(),
			"secret",
			ingest.CollectorSessionAdmissionRequest{
				CollectorID: lease.CollectorID,
				BootEpoch:   lease.BootEpoch,
				StreamID:    lease.StreamID,
				AcceptedAt:  time.Now().UTC(),
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		assertRuntimeIngestionRateLimits(
			t,
			admission.Authorization,
			tokenLimits,
			indexLimits,
		)
		admission.Authorization.AuthorizedIndexes[0].IngestionRateLimits =
			ingestquota.Limits{}
		if indexes[0].IngestionRateLimits != indexLimits {
			t.Fatal("durable admission aliases authenticated index policy")
		}
	})
}

func assertRuntimeIngestionRateLimits(
	t *testing.T,
	authorization ingest.Authorization,
	wantToken ingestquota.Limits,
	wantIndex ingestquota.Limits,
) {
	t.Helper()

	if authorization.SubjectID != "token-rate-limits" ||
		authorization.TenantID != "tenant-rate-limits" ||
		authorization.CollectorID != "collector-rate-limits" ||
		authorization.TokenRateLimits != wantToken ||
		len(authorization.AuthorizedIndexes) != 1 ||
		authorization.AuthorizedIndexes[0].Name != "main" ||
		authorization.AuthorizedIndexes[0].IngestionRateLimits != wantIndex {
		t.Fatalf("runtime authorization = %+v", authorization)
	}
}
