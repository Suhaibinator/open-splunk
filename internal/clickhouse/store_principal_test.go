package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func TestStorePropagatesStablePrincipalWithoutRateQuota(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"native", "grouped-native", "hec-stage"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			sequencer := &fakeVisibilitySequencer{}
			store := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{}}, fixedRetention(time.Hour), sequencer)
			batch := validStoreBatch()
			if batch.QuotaAdmission != nil {
				t.Fatal("test requires a legitimate caller without rate quota")
			}
			source := ingest.NativeCollectorSource(batch.CollectorID)
			var err error
			switch mode {
			case "native":
				_, err = store.Store(context.Background(), batch)
			case "grouped-native":
				_, _, _, _, err = store.reserveForGroupedStore(context.Background(), batch, false)
			case "hec-stage":
				source = ingest.HECSource("stable-hec-token")
				batch.Source = source
				batch.CollectorID = ""
				batch.Events[0].Source = source
				batch.Events[0].CollectorID = ""
				batch.HECAdmission = &ingest.HECStageAdmission{
					TokenID: source.ID, TokenVersion: 1, RequestID: batch.BatchID, CreatedAt: batch.ReceivedAt,
				}
				_, err = store.Stage(context.Background(), batch)
			}
			if err != nil {
				t.Fatal(err)
			}
			want := ingestionPrincipalSHA256(batch.TenantID, source)
			if len(sequencer.reserveRequests) != 1 || sequencer.reserveRequests[0].PrincipalSHA256 != want || want == ([sha256.Size]byte{}) {
				t.Fatalf("fresh reservation did not carry stable source: %+v", sequencer.reserveRequests)
			}
		})
	}
}

func TestStorePrincipalIgnoresRequestMetadataAndCanonicalizesNativeSource(t *testing.T) {
	t.Parallel()
	store := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{}}, fixedRetention(time.Hour), &fakeVisibilitySequencer{})
	first := validStoreBatch()
	firstPayload, err := store.freshReservationPayload(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	second := validStoreBatch()
	second.Source = ingest.NativeCollectorSource(second.CollectorID)
	second.Events[0].Source = second.Source
	second.BatchID = "other-batch"
	second.BatchSequence++
	second.Events[0].BatchID = second.BatchID
	second.Events[0].Event.Host = "other-host"
	second.Events[0].Event.Source = "other-event-source"
	secondPayload, err := store.freshReservationPayload(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if firstPayload.principalSHA256 != secondPayload.principalSHA256 {
		t.Fatal("request metadata or explicit native source created a new pending budget")
	}
	for _, test := range []struct {
		name   string
		tenant string
		source ingest.IngestionSource
	}{
		{name: "tenant", tenant: "other-tenant", source: second.Source},
		{name: "source", tenant: first.TenantID, source: ingest.NativeCollectorSource("other-collector")},
		{name: "kind", tenant: first.TenantID, source: ingest.HECSource(first.CollectorID)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if other := ingestionPrincipalSHA256(test.tenant, test.source); other == firstPayload.principalSHA256 {
				t.Fatal("independent tenant/source shared a budget")
			}
		})
	}
	if ingestionPrincipalSHA256("ab", ingest.NativeCollectorSource("c")) == ingestionPrincipalSHA256("a", ingest.NativeCollectorSource("bc")) {
		t.Fatal("principal framing aliases different identities")
	}
}

func TestStagePrincipalIgnoresHECChannelRequestAndIndex(t *testing.T) {
	t.Parallel()
	var first [sha256.Size]byte
	for _, variant := range []string{"first", "second"} {
		sequencer := &fakeVisibilitySequencer{}
		store := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{}}, fixedRetention(time.Hour), sequencer)
		batch := validStoreBatch()
		batch.Source = ingest.HECSource("same-hec-token")
		batch.CollectorID = ""
		batch.BatchID = variant
		batch.Events[0].Source = batch.Source
		batch.Events[0].CollectorID = ""
		batch.Events[0].BatchID = variant
		batch.Events[0].Event.IndexName = variant
		batch.HECAdmission = &ingest.HECStageAdmission{
			TokenID: batch.Source.ID, TokenVersion: 1, RequestID: variant,
			Channel: variant, CreatedAt: batch.ReceivedAt,
		}
		if _, err := store.Stage(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
		principal := sequencer.reserveRequests[0].PrincipalSHA256
		if first == ([sha256.Size]byte{}) {
			first = principal
		} else if principal != first {
			t.Fatal("HEC channel, request or index variation reset pending budget")
		}
	}
}

func TestStorePrincipalCapacityUsesExistingRetryContract(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{reserveErr: visibility.ErrPendingCapacity}
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	_, err := store.Store(context.Background(), validStoreBatch())
	var transient *ingest.TransientStoreError
	if !errors.As(err, &transient) || transient.Reason != opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY || transient.RetryAfter <= 0 {
		t.Fatalf("pending capacity retry = %v", err)
	}
	if connection.prepareCalls != 0 {
		t.Fatal("denied principal reached physical storage")
	}
}
