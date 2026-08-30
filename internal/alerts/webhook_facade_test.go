package alerts

import (
	"errors"
	"testing"
	"time"
)

func TestWebhookFacadePreservesAlertDomainTypesAndErrors(t *testing.T) {
	t.Parallel()
	_, err := BuildSignedPayload(WebhookPayload{
		AlertID: "alert-1", AlertRunID: "run-1", SearchJobID: "job-1",
		AlertName: "errors", Application: "search", DeliveryAt: time.Now(),
		Operator: ConditionEqual, ResultsURL: "https://splunk.example.test/search/?searchJobId=job-1",
	}, "delivery-1", nil)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("BuildSignedPayload() error = %v; want alert-domain invalid argument", err)
	}
	if _, err := ParseDestination("http://hooks.example.test"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ParseDestination() error = %v; want alert-domain invalid argument", err)
	}
}
