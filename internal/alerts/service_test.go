package alerts

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

type memoryRepository struct {
	alert         Alert
	createRecord  CreateRecord
	createCalls   int
	setStateCalls int
	deleteErr     error
	deliveryID    string
}

func (repository *memoryRepository) FindCreateReplay(_ context.Context, ownerID, clientRequestID string, fingerprint [32]byte) (Alert, bool, error) {
	if repository.createRecord.ClientRequestID != clientRequestID || repository.alert.OwnerID != ownerID {
		return Alert{}, false, nil
	}
	if repository.createRecord.RequestFingerprint != fingerprint {
		return Alert{}, false, ErrIdempotencyConflict
	}
	return repository.alert, true, nil
}

func (repository *memoryRepository) Create(_ context.Context, record CreateRecord) (CreateResult, error) {
	repository.createCalls++
	if record.ClientRequestID != "" && repository.createRecord.ClientRequestID == record.ClientRequestID {
		if repository.createRecord.RequestFingerprint != record.RequestFingerprint {
			return CreateResult{}, ErrIdempotencyConflict
		}
		return CreateResult{Alert: repository.alert, Disposition: CreateReplayed}, nil
	}
	repository.createRecord = record
	repository.alert = Alert{
		ID: record.ID, OwnerID: record.OwnerID, Version: 1, State: record.State,
		Definition: record.Definition, Endpoint: record.Endpoint,
		EndpointGeneration: record.EndpointGeneration, WebhookHostname: record.WebhookHostname, SecretGeneration: record.SecretGeneration,
		CreatedAt: record.CreatedAt, UpdatedAt: record.CreatedAt,
	}
	return CreateResult{Alert: repository.alert, Disposition: CreateCommitted}, nil
}

func TestServiceCreateRetryReplaysWithoutReissuingSecret(t *testing.T) {
	t.Parallel()
	repository := &memoryRepository{}
	service := newTestService(t, repository, "")
	input := CreateInput{
		OwnerID: "owner-1", ClientRequestID: "alert-create-request-0001",
		Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	}
	committed, err := service.Create(context.Background(), input)
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	if committed.Replayed || committed.PlaintextSecret == "" {
		t.Fatalf("first Create() = %#v", committed)
	}
	replayed, err := service.Create(context.Background(), input)
	if err != nil {
		t.Fatalf("replayed Create() error = %v", err)
	}
	if !replayed.Replayed || replayed.Alert.ID != committed.Alert.ID || replayed.PlaintextSecret != "" || repository.createCalls != 1 {
		t.Fatalf("replayed Create() = %#v, repository calls = %d", replayed, repository.createCalls)
	}
	changed := input
	changed.Definition = validDefinition()
	changed.Definition.Description = "different request intent"
	if _, err := service.Create(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed Create() error = %v, want ErrIdempotencyConflict", err)
	}
}

func (repository *memoryRepository) GetSecretBearing(_ context.Context, ownerID, id string) (Alert, error) {
	if repository.alert.ID != id || repository.alert.OwnerID != ownerID {
		return Alert{}, ErrNotFound
	}
	return repository.alert, nil
}

func (repository *memoryRepository) GetSummary(_ context.Context, ownerID, id string) (AlertSummary, error) {
	if repository.alert.ID != id || repository.alert.OwnerID != ownerID {
		return AlertSummary{}, ErrNotFound
	}
	return AlertSummary{
		ID: id, OwnerID: ownerID, Version: repository.alert.Version, State: repository.alert.State,
		Definition: repository.alert.Definition,
	}, nil
}

func (repository *memoryRepository) List(_ context.Context, ownerID string, _ int) ([]AlertSummary, error) {
	if repository.alert.ID == "" || repository.alert.OwnerID != ownerID {
		return nil, nil
	}
	return []AlertSummary{{
		ID: repository.alert.ID, OwnerID: ownerID, Version: repository.alert.Version,
		State: repository.alert.State, Definition: repository.alert.Definition,
	}}, nil
}

func (repository *memoryRepository) Update(_ context.Context, record UpdateRecord) (Alert, error) {
	if repository.alert.Version != record.ExpectedVersion {
		return Alert{}, ErrVersionConflict
	}
	repository.alert.Version++
	repository.alert.Definition = record.Definition
	repository.alert.Endpoint = record.Endpoint
	repository.alert.EndpointGeneration = record.EndpointGeneration
	repository.alert.WebhookHostname = record.WebhookHostname
	repository.alert.UpdatedAt = record.UpdatedAt
	return repository.alert, nil
}

func (repository *memoryRepository) SetState(_ context.Context, record SetStateRecord) (Alert, error) {
	repository.setStateCalls++
	if repository.alert.Version != record.ExpectedVersion {
		return Alert{}, ErrVersionConflict
	}
	repository.alert.Version++
	repository.alert.State = record.State
	repository.alert.UpdatedAt = record.UpdatedAt
	return repository.alert, nil
}

func (repository *memoryRepository) RotateSecret(_ context.Context, record RotateSecretRecord) (Alert, error) {
	if repository.alert.Version != record.ExpectedVersion || repository.alert.SecretGeneration.Generation != record.ExpectedGeneration {
		return Alert{}, ErrVersionConflict
	}
	repository.alert.Version++
	repository.alert.SecretGeneration = record.SecretGeneration
	repository.alert.UpdatedAt = record.UpdatedAt
	return repository.alert, nil
}

func (repository *memoryRepository) AuthorizeDelivery(_ context.Context, record AuthorizeDeliveryRecord) (DeliveryAuthorization, error) {
	if repository.alert.SecretGeneration.Generation != record.SecretGeneration {
		return DeliverySecretRotated, nil
	}
	if repository.deliveryID != "" {
		return DeliveryAlreadyAttempted, nil
	}
	repository.deliveryID = record.DeliveryID
	return DeliveryAuthorized, nil
}

func (repository *memoryRepository) DeleteIfIdle(context.Context, DeleteRecord) error {
	return repository.deleteErr
}

func (*memoryRepository) ListRuns(context.Context, string, string, int) ([]RunSummary, error) {
	return nil, nil
}

type fixedSecretGenerator struct {
	value []byte
}

func (generator fixedSecretGenerator) Generate() ([]byte, error) {
	return append([]byte(nil), generator.value...), nil
}

func TestServiceCreateEncryptsSecretsAndStartsDisabled(t *testing.T) {
	t.Parallel()
	repository := &memoryRepository{}
	service := newTestService(t, repository, "")
	issued, err := service.Create(context.Background(), CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if issued.Alert.State != AlertDisabled || repository.createRecord.State != AlertDisabled {
		t.Fatalf("created state = %q / %q", issued.Alert.State, repository.createRecord.State)
	}
	if issued.PlaintextSecret == "" {
		t.Fatal("Create() did not issue the one-time secret")
	}
	if bytes.Contains(repository.createRecord.Endpoint.Ciphertext, []byte("hooks.example.com")) {
		t.Fatal("repository received a plaintext endpoint")
	}
	if bytes.Contains(repository.createRecord.SecretGeneration.Encrypted.Ciphertext, bytes.Repeat([]byte{9}, SecretBytes)) {
		t.Fatal("repository received a plaintext HMAC secret")
	}
	if repository.createRecord.EndpointGeneration != 1 || repository.createRecord.SecretGeneration.Generation != 1 {
		t.Fatalf("initial generations = %d / %d", repository.createRecord.EndpointGeneration, repository.createRecord.SecretGeneration.Generation)
	}
}

func TestServiceCreateAndUpdateAcceptEmptyRetentionDefaults(t *testing.T) {
	t.Parallel()
	repository := &memoryRepository{}
	service := newTestService(t, repository, "")
	definition := validDefinition()
	definition.DispatchTTL = ""
	definition.WebhookTTL = ""
	created, err := service.Create(context.Background(), CreateInput{
		OwnerID: "owner-1", Definition: definition, WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create(empty TTL defaults) error = %v", err)
	}
	if created.Alert.Definition.DispatchTTL != "" || created.Alert.Definition.WebhookTTL != "" {
		t.Fatalf("persisted default intent = dispatch %q webhook %q", created.Alert.Definition.DispatchTTL, created.Alert.Definition.WebhookTTL)
	}
	definition.Description = "updated with defaults"
	updated, err := service.Update(context.Background(), UpdateInput{
		ID: created.Alert.ID, OwnerID: "owner-1", ExpectedVersion: created.Alert.Version,
		Definition: definition,
	})
	if err != nil {
		t.Fatalf("Update(empty TTL defaults) error = %v", err)
	}
	if updated.Definition.DispatchTTL != "" || updated.Definition.WebhookTTL != "" {
		t.Fatalf("updated default intent = dispatch %q webhook %q", updated.Definition.DispatchTTL, updated.Definition.WebhookTTL)
	}
}

func TestServiceRequiresPublicBaseURLBeforeEnabling(t *testing.T) {
	t.Parallel()
	repository := &memoryRepository{}
	service := newTestService(t, repository, "")
	created, err := service.Create(context.Background(), CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.SetEnabled(context.Background(), "owner-1", created.Alert.ID, created.Alert.Version, true); err == nil {
		t.Fatal("SetEnabled() accepted a missing public base URL")
	}
	if repository.setStateCalls != 0 {
		t.Fatalf("SetEnabled() reached repository %d times", repository.setStateCalls)
	}
	service.publicBaseURL = "https://splunk.example.com/base"
	enabled, err := service.SetEnabled(context.Background(), "owner-1", created.Alert.ID, created.Alert.Version, true)
	if err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}
	if enabled.State != AlertEnabled {
		t.Fatalf("enabled state = %q", enabled.State)
	}
}

func TestServiceRejectsPersistedEnabledAlertsWithoutPublicBaseURL(t *testing.T) {
	t.Parallel()
	repository := &memoryRepository{alert: Alert{
		ID: "alert-enabled", OwnerID: "owner-1", State: AlertEnabled,
		Definition: validDefinition(),
	}}
	service := newTestService(t, repository, "")
	if err := service.ValidateEnabledRuntime(context.Background(), "owner-1"); err == nil {
		t.Fatal("ValidateEnabledRuntime() accepted an enabled alert without a public base URL")
	}
	service.publicBaseURL = "https://splunk.example.com"
	if err := service.ValidateEnabledRuntime(context.Background(), "owner-1"); err != nil {
		t.Fatalf("ValidateEnabledRuntime() with public base error = %v", err)
	}
	repository.alert.State = AlertDisabled
	service.publicBaseURL = ""
	if err := service.ValidateEnabledRuntime(context.Background(), "owner-1"); err != nil {
		t.Fatalf("ValidateEnabledRuntime() rejected disabled alerts: %v", err)
	}
}

func TestServiceSecretRotationSkipsOldRunDelivery(t *testing.T) {
	t.Parallel()
	repository := &memoryRepository{}
	service := newTestService(t, repository, "https://splunk.example.com")
	created, err := service.Create(context.Background(), CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	oldSnapshot := RunSnapshot{
		AlertID: created.Alert.ID, AlertRunID: "run-old", OwnerID: created.Alert.OwnerID,
		Endpoint: created.Alert.Endpoint, EndpointGeneration: created.Alert.EndpointGeneration,
		SecretGeneration: created.Alert.SecretGeneration,
	}
	rotated, err := service.RotateSecret(context.Background(), "owner-1", created.Alert.ID, created.Alert.Version)
	if err != nil {
		t.Fatalf("RotateSecret() error = %v", err)
	}
	if rotated.Alert.SecretGeneration.Generation != 2 || rotated.PlaintextSecret == "" {
		t.Fatalf("rotated alert = %#v", rotated)
	}
	oldDeliveryIDGenerated := false
	if _, err := service.AuthorizeAndOpenDelivery(context.Background(), oldSnapshot, func() (string, error) {
		oldDeliveryIDGenerated = true
		return "delivery-old", nil
	}); !errors.Is(err, ErrSecretRotated) {
		t.Fatalf("AuthorizeAndOpenDelivery(old) error = %v", err)
	}
	if oldDeliveryIDGenerated {
		t.Fatal("rotated snapshot generated a delivery ID")
	}
	currentSnapshot := oldSnapshot
	currentSnapshot.AlertRunID = "run-current"
	currentSnapshot.SecretGeneration = rotated.Alert.SecretGeneration
	opened, err := service.AuthorizeAndOpenDelivery(context.Background(), currentSnapshot, func() (string, error) { return "delivery-current", nil })
	if err != nil {
		t.Fatalf("AuthorizeAndOpenDelivery(current) error = %v", err)
	}
	if opened.DeliveryID != "delivery-current" || opened.Endpoint != "https://hooks.example.com/notify" || len(opened.Secret) != SecretBytes {
		t.Fatalf("opened secrets = endpoint %q, secret length %d", opened.Endpoint, len(opened.Secret))
	}
}

func TestServiceAllowsOnlyOneDeliveryAttempt(t *testing.T) {
	t.Parallel()
	repository := &memoryRepository{}
	service := newTestService(t, repository, "https://splunk.example.com")
	created, err := service.Create(context.Background(), CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	snapshot := RunSnapshot{
		AlertID: created.Alert.ID, AlertRunID: "run-1", OwnerID: created.Alert.OwnerID,
		Endpoint: created.Alert.Endpoint, EndpointGeneration: created.Alert.EndpointGeneration,
		SecretGeneration: created.Alert.SecretGeneration,
	}
	if _, err := service.AuthorizeAndOpenDelivery(context.Background(), snapshot, func() (string, error) { return "delivery-1", nil }); err != nil {
		t.Fatalf("first AuthorizeAndOpenDelivery() error = %v", err)
	}
	if _, err := service.AuthorizeAndOpenDelivery(context.Background(), snapshot, func() (string, error) { return "delivery-2", nil }); !errors.Is(err, ErrDeliveryAttempted) {
		t.Fatalf("second AuthorizeAndOpenDelivery() error = %v", err)
	}
}

func TestServiceUpdateSnapshotsANewEndpointGeneration(t *testing.T) {
	t.Parallel()
	repository := &memoryRepository{}
	service := newTestService(t, repository, "")
	created, err := service.Create(context.Background(), CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/one",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	updated, err := service.Update(context.Background(), UpdateInput{
		ID: created.Alert.ID, OwnerID: "owner-1", ExpectedVersion: created.Alert.Version,
		Definition: validDefinition(), WebhookURL: "https://hooks.example.com/two",
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.EndpointGeneration != 2 || updated.Version != 2 {
		t.Fatalf("updated generations/version = %d / %d", updated.EndpointGeneration, updated.Version)
	}
}

func TestServiceUpdatePreservesRedactedEndpointWhenURLIsOmitted(t *testing.T) {
	t.Parallel()
	repository := &memoryRepository{}
	service := newTestService(t, repository, "")
	created, err := service.Create(context.Background(), CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/one",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	original := append([]byte(nil), created.Alert.Endpoint.Ciphertext...)
	definition := validDefinition()
	definition.Description = "updated without revealing the endpoint"
	updated, err := service.Update(context.Background(), UpdateInput{
		ID: created.Alert.ID, OwnerID: "owner-1", ExpectedVersion: created.Alert.Version,
		Definition: definition,
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.EndpointGeneration != created.Alert.EndpointGeneration || updated.WebhookHostname != "hooks.example.com" || !bytes.Equal(updated.Endpoint.Ciphertext, original) {
		t.Fatalf("preserved endpoint = generation %d hostname %q ciphertext match %t", updated.EndpointGeneration, updated.WebhookHostname, bytes.Equal(updated.Endpoint.Ciphertext, original))
	}
}

func TestServiceOpensCurrentSecretsForTestWebhook(t *testing.T) {
	t.Parallel()
	repository := &memoryRepository{}
	service := newTestService(t, repository, "https://splunk.example.com")
	created, err := service.Create(context.Background(), CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/test",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	opened, err := service.OpenTestDeliverySecrets(context.Background(), "owner-1", created.Alert.ID)
	if err != nil {
		t.Fatalf("OpenTestDeliverySecrets() error = %v", err)
	}
	if opened.Endpoint != "https://hooks.example.com/test" || len(opened.Secret) != SecretBytes {
		t.Fatalf("opened test secrets = %q / %d", opened.Endpoint, len(opened.Secret))
	}
}

func newTestService(t *testing.T, repository Repository, publicBaseURL string) *Service {
	t.Helper()
	random := make([]byte, 12*8)
	for index := range random {
		random[index] = byte(index + 1)
	}
	cipher, err := NewAESGCMCipher(bytes.Repeat([]byte{4}, 32), bytes.NewReader(random))
	if err != nil {
		t.Fatalf("NewAESGCMCipher() error = %v", err)
	}
	service, err := NewService(repository, cipher, ServiceOptions{
		Clock:           func() time.Time { return time.Date(2026, time.August, 29, 1, 2, 3, 0, time.UTC) },
		IDGenerator:     func() (string, error) { return "alert-1", nil },
		SecretGenerator: fixedSecretGenerator{value: bytes.Repeat([]byte{9}, SecretBytes)},
		PublicBaseURL:   publicBaseURL,
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service
}

func validDefinition() Definition {
	return Definition{
		Name: "errors", Application: "search", SPL: "search index=main error", IndexScope: []string{"main"},
		Earliest: "-5m", Latest: "now", Cron: "*/5 * * * *", Timezone: "UTC",
		Condition:  Condition{Operator: ConditionGreaterThan, Threshold: 0},
		SampleRows: 5, DispatchTTL: "2p", WebhookTTL: "10p",
	}
}

func TestAlertServiceUsesClaimedPeriodScheduleValidation(t *testing.T) {
	t.Parallel()
	service := newTestService(t, &memoryRepository{}, "https://open-splunk.example.test")
	definition := validDefinition()
	definition.Cron = "0 0 * * 7"
	if _, err := service.Create(context.Background(), CreateInput{
		OwnerID: "owner-1", Definition: definition, WebhookURL: "https://alerts.example.test/hook",
	}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("weekday 7 error = %v, want ErrInvalidArgument", err)
	}
	definition = validDefinition()
	definition.WebhookTTL = "315360001"
	if _, err := service.Create(context.Background(), CreateInput{
		OwnerID: "owner-1", Definition: definition, WebhookURL: "https://alerts.example.test/hook",
	}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("over-limit TTL error = %v, want ErrInvalidArgument", err)
	}
}
