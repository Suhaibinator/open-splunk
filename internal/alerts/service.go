package alerts

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/schedulevalidation"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
	"github.com/google/uuid"
)

const (
	maximumNameBytes          = 128
	maximumDescriptionBytes   = 2048
	maximumApplicationBytes   = 128
	maximumSPLBytes           = 64 * 1024
	maximumTimeRangeBytes     = 256
	maximumCronBytes          = 256
	maximumTimezoneBytes      = 128
	maximumTTLBytes           = searchretention.MaximumTTLExpressionBytes
	maximumSelectedFields     = 256
	maximumFieldNameBytes     = 256
	maximumIndexScope         = 256
	maximumIndexNameBytes     = 255
	maximumVisualizationBytes = 64 * 1024
	maximumAlertIDBytes       = 128
	maximumOwnerIDBytes       = 255
	maximumWebhookURLBytes    = 16*1024 - 16
)

type ServiceOptions struct {
	Clock           func() time.Time
	IDGenerator     func() (string, error)
	SecretGenerator SecretGenerator
	PublicBaseURL   string
}

type Service struct {
	repository      Repository
	cipher          SensitiveCipher
	clock           func() time.Time
	idGenerator     func() (string, error)
	secretGenerator SecretGenerator
	publicBaseURL   string
}

type CreateInput struct {
	OwnerID         string
	ClientRequestID string
	Definition      Definition
	WebhookURL      string
}

type UpdateInput struct {
	ID              string
	OwnerID         string
	ExpectedVersion uint64
	Definition      Definition
	WebhookURL      string
}

type IssuedAlert struct {
	Alert           Alert
	PlaintextSecret string
	Replayed        bool
}

type OpenedDeliverySecrets struct {
	DeliveryID string
	Endpoint   string
	Secret     []byte
}

func NewService(repository Repository, cipher SensitiveCipher, options ServiceOptions) (*Service, error) {
	if repository == nil || cipher == nil {
		return nil, fmt.Errorf("%w: repository and cipher are required", ErrInvalidArgument)
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	idGenerator := options.IDGenerator
	if idGenerator == nil {
		idGenerator = func() (string, error) { return uuid.NewString(), nil }
	}
	secretGenerator := options.SecretGenerator
	if secretGenerator == nil {
		secretGenerator = RandomSecretGenerator{}
	}
	return &Service{
		repository: repository, cipher: cipher, clock: clock,
		idGenerator: idGenerator, secretGenerator: secretGenerator,
		publicBaseURL: options.PublicBaseURL,
	}, nil
}

func (service *Service) Create(ctx context.Context, input CreateInput) (IssuedAlert, error) {
	if err := validateOwner(input.OwnerID); err != nil {
		return IssuedAlert{}, err
	}
	if err := service.validateDefinition(input.Definition); err != nil {
		return IssuedAlert{}, err
	}
	if len(input.WebhookURL) > maximumWebhookURLBytes {
		return IssuedAlert{}, fmt.Errorf("%w: webhook URL is too long", ErrInvalidArgument)
	}
	var err error
	var fingerprint [32]byte
	if input.ClientRequestID != "" {
		if err := validateClientRequestID(input.ClientRequestID); err != nil {
			return IssuedAlert{}, err
		}
		fingerprint, err = createRequestFingerprint(input.Definition, input.WebhookURL)
		if err != nil {
			return IssuedAlert{}, err
		}
		replayed, found, findErr := service.repository.FindCreateReplay(
			ctx,
			input.OwnerID,
			input.ClientRequestID,
			fingerprint,
		)
		if findErr != nil {
			return IssuedAlert{}, findErr
		}
		if found {
			return IssuedAlert{Alert: replayed, Replayed: true}, nil
		}
	}
	destination, err := ParseDestination(input.WebhookURL)
	if err != nil {
		return IssuedAlert{}, err
	}
	id, err := service.idGenerator()
	if err != nil || strings.TrimSpace(id) == "" || len(id) > maximumAlertIDBytes {
		return IssuedAlert{}, errors.New("alerts: generate alert ID")
	}
	const generation = uint64(1)
	endpoint, err := encryptString(service.cipher, SecretContext{AlertID: id, Purpose: PurposeEndpoint, Generation: generation}, input.WebhookURL)
	if err != nil {
		return IssuedAlert{}, err
	}
	secret, err := service.secretGenerator.Generate()
	if err != nil {
		return IssuedAlert{}, err
	}
	defer clear(secret)
	if len(secret) != SecretBytes {
		return IssuedAlert{}, errors.New("alerts: secret generator returned an invalid value")
	}
	encryptedSecret, err := service.cipher.Encrypt(SecretContext{AlertID: id, Purpose: PurposeHMACSecret, Generation: generation}, secret)
	if err != nil {
		return IssuedAlert{}, err
	}
	now := service.clock().UTC()
	created, err := service.repository.Create(ctx, CreateRecord{
		ID: id, OwnerID: input.OwnerID, ClientRequestID: input.ClientRequestID, RequestFingerprint: fingerprint,
		State: AlertDisabled, Definition: cloneDefinition(input.Definition),
		Endpoint: endpoint, EndpointGeneration: generation,
		WebhookHostname:  destination.Hostname,
		SecretGeneration: SecretGeneration{Generation: generation, Encrypted: encryptedSecret, CreatedAt: now},
		CreatedAt:        now,
	})
	if err != nil {
		return IssuedAlert{}, err
	}
	if created.Disposition == CreateReplayed {
		return IssuedAlert{Alert: created.Alert, Replayed: true}, nil
	}
	if created.Disposition != CreateCommitted {
		return IssuedAlert{}, errors.New("alerts: repository returned an invalid create disposition")
	}
	encoded, err := EncodeIssuedSecret(secret)
	if err != nil {
		return IssuedAlert{}, err
	}
	return IssuedAlert{Alert: created.Alert, PlaintextSecret: encoded}, nil
}

func (service *Service) Update(ctx context.Context, input UpdateInput) (Alert, error) {
	if err := validateIdentity(input.ID, input.OwnerID, input.ExpectedVersion); err != nil {
		return Alert{}, err
	}
	if err := service.validateDefinition(input.Definition); err != nil {
		return Alert{}, err
	}
	if len(input.WebhookURL) > maximumWebhookURLBytes {
		return Alert{}, fmt.Errorf("%w: webhook URL is too long", ErrInvalidArgument)
	}
	current, err := service.repository.GetSecretBearing(ctx, input.OwnerID, input.ID)
	if err != nil {
		return Alert{}, err
	}
	if current.Version != input.ExpectedVersion {
		return Alert{}, ErrVersionConflict
	}
	endpoint := EncryptedValue{Nonce: append([]byte(nil), current.Endpoint.Nonce...), Ciphertext: append([]byte(nil), current.Endpoint.Ciphertext...)}
	endpointGeneration := current.EndpointGeneration
	webhookHostname := current.WebhookHostname
	if strings.TrimSpace(input.WebhookURL) != "" {
		destination, parseErr := ParseDestination(input.WebhookURL)
		if parseErr != nil {
			return Alert{}, parseErr
		}
		if current.EndpointGeneration >= math.MaxInt64 {
			return Alert{}, errors.New("alerts: endpoint generation exhausted")
		}
		endpointGeneration++
		endpoint, err = encryptString(service.cipher, SecretContext{AlertID: input.ID, Purpose: PurposeEndpoint, Generation: endpointGeneration}, input.WebhookURL)
		if err != nil {
			return Alert{}, err
		}
		webhookHostname = destination.Hostname
	}
	return service.repository.Update(ctx, UpdateRecord{
		ID: input.ID, OwnerID: input.OwnerID, ExpectedVersion: input.ExpectedVersion,
		Definition: cloneDefinition(input.Definition), Endpoint: endpoint,
		EndpointGeneration: endpointGeneration, WebhookHostname: webhookHostname,
		UpdatedAt: service.clock().UTC(),
	})
}

func (service *Service) SetEnabled(ctx context.Context, ownerID, id string, expectedVersion uint64, enabled bool) (Alert, error) {
	if err := validateIdentity(id, ownerID, expectedVersion); err != nil {
		return Alert{}, err
	}
	state := AlertDisabled
	if enabled {
		if err := ValidatePublicBaseURL(service.publicBaseURL); err != nil {
			return Alert{}, err
		}
		state = AlertEnabled
	}
	return service.repository.SetState(ctx, SetStateRecord{
		ID: id, OwnerID: ownerID, ExpectedVersion: expectedVersion,
		State: state, UpdatedAt: service.clock().UTC(),
	})
}

func (service *Service) RotateSecret(ctx context.Context, ownerID, id string, expectedVersion uint64) (IssuedAlert, error) {
	if err := validateIdentity(id, ownerID, expectedVersion); err != nil {
		return IssuedAlert{}, err
	}
	current, err := service.repository.GetSecretBearing(ctx, ownerID, id)
	if err != nil {
		return IssuedAlert{}, err
	}
	if current.Version != expectedVersion {
		return IssuedAlert{}, ErrVersionConflict
	}
	if current.SecretGeneration.Generation >= math.MaxInt64 {
		return IssuedAlert{}, errors.New("alerts: secret generation exhausted")
	}
	generation := current.SecretGeneration.Generation + 1
	secret, err := service.secretGenerator.Generate()
	if err != nil {
		return IssuedAlert{}, err
	}
	defer clear(secret)
	if len(secret) != SecretBytes {
		return IssuedAlert{}, errors.New("alerts: secret generator returned an invalid value")
	}
	encrypted, err := service.cipher.Encrypt(SecretContext{AlertID: id, Purpose: PurposeHMACSecret, Generation: generation}, secret)
	if err != nil {
		return IssuedAlert{}, err
	}
	now := service.clock().UTC()
	rotated, err := service.repository.RotateSecret(ctx, RotateSecretRecord{
		ID: id, OwnerID: ownerID, ExpectedVersion: expectedVersion,
		ExpectedGeneration: current.SecretGeneration.Generation,
		SecretGeneration:   SecretGeneration{Generation: generation, Encrypted: encrypted, CreatedAt: now},
		UpdatedAt:          now,
	})
	if err != nil {
		return IssuedAlert{}, err
	}
	plaintext, err := EncodeIssuedSecret(secret)
	if err != nil {
		return IssuedAlert{}, err
	}
	return IssuedAlert{Alert: rotated, PlaintextSecret: plaintext}, nil
}

func (service *Service) Delete(ctx context.Context, ownerID, id string, expectedVersion uint64) error {
	if err := validateIdentity(id, ownerID, expectedVersion); err != nil {
		return err
	}
	return service.repository.DeleteIfIdle(ctx, DeleteRecord{ID: id, OwnerID: ownerID, ExpectedVersion: expectedVersion})
}

// AuthorizeAndOpenDelivery checks the current secret generation before invoking
// the delivery-ID generator, then atomically records the run's one permitted
// attempt before decrypting its immutable snapshot. A crash after authorization
// is intentionally an unknown best-effort outcome and is never retried.
func (service *Service) AuthorizeAndOpenDelivery(ctx context.Context, snapshot RunSnapshot, generateDeliveryID func() (string, error)) (OpenedDeliverySecrets, error) {
	if strings.TrimSpace(snapshot.AlertRunID) == "" || generateDeliveryID == nil {
		return OpenedDeliverySecrets{}, fmt.Errorf("%w: alert run and delivery ID generator are required", ErrInvalidArgument)
	}
	current, err := service.repository.GetSecretBearing(ctx, snapshot.OwnerID, snapshot.AlertID)
	if err != nil {
		return OpenedDeliverySecrets{}, err
	}
	if current.SecretGeneration.Generation != snapshot.SecretGeneration.Generation {
		return OpenedDeliverySecrets{}, ErrSecretRotated
	}
	deliveryID, err := generateDeliveryID()
	if err != nil || strings.TrimSpace(deliveryID) == "" || strings.TrimSpace(deliveryID) != deliveryID || len(deliveryID) > 128 {
		if err == nil {
			err = errors.New("alerts: delivery ID generator returned an invalid value")
		}
		return OpenedDeliverySecrets{}, errors.Join(ErrDeliveryIDGeneration, err)
	}
	authorization, err := service.repository.AuthorizeDelivery(ctx, AuthorizeDeliveryRecord{
		AlertID: snapshot.AlertID, AlertRunID: snapshot.AlertRunID, OwnerID: snapshot.OwnerID,
		DeliveryID: deliveryID, SecretGeneration: snapshot.SecretGeneration.Generation,
		AuthorizedAt: service.clock().UTC(),
	})
	if err != nil {
		return OpenedDeliverySecrets{}, err
	}
	switch authorization {
	case DeliverySecretRotated:
		return OpenedDeliverySecrets{}, ErrSecretRotated
	case DeliveryAlreadyAttempted:
		return OpenedDeliverySecrets{}, ErrDeliveryAttempted
	case DeliveryAuthorized:
	default:
		return OpenedDeliverySecrets{}, errors.New("alerts: repository returned an invalid delivery authorization")
	}
	endpoint, err := service.cipher.Decrypt(SecretContext{
		AlertID: snapshot.AlertID, Purpose: PurposeEndpoint, Generation: snapshot.EndpointGeneration,
	}, snapshot.Endpoint)
	if err != nil {
		return OpenedDeliverySecrets{}, err
	}
	defer clear(endpoint)
	secret, err := service.cipher.Decrypt(SecretContext{
		AlertID: snapshot.AlertID, Purpose: PurposeHMACSecret, Generation: snapshot.SecretGeneration.Generation,
	}, snapshot.SecretGeneration.Encrypted)
	if err != nil {
		return OpenedDeliverySecrets{}, err
	}
	if len(secret) != SecretBytes {
		clear(secret)
		return OpenedDeliverySecrets{}, errors.New("alerts: stored signing secret is invalid")
	}
	return OpenedDeliverySecrets{DeliveryID: deliveryID, Endpoint: string(endpoint), Secret: secret}, nil
}

// OpenTestDeliverySecrets opens the current generation for the explicit test
// webhook route. It does not consume an alert-run delivery attempt.
func (service *Service) OpenTestDeliverySecrets(ctx context.Context, ownerID, id string) (OpenedDeliverySecrets, error) {
	if err := validateOwner(ownerID); err != nil {
		return OpenedDeliverySecrets{}, err
	}
	if strings.TrimSpace(id) == "" || len(id) > maximumAlertIDBytes {
		return OpenedDeliverySecrets{}, fmt.Errorf("%w: alert ID is required", ErrInvalidArgument)
	}
	current, err := service.repository.GetSecretBearing(ctx, ownerID, id)
	if err != nil {
		return OpenedDeliverySecrets{}, err
	}
	endpoint, err := service.cipher.Decrypt(SecretContext{
		AlertID: id, Purpose: PurposeEndpoint, Generation: current.EndpointGeneration,
	}, current.Endpoint)
	if err != nil {
		return OpenedDeliverySecrets{}, err
	}
	defer clear(endpoint)
	secret, err := service.cipher.Decrypt(SecretContext{
		AlertID: id, Purpose: PurposeHMACSecret, Generation: current.SecretGeneration.Generation,
	}, current.SecretGeneration.Encrypted)
	if err != nil {
		return OpenedDeliverySecrets{}, err
	}
	if len(secret) != SecretBytes {
		clear(secret)
		return OpenedDeliverySecrets{}, errors.New("alerts: stored signing secret is invalid")
	}
	return OpenedDeliverySecrets{Endpoint: string(endpoint), Secret: secret}, nil
}

func (service *Service) Get(ctx context.Context, ownerID, id string) (AlertSummary, error) {
	if err := validateOwner(ownerID); err != nil {
		return AlertSummary{}, err
	}
	if strings.TrimSpace(id) == "" || len(id) > maximumAlertIDBytes {
		return AlertSummary{}, fmt.Errorf("%w: alert ID is required", ErrInvalidArgument)
	}
	return service.repository.GetSummary(ctx, ownerID, id)
}

func (service *Service) List(ctx context.Context, ownerID string, limit int) ([]AlertSummary, error) {
	if err := validateOwner(ownerID); err != nil {
		return nil, err
	}
	return service.repository.List(ctx, ownerID, limit)
}

// ValidateEnabledRuntime prevents persisted enabled alerts from running when
// the configured public result-link base was removed between restarts.
func (service *Service) ValidateEnabledRuntime(ctx context.Context, ownerID string) error {
	if ctx == nil {
		return ErrInvalidArgument
	}
	summaries, err := service.List(ctx, ownerID, MaximumAlertsPerOwner)
	if err != nil {
		return err
	}
	for _, summary := range summaries {
		if summary.State != AlertEnabled {
			continue
		}
		if err := ValidatePublicBaseURL(service.publicBaseURL); err != nil {
			return fmt.Errorf("%w: enabled alert %q requires a public base URL", err, summary.ID)
		}
	}
	return nil
}

func (service *Service) validateDefinition(definition Definition) error {
	checks := []struct {
		name     string
		value    string
		maximum  int
		required bool
	}{
		{"name", definition.Name, maximumNameBytes, true},
		{"description", definition.Description, maximumDescriptionBytes, false},
		{"application", definition.Application, maximumApplicationBytes, true},
		{"SPL", definition.SPL, maximumSPLBytes, true},
		{"earliest time", definition.Earliest, maximumTimeRangeBytes, true},
		{"latest time", definition.Latest, maximumTimeRangeBytes, true},
		{"search timezone", definition.SearchTimezone, maximumTimezoneBytes, false},
		{"cron schedule", definition.Cron, maximumCronBytes, true},
		{"timezone", definition.Timezone, maximumTimezoneBytes, true},
		{"dispatch TTL", definition.DispatchTTL, maximumTTLBytes, false},
		{"webhook TTL", definition.WebhookTTL, maximumTTLBytes, false},
	}
	for _, check := range checks {
		trimmed := strings.TrimSpace(check.value)
		if check.required && trimmed == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidArgument, check.name)
		}
		if len(check.value) > check.maximum {
			return fmt.Errorf("%w: %s is too long", ErrInvalidArgument, check.name)
		}
	}
	if definition.SampleRows < 0 || definition.SampleRows > MaximumSampleRows {
		return fmt.Errorf("%w: sample row count must be between 0 and %d", ErrInvalidArgument, MaximumSampleRows)
	}
	if len(definition.IndexScope) == 0 || len(definition.IndexScope) > maximumIndexScope {
		return fmt.Errorf("%w: index scope must contain between 1 and %d indexes", ErrInvalidArgument, maximumIndexScope)
	}
	for _, index := range definition.IndexScope {
		if strings.TrimSpace(index) == "" || len(index) > maximumIndexNameBytes {
			return fmt.Errorf("%w: index scope contains an invalid index", ErrInvalidArgument)
		}
	}
	if len(definition.SelectedFields) > maximumSelectedFields || len(definition.Visualization) > maximumVisualizationBytes {
		return fmt.Errorf("%w: selected fields or visualization metadata exceeds its limit", ErrInvalidArgument)
	}
	for _, field := range definition.SelectedFields {
		if strings.TrimSpace(field) == "" || len(field) > maximumFieldNameBytes {
			return fmt.Errorf("%w: selected field name is invalid", ErrInvalidArgument)
		}
	}
	switch definition.PreferredResultTab {
	case ResultTabUnspecified, ResultTabEvents, ResultTabStatistics, ResultTabVisualization:
	default:
		return fmt.Errorf("%w: preferred result tab is invalid", ErrInvalidArgument)
	}
	if err := ValidateCondition(definition.Condition); err != nil {
		return err
	}
	if definition.SearchTimezone != "" {
		if _, err := time.LoadLocation(definition.SearchTimezone); err != nil {
			return fmt.Errorf("%w: search timezone is invalid", ErrInvalidArgument)
		}
	}
	scheduleResult, err := schedulevalidation.ValidateAt(schedulevalidation.Input{
		Mode:        schedulevalidation.ModeWebhookAlert,
		Cron:        definition.Cron,
		Timezone:    definition.Timezone,
		DispatchTTL: definition.DispatchTTL,
		WebhookTTL:  definition.WebhookTTL,
	}, service.clock().UTC())
	if err != nil {
		return errors.Join(ErrInvalidArgument, err)
	}
	if !scheduleResult.Valid() {
		return fmt.Errorf("%w: alert schedule or retention is invalid", ErrInvalidArgument)
	}
	return nil
}

func validateOwner(ownerID string) error {
	if strings.TrimSpace(ownerID) == "" || len(ownerID) > maximumOwnerIDBytes {
		return fmt.Errorf("%w: owner ID is required", ErrInvalidArgument)
	}
	return nil
}

func validateIdentity(id, ownerID string, version uint64) error {
	if err := validateOwner(ownerID); err != nil {
		return err
	}
	if strings.TrimSpace(id) == "" || len(id) > maximumAlertIDBytes || version == 0 {
		return fmt.Errorf("%w: alert ID and positive version are required", ErrInvalidArgument)
	}
	return nil
}

func cloneDefinition(definition Definition) Definition {
	definition.IndexScope = append([]string(nil), definition.IndexScope...)
	definition.SelectedFields = append([]string(nil), definition.SelectedFields...)
	definition.Visualization = append([]byte(nil), definition.Visualization...)
	return definition
}

func encryptString(cipher SensitiveCipher, secretContext SecretContext, value string) (EncryptedValue, error) {
	plaintext := []byte(value)
	defer clear(plaintext)
	return cipher.Encrypt(secretContext, plaintext)
}
