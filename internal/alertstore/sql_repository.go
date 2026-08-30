package alertstore

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"fortio.org/safecast"
	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/scheduler"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type SQLRepositoryOptions struct {
	Clock       func() time.Time
	IDGenerator func() (string, error)
	TenantID    string
}

type SQLRepository struct {
	db          *gorm.DB
	clock       func() time.Time
	idGenerator func() (string, error)
	tenantID    string
}

var (
	_ alerts.Repository    = (*SQLRepository)(nil)
	_ alerts.RunRepository = (*SQLRepository)(nil)
)

const (
	maximumClientRequestIDBytes = 128
	maximumOwnerIDBytes         = 255
	minimumClientRequestIDBytes = 16
)

type persistedRunOutcome int64

const (
	persistedRunOutcomeActive persistedRunOutcome = iota + 1
	persistedRunOutcomeSearchFailed
	persistedRunOutcomeSearchCanceled
	persistedRunOutcomeSearchExpired
	persistedRunOutcomeNotTriggered
	persistedRunOutcomeIndeterminate
	persistedRunOutcomeDelivered
	persistedRunOutcomeDeliveryFailed
	persistedRunOutcomeDeliveryUnknown
	persistedRunOutcomeOverlapSkipped
	persistedRunOutcomeInterrupted
	persistedRunOutcomeDeliverySkipped
)

type alertSQLRecord struct {
	AlertID             string               `gorm:"column:alert_id;primaryKey"`
	Version             int64                `gorm:"column:version"`
	TenantID            string               `gorm:"column:tenant_id"`
	OwnerID             string               `gorm:"column:owner_id"`
	ClientRequestID     *string              `gorm:"column:client_request_id"`
	CreateRequestSHA256 []byte               `gorm:"column:create_request_sha256"`
	AppID               string               `gorm:"column:app_id"`
	Name                string               `gorm:"column:name"`
	Enabled             int64                `gorm:"column:enabled"`
	Definition          []byte               `gorm:"column:definition_proto"`
	EndpointCiphertext  []byte               `gorm:"column:endpoint_ciphertext"`
	EndpointNonce       []byte               `gorm:"column:endpoint_nonce"`
	EndpointGeneration  int64                `gorm:"column:endpoint_generation"`
	WebhookHostname     string               `gorm:"column:webhook_hostname"`
	SecretGeneration    int64                `gorm:"column:secret_generation"`
	SecretCiphertext    []byte               `gorm:"column:secret_ciphertext"`
	SecretNonce         []byte               `gorm:"column:secret_nonce"`
	SecretRotatedAt     int64                `gorm:"column:secret_rotated_at_unix_micro"`
	NextRunAt           *int64               `gorm:"column:next_run_at_unix_micro"`
	LastClaimedAt       *int64               `gorm:"column:last_claimed_at_unix_micro"`
	LastOutcome         *persistedRunOutcome `gorm:"column:last_outcome"`
	LastOutcomeAt       *int64               `gorm:"column:last_outcome_scheduled_at_unix_micro"`
	LastEvaluatedAt     *int64               `gorm:"column:last_evaluated_at_unix_micro"`
	LastDeliveredAt     *int64               `gorm:"column:last_delivered_at_unix_micro"`
	CreatedAt           int64                `gorm:"column:created_at_unix_micro;autoCreateTime:false"`
	UpdatedAt           int64                `gorm:"column:updated_at_unix_micro;autoUpdateTime:false"`
}

func (alertSQLRecord) TableName() string { return "alerts" }

type alertRunSQLRecord struct {
	AlertRunID            string              `gorm:"column:alert_run_id;primaryKey"`
	AlertID               string              `gorm:"column:alert_id"`
	AlertVersion          int64               `gorm:"column:alert_version"`
	SecretGeneration      int64               `gorm:"column:secret_generation"`
	ScheduledAt           int64               `gorm:"column:scheduled_at_unix_micro"`
	StartedAt             *int64              `gorm:"column:started_at_unix_micro"`
	FinishedAt            *int64              `gorm:"column:finished_at_unix_micro"`
	Outcome               persistedRunOutcome `gorm:"column:outcome"`
	MissedOccurrenceCount int64               `gorm:"column:missed_occurrence_count"`
	SearchJobID           *string             `gorm:"column:search_job_id"`
	SearchJobExpiresAt    *int64              `gorm:"column:search_job_expires_at_unix_micro"`
	DeliveryID            *string             `gorm:"column:delivery_id"`
	DeliveryAuthorizedAt  *int64              `gorm:"column:delivery_authorized_at_unix_micro"`
	DeliveryAttemptedAt   *int64              `gorm:"column:delivery_attempted_at_unix_micro"`
	DeliveryStatusCode    *int64              `gorm:"column:delivery_status_code"`
	Evaluation            int64               `gorm:"column:evaluation"`
	ResultCount           *int64              `gorm:"column:result_count"`
	ResultCountExact      *int64              `gorm:"column:result_count_exact"`
	FailureCategory       *string             `gorm:"column:failure_category"`
	Snapshot              []byte              `gorm:"column:snapshot_proto"`
}

func (alertRunSQLRecord) TableName() string { return "alert_runs" }

func NewSQLRepository(database *control.DB, options SQLRepositoryOptions) (*SQLRepository, error) {
	if database == nil || database.GORMDB() == nil {
		return nil, fmt.Errorf("%w: control database is required", alerts.ErrInvalidArgument)
	}
	if strings.TrimSpace(options.TenantID) == "" || len(options.TenantID) > 1024 {
		return nil, fmt.Errorf("%w: tenant ID is invalid", alerts.ErrInvalidArgument)
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	idGenerator := options.IDGenerator
	if idGenerator == nil {
		idGenerator = func() (string, error) { return uuid.NewString(), nil }
	}
	return &SQLRepository{db: database.GORMDB(), clock: clock, idGenerator: idGenerator, tenantID: options.TenantID}, nil
}

func (repository *SQLRepository) FindCreateReplay(ctx context.Context, ownerID, clientRequestID string, fingerprint [32]byte) (alerts.Alert, bool, error) {
	if err := validateOwner(ownerID); err != nil {
		return alerts.Alert{}, false, err
	}
	if err := validateClientRequestID(clientRequestID); err != nil {
		return alerts.Alert{}, false, err
	}
	replayed, found, err := repository.findCreateReplay(repository.db.WithContext(ctx), ownerID, clientRequestID, fingerprint)
	if err != nil {
		return alerts.Alert{}, false, mapSQLError(ctx, "find alert create replay", err)
	}
	return replayed, found, nil
}

func (repository *SQLRepository) Create(ctx context.Context, input alerts.CreateRecord) (alerts.CreateResult, error) {
	if input.State != alerts.AlertDisabled {
		return alerts.CreateResult{}, fmt.Errorf("%w: new alerts must be disabled", alerts.ErrInvalidArgument)
	}
	if input.ClientRequestID != "" {
		if err := validateClientRequestID(input.ClientRequestID); err != nil {
			return alerts.CreateResult{}, err
		}
	}
	definition, err := encodeDefinition(input.Definition)
	if err != nil {
		return alerts.CreateResult{}, err
	}
	endpointGeneration, err := safecast.Conv[int64](input.EndpointGeneration)
	if err != nil {
		return alerts.CreateResult{}, fmt.Errorf("%w: endpoint generation exceeds storage bounds", alerts.ErrInvalidArgument)
	}
	secretGeneration, err := safecast.Conv[int64](input.SecretGeneration.Generation)
	if err != nil {
		return alerts.CreateResult{}, fmt.Errorf("%w: secret generation exceeds storage bounds", alerts.ErrInvalidArgument)
	}
	var clientRequestID *string
	var createRequestSHA256 []byte
	if input.ClientRequestID != "" {
		clientRequestID = new(input.ClientRequestID)
		createRequestSHA256 = append([]byte(nil), input.RequestFingerprint[:]...)
	}
	record := alertSQLRecord{
		AlertID: input.ID, Version: 1, TenantID: repository.tenantID, OwnerID: input.OwnerID,
		ClientRequestID: clientRequestID, CreateRequestSHA256: createRequestSHA256,
		AppID: input.Definition.Application, Name: input.Definition.Name, Enabled: 0, Definition: definition,
		EndpointCiphertext: append([]byte(nil), input.Endpoint.Ciphertext...), EndpointNonce: append([]byte(nil), input.Endpoint.Nonce...),
		EndpointGeneration: endpointGeneration, WebhookHostname: input.WebhookHostname,
		SecretGeneration: secretGeneration, SecretCiphertext: append([]byte(nil), input.SecretGeneration.Encrypted.Ciphertext...),
		SecretNonce: append([]byte(nil), input.SecretGeneration.Encrypted.Nonce...), SecretRotatedAt: input.SecretGeneration.CreatedAt.UTC().UnixMicro(),
		CreatedAt: input.CreatedAt.UTC().UnixMicro(), UpdatedAt: input.CreatedAt.UTC().UnixMicro(),
	}
	var created alerts.CreateResult
	err = repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if input.ClientRequestID != "" {
			replayed, found, replayErr := repository.findCreateReplay(tx, input.OwnerID, input.ClientRequestID, input.RequestFingerprint)
			if replayErr != nil {
				return replayErr
			}
			if found {
				created = alerts.CreateResult{Alert: replayed, Disposition: alerts.CreateReplayed}
				return nil
			}
		}
		var count int64
		if err := tx.Model(&alertSQLRecord{}).Where("tenant_id = ? AND owner_id = ?", repository.tenantID, input.OwnerID).Count(&count).Error; err != nil {
			return err
		}
		if count >= alerts.MaximumAlertsPerOwner {
			return alerts.ErrCapacity
		}
		if err := tx.Create(&record).Error; err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				if input.ClientRequestID != "" {
					replayed, found, replayErr := repository.findCreateReplay(tx, input.OwnerID, input.ClientRequestID, input.RequestFingerprint)
					if replayErr != nil {
						return replayErr
					}
					if found {
						created = alerts.CreateResult{Alert: replayed, Disposition: alerts.CreateReplayed}
						return nil
					}
				}
				return alerts.ErrAlreadyExists
			}
			return err
		}
		var conversionErr error
		alert, conversionErr := alertFromSQL(record)
		created = alerts.CreateResult{Alert: alert, Disposition: alerts.CreateCommitted}
		return conversionErr
	})
	if err != nil {
		return alerts.CreateResult{}, mapSQLError(ctx, "create alert", err)
	}
	return created, nil
}

func (repository *SQLRepository) findCreateReplay(tx *gorm.DB, ownerID, clientRequestID string, fingerprint [32]byte) (alerts.Alert, bool, error) {
	var record alertSQLRecord
	err := tx.Where(
		"tenant_id = ? AND owner_id = ? AND client_request_id = ?",
		repository.tenantID,
		ownerID,
		clientRequestID,
	).Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return alerts.Alert{}, false, nil
	}
	if err != nil {
		return alerts.Alert{}, false, err
	}
	if len(record.CreateRequestSHA256) != len(fingerprint) || subtle.ConstantTimeCompare(record.CreateRequestSHA256, fingerprint[:]) != 1 {
		return alerts.Alert{}, false, alerts.ErrIdempotencyConflict
	}
	replayed, err := alertFromSQL(record)
	if err != nil {
		return alerts.Alert{}, false, err
	}
	return replayed, true, nil
}

func (repository *SQLRepository) GetSecretBearing(ctx context.Context, ownerID, id string) (alerts.Alert, error) {
	var record alertSQLRecord
	err := repository.db.WithContext(ctx).Where("tenant_id = ? AND owner_id = ? AND alert_id = ?", repository.tenantID, ownerID, id).Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return alerts.Alert{}, alerts.ErrNotFound
	}
	if err != nil {
		return alerts.Alert{}, mapSQLError(ctx, "get alert", err)
	}
	return alertFromSQL(record)
}

func (repository *SQLRepository) GetSummary(ctx context.Context, ownerID, id string) (alerts.AlertSummary, error) {
	var record alertSQLRecord
	err := repository.db.WithContext(ctx).
		Select("alert_id", "version", "tenant_id", "owner_id", "app_id", "name", "enabled", "definition_proto", "webhook_hostname", "secret_generation", "secret_rotated_at_unix_micro", "next_run_at_unix_micro", "last_outcome", "last_outcome_scheduled_at_unix_micro", "last_evaluated_at_unix_micro", "last_delivered_at_unix_micro", "created_at_unix_micro", "updated_at_unix_micro").
		Where("tenant_id = ? AND owner_id = ? AND alert_id = ?", repository.tenantID, ownerID, id).Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return alerts.AlertSummary{}, alerts.ErrNotFound
	}
	if err != nil {
		return alerts.AlertSummary{}, mapSQLError(ctx, "get alert summary", err)
	}
	definition, err := decodeDefinition(record.Definition)
	if err != nil {
		return alerts.AlertSummary{}, err
	}
	return alertSummaryFromSQL(record, definition), nil
}

func (repository *SQLRepository) List(ctx context.Context, ownerID string, limit int) ([]alerts.AlertSummary, error) {
	if limit <= 0 || limit > alerts.MaximumAlertsPerOwner {
		limit = alerts.MaximumAlertsPerOwner
	}
	var records []alertSQLRecord
	err := repository.db.WithContext(ctx).
		Select("alert_id", "version", "tenant_id", "owner_id", "app_id", "name", "enabled", "definition_proto", "webhook_hostname", "secret_generation", "secret_rotated_at_unix_micro", "next_run_at_unix_micro", "last_outcome", "last_outcome_scheduled_at_unix_micro", "last_evaluated_at_unix_micro", "last_delivered_at_unix_micro", "created_at_unix_micro", "updated_at_unix_micro").
		Where("tenant_id = ? AND owner_id = ?", repository.tenantID, ownerID).
		Order("updated_at_unix_micro DESC, alert_id DESC").Limit(limit).Find(&records).Error
	if err != nil {
		return nil, mapSQLError(ctx, "list alerts", err)
	}
	result := make([]alerts.AlertSummary, len(records))
	for index, record := range records {
		definition, decodeErr := decodeDefinition(record.Definition)
		if decodeErr != nil {
			return nil, decodeErr
		}
		result[index] = alertSummaryFromSQL(record, definition)
	}
	return result, nil
}

func (repository *SQLRepository) Update(ctx context.Context, input alerts.UpdateRecord) (alerts.Alert, error) {
	definition, err := encodeDefinition(input.Definition)
	if err != nil {
		return alerts.Alert{}, err
	}
	expectedVersion, err := safecast.Conv[int64](input.ExpectedVersion)
	if err != nil {
		return alerts.Alert{}, fmt.Errorf("%w: expected version exceeds storage bounds", alerts.ErrInvalidArgument)
	}
	updates := map[string]any{
		"version": gorm.Expr("version + 1"), "app_id": input.Definition.Application, "name": input.Definition.Name,
		"definition_proto": definition, "endpoint_ciphertext": input.Endpoint.Ciphertext, "endpoint_nonce": input.Endpoint.Nonce,
		"endpoint_generation": input.EndpointGeneration, "webhook_hostname": input.WebhookHostname, "updated_at_unix_micro": input.UpdatedAt.UTC().UnixMicro(),
	}
	var updated alerts.Alert
	err = repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current alertSQLRecord
		if err := tx.Where("tenant_id = ? AND owner_id = ? AND alert_id = ?", repository.tenantID, input.OwnerID, input.ID).Take(&current).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return alerts.ErrNotFound
			}
			return err
		}
		if current.Version != expectedVersion {
			return alerts.ErrVersionConflict
		}
		if current.Enabled == 1 {
			next, nextErr := nextOccurrence(input.Definition, input.UpdatedAt)
			if nextErr != nil {
				return nextErr
			}
			updates["next_run_at_unix_micro"] = next.UnixMicro()
		}
		result := tx.Model(&alertSQLRecord{}).
			Where("tenant_id = ? AND owner_id = ? AND alert_id = ? AND version = ?", repository.tenantID, input.OwnerID, input.ID, input.ExpectedVersion).
			Updates(updates)
		if result.Error != nil {
			if strings.Contains(strings.ToLower(result.Error.Error()), "unique") {
				return alerts.ErrAlreadyExists
			}
			return result.Error
		}
		if result.RowsAffected != 1 {
			return repository.notFoundOrConflict(tx, input.OwnerID, input.ID)
		}
		var record alertSQLRecord
		if err := tx.Where("alert_id = ?", input.ID).Take(&record).Error; err != nil {
			return err
		}
		var conversionErr error
		updated, conversionErr = alertFromSQL(record)
		return conversionErr
	})
	if err != nil {
		return alerts.Alert{}, mapSQLError(ctx, "update alert", err)
	}
	return updated, nil
}

func (repository *SQLRepository) SetState(ctx context.Context, input alerts.SetStateRecord) (alerts.Alert, error) {
	expectedVersion, conversionErr := safecast.Conv[int64](input.ExpectedVersion)
	if conversionErr != nil {
		return alerts.Alert{}, fmt.Errorf("%w: expected version exceeds storage bounds", alerts.ErrInvalidArgument)
	}
	var updated alerts.Alert
	err := repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var record alertSQLRecord
		if err := tx.Where("tenant_id = ? AND owner_id = ? AND alert_id = ?", repository.tenantID, input.OwnerID, input.ID).Take(&record).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return alerts.ErrNotFound
			}
			return err
		}
		if record.Version != expectedVersion {
			return alerts.ErrVersionConflict
		}
		var nextRun *int64
		if input.State == alerts.AlertEnabled {
			definition, err := decodeDefinition(record.Definition)
			if err != nil {
				return err
			}
			next, err := nextOccurrence(definition, input.UpdatedAt)
			if err != nil {
				return err
			}
			value := next.UnixMicro()
			nextRun = &value
		} else if input.State != alerts.AlertDisabled {
			return alerts.ErrInvalidArgument
		}
		result := tx.Model(&alertSQLRecord{}).Where("alert_id = ? AND version = ?", input.ID, input.ExpectedVersion).Updates(map[string]any{
			"version": gorm.Expr("version + 1"), "enabled": boolInteger(input.State == alerts.AlertEnabled),
			"next_run_at_unix_micro": nextRun, "updated_at_unix_micro": input.UpdatedAt.UTC().UnixMicro(),
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return alerts.ErrVersionConflict
		}
		if err := tx.Where("alert_id = ?", input.ID).Take(&record).Error; err != nil {
			return err
		}
		var err error
		updated, err = alertFromSQL(record)
		return err
	})
	if err != nil {
		return alerts.Alert{}, mapSQLError(ctx, "set alert state", err)
	}
	return updated, nil
}

func (repository *SQLRepository) RotateSecret(ctx context.Context, input alerts.RotateSecretRecord) (alerts.Alert, error) {
	result := repository.db.WithContext(ctx).Model(&alertSQLRecord{}).
		Where("tenant_id = ? AND owner_id = ? AND alert_id = ? AND version = ? AND secret_generation = ?", repository.tenantID, input.OwnerID, input.ID, input.ExpectedVersion, input.ExpectedGeneration).
		Updates(map[string]any{
			"version": gorm.Expr("version + 1"), "secret_generation": input.SecretGeneration.Generation,
			"secret_ciphertext": input.SecretGeneration.Encrypted.Ciphertext, "secret_nonce": input.SecretGeneration.Encrypted.Nonce,
			"secret_rotated_at_unix_micro": input.SecretGeneration.CreatedAt.UTC().UnixMicro(), "updated_at_unix_micro": input.UpdatedAt.UTC().UnixMicro(),
		})
	if result.Error != nil {
		return alerts.Alert{}, mapSQLError(ctx, "rotate alert secret", result.Error)
	}
	if result.RowsAffected != 1 {
		return alerts.Alert{}, repository.lookupConflict(ctx, input.OwnerID, input.ID)
	}
	return repository.GetSecretBearing(ctx, input.OwnerID, input.ID)
}

func (repository *SQLRepository) AuthorizeDelivery(ctx context.Context, input alerts.AuthorizeDeliveryRecord) (alerts.DeliveryAuthorization, error) {
	if input.DeliveryID == "" || len(input.DeliveryID) > 128 {
		return "", fmt.Errorf("%w: delivery ID is invalid", alerts.ErrInvalidArgument)
	}
	secretGeneration, err := safecast.Conv[int64](input.SecretGeneration)
	if err != nil {
		return "", fmt.Errorf("%w: secret generation exceeds storage bounds", alerts.ErrInvalidArgument)
	}
	var authorization alerts.DeliveryAuthorization
	err = repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var record alertSQLRecord
		if err := tx.Select("secret_generation").Where("tenant_id = ? AND owner_id = ? AND alert_id = ?", repository.tenantID, input.OwnerID, input.AlertID).Take(&record).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return alerts.ErrNotFound
			}
			return err
		}
		if record.SecretGeneration != secretGeneration {
			authorization = alerts.DeliverySecretRotated
			return nil
		}
		result := tx.Model(&alertRunSQLRecord{}).
			Where("alert_run_id = ? AND alert_id = ? AND secret_generation = ? AND outcome = ? AND delivery_id IS NULL", input.AlertRunID, input.AlertID, input.SecretGeneration, persistedRunOutcomeActive).
			Updates(map[string]any{"delivery_id": input.DeliveryID, "delivery_authorized_at_unix_micro": input.AuthorizedAt.UTC().UnixMicro()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 1 {
			authorization = alerts.DeliveryAuthorized
			return nil
		}
		var run alertRunSQLRecord
		if err := tx.Select("secret_generation", "delivery_id").Where("alert_run_id = ? AND alert_id = ?", input.AlertRunID, input.AlertID).Take(&run).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return alerts.ErrNotFound
			}
			return err
		}
		if run.SecretGeneration != secretGeneration {
			authorization = alerts.DeliverySecretRotated
			return nil
		}
		if run.DeliveryID != nil {
			authorization = alerts.DeliveryAlreadyAttempted
			return nil
		}
		return alerts.ErrNotFound
	})
	if err != nil {
		return "", mapSQLError(ctx, "authorize alert delivery", err)
	}
	return authorization, nil
}

func (repository *SQLRepository) DeleteIfIdle(ctx context.Context, input alerts.DeleteRecord) error {
	expectedVersion, conversionErr := safecast.Conv[int64](input.ExpectedVersion)
	if conversionErr != nil {
		return fmt.Errorf("%w: expected version exceeds storage bounds", alerts.ErrInvalidArgument)
	}
	err := repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var record alertSQLRecord
		if err := tx.Select("version").Where("tenant_id = ? AND owner_id = ? AND alert_id = ?", repository.tenantID, input.OwnerID, input.ID).Take(&record).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return alerts.ErrNotFound
			}
			return err
		}
		if record.Version != expectedVersion {
			return alerts.ErrVersionConflict
		}
		var active int64
		if err := tx.Model(&alertRunSQLRecord{}).Where("alert_id = ? AND outcome = ?", input.ID, persistedRunOutcomeActive).Count(&active).Error; err != nil {
			return err
		}
		if active != 0 {
			return alerts.ErrActiveRun
		}
		result := tx.Where("alert_id = ? AND version = ?", input.ID, input.ExpectedVersion).Delete(&alertSQLRecord{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return alerts.ErrVersionConflict
		}
		return nil
	})
	return mapSQLError(ctx, "delete alert", err)
}

func (repository *SQLRepository) ClaimDue(ctx context.Context, now time.Time, limit int) ([]alerts.RunSnapshot, error) {
	if limit <= 0 || limit > alerts.MaximumAlertsPerOwner {
		limit = alerts.MaximumAlertsPerOwner
	}
	claimed := make([]alerts.RunSnapshot, 0, limit)
	err := repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var records []alertSQLRecord
		if err := tx.Where("tenant_id = ? AND enabled = 1 AND next_run_at_unix_micro <= ?", repository.tenantID, now.UTC().UnixMicro()).Order("next_run_at_unix_micro, alert_id").Limit(limit).Find(&records).Error; err != nil {
			return err
		}
		alertIDs := make([]string, len(records))
		for index, record := range records {
			alertIDs[index] = record.AlertID
		}
		activeAlertIDs := make(map[string]struct{}, len(records))
		if len(alertIDs) > 0 {
			var activeRows []struct {
				AlertID string `gorm:"column:alert_id"`
			}
			if err := tx.Model(&alertRunSQLRecord{}).
				Select("alert_id").
				Where("alert_id IN ? AND outcome = ?", alertIDs, persistedRunOutcomeActive).
				Group("alert_id").
				Find(&activeRows).Error; err != nil {
				return err
			}
			for _, row := range activeRows {
				activeAlertIDs[row.AlertID] = struct{}{}
			}
		}
		for _, record := range records {
			definition, err := decodeDefinition(record.Definition)
			if err != nil {
				return err
			}
			firstDue := databaseTime(*record.NextRunAt)
			scheduled, next, period, missed, err := advanceSchedule(ctx, definition, firstDue, now.UTC())
			if err != nil {
				return err
			}
			dispatch, triggered, err := resolveAlertRetention(definition, period)
			if err != nil {
				return err
			}
			runID, err := repository.idGenerator()
			if err != nil || runID == "" || len(runID) > 128 {
				return errors.New("alerts: generate alert run ID")
			}
			snapshot := alerts.RunSnapshot{
				AlertID: record.AlertID, AlertRunID: runID, AlertVersion: safecast.MustConv[uint64](record.Version), OwnerID: record.OwnerID,
				TenantID:   record.TenantID,
				Definition: definition, Endpoint: alerts.EncryptedValue{Nonce: record.EndpointNonce, Ciphertext: record.EndpointCiphertext},
				EndpointGeneration: safecast.MustConv[uint64](record.EndpointGeneration), SecretGeneration: alerts.SecretGeneration{
					Generation: safecast.MustConv[uint64](record.SecretGeneration), Encrypted: alerts.EncryptedValue{Nonce: record.SecretNonce, Ciphertext: record.SecretCiphertext}, CreatedAt: databaseTime(record.SecretRotatedAt),
				}, ScheduledAt: scheduled, ClaimedAt: now.UTC(), NextScheduledAt: next,
				MissedOccurrenceCount: missed, DispatchRetention: dispatch, TriggeredRetention: triggered,
			}
			encoded, err := json.Marshal(snapshot)
			if err != nil || len(encoded) > 524288 {
				return errors.New("alerts: encode run snapshot")
			}
			_, hasActiveRun := activeAlertIDs[record.AlertID]
			outcome := persistedRunOutcomeActive
			started := now.UTC().UnixMicro()
			var finished *int64
			if hasActiveRun {
				outcome = persistedRunOutcomeOverlapSkipped
				finished = &started
			}
			run := alertRunSQLRecord{
				AlertRunID: runID, AlertID: record.AlertID, AlertVersion: record.Version, SecretGeneration: record.SecretGeneration,
				ScheduledAt: scheduled.UnixMicro(), StartedAt: &started, FinishedAt: finished, Outcome: outcome,
				MissedOccurrenceCount: int64(missed), Snapshot: encoded,
			}
			if err := tx.Create(&run).Error; err != nil {
				return err
			}
			nextMicro := next.UnixMicro()
			advance := tx.Model(&alertSQLRecord{}).Where("alert_id = ? AND version = ?", record.AlertID, record.Version).Updates(map[string]any{
				"next_run_at_unix_micro": nextMicro, "last_claimed_at_unix_micro": now.UTC().UnixMicro(),
			})
			if advance.Error != nil {
				return advance.Error
			}
			if advance.RowsAffected != 1 {
				return alerts.ErrVersionConflict
			}
			status := alerts.RunSummary{AlertID: record.AlertID, ScheduledAt: scheduled, StartedAt: now.UTC(), Outcome: alerts.RunSearching}
			if hasActiveRun {
				status.Outcome = alerts.RunOverlapSkipped
				status.FinishedAt = now.UTC()
			}
			if err := updateAlertSummaryStatus(tx, status); err != nil {
				return err
			}
			if hasActiveRun {
				if err := pruneRuns(tx, record.AlertID); err != nil {
					return err
				}
			} else {
				claimed = append(claimed, snapshot)
			}
		}
		return nil
	})
	if err != nil {
		return nil, mapSQLError(ctx, "claim due alerts", err)
	}
	return claimed, nil
}

// ClaimRunNow snapshots an immediate operator-triggered occurrence without
// moving the alert's persisted cron cursor. A concurrent active run produces
// a terminal overlap record and returns active=false.
func (repository *SQLRepository) ClaimRunNow(ctx context.Context, ownerID, alertID string, now time.Time) (alerts.RunSnapshot, bool, error) {
	if strings.TrimSpace(ownerID) == "" || strings.TrimSpace(alertID) == "" || now.IsZero() {
		return alerts.RunSnapshot{}, false, fmt.Errorf("%w: owner, alert, and run time are required", alerts.ErrInvalidArgument)
	}
	now = now.UTC()
	var snapshot alerts.RunSnapshot
	active := false
	err := repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var record alertSQLRecord
		if err := tx.Where("tenant_id = ? AND owner_id = ? AND alert_id = ?", repository.tenantID, ownerID, alertID).Take(&record).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return alerts.ErrNotFound
			}
			return err
		}
		definition, err := decodeDefinition(record.Definition)
		if err != nil {
			return err
		}
		reference, err := nextOccurrence(definition, now)
		if err != nil {
			return err
		}
		afterReference, err := nextOccurrence(definition, reference)
		if err != nil {
			return err
		}
		period := afterReference.Sub(reference)
		dispatch, triggered, err := resolveAlertRetention(definition, period)
		if err != nil {
			return err
		}
		runID, err := repository.idGenerator()
		if err != nil || runID == "" || len(runID) > 128 {
			return errors.New("alerts: generate alert run ID")
		}
		nextScheduled := reference
		if record.NextRunAt != nil {
			nextScheduled = databaseTime(*record.NextRunAt)
		}
		snapshot = alerts.RunSnapshot{
			AlertID: record.AlertID, AlertRunID: runID, AlertVersion: safecast.MustConv[uint64](record.Version),
			OwnerID: record.OwnerID, TenantID: record.TenantID, Definition: definition,
			Endpoint:           alerts.EncryptedValue{Nonce: append([]byte(nil), record.EndpointNonce...), Ciphertext: append([]byte(nil), record.EndpointCiphertext...)},
			EndpointGeneration: safecast.MustConv[uint64](record.EndpointGeneration),
			SecretGeneration: alerts.SecretGeneration{
				Generation: safecast.MustConv[uint64](record.SecretGeneration),
				Encrypted:  alerts.EncryptedValue{Nonce: append([]byte(nil), record.SecretNonce...), Ciphertext: append([]byte(nil), record.SecretCiphertext...)},
				CreatedAt:  databaseTime(record.SecretRotatedAt),
			},
			ScheduledAt: now, ClaimedAt: now, NextScheduledAt: nextScheduled,
			DispatchRetention: dispatch, TriggeredRetention: triggered,
		}
		encoded, err := json.Marshal(snapshot)
		if err != nil || len(encoded) > 524288 {
			return errors.New("alerts: encode run snapshot")
		}
		var activeCount int64
		if err := tx.Model(&alertRunSQLRecord{}).Where("alert_id = ? AND outcome = ?", record.AlertID, persistedRunOutcomeActive).Count(&activeCount).Error; err != nil {
			return err
		}
		started := now.UnixMicro()
		outcome := persistedRunOutcomeActive
		var finished *int64
		active = activeCount == 0
		if !active {
			outcome = persistedRunOutcomeOverlapSkipped
			finished = &started
		}
		if err := tx.Create(&alertRunSQLRecord{
			AlertRunID: runID, AlertID: record.AlertID, AlertVersion: record.Version,
			SecretGeneration: record.SecretGeneration, ScheduledAt: started, StartedAt: &started,
			FinishedAt: finished, Outcome: outcome, Snapshot: encoded,
		}).Error; err != nil {
			return err
		}
		status := alerts.RunSummary{AlertID: record.AlertID, ScheduledAt: now, StartedAt: now, Outcome: alerts.RunSearching}
		if !active {
			status.Outcome = alerts.RunOverlapSkipped
			status.FinishedAt = now
		}
		if err := updateAlertSummaryStatus(tx, status); err != nil {
			return err
		}
		if !active {
			return pruneRuns(tx, record.AlertID)
		}
		return nil
	})
	if err != nil {
		return alerts.RunSnapshot{}, false, mapSQLError(ctx, "claim immediate alert run", err)
	}
	return snapshot, active, nil
}

func (repository *SQLRepository) RecordOverlap(ctx context.Context, summary alerts.RunSummary) error {
	if summary.AlertID == "" || summary.AlertRunID == "" || summary.ScheduledAt.IsZero() {
		return fmt.Errorf("%w: overlap run identity and schedule are required", alerts.ErrInvalidArgument)
	}
	err := repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var alert alertSQLRecord
		if err := tx.Where("tenant_id = ? AND alert_id = ?", repository.tenantID, summary.AlertID).Take(&alert).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return alerts.ErrNotFound
			}
			return err
		}
		snapshot, err := json.Marshal(alerts.RunSnapshot{
			AlertID: summary.AlertID, AlertRunID: summary.AlertRunID,
			AlertVersion: safecast.MustConv[uint64](alert.Version), OwnerID: alert.OwnerID,
			ScheduledAt: summary.ScheduledAt, ClaimedAt: summary.StartedAt,
			MissedOccurrenceCount: summary.MissedOccurrenceCount,
		})
		if err != nil {
			return errors.New("alerts: encode overlap snapshot")
		}
		started := summary.StartedAt
		if started.IsZero() {
			started = repository.clock().UTC()
		}
		finished := summary.FinishedAt
		if finished.IsZero() {
			finished = started
		}
		startedMicro := started.UTC().UnixMicro()
		finishedMicro := finished.UTC().UnixMicro()
		run := alertRunSQLRecord{
			AlertRunID: summary.AlertRunID, AlertID: summary.AlertID,
			AlertVersion: alert.Version, SecretGeneration: alert.SecretGeneration,
			ScheduledAt: summary.ScheduledAt.UTC().UnixMicro(), StartedAt: &startedMicro,
			FinishedAt: &finishedMicro, Outcome: persistedRunOutcomeOverlapSkipped,
			MissedOccurrenceCount: int64(summary.MissedOccurrenceCount), Snapshot: snapshot,
		}
		if err := tx.Create(&run).Error; err != nil {
			return err
		}
		summary.Outcome = alerts.RunOverlapSkipped
		summary.StartedAt = started
		summary.FinishedAt = finished
		summary.Evaluation = ""
		summary.Delivery = alerts.DeliveryResult{}
		if err := updateAlertSummaryStatus(tx, summary); err != nil {
			return err
		}
		return pruneRuns(tx, summary.AlertID)
	})
	return mapSQLError(ctx, "record alert overlap", err)
}

func (repository *SQLRepository) AttachSearchJob(ctx context.Context, alertID, runID, jobID string, expiresAt time.Time) error {
	if jobID == "" || len(jobID) > 256 || expiresAt.IsZero() {
		return fmt.Errorf("%w: retained search job identity and expiry are required", alerts.ErrInvalidArgument)
	}
	result := repository.db.WithContext(ctx).Model(&alertRunSQLRecord{}).
		Where("alert_id = ? AND alert_run_id = ? AND outcome = ? AND search_job_id IS NULL", alertID, runID, persistedRunOutcomeActive).
		Updates(map[string]any{"search_job_id": jobID, "search_job_expires_at_unix_micro": expiresAt.UTC().UnixMicro()})
	if result.Error != nil {
		return mapSQLError(ctx, "attach alert search job", result.Error)
	}
	if result.RowsAffected == 1 {
		return nil
	}
	// A transport or driver can report an ambiguous commit after the update
	// reached SQLite. Make an exact retry idempotent without allowing a caller
	// to replace the immutable job identity or expiry.
	var current alertRunSQLRecord
	if err := repository.db.WithContext(ctx).
		Where("alert_id = ? AND alert_run_id = ?", alertID, runID).
		Take(&current).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return alerts.ErrNotFound
		}
		return mapSQLError(ctx, "verify attached alert search job", err)
	}
	if stringValue(current.SearchJobID) == jobID &&
		databaseTimesEqual(optionalTimeValue(current.SearchJobExpiresAt), expiresAt) {
		return nil
	}
	return alerts.ErrVersionConflict
}

func (repository *SQLRepository) CompleteRun(ctx context.Context, summary alerts.RunSummary) error {
	outcome, err := outcomeToSQL(summary.Outcome)
	if err != nil {
		return err
	}
	resultCount, err := safecast.Conv[int64](summary.ResultCount)
	if err != nil {
		return fmt.Errorf("%w: result count exceeds storage bounds", alerts.ErrInvalidArgument)
	}
	finishedAt := summary.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = repository.clock().UTC()
	}
	summary.FinishedAt = finishedAt
	finished := finishedAt.UTC().UnixMicro()
	updates := map[string]any{"outcome": outcome, "finished_at_unix_micro": finished}
	updates["evaluation"] = evaluationToSQL(summary.Evaluation)
	updates["result_count"] = resultCount
	updates["result_count_exact"] = boolInteger(summary.ResultCountExact)
	if !summary.SearchJobExpiresAt.IsZero() {
		updates["search_job_expires_at_unix_micro"] = summary.SearchJobExpiresAt.UTC().UnixMicro()
	}
	if !summary.Delivery.AttemptedAt.IsZero() {
		updates["delivery_attempted_at_unix_micro"] = summary.Delivery.AttemptedAt.UTC().UnixMicro()
	}
	if summary.Delivery.StatusCode != 0 {
		updates["delivery_status_code"] = summary.Delivery.StatusCode
	}
	if len(summary.FailureCategory) > 128 {
		return fmt.Errorf("%w: run failure category is too long", alerts.ErrInvalidArgument)
	}
	if summary.FailureCategory != "" {
		updates["failure_category"] = summary.FailureCategory
	} else if summary.Delivery.Category != "" && summary.Delivery.Category != alerts.DeliverySucceeded {
		updates["failure_category"] = string(summary.Delivery.Category)
	}
	err = repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current alertRunSQLRecord
		if err := tx.
			Where("alert_id = ? AND alert_run_id = ?", summary.AlertID, summary.AlertRunID).
			Take(&current).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return alerts.ErrNotFound
			}
			return err
		}
		if current.Outcome != persistedRunOutcomeActive {
			if completedAlertRunMatches(current, summary) {
				return nil
			}
			return alerts.ErrVersionConflict
		}
		result := tx.Model(&alertRunSQLRecord{}).
			Where("alert_id = ? AND alert_run_id = ? AND outcome = ?", summary.AlertID, summary.AlertRunID, persistedRunOutcomeActive).Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return alerts.ErrNotFound
		}
		summary.ScheduledAt = databaseTime(current.ScheduledAt)
		if err := updateAlertSummaryStatus(tx, summary); err != nil {
			return err
		}
		return pruneRuns(tx, summary.AlertID)
	})
	return mapSQLError(ctx, "complete alert run", err)
}

func completedAlertRunMatches(record alertRunSQLRecord, expected alerts.RunSummary) bool {
	actual := runSummaryFromSQL(record)
	if actual.Outcome != expected.Outcome ||
		!databaseTimesEqual(actual.FinishedAt, expected.FinishedAt) ||
		actual.Evaluation != expected.Evaluation ||
		actual.ResultCount != expected.ResultCount ||
		actual.ResultCountExact != expected.ResultCountExact {
		return false
	}
	if !expected.SearchJobExpiresAt.IsZero() &&
		!databaseTimesEqual(actual.SearchJobExpiresAt, expected.SearchJobExpiresAt) {
		return false
	}
	if !expected.Delivery.AttemptedAt.IsZero() &&
		!databaseTimesEqual(actual.Delivery.AttemptedAt, expected.Delivery.AttemptedAt) {
		return false
	}
	if expected.Delivery.StatusCode != 0 && actual.Delivery.StatusCode != expected.Delivery.StatusCode {
		return false
	}
	wantFailure := strings.TrimSpace(expected.FailureCategory)
	if wantFailure == "" && expected.Delivery.Category != "" && expected.Delivery.Category != alerts.DeliverySucceeded {
		wantFailure = string(expected.Delivery.Category)
	}
	return actual.FailureCategory == wantFailure
}

func databaseTimesEqual(actual, expected time.Time) bool {
	return !actual.IsZero() && actual.UnixMicro() == expected.UTC().UnixMicro()
}

func (repository *SQLRepository) InterruptUnfinished(ctx context.Context, now time.Time) (int64, error) {
	var interrupted int64
	err := repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var activeRuns []alertRunSQLRecord
		if err := tx.Model(&alertRunSQLRecord{}).
			Where("outcome = ? AND alert_id IN (SELECT alert_id FROM alerts WHERE tenant_id = ?)", persistedRunOutcomeActive, repository.tenantID).
			Select("alert_id", "scheduled_at_unix_micro").Find(&activeRuns).Error; err != nil {
			return err
		}
		result := tx.Model(&alertRunSQLRecord{}).
			Where("outcome = ? AND alert_id IN (SELECT alert_id FROM alerts WHERE tenant_id = ?)", persistedRunOutcomeActive, repository.tenantID).
			Updates(map[string]any{"outcome": persistedRunOutcomeInterrupted, "finished_at_unix_micro": now.UTC().UnixMicro(), "failure_category": string(alerts.FailureProcessRestart)})
		if result.Error != nil {
			return result.Error
		}
		interrupted = result.RowsAffected
		for _, run := range activeRuns {
			if err := updateAlertSummaryStatus(tx, alerts.RunSummary{
				AlertID: run.AlertID, ScheduledAt: databaseTime(run.ScheduledAt),
				FinishedAt: now.UTC(), Outcome: alerts.RunInterrupted,
			}); err != nil {
				return err
			}
			if err := pruneRuns(tx, run.AlertID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, mapSQLError(ctx, "interrupt unfinished alert runs", err)
	}
	return interrupted, nil
}

func (repository *SQLRepository) ListRuns(ctx context.Context, ownerID, alertID string, limit int) ([]alerts.RunSummary, error) {
	if limit <= 0 || limit > alerts.MaximumRunHistory {
		limit = alerts.MaximumRunHistory
	}
	var records []alertRunSQLRecord
	err := repository.db.WithContext(ctx).Table("alert_runs AS runs").
		Select("runs.*").Joins("JOIN alerts ON alerts.alert_id = runs.alert_id").
		Where("alerts.tenant_id = ? AND alerts.owner_id = ? AND runs.alert_id = ?", repository.tenantID, ownerID, alertID).
		Order("runs.scheduled_at_unix_micro DESC, runs.alert_run_id DESC").Limit(limit).Scan(&records).Error
	if err != nil {
		return nil, mapSQLError(ctx, "list alert runs", err)
	}
	result := make([]alerts.RunSummary, len(records))
	for index, record := range records {
		result[index] = runSummaryFromSQL(record)
	}
	return result, nil
}

func (repository *SQLRepository) notFoundOrConflict(tx *gorm.DB, ownerID, id string) error {
	var count int64
	if err := tx.Model(&alertSQLRecord{}).Where("tenant_id = ? AND owner_id = ? AND alert_id = ?", repository.tenantID, ownerID, id).Count(&count).Error; err != nil {
		return err
	}
	if count == 0 {
		return alerts.ErrNotFound
	}
	return alerts.ErrVersionConflict
}

func (repository *SQLRepository) lookupConflict(ctx context.Context, ownerID, id string) error {
	return mapSQLError(ctx, "read alert conflict", repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return repository.notFoundOrConflict(tx, ownerID, id) }))
}

func alertFromSQL(record alertSQLRecord) (alerts.Alert, error) {
	definition, err := decodeDefinition(record.Definition)
	if err != nil {
		return alerts.Alert{}, err
	}
	return alerts.Alert{
		ID: record.AlertID, OwnerID: record.OwnerID, Version: safecast.MustConv[uint64](record.Version), State: stateFromEnabled(record.Enabled), Definition: definition,
		Endpoint: alerts.EncryptedValue{Nonce: append([]byte(nil), record.EndpointNonce...), Ciphertext: append([]byte(nil), record.EndpointCiphertext...)}, EndpointGeneration: safecast.MustConv[uint64](record.EndpointGeneration),
		WebhookHostname:  record.WebhookHostname,
		SecretGeneration: alerts.SecretGeneration{Generation: safecast.MustConv[uint64](record.SecretGeneration), Encrypted: alerts.EncryptedValue{Nonce: append([]byte(nil), record.SecretNonce...), Ciphertext: append([]byte(nil), record.SecretCiphertext...)}, CreatedAt: databaseTime(record.SecretRotatedAt)},
		NextRunAt:        optionalDatabaseTime(record.NextRunAt), LastOutcome: optionalOutcomeFromSQL(record.LastOutcome),
		LastEvaluatedAt: optionalDatabaseTime(record.LastEvaluatedAt), LastDeliveredAt: optionalDatabaseTime(record.LastDeliveredAt),
		CreatedAt: databaseTime(record.CreatedAt), UpdatedAt: databaseTime(record.UpdatedAt),
	}, nil
}

func alertSummaryFromSQL(record alertSQLRecord, definition alerts.Definition) alerts.AlertSummary {
	return alerts.AlertSummary{
		ID: record.AlertID, OwnerID: record.OwnerID, Version: safecast.MustConv[uint64](record.Version), State: stateFromEnabled(record.Enabled),
		Definition: definition, WebhookHostname: record.WebhookHostname, SecretGeneration: safecast.MustConv[uint64](record.SecretGeneration),
		SecretRotatedAt: databaseTime(record.SecretRotatedAt), NextRunAt: optionalDatabaseTime(record.NextRunAt),
		LastOutcome: optionalOutcomeFromSQL(record.LastOutcome), LastEvaluatedAt: optionalDatabaseTime(record.LastEvaluatedAt),
		LastDeliveredAt: optionalDatabaseTime(record.LastDeliveredAt),
		CreatedAt:       databaseTime(record.CreatedAt), UpdatedAt: databaseTime(record.UpdatedAt),
	}
}

func encodeDefinition(definition alerts.Definition) ([]byte, error) {
	encoded, err := json.Marshal(definition)
	if err != nil || len(encoded) == 0 || len(encoded) > 524288 {
		return nil, fmt.Errorf("%w: alert definition cannot be encoded", alerts.ErrInvalidArgument)
	}
	return encoded, nil
}

func decodeDefinition(encoded []byte) (alerts.Definition, error) {
	var definition alerts.Definition
	if len(encoded) == 0 || json.Unmarshal(encoded, &definition) != nil {
		return alerts.Definition{}, errors.New("alerts: persisted definition is invalid")
	}
	return definition, nil
}

func nextOccurrence(definition alerts.Definition, after time.Time) (time.Time, error) {
	schedule, err := scheduler.ParseCron(definition.Cron, definition.Timezone)
	if err != nil {
		return time.Time{}, errors.New("alerts: persisted schedule is invalid")
	}
	return schedule.Next(after), nil
}

func advanceSchedule(
	ctx context.Context,
	definition alerts.Definition,
	firstDue time.Time,
	now time.Time,
) (time.Time, time.Time, time.Duration, uint32, error) {
	schedule, err := scheduler.ParseCron(definition.Cron, definition.Timezone)
	if err != nil {
		return time.Time{}, time.Time{}, 0, 0, err
	}
	firstNext := schedule.Next(firstDue)
	latestDue, next, skipped, err := schedule.AdvancePastContext(ctx, firstNext, now)
	if err != nil {
		return time.Time{}, time.Time{}, 0, 0, err
	}
	claimedOccurrence := firstDue
	missed := skipped
	if !latestDue.IsZero() {
		claimedOccurrence = latestDue
		missed++
	}
	if missed > math.MaxUint32 {
		return time.Time{}, time.Time{}, 0, 0, errors.New("alerts: missed occurrence count exceeds storage bounds")
	}
	// A coalesced run represents the latest due occurrence. Resolve Np from
	// that immutable occurrence to its successor so relative search time and
	// DST-sensitive retention cannot be anchored to stale pre-downtime intent.
	period, err := schedule.Period(claimedOccurrence)
	if err != nil {
		return time.Time{}, time.Time{}, 0, 0, err
	}
	return claimedOccurrence, next, period, uint32(missed), nil
}

func resolveAlertRetention(definition alerts.Definition, period time.Duration) (time.Duration, time.Duration, error) {
	dispatch, err := searchretention.ScheduledAlert(definition.DispatchTTL, period)
	if err != nil {
		return 0, 0, fmt.Errorf("alerts: resolve dispatch retention: %w", err)
	}
	triggered, err := searchretention.Alert(definition.DispatchTTL, definition.WebhookTTL, period)
	if err != nil {
		return 0, 0, fmt.Errorf("alerts: resolve triggered retention: %w", err)
	}
	return dispatch.Lifetime, triggered.Lifetime, nil
}

func outcomeToSQL(outcome alerts.RunOutcome) (persistedRunOutcome, error) {
	switch outcome {
	case alerts.RunSearchFailed:
		return persistedRunOutcomeSearchFailed, nil
	case alerts.RunSearchCanceled:
		return persistedRunOutcomeSearchCanceled, nil
	case alerts.RunSearchExpired:
		return persistedRunOutcomeSearchExpired, nil
	case alerts.RunNotTriggered:
		return persistedRunOutcomeNotTriggered, nil
	case alerts.RunIndeterminate:
		return persistedRunOutcomeIndeterminate, nil
	case alerts.RunDelivered:
		return persistedRunOutcomeDelivered, nil
	case alerts.RunDeliveryFailed:
		return persistedRunOutcomeDeliveryFailed, nil
	case alerts.RunDeliveryUnknown:
		return persistedRunOutcomeDeliveryUnknown, nil
	case alerts.RunOverlapSkipped:
		return persistedRunOutcomeOverlapSkipped, nil
	case alerts.RunInterrupted:
		return persistedRunOutcomeInterrupted, nil
	case alerts.RunDeliverySkipped:
		return persistedRunOutcomeDeliverySkipped, nil
	default:
		return 0, fmt.Errorf("%w: terminal alert run outcome is invalid", alerts.ErrInvalidArgument)
	}
}

func runSummaryFromSQL(record alertRunSQLRecord) alerts.RunSummary {
	deliveryCategory := alerts.DeliveryCategory(stringValue(record.FailureCategory))
	if record.Outcome == persistedRunOutcomeDelivered {
		deliveryCategory = alerts.DeliverySucceeded
	}
	return alerts.RunSummary{
		AlertID: record.AlertID, AlertRunID: record.AlertRunID, AlertVersion: safecast.MustConv[uint64](record.AlertVersion), SearchJobID: stringValue(record.SearchJobID),
		SearchJobExpiresAt: optionalTimeValue(record.SearchJobExpiresAt), DeliveryID: stringValue(record.DeliveryID), FailureCategory: stringValue(record.FailureCategory),
		Outcome: outcomeFromSQL(record.Outcome), ScheduledAt: databaseTime(record.ScheduledAt), StartedAt: optionalTimeValue(record.StartedAt), FinishedAt: optionalTimeValue(record.FinishedAt),
		MissedOccurrenceCount: safecast.MustConv[uint32](record.MissedOccurrenceCount), Evaluation: evaluationFromSQL(record.Evaluation),
		ResultCount: recordResultCount(record.ResultCount), ResultCountExact: intBool(record.ResultCountExact),
		Delivery: alerts.DeliveryResult{Category: deliveryCategory, Delivered: record.Outcome == persistedRunOutcomeDelivered, AttemptedAt: optionalTimeValue(record.DeliveryAttemptedAt), StatusCode: intValue(record.DeliveryStatusCode)},
	}
}

func outcomeFromSQL(value persistedRunOutcome) alerts.RunOutcome {
	switch value {
	case persistedRunOutcomeActive:
		return alerts.RunSearching
	case persistedRunOutcomeSearchFailed:
		return alerts.RunSearchFailed
	case persistedRunOutcomeSearchCanceled:
		return alerts.RunSearchCanceled
	case persistedRunOutcomeSearchExpired:
		return alerts.RunSearchExpired
	case persistedRunOutcomeNotTriggered:
		return alerts.RunNotTriggered
	case persistedRunOutcomeIndeterminate:
		return alerts.RunIndeterminate
	case persistedRunOutcomeDelivered:
		return alerts.RunDelivered
	case persistedRunOutcomeDeliveryFailed:
		return alerts.RunDeliveryFailed
	case persistedRunOutcomeDeliveryUnknown:
		return alerts.RunDeliveryUnknown
	case persistedRunOutcomeOverlapSkipped:
		return alerts.RunOverlapSkipped
	case persistedRunOutcomeInterrupted:
		return alerts.RunInterrupted
	case persistedRunOutcomeDeliverySkipped:
		return alerts.RunDeliverySkipped
	default:
		return ""
	}
}

func optionalOutcomeFromSQL(value *persistedRunOutcome) alerts.RunOutcome {
	if value == nil {
		return ""
	}
	return outcomeFromSQL(*value)
}

// updateAlertSummaryStatus maintains three independent bounded projections:
// the newest occurrence outcome, the newest actual condition evaluation, and
// the newest successful delivery. A later overlap or running occurrence may
// change the outcome without erasing either historical timestamp.
func updateAlertSummaryStatus(tx *gorm.DB, summary alerts.RunSummary) error {
	outcome, err := summaryOutcomeToSQL(summary.Outcome)
	if err != nil || summary.AlertID == "" || summary.ScheduledAt.IsZero() {
		if err != nil {
			return err
		}
		return fmt.Errorf("%w: alert summary status is incomplete", alerts.ErrInvalidArgument)
	}
	scheduled := summary.ScheduledAt.UTC().UnixMicro()
	result := tx.Model(&alertSQLRecord{}).
		Where("alert_id = ? AND (last_outcome_scheduled_at_unix_micro IS NULL OR last_outcome_scheduled_at_unix_micro <= ?)", summary.AlertID, scheduled).
		Updates(map[string]any{"last_outcome": outcome, "last_outcome_scheduled_at_unix_micro": scheduled})
	if result.Error != nil {
		return result.Error
	}
	if summary.Evaluation != "" {
		if evaluationToSQL(summary.Evaluation) == 0 || summary.FinishedAt.IsZero() {
			return fmt.Errorf("%w: evaluated alert summary status is invalid", alerts.ErrInvalidArgument)
		}
		evaluated := summary.FinishedAt.UTC().UnixMicro()
		if err := tx.Model(&alertSQLRecord{}).
			Where("alert_id = ? AND (last_evaluated_at_unix_micro IS NULL OR last_evaluated_at_unix_micro < ?)", summary.AlertID, evaluated).
			Update("last_evaluated_at_unix_micro", evaluated).Error; err != nil {
			return err
		}
	}
	if summary.Outcome == alerts.RunDelivered && !summary.Delivery.AttemptedAt.IsZero() {
		delivered := summary.Delivery.AttemptedAt.UTC().UnixMicro()
		if err := tx.Model(&alertSQLRecord{}).
			Where("alert_id = ? AND (last_delivered_at_unix_micro IS NULL OR last_delivered_at_unix_micro < ?)", summary.AlertID, delivered).
			Update("last_delivered_at_unix_micro", delivered).Error; err != nil {
			return err
		}
	}
	return nil
}

func summaryOutcomeToSQL(outcome alerts.RunOutcome) (persistedRunOutcome, error) {
	if outcome == alerts.RunSearching {
		return persistedRunOutcomeActive, nil
	}
	return outcomeToSQL(outcome)
}

func pruneRuns(tx *gorm.DB, alertID string) error {
	return tx.Exec(`DELETE FROM alert_runs WHERE alert_id = ? AND outcome <> ? AND alert_run_id IN (SELECT alert_run_id FROM alert_runs WHERE alert_id = ? AND outcome <> ? ORDER BY scheduled_at_unix_micro DESC, alert_run_id DESC LIMIT -1 OFFSET ?)`, alertID, persistedRunOutcomeActive, alertID, persistedRunOutcomeActive, alerts.MaximumRunHistory).Error
}

func stateFromEnabled(value int64) alerts.AlertState {
	if value == 1 {
		return alerts.AlertEnabled
	}
	return alerts.AlertDisabled
}
func boolInteger(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
func databaseTime(value int64) time.Time { return time.UnixMicro(value).UTC() }
func optionalDatabaseTime(value *int64) *time.Time {
	if value == nil {
		return nil
	}
	result := databaseTime(*value)
	return &result
}
func optionalTimeValue(value *int64) time.Time {
	if value == nil {
		return time.Time{}
	}
	return databaseTime(*value)
}
func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func evaluationToSQL(value alerts.EvaluationCertainty) int64 {
	switch value {
	case alerts.EvaluationTrue:
		return 1
	case alerts.EvaluationFalse:
		return 2
	case alerts.EvaluationIndeterminate:
		return 3
	default:
		return 0
	}
}

func evaluationFromSQL(value int64) alerts.EvaluationCertainty {
	switch value {
	case 1:
		return alerts.EvaluationTrue
	case 2:
		return alerts.EvaluationFalse
	case 3:
		return alerts.EvaluationIndeterminate
	default:
		return ""
	}
}

func intBool(value *int64) bool { return value != nil && *value == 1 }
func intValue(value *int64) int {
	if value == nil {
		return 0
	}
	return safecast.MustConv[int](*value)
}
func recordResultCount(value *int64) uint64 {
	if value == nil || *value < 0 {
		return 0
	}
	return safecast.MustConv[uint64](*value)
}

func validateClientRequestID(value string) error {
	if len(value) < minimumClientRequestIDBytes || len(value) > maximumClientRequestIDBytes {
		return fmt.Errorf(
			"%w: client request ID must contain between %d and %d bytes",
			alerts.ErrInvalidArgument,
			minimumClientRequestIDBytes,
			maximumClientRequestIDBytes,
		)
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return fmt.Errorf("%w: client request ID must contain printable ASCII without spaces", alerts.ErrInvalidArgument)
		}
	}
	return nil
}

func validateOwner(ownerID string) error {
	if strings.TrimSpace(ownerID) == "" || len(ownerID) > maximumOwnerIDBytes {
		return fmt.Errorf("%w: owner ID is required", alerts.ErrInvalidArgument)
	}
	return nil
}

func mapSQLError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, alerts.ErrCapacity) || errors.Is(err, alerts.ErrAlreadyExists) || errors.Is(err, alerts.ErrIdempotencyConflict) || errors.Is(err, alerts.ErrNotFound) || errors.Is(err, alerts.ErrVersionConflict) || errors.Is(err, alerts.ErrActiveRun) || errors.Is(err, alerts.ErrInvalidArgument) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, gorm.ErrRecordNotFound) {
		return alerts.ErrNotFound
	}
	return fmt.Errorf("alerts: %s: %w", operation, err)
}
