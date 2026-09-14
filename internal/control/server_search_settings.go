package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"gorm.io/gorm"
)

type ServerSearchSettings struct {
	Version   uint64
	Limits    searchlimits.Policy
	UpdatedAt time.Time
}

type serverSearchSettingsRecord struct {
	SingletonID                int64 `gorm:"column:singleton_id;primaryKey"`
	Version                    int64 `gorm:"column:version"`
	MaximumRuntimeNanoseconds  int64 `gorm:"column:maximum_runtime_nanoseconds"`
	MaximumMemoryBytes         int64 `gorm:"column:maximum_memory_bytes"`
	MaximumRowsToRead          int64 `gorm:"column:maximum_rows_to_read"`
	MaximumBytesToRead         int64 `gorm:"column:maximum_bytes_to_read"`
	MaximumGroupedRows         int64 `gorm:"column:maximum_grouped_rows"`
	MaximumThreads             int64 `gorm:"column:maximum_threads"`
	MaximumResultRows          int64 `gorm:"column:maximum_result_rows"`
	MaximumResultBytes         int64 `gorm:"column:maximum_result_bytes"`
	MaximumTotalResultBytes    int64 `gorm:"column:maximum_total_result_bytes"`
	MaximumConcurrentSearches  int64 `gorm:"column:maximum_concurrent_searches"`
	ResultRetentionNanoseconds int64 `gorm:"column:result_retention_nanoseconds"`
	UpdatedAtUnixMicro         int64 `gorm:"column:updated_at_unix_micro"`
}

func (serverSearchSettingsRecord) TableName() string { return "server_search_settings" }

const maximumServerSettingsVersion = uint64(1<<63 - 1)

type ServerSearchSettingsStore struct {
	db       *DB
	tenantID string
	appender ServerSettingsMutationAuditAppender
	now      func() time.Time
}

func NewServerSearchSettingsStore(
	db *DB,
	tenantID string,
	appender ServerSettingsMutationAuditAppender,
) (*ServerSearchSettingsStore, error) {
	if db == nil || db.orm == nil {
		return nil, fmt.Errorf("%w: settings database is required", ErrInvalidArgument)
	}
	if err := validateTenantID(tenantID); err != nil {
		return nil, fmt.Errorf("%w: settings tenant is invalid", ErrInvalidArgument)
	}
	if nilcheck.IsNil(appender) {
		return nil, fmt.Errorf("%w: settings audit appender is required", ErrInvalidArgument)
	}
	return &ServerSearchSettingsStore{db: db, tenantID: tenantID, appender: appender, now: time.Now}, nil
}

func (store *ServerSearchSettingsStore) Get(ctx context.Context) (ServerSearchSettings, error) {
	if ctx == nil || store == nil || store.db == nil {
		return ServerSearchSettings{}, fmt.Errorf("%w: settings context is required", ErrInvalidArgument)
	}
	var record serverSearchSettingsRecord
	err := store.db.orm.WithContext(ctx).First(&record, "singleton_id = ?", 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ServerSearchSettings{Limits: searchlimits.Default()}, nil
	}
	if err != nil {
		return ServerSearchSettings{}, fmt.Errorf("read server search settings: %w", err)
	}
	return serverSearchSettingsFromRecord(record)
}

func (store *ServerSearchSettingsStore) Update(
	ctx context.Context,
	expectedVersion uint64,
	limits searchlimits.Policy,
) (ServerSearchSettings, error) {
	if ctx == nil || store == nil || store.db == nil {
		return ServerSearchSettings{}, fmt.Errorf("%w: settings context is required", ErrInvalidArgument)
	}
	if err := searchlimits.Validate(limits); err != nil {
		return ServerSearchSettings{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	if expectedVersion >= maximumServerSettingsVersion {
		return ServerSearchSettings{}, ErrVersionConflict
	}
	now := store.now().Round(0).UTC().Truncate(time.Microsecond)
	if now.UnixMicro() <= 0 || now.UnixMicro() > maximumControlTimestampUnixMicro {
		return ServerSearchSettings{}, fmt.Errorf("%w: settings time is invalid", ErrInvalidArgument)
	}
	next := expectedVersion + 1
	record, err := serverSearchSettingsRecordFrom(next, limits, now)
	if err != nil {
		return ServerSearchSettings{}, err
	}
	err = store.db.orm.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current serverSearchSettingsRecord
		readErr := tx.First(&current, "singleton_id = ?", 1).Error
		switch {
		case errors.Is(readErr, gorm.ErrRecordNotFound):
			if expectedVersion != 0 {
				return ErrVersionConflict
			}
			if err := tx.Create(&record).Error; err != nil {
				return err
			}
		case readErr != nil:
			return readErr
		case current.Version < 1:
			return ErrVersionConflict
		case uint64(current.Version) != expectedVersion:
			return ErrVersionConflict
		default:
			result := tx.Model(&serverSearchSettingsRecord{}).
				Where("singleton_id = ? AND version = ?", 1, current.Version).
				Updates(&record)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrVersionConflict
			}
		}
		return store.appender.AppendServerSettingsMutationInTransaction(
			ctx,
			tx,
			store.tenantID,
			ServerSettingsMutationAuditEvent{
				OccurredAt: now, Target: ServerSettingsTargetSearchLimits, Version: next,
			},
		)
	})
	if err != nil {
		if errors.Is(err, ErrVersionConflict) {
			return ServerSearchSettings{}, ErrVersionConflict
		}
		return ServerSearchSettings{}, fmt.Errorf("update server search settings: %w", err)
	}
	return ServerSearchSettings{Version: next, Limits: limits, UpdatedAt: now}, nil
}

func serverSearchSettingsRecordFrom(
	version uint64,
	limits searchlimits.Policy,
	updatedAt time.Time,
) (serverSearchSettingsRecord, error) {
	signedVersion, err := signedServerSettingsValue(version)
	if err != nil {
		return serverSearchSettingsRecord{}, err
	}
	maximumMemoryBytes, err := signedServerSettingsValue(limits.MaxMemoryBytes)
	if err != nil {
		return serverSearchSettingsRecord{}, err
	}
	maximumRowsToRead, err := signedServerSettingsValue(limits.MaxRowsToRead)
	if err != nil {
		return serverSearchSettingsRecord{}, err
	}
	maximumBytesToRead, err := signedServerSettingsValue(limits.MaxBytesToRead)
	if err != nil {
		return serverSearchSettingsRecord{}, err
	}
	maximumGroupedRows, err := signedServerSettingsValue(limits.MaxGroupedRows)
	if err != nil {
		return serverSearchSettingsRecord{}, err
	}
	maximumThreads, err := signedServerSettingsValue(limits.MaxThreads)
	if err != nil {
		return serverSearchSettingsRecord{}, err
	}
	maximumResultRows, err := signedServerSettingsValue(limits.MaxResultRows)
	if err != nil {
		return serverSearchSettingsRecord{}, err
	}
	maximumResultBytes, err := signedServerSettingsValue(limits.MaxResultBytes)
	if err != nil {
		return serverSearchSettingsRecord{}, err
	}
	maximumTotalResultBytes, err := signedServerSettingsValue(limits.MaxTotalResultBytes)
	if err != nil {
		return serverSearchSettingsRecord{}, err
	}
	return serverSearchSettingsRecord{
		SingletonID:                1,
		Version:                    signedVersion,
		MaximumRuntimeNanoseconds:  int64(limits.MaxRuntime),
		MaximumMemoryBytes:         maximumMemoryBytes,
		MaximumRowsToRead:          maximumRowsToRead,
		MaximumBytesToRead:         maximumBytesToRead,
		MaximumGroupedRows:         maximumGroupedRows,
		MaximumThreads:             maximumThreads,
		MaximumResultRows:          maximumResultRows,
		MaximumResultBytes:         maximumResultBytes,
		MaximumTotalResultBytes:    maximumTotalResultBytes,
		MaximumConcurrentSearches:  int64(limits.MaxConcurrent),
		ResultRetentionNanoseconds: int64(limits.ResultRetention),
		UpdatedAtUnixMicro:         updatedAt.UnixMicro(),
	}, nil
}

func signedServerSettingsValue(value uint64) (int64, error) {
	if value > maximumServerSettingsVersion {
		return 0, fmt.Errorf("%w: server setting value is too large", ErrInvalidArgument)
	}
	return int64(value), nil
}

func serverSearchSettingsFromRecord(record serverSearchSettingsRecord) (ServerSearchSettings, error) {
	if record.SingletonID != 1 || record.Version < 1 ||
		record.MaximumRuntimeNanoseconds <= 0 ||
		record.MaximumMemoryBytes <= 0 || record.MaximumRowsToRead <= 0 ||
		record.MaximumBytesToRead <= 0 || record.MaximumGroupedRows <= 0 ||
		record.MaximumThreads <= 0 || record.MaximumResultRows <= 0 ||
		record.MaximumResultBytes <= 0 || record.MaximumTotalResultBytes <= 0 ||
		record.MaximumConcurrentSearches <= 0 || record.MaximumConcurrentSearches > 256 ||
		record.ResultRetentionNanoseconds <= 0 || record.UpdatedAtUnixMicro <= 0 ||
		record.UpdatedAtUnixMicro > maximumControlTimestampUnixMicro {
		return ServerSearchSettings{}, errors.New("server search settings are corrupt")
	}
	limits := searchlimits.Policy{
		MaxRuntime:          time.Duration(record.MaximumRuntimeNanoseconds),
		MaxMemoryBytes:      uint64(record.MaximumMemoryBytes),
		MaxRowsToRead:       uint64(record.MaximumRowsToRead),
		MaxBytesToRead:      uint64(record.MaximumBytesToRead),
		MaxGroupedRows:      uint64(record.MaximumGroupedRows),
		MaxThreads:          uint64(record.MaximumThreads),
		MaxResultRows:       uint64(record.MaximumResultRows),
		MaxResultBytes:      uint64(record.MaximumResultBytes),
		MaxTotalResultBytes: uint64(record.MaximumTotalResultBytes),
		MaxConcurrent:       uint32(record.MaximumConcurrentSearches),
		ResultRetention:     time.Duration(record.ResultRetentionNanoseconds),
	}
	if err := searchlimits.Validate(limits); err != nil {
		return ServerSearchSettings{}, fmt.Errorf("server search settings are corrupt: %w", err)
	}
	return ServerSearchSettings{
		Version:   uint64(record.Version),
		Limits:    limits,
		UpdatedAt: time.UnixMicro(record.UpdatedAtUnixMicro).UTC(),
	}, nil
}
