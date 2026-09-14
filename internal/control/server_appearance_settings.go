package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/uipalette"
	"gorm.io/gorm"
)

// ServerAppearanceSettings is the versioned instance-wide UI palette. It is a
// separate singleton from ServerSearchSettings so a palette save never bumps
// the search-limits version or republishes the search policy.
type ServerAppearanceSettings struct {
	Version   uint64
	Palette   uipalette.Palette
	UpdatedAt time.Time
}

type serverAppearanceSettingsRecord struct {
	SingletonID        int64  `gorm:"column:singleton_id;primaryKey"`
	Version            int64  `gorm:"column:version"`
	Palette            string `gorm:"column:palette"`
	UpdatedAtUnixMicro int64  `gorm:"column:updated_at_unix_micro"`
}

func (serverAppearanceSettingsRecord) TableName() string { return "server_appearance_settings" }

type ServerAppearanceSettingsStore struct {
	db       *DB
	tenantID string
	appender ServerSettingsMutationAuditAppender
	now      func() time.Time
}

func NewServerAppearanceSettingsStore(
	db *DB,
	tenantID string,
	appender ServerSettingsMutationAuditAppender,
) (*ServerAppearanceSettingsStore, error) {
	if db == nil || db.orm == nil {
		return nil, fmt.Errorf("%w: appearance database is required", ErrInvalidArgument)
	}
	if err := validateTenantID(tenantID); err != nil {
		return nil, fmt.Errorf("%w: appearance tenant is invalid", ErrInvalidArgument)
	}
	if nilcheck.IsNil(appender) {
		return nil, fmt.Errorf("%w: appearance audit appender is required", ErrInvalidArgument)
	}
	return &ServerAppearanceSettingsStore{db: db, tenantID: tenantID, appender: appender, now: time.Now}, nil
}

// Get returns the persisted palette, or version 0 with the default palette
// before an administrator has chosen one.
func (store *ServerAppearanceSettingsStore) Get(ctx context.Context) (ServerAppearanceSettings, error) {
	if ctx == nil || store == nil || store.db == nil {
		return ServerAppearanceSettings{}, fmt.Errorf("%w: appearance context is required", ErrInvalidArgument)
	}
	var record serverAppearanceSettingsRecord
	err := store.db.orm.WithContext(ctx).First(&record, "singleton_id = ?", 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ServerAppearanceSettings{Palette: uipalette.Default()}, nil
	}
	if err != nil {
		return ServerAppearanceSettings{}, fmt.Errorf("read server appearance settings: %w", err)
	}
	return serverAppearanceSettingsFromRecord(record)
}

// Update replaces the palette when expectedVersion matches the stored
// version (0 before the first save), appending the audit row in the same
// transaction so a failed audit write rolls the palette back.
func (store *ServerAppearanceSettingsStore) Update(
	ctx context.Context,
	expectedVersion uint64,
	palette uipalette.Palette,
) (ServerAppearanceSettings, error) {
	if ctx == nil || store == nil || store.db == nil {
		return ServerAppearanceSettings{}, fmt.Errorf("%w: appearance context is required", ErrInvalidArgument)
	}
	if err := uipalette.Validate(palette); err != nil {
		return ServerAppearanceSettings{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	if expectedVersion >= maximumServerSettingsVersion {
		return ServerAppearanceSettings{}, ErrVersionConflict
	}
	now := store.now().Round(0).UTC().Truncate(time.Microsecond)
	if now.UnixMicro() <= 0 || now.UnixMicro() > maximumControlTimestampUnixMicro {
		return ServerAppearanceSettings{}, fmt.Errorf("%w: appearance time is invalid", ErrInvalidArgument)
	}
	next := expectedVersion + 1
	signedVersion, err := signedServerSettingsValue(next)
	if err != nil {
		return ServerAppearanceSettings{}, err
	}
	record := serverAppearanceSettingsRecord{
		SingletonID:        1,
		Version:            signedVersion,
		Palette:            string(palette),
		UpdatedAtUnixMicro: now.UnixMicro(),
	}
	err = store.db.orm.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current serverAppearanceSettingsRecord
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
			result := tx.Model(&serverAppearanceSettingsRecord{}).
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
				OccurredAt: now, Target: ServerSettingsTargetUIPalette, Version: next,
			},
		)
	})
	if err != nil {
		if errors.Is(err, ErrVersionConflict) {
			return ServerAppearanceSettings{}, ErrVersionConflict
		}
		return ServerAppearanceSettings{}, fmt.Errorf("update server appearance settings: %w", err)
	}
	return ServerAppearanceSettings{Version: next, Palette: palette, UpdatedAt: now}, nil
}

func serverAppearanceSettingsFromRecord(record serverAppearanceSettingsRecord) (ServerAppearanceSettings, error) {
	if record.SingletonID != 1 || record.Version < 1 ||
		record.UpdatedAtUnixMicro <= 0 || record.UpdatedAtUnixMicro > maximumControlTimestampUnixMicro {
		return ServerAppearanceSettings{}, errors.New("server appearance settings are corrupt")
	}
	palette := uipalette.Palette(record.Palette)
	if err := uipalette.Validate(palette); err != nil {
		return ServerAppearanceSettings{}, fmt.Errorf("server appearance settings are corrupt: %w", err)
	}
	return ServerAppearanceSettings{
		Version:   uint64(record.Version),
		Palette:   palette,
		UpdatedAt: time.UnixMicro(record.UpdatedAtUnixMicro).UTC(),
	}, nil
}
