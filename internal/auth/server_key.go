package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

const serverMasterKeyIdentityName = "server-master-v1"

var (
	// ErrServerMasterKeyIdentityConflict means the control database is already
	// bound to a different external server master key.
	ErrServerMasterKeyIdentityConflict = errors.New(
		"auth: control database is bound to a different server master key",
	)
	// ErrServerMasterKeyIdentityUnsafe means collector credentials already
	// exist in a database that has no verifiable server-master-key binding.
	ErrServerMasterKeyIdentityUnsafe = errors.New(
		"auth: collector tokens exist without a server master-key identity",
	)
	// ErrServerMasterKeyIdentityCorrupt means persisted key-identity state
	// cannot be interpreted safely.
	ErrServerMasterKeyIdentityCorrupt = errors.New(
		"auth: server master-key identity is corrupt",
	)
)

// ReadServerMasterKeyIdentity returns a detached SHA-256 fingerprint when the
// control database is already bound to an external server master key.
func ReadServerMasterKeyIdentity(
	ctx context.Context,
	database *control.DB,
) ([]byte, bool, error) {
	orm, err := serverKeyDatabase(ctx, database)
	if err != nil {
		return nil, false, err
	}
	record, registered, err := readServerKeyStateRecord(orm)
	if err != nil {
		return nil, false, fmt.Errorf("read server master-key identity: %w", err)
	}
	if !registered {
		return nil, false, nil
	}
	return append([]byte(nil), record.Fingerprint...), true, nil
}

// ValidateServerMasterKeyInitialization performs the early startup check that
// avoids creating a new key file for an unverifiable database. Registration
// repeats this check inside its writer transaction; this method is not the
// atomic authorization boundary.
func ValidateServerMasterKeyInitialization(
	ctx context.Context,
	database *control.DB,
) error {
	orm, err := serverKeyDatabase(ctx, database)
	if err != nil {
		return err
	}
	_, registered, err := readServerKeyStateRecord(orm)
	if err != nil {
		return fmt.Errorf("read server master-key identity for initialization: %w", err)
	}
	if registered {
		return nil
	}
	count, err := countCollectorTokensForServerKey(orm)
	if err != nil {
		return fmt.Errorf("count collector tokens for master-key registration: %w", err)
	}
	if count != 0 {
		return ErrServerMasterKeyIdentityUnsafe
	}
	return nil
}

// RegisterServerMasterKeyIdentity atomically binds a tokenless control
// database to fingerprint. Registration is idempotent for the same
// fingerprint and fails closed for a different or corrupt identity.
func RegisterServerMasterKeyIdentity(
	ctx context.Context,
	database *control.DB,
	fingerprint []byte,
) error {
	return registerServerMasterKeyIdentityAt(
		ctx,
		database,
		fingerprint,
		time.Now().UTC(),
	)
}

func registerServerMasterKeyIdentityAt(
	ctx context.Context,
	database *control.DB,
	fingerprint []byte,
	registeredAt time.Time,
) (returnedErr error) {
	orm, err := serverKeyDatabase(ctx, database)
	if err != nil {
		return err
	}
	if len(fingerprint) != sha256.Size {
		return fmt.Errorf(
			"%w: server master-key fingerprint must contain exactly %d bytes",
			control.ErrInvalidArgument,
			sha256.Size,
		)
	}
	registeredAtUnixMicro := registeredAt.UTC().UnixMicro()
	if registeredAt.IsZero() || registeredAtUnixMicro <= 0 {
		return fmt.Errorf(
			"%w: server master-key registration time is invalid",
			control.ErrInvalidArgument,
		)
	}
	detachedFingerprint := append([]byte(nil), fingerprint...)

	tx := orm.Begin()
	if tx.Error != nil {
		return fmt.Errorf("begin server master-key registration: %w", tx.Error)
	}
	finished := false
	defer finishServerKeyTransaction(tx, &finished, &returnedErr)

	if err := registerServerKeyStateInTransaction(
		tx,
		detachedFingerprint,
		registeredAtUnixMicro,
	); err != nil {
		return err
	}
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("commit server master-key registration: %w", err)
	}
	finished = true
	return nil
}

func registerServerKeyStateInTransaction(
	tx *gorm.DB,
	fingerprint []byte,
	registeredAtUnixMicro int64,
) error {
	existing, registered, err := readServerKeyStateRecord(tx)
	if err != nil {
		return fmt.Errorf("read server master-key registration: %w", err)
	}
	if registered {
		if !hmac.Equal(existing.Fingerprint, fingerprint) {
			return ErrServerMasterKeyIdentityConflict
		}
		return nil
	}
	count, err := countCollectorTokensForServerKey(tx)
	if err != nil {
		return fmt.Errorf(
			"count collector tokens for master-key registration: %w",
			err,
		)
	}
	if count != 0 {
		return ErrServerMasterKeyIdentityUnsafe
	}
	record := serverKeyStateRecord{
		KeyName:            serverMasterKeyIdentityName,
		Fingerprint:        append([]byte(nil), fingerprint...),
		CreatedAtUnixMicro: registeredAtUnixMicro,
	}
	if err := tx.Create(&record).Error; err != nil {
		return fmt.Errorf("register server master-key identity: %w", err)
	}
	return nil
}

func serverKeyDatabase(ctx context.Context, database *control.DB) (*gorm.DB, error) {
	if ctx == nil || database == nil || database.GORMDB() == nil {
		return nil, fmt.Errorf(
			"%w: server master-key identity context and control database are required",
			control.ErrInvalidArgument,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return database.GORMDB().WithContext(ctx), nil
}

func readServerKeyStateRecord(
	database *gorm.DB,
) (serverKeyStateRecord, bool, error) {
	var records []serverKeyStateRecord
	query := database.Order("key_name ASC").Limit(2).Find(&records)
	if query.Error != nil {
		return serverKeyStateRecord{}, false, query.Error
	}
	if len(records) == 0 {
		return serverKeyStateRecord{}, false, nil
	}
	if len(records) != 1 ||
		records[0].KeyName != serverMasterKeyIdentityName ||
		len(records[0].Fingerprint) != sha256.Size ||
		records[0].CreatedAtUnixMicro <= 0 {
		return serverKeyStateRecord{}, false, ErrServerMasterKeyIdentityCorrupt
	}
	records[0].Fingerprint = append([]byte(nil), records[0].Fingerprint...)
	return records[0], true, nil
}

func countCollectorTokensForServerKey(database *gorm.DB) (uint64, error) {
	var count int64
	if err := database.Model(&collectorTokenRecord{}).Count(&count).Error; err != nil {
		return 0, err
	}
	if count < 0 {
		return 0, errors.New("invalid negative collector-token count")
	}
	return uint64(count), nil
}

func finishServerKeyTransaction(
	tx *gorm.DB,
	finished *bool,
	returnedErr *error,
) {
	if tx == nil || finished == nil || *finished ||
		returnedErr == nil || *returnedErr == nil {
		return
	}
	if err := tx.Rollback().Error; err != nil {
		*returnedErr = errors.Join(
			*returnedErr,
			fmt.Errorf("roll back server master-key registration: %w", err),
		)
	}
}
