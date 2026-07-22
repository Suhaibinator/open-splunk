package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const masterKeyIdentityName = "server-master-v1"

// loadVerifiedMasterKey binds the external key file to its control database.
// A missing/replaced key therefore cannot silently invalidate every persisted
// collector token. Existing token records without a prior binding require an
// explicit operator migration rather than an unverifiable key guess.
func loadVerifiedMasterKey(ctx context.Context, db *control.DB, path string) ([]byte, error) {
	if ctx == nil || db == nil || db.SQLDB() == nil {
		return nil, errors.New("verify server master key: context and control database are required")
	}
	stored, registered, err := readMasterKeyIdentity(ctx, db)
	if err != nil {
		return nil, err
	}
	if !registered {
		count, err := collectorTokenCount(ctx, db.SQLDB())
		if err != nil {
			return nil, err
		}
		if count != 0 {
			return nil, errors.New("verify server master key: control database has collector tokens but no key identity; migrate or recreate those tokens explicitly")
		}
	}

	var key []byte
	if registered {
		absPath, err := resolveMasterKeyPath(path)
		if err != nil {
			return nil, err
		}
		key, err = readMasterKey(absPath)
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("verify server master key: key file is missing for this control database; restore it before startup")
		}
		if err != nil {
			return nil, err
		}
	} else {
		key, err = loadOrCreateMasterKey(path, nil)
		if err != nil {
			return nil, err
		}
	}
	fingerprint := fingerprintMasterKey(key)
	if registered && !hmac.Equal(stored, fingerprint[:]) {
		clear(key)
		return nil, errors.New("verify server master key: key file does not match this control database")
	}
	if err := registerMasterKeyIdentity(ctx, db.SQLDB(), fingerprint[:]); err != nil {
		clear(key)
		return nil, err
	}
	return key, nil
}

func readMasterKeyIdentity(ctx context.Context, db *control.DB) ([]byte, bool, error) {
	var fingerprint []byte
	err := db.SQLDB().QueryRowContext(ctx, `
		SELECT fingerprint FROM server_key_state WHERE key_name = ?`, masterKeyIdentityName,
	).Scan(&fingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read server master-key identity: %w", err)
	}
	if len(fingerprint) != sha256.Size {
		return nil, false, errors.New("read server master-key identity: control database record is corrupt")
	}
	return fingerprint, true, nil
}

func registerMasterKeyIdentity(ctx context.Context, db *sql.DB, fingerprint []byte) (returnedErr error) {
	if len(fingerprint) != sha256.Size {
		return errors.New("register server master-key identity: fingerprint is invalid")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin server master-key registration: %w", err)
	}
	defer func() {
		if returnedErr != nil {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				returnedErr = errors.Join(returnedErr, fmt.Errorf("roll back server master-key registration: %w", err))
			}
		}
	}()

	var existing []byte
	err = tx.QueryRowContext(ctx, `
		SELECT fingerprint FROM server_key_state WHERE key_name = ?`, masterKeyIdentityName,
	).Scan(&existing)
	switch {
	case err == nil:
		if len(existing) != sha256.Size || !hmac.Equal(existing, fingerprint) {
			return errors.New("register server master-key identity: control database is bound to a different key")
		}
	case errors.Is(err, sql.ErrNoRows):
		count, countErr := collectorTokenCount(ctx, tx)
		if countErr != nil {
			return countErr
		}
		if count != 0 {
			return errors.New("register server master-key identity: refusing to bind a database with existing collector tokens")
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO server_key_state (key_name, fingerprint, created_at_unix_micro)
			VALUES (?, ?, ?)`, masterKeyIdentityName, fingerprint, time.Now().UTC().UnixMicro()); err != nil {
			return fmt.Errorf("register server master-key identity: %w", err)
		}
	default:
		return fmt.Errorf("read server master-key registration: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit server master-key registration: %w", err)
	}
	return nil
}

type tokenCountQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func collectorTokenCount(ctx context.Context, query tokenCountQuerier) (uint64, error) {
	var count int64
	if err := query.QueryRowContext(ctx, `SELECT count(*) FROM ingestion_tokens`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count collector tokens for master-key registration: %w", err)
	}
	if count < 0 || uint64(count) > math.MaxInt64 {
		return 0, errors.New("count collector tokens for master-key registration: invalid count")
	}
	return uint64(count), nil
}

func fingerprintMasterKey(key []byte) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("open-splunk/server-master-key-fingerprint/v1\x00"))
	_, _ = hash.Write(key)
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}
