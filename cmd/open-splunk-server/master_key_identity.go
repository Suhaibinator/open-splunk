package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"os"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

// loadVerifiedMasterKey binds the external key file to its control database.
// A missing/replaced key therefore cannot silently invalidate every persisted
// collector token. Existing token records without a prior binding require an
// explicit operator migration rather than an unverifiable key guess.
func loadVerifiedMasterKey(ctx context.Context, db *control.DB, path string) ([]byte, error) {
	if ctx == nil || db == nil || db.GORMDB() == nil {
		return nil, errors.New("verify server master key: context and control database are required")
	}
	stored, registered, err := auth.ReadServerMasterKeyIdentity(ctx, db)
	if err != nil {
		return nil, err
	}
	if !registered {
		if err := auth.ValidateServerMasterKeyInitialization(ctx, db); errors.Is(
			err,
			auth.ErrServerMasterKeyIdentityUnsafe,
		) {
			return nil, errors.New("verify server master key: control database has collector tokens but no key identity; migrate or recreate those tokens explicitly")
		} else if err != nil {
			return nil, err
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
	if err := auth.RegisterServerMasterKeyIdentity(ctx, db, fingerprint[:]); err != nil {
		clear(key)
		return nil, err
	}
	return key, nil
}

func fingerprintMasterKey(key []byte) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("open-splunk/server-master-key-fingerprint/v1\x00"))
	_, _ = hash.Write(key)
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}
