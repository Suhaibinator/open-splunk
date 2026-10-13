package searchartifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
)

// ReferencedBlobs returns only SQL-published completed result objects. Callers
// taking a coherent backup must hold the application's maintenance boundary.
func (store *Store) ReferencedBlobs(ctx context.Context) ([]BlobReference, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT m.manifest,m.sha256 FROM search_capture_manifests m JOIN durable_search_jobs j ON j.id=m.job_id WHERE j.state=$1`, StateCompleted)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var references []BlobReference
	for rows.Next() {
		var payload, digest []byte
		if err := rows.Scan(&payload, &digest); err != nil {
			return nil, err
		}
		hash := sha256.Sum256(payload)
		if len(payload) > maximumCaptureManifestBytes || !bytes.Equal(hash[:], digest) {
			return nil, ErrCorrupt
		}
		var manifest captureManifest
		if err := decodeExactJSON(payload, &manifest); err != nil {
			return nil, ErrCorrupt
		}
		for _, chunk := range manifest.Chunks {
			checksum, err := hex.DecodeString(chunk.Digest)
			if err != nil || len(checksum) != sha256.Size {
				return nil, ErrCorrupt
			}
			references = append(references, BlobReference{Identity: chunk.Identity, SHA256: checksum})
		}
	}
	return references, rows.Err()
}

func ReferencedBlobs(ctx context.Context, database *sql.DB) ([]BlobReference, error) {
	return (&Store{db: database}).ReferencedBlobs(ctx)
}

func RebindBlobs(ctx context.Context, database *sql.DB, replacements map[string]BlobIdentity) error {
	tx, err := pgmeta.BeginSQLLocked(ctx, database, pgmeta.RetainedStorage)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := RebindBlobsInTransaction(ctx, tx, replacements); err != nil {
		return err
	}
	return tx.Commit()
}

// RebindBlobsInTransaction lets recovery co-commit capture and export bindings.
// The caller has verified each restored object's SHA256 before supplying its
// new conditional identity. Object keys and bytes cannot change during rebind.
func RebindBlobsInTransaction(ctx context.Context, tx *sql.Tx, replacements map[string]BlobIdentity) error {
	rows, err := tx.QueryContext(ctx, `SELECT job_id,manifest,sha256 FROM search_capture_manifests`)
	if err != nil {
		return err
	}
	type change struct {
		id              string
		payload, digest []byte
		objects         []BlobIdentity
	}
	var changes []change
	for rows.Next() {
		var id string
		var payload, digest []byte
		if err := rows.Scan(&id, &payload, &digest); err != nil {
			_ = rows.Close()
			return err
		}
		hash := sha256.Sum256(payload)
		if !bytes.Equal(hash[:], digest) {
			_ = rows.Close()
			return ErrCorrupt
		}
		var manifest captureManifest
		if err := decodeExactJSON(payload, &manifest); err != nil {
			_ = rows.Close()
			return err
		}
		var objects []BlobIdentity
		for index, chunk := range manifest.Chunks {
			replacement, ok := replacements[chunk.Identity.Key]
			if !ok {
				continue
			}
			if replacement.Key != chunk.Identity.Key || replacement.Bytes != chunk.Identity.Bytes || (replacement.ETag == "" && replacement.Version == "") {
				_ = rows.Close()
				return ErrInvalid
			}
			manifest.Chunks[index].Identity = replacement
			objects = append(objects, replacement)
		}
		if len(objects) != 0 {
			encoded, err := json.Marshal(manifest)
			if err != nil {
				_ = rows.Close()
				return err
			}
			hash := sha256.Sum256(encoded)
			changes = append(changes, change{id, encoded, hash[:], objects})
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range changes {
		if _, err := tx.ExecContext(ctx, `UPDATE search_capture_manifests SET manifest=$1,sha256=$2 WHERE job_id=$3`, item.payload, item.digest, item.id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE durable_search_jobs SET artifact_sha256=$1 WHERE id=$2`, item.digest, item.id); err != nil {
			return err
		}
		for _, object := range item.objects {
			if _, err := tx.ExecContext(ctx, `UPDATE search_capture_objects SET etag=$1,object_version=$2 WHERE object_key=$3`, object.ETag, object.Version, object.Key); err != nil {
				return err
			}
		}
	}
	return nil
}
