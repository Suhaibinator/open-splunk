package exportjournal

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"gorm.io/gorm"

	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
)

// ReferencedBlobs is read-only and does not apply startup interruption repair.
// Backup callers hold the application's maintenance boundary while querying.
func (store *Store) ReferencedBlobs(ctx context.Context) ([]searchartifacts.BlobReference, error) {
	var records []record
	if err := store.db.WithContext(ctx).Where("artifact_name <> ? AND artifact_sha256 IS NOT NULL", "").Find(&records).Error; err != nil {
		return nil, err
	}
	var result []searchartifacts.BlobReference
	for _, record := range records {
		retained, err := decode(record)
		if err != nil {
			return nil, err
		}
		if retained.Job.State != exportjobs.StateCompleted {
			continue
		}
		var identity searchartifacts.BlobIdentity
		if json.Unmarshal([]byte(record.ArtifactName), &identity) != nil {
			return nil, errors.New("corrupt committed export blob reference")
		}
		if !strings.HasPrefix(identity.Key, "open-splunk/exports/") || len(record.ArtifactSHA256) != sha256.Size {
			return nil, errors.New("corrupt export blob reference")
		}
		result = append(result, searchartifacts.BlobReference{Identity: identity, SHA256: append([]byte(nil), record.ArtifactSHA256...)})
	}
	return result, nil
}

func ReferencedBlobs(ctx context.Context, database *gorm.DB) ([]searchartifacts.BlobReference, error) {
	return (&Store{db: database}).ReferencedBlobs(ctx)
}

func RebindBlobsInTransaction(ctx context.Context, tx *sql.Tx, replacements map[string]searchartifacts.BlobIdentity) error {
	rows, err := tx.QueryContext(ctx, `SELECT export_id,artifact_name,artifact_sha256 FROM durable_export_jobs WHERE artifact_name<>'' AND artifact_sha256 IS NOT NULL`)
	if err != nil {
		return err
	}
	type change struct{ id, name string }
	var changes []change
	for rows.Next() {
		var id, encoded string
		var digest []byte
		if err := rows.Scan(&id, &encoded, &digest); err != nil {
			_ = rows.Close()
			return err
		}
		var identity searchartifacts.BlobIdentity
		if json.Unmarshal([]byte(encoded), &identity) != nil {
			_ = rows.Close()
			return errors.New("corrupt committed export blob reference")
		}
		replacement, ok := replacements[identity.Key]
		if !ok {
			continue
		}
		if replacement.Key != identity.Key || replacement.Bytes != identity.Bytes || len(digest) != sha256.Size || (replacement.ETag == "" && replacement.Version == "") {
			_ = rows.Close()
			return errors.New("invalid restored export identity")
		}
		payload, err := json.Marshal(replacement)
		if err != nil {
			_ = rows.Close()
			return err
		}
		changes = append(changes, change{id, string(payload)})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range changes {
		if _, err := tx.ExecContext(ctx, `UPDATE durable_export_jobs SET artifact_name=$1 WHERE export_id=$2`, item.name, item.id); err != nil {
			return err
		}
	}
	return nil
}
