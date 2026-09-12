// Package exportjournal persists export admission independently of worker and
// HTTP lifetimes. The caller owns the database and its audit store.
package exportjournal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"gorm.io/gorm"
)

const maximumProjectionBytes = 2 << 20

type Store struct {
	db    *gorm.DB
	audit *audit.Store
	now   func() time.Time
}

func New(db *gorm.DB, events *audit.Store) (*Store, error) {
	if db == nil || events == nil {
		return nil, errors.New("export journal requires database and audit store")
	}
	return &Store{db: db, audit: events, now: time.Now}, nil
}

type record struct {
	TenantID       string `gorm:"column:tenant_id"`
	OwnerID        string `gorm:"column:owner_id"`
	ExportID       string `gorm:"column:export_id"`
	Version        uint64 `gorm:"column:version"`
	JobJSON        []byte `gorm:"column:job_json"`
	CreatedAt      int64  `gorm:"column:created_at_us"`
	UpdatedAt      int64  `gorm:"column:updated_at_us"`
	ExpiresAt      *int64 `gorm:"column:expires_at_us"`
	MetadataBytes  uint64 `gorm:"column:metadata_bytes"`
	ArtifactName   string `gorm:"column:artifact_name"`
	ArtifactSHA256 []byte `gorm:"column:artifact_sha256"`
}

func (record) TableName() string { return "durable_export_jobs" }

func encode(retained exportjobs.DurableJob) (record, error) {
	job := retained.Job
	payload, err := json.Marshal(job)
	if err != nil || len(payload) > maximumProjectionBytes {
		return record{}, errors.New("export metadata exceeds persistence bound")
	}
	if retained.Access.TenantID == "" || retained.Access.OwnerID == "" || job.ID == "" || job.Version == 0 || job.CreatedAt.IsZero() {
		return record{}, errors.New("invalid export journal identity")
	}
	var expires *int64
	if !job.ExpiresAt.IsZero() {
		value := job.ExpiresAt.UnixMicro()
		expires = &value
	}
	return record{TenantID: retained.Access.TenantID, OwnerID: retained.Access.OwnerID, ExportID: job.ID, Version: job.Version, JobJSON: payload, CreatedAt: job.CreatedAt.UnixMicro(), UpdatedAt: job.Progress.UpdatedAt.UnixMicro(), ExpiresAt: expires, MetadataBytes: max(retained.MetadataBytes, uint64(len(payload)+4096)), ArtifactName: retained.ArtifactName, ArtifactSHA256: retained.ArtifactSHA256}, nil
}

func decode(row record) (exportjobs.DurableJob, error) {
	if len(row.JobJSON) == 0 || len(row.JobJSON) > maximumProjectionBytes {
		return exportjobs.DurableJob{}, errors.New("invalid export journal payload")
	}
	var job exportjobs.Job
	if err := json.Unmarshal(row.JobJSON, &job); err != nil {
		return exportjobs.DurableJob{}, errors.New("invalid export journal encoding")
	}
	if job.ID != row.ExportID || job.Version != row.Version || job.CreatedAt.UnixMicro() != row.CreatedAt || row.OwnerID == "" || row.TenantID == "" {
		return exportjobs.DurableJob{}, errors.New("invalid export journal authority")
	}
	return exportjobs.DurableJob{Access: searchjobs.AccessScope{TenantID: row.TenantID, OwnerID: row.OwnerID}, Job: job, ArtifactName: row.ArtifactName, ArtifactSHA256: append([]byte(nil), row.ArtifactSHA256...), MetadataBytes: row.MetadataBytes}, nil
}

func (store *Store) Admit(ctx context.Context, access searchjobs.AccessScope, job exportjobs.Job) error {
	return store.admit(ctx, access, job, nil)
}
func (store *Store) AdmitIdempotent(ctx context.Context, access searchjobs.AccessScope, job exportjobs.Job, intent requestidempotency.Intent) error {
	return store.admit(ctx, access, job, &intent)
}
func (store *Store) admit(ctx context.Context, access searchjobs.AccessScope, job exportjobs.Job, intent *requestidempotency.Intent) error {
	row, err := encode(exportjobs.DurableJob{Access: access, Job: job})
	if err != nil {
		return err
	}
	if intent != nil && (intent.TenantID != access.TenantID || intent.Route != "/api/search/exports/create") {
		return requestidempotency.ErrUnavailable
	}
	err = store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Terminal metadata may leave the durable catalog after an additional
		// seven-day tombstone interval. Its independent receipt remains authoritative
		// and returns unavailable, never admitting the request again.
		cutoff := store.now().Add(-7 * 24 * time.Hour).UnixMicro()
		if err := tx.Where("tenant_id = ? AND expires_at_us IS NOT NULL AND expires_at_us < ?", access.TenantID, cutoff).Delete(&record{}).Error; err != nil {
			return err
		}
		var budget struct {
			Count int64
			Bytes int64
		}
		if err := tx.Model(&record{}).Select("count(*) AS count, coalesce(sum(metadata_bytes),0) AS bytes").Where("tenant_id = ?", access.TenantID).Scan(&budget).Error; err != nil {
			return err
		}
		if budget.Count >= 100000 || budget.Bytes < 0 || uint64(budget.Bytes) > 64<<20 || row.MetadataBytes > (64<<20)-uint64(budget.Bytes) {
			return exportjobs.ErrCapacity
		}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		event, err := store.audit.AppendInTransaction(ctx, tx, access.TenantID, audit.SuccessfulEvent{OccurredAt: job.CreatedAt, Action: audit.ActionExportCreate, TargetKind: audit.TargetKindExportJob, TargetID: job.ID, TargetVersion: job.Version})
		if err != nil {
			return err
		}
		if intent == nil {
			return nil
		}
		sequence := event.Sequence
		_, err = requestidempotency.AppendInTransaction(ctx, tx, *intent, requestidempotency.Target{Kind: "export_job", ID: job.ID, Version: job.Version}, &sequence, job.CreatedAt)
		return err
	})
	if err == nil {
		return nil
	}
	// A committed transaction wins over a lost COMMIT acknowledgement or request
	// cancellation. Only the exact newly admitted identity can authorize enqueue.
	reconcile := context.WithoutCancel(ctx)
	retained, readErr := store.Get(reconcile, access, job.ID)
	if readErr == nil && retained.Job.ID == job.ID {
		if intent == nil {
			return nil
		}
		receipt, found, receiptErr := requestidempotency.Read(reconcile, store.db, *intent)
		if receiptErr == nil && found && receipt.Target.ID == job.ID {
			return nil
		}
	}
	return err
}

func (store *Store) Update(ctx context.Context, retained exportjobs.DurableJob) error {
	row, err := encode(retained)
	if err != nil {
		return err
	}
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var previous record
		if err := tx.Where("tenant_id = ? AND owner_id = ? AND export_id = ?", row.TenantID, row.OwnerID, row.ExportID).Take(&previous).Error; err != nil {
			return err
		}
		if row.Version < previous.Version {
			return errors.New("export journal update regressed target version")
		}
		var total int64
		if err := tx.Model(&record{}).Select("coalesce(sum(metadata_bytes),0)").Where("tenant_id = ?", row.TenantID).Scan(&total).Error; err != nil {
			return err
		}
		if total < 0 || uint64(total) < previous.MetadataBytes || uint64(total)-previous.MetadataBytes > 64<<20 || row.MetadataBytes > (64<<20)-(uint64(total)-previous.MetadataBytes) {
			return exportjobs.ErrCapacity
		}
		result := tx.Model(&record{}).Where("tenant_id = ? AND owner_id = ? AND export_id = ? AND version <= ?", row.TenantID, row.OwnerID, row.ExportID, row.Version).Updates(map[string]any{"version": row.Version, "job_json": row.JobJSON, "updated_at_us": row.UpdatedAt, "expires_at_us": row.ExpiresAt, "metadata_bytes": row.MetadataBytes, "artifact_name": row.ArtifactName, "artifact_sha256": row.ArtifactSHA256})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("export journal update lost its target version")
		}
		if retained.Job.State >= exportjobs.StateCompleted {
			return requestidempotency.MarkTargetTerminalInTransaction(ctx, tx, row.TenantID, requestidempotency.TargetExportJob, row.ExportID, retained.Job.Progress.UpdatedAt)
		}
		return nil
	})
}
func (store *Store) Get(ctx context.Context, access searchjobs.AccessScope, id string) (exportjobs.DurableJob, error) {
	var row record
	err := store.db.WithContext(ctx).Where("tenant_id = ? AND owner_id = ? AND export_id = ?", access.TenantID, access.OwnerID, id).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return exportjobs.DurableJob{}, exportjobs.ErrNotFound
	}
	if err != nil {
		return exportjobs.DurableJob{}, err
	}
	retained, err := decode(row)
	if err != nil {
		return exportjobs.DurableJob{}, err
	}
	if !retained.Job.ExpiresAt.IsZero() && !retained.Job.ExpiresAt.After(store.now()) && retained.Job.State != exportjobs.StateExpired {
		retained.Job.State = exportjobs.StateExpired
		retained.Job.Version++
		retained.Job.Artifact = nil
	}
	return retained, nil
}
func (store *Store) Lookup(ctx context.Context, access searchjobs.AccessScope, intent requestidempotency.Intent) (exportjobs.Job, bool, error) {
	if intent.TenantID != access.TenantID {
		return exportjobs.Job{}, false, requestidempotency.ErrUnavailable
	}
	receipt, found, err := requestidempotency.Read(ctx, store.db, intent)
	if err != nil || !found {
		return exportjobs.Job{}, found, err
	}
	if receipt.Target.Kind != "export_job" {
		return exportjobs.Job{}, true, requestidempotency.ErrUnavailable
	}
	retained, err := store.Get(ctx, access, receipt.Target.ID)
	return retained.Job, true, err
}
func (store *Store) Restore(ctx context.Context, limit int) ([]exportjobs.DurableJob, error) {
	if limit <= 0 || limit > 100000 {
		return nil, errors.New("invalid export restore bound")
	}
	var rows []record
	if err := store.db.WithContext(ctx).Where("expires_at_us IS NULL OR expires_at_us > ?", store.now().UnixMicro()).Order("created_at_us ASC, export_id ASC").Limit(limit + 1).Find(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) > limit {
		return nil, exportjobs.ErrCapacity
	}
	result := make([]exportjobs.DurableJob, 0, len(rows))
	for _, row := range rows {
		retained, err := decode(row)
		if err != nil {
			return nil, fmt.Errorf("restore export: %w", err)
		}
		result = append(result, retained)
	}
	return result, nil
}
