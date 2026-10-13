package searchartifacts

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"sync"
	"time"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	DefaultCaptureRows              = uint64(100_000)
	DefaultCaptureBytes             = uint64(256 << 20)
	MaximumCaptureRows              = uint64(10_000_000)
	MaximumCaptureBytes             = uint64(4 << 30)
	CaptureChunkBytes               = 8 << 20
	DefaultOrganizationCaptureBytes = uint64(4 << 30)
	DefaultGlobalCaptureBytes       = uint64(16 << 30)
	maximumCaptureManifestBytes     = 2 << 20
)

// CaptureOptions separates storage/output budgets from compute and preview.
// Authorize must resolve current identity, membership and source-index grants.
type CaptureOptions struct {
	MaximumRows, MaximumBytes      uint64
	OrganizationBytes, GlobalBytes uint64
	Authorize                      func(context.Context, searchjobs.Job) error
}

func (options CaptureOptions) resolved() (CaptureOptions, error) {
	if options.MaximumRows == 0 {
		options.MaximumRows = DefaultCaptureRows
	}
	if options.MaximumBytes == 0 {
		options.MaximumBytes = DefaultCaptureBytes
	}
	if options.OrganizationBytes == 0 {
		options.OrganizationBytes = DefaultOrganizationCaptureBytes
	}
	if options.GlobalBytes == 0 {
		options.GlobalBytes = DefaultGlobalCaptureBytes
	}
	if options.MaximumRows > MaximumCaptureRows || options.MaximumBytes > MaximumCaptureBytes || options.MaximumBytes > options.OrganizationBytes || options.OrganizationBytes > options.GlobalBytes {
		return options, ErrInvalid
	}
	return options, nil
}

type captureChunk struct {
	Identity    BlobIdentity `json:"identity"`
	First, Rows uint64
	Digest      string
	Offsets     []uint32
}
type captureManifest struct {
	Version                 uint32
	JobID                   string
	Generation, Rows, Bytes uint64
	Schema                  searchjobs.Schema
	Chunks                  []captureChunk
}
type captureTransaction struct {
	store             *Store
	organization      int64
	ctx               context.Context
	cancel            context.CancelFunc
	id                string
	job               searchjobs.Job
	options           CaptureOptions
	manifest          captureManifest
	buffer            bytes.Buffer
	chunkRows         uint64
	chunkOffsets      []uint32
	uploads           sync.WaitGroup
	gate              chan struct{}
	mu                sync.Mutex
	err               error
	closed            bool
	published         bool
	preparing         bool
	prepared          bool
	preparedManifest  []byte
	preparedPayload   []byte
	preparedDigest    [32]byte
	lastAuthorization time.Time
}

// NewCaptureFactory returns the store's private full-output capture port.
func NewCaptureFactory(store *Store) (searchjobs.CaptureFactory, error) {
	if store == nil || store.blobs == nil {
		return nil, ErrInvalid
	}
	if _, err := store.captureOptions.resolved(); err != nil {
		return nil, err
	}
	return store, nil
}

func (store *Store) BeginCapture(ctx context.Context, job searchjobs.Job, compiled clickhouse.CompiledQuery, generation uint64) (searchjobs.CaptureTransaction, error) {
	if ctx == nil || store.blobs == nil || generation == 0 || !validIdentity(searchjobs.AccessScope{TenantID: job.TenantID, OwnerID: job.OwnerID}, job.ID) {
		return nil, ErrInvalid
	}
	tenant, indexes, ok := compiled.ReadScope()
	if !ok || len(indexes) == 0 || tenant != job.TenantID || !slices.Equal(indexes, job.EffectiveIndexes) {
		return nil, searchjobs.ErrInvalidResult
	}
	options, err := store.captureOptions.resolved()
	if err != nil {
		return nil, err
	}
	organization, err := strconv.ParseInt(job.OrganizationID, 10, 64)
	if err != nil || organization <= 0 || strconv.FormatInt(organization, 10) != job.OrganizationID {
		return nil, ErrInvalid
	}
	if options.Authorize != nil {
		if err := options.Authorize(ctx, job); err != nil {
			return nil, err
		}
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(random[:])
	_, err = store.db.ExecContext(ctx, `INSERT INTO search_capture_sessions (capture_id,job_id,tenant_id,owner_id,organization_id,reserved_bytes,created_at_us) VALUES ($1,$2,$3,$4,$5,0,$6)`, id, job.ID, job.TenantID, job.OwnerID, organization, toUnixMicro(store.nowUTC()))
	if err != nil {
		return nil, err
	}
	uploadCtx, cancel := context.WithCancel(ctx)
	return &captureTransaction{store: store, organization: organization, ctx: uploadCtx, cancel: cancel, id: id, job: job, options: options, manifest: captureManifest{Version: 1, JobID: job.ID, Generation: generation}, gate: make(chan struct{}, 2)}, nil
}
func (capture *captureTransaction) Limits() (uint64, uint64) {
	return capture.options.MaximumRows, capture.options.MaximumBytes
}
func (capture *captureTransaction) SetSchema(schema searchjobs.Schema) error {
	if len(capture.manifest.Schema.Columns) != 0 || capture.closed || capture.preparing {
		return ErrInvalid
	}
	if len(schema.Columns) == 0 {
		return ErrInvalid
	}
	capture.manifest.Schema = cloneSchema(schema)
	return nil
}
func (capture *captureTransaction) Append(ctx context.Context, row searchjobs.ResultRow) error {
	if capture.closed || capture.preparing || len(capture.manifest.Schema.Columns) == 0 || row.Ordinal != capture.manifest.Rows || len(row.Values) != len(capture.manifest.Schema.Columns) {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if capture.options.Authorize != nil && (capture.manifest.Rows%256 == 0 || time.Since(capture.lastAuthorization) >= time.Second) {
		if err := capture.options.Authorize(ctx, capture.job); err != nil {
			return err
		}
		capture.lastAuthorization = time.Now()
	}
	capture.mu.Lock()
	err := capture.err
	capture.mu.Unlock()
	if err != nil {
		return err
	}
	if capture.manifest.Rows >= capture.options.MaximumRows {
		return searchjobs.ErrRowLimit
	}
	stored, err := storedRow(row)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	if len(payload) > int(maximumArtifactRowBytes) {
		return searchjobs.ErrByteLimit
	}
	encodedBytes := uint64(len(payload) + 4)
	if encodedBytes > CaptureChunkBytes || encodedBytes > capture.options.MaximumBytes-capture.manifest.Bytes {
		return searchjobs.ErrByteLimit
	}
	if capture.buffer.Len() != 0 && capture.buffer.Len()+len(payload)+4 > CaptureChunkBytes {
		if err := capture.flush(ctx); err != nil {
			return err
		}
	}
	if capture.chunkRows%uint64(artifactIndexStride) == 0 {
		capture.chunkOffsets = append(capture.chunkOffsets, safecast.MustConv[uint32](capture.buffer.Len()))
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], safecast.MustConv[uint32](len(payload)))
	_, _ = capture.buffer.Write(length[:])
	_, _ = capture.buffer.Write(payload)
	capture.chunkRows++
	capture.manifest.Rows++
	capture.manifest.Bytes += encodedBytes
	if capture.buffer.Len() >= CaptureChunkBytes {
		return capture.flush(ctx)
	}
	return nil
}

func (capture *captureTransaction) flush(ctx context.Context) error {
	if capture.chunkRows == 0 {
		return nil
	}
	select {
	case capture.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	payload := capture.buffer.Bytes()
	capture.buffer = bytes.Buffer{}
	capture.mu.Lock()
	index := len(capture.manifest.Chunks)
	capture.mu.Unlock()
	key := fmt.Sprintf("open-splunk/captures/%s/%08d", capture.id, index)
	digest := sha256.Sum256(payload)
	chunk := captureChunk{Identity: BlobIdentity{Key: key, Bytes: int64(len(payload))}, First: capture.manifest.Rows - capture.chunkRows, Rows: capture.chunkRows, Digest: hex.EncodeToString(digest[:]), Offsets: capture.chunkOffsets}
	capture.chunkRows = 0
	capture.chunkOffsets = nil
	if err := capture.reserve(ctx, uint64(len(payload)), chunk.Identity); err != nil {
		<-capture.gate
		return err
	}
	capture.mu.Lock()
	capture.manifest.Chunks = append(capture.manifest.Chunks, chunk)
	capture.mu.Unlock()
	capture.uploads.Go(func() {
		defer func() { <-capture.gate }()
		identity, err := capture.store.blobs.Put(capture.ctx, key, bytes.NewReader(payload), int64(len(payload)))
		if errors.Is(err, ErrConflict) {
			// A create-only collision belongs to an existing object. Persist this
			// exclusion so abort/reconciliation never reclaim that version.
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(capture.ctx), 30*time.Second)
			_, markErr := capture.store.db.ExecContext(cleanup, `UPDATE search_capture_objects SET object_version='!conflict' WHERE capture_id=$1 AND object_key=$2`, capture.id, key)
			cancel()
			if markErr != nil {
				err = errors.Join(err, markErr)
			}
		}
		if err == nil && (identity.Key != key || identity.Bytes != int64(len(payload)) || (identity.ETag == "" && identity.Version == "")) {
			err = ErrCorrupt
		}
		if err == nil {
			_, err = capture.store.db.ExecContext(capture.ctx, `UPDATE search_capture_objects SET etag=$1,object_version=$2 WHERE capture_id=$3 AND object_key=$4`, identity.ETag, identity.Version, capture.id, key)
		}
		capture.mu.Lock()
		defer capture.mu.Unlock()
		if err != nil {
			if capture.err == nil {
				capture.err = err
			}
			capture.cancel()
			return
		}
		capture.manifest.Chunks[index].Identity = identity
	})
	return nil
}

func (capture *captureTransaction) reserve(ctx context.Context, size uint64, identity BlobIdentity) (returnedErr error) {
	tx, err := pgmeta.BeginSQLLocked(ctx, capture.store.db, pgmeta.RetainedStorage)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var global, organization uint64
	if err = tx.QueryRowContext(ctx, retainedUsageSQL, capture.organization).Scan(&global, &organization); err != nil {
		return err
	}
	if global > capture.options.GlobalBytes || organization > capture.options.OrganizationBytes || size > capture.options.GlobalBytes-global || size > capture.options.OrganizationBytes-organization {
		return errors.Join(ErrCapacity, searchjobs.ErrCapacity)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE search_capture_sessions SET reserved_bytes=reserved_bytes+$1 WHERE capture_id=$2`, size, capture.id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO search_capture_objects(capture_id,object_key,etag,object_version,size_bytes) VALUES($1,$2,'','',$3)`, capture.id, identity.Key, identity.Bytes); err != nil {
		return err
	}
	return tx.Commit()
}

// Authorization sees an active private execution until the manifest commit.
// A completed candidate must not bypass revocation of privileges used at admission.
func (capture *captureTransaction) authorizePublication(ctx context.Context, job searchjobs.Job) error {
	if capture.options.Authorize == nil {
		return nil
	}
	job.State = searchjobs.StateRunning
	job.CaptureComplete = false
	return capture.options.Authorize(ctx, job)
}

func (capture *captureTransaction) PreparePublication(ctx context.Context, job searchjobs.Job) error {
	if capture.closed || job.ID != capture.job.ID || job.OwnerID != capture.job.OwnerID || job.TenantID != capture.job.TenantID || job.State != searchjobs.StateCompleted || !job.CaptureComplete {
		return ErrInvalid
	}
	if capture.prepared {
		payload, err := encodeJob(job)
		if err != nil {
			return err
		}
		if !bytes.Equal(payload, capture.preparedPayload) {
			return ErrInvalid
		}
		return nil
	}
	if capture.preparing {
		return ErrConflict
	}
	capture.preparing = true
	if capture.options.Authorize != nil {
		if err := capture.authorizePublication(ctx, job); err != nil {
			return err
		}
	}
	if err := capture.flush(ctx); err != nil {
		return err
	}
	uploadDone := make(chan struct{})
	go func() { capture.uploads.Wait(); close(uploadDone) }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
waitUploads:
	for {
		select {
		case <-uploadDone:
			break waitUploads
		case <-ctx.Done():
			capture.cancel()
			return ctx.Err()
		case <-ticker.C:
			if capture.options.Authorize != nil {
				if err := capture.authorizePublication(ctx, job); err != nil {
					capture.cancel()
					return err
				}
			}
		}
	}
	capture.mu.Lock()
	err := capture.err
	capture.mu.Unlock()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if job.OrganizationID != capture.job.OrganizationID {
		return ErrInvalid
	}
	lifetime := job.ExpiresAt.Sub(job.FinishedAt)
	if !validLifetime(lifetime) || job.RowCount != capture.manifest.Rows || job.Schema == nil {
		return ErrInvalid
	}
	publishedSchema, schemaErr := json.Marshal(job.Schema)
	capturedSchema, captureSchemaErr := json.Marshal(capture.manifest.Schema)
	if schemaErr != nil || captureSchemaErr != nil || !bytes.Equal(publishedSchema, capturedSchema) {
		return ErrInvalid
	}

	if capture.options.Authorize != nil {
		if err := capture.authorizePublication(ctx, job); err != nil {
			return err
		}
	}
	manifest, err := json.Marshal(capture.manifest)
	if err != nil {
		return err
	}
	if len(manifest) > maximumCaptureManifestBytes {
		return errors.Join(ErrCapacity, searchjobs.ErrCapacity)
	}
	digest := sha256.Sum256(manifest)
	payload, err := encodeJob(job)
	if err != nil {
		return err
	}
	capture.preparedManifest = manifest
	capture.preparedPayload = payload
	capture.preparedDigest = digest
	capture.prepared = true
	return nil
}

// Publish performs only the fenced PostgreSQL commit once private objects and
// the immutable manifest have been prepared. It rechecks current authority.
func (capture *captureTransaction) Publish(ctx context.Context, job searchjobs.Job) error {
	if err := capture.PreparePublication(ctx, job); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if capture.options.Authorize != nil {
		if err := capture.authorizePublication(ctx, job); err != nil {
			return err
		}
	}
	manifest, payload, digest := capture.preparedManifest, capture.preparedPayload, capture.preparedDigest
	lifetime := job.ExpiresAt.Sub(job.FinishedAt)
	tx, err := pgmeta.BeginSQLLocked(ctx, capture.store.db, pgmeta.RetainedStorage)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var reclaiming bool
	if err := tx.QueryRowContext(ctx, `SELECT reclaiming FROM search_capture_sessions WHERE capture_id=$1`, capture.id).Scan(&reclaiming); err != nil {
		return err
	}
	if reclaiming {
		return ErrConflict
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO search_capture_manifests(job_id,capture_id,manifest,sha256,size_bytes) VALUES($1,$2,$3,$4,$5)`, job.ID, capture.id, manifest, digest[:], capture.manifest.Bytes); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE durable_search_jobs SET state=$1,artifact_name=$2,artifact_sha256=$3,artifact_size_bytes=$4,job_payload=$5,version=$6,finished_at_us=$7,expires_at_us=$8,lifetime_ns=$16 WHERE id=$9 AND tenant_id=$10 AND owner_id=$11 AND artifact_name IS NULL AND state IN ($12,$13,$14,$15)`, StateCompleted, artifactName(job.ID), digest[:], capture.manifest.Bytes, payload, job.Version, toUnixMicro(job.FinishedAt), toUnixMicro(job.ExpiresAt), job.ID, job.TenantID, job.OwnerID, StateQueued, StateParsing, StatePlanning, StateRunning, int64(lifetime))
	if err != nil {
		return err
	}
	if err = requireChanged(result); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	capture.closed = true
	capture.published = true
	capture.cancel()
	capture.store.mu.Lock()
	capture.store.artifactBytes += capture.manifest.Bytes
	capture.store.mu.Unlock()
	return nil
}

func (capture *captureTransaction) Abort(ctx context.Context) error {
	if capture.published {
		return nil
	}
	capture.closed = true
	capture.cancel()
	capture.uploads.Wait()
	return capture.store.deleteCapture(ctx, capture.id, false)
}

func (store *Store) deleteCapture(ctx context.Context, id string, allowPublished bool) error {
	claim, err := store.claimCaptureReclamation(ctx, id, allowPublished)
	if err != nil {
		return err
	}
	if !claim {
		return nil
	}
	rows, err := store.db.QueryContext(ctx, `SELECT object_key,etag,object_version,size_bytes FROM search_capture_objects WHERE capture_id=$1`, id)
	if err != nil {
		return err
	}
	var objects []BlobIdentity
	for rows.Next() {
		var object BlobIdentity
		if err := rows.Scan(&object.Key, &object.ETag, &object.Version, &object.Bytes); err != nil {
			_ = rows.Close()
			return err
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, object := range objects {
		if object.Version == "!conflict" {
			continue
		}
		if err := store.blobs.Delete(ctx, object); err != nil {
			return err
		}
	}
	tx, err := pgmeta.BeginSQLLocked(ctx, store.db, pgmeta.RetainedStorage)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `DELETE FROM search_capture_manifests WHERE capture_id=$1`, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM search_capture_objects WHERE capture_id=$1`, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM search_capture_sessions WHERE capture_id=$1`, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) AcquireCapture(ctx context.Context, access searchjobs.AccessScope, jobID string) (searchjobs.ResultLease, error) {
	return store.acquireCapture(ctx, access, jobID, nil)
}

func (store *Store) AcquireCaptureBounded(ctx context.Context, access searchjobs.AccessScope, jobID string, reserve func(uint64) (func(), bool)) (searchjobs.ResultLease, error) {
	return store.acquireCapture(ctx, access, jobID, reserve)
}
func (store *Store) acquireCapture(ctx context.Context, access searchjobs.AccessScope, jobID string, reserve func(uint64) (func(), bool)) (searchjobs.ResultLease, error) {
	if ctx == nil || !validIdentity(access, jobID) {
		return nil, ErrInvalid
	}
	record, err := store.Get(ctx, access, jobID, AccessRefresh)
	if err != nil {
		return nil, err
	}
	if record.State != StateCompleted || !record.Job.CaptureComplete {
		return nil, ErrNotReady
	}
	if store.captureOptions.Authorize != nil {
		if err := store.captureOptions.Authorize(ctx, record.Job); err != nil {
			return nil, err
		}
	}
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return nil, ErrClosed
	}
	current, err := store.getLocked(ctx, access, jobID, AccessRefresh)
	if err != nil || current.State != StateCompleted {
		store.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, ErrNotReady
	}
	store.pins[jobID]++
	store.mu.Unlock()
	owned := true
	defer func() {
		if owned {
			store.releasePin(jobID)
		}
	}()
	var payload, digest []byte
	if err := store.db.QueryRowContext(ctx, `SELECT manifest,sha256 FROM search_capture_manifests WHERE job_id=$1`, jobID).Scan(&payload, &digest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotReady
		}
		return nil, err
	}
	if len(payload) > maximumCaptureManifestBytes {
		return nil, ErrCorrupt
	}
	actual := sha256.Sum256(payload)
	if !bytes.Equal(actual[:], digest) {
		return nil, ErrCorrupt
	}
	releaseMetadata := func() {}
	if reserve != nil {
		charge, valid := boundedJSONDecodeBytes(uint64(len(payload)), boundedRowFixedBytes)
		if !valid {
			return nil, ErrCapacity
		}
		var reserved bool
		releaseMetadata, reserved = reserve(charge)
		if !reserved || releaseMetadata == nil {
			return nil, ErrCapacity
		}
	}
	defer func() {
		if owned {
			releaseMetadata()
		}
	}()
	var manifest captureManifest
	if err := decodeExactJSON(payload, &manifest); err != nil {
		return nil, ErrCorrupt
	}
	if manifest.Version != 1 || manifest.JobID != jobID || manifest.Generation == 0 || manifest.Rows != record.Job.RowCount || manifest.Rows > MaximumCaptureRows || manifest.Bytes > MaximumCaptureBytes {
		return nil, ErrCorrupt
	}
	var expected, encodedBytes uint64
	for _, chunk := range manifest.Chunks {
		if chunk.First != expected || chunk.Rows == 0 || chunk.Identity.Bytes <= 0 || chunk.Identity.Bytes > CaptureChunkBytes || len(chunk.Digest) != sha256.Size*2 || uint64(len(chunk.Offsets)) != artifactIndexCount(chunk.Rows) {
			return nil, ErrCorrupt
		}
		if chunk.Offsets[0] != 0 {
			return nil, ErrCorrupt
		}
		for index, offset := range chunk.Offsets {
			if uint64(offset)+4 > uint64(chunk.Identity.Bytes) || (index > 0 && offset <= chunk.Offsets[index-1]) {
				return nil, ErrCorrupt
			}
		}
		if _, err := hex.DecodeString(chunk.Digest); err != nil {
			return nil, ErrCorrupt
		}
		expected += chunk.Rows
		encodedBytes += uint64(chunk.Identity.Bytes)
	}
	if expected != manifest.Rows || encodedBytes != manifest.Bytes {
		return nil, ErrCorrupt
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	lease := &captureLease{releaseMetadata: releaseMetadata, store: store, jobID: jobID, manifest: manifest, reserve: reserve, ctx: leaseCtx, cancel: cancel, job: record.Job}
	stop := context.AfterFunc(ctx, func() { _ = lease.Close() })
	lease.mu.Lock()
	if lease.closed {
		lease.mu.Unlock()
		stop()
	} else {
		lease.stop = stop
		lease.mu.Unlock()
	}
	owned = false
	return lease, nil
}

// ReconcileCaptures removes uncommitted uploads after restart. Committed
// manifests remain SQL-owned; object listing is never publication authority.
func (store *Store) ReconcileCaptures(ctx context.Context) error {
	if store.blobs == nil {
		return nil
	}
	rows, err := store.db.QueryContext(ctx, `SELECT s.capture_id FROM search_capture_sessions s LEFT JOIN search_capture_manifests m ON m.capture_id=s.capture_id WHERE m.capture_id IS NULL`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := store.deleteCapture(ctx, id, false); err != nil {
			return err
		}
	}
	return nil
}

type captureLease struct {
	job               searchjobs.Job
	lastAuthorization time.Time
	ctx               context.Context
	cancel            context.CancelFunc
	store             *Store
	jobID             string
	manifest          captureManifest
	reserve           func(uint64) (func(), bool)
	mu                sync.Mutex
	next              uint64
	chunkIndex        int
	chunk             []byte
	position          int
	releaseChunk      func()
	releaseMetadata   func()
	closed            bool
	stop              func() bool
}

func (lease *captureLease) Schema() searchjobs.Schema { return cloneSchema(lease.manifest.Schema) }
func (lease *captureLease) RowCount() uint64          { return lease.manifest.Rows }
func (*captureLease) RowCountExact() bool             { return true }
func (*captureLease) ResultsTruncated() bool          { return false }
func (lease *captureLease) Generation() uint64        { return lease.manifest.Generation }
func (lease *captureLease) BoundedRead() bool         { return lease.reserve != nil }
func (lease *captureLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	row, present, _, release, err := lease.NextBounded(ctx)
	if release != nil {
		release()
	}
	return row, present, err
}
func (lease *captureLease) NextBounded(ctx context.Context) (searchjobs.ResultRow, bool, uint64, func(), error) {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return searchjobs.ResultRow{}, false, 0, nil, searchjobs.ErrResultLeaseClosed
	}
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, 0, nil, err
	}
	if lease.store.captureOptions.Authorize != nil && (lease.next%256 == 0 || time.Since(lease.lastAuthorization) >= time.Second) {
		if err := lease.store.captureOptions.Authorize(lease.ctx, lease.job); err != nil {
			return searchjobs.ResultRow{}, false, 0, nil, err
		}
		lease.lastAuthorization = time.Now()
	}
	if lease.next >= lease.manifest.Rows {
		return searchjobs.ResultRow{}, false, 0, nil, nil
	}
	if err := lease.positionRow(ctx, lease.next); err != nil {
		return searchjobs.ResultRow{}, false, 0, nil, err
	}
	if len(lease.chunk)-lease.position < 4 {
		return searchjobs.ResultRow{}, false, 0, nil, ErrCorrupt
	}
	length := int(binary.BigEndian.Uint32(lease.chunk[lease.position:]))
	lease.position += 4
	if length <= 0 || length > int(maximumArtifactRowBytes) || length > len(lease.chunk)-lease.position {
		return searchjobs.ResultRow{}, false, 0, nil, ErrCorrupt
	}
	charge, valid := boundedJSONDecodeBytes(uint64(length), boundedRowFixedBytes)
	if !valid {
		return searchjobs.ResultRow{}, false, 0, nil, ErrCapacity
	}
	release := func() {}
	if lease.reserve != nil {
		var ok bool
		release, ok = lease.reserve(charge)
		if !ok || release == nil {
			return searchjobs.ResultRow{}, false, 0, nil, ErrCapacity
		}
	}
	var stored storedResultRow
	if err := decodeExactJSON(lease.chunk[lease.position:lease.position+length], &stored); err != nil {
		release()
		return searchjobs.ResultRow{}, false, 0, nil, ErrCorrupt
	}
	row, err := restoreRow(stored)
	if err != nil || row.Ordinal != lease.next || len(row.Values) != len(lease.manifest.Schema.Columns) {
		release()
		return searchjobs.ResultRow{}, false, 0, nil, ErrCorrupt
	}
	lease.position += length
	lease.next++
	return row, true, charge, release, nil
}
func (lease *captureLease) positionRow(ctx context.Context, offset uint64) error {
	readContext, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lease.ctx, cancel)
	defer cancel()
	defer stop()
	ctx = readContext
	index := 0
	for index < len(lease.manifest.Chunks) && offset >= lease.manifest.Chunks[index].First+lease.manifest.Chunks[index].Rows {
		index++
	}
	if index >= len(lease.manifest.Chunks) {
		return ErrCorrupt
	}
	if lease.chunk != nil && lease.chunkIndex == index {
		return nil
	}
	if lease.releaseChunk != nil {
		lease.releaseChunk()
		lease.releaseChunk = nil
	}
	lease.chunk = nil
	chunk := lease.manifest.Chunks[index]
	if lease.reserve != nil {
		release, ok := lease.reserve(safecast.MustConv[uint64](chunk.Identity.Bytes) * 2)
		if !ok || release == nil {
			return ErrCapacity
		}
		lease.releaseChunk = release
	}
	reader, err := lease.store.blobs.Open(ctx, chunk.Identity)
	if err != nil {
		return err
	}
	payload, readErr := io.ReadAll(io.LimitReader(reader, chunk.Identity.Bytes+1))
	closeErr := reader.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	digest := sha256.Sum256(payload)
	if int64(len(payload)) != chunk.Identity.Bytes || hex.EncodeToString(digest[:]) != chunk.Digest {
		return ErrCorrupt
	}
	lease.chunk = payload
	lease.chunkIndex = index
	lease.position = 0
	return nil
}
func (lease *captureLease) Seek(ctx context.Context, offset uint64) error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return searchjobs.ErrResultLeaseClosed
	}
	if lease.store.captureOptions.Authorize != nil {
		if err := lease.store.captureOptions.Authorize(lease.ctx, lease.job); err != nil {
			return err
		}
		lease.lastAuthorization = time.Now()
	}
	if offset > lease.manifest.Rows {
		return ErrInvalid
	}
	if offset == lease.manifest.Rows {
		lease.next = offset
		return nil
	}
	if err := lease.positionRow(ctx, offset); err != nil {
		return err
	}
	chunk := lease.manifest.Chunks[lease.chunkIndex]
	relative := offset - chunk.First
	checkpoint := relative / uint64(artifactIndexStride)
	if checkpoint >= uint64(len(chunk.Offsets)) {
		return ErrCorrupt
	}
	lease.position = int(chunk.Offsets[checkpoint])
	for row := checkpoint * uint64(artifactIndexStride); row < relative; row++ {
		if len(lease.chunk)-lease.position < 4 {
			return ErrCorrupt
		}
		length := int(binary.BigEndian.Uint32(lease.chunk[lease.position:]))
		if length <= 0 || length > len(lease.chunk)-lease.position-4 {
			return ErrCorrupt
		}
		lease.position += 4 + length
	}
	lease.next = offset
	return nil
}
func (lease *captureLease) Close() error {
	lease.cancel()
	lease.mu.Lock()
	if lease.closed {
		lease.mu.Unlock()
		return nil
	}
	lease.closed = true
	if lease.stop != nil {
		lease.stop()
	}
	if lease.releaseChunk != nil {
		lease.releaseChunk()
		lease.releaseChunk = nil
	}
	lease.chunk = nil
	if lease.releaseMetadata != nil {
		lease.releaseMetadata()
		lease.releaseMetadata = nil
	}
	lease.mu.Unlock()
	lease.store.releasePin(lease.jobID)
	return nil
}

var _ searchjobs.CaptureFactory = (*Store)(nil)
var _ searchjobs.ResultLease = (*captureLease)(nil)

// Claim before external deletion. Publication uses the same SQL write lock;
// an uncertain COMMIT cannot be mistaken for an unpublished upload, and no
// future writer can publish a session already being reclaimed.
func (store *Store) claimCaptureReclamation(ctx context.Context, id string, allowPublished bool) (bool, error) {
	tx, err := pgmeta.BeginSQLLocked(ctx, store.db, pgmeta.RetainedStorage)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var published bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM search_capture_manifests WHERE capture_id=$1)`, id).Scan(&published); err != nil {
		return false, err
	}
	if published && !allowPublished {
		return false, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE search_capture_sessions SET reclaiming=TRUE WHERE capture_id=$1`, id)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return count == 1, nil
}
