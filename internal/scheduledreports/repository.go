package scheduledreports

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"fortio.org/safecast"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const maximumIDAttempts = 4

type scheduleRecord struct {
	OrganizationID     int64  `gorm:"column:organization_id"`
	SavedSearchID      string `gorm:"column:saved_search_id"`
	OwnerID            string `gorm:"column:owner_id"`
	TenantID           string `gorm:"column:tenant_id"`
	ConfigVersion      int64  `gorm:"column:config_version"`
	RuntimeVersion     int64  `gorm:"column:runtime_version"`
	CronExpression     string `gorm:"column:cron_expression"`
	Timezone           string `gorm:"column:timezone"`
	DispatchTTL        string `gorm:"column:dispatch_ttl"`
	Enabled            bool   `gorm:"column:enabled"`
	NextRunAtUnixMicro *int64 `gorm:"column:next_run_at_unix_micro"`
	CreatedAtUnixMicro int64  `gorm:"column:created_at_unix_micro"`
	UpdatedAtUnixMicro int64  `gorm:"column:updated_at_unix_micro"`
}

func (scheduleRecord) TableName() string { return "saved_search_schedules" }

type runRecord struct {
	OrganizationID                int64   `gorm:"column:organization_id"`
	RunID                         string  `gorm:"column:run_id"`
	SavedSearchID                 string  `gorm:"column:saved_search_id"`
	OwnerID                       string  `gorm:"column:owner_id"`
	TenantID                      string  `gorm:"column:tenant_id"`
	DefinitionVersion             int64   `gorm:"column:definition_version"`
	DefinitionProto               []byte  `gorm:"column:definition_proto"`
	CronExpression                string  `gorm:"column:cron_expression"`
	Timezone                      string  `gorm:"column:timezone"`
	DispatchTTL                   string  `gorm:"column:dispatch_ttl"`
	SchedulePeriodMicroseconds    int64   `gorm:"column:schedule_period_microseconds"`
	RetentionLifetimeMicroseconds int64   `gorm:"column:retention_lifetime_microseconds"`
	ScheduledAtUnixMicro          int64   `gorm:"column:scheduled_at_unix_micro"`
	ClaimedAtUnixMicro            int64   `gorm:"column:claimed_at_unix_micro"`
	SkippedOccurrenceCount        int64   `gorm:"column:skipped_occurrence_count"`
	Outcome                       string  `gorm:"column:outcome"`
	SearchJobID                   *string `gorm:"column:search_job_id"`
	FailureCategory               *string `gorm:"column:failure_category"`
	FinishedAtUnixMicro           *int64  `gorm:"column:finished_at_unix_micro"`
}

func (runRecord) TableName() string { return "saved_search_schedule_runs" }

// RepositoryOptions contains injectable process dependencies.
type RepositoryOptions struct {
	ResolveSourceScope func(context.Context, AccessScope) (AccessScope, error)
	Clock              func() time.Time
	IDGenerator        func() (string, error)
	CursorKey          []byte
}

// Repository persists schedules and immutable occurrence snapshots in the
// control database. Versioned SQL migrations, not GORM, own the schema.
type Repository struct {
	resolveSourceScope func(context.Context, AccessScope) (AccessScope, error)
	orm                *gorm.DB
	clock              func() time.Time
	idGenerator        func() (string, error)
	cursorKey          []byte
}

// NewRepository constructs a repository over an already configured control
// database handle.
func NewRepository(orm *gorm.DB, options RepositoryOptions) (*Repository, error) {
	if orm == nil {
		return nil, fmt.Errorf("%w: database is required", ErrInvalidArgument)
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	idGenerator := options.IDGenerator
	if idGenerator == nil {
		idGenerator = newRunID
	}
	cursorKey := slices.Clone(options.CursorKey)
	if len(cursorKey) == 0 {
		cursorKey = make([]byte, 32)
		if _, err := rand.Read(cursorKey); err != nil {
			return nil, fmt.Errorf("create scheduled-report cursor key: %w", err)
		}
	}
	if len(cursorKey) < 32 {
		return nil, fmt.Errorf("%w: cursor key must contain at least 32 bytes", ErrInvalidArgument)
	}
	return &Repository{orm: orm, clock: clock, idGenerator: idGenerator, cursorKey: cursorKey, resolveSourceScope: options.ResolveSourceScope}, nil
}

// Configure creates or replaces one schedule under optimistic concurrency.
// expectedVersion must be zero for creation and the current config version for
// replacement. The saved search ownership check occurs in the same transaction.
func (repository *Repository) Configure(
	ctx context.Context,
	scope AccessScope, tenantID, savedSearchID string,
	expectedVersion uint64,
	configuration Configuration,
	nextRunAt *time.Time,
) (Schedule, error) {
	if scope.SiteAdministrator && scope.OrganizationID == "" && expectedVersion > 0 {
		current, err := repository.Get(ctx, scope, savedSearchID)
		if err != nil {
			return Schedule{}, err
		}
		scope.OrganizationID = current.OrganizationID
	}
	sourceScope, scopeErr := repository.sourceScope(ctx, scope)
	if scopeErr != nil {
		return Schedule{}, scopeErr
	}

	ownerID := scope.OwnerID

	if err := validateIdentity(ownerID, tenantID, savedSearchID); err != nil {
		return Schedule{}, err
	}
	now, err := normalizedTime(repository.clock())
	if err != nil {
		return Schedule{}, err
	}
	tx := pgmeta.BeginLocked(repository.orm.WithContext(ctx), pgmeta.Catalog)
	if tx.Error != nil {
		return Schedule{}, fmt.Errorf("begin schedule configuration: %w", tx.Error)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback().Error
		}
	}()

	if err := validateScope(scope); err != nil {
		return Schedule{}, err
	}
	if scope.OrganizationID == "" {
		return Schedule{}, ErrInvalidArgument
	}
	if _, err := savedobjects.GetInTransaction(ctx, tx, sourceScope, savedSearchID); err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return Schedule{}, ErrNotFound
		}
		return Schedule{}, err
	}
	nextMicro := timePointerToUnixMicro(nextRunAt)
	if expectedVersion == 0 {
		record := scheduleRecord{
			SavedSearchID: savedSearchID, OwnerID: ownerID, TenantID: tenantID, OrganizationID: scopeOrganization(scope),
			ConfigVersion: 1, RuntimeVersion: 1, CronExpression: configuration.Cron,
			Timezone: configuration.Timezone, DispatchTTL: configuration.DispatchTTL,
			Enabled: configuration.Enabled, NextRunAtUnixMicro: nextMicro,
			CreatedAtUnixMicro: now.UnixMicro(), UpdatedAtUnixMicro: now.UnixMicro(),
		}
		create := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&record)
		if create.Error != nil {
			return Schedule{}, fmt.Errorf("create saved-search schedule: %w", create.Error)
		}
		if create.RowsAffected != 1 {
			return Schedule{}, ErrConflict
		}

	} else {
		if expectedVersion > math.MaxInt64 {
			return Schedule{}, ErrConflict
		}
		result := scopedScheduleQuery(tx.Model(&scheduleRecord{}), scope).
			Where("saved_search_id = ? AND TRUE AND config_version = ?", savedSearchID, expectedVersion).
			Updates(map[string]any{
				"config_version":         gorm.Expr("config_version + 1"),
				"runtime_version":        gorm.Expr("runtime_version + 1"),
				"tenant_id":              tenantID,
				"cron_expression":        configuration.Cron,
				"timezone":               configuration.Timezone,
				"dispatch_ttl":           configuration.DispatchTTL,
				"enabled":                configuration.Enabled,
				"next_run_at_unix_micro": nextMicro,
				"updated_at_unix_micro":  now.UnixMicro(),
			})
		if result.Error != nil {
			return Schedule{}, fmt.Errorf("update saved-search schedule: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			return Schedule{}, ErrConflict
		}
	}
	var record scheduleRecord
	if err := scopedScheduleQuery(tx, scope).Where("saved_search_id = ? AND TRUE", savedSearchID).Take(&record).Error; err != nil {
		return Schedule{}, fmt.Errorf("read configured schedule: %w", err)
	}
	if err := tx.Commit().Error; err != nil {
		return Schedule{}, fmt.Errorf("commit schedule configuration: %w", err)
	}
	committed = true
	return scheduleFromRecord(record)
}

// Get returns an owner-scoped detached schedule.
func (repository *Repository) Get(ctx context.Context, scope AccessScope, savedSearchID string) (Schedule, error) {
	ownerID := scope.OwnerID

	if strings.TrimSpace(ownerID) == "" || strings.TrimSpace(savedSearchID) == "" {
		return Schedule{}, ErrInvalidArgument
	}
	if err := repository.authorizeMetadataSource(ctx, scope, savedSearchID); err != nil {
		return Schedule{}, err
	}
	var record scheduleRecord
	err := scopedScheduleQuery(repository.orm.WithContext(ctx), scope).
		Where("saved_search_id = ? AND TRUE", savedSearchID).
		Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Schedule{}, ErrNotFound
	}
	if err != nil {
		return Schedule{}, fmt.Errorf("get saved-search schedule: %w", err)
	}
	return scheduleFromRecord(record)
}

// ListCurrentProjections reads a bounded set of schedules, each schedule's
// newest occurrence, and its newest submitted or succeeded result-bearing
// occurrence in two queries. The run query returns at most two rows per
// schedule. Missing IDs are omitted because unscheduled saved searches have no
// projection row.
func (repository *Repository) ListCurrentProjections(
	ctx context.Context,
	scope AccessScope,
	savedSearchIDs []string,
) (map[string]CurrentProjection, error) {
	ownerID := scope.OwnerID

	if strings.TrimSpace(ownerID) == "" || len(savedSearchIDs) > MaximumProjectionBatch {
		return nil, ErrInvalidArgument
	}
	ids := make([]string, 0, len(savedSearchIDs))
	seen := make(map[string]struct{}, len(savedSearchIDs))
	for _, id := range savedSearchIDs {
		if strings.TrimSpace(id) == "" || len(id) > 255 || strings.IndexByte(id, 0) >= 0 {
			return nil, ErrInvalidArgument
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	result := make(map[string]CurrentProjection, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	var schedules []scheduleRecord
	if err := repository.visibleMetadataSources(ctx, scopedScheduleQuery(repository.orm.WithContext(ctx), scope), scope).
		Where("TRUE AND saved_search_id IN ?", ids).
		Find(&schedules).Error; err != nil {
		return nil, fmt.Errorf("list current saved-search schedules: %w", err)
	}
	for _, record := range schedules {
		schedule, err := scheduleFromRecord(record)
		if err != nil {
			return nil, err
		}
		result[schedule.SavedSearchID] = CurrentProjection{Schedule: schedule}
	}
	if len(result) == 0 {
		return result, nil
	}
	ids = ids[:0]
	for id := range result {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var runs []runRecord
	organizationID, _ := strconv.ParseInt(scope.OrganizationID, 10, 64)
	if err := repository.orm.WithContext(ctx).Raw(`
		SELECT candidate.*
		FROM saved_search_schedule_runs AS candidate
		WHERE (candidate.owner_id = ? OR ?)
 AND (candidate.organization_id = ? OR ?)
		  AND candidate.saved_search_id IN ?
		  AND (
			NOT EXISTS (
			  SELECT 1
			  FROM saved_search_schedule_runs AS newer
			  WHERE newer.owner_id = candidate.owner_id AND newer.organization_id = candidate.organization_id
				AND newer.saved_search_id = candidate.saved_search_id
				AND (
				  newer.claimed_at_unix_micro > candidate.claimed_at_unix_micro
				  OR (
					newer.claimed_at_unix_micro = candidate.claimed_at_unix_micro
					AND newer.run_id > candidate.run_id
				  )
				)
			)
			OR (
			  candidate.search_job_id IS NOT NULL
			  AND candidate.search_job_id <> ''
			  AND candidate.outcome IN ('submitted', 'succeeded')
			  AND NOT EXISTS (
				SELECT 1
				FROM saved_search_schedule_runs AS newer_result
				WHERE newer_result.owner_id = candidate.owner_id AND newer_result.organization_id = candidate.organization_id
				  AND newer_result.saved_search_id = candidate.saved_search_id
				  AND newer_result.search_job_id IS NOT NULL
				  AND newer_result.search_job_id <> ''
				  AND newer_result.outcome IN ('submitted', 'succeeded')
				  AND (
					newer_result.claimed_at_unix_micro > candidate.claimed_at_unix_micro
					OR (
					  newer_result.claimed_at_unix_micro = candidate.claimed_at_unix_micro
					  AND newer_result.run_id > candidate.run_id
					)
				  )
			  )
			)
		  )`, ownerID, scope.SiteAdministrator, organizationID, scope.SiteAdministrator && scope.OrganizationID == "", ids).Scan(&runs).Error; err != nil {
		return nil, fmt.Errorf("list latest scheduled-report runs: %w", err)
	}
	for _, record := range runs {
		projection, exists := result[record.SavedSearchID]
		if !exists {
			continue
		}
		run, err := runFromRecord(record)
		if err != nil {
			return nil, err
		}
		if projection.LatestRun == nil || runNewerThan(run, *projection.LatestRun) {
			projection.LatestRun = &run
		}
		if run.resultBearing() && (projection.LatestResultRun == nil || runNewerThan(run, *projection.LatestResultRun)) {
			projection.LatestResultRun = &run
		}
		result[record.SavedSearchID] = projection
	}
	return result, nil
}

func (run Run) resultBearing() bool {
	return run.SearchJobID != "" && (run.Outcome == RunOutcomeSubmitted || run.Outcome == RunOutcomeSucceeded)
}

func runNewerThan(candidate, current Run) bool {
	return candidate.ClaimedAt.After(current.ClaimedAt) ||
		(candidate.ClaimedAt.Equal(current.ClaimedAt) && candidate.RunID > current.RunID)
}

// ListDue returns a bounded snapshot ordered by occurrence and ID. It does not
// mutate schedule state; ClaimDue performs the compare-and-swap.
func (repository *Repository) ListDue(ctx context.Context, now time.Time, limit int) ([]Schedule, error) {
	if limit <= 0 {
		limit = DefaultClaimLimit
	}
	if limit > MaximumClaimLimit {
		return nil, ErrInvalidArgument
	}
	now, err := normalizedTime(now)
	if err != nil {
		return nil, err
	}
	var records []scheduleRecord
	if err := repository.orm.WithContext(ctx).
		Where("enabled = ? AND next_run_at_unix_micro IS NOT NULL AND next_run_at_unix_micro <= ?", true, now.UnixMicro()).
		Order("next_run_at_unix_micro ASC, saved_search_id ASC").
		Limit(limit).
		Find(&records).Error; err != nil {
		return nil, fmt.Errorf("list due saved-search schedules: %w", err)
	}
	result := make([]Schedule, 0, len(records))
	for _, record := range records {
		schedule, err := scheduleFromRecord(record)
		if err != nil {
			return nil, err
		}
		result = append(result, schedule)
	}
	return result, nil
}

// ClaimDue advances the schedule and snapshots the current saved-search
// definition in one transaction. claimed is false when another worker won.
func (repository *Repository) ClaimDue(
	ctx context.Context,
	candidate Schedule,
	scheduledAt, nextRunAt, claimedAt time.Time,
	skipped uint64,
	period, retention time.Duration,
) (run Run, claimed bool, returnedErr error) {
	if candidate.NextRunAt == nil || candidate.RuntimeVersion == 0 || skipped > math.MaxInt64 {
		return Run{}, false, ErrInvalidArgument
	}
	return repository.claim(ctx, candidate, scheduledAt, &nextRunAt, claimedAt, skipped, period, retention, true, true)
}

// ClaimRunNow snapshots an immediate operator-triggered run without moving the
// persisted cron cursor.
func (repository *Repository) ClaimRunNow(
	ctx context.Context,
	candidate Schedule,
	claimedAt time.Time,
	period, retention time.Duration,
) (Run, bool, error) {
	return repository.claim(ctx, candidate, claimedAt, nil, claimedAt, 0, period, retention, false, true)
}

// ClaimOneOff snapshots a saved search that has no persisted schedule. It
// records explicit schedule metadata for retention/history without creating a
// future schedule.
func (repository *Repository) ClaimOneOff(
	ctx context.Context,
	scope AccessScope, tenantID, savedSearchID string,
	claimedAt time.Time,
	period, retention time.Duration,
) (Run, bool, error) {
	ownerID := scope.OwnerID

	if err := validateIdentity(ownerID, tenantID, savedSearchID); err != nil {
		return Run{}, false, err
	}
	candidate := Schedule{
		SavedSearchID: savedSearchID,
		OwnerID:       ownerID,
		TenantID:      tenantID, OrganizationID: scope.OrganizationID,
		Cron:        DefaultOneOffCron,
		Timezone:    DefaultOneOffTimezone,
		DispatchTTL: DefaultOneOffDispatchTTL,
	}
	return repository.claim(ctx, candidate, claimedAt, nil, claimedAt, 0, period, retention, false, false)
}

func (repository *Repository) claim(
	ctx context.Context,
	candidate Schedule,
	scheduledAt time.Time,
	nextRunAt *time.Time,
	claimedAt time.Time,
	skipped uint64,
	period, retention time.Duration,
	advance, requireSchedule bool,
) (run Run, claimed bool, returnedErr error) {
	sourceScope, scopeErr := repository.sourceScope(ctx, AccessScope{OwnerID: candidate.OwnerID, OrganizationID: candidate.OrganizationID})
	if scopeErr != nil {
		return Run{}, false, scopeErr
	}

	if period <= 0 || retention <= 0 || candidate.RuntimeVersion > math.MaxInt64 {
		return Run{}, false, ErrInvalidArgument
	}
	scheduledAt, err := normalizedTime(scheduledAt)
	if err != nil {
		return Run{}, false, err
	}
	claimedAt, err = normalizedTime(claimedAt)
	if err != nil {
		return Run{}, false, err
	}
	if period.Microseconds() <= 0 || retention.Microseconds() <= 0 {
		return Run{}, false, ErrInvalidArgument
	}
	runID, err := repository.generateID()
	if err != nil {
		return Run{}, false, err
	}
	tx := pgmeta.BeginLocked(repository.orm.WithContext(ctx), pgmeta.Catalog)
	if tx.Error != nil {
		return Run{}, false, fmt.Errorf("begin scheduled-report claim: %w", tx.Error)
	}
	defer func() {
		if returnedErr != nil || !claimed {
			_ = tx.Rollback().Error
		}
	}()
	if advance {
		result := tx.Model(&scheduleRecord{}).
			Where("saved_search_id = ? AND owner_id = ? AND enabled = ? AND runtime_version = ? AND next_run_at_unix_micro = ?",
				candidate.SavedSearchID, candidate.OwnerID, true, candidate.RuntimeVersion, candidate.NextRunAt.UTC().UnixMicro()).
			Updates(map[string]any{
				"runtime_version":        gorm.Expr("runtime_version + 1"),
				"next_run_at_unix_micro": nextRunAt.UTC().UnixMicro(),
			})
		if result.Error != nil {
			return Run{}, false, fmt.Errorf("advance saved-search schedule: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			return Run{}, false, nil
		}
	} else if requireSchedule {
		var count int64
		if err := tx.Model(&scheduleRecord{}).
			Where("saved_search_id = ? AND owner_id = ? AND runtime_version = ?", candidate.SavedSearchID, candidate.OwnerID, candidate.RuntimeVersion).
			Count(&count).Error; err != nil {
			return Run{}, false, fmt.Errorf("verify run-now schedule: %w", err)
		}
		if count != 1 {
			return Run{}, false, nil
		}
	}

	var activeCount int64
	if err := tx.Model(&runRecord{}).
		Where("saved_search_id = ? AND outcome IN ?", candidate.SavedSearchID, []string{RunOutcomeClaimed.String(), RunOutcomeSubmitted.String()}).
		Count(&activeCount).Error; err != nil {
		return Run{}, false, fmt.Errorf("check active scheduled-report run: %w", err)
	}
	source, err := savedobjects.GetInTransaction(ctx, tx, sourceScope, candidate.SavedSearchID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return Run{}, false, ErrNotFound
		}
		return Run{}, false, err
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(source.Definition)
	if err != nil {
		return Run{}, false, err
	}
	sourceVersion, err := safecast.Conv[int64](source.Version)
	if err != nil {
		return Run{}, false, fmt.Errorf("saved source version: %w", err)
	}
	saved := struct {
		Version         int64
		DefinitionProto []byte
	}{Version: sourceVersion, DefinitionProto: encoded}
	outcome := RunOutcomeClaimed
	finishedAt := (*int64)(nil)
	if activeCount > 0 {
		outcome = RunOutcomeSkippedOverlap
		value := claimedAt.UnixMicro()
		finishedAt = &value
	}
	skippedOccurrenceCount, err := safecast.Conv[int64](skipped)
	if err != nil {
		return Run{}, false, fmt.Errorf("%w: skipped occurrence count is out of range", ErrInvalidArgument)
	}
	record := runRecord{
		RunID: runID, SavedSearchID: candidate.SavedSearchID, OwnerID: candidate.OwnerID,
		TenantID: candidate.TenantID, DefinitionVersion: saved.Version, OrganizationID: scopeOrganization(AccessScope{OrganizationID: candidate.OrganizationID}),
		DefinitionProto: saved.DefinitionProto, CronExpression: candidate.Cron,
		Timezone: candidate.Timezone, DispatchTTL: candidate.DispatchTTL,
		SchedulePeriodMicroseconds: period.Microseconds(), RetentionLifetimeMicroseconds: retention.Microseconds(),
		ScheduledAtUnixMicro: scheduledAt.UnixMicro(), ClaimedAtUnixMicro: claimedAt.UnixMicro(),
		SkippedOccurrenceCount: skippedOccurrenceCount, Outcome: outcome.String(), FinishedAtUnixMicro: finishedAt,
	}
	if err := tx.Create(&record).Error; err != nil {
		return Run{}, false, fmt.Errorf("persist scheduled-report run: %w", err)
	}
	if outcome.terminal() {
		if err := pruneRunHistory(tx, candidate.SavedSearchID); err != nil {
			return Run{}, false, err
		}
	}
	if err := tx.Commit().Error; err != nil {
		return Run{}, false, fmt.Errorf("commit scheduled-report claim: %w", err)
	}
	claimed = true
	run, err = runFromRecord(record)
	if err != nil {
		return Run{}, true, err
	}
	return run, true, nil
}

// MarkSubmitted attaches the admitted search job while retaining the run as
// active until an observer records the terminal search outcome.
func (repository *Repository) MarkSubmitted(ctx context.Context, runID, searchJobID string) error {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(searchJobID) == "" {
		return ErrInvalidArgument
	}
	result := repository.orm.WithContext(ctx).Model(&runRecord{}).
		Where("run_id = ? AND outcome = ?", runID, RunOutcomeClaimed.String()).
		Updates(map[string]any{"outcome": RunOutcomeSubmitted.String(), "search_job_id": searchJobID})
	if result.Error != nil {
		return fmt.Errorf("mark scheduled-report run submitted: %w", result.Error)
	}
	if result.RowsAffected == 1 {
		return nil
	}
	var record runRecord
	if err := repository.orm.WithContext(ctx).Where("run_id = ?", runID).Take(&record).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	} else if err != nil {
		return fmt.Errorf("read submitted scheduled-report run: %w", err)
	}
	// The search worker may finish before the admission call returns. Retrying
	// the same immutable attachment is valid even after that terminal update;
	// a different job ID remains a hard conflict.
	if record.SearchJobID != nil && *record.SearchJobID == searchJobID {
		return nil
	}
	return ErrConflict
}

// Finish records exactly one terminal search outcome and prunes old summaries.
func (repository *Repository) Finish(ctx context.Context, runID string, outcome RunOutcome, failureCategory string, finishedAt time.Time) error {
	if strings.TrimSpace(runID) == "" || !outcome.terminal() || outcome == RunOutcomeSkippedOverlap {
		return ErrInvalidArgument
	}
	finishedAt, err := normalizedTime(finishedAt)
	if err != nil {
		return err
	}
	tx := pgmeta.BeginLocked(repository.orm.WithContext(ctx), pgmeta.Catalog)
	if tx.Error != nil {
		return fmt.Errorf("begin scheduled-report finish: %w", tx.Error)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback().Error
		}
	}()
	var record runRecord
	if err := tx.Where("run_id = ?", runID).Take(&record).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	} else if err != nil {
		return fmt.Errorf("read scheduled-report run: %w", err)
	}
	currentOutcome := parseRunOutcome(record.Outcome)
	if currentOutcome == outcome && record.FinishedAtUnixMicro != nil {
		// A retry may follow an ambiguous commit response. Treat the identical
		// immutable terminal transition as success so completion is idempotent.
		return nil
	}
	if currentOutcome.terminal() {
		return ErrConflict
	}
	updates := map[string]any{"outcome": outcome.String(), "finished_at_unix_micro": finishedAt.UnixMicro(), "failure_category": nil}
	if normalized := strings.TrimSpace(failureCategory); normalized != "" {
		updates["failure_category"] = normalized
	}
	result := tx.Model(&runRecord{}).
		Where("run_id = ? AND outcome IN ?", runID, []string{RunOutcomeClaimed.String(), RunOutcomeSubmitted.String()}).
		Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("finish scheduled-report run: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return ErrConflict
	}
	if err := pruneRunHistory(tx, record.SavedSearchID); err != nil {
		return err
	}
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("commit scheduled-report finish: %w", err)
	}
	committed = true
	return nil
}

// InterruptActive marks runs that could not survive process restart.
func (repository *Repository) InterruptActive(ctx context.Context, now time.Time) (int64, error) {
	now, err := normalizedTime(now)
	if err != nil {
		return 0, err
	}
	result := repository.orm.WithContext(ctx).Model(&runRecord{}).
		Where("outcome IN ?", []string{RunOutcomeClaimed.String(), RunOutcomeSubmitted.String()}).
		Updates(map[string]any{"outcome": RunOutcomeInterrupted.String(), "finished_at_unix_micro": now.UnixMicro(), "failure_category": "process_restart"})
	if result.Error != nil {
		return 0, fmt.Errorf("interrupt active scheduled-report runs: %w", result.Error)
	}
	return result.RowsAffected, nil
}

// ListRuns returns newest-first owner-scoped run history.
func (repository *Repository) ListRuns(ctx context.Context, scope AccessScope, savedSearchID string, limit int) ([]Run, error) {

	page, err := repository.ListRunPage(ctx, scope, savedSearchID, RunPageRequest{Limit: limit})
	return page.Runs, err
}

// ListRunPage returns newest-first owner-scoped history using a signed keyset
// cursor. Inserts ahead of the boundary cannot duplicate or skip older rows.
func (repository *Repository) ListRunPage(
	ctx context.Context,
	scope AccessScope, savedSearchID string,
	request RunPageRequest,
) (RunPage, error) {
	ownerID := scope.OwnerID

	if strings.TrimSpace(ownerID) == "" || strings.TrimSpace(savedSearchID) == "" || request.Limit < 0 || request.Limit > RunHistoryLimit {
		return RunPage{}, ErrInvalidArgument
	}
	if request.Limit == 0 {
		request.Limit = RunHistoryLimit
	}
	if err := repository.authorizeMetadataSource(ctx, scope, savedSearchID); err != nil {
		return RunPage{}, err
	}
	cursorScope, err := policyCursorIdentity(ctx, scope)
	if err != nil {
		return RunPage{}, err
	}
	boundary, err := decodeRunCursor(repository.cursorKey, cursorScope, savedSearchID, request.PageToken)
	if err != nil {
		return RunPage{}, err
	}
	query := scopedScheduleQuery(repository.orm.WithContext(ctx), scope).Where("TRUE AND saved_search_id = ?", savedSearchID)
	if boundary != nil {
		query = query.Where("claimed_at_unix_micro < ? OR (claimed_at_unix_micro = ? AND run_id < ?)", boundary.ClaimedAtUnixMicro, boundary.ClaimedAtUnixMicro, boundary.RunID)
	}
	var records []runRecord
	if err := query.Order("claimed_at_unix_micro DESC, run_id DESC").Limit(request.Limit + 1).Find(&records).Error; err != nil {
		return RunPage{}, fmt.Errorf("list scheduled-report runs: %w", err)
	}
	hasNext := len(records) > request.Limit
	if hasNext {
		records = records[:request.Limit]
	}
	runs := make([]Run, 0, len(records))
	for _, record := range records {
		run, err := runFromRecord(record)
		if err != nil {
			return RunPage{}, err
		}
		runs = append(runs, run)
	}
	page := RunPage{Runs: runs}
	if hasNext {
		last := records[len(records)-1]
		page.NextPageToken, err = encodeRunCursor(repository.cursorKey, cursorScope, savedSearchID, last.ClaimedAtUnixMicro, last.RunID)
		if err != nil {
			return RunPage{}, err
		}
	}
	if request.IncludeTotal {
		var count int64
		if err := scopedScheduleQuery(repository.orm.WithContext(ctx).Model(&runRecord{}), scope).
			Where("TRUE AND saved_search_id = ?", savedSearchID).Count(&count).Error; err != nil {
			return RunPage{}, fmt.Errorf("count scheduled-report runs: %w", err)
		}
		if count < 0 {
			return RunPage{}, errors.New("count scheduled-report runs: invalid result")
		}
		total := uint64(count)
		page.TotalSize = &total
	}
	return page, nil
}

func (repository *Repository) generateID() (string, error) {
	for range maximumIDAttempts {
		id, err := repository.idGenerator()
		if err != nil {
			return "", fmt.Errorf("generate scheduled-report run ID: %w", err)
		}
		if strings.TrimSpace(id) != "" && len(id) <= 128 {
			return id, nil
		}
	}
	return "", errors.New("generate scheduled-report run ID: repeated invalid ID")
}

func newRunID() (string, error) {
	var raw [18]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "report-run-" + base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func scheduleFromRecord(record scheduleRecord) (Schedule, error) {
	if record.OrganizationID <= 0 {
		return Schedule{}, errors.New("persisted schedule organization is invalid")
	}
	if record.ConfigVersion < 1 || record.RuntimeVersion < 1 {
		return Schedule{}, errors.New("read saved-search schedule: invalid persisted version")
	}
	created := time.UnixMicro(record.CreatedAtUnixMicro).UTC()
	updated := time.UnixMicro(record.UpdatedAtUnixMicro).UTC()
	var next *time.Time
	if record.NextRunAtUnixMicro != nil {
		value := time.UnixMicro(*record.NextRunAtUnixMicro).UTC()
		next = &value
	}
	return Schedule{
		SavedSearchID: record.SavedSearchID, OwnerID: record.OwnerID, TenantID: record.TenantID, OrganizationID: strconv.FormatInt(record.OrganizationID, 10),
		Cron: record.CronExpression, Timezone: record.Timezone, DispatchTTL: record.DispatchTTL,
		Enabled: record.Enabled, ConfigVersion: uint64(record.ConfigVersion), RuntimeVersion: uint64(record.RuntimeVersion),
		NextRunAt: next, CreatedAt: created, UpdatedAt: updated,
	}, nil
}

func runFromRecord(record runRecord) (Run, error) {
	if record.OrganizationID <= 0 {
		return Run{}, errors.New("persisted schedule run organization is invalid")
	}
	if record.DefinitionVersion < 1 || record.SchedulePeriodMicroseconds <= 0 || record.RetentionLifetimeMicroseconds <= 0 || record.SkippedOccurrenceCount < 0 {
		return Run{}, errors.New("read scheduled-report run: invalid persisted values")
	}
	definition := new(opensplunk.SavedSearchDefinition)
	if err := proto.Unmarshal(record.DefinitionProto, definition); err != nil {
		return Run{}, fmt.Errorf("decode scheduled-report definition: %w", err)
	}
	outcome := parseRunOutcome(record.Outcome)
	if outcome == RunOutcomeInvalid {
		return Run{}, errors.New("read scheduled-report run: invalid persisted outcome")
	}
	result := Run{
		RunID: record.RunID, SavedSearchID: record.SavedSearchID, OwnerID: record.OwnerID, TenantID: record.TenantID, OrganizationID: strconv.FormatInt(record.OrganizationID, 10),
		DefinitionVersion: uint64(record.DefinitionVersion), Definition: definition,
		Cron: record.CronExpression, Timezone: record.Timezone, DispatchTTL: record.DispatchTTL,
		SchedulePeriod:    time.Duration(record.SchedulePeriodMicroseconds) * time.Microsecond,
		RetentionLifetime: time.Duration(record.RetentionLifetimeMicroseconds) * time.Microsecond,
		ScheduledAt:       time.UnixMicro(record.ScheduledAtUnixMicro).UTC(), ClaimedAt: time.UnixMicro(record.ClaimedAtUnixMicro).UTC(),
		SkippedOccurrenceCount: uint64(record.SkippedOccurrenceCount), Outcome: outcome,
	}
	if record.SearchJobID != nil {
		result.SearchJobID = *record.SearchJobID
	}
	if record.FailureCategory != nil {
		result.FailureCategory = *record.FailureCategory
	}
	if record.FinishedAtUnixMicro != nil {
		value := time.UnixMicro(*record.FinishedAtUnixMicro).UTC()
		result.FinishedAt = &value
	}
	return result, nil
}

func validateIdentity(ownerID, tenantID, savedSearchID string) error {
	for _, value := range []string{ownerID, tenantID, savedSearchID} {
		if strings.TrimSpace(value) == "" || len(value) > 255 || strings.IndexByte(value, 0) >= 0 {
			return ErrInvalidArgument
		}
	}
	return nil
}

func normalizedTime(value time.Time) (time.Time, error) {
	if value.IsZero() || value.UnixMicro() <= 0 {
		return time.Time{}, ErrInvalidArgument
	}
	return value.UTC().Truncate(time.Microsecond), nil
}

func timePointerToUnixMicro(value *time.Time) *int64 {
	if value == nil {
		return nil
	}
	microseconds := value.UTC().Truncate(time.Microsecond).UnixMicro()
	return &microseconds
}

func pruneRunHistory(tx *gorm.DB, savedSearchID string) error {
	result := tx.Exec(`
		DELETE FROM saved_search_schedule_runs
		WHERE saved_search_id = ?
		  AND run_id IN (
			SELECT run_id
			FROM saved_search_schedule_runs
			WHERE saved_search_id = ?
			  AND outcome NOT IN ('claimed', 'submitted')
			ORDER BY claimed_at_unix_micro DESC, run_id DESC
			OFFSET ?
		  )`, savedSearchID, savedSearchID, RunHistoryLimit)
	if result.Error != nil {
		return fmt.Errorf("prune scheduled-report run history: %w", result.Error)
	}
	return nil
}

func scopeOrganization(scope AccessScope) int64 {
	value, _ := strconv.ParseInt(scope.OrganizationID, 10, 64)
	return value
}
