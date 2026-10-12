package main

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type identityDirectSearchExecutor struct {
	reads, inventories atomic.Int32
	inventory          func(context.Context, clickhouse.CompiledStatsWildcardInventory) (plan.StatsWildcardExpansion, error)
}

func (executor *identityDirectSearchExecutor) Execute(_ context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
	executor.reads.Add(1)
	if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}); err != nil {
		return err
	}
	return sink.AddRow([]searchjobs.Value{searchjobs.StringValue("allowed")})
}

func (executor *identityDirectSearchExecutor) ExecuteStatsWildcardInventory(ctx context.Context, compiled clickhouse.CompiledStatsWildcardInventory) (plan.StatsWildcardExpansion, error) {
	executor.inventories.Add(1)
	return executor.inventory(ctx, compiled)
}

type identityDirectSearchSnapshotter struct{}

func (identityDirectSearchSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return 1, nil
}

// This runs with the actual shared GoCommon user/membership/index authority,
// rather than mocking the runtime callback that rejected queued direct SPL.
func runtimeIdentityDirectSearchAcceptance(t *testing.T, runtime *runtimeIdentity, db *control.DB, membership pm.UserOrganization, access searchjobs.AccessScope, indexID string) {
	t.Helper()
	for _, fixture := range []struct {
		name, spl, revoke string
	}{
		{"ordinary direct search", "index=events | table message", ""},
		{"parse failure metadata", "index=events | eval (", ""},
		{"index revoked after queued admission", "index=events | table message", "index"},
		{"membership revoked after queued admission", "index=events | table message", "membership"},
		{"inventory denied before backend", "index=events | stats sum(*)", "index"},
		{"inventory failure retains source binding", "index=events | stats sum(*)", "after_inventory"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			orm := db.GORMDB().WithContext(t.Context())
			defer func() {
				switch fixture.revoke {
				case "index", "after_inventory":
					if err := orm.Exec("DELETE FROM os_index_permissions WHERE index_id=? AND subject_kind='user' AND subject_id=?", indexID, membership.UserID).Error; err != nil {
						t.Error(err)
					}
				case "membership":
					if err := orm.Create(&membership).Error; err != nil {
						t.Error(err)
					}
				}
			}()
			readContext := authorization.WithScope(t.Context(), authorization.Scope{Subject: authorization.Subject{UserID: membership.UserID, UserType: int32(pb.UserType_USER_TYPE_REGULAR_TIER_0)}, OrganizationID: membership.OrganizationID, PhysicalTenant: access.TenantID})
			var durable *searchartifacts.Store
			var journal searchjobs.JobJournal
			if fixture.name == "parse failure metadata" {
				var err error
				durable, err = searchartifacts.New(t.Context(), searchartifacts.Config{DB: db.SQLDB(), Directory: t.TempDir(), Blobs: runtime.blobs, CleanupInterval: -1, CaptureOptions: searchartifacts.CaptureOptions{Authorize: runtime.authorizeCapture}})
				if err != nil {
					t.Fatal(err)
				}
				defer durable.Close()
				journal = durable
			}
			var queuedChecked atomic.Bool
			executor := &identityDirectSearchExecutor{inventory: func(context.Context, clickhouse.CompiledStatsWildcardInventory) (plan.StatsWildcardExpansion, error) {
				return plan.StatsWildcardExpansion{}, errors.New("intentional bounded inventory failure")
			}}
			manager, err := searchjobs.New(searchjobs.Config{
				Executor: executor, Snapshotter: identityDirectSearchSnapshotter{}, CleanupInterval: -1, Journal: journal,
				ResolveAdmissionScope: func(ctx context.Context, scope searchjobs.AccessScope) (searchjobs.AccessScope, error) {
					return runtime.freshAccess(ctx, scope.OwnerID, scope.OrganizationID)
				},
				AuthorizeJob: func(ctx context.Context, scope searchjobs.AccessScope, job searchjobs.Job) error {
					if err := runtime.authorizeJob(ctx, scope, job); err != nil {
						return err
					}
					if job.State == searchjobs.StateQueued && len(job.EffectiveIndexes) == 0 && queuedChecked.CompareAndSwap(false, true) {
						if durable != nil {
							if _, err := durable.Get(readContext, access, job.ID, searchartifacts.AccessInspect); err != nil {
								return err
							}
						}
						switch fixture.revoke {
						case "index":
							return orm.Exec("INSERT INTO os_index_permissions(index_id,subject_kind,subject_id,capability,effect) VALUES(?,'user',?,'search','deny')", indexID, membership.UserID).Error
						case "membership":
							return orm.Delete(&membership).Error
						}
					}
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Close()
			interval, err := searchtime.NewAbsoluteRange(time.Now().Add(-time.Hour), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			job, err := manager.Create(t.Context(), searchjobs.CreateRequest{
				SPL: fixture.spl, TenantID: access.TenantID, OwnerID: access.OwnerID, OrganizationID: access.OrganizationID,
				AuthorizedIndexes: []string{"events"}, RequestedIndexes: []string{"events"}, TimeRange: interval,
			})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for !job.State.Terminal() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
				job, err = manager.Get(job.ID) // trusted test inspection; browser uses GetFor
				if err != nil {
					t.Fatal(err)
				}
			}
			if !queuedChecked.Load() || !job.State.Terminal() {
				t.Fatalf("direct search did not pass queued identity check: %+v", job)
			}
			if fixture.revoke == "after_inventory" {
				if job.State != searchjobs.StateFailed || !slices.Equal(job.EffectiveIndexes, []string{"events"}) || executor.inventories.Load() != 1 || executor.reads.Load() != 0 {
					t.Fatalf("failed inventory lost its proven source binding: job=%+v reads=%d inventories=%d", job, executor.reads.Load(), executor.inventories.Load())
				}
				if _, err := manager.GetForContext(t.Context(), access, job.ID); err != nil {
					t.Fatalf("authorized failed inventory metadata: %v", err)
				}
				if err := orm.Exec("INSERT INTO os_index_permissions(index_id,subject_kind,subject_id,capability,effect) VALUES(?,'user',?,'search','deny')", indexID, membership.UserID).Error; err != nil {
					t.Fatal(err)
				}
				if _, err := manager.GetForContext(t.Context(), access, job.ID); !errors.Is(err, authorization.ErrForbidden) {
					t.Fatalf("inventory source revocation did not fence failed metadata: %v", err)
				}
				return
			}
			if fixture.revoke != "" {
				if job.State != searchjobs.StateFailed || job.Failure == nil || job.Failure.Code != searchjobs.FailureIndexForbidden || executor.reads.Load() != 0 || executor.inventories.Load() != 0 {
					t.Fatalf("revocation did not fence all backend reads: job=%+v reads=%d inventories=%d", job, executor.reads.Load(), executor.inventories.Load())
				}
				if _, err := manager.GetForContext(t.Context(), access, job.ID); !errors.Is(err, authorization.ErrForbidden) {
					t.Fatalf("revoked job metadata error=%v", err)
				}
				return
			}
			if fixture.name == "parse failure metadata" {
				if job.State != searchjobs.StateFailed || job.Failure == nil || job.Failure.Code != searchjobs.FailureInvalidSPL || executor.reads.Load() != 0 {
					t.Fatalf("parse failure escaped bounded diagnostics: %+v", job)
				}
				if _, err := manager.GetForContext(t.Context(), access, job.ID); err != nil {
					t.Fatalf("owner cannot read parse failure diagnostics: %v", err)
				}
				site := pm.User{Username: "diagnostic-site", Email: "diagnostic-site@example.com", UserType: pb.UserType_USER_TYPE_ADMIN_TIER_10}
				if err := orm.Create(&site).Error; err != nil {
					t.Fatal(err)
				}
				siteAccess := access
				siteAccess.OwnerID = strconv.FormatUint(site.ID, 10)
				if err := runtime.authorizeJob(t.Context(), siteAccess, job); err != nil {
					t.Fatalf("site administrator cannot inspect another creator's parse failure: %v", err)
				}
				queued := job
				queued.State, queued.Failure = searchjobs.StateQueued, nil
				if err := runtime.authorizeJob(t.Context(), siteAccess, queued); err != nil {
					t.Fatalf("site administrator cannot inspect another creator's queued job: %v", err)
				}
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				if record, err := durable.Get(readContext, access, job.ID, searchartifacts.AccessInspect); err != nil || record.State != searchartifacts.StateFailed {
					t.Fatalf("ordinary owner durable parse diagnostics: state=%v error=%v", record.State, err)
				}
				if err := runtime.exportAuthorizer(durable)(readContext, access, job.ID); !errors.Is(err, authorization.ErrForbidden) {
					t.Fatalf("unbound diagnostic export authorization: %v", err)
				}
				parsed, err := spl.Parse("index=events | table message")
				if err != nil {
					t.Fatal(err)
				}
				cutoff := job.VisibilityCutoff
				logical, err := plan.Build(parsed, plan.Scope{TenantID: access.TenantID, AuthorizedIndexes: []string{"events"}, RequestedIndexes: []string{"events"}, SearchJobID: job.ID, Earliest: job.Earliest, Latest: job.Latest, SearchStart: job.CreatedAt, SearchTimezone: job.TimeRange.Timezone, IndexTimeCutoff: job.IndexTimeCutoff, VisibilityCutoff: &cutoff})
				if err != nil {
					t.Fatal(err)
				}
				compiled, err := (clickhouse.Compiler{}).CompileContext(t.Context(), logical)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := durable.BeginCapture(readContext, job, compiled, 1); !errors.Is(err, searchjobs.ErrInvalidResult) {
					t.Fatalf("unbound diagnostic entered capture: %v", err)
				}
				canceled := job
				canceled.ID += "-canceled"
				canceled.State, canceled.Failure, canceled.Version = searchjobs.StateQueued, nil, 1
				canceled.FinishedAt, canceled.ExpiresAt = time.Time{}, time.Time{}
				if err := durable.Admit(t.Context(), canceled); err != nil {
					t.Fatal(err)
				}
				canceled.State, canceled.Version = searchjobs.StateCanceled, 2
				canceled.FinishedAt, canceled.ExpiresAt = time.Now().UTC(), time.Now().UTC().Add(time.Hour)
				if err := durable.Finalize(t.Context(), canceled); err != nil {
					t.Fatal(err)
				}
				if _, err := durable.Get(readContext, access, canceled.ID, searchartifacts.AccessInspect); err != nil {
					t.Fatalf("ordinary owner durable queued-cancellation diagnostics: %v", err)
				}
				for _, mutate := range []func(*searchjobs.Job){
					func(job *searchjobs.Job) { job.Schema = &searchjobs.Schema{} },
					func(job *searchjobs.Job) { job.RowCount = 1 },
					func(job *searchjobs.Job) { job.ScannedRows = 1 },
					func(job *searchjobs.Job) { job.CaptureComplete = true },
				} {
					corrupt := job
					mutate(&corrupt)
					if err := runtime.authorizeJob(t.Context(), access, corrupt); !errors.Is(err, authorization.ErrForbidden) {
						t.Fatalf("unbound result authority accepted: %v", err)
					}
				}
				return
			}
			if job.State != searchjobs.StateCompleted || job.Failure != nil || !slices.Equal(job.EffectiveIndexes, []string{"events"}) || executor.reads.Load() != 1 || job.RowCount != 1 {
				t.Fatalf("ordinary direct search failed: %+v reads=%d", job, executor.reads.Load())
			}
		})
	}
}
