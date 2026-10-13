package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	cp "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	gerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

func TestNativeChildMembershipCannotOutliveParentRemoval(t *testing.T) {
	runtime, pool, _ := newPostgresIdentityRuntime(t)
	site := pm.User{Username: "hierarchysite", Email: "hierarchysite@example.com", UserType: pb.UserType_USER_TYPE_ADMIN_TIER_10}
	target := pm.User{Username: "hierarchytarget", Email: "hierarchytarget@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0}
	for _, user := range []*pm.User{&site, &target} {
		if err := pool.Create(user).Error; err != nil {
			t.Fatal(err)
		}
	}
	actorCtx := scontext.WithUser[sm.UserIdType](t.Context(), &sm.User{ID: site.ID, UserType: site.UserType})
	actorCtx = scontext.WithUserID[sm.UserIdType, sm.User](actorCtx, site.ID)
	handler := runtime.service.GetOrganizationHandler()
	create := func(slug string, parent *int64) int64 {
		org, err := identityNativeTransaction(runtime, actorCtx, func(ctx context.Context) (*organization.OrganizationResponse, error) {
			return runtime.CreateOrganizationWithInitialMemberships(ctx, &sm.Organization{Name: slug, Slug: slug, ParentOrganizationID: parent}, []InitialMembership{{UserID: site.ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
		})
		if err != nil {
			t.Fatal(err)
		}
		return org.ID
	}
	for _, operation := range []string{"existing-child-member", "new-child-owner"} {
		t.Run(operation, func(t *testing.T) {
			root := create(operation+"-root", nil)
			child := create(operation+"-child", &root)
			if _, err := identityNativeTransaction(runtime, actorCtx, func(ctx context.Context) (*sm.UserOrganizationResponse, error) {
				return handler.AddMember(ctx, target.ID, root, pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0)
			}); err != nil {
				t.Fatal(err)
			}
			orm, err := runtime.Persistence().BeginTransaction(actorCtx)
			if err != nil {
				t.Fatal(err)
			}
			removal := &cp.GormTransactionWithHooks{Tx: orm}
			defer func() { _ = removal.Rollback() }()
			removalCtx := scontext.WithTransaction[sm.UserIdType, sm.User](actorCtx, removal)
			if err := handler.RemoveMember(removalCtx, target.ID, root); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(actorCtx, 5*time.Second)
			defer cancel()
			pidReady := make(chan int, 1)
			finished := make(chan error, 1)
			newSlug := operation + "-new-child"
			go func() {
				_, err := identityNativeTransaction(runtime, ctx, func(txCtx context.Context) (struct{}, error) {
					ambient, _ := scontext.GetTransaction(txCtx)
					var pid int
					if err := ambient.GetDB().Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
						return struct{}{}, err
					}
					pidReady <- pid
					if operation == "new-child-owner" {
						_, err := runtime.CreateOrganizationWithInitialMemberships(txCtx, &sm.Organization{Name: newSlug, Slug: newSlug, ParentOrganizationID: &root}, []InitialMembership{{UserID: target.ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
						return struct{}{}, err
					}
					_, err := handler.AddMember(txCtx, target.ID, child, pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0)
					return struct{}{}, err
				})
				finished <- err
			}()
			var pid int
			select {
			case pid = <-pidReady:
			case err := <-finished:
				t.Fatalf("membership operation failed before its transaction: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				var waiting bool
				if err := pool.Raw("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=? AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock%')", pid).Scan(&waiting).Error; err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case err := <-finished:
					t.Fatalf("membership insertion did not wait for parent removal: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-ticker.C:
				}
			}
			if err := removal.Commit(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-finished:
				if !errors.Is(err, gerrors.ErrOrganizationAncestorMembershipRequired) {
					t.Fatalf("child membership authorized from removed parent: %v", err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var count int64
			if err := pool.Model(&pm.UserOrganization{}).Where("user_id=? AND organization_id IN ?", target.ID, []int64{root, child}).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("orphaned membership count=%d: %v", count, err)
			}
			if err := pool.Unscoped().Model(&pm.Organization{}).Where("slug=?", newSlug).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("rejected initial owner left child organization count=%d: %v", count, err)
			}
		})
	}
}
