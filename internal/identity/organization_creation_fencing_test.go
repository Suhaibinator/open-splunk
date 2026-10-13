package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	gerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

func TestNativeChildCreationFencesAncestorSoftDeletion(t *testing.T) {
	runtime, pool, _ := newPostgresIdentityRuntime(t)
	actor := pm.User{Username: "creationfence", Email: "creationfence@example.com", UserType: pb.UserType_USER_TYPE_ADMIN_TIER_10}
	if err := pool.Create(&actor).Error; err != nil {
		t.Fatal(err)
	}
	actorCtx := scontext.WithUser[sm.UserIdType](t.Context(), &sm.User{ID: actor.ID, UserType: actor.UserType})
	actorCtx = scontext.WithUserID[sm.UserIdType, sm.User](actorCtx, actor.ID)
	create := func(ctx context.Context, slug string, parent *int64) (*organization.OrganizationResponse, error) {
		return identityNativeTransaction(runtime, ctx, func(txCtx context.Context) (*organization.OrganizationResponse, error) {
			return runtime.CreateOrganizationWithInitialMemberships(txCtx, &sm.Organization{Name: slug, Slug: slug, ParentOrganizationID: parent}, []InitialMembership{{UserID: actor.ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
		})
	}
	for _, deletedLevel := range []string{"parent", "ancestor"} {
		t.Run(deletedLevel, func(t *testing.T) {
			root, err := create(actorCtx, deletedLevel+"-root", nil)
			if err != nil {
				t.Fatal(err)
			}
			parent, err := create(actorCtx, deletedLevel+"-parent", &root.ID)
			if err != nil {
				t.Fatal(err)
			}
			deletedID := parent.ID
			if deletedLevel == "ancestor" {
				deletedID = root.ID
			}
			deletion := pool.WithContext(t.Context()).Begin()
			if deletion.Error != nil {
				t.Fatal(deletion.Error)
			}
			defer deletion.Rollback()
			if err := deletion.Exec("UPDATE gocommon_organizations SET deleted_at=clock_timestamp() WHERE id=?", deletedID).Error; err != nil {
				t.Fatal(err)
			}
			creationCtx, cancel := context.WithTimeout(actorCtx, 5*time.Second)
			defer cancel()
			pidReady := make(chan int, 1)
			finished := make(chan error, 1)
			slug := deletedLevel + "-blocked-child"
			go func() {
				_, err := identityNativeTransaction(runtime, creationCtx, func(txCtx context.Context) (*organization.OrganizationResponse, error) {
					ambient, _ := scontext.GetTransaction(txCtx)
					var pid int
					if err := ambient.GetDB().Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
						return nil, err
					}
					pidReady <- pid
					return runtime.CreateOrganizationWithInitialMemberships(txCtx, &sm.Organization{Name: slug, Slug: slug, ParentOrganizationID: &parent.ID}, []InitialMembership{{UserID: actor.ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
				})
				finished <- err
			}()
			var pid int
			select {
			case pid = <-pidReady:
			case err := <-finished:
				t.Fatalf("creation failed before entering its transaction: %v", err)
			case <-creationCtx.Done():
				t.Fatal(creationCtx.Err())
			}
			// Observe an actual PostgreSQL row-lock wait, rather than infer one
			// from execution timing. The native hook must lock before INSERT.
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				var waiting bool
				if err := pool.Raw("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=? AND wait_event_type='Lock' AND query LIKE '%deleted_at IS NULL FOR SHARE%')", pid).Scan(&waiting).Error; err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case err := <-finished:
					t.Fatalf("creation did not wait for ancestor deletion: %v", err)
				case <-creationCtx.Done():
					t.Fatal(creationCtx.Err())
				case <-ticker.C:
				}
			}
			if err := deletion.Commit().Error; err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-finished:
				if !errors.Is(err, gerrors.ErrOrganizationNotFound) {
					t.Fatalf("creation accepted deleted %s: %v", deletedLevel, err)
				}
			case <-creationCtx.Done():
				t.Fatal(creationCtx.Err())
			}
			var count int64
			if err := pool.Unscoped().Model(&pm.Organization{}).Where("slug=?", slug).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("orphaned child count=%d: %v", count, err)
			}
			if err := pool.Unscoped().Model(&pm.Organization{}).Where("id=?", deletedID).Update("deleted_at", nil).Error; err != nil {
				t.Fatal(err)
			}
			child, err := create(actorCtx, slug, &parent.ID)
			if err != nil {
				t.Fatalf("live hierarchy creation failed: %v", err)
			}
			if err := pool.Model(&pm.UserOrganization{}).Where("user_id=? AND organization_id IN ?", actor.ID, []int64{root.ID, parent.ID, child.ID}).Count(&count).Error; err != nil || count != 3 {
				t.Fatalf("native ancestor membership invariant lost: count=%d %v", count, err)
			}
		})
	}
}

func TestChildCreationPolicyRequiresAmbientTransaction(t *testing.T) {
	parent := int64(1)
	input := OrganizationCreationInput{
		ActorID: 1, ActorType: pb.UserType_USER_TYPE_ADMIN_TIER_10,
		Organization:         &sm.Organization{ParentOrganizationID: &parent, Lineage: []int64{parent}},
		RequestedMemberships: []InitialMembership{{UserID: 1, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}},
	}
	if _, err := (&Runtime{}).initialMemberships(t.Context(), input); !errors.Is(err, gerrors.ErrTransactionNotFound) {
		t.Fatalf("child policy without transaction: %v", err)
	}
}
