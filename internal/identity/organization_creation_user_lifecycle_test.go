package identity

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

func TestChildProvisioningFencesEveryInitialUserBeforeHierarchyGuard(t *testing.T) {
	runtime, pool, _ := newPostgresIdentityRuntime(t)
	users := []pm.User{
		{Username: "provisionsite", Email: "provisionsite@example.com", UserType: pb.UserType_USER_TYPE_ADMIN_TIER_10},
		{Username: "provisionowner", Email: "provisionowner@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0},
		{Username: "provisionadmin", Email: "provisionadmin@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0},
	}
	for index := range users {
		if err := pool.Create(&users[index]).Error; err != nil {
			t.Fatal(err)
		}
	}
	userCtx := func(user pm.User) context.Context {
		ctx := scontext.WithUser[sm.UserIdType](t.Context(), &sm.User{ID: user.ID, Username: user.Username, Email: user.Email, UserType: user.UserType})
		return scontext.WithUserID[sm.UserIdType, sm.User](ctx, user.ID)
	}
	siteCtx, adminCtx := userCtx(users[0]), userCtx(users[2])
	handler := runtime.service.GetOrganizationHandler()
	root, err := identityNativeTransaction(runtime, siteCtx, func(ctx context.Context) (*organization.OrganizationResponse, error) {
		return runtime.CreateOrganizationWithInitialMemberships(ctx, &sm.Organization{Name: "Provision root", Slug: "provision-root"}, []InitialMembership{{UserID: users[1].ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identityNativeTransaction(runtime, siteCtx, func(ctx context.Context) (*sm.UserOrganizationResponse, error) {
		return handler.AddMember(ctx, users[2].ID, root.ID, pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := identityNativeTransaction(runtime, adminCtx, func(ctx context.Context) (*sm.Organization, error) {
		return runtime.service.GetPersonalWorkspaceService().EnsureForUser(ctx, &sm.User{ID: users[2].ID, Username: users[2].Username, Email: users[2].Email, UserType: users[2].UserType})
	}); err != nil {
		t.Fatal(err)
	}
	// Pause after the first (Owner) insertion holds the hierarchy guard and
	// before the second (acting Admin) insertion. Deletion must already be
	// fenced by the Admin user's lifetime, rather than acquire it in reverse.
	const gateKey int64 = 9901301
	if err := pool.Exec(`CREATE FUNCTION test_pause_initial_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.user_id = ` + strconv.FormatUint(users[1].ID, 10) + ` AND EXISTS(SELECT 1 FROM gocommon_organizations WHERE id=NEW.organization_id AND slug='provision-child') THEN
  PERFORM pg_advisory_xact_lock(9901301);
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER test_pause_initial_owner AFTER INSERT ON gocommon_user_organizations FOR EACH ROW EXECUTE FUNCTION test_pause_initial_owner();`).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	poolSQL, err := pool.DB()
	if err != nil {
		t.Fatal(err)
	}
	gate, err := poolSQL.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gate.Close() }()
	defer func() { _, _ = gate.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", gateKey) }()
	if _, err := gate.ExecContext(ctx, "SELECT pg_advisory_lock($1)", gateKey); err != nil {
		t.Fatal(err)
	}
	creationPID := make(chan int, 1)
	creationResult := make(chan error, 1)
	var childID int64
	go func() {
		_, err := identityNativeTransaction(runtime, adminCtx, func(txCtx context.Context) (struct{}, error) {
			ambient, _ := scontext.GetTransaction(txCtx)
			var pid int
			if err := ambient.GetDB().Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
				return struct{}{}, err
			}
			creationPID <- pid
			child, err := runtime.CreateOrganizationWithInitialMemberships(txCtx, &sm.Organization{Name: "Provision child", Slug: "provision-child", ParentOrganizationID: &root.ID}, []InitialMembership{{UserID: users[1].ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
			if err == nil {
				childID = child.ID
			}
			return struct{}{}, err
		})
		creationResult <- err
	}()
	var pid int
	select {
	case pid = <-creationPID:
	case err := <-creationResult:
		t.Fatalf("child creation failed before its transaction: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	wait := func(predicate string, pid int, result <-chan error) {
		t.Helper()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			var waiting bool
			if err := pool.Raw(predicate, pid).Scan(&waiting).Error; err != nil {
				t.Fatal(err)
			}
			if waiting {
				return
			}
			select {
			case err := <-result:
				t.Fatalf("operation returned before its expected row/guard wait: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-ticker.C:
			}
		}
	}
	wait("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=? AND locktype='advisory' AND NOT granted)", pid, creationResult)
	deletionPID := make(chan int, 1)
	deletionResult := make(chan error, 1)
	go func() {
		_, err := identityNativeTransaction(runtime, siteCtx, func(txCtx context.Context) (struct{}, error) {
			ambient, _ := scontext.GetTransaction(txCtx)
			var deletionBackendPID int
			if err := ambient.GetDB().Raw("SELECT pg_backend_pid()").Scan(&deletionBackendPID).Error; err != nil {
				return struct{}{}, err
			}
			deletionPID <- deletionBackendPID
			return struct{}{}, runtime.service.AdminDeleteUser(txCtx, users[2].ID, false, "concurrent child provisioning")
		})
		deletionResult <- err
	}()
	select {
	case pid = <-deletionPID:
	case err := <-deletionResult:
		t.Fatalf("user deletion failed before its transaction: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	wait("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=? AND wait_event_type='Lock' AND query LIKE '%gocommon_users%' AND query LIKE '%FOR UPDATE%')", pid, deletionResult)
	if _, err := gate.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", gateKey); err != nil {
		t.Fatal(err)
	}
	for _, result := range []<-chan error{creationResult, deletionResult} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	var members []pm.UserOrganization
	if err := pool.Where("organization_id=?", childID).Find(&members).Error; err != nil || len(members) != 1 || members[0].UserID != users[1].ID || members[0].Role != pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10 {
		t.Fatalf("child ownership after actor deletion: %+v %v", members, err)
	}
}
