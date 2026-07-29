package collectoradmission

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
)

func admitAuthorizationFixture(
	t *testing.T,
	fixture admissionFixture,
	issued auth.IssuedCollectorToken,
	streamID string,
) Result {
	t.Helper()
	acceptedAt := issued.Token.CreatedAt.Add(time.Minute)
	result, err := fixture.store.Admit(
		context.Background(),
		issued.Secret.Plaintext(),
		admissionRequest(
			issued.Token.BoundCollectorID,
			streamID,
			acceptedAt,
			"main",
		),
	)
	if err != nil {
		t.Fatalf("Admit(): %v", err)
	}
	return result
}

func TestAuthorizeLeaseReturnsFreshScopeWithoutRecordingTokenUse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := openAdmissionFixture(t, "audit", "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	admitted := admitAuthorizationFixture(
		t,
		fixture,
		issued,
		"stream-1",
	)
	before, err := fixture.tokens.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(before): %v", err)
	}
	updated, err := fixture.tokens.UpdateCollectorToken(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
		auth.UpdateCollectorTokenRequest{
			Name:              issued.Token.Name,
			Description:       issued.Token.Description,
			AllowedIndexNames: []string{"audit"},
			BoundCollectorID:  testCollectorID,
		},
	)
	if err != nil {
		t.Fatalf("UpdateCollectorToken(scope): %v", err)
	}
	if !slices.Equal(updated.AllowedIndexNames, []string{"audit"}) {
		t.Fatalf("updated token scope = %v", updated.AllowedIndexNames)
	}

	got, err := fixture.store.AuthorizeLease(
		ctx,
		issued.Secret.Plaintext(),
		admitted.Lease,
		issued.Token.CreatedAt.Add(2*time.Minute),
	)
	if err != nil {
		t.Fatalf("AuthorizeLease(): %v", err)
	}
	if got.TokenID != issued.Token.ID ||
		got.BoundCollectorID != testCollectorID ||
		!slices.Equal(got.AllowedIndexNames, []string{"audit"}) {
		t.Fatalf("fresh authentication = %#v", got)
	}
	after, err := fixture.tokens.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(after): %v", err)
	}
	if !after.LastUsedAt.Equal(before.LastUsedAt) {
		t.Fatalf(
			"AuthorizeLease() changed LastUsedAt from %v to %v",
			before.LastUsedAt,
			after.LastUsedAt,
		)
	}
}

func TestAuthorizeLeaseFailsClosedAtCredentialAndDurableFences(t *testing.T) {
	t.Run("superseded lease", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fixture := openAdmissionFixture(t, "main")
		issued := issueToken(
			t,
			fixture,
			"collector",
			testCollectorID,
			"main",
		)
		first := admitAuthorizationFixture(
			t,
			fixture,
			issued,
			"stream-1",
		)
		second := admitAuthorizationFixture(
			t,
			fixture,
			issued,
			"stream-2",
		)
		if second.Lease.Generation <= first.Lease.Generation {
			t.Fatalf(
				"successor generation = %d, first = %d",
				second.Lease.Generation,
				first.Lease.Generation,
			)
		}
		if _, err := fixture.store.AuthorizeLease(
			ctx,
			issued.Secret.Plaintext(),
			first.Lease,
			issued.Token.CreatedAt.Add(3*time.Minute),
		); !errors.Is(err, ErrLeaseNotCurrent) {
			t.Fatalf("stale lease error = %v, want ErrLeaseNotCurrent", err)
		}
	})

	t.Run("disabled collector", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fixture := openAdmissionFixture(t, "main")
		issued := issueToken(
			t,
			fixture,
			"collector",
			testCollectorID,
			"main",
		)
		admitted := admitAuthorizationFixture(
			t,
			fixture,
			issued,
			"stream-1",
		)
		if _, err := fixture.fleet.UpdateAdministration(
			ctx,
			admitted.Lease.Scope,
			admitted.Lease.CollectorID,
			admitted.Collector.Version,
			collectorfleet.Administration{
				State: collectorfleet.AdministrativeStateDisabled,
			},
			issued.Token.CreatedAt.Add(2*time.Minute),
		); err != nil {
			t.Fatalf("UpdateAdministration(disable): %v", err)
		}
		if _, err := fixture.store.AuthorizeLease(
			ctx,
			issued.Secret.Plaintext(),
			admitted.Lease,
			issued.Token.CreatedAt.Add(3*time.Minute),
		); !errors.Is(err, ErrLeaseNotCurrent) {
			t.Fatalf("disabled lease error = %v, want ErrLeaseNotCurrent", err)
		}
	})

	t.Run("revoked credential", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fixture := openAdmissionFixture(t, "main")
		issued := issueToken(
			t,
			fixture,
			"collector",
			testCollectorID,
			"main",
		)
		admitted := admitAuthorizationFixture(
			t,
			fixture,
			issued,
			"stream-1",
		)
		if _, err := fixture.tokens.RevokeCollectorToken(
			ctx,
			issued.Token.ID,
			issued.Token.Version,
		); err != nil {
			t.Fatalf("RevokeCollectorToken(): %v", err)
		}
		if _, err := fixture.store.AuthorizeLease(
			ctx,
			issued.Secret.Plaintext(),
			admitted.Lease,
			issued.Token.CreatedAt.Add(3*time.Minute),
		); !errors.Is(err, auth.ErrUnauthorized) {
			t.Fatalf("revoked token error = %v, want ErrUnauthorized", err)
		}
	})

	t.Run("collector binding mismatch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fixture := openAdmissionFixture(t, "main")
		issued := issueToken(
			t,
			fixture,
			"collector",
			testCollectorID,
			"main",
		)
		admitted := admitAuthorizationFixture(
			t,
			fixture,
			issued,
			"stream-1",
		)
		other := issueToken(
			t,
			fixture,
			"other collector",
			"123e4567-e89b-12d3-a456-426614174999",
			"main",
		)
		if _, err := fixture.store.AuthorizeLease(
			ctx,
			other.Secret.Plaintext(),
			admitted.Lease,
			issued.Token.CreatedAt.Add(3*time.Minute),
		); !errors.Is(err, auth.ErrUnauthorized) {
			t.Fatalf("binding mismatch error = %v, want ErrUnauthorized", err)
		}
	})

	t.Run("cross-tenant lease", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fixture := openAdmissionFixture(t, "main")
		issued := issueToken(
			t,
			fixture,
			"collector",
			testCollectorID,
			"main",
		)
		_ = admitAuthorizationFixture(
			t,
			fixture,
			issued,
			"stream-1",
		)
		request := admissionRequest(
			testCollectorID,
			"stream-tenant-b",
			issued.Token.CreatedAt.Add(2*time.Minute),
			"main",
		)
		request.Hello.AuthorizedIndexes = []string{"main"}
		_, otherLease, err := fixture.fleet.Claim(
			ctx,
			collectorfleet.ClaimRequest{
				Scope:       collectorfleet.Scope{TenantID: "tenant-b"},
				CollectorID: request.CollectorID,
				BootEpoch:   request.BootEpoch,
				StreamID:    request.StreamID,
				ReceivedAt:  request.AcceptedAt,
				Hello:       request.Hello,
			},
		)
		if err != nil {
			t.Fatalf("Claim(tenant-b): %v", err)
		}
		if _, err := fixture.store.AuthorizeLease(
			ctx,
			issued.Secret.Plaintext(),
			otherLease,
			issued.Token.CreatedAt.Add(3*time.Minute),
		); !errors.Is(err, ErrLeaseNotCurrent) {
			t.Fatalf("cross-tenant error = %v, want ErrLeaseNotCurrent", err)
		}
	})
}

func TestAuthorizeLeaseFailsClosedOnCorruptDurableLease(t *testing.T) {
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	admitted := admitAuthorizationFixture(
		t,
		fixture,
		issued,
		"stream-1",
	)
	connection, err := fixture.database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("SQLDB().Conn(): %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA ignore_check_constraints = ON`,
	); err != nil {
		t.Fatalf("enable corrupt fixture: %v", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		`UPDATE collector_runtime
		 SET active_instance_id = ''
		 WHERE tenant_id = ? AND collector_id = ?`,
		admitted.Lease.TenantID,
		admitted.Lease.CollectorID,
	); err != nil {
		t.Fatalf("corrupt durable lease: %v", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA ignore_check_constraints = OFF`,
	); err != nil {
		t.Fatalf("disable corrupt fixture: %v", err)
	}

	got, err := fixture.store.AuthorizeLease(
		ctx,
		issued.Secret.Plaintext(),
		admitted.Lease,
		issued.Token.CreatedAt.Add(2*time.Minute),
	)
	if err == nil ||
		errors.Is(err, ErrLeaseNotCurrent) ||
		strings.Contains(err.Error(), issued.Secret.Plaintext()) ||
		got.TokenID != "" {
		t.Fatalf(
			"AuthorizeLease(corrupt) = (%#v, %v), want sanitized hard failure",
			got,
			err,
		)
	}
}

func TestAuthorizeLeaseUsesCommitOrdering(t *testing.T) {
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	admitted := admitAuthorizationFixture(
		t,
		fixture,
		issued,
		"stream-1",
	)

	writer := fixture.database.GORMDB().WithContext(ctx).Begin()
	if writer.Error != nil {
		t.Fatalf("begin writer: %v", writer.Error)
	}
	writerFinished := false
	defer func() {
		if !writerFinished {
			_ = writer.Rollback().Error
		}
	}()
	if err := writer.Exec(
		`UPDATE ingestion_tokens
		 SET state = 'disabled'
		 WHERE ingestion_token_id = ?`,
		issued.Token.ID,
	).Error; err != nil {
		t.Fatalf("stage token disable: %v", err)
	}

	type authorizationResult struct {
		authentication auth.Authentication
		err            error
	}
	result := make(chan authorizationResult, 1)
	go func() {
		authentication, err := fixture.store.AuthorizeLease(
			ctx,
			issued.Secret.Plaintext(),
			admitted.Lease,
			issued.Token.CreatedAt.Add(2*time.Minute),
		)
		result <- authorizationResult{
			authentication: authentication,
			err:            err,
		}
	}()
	select {
	case beforeCommit := <-result:
		if beforeCommit.err != nil ||
			beforeCommit.authentication.TokenID != issued.Token.ID {
			t.Fatalf(
				"authorization before writer commit = (%#v, %v)",
				beforeCommit.authentication,
				beforeCommit.err,
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read authorization blocked behind uncommitted WAL writer")
	}
	if err := writer.Commit().Error; err != nil {
		t.Fatalf("commit token disable: %v", err)
	}
	writerFinished = true

	if _, err := fixture.store.AuthorizeLease(
		ctx,
		issued.Secret.Plaintext(),
		admitted.Lease,
		issued.Token.CreatedAt.Add(3*time.Minute),
	); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("authorization after writer commit = %v, want ErrUnauthorized", err)
	}
}

func TestAuthorizationHelpersShareOneUnfracturedSnapshot(t *testing.T) {
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	admitted := admitAuthorizationFixture(
		t,
		fixture,
		issued,
		"stream-1",
	)

	reader := fixture.database.GORMDB().WithContext(ctx).Begin(
		&sql.TxOptions{ReadOnly: true},
	)
	if reader.Error != nil {
		t.Fatalf("begin authorization snapshot: %v", reader.Error)
	}
	readerFinished := false
	defer func() {
		if !readerFinished {
			_ = reader.Rollback().Error
		}
	}()
	authentication, err := fixture.tokens.RevalidateCollectorInTransaction(
		ctx,
		reader,
		issued.Secret.Plaintext(),
		issued.Token.CreatedAt.Add(2*time.Minute),
	)
	if err != nil {
		t.Fatalf("RevalidateCollectorInTransaction(): %v", err)
	}

	writer := fixture.database.GORMDB().WithContext(ctx).Begin()
	if writer.Error != nil {
		t.Fatalf("begin state-change writer: %v", writer.Error)
	}
	writerFinished := false
	defer func() {
		if !writerFinished {
			_ = writer.Rollback().Error
		}
	}()
	if err := writer.Exec(
		`UPDATE ingestion_tokens
		 SET state = 'disabled'
		 WHERE ingestion_token_id = ?`,
		issued.Token.ID,
	).Error; err != nil {
		t.Fatalf("stage token disable: %v", err)
	}
	if err := writer.Exec(
		`UPDATE collector_fleet
		 SET administrative_state = 'disabled'
		 WHERE tenant_id = ? AND collector_id = ?`,
		admitted.Lease.TenantID,
		admitted.Lease.CollectorID,
	).Error; err != nil {
		t.Fatalf("stage fleet disable: %v", err)
	}
	if err := writer.Commit().Error; err != nil {
		t.Fatalf("commit state change: %v", err)
	}
	writerFinished = true

	current, err := fixture.fleet.IsCurrentLeaseInTransaction(
		ctx,
		reader,
		admitted.Lease,
	)
	if err != nil {
		t.Fatalf("IsCurrentLeaseInTransaction(): %v", err)
	}
	if authentication.TokenID != issued.Token.ID || !current {
		t.Fatalf(
			"fractured authorization snapshot: authentication=%#v current=%t",
			authentication,
			current,
		)
	}
	if err := reader.Commit().Error; err != nil {
		t.Fatalf("commit authorization snapshot: %v", err)
	}
	readerFinished = true

	if _, err := fixture.store.AuthorizeLease(
		ctx,
		issued.Secret.Plaintext(),
		admitted.Lease,
		issued.Token.CreatedAt.Add(3*time.Minute),
	); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("fresh snapshot error = %v, want ErrUnauthorized", err)
	}
}
