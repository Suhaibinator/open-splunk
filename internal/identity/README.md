OpenSplunk uses GoCommon's Google OAuth, HttpOnly cookie sessions, users and
organization membership services. `Runtime.New` borrows the application-owned
PostgreSQL/GORM pool and initializes GoCommon's schema; `Runtime.Close` never
closes that pool. Native routes are served under `/api/identity`.

Google sign-in may create any verified Google account, with regular user type
and no organizations or memberships. Password registration/login/reset/addition,
native TOTP, email-domain association and managed personal workspaces are disabled.
There is no administrator token or bootstrap workflow.

After the account exists, an operator can promote its exact user ID directly in
PostgreSQL:

```sql
UPDATE gocommon_users SET user_type = 44 WHERE id = <existing_google_user_id>;
```

`44` is `USER_TYPE_ADMIN_TIER_10`; only this recognized type is an OpenSplunk site
administrator. User projections are uncached, so committed changes affect the
next request using the existing session. Reloading the page refreshes `/user/me`
and updates the UI. `CurrentSubject`/`LookupSubject` also reload account type and
ban/deletion status for schedulers and long-lived work.

Viewer, Admin and Owner map to GoCommon roles 1, 23 and 44. Admins may manage
non-Owner peers; Owners and site administrators handle ownership transitions.
GoCommon retains operation checks, ancestor-membership prerequisites and
last-explicit-owner guards. A user-specific index allow never replaces owning-org
membership; index capabilities are evaluated by `internal/authorization`.

Account deletion uses GoCommon's user lifecycle callback in the native transaction.
It removes root and descendant memberships with GoCommon's last-Owner guard before
the stock account tombstone, OAuth removal and session revocation. Deleting a sole
Owner fails atomically; assign another Owner first. Successful deletion leaves no
active hierarchy memberships for that user.

Native organization creation has no initial-owner field. The application uses
`POST /api/identity/organizations/create-with-owner` with JSON
`{name, slug, description?, parent_organization_id?, owner_user_id}`. It invokes
GoCommon's sanitizer and transaction middleware, then its authorized creation
service and initial-membership policy. Roots require a site administrator. Child
creation uses parent authority; a parent Admin selects an existing parent Owner
and remains an Admin in the child. Every initial child member must already belong
to its ancestors. The response contains `organization_id`, `name`, `slug`, and
optional `parent_organization_id`. Missing-owner native create calls are rejected.

`BeforeOrganizationDelete` must check OpenSplunk resources across the complete
subtree using the supplied ambient GORM transaction. With no callback, deletion
fails closed. Result storage uses the versioned private bucket through
`Runtime.Persistence().PutObjectStreamVersion` and version/ETag-pinned readers; these storage
methods do not authorize resources themselves.

Catalog scope uses the canonical PostgreSQL organization ID separately from the
physical ClickHouse tenant. Apps cannot move between organizations. Knowledge
objects and CSV lookup registries retain the app's organization, enforced by
PostgreSQL guards; immutable definition/version/projection data derives access
through current registry authority. GLOBAL sharing is limited to that organization.
Catalog cursors and knowledge mutation receipts bind the selected organization.
Site administrators bypass private resource ownership; browsing still selects an
organization so each catalog and query has one explicit scope.

GoCommon request transactions acquire the application's schema-aware metadata
lock immediately after begin, before user/member row locks. ACL mutations use the
same protocol, then resolve the native user type and membership on that SQL
transaction through `AuthorizationTransactionContext`. Scheduled/background
`CurrentSubject` lookups deliberately bypass an ambient request transaction.

The shared session method policy accepts only `AUTH_PROVIDER_OAUTH` with the
`google` slug. It rejects email, phone/OTP and other OAuth session rows, including
restored rows, before native API or application authentication succeeds. This
policy also guards shared-facade session issuance and refresh.

Native collector and HEC credentials remain machine ingestion credentials.
Each records one immutable organization and its issuing user for audit. Issuing,
changing or enabling a credential requires the selected organization's Admin or
Owner, with current grants and ingestion permission for every scoped index.
Site administrators also select one organization when issuing credentials.
Credential authentication and durable HEC staging verify that every index still
belongs to that organization and permits ingestion. Moving an index fences old
credentials. Credentials do not depend on the issuing user's later membership;
an organization administrator can inspect, disable or revoke an owned credential
even after its old source index moves. HEC operational telemetry aggregates the
whole deployment and remains restricted to site administrators.
