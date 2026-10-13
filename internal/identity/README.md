OpenSplunk uses GoCommon's Google OAuth, HttpOnly cookie sessions, users and
organization membership services. `Runtime.New` borrows the application-owned
PostgreSQL/GORM pool and initializes GoCommon's schema; `Runtime.Close` never
closes that pool. Native routes are served under `/api/identity`.

Google sign-in may create any verified Google account, with regular user type
and no team organizations or team memberships. GoCommon provisions a managed
personal workspace of the Basic tier 0 type; OpenSplunk lists only its Business
tier 0 team organizations and ignores the profile workspace root ID. Password registration/login/reset/addition,
native TOTP and email-domain association are disabled. Managed workspace creation
and deletion follow GoCommon’s normal coordinated lifecycle.
There is no administrator token or bootstrap workflow.

After the account exists, an operator can promote its exact user ID directly in
PostgreSQL:

```sql
UPDATE gocommon_users SET user_type = 44 WHERE id = <existing_google_user_id>;
```

`44` is `USER_TYPE_ADMIN_TIER_10`; only this recognized type is an OpenSplunk site
administrator. Restart the server after this manual database change, then reload
the browser. GoCommon's native token and user caches remain enabled with their
configured capacities and lifetime (defaults: 5,000 users and 15 minutes). The
existing session remains valid across the restart and reads the new user type.
`CurrentSubject`/`LookupSubject` still reload account type and ban/deletion status
for resource authorization, schedulers and long-lived work.

Viewer, Admin and Owner map to GoCommon roles 1, 23 and 44. Native role
transitions apply: Admins manage Viewers; Owners manage Admins and ownership
transitions. Site administrators retain native permission overrides.
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
GoCommon's sanitizer and transaction middleware, then composes its native
creation and privileged member-add operations in that same transaction. Native
ordinary/admin create endpoints are disabled so every application-created team
has an explicit initial Owner. Roots require a site administrator. Child
creation uses parent authority; a parent Admin selects an existing parent Owner
and remains an Admin in the child. Every initial child member must already belong
to its ancestors. The response contains `organization_id`, `name`, `slug`, and
optional `parent_organization_id`. The HTTP role boundary accepts only roles 1, 23 and 44 for ordinary and
privileged mutations, including organization invitations. The administrative
role-update route keeps native site permissions and codecs while invoking the
ordinary native generic update, preserving its last-Owner guard even for site
administrators. This installs no GoCommon role-policy callback.

`BeforeOrganizationDelete` must check OpenSplunk resources across the complete
subtree using the supplied ambient GORM transaction. With no callback, deletion
fails closed. Result storage uses OpenSplunk's `internal/objectstore` MinIO backend and its
dedicated operator-versioned private bucket. Create-only streamed PUT receipts
and version/ETag-pinned readers retain exact object identities; capture/export
journals verify checksums and authorize resources before reads or cleanup.
These application-specific guarantees do not widen GoCommon's attachment APIs
or initialize a partial GoCommon persistence handler.

Catalog scope uses the canonical PostgreSQL organization ID separately from the
physical ClickHouse tenant. Apps cannot move between organizations. Knowledge
objects and CSV lookup registries retain the app's organization, enforced by
PostgreSQL guards; immutable definition/version/projection data derives access
through current registry authority. GLOBAL sharing is limited to that organization.
Catalog cursors and knowledge mutation receipts bind the selected organization.
Site administrators bypass private resource ownership; browsing still selects an
organization so each catalog and query has one explicit scope.

GoCommon request transactions use its native boundaries on the borrowed pool.
Application policy mutations lock live user/organization/membership rows first,
then the specific index policy and validate its revision again. Native user type
and membership resolve on that SQL transaction through
`AuthorizationTransactionContext`. Scheduled/background `CurrentSubject`
lookups deliberately bypass an ambient request transaction.

Google OAuth and disabled email login are restart-bound GoCommon settings:
password and TOTP routes are absent at startup. GoCommon deliberately keeps
existing sessions valid when their login method is disabled. OpenSplunk's
adapter additionally validates the native signed session and current persisted
session row, accepting only `AUTH_PROVIDER_OAUTH` with the `google` slug. Both
native API and application authentication reject restored non-Google or revoked
sessions. This application boundary uses native validation/persistence methods;
it installs no GoCommon session-policy callback. Native user projections use
GoCommon's cache and its normal invalidation for application-managed mutations.

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


The borrowed pool has no transaction-start hook or application writer triggers on
GoCommon tables. Application invariants use explicit resource locks and native
constraints; see [the concurrency protocol](../../docs/architecture.md).
Organization resource insertion takes a live organization row lock. Deletion
locks the complete native subtree before checking application dependencies, so
soft deletion cannot race an insertion into a removed organization.
