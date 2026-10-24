"use client";

import { useCallback, useEffect, useId, useRef, useState, type FormEvent } from "react";

import type { AdminUser, IdentityClient, IdentityError, UserId } from "@/lib/api/identity";

import { AppIcon } from "../_components/app-icon";
import { BackendResourceState } from "../_components/backend-resource-state";
import { formatMediumDateTime } from "../_components/date-format";
import { FieldNote, fieldControlProps } from "../_components/field-validation";
import { useIdentity } from "../_components/identity-provider";
import { Modal } from "../_components/modal";
import { StatusLabel } from "../_components/status";
import {
  ACCESS_MODE_OPTIONS,
  EMPTY_CREATE_USER_DRAFT,
  MINIMUM_PASSWORD_LENGTH,
  SITE_DIRECTORY_PAGE_SIZE,
  USER_FILTERS,
  adminUserStatus,
  asIdentityError,
  authMethodsLabel,
  createUserErrorField,
  createUserInput,
  createUserSuccessMessage,
  hasErrors,
  isSiteAdmin,
  isUserFilter,
  pageAfterRemoval,
  presentMutationError,
  userActionCopy,
  userActionSuccessMessage,
  userActionsFor,
  userDisplayName,
  userQueryFor,
  validateCreateUserDraft,
  type CreateUserDraft,
  type CreateUserErrors,
  type UserAction,
  type UserFilter,
} from "./site-directory-data";
import {
  DirectoryErrorState,
  DirectoryFormError,
  DirectoryHeader,
  DirectoryPager,
  DirectoryRowMenu,
  DirectorySkeletonRows,
  DirectoryToast,
  SiteDirectoryAccessState,
  useDirectoryPage,
  useSiteDirectoryAccess,
} from "./site-directory-shared";

const USERS_TITLE = "Users";
const USERS_DESCRIPTION = "Every account on this server. Create accounts, grant site administrator access, and suspend or remove users.";
const USER_TABLE_COLUMNS = 6;

/** Focuses an element after the current commit, once a closing dialog has handed focus back. */
function focusAfterCommit(element: HTMLElement | null): void {
  globalThis.setTimeout(() => element?.focus(), 0);
}

export interface SiteUsersPanelProps {
  /** Rows per page; tests page a short directory with a smaller one. */
  readonly pageSize?: number;
}

/** The site-admin Users panel: gated on `users.admin`, then the directory. */
export function SiteUsersPanel({ pageSize = SITE_DIRECTORY_PAGE_SIZE }: SiteUsersPanelProps) {
  const access = useSiteDirectoryAccess("users");
  const headingId = useId();
  if (!access.allowed) {
    return (
      <section className="admin-section-stack" aria-labelledby={headingId}>
        <DirectoryHeader
          headingId={headingId}
          title={USERS_TITLE}
          description={USERS_DESCRIPTION}
          actionLabel="Create user"
          onAction={() => undefined}
          disabledReason={access.reason}
        />
        <SiteDirectoryAccessState subject="users" />
      </section>
    );
  }
  return <UsersDirectory client={access.client} headingId={headingId} pageSize={pageSize} selfId={access.selfId} />;
}

type UsersDialog =
  | { readonly kind: "create" }
  | { readonly kind: "action"; readonly action: UserAction; readonly user: AdminUser; readonly returnFocus: HTMLElement | null };

interface UsersDirectoryProps {
  readonly client: IdentityClient;
  readonly headingId: string;
  readonly pageSize: number;
  readonly selfId: UserId;
}

function UsersDirectory({ client, headingId, pageSize, selfId }: UsersDirectoryProps) {
  const identity = useIdentity();
  const [searchDraft, setSearchDraft] = useState("");
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<UserFilter>("all");
  const [pageNumber, setPageNumber] = useState(1);
  const [dialog, setDialog] = useState<UsersDialog | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const filterName = useId();

  const key = JSON.stringify([search, filter, pageNumber, pageSize]);
  const load = useCallback(
    () => client.adminListUsers(userQueryFor(search, filter), { limit: pageSize, page: pageNumber }),
    [client, filter, pageNumber, pageSize, search],
  );
  const directory = useDirectoryPage(key, load);
  const errorKind = directory.error?.kind ?? null;

  // A session that ended, or lost site-admin access, is the provider's news:
  // re-reading the principal moves the whole panel to the matching state.
  const { refresh } = identity;
  useEffect(() => {
    if (errorKind === "unauthenticated" || errorKind === "forbidden") refresh().catch(() => undefined);
  }, [errorKind, refresh]);

  function applySearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSearch(searchDraft.trim());
    setPageNumber(1);
  }

  function clearSearch() {
    setSearchDraft("");
    setSearch("");
    setFilter("all");
    setPageNumber(1);
    // Already clear (an invalid-search error with nothing typed): ask again.
    directory.reload();
    focusAfterCommit(searchRef.current);
  }

  function chooseFilter(value: string) {
    if (!isUserFilter(value)) return;
    setFilter(value);
    setPageNumber(1);
  }

  function finishAction(action: UserAction, user: AdminUser, outcome: { updated?: AdminUser; revoked?: number }) {
    setDialog(null);
    setToast(userActionSuccessMessage(action, user, outcome.revoked));
    if (action === "delete") {
      const page = directory.page;
      if (page !== null) setPageNumber(pageAfterRemoval(page.page, page.items.length - 1));
      directory.updateItems((items) => items.filter((item) => item.user.id !== user.user.id));
      directory.reload();
      focusAfterCommit(searchRef.current);
      return;
    }
    const { updated } = outcome;
    if (updated !== undefined) {
      directory.updateItems((items) => items.map((item) => (item.user.id === updated.user.id ? updated : item)));
    } else if (action === "revoke_sessions") {
      directory.updateItems((items) => items.map((item) => (item.user.id === user.user.id ? { ...item, activeSessions: 0 } : item)));
    }
    directory.reload();
    if (action === "revoke_sessions" && user.user.id === selfId) refresh().catch(() => undefined);
  }

  function closeAfterFailure(error: IdentityError | null) {
    setDialog(null);
    if (error === null) return;
    if (error.kind === "not_found" || error.kind === "conflict") directory.reload();
    if (error.kind === "unauthenticated" || error.kind === "forbidden") refresh().catch(() => undefined);
  }

  const page = directory.page;
  const busy = directory.status === "loading";
  const filtered = search.length > 0 || filter !== "all";

  let body;
  if (directory.status === "error" && directory.error !== null) {
    body = (
      <DirectoryErrorState
        error={directory.error}
        errorAt={directory.errorAt}
        subject="users"
        onRetry={directory.reload}
        onClearSearch={clearSearch}
      />
    );
  } else if (page === null) {
    body = (
      <div className="suite-card resource-table-card">
        <div className="table-wrap">
          <table className="table site-directory-users-table" aria-busy="true">
            <caption className="sr-only">Loading users…</caption>
            <UsersTableHead />
            <tbody><DirectorySkeletonRows columns={USER_TABLE_COLUMNS} /></tbody>
          </table>
        </div>
        <p className="sr-only" role="status">Loading users…</p>
      </div>
    );
  } else if (page.items.length === 0 && !busy) {
    body = filtered ? (
      <BackendResourceState
        kind="empty"
        title="No users match"
        message={search.length > 0 ? `Nothing matches “${search}” with this filter. Search by name, email, username, or an exact user ID.` : "No account matches this filter."}
        action={<button type="button" onClick={clearSearch}>Clear search</button>}
      />
    ) : (
      <BackendResourceState
        kind="empty"
        title="No users yet"
        message="Create the first account, or let people sign in through a configured identity provider."
        action={<button type="button" onClick={() => setDialog({ kind: "create" })}>Create user</button>}
      />
    );
  } else {
    body = (
      <div className="suite-card resource-table-card">
        <div className="table-wrap">
          <table className="table table--cards site-directory-users-table" aria-busy={busy}>
            <caption className="sr-only">Users{filtered ? " matching the current search" : ""}</caption>
            <UsersTableHead />
            <tbody>
              {page.items.map((admin) => (
                <UserRow
                  key={admin.user.id}
                  admin={admin}
                  self={admin.user.id === selfId}
                  onAction={(action, returnFocus) => setDialog({ action, kind: "action", returnFocus, user: admin })}
                  selfId={selfId}
                />
              ))}
            </tbody>
          </table>
        </div>
        <DirectoryPager label="Users pages" noun="users" page={page} busy={busy} onPage={setPageNumber} />
      </div>
    );
  }

  return (
    <section className="admin-section-stack" aria-labelledby={headingId}>
      <DirectoryHeader
        headingId={headingId}
        title={USERS_TITLE}
        description={USERS_DESCRIPTION}
        actionLabel="Create user"
        onAction={() => setDialog({ kind: "create" })}
        disabledReason={null}
      />
      <form className="site-directory-toolbar" role="search" aria-label="Search users" onSubmit={applySearch}>
        <label className="site-directory-search">
          <span className="sr-only">Search users</span>
          <i aria-hidden="true"><AppIcon name="search" size="sm" /></i>
          <input
            ref={searchRef}
            type="search"
            value={searchDraft}
            placeholder="Name, email, username or user ID"
            onChange={(event) => setSearchDraft(event.target.value)}
          />
        </label>
        <button className="button" type="submit">Search</button>
        <fieldset className="site-directory-filters">
          <legend className="sr-only">Show</legend>
          {USER_FILTERS.map((option) => (
            <label key={option.value} className={filter === option.value ? "is-selected" : undefined}>
              <input
                type="radio"
                name={filterName}
                value={option.value}
                checked={filter === option.value}
                onChange={(event) => chooseFilter(event.target.value)}
              />
              <span>{option.label}</span>
            </label>
          ))}
        </fieldset>
      </form>
      {body}

      {dialog?.kind === "create" ? (
        <CreateUserDialog
          client={client}
          onClose={() => setDialog(null)}
          onCreated={(created, accessMode) => {
            setDialog(null);
            setToast(createUserSuccessMessage(created, accessMode));
            setPageNumber(1);
            directory.reload();
          }}
        />
      ) : null}
      {dialog?.kind === "action" ? (
        <UserActionDialog
          action={dialog.action}
          admin={dialog.user}
          client={client}
          returnFocus={dialog.returnFocus}
          self={dialog.user.user.id === selfId}
          onClose={closeAfterFailure}
          onDone={(outcome) => finishAction(dialog.action, dialog.user, outcome)}
        />
      ) : null}
      <DirectoryToast message={toast} onDismiss={() => setToast(null)} />
    </section>
  );
}

function UsersTableHead() {
  return (
    <thead>
      <tr>
        <th scope="col">User</th>
        <th scope="col">Status</th>
        <th scope="col">Access</th>
        <th scope="col">Sessions</th>
        <th scope="col">Last sign-in</th>
        <th scope="col"><span className="sr-only">Actions</span></th>
      </tr>
    </thead>
  );
}

function UserRow({ admin, self, selfId, onAction }: {
  readonly admin: AdminUser;
  readonly self: boolean;
  readonly selfId: UserId;
  readonly onAction: (action: UserAction, returnFocus: HTMLElement | null) => void;
}) {
  const { user } = admin;
  const name = userDisplayName(user);
  const status = adminUserStatus(admin);
  const lastLogin = admin.lastLogin === undefined ? null : new Date(admin.lastLogin);
  return (
    <tr data-user-id={user.id}>
      <td data-label="User">
        <strong className="site-directory-name">{name}{self ? <span className="badge badge--outline">You</span> : null}</strong>
        <small className="table-secondary">{user.email} · @{user.username}</small>
      </td>
      <td data-label="Status"><StatusLabel tone={status.tone}>{status.label}</StatusLabel></td>
      <td data-label="Access">
        <span className="site-directory-access">
          {isSiteAdmin(user) ? <span className="badge badge--info">Site admin</span> : <span className="badge badge--neutral">User</span>}
          <small>{authMethodsLabel(admin.authMethods)}</small>
        </span>
      </td>
      <td data-label="Sessions" className="numeric-data">{admin.activeSessions}</td>
      <td data-label="Last sign-in">{formatMediumDateTime(lastLogin, "Never")}</td>
      <td className="site-directory-row-actions">
        <DirectoryRowMenu
          label={`Actions for ${name}`}
          items={userActionsFor(admin, selfId).map((item) => ({
            danger: item.danger,
            disabledReason: item.disabledReason,
            key: item.action,
            label: item.label,
            onSelect: (trigger) => onAction(item.action, trigger),
          }))}
        />
      </td>
    </tr>
  );
}

// --------------------------------------------------------------------------
// Dialogs
// --------------------------------------------------------------------------

interface UserActionDialogProps {
  readonly action: UserAction;
  readonly admin: AdminUser;
  readonly client: IdentityClient;
  readonly returnFocus: HTMLElement | null;
  readonly self: boolean;
  /** Cancelled, with the last failure (if any) so the panel can refresh what it invalidated. */
  readonly onClose: (lastError: IdentityError | null) => void;
  readonly onDone: (outcome: { updated?: AdminUser; revoked?: number }) => void;
}

async function runUserAction(
  client: IdentityClient,
  action: UserAction,
  userId: UserId,
  reason: string | undefined,
): Promise<{ updated?: AdminUser; revoked?: number }> {
  switch (action) {
    case "grant_site_admin":
      return { updated: await client.adminSetSiteAdmin(userId, true, reason) };
    case "revoke_site_admin":
      return { updated: await client.adminSetSiteAdmin(userId, false, reason) };
    case "ban":
      return { updated: await client.adminBanUser(userId, reason) };
    case "unban":
      return { updated: await client.adminUnbanUser(userId, reason) };
    case "revoke_sessions":
      return { revoked: await client.adminRevokeUserSessions(userId, reason) };
    case "delete":
      await client.adminDeleteUser(userId, reason);
      return {};
  }
}

function UserActionDialog({ action, admin, client, returnFocus, self, onClose, onDone }: UserActionDialogProps) {
  const copy = userActionCopy(action, admin, self);
  const [reason, setReason] = useState("");
  const [typed, setTyped] = useState("");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<IdentityError | null>(null);
  const formId = useId();
  const reasonId = useId();
  const typedId = useId();
  const confirmed = copy.typedConfirmation === null || typed.trim().toLowerCase() === copy.typedConfirmation.toLowerCase();

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!confirmed || pending) return;
    setPending(true);
    setError(null);
    try {
      const trimmed = reason.trim();
      const outcome = await runUserAction(client, action, admin.user.id, trimmed.length === 0 ? undefined : trimmed);
      onDone(outcome);
    } catch (caught) {
      setError(asIdentityError(caught));
      setPending(false);
    }
  }

  return (
    <Modal
      title={copy.title}
      subtitle={copy.description}
      dismissible={!pending}
      returnFocus={returnFocus}
      initialFocus={copy.typedConfirmation === null ? `[data-autofocus="reason"]` : `[data-autofocus="typed"]`}
      onClose={() => onClose(error)}
      footer={(
        <>
          <button className="button button--secondary" type="button" disabled={pending} onClick={() => onClose(error)}>Cancel</button>
          <button
            className={copy.danger ? "button button--danger" : "button button--primary"}
            type="submit"
            form={formId}
            disabled={!confirmed}
            aria-disabled={pending ? true : undefined}
          >
            {pending ? copy.pendingLabel : copy.confirmLabel}
          </button>
        </>
      )}
    >
      <form className="admin-form" id={formId} onSubmit={(event) => void submit(event)} aria-busy={pending}>
        <DirectoryFormError message={error === null ? null : presentMutationError(error)} />
        {copy.typedConfirmation === null ? null : (
          <label htmlFor={typedId}>
            <span>Type <code>{copy.typedConfirmation}</code> to confirm</span>
            <input
              id={typedId}
              data-autofocus="typed"
              autoComplete="off"
              value={typed}
              disabled={pending}
              onChange={(event) => setTyped(event.target.value)}
            />
          </label>
        )}
        <label htmlFor={reasonId}>
          <span>Reason (optional)</span>
          <textarea
            id={reasonId}
            data-autofocus="reason"
            value={reason}
            disabled={pending}
            maxLength={500}
            onChange={(event) => setReason(event.target.value)}
          />
          <small>Recorded with the change in the audit log.</small>
        </label>
      </form>
    </Modal>
  );
}

const CREATE_FIELD_IDS = {
  email: "site-users-create-email",
  firstName: "site-users-create-first-name",
  lastName: "site-users-create-last-name",
  password: "site-users-create-password",
  username: "site-users-create-username",
} as const;

function CreateUserDialog({ client, onClose, onCreated }: {
  readonly client: IdentityClient;
  readonly onClose: () => void;
  readonly onCreated: (created: AdminUser, accessMode: CreateUserDraft["accessMode"]) => void;
}) {
  const [draft, setDraft] = useState<CreateUserDraft>(EMPTY_CREATE_USER_DRAFT);
  const [attempted, setAttempted] = useState(false);
  const [pending, setPending] = useState(false);
  const [serverError, setServerError] = useState<IdentityError | null>(null);
  const formId = useId();
  const reasonId = useId();
  const accessName = useId();

  const clientErrors = attempted ? validateCreateUserDraft(draft) : {};
  const serverField = serverError === null ? null : createUserErrorField(serverError);
  const errors: CreateUserErrors = serverField === null || serverError === null
    ? clientErrors
    : { ...clientErrors, [serverField]: clientErrors[serverField] ?? serverError.message };
  const formMessage = serverError !== null && serverField === null
    ? presentMutationError(serverError)
    : attempted && hasErrors(clientErrors) ? "Fix the highlighted fields, then create the user again." : null;

  function update(patch: Partial<CreateUserDraft>) {
    setDraft((current) => ({ ...current, ...patch }));
    setServerError(null);
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (pending) return;
    setAttempted(true);
    if (hasErrors(validateCreateUserDraft(draft))) return;
    setPending(true);
    setServerError(null);
    try {
      const created = await client.adminCreateUser(createUserInput(draft));
      onCreated(created, draft.accessMode);
    } catch (caught) {
      setServerError(asIdentityError(caught));
      setPending(false);
    }
  }

  const field = (name: keyof typeof CREATE_FIELD_IDS) => fieldControlProps(CREATE_FIELD_IDS[name], errors[name] ?? null);

  return (
    <Modal
      title="Create user"
      subtitle="The account can sign in once its access is set up. Organization membership is managed per organization."
      wide
      dismissible={!pending}
      initialFocus={`#${CREATE_FIELD_IDS.email}`}
      onClose={onClose}
      footer={(
        <>
          <button className="button button--secondary" type="button" disabled={pending} onClick={onClose}>Cancel</button>
          <button className="button button--primary" type="submit" form={formId} aria-disabled={pending ? true : undefined}>
            {pending ? "Creating…" : "Create user"}
          </button>
        </>
      )}
    >
      <form className="admin-form" id={formId} noValidate onSubmit={(event) => void submit(event)} aria-busy={pending}>
        <DirectoryFormError message={formMessage} />
        <div className="site-directory-form-grid">
          <label htmlFor={CREATE_FIELD_IDS.email}>
            <span>Email</span>
            <input
              id={CREATE_FIELD_IDS.email}
              type="email"
              autoComplete="off"
              required
              value={draft.email}
              disabled={pending}
              onChange={(event) => update({ email: event.target.value })}
              {...field("email")}
            />
            <FieldNote fieldId={CREATE_FIELD_IDS.email} error={errors.email ?? null}>Where sign-in and invitation emails go.</FieldNote>
          </label>
          <label htmlFor={CREATE_FIELD_IDS.username}>
            <span>Username</span>
            <input
              id={CREATE_FIELD_IDS.username}
              autoComplete="off"
              required
              value={draft.username}
              disabled={pending}
              onChange={(event) => update({ username: event.target.value })}
              {...field("username")}
            />
            <FieldNote fieldId={CREATE_FIELD_IDS.username} error={errors.username ?? null}>3–32 letters, digits, dots, dashes or underscores.</FieldNote>
          </label>
          <label htmlFor={CREATE_FIELD_IDS.firstName}>
            <span>First name</span>
            <input
              id={CREATE_FIELD_IDS.firstName}
              autoComplete="off"
              value={draft.firstName}
              disabled={pending}
              onChange={(event) => update({ firstName: event.target.value })}
              {...field("firstName")}
            />
            <FieldNote fieldId={CREATE_FIELD_IDS.firstName} error={errors.firstName ?? null}>Optional.</FieldNote>
          </label>
          <label htmlFor={CREATE_FIELD_IDS.lastName}>
            <span>Last name</span>
            <input
              id={CREATE_FIELD_IDS.lastName}
              autoComplete="off"
              value={draft.lastName}
              disabled={pending}
              onChange={(event) => update({ lastName: event.target.value })}
              {...field("lastName")}
            />
            <FieldNote fieldId={CREATE_FIELD_IDS.lastName} error={errors.lastName ?? null}>Optional.</FieldNote>
          </label>
        </div>
        <fieldset disabled={pending}>
          <legend>How they sign in</legend>
          {ACCESS_MODE_OPTIONS.map((option) => (
            <label className="admin-checkbox" key={option.value}>
              <input
                type="radio"
                name={accessName}
                value={option.value}
                checked={draft.accessMode === option.value}
                onChange={() => update({ accessMode: option.value, password: "" })}
              />
              <span>{option.label}<small>{option.description}</small></span>
            </label>
          ))}
        </fieldset>
        {draft.accessMode === "password" ? (
          <label htmlFor={CREATE_FIELD_IDS.password}>
            <span>First password</span>
            <input
              id={CREATE_FIELD_IDS.password}
              type="password"
              autoComplete="new-password"
              value={draft.password}
              disabled={pending}
              onChange={(event) => update({ password: event.target.value })}
              {...field("password")}
            />
            <FieldNote fieldId={CREATE_FIELD_IDS.password} error={errors.password ?? null}>At least {MINIMUM_PASSWORD_LENGTH} characters.</FieldNote>
          </label>
        ) : null}
        <label className="admin-checkbox">
          <input
            type="checkbox"
            checked={draft.siteAdmin}
            disabled={pending}
            onChange={(event) => update({ siteAdmin: event.target.checked })}
          />
          <span>Site administrator<small>Every capability in every organization, plus users, organizations and server settings.</small></span>
        </label>
        <label htmlFor={reasonId}>
          <span>Reason (optional)</span>
          <textarea id={reasonId} value={draft.reason} disabled={pending} maxLength={500} onChange={(event) => update({ reason: event.target.value })} />
          <small>Recorded with the change in the audit log.</small>
        </label>
      </form>
    </Modal>
  );
}
